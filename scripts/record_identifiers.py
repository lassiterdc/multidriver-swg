#!/usr/bin/env python3
"""Record a release's version DOI and archive locators, in the citation and beside the release.

A release's version DOI and its two archive locators exist only after the archive services have
seen the tag, at moments the publishing workflow cannot wait for. This step runs afterwards: it
asks the archive to save the origin, reads the archive's revision of the tagged commit, reads the
deposit record's DOIs, and downloads the enumeration the release carries as an asset. Only when
every one of those is in hand does it write the two files:

- `CITATION.cff` gains one top-level `doi` line carrying the concept DOI, and one `identifiers`
  list carrying the version DOI, the revision locator and the directory locator. Every other key of
  the file reads exactly as it did, and a key this step does not own is never rewritten.
- `releases/{tag}/release-enumeration.json` receives the bytes of the enumeration the release
  carries, so the same commit that records the identifiers also keeps the byte record.

The commit that carries those two files is made by the workflow that runs this step, which is the
part holding write permission; this step writes the files and nothing more.

The archival request is the FIRST request this step makes. Repeating it is harmless: the archive
answers an origin it already holds as it answers one it does not.

The identifier a citation uses is the version DOI; the concept DOI names the tool in general and
is never a release's identifier. The release's own `CITATION.cff` is never required to carry the
release's own identifier, because the tagged commit's hashes cover that file.

Five grounds refuse, each printed as one line `refused: {id}: {detail}`, and the closing line reads
`release identifiers for {tag} at {commit}: {n} refusal(s)` or names the two files written:

- `save-failed`: the archival request was refused, or could not be made.
- `archive-locator`: the archive holds no revision of the tagged commit, or that revision carries
  no directory.
- `zenodo-record`: no single deposit record names this origin at this tag with both a version DOI
  and a concept DOI.
- `release-absent`: the repository holds no release for the tag, or that release carries no
  `release-enumeration.json` asset.
- `asset-digest`: the downloaded asset's digest is not one the release's own notes state.

Every ground that can be evaluated is evaluated on every invocation, so one run reports every fact
it finds. Nothing is written unless every ground is clear, so a run that refuses leaves the
checkout exactly as it found it. The exit status is 0 when both files were written, 1 when anything
was refused, and 2 on a usage error or a write failure.

Every remote endpoint is a flag whose default is the real service, so the whole step runs against a
local server in a test without any network, and those flags are the only seam a test needs.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

CITATION_FILE = "CITATION.cff"
RELEASES_DIRECTORY = "releases"
ASSET_NAME = "release-enumeration.json"
DEFAULT_GITHUB_API = "https://api.github.com"
DEFAULT_ZENODO_API = "https://zenodo.org/api"
DEFAULT_SWH_API = "https://archive.softwareheritage.org/api/1"
TIMEOUT_SECONDS = 30
EXCERPT_LIMIT = 200

# The archive's own identifier syntax: a revision and a directory are both 40 lowercase hex
# characters, so anything else the archive answers with is not a locator a citation may carry.
SWH_HASH = re.compile(r"[0-9a-f]{40}")

# A tag names one directory under the releases directory, so a tag carrying a separator or `..`
# would write outside it; the released tag of this repository is always of this shape.
SAFE_TAG = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]*$")

# Any run of 64 hex characters in the notes, so the digest may be written with or without a label,
# in prose or in a list, and in either case.
DIGEST_IN_TEXT = re.compile(r"[0-9a-fA-F]{64}")

# A key at column 0. A mapping nested under another key is indented, so its keys are not these, and
# a `- type: doi` item of a list is indented too. That is what makes a block boundary findable in a
# file this step edits without a YAML reader, which is not available before a project environment.
TOP_LEVEL_KEY = re.compile(r"^(?P<key>[A-Za-z0-9_.-]+):")


def _request(method: str, url: str, data: bytes | None = None) -> tuple[int | None, bytes, str]:
    """One HTTP exchange's status and body, or a transport failure's reason with no status."""
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("Accept", "application/json")
    request.add_header("User-Agent", "multidriver-swg-record-identifiers")
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            return response.status, response.read(), ""
    except urllib.error.HTTPError as error:
        try:
            body = error.read()
        except OSError:
            body = b""
        return error.code, body or b"", ""
    except (urllib.error.URLError, OSError, TimeoutError) as error:
        return None, b"", f"{method} {url} could not be made ({error})"


def _excerpt(body: bytes) -> str:
    """The first line of a response body, so a refusal can quote what the service answered."""
    text = body.decode("utf-8", "replace").strip()
    first = text.splitlines()[0] if text else "no body"
    return first[:EXCERPT_LIMIT]


def _json_object(body: bytes) -> tuple[dict | None, str]:
    """A response body as one JSON object, and the reason it is not one when it is None."""
    try:
        value = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        return None, f"the response is not one JSON object ({error})"
    if not isinstance(value, dict):
        return None, "the response holds a JSON value that is not an object"
    return value, ""


def _origin_slug(origin: str) -> tuple[str | None, str]:
    """`owner/name` from a repository origin URL, and why it cannot be read when it is None."""
    trimmed = origin.strip().rstrip("/").removesuffix(".git")
    parts = [part for part in trimmed.replace(":", "/").split("/") if part]
    if len(parts) < 2:
        return None, f"{origin!r} names no owner and repository; pass the origin URL"
    return f"{parts[-2]}/{parts[-1]}", ""


def _save_request(archive: str, origin: str) -> str | None:
    """Ask the archive to save the origin, and refuse when the request did not reach it."""
    url = f"{archive.rstrip('/')}/origin/save/git/url/{origin.rstrip('/')}/"
    status, body, problem = _request("POST", url, data=b"")
    if problem:
        return f"{problem}; the origin was never saved, so no locator can be recorded"
    if status is None or not 200 <= status < 300:
        return f"the archive answered {status} to the save request for {origin} ({_excerpt(body)})"
    return None


def _locators(archive: str, commit: str) -> tuple[dict | None, str]:
    """The archived revision and directory of `commit`, and why they are absent when they are None."""
    url = f"{archive.rstrip('/')}/revision/{commit}/"
    status, body, problem = _request("GET", url)
    if problem:
        return None, f"{problem}; the archive's locator for {commit} cannot be read"
    if status == 404:
        return None, f"the archive holds no revision of {commit}; save the origin and wait for the run"
    if status is None or not 200 <= status < 300:
        return None, f"the archive answered {status} for {commit} ({_excerpt(body)})"
    revision, object_problem = _json_object(body)
    if revision is None:
        return None, f"the archive's revision of {commit} is not readable: {object_problem}"
    identifier = revision.get("id")
    directory = revision.get("directory")
    if not isinstance(identifier, str) or not SWH_HASH.fullmatch(identifier):
        return None, f"the archive's revision of {commit} carries no 40-hex id"
    if not isinstance(directory, str) or not SWH_HASH.fullmatch(directory):
        return None, f"the archive's revision of {commit} carries no 40-hex directory locator"
    return {"revision": f"swh:1:rev:{identifier}", "directory": f"swh:1:dir:{directory}"}, ""


def _names_origin(record: object, origin: str, tag: str) -> bool:
    """Whether one deposit record's related identifiers name `origin` at `tag`, which fixes what it deposits."""
    if not isinstance(record, dict):
        return False
    metadata = record.get("metadata")
    if not isinstance(metadata, dict):
        return False
    related = metadata.get("related_identifiers")
    if not isinstance(related, list):
        return False
    base = origin.strip().rstrip("/").removesuffix(".git")
    identifiers = [item.get("identifier") for item in related if isinstance(item, dict)]
    accepted = (f"{base}/tree/{tag}", f"{base}/releases/tag/{tag}")
    return any(isinstance(named, str) and named in accepted for named in identifiers)


def _dois(deposit: str, tag: str, origin: str) -> tuple[dict | None, str]:
    """The version and concept DOIs of one deposit record for `tag`, and why there is none."""
    query = urllib.parse.urlencode({"q": f'version:"{tag}"', "size": "100"})
    url = f"{deposit.rstrip('/')}/records?{query}"
    status, body, problem = _request("GET", url)
    if problem:
        return None, f"{problem}; the deposit record's DOIs cannot be read"
    if status is None or not 200 <= status < 300:
        return None, f"the deposit answered {status} for the records at {tag} ({_excerpt(body)})"
    payload, object_problem = _json_object(body)
    if payload is None:
        return None, f"the deposit's answer for {tag} is not readable: {object_problem}"
    hits = payload.get("hits")
    records = hits.get("hits") if isinstance(hits, dict) else None
    if not isinstance(records, list):
        return None, f"the deposit's answer for {tag} names no records"
    named = [record for record in records if _names_origin(record, origin, tag)]
    if len(named) != 1:
        return None, f"{len(named)} record(s) name {origin} at {tag} and the step needs exactly one"
    record = named[0]
    version = record.get("doi")
    concept = record.get("conceptdoi")
    if not isinstance(version, str) or not version:
        return None, f"the deposit record for {tag} carries no version doi"
    if not isinstance(concept, str) or not concept:
        return None, f"the deposit record for {tag} carries no concept doi"
    return {"version": version, "concept": concept}, ""


def _release(api: str, slug: str, tag: str) -> tuple[dict | None, str]:
    """The release for `tag` as one object, and why it cannot be read when it is None."""
    url = f"{api.rstrip('/')}/repos/{slug}/releases/tags/{urllib.parse.quote(tag, safe='')}"
    status, body, problem = _request("GET", url)
    if problem:
        return None, f"{problem}; the release of {tag} cannot be read"
    if status == 404:
        return None, f"{slug} holds no release for {tag}; create it with the enumeration attached"
    if status is None or not 200 <= status < 300:
        return None, f"{slug} answered {status} for the release of {tag} ({_excerpt(body)})"
    payload, object_problem = _json_object(body)
    if payload is None:
        return None, f"the release of {tag} is not readable: {object_problem}"
    return payload, ""


def _notes(payload: dict) -> str:
    """The release's notes as text, empty when the release carries none."""
    body = payload.get("body")
    return body if isinstance(body, str) else ""


def _enumeration_url(payload: dict) -> tuple[str | None, str]:
    """The download URL of the release's one enumeration asset, and why there is none."""
    assets = payload.get("assets")
    if not isinstance(assets, list):
        return None, f"the release names no assets; attach {ASSET_NAME} when creating it"
    named = [asset for asset in assets if isinstance(asset, dict) and asset.get("name") == ASSET_NAME]
    urls = [asset.get("browser_download_url") or asset.get("url") for asset in named]
    urls = [url for url in urls if isinstance(url, str) and url]
    if not urls:
        carried = ", ".join(sorted({str(asset.get("name")) for asset in assets if isinstance(asset, dict)}))
        return None, f"the release carries no {ASSET_NAME} asset (it carries {carried or 'none'})"
    if len(urls) > 1:
        return None, f"the release carries {len(urls)} {ASSET_NAME} assets and needs one"
    return urls[0], ""


def _enumeration(url: str, notes: str) -> tuple[bytes | None, str, str]:
    """The enumeration's bytes, their digest, and why they cannot be recorded when they are None."""
    status, body, problem = _request("GET", url)
    if problem:
        return None, "", f"{problem}; the enumeration the release carries cannot be downloaded"
    if status is None or not 200 <= status < 300:
        return None, "", f"the enumeration asset answered {status} ({_excerpt(body)})"
    digest = hashlib.sha256(body).hexdigest()
    stated = sorted({run.lower() for run in DIGEST_IN_TEXT.findall(notes)})
    if digest not in stated:
        detail = f"the asset hashes to {digest} and the notes state {', '.join(stated) or 'no digest'}"
        return None, "", detail
    return body, digest, ""


def _spanned_block(lines: list[str], key: str) -> tuple[int, int] | None:
    """The half-open line range of the top-level `key` block, or None when no such key is present."""
    for index, line in enumerate(lines):
        matched = TOP_LEVEL_KEY.match(line)
        if matched is None or matched.group("key") != key:
            continue
        end = index + 1
        while end < len(lines) and not TOP_LEVEL_KEY.match(lines[end]) and not lines[end].startswith("#"):
            end += 1
        while end > index + 1 and not lines[end - 1].strip():
            end -= 1
        return index, end
    return None


def _set_block(lines: list[str], key: str, rendered: list[str]) -> list[str]:
    """`lines` with the top-level `key` block replaced by `rendered`, or appended when absent."""
    found = _spanned_block(lines, key)
    if found is None:
        return [*lines, *rendered] if not lines or not lines[-1].strip() else [*lines, "", *rendered]
    start, end = found
    return [*lines[:start], *rendered, *lines[end:]]


def _identifier_lines(version_doi: str, locators: dict) -> list[str]:
    """The `identifiers` block: the version DOI, the revision locator and the directory locator."""
    return [
        "identifiers:",
        "  - type: doi",
        f'    value: "{version_doi}"',
        '    description: "the version this release published"',
        "  - type: swh",
        f'    value: "{locators["revision"]}"',
        '    description: "the archived revision of the tagged commit"',
        "  - type: swh",
        f'    value: "{locators["directory"]}"',
        '    description: "the archived directory of the tagged commit"',
    ]


def _citation_text(text: str, dois: dict, locators: dict) -> str:
    """The citation file with its concept DOI and its identifiers list set, every other key kept."""
    lines = text.splitlines()
    lines = _set_block(lines, "doi", [f'doi: "{dois["concept"]}"'])
    lines = _set_block(lines, "identifiers", _identifier_lines(dois["version"], locators))
    return "\n".join(lines) + "\n"


def _parse_arguments(argv: list[str] | None) -> argparse.Namespace:
    """The release to record, the checkout to write in, and the three services to read."""
    parser = argparse.ArgumentParser(
        description="Record a release's version DOI and archive locators, in the citation and beside it."
    )
    parser.add_argument("--tag", required=True, help="the released tag, for example v0.2.0.dev0")
    parser.add_argument("--commit", required=True, help="the commit the tag names")
    parser.add_argument("--origin", required=True, help="the repository's public origin URL")
    parser.add_argument("--repo", default=".", help="the checkout the two files are written in")
    parser.add_argument("--citation", default=CITATION_FILE, help="the citation file, relative to --repo")
    parser.add_argument("--releases", default=RELEASES_DIRECTORY, help="the directory the record is kept in")
    parser.add_argument("--github-api", default=DEFAULT_GITHUB_API, help="the host serving the release API")
    parser.add_argument("--zenodo-api", default=DEFAULT_ZENODO_API, help="the host serving the deposit API")
    parser.add_argument("--swh-api", default=DEFAULT_SWH_API, help="the host serving the archive API")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Read the identifiers, write the two files, and return the exit status."""
    arguments = _parse_arguments(argv)
    if SAFE_TAG.match(arguments.tag) is None:
        print(f"cannot record identifiers for {arguments.tag!r}: a tag names one directory", file=sys.stderr)
        return 2
    slug, slug_problem = _origin_slug(arguments.origin)
    if slug is None:
        print(f"cannot record identifiers for {arguments.tag}: {slug_problem}", file=sys.stderr)
        return 2

    refusals: list[tuple[str, str]] = []

    save_problem = _save_request(arguments.swh_api, arguments.origin)
    if save_problem is not None:
        refusals.append(("save-failed", save_problem))

    locators, locator_problem = _locators(arguments.swh_api, arguments.commit)
    if locators is None:
        refusals.append(("archive-locator", locator_problem))

    dois, doi_problem = _dois(arguments.zenodo_api, arguments.tag, arguments.origin)
    if dois is None:
        refusals.append(("zenodo-record", doi_problem))

    payload, release_problem = _release(arguments.github_api, slug, arguments.tag)
    if payload is None:
        refusals.append(("release-absent", release_problem))

    url = None
    if payload is not None:
        url, url_problem = _enumeration_url(payload)
        if url is None:
            refusals.append(("release-absent", url_problem))

    data: bytes | None = None
    digest = ""
    if payload is not None and url is not None:
        data, digest, asset_problem = _enumeration(url, _notes(payload))
        if data is None:
            refusals.append(("asset-digest", asset_problem))

    if refusals or locators is None or dois is None or data is None:
        for name, detail in refusals:
            print(f"refused: {name}: {detail}")
        print(f"release identifiers for {arguments.tag} at {arguments.commit}: {len(refusals)} refusal(s)")
        return 1

    citation_path = Path(arguments.repo) / arguments.citation
    record_path = Path(arguments.repo) / arguments.releases / arguments.tag / ASSET_NAME
    try:
        text = citation_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        print(f"cannot read {citation_path}: {error}", file=sys.stderr)
        return 2
    updated = _citation_text(text, dois, locators)
    staged = citation_path.with_name(citation_path.name + ".tmp")
    try:
        record_path.parent.mkdir(parents=True, exist_ok=True)
        staged.write_text(updated, encoding="utf-8")
        record_path.write_bytes(data)
        staged.replace(citation_path)
    except OSError as error:
        staged.unlink(missing_ok=True)
        print(f"cannot write the identifiers ({error})", file=sys.stderr)
        return 2

    print(
        f"release identifiers for {arguments.tag} at {arguments.commit}: "
        f"version doi {dois['version']}, concept doi {dois['concept']}, {locators['revision']}, "
        f"{locators['directory']}, asset {digest} written to {citation_path} and {record_path}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
