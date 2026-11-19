"""Tests of the identifiers recorder: the two files it writes, its five grounds, and its order.

The step runs after a release exists, so every service it reads is stood up here as one local HTTP
server that answers canned payloads and records the order it was asked. The server is real HTTP
served over a loopback socket and the script reaches it through `urllib`, so what these tests
exercise is the script's own request path and not a substituted one: the base URL of each service
is the script's flag, and that flag is the whole seam.

The checkout under test is a throwaway directory holding one citation file, because what the step
must get right is which keys of that file it owns and which it leaves alone, and a copy of the real
tree would make every test measure the real tree's prose instead.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "record_identifiers.py"
TAG_CHECK_SCRIPT = REPO_ROOT / "scripts" / "check_release_tag.py"

REFUSAL_LINE = re.compile(r"^refused: (?P<name>[a-z-]+): (?P<detail>.+)$")

ASSET_NAME = "release-enumeration.json"
ASSET = b'{"schema": 1, "tag": "v0.2.0.dev0", "entries": []}\n'
TAG = "v0.2.0.dev0"
COMMIT = "0123456789abcdef0123456789abcdef01234567"
ORIGIN = "https://github.com/example/multidriver-swg"
SLUG = "example/multidriver-swg"
VERSION_DOI = "10.5281/zenodo.10000001"
CONCEPT_DOI = "10.5281/zenodo.10000000"
REVISION = "1" * 40
DIRECTORY = "2" * 40
SAVE_PATH = f"/api/1/origin/save/git/url/{ORIGIN}/"
REVISION_PATH = f"/api/1/revision/{COMMIT}/"
RECORDS_PATH = "/api/records"
RELEASE_PATH = f"/repos/{SLUG}/releases/tags/{TAG}"
ASSET_PATH = "/assets/release-enumeration.json"

CITATION = """cff-version: 1.2.0
message: "If you use this software, please cite it using the metadata from this file."
type: software
title: "multidriver-swg"
version: "0.2.0.dev0"
authors:
  - given-names: Example
    family-names: Author
license: AGPL-3.0-or-later
repository-code: "https://github.com/example/multidriver-swg.git"
"""


def _digest(data: bytes) -> str:
    """The lowercase hex sha256 of `data`, the digest form the release's notes state."""
    return hashlib.sha256(data).hexdigest()


def _record(**changes):
    """One deposit record naming this origin at this tag, with `changes` applied over it."""
    record = {
        "doi": VERSION_DOI,
        "conceptdoi": CONCEPT_DOI,
        "metadata": {"related_identifiers": [{"identifier": f"{ORIGIN}/tree/{TAG}"}]},
    }
    record.update(changes)
    return record


class _Service:
    """One local stand-in for the three remote services, answering by method and path."""

    def __init__(self, base: str) -> None:
        self.base = base
        self.routes: dict[tuple[str, str], tuple[int, bytes]] = {}
        self.seen: list[tuple[str, str]] = []

    def route(self, method: str, path: str, status: int, payload: object) -> None:
        """Answer `method` on `path` with `status` and `payload`, a mapping or raw bytes."""
        body = payload if isinstance(payload, bytes) else json.dumps(payload).encode("utf-8")
        self.routes[(method, path)] = (status, body)

    def url(self, path: str) -> str:
        """The absolute URL of `path` on this server, which is what the release's asset names."""
        return f"{self.base}{path}"

    def answers(self, method: str) -> list[str]:
        """Every path `method` was asked on, in the order it was asked."""
        return [path for asked, path in self.seen if asked == method]


@pytest.fixture
def service():
    """A local server the script reaches through its three base-URL flags."""
    holder: dict[str, _Service] = {}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802  (the name is the protocol's)
            self._answer("GET")

        def do_POST(self) -> None:  # noqa: N802  (the name is the protocol's)
            self._answer("POST")

        def _answer(self, method: str) -> None:
            stand = holder["service"]
            stand.seen.append((method, self.path))
            missing = (404, b'{"message": "no route"}')
            status, body = stand.routes.get((method, urlparse(self.path).path), missing)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt: str, *args: object) -> None:
            """Silence the server's own access log, which no assertion here reads."""

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    holder["service"] = _Service(f"http://127.0.0.1:{server.server_address[1]}")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield holder["service"]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)


def _environment() -> dict[str, str]:
    """The child environment, every proxy bypassed so a loopback request never leaves the host."""
    return {**os.environ, "no_proxy": "*", "NO_PROXY": "*"}


def _accepted(stand: _Service, *, notes=None, asset=ASSET, revision=None) -> None:
    """Seed the server so every service a passing run reads answers it."""
    stand.route("POST", SAVE_PATH, 200, {"save_request_status": "accepted"})
    location = revision if revision is not None else {"id": REVISION, "directory": DIRECTORY}
    stand.route("GET", REVISION_PATH, 200, location)
    deposit = {"hits": {"hits": [_record()]}}
    stand.route("GET", RECORDS_PATH, 200, deposit)
    stated = notes if notes is not None else f"the enumeration hashes to {_digest(asset)}"
    attachment = {"name": ASSET_NAME, "browser_download_url": stand.url(ASSET_PATH)}
    stand.route("GET", RELEASE_PATH, 200, {"body": stated, "assets": [attachment]})
    stand.route("GET", ASSET_PATH, 200, asset)


def _run(
    stand: _Service,
    repo: Path,
    *extra: str,
    tag: str = TAG,
    commit: str = COMMIT,
    origin: str = ORIGIN,
) -> subprocess.CompletedProcess[str]:
    """Run the recorder against `stand`, writing into `repo`, and return the completed process."""
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--tag",
            tag,
            "--commit",
            commit,
            "--origin",
            origin,
            "--repo",
            str(repo),
            "--github-api",
            stand.base,
            "--zenodo-api",
            f"{stand.base}/api",
            "--swh-api",
            f"{stand.base}/api/1",
            *extra,
        ],
        capture_output=True,
        text=True,
        env=_environment(),
        check=False,
    )


def _citation(repo: Path) -> Path:
    """The one citation file of the throwaway checkout, holding `CITATION`'s content."""
    path = repo / "CITATION.cff"
    path.write_text(CITATION, encoding="utf-8")
    return path


def _refusals(stdout: str) -> list[tuple[str, str]]:
    """Every refusal line of a run, as (id, detail) pairs."""
    lines = [REFUSAL_LINE.match(line) for line in stdout.splitlines()]
    return [(found.group("name"), found.group("detail")) for found in lines if found]


def _summary(stdout: str) -> str:
    """The closing line of a run."""
    return stdout.strip().splitlines()[-1]


def _identifiers(text: str) -> list[str]:
    """The lines of the `identifiers` block of a citation file, the block's own key line included."""
    lines = text.splitlines()
    start = lines.index("identifiers:")
    end = start + 1
    while end < len(lines) and (not lines[end].strip() or lines[end].startswith(" ")):
        end += 1
    return lines[start:end]


def test_a_passing_run_writes_the_identifiers_and_the_record(service, tmp_path):
    """The concept DOI and the three identifiers land in the citation, and the asset lands beside it."""
    _accepted(service)
    citation = _citation(tmp_path)

    completed = _run(service, tmp_path)

    assert completed.returncode == 0, completed.stderr + completed.stdout
    text = citation.read_text(encoding="utf-8")
    lines = text.splitlines()
    assert lines.count(f'doi: "{CONCEPT_DOI}"') == 1
    assert lines.count("identifiers:") == 1
    block = _identifiers(text)
    assert f'    value: "{VERSION_DOI}"' in block
    assert f'    value: "swh:1:rev:{REVISION}"' in block
    assert f'    value: "swh:1:dir:{DIRECTORY}"' in block
    kinds = [line.strip() for line in block if line.strip().startswith("- type:")]
    assert kinds == ["- type: doi", "- type: swh", "- type: swh"]
    assert (tmp_path / "releases" / TAG / ASSET_NAME).read_bytes() == ASSET
    summary = _summary(completed.stdout)
    assert VERSION_DOI in summary and CONCEPT_DOI in summary
    assert f"swh:1:rev:{REVISION}" in summary and f"swh:1:dir:{DIRECTORY}" in summary
    assert _digest(ASSET) in summary


def test_every_key_the_step_does_not_own_reads_as_it_did(service, tmp_path):
    """A key this step does not own is carried through byte for byte, and gains no second copy."""
    _accepted(service)
    citation = _citation(tmp_path)

    completed = _run(service, tmp_path)

    assert completed.returncode == 0, completed.stderr + completed.stdout
    written = citation.read_text(encoding="utf-8").splitlines()
    for line in CITATION.splitlines():
        if line.strip():
            assert line in written
            assert written.count(line) == 1
    assert written.count('version: "0.2.0.dev0"') == 1


def test_the_archive_save_request_is_the_first_request_made(service, tmp_path):
    """The archival request precedes the revision read, the record read and the release read."""
    _accepted(service)
    _citation(tmp_path)

    completed = _run(service, tmp_path)

    assert completed.returncode == 0, completed.stderr + completed.stdout
    assert service.seen[0] == ("POST", SAVE_PATH)
    assert len(service.answers("POST")) == 1
    assert service.answers("GET")[0] == REVISION_PATH


def test_an_existing_doi_and_identifier_block_are_replaced_in_place(service, tmp_path):
    """A file already carrying the two keys gains no second copy of either."""
    _accepted(service)
    path = tmp_path / "CITATION.cff"
    older = 'doi: "10.5281/zenodo.99999999"\nidentifiers:\n'
    older += '  - type: url\n    value: "https://example.invalid/older"\n'
    older += 'references:\n  - type: software\n    title: "an older entry"\n'
    path.write_text(CITATION + older, encoding="utf-8")

    completed = _run(service, tmp_path)

    assert completed.returncode == 0, completed.stderr + completed.stdout
    lines = path.read_text(encoding="utf-8").splitlines()
    assert lines.count(f'doi: "{CONCEPT_DOI}"') == 1
    assert lines.count('doi: "10.5281/zenodo.99999999"') == 0
    assert lines.count("identifiers:") == 1
    assert "https://example.invalid/older" not in lines
    assert '    title: "an older entry"' in lines


def test_a_refused_save_request_refuses_the_run(service, tmp_path):
    """A save request the archive answers with an error is one refusal and no write."""
    _accepted(service)
    service.route("POST", SAVE_PATH, 500, {"message": "unavailable"})
    citation = _citation(tmp_path)

    completed = _run(service, tmp_path)

    assert completed.returncode == 1
    assert [name for name, _ in _refusals(completed.stdout)] == ["save-failed"]
    assert citation.read_text(encoding="utf-8") == CITATION
    assert not (tmp_path / "releases").exists()


def test_an_unreached_service_refuses_the_run(service, tmp_path):
    """A service that cannot be reached at all is a refusal and not a traceback."""
    _citation(tmp_path)

    completed = _run(service, tmp_path, "--swh-api", "http://127.0.0.1:1/api/1")

    assert completed.returncode == 1
    assert [name for name, _ in _refusals(completed.stdout)][0] == "save-failed"
    assert "Traceback" not in completed.stderr
    assert not (tmp_path / "releases").exists()


def test_an_unarchived_commit_refuses_the_run(service, tmp_path):
    """A revision the archive does not hold is one refusal, and nothing is written."""
    _accepted(service)
    service.route("GET", REVISION_PATH, 404, {"message": "not found"})
    citation = _citation(tmp_path)

    completed = _run(service, tmp_path)

    assert completed.returncode == 1
    assert [name for name, _ in _refusals(completed.stdout)] == ["archive-locator"]
    assert citation.read_text(encoding="utf-8") == CITATION
    assert not (tmp_path / "releases").exists()


def test_an_archived_revision_without_a_directory_refuses_the_run(service, tmp_path):
    """A revision carrying no directory locator cannot name where the bytes are, so it is refused."""
    _accepted(service, revision={"id": REVISION})
    _citation(tmp_path)

    completed = _run(service, tmp_path)

    assert completed.returncode == 1
    refusals = _refusals(completed.stdout)
    assert [name for name, _ in refusals] == ["archive-locator"]
    assert "directory" in refusals[0][1]


def test_a_deposit_record_for_another_origin_refuses_the_run(service, tmp_path):
    """A record whose related identifiers name another repository is not this release's record."""
    _accepted(service)
    elsewhere = {"related_identifiers": [{"identifier": "https://example.invalid/tree/v0.2.0.dev0"}]}
    service.route("GET", RECORDS_PATH, 200, {"hits": {"hits": [_record(metadata=elsewhere)]}})
    _citation(tmp_path)

    completed = _run(service, tmp_path)

    assert completed.returncode == 1
    assert [name for name, _ in _refusals(completed.stdout)] == ["zenodo-record"]


def test_two_deposit_records_for_the_origin_refuse_the_run(service, tmp_path):
    """Two records naming this origin at this tag leave no single DOI to cite, so the run refuses."""
    _accepted(service)
    service.route("GET", RECORDS_PATH, 200, {"hits": {"hits": [_record(), _record()]}})

    completed = _run(service, tmp_path)

    assert completed.returncode == 1
    refusals = _refusals(completed.stdout)
    assert [name for name, _ in refusals] == ["zenodo-record"]
    assert "2 record(s)" in refusals[0][1]


def test_a_record_without_a_concept_doi_refuses_the_run(service, tmp_path):
    """The concept DOI is what the citation names the tool by, so its absence is refused."""
    _accepted(service)
    record = _record()
    del record["conceptdoi"]
    service.route("GET", RECORDS_PATH, 200, {"hits": {"hits": [record]}})

    completed = _run(service, tmp_path)

    assert completed.returncode == 1
    refusals = _refusals(completed.stdout)
    assert [name for name, _ in refusals] == ["zenodo-record"]
    assert "concept doi" in refusals[0][1]


def test_a_tag_without_a_release_refuses_the_run(service, tmp_path):
    """A tag whose release does not exist yet is refused before anything is written."""
    _accepted(service)
    service.route("GET", RELEASE_PATH, 404, {"message": "not found"})
    citation = _citation(tmp_path)

    completed = _run(service, tmp_path)

    assert completed.returncode == 1
    assert [name for name, _ in _refusals(completed.stdout)] == ["release-absent"]
    assert citation.read_text(encoding="utf-8") == CITATION
    assert not (tmp_path / "releases").exists()


def test_a_release_without_the_enumeration_asset_refuses_the_run(service, tmp_path):
    """A release carrying no enumeration is refused, and the refusal names what it does carry."""
    _accepted(service)
    attachment = {"name": "something-else.json", "browser_download_url": service.url(ASSET_PATH)}
    service.route("GET", RELEASE_PATH, 200, {"body": "notes", "assets": [attachment]})

    completed = _run(service, tmp_path)

    assert completed.returncode == 1
    refusals = _refusals(completed.stdout)
    assert [name for name, _ in refusals] == ["release-absent"]
    assert "something-else.json" in refusals[0][1]


def test_an_asset_whose_digest_the_notes_do_not_state_refuses_the_run(service, tmp_path):
    """The notes state the asset's digest, so a digest they do not state means the asset moved."""
    _accepted(service, notes=f"the enumeration hashes to {_digest(b'other bytes')}")
    citation = _citation(tmp_path)

    completed = _run(service, tmp_path)

    assert completed.returncode == 1
    refusals = _refusals(completed.stdout)
    assert [name for name, _ in refusals] == ["asset-digest"]
    assert _digest(ASSET) in refusals[0][1]
    assert citation.read_text(encoding="utf-8") == CITATION
    assert not (tmp_path / "releases").exists()


def test_an_asset_that_cannot_be_downloaded_refuses_the_run(service, tmp_path):
    """An asset the release names but the host will not serve is one refusal and no write."""
    _accepted(service)
    service.route("GET", ASSET_PATH, 500, {"message": "unavailable"})

    completed = _run(service, tmp_path)

    assert completed.returncode == 1
    assert [name for name, _ in _refusals(completed.stdout)] == ["asset-digest"]
    assert not (tmp_path / "releases").exists()


def test_every_ground_that_can_be_evaluated_is_reported_once(service, tmp_path):
    """One run reports every ground it finds rather than stopping at the first."""
    service.route("POST", SAVE_PATH, 500, {"message": "unavailable"})
    service.route("GET", REVISION_PATH, 404, {"message": "not found"})
    service.route("GET", RECORDS_PATH, 200, {"hits": {"hits": []}})
    service.route("GET", RELEASE_PATH, 404, {"message": "not found"})

    completed = _run(service, tmp_path)

    assert completed.returncode == 1
    names = [name for name, _ in _refusals(completed.stdout)]
    assert names == ["save-failed", "archive-locator", "zenodo-record", "release-absent"]
    assert _summary(completed.stdout) == f"release identifiers for {TAG} at {COMMIT}: 4 refusal(s)"


def test_a_tag_that_names_a_path_is_a_usage_error(service, tmp_path):
    """A tag carrying a separator would write outside the releases directory, so it never runs."""
    _citation(tmp_path)

    completed = _run(service, tmp_path, tag="../v0.2.0.dev0")

    assert completed.returncode == 2
    assert service.seen == []
    assert not (tmp_path / "releases").exists()


def test_an_origin_that_names_no_repository_is_a_usage_error(service, tmp_path):
    """An origin URL without an owner and a repository names no release to read."""
    _citation(tmp_path)

    completed = _run(service, tmp_path, origin="multidriver-swg")

    assert completed.returncode == 2
    assert service.seen == []
    assert "owner and repository" in completed.stderr
