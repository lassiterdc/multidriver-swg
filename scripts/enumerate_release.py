#!/usr/bin/env python3
"""Write one record of the bytes a release ships: its artifacts and three archives' members.

A release's byte record is written once, by the build that produced the artifacts, and read by
every later step of the release: the comparison against the release-state table, the scan of the
released content, and the acceptance read that closes the run. The record is one JSON object:

    schema: 1
    tag, commit, created (UTC, ISO 8601)
    whole_suite_run: {run_id, run_attempt, commit}
    artifacts: [{set, file, sha256, size}]
    entries:   [{set, path, sha256, size}]
    scan:      {status, predicate, corpus, date, counts}

`artifacts` names the one sdist and the one wheel in the dist directory, each with the digest and
size of the file itself. `entries` lists every member of three byte sets: the sdist, the wheel, and
the source archive that `git archive --format=tar {commit}` produces. The sdist's top directory is
stripped from its members' paths, so a path names the same file in the sdist and in the source
archive. The source archive is enumerated by its members only and carries no digest of its own,
because the tarball a hosting service serves for a tag is compressed at request time and its own
digest is not one this step can know.

A directory is a member too, for the two tar sets: its path ends in `/`, it carries no bytes, and
so its `sha256` is null and its `size` is 0. This is the shape that lets the comparison refuse an
archive carrying an empty directory of an excluded class, which is what the glob form of an
export-ignore line leaves behind. `git archive` reads export-ignore from the `.gitattributes` of
the archived commit and not from the working tree, so the set is what a reader of the tag's source
archive receives.

The `scan` object is written `not-run` with its other four fields null; the pre-release scan fills
it. A later step that reads this record therefore sees an explicit "no scan ran" rather than a
missing key.

Three grounds refuse a release, each printed as one line `refused: {id}: {detail}`, and the closing
line reads `release enumeration for {tag} at {commit}: {n} refusal(s)` or names what was written:

- `wrong-artifacts`: the dist directory does not hold exactly one sdist and one wheel.
- `run-not-at-commit`: the whole-suite run the gate read measured a different commit.
- `archive-failed`: an artifact or the commit's source archive could not be read, or holds a
  member naming a path outside its own root.

All grounds that can be evaluated are evaluated on every invocation, so one run reports every fact
it finds. The exit status is 0 when the record was written, 1 when anything was refused, and 2 on a
usage error or an output path that cannot be written.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import subprocess
import sys
import tarfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path

SCHEMA = 1
SDIST = "sdist"
WHEEL = "wheel"
SOURCE_ARCHIVE = "source-archive"
SDIST_SUFFIX = ".tar.gz"
WHEEL_SUFFIX = ".whl"


def _digest(data: bytes) -> str:
    """The lowercase hex sha256 of `data`, the digest form every record this release writes uses."""
    return hashlib.sha256(data).hexdigest()


def _entry(set_name: str, path: str, sha256: str | None, size: int) -> dict[str, object]:
    """One member of one set; a directory carries no bytes, so its digest is null and its size 0."""
    return {"set": set_name, "path": path, "sha256": sha256, "size": size}


def _escapes_root(path: str) -> bool:
    """Whether a member path names something outside the archive's own root."""
    parts = path.split("/")
    return path.startswith("/") or ".." in parts or parts[0].endswith(":")


def _file_digest(path: Path) -> tuple[str | None, int, str]:
    """A file's sha256 and size, and the reason they could not be read when the digest is None."""
    try:
        data = path.read_bytes()
    except OSError as error:
        return None, 0, f"{path} could not be read ({error})"
    return _digest(data), len(data), ""


def _artifact_files(dist: Path) -> tuple[list[tuple[str, Path]], str]:
    """The one sdist and one wheel in `dist`, as (set name, path) pairs, and why not when they are not."""
    if not dist.is_dir():
        return [], f"{dist} is not a directory; point --dist at the directory the build wrote"
    sdists = sorted(path for path in dist.iterdir() if path.is_file() and path.name.endswith(SDIST_SUFFIX))
    wheels = sorted(path for path in dist.iterdir() if path.is_file() and path.name.endswith(WHEEL_SUFFIX))
    if len(sdists) == 1 and len(wheels) == 1:
        return [(SDIST, sdists[0]), (WHEEL, wheels[0])], ""
    return [], (
        f"{dist} holds {len(sdists)} sdist(s) ({_names(sdists)}) and {len(wheels)} wheel(s) ({_names(wheels)}); "
        "a release ships exactly one of each, so enumerate the directory of a single build"
    )


def _names(paths: list[Path]) -> str:
    """The file names of `paths`, or `none`, so a refusal counts and names what it found."""
    return ", ".join(path.name for path in paths) if paths else "none"


def _artifact_entries(pairs: list[tuple[str, Path]]) -> tuple[list[dict[str, object]] | None, str]:
    """The artifacts' own digests and sizes, and why they could not be read when they are None."""
    entries = []
    for set_name, path in pairs:
        digest, size, problem = _file_digest(path)
        if digest is None:
            return None, f"the {set_name} {problem}"
        entries.append({"set": set_name, "file": path.name, "sha256": digest, "size": size})
    return entries, ""


def _tar_entry(
    archive: tarfile.TarFile, member: tarfile.TarInfo, path: str, set_name: str
) -> tuple[dict[str, object] | None, str]:
    """One tar member as an entry, or why it cannot be one."""
    if _escapes_root(path):
        return None, f"member {path!r} names a path outside the archive"
    if member.isdir():
        return _entry(set_name, path.rstrip("/") + "/", None, 0), ""
    if not member.isfile():
        return None, f"member {path!r} is neither a regular file nor a directory"
    extracted = archive.extractfile(member)
    if extracted is None:
        return None, f"member {path!r} holds no bytes"
    try:
        data = extracted.read()
    except (OSError, tarfile.TarError) as error:
        return None, f"member {path!r} could not be read ({error})"
    return _entry(set_name, path, _digest(data), len(data)), ""


def _sdist_entries(path: Path) -> tuple[list[dict[str, object]] | None, str]:
    """Every member of the sdist, its one top directory stripped from each path."""
    try:
        with tarfile.open(path, mode="r:gz") as archive:
            return _sdist_members(path, archive)
    except (OSError, tarfile.TarError) as error:
        return None, f"{path.name} could not be read as a gzip tar ({error})"


def _sdist_members(path: Path, archive: tarfile.TarFile) -> tuple[list[dict[str, object]] | None, str]:
    """The members of an opened sdist, with the archive's one top directory stripped from each path."""
    members = archive.getmembers()
    roots = {member.name.split("/", 1)[0] for member in members if member.name}
    if len(roots) != 1:
        return None, f"{path.name} holds members under {len(roots)} top-level names, and an sdist has one"
    root = next(iter(roots))
    if not root:
        return None, f"{path.name} holds a member at an absolute path"
    entries = []
    for member in members:
        name = member.name
        if name in (root, f"{root}/"):
            continue
        if not name.startswith(f"{root}/"):
            return None, f"{path.name} holds {name!r}, which is outside its top directory {root!r}"
        relative = name[len(root) + 1 :]
        if not relative:
            continue
        entry, problem = _tar_entry(archive, member, relative, SDIST)
        if problem:
            return None, f"{path.name} {problem}"
        entries.append(entry)
    return entries, ""


def _wheel_entries(path: Path) -> tuple[list[dict[str, object]] | None, str]:
    """Every member of the wheel, its paths relative to the archive root as the archive writes them."""
    try:
        archive = zipfile.ZipFile(path)
    except (OSError, zipfile.BadZipFile) as error:
        return None, f"{path.name} could not be read as a zip ({error})"
    with archive:
        entries = []
        for info in archive.infolist():
            name = info.filename
            if _escapes_root(name):
                return None, f"{path.name} holds {name!r}, which names a path outside the archive"
            if info.is_dir():
                entries.append(_entry(WHEEL, name.rstrip("/") + "/", None, 0))
                continue
            try:
                data = archive.read(info)
            except (OSError, RuntimeError, zipfile.BadZipFile) as error:
                return None, f"{path.name} holds {name!r} and could not read it ({error})"
            entries.append(_entry(WHEEL, name, _digest(data), len(data)))
        return entries, ""


def _source_archive(path: Path, commit: str) -> tuple[bytes | None, str]:
    """The tar `git archive` writes for `commit`, and why it could not be written when it is None."""
    try:
        completed = subprocess.run(
            ["git", "archive", "--format=tar", commit], cwd=path, capture_output=True, check=False
        )
    except OSError as error:
        return None, f"git archive of {commit} in {path} could not run ({error})"
    if completed.returncode == 0:
        return completed.stdout, ""
    stderr = completed.stderr.decode("utf-8", "replace")
    reason = next((line.strip() for line in stderr.splitlines() if line.strip()), f"git exited {completed.returncode}")
    return None, f"git archive of {commit} in {path} failed ({reason})"


def _source_archive_entries(data: bytes) -> tuple[list[dict[str, object]] | None, str]:
    """Every member of the commit's source archive, directory entries included."""
    try:
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
            return _source_archive_members(archive)
    except (OSError, tarfile.TarError) as error:
        return None, f"the commit's source archive could not be read as a tar ({error})"


def _source_archive_members(archive: tarfile.TarFile) -> tuple[list[dict[str, object]] | None, str]:
    """The members of an opened source archive, every one of them an entry of the record."""
    entries = []
    for member in archive.getmembers():
        if not member.name:
            continue
        entry, problem = _tar_entry(archive, member, member.name, SOURCE_ARCHIVE)
        if problem:
            return None, problem
        entries.append(entry)
    return entries, ""


def _build_record(
    arguments: argparse.Namespace, pairs: list[tuple[str, Path]]
) -> tuple[dict[str, object] | None, list[tuple[str, str]]]:
    """The record, or every archive ground that refused it."""
    artifacts, problem = _artifact_entries(pairs)
    if artifacts is None:
        return None, [("archive-failed", f"{problem}; build the artifacts again and enumerate that build")]

    problems: list[tuple[str, str]] = []
    entries: list[dict[str, object]] = []
    locations = dict(pairs)
    for path, reader in ((locations[SDIST], _sdist_entries), (locations[WHEEL], _wheel_entries)):
        members, member_problem = reader(path)
        if members is None:
            problems.append(("archive-failed", f"{member_problem}; rebuild the artifacts and enumerate that build"))
        else:
            entries.extend(members)

    data, problem = _source_archive(Path(arguments.repo), arguments.commit)
    if data is None:
        problems.append(("archive-failed", f"{problem}; archive the commit the release names from a full clone"))
    else:
        members, member_problem = _source_archive_entries(data)
        if members is None:
            problems.append(("archive-failed", f"{member_problem}; archive the commit the release names again"))
        else:
            entries.extend(members)

    if problems:
        return None, problems
    record: dict[str, object] = {
        "schema": SCHEMA,
        "tag": arguments.tag,
        "commit": arguments.commit,
        "created": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "whole_suite_run": {
            "run_id": arguments.run_id,
            "run_attempt": arguments.run_attempt,
            "commit": arguments.run_commit,
        },
        "artifacts": artifacts,
        "entries": entries,
        "scan": {"status": "not-run", "predicate": None, "corpus": None, "date": None, "counts": None},
    }
    return record, []


def _parse_arguments(argv: list[str] | None) -> argparse.Namespace:
    """The facts a release enumeration records, and where it writes the record."""
    parser = argparse.ArgumentParser(
        description="Write one record of the bytes a release ships: its artifacts and three archives' members."
    )
    parser.add_argument("--dist", required=True, help="the directory holding the built sdist and wheel")
    parser.add_argument("--repo", required=True, help="the repository `git archive` reads the commit from")
    parser.add_argument("--commit", required=True, help="the commit the release names")
    parser.add_argument("--tag", required=True, help="the tag the release names")
    parser.add_argument("--run-id", required=True, help="the whole-suite run the release gate read")
    parser.add_argument("--run-attempt", required=True, type=int, help="that run's attempt number")
    parser.add_argument("--run-commit", required=True, help="the commit that whole-suite run measured")
    parser.add_argument("--out", required=True, help="the file the record is written to")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Enumerate the release's bytes, print one line per refusal, and return the exit status."""
    arguments = _parse_arguments(argv)
    pairs, artifact_problem = _artifact_files(Path(arguments.dist))

    refusals: list[tuple[str, str]] = []
    if artifact_problem:
        refusals.append(("wrong-artifacts", artifact_problem))
    if arguments.run_commit != arguments.commit:
        refusals.append(
            (
                "run-not-at-commit",
                f"the whole-suite run measured {arguments.run_commit} and the release names {arguments.commit}; "
                f"tag the commit the gate's run measured, or run the gate again at {arguments.commit}",
            )
        )

    record = None
    if not artifact_problem:
        record, archive_problems = _build_record(arguments, pairs)
        refusals.extend(archive_problems)

    if refusals or record is None:
        for name, detail in refusals:
            print(f"refused: {name}: {detail}")
        print(f"release enumeration for {arguments.tag} at {arguments.commit}: {len(refusals)} refusal(s)")
        return 1

    out = Path(arguments.out)
    text = json.dumps(record, indent=2) + "\n"
    try:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text, encoding="utf-8")
    except OSError as error:
        print(f"cannot write {out}: {error}", file=sys.stderr)
        return 2
    print(
        f"release enumeration for {arguments.tag} at {arguments.commit}: "
        f"{len(record['artifacts'])} artifacts, {len(record['entries'])} entries, written to {out}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
