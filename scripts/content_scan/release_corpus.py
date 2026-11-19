#!/usr/bin/env python3
"""The pre-release scan: the scanner's reading contract over the bytes a release ships.

The push run reads the AUTHORED tree (`git ls-files`), and that is the wrong corpus for a
release: a credential reaches a published byte in a built sdist, in a wheel, or in the source
archive a hosting service serves for the tag, and none of those three is the working tree. So this
module takes its corpus from the release enumeration `scripts/enumerate_release.py` writes -- the
members of the sdist, the wheel and the commit's `git archive` -- reads each set's bytes from the
two sources the record names (`--dist`, and `git archive` in `--repo` at the record's commit), and
runs `core.scan` once per set over exactly the members the record lists.

Two classes run. `machine_path` REPORTED and refusing nothing, because a machine-specific path in
a public repository is a defect and not a release blocker. `secret` REFUSED, because no secret
reaches a published byte: that is section 4.8.1's invariant and the predicate this record states.
Both read BYTES, and the secret class is loaded byte-preserving here rather than in `secret.py`:
the push run's corpus is the authored tree's text files, and this one is a release's bytes, where a
member the text reader declines -- a NUL anywhere in it, or a byte UTF-8 cannot decode -- is still
a published byte and a credential in it is still a credential. The identity class is not here --
its labels are the private carrier's, which a release runner does not hold, and it fails closed
without one -- and neither is the planning-reference class, whose corpus is the repository's own
file list and not an archive's members.

The record's `scan` object is filled in place:

    status:     `clean` or `refused`
    predicate:  `secret == 0`, the condition the release is refused on
    corpus:     the sets ranged over, their member count, their file count, and the files read
    date:       when the scan ran (UTC, ISO 8601), because an undated scan cannot be compared to
                the next one and a count without it is zero by construction
    counts:     findings per class, so a reader sees what was looked for and what it found

Exit codes follow the scanner's: 0 clean, 1 the released bytes hold a secret, 2 the scan could not
report (an unreadable or inconsistent record, an artifact that is not in `--dist`, a member the
record names and its archive does not hold, a record whose members collide once written to one
directory, or a corpus of no file). The two are kept apart for the reason the core keeps them: a
refused release is not the same fact as a machine that never read its corpus, and only the first is
evidence about the bytes.

The located report -- a path, a line and a class label -- is printed only when the scan runs off a
GitHub runner, for the reason `core.count_only_in_force` states: a CI log of a public repository
is public, and a finding's location points at the public line holding the private string.
"""

from __future__ import annotations

import argparse
import io
import json
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path

try:  # imported as part of the scanner's package (the test suite, and `python3 -m`)
    from . import core, machine_path, secret
except ImportError:  # run as a directory: `python3 scripts/content_scan` puts this directory on sys.path
    import core  # type: ignore[no-redef]
    import machine_path  # type: ignore[no-redef]
    import secret  # type: ignore[no-redef]

SDIST = "sdist"
WHEEL = "wheel"
SOURCE_ARCHIVE = "source-archive"
# The record's closed domain of sets, fixed by `enumerate_release.py`. A set outside it is refused
# rather than skipped: a corpus missing one set reports a clean result that is indistinguishable
# from a true one, which is the vacuous green this scan exists to refuse.
SETS = (SDIST, WHEEL, SOURCE_ARCHIVE)
SDIST_SUFFIX = ".tar.gz"
WHEEL_SUFFIX = ".whl"

# What is looked for, in report order, and the one condition the release is refused on.
PREDICATE = "secret == 0"
REFUSING_CLASS = secret.NAME
STATUS_CLEAN = "clean"
STATUS_REFUSED = "refused"


@dataclass(frozen=True)
class Corpus:
    """The record's file members, grouped by set, and the member count including directories."""

    files: dict[str, list[str]]
    members: int

    @property
    def file_count(self) -> int:
        return sum(len(paths) for paths in self.files.values())


def read_record(path: Path) -> dict[str, object]:
    """The enumeration's one JSON object, or `core.UnreadableError` when it is not one."""
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise core.UnreadableError(f"the release enumeration {path} could not be read: {error.strerror}") from error
    except UnicodeDecodeError as error:
        raise core.UnreadableError(f"the release enumeration {path} is not UTF-8 text") from error
    except json.JSONDecodeError as error:
        raise core.UnreadableError(f"the release enumeration {path} is not JSON: {error}") from error
    if not isinstance(record, dict):
        raise core.UnreadableError(f"the release enumeration {path} holds a {type(record).__name__}, not an object")
    return record


def record_commit(record: dict[str, object]) -> str:
    """The commit the record names, whose `git archive` is the source-archive set."""
    commit = record.get("commit")
    if not isinstance(commit, str) or not commit:
        raise core.UnreadableError("the release enumeration names no commit, so its source archive cannot be taken")
    return commit


def _escapes_root(path: str) -> bool:
    """Whether a member path names something outside the archive's own root."""
    return path.startswith("/") or ".." in path.split("/")


def matchers() -> list[core.Matcher]:
    """The two classes this corpus is scanned by, each in the reading mode the corpus needs.

    `machine_path` already reads bytes. The secret class is loaded BYTE-PRESERVING here rather than
    in `secret.py`, because the two corpora are different kinds of thing: the push run reads text
    files from the authored tree, and this one reads the bytes a release ships. A member the text
    reader declines -- a NUL anywhere in it, or one byte it cannot decode as UTF-8 -- is still a
    published byte, so a class that skipped it would miss a credential in a data file, a notebook
    with a stray NUL, or a Latin-1 source, and would report `corpus.scanned` as a file it never
    judged. `core.read_bytes_or_none` maps every byte UTF-8 cannot decode to a lone surrogate, so
    nothing is dropped and the line numbers are the file's own newlines.

    The order is the report order: the class that reports, then the class that refuses.
    """
    return [machine_path.load(), replace(secret.load(), reads_bytes=True)]


def corpus_of(record: dict[str, object]) -> Corpus:
    """The record's members grouped by set, with every directory member counted and dropped.

    A directory member carries no bytes, so it is counted in `members` and is not part of the
    corpus the scan reads; the two numbers together are what makes the record's corpus field
    comparable to the next release's.
    """
    entries = record.get("entries")
    if not isinstance(entries, list) or not entries:
        raise core.UnreadableError("the release enumeration lists no entries, so there is nothing to scan")
    grouped: dict[str, list[str]] = {name: [] for name in SETS}
    for entry in entries:
        if not isinstance(entry, dict):
            raise core.UnreadableError(f"the release enumeration holds an entry that is a {type(entry).__name__}")
        set_name = entry.get("set")
        if set_name not in SETS:
            raise core.UnreadableError(
                f"the release enumeration names the set {set_name!r}, and this scan ranges over {list(SETS)}; "
                "a set it does not read would be reported clean without being read"
            )
        member = entry.get("path")
        if not isinstance(member, str) or not member:
            raise core.UnreadableError(f"the release enumeration holds a member of {set_name} with no path")
        if _escapes_root(member):
            raise core.UnreadableError(f"the release enumeration names {member!r}, which its own archive root excludes")
        if member.endswith("/"):
            continue
        grouped[set_name].append(member)
    return Corpus(files={name: paths for name, paths in grouped.items() if paths}, members=len(entries))


def artifact_in(dist: Path, set_name: str, suffix: str) -> Path:
    """The one file in `dist` this set's members are read from.

    Exactly one, because the enumeration of a release built once names one artifact per set: two
    candidates mean the directory holds another build's, and reading either would judge a release
    against bytes the record does not describe.
    """
    if not dist.is_dir():
        raise core.UnreadableError(f"{dist} is not a directory, so the {set_name} set cannot be read")
    found = sorted(path for path in dist.iterdir() if path.is_file() and path.name.endswith(suffix))
    if len(found) == 1:
        return found[0]
    names = ", ".join(path.name for path in found) if found else "none"
    raise core.UnreadableError(
        f"{dist} holds {len(found)} file(s) ending {suffix} ({names}); the {set_name} set is read from exactly "
        "one, so point --dist at the directory of the build this record enumerates"
    )


def _tar_payload(archive: tarfile.TarFile, path: Path) -> dict[str, bytes]:
    """Every regular file member of an opened tar, keyed by its own member name."""
    payload: dict[str, bytes] = {}
    for member in archive.getmembers():
        if not member.isfile():
            continue
        extracted = archive.extractfile(member)
        if extracted is None:
            raise core.UnreadableError(f"{path.name} holds {member.name!r}, which carries no bytes")
        payload[member.name] = extracted.read()
    return payload


def sdist_payload(path: Path) -> dict[str, bytes]:
    """The sdist's file members, its one top directory stripped from each name.

    The strip is what makes a path mean the same file in the sdist and in the source archive, and
    it is the shape the enumeration records, so the two are compared member for member.
    """
    try:
        with tarfile.open(path, mode="r:gz") as archive:
            members = archive.getmembers()
            roots = {member.name.split("/", 1)[0] for member in members if member.name}
            if len(roots) != 1:
                raise core.UnreadableError(
                    f"{path.name} holds members under {len(roots)} top-level names, and an sdist has one"
                )
            root = next(iter(roots))
            if not root:
                raise core.UnreadableError(f"{path.name} holds a member at an absolute path")
            payload = {}
            for member in members:
                if not member.isfile():
                    continue
                if not member.name.startswith(f"{root}/"):
                    raise core.UnreadableError(
                        f"{path.name} holds {member.name!r}, which is outside its top directory {root!r}"
                    )
                extracted = archive.extractfile(member)
                if extracted is None:
                    raise core.UnreadableError(f"{path.name} holds {member.name!r}, which carries no bytes")
                payload[member.name[len(root) + 1 :]] = extracted.read()
            return payload
    except (OSError, tarfile.TarError) as error:
        raise core.UnreadableError(f"{path.name} could not be read as a gzip tar ({error})") from error


def wheel_payload(path: Path) -> dict[str, bytes]:
    """The wheel's file members, keyed by the path the archive writes."""
    try:
        with zipfile.ZipFile(path) as archive:
            return {info.filename: archive.read(info) for info in archive.infolist() if not info.is_dir()}
    except (OSError, RuntimeError, zipfile.BadZipFile) as error:
        raise core.UnreadableError(f"{path.name} could not be read as a zip ({error})") from error


def source_archive_payload(repo: Path, commit: str) -> dict[str, bytes]:
    """The members of the tar `git archive` writes for `commit`, taken in `repo`.

    Read from the commit and not from the working tree, for the reason `enumerate_release.py`
    gives: `git archive` honours the `.gitattributes` of the commit being archived, so this is the
    set a reader of the tag's source archive receives.
    """
    try:
        completed = subprocess.run(
            ["git", "archive", "--format=tar", commit], cwd=repo, capture_output=True, check=False
        )
    except OSError as error:
        raise core.UnreadableError(f"git archive of {commit} in {repo} could not run ({error})") from error
    if completed.returncode != 0:
        stderr = completed.stderr.decode("utf-8", "replace")
        reason = next(
            (line.strip() for line in stderr.splitlines() if line.strip()), f"git exited {completed.returncode}"
        )
        raise core.UnreadableError(f"git archive of {commit} in {repo} failed ({reason})")
    try:
        with tarfile.open(fileobj=io.BytesIO(completed.stdout), mode="r:") as archive:
            return _tar_payload(archive, Path(commit))
    except (OSError, tarfile.TarError) as error:
        raise core.UnreadableError(f"the source archive of {commit} could not be read as a tar ({error})") from error


def payload_of(repo: Path, dist: Path, set_name: str, commit: str) -> dict[str, bytes]:
    """The bytes of one set, read from the source the record's own writer used for it."""
    if set_name == SDIST:
        return sdist_payload(artifact_in(dist, set_name, SDIST_SUFFIX))
    if set_name == WHEEL:
        return wheel_payload(artifact_in(dist, set_name, WHEEL_SUFFIX))
    return source_archive_payload(repo, commit)


def materialize(work: Path, set_name: str, payload: dict[str, bytes], members: list[str]) -> None:
    """Write the record's members of one set under `work`, refusing a member its archive lacks.

    A member the record names and its archive does not hold means the record and the bytes are two
    different releases, and the scan would otherwise read a corpus smaller than the one the record
    says it read.

    A name that collides with another member once written to a directory is refused the same way: a
    wheel may hold `sample/x` and `sample/x/y`, and the second write cannot land. `OSError` is
    mapped to `core.UnreadableError` here rather than left to escape, because the exception's status
    is 1 -- the number the scanner reserves for "the released bytes hold a secret" -- and a crafted
    archive would then be recorded as a secret finding, which is the one kind of evidence about the
    bytes this exit code must not manufacture.
    """
    for member in members:
        data = payload.get(member)
        if data is None:
            raise core.UnreadableError(
                f"the release enumeration names {member} in the {set_name} set, and that archive does not hold it"
            )
        target = work / member
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        except OSError as error:
            raise core.UnreadableError(
                f"the {set_name} set holds {member!r}, which cannot be written beside another of its own members "
                f"({error.strerror}); the record names a tree no single archive can hold"
            ) from error


def _merge(reports: list[core.Report]) -> core.Report:
    """One report over every set, with the classes summed and the files read added up.

    The sets are separate scans because a path must mean the same thing to the machine-path
    allowlist and to the scanner's own self-exclusion in each of them; the merge is what the count
    line and the record's `counts` are written from.
    """
    merged = core.Report()
    by_name: dict[str, core.ClassReport] = {}
    for report in reports:
        merged.scanned += report.scanned
        merged.skipped.extend(report.skipped)
        for found in report.classes:
            target = by_name.get(found.name)
            if target is None:
                by_name[found.name] = target = core.ClassReport(
                    name=found.name, labels=found.labels, loaded=found.loaded, ledger=found.ledger
                )
            target.findings.extend(found.findings)
            target.stale += found.stale
            target.kept += found.kept
    merged.classes.extend(by_name.values())
    return merged


def scan_release(repo: Path, dist: Path, record: dict[str, object]) -> tuple[core.Report, list[str], Corpus]:
    """Scan every set the record lists and return the merged report, the located lines, the corpus."""
    corpus = corpus_of(record)
    commit = record_commit(record)
    reports: list[core.Report] = []
    located: list[str] = []
    loaded = matchers()
    with tempfile.TemporaryDirectory(prefix="release-scan-") as scratch:
        for set_name, members in corpus.files.items():
            work = Path(scratch) / set_name
            materialize(work, set_name, payload_of(repo, dist, set_name, commit), members)
            report = core.scan(work, loaded, corpus=lambda _root, names=members: names)
            located.extend(f"{set_name}/{hit.render()}" for hit in report.hits)
            reports.append(report)
    return _merge(reports), located, corpus


def _refusing_count(report: core.Report) -> int:
    """The findings of the one class a release is refused on.

    Deliberately NOT `report.refused`: that is true of any finding in any class, and a
    machine-specific path is a defect this record reports and does not refuse.
    """
    return sum(len(found.findings) for found in report.classes if found.name == REFUSING_CLASS)


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def scanned_object(report: core.Report, corpus: Corpus, status: str) -> dict[str, object]:
    """The record's `scan` object: the predicate, the corpus, the date and the counts."""
    return {
        "status": status,
        "predicate": PREDICATE,
        "corpus": {
            "sets": sorted(corpus.files),
            "members": corpus.members,
            "files": corpus.file_count,
            "scanned": report.scanned,
        },
        "date": _now(),
        "counts": {found.name: len(found.findings) for found in report.classes},
    }


def _parse_arguments(argv: list[str] | None) -> argparse.Namespace:
    """The record to fill, and the two sources its sets' bytes are read from."""
    parser = argparse.ArgumentParser(
        description="Scan the bytes a release ships, as the release enumeration lists them, and refuse on a secret."
    )
    parser.add_argument("--enumeration", required=True, type=Path, help="the release enumeration to read and fill")
    parser.add_argument("--dist", required=True, type=Path, help="the directory holding the built sdist and wheel")
    parser.add_argument("--repo", required=True, type=Path, help="the repository `git archive` reads the commit from")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Scan the released bytes, write the record's `scan` object, and refuse on a secret."""
    arguments = _parse_arguments(argv)
    try:
        record = read_record(arguments.enumeration)
        report, located, corpus = scan_release(arguments.repo, arguments.dist, record)
    except core.UnreadableError as error:
        print(f"{core.COUNT_ONLY_PREFIX} {error}", file=sys.stderr)
        return core.EXIT_UNUSABLE

    if report.scanned == 0:
        # A record whose members are all directories, or a scan that opened no file, is the same
        # vacuous green a corpus of zero tokens produces: it is a run that checked nothing.
        print(f"{core.COUNT_ONLY_PREFIX} nothing to scan", file=sys.stderr)
        return core.EXIT_UNUSABLE

    secret_findings = _refusing_count(report)
    record["scan"] = scanned_object(report, corpus, STATUS_REFUSED if secret_findings else STATUS_CLEAN)
    try:
        arguments.enumeration.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    except OSError as error:
        print(
            f"{core.COUNT_ONLY_PREFIX} {arguments.enumeration} could not be written: {error.strerror}", file=sys.stderr
        )
        return core.EXIT_UNUSABLE

    if core.count_only_in_force(False):
        print(core.count_line(report))
        if secret_findings:
            print(
                f"{core.COUNT_ONLY_PREFIX} refused: {secret_findings} secret finding(s) in the released bytes",
                file=sys.stderr,
            )
        return core.EXIT_FINDINGS if secret_findings else core.EXIT_CLEAN

    for line in report.skipped:
        print(line, file=sys.stderr)
    print(core.count_line(report))
    if not secret_findings:
        return core.EXIT_CLEAN
    print(
        f"{core.COUNT_ONLY_PREFIX} refused: {secret_findings} secret finding(s) in the released bytes", file=sys.stderr
    )
    for line in located:
        print(f"  {line}", file=sys.stderr)
    return core.EXIT_FINDINGS


if __name__ == "__main__":
    sys.exit(main())
