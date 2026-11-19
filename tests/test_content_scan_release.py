"""Tests of the pre-release scan: the released bytes, the one class that refuses, and the record.

The scan is the release's only check on the bytes rather than on the tag or the table. It reads the
three byte sets the release enumeration lists -- the sdist, the wheel and the tagged commit's
source archive -- and it refuses the release on a SECRET in any of them. A machine-specific path is
a defect and not a release blocker, so its count is reported and refuses nothing. Those two facts,
the five fields the record gains, the count-only report a public log gets, and the refusal of a
record that disagrees with its own archives are what this file holds.

Two of the tests build their own release: a throwaway repository, an sdist and a wheel of the same
members, and the enumeration `scripts/enumerate_release.py` writes over them. The script is driven
as a subprocess, as the release workflow drives it, so the interpreter's path setup, the exit codes
and the printed lines are all the ones the runner sees; the one test that builds a real artifact
builds from a copy of this checkout, because what it asserts is that the bytes this repository
actually ships carry no secret.

The samples are assembled from adjacent string literals. A tracked file of this repository must
itself pass the scan that runs on every push, and a whole PEM header or a home-directory path
written out on one line would red the guard this scan shares its patterns with.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "content_scan" / "release_corpus.py"
ENUMERATOR = REPO_ROOT / "scripts" / "enumerate_release.py"

IDENTITY = ("-c", "user.name=Release Scan", "-c", "user.email=release-scan@example.invalid")

# The sample shapes, split so no line of this file carries one whole. The PEM header is the secret
# class's first label and the path is the machine-path class's first alternation; both classes are
# run over every byte set, so one line of each proves the two arms apart.
PEM_HEADER = "-----BEGIN " + "RSA PRIVATE KEY-----"
MACHINE_PATH = "/home/" + "sampleuser/private/notes.txt"

DATE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

# The record's five scan fields, in the order the record writes them.
SCAN_FIELDS = ("status", "predicate", "corpus", "date", "counts")

# The three sets the record fixes, sorted, and the number of findings one planted line produces in
# a release that ships the same file in all three.
SETS = ["sdist", "source-archive", "wheel"]
THREE = 3


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _tar_directory(archive: tarfile.TarFile, name: str) -> None:
    info = tarfile.TarInfo(name)
    info.type = tarfile.DIRTYPE
    info.mode = 0o755
    archive.addfile(info)


def _sdist(path: Path, members: dict[str, bytes], top: str = "sample-1.0.0") -> Path:
    """A gzip tar holding `members` under `top`, with the directory entries a built sdist carries."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(path, "w:gz") as archive:
        _tar_directory(archive, top)
        added = set()
        for name, data in sorted(members.items()):
            parts = name.split("/")[:-1]
            for depth in range(1, len(parts) + 1):
                parent = f"{top}/" + "/".join(parts[:depth])
                if parent not in added:
                    _tar_directory(archive, parent)
                    added.add(parent)
            info = tarfile.TarInfo(f"{top}/{name}")
            info.size = len(data)
            info.mode = 0o644
            archive.addfile(info, io.BytesIO(data))
    return path


def _wheel(path: Path, members: dict[str, bytes]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        for name, data in sorted(members.items()):
            archive.writestr(name, data)
    return path


def _git(repo: Path, *arguments: str) -> str:
    completed = subprocess.run(["git", *IDENTITY, *arguments], cwd=repo, capture_output=True, text=True, check=False)
    assert completed.returncode == 0, f"git {' '.join(arguments)}: {completed.stderr}"
    return completed.stdout


def _repository(path: Path, members: dict[str, bytes]) -> str:
    """A throwaway repository holding `members` in one commit, whose sha is returned."""
    path.mkdir(parents=True, exist_ok=True)
    _git(path, "init", "--quiet", "-b", "main")
    for name, data in members.items():
        target = path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    _git(path, "add", "--all")
    _git(path, "commit", "--quiet", "-m", "the release")
    return _git(path, "rev-parse", "HEAD").strip()


def _enumerate(dist: Path, repo: Path, record: Path) -> None:
    """Write the release enumeration over `dist` and `repo`, as the release workflow does."""
    commit = _git(repo, "rev-parse", "HEAD").strip()
    completed = subprocess.run(
        [
            sys.executable,
            str(ENUMERATOR),
            "--dist",
            str(dist),
            "--repo",
            str(repo),
            "--commit",
            commit,
            "--tag",
            "v1.0.0",
            "--run-id",
            "1234567890",
            "--run-attempt",
            "1",
            "--run-commit",
            commit,
            "--out",
            str(record),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def _release(root: Path, members: dict[str, bytes]) -> tuple[Path, Path, Path]:
    """A release of one member set: the record, the `dist/` directory and the repository.

    The same members reach all three byte sets, so one planted line is one finding per set -- which
    is the fact the counts below assert, and the difference between counting bytes and counting
    paths.
    """
    repo = root / "repo"
    dist = root / "dist"
    _repository(repo, members)
    _sdist(dist / "sample-1.0.0.tar.gz", members)
    _wheel(dist / "sample-1.0.0-py3-none-any.whl", {f"sample/{name}": data for name, data in members.items()})
    record = root / "release-enumeration.json"
    _enumerate(dist, repo, record)
    return record, dist, repo


def _run(record: Path, dist: Path, repo: Path, *, runner: bool = False) -> subprocess.CompletedProcess:
    """One invocation of the scan, in the form the release workflow gives it.

    `GITHUB_ACTIONS` is always removed from the child's environment and set only when a test asks
    for the runner form, so the located and the count-only reports are two deliberate choices here
    and not whatever the machine running the suite happens to export.
    """
    environment = {name: value for name, value in os.environ.items() if name != "GITHUB_ACTIONS"}
    if runner:
        environment["GITHUB_ACTIONS"] = "true"
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--enumeration",
            str(record),
            "--dist",
            str(dist),
            "--repo",
            str(repo),
        ],
        capture_output=True,
        text=True,
        check=False,
        env=environment,
    )


def _scan_of(record: Path) -> dict[str, object]:
    """The record's `scan` object as the file now holds it."""
    document = json.loads(record.read_text(encoding="utf-8"))
    assert isinstance(document, dict), document
    scan = document.get("scan")
    assert isinstance(scan, dict), scan
    return scan


def _extract(repo: Path, commit: str, into: Path) -> None:
    """Write the regular files of `commit`'s `git archive` under `into`."""
    completed = subprocess.run(["git", "archive", "--format=tar", commit], cwd=repo, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr.decode("utf-8", "replace")
    with tarfile.open(fileobj=io.BytesIO(completed.stdout), mode="r:") as archive:
        for member in archive.getmembers():
            if not member.isfile():
                continue
            extracted = archive.extractfile(member)
            assert extracted is not None, member.name
            target = into / member.name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(extracted.read())


def _build(copy: Path, flag: str, dist: Path) -> None:
    """Build one artifact from `copy` offline, with the `build` and `setuptools` the suite's group pins."""
    completed = subprocess.run(
        [sys.executable, "-m", "build", flag, "--no-isolation", "--outdir", str(dist)],
        cwd=copy,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_this_checkouts_sdist_and_source_archive_carry_no_secret(tmp_path):
    """Property: the bytes this repository actually releases -- an sdist built from a `git archive`
    copy of the checkout, the wheel built beside it, and the commit's source archive -- hold no
    secret, so the record of a release of this tree says `clean`.

    Class: NEW CAPABILITY — plausible wrong implementations are a scan that reads the authored tree
    instead of the archives, which cannot see a file `.gitattributes` removes from the source
    archive; one that reads an archive's member NAMES without their bytes; and a corpus of zero
    files, which reports zero findings over nothing. The first two keep the exit status of a clean
    release and the third does too; the test is red on the counts and the scanned figure.

    Why: this is the only assertion in the suite with the shipped bytes as its corpus, and a guard
    that only ever runs over a fixture proves nothing about the release it exists to stop.

    Killed by: pointing the scan at the tree, dropping the archive reads, or reporting a scan that
    opened no file as clean. Second correct implementation: any build of the same commit, and the
    flags in another order.
    """
    copy = tmp_path / "src"
    copy.mkdir()
    _extract(REPO_ROOT, "HEAD", copy)

    dist = tmp_path / "dist"
    _build(copy, "--sdist", dist)
    _build(copy, "--wheel", dist)

    record = tmp_path / "release-enumeration.json"
    _enumerate(dist, REPO_ROOT, record)

    completed = _run(record, dist, REPO_ROOT)
    assert completed.returncode == 0, completed.stdout + completed.stderr

    scan = _scan_of(record)
    assert scan["status"] == "clean"
    assert scan["counts"]["secret"] == 0, scan
    corpus = scan["corpus"]
    assert isinstance(corpus, dict), corpus
    assert corpus["sets"] == SETS, corpus
    assert corpus["scanned"] > 0, corpus


def test_a_secret_in_a_released_file_refuses_the_release_and_is_recorded(tmp_path):
    """Property: a file of a released set that spells a whole secret shape on one line makes the
    scan exit 1 and its record say `refused` with a secret count, so no upload job starts.

    Class: NEW CAPABILITY — plausible wrong implementations are a scan that reports the finding and
    exits 0, which is a guard that never blocks anything; one that refuses on ANY class's finding,
    which stops a release over a machine-specific path that section 4.8.1 says does not block one;
    and one that writes the record only when clean, which leaves the refused release with nothing to
    read. The test is red under each.

    Why: the refusal is the whole point of the step, and the record is what a later reader has of it
    once the run is over.

    Killed by: dropping the secret class, keying the exit status on `report.refused`, or writing the
    record after the exit decision. Second correct implementation: any planted shape of the class,
    and a secret in any one set rather than all three.
    """
    record, dist, repo = _release(
        tmp_path,
        {
            "README.md": b"# sample\n",
            "src/sample/__init__.py": b"",
            "src/sample/leaked.py": f"{PEM_HEADER}\nnot-a-real-key\n".encode(),
        },
    )

    completed = _run(record, dist, repo)
    assert completed.returncode == 1, completed.stdout + completed.stderr

    scan = _scan_of(record)
    assert scan["status"] == "refused"
    assert scan["counts"]["secret"] == THREE, scan
    assert scan["predicate"] == "secret == 0", scan
    # The located form names the set, the path and the class label: the file reaches all three sets,
    # so it is three findings and not one, and the set prefix is what says which archive held it.
    assert "sdist/src/sample/leaked.py:1: PEM private-key header" in completed.stderr, completed.stderr
    assert "refused: 3 secret finding(s) in the released bytes" in completed.stderr, completed.stderr


def test_a_machine_specific_path_is_reported_and_refuses_nothing(tmp_path):
    """Property: a home-directory path in a released file is counted in the record's machine-path
    figure and the release is not refused, so the scan exits 0 and the record says `clean`.

    Class: NEW CAPABILITY — plausible wrong implementation is a scan that refuses on any finding,
    which stops every release whose bytes name the author's machine, where section 4.8.1 rules that
    a machine-specific path is a defect and not a release blocker. A scan that skips the class
    instead reports a count of zero and is red on the count.

    Why: the two classes are told apart by the gain to an attacker and not by whether a string looks
    personal, and the record must show both that the class ran and that it blocked nothing.

    Killed by: refusing on `report.refused`, or dropping the machine-path class. Second correct
    implementation: any alternation of the class's pattern, and the finding in any one set.
    """
    record, dist, repo = _release(
        tmp_path,
        {
            "README.md": b"# sample\n",
            "src/sample/noted.py": f"NOTES = {MACHINE_PATH}\n".encode(),
        },
    )

    completed = _run(record, dist, repo)
    assert completed.returncode == 0, completed.stdout + completed.stderr

    scan = _scan_of(record)
    assert scan["status"] == "clean"
    assert scan["counts"]["machine_path"] == THREE, scan
    assert scan["counts"]["secret"] == 0, scan


def test_the_scan_writes_the_five_fields_and_leaves_the_rest_of_the_record_alone(tmp_path):
    """Property: the scan fills `status`, `predicate`, `corpus`, `date` and `counts` in place, and
    every other key of the enumeration survives byte for byte in meaning.

    Class: NEW CAPABILITY — plausible wrong implementations are a scan that writes a new record and
    drops the artifacts and entries every later step reads, and one that leaves the scan `not-run`,
    which a later reader cannot tell from a scan that never happened. The test is red under each.

    Why: the record is one object written by the build and filled by three later steps, so a step
    that replaces it breaks the comparison and the acceptance read without failing anything itself.
    The date is asserted as a parseable stamp and not as a value, because the scan's date is when
    it ran.

    Killed by: writing a fresh object, dropping a field, or leaving the `not-run` status. Second
    correct implementation: any indentation, and any key order.
    """
    record, dist, repo = _release(tmp_path, {"README.md": b"# sample\n"})
    before = json.loads(record.read_text(encoding="utf-8"))

    completed = _run(record, dist, repo)
    assert completed.returncode == 0, completed.stdout + completed.stderr

    after = json.loads(record.read_text(encoding="utf-8"))
    assert set(after) == set(before), sorted(after)
    for key in set(before) - {"scan"}:
        assert after[key] == before[key], key

    scan = _scan_of(record)
    assert set(scan) == set(SCAN_FIELDS), scan
    assert scan["predicate"] == "secret == 0", scan
    assert DATE.match(str(scan["date"])), scan["date"]
    corpus = scan["corpus"]
    assert isinstance(corpus, dict), corpus
    assert corpus["members"] == len(before["entries"]), corpus
    assert set(scan["counts"]) == {"machine_path", "secret"}, scan["counts"]


def test_the_scan_refuses_a_member_its_archive_does_not_hold_and_leaves_the_record_unscanned(tmp_path):
    """Property: a record naming a member its own archive does not hold makes the scan exit 2 and
    leaves the record's `scan` object at `not-run`.

    Class: NEW CAPABILITY — plausible wrong implementations are a scan that skips a member it cannot
    read, which reports a clean result over a corpus smaller than the record says it read, and one
    that fills the record from a failed scan, which records a verdict about bytes it never opened.
    The test is red under each.

    Why: exit 2 is the scanner's "this run could not report", kept apart from exit 1's "the bytes
    are refused", because only the second is evidence about the release. A record and a `dist/`
    from two different builds are exactly how a scan silently ranges over the wrong corpus.

    Killed by: tolerating a missing member, or writing the scan object before the exit decision.
    Second correct implementation: any missing member of any set, and any reason it is missing.
    """
    record, dist, repo = _release(tmp_path, {"README.md": b"# sample\n"})
    document = json.loads(record.read_text(encoding="utf-8"))
    document["entries"].append(
        {"set": "sdist", "path": "src/sample/absent.py", "sha256": _sha256(b"absent"), "size": 6}
    )
    record.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")

    completed = _run(record, dist, repo)
    assert completed.returncode == 2, completed.stdout + completed.stderr
    assert "does not hold it" in completed.stderr, completed.stderr
    assert _scan_of(record)["status"] == "not-run"


def test_the_runner_report_counts_without_naming_a_path_or_a_line(tmp_path):
    """Property: with `GITHUB_ACTIONS` set the scan prints counts and the refusal sentence, and no
    path, line number or class label that locates a finding -- even for a release it refuses.

    Class: NEW CAPABILITY — plausible wrong implementations are the located form printed
    unconditionally, which publishes the path and line of whatever the finding is, and a count-only
    form that still prints the label names. The test is red under each, and it is red on the LOCATED
    form's absence from the located run as well, so a scan that never prints a location cannot pass
    by printing none in both forms.

    Why: a CI log of a public repository is public. The count is what a reader needs from a refused
    release; the location is what a developer needs on their own terminal.

    Killed by: ignoring `GITHUB_ACTIONS`, or printing the located lines beside the counts. Second
    correct implementation: the counts in any order, and the refusal sentence's wording, which this
    test does not pin beyond the finding count it states.
    """
    record, dist, repo = _release(
        tmp_path,
        {
            "README.md": b"# sample\n",
            "src/sample/leaked.py": f"{PEM_HEADER}\nnot-a-real-key\n".encode(),
        },
    )

    located = _run(record, dist, repo)
    assert located.returncode == 1, located.stdout + located.stderr
    assert "leaked.py" in located.stderr, located.stderr

    completion = _run(record, dist, repo, runner=True)
    assert completion.returncode == 1, completion.stdout + completion.stderr
    printed = completion.stdout + completion.stderr
    assert "content-scan:" in printed, printed
    assert "refused: 3 secret finding(s) in the released bytes" in printed, printed
    assert "leaked.py" not in printed, printed
    assert "PEM private-key header" not in printed, printed
    assert ":1:" not in printed, printed
