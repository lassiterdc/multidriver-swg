"""Tests of the release enumeration script: the record's shape, its three byte sets, and its refusals.

The script writes the one record of a release's bytes that every later step reads, so each test below
drives it as a subprocess, as the release workflow does, and reads the file it wrote. The artifacts are
built here rather than by a build backend: what the script must get right is the enumeration, and a
real build would make every test measure the backend instead.

The throwaway repository carries an identity on the command line, because a runner has no git
identity and a test that relied on one would pass on a workstation and fail there.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import subprocess
import sys
import tarfile
import zipfile
from datetime import datetime
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "enumerate_release.py"

IDENTITY = ("-c", "user.name=Release Enumeration", "-c", "user.email=release-enumeration@example.invalid")
REFUSAL_LINE = re.compile(r"^refused: (?P<name>[a-z-]+): (?P<detail>.+)$")


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


def _repository(path: Path, files: dict[str, str]) -> str:
    """A throwaway repository holding `files` in one commit, whose sha is returned."""
    path.mkdir(parents=True, exist_ok=True)
    _git(path, "init", "--quiet", "-b", "main")
    for name, text in files.items():
        target = path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
    _git(path, "add", "--all")
    _git(path, "commit", "--quiet", "-m", "the release")
    return _git(path, "rev-parse", "HEAD").strip()


def _run(
    dist: Path,
    repo: Path,
    out: Path,
    *,
    commit: str = "HEAD",
    run_commit: str | None = None,
    tag: str = "v1.0.0",
    run_id: str = "1234567890",
    run_attempt: str = "1",
) -> subprocess.CompletedProcess:
    """One invocation of the script, in the form the release workflow gives it."""
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--dist",
            str(dist),
            "--repo",
            str(repo),
            "--commit",
            commit,
            "--tag",
            tag,
            "--run-id",
            run_id,
            "--run-attempt",
            run_attempt,
            "--run-commit",
            run_commit if run_commit is not None else commit,
            "--out",
            str(out),
        ],
        capture_output=True,
        text=True,
        check=False,
    )


def _refusals(result: subprocess.CompletedProcess) -> dict[str, str]:
    """The `refused: {id}: {detail}` lines of a run, as id -> detail."""
    found = {}
    for line in result.stdout.splitlines():
        matched = REFUSAL_LINE.match(line)
        if matched is not None:
            found[matched.group("name")] = matched.group("detail")
    return found


def _entries(record: dict, set_name: str) -> dict[str, dict]:
    """The record's entries of one set, keyed by path."""
    return {entry["path"]: entry for entry in record["entries"] if entry["set"] == set_name}


SDIST_MEMBERS = {"PKG-INFO": b"Metadata-Version: 2.4\n", "sample/__init__.py": b"VALUE = 1\n"}
WHEEL_MEMBERS = {"sample/__init__.py": b"VALUE = 1\n", "sample-1.0.0.dist-info/METADATA": b"Metadata-Version: 2.4\n"}


def test_the_record_names_the_artifacts_and_every_member_of_the_three_sets(tmp_path):
    """Property: one record holds the two artifacts' own digests and every member of the sdist, the wheel and the source archive.

    The sdist's top directory is stripped, so `PKG-INFO` is one path whatever the version in the
    artifact's name is; the wheel's paths are the archive's own; and the source archive's set is the
    commit's tree, which `git archive` filters with the commit's `.gitattributes`. A directory is a
    member with a trailing slash and no bytes, which is what lets the comparison refuse an empty
    directory of an excluded class.

    Class: NEW CAPABILITY — the plausible wrong implementation keeps the sdist's member names as the
    archive writes them and drops directory members; red under it, because `PKG-INFO` is then
    `sample-1.0.0/PKG-INFO` and `sample/` is absent. A second correct implementation reads the members
    through `tarfile.getnames()` and their bytes by name, and passes: the assertions are on the
    record, not on the reader.
    """
    repo = tmp_path / "repo"
    commit = _repository(repo, {"README.md": "# sample\n", "sample/__init__.py": "VALUE = 1\n"})
    dist = tmp_path / "dist"
    sdist = _sdist(dist / "sample-1.0.0.tar.gz", SDIST_MEMBERS)
    wheel = _wheel(dist / "sample-1.0.0-py3-none-any.whl", WHEEL_MEMBERS)
    out = tmp_path / "release-enumeration.json"

    result = _run(dist, repo, out, commit=commit)

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.splitlines()[-1] == (
        f"release enumeration for v1.0.0 at {commit}: 2 artifacts, 8 entries, written to {out}"
    )
    record = json.loads(out.read_text(encoding="utf-8"))

    assert record["schema"] == 1
    assert record["tag"] == "v1.0.0"
    assert record["commit"] == commit
    assert record["created"].endswith("Z")
    datetime.strptime(record["created"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=None)
    assert record["whole_suite_run"] == {"run_id": "1234567890", "run_attempt": 1, "commit": commit}
    assert record["scan"] == {"status": "not-run", "predicate": None, "corpus": None, "date": None, "counts": None}

    assert [(item["set"], item["file"]) for item in record["artifacts"]] == [
        ("sdist", sdist.name),
        ("wheel", wheel.name),
    ]
    assert [item["sha256"] for item in record["artifacts"]] == [
        _sha256(sdist.read_bytes()),
        _sha256(wheel.read_bytes()),
    ]
    assert [item["size"] for item in record["artifacts"]] == [sdist.stat().st_size, wheel.stat().st_size]

    sdist_entries = _entries(record, "sdist")
    assert set(sdist_entries) == {"PKG-INFO", "sample/", "sample/__init__.py"}
    assert sdist_entries["PKG-INFO"]["sha256"] == _sha256(SDIST_MEMBERS["PKG-INFO"])
    assert sdist_entries["PKG-INFO"]["size"] == len(SDIST_MEMBERS["PKG-INFO"])
    assert sdist_entries["sample/"]["sha256"] is None
    assert sdist_entries["sample/"]["size"] == 0

    wheel_entries = _entries(record, "wheel")
    assert set(wheel_entries) == set(WHEEL_MEMBERS)
    assert wheel_entries["sample-1.0.0.dist-info/METADATA"]["sha256"] == _sha256(
        WHEEL_MEMBERS["sample-1.0.0.dist-info/METADATA"]
    )

    archive_entries = _entries(record, "source-archive")
    assert set(archive_entries) == {"README.md", "sample/", "sample/__init__.py"}
    assert archive_entries["README.md"]["sha256"] == _sha256(b"# sample\n")
    assert archive_entries["sample/"]["sha256"] is None


def test_the_source_archive_set_is_the_committed_tree_and_not_the_working_tree(tmp_path):
    """Property: a file the working tree carries and the commit does not is not in the source archive's set.

    The record describes what a reader of the tag's source archive receives, so the set is read from
    the commit the release names. A test that enumerated the working tree would pass while the record
    named bytes no release carries.

    Class: NEW CAPABILITY — the plausible wrong implementation walks the working directory instead of
    archiving the commit; red under it, because `UNTRACKED.md` appears in the source-archive set. A
    second correct implementation runs `git ls-tree -r` on the commit and reads each blob, and passes:
    the same paths arrive from a different instrument.
    """
    repo = tmp_path / "repo"
    commit = _repository(repo, {"README.md": "# sample\n"})
    (repo / "UNTRACKED.md").write_text("a file no commit holds\n", encoding="utf-8")
    dist = tmp_path / "dist"
    _sdist(dist / "sample-1.0.0.tar.gz", SDIST_MEMBERS)
    _wheel(dist / "sample-1.0.0-py3-none-any.whl", WHEEL_MEMBERS)
    out = tmp_path / "release-enumeration.json"

    result = _run(dist, repo, out, commit=commit)

    assert result.returncode == 0, result.stdout + result.stderr
    record = json.loads(out.read_text(encoding="utf-8"))
    assert set(_entries(record, "source-archive")) == {"README.md"}
    assert "UNTRACKED.md" not in {entry["path"] for entry in record["entries"]}


@pytest.mark.parametrize(
    ("sdists", "wheels"),
    [
        pytest.param(0, 0, id="no-artifacts"),
        pytest.param(2, 1, id="two-sdists"),
        pytest.param(1, 2, id="two-wheels"),
    ],
)
def test_a_dist_directory_without_exactly_one_sdist_and_one_wheel_is_refused(tmp_path, sdists, wheels):
    """Property: a dist directory holding no artifact, or two of one kind, is refused and writes nothing.

    A release ships exactly one sdist and one wheel, and a record naming two of either would name
    bytes no one selected for release. The detail counts and names what the directory holds, which is
    the fact a reader of the refusal needs.

    Class: NEW CAPABILITY — the plausible wrong implementation takes the first file of each suffix,
    so a directory holding two sdists passes and the record names whichever the directory listed
    first; red under it, because the two-sdists arm exits 0. The conforming control in this module
    is the differently positioned satisfying input: one sdist and one wheel pass.
    """
    repo = tmp_path / "repo"
    commit = _repository(repo, {"README.md": "# sample\n"})
    dist = tmp_path / "dist"
    dist.mkdir(parents=True, exist_ok=True)
    for index in range(sdists):
        _sdist(dist / f"sample-1.0.{index}.tar.gz", SDIST_MEMBERS)
    for index in range(wheels):
        _wheel(dist / f"sample-1.0.{index}-py3-none-any.whl", WHEEL_MEMBERS)
    out = tmp_path / "release-enumeration.json"

    result = _run(dist, repo, out, commit=commit)

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"wrong-artifacts"}, sorted(refusals)
    assert f"{sdists} sdist(s)" in refusals["wrong-artifacts"]
    assert f"{wheels} wheel(s)" in refusals["wrong-artifacts"]
    assert not out.exists()


def test_a_run_that_measured_another_commit_is_refused(tmp_path):
    """Property: an enumeration whose whole-suite run measured another commit is refused, naming both commits.

    The record states which run the release gate read, and a run of another commit is no evidence
    about this one. The two shas are printed because the reader decides which of them to retag.

    Class: NEW CAPABILITY — the plausible wrong implementation compares the run commit to the
    repository's HEAD instead of to `--commit`, so the arm where the two agree at HEAD and differ from
    the named commit passes; red under it, because this invocation exits 0. The differently positioned
    satisfying input is the same invocation with `--run-commit` equal to `--commit`.
    """
    repo = tmp_path / "repo"
    commit = _repository(repo, {"README.md": "# sample\n"})
    dist = tmp_path / "dist"
    _sdist(dist / "sample-1.0.0.tar.gz", SDIST_MEMBERS)
    _wheel(dist / "sample-1.0.0-py3-none-any.whl", WHEEL_MEMBERS)
    out = tmp_path / "release-enumeration.json"
    other = "0" * 40

    result = _run(dist, repo, out, commit=commit, run_commit=other)

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"run-not-at-commit"}, sorted(refusals)
    assert other in refusals["run-not-at-commit"]
    assert commit in refusals["run-not-at-commit"]
    assert not out.exists()


@pytest.mark.parametrize(
    ("sdist_text", "wheel_text", "commit"),
    [
        pytest.param(None, None, "0" * 40, id="unknown-commit"),
        pytest.param(b"not a tar at all\n", None, "HEAD", id="sdist-not-a-tar"),
        pytest.param(None, b"not a zip at all\n", "HEAD", id="wheel-not-a-zip"),
    ],
)
def test_an_artifact_or_a_commit_that_cannot_be_read_is_refused(tmp_path, sdist_text, wheel_text, commit):
    """Property: an unreadable artifact or an unarchivable commit is refused as `archive-failed`, and writes nothing.

    Each arm is a real failure of a release step: a truncated download, and a commit that the clone
    does not hold. A script that wrote a partial record would leave every later step comparing a
    release it never enumerated.

    Class: NEW CAPABILITY — the plausible wrong implementation lets the archive exception propagate,
    so the run prints a traceback and no `refused:` line, or writes a record with the failed set
    missing; red under it, because the refusal line is absent or the file exists. The differently
    positioned satisfying input is the conforming dist of the first test, which exits 0 and writes.
    """
    repo = tmp_path / "repo"
    _repository(repo, {"README.md": "# sample\n"})
    dist = tmp_path / "dist"
    sdist = _sdist(dist / "sample-1.0.0.tar.gz", SDIST_MEMBERS)
    wheel = _wheel(dist / "sample-1.0.0-py3-none-any.whl", WHEEL_MEMBERS)
    if sdist_text is not None:
        sdist.write_bytes(sdist_text)
    if wheel_text is not None:
        wheel.write_bytes(wheel_text)
    out = tmp_path / "release-enumeration.json"

    result = _run(dist, repo, out, commit=commit)

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"archive-failed"}, sorted(refusals)
    assert not out.exists()


def test_a_missing_argument_is_a_usage_error(tmp_path):
    """Property: an argument the script requires and did not get exits 2, the status argparse reserves.

    Exit 2 must not read as "refused" to the release workflow: a refusal is a fact about the release,
    and a usage error is a broken invocation of the tool that produces its record.

    Class: NEW CAPABILITY — the plausible wrong implementation gives `--run-id` a default, so a run
    with no run id records an empty string and exits 0; red under it, because the run then exits 0.
    The differently positioned satisfying input is a full invocation, which exits 0 and writes.
    """
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--dist", str(tmp_path), "--repo", str(tmp_path)],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "--run-id" in result.stderr
    assert "refused:" not in result.stdout
