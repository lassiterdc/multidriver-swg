"""Reviewer tests of the release enumeration and its comparison: three classes of input the author's tests do not reach.

The pair decides what a release ships, and each of the three cases below is a way the pair can pass a
release while the artifact or the record says something no reader expects. The scripts are invoked as
subprocesses, as the release workflow invokes them, and only their stdout, their exit status and the
file they write are read.
"""

from __future__ import annotations

import io
import json
import os
import re
import subprocess
import sys
import tarfile
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
ENUMERATE = REPO_ROOT / "scripts" / "enumerate_release.py"
COMPARE = REPO_ROOT / "scripts" / "compare_release_state.py"

IDENTITY = ("-c", "user.name=Release Enumeration", "-c", "user.email=release-enumeration@example.invalid")
REFUSAL_LINE = re.compile(r"^refused: (?P<name>[a-z-]+): (?P<detail>.+)$")


def _sdist(path: Path, members: dict[str, bytes], top: str = "sample-1.0.0") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(path, "w:gz") as archive:
        info = tarfile.TarInfo(top)
        info.type = tarfile.DIRTYPE
        archive.addfile(info)
        for name, data in sorted(members.items()):
            entry = tarfile.TarInfo(name if name.startswith("/") else f"{top}/{name}")
            entry.size = len(data)
            archive.addfile(entry, io.BytesIO(data))
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


def _repository(path: Path) -> str:
    path.mkdir(parents=True, exist_ok=True)
    _git(path, "init", "--quiet", "-b", "main")
    (path / "README.md").write_text("# sample\n", encoding="utf-8")
    _git(path, "add", "--all")
    _git(path, "commit", "--quiet", "-m", "the release")
    return _git(path, "rev-parse", "HEAD").strip()


def _enumerate(
    dist: Path, repo: Path, out: Path, commit: str = "HEAD", env: dict | None = None
) -> subprocess.CompletedProcess:
    return subprocess.run(
        [
            sys.executable,
            str(ENUMERATE),
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
            str(out),
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


def _refusals(result: subprocess.CompletedProcess) -> dict[str, str]:
    found = {}
    for line in result.stdout.splitlines():
        matched = REFUSAL_LINE.match(line)
        if matched is not None:
            found[matched.group("name")] = matched.group("detail")
    return found


@pytest.mark.parametrize(
    ("sdist_members", "wheel_members"),
    [
        pytest.param({"../escape.txt": b"x\n"}, {}, id="sdist-member-above-the-top-directory"),
        pytest.param({}, {"../escape.txt": b"x\n"}, id="wheel-member-above-the-root"),
        pytest.param({}, {"/etc/passwd": b"x\n"}, id="wheel-member-at-an-absolute-path"),
    ],
)
def test_a_member_naming_a_path_outside_the_archive_is_refused(tmp_path, sdist_members, wheel_members):
    """Property: an artifact holding a member that names a path outside its own root is refused as `archive-failed`.

    Class: members of an artifact whose recorded name escapes the archive root -- one whose path
    climbs with `..`, and one written with an absolute path. No build backend produces one, and an
    artifact that carries one is either corrupt or hand-made.

    Why required: the enumeration's paths are what the comparison classifies and what every later
    reader believes is inside the release. A member recorded as `../escape.txt` would be compared as
    a path outside every class, so it would pass as unclassified while naming a file beside the
    artifact; an absolute path would be compared against patterns that were never meant to reach
    outside the archive. Refusing the artifact is the only outcome that keeps the record's paths
    meaning what they say.

    What kills it: dropping the escape check, which is `_escapes_root` in the script and the
    top-directory check in its sdist reader. With either removed, the run exits 0 and writes a
    record naming the escaped member.

    A second correct implementation, differing in what the test asserts on: a reader that calls
    `tarfile.extract` with a filter and refuses a member whose member name is not a prefix of the
    resolved path. It would refuse these artifacts too, and the test would still pass, because it
    asserts the refusal id and the absence of a record rather than the check that produced them.
    """
    repo = tmp_path / "repo"
    commit = _repository(repo)
    dist = tmp_path / "dist"
    _sdist(dist / "sample-1.0.0.tar.gz", sdist_members or {"PKG-INFO": b"Metadata-Version: 2.4\n"})
    _wheel(dist / "sample-1.0.0-py3-none-any.whl", wheel_members or {"sample/__init__.py": b"VALUE = 1\n"})
    out = tmp_path / "release-enumeration.json"

    result = _enumerate(dist, repo, out, commit=commit)

    assert result.returncode == 1, result.stdout
    assert set(_refusals(result)) == {"archive-failed"}, result.stdout
    assert not out.exists()


def test_a_directory_entry_in_an_excluded_class_refuses_the_release(tmp_path):
    """Property: a directory member that a class's glob matches, and that the class declares excluded, refuses the release.

    Class: an entry whose path ends in `/`, which is how the enumeration records a directory member
    of a tar. A class that declares a directory excluded is exactly the case an export-ignore line
    written in its glob form leaves behind: the archive keeps an empty `dir/` entry, so the member
    that proves the exclusion failed is a directory and not a file.

    Why required: the comparison is the only reader that can refuse a release over a directory
    member. An implementation that skipped entries ending in `/` before classifying them, because a
    directory carries no bytes and no digest, would pass such a release and let the empty directory
    of an excluded class ship in the source archive.

    What kills it: classifying only entries whose `sha256` is not null, or matching with a rule in
    which `tests/**` requires at least one character after the separator. The pattern `tests/**`
    matches `tests/` in this comparison, so the entry is refused.

    A second correct implementation, differing in what the test asserts on: a rule that matches a
    directory member against the class's pattern after stripping the trailing slash, so `tests/`
    becomes `tests` and a class written `tests/{,/**}` matches it. The test asserts that the entry
    named `tests/` appears in a refusal, which such a rule still produces.
    """
    enumeration = tmp_path / "enumeration.json"
    enumeration.write_text(
        json.dumps(
            {
                "schema": 1,
                "entries": [
                    {
                        "set": "source-archive",
                        "path": "tests/",
                        "sha256": None,
                        "size": 0,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    table = tmp_path / "release_state.toml"
    table.write_text(
        '[[class]]\nname = "tests"\nsystem = "private-estate"\nstate = "excluded"\npaths = ["tests/**"]\n',
        encoding="utf-8",
    )

    result = subprocess.run(
        [sys.executable, str(COMPARE), "--enumeration", str(enumeration), "--table", str(table)],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"excluded-shipped"}, sorted(refusals)
    assert "tests/" in refusals["excluded-shipped"]


def test_the_record_timestamp_is_utc_whatever_the_machine_timezone_is(tmp_path):
    """Property: the record's `created` stamp is UTC to the second even when the machine's zone is not.

    Class: the value the script computes for `created` when the process runs under a non-UTC `TZ`.
    A stamp built from `datetime.now()` without a zone is the machine's local time, which reads as a
    plausible instant and is wrong by the zone's offset.

    Why required: the stamp is the record's only statement about when the release was enumerated, and
    a reader in another zone resolves it against the run log. An offset of hours between the two is
    unattributable after the fact, and a local stamp is indistinguishable from a UTC one by shape, so
    only a test that fixes the process's zone and compares against the UTC clock can catch it.

    What kills it: `datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")`, or `datetime.now().astimezone()`
    printed with an offset, because the format the schema states ends in `Z` and carries no offset.
    The environment below runs the script six hours behind UTC, where a local stamp lands outside the
    window this test allows.

    A second correct implementation, differing in what the test asserts on: a stamp taken from
    `time.time()` and formatted through `time.gmtime`, which is UTC as well. The test asserts the
    parsed instant, not the call that produced it.
    """
    repo = tmp_path / "repo"
    commit = _repository(repo)
    dist = tmp_path / "dist"
    _sdist(dist / "sample-1.0.0.tar.gz", {"PKG-INFO": b"Metadata-Version: 2.4\n"})
    _wheel(dist / "sample-1.0.0-py3-none-any.whl", {"sample/__init__.py": b"VALUE = 1\n"})
    out = tmp_path / "release-enumeration.json"
    environment = {**os.environ, "TZ": "America/New_York"}

    before = datetime.now(UTC).replace(microsecond=0)
    result = _enumerate(dist, repo, out, commit=commit, env=environment)
    after = datetime.now(UTC).replace(microsecond=0) + timedelta(seconds=2)

    assert result.returncode == 0, result.stdout + result.stderr
    record = json.loads(out.read_text(encoding="utf-8"))
    created = datetime.strptime(record["created"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    assert before - timedelta(seconds=2) <= created <= after, record["created"]


def test_a_repository_directory_that_does_not_exist_is_refused_without_a_traceback(tmp_path):
    """Property: a `--repo` that is not a directory is refused as `archive-failed` on a `refused:` line, never as a traceback.

    Class: the one input of the commit's source archive that fails before git starts. A repository
    path that does not exist (a checkout step that did not run, which is the order hazard the
    release workflow's own checkout-before-download rule exists for) is an input the module's contract
    covers: `archive-failed` is "an artifact or the commit's source archive could not be read".

    Why required: the workflow reads the refusal lines and the closing line, and the exit status the
    interpreter gives an uncaught exception is 1, the same as a refusal. A run that dies with a
    traceback therefore reads as a refused release while printing no refusal id, no closing line and
    no remedy, which is the one run whose reader needs the remedy most.

    What kills it: `subprocess.run(["git", "archive", ...], cwd=path)` with no handling of the
    `FileNotFoundError` (and `NotADirectoryError`) it raises for a missing `cwd`; red under it,
    because stderr then holds a traceback and stdout holds no refusal.

    A second correct implementation, differing in what the test asserts on: one that checks
    `Path(repo).is_dir()` before running git and refuses with its own detail; the test asserts the
    id, the exit status and the absence of a traceback, not the detail.
    """
    dist = tmp_path / "dist"
    _sdist(dist / "sample-1.0.0.tar.gz", {"PKG-INFO": b"Metadata-Version: 2.4\n"})
    _wheel(dist / "sample-1.0.0-py3-none-any.whl", {"sample/__init__.py": b"VALUE = 1\n"})
    out = tmp_path / "release-enumeration.json"

    result = _enumerate(dist, tmp_path / "no-such-checkout", out, commit="0" * 40)

    assert "Traceback" not in result.stderr, result.stderr
    assert result.returncode == 1, result.stdout + result.stderr
    assert set(_refusals(result)) == {"archive-failed"}, result.stdout
    assert result.stdout.splitlines()[-1].endswith("1 refusal(s)"), result.stdout
    assert not out.exists()


def test_every_ground_that_can_be_evaluated_is_reported_in_one_run(tmp_path):
    """Property: a dist directory of the wrong shape and a run of another commit are both reported by one invocation.

    Class: two independent facts about one release. The module's contract is that one run reports
    every fact it finds, so the person who cut the tag fixes both before retagging and not one per
    attempt.

    Why required: a short-circuiting implementation passes every single-ground test of the author
    and still costs the releaser one retag per ground.

    What kills it: returning after the first refusal. The control is the same invocation with one of
    the two facts corrected, which reports exactly the other.

    A second correct implementation, differing in what the test asserts on: collecting the grounds
    in a different order; the test asserts the set of ids and the count of refusal lines, not their
    order.
    """
    repo = tmp_path / "repo"
    commit = _repository(repo)
    dist = tmp_path / "dist"
    dist.mkdir()
    _wheel(dist / "sample-1.0.0-py3-none-any.whl", {"sample/__init__.py": b"VALUE = 1\n"})
    out = tmp_path / "release-enumeration.json"
    arguments = [
        sys.executable,
        str(ENUMERATE),
        "--dist",
        str(dist),
        "--repo",
        str(repo),
        "--commit",
        commit,
        "--tag",
        "v1.0.0",
        "--run-id",
        "1",
        "--run-attempt",
        "1",
        "--run-commit",
        "f" * 40,
        "--out",
        str(out),
    ]

    both = subprocess.run(arguments, capture_output=True, text=True, check=False)
    assert both.returncode == 1, both.stdout
    assert set(_refusals(both)) == {"wrong-artifacts", "run-not-at-commit"}, both.stdout
    assert both.stdout.splitlines()[-1].endswith("2 refusal(s)"), both.stdout

    arguments[arguments.index("--run-commit") + 1] = commit
    one = subprocess.run(arguments, capture_output=True, text=True, check=False)
    assert set(_refusals(one)) == {"wrong-artifacts"}, one.stdout
    assert one.stdout.splitlines()[-1].endswith("1 refusal(s)"), one.stdout
    assert not out.exists()


def test_the_source_archive_set_follows_the_attribute_file_of_the_commit_and_not_the_working_tree(tmp_path):
    """Property: an export-ignore line the commit holds removes a path from the set, and one only the working tree holds does not.

    Class: the two places an attribute file can live when the enumeration runs, the committed tree
    and the checkout beside it. The record claims to describe what a reader of the tag's source
    archive receives, and that archive is built from the committed tree.

    Why required: the only real-tree evidence that the committed lines work is a run at the commit
    that carries them. A reader that enumerated with `git ls-files` and filtered through the working
    tree's `.gitattributes`, or that archived the working directory, passes the author's
    untracked-file test and records bytes no archive holds, or omits bytes it holds.

    What kills it: any reader that consults the working tree's attribute file. In this repository
    the commit ignores `/private` and the working tree no longer does, so the set must lack
    `private/` and its file; the control commit holds no line, where the working tree has one, so
    the set must keep both.

    A second correct implementation, differing in what the test asserts on: a reader that parses the
    committed `.gitattributes` itself and filters `git ls-tree` output. It yields the same two sets.
    """
    ignored_repo = tmp_path / "ignored"
    ignored_repo.mkdir()
    _git(ignored_repo, "init", "--quiet", "-b", "main")
    (ignored_repo / "private").mkdir()
    (ignored_repo / "private" / "note.txt").write_text("x\n", encoding="utf-8")
    (ignored_repo / "README.md").write_text("# sample\n", encoding="utf-8")
    (ignored_repo / ".gitattributes").write_text("/private export-ignore\n", encoding="utf-8")
    _git(ignored_repo, "add", "--all")
    _git(ignored_repo, "commit", "--quiet", "-m", "the release")
    ignored_commit = _git(ignored_repo, "rev-parse", "HEAD").strip()
    (ignored_repo / ".gitattributes").write_text("# working tree only\n", encoding="utf-8")

    kept_repo = tmp_path / "kept"
    kept_repo.mkdir()
    _git(kept_repo, "init", "--quiet", "-b", "main")
    (kept_repo / "private").mkdir()
    (kept_repo / "private" / "note.txt").write_text("x\n", encoding="utf-8")
    (kept_repo / "README.md").write_text("# sample\n", encoding="utf-8")
    _git(kept_repo, "add", "--all")
    _git(kept_repo, "commit", "--quiet", "-m", "the release")
    kept_commit = _git(kept_repo, "rev-parse", "HEAD").strip()
    (kept_repo / ".gitattributes").write_text("/private export-ignore\n", encoding="utf-8")

    dist = tmp_path / "dist"
    _sdist(dist / "sample-1.0.0.tar.gz", {"PKG-INFO": b"Metadata-Version: 2.4\n"})
    _wheel(dist / "sample-1.0.0-py3-none-any.whl", {"sample/__init__.py": b"VALUE = 1\n"})

    def archive_paths(repo: Path, commit: str) -> set[str]:
        out = tmp_path / f"{repo.name}.json"
        result = _enumerate(dist, repo, out, commit=commit)
        assert result.returncode == 0, result.stdout + result.stderr
        record = json.loads(out.read_text(encoding="utf-8"))
        return {entry["path"] for entry in record["entries"] if entry["set"] == "source-archive"}

    assert archive_paths(ignored_repo, ignored_commit) == {".gitattributes", "README.md"}
    assert archive_paths(kept_repo, kept_commit) == {"README.md", "private/", "private/note.txt"}


@pytest.mark.parametrize(
    ("first_state", "second_state"),
    [
        pytest.param("released", "released", id="two-released-classes"),
        pytest.param("released", "excluded", id="released-and-excluded"),
        pytest.param("excluded", "estate-resident", id="excluded-and-estate-resident"),
    ],
)
def test_a_path_in_two_classes_is_refused_once_whatever_the_two_states_are(tmp_path, first_state, second_state):
    """Property: a path two classes name is refused as `two-classes` only, and is not also reported under either class's own state.

    Class: the three pairings of the two states that can disagree about one path. The release states
    no single state for such a path, so the refusal is about the pair and the per-state refusals
    would double-report it.

    Why required: the comparison's exit and its listing must agree with the table's own rule that a
    shipped file in two classes refuses the release. A comparison that classified by the first
    matching class would refuse the excluded-first pairing as `excluded-shipped` and pass the
    released-first pairing, hiding the double declaration in exactly the order a sibling's row
    happens to be written.

    What kills it: first-match classification, which passes the released-first arm with exit 0.

    A second correct implementation, differing in what the test asserts on: one that reports both
    `two-classes` and the per-state refusal; this test asserts the id set is exactly `two-classes`,
    which is the approved comparison's reading (each entry lands in one of the four counts).
    """
    licence = 'licence = "AGPL-3.0-or-later"\n'
    table = tmp_path / "release_state.toml"
    table.write_text(
        f'[[class]]\nname = "first"\nsystem = "one"\nstate = "{first_state}"\n'
        f'{licence if first_state == "released" else ""}paths = ["shared/**"]\n\n'
        f'[[class]]\nname = "second"\nsystem = "two"\nstate = "{second_state}"\n'
        f'{licence if second_state == "released" else ""}paths = ["shared/file.txt"]\n',
        encoding="utf-8",
    )
    enumeration = tmp_path / "enumeration.json"
    enumeration.write_text(
        json.dumps(
            {
                "schema": 1,
                "entries": [{"set": "wheel", "path": "shared/file.txt", "sha256": "0" * 64, "size": 1}],
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [sys.executable, str(COMPARE), "--enumeration", str(enumeration), "--table", str(table)],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1, result.stdout
    assert set(_refusals(result)) == {"two-classes"}, result.stdout
    assert result.stdout.splitlines()[-1] == "release state comparison: 1 refusal(s)"
