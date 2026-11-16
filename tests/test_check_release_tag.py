"""Tests of the release-tag refusal script: its five grounds, its output shape, and its exit status.

The script is the release step's brain: the workflow around it runs it on a tag push and lets the
build follow only when it exits 0. Every ground it enforces is a fact about the TAGGED COMMIT, so
each test below builds a throwaway repository and commits into it with an identity given on the
command line -- a GitHub runner has no git identity, and a test that relied on one would pass on a
workstation and fail there.

Two tests are not fixture-driven. One runs the version grounds over THIS checkout's own HEAD, so a
version bump that edits `pyproject.toml` and forgets `CITATION.cff` reds the suite on the push that
makes it rather than at the next tag. The other drives the real result-record emitter through a
`pytester` tree, because the record's four required facts are another instrument's output: a test
that hand-wrote every record it fed the gate would keep passing after the emitter renamed a field,
which is exactly the drift the gate must not survive.

The script is invoked as a subprocess everywhere, not imported. Its deliverable is its stdout and
its exit status, and both are what the release workflow reads.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "check_release_tag.py"

# Passed to every git call: a runner has no identity configured, and `git commit` refuses without one.
IDENTITY = ("-c", "user.name=Release Gate", "-c", "user.email=release-gate@example.invalid")

# The refusal ids, in the order the script prints them: the order its grounds are listed in.
REFUSAL_IDS = ("tag-version", "citation-version", "not-on-main", "legacy-tree", "whole-suite-record")

_REFUSAL_LINE = re.compile(r"^refused: (?P<name>[a-z-]+): (?P<detail>.+)$")


def _pyproject(version: str) -> str:
    return f'[project]\nname = "sample"\nversion = "{version}"\n'


def _citation(version: str) -> str:
    """A citation file whose one top-level `version:` line carries `version`, double-quoted as CFF wants."""
    return (
        "cff-version: 1.2.0\n"
        'message: "If you use this software, cite it using the metadata from this file."\n'
        "type: software\n"
        'title: "sample"\n'
        f'version: "{version}"\n'
        "license: AGPL-3.0-or-later\n"
    )


def _git(repo: Path, *arguments: str) -> str:
    """One git command in `repo`, its stdout returned and a failure raising with git's own message."""
    completed = subprocess.run(["git", *IDENTITY, *arguments], cwd=repo, capture_output=True, text=True, check=False)
    assert completed.returncode == 0, f"git {' '.join(arguments)}: {completed.stderr}"
    return completed.stdout


def _commit(repo: Path, message: str) -> str:
    _git(repo, "add", "--all")
    _git(repo, "commit", "--quiet", "-m", message)
    return _git(repo, "rev-parse", "HEAD").strip()


def _repository(
    path: Path,
    *,
    version: str = "1.0.0",
    citation: str | None = None,
    legacy: tuple[str, ...] = (),
) -> str:
    """A throwaway repository whose HEAD conforms on every ground but the deviations named.

    `citation` replaces the whole citation file, so a case can give it no version line or two;
    `legacy` names files to commit under the directory a release must not carry. The commit's sha is
    returned because the script reads the commit and never the working tree.
    """
    path.mkdir(parents=True, exist_ok=True)
    _git(path, "init", "--quiet", "-b", "main")
    (path / "pyproject.toml").write_text(_pyproject(version), encoding="utf-8")
    (path / "CITATION.cff").write_text(_citation(version) if citation is None else citation, encoding="utf-8")
    for name in legacy:
        target = path / "_old_code_to_refactor" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("# the unported legacy module\n", encoding="utf-8")
    return _commit(path, "the release")


# The record a whole, complete, passing run leaves: the four facts the gate reads, plus the fields it
# ignores, which are here so that the gate's indifference to them is exercised rather than assumed.
CONFORMING_RECORD = {
    "invocation": {"rootdir": "/checkout", "collection_scope": None, "population_digest": "sha256:" + "0" * 64},
    "environment": "/checkout/src/sample/__init__.py",
    "population": ["tests/test_pair.py::test_alpha", "tests/test_pair.py::test_beta"],
    "surviving": ["tests/test_pair.py::test_alpha", "tests/test_pair.py::test_beta"],
    "exclusion": [],
    "subject_resolution": {"state": "resolved"},
    "declared_context": "/checkout",
    "verdict": "pass",
    "completeness": "complete",
}

_ABSENT = object()


def _record(path: Path, **fields) -> Path:
    """Write the conforming record with the named top-level fields replaced; `_ABSENT` removes a field."""
    record = json.loads(json.dumps(CONFORMING_RECORD))
    for key, value in fields.items():
        if value is _ABSENT:
            record.pop(key, None)
        else:
            record[key] = value
    path.write_text(json.dumps(record), encoding="utf-8")
    return path


def _run(
    repo: Path, *, tag: str, record: Path, commit: str = "HEAD", main_ref: str = "main"
) -> subprocess.CompletedProcess:
    """One invocation of the gate, in the form the release workflow gives it."""
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--repo",
            str(repo),
            "--tag",
            tag,
            "--commit",
            commit,
            "--main-ref",
            main_ref,
            "--record",
            str(record),
        ],
        capture_output=True,
        text=True,
        check=False,
    )


def _refusal_lines(result: subprocess.CompletedProcess) -> list[tuple[str, str]]:
    """The `refused: {id}: {detail}` lines a run printed, as (id, detail) pairs, in print order."""
    found = []
    for line in result.stdout.splitlines():
        matched = _REFUSAL_LINE.match(line)
        if matched is not None:
            found.append((matched.group("name"), matched.group("detail")))
    return found


def _refusals(result: subprocess.CompletedProcess) -> dict[str, str]:
    """The same lines as id -> detail, for a test that asks whether one ground fired."""
    return dict(_refusal_lines(result))


def _summary(result: subprocess.CompletedProcess) -> str:
    return result.stdout.splitlines()[-1]


def test_a_conforming_tag_conforms_and_exits_zero(tmp_path):
    """Property: a repository that conforms on all five grounds is refused nowhere and exits 0.

    This is the control the other tests are read against: without it, a gate that refuses everything
    would satisfy every refusal test in this file. The closing line is asserted whole, because the
    workflow and whoever reads a run's log take the count and the word from it.
    """
    repo = tmp_path / "conforming"
    commit = _repository(repo)
    _git(repo, "tag", "v1.0.0", commit)
    record = _record(tmp_path / "record.json")

    result = _run(repo, tag="v1.0.0", commit=commit, record=record)

    assert result.returncode == 0, result.stdout + result.stderr
    assert _refusal_lines(result) == []
    assert _summary(result) == f"release tag v1.0.0 at {commit}: conforms"


@pytest.mark.parametrize("tag", ["v1.0.1", "1.0.0", "v1.0", "1.0.0.0"])
def test_the_tag_must_be_v_plus_the_version_as_written(tmp_path, tag):
    """Property: any tag but `v` + the version is refused, because the version is compared as written.

    The near misses are the point. `1.0.0` loses the prefix, `v1.0` normalises a release nobody
    cut, and `1.0.0.0` is a fourth component the version does not have -- each of them looks like
    the release and names something else, and a gate that normalised any of them would accept a tag
    the procedure never produces.
    """
    repo = tmp_path / "wrong-tag"
    commit = _repository(repo)
    record = _record(tmp_path / "record.json")

    result = _run(repo, tag=tag, commit=commit, record=record)

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"tag-version"}, sorted(refusals)
    assert "v1.0.0" in refusals["tag-version"]


@pytest.mark.parametrize(
    ("citation", "why"),
    [
        pytest.param(_citation("0.9.0"), "another version", id="another-version"),
        pytest.param(_citation("1.0.0").replace('version: "1.0.0"\n', ""), "no version line", id="no-version-line"),
        pytest.param(_citation("1.0.0") + 'version: "1.0.0"\n', "two version lines", id="two-version-lines"),
    ],
)
def test_the_citation_must_carry_exactly_one_top_level_version_equal_to_the_version(tmp_path, citation, why):
    """Property: the citation ground refuses a file with another version, none, or two, and names the file.

    The two-line case is the one a reader of the file would not notice: a YAML reader takes the last
    key, so a duplicate is invisible in everything but the file's text, while the value a release
    would cite is decided by an accident of line order.
    """
    repo = tmp_path / "citation"
    commit = _repository(repo, citation=citation)
    record = _record(tmp_path / "record.json")

    result = _run(repo, tag="v1.0.0", commit=commit, record=record)

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"citation-version"}, (why, sorted(refusals))
    assert "CITATION.cff" in refusals["citation-version"]


def test_a_nested_version_line_is_not_a_top_level_one(tmp_path):
    """Property: a `version:` nested under another key neither counts nor is compared.

    A citation with one top-level line and one nested one is a conforming file, and a gate that
    counted every `version:` in the text would refuse it while naming a value the release never
    declares. This is the positive control for the counting rule the test above exercises.
    """
    nested = (
        "cff-version: 1.2.0\n"
        'message: "If you use this software, cite it using the metadata from this file."\n'
        "type: software\n"
        'title: "sample"\n'
        'version: "1.0.0"\n'
        "authors:\n"
        "  - family-names: Sample\n"
        '    version: "9.9.9"\n'
        "license: AGPL-3.0-or-later\n"
    )
    repo = tmp_path / "nested-version"
    commit = _repository(repo, citation=nested)
    record = _record(tmp_path / "record.json")

    result = _run(repo, tag="v1.0.0", commit=commit, record=record)

    assert result.returncode == 0, result.stdout
    assert _refusals(result) == {}


def test_a_commit_off_the_main_reference_is_refused(tmp_path):
    """Property: a commit the main reference does not hold is refused, with the landing step named.

    A feature-branch commit that carries a correct version and a clean tree passes four grounds; it
    is the fifth that refuses it, and that is the ground a release cut from a working branch trips.
    """
    repo = tmp_path / "off-main"
    base = _repository(repo)
    _git(repo, "checkout", "--quiet", "-b", "feature")
    (repo / "NOTES.md").write_text("a change that main does not hold\n", encoding="utf-8")
    side = _commit(repo, "a change off main")
    record = _record(tmp_path / "record.json")

    result = _run(repo, tag="v1.0.0", commit=side, record=record)

    assert side != base
    assert result.returncode == 1, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"not-on-main"}, sorted(refusals)
    assert "main" in refusals["not-on-main"]


def test_a_tree_carrying_the_legacy_directory_is_refused_and_counts_its_files(tmp_path):
    """Property: a tree listing the legacy directory is refused, and the detail counts the tracked files.

    The count comes from the tree and not from the working directory, so it is the number a release
    would ship. Untracked leftovers are not part of any commit and must not appear in it.
    """
    repo = tmp_path / "legacy"
    commit = _repository(repo, legacy=("b1_module.py", "b2_module.py", "nested/b3_module.py"))
    (repo / "_old_code_to_refactor" / "untracked.py").write_text("# never committed\n", encoding="utf-8")
    record = _record(tmp_path / "record.json")

    result = _run(repo, tag="v1.0.0", commit=commit, record=record)

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"legacy-tree"}, sorted(refusals)
    assert refusals["legacy-tree"].startswith("3 tracked files under _old_code_to_refactor/")


def test_one_run_reports_every_ground_it_finds(tmp_path):
    """Property: all five grounds are evaluated on every invocation, so one run reports all of them.

    A gate that stopped at its first refusal would report one fact per push, and a release cut from a
    branch would take five pushes to learn five things. The order is asserted as well as the set,
    because the closing count and the whole-suite detail are read by a person scanning a run's log.
    """
    repo = tmp_path / "everything"
    _repository(repo)
    _git(repo, "checkout", "--quiet", "-b", "feature")
    (repo / "CITATION.cff").write_text(_citation("0.9.0"), encoding="utf-8")
    (repo / "_old_code_to_refactor").mkdir()
    (repo / "_old_code_to_refactor" / "old_module.py").write_text("# legacy\n", encoding="utf-8")
    bad = _commit(repo, "everything wrong at once")
    _git(repo, "tag", "v2.0.0", bad)
    record = _record(tmp_path / "record.json", verdict="fail")

    result = _run(repo, tag="v2.0.0", commit=bad, record=record)

    assert result.returncode == 1, result.stdout
    assert tuple(name for name, _ in _refusal_lines(result)) == REFUSAL_IDS
    assert _summary(result) == f"release tag v2.0.0 at {bad}: 5 refusal(s)"


@pytest.mark.parametrize(
    ("fields", "named"),
    [
        pytest.param({"verdict": "fail"}, "verdict", id="failing-run"),
        pytest.param({"verdict": "VOID"}, "verdict", id="void-run"),
        pytest.param({"completeness": "collected"}, "completeness", id="run-that-stopped"),
        pytest.param({"exclusion": ["tests/test_pair.py::test_beta"]}, "exclusion", id="deselected-tests"),
        pytest.param(
            {"invocation": {"rootdir": "/checkout", "collection_scope": "tests/test_pair.py"}},
            "collection_scope",
            id="path-narrowed-run",
        ),
        pytest.param({"verdict": _ABSENT}, "verdict", id="no-verdict"),
        pytest.param({"completeness": _ABSENT}, "completeness", id="no-completeness"),
        pytest.param({"exclusion": _ABSENT}, "exclusion", id="no-exclusion"),
        pytest.param({"invocation": _ABSENT}, "collection_scope", id="no-invocation"),
        pytest.param({"invocation": {"rootdir": "/checkout"}}, "collection_scope", id="no-collection-scope"),
    ],
)
def test_a_record_that_is_not_a_whole_passing_run_is_refused_naming_its_field(tmp_path, fields, named):
    """Property: each of the four required facts is refused, with a wrong value and with no value.

    The four are independent: a run can fail, stop after collection, deselect a test, or be narrowed
    by a path argument, and each is a different way to read a subset's result as the whole tree's.
    Both arms of every row are here because the missing-value arm is what a record from an older
    emitter looks like -- a gate that read a missing key as "not a failure" would accept it.
    """
    repo = tmp_path / "record"
    commit = _repository(repo)
    record = _record(tmp_path / "record.json", **fields)

    result = _run(repo, tag="v1.0.0", commit=commit, record=record)

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"whole-suite-record"}, sorted(refusals)
    assert named in refusals["whole-suite-record"]


def test_an_absent_record_is_refused_rather_than_skipped(tmp_path):
    """Property: a record path that holds no file is a refusal, which is what a lost artifact becomes.

    The release workflow downloads the record with `continue-on-error`, so the missing-file arm is a
    real path through the gate and not a test convenience: a gate that treated it as nothing to check
    would publish on a release whose suite never reported.
    """
    repo = tmp_path / "absent"
    commit = _repository(repo)

    result = _run(repo, tag="v1.0.0", commit=commit, record=tmp_path / "nowhere" / "result-record.json")

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"whole-suite-record"}, sorted(refusals)
    assert "no result record at" in refusals["whole-suite-record"]


@pytest.mark.parametrize(
    "text",
    [
        pytest.param('{"verdict": "pass", "completeness": "complete"', id="truncated"),
        pytest.param("[]", id="a-json-list"),
        pytest.param("", id="empty"),
    ],
)
def test_a_record_that_is_not_one_json_object_is_refused(tmp_path, text):
    """Property: a file that is not one JSON object is refused, naming the read that failed.

    A truncated write is what a killed run leaves, and it parses as neither a record nor an error the
    gate can attribute to a field; the point is that the gate refuses it rather than raising.
    """
    repo = tmp_path / "not-json"
    commit = _repository(repo)
    record = tmp_path / "record.json"
    record.write_text(text, encoding="utf-8")

    result = _run(repo, tag="v1.0.0", commit=commit, record=record)

    assert result.returncode == 1, result.stdout
    assert result.stderr == "", result.stderr
    refusals = _refusals(result)
    assert set(refusals) == {"whole-suite-record"}, sorted(refusals)


def test_a_missing_fact_is_a_usage_error(tmp_path):
    """Property: an argument the gate requires and did not get exits 2, the status argparse reserves.

    Exit 2 must not read as "refused" to the workflow: a refusal is a decision about a tag, and a
    usage error is a broken invocation of the gate itself.
    """
    repo = tmp_path / "usage"
    _repository(repo)

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--repo", str(repo), "--tag", "v1.0.0"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "--commit" in result.stderr
    assert "refused:" not in result.stdout


def test_the_checkout_declares_one_version_in_both_files(tmp_path):
    """Property: over this checkout's own HEAD, with the tag its version names, neither version ground fires.

    This is the real-tree half of the version requirement: a bump that edits one of the two files and
    not the other reds the suite on the push that makes it, instead of surfacing at the next tag. The
    record it feeds is conforming and unrelated to the property, so no other ground can excuse a
    pass; the exit status is not asserted, because the tree's ancestry and its legacy directory are
    facts about the branch rather than about the version.
    """
    version = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]["version"]
    record = _record(tmp_path / "record.json")

    result = _run(REPO_ROOT, tag=f"v{version}", record=record)

    refusals = _refusals(result)
    assert "tag-version" not in refusals, refusals.get("tag-version")
    assert "citation-version" not in refusals, refusals.get("citation-version")


INNER_CONFTEST = """\
pytest_plugins = ["tests.correspondence.inventory", "tests.correspondence.record"]


def pytest_addoption(parser):
    from tests.correspondence import record

    record.add_options(parser)
"""

PASSING_TREE = """\
def test_alpha():
    assert True


def test_beta():
    assert True
"""

FAILING_TREE = """\
def test_alpha():
    assert True


def test_beta():
    assert False
"""


def _inner_tree(pytester, source: str) -> None:
    """Write an inner tree whose collection is the whole ini `testpaths`: a `tests` directory and a conftest.

    The ini's `testpaths` is load-bearing: the emitter reports a null collection scope only when the
    path arguments equal `testpaths`, so a tree without it would write a narrowed-looking scope for
    every run and the accepted arm below could not exist.
    """
    pytester.makeini("[pytest]\ntestpaths = tests\n")
    pytester.mkdir("tests")
    pytester.path.joinpath("tests", "test_pair.py").write_text(source, encoding="utf-8")
    pytester.makeconftest(INNER_CONFTEST)


def test_the_record_the_emitter_writes_is_accepted_and_each_narrowing_is_refused(pytester, tmp_path):
    """Property: the gate reads the record the real emitter writes, accepting a whole run and refusing three narrowings.

    The record's four facts are another module's output, so a hand-written record would keep this
    gate's tests green through a rename in the emitter and let a release publish on a subset. The
    inner tree therefore writes its records with the same plugins the suite registers, and each
    narrowing is produced by running that tree a different way rather than by editing the object:
    a marker expression populates `exclusion`, a path argument populates `collection_scope`, and a
    failing test sets `verdict`. Each arm asserts the field the gate names, which is the difference
    between refusing for the right reason and refusing because a key moved.
    """
    repo = tmp_path / "emitter"
    commit = _repository(repo)
    _git(repo, "tag", "v1.0.0", commit)
    record = pytester.path / "result-record.json"
    uncached = ("-p", "no:cacheprovider")

    _inner_tree(pytester, PASSING_TREE)
    whole = pytester.runpytest(*uncached, "--result-record", str(record))
    whole.assert_outcomes(passed=2)
    accepted = _run(repo, tag="v1.0.0", commit=commit, record=record)
    assert accepted.returncode == 0, accepted.stdout + accepted.stderr
    assert "whole-suite-record" not in _refusals(accepted), _refusals(accepted).get("whole-suite-record")

    limited = pytester.runpytest(*uncached, "--result-record", str(record), "-k", "test_alpha")
    limited.assert_outcomes(passed=1, deselected=1)
    by_marker = _refusals(_run(repo, tag="v1.0.0", commit=commit, record=record))["whole-suite-record"]
    assert "exclusion" in by_marker and "test_beta" in by_marker, by_marker

    narrowed = pytester.runpytest(*uncached, "--result-record", str(record), "tests/test_pair.py")
    narrowed.assert_outcomes(passed=2)
    by_path = _refusals(_run(repo, tag="v1.0.0", commit=commit, record=record))["whole-suite-record"]
    assert "collection_scope" in by_path and "tests/test_pair.py" in by_path, by_path

    pytester.path.joinpath("tests", "test_pair.py").write_text(FAILING_TREE, encoding="utf-8")
    failing = pytester.runpytest(*uncached, "--result-record", str(record))
    failing.assert_outcomes(passed=1, failed=1)
    by_verdict = _refusals(_run(repo, tag="v1.0.0", commit=commit, record=record))["whole-suite-record"]
    assert "verdict" in by_verdict and '"fail"' in by_verdict, by_verdict
