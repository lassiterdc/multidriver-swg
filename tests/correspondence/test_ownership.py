"""Tests of checks 4 to 7: one violating case per refusal, the guard helper's use of them, and the real tree at zero.

Fixtures copy the record, the tracked corpus, the ledger and the system-scope file into a git
repository inside `tmp_path` (staged, never committed), make one discarded ledger row scope-in by
citing a real covers statement, and write synthetic family tests under that tree's `tests/`, which
the static inventory reads and no outer run collects. Each test asserts on its own check's
findings only, because one input can earn findings from two checks: a test owned by a system
outside the file is, for instance, also a test whose owner differs from its row's cited system.
"""

import shutil
import subprocess
from pathlib import Path

import pytest

from tests.correspondence import corpus_record, ledger, ownership, system_scope
from tests.correspondence import test_guard_cases as guard_cases

pytestmark = pytest.mark.state_check

D_TOML = "_d_build_water_level_generator.toml"
UNIT = "_d_build_water_level_generator.py:1-18"
DISCARDED = "_d_build_water_level_generator.py:19-27"
LANDING = "checks 4 to 7: scoped-in rows 0, family tests 0 — nothing to check; findings 0"


def _fixture(tmp_path: Path) -> Path:
    """Copy every input the four checks read into a staged git repository at `tmp_path`; return it."""
    root = corpus_record.REPO_ROOT
    listed = subprocess.run(
        ["git", "ls-files", "-z", "--", "_old_code_to_refactor"], cwd=root, capture_output=True, check=True
    ).stdout.decode("utf-8")
    paths = [p for p in listed.split("\0") if p]
    paths += [corpus_record.RECORD_NAME, str(system_scope.SCOPE_PATH)]
    paths += [str(p.relative_to(root)) for p in (root / ledger.LEDGER_DIR).glob("*.toml")]
    for rel in paths:
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(root / rel, tmp_path / rel)
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "-A"], cwd=tmp_path, check=True)
    return tmp_path


def _covers(root: Path, slug: str) -> str:
    """The identifier of the first covers statement of system `slug` in the fixture's system-scope file."""
    return next(
        e["id"]
        for s in system_scope.load(root)[0]
        if s["slug"] == slug
        for e in s["statement"]
        if e["mark"] == "covers"
    )


def _scope_in(root: Path, slug: str) -> None:
    """Make the fixture's first row, `UNIT`, scope-in, citing a covers statement of system `slug`."""
    path = root / ledger.LEDGER_DIR / D_TOML
    text = path.read_text(encoding="utf-8")
    old = f'key = "{UNIT}"\ndisposition = "discard"'
    assert text.count(old) == 1
    path.write_text(
        text.replace(old, f'key = "{UNIT}"\ndisposition = "scope-in"\nscope = "{_covers(root, slug)}"'),
        encoding="utf-8",
    )


def _tests(root: Path, *decorated: tuple[str, str, str]) -> None:
    """Write one synthetic test file whose functions carry the given (family, unit, system) markers."""
    body = ["import pytest", ""]
    for n, (family, unit, system) in enumerate(decorated):
        body += [
            "",
            f'@pytest.mark.{family}(unit="{unit}", system="{system}", part="p{n}")',
            f"def test_{n}():",
            "    pass",
        ]
    path = root / "tests" / "synthetic" / "test_probe.py"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(body) + "\n", encoding="utf-8")


def _names(finding: str, *tokens: str) -> bool:
    """Whether the finding names every token as a whole word, never as a substring.

    Unit keys and system slugs can contain one another (`:1-18` inside `:1-180`), so a substring
    test would pass on a finding that names a different unit or system.
    """
    words = {word.strip("'\":,;") for word in finding.split()}
    return set(tokens) <= words


def test_a_scoped_in_unit_with_no_test_is_named(tmp_path):
    """Property: check 4 names a scoped-in unit that no equivalence or refactor-reminder test carries.

    Class: any scope-in row whose unit key no family test bears. Why: a unit ruled in for porting
    with no test has nothing to show it was ported, and a reminder deleted with no replacing test
    leaves exactly this state behind. Killed by: skipping units with no test. Second correct
    implementation: a set difference of scoped-in keys minus the keys the tests bear.
    """
    root = _fixture(tmp_path)
    _scope_in(root, "dependence-modelling")
    findings = ownership.check_4(root)
    assert any(_names(f, UNIT) and "no" in f.split() for f in findings), findings


def test_a_unit_whose_tests_name_two_systems_is_named(tmp_path):
    """Property: check 4 names a scoped-in unit whose tests name two owning systems, and not one whose tests name one.

    Class: any scoped-in unit with two or more tests, by the number of DISTINCT systems they name,
    never by the number of tests. Why: a unit has one owner, and two tests owned by two systems mean
    nobody can say which system's change may break it. The satisfying arm, two tests of the same
    system, is what separates counting systems from counting tests, which agree on the violating
    input. Killed by: counting tests instead of distinct systems. Second correct implementation: a
    Counter of systems per unit, flagged when it has more than one key.
    """
    root = _fixture(tmp_path)
    _scope_in(root, "dependence-modelling")
    _tests(root, ("equivalence", UNIT, "dependence-modelling"), ("refactor_reminder", UNIT, "generator-validation"))
    findings = ownership.check_4(root)
    assert any(_names(f, UNIT, "dependence-modelling", "generator-validation") for f in findings), findings
    _tests(root, ("equivalence", UNIT, "dependence-modelling"), ("equivalence", UNIT, "dependence-modelling"))
    assert ownership.check_4(root) == []


def test_a_test_keyed_to_a_discarded_unit_is_named(tmp_path):
    """Property: check 5 names a test whose unit key is a discarded or a scoped-out unit's, naming the test and the unit.

    Class: any family test bearing the key of a row that is not scope-in — discard or scope-out,
    one input each. A scoped-out unit's new code is another system's to write, so a test of it here
    tests code this system will never hold. Why: a test of a unit ruled out of the port tests code
    nobody will write, and it would keep passing or failing for a reason no ruling owns. The two
    inputs share one function because accepting a row of any disposition necessarily lets both
    through. Killed by: accepting a row of any disposition; and refusing only a key whose row is a
    discard, which lets the scoped-out input through. Second correct implementation: a set of the
    keys of every row that is not scope-in, looked up once per test.
    """
    root = _fixture(tmp_path / "discard")
    _tests(root, ("equivalence", DISCARDED, "dependence-modelling"))
    findings = ownership.check_5(root)
    assert any(_names(f, "tests/synthetic/test_probe.py::test_0", DISCARDED) for f in findings), findings

    root = _fixture(tmp_path / "scope_out")
    path = root / ledger.LEDGER_DIR / D_TOML
    text = path.read_text(encoding="utf-8")
    old = f'key = "{DISCARDED}"\ndisposition = "discard"'
    assert text.count(old) == 1
    path.write_text(
        text.replace(old, f'key = "{DISCARDED}"\ndisposition = "scope-out"\ndestination = "elsewhere"'),
        encoding="utf-8",
    )
    _tests(root, ("equivalence", DISCARDED, "dependence-modelling"))
    findings = ownership.check_5(root)
    words = {w.strip("'\":,;") for f in findings for w in f.split()}
    assert {"tests/synthetic/test_probe.py::test_0", DISCARDED} <= words, findings


def test_a_test_keyed_to_a_unit_with_two_scoped_in_rows_is_named(tmp_path):
    """Property: check 5 names a test whose key resolves to two scoped-in rows, not only to none.

    Class: any family test whose unit key two or more scope-in rows carry. Why: the row the test
    answers to is then undecided, and each row may cite a different system's statement, so the
    owner check has two answers. Killed by: refusing only a key with no scoped-in row, so a key
    with two passes. Second correct implementation: grouping the scope-in rows by key and refusing
    a group whose length is not one.
    """
    root = _fixture(tmp_path)
    _scope_in(root, "dependence-modelling")
    path = root / ledger.LEDGER_DIR / D_TOML
    text = path.read_text(encoding="utf-8")
    start = text.index(f'[[row]]\nkey = "{UNIT}"')
    end = text.index("[[row]]", start + 1)
    path.write_text(text + "\n" + text[start:end], encoding="utf-8")
    assert sum(1 for r in ledger.load(root)[0] if r.get("key") == UNIT and r.get("disposition") == "scope-in") == 2
    _tests(root, ("equivalence", UNIT, "dependence-modelling"))
    findings = ownership.check_5(root)
    words = {w.strip("'\":,;") for f in findings for w in f.split()}
    assert {"tests/synthetic/test_probe.py::test_0", UNIT} <= words, findings


def test_a_test_keyed_to_a_unit_with_no_row_is_named(tmp_path):
    """Property: check 5 names a test whose unit key resolves to no ledger row at all.

    Class: any family test bearing a key that no ledger row carries, a misspelt or stale key
    among them. Why: such a test claims a unit the ledger never ruled on, so no scoped-in row
    answers for it and its owner is checked against nothing. Killed by: accepting a test whose key
    has no row. Second correct implementation: counting the scoped-in rows with the key and
    requiring exactly one.
    """
    root = _fixture(tmp_path)
    _tests(root, ("equivalence", "_d_build_water_level_generator.py:1-17", "dependence-modelling"))
    findings = ownership.check_5(root)
    assert any(
        _names(f, "tests/synthetic/test_probe.py::test_0", "_d_build_water_level_generator.py:1-17") for f in findings
    )


def test_an_owner_outside_the_system_scope_file_is_named(tmp_path):
    """Property: check 6 names a test whose owning system is not a system of the system-scope file.

    Class: any family test's `system=` value outside the file's slugs, as the static inventory reads
    it. Why: an owner outside the partition is owned by nobody, and selecting tests by system would
    never select it. Killed by: dropping the slug test. Second correct implementation: a set
    difference of the tests' systems minus the file's slugs, reported per test.
    """
    root = _fixture(tmp_path)
    _scope_in(root, "dependence-modelling")
    _tests(root, ("equivalence", UNIT, "not-a-system"))
    findings = ownership.check_6(root)
    assert any(_names(f, "tests/synthetic/test_probe.py::test_0", "not-a-system") for f in findings), findings


def test_a_row_whose_cited_system_differs_from_its_tests_owner_is_named(tmp_path):
    """Property: check 7 names both systems when a row cites one system's statement and its tests name another.

    Class: any scoped-in unit whose tests name one owner that is not the system of the statement
    its row cites. Why: the row and the tests each say who owns the unit, and when they disagree
    neither is known to be the right one, so the message says that choosing is a human ruling
    rather than picking one. Killed by: dropping the comparison. Second correct implementation:
    resolving every cited statement once into a key-to-system map and comparing it with a
    key-to-owner map.
    """
    root = _fixture(tmp_path)
    _scope_in(root, "dependence-modelling")
    _tests(root, ("equivalence", UNIT, "generator-validation"))
    findings = ownership.check_7(root)
    assert any(
        _names(f, UNIT, "dependence-modelling", "generator-validation") and "human ruling" in f for f in findings
    ), findings


def test_a_consistent_scoped_in_unit_draws_no_finding_from_the_ownership_checks(tmp_path):
    """Property: a scoped-in unit whose one owner is the system of the statement its row cites draws no finding.

    Class: any scoped-in row citing a covers statement of system S, with a family test keyed to
    that row and naming S. Why: this is the state the first ported unit puts the real tree in; a
    check that over-fires there turns the State check red for a reason no row or test owns, and at
    zero no test can see it. Killed by: check 7 with its slug comparison dropped, so it names every
    one-owner unit; check 5 refusing a key that resolves to exactly one scoped-in row; check 6
    testing the unit key, not the owning system, against the file's slugs. Second correct
    implementation: checks that build a key-to-owner map and a key-to-cited-system map once and
    compare them, which also return nothing here.
    """
    root = _fixture(tmp_path)
    _scope_in(root, "dependence-modelling")
    # One test, not two: two tests of one system would also red under the two-systems test's
    # count-the-tests mutation, and that test must be the only one it reds.
    _tests(root, ("equivalence", UNIT, "dependence-modelling"))
    found = {c.__name__: c(root) for c in (ownership.check_4, ownership.check_5, ownership.check_6, ownership.check_7)}
    assert found == {"check_4": [], "check_5": [], "check_6": [], "check_7": []}, found


def test_the_guard_helper_runs_the_ownership_checks(tmp_path):
    """Property: the guard cases' helper runs checks 4 to 7, so a check-4 failure is named in its message.

    Class: any ledger and test tree failing one of checks 4 to 7. Why: the guard cases are how one
    system's work runs the whole correspondence check, so a check the helper does not run is a check
    no guard case can fail on. Killed by: iterating the ledger's own tuple, which stops at check 3.
    Second correct implementation: a helper that calls each of the seven checks by name and still
    names each failing one on its message's first line.
    """
    root = _fixture(tmp_path)
    _scope_in(root, "dependence-modelling")
    _tests(root, ("equivalence", UNIT, "dependence-modelling"), ("equivalence", UNIT, "generator-validation"))
    with pytest.raises(AssertionError) as caught:
        guard_cases.run_guard_case(root)
    first_line = str(caught.value).splitlines()[0]
    assert "check_4" in {word.strip(":;") for word in first_line.split()}, first_line


def test_at_zero_the_summary_line_states_its_denominators():
    """Property: on the real tree the four checks find nothing, and the line says how much they looked at.

    Class: the committed ledger and test tree, whose scoped-in rows and family tests are both zero
    until the first unit is ruled in. Why: a check over nothing passes, so a bare `findings 0` would
    read as a pass over the ledger; the line states both denominators and says there is nothing to
    check. Ruling in the first unit, or adding the first family test, changes the line on purpose.
    Killed by: printing `findings 0` alone. Second correct implementation: a line built from the
    inventory's and the ledger's own summaries, with the same words.
    """
    line, findings = ownership.summary()
    assert not findings, "\n".join(findings)
    assert line == LANDING
