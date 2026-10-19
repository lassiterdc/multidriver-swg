"""Tests of the per-system plan-coverage check: one violating case per finding, and a clean case.

Each test builds a small toolkit tree in `tmp_path`: the corpus record, the tracked legacy
files, the ledger and the system-scope file copied from this repository into a staged git
repository (never committed), so the check reads a real ledger; one synthetic test file whose
decorators give the system its tests; and the planning documents the case needs. The check is
then run on that tree by path, and each assertion reads one finding by its name and the tokens
it must carry.
"""

import shutil
import subprocess
from pathlib import Path

import pytest

from _refactor_tools import plan_coverage
from tests.correspondence import corpus_record, ledger, system_scope

pytestmark = pytest.mark.state_check

SYSTEM, OTHER = system_scope.slugs()[:2]
D_TOML = "_d_build_water_level_generator.toml"
D_FIRST_UNIT = "_d_build_water_level_generator.py:1-18"  # the first row of that ledger file, a real unit


def _tree(tmp_path: Path, tests: list[tuple[str, str, str, str | None]] = ()) -> Path:
    """Copy the ledger's inputs into a staged git repository at `tmp_path`, add one test file, and return it.

    `tests` holds (family, unit, system, part) for each decorated function of the test file;
    a part of None writes no `part=` keyword.
    """
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
    lines = ["import pytest", ""]
    for n, (family, unit, system, part) in enumerate(tests):
        part_keyword = f", part='{part}'" if part is not None else ""
        lines += ["", f"@pytest.mark.{family}(unit='{unit}', system='{system}'{part_keyword})", f"def test_{n}():"]
        lines += ["    pass", ""]
    (tmp_path / "tests").mkdir(exist_ok=True)
    (tmp_path / "tests" / "test_synthetic.py").write_text("\n".join(lines), encoding="utf-8")
    return tmp_path


def _doc(root: Path, name: str, *lines: str) -> str:
    """Write a planning document of these lines outside the toolkit tree's test directory and return its path."""
    path = root / "docs" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(path)


def _scope_in(root: Path, key: str, scope_id: str) -> None:
    """Make the ledger row of `key` in the water-level script's ledger file scope-in, citing `scope_id`."""
    path = root / ledger.LEDGER_DIR / D_TOML
    old = f'key = "{key}"\ndisposition = "discard"'
    text = path.read_text(encoding="utf-8")
    assert text.count(old) == 1, old
    path.write_text(text.replace(old, f'key = "{key}"\ndisposition = "scope-in"\nscope = "{scope_id}"'), "utf-8")


def _covers(root: Path, slug: str) -> str:
    """The identifier of the first covers statement of `slug` in the tree's system-scope file."""
    systems = system_scope.load(root)[0]
    return next(e["id"] for s in systems if s["slug"] == slug for e in s["statement"] if e["mark"] == "covers")


def _named(findings: list[str], name: str, *tokens: str) -> list[str]:
    """The findings of this name that carry every token as a whole word, never as a substring.

    Unit keys and document sites contain one another (`a.py:1-2` inside `a.py:1-20`), so a
    substring test would accept a finding about a different unit or line.
    """
    return [f for f in findings if f.startswith(name + " ") and set(tokens) <= {w.strip("'\":,;`") for w in f.split()}]


def test_an_unplanned_test_is_reported_and_fails_only_under_require_complete(tmp_path, capsys):
    """Property: a test of the system that no document adopts is UNPLANNED; the run exits 0, and 1 with `--require-complete`.

    Class: any test of the system with no adoption line in any document passed, here none at all.
    Why: an unplanned test is planning still to do, so it is reported without failing a run
    made while plans are being written, and fails only when the caller asks for completeness.
    Killed by: ignoring `--require-complete`, so the second run also exits 0. Second correct
    implementation: computing the exit code inside `main` from a count of failing findings.
    """
    root = _tree(tmp_path, [("equivalence", "a.py:1-2", SYSTEM, None)])
    assert plan_coverage.main(["--system", SYSTEM], root) == 0
    first = capsys.readouterr().out.splitlines()
    assert plan_coverage.main(["--system", SYSTEM, "--require-complete"], root) == 1
    second = capsys.readouterr().out.splitlines()
    assert _named(first, "UNPLANNED", "a.py:1-2"), first
    assert _named(second, "UNPLANNED", "a.py:1-2"), second


def test_a_test_adopted_by_two_documents_is_multiply_adopted(tmp_path):
    """Property: a test of the system adopted by two documents is MULTIPLY-ADOPTED, naming the test and both sites.

    Class: one test adopted once in each of two documents, here an implementation plan and a
    prompt, since an adoption counts whatever its document's kind. Why: two documents each
    claiming one test would each plan work the other also plans. Killed by: dropping the
    MULTIPLY-ADOPTED finding. Second correct implementation: grouping adoption lines by
    (unit, part) and reporting each group drawn from more than one path.
    """
    root = _tree(tmp_path, [("equivalence", "a.py:1-2", SYSTEM, None)])
    plan = _doc(root, "plan.md", "# A plan", "ADOPTS unit=a.py:1-2")
    prompt = _doc(root, "prompt.md", "ADOPTS unit=a.py:1-2")
    findings = plan_coverage.check(SYSTEM, [plan], [prompt], root)
    assert _named(findings, "MULTIPLY-ADOPTED", "a.py:1-2", f"{plan}:2", f"{prompt}:1"), findings


def test_one_document_adopting_a_test_on_two_lines_is_not_multiply_adopted(tmp_path):
    """Property: a test adopted twice by the same document is adopted by one document, so it is not MULTIPLY-ADOPTED.

    Class: one implementation plan that carries the same adoption line twice, and no other document.
    Why: MULTIPLY-ADOPTED reports a test that two documents each claim, which would plan the same
    work twice; one document repeating its own line claims it once, and a finding there would fail a
    run (the finding exits 1) over a test that is planned exactly once. Killed by: counting adoption
    lines rather than the documents they sit in. Second correct implementation: collapsing the
    adoption lines to (path, unit, part) triples before grouping by (unit, part).
    """
    root = _tree(tmp_path, [("equivalence", "a.py:1-2", SYSTEM, None)])
    plan = _doc(root, "plan.md", "ADOPTS unit=a.py:1-2", "prose", "ADOPTS unit=a.py:1-2")
    assert plan_coverage.check(SYSTEM, [plan], [], root) == []


def test_an_adoption_line_naming_no_test_is_reported(tmp_path):
    """Property: an adoption line whose unit and part no test carries is NAMES-NO-TEST, naming the line.

    Class: a well-formed line naming a key no test of any system carries; the system's own test
    is also adopted so that only the bad line is wrong. Why: a line naming nothing plans work no
    check will ever see done. Killed by: dropping the NAMES-NO-TEST finding. Second correct
    implementation: a set of every (unit, part) the inventory lists, tested per line.
    """
    root = _tree(tmp_path, [("equivalence", "a.py:1-2", SYSTEM, None)])
    plan = _doc(root, "plan.md", "ADOPTS unit=a.py:1-2", "ADOPTS unit=b.py:3-4")
    findings = plan_coverage.check(SYSTEM, [plan], [], root)
    assert _named(findings, "NAMES-NO-TEST", f"{plan}:2", "b.py:3-4"), findings


def test_an_adoption_line_naming_another_systems_test_is_foreign(tmp_path):
    """Property: an adoption line naming a test that another system owns is FOREIGN, naming the line and both systems.

    Class: a line naming a test whose only owner is a different system. Why: a system's plan
    cannot make another system's test pass, so the line is a misrouted adoption. Killed by:
    dropping the FOREIGN finding. Second correct implementation: reading the owner of the named
    test from the inventory and comparing it with the system checked.
    """
    root = _tree(tmp_path, [("equivalence", "c.py:5-6", OTHER, None)])
    plan = _doc(root, "plan.md", "ADOPTS unit=c.py:5-6")
    findings = plan_coverage.check(SYSTEM, [plan], [], root)
    assert _named(findings, "FOREIGN", f"{plan}:1", SYSTEM, OTHER), findings


def test_another_systems_tests_and_scoped_in_units_reach_no_finding_of_this_system(tmp_path):
    """Property: the tests and the scoped-in units of another system give the checked system no finding.

    Class: a tree whose only test belongs to another system and is adopted by nothing, and whose
    only scoped-in unit cites a covers statement of that other system and has no test; the check is
    run for a system that owns neither, with no document. Why: the check is per system, so a run for
    one system that reported another system's unplanned tests or untested units would fail that
    system's `--require-complete` run, and fail every run with UNTESTED, over work it does not own;
    and the same tree run for the other system still reports that system's unplanned test, so the
    silence is a scope and not a blind spot. (The other system's UNTESTED is deliberately not
    asserted here: the UNTESTED finding's own test owns it, and asserting it again would make the
    change that drops UNTESTED fail two tests.) Killed by: taking the system's tests from the inventory
    without the owner filter (UNPLANNED for the other system's test), or reporting UNTESTED without
    comparing the cited statement's system with the checked one. Second correct implementation:
    reading the system's covers identifiers from the system-scope file first and filtering the
    ledger's scope-in rows by membership, and grouping inventory entries by owner before choosing
    the checked group.
    """
    root = _tree(tmp_path, [("equivalence", "c.py:5-6", OTHER, None)])
    _scope_in(root, D_FIRST_UNIT, _covers(root, OTHER))
    assert plan_coverage.check(SYSTEM, [], [], root) == []
    assert plan_coverage.main(["--system", SYSTEM, "--require-complete"], root) == 0
    others = plan_coverage.check(OTHER, [], [], root)
    assert _named(others, "UNPLANNED", "c.py:5-6"), others


def test_a_line_beginning_adopts_outside_the_grammar_is_malformed(tmp_path):
    """Property: a line that begins `ADOPTS` and is not an adoption line is MALFORMED, naming the line.

    Class: a line with spaces around `=`, which reads as an adoption to a person and matches no
    adoption. Why: such a line looks like an adoption to its author and counts as none, so
    without the finding the test it meant reads unplanned with no hint why. Killed by: dropping the
    MALFORMED finding. Second correct implementation: a tokenizer that splits `key=value` pairs
    and rejects any line it cannot split.
    """
    root = _tree(tmp_path)
    plan = _doc(root, "plan.md", "intro", "ADOPTS unit = x.py:1-2")
    findings = plan_coverage.check(SYSTEM, [plan], [], root)
    assert _named(findings, "MALFORMED", f"{plan}:2"), findings


def test_a_scoped_in_unit_of_the_system_with_no_test_is_untested(tmp_path):
    """Property: a scoped-in unit with no test, whose ledger row cites a covers statement of this system, is UNTESTED.

    Class: a real unit of the corpus made scope-in under the system's first covers statement,
    with no test of any family or system. Why: a unit routed to a system with no test is work no
    adoption line can reach, since there is no test to adopt. Killed by: dropping the UNTESTED
    finding. Second correct implementation: building the set of the system's covers identifiers
    once and filtering the scope-in rows by it.
    """
    root = _tree(tmp_path)
    _scope_in(root, D_FIRST_UNIT, _covers(root, SYSTEM))
    findings = plan_coverage.check(SYSTEM, [], [], root)
    assert _named(findings, "UNTESTED", D_FIRST_UNIT), findings


def test_a_scoped_in_unit_tested_only_by_another_system_is_not_untested(tmp_path):
    """Property: a scoped-in unit whose row cites this system's covers statement and which has a test of another system is not UNTESTED.

    Class: one scoped-in unit routed to the checked system whose only test names a different owner.
    Why: UNTESTED means the unit has no test at all; a unit that has a test owned by the wrong
    system is a disagreement between the row and the test's owner, which the ownership checks
    report, and reporting it here as untested would tell the reader to write a test that exists.
    Killed by: counting only the checked system's tests as tests when computing UNTESTED. Second
    correct implementation: building the tested set from the ledger keys the inventory lists,
    rather than from the inventory's entries directly.
    """
    root = _tree(tmp_path, [("equivalence", D_FIRST_UNIT, OTHER, None)])
    _scope_in(root, D_FIRST_UNIT, _covers(root, SYSTEM))
    findings = plan_coverage.check(SYSTEM, [], [], root)
    assert not _named(findings, "UNTESTED", D_FIRST_UNIT), findings


def test_a_key_mentioned_outside_an_adoption_line_does_not_count(tmp_path):
    """Property: a test whose key a document mentions only in prose stays UNPLANNED.

    Class: a document naming the test's unit in running text and in a `unit=` token mid-line, with
    no adoption line. Why: only an adoption line is a commitment; a key quoted while discussing
    the unit would otherwise mark it planned. Killed by: matching unit keys anywhere in the text.
    Second correct implementation: a parser that reads only lines whose first token is `ADOPTS`.
    """
    root = _tree(tmp_path, [("equivalence", "a.py:1-2", SYSTEM, None)])
    plan = _doc(root, "plan.md", "The unit a.py:1-2 is discussed here.", "An earlier draft said unit=a.py:1-2 inline.")
    findings = plan_coverage.check(SYSTEM, [plan], [], root)
    assert _named(findings, "UNPLANNED", "a.py:1-2"), findings


def test_an_adoption_by_a_prompt_alone_leaves_the_test_unplanned(tmp_path):
    """Property: a test adopted only by a plan-implementation prompt is UNPLANNED, naming the prompt; one a plan adopts is not.

    Class: two tests of the system, one adopted by a prompt only and one by an implementation plan
    only. Why: a prompt is an early signal of planning and not the commitment an implementation
    plan is, so completeness must not be met on it. Killed by: counting a prompt's adoption as an
    implementation plan's. Second correct implementation: computing the planned set from the
    plans' adoption lines alone and reading the prompts only to name them.
    """
    root = _tree(tmp_path, [("equivalence", "a.py:1-2", SYSTEM, None), ("equivalence", "a.py:3-4", SYSTEM, None)])
    prompt = _doc(root, "prompt.md", "ADOPTS unit=a.py:1-2")
    plan = _doc(root, "plan.md", "ADOPTS unit=a.py:3-4")
    findings = plan_coverage.check(SYSTEM, [plan], [prompt], root)
    assert _named(findings, "UNPLANNED", "a.py:1-2", prompt), findings
    assert not _named(findings, "UNPLANNED", "a.py:3-4"), findings


def test_a_system_whose_every_test_is_adopted_once_has_no_finding(tmp_path):
    """Property: when every test of the system is adopted exactly once by a plan and every scoped-in unit has a test, there is no finding.

    Class: tests with and without a part, a unit with two parts adopted by separate lines in two
    plans, a reminder and an equivalence test sharing one unit and part and adopted by one line,
    and a scoped-in unit of the system that has a test. Why: a check firing on a complete plan set
    would make `--require-complete` unreachable. Killed by: reading an adoption line's unit and
    dropping its part, so the two lines naming one unit's two parts adopt neither part. Second
    correct implementation: keying the
    system's tests by family as well and accepting one adoption line for every family of a key.
    """
    tests = [
        ("equivalence", "a.py:1-2", SYSTEM, "first"),
        ("equivalence", "a.py:1-2", SYSTEM, "second"),
        ("refactor_reminder", "b.py:3-4", SYSTEM, None),
        ("equivalence", "b.py:3-4", SYSTEM, None),
        ("equivalence", D_FIRST_UNIT, SYSTEM, None),
    ]
    root = _tree(tmp_path, tests)
    _scope_in(root, D_FIRST_UNIT, _covers(root, SYSTEM))
    one = _doc(root, "one.md", "# One", "", "ADOPTS unit=a.py:1-2 part=first", "ADOPTS unit=b.py:3-4")
    two = _doc(root, "two.md", "ADOPTS unit=a.py:1-2 part=second", f"ADOPTS unit={D_FIRST_UNIT}")
    assert plan_coverage.check(SYSTEM, [one, two], [], root) == []
    assert plan_coverage.main(["--system", SYSTEM, "--plan", one, two, "--require-complete"], root) == 0


def test_a_system_name_the_system_scope_file_does_not_carry_is_a_usage_error(tmp_path, capsys):
    """Property: `--system` naming no system of the system-scope file exits 2 with a usage error naming it, with or without `--require-complete`.

    Class: a name absent from the system-scope file, run both without and with the completeness
    flag; the same tree run for a real system returns normally. Why: a mistyped name would
    otherwise read as a system with no tests, report nothing and pass `--require-complete`, so a
    caller's typo would certify a plan set as complete. Killed by: accepting any system name.
    Second correct implementation: an argparse `choices=` list built from the system-scope file's
    slugs before parsing.
    """
    root = _tree(tmp_path)
    for flags in ([], ["--require-complete"]):
        with pytest.raises(SystemExit) as stopped:
            plan_coverage.main(["--system", "no-such-system", *flags], root)
        assert stopped.value.code == 2, flags
        err = capsys.readouterr().err
        assert "no-such-system" in {w.strip("'\":,;`") for w in err.split()}, err
    assert plan_coverage.main(["--system", SYSTEM, "--require-complete"], root) == 0


def test_the_summary_line_counts_the_tests_whose_decorators_the_inventory_could_not_read(tmp_path, capsys):
    """Property: the summary line counts, by test, the decorators the inventory could not read, and the exit code does not change.

    Class: two tests whose family decorator carries a non-literal unit key, one of them with two
    such decorators, beside one readable test of the system that a plan adopts; the inventory
    reports each unreadable decorator and leaves both tests out of its list. Why: a test the
    inventory cannot read is a test this check cannot see, so without the count a run over such a
    tree reads as complete. The count is by test, so the function with two unreadable decorators
    counts once. Killed by: leaving the count out of the line, so it reads zero. Second correct
    implementation: counting the distinct (file, function) pairs of the unreadable findings with a
    regular expression over each finding's text.
    """
    root = _tree(tmp_path, [("equivalence", "a.py:1-2", SYSTEM, None)])
    (root / "tests" / "test_unreadable.py").write_text(
        "import pytest\n\nKEY = 'b.py:3-4'\n\n\n"
        f"@pytest.mark.equivalence(unit=KEY, system='{SYSTEM}')\n"
        f"@pytest.mark.refactor_reminder(unit=KEY, system='{SYSTEM}')\n"
        "def test_twice():\n    pass\n\n\n"
        f"@pytest.mark.equivalence(unit=KEY, system='{SYSTEM}', part='p')\n"
        "def test_once():\n    pass\n",
        encoding="utf-8",
    )
    plan = _doc(root, "plan.md", "ADOPTS unit=a.py:1-2")
    assert plan_coverage.main(["--system", SYSTEM, "--plan", plan, "--require-complete"], root) == 0
    line = capsys.readouterr().out.splitlines()[-1]
    assert "; 2 tests in the tree whose family or guard decorator the inventory could not read;" in line, line
