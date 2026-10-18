"""Tests of the progress report: one violating case per rule it states.

Each test builds the small toolkit tree the plan-coverage tests use (the corpus record, the
legacy files, the ledger and the system-scope file copied into a staged git repository in
`tmp_path`, plus one synthetic test file), marks real units of the corpus scope-in for one
system, writes the planning documents the case needs, and reads the report's lines. The tree
builders are the plan-coverage tests' own, so the two modules build their inputs one way.
"""

import subprocess

import pytest

from _refactor_tools import progress_report
from tests.correspondence.test_plan_coverage import D_FIRST_UNIT, OTHER, SYSTEM, _covers, _doc, _scope_in, _tree

pytestmark = pytest.mark.state_check

D_SECOND_UNIT = "_d_build_water_level_generator.py:19-27"  # the second row of the same ledger file, a real unit
COMMIT = ["-c", "user.name=fixture", "-c", "user.email=fixture@invalid", "-c", "commit.gpgsign=false", "commit", "-q"]


def _system_line(text: str, system: str) -> str:
    """The report's line for one system."""
    return next(line for line in text.splitlines() if line.startswith(f"progress of {system}: "))


def _words(line: str) -> set[str]:
    """The line's whole tokens, with surrounding punctuation stripped."""
    return {w.strip("'\":,;()`") for w in line.split()}


def test_a_path_given_in_two_groups_is_refused(tmp_path, capsys):
    """Property: a document path given in two groups is refused with exit 1, naming the path.

    Class: one document passed under two `--group`s, here as a plan of one and a prompt of the
    other. Why: one document's adoptions would then be counted for two systems, so a unit could
    read as planned twice. Killed by: dropping the refusal. Second correct implementation:
    counting the occurrences of each resolved path across all groups with a `Counter`.
    """
    root = _tree(tmp_path)
    shared = _doc(root, "shared.md", "ADOPTS unit=a.py:1-2")
    other = progress_report.system_scope.slugs(root)[1]
    assert progress_report.main(["--group", SYSTEM, "--plan", shared, "--group", other, "--prompt", shared], root) == 1
    assert shared in _words(capsys.readouterr().out)


def test_a_unit_is_planned_only_when_every_part_is_adopted_once(tmp_path):
    """Property: a scoped-in unit counts as planned only when each of its (unit, part) keys is adopted by exactly one document.

    Class: three trees, each beside a second scoped-in unit whose one test is adopted once, which
    is planned. (a) A unit with two parts of which one is adopted. (b) A unit whose one key two
    implementation plans adopt. (c) A unit whose one key is adopted once but whose tests name two
    owning systems. Why: a unit half of whose tests no plan takes on is not planned, a key two
    documents both claim is planned twice (the plan-coverage check reports it MULTIPLY-ADOPTED),
    and a unit two systems claim is the disagreement the ownership checks report; counting any of
    them would overstate how much of the refactor is planned. Killed by: counting a unit with any
    adoption as planned; for (b) alone, counting a key adopted by one or more documents; for (c)
    alone, accepting a unit when the checked system is among its owners. Second correct
    implementation: counting, per unit, the keys with exactly one adopting document and comparing
    that count with the unit's key count, after requiring the owner set to be exactly the system.
    """
    tests = [
        ("equivalence", D_FIRST_UNIT, SYSTEM, "first"),
        ("equivalence", D_FIRST_UNIT, SYSTEM, "second"),
        ("equivalence", D_SECOND_UNIT, SYSTEM, None),
    ]
    root = _tree(tmp_path / "two_parts", tests)
    _scope_in(root, D_FIRST_UNIT, _covers(root, SYSTEM))
    _scope_in(root, D_SECOND_UNIT, _covers(root, SYSTEM))
    plan = _doc(root, "plan.md", f"ADOPTS unit={D_FIRST_UNIT} part=first", f"ADOPTS unit={D_SECOND_UNIT}")
    line = _system_line(progress_report.report([(SYSTEM, [plan], [])], root), SYSTEM)
    assert f"progress of {SYSTEM}: planned 1 of 2 scoped-in units:" in line, line

    tests = [("equivalence", D_FIRST_UNIT, SYSTEM, None), ("equivalence", D_SECOND_UNIT, SYSTEM, None)]
    root = _tree(tmp_path / "two_documents", tests)
    _scope_in(root, D_FIRST_UNIT, _covers(root, SYSTEM))
    _scope_in(root, D_SECOND_UNIT, _covers(root, SYSTEM))
    one = _doc(root, "one.md", f"ADOPTS unit={D_FIRST_UNIT}", f"ADOPTS unit={D_SECOND_UNIT}")
    two = _doc(root, "two.md", f"ADOPTS unit={D_FIRST_UNIT}")
    line = _system_line(progress_report.report([(SYSTEM, [one, two], [])], root), SYSTEM)
    assert f"progress of {SYSTEM}: planned 1 of 2 scoped-in units:" in line, line

    tests = [
        ("equivalence", D_FIRST_UNIT, SYSTEM, None),
        ("refactor_reminder", D_FIRST_UNIT, OTHER, None),
        ("equivalence", D_SECOND_UNIT, SYSTEM, None),
    ]
    root = _tree(tmp_path / "two_owners", tests)
    _scope_in(root, D_FIRST_UNIT, _covers(root, SYSTEM))
    _scope_in(root, D_SECOND_UNIT, _covers(root, SYSTEM))
    plan = _doc(root, "plan.md", f"ADOPTS unit={D_FIRST_UNIT}", f"ADOPTS unit={D_SECOND_UNIT}")
    line = _system_line(progress_report.report([(SYSTEM, [plan], [])], root), SYSTEM)
    assert f"progress of {SYSTEM}: planned 1 of 2 scoped-in units:" in line, line


def test_adoptions_by_plans_and_by_prompts_are_counted_separately(tmp_path):
    """Property: a unit planned only through a prompt is counted as planned by prompt, and not by plan.

    Class: two trees. (a) One scoped-in unit whose one test a prompt adopts, with no
    implementation plan. (b) One scoped-in unit with two parts, one adopted by an implementation
    plan and the other by a prompt. Why: a prompt is an early signal and not a commitment, so a
    reader must be able to tell how much of the planned count rests on plans, and a unit is
    committed only when every one of its keys is in an implementation plan. Killed by: merging
    the two kinds; for (b) alone, counting a unit as planned by plan when any of its documents is
    a plan. Second correct implementation: keeping one planned set per kind and printing their
    sizes, the by-plan set computed from the plans' adoption lines alone.
    """
    root = _tree(tmp_path / "prompt_only", [("equivalence", D_FIRST_UNIT, SYSTEM, None)])
    _scope_in(root, D_FIRST_UNIT, _covers(root, SYSTEM))
    prompt = _doc(root, "prompt.md", f"ADOPTS unit={D_FIRST_UNIT}")
    line = _system_line(progress_report.report([(SYSTEM, [], [prompt])], root), SYSTEM)
    assert "planned 1 of 1 scoped-in units: by plan 0, by prompt 1;" in line, line

    tests = [("equivalence", D_FIRST_UNIT, SYSTEM, "first"), ("equivalence", D_FIRST_UNIT, SYSTEM, "second")]
    root = _tree(tmp_path / "mixed_kinds", tests)
    _scope_in(root, D_FIRST_UNIT, _covers(root, SYSTEM))
    plan = _doc(root, "plan.md", f"ADOPTS unit={D_FIRST_UNIT} part=first")
    prompt = _doc(root, "prompt.md", f"ADOPTS unit={D_FIRST_UNIT} part=second")
    line = _system_line(progress_report.report([(SYSTEM, [plan], [prompt])], root), SYSTEM)
    assert "planned 1 of 1 scoped-in units: by plan 0, by prompt 1;" in line, line


def test_both_commits_and_both_dirty_flags_are_printed(tmp_path):
    """Property: the report names the toolkit's commit and the documents' repository commit, each with its dirty flag.

    Class: a committed, clean toolkit tree and a separate documents repository with one commit
    and one uncommitted file, so the two flags differ. Why: a planned count is only meaningful
    against the versions of the tests and the documents it was computed from. Killed by:
    omitting the documents' commit. Second correct implementation: printing the two states on
    separate lines, each with the full commit.
    """
    root = _tree(tmp_path / "toolkit")
    subprocess.run(["git", "add", "-A"], cwd=root, check=True)
    subprocess.run(["git", *COMMIT, "-m", "toolkit"], cwd=root, check=True)
    docs = tmp_path / "documents"
    docs.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=docs, check=True)
    (docs / "plan.md").write_text("ADOPTS unit=a.py:1-2\n", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=docs, check=True)
    subprocess.run(["git", *COMMIT, "-m", "documents"], cwd=docs, check=True)
    (docs / "draft.md").write_text("not yet committed\n", encoding="utf-8")
    toolkit_head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True).stdout.strip()
    docs_head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=docs, capture_output=True, text=True).stdout.strip()
    text = progress_report.report([(SYSTEM, [str(docs / "plan.md")], [])], root)
    line = next(line for line in text.splitlines() if line.startswith("toolkit commit "))
    assert f"toolkit commit {toolkit_head} (clean)" in line, line
    assert f"documents' commit {docs_head} (uncommitted changes)" in line, line


def test_two_runs_on_the_same_files_print_the_same_report(tmp_path, capsys):
    """Property: two runs on the same files print byte-identical output.

    Class: one tree with a planned unit, an unplanned test and a plan, run twice through `main`.
    Why: a report that changes between identical runs cannot be compared across commits, which
    is the only way a reader sees planning progress. Killed by: adding a timestamp to the
    report. Second correct implementation: a report that sorts every collection it prints.
    """
    root = _tree(tmp_path, [("equivalence", D_FIRST_UNIT, SYSTEM, None), ("equivalence", "a.py:1-2", SYSTEM, None)])
    _scope_in(root, D_FIRST_UNIT, _covers(root, SYSTEM))
    plan = _doc(root, "plan.md", f"ADOPTS unit={D_FIRST_UNIT}")
    outputs = []
    for _run in range(2):
        assert progress_report.main(["--group", SYSTEM, "--plan", plan], root) == 0
        outputs.append(capsys.readouterr().out)
    assert outputs[0] == outputs[1], outputs


def test_a_group_name_the_system_scope_file_does_not_carry_is_a_usage_error(tmp_path, capsys):
    """Property: `--group` naming no system of the system-scope file exits 2 naming it, before any document is read.

    Class: a name absent from the system-scope file, given with a document path that does not
    exist, so reading any document would raise. Why: a mistyped name would otherwise read as a
    system with nothing scoped in and print a clean line for a system that does not exist.
    Killed by: accepting any group name. Second correct implementation: validating each name
    while parsing the command line, against the system-scope file's slugs.
    """
    root = _tree(tmp_path)
    missing = str(root / "docs" / "never-written.md")
    try:
        code = progress_report.main(["--group", "no-such-system", "--plan", missing], root)
    except FileNotFoundError:
        code = "a document was read before the name was refused"
    assert code == 2, code
    assert "no-such-system" in _words(capsys.readouterr().err)


def test_a_system_given_as_two_groups_is_refused(tmp_path, capsys):
    """Property: a system given as two `--group`s is refused with exit 1, naming it.

    Class: one system given twice, each time with a different document, so no path repeats.
    Why: the system's scoped-in units would enter the total twice, so the total would state more
    scoped-in units than the tree holds. Killed by: dropping the refusal. Second correct
    implementation: merging the groups by slug and refusing when the merged count is smaller.
    """
    root = _tree(tmp_path)
    plan = _doc(root, "plan.md", "ADOPTS unit=a.py:1-2")
    prompt = _doc(root, "prompt.md", "ADOPTS unit=a.py:1-2")
    assert progress_report.main(["--group", SYSTEM, "--plan", plan, "--group", SYSTEM, "--prompt", prompt], root) == 1
    assert SYSTEM in _words(capsys.readouterr().out)


def test_the_units_line_counts_the_tests_whose_decorators_the_inventory_could_not_read(tmp_path, capsys):
    """Property: the units line counts the tests the inventory could not read, and the exit code does not change.

    Class: a scoped-in unit with one readable test that a plan adopts, beside a second test of
    the same unit (another part) whose decorator names its unit through a variable, which the
    inventory cannot read and leaves out. Why: the planned count cannot see that second test,
    so it reads the unit as planned although that test is adopted by nothing; the count is the
    one place the report states its blind spot. Killed by: leaving the count out of the line, so
    it reads zero. Second correct implementation: counting the distinct (file, function) pairs of
    the inventory's unreadable findings with a regular expression over each finding's text.
    """
    root = _tree(tmp_path, [("equivalence", D_FIRST_UNIT, SYSTEM, None)])
    (root / "tests" / "test_unreadable.py").write_text(
        f"import pytest\n\nKEY = '{D_FIRST_UNIT}'\n\n\n"
        f"@pytest.mark.equivalence(unit=KEY, system='{SYSTEM}', part='x')\n"
        "def test_hidden():\n    pass\n",
        encoding="utf-8",
    )
    _scope_in(root, D_FIRST_UNIT, _covers(root, SYSTEM))
    plan = _doc(root, "plan.md", f"ADOPTS unit={D_FIRST_UNIT}")
    assert progress_report.main(["--group", SYSTEM, "--plan", plan], root) == 0
    lines = capsys.readouterr().out.splitlines()
    units = next(line for line in lines if line.startswith("units: "))
    assert "; 1 tests whose family or guard decorator the inventory could not read" in units, units
    assert "planned 1 of 1 scoped-in units:" in _system_line("\n".join(lines), SYSTEM)
