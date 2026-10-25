"""Tests of the reach record: its tracer, its measuring command, its entries and the check over them.

Most tests build a small root in `tmp_path`: a corpus record naming one section, a stored text, a lift
declaration, and a synthetic fixture generated into that root. Two tests use the real stored text of
`__ref_ams_functions.py:23-30`. Every test writes only under pytest's temporary directory.

The one test that runs the committed entries of this checkout carries no `state_check` marker: later
entries run real units' lifts, whose libraries the State check job does not install, so that test
runs only in a full run. Every other test needs the standard library, numpy and pandas.

Each test docstring names the property, the input class, why it is required, the change that fails
it, and a second correct implementation that it still passes.
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import types
from pathlib import Path

import numpy as np
import pytest

from tests.correspondence import corpus_record, extraction, extraction_check, fixtures, lift, reach, value_codec

state_check = pytest.mark.state_check

REPO_ROOT = extraction.REPO_ROOT
RECORD = 'rule = "corpus-record:1"\n\n[[section]]\nname = "stormy"\ndirectory = "legacy"\n'
HEAD = "from tests.correspondence.lift import declares_lift\n\n\n"
BRANCH_TEXT = "if x[0] > 0:\n    branch = 1\nelse:\n    branch = 2\ntotal = branch\n"
BRANCH_LIFT = (
    HEAD
    + '@declares_lift(unit="mod.py:1-5", fixtures=["x"])\n'
    + "def declare(fixtures):\n"
    + '    """Hand the fixture to the text as x."""\n'
    + '    return {"x": fixtures["x"]}, lambda ns: ns["total"]\n'
)
POSITIVE_GENERATOR = """\
import numpy as np

from tests.correspondence.fixtures import generates_fixture


@generates_fixture(name="x", seed=1)
def x(rng):
    return np.array([1.0])
"""
ENTRY_FIELDS = {"commit": "fedcba9876543210", "measured": "2026-10-25"}


def make_root(
    tmp_path: Path, text: str = BRANCH_TEXT, source: str = BRANCH_LIFT, fixture: bool = True, last: int = 5
) -> Path:
    """A root holding the record, one stored text of `mod.py:1-{last}`, one lift module and, optionally, its fixture."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / corpus_record.RECORD_NAME).write_text(RECORD, encoding="utf-8")
    stored = tmp_path / extraction.stored_path("stormy", "mod.py", 1, last)
    stored.parent.mkdir(parents=True, exist_ok=True)
    stored.write_text(text, encoding="utf-8")
    (tmp_path / lift.LIFTS).mkdir(parents=True, exist_ok=True)
    (tmp_path / lift.LIFTS / "sys_a.py").write_text(source, encoding="utf-8")
    generators = tmp_path / fixtures.GENERATORS
    generators.mkdir(parents=True, exist_ok=True)
    (generators / "sys_a.py").write_text(POSITIVE_GENERATOR, encoding="utf-8")
    if fixture:
        fixtures.generate("sys-a", "x", root=tmp_path)
    return tmp_path


def declaration_of(root: Path) -> lift.LiftDeclaration:
    """The one declaration the root holds."""
    (declaration,) = lift.discover(root)
    return declaration


def real_inputs(tmp_path: Path, value: float) -> Path:
    """A directory holding the input `x` as the codec writes it, standing in for real data."""
    directory = tmp_path / "real_inputs"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "x.npz").write_bytes(value_codec.encode(np.array([value])))
    return directory


def commit_entry(root: Path, entry_text: str, system: str = "sys_a") -> None:
    """Write entry text into the system's reach file of the root."""
    path = root / reach.REACH / f"{system}.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(entry_text, encoding="utf-8")


@state_check
def test_a_fixture_that_misses_a_branch_real_data_takes_fails_the_reach_check_naming_the_line(tmp_path):
    """Property: an entry measured on inputs that take the `else` branch fails the check against committed fixtures that take the `if` branch, naming legacy line 4.

    Class: a two-branch unit whose measured input and committed fixture take different branches.
    Why required: this is the gap the reach record exists to expose; a check that passes here would
    let synthetic data stand in for real data over a branch it never runs.
    Killed by: a check that compares the count of recorded lines to the count reached, or that never
    runs the lift; the message must name the unreached line, and the `reached.lines` of the passing control below must differ.
    A second correct implementation: comparing the recorded set to a coverage.py measurement of the same run.
    """
    root = make_root(tmp_path)
    entry = reach.measure("sys-a", "mod.py:1-5", None, real_inputs(tmp_path, -1.0), root=root, **ENTRY_FIELDS)
    commit_entry(root, entry)
    report = reach.reach_findings(root)
    assert [f for f in report.findings if "[4]" in f and "do not reach" in f], report.findings
    assert (report.entries, report.declared, report.without_entry) == (1, 1, 0)


@state_check
def test_an_entry_measured_on_the_committed_fixture_passes(tmp_path):
    """Property (the control of the test above): an entry measured on the inputs the committed fixture holds passes, and records lines 1, 2 and 5.

    Class: the same unit and fixture as the failing case with the measured input equal to the fixture.
    Why required: it shows the failure above comes from the missing branch and not from a check that fails every entry.
    Killed by: a check that reports a finding for any entry.
    A second correct implementation: measuring through the fixture loader instead of an inputs directory.
    """
    root = make_root(tmp_path)
    entry = reach.measure("sys-a", "mod.py:1-5", None, real_inputs(tmp_path, 1.0), root=root, **ENTRY_FIELDS)
    commit_entry(root, entry)
    report = reach.reach_findings(root)
    assert report.findings == []
    assert reach.read_entries(root)[0][1]["lines"] == [1, 2, 5]


@state_check
def test_a_measured_entry_holds_no_input_value_and_no_inputs_path(tmp_path):
    """Property: the printed entry carries the unit, both digests, the date, the commit and the line numbers, and neither the inputs directory's path nor any input value.

    Class: an inputs directory under a recognisable path holding a recognisable value (-7391.25).
    Why required: the entry is committed to a public repository, and where real data lives is not for it to say.
    Killed by: recording the `--inputs` argument, or a repr of the loaded values, in the entry.
    A second correct implementation: recording a salted hash of the path, which this test would also accept.
    """
    root = make_root(tmp_path)
    inputs = real_inputs(tmp_path / "somewhere_private_name", -7391.25)
    text = reach.measure("sys-a", "mod.py:1-5", None, inputs, root=root, **ENTRY_FIELDS)
    assert str(inputs) not in text and "somewhere_private_name" not in text and "7391" not in text
    commit_entry(root, text)
    ((system, entry),) = reach.read_entries(root)
    declaration = declaration_of(root)
    assert system == "sys_a"
    assert entry["unit"] == "mod.py:1-5" and "part" not in entry
    assert entry["lift"] == lift.lift_digest(declaration)
    assert entry["cuts"] == [extraction.label(extraction.CUT_RULE, BRANCH_TEXT.encode())]
    assert (entry["measured"], entry["commit"], entry["lines"]) == ("2026-10-25", "fedcba9876543210", [1, 4, 5])


@state_check
def test_editing_the_stored_text_makes_the_entry_stale_not_trusted(tmp_path):
    """Property: an entry whose cut digests no longer match the unit's stored text is reported STALE and its lift is not run.

    Class: an edit of one byte of the stored text after the entry was measured.
    Why required: an entry keyed on a changed text describes lines of text that no longer exists.
    Killed by: running the lift and comparing lines without comparing digests; the control is the unedited pass above.
    A second correct implementation: comparing a digest of the cited range cut from the corpus.
    """
    root = make_root(tmp_path)
    commit_entry(
        root, reach.measure("sys-a", "mod.py:1-5", None, real_inputs(tmp_path, 1.0), root=root, **ENTRY_FIELDS)
    )
    assert reach.reach_findings(root).findings == []
    (root / extraction.stored_path("stormy", "mod.py", 1, 5)).write_text(BRANCH_TEXT + "# edit\n", encoding="utf-8")
    findings = reach.reach_findings(root).findings
    assert len(findings) == 1 and "STALE" in findings[0] and "cut digests" in findings[0], findings


@state_check
def test_editing_the_lift_makes_the_entry_stale(tmp_path):
    """Property: an entry whose lift digest differs from the lift's now is reported STALE.

    Class: an edit of the declaration's logic (the reduction) after the entry was measured.
    Why required: a changed lift runs different code over the fixtures, so the old lines say nothing about it.
    Killed by: omitting the lift-digest comparison.
    A second correct implementation: recording the declaration's source text and comparing it.
    """
    root = make_root(tmp_path)
    commit_entry(
        root, reach.measure("sys-a", "mod.py:1-5", None, real_inputs(tmp_path, 1.0), root=root, **ENTRY_FIELDS)
    )
    (root / lift.LIFTS / "sys_a.py").write_text(BRANCH_LIFT.replace('ns["total"]', 'ns["total"] + 0'), encoding="utf-8")
    findings = reach.reach_findings(root).findings
    assert len(findings) == 1 and "STALE" in findings[0] and "lift digest" in findings[0], findings


@state_check
def test_an_entry_with_no_declared_lift_is_reported(tmp_path):
    """Property: an entry in the file of a system whose lift module does not declare that unit is reported, naming the unit.

    Class: an entry copied into another system's reach file.
    Why required: an orphan entry checks nothing and reads as coverage.
    Killed by: skipping entries that match no declaration.
    A second correct implementation: refusing at read time.
    """
    root = make_root(tmp_path)
    entry = reach.measure("sys-a", "mod.py:1-5", None, real_inputs(tmp_path, 1.0), root=root, **ENTRY_FIELDS)
    commit_entry(root, entry, system="sys_b")
    findings = reach.reach_findings(root).findings
    assert len(findings) == 1 and "mod.py:1-5" in findings[0] and "no declared lift" in findings[0], findings


@state_check
def test_a_declared_lift_with_no_entry_is_counted_and_printed(tmp_path):
    """Property: a tree with one declared lift and no entry reports one lift with no entry, and the summary line says `1 declared lift(s) with no entry`.

    Class: a root with a declared lift and no reach directory.
    Why required: a pass over zero entries must read as what it is, not as coverage.
    Killed by: reporting only findings, or counting entries instead of lifts.
    A second correct implementation: failing when any lift has no entry, which would change this row's landing state.
    """
    root = make_root(tmp_path)
    report = reach.reach_findings(root)
    assert (report.findings, report.entries, report.declared, report.without_entry) == ([], 0, 1, 1)
    assert "1 declared lift(s) with no entry" in reach.summary_line(report)
    assert "0 declared lift(s) with no entry" in reach.summary_line(reach.reach_findings(tmp_path / "empty"))


@state_check
@pytest.mark.parametrize(
    ("edit", "fragment"),
    [
        (lambda t: t.replace('commit = "fedcba9876543210"\n', ""), "lacks the fields"),
        (lambda t: t.replace("lines = [1, 4, 5]", "lines = [5, 4, 1]"), "distinct positive integers in order"),
        (lambda t: t.replace("lines = [1, 4, 5]", "lines = [1, 1, 5]"), "distinct positive integers in order"),
        (lambda t: t.replace("lines = [1, 4, 5]", "lines = [0, 4]"), "distinct positive integers in order"),
        (lambda t: t.replace("lines = [1, 4, 5]", 'lines = [1, "4"]'), "distinct positive integers in order"),
        (lambda t: t + "\n" + t, "recorded twice"),
        (lambda t: t + "[[entry\n", "not readable TOML"),
    ],
    ids=["missing field", "unsorted", "repeated", "zero", "non-integer", "duplicate entry", "broken toml"],
)
def test_a_malformed_reach_file_is_refused_by_name(tmp_path, edit, fragment):
    """Property: `read_entries` refuses each malformed file, naming the file and what is wrong.

    Class: seven edits of one valid entry, one per way a reach file can be malformed.
    Why required: a reach file is hand-committed, and a quietly accepted malformed entry weakens the check.
    Killed by: dropping the matching validation; the control is the valid file the edits start from.
    A second correct implementation: validating with a schema library.
    """
    root = make_root(tmp_path)
    valid = reach.measure("sys-a", "mod.py:1-5", None, real_inputs(tmp_path, -1.0), root=root, **ENTRY_FIELDS)
    commit_entry(root, valid)
    assert len(reach.read_entries(root)) == 1  # control: the unedited file reads
    commit_entry(root, edit(valid))
    with pytest.raises(reach.ReachError, match=fragment):
        reach.read_entries(root)


@state_check
def test_a_part_is_part_of_the_entry_key(tmp_path):
    """Property: an entry for a lift that declares a part records the part and is matched to that part only.

    Class: a lift declared with `part="left"`, and the same entry with its part changed to `right`.
    Why required: two parts of one unit have separate lifts, so an entry for one must not vouch for the other.
    Killed by: matching entries on the unit alone; the control is the entry with the right part, which passes.
    A second correct implementation: folding the part into the unit string.
    """
    root = make_root(tmp_path, source=BRANCH_LIFT.replace('unit="mod.py:1-5"', 'unit="mod.py:1-5", part="left"'))
    entry = reach.measure("sys-a", "mod.py:1-5", "left", real_inputs(tmp_path, 1.0), root=root, **ENTRY_FIELDS)
    assert 'part = "left"' in entry
    commit_entry(root, entry)
    assert reach.reach_findings(root).findings == []
    commit_entry(root, entry.replace('part = "left"', 'part = "right"'))
    findings = reach.reach_findings(root).findings
    assert len(findings) == 1 and "no declared lift" in findings[0], findings


@state_check
def test_a_missing_input_file_and_an_unknown_declaration_are_refused_naming_them(tmp_path):
    """Property: `measure` names the input file a declaration needs when the directory lacks it, and refuses a (system, unit, part) that no declaration matches.

    Class: an empty inputs directory, and a unit key with no lift.
    Why required: a measurement with the wrong inputs or for the wrong unit would be committed as evidence.
    Killed by: running the lift on an empty dict, or taking the first declaration found.
    A second correct implementation: listing the directory's files in the message.
    """
    root = make_root(tmp_path)
    empty = tmp_path / "empty_inputs"
    empty.mkdir()
    with pytest.raises(reach.ReachError, match=r"x\.npz"):
        reach.measure("sys-a", "mod.py:1-5", None, empty, root=root, **ENTRY_FIELDS)
    with pytest.raises(reach.ReachError, match="0 declared lifts"):
        reach.measure("sys-a", "mod.py:9-9", None, empty, root=root, **ENTRY_FIELDS)


@state_check
def test_the_tracer_puts_back_the_tracer_that_was_installed(tmp_path):
    """Property: after a traced run `sys.gettrace()` and `threading.gettrace()` are the objects they were before.

    Class: a custom tracer installed by the test (as a debugger or a coverage tool would).
    Why required: a run that leaves its tracer installed corrupts any coverage measurement around it.
    Killed by: `sys.settrace(None)` in the `finally`.
    A second correct implementation: `sys.monitoring`, which composes with other tools.
    """
    root = make_root(tmp_path)
    declaration = declaration_of(root)

    def mine(_frame, _event, _arg):
        """A tracer that traces nothing."""
        return None

    sys.settrace(mine)
    threading.settrace(mine)
    try:
        reach.reached_lines(declaration)
        assert sys.gettrace() is mine and threading.gettrace() is mine
    finally:
        sys.settrace(None)
        threading.settrace(None)


THREAD_TEXT = (
    "def work():\n    inside = 1\n    return inside\n\nt = threading.Thread(target=work)\nt.start()\nt.join()\n"
)
THREAD_LIFT = (
    HEAD
    + '@declares_lift(unit="mod.py:1-7")\n'
    + "def declare(fixtures):\n"
    + '    """Bind the threading module."""\n'
    + "    import threading\n\n"
    + '    return {"threading": threading}, lambda ns: None\n'
)


@state_check
def test_a_line_run_in_a_thread_the_lift_starts_is_recorded(tmp_path):
    """Property: a legacy line executed in a thread the unit's own text starts is part of the reach.

    Class: a unit whose text defines a function and runs it in a `threading.Thread`.
    Why required: the instrument must not under-report reach for the units that use threads.
    Killed by: `sys.settrace` without `threading.settrace`; the control is that line 2 is not reached by the main thread.
    A second correct implementation: tracing with `sys.monitoring`, which sees all threads.
    """
    root = make_root(tmp_path, THREAD_TEXT, THREAD_LIFT, fixture=False, last=7)
    reached = reach.reached_lines(declaration_of(root))
    assert {1, 2, 3, 5, 6, 7} <= reached.lines


@state_check
def test_only_the_units_own_lines_are_recorded(tmp_path):
    """Property: lines of a helper the declaration binds, which live in another file, are not in the reach.

    Class: a unit whose text calls a bound helper defined in the lift module.
    Why required: the record is of legacy line numbers; a helper's line number is not one.
    Killed by: recording every traced line regardless of file; the control is that the text's own lines are recorded.
    A second correct implementation: filtering by code object identity instead of file name.
    """
    text = "result = helper()\n"
    source = (
        HEAD
        + '@declares_lift(unit="mod.py:1-1")\n'
        + "def declare(fixtures):\n"
        + '    """Bind a helper that runs several lines."""\n'
        + "    def helper():\n"
        + '        """Run three lines of its own."""\n'
        + "        a = 1\n"
        + "        b = 2\n"
        + "        return a + b\n\n"
        + '    return {"helper": helper}, lambda ns: ns["result"]\n'
    )
    root = make_root(tmp_path, text, source, fixture=False, last=1)
    assert reach.reached_lines(declaration_of(root)).lines == {1}


class Data:
    """The series the real stored function receives: its minimum decides which branch runs."""

    def __init__(self, minimum: float):
        self.minimum = minimum

    def min(self):
        """Return the configured minimum."""
        return self.minimum

    def __iadd__(self, _shift):
        """Absorb the shift the function adds when the minimum is not positive."""
        return self


def real_text_declaration(minimum: float) -> lift.LiftDeclaration:
    """The real stored text of `__ref_ams_functions.py:23-30`, called on data with the given minimum."""
    stats = types.SimpleNamespace(boxcox=lambda data: (data, 0.5))
    return lift.LiftDeclaration(
        unit="__ref_ams_functions.py:23-30",
        part=None,
        form="unchanged",
        fixtures=(),
        draws="none",
        function=lambda _fixtures: (
            {"stats": stats},
            lambda ns: ns["boxcox_transformation"](Data(minimum)),
        ),
        system="real",
        path=REPO_ROOT / "tests" / "lifts" / "real.py",
        root=REPO_ROOT,
    )


@state_check
def test_the_real_stored_text_reports_lines_inside_23_to_30_and_the_branch_the_data_takes():
    """Property: tracing the real stored text reports legacy lines inside 23 to 30 only, and the branch taken depends on the data.

    Class: the real extraction run with a positive minimum and with a negative minimum.
    Why required: the instrument must name legacy lines without translation, and must tell two inputs apart.
    Killed by: recording stored-copy line numbers (1 to 8), or the line shift lost; the pair of runs is the control that reach is not constant.
    A second correct implementation: running under coverage.py and mapping its lines.
    """
    positive = reach.reached_lines(real_text_declaration(1.0))
    negative = reach.reached_lines(real_text_declaration(-1.0))
    assert positive.lines <= set(range(23, 31)) and negative.lines <= set(range(23, 31))
    assert positive.lines == {24, 25, 26, 29, 30}
    assert negative.lines == {24, 25, 26, 27, 28, 29, 30}


@state_check
def test_an_adapted_copy_with_an_inserted_line_records_only_mapped_lines_inside_23_to_30(tmp_path):
    """Property: an adapted copy of the real text with one inserted line records legacy lines inside 23 to 30 only, and lists the inserted line as unmapped.

    Class: an adapted lift whose copy differs from the stored text by one added statement.
    Why required: a line the adapter added has no legacy number and must never count as reach.
    Killed by: recording adapted line numbers directly (they run past 30 and shift every later line), or mapping the inserted line to its neighbour.
    A second correct implementation: mapping through a `difflib.SequenceMatcher` instead of the stored diff.
    """
    real = (REPO_ROOT / extraction.stored_path("stormy", "__ref_ams_functions.py", 23, 30)).read_text(encoding="utf-8")
    lines = real.splitlines(keepends=True)
    adapted = "".join(lines[:2] + ["    unused_marker = 0\n"] + lines[2:])
    root = tmp_path
    (root / corpus_record.RECORD_NAME).write_text(RECORD, encoding="utf-8")
    stored = root / extraction.stored_path("stormy", "__ref_ams_functions.py", 23, 30)
    stored.parent.mkdir(parents=True)
    stored.write_text(real, encoding="utf-8")
    copy = root / lift.ADAPTED / "sys_a" / "__ref_ams_functions"
    copy.mkdir(parents=True)
    (copy / "23-30.txt").write_text(adapted, encoding="utf-8")
    (copy / "23-30.diff").write_text(lift._adapted_diff(real.encode(), adapted.encode()), encoding="utf-8")
    source = (
        HEAD
        + '@declares_lift(unit="__ref_ams_functions.py:23-30", form="adapted")\n'
        + "def declare(fixtures):\n"
        + '    """Bind a stub for stats and call the function on data with a positive minimum."""\n'
        + "    import types\n\n"
        + "    class Data:\n"
        + '        """A series whose minimum is positive."""\n\n'
        + "        def min(self):\n"
        + '            """Return 1.0."""\n'
        + "            return 1.0\n\n"
        + "    stats = types.SimpleNamespace(boxcox=lambda data: (data, 0.5))\n"
        + '    return {"stats": stats}, lambda ns: ns["boxcox_transformation"](Data())\n'
    )
    (root / lift.LIFTS).mkdir(parents=True, exist_ok=True)
    (root / lift.LIFTS / "sys_a.py").write_text(source, encoding="utf-8")
    reached = reach.reached_lines(declaration_of(root))
    assert reached.lines <= set(range(23, 31)) and {24, 25, 26} <= reached.lines
    assert any(number == 3 for _file, number in reached.unmapped), reached.unmapped
    assert all(number != 3 for number in reached.lines)


@state_check
def test_the_command_prints_the_entry_and_refuses_with_status_one(tmp_path):
    """Property: `python -m tests.correspondence.reach measure` prints a readable entry and exits 0; with a missing input it prints `refused:` and exits 1.

    Class: the command run as a child process against a temporary root.
    Why required: the command, not the function, is how an author measures, against data kept outside this repository.
    Killed by: a `main` that lets the exception escape (a traceback) or exits 0 on refusal.
    A second correct implementation: a console-script entry point calling `main`.
    """
    root = make_root(tmp_path)
    inputs = real_inputs(tmp_path, -1.0)
    base = [sys.executable, "-m", "tests.correspondence.reach", "measure", "--root", str(root)]
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    good = subprocess.run(
        [*base, "--system", "sys-a", "--unit", "mod.py:1-5", "--inputs", str(inputs)],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert good.returncode == 0, good.stderr
    commit_entry(root, good.stdout)
    assert reach.read_entries(root)[0][1]["lines"] == [1, 4, 5]
    assert str(inputs) not in good.stdout
    bad = subprocess.run(
        [*base, "--system", "sys-a", "--unit", "mod.py:1-5", "--inputs", str(tmp_path / "nowhere")],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert bad.returncode == 1 and bad.stderr.startswith("refused:") and "Traceback" not in bad.stderr


def test_the_entries_committed_in_this_checkout_are_reached_by_their_committed_fixtures(capsys):
    """Property: every reach entry committed in this checkout is current and reached by its lift on its committed fixtures; the counts are printed.

    Class: every entry under `tests/correspondence/reach/` now or later, read from the directory.
    Why required: this is the check that synthetic inputs reach the lines real data reaches.
    Killed by: removing a fixture that an entry relies on, or editing a lift or stored text after its entry was measured.
    A second correct implementation: running the same findings from the command's `summary` subcommand.
    Not established: with no entry committed this passes over zero entries; the printed count of declared lifts with no entry is what says so.
    """
    extraction_check.require_extractions(REPO_ROOT)
    report = reach.reach_findings(REPO_ROOT)
    with capsys.disabled():
        print("\n" + reach.summary_line(report))
    assert report.findings == []
