"""Check that every equivalence test records the ruling of each known defect that lies inside its unit.

The known-defect register says, for each defect found in the legacy code, whether the new code must still be able
to reproduce it (`OBLIGED`) or may depart from it (`LICENSED`). A test written over such a defect has to carry that
ruling, or the way the test treats the defect would be a choice nobody can see. No harness module reads the
register except the register's own loader, so without this module a test could be authored over a defect with no
ruling, or with the wrong one, and nothing would fail.

The record is a module-level literal in each equivalence test module:

    RULINGS = {"{unit key}": {{line}: "OBLIGED" or "LICENSED", ...}, ...}

and `rulings_findings` compares it, unit by unit, with the register. For every equivalence test the unit key is
resolved to its line ranges, the register entries whose line lies inside those ranges are collected, and the
module's `RULINGS` entry for that unit has to equal them exactly: the same lines, each with the register's ruling.
A unit whose ranges hold no entry needs no `RULINGS` entry (an empty one is allowed). A register entry still
`UNRULED` is refused whatever the module records, because no test may be authored over an entry without a ruling.
A refactor-reminder test is not read: it compares nothing, so there is nothing to rule on.

The check reads the register's text, the test files' syntax trees and nothing else, so it runs on every push in the
State check job. It is a function of a root, which is how the tests below plant a violating tree.

Not established: that a register entry's `line` lies inside its own unit; the register's loader checks only that it
is an integer, and an entry that lies in no unit is never required of any test.
"""

import ast
import tomllib
from pathlib import Path

import pytest

from tests.correspondence import corpus_record, inventory, known_defects, ledger

pytestmark = pytest.mark.state_check

REPO_ROOT = inventory.REPO_ROOT
LITERAL = "RULINGS"
RULED = ("OBLIGED", "LICENSED")


def _spans(unit: str) -> tuple[str, list[tuple[int, int]]]:
    """The file name and the (first, last) line ranges of a canonical unit key."""
    match = ledger.KEY_RE.match(unit)
    if match is None:
        raise ValueError(f"{unit!r} is not a canonical unit key")
    return match["file"], [tuple(map(int, span.split("-"))) for span in match["ranges"].split(",")]


def register_entries(root: Path) -> list[dict]:
    """Every entry of the known-defect register under `root`, as the register's text states it."""
    path = root / known_defects.REGISTER_PATH
    return tomllib.loads(path.read_text(encoding="utf-8")).get("entry", []) if path.is_file() else []


def entries_inside(unit: str, entries: list[dict]) -> list[dict]:
    """The register entries whose file is the unit's file and whose line lies in one of the unit's ranges."""
    file, ranges = _spans(unit)
    return [
        e
        for e in entries
        if isinstance(e.get("key"), str)
        and e["key"].split(":", 1)[0] == file
        and isinstance(e.get("line"), int)
        and any(first <= e["line"] <= last for first, last in ranges)
    ]


def _form_problems(value: object) -> list[str]:
    """Each way a `RULINGS` value is not a dict of unit key to a dict of integer line to a ruling name."""
    if not isinstance(value, dict):
        return [f"{LITERAL} is not a dict of unit key to a dict of line to ruling"]
    return [
        f"{LITERAL}[{unit!r}] is not a dict of integer line to {' or '.join(RULED)}"
        for unit, lines in value.items()
        if not isinstance(lines, dict)
        or not all(isinstance(n, int) and not isinstance(n, bool) and r in RULED for n, r in lines.items())
    ]


def read_rulings(path: Path) -> tuple[dict | None, list[str]]:
    """The module's `RULINGS` literal, or None when it has none or its form is wrong, and each finding about the form.

    The literal is read from the syntax tree and never imported, so a module that does not import yet is still
    read. A value that cannot be read as a literal at all (a computed one) is a finding too.
    """
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            targets = [node.target]
        else:
            continue
        if not any(isinstance(t, ast.Name) and t.id == LITERAL for t in targets):
            continue
        try:
            value = ast.literal_eval(node.value)
        except ValueError:
            return None, [f"{LITERAL} is not a literal, so it cannot be read without importing the module"]
        problems = _form_problems(value)
        return (None if problems else value), problems
    return None, []


def _expected(unit: str, entries: list[dict]) -> tuple[dict[int, str], list[str]]:
    """The register's ruling at each line inside `unit`, and a finding for each entry that cannot be recorded."""
    found, problems = {}, []
    for entry in entries_inside(unit, entries):
        line, ruling = entry["line"], entry.get("ruling")
        if ruling not in RULED:
            problems.append(
                f"unit {unit} line {line}: the register ruling is {ruling!r}, so no test may be authored over it"
            )
        elif found.setdefault(line, ruling) != ruling:
            problems.append(
                f"unit {unit} line {line}: two register entries carry different rulings and {LITERAL} holds one"
            )
    return found, problems


def _legacy_tree(root: Path, name: str) -> ast.Module | None:
    """The syntax tree of a legacy file under `root`, or None when the corpus record or the file is not there."""
    try:
        record = tomllib.loads((root / corpus_record.RECORD_NAME).read_text(encoding="utf-8"))
        directory = record["section"][0]["directory"]
        return ast.parse((root / directory / name).read_text(encoding="utf-8"))
    except (OSError, KeyError, IndexError, SyntaxError, UnicodeDecodeError):
        return None


def _star_modules(tree: ast.Module) -> list[str]:
    """The file names of the modules a legacy file star-imports, latest import first, as a later star-import shadows an earlier."""
    return [
        f"{node.module}.py"
        for node in reversed(tree.body)
        if isinstance(node, ast.ImportFrom) and node.level == 0 and any(a.name == "*" for a in node.names)
    ]


def _definition(root: Path, file: str, name: str) -> tuple[str, ast.FunctionDef | ast.AsyncFunctionDef] | None:
    """The top-level function `name` as the file's own legacy module would resolve it: its own file first, then its star-imports."""
    tree = _legacy_tree(root, file)
    if tree is None:
        return None
    for candidate in (file, *_star_modules(tree)):
        found = _legacy_tree(root, candidate)
        for node in found.body if found else []:
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name == name:
                return candidate, node
    return None


def _called_names(nodes) -> set[str]:
    """The bare names called anywhere under `nodes`, less the names defined under them."""
    called, defined = set(), set()
    for top in nodes:
        for node in ast.walk(top):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                called.add(node.func.id)
            elif isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
                defined.add(node.name)
    return called - defined


def reached_definitions(unit: str, root: Path) -> list[tuple[str, int, int]]:
    """Each legacy function a unit reaches by bare-name calls, as (file, first line, last line), transitively.

    A name is resolved as the legacy module would resolve it (`_definition`). A function the unit defines in its
    own ranges is not a callee. The result is a floor: a function passed as an argument or reached through an
    attribute is not seen. A root with no readable corpus gives an empty list.
    """
    file, ranges = _spans(unit)
    tree = _legacy_tree(root, file)
    if tree is None:
        return []
    inside = [
        n for n in ast.walk(tree) if hasattr(n, "lineno") and any(first <= n.lineno <= last for first, last in ranges)
    ]
    own = {n.name for n in inside if isinstance(n, ast.FunctionDef | ast.AsyncFunctionDef)}
    calls = {n.func.id for n in inside if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
    pending = [(file, name) for name in sorted(calls - own)]
    seen: dict[tuple[str, str], tuple[str, int, int]] = {}
    while pending:
        origin, name = pending.pop()
        found = _definition(root, origin, name)
        if found is None:
            continue
        where, node = found
        if (where, name) in seen:
            continue
        first = min([node.lineno, *(d.lineno for d in node.decorator_list)])
        seen[(where, name)] = (where, first, node.end_lineno)
        pending += [(where, called) for called in sorted(_called_names([node]))]
    return sorted(seen.values())


def expected_records(unit: str, entries: list[dict], root: Path) -> tuple[dict[str, dict[int, str]], list[str]]:
    """What a test of `unit` must record: unit key to line to ruling, for its own ranges and each reached callee.

    A site inside a reached callee is recorded under the register's own key for the entry, as the form of Q9 says.
    Also returns a finding for each entry that cannot be recorded (no ruling).
    """
    own, problems = _expected(unit, entries)
    wanted: dict[str, dict[int, str]] = {unit: dict(own)} if own else {}
    for file, first, last in reached_definitions(unit, root):
        for entry in entries:
            key, line = entry.get("key"), entry.get("line")
            if not (isinstance(key, str) and key.split(":", 1)[0] == file and isinstance(line, int)):
                continue
            if not first <= line <= last:
                continue
            ruling = entry.get("ruling")
            if ruling not in RULED:
                problems.append(
                    f"unit {key} line {line}: the register ruling is {ruling!r}, so no test may be authored over it"
                )
            else:
                wanted.setdefault(key, {})[line] = ruling
    return wanted, problems


def rulings_findings(root: Path = REPO_ROOT) -> list[str]:
    """One finding per way an equivalence test module's `RULINGS` differs from the register, each naming file and unit.

    Only equivalence tests are read. A test's rulings are those of every register site its lift reaches: the sites in
    its unit's ranges and those in each legacy function it reaches by bare-name calls (`expected_records`). The
    module's `RULINGS` must hold exactly those unit keys, each with exactly those lines and rulings. A module with no
    `RULINGS` is read as recording nothing.
    """
    tests, _unreadable = inventory.listing(root)  # an unreadable marker is the inventory's finding, not this one's
    register = register_entries(root)
    by_file: dict[str, list[str]] = {}
    for test in tests:
        if test["family"] == "equivalence":
            by_file.setdefault(test["file"], []).append(test["unit"])
    findings = []
    for file, units in sorted(by_file.items()):
        rulings, problems = read_rulings(root / file)
        findings += [f"{file}: {problem}" for problem in problems]
        recorded = rulings or {}
        wanted: dict[str, dict[int, str]] = {}
        for unit in sorted(set(units)):
            expected, unrecordable = expected_records(unit, register, root)
            findings += [f"{file}: {problem}" for problem in unrecordable]
            for key, lines in expected.items():
                wanted.setdefault(key, {}).update(lines)
        for key, lines in sorted(wanted.items()):
            have = recorded.get(key, {})
            findings += [
                f"{file}: unit {key} line {line}: {LITERAL} records {have.get(line)!r} and the register rules {ruling}"
                for line, ruling in sorted(lines.items())
                if have.get(line) != ruling
            ]
        for key, have in sorted(recorded.items()):
            if key not in wanted and have:
                findings.append(f"{file}: {LITERAL} names unit {key}, which no equivalence test of this module reaches")
            findings += [
                f"{file}: unit {key} line {line}: {LITERAL} records {ruling!r} for a line with no register entry"
                for line, ruling in sorted(have.items())
                if key in wanted and line not in wanted[key]
            ]
    return findings


def coverage(root: Path = REPO_ROOT) -> tuple[int, int]:
    """How many equivalence-tested units reach a register entry, and how many (unit, entry) pairs they reach."""
    tests, _unreadable = inventory.listing(root)
    register = register_entries(root)
    units = sorted({t["unit"] for t in tests if t["family"] == "equivalence"})
    reached = {u: sum(len(lines) for lines in expected_records(u, register, root)[0].values()) for u in units}
    return sum(1 for n in reached.values() if n), sum(reached.values())


# ---- tests ----

UNIT = "mod.py:1-9"
OTHER = "mod.py:10-19"
TEST = '@pytest.mark.equivalence(unit="{unit}", system="sys-a")\ndef test_{n}():\n    pass\n'


def _entries(*rows: tuple[str, int, str]) -> str:
    """A register holding one entry per (key, line, ruling)."""
    return "".join(f'[[entry]]\nkey = "{k}"\nline = {n}\nruling = "{r}"\n\n' for k, n, r in rows)


def _module(rulings: str | None, *units: str, family: str = "equivalence") -> str:
    """A test module covering `units`, with the `RULINGS` source given, or none when it is None."""
    head = "import pytest\n\n" + (f"RULINGS = {rulings}\n\n\n" if rulings is not None else "\n")
    return head + "\n".join(TEST.format(unit=u, n=i).replace("equivalence", family) for i, u in enumerate(units))


def _tree(tmp_path: Path, register: str, **modules: str) -> Path:
    """A root holding the register text and one test module per keyword."""
    (tmp_path / known_defects.REGISTER_PATH).parent.mkdir(parents=True)
    (tmp_path / known_defects.REGISTER_PATH).write_text(register, encoding="utf-8")
    (tmp_path / "tests" / "equivalence").mkdir(parents=True)
    for name, source in modules.items():
        (tmp_path / "tests" / "equivalence" / f"test_{name}.py").write_text(source, encoding="utf-8")
    return tmp_path


def test_a_module_recording_exactly_the_registers_rulings_has_no_finding(tmp_path):
    """Property: a module whose `RULINGS` equals the register for its unit, line for line and ruling for ruling, has no finding.

    Class: one unit holding two entries, `OBLIGED` at line 3 and `LICENSED` at line 7, recorded as the register has
    them. Why it is required: the satisfying arm of the check; without it every other test here would pass for a
    check that refuses everything. Killed by: a check that always reports a finding, or that reads the record but
    never compares lines. Second correct implementation: comparing the record with the register as two sets of
    (line, ruling) pairs passes the same input.
    """
    root = _tree(
        tmp_path,
        _entries((UNIT, 3, "OBLIGED"), (UNIT, 7, "LICENSED")),
        a=_module('{"mod.py:1-9": {3: "OBLIGED", 7: "LICENSED"}}', UNIT),
    )
    assert rulings_findings(root) == []


def test_a_unit_whose_defect_is_not_recorded_is_a_finding_naming_the_unit_and_line(tmp_path):
    """Property: a module with no `RULINGS` over a unit holding a register entry is a finding naming file, unit and line.

    Class: the test is authored over `mod.py:1-9`, which holds an `OBLIGED` entry at line 3, and the module records
    nothing. Why it is required: it is the failure the record exists to prevent, a test authored over a defect with
    no visible ruling. Killed by: a check that reads a missing `RULINGS` as "nothing to check". Second correct
    implementation: treating a missing literal as an empty dict and comparing passes the same input.
    """
    root = _tree(tmp_path, _entries((UNIT, 3, "OBLIGED")), a=_module(None, UNIT))
    (finding,) = rulings_findings(root)
    assert "test_a.py" in finding and UNIT in finding and "line 3" in finding and "OBLIGED" in finding


def test_a_recorded_ruling_that_differs_from_the_register_is_a_finding(tmp_path):
    """Property: recording `LICENSED` where the register rules `OBLIGED` is a finding naming both.

    Class: the right line with the other ruling. Why it is required: a test licensed to depart from a defect the
    new code is obliged to reproduce passes for the wrong reason. Killed by: a check that compares only the set of
    lines. Second correct implementation: comparing the (line, ruling) pairs passes the same input.
    """
    root = _tree(tmp_path, _entries((UNIT, 3, "OBLIGED")), a=_module('{"mod.py:1-9": {3: "LICENSED"}}', UNIT))
    (finding,) = rulings_findings(root)
    assert "'LICENSED'" in finding and "OBLIGED" in finding


def test_a_recorded_line_with_no_register_entry_is_a_finding(tmp_path):
    """Property: a line the module records and the register does not hold is a finding.

    Class: a record of line 5 beside the register's single entry at line 3. Why it is required: "equal exactly"
    includes the extra line; a record that only had to contain the register's rulings could drift from it unseen.
    Killed by: a check that looks up each register entry in the record and never the other way. Second correct
    implementation: comparing the two dicts for equality passes the same input.
    """
    root = _tree(
        tmp_path,
        _entries((UNIT, 3, "OBLIGED")),
        a=_module('{"mod.py:1-9": {3: "OBLIGED", 5: "LICENSED"}}', UNIT),
    )
    (finding,) = rulings_findings(root)
    assert "line 5" in finding and "no register entry" in finding


def test_a_register_entry_still_unruled_is_refused_whatever_the_module_records(tmp_path):
    """Property: an `UNRULED` register entry inside a tested unit is a finding even when the module records it.

    Class: an `UNRULED` entry at line 3 and a module that records `3: "UNRULED"`. Why it is required: no test may be
    authored over an entry that has no ruling, and the record would otherwise be a way to authorise one. Killed by:
    a check that compares the record with the register's ruling field without testing it against the two rulings.
    Second correct implementation: refusing the entry before the comparison passes the same input.
    """
    root = _tree(tmp_path, _entries((UNIT, 3, "UNRULED")), a=_module('{"mod.py:1-9": {3: "UNRULED"}}', UNIT))
    findings = rulings_findings(root)
    assert any("UNRULED" in f and "no test may be authored" in f for f in findings)


def test_a_record_for_a_unit_the_module_does_not_test_is_a_finding(tmp_path):
    """Property: a `RULINGS` key naming a unit that no equivalence test of the module covers is a finding.

    Class: a record for `mod.py:10-19` in a module that tests only `mod.py:1-9`. Why it is required: a record kept
    after its test moved or was deleted would keep the register's rulings looking recorded. Killed by: a check that
    walks only the units the tests name. Second correct implementation: comparing the record's keys with the
    module's units passes the same input.
    """
    root = _tree(tmp_path, _entries((OTHER, 12, "LICENSED")), a=_module('{"mod.py:10-19": {12: "LICENSED"}}', UNIT))
    (finding,) = rulings_findings(root)
    assert OTHER in finding and "no equivalence test" in finding


def test_an_entry_outside_the_units_ranges_is_not_required_and_one_in_a_later_range_is(tmp_path):
    """Property: only entries inside the unit's own ranges are required, and a multi-range key counts every range.

    Class: a unit `mod.py:1-2,6-7` with entries at line 5 (between its ranges, so outside them) and line 6 (inside
    the second range). Why it is required: it separates "the unit's lines" from "the unit's file" and from "the
    unit's first range"; a position that only the file test, or only the first-range test, gets wrong. Killed by:
    matching on the file alone, which also requires line 5, or on the first range alone, which misses line 6.
    Second correct implementation: matching on the set of the unit's lines passes the same input.
    """
    unit = "mod.py:1-2,6-7"
    root = _tree(
        tmp_path,
        _entries((unit, 6, "OBLIGED"), ("mod.py:3-5", 5, "LICENSED")),
        a=_module('{"mod.py:1-2,6-7": {6: "OBLIGED"}}', unit),
    )
    assert rulings_findings(root) == []
    (finding,) = rulings_findings(_tree(tmp_path / "second", _entries((unit, 6, "OBLIGED")), a=_module(None, unit)))
    assert "line 6" in finding


def test_a_unit_without_an_entry_needs_no_record_and_may_carry_an_empty_one(tmp_path):
    """Property: a unit holding no register entry has no finding with no `RULINGS` entry or an empty one.

    Class: two units in one module, only one of which holds an entry, recorded for that one and empty for the other.
    Why it is required: the record covers the units that hold entries, and a check demanding a record of every
    unit would force an empty entry into each of the other tests. Killed by: a check that requires a key for every
    tested unit. Second correct implementation: looking up `RULINGS.get(unit, {})` passes the same input.
    """
    root = _tree(
        tmp_path,
        _entries((UNIT, 3, "OBLIGED")),
        a=_module('{"mod.py:1-9": {3: "OBLIGED"}, "mod.py:10-19": {}}', UNIT, OTHER),
        b=_module(None, OTHER),
    )
    assert rulings_findings(root) == []


def test_a_reminder_over_a_defect_is_not_read(tmp_path):
    """Property: a refactor-reminder test over a unit holding an entry needs no record.

    Class: the same module shape as the finding above, with the family marker `refactor_reminder`. Why it is
    required: a reminder compares nothing, so ruling on a defect there would be a requirement with nothing to
    attach to. Killed by: a check that reads every family test. Second correct implementation: filtering the
    inventory by family before grouping passes the same input.
    """
    root = _tree(tmp_path, _entries((UNIT, 3, "OBLIGED")), a=_module(None, UNIT, family="refactor_reminder"))
    assert rulings_findings(root) == []


def test_a_record_that_is_computed_or_malformed_is_a_finding_and_not_an_error(tmp_path):
    """Property: a `RULINGS` that is not a literal, or not a dict of unit to dict of line to ruling, is a finding.

    Class: a computed value, and a ruling name outside the two. Why it is required: the record is read from the
    syntax tree, so a form the tree cannot give back as a literal has to be named, not skipped or raised. Killed by:
    `eval` of the source, which accepts a computed record, or a bare `literal_eval` that lets the exception escape.
    Second correct implementation: importing the module and reading the attribute passes the first input.
    """
    computed = _tree(tmp_path / "c", _entries(), a=_module("dict()", UNIT))
    (finding,) = rulings_findings(computed)
    assert "not a literal" in finding
    malformed = _tree(tmp_path / "m", _entries(), a=_module('{"mod.py:1-9": {3: "WAIVED"}}', UNIT))
    assert any("not a dict of integer line" in f for f in rulings_findings(malformed))


def test_coverage_counts_the_tested_units_that_hold_entries_and_those_entries(tmp_path):
    """Property: `coverage` returns the number of equivalence-tested units holding an entry and the entries inside them.

    Class: one tested unit with two entries, one tested unit with none, and an entry in an untested unit. Why it is
    required: the completed run is stated as "20 units, 35 entries", and the figure has to be read from this
    function rather than counted by hand. Killed by: counting every register entry, which gives 3 entries, or
    every tested unit, which gives 2 units. Second correct implementation: summing `len(entries_inside(unit))` over
    the tested units passes the same input.
    """
    root = _tree(
        tmp_path,
        _entries((UNIT, 3, "OBLIGED"), (UNIT, 7, "LICENSED"), ("other.py:1-9", 2, "OBLIGED")),
        a=_module('{"mod.py:1-9": {3: "OBLIGED", 7: "LICENSED"}}', UNIT, OTHER),
    )
    assert coverage(root) == (1, 2)


def _legacy(root: Path, **files: str) -> Path:
    """Give `root` a one-section corpus record and legacy files named by keyword (`name_py` for `name.py`)."""
    (root / "legacy").mkdir(exist_ok=True)
    (root / corpus_record.RECORD_NAME).write_text(
        '[[section]]\nname = "stormy"\ndirectory = "legacy"\n', encoding="utf-8"
    )
    for name, text in files.items():
        (root / "legacy" / (name.removesuffix("_py") + ".py")).write_text(text, encoding="utf-8")
    return root


LIB = "def helper(x):\n    return x + 1\n\n\ndef deep(x):\n    return helper(x)\n\n\ndef unused(x):\n    return x\n"
MOD = "from lib import *\n\nresult = deep(1)\n"
CALLEE = "lib.py:1-2"
DEEP = "lib.py:5-6"
UNUSED = "lib.py:9-10"
UNIT_MOD = "mod.py:3-3"


def _callee_tree(tmp_path: Path, register: str, rulings: str | None) -> Path:
    """A root with a unit that calls `deep`, which calls `helper`, both in a star-imported file, and an unused `unused`."""
    root = _tree(tmp_path, register, a=_module(rulings, UNIT_MOD))
    return _legacy(root, lib_py=LIB, mod_py=MOD)


def test_a_site_in_a_function_the_unit_calls_must_be_recorded_under_the_registers_key(tmp_path):
    """Property: an `OBLIGED` entry inside a callee the unit's text calls is a finding until the module records it under the entry's own key.

    Class: a unit calling `deep` (a star-imported function), with a register entry at line 2 of `helper`, which
    `deep` calls, so it is reached through two calls; the module records nothing, then records the key. Why it is
    required: the unwidened check keyed on the unit's own ranges and read this tree as clean, so a test authored
    over a defect its lift runs recorded no ruling. Killed by: the check as it stood (own ranges only), which gives
    no finding for the first input. Second correct implementation: recording every register entry of the unit's
    file closure would pass the second input and over-require the first.
    """
    register = _entries((CALLEE, 2, "OBLIGED"))
    (finding,) = rulings_findings(_callee_tree(tmp_path / "bare", register, None))
    assert CALLEE in finding and "line 2" in finding and "OBLIGED" in finding
    assert rulings_findings(_callee_tree(tmp_path / "ok", register, '{"lib.py:1-2": {2: "OBLIGED"}}')) == []


def test_a_function_the_unit_does_not_reach_is_not_required_and_recording_it_is_a_finding(tmp_path):
    """Property: an entry in a function no call of the unit reaches needs no record, and a record of it is refused.

    Class: an entry in `unused`, which nothing calls. Why it is required: "every site the lift reaches" is not
    "every site of the file"; over-requiring would force a ruling onto code the lift never runs. Killed by:
    requiring every entry of a file the unit's text names. Second correct implementation: tracing the lift run and
    recording the executed sites passes the first half.
    """
    register = _entries((UNUSED, 10, "LICENSED"))
    assert rulings_findings(_callee_tree(tmp_path / "bare", register, None)) == []
    (finding,) = rulings_findings(_callee_tree(tmp_path / "extra", register, '{"lib.py:9-10": {10: "LICENSED"}}'))
    assert "lib.py:9-10" in finding and "reaches" in finding


def test_a_callee_site_with_no_ruling_is_refused_and_a_function_the_unit_defines_is_no_callee(tmp_path):
    """Property: an `UNRULED` callee entry is refused; a function defined in the unit's own text is not looked up elsewhere.

    Class: an `UNRULED` entry in `helper`, and a unit whose text defines `deep` itself and calls it. Why it is
    required: no test may be authored over an entry without a ruling, through a call as in the unit's own ranges; and
    a locally defined name must not be resolved to a same-named function of the star-imported file. Killed by:
    skipping the ruling test for callee entries; resolving a call before checking local definitions. Second correct
    implementation: resolving by the file's scope analysis passes both.
    """
    unruled = _callee_tree(tmp_path / "unruled", _entries((CALLEE, 2, "UNRULED")), None)
    assert any("UNRULED" in f and "no test may be authored" in f for f in rulings_findings(unruled))
    own = _tree(tmp_path / "own", _entries((DEEP, 6, "OBLIGED")), a=_module(None, "mod.py:3-4"))
    _legacy(own, lib_py=LIB, mod_py="from lib import *\n\ndef deep(x):\n    return x\n")
    assert rulings_findings(own) == []


def test_a_callee_is_resolved_in_its_own_file_before_the_star_import(tmp_path):
    """Property: a name defined in the unit's file is that file's function even when a star-imported file defines it too.

    Class: `helper` defined in both `mod.py` and `lib.py`, the unit calling it, the register holding an entry only in
    `lib.py`. Why it is required: the legacy module resolves a bare name in its own file first, so the star-imported
    definition is not reached. Killed by: looking in the star-imported file first. Second correct implementation:
    importing both files and asking Python's namespace for the name passes.
    """
    mod = "from lib import *\n\ndef helper(x):\n    return x\n\n\nresult = helper(1)\n"
    root = _tree(tmp_path, _entries((CALLEE, 2, "OBLIGED")), a=_module(None, "mod.py:7-7"))
    _legacy(root, lib_py=LIB, mod_py=mod)
    assert rulings_findings(root) == []


def test_the_sites_the_lifts_of_the_combined_event_units_reach_are_the_ones_measured(tmp_path):
    """Property: on this checkout the check requires, for the four units that reach register sites through a call, the sites a trace of their lift runs executed.

    Class: the four units (`_b2_event_selection.py:273-324`, `:553-568`, `:588-617` and `_utils.py:1786-1989`) with
    the sites a `sys.settrace` over `lift.run_lift` recorded in frames of the stored legacy texts: 1274, 1390 and 1563;
    1477 and 1563; 1563 and 1633; 1563 and 1633. Why it is required: the check's reach is computed over syntax and is
    a floor on calls; the trace is the measurement it must contain. It also records the one difference found: the
    check adds 1765 for the last two units, a site their fixtures do not execute. Killed by: an own-range-only check
    (it requires none of these). Second correct implementation: a check that traced the lift would require exactly
    the executed sites. Not established: that a callee reached through an argument or an attribute is seen.
    """
    register = register_entries(REPO_ROOT)
    measured = {
        "_b2_event_selection.py:273-324": {1274, 1390, 1563},
        "_b2_event_selection.py:553-568": {1477, 1563},
        "_b2_event_selection.py:588-617": {1563, 1633},
        "_utils.py:1786-1989": {1563, 1633},
    }
    for unit, executed in measured.items():
        required = {line for lines in expected_records(unit, register, REPO_ROOT)[0].values() for line in lines}
        assert executed <= required, (unit, executed - required)
        assert required - executed <= {1765}, (unit, required - executed)


def test_every_equivalence_test_of_this_checkout_records_the_registers_rulings():
    """Property: on this checkout `rulings_findings` is empty.

    Class: the committed tree, whose equivalence tests are authored a system at a time. Why it is required: it is
    the check the push job runs, and the one that goes red when a test is authored over a defect with no record.
    Killed by: a check that cannot read the committed register. Second correct implementation: none other. Not
    established: that any equivalence test exists yet, so a pass here says nothing about coverage until
    `coverage` is read.
    """
    assert rulings_findings(REPO_ROOT) == []
