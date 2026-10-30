"""Reviewer tests for the reach check's named-absence record, specified by U-26's outcome 3.

`7a.1` landed the named-absence mechanism in `tests/correspondence/reach.py`, and no committed test
exercises it: `tests/correspondence/test_reach.py` holds no test of `read_named`, of
`ReachReport.named`, or of the two findings `reach_findings` must report for a named table, and
U-26's own text records that file as outside `7a`'s edit surface. The coder measured the mechanism
with three ad-hoc probes pasted into one round; nothing re-runs them, so the property that a named
absence stays a RECORD rather than becoming an EXEMPTION would survive a later edit that deleted the
arm silently.

Each test builds a small root under pytest's temporary directory in the shape `test_reach.py` uses: a
corpus record naming one section, one stored text per declared unit, a lift module with two
declarations, one generated fixture, and an optional reach file holding entries and named tables.
Every test writes only under `tmp_path`, and none reads a path outside the checkout.

Each docstring names the property, the input class, why it is required, the change that kills it, and
a second correct implementation it still passes.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from tests.correspondence import corpus_record, extraction, fixtures, lift, reach, value_codec

state_check = pytest.mark.state_check

RECORD = 'rule = "corpus-record:1"\n\n[[section]]\nname = "stormy"\ndirectory = "legacy"\n'
HEAD = "from tests.correspondence.lift import declares_lift\n\n\n"
UNIT_FIRST = "mod.py:1-5"
UNIT_SECOND = "mod.py:6-10"
BRANCH_TEXT = "if x[0] > 0:\n    branch = 1\nelse:\n    branch = 2\ntotal = branch\n"
TWO_DECLARATIONS = (
    HEAD
    + f'@declares_lift(unit="{UNIT_FIRST}", fixtures=["x"])\n'
    + "def declare_first(fixtures):\n"
    + '    """Hand the fixture to the first text as x."""\n'
    + '    return {"x": fixtures["x"]}, lambda ns: ns["total"]\n'
    + "\n\n"
    + f'@declares_lift(unit="{UNIT_SECOND}", fixtures=["x"])\n'
    + "def declare_second(fixtures):\n"
    + '    """Hand the fixture to the second text as x."""\n'
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


def make_root(root: Path) -> Path:
    """A root holding the corpus record, two stored texts, a lift module of two declarations and their one fixture."""
    root.mkdir(parents=True, exist_ok=True)
    (root / corpus_record.RECORD_NAME).write_text(RECORD, encoding="utf-8")
    for first, last in ((1, 5), (6, 10)):
        stored = root / extraction.stored_path("stormy", "mod.py", first, last)
        stored.parent.mkdir(parents=True, exist_ok=True)
        stored.write_text(BRANCH_TEXT, encoding="utf-8")
    (root / lift.LIFTS).mkdir(parents=True, exist_ok=True)
    (root / lift.LIFTS / "sys_a.py").write_text(TWO_DECLARATIONS, encoding="utf-8")
    generators = root / fixtures.GENERATORS
    generators.mkdir(parents=True, exist_ok=True)
    (generators / "sys_a.py").write_text(POSITIVE_GENERATOR, encoding="utf-8")
    fixtures.generate("sys-a", "x", root=root)
    return root


def real_inputs(root: Path) -> Path:
    """A directory holding the input `x` as the codec writes it, standing in for real data."""
    directory = root / "real_inputs"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "x.npz").write_bytes(value_codec.encode(np.array([1.0])))
    return directory


def measured_entry(root: Path, unit: str) -> str:
    """The rendered `[[entry]]` text of `unit`, measured on the real-inputs directory rather than on the fixture."""
    return reach.measure("sys-a", unit, None, real_inputs(root), root=root, **ENTRY_FIELDS)


def write_reach_file(root: Path, *chunks: str) -> None:
    """Write the given table chunks, in order, into the one reach file of `sys_a`."""
    path = root / reach.REACH / "sys_a.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(chunks), encoding="utf-8")


def named_table(unit: str, reason: str = "no real input carries the record its cell reads") -> str:
    """One `[[named]]` table for `unit`."""
    return f'[[named]]\nunit = "{unit}"\nreason = "{reason}"\n'


@state_check
def test_naming_an_absent_declaration_moves_it_from_the_unentered_count_into_the_named_count(tmp_path):
    """Property: a declaration that is neither entered nor named is counted in `without_entry`; naming it moves it into `named`, and the printed line carries both counts beside each other.

    Class: a root with two declared lifts and no entries, one of which then gains a `[[named]]` table
    with a non-empty reason — the input class a system's unmeasurable declaration takes.
    Why required: this is the arithmetic U-26's outcome 3 rests on. The printed pair is what the
    Definition of Done's reach line reads, and a count that folded the named set into `without_entry`
    would let `0 declared lift(s) with no entry` pass over a declaration nothing measured — the exact
    silent pass the mechanism exists to prevent.
    Killed by: dropping `- absent` from the `without_entry` expression, or leaving `ReachReport.named`
    at zero while the table parses; either makes the second arm read `(2, 0)`.
    A second correct implementation: computing the two counts as set differences over
    `set(by_key) - covered` and the named keys at the end, instead of accumulating `absent` in the loop.
    """
    root = make_root(tmp_path / "root")
    before = reach.reach_findings(root)
    assert (before.declared, before.without_entry, before.named, before.findings) == (2, 2, 0, [])

    write_reach_file(root, named_table(UNIT_FIRST))
    after = reach.reach_findings(root)
    assert (after.declared, after.without_entry, after.named, after.findings) == (2, 1, 1, [])

    line = reach.summary_line(after)
    assert "1 declared lift(s) with no entry" in line, line
    assert "1 named absence(s)" in line, line


@state_check
def test_a_named_table_that_contradicts_an_entry_or_matches_no_lift_is_a_finding(tmp_path):
    """Property: naming a declaration that has a reach entry, and naming a unit no declaration carries, are each reported as a finding; naming a real declaration that has no entry is not, and moves no count.

    Class: three reach files over the same two-declaration root — a name colliding with an entry's
    unit, a name for a unit no lift declares, and a name for the other, unentered declaration.
    Why required: U-26's outcome 3 states the named set is "a record, not an exemption". An exemption
    reading would subtract every name from `without_entry` whether or not it resolves, which turns a
    typo in a unit key into a silent pass; this test makes both refusals re-run on every run of the
    check rather than on the one round that pasted them.
    Killed by: deleting either arm of the named loop in `reach_findings` — the `key not in by_key`
    arm or the `key in covered` arm — which turns that arm's assertion into an empty finding list.
    A second correct implementation: resolving each named key through a dict of declared lifts and
    reporting `None` and `covered` cases from that lookup, instead of the two membership tests.
    """
    contradiction = make_root(tmp_path / "contradiction")
    write_reach_file(contradiction, measured_entry(contradiction, UNIT_FIRST), named_table(UNIT_FIRST))
    report = reach.reach_findings(contradiction)
    assert report.named == 0, (report.named, report.findings)
    assert report.without_entry == 1, (report.without_entry, report.findings)
    assert len(report.findings) == 1, report.findings
    assert UNIT_FIRST in report.findings[0], report.findings

    unmatched = make_root(tmp_path / "unmatched")
    write_reach_file(unmatched, measured_entry(unmatched, UNIT_FIRST), named_table("mod.py:77-80"))
    report = reach.reach_findings(unmatched)
    assert report.named == 0, (report.named, report.findings)
    assert report.without_entry == 1, (report.without_entry, report.findings)
    assert len(report.findings) == 1, report.findings
    assert "mod.py:77-80" in report.findings[0], report.findings

    satisfying = make_root(tmp_path / "satisfying")
    write_reach_file(satisfying, measured_entry(satisfying, UNIT_FIRST), named_table(UNIT_SECOND))
    report = reach.reach_findings(satisfying)
    assert (report.without_entry, report.named, report.findings) == (0, 1, [])


@state_check
def test_a_named_table_without_a_reason_or_recorded_twice_is_refused_by_name(tmp_path):
    """Property: `read_named` refuses a table that lacks `reason`, holds a blank one, or repeats a (unit, part) in one file, naming the file in the refusal.

    Class: three malformed reach files — a table with no `reason` key, one with `reason = "  "`, and
    two tables for the same unit and part.
    Why required: the reason IS the measurement that shows the absence, so a named table without one
    is a bare exemption claim. A refusal rather than a finding keeps the malformed state from being
    read as a pass over the declaration it names.
    Killed by: dropping the `_NAMED_FIELDS` completeness check, the empty-reason test, or the `seen`
    set; each turns its arm's `pytest.raises` into a silent read.
    A second correct implementation: parsing with a small schema table that lists each field's type
    and required-ness, and refusing the first failure it finds, which reports the same malformed
    inputs as refusals.
    """
    root = tmp_path / "root"
    root.mkdir()

    write_reach_file(root, '[[named]]\nunit = "mod.py:1-5"\n')
    with pytest.raises(reach.ReachError) as missing:
        reach.read_named(root)
    assert "sys_a.toml" in str(missing.value)

    write_reach_file(root, named_table(UNIT_FIRST, reason="   "))
    with pytest.raises(reach.ReachError):
        reach.read_named(root)

    write_reach_file(root, named_table(UNIT_FIRST) + named_table(UNIT_FIRST))
    with pytest.raises(reach.ReachError) as repeated:
        reach.read_named(root)
    assert UNIT_FIRST in str(repeated.value)
