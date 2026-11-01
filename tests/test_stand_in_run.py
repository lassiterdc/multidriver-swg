"""A stand-in for one whole run: select sides, record them, and end the run with the report.

The side selector, the side record and the divergence report are three pieces that a run joins:
the run selects a side for every registered pair, records what it selected, and, when any pair
ran its legacy side, ends by writing the report. The pieces that make a real run, the
configuration that carries the selection, the provenance record that carries the side record and
the step that ends a run, do not exist yet. These tests stand in for them with constructed
values, so the join can be checked now: a selection built in the test, a registry swapped in for
the test, a ledger built through `well_formed_ledger`, and a three-line function that plays the
end-of-run step.

What this module does not show: that a real configuration delivers the selection, that a real
provenance record carries the side record, or that a real run calls the report. Those parts are
unmet until the systems that own them exist.

An autouse fixture swaps in an empty registry for each test and restores the real one after it,
so no test here registers anything in the real registry and none asserts what that registry
holds.
"""

from __future__ import annotations

import io

import pytest

from multidriver_swg import divergence_ledger, pairs, side_record
from multidriver_swg.dispatch import SideSelection
from multidriver_swg.divergence_ledger import Ledger
from multidriver_swg.divergence_report import DivergenceReport, build_report, render_report, write_report
from multidriver_swg.side_record import SideRecord
from tests.test_divergence_report import well_formed_ledger

pytestmark = pytest.mark.state_check

PRE = pairs.PRE_REFACTOR
CUR = pairs.CURRENT
PAIRS = ("alpha", "beta", "gamma")

DEPARTURE_STATEMENT = "The run did not reproduce the legacy code at these sites ({count}):"


@pytest.fixture(autouse=True)
def three_registered_pairs(monkeypatch):
    """Swap in a registry of three pairs with both sides each, discarded with the test."""
    monkeypatch.setattr(pairs, "_REGISTRY", {})
    _register(*PAIRS)


def _register(*identifiers: str) -> None:
    """Register `identifiers` as pairs with both sides, one stand-in function per side."""
    for pair in identifiers:
        for side in (PRE, CUR):

            def stand_in(x):
                """A stand-in whose identity is all a test reads."""
                return x

            pairs.pair_side(pair=pair, side=side)(stand_in)


def _ledger() -> Ledger:
    """A ledger of three pair entries, two divergent, and two departure entries, given in descending order.

    The departure entries are supplied with the later site first so that the report's ordering is
    a property of the report and not of the order the ledger was built in. The fourth-finding
    entry names a pair that is not registered, so it stays listed.
    """
    return well_formed_ledger(
        [
            {"id": "alpha", "diverges": True, "changed": "alpha uses a new rule", "why": "the old rule was wrong"},
            {"id": "beta", "diverges": False},
            {"id": "gamma", "diverges": True, "changed": "gamma drops a step", "why": "the step was redundant"},
        ],
        [
            {"unit": "legacy_b.py:1-9", "site": 8, "kind": "finding-4", "future_pair": "delta", "why": "b departs"},
            {"unit": "legacy_a.py:1-9", "site": 5, "kind": "licensed", "why": "a departs"},
        ],
    )


def end_of_run(record: SideRecord, stream: io.StringIO, ledger: Ledger) -> DivergenceReport | None:
    """Play the end-of-run step: write the report only when some pair ran its legacy side."""
    if record.ran_pre_refactor:
        return write_report(stream, ledger)
    return None


def test_a_run_wide_pre_refactor_run_records_every_pair_and_ends_with_the_report():
    """Property: a run-wide legacy selection records all three pairs on the legacy side and writes a report naming both divergences.

    Class: one run-wide pre-refactor selection over three registered pairs and a ledger with two
    divergent pairs and two departure entries; the satisfying inputs are the per-pair and
    all-current runs below. Why: this is the join the run exists for: what was selected, what was
    recorded, and what the reader is told at the end. Killed by: a record that lists fewer pairs,
    a report that omits a divergent pair or its reason, a departure section that is not listed, or
    one that is not ordered by unit and then site. Second correct implementation: a record built
    in a loop over the registry and a report whose departures are sorted after being rendered.
    """
    ledger = _ledger()
    record = side_record.side_record(SideSelection(default_side=PRE))
    assert record.as_mapping() == {"alpha": PRE, "beta": PRE, "gamma": PRE}
    stream = io.StringIO()
    report = end_of_run(record, stream, ledger)
    assert report == build_report(ledger)
    lines = stream.getvalue().splitlines()
    assert "Pairs covered: 3" in lines
    assert "Pairs registered: 3" in lines
    assert "Pair alpha diverges: alpha uses a new rule (reason: the old rule was wrong)" in lines
    assert "Pair gamma diverges: gamma drops a step (reason: the step was redundant)" in lines
    assert not any(line.startswith("Pair beta") for line in lines)
    assert "No pair diverges." not in lines
    assert DEPARTURE_STATEMENT.format(count=2) in lines
    sites = [line for line in lines if " line " in line and not line.startswith("The run")]
    assert sites == ["legacy_a.py:1-9 line 5: a departs", "legacy_b.py:1-9 line 8: b departs"]


def test_a_per_pair_run_records_that_pair_on_the_legacy_side_and_the_rest_on_the_current_side():
    """Property: selecting one pair records it as legacy and lists the other two as current, and the run still ends with a report.

    Class: a selection that overrides each pair in turn. Why: measuring one issue in one old
    method needs that method beside the current code of everything else, and the record must still
    name all three. Killed by: a record that lists only the switched pair. Second correct
    implementation: a record built from `selected_sides` after the selector sorts the pairs.
    """
    for switched in PAIRS:
        record = side_record.side_record(SideSelection(per_pair={switched: PRE}))
        assert record.as_mapping() == {pair: PRE if pair == switched else CUR for pair in PAIRS}
        assert record.ran_pre_refactor is True
        stream = io.StringIO()
        assert end_of_run(record, stream, _ledger()) is not None
        assert "Pairs covered: 3" in stream.getvalue().splitlines()


def test_an_all_current_run_records_every_pair_and_writes_no_report():
    """Property: a run that selects no legacy side lists every pair as current and writes nothing at its end.

    Class: the default selection, and a legacy default overridden back to current for every pair.
    Why: a run that used no legacy function has nothing to report, and its record must still
    separate it from a run with no pairs. Killed by: an end-of-run step that writes whenever
    there are entries, or a record that is empty when nothing was switched. Second correct
    implementation: a step that checks the recorded sides directly instead of `ran_pre_refactor`.
    """
    for selection in (SideSelection(), SideSelection(default_side=PRE, per_pair=dict.fromkeys(PAIRS, CUR))):
        record = side_record.side_record(selection)
        assert record.as_mapping() == dict.fromkeys(PAIRS, CUR)
        stream = io.StringIO()
        assert end_of_run(record, stream, _ledger()) is None
        assert stream.getvalue() == ""


@pytest.mark.parametrize(
    "selection",
    [
        SideSelection(),
        SideSelection(default_side=PRE),
        SideSelection(per_pair={"beta": PRE}),
    ],
    ids=["all-current", "run-wide", "one-override"],
)
def test_the_record_lists_as_many_pairs_as_the_complete_ledger_covers(selection):
    """Property: for a ledger covering every registered pair, the record has exactly the ledger's pairs, as many as the report covers.

    Class: a complete ledger against the all-current, run-wide and one-override selections; and,
    as the arm that gives the cross-check its meaning, the same record with its last pair removed,
    which must disagree. Why: this is the check that a record omitting a current-side pair is
    caught, since a run on the current side of a pair is exactly what a short record forgets.
    Killed by: a record that omits current-side pairs, or one that omits any pair. Second correct
    implementation: a record that stores a sorted tuple of items and compares by identifier.
    """
    ledger = _ledger()
    record = side_record.side_record(selection)
    report = build_report(ledger)
    assert len(record.sides) == report.pairs_covered
    assert set(record.sides) == {entry["id"] for entry in ledger.pairs}
    short = SideRecord(dict(list(record.as_mapping().items())[:-1]))
    assert len(short.sides) != report.pairs_covered
    assert set(short.sides) != {entry["id"] for entry in ledger.pairs}


def test_the_packaged_ledger_states_its_own_counts_and_lists_every_departure_in_unit_then_site_order():
    """Property: with the packaged ledger and no pair registered, the report counts and lists exactly what that ledger holds.

    Class: the ledger the package ships, read with a registry that holds no pair. Every figure is
    read from the ledger at run time, never written as a literal, so the test follows the ledger
    as it grows; the left-out count is computed from the same rule the report states. Why: the
    packaged ledger is the one real input available, and its departure entries are not held in
    unit and site order in the file, so a report that echoed file order would list them wrongly.
    Killed by: a report that ignores the ledger it is given, that drops or repeats a departure,
    or that lists them in file order. Second correct implementation: a report that sorts once
    when it reads the ledger instead of when it builds the departure section.
    """
    pairs._REGISTRY.clear()
    ledger = divergence_ledger.load()
    report = build_report()
    registered: set[str] = set()
    left_out = sum(1 for e in ledger.departures if e["kind"] == "finding-4" and e["future_pair"] in registered)
    assert report.pairs_covered == len(ledger.pairs)
    assert report.pairs_registered == len(registered)
    assert len(report.departures) == len(ledger.departures) - left_out
    lines = render_report(report).splitlines()
    assert f"Pairs covered: {len(ledger.pairs)}" in lines
    if not ledger.pairs:
        assert "No pair diverges." in lines
    expected = sorted((e["unit"], e["site"]) for e in ledger.departures)
    prefixes = [f"{unit} line {site}: " for unit, site in expected]
    positions = []
    for prefix in prefixes:
        matching = [index for index, line in enumerate(lines) if line.startswith(prefix)]
        assert len(matching) == 1, prefix
        positions.append(matching[0])
    assert positions == sorted(positions)
    assert DEPARTURE_STATEMENT.format(count=len(expected)) in lines


def test_a_ledger_short_of_the_registry_shows_in_the_report_and_disagrees_with_the_record():
    """Property: a ledger covering two of three registered pairs prints 2 covered and 3 registered, and the record's three pairs disagree with the covered count.

    Class: a ledger one pair short of the registry, under a run-wide legacy selection. Why: the
    two counts are printed side by side so a ledger that is short of the code is visible, and the
    record-against-covered cross-check is what reports it from the record's side. Killed by: a
    report that prints the registry count as the covered count, or one that omits the registered
    count. Second correct implementation: a report that prints the counts on one line.
    """
    short_ledger = well_formed_ledger([{"id": "alpha", "diverges": False}, {"id": "beta", "diverges": False}], [])
    record = side_record.side_record(SideSelection(default_side=PRE))
    stream = io.StringIO()
    report = end_of_run(record, stream, short_ledger)
    assert report is not None
    lines = stream.getvalue().splitlines()
    assert "Pairs covered: 2" in lines
    assert "Pairs registered: 3" in lines
    assert len(record.sides) != report.pairs_covered
