"""Equivalence tests of the legacy units of `_utils.py` owned by combined-event-identification.

Each test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures),
asks the owning system's registered callable for the same output, and compares the two. No callable is registered
yet, so each test fails at its comparison against a placeholder lying outside its licence. The licence is a literal
in each test: floating-point data compares at a relative tolerance of one part in a billion, with everything else
(labels, texts, integers, flags, dates, time spans) exact.

Each unit of this file is one legacy function (or two sibling chart functions), so its lift calls the function, as the
legacy cells do, on fixtures chosen to reach its branches, and each test compares what the calls return (or, for a
chart, the data it draws) packed as the named components of one dataset. The registered callable is called as
`callable(part, inputs)` and returns that dataset. Three units run a stored function that stops unconditionally at a
register stop marker (the rain-event selection at line 1274 and the event statistics at line 1563), and the combined
statistics unit calls the second; each such lift binds `sys` to a recorder that passes exactly those lines, so the
reference is the pre-stop run the register's `OBLIGED` rulings ask the preserved method to reproduce.

`RULINGS` records the register entries that lie inside each unit's own ranges and those a unit reaches through the
functions it calls: the combined-statistics unit (1786-1989) reaches 1563 and 1633 (and 1765, which its fixtures do
not execute), each under the register's own key, which the literal already holds for the units tested here.
"""

import pytest

from tests.correspondence import compare, fixtures
from tests.equivalence import _support

RULINGS = {
    "_utils.py:1269-1422": {1274: "OBLIGED", 1390: "OBLIGED"},
    "_utils.py:1423-1539": {1477: "OBLIGED"},
    "_utils.py:1540-1577": {1563: "OBLIGED"},
    "_utils.py:1613-1785": {1633: "OBLIGED", 1765: "OBLIGED"},
}


@pytest.mark.equivalence(unit="_utils.py:1269-1422", system="combined-event-identification")
def test_the_rain_event_selection_function_returns_the_same_events(new_side, request):
    """Equivalence of the rain-event selection function's events, series, threshold and arrival rate, for a threshold and for a target count."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))


@pytest.mark.equivalence(unit="_utils.py:1423-1539", system="combined-event-identification")
def test_the_surge_event_selection_function_returns_the_same_events(new_side, request):
    """Equivalence of the surge-event selection function's events, series, derived threshold and arrival rate, for a threshold and for a target count."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))


@pytest.mark.equivalence(unit="_utils.py:1540-1577", system="combined-event-identification")
def test_the_event_statistics_function_returns_the_same_tables(new_side, request):
    """Equivalence of the event-statistics function's tables for surge events, rain events and a single event."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))


@pytest.mark.equivalence(unit="_utils.py:1578-1595", system="combined-event-identification")
def test_the_annual_totals_chart_draws_the_same_data(new_side, request):
    """Equivalence of the data the annual event-count chart draws, saved with a title and left open without one."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))


@pytest.mark.equivalence(unit="_utils.py:1596-1612", system="combined-event-identification")
def test_the_surge_histogram_chart_draws_the_same_data(new_side, request):
    """Equivalence of the data the surge-histogram chart draws, saved to a file and shown."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))


@pytest.mark.equivalence(unit="_utils.py:1613-1785", system="combined-event-identification")
def test_the_pairing_function_returns_the_same_combined_events(new_side, request):
    """Equivalence of the combined events the pairing function returns for an ordinary and an overlapping scenario."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))


@pytest.mark.equivalence(unit="_utils.py:1786-1989", system="combined-event-identification")
def test_the_combined_statistics_function_returns_the_same_tables(new_side, request):
    """Equivalence of the combined-event statistics and series for the radar and the gauge rainfall."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))


@pytest.mark.equivalence(unit="_utils.py:2060-2071", system="combined-event-identification")
def test_the_classification_function_labels_events_the_same(new_side, request):
    """Equivalence of the compound, surge or rain label the classification function gives each event."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))


@pytest.mark.state_check
def test_no_fixture_of_the_classification_unit_holds_an_event_with_no_flag_set():
    """Property: every event row of the fixtures the classification lift passes carries at least one of the three flags.

    Class: each row of each summary table named by the classification lift's declaration. Why it is required: the
    legacy function assigns a type only under three flag arms, so a row with none set takes the previous row's type
    (and a first such row raises); the developer ruled that site unregistered and that the fixtures hold no such row
    (U-13, outcome 2), and the line-based reach record cannot see the difference, so this reads the data. Killed by:
    a fixture that adds a row on which `compound`, `surge_event` and `rain_event` are all false. Second correct
    implementation: counting the rows whose flags sum to zero with `numpy` instead of `any`. Not established: that no
    other fixture feeds the function, which the declaration's fixture list states.
    """
    declaration = _support.find_declaration("_utils.py:2060-2071")
    assert declaration.fixtures, "the classification lift names no fixtures, so there is nothing to check"
    for name in declaration.fixtures:
        table = fixtures.load("combined_event_identification", name)
        flagless = ~(
            table["compound"].astype(bool) | table["surge_event"].astype(bool) | table["rain_event"].astype(bool)
        )
        assert not flagless.any(), f"fixture {name} holds event rows with no flag set: {list(table.index[flagless])}"
