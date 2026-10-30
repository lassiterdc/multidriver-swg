"""Equivalence tests of the legacy units of `_b2_event_selection.py` owned by combined-event-identification.

Each test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures),
asks the owning system's registered callable for the same output, and compares the two. No callable is registered
yet, so each test fails at its comparison against a placeholder lying outside its licence. The licence is a literal
in each test: floating-point data compares at a relative tolerance of one part in a billion, with everything else
(labels, texts, integers, time spans) exact.

The four units of this file each yield several tables or figures, so each test compares them packed as the named
components of one dataset: the final rain-event selection (lines 273-324), the final surge-event selection
(553-568), the report of the surge-event selection, whose two figures are compared as the data they draw (569-587),
and the pairing of surge and rain events into combined events (588-617). The first, second and fourth run legacy
functions that stop unconditionally at a register stop marker; each lift binds `sys` to a recorder that passes
exactly those stops, so the reference is the pre-stop run the register's `OBLIGED` rulings ask the new code to reproduce. The
registered callable is called as `callable(part, inputs)` and returns the dataset.

`RULINGS` records the register entries these units reach only through the functions they call, each under the
register's own key for it (none of the four holds an entry in its own lines): the rain-event selection (273-324)
reaches the stop markers at 1274 and 1390 and the event statistics' stop at 1563, the surge-event selection
(553-568) reaches 1477 and 1563, and the pairing (588-617) reaches 1563, 1633 and 1765. All are `OBLIGED`.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support

RULINGS = {
    "_utils.py:1269-1422": {1274: "OBLIGED", 1390: "OBLIGED"},
    "_utils.py:1423-1539": {1477: "OBLIGED"},
    "_utils.py:1540-1577": {1563: "OBLIGED"},
    "_utils.py:1613-1785": {1633: "OBLIGED", 1765: "OBLIGED"},
}


@pytest.mark.equivalence(unit="_b2_event_selection.py:569-587", system="combined-event-identification")
def test_the_surge_event_report_draws_the_same_data(new_side, request):
    """Equivalence of the data the surge-event report's two figures draw and of the rate and record length it computes."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))


@pytest.mark.equivalence(unit="_b2_event_selection.py:273-324", system="combined-event-identification")
def test_the_final_rain_event_selection_gives_the_same_events(new_side, request):
    """Equivalence of the final rain events, their series, thresholds and arrival rates for the three rainfall sources."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))


@pytest.mark.equivalence(unit="_b2_event_selection.py:553-568", system="combined-event-identification")
def test_the_final_surge_event_selection_gives_the_same_events(new_side, request):
    """Equivalence of the final surge events, their series, their statistics and the rates the cell computes."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))


@pytest.mark.equivalence(unit="_b2_event_selection.py:588-617", system="combined-event-identification")
def test_the_pairing_of_surge_and_rain_events_gives_the_same_combined_events(new_side, request):
    """Equivalence of the combined-event summaries and series for the three rainfall sources."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))
