"""Equivalence test of the legacy unit of `_b3_event_analysis.py` owned by event-occurrence-modelling.

The test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures), asks the
owning system's registered callable for the same output, and compares the two. No callable is registered yet, so the
test fails at its comparison against a placeholder lying outside its licence. The licence is a literal: floating-point
data compares at a relative tolerance of one part in a billion, with everything else (labels, texts, counts) exact.

The unit counts the observed compound, surge-only and rain-only events for each rainfall source and saves the counts as
pie charts, so its test compares the data the three charts draw (wedge angles, labels, captions, the save's keyword
arguments), the source column the cell adds to the gauge table, and the counts the cell leaves, packed as the named
components of one dataset. The registered callable is called as `callable(part, inputs)` and returns the dataset. The
unit has no callee and reaches no register stop marker, and no register entry lies in its ranges, so no ruling is
recorded.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support


@pytest.mark.equivalence(unit="_b3_event_analysis.py:98-134", system="event-occurrence-modelling")
def test_the_event_type_pie_charts_draw_the_same_data(new_side, request):
    """Equivalence of the data the three event-type pie charts draw and of the counts the cell leaves."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))
