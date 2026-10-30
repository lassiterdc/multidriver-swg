"""Equivalence test of the legacy unit of `_b2_event_selection.py` owned by event-occurrence-modelling.

The test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures), asks the
owning system's registered callable for the same output, and compares the two. No callable is registered yet, so the
test fails at its comparison against a placeholder lying outside its licence. The licence is a literal: floating-point
data compares at a relative tolerance of one part in a billion, with everything else (dates, counts, flags) exact.

The unit is the function that counts the events starting in each rolling 365-day window of a record, keeping the windows
fully covered. Its test compares the table the function returns for the rain starts and for the surge starts, packed as
the named components of one dataset. The registered callable is called as `callable(part, inputs)` and returns the
dataset. The unit has no callee and reaches no register stop marker, and no register entry lies in its ranges or in a
function it reaches, so no ruling is recorded.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support


@pytest.mark.equivalence(unit="_b2_event_selection.py:95-108", system="event-occurrence-modelling")
def test_the_occurrence_count_function_keeps_the_same_windows_and_counts(new_side, request):
    """Equivalence of the rolling-window event and valid-day counts for the rain starts and for the surge starts."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))
