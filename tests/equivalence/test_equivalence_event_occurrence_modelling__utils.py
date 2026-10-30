"""Equivalence test of the legacy unit of `_utils.py` owned by event-occurrence-modelling.

The test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures), asks the
owning system's registered callable for the same output, and compares the two. No callable is registered yet, so the
test fails at its comparison against a placeholder lying outside its licence. The licence is a literal: floating-point
data compares at a relative tolerance of one part in a billion, with everything else (labels, texts, counts) exact.

The unit charts the simulated against the observed annual event counts as side-by-side density histograms, for all
events and for rain-only, surge-only and compound events, with an optional sample-size note and caption, and either
saves the figure or shows it. Its test compares the data the charts draw (bar edges, widths and heights, bar colours,
axis limits, texts with their positions, titles, labels and legend entries, the figure's size, and the save's keyword
arguments), for three calls of the function: saved plain, saved with note and caption, and shown. The registered
callable is called as `callable(part, inputs)` and returns the dataset. The unit has no callee and reaches no register
stop marker, and no register entry lies in its range, so no ruling is recorded.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support


@pytest.mark.equivalence(unit="_utils.py:467-573", system="event-occurrence-modelling")
def test_the_occurrence_histograms_draw_the_same_data(new_side, request):
    """Equivalence of the data the simulated-against-observed occurrence histograms draw in all three output modes."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))
