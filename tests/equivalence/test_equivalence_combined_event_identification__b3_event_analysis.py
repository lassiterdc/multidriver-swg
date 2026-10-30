"""Equivalence tests of the legacy units of `_b3_event_analysis.py` owned by combined-event-identification.

Each test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures),
asks the owning system's registered callable for the same output, and compares the two. No callable is registered
yet, so each test fails at its comparison against a placeholder lying outside its licence. The licence is a literal
in each test: floating-point data compares at a relative tolerance of one part in a billion, with everything else
(labels, texts, integers, flags, dates) exact.

Both units yield several tables or a chart, so each test compares them packed as the named components of one
dataset: the six combined-event tables the analysis reads back and labels (lines 24-41), and the data of the bar
chart of the radar-paired events' rain depths from 2015 on (lines 62-97). The registered callable is called as
`callable(part, inputs)` and returns the dataset. Neither unit, nor the one legacy function the first calls, reaches
a register stop marker, and no register entry lies in either unit's ranges, so no ruling is recorded.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support


@pytest.mark.equivalence(unit="_b3_event_analysis.py:24-41", system="combined-event-identification")
def test_the_combined_event_tables_are_read_back_and_labelled_the_same(new_side, request):
    """Equivalence of the six combined-event tables the analysis reads back, the summaries labelled compound, surge or rain."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))


@pytest.mark.equivalence(unit="_b3_event_analysis.py:62-97", system="combined-event-identification")
def test_the_event_rain_depth_chart_draws_the_same_data(new_side, request):
    """Equivalence of the data the chart of radar-paired events' rain depths draws, and of its labels and figure size."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))
