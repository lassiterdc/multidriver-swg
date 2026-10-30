"""Equivalence test of the legacy storm-rescaling cell of `_b7_generate_time_series_from_event_stats.py` owned by timeseries-synthesis.

The test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures), asks the
owning system's registered callable for the same output, and compares the two. No callable is registered yet, so the
test fails at its comparison against a placeholder lying outside its licence. The licence is a literal: floating-point
data compares at a relative tolerance of one part in a billion, with everything else (labels, texts, counts) exact.

Both cells of the file are tested here. The lift of lines 381-890 runs unchanged on committed fixtures; the tide it adds
is drawn from numpy's global state, so that cell is `unseeded` and its test compares the files written, the final
series' surge and rainfall values, and every summary column except the two the tide sets. The lift of lines 88-380
runs an adapted copy of the cell whose one changed line empties `lst_skip`, the list the legacy workflow used to skip
the event types whose outputs were already written to disk; the body therefore runs. Its neighbour selection draws
with pandas `sample` and no seed, so the value holds only what the draw cannot change: the skip list, the event type's
modelled statistics and rain-intensity statistic, the observed event ids each source filter keeps, the renamed target
columns, the neighbour column names, the files the cell wrote, the summary file's columns, the count of simulated
events and the count of summary rows whose rainfall is zero (the zero-rainfall branch at line 279). The two register
sites run: the stop marker at line 155 is passed by the recorder and the branch at line 279 is taken.

`RULINGS` records the register entries inside the cell and those it reaches through the functions it calls, each
under the register's own key.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support

RULINGS = {
    "_b7_generate_time_series_from_event_stats.py:88-380": {155: "OBLIGED", 279: "OBLIGED"},
    "_b7_generate_time_series_from_event_stats.py:381-890": {628: "OBLIGED", 763: "OBLIGED", 771: "LICENSED"},
    "_utils.py:236-270": {238: "OBLIGED", 243: "LICENSED", 247: "LICENSED"},
    "_utils.py:104-171": {143: "OBLIGED"},
    "_utils.py:172-212": {210: "LICENSED"},
    "_utils.py:1540-1577": {1563: "OBLIGED"},
}


@pytest.mark.equivalence(unit="_b7_generate_time_series_from_event_stats.py:88-380", system="timeseries-synthesis")
def test_the_storm_rescaling_cell_reads_the_modelled_statistics_of_each_type(new_side, request):
    """Equivalence of the skip list and of the last event type's modelled statistics and rain-intensity statistic."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))


@pytest.mark.equivalence(unit="_b7_generate_time_series_from_event_stats.py:381-890", system="timeseries-synthesis")
def test_the_finalised_time_series_cell_writes_the_same_series_and_summary(new_side, request):
    """Equivalence of the files the cell writes, the final series' surge and rainfall values, and the summary's draw-independent columns."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))
