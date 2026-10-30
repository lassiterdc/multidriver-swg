"""Equivalence tests of the legacy units of `_b7_generate_time_series_from_event_stats.py` owned by boundary-condition-output.

Each test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures),
asks the owning system's registered callable for the same output, and compares the two. No callable is registered
yet, so each test fails at its comparison against a placeholder lying outside its licence. The licence is a literal
in each test: floating-point data compares at a relative tolerance of one part in a billion, with everything else
(labels, index levels, names, integers) exact.

The units each yield two outputs that cannot share one table, so each takes one test per output, `part=` naming the
legacy variable: the first unit gathers the read-back statistics tables and series, the second writes the combined
table and series (its writes are kept in memory in its adapted lift, so the compared data is what would be written).
The registered callable is called as `callable(part, inputs)` and returns the output the part names.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support


@pytest.mark.equivalence(
    unit="_b7_generate_time_series_from_event_stats.py:891-914",
    system="boundary-condition-output",
    part="lst_summaries",
)
def test_read_back_summaries_are_gathered_into_one_table(new_side, request):
    """Equivalence of the read-back cell's gathered statistics tables, joined into one frame."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))


@pytest.mark.equivalence(
    unit="_b7_generate_time_series_from_event_stats.py:891-914",
    system="boundary-condition-output",
    part="lst_ds_tseries",
)
def test_read_back_series_are_gathered_along_the_event_type(new_side, request):
    """Equivalence of the read-back cell's gathered series, joined along the event type."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))


@pytest.mark.equivalence(
    unit="_b7_generate_time_series_from_event_stats.py:915-969",
    system="boundary-condition-output",
    part="written_summaries",
)
def test_the_combined_table_written_in_metres(new_side, request):
    """Equivalence of the combined statistics table the join cell writes, surge and tide values converted to metres."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))


@pytest.mark.equivalence(
    unit="_b7_generate_time_series_from_event_stats.py:915-969",
    system="boundary-condition-output",
    part="written_time_series",
)
def test_the_combined_series_written_in_metres(new_side, request):
    """Equivalence of the combined series the join cell writes, water level, surge and tide converted to metres."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))
