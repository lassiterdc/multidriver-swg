"""Equivalence tests of the legacy event-analysis cells of `_b3_event_analysis.py` owned by observed-record-assembly.

Each test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures), asks
the owning system's registered callable for the same output, and compares the two. No callable is registered yet, so
each test fails at its comparison against a placeholder lying outside its licence. The licence is a literal in each
test: floating-point data compares at a relative tolerance of one part in a billion, with everything else (time stamps,
counts, labels, texts) exact. The registered callable is called as `callable(part, inputs)`; the fixtures the lifts read
(`ora_*`, `oea_*`) are in `inputs`.

Four units ran, so four tests, one per unit, all `draws="none"` and stored. The radar-rainfall loading cell (lines
42-51) and the chart of that series (52-61, compared as the data the figure holds) are compared in full. The cell that
computes and compares the sources' event statistics (228-261 with 270-296, a unit of two ranges) calls
`compute_event_timeseries_statistics`, whose unconditional stop (`_utils.py:1563`, `OBLIGED`) the lift passes as the run
behind the kept outputs did, so the registered callable is the owning system's preserved method there; the test compares
the three statistics tables, the 14 saved figures and the lines printed.

The cell that combines two sources' event series by priority (135-220) calls `combine_dataset_according_to_priority`,
whose one registered entry (`_utils.py:2123`, `LICENSED`) lets the new code remove a preferred event that an added
lower-priority event only partly overlaps, where the legacy code keeps its remaining steps and its summary row. Under
developer ruling U-21 (outcome 2) the lift runs the function on two inputs, a full-window replacement and a
loses-last-steps partial overlap with an untouched later event, and splits each returned table into two named
components: the overlapped radar event's rows, keyed by its data source and its original span (its first to last stamp,
and in the summary table `event_start` equal to its first stamp), and the rest. In the rest, `event_id` is the event's
rank by start time, the map taken from the time series and applied to both tables, because the departure renumbers the
events that follow it and changes nothing else about their ids. This test drops the four overlapped components by name
(`full_tseries__overlapped`, `full_summaries__overlapped`, `partial_tseries__overlapped`,
`partial_summaries__overlapped`) and compares everything else, so the registered callable must apply the same split key
and the same renumbering to its own tables: a new side that disturbs any other event's rows, summaries or ids fails,
including one whose summary rows no longer carry their time series' ids.

`RULINGS` records the register entries the units reach through the functions they call: line 2123 (lines 135-220) and
line 1563 (lines 228-261 with 270-296), each under the register's key.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support

RULINGS = {
    "_utils.py:2075-2153": {2123: "LICENSED"},
    "_utils.py:1540-1577": {1563: "OBLIGED"},
}

DEFAULT = compare.Tolerance(rtol=1e-9, atol=0.0)
# the two cases' overlapped radar event rows: the departure changes which of them exist, and nothing else in the tables
OVERLAPPED_ROWS = [
    f"{case}_{table}__overlapped__{column}"
    for case in ("full", "partial")
    for table, columns in (
        ("tseries", ("event_id", "date_time", "mm_per_hr", "rain_data_source")),
        ("summaries", ("event_id", "event_start", "rain_data_source", "depth_mm")),
    )
    for column in columns
]


@pytest.mark.equivalence(unit="_b3_event_analysis.py:42-51", system="observed-record-assembly")
def test_the_radar_rainfall_loading_cell_builds_the_same_series_and_time_step(new_side, request):
    """Equivalence of the bias-corrected, rate and depth series with their time stamps, and of the time step."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_b3_event_analysis.py:52-61", system="observed-record-assembly")
def test_the_radar_rainfall_chart_draws_the_same_data(new_side, request):
    """Equivalence of the size, labels, tick labels and line of the chart of the depth series from 2015 on."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_b3_event_analysis.py:135-220", system="observed-record-assembly")
def test_the_priority_combination_cell_leaves_the_same_series_and_summaries(new_side, request):
    """Equivalence of both cases' combined event series and summaries, the rest of each table renumbered by start time, and of the series on the time axis; the overlapped radar event's rows are dropped by name."""
    licence = _support.Rescoped(DEFAULT, OVERLAPPED_ROWS)
    _support.check(request, new_side, licence)


@pytest.mark.equivalence(unit="_b3_event_analysis.py:228-261,270-296", system="observed-record-assembly")
def test_the_event_statistics_cells_leave_the_same_tables_figures_and_messages(new_side, request):
    """Equivalence of the three statistics tables, the 14 figures saved and the lines printed."""
    _support.check(request, new_side, DEFAULT)
