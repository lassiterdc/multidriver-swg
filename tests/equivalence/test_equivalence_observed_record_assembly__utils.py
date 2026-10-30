"""Equivalence tests of the legacy helper functions of `_utils.py` owned by observed-record-assembly.

Each test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures), asks
the owning system's registered callable for the same output, and compares the two. No callable is registered yet, so
each test fails at its comparison against a placeholder lying outside its licence. The licence is a literal in each
test: floating-point data compares at a relative tolerance of one part in a billion, with everything else (time stamps,
counts, labels, texts) exact. The registered callable is called as `callable(part, inputs)`; the fixtures the lifts read
(`ou_*`, `oea_*`) are in `inputs`.

Six units ran, so six value tests, one per unit, plus a draw-law test for the one unit that holds a random-draw routine.
Five values are stored (`draws="none"`): the p-value plot function (922-990, saved and shown), the airport-precipitation
processor (1990-2016, with a year excluded and without), the single-test comparator (2017-2025, under two tests and three
cases), the two-sample comparison function (2026-2059, charted and uncharted), and the priority combination function
(2075-2153, see below).

The function that compares two distributions (991-1055) resamples both samples with pandas `sample` and no seed (lines
1015 and 1016, `LICENSED` unseeded draws), so its lift declares `unseeded` and its value is not stored: the test compares
what the draw does not decide and drops, by name, the p-value panels' titles and bars. Its second test, `part="draw-law"`,
tests the new sampling routine against the law the legacy call specifies: two independent streams of iid uniform picks,
with replacement, one over each sample's rows, at 100,000 draws per stream with four tests at 0.001 each (B1 and B2 the
frequency of each stream, B3 the within-stream pairs pooled, B4 the independence of the two streams, whose defect class is
one draw shared by both); the seed written here is the one `derived_seed` gives for the unit, and the reference is the two
row counts the legacy call drew from, read by the lift's reduction as the call passed them.

The priority combination function (2075-2153) holds the register entry at line 2123 in its OWN ranges, and developer
ruling U-21 (outcome 2) gives it the same treatment as row 108: the lift runs it on a full-window replacement and on a
loses-last-steps partial overlap with an untouched later event, and splits each returned table into the overlapped radar
event's rows, keyed by its data source and its original span (its first to last stamp, and in the summary table
`event_start` equal to its first stamp), and the rest. In the rest, `event_id` is the event's rank by start time, the map
taken from the time series and applied to both tables. The test drops the four overlapped components by name and compares
everything else, so the registered callable must apply the same split key and the same renumbering to its own tables.

`RULINGS` records the register sites the units reach: 1015 and 1016 in the comparison function's own lines, and 2123 in
the combination function's.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support

RULINGS = {
    "_utils.py:857-921": {902: "OBLIGED"},
    "_utils.py:991-1055": {1015: "LICENSED", 1016: "LICENSED"},
    "_utils.py:2075-2153": {2123: "LICENSED"},
}

DEFAULT = compare.Tolerance(rtol=1e-9, atol=0.0)
# the bootstrap draws of the comparison function decide its p-value panels' titles and bars, and nothing else
DRAW_COMPONENTS = ["fig00__draw_titles", "fig00__draw_bars"]
# the overlapped radar event's rows of the two cases; the departure changes which of them exist, and nothing else
OVERLAPPED_ROWS = [
    f"{case}_{table}__overlapped__{column}"
    for case in ("full", "partial")
    for table, columns in (
        ("tseries", ("event_id", "date_time", "mm_per_hr", "rain_data_source")),
        ("summaries", ("event_id", "event_start", "rain_data_source", "depth_mm")),
    )
    for column in columns
]


@pytest.mark.equivalence(unit="_utils.py:922-990", system="observed-record-assembly")
def test_the_pvalue_plot_function_draws_the_same_figures(new_side, request):
    """Equivalence of the saved and the shown figure: their paths, titles, axis labels, x limits, bars and legend."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_utils.py:991-1055", system="observed-record-assembly")
def test_the_two_sample_comparison_saves_the_same_figures(new_side, request):
    """Equivalence of the paths, the folders asked for, the histogram figures and the samples' means and medians; the p-value panels' titles and bars are dropped by name."""
    licence = _support.Rescoped(DEFAULT, DRAW_COMPONENTS)
    _support.check(request, new_side, licence)


@pytest.mark.equivalence(unit="_utils.py:991-1055", system="observed-record-assembly", part="draw-law")
def test_the_resampling_follows_uniform_picks_over_the_rows(new_side, request):
    """Draw-law test: 100,000 draws of the new resampling routine are uniform picks with replacement over the first sample's rows."""
    _support.check(request, new_side, _support.two_stream_law(), seed=1607439588)


@pytest.mark.equivalence(unit="_utils.py:1990-2016", system="observed-record-assembly")
def test_the_airport_precipitation_processor_returns_the_same_series(new_side, request):
    """Equivalence of the rate and depth series the function returns, with a year excluded and without."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_utils.py:2017-2025", system="observed-record-assembly")
def test_the_single_test_comparator_reports_the_same_verdicts(new_side, request):
    """Equivalence of the p-value and verdict for two scipy tests, on differing samples, identical samples and a loose level."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_utils.py:2026-2059", system="observed-record-assembly")
def test_the_two_sample_chart_function_reports_the_same_verdicts_and_figure(new_side, request):
    """Equivalence of the two verdicts and of the saved figure's titles, captions and bars."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_utils.py:2075-2153", system="observed-record-assembly")
def test_the_priority_combination_function_leaves_the_same_series_and_summaries(new_side, request):
    """Equivalence of both cases' combined series and summaries, the rest of each table renumbered by start time; the overlapped radar event's rows are dropped by name."""
    licence = _support.Rescoped(DEFAULT, OVERLAPPED_ROWS)
    _support.check(request, new_side, licence)
