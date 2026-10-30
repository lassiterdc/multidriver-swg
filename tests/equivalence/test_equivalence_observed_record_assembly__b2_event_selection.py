"""Equivalence tests of the legacy event-selection cells of `_b2_event_selection.py` owned by observed-record-assembly.

Each test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures), asks
the owning system's registered callable for the same output, and compares the two. No callable is registered yet, so
each test fails at its comparison against a placeholder lying outside its licence. The licence is a literal in each
test: floating-point data compares at a relative tolerance of one part in a billion, with everything else (time stamps,
counts, labels, texts) exact. The registered callable is called as `callable(part, inputs)`; the fixtures the lifts
read (`ora_*`) are in `inputs`.

Five units ran, so five tests, one per unit. Four draw nothing and compare in full (`draws="none"`, each value
stored): the loading cell (lines 25-87) with the series and tables it builds, the function that finds the dates that
hold data (88-94), the cells that count the selected events per year and test the count differences between sources
(386-392 with 406-411, a unit of two ranges), and the function that tests whether a mean is zero (393-405).

The plotting cell (348-385) charts three sources' events per year and compares the sources' event statistics four times
through `compare_2_distributions`, which resamples with pandas `sample` and no seed (`_utils.py:1015` and `:1016`,
`LICENSED` unseeded draws), so its lift declares `unseeded` and its value is not stored. The test compares everything
that does not depend on those draws: the folders asked for, and for every saved figure its path, the keyword arguments
of the save, the labels, the captions and, for the charts of events per year and the histograms, the bars, lines and
texts; for each p-value figure the names of its tests. Dropped by name: the panel titles that state a rejection rate and
the histogram bars of those four figures (measured: running the lift twice, the only components that moved are these
eight). The cell also reaches the `OBLIGED` entry at `_utils.py:902` through the two-sample tests it calls, recorded in
`RULINGS`; the unit's registered callable is the owning system's preserved method there.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support

RULINGS = {
    "_utils.py:857-921": {902: "OBLIGED"},
    "_utils.py:991-1055": {1015: "LICENSED", 1016: "LICENSED"},
}

DEFAULT = compare.Tolerance(rtol=1e-9, atol=0.0)
# the four `compare_2_distributions` calls save a p-value figure and then a histogram, after the three charts of events per year
BOOTSTRAP_DRAW_COMPONENTS = [
    f"fig{index:02d}__{name}" for index in (3, 5, 7, 9) for name in ("draw_titles", "draw_bars")
]


@pytest.mark.equivalence(unit="_b2_event_selection.py:25-87", system="observed-record-assembly")
def test_the_loading_cell_builds_the_same_rainfall_and_water_level_series(new_side, request):
    """Equivalence of the rainfall series and water-level tables the cell builds from the five input files, and of the time steps it computes."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_b2_event_selection.py:88-94", system="observed-record-assembly")
def test_the_dates_with_data_are_flagged_the_same_way(new_side, request):
    """Equivalence of the calendar table that flags each date between the first and last as holding data or not."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_b2_event_selection.py:348-385", system="observed-record-assembly")
def test_the_event_source_comparison_cell_saves_the_same_figures(new_side, request):
    """Equivalence of the eleven saved figures; the bootstrap-dependent rates and bars of the four p-value figures are dropped by name."""
    licence = _support.Rescoped(DEFAULT, BOOTSTRAP_DRAW_COMPONENTS)
    _support.check(request, new_side, licence)


@pytest.mark.equivalence(unit="_b2_event_selection.py:386-392,406-411", system="observed-record-assembly")
def test_the_annual_event_counts_and_their_differences_are_the_same(new_side, request):
    """Equivalence of the yearly event counts of each source, the count differences between sources, and what the mean test printed."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_b2_event_selection.py:393-405", system="observed-record-assembly")
def test_the_mean_test_reports_the_same_verdicts(new_side, request):
    """Equivalence of the verdict of the test whether a mean is zero, for a clear mean, no mean, and a looser level."""
    _support.check(request, new_side, DEFAULT)
