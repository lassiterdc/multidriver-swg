"""Equivalence tests of the legacy units of `_utils.py` owned by interactive-decision-support.

Each test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures), asks
the owning system's registered callable for the same output, and compares the two. No callable is registered yet, so
each test fails at its comparison against a placeholder lying outside its licence. The licence is a literal in each
test: floating-point data compares at a relative tolerance of one part in a billion, with everything else (labels,
texts, counts) exact.

Seven units ran, so seven tests. Each unit is a function definition (or two), so its lift calls the function as the
legacy cells do, on invented inputs chosen to reach its branches, and each test compares what the calls return or the
data they draw. The units draw nothing at random and hold no register stop marker.

- lines 1065-1197 (the function that refits and draws the admitted fits) and 1239-1256 (the function that redraws
  named fits and reports the statistics they cover) reach the fitting function. Its AIC and test p-values are changed by
  the register's licensed sites (lines 158, 164 and 199 of `__ref_ams_functions.py`), so the lifts set apart the
  opening line of each figure's title (`scores_line`) and, for the fit the first function returns, its three scores
  (`fit_scores`), and the licences drop those components by name; everything else (the paths, the rest of the title,
  every patch, line and text, the fit's parameters and support, the fitted table) is compared. No fixed location is asked
  of a gamma or lognormal fit, which the installed scipy refuses.
- lines 2154-2197, 2198-2283, 2310-2322 and 2323-2328 compute plotting positions, list correlations, filter a listing
  and pick a row; the register's line 2200 (`OBLIGED`, differs from the manuscript) lies in the correlation listing.
- lines 2284-2309 plot chosen relationships in plotting-position and real space.

`RULINGS` records the register entries these units reach through the functions they call, each under the register's
own key.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support

RULINGS = {
    "__ref_ams_functions.py:111-214": {158: "LICENSED", 164: "LICENSED", 199: "LICENSED"},
    "_utils.py:2198-2283": {2200: "OBLIGED"},
}

DEFAULT = compare.Tolerance(rtol=1e-9, atol=0.0)
FITS_DRAWN = 23  # every admitted candidate fit of the three event types
COVERAGE_FIGURES = 8  # the fits redrawn by the three coverage calls
FIT_RETURNED_DROPPED = [f"fig{number:02d}__scores_line" for number in range(FITS_DRAWN)] + ["fit_scores"]
COVERAGE_DROPPED = [f"fig{number:02d}__scores_line" for number in range(COVERAGE_FIGURES)]


@pytest.mark.equivalence(unit="_utils.py:1065-1197", system="interactive-decision-support")
def test_the_admitted_fits_are_redrawn_and_the_named_fit_returned(new_side, request):
    """Equivalence of the 23 figures drawn for every admitted fit and of the fit and table returned for a named fit; the score components are dropped by name."""
    _support.check(request, new_side, _support.Rescoped(DEFAULT, FIT_RETURNED_DROPPED))


@pytest.mark.equivalence(unit="_utils.py:1239-1256", system="interactive-decision-support")
def test_the_coverage_of_hand_picked_fits_is_reported_the_same(new_side, request):
    """Equivalence of the statistics reported as covered for three lists of named fits and of the figures redrawn; the score lines are dropped by name."""
    _support.check(request, new_side, _support.Rescoped(DEFAULT, COVERAGE_DROPPED))


@pytest.mark.equivalence(unit="_utils.py:2154-2197", system="interactive-decision-support")
def test_the_plotting_positions_of_each_column_are_the_same(new_side, request):
    """Equivalence of the plotting positions of each column on its non-missing events, for a table with missing values and an all-missing column."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_utils.py:2198-2283", system="interactive-decision-support")
def test_the_quantile_correlation_listing_is_the_same(new_side, request):
    """Equivalence of the listing of surge-related and rain-related correlations for the surge-only, rain-only and compound events."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_utils.py:2284-2309", system="interactive-decision-support")
def test_the_relationship_plots_hold_the_same_data(new_side, request):
    """Equivalence of the data the relationship plots hold, left open with a figure title and saved with per-figure titles and file names."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_utils.py:2310-2322", system="interactive-decision-support")
def test_the_correlation_selections_are_the_same(new_side, request):
    """Equivalence of the correlation selections for a statistic and a kind of statistic, with and without a name filtered out."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_utils.py:2323-2328", system="interactive-decision-support")
def test_the_selected_variable_rows_appended_are_the_same(new_side, request):
    """Equivalence of the rows appended for two chosen variable pairs."""
    _support.check(request, new_side, DEFAULT)
