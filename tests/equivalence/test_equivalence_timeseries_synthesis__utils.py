"""Equivalence tests of the legacy units of `_utils.py` owned by timeseries-synthesis.

Each test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures), asks
the owning system's registered callable for the same output, and compares the two. No callable is registered yet, so
each test fails at its comparison against a placeholder lying outside its licence. The licence is a literal in each
test: floating-point data compares at a relative tolerance of one part in a billion, with everything else (labels,
texts, counts) exact.

Eight units ran, so eight value tests, and two of the units hold a draw routine and get a draw-law test each.

- lines 29-95, 96-103, 104-171, 213-227, 271-334 and 335-398 draw nothing; their tests compare every component.
  The rescaling loop of 104-171 is exercised on a target the observed series already meets and on one it cannot meet
  within the attempt cutoff, so both outcomes and the problem text are compared.
- lines 172-212 select each simulated event's nearest observed events by a rank-weighted sample. The sample is
  unseeded, so the value holds what the draw cannot change — the observed ids each row holds, sorted, the columns and
  the index — and a draw-law test checks the draw against the rank-weighted permutation law.
- lines 236-270 shift an observed tide at random and pick a year; the tide and water-level values and the shift are
  components named with `draw`, dropped by the value test, which compares the index, the names and the identity of the
  water level against the surge plus that tide. A draw-law test checks the (shift, year) draw against the tide law.
  The frame the buffering helper extends must be dated: the helper builds its new index with `pd.date_range`, so an
  elapsed-time index raises inside it.

`RULINGS` records the register entries inside these units and those they reach through the functions they call, each
under the register's own key.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support

RULINGS = {
    "_utils.py:104-171": {143: "OBLIGED"},
    "_utils.py:172-212": {210: "LICENSED"},
    "_utils.py:236-270": {238: "OBLIGED", 243: "LICENSED", 247: "LICENSED"},
    "_utils.py:1540-1577": {1563: "OBLIGED"},
}

DEFAULT = compare.Tolerance(rtol=1e-9, atol=0.0)
TIDE_DRAW_COMPONENTS = ["draw_shift", "draw_tide_values", "draw_wlevel_values"]


@pytest.mark.equivalence(unit="_utils.py:29-95", system="timeseries-synthesis")
def test_the_series_is_rescaled_to_the_target_depth(new_side, request):
    """Equivalence of the rescaled series' values, its step count and its step regularity."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_utils.py:96-103", system="timeseries-synthesis")
def test_the_largest_four_hour_mean_intensity_is_the_same(new_side, request):
    """Equivalence of the largest four-hour mean intensity the helper returns."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_utils.py:104-171", system="timeseries-synthesis")
def test_the_rescaling_loop_returns_the_same_series_and_problems(new_side, request):
    """Equivalence of the rescaled series, its length and regularity, and the problem text, for a met target and for a cutoff."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_utils.py:172-212", system="timeseries-synthesis")
def test_the_neighbour_rows_hold_the_same_observed_events(new_side, request):
    """Equivalence of the observed ids each row holds, sorted, and of the columns and index of the neighbour table."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_utils.py:172-212", system="timeseries-synthesis", part="draw-law")
def test_the_neighbour_draw_is_a_rank_weighted_permutation(new_side, request):
    """Draw-law test: 100,000 rank-weighted samples are permutations drawn in proportion to one over the rank."""
    _support.check(request, new_side, _support.plackett_luce_law(), seed=1156986945)


@pytest.mark.equivalence(unit="_utils.py:213-227", system="timeseries-synthesis")
def test_the_rescaling_attempt_reads_the_same_event_and_row(new_side, request):
    """Equivalence of the neighbour id, the observed event's dates and id, and the observed and simulated rows."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_utils.py:236-270", system="timeseries-synthesis")
def test_the_tide_shift_produces_the_same_series_shape(new_side, request):
    """Equivalence of the tide and water-level series' index, names and lengths and of the water level's identity against the surge; the drawn values are dropped by name."""
    _support.check(request, new_side, _support.Rescoped(DEFAULT, TIDE_DRAW_COMPONENTS))


@pytest.mark.equivalence(unit="_utils.py:236-270", system="timeseries-synthesis", part="draw-law")
def test_the_tide_draw_is_a_uniform_shift_and_a_uniform_year(new_side, request):
    """Draw-law test: 100,000 draws take a shift uniform on the window's lattice and a year uniform over the valid years."""
    _support.check(request, new_side, _support.tide_law(), seed=1604825480)


@pytest.mark.equivalence(unit="_utils.py:271-334", system="timeseries-synthesis")
def test_the_rain_and_surge_series_are_filled_and_extended_the_same(new_side, request):
    """Equivalence of the filled rainfall, the returned or filled surge, and the observed window, for a complete series and one with gaps."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_utils.py:335-398", system="timeseries-synthesis")
def test_the_buffered_index_is_extended_the_same(new_side, request):
    """Equivalence of the reindexed frames for a short surge series, a short rainfall plug and an already buffered frame."""
    _support.check(request, new_side, DEFAULT)
