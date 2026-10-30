"""Equivalence tests of the legacy units of `_b6_simulate_from_vincop.py` owned by generator-validation.

Each test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures),
asks the owning system's registered callable for the same output, and compares the two. No callable is registered
yet, so each test fails at its comparison against a placeholder lying outside its licence. The licence is a literal
in each test: floating-point data compares at a relative tolerance of one part in a billion, with everything else
(labels, texts, counts) exact.

Both units are cells that compare the simulated events with the observed ones and reach a draw only through a function
they call: the pandas bootstrap of `_utils.py:1015-1016` (lines 597-626) and of `_utils.py:733` (lines 746-770), each
a resample that reads numpy's global state and names no seed. Neither draws from the vine library, so no seed fixture
is read. Because a draw reached through a callee cannot be fixed by the lift, each lift declares `unseeded`, its value is
not stored, and the test compares only what does not depend on the draw: the lift packs the draw-dependent components
under names containing `draw`, and the licence drops them by name. Running each lift twice, the full values differ
from run to run, so the draws are real; every component that moved between the two runs is among the dropped ones.

- lines 597-626: the per-source simulated tables, the folders asked for, and for each of the twelve saved figures
  the path, the keyword arguments of the save, the axis counts, labels and limits; for the six full-sample
  histograms their bars, lines, texts and legend, and for the six bootstrap p-value figures the names of their
  panels. Dropped: the panel titles that state a rejection rate and the p-value histogram bars of those six figures.
- lines 746-770: the pair names and observed Kendall's tau of the last source tested, and for each of the four saved
  figures the path, the keyword arguments, the axis labels, the dashed observed-statistic lines and the titles of the
  observed and simulated panels. Dropped: the test's result, p-value and simulated tau bounds, and per figure the
  titles that state a test's result, the histogram bars and the percentile text.

`RULINGS` records the register entries these units reach through the functions they call, each under the register's
own key: the `OBLIGED` entries at `_utils.py:902` (lines 597-626) and at `_utils.py:598` and `:723` (lines 746-770),
and the `LICENSED` unseeded-draw entries at `_utils.py:1015`, `:1016` and `:733`, whose draws are exactly what the
dropped components depend on, and the one at `_utils.py:431`, which lines 746-770 reach by static call (the
evaluator names `simulate_from_copula`) and never execute, since the cell passes no copula. No unit of this file holds an entry in its own lines and none of the texts they run
holds a stop marker.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support

RULINGS = {
    "_utils.py:421-434": {431: "LICENSED"},
    "_utils.py:857-921": {902: "OBLIGED"},
    "_utils.py:991-1055": {1015: "LICENSED", 1016: "LICENSED"},
    "_utils.py:597-604": {598: "OBLIGED"},
    "_utils.py:718-746": {723: "OBLIGED", 733: "LICENSED"},
}

DEFAULT = compare.Tolerance(rtol=1e-9, atol=0.0)
# six `compare_2_distributions` calls save a bootstrap p-value figure and then a histogram, so figures 0, 2, ... 10 are the first kind
DISTRIBUTION_DRAW_COMPONENTS = [
    f"fig{index:02d}__{name}" for index in range(0, 12, 2) for name in ("draw_titles", "draw_bars")
]
PAIR_DRAW_COMPONENTS = [
    "result_draw_mc_result",
    "result_draw_mc_pvalue",
    "result_draw_lowerbound",
    "result_draw_upperbound",
] + [f"fig{index:02d}__{name}" for index in range(4) for name in ("draw_titles_tests", "draw_bars", "draw_texts")]


@pytest.mark.equivalence(unit="_b6_simulate_from_vincop.py:597-626", system="generator-validation")
def test_the_distribution_comparison_cell_saves_the_same_figures_and_tables(new_side, request):
    """Equivalence of the per-source simulated tables and the twelve saved figures; the bootstrap-dependent rates and bars are dropped by name."""
    licence = _support.Rescoped(DEFAULT, DISTRIBUTION_DRAW_COMPONENTS)
    _support.check(request, new_side, licence)


@pytest.mark.equivalence(unit="_b6_simulate_from_vincop.py:746-770", system="generator-validation")
def test_the_pairwise_rank_correlation_cell_reports_the_same_pairs_and_figures(new_side, request):
    """Equivalence of the pairs, observed Kendall's tau and the four saved figures; the bootstrap-dependent test results, bounds, bars and texts are dropped by name."""
    licence = _support.Rescoped(DEFAULT, PAIR_DRAW_COMPONENTS)
    _support.check(request, new_side, licence)
