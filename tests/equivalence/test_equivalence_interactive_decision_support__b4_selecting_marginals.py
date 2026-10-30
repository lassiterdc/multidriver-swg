"""Equivalence tests of the legacy units of `_b4_selecting_marginals.py` owned by interactive-decision-support.

Each test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures), asks
the owning system's registered callable for the same output, and compares the two. No callable is registered yet, so
each test fails at its comparison against a placeholder lying outside its licence. The licence is a literal in each
test: floating-point data compares at a relative tolerance of one part in a billion, with everything else (labels,
texts, counts) exact.

Four units ran, so four tests; the unit of lines 250-267 never ran and keeps its reminder. The units are the plotting
cells of the legacy marginal selection: the first refits and draws every candidate fit the performance table admits, for
each event type (23 figures); the other three redraw the hand-picked fits of the surge-only (9 figures), rain-only (4)
and compound (10) events by the file names the fits generate, and check that every statistic is covered. Each reaches
the fitting function `fit_dist` through the functions it calls, with no fixed location asked of a gamma or lognormal
fit (the installed scipy refuses it; the fixtures avoid it), draws nothing at random and holds no stop marker.

The title of each figure opens with the fit's AIC and test p-values. The legacy AIC (lines 158 and 164 of
`__ref_ams_functions.py`) is made infinite by underflow where the new code sums log-densities, and the legacy
Cramer-von Mises test (line 199) tests the data against values derived from the data, where the new code tests them
against the fitted distribution; both are `LICENSED` departures. The lifts therefore set that opening line apart as a
component named with `scores` for each figure, and the licence drops those components by name; the rest of the title
(the support and the transformations), the paths and every patch, line and text of each figure are compared.

`RULINGS` records the register entries these units reach through the functions they call, each under the register's own
key.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support

RULINGS = {
    "__ref_ams_functions.py:111-214": {158: "LICENSED", 164: "LICENSED", 199: "LICENSED"},
}

DEFAULT = compare.Tolerance(rtol=1e-9, atol=0.0)


def _scores(count: int) -> list[str]:
    """The names of the score-line components of the first `count` figures."""
    return [f"fig{number:02d}__scores_line" for number in range(count)]


@pytest.mark.equivalence(unit="_b4_selecting_marginals.py:182-188", system="interactive-decision-support")
def test_the_plot_of_every_admitted_fit_draws_the_same_figures(new_side, request):
    """Equivalence of the 23 figures drawn for every admitted candidate fit; the opening score line of each title is dropped by name."""
    _support.check(request, new_side, _support.Rescoped(DEFAULT, _scores(23)))


@pytest.mark.equivalence(unit="_b4_selecting_marginals.py:189-208", system="interactive-decision-support")
def test_the_coarse_surge_fit_selection_redraws_the_same_figures(new_side, request):
    """Equivalence of the 9 figures redrawn for the hand-picked surge fits and of the statistics accounted for; the score lines are dropped by name."""
    _support.check(request, new_side, _support.Rescoped(DEFAULT, _scores(9)))


@pytest.mark.equivalence(unit="_b4_selecting_marginals.py:209-221", system="interactive-decision-support")
def test_the_coarse_rain_fit_selection_redraws_the_same_figures(new_side, request):
    """Equivalence of the 4 figures redrawn for the hand-picked rain fits and of the statistics accounted for; the score lines are dropped by name."""
    _support.check(request, new_side, _support.Rescoped(DEFAULT, _scores(4)))


@pytest.mark.equivalence(unit="_b4_selecting_marginals.py:222-249", system="interactive-decision-support")
def test_the_coarse_compound_fit_selection_redraws_the_same_figures(new_side, request):
    """Equivalence of the 10 figures redrawn for the hand-picked compound fits and of the statistics accounted for; the score lines are dropped by name."""
    _support.check(request, new_side, _support.Rescoped(DEFAULT, _scores(10)))
