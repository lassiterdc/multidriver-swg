"""Equivalence tests of the legacy fitted-marginal helpers of `_utils.py` owned by marginal-distribution-fitting.

Each test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures), asks
the owning system's registered callable for the same output, and compares the two. No callable is registered yet, so
each test fails at its comparison against a placeholder lying outside its licence. The licence is a literal in each
test: floating-point data compares at a relative tolerance of one part in a billion, with everything else exact. The
registered callable is called as `callable(part, inputs)`; the fixtures the lifts read (`msel_*`) are in `inputs`.

Two units ran, so two tests, both `draws="none"` and stored. The function that pivots the long table of fitted
probabilities (lines 410-420) is compared in full, with its message when it drops observations. The function that plots
the chosen fits of an event type and writes their tables (lines 1198-1238) refits the named fits through `fit_dist`, whose
registered departures (`__ref_ams_functions.py:111-214`, lines 158, 164 and 199) change the AIC and the Kolmogorov-Smirnov
p-value of a fit; the test drops those two columns of each type's table of fits by name, and each figure's super title,
which states both. Its fixtures name only fits that exist and that `fit_dist` accepts, so none of the fixed-location gamma
or lognormal fits that raise is asked for (ruling U-20: those are constrained through the test of
`_b4_selecting_marginals.py:66-181`, which catches and records them, and this function does not).

`RULINGS` records the three register entries the second unit reaches through `fit_dist`, under the register's key.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support

RULINGS = {"__ref_ams_functions.py:111-214": {158: "LICENSED", 164: "LICENSED", 199: "LICENSED"}}

DEFAULT = compare.Tolerance(rtol=1e-9, atol=0.0)
FIT_SCORES = [f"fits_{key}__{score}" for key in ("surge_events", "rain_events") for score in ("aic", "ks_pval")]
FIGURE_TITLES = [f"fig{number:02d}__suptitle" for number in range(7)]


@pytest.mark.equivalence(unit="_utils.py:410-420", system="marginal-distribution-fitting")
def test_the_fitted_probability_pivot_returns_the_same_tables_and_message(new_side, request):
    """Equivalence of the table of fitted probabilities, one column per variable, with and without the message about dropped observations."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_utils.py:1198-1238", system="marginal-distribution-fitting")
def test_the_final_selection_function_writes_the_same_tables_and_figures(new_side, request):
    """Equivalence of the fit and empirical-versus-fitted tables, the seven figures and the warning for an unmodelled statistic; the AIC, the Kolmogorov-Smirnov p-value and the super titles are dropped by name."""
    licence = _support.Rescoped(DEFAULT, FIT_SCORES + FIGURE_TITLES)
    _support.check(request, new_side, licence)
