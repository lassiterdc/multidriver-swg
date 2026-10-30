"""Equivalence tests of the legacy marginal-fitting helpers of `__ref_ams_functions.py` owned by marginal-distribution-fitting.

Each test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures), asks
the owning system's registered callable for the same output, and compares the two. No callable is registered yet, so
each test fails at its comparison against a placeholder lying outside its licence. The registered callable is called
as `callable(part, inputs)`; the lifts read the fixtures `series_positive` and `series_mixed`, so `inputs` holds them.

Eleven units ran, so eleven tests; the two rescaling helpers (lines 39-43 and 44-47) never ran and keep their
reminders, as the shape rules. Every unit is a function of its arguments and draws nothing (`draws="none"`), so each
value is stored. Floating-point data compares at a relative tolerance of one part in a billion, with text, flags and
names exact. A unit that calls another legacy function by name runs that function's own stored text, so the helpers
are compared as the fitting function uses them.

The fitting function (lines 111-214) holds three registered departures, all `LICENSED` and all changing a score that is
not a fit parameter: the AIC of a fit (lines 158 and 164: the new code sums log-densities instead of taking the log of
their product, which underflows) and the Kolmogorov-Smirnov p-value (line 199: the new code tests the data against the
fitted distribution). Its test drops the components that hold them, by name, for each accepted fit, and compares the
parameters, the Cramer-von Mises p-value, the two relative-difference scores, the support and the table of values.
The fixtures take the branch that rejects a fit (a fixed location the support does not match, and an upper bound under
a support unbounded below), and the legacy text's raise on a gamma fit with its location fixed at the minimum (the
data would sit on the open bound) is avoided: no fixture takes it.

`RULINGS` records the three register entries in the fitting function's own lines, under the register's key.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support

RULINGS = {"__ref_ams_functions.py:111-214": {158: "LICENSED", 164: "LICENSED", 199: "LICENSED"}}

DEFAULT = compare.Tolerance(rtol=1e-9, atol=0.0)
ACCEPTED_FITS = ["norm", "gamma", "weibull_fixed_loc", "lognorm_all", "gamma_bounded"]
DEPARTING_SCORES = [f"{case}_{score}" for case in ACCEPTED_FITS for score in ("aic", "ks_pval")]


@pytest.mark.equivalence(unit="__ref_ams_functions.py:23-30", system="marginal-distribution-fitting")
def test_the_box_cox_helper_returns_the_same_transform_shift_and_in_place_series(new_side, request):
    """Equivalence of the transformed values, power and shift for a positive and a mixed series, and of the series the helper shifts in place."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="__ref_ams_functions.py:31-38", system="marginal-distribution-fitting")
def test_the_inverse_box_cox_returns_the_same_values(new_side, request):
    """Equivalence of the inverse transform for a zero and a non-zero power."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="__ref_ams_functions.py:48-51", system="marginal-distribution-fitting")
def test_the_shift_returns_the_same_series(new_side, request):
    """Equivalence of the series shifted by a non-zero and a zero amount."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="__ref_ams_functions.py:52-55", system="marginal-distribution-fitting")
def test_the_unshift_returns_the_same_series(new_side, request):
    """Equivalence of the series unshifted by a non-zero and a zero amount."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="__ref_ams_functions.py:56-70", system="marginal-distribution-fitting")
def test_the_recorded_transformations_apply_the_same_way_to_new_values(new_side, request):
    """Equivalence of the values transformed under a fit with every transformation and under one with a shift only."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="__ref_ams_functions.py:71-88", system="marginal-distribution-fitting")
def test_the_pre_fit_transformation_returns_the_same_series_and_parameters(new_side, request):
    """Equivalence of the transformed series and the parameters needed to reverse the transformations, for three transformation sets."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="__ref_ams_functions.py:89-104", system="marginal-distribution-fitting")
def test_the_back_transformation_returns_the_same_values(new_side, request):
    """Equivalence of the values returned to the units of the event statistic under two fits."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="__ref_ams_functions.py:105-107", system="marginal-distribution-fitting")
def test_the_mean_squared_relative_difference_is_the_same(new_side, request):
    """Equivalence of the score for a perturbed series and for an identical one."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="__ref_ams_functions.py:108-110", system="marginal-distribution-fitting")
def test_the_mean_absolute_relative_difference_is_the_same(new_side, request):
    """Equivalence of the score for a perturbed series and for an identical one."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="__ref_ams_functions.py:111-214", system="marginal-distribution-fitting")
def test_the_distribution_fit_records_and_tables_are_the_same(new_side, request):
    """Equivalence of the fit record and the table of values for seven candidate fits, two of them rejected; the AIC and the Kolmogorov-Smirnov p-value of each accepted fit are dropped by name (lines 158, 164 and 199)."""
    licence = _support.Rescoped(DEFAULT, DEPARTING_SCORES)
    _support.check(request, new_side, licence)


@pytest.mark.equivalence(unit="__ref_ams_functions.py:325-387", system="marginal-distribution-fitting")
def test_the_candidate_distributions_are_the_same(new_side, request):
    """Equivalence of the name and parameter count of each of the thirteen candidate distributions, in order."""
    _support.check(request, new_side, DEFAULT)
