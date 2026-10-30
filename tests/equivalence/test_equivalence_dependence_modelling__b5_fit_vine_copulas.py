"""Equivalence tests of the legacy units of `_b5_fit_vine_copulas.py` owned by dependence-modelling.

Each test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures),
asks the owning system's registered callable for the same output, and compares the two. No callable is registered
yet, so each test fails at its comparison against a placeholder lying outside its licence. The licence is a literal
in each test.

Seven units ran, so seven tests; the eighth unit of the file (lines 38-57) never ran and keeps its reminder. Four
units compute a number or table directly (lines 58-71, 72-75, 120-127 and, with a test of its own, 76-96); three
draw figures or write tables (97-119, 128-240, 297-328), compared as the data they carry: the paths they save to,
their axis labels, the dashed lines marking an observed statistic, and for the sensitivity cell the table of tested
settings.

The vine library's `cdf` and `simulate` are seeded by the lift from one committed fixture, `seed`, which each
declaration lists and reads, so the test's `inputs` carry the same value as `inputs["seed"]` (no literal in a test or a
declaration, and no `seed=` to `check`, which refuses one for a licence that is not a draw law). A `cdf` call takes the
seed itself. A `simulate` call whose legacy call names no seeds takes a seed drawn from one generator built from it,
`numpy.random.default_rng(seed)`, one `integers(1, 2**31 - 1)` per call, in the order the legacy text calls (U-16), so
the repeated simulations of a Monte Carlo test are independent and repeat exactly; the registered callable builds the
same generator from `inputs["seed"]` and passes a seed from it to each `simulate` call in the same order. Every test
therefore compares the outputs that depend on those calls exactly, under the default licence of one part in a billion,
and each licence below drops only what a ruled departure decides:

- lines 76-96: the result (`mc_result`), which the licensed one-sided rejection at line 78 changes; the p-value and the
  simulated distances are compared;
- lines 128-240: the column `result`, for the same reason; the p-values are compared;
- lines 97-119 and 297-328: the titles that state a test's result and rate (`titles_tests` of each figure), which the
  licensed stop markers at lines 48 and 78 change; the titles of the observed panels and of the simulated panels are
  compared, with the paths, labels and observed lines.

All five library-reaching units execute no draw but the vine library's on their committed fixtures (the pandas
bootstrap of `_utils.py:733` sits in a branch their call path does not take, because each passes a copula), so lines
76-96, 97-119, 120-127, 128-240 and 297-328 declare `seeded` and their values are stored, as are those of lines 58-71
and 72-75, which draw nothing.

`RULINGS` records the register entries these units reach through the functions they call, each under the register's
own key; no unit of this file holds an entry in its own lines except lines 76-96 (a `LICENSED` stop marker at 78).
The stop markers at 48 and 78 are passed by the lifts' `sys` recorders, as the run behind the kept outputs passed them.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support

RULINGS = {
    "_b5_fit_vine_copulas.py:38-57": {48: "LICENSED"},
    "_b5_fit_vine_copulas.py:76-96": {78: "LICENSED"},
    "_utils.py:421-434": {431: "LICENSED"},
    "_utils.py:597-604": {598: "OBLIGED"},
    "_utils.py:718-746": {723: "OBLIGED", 733: "LICENSED"},
    "_utils.py:857-921": {902: "OBLIGED"},
}

DEFAULT = compare.Tolerance(rtol=1e-9, atol=0.0)
FIGURE_TEST_TITLES = [f"fig{index:02d}_titles_tests" for index in range(7)]


@pytest.mark.equivalence(unit="_b5_fit_vine_copulas.py:58-71", system="dependence-modelling")
def test_the_empirical_joint_probability_of_each_observation(new_side, request):
    """Equivalence of the share of observations at or below each observation in every variable, over the count plus one."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_b5_fit_vine_copulas.py:72-75", system="dependence-modelling")
def test_the_cramer_von_mises_distance_is_the_sum_of_squared_differences(new_side, request):
    """Equivalence of the distance between two series of probabilities."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_b5_fit_vine_copulas.py:76-96", system="dependence-modelling")
def test_the_monte_carlo_distance_test_reports_the_same_observed_distance(new_side, request):
    """Equivalence of the observed distance, the p-value and the simulated distances; the result is dropped by name, for the licensed one-sided rejection (line 78)."""
    licence = _support.Rescoped(DEFAULT, ["mc_result"])
    _support.check(request, new_side, licence)


@pytest.mark.equivalence(unit="_b5_fit_vine_copulas.py:97-119", system="dependence-modelling")
def test_the_goodness_of_fit_figures_are_saved_with_the_same_observed_statistics(new_side, request):
    """Equivalence of the seven saved figures: their paths, axis labels, dashed observed-statistic lines and observed and simulated panel titles; the test-result titles dropped."""
    licence = _support.Rescoped(DEFAULT, FIGURE_TEST_TITLES)
    _support.check(request, new_side, licence)


@pytest.mark.equivalence(unit="_b5_fit_vine_copulas.py:120-127", system="dependence-modelling")
def test_the_observed_distance_of_a_fitted_copula(new_side, request):
    """Equivalence of the distance between a fitted copula's probabilities at the observations and their empirical ones."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_b5_fit_vine_copulas.py:128-240", system="dependence-modelling")
def test_the_fit_sensitivity_table_and_figures_for_each_setting(new_side, request):
    """Equivalence of the 48 tested settings' observed distances, truncation levels, p-values and saved figures; the result dropped, for the licensed one-sided rejection (line 78)."""
    licence = _support.Rescoped(DEFAULT, ["result"])
    _support.check(request, new_side, licence)


@pytest.mark.equivalence(unit="_b5_fit_vine_copulas.py:297-328", system="dependence-modelling")
def test_the_final_copula_is_saved_and_its_figures_drawn(new_side, request):
    """Equivalence of the saved copy of the chosen fit and the seven figures drawn for it; the test-result titles dropped."""
    licence = _support.Rescoped(DEFAULT, FIGURE_TEST_TITLES)
    _support.check(request, new_side, licence)
