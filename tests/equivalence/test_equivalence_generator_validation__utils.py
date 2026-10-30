"""Equivalence tests of the legacy units of `_utils.py` owned by generator-validation.

Each test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures),
asks the owning system's registered callable for the same output, and compares the two. No callable is registered
yet, so each test fails at its comparison against a placeholder lying outside its licence. The licence is a literal
in each test: floating-point data compares at a relative tolerance of one part in a billion, with everything else
(labels, texts, counts) exact.

Six units ran, so six value tests, and the Kendall-tau Monte Carlo test has a seventh, a draw-law test. Four units
compute numbers, tables or figures from their arguments alone (lines 597-604, 605-660, 661-717 and 857-921), and
their tests compare everything. The other two draw:

- lines 718-746 and 747-856 reach the vine library's `simulate` through `simulate_from_copula`, and the bootstrap
  through pandas `sample` with no seed (line 733). Each lift runs both. The lift's vine stand-in gives every
  `simulate` call whose legacy call names no seeds the next seed of one generator built from the committed fixture
  `seed` (`numpy.random.default_rng(seed)`, one `integers(1, 2**31 - 1)` per call, in the legacy call order), so the
  copula results and figures compare exactly; the registered callable builds the same generator from
  `inputs["seed"]` and passes a seed from it to each `simulate` call in the same order. The bootstrap results
  (result, p-value, simulated taus and the histogram panel that shows them) depend on the unseeded draw: the lift
  names them with `draw`, declares `unseeded`, and the licence drops them by name. The observed tau and the
  bootstrap table's own panels are compared.
- the draw-law test of lines 718-746 tests the new bootstrap routine's picks against the law the legacy call
  specifies (uniform picks with replacement over the rows of the simulated table), with the seed written below,
  which is the one `derived_seed` gives for the unit.

`RULINGS` records the register entries these units hold or reach through the functions they call, each under the
register's own key.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support

RULINGS = {
    "_utils.py:421-434": {431: "LICENSED"},
    "_utils.py:597-604": {598: "OBLIGED"},
    "_utils.py:718-746": {723: "OBLIGED", 733: "LICENSED"},
    "_utils.py:857-921": {902: "OBLIGED"},
}

DEFAULT = compare.Tolerance(rtol=1e-9, atol=0.0)
# the histogram panel of each of the three bootstrapped pair figures, and the bootstrap's own table columns
HISTOGRAM_PANEL = [
    "suptitle",
    "size",
    *(
        f"ax0__{name}"
        for name in (
            "title",
            "labels",
            "lims",
            "bars",
            "colours",
            "lines",
            "texts",
            "legend",
            "scatter",
            "n_collections",
        )
    ),
]
EVALUATION_DRAW_COMPONENTS = [
    "bootstrap__draw_mc_result",
    "bootstrap__draw_mc_pvalue",
    "bootstrap__draw_lowerbound",
    "bootstrap__draw_upperbound",
    *(f"bootstrap_fig{number}__draw__{name}" for number in range(3) for name in HISTOGRAM_PANEL),
]
TEST_DRAW_COMPONENTS = ["bootstrap__draw_result", "bootstrap__draw_pvalue", "bootstrap__draw_taus"]


@pytest.mark.equivalence(unit="_utils.py:597-604", system="generator-validation")
def test_the_quantile_of_the_nearest_value_is_its_rank(new_side, request):
    """Equivalence of the quantile of the sample member nearest a value, for a member, between members, beyond both ends and for tied members."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_utils.py:605-660", system="generator-validation")
def test_the_common_density_percentiles_are_the_same(new_side, request):
    """Equivalence of the common maximum density and the two difference percentiles over every pair, for two percentiles."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_utils.py:661-717", system="generator-validation")
def test_the_statistic_histogram_figures_hold_the_same_data(new_side, request):
    """Equivalence of the data six statistic histograms hold: bars, shaded regions, observed line, percentile text with its suffix, legend and titles, left open and saved."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_utils.py:718-746", system="generator-validation")
def test_the_kendall_tau_monte_carlo_test_gives_the_same_results(new_side, request):
    """Equivalence of the test's results from a fitted copula (seeded, exact) and the bootstrap's observed tau; the bootstrap's result, p-value and simulated taus are dropped by name."""
    licence = _support.Rescoped(DEFAULT, TEST_DRAW_COMPONENTS)
    _support.check(request, new_side, licence)


@pytest.mark.equivalence(unit="_utils.py:718-746", system="generator-validation", part="draw-law")
def test_the_bootstrap_draws_follow_uniform_picks_over_the_simulated_rows(new_side, request):
    """Draw-law test: 100,000 draws of the new bootstrap routine are uniform picks with replacement over the rows of the simulated table."""
    _support.check(request, new_side, _support.bootstrap_law(), seed=4179285349)


@pytest.mark.equivalence(unit="_utils.py:747-856", system="generator-validation")
def test_the_per_pair_evaluation_gives_the_same_results_and_figures(new_side, request):
    """Equivalence of the per-pair results table and saved figures from a fitted copula (seeded, exact) and of the bootstrap run's pairs, observed taus and fixed panels; the bootstrap's results, bounds and histogram panels are dropped by name."""
    licence = _support.Rescoped(DEFAULT, EVALUATION_DRAW_COMPONENTS)
    _support.check(request, new_side, licence)


@pytest.mark.equivalence(unit="_utils.py:857-921", system="generator-validation")
def test_the_two_sample_test_battery_gives_the_same_table(new_side, request):
    """Equivalence of the table of seven two-sample tests' p-values and conclusions, at a strict and at a loose level."""
    _support.check(request, new_side, DEFAULT)
