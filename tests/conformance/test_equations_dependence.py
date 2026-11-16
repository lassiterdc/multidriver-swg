r"""The five dependence equations of the SWG supplement, each checked against the code that runs it.

Every function here is one equation of the supplementary manuscript's dependence section, named by the name the
equation register proposes for it and carrying that equation's id in its conformance marker. The observed side is
the legacy unit that executes the published mathematics today, reached as its stored extraction through the lift
harness and run by `equation_check.implementation` (nothing under `src/` implements these equations yet). The
reference side is the manuscript's formula, computed in the test's own body over literal inputs. One `check` call
compares them, so a disagreement arrives as a `ConformanceDivergence` carrying the ledger's binding for that row.

Four of the five diverge, and each divergence is the register's own record rather than a surprise: E-11 keeps
a different pair set and ranks each column over its own sample, E-13's empirical CDF divides by `n + 1` where
the manuscript's `\hat{F}` divides by `n`, E-15's `scipy.stats.kendalltau` is tau-b where the manuscript writes
tau-a, and E-16 reads the null distribution at the nearest simulated value instead of at `t_{obs}`. E-30 is
green: the bootstrap draws `b = n_samples` samples of the observed row count, which is what the manuscript
asks for, and the recording frame observes both numbers separately.

What this module does NOT establish: that the class a reader assigns to each divergence (D1..D9) is right. A
divergence record carries the two values and the ledger's binding; class assignment is `taxonomy.classify`
over an authored diagnosis, and no test here authors one. Nor does it establish that the literal inputs span
each equation's domain: each row names the case it discriminates, and that case is the divergence it exists
to see.
"""

from __future__ import annotations

import itertools

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from tests.conformance.equation_check import check, implementation, seeded_global_numpy
from tests.correspondence import compare

# The legacy units the five equations run — the register's implementation unit for each: the tau test shared by
# E-15, E-16 and E-30, the two halves of E-13's CVM statistic, and E-11's quantile-correlation listing.
TAU_UNIT = "_utils.py:718-746"
EMPIRICAL_CDF_UNIT = "_b5_fit_vine_copulas.py:58-71"
CVM_UNIT = "_b5_fit_vine_copulas.py:72-75"
QUANTILE_CORRELATION_UNIT = "_utils.py:2198-2283"

# The comparison policy these tests take by default, the equivalence family's own: numbers within
# `1e-9 * |reference|`, and every other kind exact. `Tolerance` loosens numeric data and nothing else, so a label,
# string or integer carried in the same value is still compared exactly — which is why the one-`check`-call
# convention is satisfiable on an equation whose two components have different intents (E-11's pairs, E-16's
# decision).
TOLERANCE = compare.Tolerance(rtol=1e-9, atol=0.0)

# The one seed the stochastic equations set. The legacy texts draw through `np.random.*` and through
# `DataFrame.sample` with no generator argument, so numpy's global state is the only channel that steers them.
SEED = 20240117


@pytest.mark.conformance(unit="E-11", part="test_eq11_quantile_correlation", system="publication-math-conformance")
def test_eq11_quantile_correlation(request: pytest.FixtureRequest) -> None:
    r"""E-11: the Spearman rank correlation of each retained pair of marginals.

    The register's span for this equation, verbatim:

    Compute the Spearman rank correlation coefficient for each pair of marginals while omitting combinations of two different measures of rainfall intensity and two different measures of maximum surge.

    Discriminating inputs. Each of the six columns misses exactly one row and no two miss the same row, so the
    code ranks every column over its own nine or ten values (`_utils:2185`) and then correlates what survives
    the intersection of all six (`_utils:2202`) — the register's first recorded divergence. The pair set is
    the second: the code keeps only pairs carrying exactly one `surge` name (`_utils:2211-2242`), where the
    rule quoted above omits two of the fifteen pairs and keeps the other thirteen.

    The reading: the rule is read literally, so only two rainfall-intensity measures and two maximum-surge
    measures are omitted; a rain depth against a rain intensity is a pair of marginals the rule keeps.

    One value carries both components — the pairs exactly, the coefficients within tolerance — because
    `Tolerance` leaves the text of a pair exact.
    """
    frame = pd.DataFrame(
        {
            "rain_depth_total": [10.0, 12.0, 9.0, 8.0, 15.0, 9.5, 11.0, 14.0, 7.0, 13.0],
            "rain_intensity_1h": [2.0, np.nan, 3.0, 2.5, 4.0, 1.5, 2.2, 3.3, 1.9, 2.8],
            "rain_intensity_6h": [1.0, 1.5, 1.8, np.nan, 2.5, 1.1, 1.4, 2.0, 1.2, 1.7],
            "surge_peak": [1.2, 1.6, 1.9, 1.4, 2.2, np.nan, 1.5, 1.7, 1.3, 1.8],
            "surge_1h_mean": [1.0, 1.4, 1.7, 1.2, 2.0, 1.1, np.nan, 1.5, 1.1, 1.6],
            "lag_peak_ms": [3.0, 5.0, 4.0, 6.0, 2.0, 7.0, 5.0, np.nan, 4.0, 6.0],
        }
    )

    def reduce(finished):
        """The retained pairs, each as its own sorted label pair, and its coefficient in that same order."""
        listing = finished["calculate_quantile_correlations"](frame)
        pairs = sorted({tuple(sorted((one, two))) for one, two in zip(listing["var1"], listing["var2"], strict=True)})
        coefficients = {
            tuple(sorted((one, two))): float(value)
            for one, two, value in zip(listing["var1"], listing["var2"], listing["corr"], strict=True)
        }
        return pairs, [coefficients[pair] for pair in pairs]

    observed_pairs, observed_coefficients = implementation(QUANTILE_CORRELATION_UNIT, None, reduce)

    complete = frame.dropna()
    correlations = complete.rank().corr()
    # The same canonical layout the reduction gives the observed side: each pair sorted inside itself, and the
    # pairs sorted among themselves. A faithful listing may emit either member first and in any row order, so a
    # reference kept in column order would refuse it for the layout rather than for the coefficient.
    expected_pairs = sorted(
        tuple(sorted(pair))
        for pair in itertools.combinations(frame.columns, 2)
        if not ("rain_intensity" in pair[0] and "rain_intensity" in pair[1])
        and not ("surge" in pair[0] and "surge" in pair[1])
    )
    expected_coefficients = [float(correlations.loc[one, two]) for one, two in expected_pairs]

    def rendered(pairs, coefficients):
        """One value holding both components, each in the rendering the comparison policy fixes for its kind."""
        return xr.Dataset(
            {
                "pairs": (("pair", "label"), np.asarray(pairs)),
                "coefficient": (("pair",), np.asarray(coefficients, dtype=float)),
            }
        )

    check(
        request,
        rendered(observed_pairs, observed_coefficients),
        rendered(expected_pairs, expected_coefficients),
        TOLERANCE,
    )


@pytest.mark.conformance(unit="E-13", part="test_eq13_cvm_statistic", system="publication-math-conformance")
def test_eq13_cvm_statistic(request: pytest.FixtureRequest) -> None:
    r"""E-13: the Cramer-von Mises distance between a sample's empirical CDF and the copula's own CDF.

    The register's span for this equation, verbatim:

    T_{\text{cvm}}(\boldsymbol{x}) = \sum_{i=0}^n \left[ \hat{F}(\vec{x}_{i}) - C_{0}(F_{1}(x_{1}),\dots,F_{4}(x_{4})) \right]^2

    Discriminating inputs. The sample's six rows are strictly nested — every row is componentwise no smaller
    than every row before it — so the empirical CDF takes all six of its values instead of collapsing to one,
    and the `n + 1` denominator at `_b5:69` moves every one of them. `C_0` is a literal series of the copula's
    own values at those six points; the manuscript's formula takes it as given, so computing it is not this
    row's subject.

    The two readings this equation needs. The manuscript does not define `\hat{F}`, so the conventional
    empirical CDF, `count / n`, is the reference. The printed lower bound `i = 0` addresses no observation and
    reads as `i = 1..n`, the `n` observations the sample has.
    """
    sample = pd.DataFrame(
        {
            "x1": [0.10, 0.25, 0.40, 0.55, 0.70, 0.90],
            "x2": [0.15, 0.22, 0.45, 0.50, 0.75, 0.85],
            "x3": [0.12, 0.30, 0.38, 0.60, 0.68, 0.92],
            "x4": [0.18, 0.28, 0.42, 0.52, 0.72, 0.95],
        }
    )
    copula_cdf = pd.Series([0.10, 0.25, 0.40, 0.55, 0.70, 0.85], index=sample.index)

    def reduce_empirical(finished):
        """The sample's empirical joint probability at each of its own points."""
        return finished["empirical_multivariate_cdf"](sample)

    def reduce_cvm(finished):
        """The distance from the empirical series to the copula's own series."""
        return float(finished["compute_cvm_criterion"](empirical, copula_cdf))

    empirical = implementation(EMPIRICAL_CDF_UNIT, None, reduce_empirical)
    observed = implementation(CVM_UNIT, None, reduce_cvm)

    n_observations = len(sample)
    counts = [int((sample <= sample.loc[row]).all(axis=1).sum()) for row in sample.index]
    expected = float(
        sum((counts[position] / n_observations - copula_cdf.iloc[position]) ** 2 for position in range(n_observations))
    )

    check(request, observed, expected, TOLERANCE)


@pytest.mark.conformance(unit="E-15", part="test_eq15_kendall_tau_statistic", system="publication-math-conformance")
def test_eq15_kendall_tau_statistic(request: pytest.FixtureRequest) -> None:
    r"""E-15: the Kendall tau statistic of a bivariate sample.

    The register's span for this equation, verbatim:

    $H_{0}: \tau^{x_{i},x_{j}} = \tau_0^{x_{i},x_{j}}$
                    \item $H_{1}: \tau^{x_{i},x_{j}} \neq \tau_0^{x_{i},x_{j}}$
                    \item Method:
                    \begin{itemize}
                        \item Test statistic, $T_{\tau}$ is given by the Kendall's Tau Coefficient for a bivariate sample $$T_{\tau}(\vec{x}_{i}, \vec{x}_{j})=\frac{2}{n(n-1)}\sum_{1\leq p \leq q \leq n}
                        \begin{cases}
                        1 \text{ if } (x_{p,i}- x_{q,i})(x_{p,j}- x_{q,j})>0 \\
                        -1 \text{ otherwise}
                        \end{cases}

    Discriminating inputs. The five-row frame carries ties in both columns, which is the case the register
    records: `scipy.stats.kendalltau` is tau-b, whose denominator corrects for ties and whose value on this
    frame is `0.8888888888888888`, where the manuscript's tau-a scores every tied comparison `-1` and gives
    `0.1`. A frame without ties would leave the two forms equal and the test vacuous.

    The reading this equation needs. The printed bound `p <= q` is kept as printed, because it is defined — the
    `p = q` term's product is zero, which scores `-1`. Under the conventional `p < q` the same frame would give
    `0.6`; the test is red either way, and this docstring records which reading it carries.

    The bootstrap draws inside the called function are steered by a literal seed even though the compared
    statistic is computed before them, so the whole call is repeatable rather than merely its comparison.
    """
    frame = pd.DataFrame({"x": [1.0, 2.0, 2.0, 3.0, 4.0], "y": [1.0, 3.0, 2.0, 3.0, 5.0]})

    def reduce(finished):
        """The observed sample's tau, the fourth value the test returns."""
        _result, _pvalue, _taus, tau_observed = finished["monte_carlo_tau_test"](
            df_obs=frame, col1_iloc=0, col2_iloc=1, df_simulated=frame, n_samples=2
        )
        return float(tau_observed)

    with seeded_global_numpy(SEED):
        observed = implementation(TAU_UNIT, None, reduce)

    n_observations = len(frame)
    score = 0
    for first in range(n_observations):
        for second in range(first, n_observations):
            product = (frame.iloc[first, 0] - frame.iloc[second, 0]) * (frame.iloc[first, 1] - frame.iloc[second, 1])
            score += 1 if product > 0 else -1
    expected = 2.0 / (n_observations * (n_observations - 1)) * score

    check(request, observed, expected, TOLERANCE)


@pytest.mark.conformance(unit="E-16", part="test_eq16_tau_null_and_rejection", system="publication-math-conformance")
def test_eq16_tau_null_and_rejection(request: pytest.FixtureRequest) -> None:
    r"""E-16: the Monte-Carlo null distribution of tau, and the two-sided rejection built on it.

    The register's span for this equation, verbatim:

    Monte-Carlo-derived null distribution of $T_{\tau}$ from $b$ samples of the vine copula given number of observations $n$, $\hat{F}_{\tau,n}(t)=P(T_{\tau}\leq t|n,H_{0})$
                        \item The lower and upper critical thresholds for this two sided test, $c_{\text{l}}$ and $c_{\text{u}}$, to achieve target significance level $\alpha$ are given by $c_{l}=\hat{F}^{-1}_{\tau,n}\left( \frac{\alpha}{2} \right)$ and $c_{u}=\hat{F}^{-1}_{\tau,n}\left( 1- \frac{\alpha}{2} \right)$
                        \item rejection region $R={t:(t<c_{l}) \text{ or } (t>c_{u}})$
                        \item p-value is given by the null distribution operated at the test statistic of the observed sample, $t_{obs}$, $\text{p-value}=\hat{F}_{\tau,n}(t_{obs})$.

    Discriminating inputs. The observed frame is perfectly anti-monotone, so $t_{obs} \approx -1$ lies below
    every bootstrapped tau of an almost independent simulated frame. The manuscript's p-value is then exactly
    `0.0` and the observed statistic falls in the region the manuscript defines, so the decision is `reject`.
    The code reads the null distribution at the NEAREST simulated value instead (`_utils:598-604`), whose
    `rank(pct=True)` is `0.05` here, and `0.05` is neither below `0.025` nor above `0.975` — so the same call
    answers `fail to reject`. Both the p-value and the decision therefore separate the two forms, and the
    separation does not depend on which taus the seed happens to draw: a bootstrap tau can only reach `-1` by
    reproducing a perfect anti-monotone sample.

    The reading this equation needs. `F^{-1}(p)` is the smallest `t` with `F(t) >= p` (numpy's `inverted_cdf`),
    which is the only reading of the printed inverse that keeps `t < c_{l}` equivalent to `F(t) < \alpha/2`.
    """
    observed_frame = pd.DataFrame({"rain": list(range(12)), "surge": list(reversed(range(12)))})
    simulated_frame = pd.DataFrame({"rain": np.arange(50), "surge": (37 * np.arange(50) + 11) % 50})
    n_samples = 20
    significance = 0.05

    def reduce(finished):
        """The decision, the p-value, and the simulated taus and observed statistic the reference reads."""
        result, pvalue, taus, tau_observed = finished["monte_carlo_tau_test"](
            df_obs=observed_frame,
            col1_iloc=0,
            col2_iloc=1,
            df_simulated=simulated_frame,
            n_samples=n_samples,
            alpha=significance,
        )
        return result, float(pvalue), np.asarray(taus, dtype=float), float(tau_observed)

    with seeded_global_numpy(SEED):
        observed_result, observed_pvalue, simulated_taus, tau_observed = implementation(TAU_UNIT, None, reduce)

    expected_pvalue = float(np.count_nonzero(simulated_taus <= tau_observed) / simulated_taus.size)
    lower = float(np.quantile(simulated_taus, significance / 2, method="inverted_cdf"))
    upper = float(np.quantile(simulated_taus, 1 - significance / 2, method="inverted_cdf"))
    expected_result = "reject" if (tau_observed < lower or tau_observed > upper) else "fail to reject"

    def rendered(result, pvalue):
        """One value holding the decision, which is text, beside the p-value, which is a number."""
        return xr.Dataset({"decision": ((), result), "p_value": ((), pvalue)})

    check(request, rendered(observed_result, observed_pvalue), rendered(expected_result, expected_pvalue), TOLERANCE)


@pytest.mark.conformance(unit="E-30", part="test_eq30_generated_tau_test", system="publication-math-conformance")
def test_eq30_generated_tau_test(request: pytest.FixtureRequest) -> None:
    r"""E-30: the bootstrapped null distribution of tau, and the two numbers its draw is defined by.

    The register's span for this equation, verbatim:

    bootstrapped-derived null distribution of $T_{\tau}$ from $b$ samples of the generated dataset given number of observations $n$, $\hat{F}_{\tau,n}(t)=P(T_{\tau}\leq t|n,H_{0})$

    Discriminating inputs. The distribution itself is E-16's subject; this row asks the two numbers the span
    names, so the frame below is the observable: `sim` is a `pd.DataFrame` subclass whose `sample` records the
    size of every draw before it delegates. `b` moves with `n_samples` and the recorded size moves with the
    observed row count, so each compared value carries one quantity and neither can stand in for the other. The
    declared call leaves `cop_fitted` at `None`, which is the branch the bootstrap reaches, and the drawn sample
    size is the observed sample's own `n` — the row count of `obs`, not of `sim`.

    Nothing separates the two sides here: the code draws `b = n_samples` samples of `n` observations, and the
    span asks for exactly that. This row is the family's control that a green is representable — the register
    records no defect for it.
    """
    drawn_sizes: list[int] = []

    class RecordingFrame(pd.DataFrame):
        """A frame whose every resample records the size it was asked for."""

        @property
        def _constructor(self):
            """Rebuild as this subclass, so a resample is a recording frame too."""
            return RecordingFrame

        def sample(self, n=None, **kwargs):
            """Record the bootstrap size the legacy call asks for, then resample."""
            drawn_sizes.append(n)
            return super().sample(n=n, **kwargs)

    observed_frame = pd.DataFrame({"rain": list(range(20)), "surge": [(7 * index + 3) % 20 for index in range(20)]})
    simulated_frame = RecordingFrame({"rain": np.arange(50), "surge": (37 * np.arange(50) + 11) % 50})
    n_samples = 7

    def reduce(finished):
        """How many taus the null distribution holds, and the recorded size of every bootstrap draw."""
        _result, _pvalue, taus, _tau_observed = finished["monte_carlo_tau_test"](
            observed_frame, 0, 1, df_simulated=simulated_frame, n_samples=n_samples
        )
        return len(taus), np.asarray(drawn_sizes, dtype=int)

    with seeded_global_numpy(SEED):
        observed_b, observed_sizes = implementation(TAU_UNIT, None, reduce)

    expected_b = n_samples
    expected_sizes = np.full(n_samples, len(observed_frame), dtype=int)

    def rendered(draws, sizes):
        """One value holding the count of draws beside the size of each, both integers."""
        return xr.Dataset({"b": ((), draws), "sample_size": (("draw",), sizes)})

    check(request, rendered(observed_b, observed_sizes), rendered(expected_b, expected_sizes), compare.Exact())
