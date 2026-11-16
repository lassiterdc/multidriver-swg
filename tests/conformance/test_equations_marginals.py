r"""Conformance test of the marginal-fitting equation E-12.

The equation fits each of thirteen candidate distributions to one marginal sample and reports, for
every fit, two test p-values and four relative goodness-of-fit measures. The test runs the legacy
unit that performs that fitting through `equation_check.implementation`, writes the equation's own
list and metric definitions in its own body over a literal sample, and compares the two through one
`check`. Nothing here reaches a legacy text outside the lift harness.

The register's span, transcribed: "fit each distribution in [genextreme, weibull min, weibull max,
gumbel r, gumbel l, norm, lognorm, t, genpareto, chi, gamma, pearson3, exponpow] ... Return the
p-value of the Cramer-von Mises (CVM) goodness of fit test and the Kolmogorov-Smirnov goodness of
fit test, and relative goodness-of-fit metrics, the Akaike information criterion, the Bayesian
information criterion, the mean square deviation index, and the mean absolute deviation index
(equations for the latter 2 metrics are defined in `teegavarapu_chapter_2019`)."

The elided middle of that span lists the transformation trials: Box-Cox and standardization, an
upper bound applied by reflection, a lower bound, and a lower bound assigned through the location
parameter. This test holds every one of them at "none", so the record it compares is the
untransformed fit and the metric definitions are weighed on one legible footing. That choice is the
test's, not the equation's, and the transformation trials are not compared here.

The two quantities the manuscript names without defining are the deviation indices, cited there to a
book chapter that is not part of this repository. What the comparison rests on is therefore the form
written here in words, and a reader does not have to fetch anything to check it. The chapter's mean
absolute deviation index is `(1/N) * sum |(x_i - z_i) / x_i|` and its mean square deviation index is
`(1/N) * sum ((x_i - z_i) / x_i)**2` — its equations 2-31 and 2-32, which it attributes to Jain and
Singh (1987) — where `x_i` is the observation, `z_i` the fitted value at that observation's plotting
position, and the plotting position is the Gringorten formula `(i - 0.44) / (N + 0.12)`. The
assertion this file makes is that the record equals THAT form; whether the chapter states it in
those words is a question for a reader who holds the chapter.

The same reading fixes the two test statistics, which the span also names without printing: the CVM
p-value is the one-sample Cramer-von Mises test of the sample against the fitted CDF, and the KS
p-value the one-sample Kolmogorov-Smirnov test of the same pair.

The discriminating case is structural rather than a matter of the inputs. The register records no
departure for this equation, and the fit record it carries holds `aic` and no Bayesian information
criterion at all, so the observed BIC slot is empty whatever the sample; the reference computes it
from the fitted parameters. The sample therefore follows the manuscript's domain — forty positive
event statistics, sorted, of the shape a rain depth or a peak surge takes — and is long enough that
every one of the thirteen fits converges without a boundary solution and that the product of the
densities inside the record does not underflow to zero, which would drive the AIC slot to infinity
for an arithmetic reason and leave its definition untested.

What this does NOT establish: which artifact is authoritative where the paper and the code disagree.
The red this test lands on is a record of the disagreement, and the class assignment stays with the
classifier over an authored `Diagnosis`. Nor does the test establish that the record's fit is the
maximum-likelihood solution the equation asks for: this file fits the same families itself and does
not compare the two parameter sets. A green would mean the record and the equation agree on the
family list and on all six measures for every family, which is the outcome to expect once the record
carries a BIC and the KS statistic is the one-sample form.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from scipy import stats

from tests.conformance import equation_check
from tests.correspondence import compare

# The two units `tests/lifts/marginal_distribution_fitting.py` declares for this equation, each with
# `part=None`: the fitting function, and the cell that defines the candidate list it is called with.
FITTING_UNIT = "__ref_ams_functions.py:111-214"
CANDIDATE_UNIT = "__ref_ams_functions.py:325-387"

# The thirteen families the span lists, in scipy's spelling: "weibull min"/"weibull max" and
# "gumbel r"/"gumbel l" are `weibull_min`/`weibull_max` and `gumbel_r`/`gumbel_l`, and "sep" in the
# legacy cell is `exponpow`.
PAPER_FAMILIES = (
    "genextreme",
    "weibull_min",
    "weibull_max",
    "gumbel_r",
    "gumbel_l",
    "norm",
    "lognorm",
    "t",
    "genpareto",
    "chi",
    "gamma",
    "pearson3",
    "exponpow",
)

# Forty positive event statistics, sorted and strictly increasing. An explicit list, not a draw: the
# fits are deterministic and a drawn sample would move every expected value with the environment.
SAMPLE = np.array(
    [
        6.2,
        7.1,
        7.9,
        8.4,
        9.0,
        9.3,
        9.8,
        10.1,
        10.5,
        10.9,
        11.2,
        11.6,
        12.0,
        12.4,
        12.9,
        13.3,
        13.8,
        14.2,
        14.7,
        15.2,
        15.7,
        16.1,
        16.6,
        17.2,
        17.8,
        18.4,
        19.1,
        19.8,
        20.6,
        21.4,
        22.3,
        23.2,
        24.2,
        25.3,
        26.5,
        27.9,
        29.4,
        31.2,
        33.5,
        36.4,
    ]
)


@pytest.mark.conformance(unit="E-12", part="test_eq12_marginal_fit_and_metrics", system="publication-math-conformance")
def test_eq12_marginal_fit_and_metrics(request: pytest.FixtureRequest) -> None:
    r"""Fit thirteen distributions to one literal sample and compare the family list and six measures.

    `fit each distribution in [genextreme, weibull min, weibull max, gumbel r, gumbel l, norm,
    lognorm, t, genpareto, chi, gamma, pearson3, exponpow]`; for each, return the CVM and KS
    p-values, the AIC, the BIC, the mean square deviation index and the mean absolute deviation
    index. Both indices take the form the module docstring writes out.

    The observed side is the code's own family list and the fit record of each family on the literal
    sample, with every transformation trial held at "none": no upper bound, no lower bound, no
    location fixed at the minimum, no shift, no standardization and no Box-Cox. The expected side is
    the manuscript's thirteen names and, for each, the six measures this body computes after fitting
    that family to the same sample here.

    The two sides are compared as one `xr.Dataset` in two variables: `distribution`, the thirteen
    names rendered as `np.asarray(sorted(names))`, and `metrics`, a thirteen-by-six float array in
    that same sorted order. Each variable is ordered by its own sorted names, so the two sides line
    up row for row whenever the names agree and a list of another length is refused as a size
    difference. The two variables carry separate dimensions on purpose: were they one dimension, a
    family list of another length could not be assembled into a value at all, and the family-list
    comparison would fail as an array-construction error instead of a `ConformanceDivergence`. One
    structured value, because the row owes two comparisons — names exactly, measures within
    tolerance — and `Tolerance` compares a string variable exactly while loosening only numeric data,
    which is the split the row asks for in a single `check`.

    The red this test lands on: the record carries `aic` and no BIC key, so the observed BIC slot
    renders as NaN against the reference's number, and a NaN is refused against a value. The KS slot
    separates the two forms of the statistic for twelve of the thirteen families (all but
    `weibull_min`, whose two p-values both round to one), since the record measures a two-sample
    distance between the observations and the fitted quantiles at the Gringorten plotting positions
    rather than the one-sample distance the manuscript's test names. Both are candidate divergences
    the test reports and neither is classified here.

    The sample discriminates neither departure by its values — an absent key is absent on any input
    — which is why it is chosen instead for the manuscript's domain and for a fit that converges for
    every family without an infinite AIC.
    """

    # The candidate list and each family's argument count, from the cell that defines them.
    def candidates(finished: Mapping[str, Any]) -> list[tuple[Any, int]]:
        """Each candidate as its scipy family object and the number of arguments its fit estimates."""
        return [(entry["args"]["fx"], entry["args"]["n_params"]) for entry in finished["fxs"]]

    declared = equation_check.implementation(CANDIDATE_UNIT, None, candidates)

    # The fit record of every declared family on the literal sample, with every trial at "none".
    def records(finished: Mapping[str, Any]) -> dict[str, pd.Series]:
        """Each family's fit record, keyed by family name, from one untransformed fit of the sample."""
        return {
            family.name: finished["fit_dist"](
                pd.Series(SAMPLE, name="event_statistic"),
                family,
                n_params,
                np.nan,
                False,
                0,
                False,
                False,
            )[0]
            for family, n_params in declared
        }

    fitted = equation_check.implementation(FITTING_UNIT, None, records)

    # The Gringorten plotting position of the i-th of N sorted observations, written from the cited
    # chapter's formula rather than called from a helper, so the reference cannot move with a library.
    plotting = (np.arange(1, SAMPLE.size + 1) - 0.44) / (SAMPLE.size + 0.12)

    def measures(name: str) -> list[float]:
        """The six measures of one family, fitted to the literal sample here, in the manuscript's order."""
        family = getattr(stats, name)
        parameters = tuple(float(parameter) for parameter in family.fit(SAMPLE))
        log_likelihood = float(np.sum(np.log(family.pdf(SAMPLE, *parameters))))
        at_plotting = family.ppf(plotting, *parameters)
        ratio = (SAMPLE - at_plotting) / SAMPLE
        return [
            float(stats.cramervonmises(SAMPLE, name, args=parameters).pvalue),
            float(stats.kstest(SAMPLE, name, args=parameters).pvalue),
            2.0 * len(parameters) - 2.0 * log_likelihood,
            len(parameters) * np.log(SAMPLE.size) - 2.0 * log_likelihood,
            float(np.mean(ratio**2)),
            float(np.mean(np.abs(ratio))),
        ]

    code_names = np.asarray(sorted(fitted))
    observed_rows = [
        [
            float(fitted[name]["cvm_pval"]),
            float(fitted[name]["ks_pval"]),
            float(fitted[name]["aic"]),
            # The record's missing BIC renders as a gap, which no number equals: the divergence.
            float(fitted[name].get("bic", math.nan)),
            float(fitted[name]["msdi"]),
            float(fitted[name]["madi"]),
        ]
        for name in code_names
    ]
    paper_names = np.asarray(sorted(PAPER_FAMILIES))
    expected_rows = [measures(name) for name in paper_names]

    def structured(distribution: np.ndarray, rows: list[list[float]]) -> xr.Dataset:
        """The family names and the six measures per family, each variable on a dimension of its own."""
        return xr.Dataset(
            {
                "distribution": ("name", distribution),
                "metrics": (("row", "metric"), np.asarray(rows, dtype=float)),
            }
        )

    observed = structured(code_names, observed_rows)
    expected = structured(paper_names, expected_rows)

    equation_check.check(request, observed, expected, compare.Tolerance(rtol=1e-9, atol=0.0))
