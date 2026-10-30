"""Synthetic inputs for the equivalence tests of the legacy generator-validation units that compare simulated with observed events.

The legacy simulation script checks its stochastic output against the observations in two cells. The first compares,
for each copula source, every statistic the simulated events and the observed events have in common by two-sample
tests, bootstrapped and on the full samples, and draws a histogram of each. The second tests, for each pair of
statistics, whether the simulated events reproduce the observed rank correlation, and draws contour plots of both. None
of the real data may be published, so each input below is invented in the form the script holds it: a table of
simulated events with the copula that produced each (columns that source does not model are missing), the observed
event summaries of each event type, and the long table of fitted probabilities the second cell reads back to learn
which statistics a type has. A generator opens no file and reads nothing at module level, which the fixture machinery
enforces while it runs.

Three event types are invented: `rain` (two statistics), `surge` (one statistic) and `compound` (all three, with the
simulated surge peak correlated with the rain depth). The simulated table holds 40 rows of each type and the observed
summaries 14, with one missing value in the observed rain depth of the compound type so that the cells' `dropna` has
something to drop.
"""

import contextlib
import math

import numpy as np
import pandas as pd

from tests.correspondence.fixtures import generates_fixture

with contextlib.suppress(
    ImportError
):  # the vine library, imported here so that its import-time reads happen outside the audit hook
    import pyvinecopulib as pv

EVENT_TYPES = ("rain", "surge", "compound")
STATISTICS = {
    "rain": ("rain_depth_mm", "rain_duration_hr"),
    "surge": ("surge_peak_ft",),
    "compound": ("rain_depth_mm", "rain_duration_hr", "surge_peak_ft"),
}
N_SIMULATED = 40
N_OBSERVED = 14


def _draw(rng: np.random.Generator, count: int, names: tuple[str, ...]) -> pd.DataFrame:
    """`count` events with positive, skewed statistics; the first two statistics of a type are mildly correlated."""
    base = rng.normal(size=(count, len(names)))
    if len(names) > 1:
        base[:, 1] += 0.6 * base[:, 0]
    scales = {"rain_depth_mm": 3.0, "rain_duration_hr": 2.0, "surge_peak_ft": 1.2}
    return pd.DataFrame({name: scales[name] * np.exp(0.5 * base[:, i]) for i, name in enumerate(names)})


@generates_fixture(name="gv_sim_realspace", seed=4301)
def gv_sim_realspace(rng: np.random.Generator) -> pd.DataFrame:
    """Simulated events of the three types: the copula that made each, three event flags, and the statistics of its type (the others missing)."""
    parts = []
    for event_type in EVENT_TYPES:
        frame = _draw(rng, N_SIMULATED, STATISTICS[event_type])
        frame.insert(0, "copula_source", event_type)
        frame.insert(1, "rain_event", event_type in ("rain", "compound"))
        frame.insert(2, "surge_event", event_type in ("surge", "compound"))
        frame.insert(3, "compound_event", event_type == "compound")
        parts.append(frame)
    table = pd.concat(parts, ignore_index=True)
    return table[
        [
            "copula_source",
            "rain_event",
            "surge_event",
            "compound_event",
            "rain_depth_mm",
            "rain_duration_hr",
            "surge_peak_ft",
        ]
    ]


@generates_fixture(name="gv_obs_summaries", seed=4302)
def gv_obs_summaries(rng: np.random.Generator) -> pd.DataFrame:
    """Observed event summaries of the three types in one table with an `event_type` column; the compound type misses one rain depth."""
    parts = []
    for event_type in EVENT_TYPES:
        frame = _draw(rng, N_OBSERVED, STATISTICS[event_type])
        frame.insert(0, "event_type", event_type)
        frame.insert(1, "event_id", np.arange(1, N_OBSERVED + 1))
        parts.append(frame)
    table = pd.concat(parts, ignore_index=True)
    table.loc[(table["event_type"] == "compound") & (table["event_id"] == 5), "rain_depth_mm"] = np.nan
    return table[
        [
            "event_type",
            "event_id",
            "rain_depth_mm",
            "rain_duration_hr",
            "surge_peak_ft",
        ]
    ]


@generates_fixture(name="gv_fitted_cdf_long", seed=4303)
def gv_fitted_cdf_long(rng: np.random.Generator) -> pd.DataFrame:
    """The long table of fitted probabilities the marginal-fitting step saves, for the three event types: type, observation id, statistic name and probability.

    One probability of the compound type is missing, so the cell that reads the table drops that observation and
    prints how many it dropped.
    """
    rows = []
    for event_type in EVENT_TYPES:
        for name in sorted(STATISTICS[event_type]):
            for observation in range(1, N_OBSERVED + 1):
                rows.append((event_type, observation, name, float(rng.uniform(0.02, 0.98))))
    table = pd.DataFrame(rows, columns=["event_type", "observation_id", "data", "cdf_fit"])
    missing = (table["event_type"] == "compound") & (table["observation_id"] == 3) & (table["data"] == "surge_peak_ft")
    table.loc[missing, "cdf_fit"] = np.nan
    return table


# ---- the inputs of the legacy validation helpers of `_utils.py` ----
#
# The helpers compare observed with simulated events: a quantile lookup, a density-percentile function, a histogram of
# a Monte Carlo statistic, a Kendall-tau Monte Carlo test (from a fitted vine copula or by bootstrapping a table of
# simulated events), the evaluator that runs that test for every pair of variables and draws its figures, and a battery
# of two-sample tests. Every input is invented: three correlated variables already converted to probabilities, a vine
# copula fitted to them and saved as its json text, a larger table of further draws of the same dependence for the
# bootstrap, three equal-length samples for the two-sample tests, and the one seed of the vine library's draws.

COPULA_COLUMNS = ["rain_depth_mm", "surge_peak_ft", "duration_hr"]
N_COPULA_OBS = 30
N_COPULA_SIM = 120


def _correlated_probabilities(rng: np.random.Generator, rows: int) -> np.ndarray:
    """`rows` x 3 probabilities from correlated normals: columns 0-1 strongly, 1-2 moderately."""
    z = rng.normal(size=(rows, 3))
    z[:, 1] += 1.6 * z[:, 0]
    z[:, 2] -= 0.9 * z[:, 1]
    z = (z - z.mean(axis=0)) / z.std(axis=0)
    return 0.5 * (1.0 + np.vectorize(math.erf)(z / math.sqrt(2.0)))


@generates_fixture(name="gv_copula_obs", seed=4304)
def gv_copula_obs(rng: np.random.Generator) -> pd.DataFrame:
    """The 30 x 3 table of observed probabilities."""
    return pd.DataFrame(_correlated_probabilities(rng, N_COPULA_OBS), columns=COPULA_COLUMNS)


@generates_fixture(name="gv_copula_sim", seed=4305)
def gv_copula_sim(rng: np.random.Generator) -> pd.DataFrame:
    """A table of 120 simulated events of the same three variables, the pool a bootstrap Monte Carlo test resamples."""
    return pd.DataFrame(_correlated_probabilities(rng, N_COPULA_SIM), columns=COPULA_COLUMNS)


@generates_fixture(name="gv_vinecop_json", seed=4306)
def gv_vinecop_json(rng: np.random.Generator) -> np.ndarray:
    """A vine copula fitted to a table drawn like the observations, as its json text in a one-element array."""
    frame = _correlated_probabilities(rng, N_COPULA_OBS)
    controls = pv.FitControlsVinecop()
    controls.selection_criterion = "loglik"
    controls.parametric_method = "itau"
    controls.tree_criterion = "tau"
    controls.threshold = 0.01
    controls.select_trunc_lvl = False
    fitted = pv.Vinecop.from_data(np.asfortranarray(frame), controls=controls)
    return np.array([fitted.to_json()])


@generates_fixture(name="gv_samples", seed=4307)
def gv_samples(rng: np.random.Generator) -> pd.DataFrame:
    """Three samples of 40 numbers: two separated samples for the two-sample tests, and simulated Kendall taus.

    The second sample's centre stands two standard deviations away from the first, so that the Cramer-von Mises and
    Kolmogorov-Smirnov tests both reject the null at the battery's 5% level: the branch of each test's outcome that
    the real observed-versus-simulated comparison took, and that a pair of overlapping samples leaves untaken. The
    draw that produces it is the same draw as before, so the Kendall-tau sample after it is unchanged.
    """
    return pd.DataFrame(
        {
            "sample1": rng.normal(0.0, 1.0, size=40),
            "sample2": rng.normal(2.0, 1.3, size=40),
            "taus": rng.normal(0.3, 0.1, size=40),
        }
    )


@generates_fixture(name="seed", seed=4308)
def seed(rng: np.random.Generator) -> int:
    """The one seed of every unit's vine-library draws: the lift builds a generator from it and takes a seed from it for each `simulate` call, and the equivalence test hands the same value to the registered callable as `inputs["seed"]`."""
    return int(rng.integers(1, 2**31 - 1))
