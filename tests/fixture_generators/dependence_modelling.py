"""Synthetic inputs for the equivalence tests of the legacy vine-copula goodness-of-fit units owned by dependence-modelling.

The legacy copula script tests a fitted vine copula against the observations it was fitted to: it computes each
observation's empirical joint probability, a Cramer-von Mises distance to the copula's probabilities, a Monte
Carlo test of that distance and of Kendall's tau for each pair of variables, and the figures of those tests. None
of the real data may be published, so each input below is invented: a table of 30 observations of three variables
already converted to probabilities (the form the script fits on), a vine copula fitted to that table and saved
as the text the legacy script saves (json), and the two csv tables the legacy cells read back, as
`pandas.read_csv` returns them.

The one seed of the vine library's draws is a fifth fixture, so that it has a single source: the lift reads it, and the
equivalence test hands the same value to the new side.

The three variables are correlated through a Gaussian dependence of different strengths, so the three pairs have
different Kendall's tau and the fitted copula is not an independence copula. A generator opens no file and reads
nothing at module level, which the fixture machinery enforces while it runs.
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

COLUMNS = ["rain_depth_mm", "surge_peak_ft", "duration_hr"]
N_OBS = 30


def _probabilities(rng: np.random.Generator) -> np.ndarray:
    """30 x 3 probabilities from correlated normals: columns 0-1 strongly, 1-2 moderately, 0-2 through column 1."""
    z = rng.normal(size=(N_OBS, 3))
    z[:, 1] += 1.6 * z[:, 0]
    z[:, 2] -= 0.9 * z[:, 1]
    z = (z - z.mean(axis=0)) / z.std(axis=0)
    return 0.5 * (1.0 + np.vectorize(math.erf)(z / math.sqrt(2.0)))


def _frame(rng: np.random.Generator) -> pd.DataFrame:
    """The observations as the legacy script holds them: probabilities in (0, 1), indexed by observation id."""
    return pd.DataFrame(_probabilities(rng), columns=COLUMNS, index=pd.Index(np.arange(N_OBS), name="observation_id"))


@generates_fixture(name="obs", seed=20262701)
def obs(rng):
    """The 30 x 3 table of observed probabilities."""
    return _frame(rng)


@generates_fixture(name="vinecop_json", seed=20262701)
def vinecop_json(rng):
    """A vine copula fitted to the same table (selection by log-likelihood, rank-based parameters), as its json text in a one-element array."""
    return np.array([_fitted_json(_frame(rng))])


def _fitted_json(frame: pd.DataFrame) -> str:
    """Fit a vine copula to `frame` by log-likelihood with rank-based parameters and return its json text."""
    controls = pv.FitControlsVinecop()
    controls.selection_criterion = "loglik"
    controls.parametric_method = "itau"
    controls.tree_criterion = "tau"
    controls.threshold = 0.01
    controls.select_trunc_lvl = False
    return pv.Vinecop.from_data(np.asfortranarray(frame.to_numpy()), controls=controls).to_json()


def _b6_frame(rng: np.random.Generator, strength: float, rows: int = 10) -> pd.DataFrame:
    """Observations of the three simulation variables as probabilities, column 0 driving the others with `strength`.

    The table read by the cell holds ten observations; a copula is fitted on forty, because ten leave the three types'
    fitted vines identical to one another — every pair too weak to select a family — and three fixtures that are one
    value are the shared stand-in this remedy replaces.
    """
    z = rng.normal(size=(rows, 3))
    z[:, 1] += strength * z[:, 0]
    z[:, 2] -= strength * z[:, 1]
    z = (z - z.mean(axis=0)) / z.std(axis=0)
    probabilities = 0.5 * (1.0 + np.vectorize(math.erf)(z / math.sqrt(2.0)))
    return pd.DataFrame(probabilities, columns=B6_COLUMNS)


def _b6_cdf_table(rng: np.random.Generator, strength: float) -> pd.DataFrame:
    """One event type's long table of fitted probabilities, with one value missing as the legacy table has."""
    long = (
        _b6_frame(rng, strength)
        .reset_index(names="observation_id")
        .melt(id_vars="observation_id", var_name="data", value_name="cdf_fit")
    )
    long.loc[(long["observation_id"] == 4) & (long["data"] == "duration_hr"), "cdf_fit"] = np.nan
    return long[["observation_id", "data", "cdf_fit"]].reset_index(drop=True)


# The simulation cell the two declarations of `_b6_simulate_from_vincop.py:489-596` run is per event type: for each
# type it pairs THAT type's fitted copula with THAT type's table of fitted probabilities, so the six fixtures below
# exist one per type rather than as one shared pair. The run's own six values are the estate's
# `b5_vinecop_modeling/vinecop_params_{surge,rain,compound}_events.json` and
# `b4_fitting_marginals/{surge,rain,compound}_events_empirical_vs_fitted.csv`, which this chunk reads at build time to
# assemble a `reach measure --inputs` directory; none of them is committed, and the fixtures below are synthetic like
# every other fixture in this module.


@generates_fixture(name="b6_vinecop_surge", seed=20262721)
def b6_vinecop_surge(rng):
    """The surge-only type's fitted copula, as its json text in a one-element array: fitted to a table whose second column is the least dependent."""
    return np.array([_fitted_json(_b6_frame(rng, 0.2, rows=40))])


@generates_fixture(name="b6_vinecop_rain", seed=20262722)
def b6_vinecop_rain(rng):
    """The rain-only type's fitted copula, as its json text in a one-element array: fitted to a table with the strongest first-column dependence."""
    return np.array([_fitted_json(_b6_frame(rng, 0.9, rows=40))])


@generates_fixture(name="b6_vinecop_compound", seed=20262723)
def b6_vinecop_compound(rng):
    """The compound type's fitted copula, as its json text in a one-element array: fitted to a table with dependence in every pair."""
    return np.array([_fitted_json(_b6_frame(rng, 1.2, rows=40))])


@generates_fixture(name="fitted_cdf_long", seed=20262701)
def fitted_cdf_long(rng):
    """The long table the legacy marginal-fitting step saves: observation id, variable name and fitted probability; 12 observations, one value missing.

    It is shorter than the table above because the legacy sensitivity cell runs 48 fits with fixed Monte Carlo sizes, and
    its cost grows with the number of observations.
    """
    frame = _frame(rng).iloc[:12]
    long = frame.reset_index().melt(id_vars="observation_id", var_name="data", value_name="cdf_fit")
    long.loc[(long["observation_id"] == 3) & (long["data"] == "duration_hr"), "cdf_fit"] = np.nan
    return long[["observation_id", "data", "cdf_fit"]].reset_index(drop=True)


@generates_fixture(name="sensitivity_results", seed=20262702)
def sensitivity_results(rng):
    """The table of tested fit settings the legacy sensitivity step saves, as read back: two settings, the saved-fit path left for the lift to fill."""
    return pd.DataFrame(
        {"f_vinecop_params": ["", ""], "pvalue": [0.31, 0.64], "result": ["fail to reject", "fail to reject"]},
        index=pd.Index([0, 1], name="control_idx"),
    )


@generates_fixture(name="seed", seed=20262703)
def seed(rng):
    """The one seed of every unit's vine-library draws: the lift passes it to each `cdf` and `simulate` call, and the equivalence test hands the same value to the registered callable as `inputs["seed"]`."""
    return int(rng.integers(1, 2**31 - 1))


# ---- the inputs of the simulation cell of the legacy simulation script (lines 489-596) ----

B6_COLUMNS = ["precip_depth_mm", "surge_peak_ft", "duration_hr"]
_MARGINAL_COLUMNS = [
    "data",
    "distribution",
    "n_params",
    "shape",
    "loc",
    "scale",
    "upper_bound",
    "boxcox",
    "lambda",
    "scalar_shift_for_boxcox",
    "normalize",
    "mean",
    "std",
    "scalar_shift",
]
_NAN = float("nan")


def _marginals(rows: list[tuple]) -> pd.DataFrame:
    """A table of fitted marginals as the legacy fitting step saves it: one row per variable, its transformations beside its fit."""
    return pd.DataFrame(rows, columns=_MARGINAL_COLUMNS)


@generates_fixture(name="b6_surge_events", seed=20262711)
def b6_surge_events(rng):
    """The table of observed surge events; the cell reads its smallest peak as the surge-event threshold (2.0 ft)."""
    peaks = np.concatenate([[2.0], np.round(rng.uniform(2.2, 5.0, size=5), 3)])
    return pd.DataFrame({"surge_peak_ft": peaks})


@generates_fixture(name="b6_rain_events", seed=20262712)
def b6_rain_events(rng):
    """The table of observed rain events; the cell reads its smallest depth as the rain-event threshold (10.0 mm)."""
    depths = np.concatenate([[10.0], np.round(rng.uniform(11.0, 60.0, size=5), 2)])
    return pd.DataFrame({"precip_depth_mm": depths})


@generates_fixture(name="b6_occurrences", seed=20262713)
def b6_occurrences(rng):
    """The simulated yearly event counts: surge-only, rain-only and compound events for ten years, and their total."""
    table = pd.DataFrame(
        {
            "surge": rng.integers(0, 3, size=10),
            "rain": rng.integers(0, 3, size=10),
            "compound": rng.integers(1, 3, size=10),
        },
        index=pd.Index(np.arange(10), name="year"),
    )
    table["any"] = table["surge"] + table["rain"] + table["compound"]
    return table


@generates_fixture(name="b6_cdf_long_surge", seed=20262724)
def b6_cdf_long_surge(rng):
    """The surge-only type's table of fitted probabilities: observation id, variable name and probability; ten observations, one value missing."""
    return _b6_cdf_table(rng, 0.2)


@generates_fixture(name="b6_cdf_long_rain", seed=20262725)
def b6_cdf_long_rain(rng):
    """The rain-only type's table of fitted probabilities, in the same long form."""
    return _b6_cdf_table(rng, 0.9)


@generates_fixture(name="b6_cdf_long_compound", seed=20262726)
def b6_cdf_long_compound(rng):
    """The compound type's table of fitted probabilities, in the same long form."""
    return _b6_cdf_table(rng, 1.2)


@generates_fixture(name="b6_marginals_compound", seed=20262715)
def b6_marginals_compound(rng):
    """Fitted marginals under which every simulated row is a valid compound event: the rain and surge fits lie above both thresholds; the surge fit is normalized and the duration fit shifted."""
    return _marginals(
        [
            ("precip_depth_mm", "gamma", 3, 2.0, 12.0, 5.0, _NAN, False, _NAN, 0.0, False, _NAN, _NAN, 0.0),
            ("surge_peak_ft", "norm", 2, _NAN, 0.0, 1.0, _NAN, False, _NAN, 0.0, True, 4.0, 0.4, 0.0),
            ("duration_hr", "norm", 2, _NAN, 20.0, 4.0, _NAN, False, _NAN, 0.0, False, _NAN, _NAN, 2.0),
        ]
    )


@generates_fixture(name="b6_marginals_surge", seed=20262716)
def b6_marginals_surge(rng):
    """Fitted marginals under which most rows are surge-only: low rain, a surge fit that straddles the threshold, and a duration fit with an upper bound (its cdf values are flipped)."""
    return _marginals(
        [
            ("precip_depth_mm", "norm", 2, _NAN, 4.0, 1.5, _NAN, False, _NAN, 0.0, False, _NAN, _NAN, 0.0),
            ("surge_peak_ft", "norm", 2, _NAN, 2.2, 0.8, _NAN, False, _NAN, 0.0, False, _NAN, _NAN, 0.0),
            ("duration_hr", "gamma", 3, 2.0, 0.0, 6.0, 60.0, False, _NAN, 0.0, False, _NAN, _NAN, 0.0),
        ]
    )


@generates_fixture(name="b6_marginals_rain", seed=20262717)
def b6_marginals_rain(rng):
    """Fitted marginals under which most rows are rain-only: a Box-Cox rain fit whose back-transform is undefined for some draws (invalid rows), shifted above the rain threshold, and a low surge fit."""
    return _marginals(
        [
            ("precip_depth_mm", "norm", 2, _NAN, 0.4, 1.0, _NAN, True, 2.0, 0.0, False, _NAN, _NAN, -12.0),
            ("surge_peak_ft", "norm", 2, _NAN, 0.8, 0.5, _NAN, False, _NAN, 0.0, False, _NAN, _NAN, 0.0),
            ("duration_hr", "norm", 2, _NAN, 10.0, 2.0, _NAN, False, _NAN, 0.0, False, _NAN, _NAN, 0.0),
        ]
    )
