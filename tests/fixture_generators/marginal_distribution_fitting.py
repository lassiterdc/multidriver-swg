"""Synthetic inputs for the equivalence tests of the legacy marginal-fitting helper functions owned by marginal-distribution-fitting.

The legacy script fits candidate distributions to one event statistic after optional transformations (an upper bound, a
shift, a rescaling to zero mean, a Box-Cox power). None of the real data may be published, so each input below is
invented: a named series of 40 positive values with a right-skewed spread, in the form the script holds an event
statistic, and a second series that holds zero and negative values, which makes the Box-Cox helper shift it before
transforming. A generator opens no file and reads nothing at module level, which the fixture machinery enforces while it
runs.
"""

import numpy as np
import pandas as pd

from tests.correspondence.fixtures import generates_fixture

N_VALUES = 40


@generates_fixture(name="series_positive", seed=20262801)
def series_positive(rng):
    """40 positive values of a right-skewed event statistic, named as the legacy script names one."""
    return pd.Series(np.round(rng.gamma(shape=3.0, scale=6.0, size=N_VALUES) + 1.0, 4), name="precip_depth_mm")


@generates_fixture(name="series_mixed", seed=20262802)
def series_mixed(rng):
    """40 values including zero and negative ones, so that the Box-Cox helper must shift them above zero first."""
    values = np.round(rng.normal(loc=1.0, scale=4.0, size=N_VALUES), 4)
    values[0] = 0.0
    values[1] = -3.5
    return pd.Series(values, name="surge_peak_ft")


# ---- the inputs of the marginal-selection cells of the legacy selection script (lines 23-65, 66-181, 268-316) ----

KEYS = ("surge_events", "rain_events", "compound_events")
EVENT_TYPE = {"surge_events": "surge", "rain_events": "rain", "compound_events": "compound"}
LONG_STATISTICS = {
    "surge_events": "max_mean_1hrsurge_peak_after_4hrrain_peak_h",
    "rain_events": "max_mean_1hrsurge_peak_after_16hrrain_peak_h",
    "compound_events": "max_mean_16hrsurge_peak_after_24hrrain_peak_h",
}
SURGE_THRESHOLD_FT = 2.0
RAIN_THRESHOLD_MM = 12.0


def _event_table(rng, key):
    """24 events of one type: the two thresholded statistics, a long-name surge statistic and the largest four-hour intensity."""
    count = 24
    return pd.DataFrame(
        {
            "precip_depth_mm": np.round(RAIN_THRESHOLD_MM + rng.gamma(shape=2.5, scale=9.0, size=count), 3),
            "surge_peak_ft": np.round(SURGE_THRESHOLD_FT + rng.gamma(shape=2.0, scale=0.8, size=count), 3),
            LONG_STATISTICS[key]: np.round(rng.gamma(shape=3.0, scale=1.1, size=count) + 0.5, 3),
            "max_4hr_mean_mm_per_hr": np.round(rng.gamma(shape=2.5, scale=2.0, size=count) + 1.0, 3),
        }
    )


@generates_fixture(name="msel_surge_events", seed=20262821)
def msel_surge_events(rng):
    """The statistics of 24 surge events."""
    return _event_table(rng, "surge_events")


@generates_fixture(name="msel_rain_events", seed=20262822)
def msel_rain_events(rng):
    """The statistics of 24 rain events."""
    return _event_table(rng, "rain_events")


@generates_fixture(name="msel_compound_events", seed=20262823)
def msel_compound_events(rng):
    """The statistics of 24 compound events."""
    return _event_table(rng, "compound_events")


@generates_fixture(name="msel_combined_events", seed=20262824)
def msel_combined_events(rng):
    """The combined event summaries the first cell reads: eight events of each type, with the four time columns the cell parses."""
    frames = []
    for key in KEYS:
        table = _event_table(rng, key).iloc[:8].copy()
        table.insert(0, "event_type", EVENT_TYPE[key])
        starts = pd.date_range("2012-01-01", periods=8, freq="90D")
        table.insert(1, "event_start", starts)
        table.insert(2, "event_end", starts + pd.Timedelta(hours=30))
        table.insert(3, "surge_peak_tstep", starts + pd.Timedelta(hours=12))
        table.insert(4, "precip_max_intensity_tstep", starts + pd.Timedelta(hours=9))
        frames.append(table)
    return pd.concat(frames, ignore_index=True)


@generates_fixture(name="msel_stats_compound", seed=20262825)
def msel_stats_compound(rng):
    """The pairs of statistics the correlation step kept for the compound events (two columns of names)."""
    return pd.DataFrame(
        {
            "var1": ["precip_depth_mm", "precip_depth_mm", "surge_peak_ft"],
            "var2": ["surge_peak_ft", "max_4hr_mean_mm_per_hr", LONG_STATISTICS["compound_events"]],
        }
    )


@generates_fixture(name="msel_stats_rain", seed=20262826)
def msel_stats_rain(rng):
    """The pairs of statistics kept for the rain events."""
    return pd.DataFrame(
        {
            "var1": ["precip_depth_mm", "precip_depth_mm"],
            "var2": ["max_4hr_mean_mm_per_hr", "surge_peak_ft"],
        }
    )


@generates_fixture(name="msel_stats_surge", seed=20262827)
def msel_stats_surge(rng):
    """The pairs of statistics kept for the surge events."""
    return pd.DataFrame(
        {"var1": ["surge_peak_ft", "surge_peak_ft"], "var2": ["precip_depth_mm", LONG_STATISTICS["surge_events"]]}
    )


@generates_fixture(name="msel_stats_other", seed=20262828)
def msel_stats_other(rng):
    """A table of statistic pairs in a file whose name the cell does not recognize."""
    return pd.DataFrame({"var1": ["precip_depth_mm"], "var2": ["surge_peak_ft"]})


_PERF_COLUMNS = [
    "data",
    "distribution",
    "n_params",
    "cvm_pval",
    "ks_pval",
    "aic",
    "upper_bound",
    "scalar_shift",
    "normalize",
    "boxcox",
    "fix_loc_to_min",
]
_NAN = float("nan")


def _performance(rows):
    """A table of fit performance in the legacy script's columns, from tuples (data, distribution, n_params, cvm, ks, aic, upper bound, boxcox, fixed location)."""
    table = pd.DataFrame(
        [(d, f, n, c, k, a, ub, 0, False, bx, fl) for d, f, n, c, k, a, ub, bx, fl in rows], columns=_PERF_COLUMNS
    )
    return table.astype({"normalize": bool, "boxcox": bool, "fix_loc_to_min": bool})


@generates_fixture(name="msel_perf_surge_events", seed=20262831)
def msel_perf_surge_events(rng):
    """The surge events' fit performance: four fits the final selection names, a passing fit it does not, and a fit that fails the cutoffs."""
    s = LONG_STATISTICS["surge_events"]
    return _performance(
        [
            ("surge_peak_ft", "weibull_min", 3, 0.86, 0.9, 120.3, _NAN, False, True),
            ("surge_peak_ft", "gamma", 3, 0.5, 0.8, 130.0, _NAN, False, False),
            ("surge_peak_ft", "norm", 2, 0.05, 0.9, 150.0, _NAN, False, False),
            ("precip_depth_mm", "genpareto", 3, 0.72, 0.6, 210.5, 150.0, False, False),
            (s, "gumbel_l", 2, 0.85, 0.7, 80.2, _NAN, False, False),
            ("max_4hr_mean_mm_per_hr", "genpareto", 3, 0.91, 0.95, 60.8, _NAN, False, True),
        ]
    )


@generates_fixture(name="msel_perf_rain_events", seed=20262832)
def msel_perf_rain_events(rng):
    """The rain events' fit performance: the four fits the final selection names."""
    s = LONG_STATISTICS["rain_events"]
    return _performance(
        [
            ("surge_peak_ft", "exponpow", 3, 0.96, 0.9, 99.1, 12.0, False, False),
            ("precip_depth_mm", "weibull_min", 3, 0.96, 0.8, 180.0, _NAN, False, True),
            (s, "t", 3, 0.98, 0.9, 70.4, _NAN, False, False),
            ("max_4hr_mean_mm_per_hr", "chi", 3, 0.96, 0.85, 75.9, _NAN, False, False),
        ]
    )


@generates_fixture(name="msel_perf_compound_events", seed=20262833)
def msel_perf_compound_events(rng):
    """The compound events' fit performance: the four fits the final selection names."""
    s = LONG_STATISTICS["compound_events"]
    return _performance(
        [
            ("surge_peak_ft", "weibull_min", 3, 0.94, 0.9, 101.0, _NAN, False, True),
            ("precip_depth_mm", "genpareto", 3, 0.86, 0.7, 200.4, _NAN, False, False),
            (s, "weibull_min", 3, 0.94, 0.8, 66.2, _NAN, False, False),
            ("max_4hr_mean_mm_per_hr", "t", 3, 0.97, 0.9, 85.0, _NAN, True, False),
        ]
    )


@generates_fixture(name="msel_cdf_long", seed=20262841)
def msel_cdf_long(rng):
    """The long table the marginal-fitting step saves: observation id, variable name and fitted probability; nine observations of two variables, one value missing."""
    table = pd.DataFrame(rng.uniform(0.02, 0.98, size=(9, 2)), columns=["precip_depth_mm", "surge_peak_ft"])
    long = table.reset_index(names="observation_id").melt(
        id_vars="observation_id", var_name="data", value_name="cdf_fit"
    )
    long.loc[(long["observation_id"] == 3) & (long["data"] == "surge_peak_ft"), "cdf_fit"] = np.nan
    return long[["observation_id", "data", "cdf_fit"]].reset_index(drop=True)
