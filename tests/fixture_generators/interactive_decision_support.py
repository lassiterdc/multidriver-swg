"""Synthetic inputs for the equivalence tests of the legacy units owned by interactive-decision-support.

The legacy distribution-fitting module draws, for each candidate distribution fitted to an event statistic, a
four-panel figure that a user reads to choose a fit. The figure function reads a one-row summary of the fit (the
distribution, its parameters, its scores, its support and the transformations applied to the data) and a table of the
observations beside their fitted and empirical probabilities. None of the real data may be published, so each input
is invented in that form: three cases that reach the function's branches. The first is a three-parameter fit with no
transformation; the second a two-parameter fit under every transformation (an upper bound, a shift, a normalization
and a Box-Cox transform) with an unbounded support; the third a three-parameter fit under a shift only, with a bounded
lower support and a location fixed to the minimum. A generator opens no file and reads nothing at module level, which
the fixture machinery enforces while it runs.
"""

import contextlib
import math

import numpy as np
import pandas as pd
from scipy import stats

from tests.correspondence.fixtures import generates_fixture

with contextlib.suppress(
    ImportError
):  # the vine library, imported here so that its import-time reads happen outside the audit hook
    import pyvinecopulib as pv

FIT_COLUMNS = [
    "name",
    "distribution",
    "shape",
    "loc",
    "scale",
    "aic",
    "cvm_pval",
    "ks_pval",
    "support_lb",
    "support_ub",
    "upper_bound",
    "scalar_shift",
    "normalize",
    "boxcox",
    "scalar_shift_for_boxcox",
    "lambda",
    "mean",
    "std",
    "fix_loc_to_min",
]


def _observations(rng: np.random.Generator, count: int = 36) -> np.ndarray:
    """Positive, right-skewed event statistics, like peak surge or rain depth."""
    return np.sort(3.0 + rng.gumbel(loc=4.0, scale=1.6, size=count))


def _table(transformed: np.ndarray, realspace: np.ndarray, cdf_fit: np.ndarray) -> pd.DataFrame:
    """The observations beside the fitted and the empirical (Gringorten) probabilities, as the legacy fit table holds them."""
    empirical = np.asarray(stats.mstats.plotting_positions(transformed, 0.44, 0.44), dtype=float)
    return pd.DataFrame(
        {
            "observations_transformed": transformed,
            "observations_realspace": realspace,
            "cdf_fit": cdf_fit,
            "cdf_emp": empirical,
        }
    )


def _fit_row(**values) -> pd.DataFrame:
    """A one-row table with the summary's fields in the legacy order."""
    return pd.DataFrame([values], columns=FIT_COLUMNS)


@generates_fixture(name="ids_fit_plain", seed=5001)
def ids_fit_plain(rng: np.random.Generator) -> pd.DataFrame:
    """The summary of a three-parameter generalized-extreme-value fit to untransformed data."""
    return _fit_row(
        name="peak_surge_ft",
        distribution="genextreme",
        shape=0.12,
        loc=6.4,
        scale=1.7,
        aic=141.37,
        cvm_pval=0.612,
        ks_pval=0.84,
        support_lb=-np.inf,
        support_ub=14.0,
        upper_bound=np.nan,
        scalar_shift=0.0,
        normalize=False,
        boxcox=False,
        scalar_shift_for_boxcox=np.nan,
        **{"lambda": np.nan},
        mean=np.nan,
        std=np.nan,
        fix_loc_to_min=False,
    )


@generates_fixture(name="ids_table_plain", seed=5002)
def ids_table_plain(rng: np.random.Generator) -> pd.DataFrame:
    """The observations and probabilities of the plain fit."""
    x = _observations(rng)
    return _table(x, x.copy(), stats.genextreme.cdf(x, 0.12, 6.4, 1.7))


@generates_fixture(name="ids_fit_transformed", seed=5003)
def ids_fit_transformed(rng: np.random.Generator) -> pd.DataFrame:
    """The summary of a two-parameter normal fit under an upper bound, a shift, a normalization and a Box-Cox transform, with an unbounded support."""
    return _fit_row(
        name="rain_depth_mm",
        distribution="norm",
        shape=np.nan,
        loc=0.02,
        scale=0.97,
        aic=98.21,
        cvm_pval=0.2371,
        ks_pval=0.5519,
        support_lb=-np.inf,
        support_ub=np.inf,
        upper_bound=30.0,
        scalar_shift=2.5,
        normalize=True,
        boxcox=True,
        scalar_shift_for_boxcox=0.5,
        **{"lambda": 0.4},
        mean=14.2,
        std=3.1,
        fix_loc_to_min=False,
    )


@generates_fixture(name="ids_table_transformed", seed=5004)
def ids_table_transformed(rng: np.random.Generator) -> pd.DataFrame:
    """The observations of the transformed fit: real space values, and their values after each transformation."""
    x = _observations(rng) + 8.0
    shifted = (x - 30.0) * -1 + 2.5
    normalized = (shifted - 14.2) / 3.1
    boxcoxed = stats.boxcox(normalized + 0.5 + 6.0, lmbda=0.4)
    boxcoxed = (boxcoxed - boxcoxed.mean()) / boxcoxed.std()
    return _table(np.sort(boxcoxed), np.sort(x)[::-1].copy(), stats.norm.cdf(np.sort(boxcoxed), 0.02, 0.97))


@generates_fixture(name="ids_fit_shifted", seed=5005)
def ids_fit_shifted(rng: np.random.Generator) -> pd.DataFrame:
    """The summary of a three-parameter Weibull fit under a shift only, with a lower support at the location fixed to the minimum."""
    return _fit_row(
        name="surge_duration_hr",
        distribution="weibull_min",
        shape=1.6,
        loc=0.0,
        scale=9.0,
        aic=77.9,
        cvm_pval=0.0123,
        ks_pval=0.1,
        support_lb=0.0,
        support_ub=np.inf,
        upper_bound=np.nan,
        scalar_shift=-3.0,
        normalize=False,
        boxcox=False,
        scalar_shift_for_boxcox=np.nan,
        **{"lambda": np.nan},
        mean=np.nan,
        std=np.nan,
        fix_loc_to_min=True,
    )


@generates_fixture(name="ids_table_shifted", seed=5006)
def ids_table_shifted(rng: np.random.Generator) -> pd.DataFrame:
    """The observations of the shifted fit."""
    x = np.sort(rng.weibull(1.6, size=36) * 9.0 + 0.2)
    return _table(x, x + 3.0, stats.weibull_min.cdf(x, 1.6, 0.0, 9.0))


# ---- the event-selection records the legacy threshold experiments read ----
#
# The legacy script tries a range of thresholds on the rainfall of three sources, selects the events each threshold
# gives, and compares the sources by the number of events in every 365-day window of data and by the events'
# statistics. None of that data may be published, so each record is invented: three years from 2020-01-01 (so that
# many 365-day windows exist and their event counts take many values). Rainfall is exactly zero between storms,
# because the legacy selection finds an event's limits in dry windows; the three sources see the same storms a little
# apart in time and in size. The storms of the second year are fewer and those of the third more, so the window counts
# differ from window to window.

RECORD_START = pd.Timestamp("2020-01-01")
RECORD_DAYS = 1100
HOUR = pd.Timedelta(hours=1)
BASE_STORMS = (  # (day of year, hour, hours, peak mm per hour): the 12-hour depth runs from about 33 to 100 mm, bar the last
    (8, 3, 6, 22),
    (31, 10, 5, 26),
    (55, 0, 7, 18),
    (80, 14, 5, 30),
    (104, 8, 6, 24),
    (130, 2, 4, 33),
    (151, 9, 6, 28),
    (180, 20, 5, 22),
    (203, 5, 7, 27),
    (231, 11, 5, 34),
    (260, 1, 6, 20),
    (289, 15, 5, 29),
    (318, 7, 6, 25),
    (343, 12, 5, 31),
    (356, 5, 5, 9),  # too weak to reach any threshold; the legacy selection takes it as the last event
)
EXTRA_STORMS = ((45, 6, 5, 27), (95, 12, 6, 29), (165, 3, 5, 31), (215, 22, 5, 26), (275, 9, 6, 30))
STORMS = (
    BASE_STORMS
    + tuple((day + 365, hour, hours, peak) for day, hour, hours, peak in BASE_STORMS[::2])
    + tuple((day + 731, hour, hours, peak) for day, hour, hours, peak in BASE_STORMS + EXTRA_STORMS)
)


def _rain(
    rng: np.random.Generator, step_minutes: int, shift_hours: float, scale: float, nan_count: int, drop_count: int
) -> pd.Series:
    """Rainfall in mm per hour on a grid of `step_minutes`: the storms, `shift_hours` later and `scale` times as intense, zero between."""
    index = pd.date_range(
        RECORD_START, periods=RECORD_DAYS * 24 * 60 // step_minutes, freq=f"{step_minutes}min", name="date_time"
    )
    values = np.zeros(index.size)
    for day, hour, hours, peak in STORMS:
        start = RECORD_START + pd.Timedelta(days=day, hours=hour + shift_hours)
        inside = (index >= start) & (index < start + hours * HOUR)
        phase = np.linspace(0.0, np.pi, int(inside.sum()))
        values[inside] = np.round(0.3 + scale * peak * np.sin(phase) ** 2 * rng.uniform(0.9, 1.1, phase.size), 3)
    series = pd.Series(values, index=index, name="mm_per_hr")
    dry = np.flatnonzero(values == 0)
    chosen = rng.choice(dry, size=nan_count + drop_count, replace=False)
    series.iloc[chosen[:nan_count]] = np.nan
    return series.drop(series.index[chosen[nan_count:]])


@generates_fixture(name="ids_rain_mrms", seed=5101)
def ids_rain_mrms(rng: np.random.Generator) -> pd.Series:
    """The invented radar rainfall, every 30 minutes, with three readings left out."""
    return _rain(rng, 30, 0.0, 1.0, 0, 3)


@generates_fixture(name="ids_rain_aorc", seed=5102)
def ids_rain_aorc(rng: np.random.Generator) -> pd.Series:
    """The invented gridded-product rainfall, hourly, an hour later and a little lighter."""
    return _rain(rng, 60, 1.0, 0.95, 0, 0)


@generates_fixture(name="ids_rain_ncei", seed=5103)
def ids_rain_ncei(rng: np.random.Generator) -> pd.Series:
    """The invented gauge rainfall on the radar grid, hourly, two hours later and a little heavier, with two readings missing and two left out."""
    return _rain(rng, 60, 2.0, 1.05, 2, 2)


@generates_fixture(name="ids_mrms_bias_corrected", seed=5104)
def ids_mrms_bias_corrected(rng: np.random.Generator) -> pd.Series:
    """The invented bias-corrected radar rainfall (mm per hour), hourly over the record."""
    index = pd.date_range(RECORD_START, periods=RECORD_DAYS * 24, freq="60min", name="date_time")
    values = np.zeros(index.size)
    for day, hour, hours, peak in STORMS:
        start = RECORD_START + pd.Timedelta(days=day, hours=hour)
        inside = (index >= start) & (index < start + hours * HOUR)
        phase = np.linspace(0.0, np.pi, int(inside.sum()))
        values[inside] = np.round(0.3 + 0.9 * peak * np.sin(phase) ** 2, 3)
    return pd.Series(values, index=index, name="mm_per_hr_bias_corrected")


# ---- the water level records of the legacy surge-threshold experiment ----
#
# The legacy script compares an hourly water level record with a finer one resampled to hourly, by selecting surge
# events above a range of thresholds in each and comparing the events and the number of events per year. Both records
# below are invented on the rain record's three years: a small tidal residual plus thirty surges (ten a year, peaks
# from 2.3 to 4.4 ft, at least two a year above 3.5 ft, one wide enough to exceed the 48-hour cap on an event).

BASE_SURGES = (  # (day of year, hour, peak ft, width in hours)
    (9, 6, 3.9, 3.0),
    (36, 12, 2.6, 2.5),
    (70, 0, 4.3, 3.0),
    (105, 18, 3.1, 2.5),
    (140, 4, 2.3, 2.0),
    (176, 9, 3.6, 3.0),
    (211, 20, 2.9, 25.0),
    (250, 2, 3.3, 2.5),
    (298, 14, 2.45, 2.0),
    (344, 8, 4.0, 3.0),
)
SURGES = tuple(
    (day + offset, hour, round(peak + shift, 2), width)
    for offset, shift in ((0, 0.0), (365, -0.15), (731, 0.2))
    for day, hour, peak, width in BASE_SURGES
)


def _surge(rng: np.random.Generator, step_minutes: int) -> pd.DataFrame:
    """The surge and water level in feet on a grid of `step_minutes`: a small tidal residual plus the thirty surges."""
    index = pd.date_range(
        RECORD_START, periods=RECORD_DAYS * 24 * 60 // step_minutes, freq=f"{step_minutes}min", name="date_time"
    )
    hours = (index - RECORD_START) / HOUR
    surge = 0.3 + 0.2 * np.sin(2 * np.pi * hours / 12.42) + rng.normal(0.0, 0.03, index.size)
    for day, hour, peak, width in SURGES:
        centre = day * 24 + hour
        surge = surge + (peak - 0.3) * np.exp(-0.5 * ((hours - centre) / width) ** 2)
    tide = 1.5 * np.sin(2 * np.pi * hours / 12.42 + 1.0)
    return pd.DataFrame({"surge_ft": np.round(surge, 3), "waterlevel_ft": np.round(surge + tide, 3)}, index=index)


@generates_fixture(name="ids_wlevels_6min", seed=5105)
def ids_wlevels_6min(rng: np.random.Generator) -> pd.DataFrame:
    """The invented water level record of the finer gauge product, every 30 minutes."""
    return _surge(rng, 30)


@generates_fixture(name="ids_wlevels_hrly", seed=5106)
def ids_wlevels_hrly(rng: np.random.Generator) -> pd.DataFrame:
    """The invented hourly water level record."""
    return _surge(rng, 60)


# ---- the combined event summaries the legacy event-analysis cells read ----
#
# The legacy event analysis reads the table of combined (rain and surge) event summaries that the pairing step wrote,
# keeps the surge-only, rain-only and compound events apart, computes the correlations of the events' plotting
# positions, and picks the strongest relationships between a rain statistic and a surge statistic by name. None of the
# real data may be published, so the table is invented in that form: 18 events of each type, the flag and source
# columns the cells drop, and twelve numeric statistics named as the cells name them (a rain depth and intensities, a
# surge peak and mean, the delays of the surge peak after a rain peak, two lags that the correlation listing skips as
# start or end times, a time-step column it skips, and one column that was never measured, which the plotting-position
# step skips). Every statistic is non-missing, because the correlation step drops any event with a missing value.

SUMMARY_TYPES = (("surge", False, True), ("rain", True, False), ("compound", True, True))
EVENTS_PER_TYPE = 18


@generates_fixture(name="ids_combined_summaries", seed=5201)
def ids_combined_summaries(rng: np.random.Generator) -> pd.DataFrame:
    """The combined event summaries: 18 events of each of three types, with the flag, source and statistic columns."""
    parts = []
    for kind, rain_flag, surge_flag in SUMMARY_TYPES:
        shared = rng.normal(size=EVENTS_PER_TYPE)
        rain_depth = np.round(55.0 + 18.0 * shared + 6.0 * rng.normal(size=EVENTS_PER_TYPE), 2)
        intensity_1 = np.round(14.0 + 4.5 * shared + 2.0 * rng.normal(size=EVENTS_PER_TYPE), 2)
        intensity_4 = np.round(9.0 + 3.0 * shared + 1.5 * rng.normal(size=EVENTS_PER_TYPE), 2)
        surge_peak = np.round(3.2 + 0.5 * shared + 0.35 * rng.normal(size=EVENTS_PER_TYPE), 3)
        frame = pd.DataFrame(
            {
                "event_type": kind,
                "compound": kind == "compound",
                "rain_event": rain_flag,
                "surge_event": surge_flag,
                "rain_data_source": rng.choice(["mrms", "aorc", "ncei_hrly"], size=EVENTS_PER_TYPE),
                "wlevel_data_source": "hourly_surge_data",
                "surge_peak_ft": surge_peak,
                "max_16hr_mean_surge": np.round(0.6 * surge_peak + 0.2 * rng.normal(size=EVENTS_PER_TYPE), 3),
                "max_mean_1hrsurge_peak_after_4hrrain_peak_h": np.round(
                    5.0 - 1.4 * shared + 1.2 * rng.normal(size=EVENTS_PER_TYPE), 2
                ),
                "max_mean_1hrsurge_peak_after_16hrrain_peak_h": np.round(
                    9.0 + 1.1 * shared + 2.5 * rng.normal(size=EVENTS_PER_TYPE), 2
                ),
                "max_mean_16hrsurge_peak_after_24hrrain_peak_h": np.round(
                    12.0 - 0.9 * shared + 3.0 * rng.normal(size=EVENTS_PER_TYPE), 2
                ),
                "precip_depth_mm": rain_depth,
                "max_1hr_mean_mm_per_hr": intensity_1,
                "max_4hr_mean_mm_per_hr": intensity_4,
                "rain_start_lag_h": np.round(rng.normal(2.0, 1.0, size=EVENTS_PER_TYPE), 2),
                "surge_end_lag_h": np.round(rng.normal(3.0, 1.5, size=EVENTS_PER_TYPE), 2),
                "max_4hr_mean_mm_per_hr_tstep_h": np.round(rng.uniform(0.0, 12.0, size=EVENTS_PER_TYPE), 2),
                "never_measured_mm": np.nan,
            }
        )
        parts.append(frame)
    return pd.concat(parts, ignore_index=True)


# ---- the event statistics and fit-performance tables the legacy fit-selection cells read ----
#
# The legacy marginal-selection script fits every candidate distribution, with and without transformations, to each
# event statistic of each event type, and keeps a table of every fit's scores. Its plotting cells read that table back
# and refit and draw each fit that the scores admit, by a file name that states the fit. None of the real data may be
# published, so each input is invented: for each event type a table of 30 events with four statistics, and a table of
# candidate fits (the transformation settings, the scores and the distribution) whose rows reproduce the file names the
# legacy cells select by, so that the selection cells draw some of them and miss others. Every row names a fit that
# the legacy fitting accepts on the invented statistics (a fixed location is never asked of a gamma or a lognormal
# fit, which the installed scipy refuses).

MARGINAL_KEYS = ("surge_events", "rain_events", "compound_events")
MARGINAL_STATISTICS = {
    "surge_events": (
        "surge_peak_ft",
        "precip_depth_mm",
        "max_mean_1hrsurge_peak_after_4hrrain_peak_h",
        "max_4hr_mean_mm_per_hr",
    ),
    "rain_events": (
        "surge_peak_ft",
        "precip_depth_mm",
        "max_mean_1hrsurge_peak_after_16hrrain_peak_h",
        "max_4hr_mean_mm_per_hr",
    ),
    "compound_events": (
        "surge_peak_ft",
        "precip_depth_mm",
        "max_mean_16hrsurge_peak_after_24hrrain_peak_h",
        "max_4hr_mean_mm_per_hr",
    ),
}
MARGINAL_EVENTS = 30


def _marginal_summary(rng: np.random.Generator, key: str) -> pd.DataFrame:
    """Thirty events with the four statistics of `key`: positive, right-skewed, except the signed delay."""
    names = MARGINAL_STATISTICS[key]
    return pd.DataFrame(
        {
            names[0]: np.round(2.6 + rng.gamma(4.0, 0.35, MARGINAL_EVENTS), 3),
            names[1]: np.round(35.0 + rng.gamma(3.0, 14.0, MARGINAL_EVENTS), 2),
            names[2]: np.round(rng.gumbel(-6.0, 4.0, MARGINAL_EVENTS), 2),
            names[3]: np.round(4.0 + rng.gamma(3.0, 3.0, MARGINAL_EVENTS), 2),
        }
    )


@generates_fixture(name="ids_marg_summary_surge", seed=5301)
def ids_marg_summary_surge(rng: np.random.Generator) -> pd.DataFrame:
    """The statistics of the surge-only events."""
    return _marginal_summary(rng, "surge_events")


@generates_fixture(name="ids_marg_summary_rain", seed=5302)
def ids_marg_summary_rain(rng: np.random.Generator) -> pd.DataFrame:
    """The statistics of the rain-only events."""
    return _marginal_summary(rng, "rain_events")


@generates_fixture(name="ids_marg_summary_compound", seed=5303)
def ids_marg_summary_compound(rng: np.random.Generator) -> pd.DataFrame:
    """The statistics of the compound events."""
    return _marginal_summary(rng, "compound_events")


PERF_COLUMNS = [
    "data",
    "distribution",
    "n_params",
    "fix_loc_to_min",
    "upper_bound",
    "scalar_shift",
    "normalize",
    "boxcox",
    "cvm_pval",
    "ks_pval",
    "aic",
]
NO = float("nan")
PERF_ROWS = {  # (data index, distribution, n_params, fixed loc, upper bound, shift, normalize, boxcox, cvm p, ks p, aic)
    "surge_events": [
        (0, "gamma", 3, False, NO, 1.0, False, False, 0.89, 0.71, 31.2),
        (0, "weibull_min", 3, True, NO, 1.0, False, False, 0.86, 0.64, 30.1),
        (0, "weibull_min", 3, True, NO, 0.0, False, False, 0.86, 0.66, -4.8),
        (0, "norm", 2, False, NO, 0.0, False, False, 0.12, 0.8, 40.0),
        (1, "genpareto", 3, False, 220.0, 0.0, False, False, 0.72, 0.5, 280.4),
        (1, "genpareto", 3, True, 220.0, 0.0, False, False, 0.38, 0.4, 301.9),
        (2, "gumbel_l", 2, False, NO, 0.0, False, False, 0.85, 0.77, 190.3),
        (3, "genpareto", 3, True, NO, 0.0, False, False, 0.91, 0.82, 160.7),
        (3, "genpareto", 3, False, NO, 0.0, False, False, 0.76, 0.6, 158.2),
        (3, "weibull_min", 3, True, NO, 0.0, False, False, 0.51, 0.45, 166.0),
    ],
    "rain_events": [
        (0, "exponpow", 3, False, 9.0, 0.0, False, False, 0.96, 0.9, 12.3),
        (1, "weibull_min", 3, True, NO, 0.0, False, False, 0.96, 0.85, 270.2),
        (2, "t", 3, False, NO, 0.0, False, False, 0.98, 0.93, 180.8),
        (3, "chi", 3, False, NO, 0.0, False, False, 0.96, 0.88, 150.9),
    ],
    "compound_events": [
        (0, "weibull_min", 3, True, NO, 0.0, False, True, 0.96, 0.8, -2.2),
        (0, "weibull_min", 3, True, NO, 1.0, False, True, 0.94, 0.7, -1.9),
        (0, "weibull_min", 3, True, NO, 0.0, False, False, 0.94, 0.78, 25.3),
        (1, "genpareto", 3, False, NO, 0.0, False, False, 0.86, 0.65, 285.6),
        (2, "weibull_min", 3, False, NO, 0.0, False, False, 0.94, 0.9, 175.1),
        (2, "genextreme", 3, False, NO, 0.0, False, False, 0.93, 0.87, 176.4),
        (3, "t", 3, False, NO, 0.0, False, True, 0.97, 0.9, 120.2),
        (3, "lognorm", 3, False, NO, 0.0, False, False, 0.75, 0.55, 155.3),
        (3, "gamma", 3, False, NO, 0.0, False, False, 0.65, 0.5, 156.1),
        (3, "weibull_min", 3, True, NO, 0.0, False, False, 0.5, 0.46, 165.4),
        (3, "norm", 2, False, NO, 0.0, True, False, 0.1, 0.9, 180.0),
    ],
}


def _performance(key: str) -> pd.DataFrame:
    """The candidate-fit table of `key`: the rows above with the statistic names filled in."""
    names = MARGINAL_STATISTICS[key]
    rows = [(names[row[0]], *row[1:]) for row in PERF_ROWS[key]]
    return pd.DataFrame(rows, columns=PERF_COLUMNS)


@generates_fixture(name="ids_marg_perf_surge", seed=5304)
def ids_marg_perf_surge(rng: np.random.Generator) -> pd.DataFrame:
    """The candidate fits of the surge-only events."""
    return _performance("surge_events")


@generates_fixture(name="ids_marg_perf_rain", seed=5305)
def ids_marg_perf_rain(rng: np.random.Generator) -> pd.DataFrame:
    """The candidate fits of the rain-only events."""
    return _performance("rain_events")


@generates_fixture(name="ids_marg_perf_compound", seed=5306)
def ids_marg_perf_compound(rng: np.random.Generator) -> pd.DataFrame:
    """The candidate fits of the compound events."""
    return _performance("compound_events")


# ---- the vine-copula fit tables the legacy copula cells read ----
#
# The legacy copula script fits a vine copula to each event type's fitted marginal probabilities under many control
# settings, tabulates every setting's test p-values, keeps the settings whose p-values all lie between 0.05 and 0.95,
# and redraws the goodness-of-fit figures of the hand-picked settings. None of the real data may be published, so each
# input is invented: for each event type the long table of fitted probabilities of three statistics (one value
# missing in the rain table), one vine copula fitted to invented probabilities and saved as its json text, the table
# of 38 control settings the figure cell indexes (its saved-fit column left for the lift to fill), a table of 12
# settings with the p-value columns the selection cell filters on, and the one seed of the vine library's draws.

VINE_COLUMNS = ["rain_depth_mm", "surge_peak_ft", "duration_hr"]


def _vine_probabilities(rng: np.random.Generator, rows: int) -> np.ndarray:
    """`rows` x 3 probabilities from correlated normals: columns 0-1 strongly, 1-2 moderately."""
    z = rng.normal(size=(rows, 3))
    z[:, 1] += 1.6 * z[:, 0]
    z[:, 2] -= 0.9 * z[:, 1]
    z = (z - z.mean(axis=0)) / z.std(axis=0)
    return 0.5 * (1.0 + np.vectorize(math.erf)(z / math.sqrt(2.0)))


def _long_table(rng: np.random.Generator, missing: bool) -> pd.DataFrame:
    """The long table the legacy marginal-fitting step saves: observation id, statistic name and fitted probability, 20 observations."""
    frame = pd.DataFrame(
        _vine_probabilities(rng, 20), columns=VINE_COLUMNS, index=pd.Index(np.arange(20), name="observation_id")
    )
    long = frame.reset_index().melt(id_vars="observation_id", var_name="data", value_name="cdf_fit")
    if missing:
        long.loc[(long["observation_id"] == 3) & (long["data"] == "duration_hr"), "cdf_fit"] = np.nan
    return long[["observation_id", "data", "cdf_fit"]].reset_index(drop=True)


@generates_fixture(name="ids_cdf_long_surge", seed=5401)
def ids_cdf_long_surge(rng: np.random.Generator) -> pd.DataFrame:
    """The fitted probabilities of the surge-only events."""
    return _long_table(rng, False)


@generates_fixture(name="ids_cdf_long_rain", seed=5402)
def ids_cdf_long_rain(rng: np.random.Generator) -> pd.DataFrame:
    """The fitted probabilities of the rain-only events, one value missing."""
    return _long_table(rng, True)


@generates_fixture(name="ids_cdf_long_compound", seed=5403)
def ids_cdf_long_compound(rng: np.random.Generator) -> pd.DataFrame:
    """The fitted probabilities of the compound events."""
    return _long_table(rng, False)


@generates_fixture(name="ids_vinecop_json", seed=5404)
def ids_vinecop_json(rng: np.random.Generator) -> np.ndarray:
    """A vine copula fitted to invented probabilities, as its json text in a one-element array."""
    controls = pv.FitControlsVinecop()
    controls.selection_criterion = "loglik"
    controls.parametric_method = "itau"
    controls.tree_criterion = "tau"
    controls.threshold = 0.01
    controls.select_trunc_lvl = False
    fitted = pv.Vinecop.from_data(np.asfortranarray(_vine_probabilities(rng, 30)), controls=controls)
    return np.array([fitted.to_json()])


@generates_fixture(name="ids_sensitivity_results", seed=5405)
def ids_sensitivity_results(rng: np.random.Generator) -> pd.DataFrame:
    """The table of 38 control settings the figure cell indexes: the saved-fit path (left empty for the lift to fill) and a p-value."""
    return pd.DataFrame(
        {"f_vinecop_params": [""] * 38, "pvalue": np.round(rng.uniform(0.05, 0.95, 38), 3)},
        index=pd.Index(np.arange(38), name="control_idx"),
    )


@generates_fixture(name="ids_sensitivity_pvalues", seed=5406)
def ids_sensitivity_pvalues(rng: np.random.Generator) -> pd.DataFrame:
    """Twelve settings with their distance-test p-value and the Kendall-tau p-value of each of three variable pairs; some lie outside 0.05 to 0.95."""
    columns = {
        "control_idx": np.arange(12),
        "f_vinecop_params": [f"fit_{index}.json" for index in range(12)],
        "pvalue": np.round(rng.uniform(0.0, 1.0, 12), 3),
    }
    for pair in ("rain_depth_mm_vs_surge_peak_ft", "rain_depth_mm_vs_duration_hr", "surge_peak_ft_vs_duration_hr"):
        columns[f"{pair}_kendall_tau_pvalue"] = np.round(rng.uniform(0.0, 1.0, 12), 3)
    columns["pvalue"][:5] = [0.5, 0.3, 0.7, 0.6, 0.4]
    for name in list(columns)[3:]:
        columns[name][:5] = [0.5, 0.35, 0.6, 0.45, 0.55]
    return pd.DataFrame(columns)


@generates_fixture(name="seed", seed=5407)
def seed(rng: np.random.Generator) -> int:
    """The one seed of every unit's vine-library draws: the lift passes it to each `cdf` call and builds from it the generator that supplies each `simulate` call's seed, and the equivalence test hands the same value to the registered callable as `inputs["seed"]`."""
    return int(rng.integers(1, 2**31 - 1))


# ---- the correlation listing the legacy relationship-selection helpers read ----
#
# The legacy helpers that choose which relationships to plot read a listing of variable pairs with their correlation
# and its absolute value. None of the real data may be published, so the listing is invented: twelve pairs of a surge
# or delay statistic with a rain statistic, named as the legacy cells name them, with correlations of both signs.

LISTING_PAIRS = (
    ("surge_peak_ft", "max_4hr_mean_mm_per_hr", 0.62),
    ("surge_peak_ft", "precip_depth_mm", 0.55),
    ("surge_peak_ft", "max_1hr_mean_mm_per_hr", 0.48),
    ("max_16hr_mean_surge", "max_4hr_mean_mm_per_hr", 0.41),
    ("max_16hr_mean_surge", "precip_depth_mm", -0.37),
    ("max_mean_1hrsurge_peak_after_4hrrain_peak_h", "max_4hr_mean_mm_per_hr", -0.58),
    ("max_mean_1hrsurge_peak_after_4hrrain_peak_h", "precip_depth_mm", -0.44),
    ("max_mean_1hrsurge_peak_after_16hrrain_peak_h", "max_4hr_mean_mm_per_hr", 0.29),
    ("max_mean_1hrsurge_peak_after_16hrrain_peak_h", "precip_depth_mm", 0.33),
    ("max_mean_16hrsurge_peak_after_24hrrain_peak_h", "precip_depth_mm", -0.21),
    ("surge_peak_ft", "rain_start_lag_h", 0.12),
    ("max_16hr_mean_surge", "max_1hr_mean_mm_per_hr", 0.08),
)


@generates_fixture(name="ids_corr_listing", seed=5501)
def ids_corr_listing(rng: np.random.Generator) -> pd.DataFrame:
    """The listing of variable pairs: `var1` (surge or delay statistic), `var2` (rain statistic), the correlation and its absolute value."""
    frame = pd.DataFrame(LISTING_PAIRS, columns=["var1", "var2", "corr"])
    frame["abs_corr"] = frame["corr"].abs()
    return frame
