"""Synthetic inputs for the equivalence tests of the legacy storm-rescaling units owned by timeseries-synthesis.

The legacy rescaling script reads, for each event type, the long table of fitted probabilities that the marginal-fitting
step saved (one row per observation and modelled statistic), to learn which statistics the type models and which of them
is its rain-intensity statistic. None of the real data may be published, so each table is invented in that form:
the surge-only type models one statistic, the rain-only type two, and the compound type four (a surge peak, a rain
depth, the largest four-hour mean rain intensity and one delay of the surge peak after a rain peak). A generator
opens no file and reads nothing at module level, which the fixture machinery enforces while it runs.
"""

import numpy as np
import pandas as pd

from tests.correspondence.fixtures import generates_fixture

MODELLED = {
    "surge": ("surge_peak_ft",),
    "rain": ("precip_depth_mm", "max_4hr_mean_mm_per_hr"),
    "compound": (
        "surge_peak_ft",
        "precip_depth_mm",
        "max_4hr_mean_mm_per_hr",
        "max_mean_16hrsurge_peak_after_24hrrain_peak_h",
    ),
}
OBSERVATIONS = 12


def _long_table(rng: np.random.Generator, kind: str) -> pd.DataFrame:
    """The long table of fitted probabilities of `kind`: observation id, statistic name and probability, with one value missing."""
    rows = [
        (observation, name, float(rng.uniform(0.02, 0.98)))
        for name in MODELLED[kind]
        for observation in range(1, OBSERVATIONS + 1)
    ]
    table = pd.DataFrame(rows, columns=["observation_id", "data", "cdf_fit"])
    table.loc[(table["observation_id"] == 3) & (table["data"] == MODELLED[kind][0]), "cdf_fit"] = np.nan
    return table


@generates_fixture(name="tss_cdf_long_surge", seed=6001)
def tss_cdf_long_surge(rng: np.random.Generator) -> pd.DataFrame:
    """The fitted probabilities of the surge-only events."""
    return _long_table(rng, "surge")


@generates_fixture(name="tss_cdf_long_rain", seed=6002)
def tss_cdf_long_rain(rng: np.random.Generator) -> pd.DataFrame:
    """The fitted probabilities of the rain-only events."""
    return _long_table(rng, "rain")


@generates_fixture(name="tss_cdf_long_compound", seed=6003)
def tss_cdf_long_compound(rng: np.random.Generator) -> pd.DataFrame:
    """The fitted probabilities of the compound events."""
    return _long_table(rng, "compound")


# ---- the observed and simulated records the storm-rescaling cells read ----
#
# The legacy rescaling cell pairs every simulated event with one of its nearest observed events, copies that event's
# observed time series and rescales it to the simulated event's statistics, then writes a time series and a summary row
# per simulated event. Its consistency checks require the observed series' peak and depth to equal the summary values,
# so every observed event below is the same short storm at a different date: five hourly steps, a surge rising to 3.0
# ft, and a rainfall intensity of 8 mm per hour (a 40 mm depth). Two simulated events per event type are invented: one
# asks for the same intensity and depth (the rescaling branch) and one for a depth a single step would exceed (the
# zero-rainfall branch).

OBS_START = pd.Timestamp("2020-01-01 00:00")
OBS_HOURS = 96
SURGE_SHAPE = (0.5, 1.5, 3.0, 1.5, 0.5)
RAIN_INTENSITY = 8.0
EVENT_STARTS = (2, 26, 50, 74)  # hours after OBS_START; each event spans five hourly steps


@generates_fixture(name="tss_obs_events", seed=6010)
def tss_obs_events(rng: np.random.Generator) -> pd.DataFrame:
    """The observed events, one row per event: type, start, end, surge peak, rain depth and each source."""
    index = pd.Index(np.arange(1, len(EVENT_STARTS) + 1), name="event_id")
    starts = [OBS_START + pd.Timedelta(hours=hour) for hour in EVENT_STARTS]
    return pd.DataFrame(
        {
            "event_type": ["compound"] * len(starts),
            "event_start": starts,
            "event_end": [start + pd.Timedelta(hours=4) for start in starts],
            "surge_peak_ft": [max(SURGE_SHAPE)] * len(starts),
            "precip_depth_mm": [RAIN_INTENSITY * len(SURGE_SHAPE)] * len(starts),
            "max_4hr_mean_mm_per_hr": [RAIN_INTENSITY] * len(starts),
            "rain_data_source": ["mrms"] * len(starts),
            "wlevel_data_source": ["6min_surge_data"] * len(starts),
        },
        index=index,
    )


@generates_fixture(name="tss_obs_series_surge", seed=6011)
def tss_obs_series_surge(rng: np.random.Generator) -> pd.DataFrame:
    """The observed surge series, hourly over the record: the storm shape in every event window, zero elsewhere."""
    index = pd.date_range(OBS_START, periods=OBS_HOURS, freq="1h", name="date_time")
    values = np.zeros(index.size)
    for hour in EVENT_STARTS:
        values[hour : hour + len(SURGE_SHAPE)] = SURGE_SHAPE
    return pd.DataFrame({"surge_ft": values}, index=index)


@generates_fixture(name="tss_obs_series_mm", seed=6012)
def tss_obs_series_mm(rng: np.random.Generator) -> pd.DataFrame:
    """The observed rainfall depth per step, hourly over the record: the event intensity in every window, zero elsewhere."""
    index = pd.date_range(OBS_START, periods=OBS_HOURS, freq="1h", name="date_time")
    values = np.zeros(index.size)
    for hour in EVENT_STARTS:
        values[hour : hour + len(SURGE_SHAPE)] = RAIN_INTENSITY
    return pd.DataFrame({"mm": values}, index=index)


@generates_fixture(name="tss_obs_mrms", seed=6013)
def tss_obs_mrms(rng: np.random.Generator) -> pd.DataFrame:
    """The observed radar rainfall, hourly over the record, as the legacy table holds it: a `rain_mean` basin average and one grid cell.

    The cell renames `rain_mean` to `mm_per_hr` and recomputes that column as the mean of the grid cells, so the table
    carries at least one grid-cell column as the legacy one does.
    """
    index = pd.date_range(OBS_START, periods=OBS_HOURS, freq="1h", name="date_time")
    values = np.zeros(index.size)
    for hour in EVENT_STARTS:
        values[hour : hour + len(SURGE_SHAPE)] = RAIN_INTENSITY
    return pd.DataFrame({"rain_mean": values, "rain_cell_1": values.copy()}, index=index)


@generates_fixture(name="tss_sim_summaries", seed=6014)
def tss_sim_summaries(rng: np.random.Generator) -> pd.DataFrame:
    """Two simulated events of the compound type: the statistics to rescale to, and the flag columns the cell drops.

    The compound type is the one that models both a surge and a rain statistic, which the cell's two neighbour
    selections each require.
    """
    return pd.DataFrame(
        {
            "rain_event": [True, True],
            "surge_event": [False, False],
            "compound_event": [False, False],
            "copula_source": ["rain", "rain"],
            "year": [1, 2],
            "event_id": [1, 1],
            "surge_peak_ft": [3.0, 3.0],
            "precip_depth_mm": [40.0, 0.5],
            "max_4hr_mean_mm_per_hr": [8.0, 8.0],
        }
    )


# ---- the inputs of the legacy time-series finalising cell ----
#
# The cell that finalises the simulated time series reads, for each event type, the summary table and the time series the
# rescaling cell wrote, takes the number of events to simulate from a per-year count table, extends and buffers each
# event, adds a tide selected at random from the observed water-level record, checks that the shifted event still
# matches its statistics, and writes the final summaries and series. Its guards are exact equalities, so the inputs
# below are derived in dependency order rather than fitted one guard at a time:
#   - one short event (eleven hourly steps) whose surge and rainfall already satisfy the buffering distances, so the
#     buffering step returns the frame unchanged and never enters the missing-value branch: the surge peaks five steps
#     from either end, rain first falls four steps in, and the last significant rain falls four steps before the end;
#   - the observed water-level record carries exactly that surge shape at the event's date, so the guard comparing the
#     built surge with the observed record shifted by the event's surge adjustment holds by construction (the
#     adjustment is zero, the simulated and observed peaks being equal);
#   - the rescaling cell's summary row is generated FROM those series, so the depth, the four-hour mean intensity and
#     the surge peak in the row are the ones the series produce, and the target columns are the same values.
# The tide is a small sine so that it never moves the water-level peak far enough to change the buffered index.

FINAL_START = pd.Timestamp("2018-01-01 00:00")
FINAL_YEARS = 3
EVENT_AT = pd.Timestamp("2020-06-01 00:00")
FINAL_SURGE = (0.0, 0.0, 0.0, 1.0, 2.0, 3.0, 2.0, 1.0, 0.0, 0.0, 0.0)
FINAL_MM = (0.0, 0.0, 0.0, 0.0, 5.0, 20.0, 5.0, 0.0, 0.0, 0.0, 0.0)
TIDE_AMPLITUDE = 0.2


def _final_index() -> pd.DatetimeIndex:
    """The hourly index of the observed record, one year short of a leap year each."""
    return pd.date_range(FINAL_START, periods=FINAL_YEARS * 8760, freq="1h", name="date_time")


@generates_fixture(name="tss_wlevels", seed=6020)
def tss_wlevels(rng: np.random.Generator) -> pd.DataFrame:
    """The observed water levels, hourly over three years: the event's surge shape at its date, a small tide throughout."""
    index = _final_index()
    hours = (index - FINAL_START) / pd.Timedelta(hours=1)
    tide = np.round(TIDE_AMPLITUDE * np.sin(2 * np.pi * hours / 12.42), 4)
    surge = np.zeros(index.size)
    at = int((EVENT_AT - FINAL_START) / pd.Timedelta(hours=1))
    surge[at : at + len(FINAL_SURGE)] = FINAL_SURGE
    return pd.DataFrame({"surge_ft": surge, "tide_ft": tide, "waterlevel_ft": np.round(surge + tide, 4)}, index=index)


@generates_fixture(name="tss_event_counts", seed=6021)
def tss_event_counts(rng: np.random.Generator) -> pd.DataFrame:
    """The simulated event counts per year: one year holding one compound event."""
    return pd.DataFrame({"any": [1], "rain": [0], "surge": [0], "compound": [1]}, index=pd.Index([1], name="year"))


@generates_fixture(name="tss_sim_tseries", seed=6022)
def tss_sim_tseries(rng: np.random.Generator) -> pd.DataFrame:
    """The simulated event's time series as the rescaling cell wrote it: one event, one attempt, eleven hourly steps."""
    steps = pd.to_timedelta(np.arange(len(FINAL_SURGE)), unit="h")
    index = pd.MultiIndex.from_product([[0], [0], steps], names=["sim_event_id", "rescaling_attempt", "timestep"])
    return pd.DataFrame({"surge_ft": FINAL_SURGE, "mm_per_hr": FINAL_MM}, index=index)


@generates_fixture(name="tss_obs_summaries", seed=6023)
def tss_obs_summaries(rng: np.random.Generator) -> pd.DataFrame:
    """The observed compound events, one row: eleven hours long, so the event is not longer than the longest observed one."""
    return pd.DataFrame(
        {"event_start": [EVENT_AT], "event_end": [EVENT_AT + pd.Timedelta(hours=len(FINAL_SURGE) - 1)]},
        index=pd.Index([7], name="event_id"),
    )


@generates_fixture(name="tss_rescaled_summaries", seed=6024)
def tss_rescaled_summaries(rng: np.random.Generator) -> pd.DataFrame:
    """The rescaling cell's summary table for the one compound event, its statistics computed from the event's series."""
    mm = pd.Series(FINAL_MM, dtype=float, index=pd.date_range(EVENT_AT, periods=len(FINAL_MM), freq="1h"))
    depth = float((mm * 1.0).sum())  # one hour per step
    intensity = float(mm.rolling("240min").mean().max())
    peak = float(max(FINAL_SURGE))
    index = pd.MultiIndex.from_tuples([(0, 0)], names=["sim_event_id", "rescaling_attempt"])
    return pd.DataFrame(
        {
            "year": [1],
            "event_id": [1],
            "compound_event": [True],
            "copula_source": ["compound"],
            "surge_peak_ft_target": [peak],
            "precip_depth_mm_target": [depth],
            "max_4hr_mean_mm_per_hr_target": [intensity],
            "surge_peak_after_rain_peak_h_target": [0.0],
            "surge_peak_ft": [peak],
            "precip_depth_mm": [depth],
            "max_4hr_mean_mm_per_hr": [intensity],
            "sim_surge_duration": [pd.Timedelta(hours=len(FINAL_SURGE) - 1)],
            "sim_rain_duration": [pd.Timedelta(hours=len(FINAL_SURGE) - 1)],
            "surge_rescaling_problems": [np.nan],
            "rain_rescaling_problems": [np.nan],
            "obs_event_start_surge": [EVENT_AT],
            "obs_event_end_surge": [EVENT_AT + pd.Timedelta(hours=len(FINAL_SURGE) - 1)],
            "obs_event_start_rain": [EVENT_AT],
            "obs_event_end_rain": [EVENT_AT + pd.Timedelta(hours=len(FINAL_SURGE) - 1)],
            "obs_surge_peak_ft": [peak],
            "obs_precip_depth_mm": [depth],
            "obs_rain_intensity_stat": [intensity],
        },
        index=index,
    )


# ---- the inputs of the rescaling helpers of `_utils.py` ----
#
# The eight helpers the rescaling cell calls are tested one by one. Their inputs are invented in the form each reads:
# a rainfall time series with a basin average and one grid cell, a surge time series, an observed water-level record
# with its tide, observed event summaries, a small table of simulated event statistics, and a table of neighbour
# choices. The numbers are chosen so that the helpers' consistency checks hold: the observed rainfall depth is the
# hourly sum of its intensities, and every statistic a target names is the value the corresponding series produces.

HELPER_START = pd.Timestamp("2021-03-01 00:00")
HELPER_MM = (0.0, 2.0, 8.0, 20.0, 8.0, 2.0, 0.0, 0.0)
HELPER_CELL = (0.93, 1.86, 2.79, 3.72, 2.79, 1.86, 0.93, 0.93)
HELPER_SURGE = (0.0, 0.2, 1.0, 2.5, 1.0, 0.2, 0.0, 0.0)
HELPER_TIDE = (0.3, 0.1, -0.2, -0.3, -0.1, 0.2, 0.3, 0.1)


@generates_fixture(name="tss_helper_rain", seed=6030)
def tss_helper_rain(rng: np.random.Generator) -> pd.DataFrame:
    """The observed rainfall of one event, hourly: a basin average and one grid cell, near zero at both ends.

    The grid cell is a flatter storm than the basin average and its own depth, about 15.8 mm, is not the basin
    average's 40 mm: the basin average in the legacy table is computed from the whole grid, not from this column, so
    the average the rescaling loop recomputes from the grid cells is not the column it started from. That is the
    shape the real run met, and it is what makes the loop take the two intensity-mismatch `continue` branches before
    it settles. The cell's total is also what the declaration's second case needs: a target depth of 20 mm is close
    to this series' own total, so that case's depth rescaling keeps an hourly step and the case reaches its own end
    instead of rescaling again.
    """
    index = pd.date_range(HELPER_START, periods=len(HELPER_MM), freq="1h", name="date_time")
    return pd.DataFrame({"mm_per_hr": HELPER_MM, "cell_1": HELPER_CELL}, index=index)


@generates_fixture(name="tss_helper_wlevels", seed=6031)
def tss_helper_wlevels(rng: np.random.Generator) -> pd.DataFrame:
    """The observed water levels of the same event, hourly: surge, tide and their sum."""
    index = pd.date_range(HELPER_START, periods=len(HELPER_SURGE), freq="1h", name="date_time")
    tide = np.asarray(HELPER_TIDE)
    return pd.DataFrame(
        {"surge_ft": HELPER_SURGE, "tide_ft": tide, "waterlevel_ft": np.round(np.asarray(HELPER_SURGE) + tide, 4)},
        index=index,
    )


@generates_fixture(name="tss_helper_events", seed=6032)
def tss_helper_events(rng: np.random.Generator) -> pd.DataFrame:
    """Two observed events: start, end, peak surge, rain depth and source, as the helpers index them."""
    index = pd.Index([11, 12], name="event_id")
    return pd.DataFrame(
        {
            "event_type": ["compound", "compound"],
            "event_start": [HELPER_START, HELPER_START + pd.Timedelta(days=1)],
            "event_end": [HELPER_START + pd.Timedelta(hours=7), HELPER_START + pd.Timedelta(days=1, hours=7)],
            "surge_peak_ft": [max(HELPER_SURGE), max(HELPER_SURGE)],
            "precip_depth_mm": [sum(HELPER_MM), sum(HELPER_MM)],
            "max_4hr_mean_mm_per_hr": [float(np.mean(HELPER_MM[2:6])), float(np.mean(HELPER_MM[2:6]))],
            "rain_data_source": ["mrms", "mrms"],
            "wlevel_data_source": ["6min_surge_data", "6min_surge_data"],
        },
        index=index,
    )


@generates_fixture(name="tss_helper_sim", seed=6033)
def tss_helper_sim(rng: np.random.Generator) -> pd.DataFrame:
    """Three simulated events with their targets: two of each modelled statistic, the depth and the intensity."""
    return pd.DataFrame(
        {
            "surge_peak_ft": [2.5, 2.0, 3.0],
            "precip_depth_mm": [40.0, 20.0, 60.0],
            "max_4hr_mean_mm_per_hr": [float(np.mean(HELPER_MM[2:6])), 5.0, 10.0],
        },
        index=pd.Index([0, 1, 2], name="sim_event_id"),
    )


@generates_fixture(name="tss_helper_neighbors", seed=6034)
def tss_helper_neighbors(rng: np.random.Generator) -> pd.DataFrame:
    """The neighbour choices of three simulated events over two observed ones, as the neighbour function returns them."""
    return pd.DataFrame({"nbr_0": [11, 12, 11], "nbr_1": [12, 11, 12]}, index=pd.Index([0, 1, 2], name="sim_event_id"))


@generates_fixture(name="tss_helper_row", seed=6035)
def tss_helper_row(rng: np.random.Generator) -> pd.DataFrame:
    """One observed event row as the rescaling helpers index it, for the two-sample battery's inputs."""
    index = pd.Index([11], name="event_id")
    return pd.DataFrame(
        {
            "event_type": ["compound"],
            "event_start": [HELPER_START],
            "event_end": [HELPER_START + pd.Timedelta(hours=7)],
            "surge_peak_ft": [max(HELPER_SURGE)],
            "precip_depth_mm": [sum(HELPER_MM)],
            "max_4hr_mean_mm_per_hr": [float(np.mean(HELPER_MM[2:6]))],
        },
        index=index,
    )
