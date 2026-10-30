"""Synthetic inputs for the equivalence tests of the legacy units owned by event-occurrence-modelling.

The legacy event-selection script counts, for each rainfall source, the selected events that start in each rolling
365-day window of the source's record, and keeps only the windows the source covers on every day. Its function reads a
table with one row per calendar day of the record and a flag for whether the source had data that day, and the table of
selected events. None of that real data may be published, so each input below is invented, in the form the legacy
script produces: a day table of three years with one 20-day stretch the source did not cover, and an event table of
twelve events, two of them on the same day, with a start-time column for each of two sources. A generator opens no file
and reads nothing at module level, which the fixture machinery enforces while it runs.
"""

import numpy as np
import pandas as pd

from tests.correspondence.fixtures import generates_fixture

RECORD_START = pd.Timestamp("2015-01-01")
RECORD_DAYS = 3 * 365 + 1  # 2015-01-01 to 2017-12-31 inclusive, with 2016 a leap year
GAP_START = pd.Timestamp("2016-05-10")
GAP_DAYS = 20


@generates_fixture(name="occ_valid_dates", seed=3001)
def occ_valid_dates(rng: np.random.Generator) -> pd.DataFrame:
    """The day table the legacy function reads: one row per day of the record, `valid` false on a 20-day stretch."""
    days = pd.date_range(RECORD_START, periods=RECORD_DAYS)
    valid = ~((days >= GAP_START) & (days < GAP_START + pd.Timedelta(days=GAP_DAYS)))
    return pd.DataFrame({"valid": valid}, index=days)


def _occurrence_starts(rng: np.random.Generator) -> tuple[pd.Series, pd.Series, np.ndarray]:
    """The twelve invented events' rain start times, surge start times and ids.

    Two events lie in the first year, where no full 365-day window of data exists yet, and one lies near the uncovered
    stretch; the legacy function drops the windows they would count in.
    """
    days = RECORD_START + pd.to_timedelta([20, 130, 200, 400, 401, 475, 520, 610, 700, 765, 880, 1000], unit="D")
    hours = pd.to_timedelta(rng.integers(0, 24, size=days.size), unit="h")
    starts = days + hours
    starts = starts.to_series().reset_index(drop=True)
    starts.iloc[4] = starts.iloc[3].normalize() + pd.Timedelta(hours=23)  # the second event of the day of the first
    surge = starts + pd.to_timedelta(rng.integers(-30, 30, size=starts.size), unit="h")
    return starts, surge, np.arange(1, starts.size + 1)


@generates_fixture(name="occ_rain_events", seed=3002)
def occ_rain_events(rng: np.random.Generator) -> pd.DataFrame:
    """The selected rain events, as the rain event table holds them: an id and a rain start time.

    The legacy function is called once per event table — the rain table with its own start column, and the
    water-level table with its own — so the two are separate inputs here, as they are in the run.
    """
    starts, _surge, ids = _occurrence_starts(rng)
    return pd.DataFrame({"event_id": ids, "rain_event_start": starts})


@generates_fixture(name="occ_surge_events", seed=3002)
def occ_surge_events(rng: np.random.Generator) -> pd.DataFrame:
    """The selected surge events, as the water-level event table holds them: an id and a surge start time."""
    _starts, surge, ids = _occurrence_starts(rng)
    return pd.DataFrame({"event_id": ids, "surge_event_start": surge})


# ---- the classified combined-event summaries the event-count pie charts start from ----

EVENT_TYPES = ("compound", "surge", "rain", "compound", "rain", "surge", "compound", "rain", "compound")


def _pie_summary(source: str, rng, count: int, missing: tuple[int, ...]) -> pd.DataFrame:
    """Combined-event summaries as the legacy analysis holds them after classification, with some missing depths.

    Each row has an `event_type` consistent with its flags, so the legacy chart counts the three types; the rows in
    `missing` have no rain depth, which the legacy chart drops before counting.
    """
    types = EVENT_TYPES[:count]
    starts = pd.to_datetime("2014-03-01") + pd.to_timedelta(np.arange(count) * 41, unit="D")
    frame = pd.DataFrame(
        {
            "event_id": np.arange(count),
            "event_start": starts,
            "event_end": starts + pd.Timedelta(hours=18),
            "compound": [kind == "compound" for kind in types],
            "rain_event": [kind in ("compound", "rain") for kind in types],
            "surge_event": [kind in ("compound", "surge") for kind in types],
            "rain_data_source": source,
            "wlevel_data_source": "6min_surge_data",
            "precip_depth_mm": np.round(rng.uniform(15.0, 110.0, count), 2),
            "event_type": types,
        }
    )
    frame.loc[list(missing), "precip_depth_mm"] = np.nan
    return frame


@generates_fixture(name="pie_mrms_summary", seed=3003)
def pie_mrms_summary(rng: np.random.Generator) -> pd.DataFrame:
    """The radar-paired classified summaries: nine events, one without a rain depth."""
    return _pie_summary("mrms", rng, 9, (4,))


@generates_fixture(name="pie_aorc_summary", seed=3004)
def pie_aorc_summary(rng: np.random.Generator) -> pd.DataFrame:
    """The gridded-product classified summaries: seven events, two without a rain depth."""
    return _pie_summary("aorc", rng, 7, (1, 5))


@generates_fixture(name="pie_ncei_summary", seed=3005)
def pie_ncei_summary(rng: np.random.Generator) -> pd.DataFrame:
    """The gauge-paired classified summaries: eight events, none without a rain depth."""
    return _pie_summary("ncei", rng, 8, ())


# ---- the observed event tables and the simulated occurrences of the legacy simulation script ----

SIM_START = pd.Timestamp("2015-01-01")
SIM_END = pd.Timestamp("2018-09-30")


@generates_fixture(name="sim_valid_dates", seed=3006)
def sim_valid_dates(rng: np.random.Generator) -> pd.DataFrame:
    """The table of days with data: one row per day from 2015-01-01 to 2018-09-30, a value where the source had data.

    The rain column is missing for 20 days in March 2016 and the water-level column for the first five days and for six
    days in July 2017, so the legacy function finds groups with missing days of each kind.
    """
    days = pd.date_range(SIM_START, SIM_END)
    rain = pd.Series("rain", index=days, dtype=object)
    wlevel = pd.Series("wlevel", index=days, dtype=object)
    rain[(days >= "2016-03-01") & (days < "2016-03-21")] = np.nan
    wlevel[days < "2015-01-06"] = np.nan
    wlevel[(days >= "2017-07-10") & (days < "2017-07-16")] = np.nan
    return pd.DataFrame({"all_rain_data": rain, "wlevel_data": wlevel})


@generates_fixture(name="sim_events", seed=3007)
def sim_events(rng: np.random.Generator) -> pd.DataFrame:
    """Thirty observed events of the three types with start times, some falling on days that have no data."""
    days = rng.choice(np.arange(5, 1360), size=30, replace=False)
    types = rng.choice(["rain", "surge", "compound"], size=30, p=[0.5, 0.3, 0.2])
    starts = SIM_START + pd.to_timedelta(days, unit="D") + pd.to_timedelta(rng.integers(0, 24, size=30), unit="h")
    frame = pd.DataFrame({"event_start": starts, "event_type": types})
    frame.loc[0, "event_start"] = pd.Timestamp("2016-03-05 06:00")  # on a day the rain source has no data
    frame.loc[1, "event_start"] = pd.Timestamp("2015-01-03 12:00")  # on a day the water-level source has no data
    return frame.sort_values("event_start").reset_index(drop=True)


@generates_fixture(name="sim_counts", seed=3008)
def sim_counts(rng: np.random.Generator) -> pd.DataFrame:
    """Annual event counts by type for four observed years, as the legacy function returns them (a `group` index)."""
    rain = rng.integers(0, 5, size=4)
    surge = rng.integers(0, 4, size=4)
    compound = rng.integers(0, 3, size=4)
    frame = pd.DataFrame({"rain": rain, "surge": surge, "compound": compound, "any": rain + surge + compound})
    frame.index.name = "group"
    return frame


@generates_fixture(name="sim_observed_occurrences", seed=3009)
def sim_observed_occurrences(rng: np.random.Generator) -> pd.DataFrame:
    """Event counts for twelve observed 365-day windows: columns any, rain, surge and compound, whole numbers."""
    rain = rng.integers(0, 6, size=12)
    surge = rng.integers(0, 4, size=12)
    compound = rng.integers(0, 3, size=12)
    return pd.DataFrame({"any": rain + surge + compound, "rain": rain, "surge": surge, "compound": compound})


@generates_fixture(name="sim_occurrences", seed=3010)
def sim_occurrences(rng: np.random.Generator) -> pd.DataFrame:
    """Simulated event counts for six years, indexed by year: 7 surge-only, 14 rain-only and 5 compound events in all.

    The three simulated event tables below hold exactly those numbers of events, because the legacy cell assigns each
    simulated event to a year and a table with a spare row would leave a missing year.
    """
    rain = np.array([2, 3, 1, 2, 3, 3])
    surge = np.array([1, 2, 0, 1, 2, 1])
    compound = np.array([1, 1, 0, 1, 1, 1])
    frame = pd.DataFrame({"any": rain + surge + compound, "rain": rain, "surge": surge, "compound": compound})
    frame.index.name = "year"
    return frame


THRESHOLDS = {"surge_peak_ft": 2.5, "precip_depth_mm": 40.0}


def _observed_events(kind: str, rng, count: int) -> pd.DataFrame:
    """Observed events of one type: the surge peak and rain depth of each, with some values missing."""
    surge = np.round(rng.uniform(0.5, 4.5, count), 2)
    rain = np.round(rng.uniform(5.0, 90.0, count), 2)
    if kind == "surge_events":
        surge = np.round(rng.uniform(2.5, 4.5, count), 2)
    elif kind == "rain_events":
        rain = np.round(rng.uniform(40.0, 90.0, count), 2)
    else:
        surge = np.round(rng.uniform(2.5, 4.5, count), 2)
        rain = np.round(rng.uniform(40.0, 90.0, count), 2)
    frame = pd.DataFrame({"surge_peak_ft": surge, "precip_depth_mm": rain})
    frame.iloc[count - 1, 0] = np.nan
    return frame


def _simulated_events(kind: str, rng, count: int) -> pd.DataFrame:
    """Simulated events of one type that meet that type's thresholds and, but for one surge event, no other.

    The exception is deliberate and is the shape the real simulated surge table had: one surge event whose rain depth
    also meets the rain threshold, as 420 of the 2463 rows of the estate's real table did. The legacy cell reports
    every such event, marks the comparison a problem and ends its event-type loop there; without a row of this shape
    that check and the end of the loop are never reached, and the cell runs every event type instead.
    """
    surge = (
        np.round(rng.uniform(2.6, 4.4, count), 2)
        if kind != "rain_events"
        else np.round(rng.uniform(0.2, 2.4, count), 2)
    )
    rain = (
        np.round(rng.uniform(41.0, 88.0, count), 2)
        if kind != "surge_events"
        else np.round(rng.uniform(5.0, 39.0, count), 2)
    )
    if kind == "surge_events":
        rain[count - 1] = 41.5
    return pd.DataFrame({"surge_peak_ft": surge, "precip_depth_mm": rain})


def _fitted_cdf(kind: str, rng, count: int) -> pd.DataFrame:
    """The long table of fitted cdf values of one type's variables, with one value missing."""
    names = {
        "surge_events": ["surge_peak_ft"],
        "rain_events": ["precip_depth_mm"],
        "compound_events": ["surge_peak_ft", "precip_depth_mm"],
    }[kind]
    rows = [(i, name, round(float(rng.uniform(0.01, 0.99)), 4)) for i in range(count) for name in names]
    frame = pd.DataFrame(rows, columns=["observation_id", "data", "cdf_fit"])
    frame.loc[0, "cdf_fit"] = np.nan
    return frame


@generates_fixture(name="sim_obs_surge", seed=3011)
def sim_obs_surge(rng: np.random.Generator) -> pd.DataFrame:
    """Nine observed surge-only events."""
    return _observed_events("surge_events", rng, 9)


@generates_fixture(name="sim_obs_rain", seed=3012)
def sim_obs_rain(rng: np.random.Generator) -> pd.DataFrame:
    """Ten observed rain-only events."""
    return _observed_events("rain_events", rng, 10)


@generates_fixture(name="sim_obs_compound", seed=3013)
def sim_obs_compound(rng: np.random.Generator) -> pd.DataFrame:
    """Eight observed compound events."""
    return _observed_events("compound_events", rng, 8)


@generates_fixture(name="sim_sim_surge", seed=3014)
def sim_sim_surge(rng: np.random.Generator) -> pd.DataFrame:
    """Seven simulated surge-only events."""
    return _simulated_events("surge_events", rng, 7)


@generates_fixture(name="sim_sim_rain", seed=3015)
def sim_sim_rain(rng: np.random.Generator) -> pd.DataFrame:
    """Fourteen simulated rain-only events."""
    return _simulated_events("rain_events", rng, 14)


@generates_fixture(name="sim_sim_compound", seed=3016)
def sim_sim_compound(rng: np.random.Generator) -> pd.DataFrame:
    """Five simulated compound events."""
    return _simulated_events("compound_events", rng, 5)


@generates_fixture(name="sim_cdf_surge", seed=3017)
def sim_cdf_surge(rng: np.random.Generator) -> pd.DataFrame:
    """Fitted cdf values for the surge-only events, as the legacy script reads them from file."""
    return _fitted_cdf("surge_events", rng, 9)


@generates_fixture(name="sim_cdf_rain", seed=3018)
def sim_cdf_rain(rng: np.random.Generator) -> pd.DataFrame:
    """Fitted cdf values for the rain-only events."""
    return _fitted_cdf("rain_events", rng, 10)


@generates_fixture(name="sim_cdf_compound", seed=3019)
def sim_cdf_compound(rng: np.random.Generator) -> pd.DataFrame:
    """Fitted cdf values for the compound events."""
    return _fitted_cdf("compound_events", rng, 8)


@generates_fixture(name="seed", seed=3020)
def seed(rng: np.random.Generator) -> int:
    """The seed of the legacy bootstrap cell (`random_seed = 24`, line 22 of `_b6_simulate_from_vincop.py`), whatever the generator draws."""
    return 24
