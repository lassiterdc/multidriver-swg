"""Synthetic inputs for the equivalence tests of the legacy event-selection cells of `_b2_event_selection.py` owned by observed-record-assembly.

The legacy selection script loads rainfall and water-level records from files, finds the dates that hold data,
compares the rain events each source yields, and counts the events per year. None of the real data may be published,
so each input below is invented in the form the script holds it after reading: a radar rainfall table indexed by
time (a short run at a five-minute step), an hourly airport rainfall table with a missing-value sentinel and a
repeated row, an hourly reanalysis rainfall table, the six-minute and the hourly water-level tables, a series that
has days with no data, and for each of three rainfall sources a table of selected events (start time, depth and the
largest hourly mean intensity) and the rainfall series the events were drawn from. A generator opens no file and
reads nothing at module level, which the fixture machinery enforces while it runs.
"""

import math

import numpy as np
import pandas as pd

from tests.correspondence.fixtures import generates_fixture

START = "2015-06-01 00:00"


@generates_fixture(name="ora_mrms", seed=20262901)
def ora_mrms(rng):
    """Six hours of radar rainfall at a five-minute step: a bias-corrected series and a mean rate, indexed by `time`."""
    index = pd.date_range(START, periods=72, freq="5min", name="time")
    rate = np.round(rng.gamma(shape=0.6, scale=4.0, size=72), 3)
    return pd.DataFrame({"bias_corrected": np.round(rate * 1.1, 3), "rain_mean": rate}, index=index)


@generates_fixture(name="ora_hourly_ncei", seed=20262902)
def ora_hourly_ncei(rng):
    """Six hours of airport hourly precipitation in inches, with one missing-value sentinel and one repeated row."""
    dates = pd.date_range(START, periods=6, freq="1h")
    table = pd.DataFrame({"DATE": dates, "HPCP": np.round(rng.uniform(0.0, 0.3, size=6), 2)})
    table.loc[2, "HPCP"] = 999.99
    return pd.concat([table, table.iloc[[4]]], ignore_index=True)


@generates_fixture(name="ora_aorc", seed=20262903)
def ora_aorc(rng):
    """Seven hours of hourly reanalysis rainfall rate, indexed by `time`."""
    index = pd.date_range(START, periods=7, freq="1h", name="time")
    return pd.DataFrame({"rain_mean": np.round(rng.gamma(shape=0.6, scale=4.0, size=7), 3)}, index=index)


@generates_fixture(name="ora_wl_6min", seed=20262904)
def ora_wl_6min(rng):
    """Six hours of water levels at a six-minute step: level, tide and surge, indexed by `date_time`."""
    index = pd.date_range(START, periods=60, freq="6min", name="date_time")
    tide = np.round(2.0 * np.sin(np.arange(60) / 9.0), 3)
    surge = np.round(rng.normal(0.3, 0.2, size=60), 3)
    return pd.DataFrame({"waterlevel_ft": tide + surge, "tide_ft": tide, "surge_ft": surge}, index=index)


@generates_fixture(name="ora_wl_hourly", seed=20262905)
def ora_wl_hourly(rng):
    """Seven hours of hourly water levels, with the hourly column names the legacy cell renames."""
    index = pd.date_range(START, periods=7, freq="1h", name="date_time")
    tide = np.round(2.0 * np.sin(np.arange(7) / 1.5), 3)
    surge = np.round(rng.normal(0.3, 0.2, size=7), 3)
    return pd.DataFrame({"waterlevel_ft_hrly": tide + surge, "tide_ft": tide, "surge_ft_hrly": surge}, index=index)


@generates_fixture(name="ora_dated_series", seed=20262906)
def ora_dated_series(rng):
    """A series with several time stamps per day over ten days, with no data on three of them."""
    days = [d for d in pd.date_range("2016-03-01", periods=10) if d.day not in (4, 5, 8)]
    stamps = [d + pd.Timedelta(hours=int(h)) for d in days for h in rng.integers(0, 24, size=2)]
    return pd.Series(np.round(rng.uniform(0.0, 5.0, size=len(stamps)), 3), index=pd.DatetimeIndex(sorted(stamps)))


def _events(rng, first_year, last_year, count):
    """A table of selected rain events: start time, depth and the largest hourly mean intensity."""
    years = np.sort(rng.integers(first_year, last_year + 1, size=count))
    starts = [
        pd.Timestamp(int(y), 1, 1) + pd.Timedelta(days=int(d))
        for y, d in zip(years, rng.integers(0, 360, size=count), strict=True)
    ]
    return pd.DataFrame(
        {
            "rain_event_start": starts,
            "depth_mm": np.round(rng.gamma(shape=4.0, scale=15.0, size=count) + 20.0, 3),
            "max_1hr_mean_mm_per_hr": np.round(rng.gamma(shape=3.0, scale=5.0, size=count) + 5.0, 3),
        }
    )


@generates_fixture(name="ora_events_mrms", seed=20262907)
def ora_events_mrms(rng):
    """Fourteen radar-selected rain events over 2011 to 2019."""
    return _events(rng, 2011, 2019, 14)


@generates_fixture(name="ora_events_aorc", seed=20262908)
def ora_events_aorc(rng):
    """Twelve reanalysis-selected rain events over 2011 to 2018."""
    return _events(rng, 2011, 2018, 12)


@generates_fixture(name="ora_events_ncei", seed=20262909)
def ora_events_ncei(rng):
    """Ten airport-selected rain events over 2012 to 2019."""
    return _events(rng, 2012, 2019, 10)


def _series(rng, start):
    """A rainfall-rate series at a daily step over 800 days, named as the legacy script names one."""
    index = pd.date_range(start, periods=800, freq="1D", name="date_time")
    return pd.Series(np.round(rng.gamma(shape=0.5, scale=3.0, size=800), 3), index=index, name="mm_per_hr")


@generates_fixture(name="ora_series_mrms", seed=20262910)
def ora_series_mrms(rng):
    """The radar rainfall rate series the radar events were selected from."""
    return _series(rng, "2011-03-01")


@generates_fixture(name="ora_series_aorc", seed=20262911)
def ora_series_aorc(rng):
    """The reanalysis rainfall rate series."""
    return _series(rng, "2011-05-01")


@generates_fixture(name="ora_series_ncei", seed=20262912)
def ora_series_ncei(rng):
    """The airport rainfall rate series."""
    return _series(rng, "2012-02-01")


# ---- the inputs of the event-analysis cells of the legacy selection script (`_b3_event_analysis.py`) ----


def _event_rain_series(rng, starts, source, invalid=(), rows=48, step="30min", scale=5.0):
    """The rain time series of events: one block of `rows` stamps at `step` per start, with the rate missing for the events named in `invalid`."""
    frames = []
    for event_id, start in enumerate(starts):
        stamps = pd.date_range(start, periods=rows, freq=step)
        rate = np.round(rng.gamma(shape=0.7, scale=scale, size=rows) + 0.1, 3)
        if event_id in invalid:
            rate = np.full(rows, np.nan)
        frames.append(
            pd.DataFrame({"event_id": event_id, "date_time": stamps, "mm_per_hr": rate, "rain_data_source": source})
        )
    return pd.concat(frames, ignore_index=True)


def _event_summary(series, source):
    """The summary table that goes with a series: one row per event, starting where its series starts."""
    firsts = series.groupby("event_id")["date_time"].min()
    return pd.DataFrame(
        {
            "event_id": firsts.index.to_numpy(),
            "event_start": firsts.to_numpy(),
            "rain_data_source": source,
            "depth_mm": np.round(np.linspace(30.0, 90.0, len(firsts)), 3),
        }
    )


MRMS_STARTS = ["2015-01-01 00:00", "2015-06-01 00:00", "2015-09-01 00:00"]
AORC_STARTS = ["2015-01-01 00:00", "2015-09-01 00:00", "2015-12-01 00:00"]


@generates_fixture(name="oea_wl_mrms_tseries", seed=20262921)
def oea_wl_mrms_tseries(rng):
    """Three radar-selected events' series; the third has no valid rate, so that a lower-priority source replaces it."""
    return _event_rain_series(rng, MRMS_STARTS, "mrms", invalid=(2,), rows=6, step="5min")


@generates_fixture(name="oea_wl_aorc_tseries", seed=20262922)
def oea_wl_aorc_tseries(rng):
    """Three reanalysis-selected events' series: one over the same window as a valid radar event, one over the radar event with no valid rate, one alone."""
    return _event_rain_series(rng, AORC_STARTS, "aorc", rows=6, step="5min")


@generates_fixture(name="oea_wl_mrms_summary", seed=20262923)
def oea_wl_mrms_summary(rng):
    """The summaries of the three radar events."""
    return _event_summary(
        _event_rain_series(np.random.default_rng(20262921), MRMS_STARTS, "mrms", invalid=(2,), rows=6, step="5min"),
        "mrms",
    )


@generates_fixture(name="oea_wl_aorc_summary", seed=20262924)
def oea_wl_aorc_summary(rng):
    """The summaries of the three reanalysis events."""
    return _event_summary(
        _event_rain_series(np.random.default_rng(20262922), AORC_STARTS, "aorc", rows=6, step="5min"), "aorc"
    )


def _analysis_series(rng, source, first_year, scale=5.0):
    """Eight events of 24 hours at a half-hour step, a few rates missing, indexed by event id and time as the legacy file reads back."""
    starts = [f"{first_year + n // 3}-{1 + (n % 3) * 4:02d}-10 00:00" for n in range(8)]
    table = _event_rain_series(rng, starts, source, scale=scale)
    table.loc[[3, 70, 200], "mm_per_hr"] = np.nan
    return table.set_index(["event_id", "date_time"])


@generates_fixture(name="oea_tseries_mrms", seed=20262925)
def oea_tseries_mrms(rng):
    """Eight radar events' series."""
    return _analysis_series(rng, "mrms", 2012)


@generates_fixture(name="oea_tseries_aorc", seed=20262926)
def oea_tseries_aorc(rng):
    """Eight reanalysis events' series."""
    return _analysis_series(rng, "aorc", 2012)


@generates_fixture(name="oea_tseries_ncei", seed=20262927)
def oea_tseries_ncei(rng):
    """Eight airport events' series, at a rate much higher than the radar's, so that the sources' statistics are told apart by the tests."""
    return _analysis_series(rng, "ncei_hrly", 2012, scale=40.0)


def _analysis_summary(rng, scale):
    """Eight events' totals, indexed by event id as the legacy file reads back."""
    return pd.DataFrame(
        {"depth_mm": np.round(rng.gamma(shape=4.0, scale=scale, size=8) + 20.0, 3)},
        index=pd.Index(np.arange(8), name="event_id"),
    )


@generates_fixture(name="oea_summary_mrms", seed=20262928)
def oea_summary_mrms(rng):
    """The totals of the eight radar events."""
    return _analysis_summary(rng, 14.0)


@generates_fixture(name="oea_summary_aorc", seed=20262929)
def oea_summary_aorc(rng):
    """The totals of the eight reanalysis events."""
    return _analysis_summary(rng, 16.0)


@generates_fixture(name="oea_summary_ncei", seed=20262930)
def oea_summary_ncei(rng):
    """The totals of the eight airport events."""
    return _analysis_summary(rng, 11.0)


# ---- the partial-overlap case of the data-source priority cell (developer ruling U-21, outcome 2) ----

PARTIAL_MRMS_STARTS = [
    "2015-01-01 00:00",
    "2015-06-01 00:00",
]  # the overlapped event first, then an untouched later one
PARTIAL_AORC_STARTS = ["2015-01-01 00:15"]  # covers the last three steps of the overlapped radar event and no more


def _partial_series(rng, starts, source, invalid_steps=()):
    """Six steps at a five-minute step per event; `invalid_steps` names (event position, step) pairs whose rate is missing."""
    frames = []
    for event_id, start in enumerate(starts):
        stamps = pd.date_range(start, periods=6, freq="5min")
        rate = np.round(rng.gamma(shape=0.7, scale=5.0, size=6) + 0.1, 3)
        for position, step in invalid_steps:
            if position == event_id:
                rate[step] = np.nan
        frames.append(
            pd.DataFrame({"event_id": event_id, "date_time": stamps, "mm_per_hr": rate, "rain_data_source": source})
        )
    return pd.concat(frames, ignore_index=True)


PARTIAL_INVALID = [(0, 3), (0, 4), (0, 5)]  # the overlapped radar event loses its last three steps


@generates_fixture(name="oea_partial_tseries_mrms", seed=20262941)
def oea_partial_tseries_mrms(rng):
    """The radar series of the partial-overlap case: one event whose last three steps have no rate, and an untouched later event."""
    return _partial_series(rng, PARTIAL_MRMS_STARTS, "mrms", PARTIAL_INVALID)


@generates_fixture(name="oea_partial_tseries_aorc", seed=20262942)
def oea_partial_tseries_aorc(rng):
    """The reanalysis series of the partial-overlap case: one valid event over the overlapped radar event's last three steps."""
    return _partial_series(rng, PARTIAL_AORC_STARTS, "aorc")


@generates_fixture(name="oea_partial_summary_mrms", seed=20262943)
def oea_partial_summary_mrms(rng):
    """The summaries of the two radar events of the partial-overlap case, each starting where its series starts."""
    return _event_summary(
        _partial_series(np.random.default_rng(20262941), PARTIAL_MRMS_STARTS, "mrms", PARTIAL_INVALID), "mrms"
    )


@generates_fixture(name="oea_partial_summary_aorc", seed=20262944)
def oea_partial_summary_aorc(rng):
    """The summary of the reanalysis event of the partial-overlap case, starting where its series starts."""
    return _event_summary(_partial_series(np.random.default_rng(20262942), PARTIAL_AORC_STARTS, "aorc"), "aorc")


# ---- the inputs of the simulation script's preamble and its rejected-period cells (`_b6_simulate_from_vincop.py`) ----

ORB_EVENT_KEYS = ("compound_events", "rain_events", "surge_events")
ORB_EVENT_TYPE = {"compound_events": "compound", "rain_events": "rain", "surge_events": "surge"}


@generates_fixture(name="orb_combined_events", seed=20262951)
def orb_combined_events(rng):
    """The combined event summaries the preamble reads: six events of each type with the four time columns it parses."""
    frames = []
    for key in ORB_EVENT_KEYS:
        count = 6
        starts = pd.date_range("2013-01-15", periods=count, freq="120D")
        frames.append(
            pd.DataFrame(
                {
                    "event_type": ORB_EVENT_TYPE[key],
                    "event_start": starts,
                    "event_end": starts + pd.Timedelta(hours=36),
                    "surge_peak_tstep": starts + pd.Timedelta(hours=10),
                    "precip_max_intensity_tstep": starts + pd.Timedelta(hours=7),
                    "surge_peak_ft": np.round(rng.gamma(2.0, 0.8, size=count) + 2.0, 3),
                    "precip_depth_mm": np.round(rng.gamma(2.5, 9.0, size=count) + 12.0, 3),
                }
            )
        )
    return pd.concat(frames, ignore_index=True)


@generates_fixture(name="orb_combined_tseries", seed=20262952)
def orb_combined_tseries(rng):
    """The combined event time series the preamble reads: twelve steps at five minutes per event of each type."""
    frames = []
    for key in ORB_EVENT_KEYS:
        for event_id in range(3):
            start = pd.Timestamp("2013-01-15") + pd.Timedelta(days=40 * event_id)
            stamps = pd.date_range(start, periods=12, freq="5min")
            frames.append(
                pd.DataFrame(
                    {
                        "event_id": event_id,
                        "date_time": stamps,
                        "mm_per_hr": np.round(rng.gamma(0.7, 5.0, size=12) + 0.1, 3),
                        "surge_ft": np.round(rng.normal(0.4, 0.2, size=12), 3),
                        "rain_data_source": ORB_EVENT_TYPE[key],
                    }
                )
            )
    return pd.concat(frames, ignore_index=True)


@generates_fixture(name="orb_vinecop_json", seed=20262953)
def orb_vinecop_json(rng):
    """A vine copula fitted to thirty invented three-variable probabilities, as its json text in a one-element array."""
    import contextlib

    with contextlib.suppress(
        ImportError
    ):  # the vine library, imported here so that its import-time reads happen outside the audit hook
        import pyvinecopulib as pv

    z = rng.normal(size=(30, 3))
    z[:, 1] += 1.4 * z[:, 0]
    z[:, 2] -= 0.8 * z[:, 1]
    z = (z - z.mean(axis=0)) / z.std(axis=0)
    probabilities = 0.5 * (1.0 + np.vectorize(math.erf)(z / math.sqrt(2.0)))
    controls = pv.FitControlsVinecop()
    controls.selection_criterion = "loglik"
    controls.parametric_method = "itau"
    fitted = pv.Vinecop.from_data(np.asfortranarray(probabilities), controls=controls)
    return np.array([fitted.to_json()])


@generates_fixture(name="orb_wlevel_resampled", seed=20262954)
def orb_wlevel_resampled(rng):
    """The six-minute water levels resampled to the rainfall step, as a dataset on `date_time` with the three level columns."""
    import xarray as xr

    stamps = pd.date_range("2013-01-15", periods=36, freq="5min")
    tide = np.round(2.0 * np.sin(np.arange(36) / 6.0), 3)
    surge = np.round(rng.normal(0.3, 0.2, size=36), 3)
    return xr.Dataset(
        {
            "waterlevel_ft": ("date_time", tide + surge),
            "tide_ft": ("date_time", tide),
            "surge_ft": ("date_time", surge),
        },
        coords={"date_time": stamps},
    )


@generates_fixture(name="orb_annual_counts", seed=20262955)
def orb_annual_counts(rng):
    """The observed annual event counts the rejected-period cell reads: five years, two of them with many missing days and two with few."""
    years = np.arange(2012, 2017)
    return pd.DataFrame(
        {
            "year": years,
            "n_events": rng.integers(0, 4, size=5),
            "n_missing": [12, 3, 21, 0, 9],
            "n_rejected": [2, 0, 5, 0, 1],
            "begin_date": [pd.Timestamp(int(y), 1, 1) for y in years],
            "end_date": [pd.Timestamp(int(y), 12, 31) for y in years],
        }
    )


@generates_fixture(name="orb_valid_dates", seed=20262956)
def orb_valid_dates(rng):
    """The calendar dates with data, as one series of dates; the cell turns them into an index."""
    days = pd.date_range("2014-01-01", "2016-12-31", freq="D")
    keep = days[(days.month != 2) | (days.day != 29)]
    return pd.Series(keep.to_numpy(), name="valid_date")


@generates_fixture(name="orb_rejected_valid_dates", seed=20262957)
def orb_rejected_valid_dates(rng):
    """The three rejected periods the second cell charts, as one row per calendar date of each period."""
    frames = []
    for period, (start, end) in enumerate(
        [("2012-01-01", "2012-03-31"), ("2014-03-01", "2014-04-30"), ("2015-11-01", "2015-12-31")]
    ):
        stamps = pd.date_range(start, end, freq="D")
        frames.append(pd.DataFrame({"period": period, "date": stamps, "valid": True}))
    return pd.concat(frames, ignore_index=True)


# ---- the inputs of the helper units of `_utils.py` (rows 113-118) ----


@generates_fixture(name="ou_monte_carlo_results", seed=20262961)
def ou_monte_carlo_results(rng):
    """The table of bootstrapped two-sample test results `plot_pvalue_distributions` charts: seven tests over sixteen samples each."""
    tests = [
        "Cramer-von Mises",
        "Kolmogorov-Smirnov",
        "Mann-Whitney U",
        "Wilcoxon rank-sum",
        "Student's t-test (equal variances)",
        "Welch's t-test (unequal variances)",
        "Anderson-Darling",
    ]
    frames = []
    for sample in range(16):
        frames.append(
            pd.DataFrame(
                {
                    "Test": tests,
                    "p-value": np.round(rng.uniform(0.0, 1.0, size=len(tests)), 6),
                    "Conclusion": ["fail_to_reject"] * len(tests),
                    "samp_id": sample,
                }
            )
        )
    return pd.concat(frames, ignore_index=True)


@generates_fixture(name="ou_two_samples_a", seed=20262962)
def ou_two_samples_a(rng):
    """The first of the two samples `compare_2_distributions` compares and bootstraps: twenty-four values named `mrms`."""
    return pd.Series(np.round(rng.gamma(2.5, 5.0, size=24) + 4.0, 3), name="mrms")


@generates_fixture(name="ou_two_samples_b", seed=20262963)
def ou_two_samples_b(rng):
    """The second sample: twenty-four values from a slightly higher-scale law, named `aorc`."""
    return pd.Series(np.round(rng.gamma(2.8, 5.0, size=24) + 5.0, 3), name="aorc")


@generates_fixture(name="ou_ncei_hourly", seed=20262964)
def ou_ncei_hourly(rng):
    """Six days of hourly airport precipitation in inches at a naive local index named `DATE`, with a repeated time stamp."""
    dates = pd.date_range("2013-01-01", periods=144, freq="1h")
    values = np.round(rng.uniform(0.0, 0.4, size=144), 3)
    table = pd.DataFrame({"DATE": dates, "HPCP": values})
    return pd.concat([table, table.iloc[[10]]], ignore_index=True)
