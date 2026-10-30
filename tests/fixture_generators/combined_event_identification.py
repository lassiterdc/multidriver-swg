"""Synthetic inputs for the equivalence tests of the legacy event-selection units owned by combined-event-identification.

The legacy event-selection script ends by reporting its surge-event selection: it plots a histogram of the peak
surge of each selected event and a bar chart of how many events fall in each year, against a six-minute water
level record. None of that real data may be published, so each input below is invented, in the form the legacy
cell reads: a surge series sampled every six minutes with a few missing readings, the table of selected surge
events (one row per event, indexed by event id) and the long table of each event's time series.

The record is three short winter spells, one in each of the years 2001 to 2003, so the series spans three calendar
years without holding three years of six-minute readings; its most common time step is still six minutes, which
the legacy cell takes as the record's step. Nine events are spread over the three years, two to four in a year,
with peak surges from 2.6 to 4.4 ft, so the histogram has several occupied bins and the yearly bars differ in
height. A generator opens no file and reads nothing at module level, which the fixture machinery enforces while it
runs.

The same module also holds the inputs of the legacy final event selection (rain events from three rainfall sources,
surge events from the six-minute water levels) and of the cell that pairs them into combined events. All of them lie
on one 24-day grid of six-minute readings from 2020-01-01. The rainfall of the selection is exactly zero between
storms, because the legacy selection finds an event's limits in dry windows; the rainfall of the pairing carries a
small drizzle everywhere, because the legacy buffering of a combined event needs some rain in every window. The
event tables of the pairing are written here from the storms and surges the generators place, not computed by any
legacy code.

The last section holds the tables the legacy event analysis reads back from the csv files the pairing wrote: for each
rainfall source a summary table and a long series, as `pandas.read_csv` returns them (a default integer index, the
event id and the date columns as ordinary columns, dates already parsed).
"""

import numpy as np
import pandas as pd

from tests.correspondence.fixtures import generates_fixture

SPELL_STARTS = ("2001-01-10", "2002-01-10", "2003-01-10")
SPELL_DAYS = 14
READINGS_PER_DAY = 240  # one reading every six minutes
EVENTS_PER_YEAR = (3, 4, 2)  # nine events over the three years
EVENT_READINGS = 5  # readings of the series each event's time series holds


def _surge_series(rng: np.random.Generator) -> pd.Series:
    """The six-minute surge series of the three spells, in feet, with a few readings missing."""
    parts = [
        pd.date_range(start, periods=SPELL_DAYS * READINGS_PER_DAY, freq="6min", name="date_time")
        for start in SPELL_STARTS
    ]
    index = parts[0].append(parts[1]).append(parts[2])
    values = np.round(rng.normal(0.4, 0.6, index.size), 3)
    values[rng.choice(index.size, size=12, replace=False)] = np.nan
    return pd.Series(values, index=index, name="surge_ft")


@generates_fixture(name="surge_series_6min", seed=2002)
def surge_series_6min(rng: np.random.Generator) -> pd.Series:
    """The invented six-minute surge record the final surge-event selection runs on, with twelve missing readings."""
    return _surge_series(rng)


def _event_starts(series: pd.Series, rng: np.random.Generator) -> list[pd.Timestamp]:
    """One start time per event, each on a reading inside its year's spell, in time order."""
    starts = []
    for start, count in zip(SPELL_STARTS, EVENTS_PER_YEAR, strict=True):
        spell = series.index[(series.index >= start) & (series.index < pd.Timestamp(start) + pd.Timedelta(days=12))]
        picks = np.sort(rng.choice(spell.size - EVENT_READINGS, size=count, replace=False))
        starts += [spell[pick] for pick in picks]
    return starts


@generates_fixture(name="surge_event_summaries", seed=2003)
def surge_event_summaries(rng: np.random.Generator) -> pd.DataFrame:
    """The selected surge events: id, start, end, duration, peak surge, time of the peak and the data source."""
    series = _surge_series(np.random.default_rng(2002))  # the record these events lie in, regenerated from its seed
    starts = _event_starts(series, rng)
    step = pd.Timedelta(minutes=6)
    peaks = np.round(rng.uniform(2.6, 4.4, len(starts)), 2)
    frame = pd.DataFrame(
        {
            "surge_event_id": np.arange(1, len(starts) + 1),
            "surge_event_start": starts,
            "surge_event_end": [start + EVENT_READINGS * step for start in starts],
            "duration": [EVENT_READINGS * step] * len(starts),
            "max_surge_ft": peaks,
            "max_surge_tstep": [start + 2 * step for start in starts],
        }
    )
    frame["wlevel_data_source"] = "6min_surge_data"
    return frame.set_index("surge_event_id")


@generates_fixture(name="surge_event_tseries", seed=2004)
def surge_event_tseries(rng: np.random.Generator) -> pd.DataFrame:
    """The long table of each event's readings, indexed by (event id, time), as the selection returns it."""
    series = _surge_series(np.random.default_rng(2002))
    starts = _event_starts(series, np.random.default_rng(2003))
    rows = []
    for event_id, start in enumerate(starts, 1):
        for n in range(EVENT_READINGS):
            rows.append((event_id, start + n * pd.Timedelta(minutes=6), round(float(rng.uniform(1.0, 4.4)), 3)))
    frame = pd.DataFrame(rows, columns=["surge_event_id", "date_time", "surge_ft"])
    frame["wlevel_data_source"] = "6min_surge_data"
    return frame.set_index(["surge_event_id", "date_time"])


# ---- the 24-day grid shared by the final event selection and the pairing of combined events ----

GRID_START = pd.Timestamp("2020-01-01")
GRID_READINGS = 24 * 240  # 24 days of six-minute readings
HOUR = pd.Timedelta(hours=1)
STEP_HOURS_6MIN = 0.1


def _grid() -> pd.DatetimeIndex:
    """The six-minute index every series of this section lies on, named as the legacy tables name it."""
    return pd.date_range(GRID_START, periods=GRID_READINGS, freq="6min", name="date_time")


def _at(day: int, hour: float) -> pd.Timestamp:
    """The grid timestamp `day` days and `hour` hours after the start."""
    return GRID_START + pd.Timedelta(days=day, hours=hour)


def _storm(values: np.ndarray, index: pd.DatetimeIndex, start: pd.Timestamp, hours: float, peak: float, rng) -> None:
    """Write one storm into `values`: a smooth hump of intensity (mm per hour), positive at every reading of the storm."""
    inside = (index >= start) & (index < start + hours * HOUR)
    phase = np.linspace(0.0, np.pi, int(inside.sum()))
    values[inside] = np.round(0.3 + peak * np.sin(phase) ** 2 * rng.uniform(0.85, 1.15, phase.size), 3)


def _selection_rain(rng, shift_hours: float, drop: int) -> pd.Series:
    """Rainfall in mm per hour for the rain-event selection: seven storms and one 80-hour spell, exactly zero between.

    The seventh storm is too weak to reach the 55 mm threshold; the 80-hour spell holds a 6-hour burst and lasts
    longer than the 48-hour cap on an event. `drop` readings in dry stretches are left out of the index and four
    more hold NaN, as the legacy selection expects of a record with gaps.
    """
    index = _grid()
    values = np.zeros(index.size)
    for day, hour, hours, peak in ((2, 3, 6, 22), (5, 10, 5, 26), (8, 0, 7, 18), (11, 14, 5, 30), (14, 8, 6, 24)):
        _storm(values, index, _at(day, hour + shift_hours), hours, peak, rng)
    _storm(values, index, _at(17, 2 + shift_hours), 4, 10, rng)
    spell = _at(19, shift_hours)
    inside = (index >= spell) & (index < spell + 80 * HOUR)
    values[inside] = np.round(rng.uniform(1.0, 1.8, int(inside.sum())), 3)
    burst = (index >= spell + 30 * HOUR) & (index < spell + 36 * HOUR)
    values[burst] = np.round(rng.uniform(13.0, 17.0, int(burst.sum())), 3)
    series = pd.Series(values, index=index, name="mm_per_hr")
    dry = np.flatnonzero(values == 0)
    gaps = rng.choice(dry, size=drop + 4, replace=False)
    series.iloc[gaps[:4]] = np.nan
    return series.drop(series.index[gaps[4:]])


@generates_fixture(name="rain_mrms", seed=2005)
def rain_mrms(rng: np.random.Generator) -> pd.Series:
    """The invented radar rainfall (mm per hour, six-minute readings) for the final rain-event selection, with three readings left out."""
    return _selection_rain(rng, 0.0, 3)


@generates_fixture(name="rain_aorc", seed=2006)
def rain_aorc(rng: np.random.Generator) -> pd.Series:
    """The invented gridded-product rainfall for the final rain-event selection: storms an hour later, nothing left out."""
    return _selection_rain(rng, 1.0, 0)


@generates_fixture(name="rain_ncei", seed=2007)
def rain_ncei(rng: np.random.Generator) -> pd.Series:
    """The invented gauge rainfall on the radar grid for the final rain-event selection: storms two hours later, two readings left out."""
    return _selection_rain(rng, 2.0, 2)


SURGES = (  # (day, hour, peak ft, width in hours): the last is too weak to select, the wide one exceeds the 48-hour cap
    (2, 12, 3.2, 2.5),
    (6, 0, 2.2, 2.0),
    (8, 6, 2.8, 3.0),
    (13, 18, 3.9, 3.0),
    (17, 0, 2.7, 2.5),
    (21, 6, 3.4, 25.0),
)


def _water_levels(rng) -> pd.DataFrame:
    """The six-minute surge and water level in feet: a small tidal residual plus the surges of `SURGES`."""
    index = _grid()
    hours = (index - GRID_START) / HOUR
    surge = 0.3 + 0.2 * np.sin(2 * np.pi * hours / 12.42) + rng.normal(0.0, 0.03, index.size)
    for day, hour, peak, width in SURGES:
        centre = (_at(day, hour) - GRID_START) / HOUR
        surge = surge + (peak - 0.3) * np.exp(-0.5 * ((hours - centre) / width) ** 2)
    tide = 1.5 * np.sin(2 * np.pi * hours / 12.42 + 1.0)
    frame = pd.DataFrame({"surge_ft": np.round(surge, 3), "waterlevel_ft": np.round(surge + tide, 3)}, index=index)
    return frame


@generates_fixture(name="wlevels_6min", seed=2008)
def wlevels_6min(rng: np.random.Generator) -> pd.DataFrame:
    """The invented six-minute surge and water level for the final surge-event selection and the pairing of combined events."""
    return _water_levels(rng)


COMBINED_SURGES = ((2, 12), (8, 6), (13, 18), (17, 0))  # the four selected surge events: day and hour of the peak


def _combined_surge_tables() -> tuple[pd.DataFrame, pd.DataFrame]:
    """The four surge events as the selection returns them (summary and long series), windows of peak plus and minus 6 hours."""
    levels = _water_levels(np.random.default_rng(2008))
    rows, parts = [], []
    for event_id, (day, hour) in enumerate(COMBINED_SURGES, 1):
        window = levels["surge_ft"].loc[_at(day, hour - 6) : _at(day, hour + 6)]
        rows.append(
            {
                "surge_event_id": event_id,
                "surge_event_start": window.index[0],
                "surge_event_end": window.index[-1],
                "duration": window.index[-1] - window.index[0],
                "max_surge_ft": float(window.max()),
                "max_surge_tstep": window.idxmax(),
                "wlevel_data_source": "6min_surge_data",
            }
        )
        part = window.to_frame()
        part["surge_event_id"] = event_id
        parts.append(part)
    summary = pd.DataFrame(rows).set_index("surge_event_id")
    series = pd.concat(parts).reset_index().set_index(["surge_event_id", "date_time"])
    series["wlevel_data_source"] = "6min_surge_data"
    return summary, series


@generates_fixture(name="cmb_surge_summary", seed=2009)
def cmb_surge_summary(rng: np.random.Generator) -> pd.DataFrame:
    """The four selected surge events the pairing starts from, one row each, indexed by surge event id."""
    return _combined_surge_tables()[0]


@generates_fixture(name="cmb_surge_tseries", seed=2010)
def cmb_surge_tseries(rng: np.random.Generator) -> pd.DataFrame:
    """The long table of those four events' surge readings, indexed by (surge event id, time)."""
    return _combined_surge_tables()[1]


# storms of the pairing per rainfall source: (day, hour, hours, peak mm/hr). The first and third contain a surge peak,
# the second lies a day away from every surge event; the gauge record lacks the second storm.
PAIRING_STORMS = {
    "mrms": ((2, 10, 6, 25), (8, 20, 3, 20), (13, 16, 4, 28), (17, 7, 3, 25)),
    "aorc": ((2, 11, 6, 22), (8, 20, 3, 18), (13, 16, 4, 30)),
    "ncei": ((2, 10, 6, 24), (13, 16, 4, 26), (17, 7, 3, 24)),
}


def _pairing_rain(source: str, seed: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """The rainfall of one source with its drizzle, the rain events placed on it, and their long series.

    Two properties of this table are deliberate and both are shapes the real run met, which two of the pairing
    function's own reports exist to describe:

    - The radar and the gauge carry a storm at day 17, 07:00, three hours long. It begins an hour after the fourth
      surge event's window ends, so it shares no reading with that event's summary window and the two are not
      combined; the buffering the function applies before it computes statistics, however, extends the surge event's
      window six hours past its last rainfall, which brings that storm inside it. The event therefore stays a
      surge-only event whose rain depth beats the weakest rain event's, which is the state the reclassification
      report at the end of the function describes. The gridded-product source carries no such storm: its third
      surge event is the one whose peak falls outside the rain it overlaps, which is the other half of the pairing.
    - The `mm` column is accumulated from the intensity rounded to a tenth of a millimetre per hour, as a depth
      product written from a coarser series would be, so an event's depth summed from this column does not equal the
      depth the summary states, which is the discrepancy the function warns about after it joins the two.
    """
    rng = np.random.default_rng(seed)
    index = _grid()
    values = np.round(rng.uniform(0.02, 0.2, index.size), 3)
    spans = []
    for day, hour, hours, peak in PAIRING_STORMS[source]:
        start = _at(day, hour)
        _storm(values, index, start, hours, peak, rng)
        spans.append((start, start + (hours - STEP_HOURS_6MIN) * HOUR))
    series = pd.DataFrame({"mm_per_hr": values, "mm": np.round(np.round(values, 1) * STEP_HOURS_6MIN, 6)}, index=index)
    rows, parts = [], []
    for event_id, (start, end) in enumerate(spans, 1):
        window = series["mm_per_hr"].loc[start:end]
        rows.append(
            {
                "event_id": event_id,
                "rain_event_start": start,
                "rain_event_end": end,
                "duration": end - start,
                "depth_mm": float(window.sum() * STEP_HOURS_6MIN),
                "mean_mm_per_hr": float(window.mean()),
                "max_mm_per_hour": float(window.max()),
                "max_intensity_tstep": window.idxmax(),
                "rain_data_source": source,
            }
        )
        part = window.rename("mm_per_hour").to_frame()
        part["event_id"] = event_id
        parts.append(part)
    summary = pd.DataFrame(rows).set_index("event_id")
    events = pd.concat(parts).reset_index().set_index(["event_id", "date_time"])
    events["rain_data_source"] = source
    return series, summary, events


@generates_fixture(name="cmb_rain_mrms_series", seed=2011)
def cmb_rain_mrms_series(rng: np.random.Generator) -> pd.DataFrame:
    """The radar rainfall of the pairing, in mm per hour and in mm per reading, with a drizzle outside its three storms."""
    return _pairing_rain("mrms", 2011)[0]


@generates_fixture(name="cmb_rain_mrms_summary", seed=2012)
def cmb_rain_mrms_summary(rng: np.random.Generator) -> pd.DataFrame:
    """The three radar rain events the pairing starts from, one row each, indexed by event id."""
    return _pairing_rain("mrms", 2011)[1]


@generates_fixture(name="cmb_rain_mrms_tseries", seed=2013)
def cmb_rain_mrms_tseries(rng: np.random.Generator) -> pd.DataFrame:
    """The long table of those events' radar rainfall, indexed by (event id, time)."""
    return _pairing_rain("mrms", 2011)[2]


@generates_fixture(name="cmb_rain_aorc_series", seed=2014)
def cmb_rain_aorc_series(rng: np.random.Generator) -> pd.DataFrame:
    """The gridded-product rainfall of the pairing, with its first storm an hour later and a different drizzle."""
    return _pairing_rain("aorc", 2014)[0]


@generates_fixture(name="cmb_rain_aorc_summary", seed=2015)
def cmb_rain_aorc_summary(rng: np.random.Generator) -> pd.DataFrame:
    """The three gridded-product rain events the pairing starts from."""
    return _pairing_rain("aorc", 2014)[1]


@generates_fixture(name="cmb_rain_aorc_tseries", seed=2016)
def cmb_rain_aorc_tseries(rng: np.random.Generator) -> pd.DataFrame:
    """The long table of those events' rainfall."""
    return _pairing_rain("aorc", 2014)[2]


@generates_fixture(name="cmb_rain_ncei_series", seed=2017)
def cmb_rain_ncei_series(rng: np.random.Generator) -> pd.DataFrame:
    """The gauge rainfall of the pairing, which holds two storms and not the one a day from every surge."""
    return _pairing_rain("ncei", 2017)[0]


@generates_fixture(name="cmb_rain_ncei_summary", seed=2018)
def cmb_rain_ncei_summary(rng: np.random.Generator) -> pd.DataFrame:
    """The two gauge rain events the pairing starts from."""
    return _pairing_rain("ncei", 2017)[1]


@generates_fixture(name="cmb_rain_ncei_tseries", seed=2019)
def cmb_rain_ncei_tseries(rng: np.random.Generator) -> pd.DataFrame:
    """The long table of those events' rainfall."""
    return _pairing_rain("ncei", 2017)[2]


# ---- the combined-event tables as the event analysis reads them back from csv ----

FLAG_PATTERNS = (  # (compound, surge_event, rain_event) of the successive events: each event has at least one flag set
    (True, True, True),
    (False, True, False),
    (False, False, True),
    (True, True, True),
    (False, False, True),
    (False, True, False),
    (True, True, True),
)


def _read_back_summary(source: str, rng, years: tuple[int, ...], missing: tuple[int, ...]) -> pd.DataFrame:
    """A summary table as the analysis reads it: one row per event, with the flags the classification reads."""
    count = len(years)
    starts = [pd.Timestamp(year=year, month=int(1 + 3 * n % 12), day=5 + n, hour=3 * n) for n, year in enumerate(years)]
    frame = pd.DataFrame(
        {
            "event_id": np.arange(count),
            "event_start": starts,
            "event_end": [start + pd.Timedelta(hours=int(rng.integers(8, 30))) for start in starts],
            "compound": [FLAG_PATTERNS[n][0] for n in range(count)],
            "rain_event": [FLAG_PATTERNS[n][2] for n in range(count)],
            "surge_event": [FLAG_PATTERNS[n][1] for n in range(count)],
            "rain_data_source": source,
            "wlevel_data_source": "6min_surge_data",
            "surge_peak_ft": np.round(rng.uniform(2.5, 4.5, count), 3),
            "precip_depth_mm": np.round(rng.uniform(10.0, 120.0, count), 2),
        }
    )
    frame.loc[list(missing), "precip_depth_mm"] = np.nan
    return frame


def _read_back_series(source: str, rng, events: int, mm_name: str) -> pd.DataFrame:
    """A long series as the analysis reads it: event id, time and the surge, water level and rain columns."""
    rows = []
    for event_id in range(events):
        start = pd.Timestamp(2014 + event_id, 2, 3, 6)
        for n in range(6):
            rows.append(
                (
                    event_id,
                    start + n * pd.Timedelta(minutes=6),
                    round(float(rng.uniform(0.2, 4.0)), 3),
                    round(float(rng.uniform(-1.0, 4.5)), 3),
                    round(float(rng.uniform(0.0, 30.0)), 3),
                    round(float(rng.uniform(0.0, 3.0)), 4),
                )
            )
    frame = pd.DataFrame(rows, columns=["event_id", "date_time", "surge_ft", "waterlevel_ft", "mm_per_hr", mm_name])
    frame["wlevel_data_source"] = "6min_surge_data"
    frame["rain_data_source"] = source
    return frame


@generates_fixture(name="rb_mrms_summary", seed=2020)
def rb_mrms_summary(rng: np.random.Generator) -> pd.DataFrame:
    """The radar-paired combined-event summaries as read back from csv: seven events, one without a rain depth."""
    return _read_back_summary("mrms", rng, (2013, 2014, 2015, 2016, 2016, 2017, 2018), (4,))


@generates_fixture(name="rb_mrms_tseries", seed=2021)
def rb_mrms_tseries(rng: np.random.Generator) -> pd.DataFrame:
    """The radar-paired long series as read back from csv, its rain depth column under the old name `_mm`."""
    return _read_back_series("mrms", rng, 3, "_mm")


@generates_fixture(name="rb_aorc_summary", seed=2022)
def rb_aorc_summary(rng: np.random.Generator) -> pd.DataFrame:
    """The gridded-product summaries as read back from csv: five events."""
    return _read_back_summary("aorc", rng, (2013, 2014, 2015, 2016, 2017), (1,))


@generates_fixture(name="rb_aorc_tseries", seed=2023)
def rb_aorc_tseries(rng: np.random.Generator) -> pd.DataFrame:
    """The gridded-product long series as read back from csv, its rain depth column named `mm`."""
    return _read_back_series("aorc", rng, 2, "mm")


@generates_fixture(name="rb_ncei_summary", seed=2024)
def rb_ncei_summary(rng: np.random.Generator) -> pd.DataFrame:
    """The gauge-paired summaries as read back from csv: four events."""
    return _read_back_summary("ncei", rng, (2014, 2015, 2016, 2017), ())


@generates_fixture(name="rb_ncei_tseries", seed=2025)
def rb_ncei_tseries(rng: np.random.Generator) -> pd.DataFrame:
    """The gauge-paired long series as read back from csv."""
    return _read_back_series("ncei", rng, 2, "mm")


@generates_fixture(name="rb_mrms_classified", seed=2026)
def rb_mrms_classified(rng: np.random.Generator) -> pd.DataFrame:
    """The classified radar-paired summaries the bar chart of event rain depths starts from: nine events from 2012 to 2019.

    Two events lie before 2015 and one has no rain depth, so the chart's filter by year and by present depth both
    remove something.
    """
    years = (2012, 2014, 2015, 2015, 2016, 2017, 2017, 2018, 2019)
    frame = _read_back_summary("mrms", rng, years[: len(FLAG_PATTERNS)], ())
    extra = _read_back_summary("mrms", rng, years[len(FLAG_PATTERNS) :], ())
    extra["event_id"] += len(FLAG_PATTERNS)
    frame = pd.concat([frame, extra], ignore_index=True)
    frame["event_start"] = [pd.Timestamp(year, 1 + n, 10 + n, 2) for n, year in enumerate(years)]
    frame["event_end"] = frame["event_start"] + pd.Timedelta(hours=14)
    frame.loc[6, "precip_depth_mm"] = np.nan
    frame["event_type"] = np.where(frame["compound"], "compound", np.where(frame["surge_event"], "surge", "rain"))
    return frame


# ---- surge and rain events that overlap without sharing a surge peak, for the pairing function ----


def _overlap_tables() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Two surge events and two rain events as the selections return them (summaries and long series).

    The first pair overlaps in time while the surge peak lies outside the rain event, which the legacy pairing treats
    as an event of its own beside both inputs, so the events it returns overlap and its merging loop runs. The second
    pair is an ordinary compound pair (the surge peak lies inside the rain event).
    """
    step = pd.Timedelta(minutes=6)
    surge_rows, surge_parts, rain_rows, rain_parts = [], [], [], []
    surges = ((1, _at(1, 6), _at(1, 18), _at(1, 7)), (2, _at(3, 6), _at(3, 18), _at(3, 12)))
    rains = ((1, _at(1, 12), _at(1, 16)), (2, _at(3, 10), _at(3, 14)))
    rng = np.random.default_rng(2027)
    for event_id, start, end, peak in surges:
        index = pd.date_range(start, end, freq=step, name="date_time")
        values = np.round(rng.uniform(0.2, 2.0, index.size), 3)
        values[index.get_loc(peak)] = 3.6
        surge_rows.append(
            {
                "surge_event_id": event_id,
                "surge_event_start": start,
                "surge_event_end": end,
                "duration": end - start,
                "max_surge_ft": 3.6,
                "max_surge_tstep": peak,
                "wlevel_data_source": "6min_surge_data",
            }
        )
        part = pd.DataFrame({"surge_ft": values}, index=index)
        part["surge_event_id"] = event_id
        surge_parts.append(part)
    for event_id, start, end in rains:
        index = pd.date_range(start, end, freq=step, name="date_time")
        values = np.round(rng.uniform(1.0, 20.0, index.size), 3)
        rain_rows.append(
            {
                "event_id": event_id,
                "rain_event_start": start,
                "rain_event_end": end,
                "duration": end - start,
                "depth_mm": float(values.sum() * STEP_HOURS_6MIN),
                "rain_data_source": "mrms",
            }
        )
        part = pd.DataFrame({"mm_per_hour": values}, index=index)
        part["event_id"] = event_id
        rain_parts.append(part)
    surge_summary = pd.DataFrame(surge_rows).set_index("surge_event_id")
    surge_series = pd.concat(surge_parts).reset_index().set_index(["surge_event_id", "date_time"])
    surge_series["wlevel_data_source"] = "6min_surge_data"
    rain_summary = pd.DataFrame(rain_rows).set_index("event_id")
    rain_series = pd.concat(rain_parts).reset_index().set_index(["event_id", "date_time"])
    rain_series["rain_data_source"] = "mrms"
    return surge_summary, surge_series, rain_summary, rain_series


@generates_fixture(name="ov_surge_summary", seed=2027)
def ov_surge_summary(rng: np.random.Generator) -> pd.DataFrame:
    """The two surge events of the overlap scenario, one row each, indexed by surge event id."""
    return _overlap_tables()[0]


@generates_fixture(name="ov_surge_tseries", seed=2028)
def ov_surge_tseries(rng: np.random.Generator) -> pd.DataFrame:
    """The long table of those two surge events, indexed by (surge event id, time)."""
    return _overlap_tables()[1]


@generates_fixture(name="ov_rain_summary", seed=2029)
def ov_rain_summary(rng: np.random.Generator) -> pd.DataFrame:
    """The two rain events of the overlap scenario, one row each, indexed by event id."""
    return _overlap_tables()[2]


@generates_fixture(name="ov_rain_tseries", seed=2030)
def ov_rain_tseries(rng: np.random.Generator) -> pd.DataFrame:
    """The long table of those two rain events, indexed by (event id, time)."""
    return _overlap_tables()[3]
