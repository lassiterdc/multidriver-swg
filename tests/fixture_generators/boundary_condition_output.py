"""Synthetic inputs for the equivalence tests of the legacy event-statistics gathering and joining units.

The legacy cells read, for each of three event types, a table of simulated event statistics and a gridded
simulated time series, together with the table of simulated events per year, and then join them. None of that
real data may be published, so each input below is invented: the same three event types, a small grid of years
and event slots, and events present in only some cells, as the legacy code expects (cells with no event hold NaN in
every variable). The summary tables are derived from the series, so the statistics a legacy cell cross-checks
(rain depth, peak surge) agree exactly between the two, and the counts agree with the yearly occurrences.

The grid is years 1 to 3, event slots 0 to 2, four time steps of five minutes. Each summary table is indexed by
(event_type, year, event_id) and holds one event per occupied cell. A generator opens no file and reads nothing
at module level, which the fixture machinery enforces while it runs.
"""

import contextlib

import numpy as np
import pandas as pd
import xarray as xr

with contextlib.suppress(
    ImportError
):  # xarray's first array operation imports dask when installed, and dask reads /proc and /sys at import
    import dask  # noqa: F401

from tests.correspondence.fixtures import generates_fixture

EVENT_TYPES = ("rain", "surge", "compound")
CELLS = {  # occupied (year, event_id) cells per event type: 3, 2 and 2 events
    "rain": [(1, 0), (1, 1), (3, 0)],
    "surge": [(2, 0), (3, 1)],
    "compound": [(1, 2), (2, 1)],
}
YEARS = [1, 2, 3]
SLOTS = [0, 1, 2]
STEPS = 4
STEP_HOURS = 5 / 60


def _series(event_type: str, rng: np.random.Generator) -> xr.Dataset:
    """The gridded series of one event type: four variables over (event_type, year, event_id, timestep), NaN where no event."""
    shape = (1, len(YEARS), len(SLOTS), STEPS)
    data = {name: np.full(shape, np.nan) for name in ("mm_per_hr", "waterlevel_ft", "surge_ft", "tide_ft")}
    for year, slot in CELLS[event_type]:
        index = (0, YEARS.index(year), SLOTS.index(slot))
        data["mm_per_hr"][index] = np.round(rng.uniform(0.5, 40.0, STEPS), 3)
        data["surge_ft"][index] = np.round(rng.uniform(0.1, 4.0, STEPS), 3)
        data["tide_ft"][index] = np.round(rng.uniform(-1.5, 1.5, STEPS), 3)
        data["waterlevel_ft"][index] = data["surge_ft"][index] + data["tide_ft"][index]
    dims = ("event_type", "year", "event_id", "timestep")
    return xr.Dataset(
        {name: (dims, values) for name, values in data.items()},
        coords={"event_type": [event_type], "year": YEARS, "event_id": SLOTS, "timestep": np.arange(STEPS)},
    )


def _summaries(event_type: str, series: xr.Dataset) -> pd.DataFrame:
    """The statistics table of one event type, one row per event, derived from its series so the two agree.

    One exception, and it is the disagreement the join cell's final check exists to report: the last compound
    event's rain depth is accumulated from the series rounded to a tenth of a millimetre per step, as a summary
    written through a rounded intermediate would be, so the depth that row states is not the integral of the time
    series it is joined with. Every other row's depth is that integral exactly, and the peak surges agree exactly.
    """
    rows = []
    for year, slot in CELLS[event_type]:
        cell = series.sel(event_type=event_type, year=year, event_id=slot)
        if (event_type, year, slot) == ("compound", *CELLS["compound"][-1]):
            depth = float((np.round(cell["mm_per_hr"], 1) * STEP_HOURS).sum())
        else:
            depth = float((cell["mm_per_hr"] * STEP_HOURS).sum())
        rows.append(
            {
                "event_type": event_type,
                "year": year,
                "event_id": slot,
                "precip_depth_mm": depth,
                "surge_peak_ft": float(cell["surge_ft"].max()),
                "tide_peak_ft": float(cell["tide_ft"].max()),
                "duration_hr": STEPS * STEP_HOURS,
            }
        )
    return pd.DataFrame(rows).set_index(["event_type", "year", "event_id"])


@generates_fixture(name="occurrences", seed=20261001)
def occurrences(rng):
    """Simulated events per year and event type, the counts of the cells above."""
    counts = {t: [sum(1 for y, _ in CELLS[t] if y == year) for year in YEARS] for t in EVENT_TYPES}
    return pd.DataFrame(counts, index=pd.Index(YEARS, name="year"))


@generates_fixture(name="series_rain", seed=20261002)
def series_rain(rng):
    """The rain events' gridded series."""
    return _series("rain", rng)


@generates_fixture(name="series_surge", seed=20261003)
def series_surge(rng):
    """The surge events' gridded series."""
    return _series("surge", rng)


@generates_fixture(name="series_compound", seed=20261004)
def series_compound(rng):
    """The compound events' gridded series."""
    return _series("compound", rng)


@generates_fixture(name="summaries_rain", seed=20261002)
def summaries_rain(rng):
    """The rain events' statistics, derived from the series generated with the same seed."""
    return _summaries("rain", _series("rain", rng))


@generates_fixture(name="summaries_surge", seed=20261003)
def summaries_surge(rng):
    """The surge events' statistics, derived from the series generated with the same seed."""
    return _summaries("surge", _series("surge", rng))


@generates_fixture(name="summaries_compound", seed=20261004)
def summaries_compound(rng):
    """The compound events' statistics, derived from the series generated with the same seed."""
    return _summaries("compound", _series("compound", rng))
