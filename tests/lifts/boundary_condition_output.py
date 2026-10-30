"""Lift declarations of the legacy units owned by boundary-condition-output.

Both units belong to `_b7_generate_time_series_from_event_stats.py`: the cell that reads back each event type's
simulated statistics and series (lines 891-914) and the cell that joins them, converts feet to metres and writes
the combined table and series (lines 915-969). A declaration supplies the names the stored text reads and reduces
the finished namespace to the value a test compares; it imports no module of this checkout, and any helper it needs
sits inside it. Each unit yields two outputs that cannot share one table (a frame of statistics and a gridded
dataset), so each takes one declaration and one test per output, with `part=` naming the legacy variable.

The first unit runs unchanged. It reads files through `pd.read_csv` and `xr.open_dataset`, so `pd` and `xr` are bound
to stand-ins that return the fixtures by file name and read nothing. The second unit writes through methods of real
frames, which no binding can intercept, so it takes the adapted form: its two writes are kept in memory (the stored
diff shows exactly those two lines), and its outputs are the data that would have been written.
"""

from tests.correspondence.lift import declares_lift


@declares_lift(
    unit="_b7_generate_time_series_from_event_stats.py:891-914",
    part="lst_summaries",
    fixtures=[
        "occurrences",
        "summaries_rain",
        "summaries_surge",
        "summaries_compound",
        "series_rain",
        "series_surge",
        "series_compound",
    ],
)
def declare_read_back_summaries(fixtures):
    """Run the read-back cell on the fixtures; reduce to the gathered statistics tables, joined into one frame."""
    import types

    import pandas

    def read_csv(path, **kwargs):
        """Return the fixture frame named by the file, leaving the legacy index arguments to the fixture."""
        name = path.rsplit("/", 1)[-1]
        suffix = "_simulation_summaries.csv"
        key = "summaries_" + name[: -len(suffix)] if name.endswith(suffix) else "occurrences"
        return fixtures[key].copy()

    def exit(message):
        """Stop as the legacy error branch does, without importing `sys`."""
        raise SystemExit(message)

    def open_dataset(path, **kwargs):
        """Return the fixture dataset named by the file."""
        name = path.rsplit("/", 1)[-1]
        return fixtures["series_" + name[: -len("_simulation_time_series.nc")]].copy()

    bindings = {
        "pd": types.SimpleNamespace(read_csv=read_csv),
        "xr": types.SimpleNamespace(open_dataset=open_dataset),
        "sys": types.SimpleNamespace(exit=exit),
        "dir_simulate_from_copula": "simulated/",
        "dir_storm_rescaling": "rescaled/",
        "n_years_of_weather_to_generate": 3,
        "dic_event_summaries": {"rain": None, "surge": None, "compound": None},
    }
    return bindings, lambda ns: pandas.concat(ns["lst_summaries"])


@declares_lift(
    unit="_b7_generate_time_series_from_event_stats.py:891-914",
    part="lst_ds_tseries",
    fixtures=[
        "occurrences",
        "summaries_rain",
        "summaries_surge",
        "summaries_compound",
        "series_rain",
        "series_surge",
        "series_compound",
    ],
)
def declare_read_back_series(fixtures):
    """Run the read-back cell on the fixtures; reduce to the gathered series, joined along the event type."""
    import types

    import xarray

    def read_csv(path, **kwargs):
        """Return the fixture frame named by the file, leaving the legacy index arguments to the fixture."""
        name = path.rsplit("/", 1)[-1]
        suffix = "_simulation_summaries.csv"
        key = "summaries_" + name[: -len(suffix)] if name.endswith(suffix) else "occurrences"
        return fixtures[key].copy()

    def exit(message):
        """Stop as the legacy error branch does, without importing `sys`."""
        raise SystemExit(message)

    def open_dataset(path, **kwargs):
        """Return the fixture dataset named by the file."""
        name = path.rsplit("/", 1)[-1]
        return fixtures["series_" + name[: -len("_simulation_time_series.nc")]].copy()

    bindings = {
        "pd": types.SimpleNamespace(read_csv=read_csv),
        "xr": types.SimpleNamespace(open_dataset=open_dataset),
        "sys": types.SimpleNamespace(exit=exit),
        "dir_simulate_from_copula": "simulated/",
        "dir_storm_rescaling": "rescaled/",
        "n_years_of_weather_to_generate": 3,
        "dic_event_summaries": {"rain": None, "surge": None, "compound": None},
    }
    return bindings, lambda ns: xarray.concat(ns["lst_ds_tseries"], dim="event_type")


@declares_lift(
    unit="_b7_generate_time_series_from_event_stats.py:915-969",
    part="written_summaries",
    form="adapted",
    fixtures=[
        "summaries_rain",
        "summaries_surge",
        "summaries_compound",
        "series_rain",
        "series_surge",
        "series_compound",
    ],
)
def declare_join_summaries(fixtures):
    """Run the adapted join cell on the fixtures; reduce to the table the cell writes."""
    import numpy
    import pandas
    import xarray

    bindings = {
        "pd": pandas,
        "xr": xarray,
        "np": numpy,
        "lst_summaries": [fixtures["summaries_rain"], fixtures["summaries_surge"], fixtures["summaries_compound"]],
        "lst_ds_tseries": [fixtures["series_rain"], fixtures["series_surge"], fixtures["series_compound"]],
        "feet_per_meter": 3.28084,
        "tstep": numpy.timedelta64(5, "m"),
    }
    return bindings, lambda ns: ns["written_summaries"]


@declares_lift(
    unit="_b7_generate_time_series_from_event_stats.py:915-969",
    part="written_time_series",
    form="adapted",
    fixtures=[
        "summaries_rain",
        "summaries_surge",
        "summaries_compound",
        "series_rain",
        "series_surge",
        "series_compound",
    ],
)
def declare_join_series(fixtures):
    """Run the adapted join cell on the fixtures; reduce to the series the cell writes."""
    import numpy
    import pandas
    import xarray

    bindings = {
        "pd": pandas,
        "xr": xarray,
        "np": numpy,
        "lst_summaries": [fixtures["summaries_rain"], fixtures["summaries_surge"], fixtures["summaries_compound"]],
        "lst_ds_tseries": [fixtures["series_rain"], fixtures["series_surge"], fixtures["series_compound"]],
        "feet_per_meter": 3.28084,
        "tstep": numpy.timedelta64(5, "m"),
    }
    return bindings, lambda ns: ns["written_time_series"]
