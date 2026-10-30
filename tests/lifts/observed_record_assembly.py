"""Lift declarations of the legacy event-selection cells of `_b2_event_selection.py` owned by observed-record-assembly.

A declaration supplies the names a stored legacy text reads and reduces the finished namespace to the value a test
compares; it imports no module of this checkout, and any helper it needs sits inside it, where the lift's digest
covers it. This file is written by a generator, so each declaration repeats the kit it needs.

The cells read files and write one, and call functions the declaration binds to their stored legacy text (the
callees). `pd` is pandas except that `read_csv` returns the committed fixture the named file stands for; the one
file the first cell writes is bound to `None`, so that `to_netcdf` returns bytes and nothing is written; `plt` is a
stand-in that draws real figures in memory and keeps what each saved figure holds; `Path` records the folders the
cell asks for and creates none. The one callee that draws, `compare_2_distributions`, bootstraps with pandas
`sample` and no seed, so the lift of the plotting cell declares `unseeded` and packs what depends on the draws in
components named with `draw`, which the test drops by name.
"""

from tests.correspondence.lift import declares_lift


@declares_lift(
    unit="_b2_event_selection.py:25-87",
    fixtures=["ora_mrms", "ora_hourly_ncei", "ora_aorc", "ora_wl_6min", "ora_wl_hourly"],
    draws="none",
)
def declare_rainfall_and_water_level_loading_cell(fixtures):
    """Run the loading cell on the invented rainfall and water-level tables; reduce to the series and tables it builds, packed with their time stamps."""
    import hashlib
    import pathlib

    import numpy
    import pandas
    import xarray

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
    callee_labels = {
        ("_utils", "1990-2016"): (
            "stored-text:1:dd997b83f435d40b0ee05cc11bb5f5be016596156a8be0a63a6496828b69368c",
            1990,
        ),
    }
    tables = {
        "f_hourlyprecip_airport.csv": fixtures["ora_hourly_ncei"],
        "f_mrms_rainfall.csv": fixtures["ora_mrms"],
        "f_aorc_rainfall.csv": fixtures["ora_aorc"],
        "f_water_level_storm_surge.csv": fixtures["ora_wl_6min"],
        "f_water_level_storm_surge_hrly.csv": fixtures["ora_wl_hourly"],
    }

    class PdBinding:
        """pandas as the legacy cell names it, except that `read_csv` returns the fixture table the named file stands for."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The fixture table for the csv the legacy code reads; any other file is an error."""
            if str(path) not in tables:
                raise FileNotFoundError(f"the lift reads no csv named {path}")
            return tables[str(path)].copy()

    pd_binding = PdBinding()
    # `mm_per_inch` is a constant of the legacy inputs script (`_inputs.py`, line 10), read by the callee
    namespace = {"np": numpy, "pd": pandas, "mm_per_inch": 25.4}
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    bindings = {
        "np": numpy,
        "pd": pd_binding,
        "process_ncei_data": namespace["process_ncei_data"],
        "f_hourlyprecip_airport": "f_hourlyprecip_airport.csv",
        "f_mrms_rainfall": "f_mrms_rainfall.csv",
        "f_aorc_rainfall": "f_aorc_rainfall.csv",
        "f_water_level_storm_surge": "f_water_level_storm_surge.csv",
        "f_water_level_storm_surge_hrly": "f_water_level_storm_surge_hrly.csv",
        "f_6min_wlevel_resampled_to_mrms_res_nc": None,  # `to_netcdf(None)` returns the bytes and writes no file
    }

    def pack(data, name, table):
        """Add a Series or DataFrame to `data` as one variable per column on its own time-stamp dimension."""
        dimension = f"{name}__time"
        frame = table.to_frame() if isinstance(table, pandas.Series) else table
        data[f"{name}__index"] = (dimension, numpy.asarray(frame.index.to_numpy()))
        for column in frame.columns:
            data[f"{name}__{column}"] = (dimension, frame[column].to_numpy(dtype=float))

    def reduce(finished):
        """The series and tables the cell leaves, with their time stamps, and the time steps it computes, as hours."""
        data = {}
        for name in (
            "s_rainfall_ncei_hourly_in",
            "s_mrms_bias_corrected",
            "s_mrms_rainfall_mm_per_hr",
            "s_mrms_rainfall_mm",
            "df_hrly_water_levels_mrms_res",
            "df_6min_water_levels_mrms_res",
            "s_rainfall_ncei_hrly_mrms_res_mm_per_hr",
            "s_rainfall_ncei_hrly_mrms_res_mm",
            "s_aorc_rainfall_mm_per_hr",
            "s_aorc_rainfall_mm",
        ):
            pack(data, name, finished[name])
        for name in ("tstep_mrms", "og_tstep_aorc", "tstep_aorc"):
            data[name] = ((), float(finished[name] / numpy.timedelta64(1, "h")))
        for name in ("tstep_mrms_min", "tstep_aorc_min"):
            data[name] = ((), float(finished[name]))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_b2_event_selection.py:88-94", fixtures=["ora_dated_series"], draws="none")
def declare_find_dates_with_data(fixtures):
    """Run the function that flags each calendar date between the first and last date as holding data or not; reduce to the table it returns."""
    import numpy
    import pandas
    import xarray

    series = fixtures["ora_dated_series"].copy()

    def reduce(finished):
        """The calendar dates and whether each holds data."""
        table = finished["find_dates_with_data"](series)
        return xarray.Dataset(
            {
                "date": ("date", numpy.asarray(table.index.to_numpy())),
                "valid": ("date", table["valid"].to_numpy(dtype=bool)),
            }
        )

    return {"pd": pandas}, reduce


@declares_lift(unit="_b2_event_selection.py:393-405", fixtures=[], draws="none")
def declare_test_whether_mean_is_zero(fixtures):
    """Run the function that tests whether the mean of the yearly count differences is zero, on differences with a clear mean and with none; reduce to its two results."""
    import numpy
    import xarray

    differences = {
        "clear_mean": numpy.asarray([4, 5, 3, 6, 4, 5, 7, 4], dtype=float),
        "no_mean": numpy.asarray([2, -3, 1, -1, 3, -2, 0, 1], dtype=float),
    }

    def reduce(finished):
        """The verdict for each set of differences, and for the clear mean at a looser level."""
        test = finished["test_whether_mean_is_zero"]
        return xarray.Dataset(
            {
                "clear_mean": ((), test(differences["clear_mean"])),
                "no_mean": ((), test(differences["no_mean"])),
                "no_mean_loose": ((), test(differences["no_mean"], alpha=0.9)),
            }
        )

    return {}, reduce


@declares_lift(
    unit="_b2_event_selection.py:386-392,406-411",
    fixtures=["ora_events_mrms", "ora_events_aorc", "ora_events_ncei"],
    draws="none",
)
def declare_annual_event_counts_cells(fixtures):
    """Run the two cells that count the selected events per year and test the count differences between sources; reduce to the counts, the differences and what the test printed."""
    import hashlib
    import pathlib

    import numpy
    import pandas
    import xarray

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
    callee_labels = {
        ("_b2_event_selection", "393-405"): (
            "stored-text:1:41d7240c3c911f4c3bf6e4ae7d230fc74276a1a7d90c5a493ff0d97ec05c441f",
            393,
        ),
    }
    printed = []  # the lines the test function prints: its p-value and its verdict

    def record(*arguments, **keywords):
        """The `print` the callee reads: keep the line and print nothing."""
        printed.append(" ".join(str(argument) for argument in arguments))

    namespace = {"print": record}
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    bindings = {
        "df_mrms_rainevent_summary": fixtures["ora_events_mrms"].copy(),
        "df_aorc_rainevent_summary": fixtures["ora_events_aorc"].copy(),
        "df_ncei_hrly_rainevent_summary": fixtures["ora_events_ncei"].copy(),
        "pd": pandas,
        "test_whether_mean_is_zero": namespace["test_whether_mean_is_zero"],
    }

    def reduce(finished):
        """The yearly counts of each source, the two arrays of count differences, and the lines the test function printed."""
        data = {}
        for name in ("mrms", "aorc", "ncei"):
            counts = finished[f"{name}_annual_counts"]
            data[f"{name}_years"] = (f"{name}_year", numpy.asarray(counts.index.to_numpy(), dtype=int))
            data[f"{name}_counts"] = (f"{name}_year", numpy.asarray(counts.to_numpy(), dtype=int))
            data[f"{name}_series_name"] = ((), str(counts.name))
        data["differences_mrms_vs_aorc"] = (
            "diff_aorc",
            numpy.asarray(finished["count_diffs_mrms_vs_aorc"], dtype=float),
        )
        data["differences_mrms_vs_ncei"] = (
            "diff_ncei",
            numpy.asarray(finished["count_diffs_mrms_vs_ncei"], dtype=float),
        )
        data["printed"] = ("line", numpy.asarray(printed, dtype=str))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_b2_event_selection.py:348-385",
    fixtures=[
        "ora_events_mrms",
        "ora_events_aorc",
        "ora_events_ncei",
        "ora_series_mrms",
        "ora_series_aorc",
        "ora_series_ncei",
    ],
    draws="unseeded",
)
def declare_event_source_comparison_cell(fixtures):
    """Run the cell that charts each source's events per year and compares the sources' event statistics by bootstrap and by histogram; reduce to the figures it saves, split into what does and does not depend on the draw.

    The cell calls `plot_annual_rainfall_totals` three times and `compare_2_distributions` four times (two event
    statistics against two other sources). The second function resamples with pandas `sample` and no seed, so each
    p-value figure's panel titles and bars depend on the draw and are components of their own, named with `draw`.
    """
    import hashlib
    import pathlib

    import matplotlib
    import numpy
    import pandas
    import seaborn
    import xarray
    from matplotlib import patches as mpatches
    from scipy import stats

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"

    saved = []  # one entry per figure the callees saved: the path they named, the save's keywords and what the figure holds
    folders = []  # one entry per folder the cell asked for

    def figure_record(figure):
        """What a figure holds: the super title, the caption texts, and for each axes its title, labels, ticks, limits, bars, lines, texts and legend."""
        suptitle = figure._suptitle.get_text() if figure._suptitle is not None else ""
        axes_records = []
        for axes in figure.axes:
            legend = axes.get_legend()
            axes_records.append(
                {
                    "title": axes.get_title(),
                    "xlabel": axes.get_xlabel(),
                    "ylabel": axes.get_ylabel(),
                    "ticks": [t.get_text() for t in axes.get_xticklabels()],
                    "xlim": [float(v) for v in axes.get_xlim()],
                    "bars": [
                        (float(b.get_x()), float(b.get_width()), float(b.get_height()))
                        for b in axes.patches
                        if isinstance(b, matplotlib.patches.Rectangle)
                    ],
                    "lines": [[float(v) for v in numpy.asarray(line.get_ydata(), dtype=float)] for line in axes.lines],
                    "texts": [t.get_text() for t in axes.texts],
                    "legend": [] if legend is None else [t.get_text() for t in legend.get_texts()],
                }
            )
        return {"suptitle": suptitle, "captions": [t.get_text() for t in figure.texts], "axes": axes_records}

    class Plt:
        """The `plt` the callees draw with: real figures in memory, a record of each save, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def tight_layout(self, *args, **kwargs):
            """Lay the current figure out."""
            pyplot.tight_layout(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, with the save's keyword arguments, and write no file."""
            saved.append((str(path), repr(sorted(kwargs.items())), figure_record(pyplot.gcf())))

        def clf(self):
            """Clear the current figure, as the legacy callees do after each save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    class PathBinding:
        """`Path` as the legacy callee names it: a call returns a handle whose `mkdir` is recorded and creates nothing."""

        def __call__(self, path):
            """A handle for `path`."""

            class Handle:
                """A path whose `mkdir` records the request."""

                def mkdir(self, *args, **kwargs):
                    """Record the folder asked for."""
                    folders.append(f"{path}|{sorted(kwargs.items())}")

            return Handle()

    class Sys:
        """The `sys` the legacy texts read: `exit` raises `SystemExit`, as the legacy call does; nothing else is modelled."""

        def exit(self, *args):
            """Exit as legacy does: no register stop marker lies in the texts this lift runs."""
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    callee_labels = {
        ("_utils", "857-921"): ("stored-text:1:3e0cbd9c540ac11b9f87e7993da1f4146b429e3ae6d54c828e9bfd189dce1c8e", 857),
        ("_utils", "922-990"): ("stored-text:1:cec2082f2179b8214be6828a5a2b22b73d52c27a5e247bf7492853a5841a587a", 922),
        ("_utils", "991-1055"): ("stored-text:1:3ba1b0d48bc7d0330a6f5120079e390e678349ac5cc20ebec05fad50b6e5fc2e", 991),
        ("_utils", "1578-1595"): (
            "stored-text:1:ba41404a980520f68941da806da59c472a8582911ec73544dbb9612038d49433",
            1578,
        ),
    }
    namespace = {
        "np": numpy,
        "pd": pandas,
        "sys": Sys(),
        "plt": Plt(),
        "sns": seaborn,
        "mpatches": mpatches,
        "stats": stats,
        "Path": PathBinding(),
    }
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    summaries = {
        "mrms": fixtures["ora_events_mrms"].copy(),
        "aorc": fixtures["ora_events_aorc"].copy(),
        "ncei": fixtures["ora_events_ncei"].copy(),
    }
    series = {name: fixtures[f"ora_series_{name}"].copy() for name in summaries}

    def rate(name):
        """Events per year: the events selected over the years the series spans."""
        return len(summaries[name]) / len(series[name].index.year.unique())

    bindings = {
        "alpha": 0.05,  # the legacy script's own constant (`_b2_event_selection.py`, line 14)
        "aorc_event_return_rate_n_per_year": rate("aorc"),
        "compare_2_distributions": namespace["compare_2_distributions"],
        "df_aorc_rainevent_summary": summaries["aorc"],
        "df_mrms_rainevent_summary": summaries["mrms"],
        "df_ncei_hrly_rainevent_summary": summaries["ncei"],
        "fldr_plt_selected_events": "plots/selected/",
        "mrms_event_return_rate_n_per_year": rate("mrms"),
        "ncei_hourly_event_return_rate_n_per_year": rate("ncei"),
        "new_data_columns": ["max_1hr_mean_mm_per_hr"],  # the cell appends `depth_mm` to this list
        "plot_annual_rainfall_totals": namespace["plot_annual_rainfall_totals"],
        "rain_event_threshold_mm_selected": 55.0,
        "s_aorc_rainfall_mm_per_hr": series["aorc"],
        "s_mrms_rainfall_mm_per_hr": series["mrms"],
        "s_rainfall_ncei_hrly_mrms_res_mm_per_hr": series["ncei"],
    }

    def reduce(finished):
        """The folders asked for and the saved figures, split into what does and does not depend on the draw."""
        pyplot.close("all")
        data = {"folders": ("folder", numpy.asarray(folders, dtype=str)), "n_figures": ((), len(saved))}
        for number, (path, kwargs, figure) in enumerate(saved):
            tag = f"fig{number:02d}"
            data[f"{tag}__path"] = ((), path)
            data[f"{tag}__kwargs"] = ((), kwargs)
            data[f"{tag}__suptitle"] = ((), figure["suptitle"])
            data[f"{tag}__captions"] = (f"{tag}__caption", numpy.asarray(figure["captions"], dtype=str))
            axes = figure["axes"]
            data[f"{tag}__n_axes"] = ((), len(axes))
            data[f"{tag}__xlabels"] = (f"{tag}__axis", numpy.asarray([a["xlabel"] for a in axes], dtype=str))
            data[f"{tag}__ylabels"] = (f"{tag}__axis", numpy.asarray([a["ylabel"] for a in axes], dtype=str))
            data[f"{tag}__xlims"] = ((f"{tag}__axis", "limit"), numpy.asarray([a["xlim"] for a in axes], dtype=float))
            if len(axes) > 1:
                # a p-value figure: one panel per test; the test names do not depend on the draw, the rates and bars do
                data[f"{tag}__test_names"] = (
                    f"{tag}__axis",
                    numpy.asarray([a["title"].split("\n")[0] for a in axes], dtype=str),
                )
                data[f"{tag}__draw_titles"] = (f"{tag}__axis", numpy.asarray([a["title"] for a in axes], dtype=str))
                data[f"{tag}__draw_bars"] = (
                    (f"{tag}__draw_bar", "bar_field"),
                    numpy.asarray([bar for a in axes for bar in a["bars"]], dtype=float).reshape(-1, 3),
                )
            else:
                only = axes[0]
                data[f"{tag}__title"] = ((), only["title"])
                data[f"{tag}__ticks"] = (f"{tag}__tick", numpy.asarray(only["ticks"], dtype=str))
                data[f"{tag}__bars"] = (
                    (f"{tag}__bar", "bar_field"),
                    numpy.asarray(only["bars"], dtype=float).reshape(-1, 3),
                )
                data[f"{tag}__lines"] = (
                    (f"{tag}__line", f"{tag}__point"),
                    numpy.asarray(only["lines"], dtype=float).reshape(len(only["lines"]), -1)
                    if only["lines"]
                    else numpy.zeros((0, 0)),
                )
                data[f"{tag}__texts"] = (f"{tag}__text", numpy.asarray(only["texts"], dtype=str))
                data[f"{tag}__legend"] = (f"{tag}__entry", numpy.asarray(only["legend"], dtype=str))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_b3_event_analysis.py:42-51", fixtures=["ora_mrms"], draws="none")
def declare_radar_rainfall_loading_cell(fixtures):
    """Run the cell that loads the radar rainfall table and derives its series and time step; reduce to what it builds, packed with the time stamps."""
    import numpy
    import pandas
    import xarray

    class PdBinding:
        """pandas as the legacy cell names it, except that `read_csv` returns the fixture table the named file stands for."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The fixture table for the csv the legacy code reads; any other file is an error."""
            if str(path) != "f_mrms_rainfall.csv":
                raise FileNotFoundError(f"the lift reads no csv named {path}")
            return fixtures["ora_mrms"].copy()

    bindings = {"np": numpy, "pd": PdBinding(), "f_mrms_rainfall": "f_mrms_rainfall.csv"}

    def reduce(finished):
        """The bias-corrected series, the rate series and the depth series with their time stamps, and the time step."""
        data = {}
        for name in ("s_mrms_bias_corrected", "s_mrms_rainfall_mm_per_hr", "s_mrms_rainfall_mm"):
            series = finished[name]
            data[f"{name}__index"] = (f"{name}__time", numpy.asarray(series.index.to_numpy()))
            data[f"{name}__values"] = (f"{name}__time", series.to_numpy(dtype=float))
            data[f"{name}__name"] = ((), str(series.name))
            data[f"{name}__index_name"] = ((), str(series.index.name))
        data["tstep_mrms"] = ((), float(finished["tstep_mrms"] / numpy.timedelta64(1, "h")))
        data["tstep_mrms_min"] = ((), float(finished["tstep_mrms_min"]))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_b3_event_analysis.py:52-61", fixtures=["ora_mrms"], draws="none")
def declare_radar_rainfall_chart_cell(fixtures):
    """Run the cell that charts the radar rainfall depth series from 2015 on; reduce to what the figure holds."""
    import matplotlib
    import numpy
    import xarray

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    figures = []  # the figure the cell creates

    class Plt:
        """The `plt` the cell draws with: a real figure in memory, kept for the reduction, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes, remembered."""
            figure, axes = pyplot.subplots(*args, **kwargs)
            figures.append(figure)
            return figure, axes

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    depth = fixtures["ora_mrms"]["rain_mean"].copy()
    depth.name = "mm"

    def reduce(finished):
        """The size of the figure, its axis labels and tick labels, and the line it draws."""
        figure = figures[0]
        axes = figure.axes[0]
        line = axes.lines[0]
        data = {
            "size": ("dimension", numpy.asarray([*figure.get_size_inches(), figure.get_dpi()], dtype=float)),
            "xlabel": ((), axes.get_xlabel()),
            "ylabel": ((), axes.get_ylabel()),
            "xticklabels": ("xtick", numpy.asarray([t.get_text() for t in axes.get_xticklabels()], dtype=str)),
            "yticklabels": ("ytick", numpy.asarray([t.get_text() for t in axes.get_yticklabels()], dtype=str)),
            "line_y": ("point", numpy.asarray(line.get_ydata(), dtype=float)),
            "n_lines": ((), len(axes.lines)),
        }
        pyplot.close("all")
        return xarray.Dataset(data)

    return {"plt": Plt(), "s_mrms_rainfall_mm": depth}, reduce


@declares_lift(
    unit="_b3_event_analysis.py:135-220",
    fixtures=[
        "oea_wl_mrms_tseries",
        "oea_wl_aorc_tseries",
        "oea_wl_mrms_summary",
        "oea_wl_aorc_summary",
        "oea_partial_tseries_mrms",
        "oea_partial_tseries_aorc",
        "oea_partial_summary_mrms",
        "oea_partial_summary_aorc",
    ],
    draws="none",
)
def declare_data_source_priority_cell(fixtures):
    """Run the cell that combines the radar and reanalysis event series by source priority, on a full-window replacement and on a partial one; reduce each returned table to the overlapped radar event's rows and the rest.

    The cell calls `combine_dataset_according_to_priority` (bound to its stored text), whose register entry at
    `_utils.py:2123` is `LICENSED`: a preferred event that an added lower-priority event only partly overlaps keeps
    its remaining steps, and its summary row still describes its original span, where the new code removes the whole
    preferred event. Developer ruling U-21 (outcome 2) therefore has this lift run the function on two inputs:

    - `full`, the cell's first case: a radar event with no valid rate is replaced by a reanalysis event over its whole
      window, and the departure and the legacy code agree;
    - `partial`, the loses-last-steps case: a radar event whose last three of six steps have no rate is overlapped by a
      reanalysis event over exactly those three steps, so the legacy code keeps its first three steps and its summary
      row while the departure removes the event, and a second radar event later in time is untouched.

    Each returned table is split into two named components: the overlapped radar event's rows, selected by its data
    source and its original span (`first stamp` to `last stamp`, and `event_start` equal to its first stamp in the
    summary table), which the fixtures fix; and the rest. In the rest, `event_id` is replaced by the event's rank by
    start time, the map taken from the time series and applied to both tables, because the departure renumbers the
    events that follow it and changes nothing else about their ids. The test compares the rest and drops the
    overlapped rows by name; the registered callable applies the same split key and the same renumbering to the new
    side's tables, so a new side that disturbs any other event's rows, summaries or ids fails.
    """
    import hashlib
    import pathlib

    import numpy
    import pandas
    import xarray

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"

    class Sys:
        """The `sys` the legacy texts read: `exit` raises `SystemExit`, as the legacy call does; nothing else is modelled."""

        def exit(self, *args):
            """Exit as legacy does: no register stop marker lies in the texts this lift runs."""
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    callee_labels = {
        ("_utils", "2075-2153"): (
            "stored-text:1:e80dd910e6746f551b11ace4b82929f8bb3db7c58232d14bbfee8a836925ca84",
            2075,
        ),
    }
    namespace = {"pd": pandas, "sys": Sys()}
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    def pack(data, name, table):
        """Add a table to `data` as one variable per column on its own row dimension, with its index first; dates stay dates, numbers numbers, anything else text."""
        dimension = f"{name}__row"
        table = table.reset_index(drop=True)
        for column in table.columns:
            values = table[column]
            kind = pandas.api.types.infer_dtype(values, skipna=True)
            if kind in ("datetime64", "datetime"):
                data[f"{name}__{column}"] = (dimension, pandas.to_datetime(values).to_numpy())
            elif kind in ("integer", "floating", "mixed-integer-float", "decimal"):
                data[f"{name}__{column}"] = (dimension, values.to_numpy(dtype=float))
            elif kind == "boolean":
                data[f"{name}__{column}"] = (dimension, values.to_numpy(dtype=bool))
            else:
                data[f"{name}__{column}"] = (dimension, values.astype(str).to_numpy())

    def overlapped_spans(combined_series, preferred, source, added_source):
        """The preferred events an added event that the run kept overlaps, as (source, first stamp, last stamp).

        Read from the combined series (which added events survived) and the input fixtures (the preferred events'
        original spans), so a registered callable can apply the same key to its own tables from its own inputs.
        """
        kept = []
        for _identifier, added_event in combined_series[combined_series["rain_data_source"] == added_source].groupby(
            "event_id"
        ):
            kept.append((added_event["date_time"].min(), added_event["date_time"].max()))
        spans = set()
        for _identifier, preferred_event in preferred.groupby("event_id"):
            if preferred_event["rain_data_source"].iloc[0] != source:
                continue
            first, last = preferred_event["date_time"].min(), preferred_event["date_time"].max()
            if any(first <= kept_last and last >= kept_first for kept_first, kept_last in kept):
                spans.add((source, first, last))
        return sorted(spans)

    def split(series, summaries, spans):
        """The overlapped preferred event's rows of each table, and the rest; the summary rows keyed by their `event_start` equalling the event's first stamp."""
        in_series = numpy.zeros(len(series), dtype=bool)
        in_summaries = numpy.zeros(len(summaries), dtype=bool)
        for source, first, last in spans:
            in_series |= (
                (series["rain_data_source"] == source) & (series["date_time"] >= first) & (series["date_time"] <= last)
            )
            in_summaries |= (summaries["rain_data_source"] == source) & (summaries["event_start"] == first)
        return series[in_series], summaries[in_summaries], series[~in_series], summaries[~in_summaries]

    def renumber(series, summaries):
        """`event_id` replaced by the event's rank by start time, the map taken from the time series and applied to both tables.

        A tie in start time keeps the order the events appear in the time series, so the map is a function of the inputs alone.
        """
        starts = series.groupby("event_id")["date_time"].min().sort_values(kind="stable")
        ranks = {event_id: rank for rank, event_id in enumerate(starts.index)}
        unmapped = sorted(set(summaries["event_id"]) - set(ranks))
        if unmapped:
            raise ValueError(f"the summary rows carry event ids their time series do not: {unmapped}")
        return series.assign(event_id=series["event_id"].map(ranks)), summaries.assign(
            event_id=summaries["event_id"].map(ranks)
        )

    cases = {
        "full": (
            fixtures["oea_wl_mrms_tseries"],
            fixtures["oea_wl_aorc_tseries"],
            fixtures["oea_wl_mrms_summary"],
            fixtures["oea_wl_aorc_summary"],
        ),
        "partial": (
            fixtures["oea_partial_tseries_mrms"],
            fixtures["oea_partial_tseries_aorc"],
            fixtures["oea_partial_summary_mrms"],
            fixtures["oea_partial_summary_aorc"],
        ),
    }
    # the four input tables are declared here, because the runner requires every name the text reads to be bound; the
    # reduction rebinds them per case, so these values are the full case's and are replaced before each run
    bindings = {
        "combine_dataset_according_to_priority": namespace["combine_dataset_according_to_priority"],
        "df_6min_wlevel_mrms_event_tseries": cases["full"][0].copy(),
        "df_6min_wlevel_aorc_event_tseries": cases["full"][1].copy(),
        "df_6min_wlevel_mrms_event_summary_stats": cases["full"][2].copy(),
        "df_6min_wlevel_aorc_event_summary_stats": cases["full"][3].copy(),
    }

    def reduce(finished):
        """For each case the overlapped event's rows and the rest of each table, the rest's ids renumbered; and the time axis of the full case, which the cell leaves last."""
        function = finished["combine_dataset_according_to_priority"]
        data = {}
        axis = None
        for case in ("partial", "full"):  # the full case runs last, so that the cell's last statement is its axis
            series, added, summaries, added_summaries = cases[case]
            combined_series, combined_summaries = function(
                "rain_data_source",
                [series.copy(), added.copy()],
                "mm_per_hr",
                [summaries.copy(), added_summaries.copy()],
            )
            spans = overlapped_spans(combined_series, series, "mrms", "aorc")
            overlapped_series, overlapped_summaries, rest_series, rest_summaries = split(
                combined_series, combined_summaries, spans
            )
            rest_series, rest_summaries = renumber(rest_series, rest_summaries)
            pack(data, f"{case}_tseries__overlapped", overlapped_series)
            pack(data, f"{case}_summaries__overlapped", overlapped_summaries)
            pack(data, f"{case}_tseries__rest", rest_series)
            pack(data, f"{case}_summaries__rest", rest_summaries)
            if case == "full":
                axis = combined_series.set_index(["date_time"]).to_xarray()
        data["axis__date_time"] = ("axis_row", numpy.asarray(axis["date_time"].to_numpy()))
        for name in axis.data_vars:
            values = axis[name].to_numpy()
            data[f"axis__{name}"] = (
                "axis_row",
                values.astype(float) if values.dtype.kind in "fiu" else values.astype(str),
            )
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_b3_event_analysis.py:228-261,270-296",
    fixtures=[
        "oea_summary_mrms",
        "oea_summary_aorc",
        "oea_summary_ncei",
        "oea_tseries_mrms",
        "oea_tseries_aorc",
        "oea_tseries_ncei",
    ],
    draws="none",
)
def declare_event_statistics_comparison_cells(fixtures):
    """Run the two cells that compute the event rain statistics of three sources and compare them; reduce to the statistics tables, the figures saved and the lines printed.

    The statistics function (`_utils.py:1540-1577`) begins its loop with an unconditional stop that the register rules
    `OBLIGED`; the `sys` recorder passes it at that line, as the run behind the kept outputs did. `compare_stats`
    (`_utils.py:2026-2059`, with `perform_statistical_test`) tests two samples and, when asked, draws a pair of histograms.
    """
    import hashlib
    import inspect
    import pathlib
    import tomllib

    import matplotlib
    import numpy
    import pandas
    import xarray
    from scipy import stats

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
    saved = []  # one entry per figure the callee saved: the path, the save's keywords and what the figure holds
    printed = []  # the lines the cells print

    def figure_record(figure):
        """What a figure holds: the super title, the caption and, for each axes, its title and bars."""
        axes_records = []
        for axes in figure.axes:
            bars = [b for b in axes.patches if isinstance(b, matplotlib.patches.Rectangle)]
            axes_records.append(
                {
                    "title": axes.get_title(),
                    "bars": [v for b in bars for v in (float(b.get_x()), float(b.get_width()), float(b.get_height()))],
                }
            )
        return {
            "suptitle": figure._suptitle.get_text() if figure._suptitle is not None else "",
            "captions": [t.get_text() for t in figure.texts],
            "axes": axes_records,
        }

    class Plt:
        """The `plt` the callee draws with: real figures in memory, a record of each save, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, with the save's keyword arguments; write no file."""
            saved.append((str(path), repr(sorted(kwargs.items())), figure_record(pyplot.gcf())))

        def clf(self):
            """Clear the current figure, as the legacy callee does after each save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    tables = {
        "mrms_summary.csv": "oea_summary_mrms",
        "mrms_tseries.csv": "oea_tseries_mrms",
        "aorc_summary.csv": "oea_summary_aorc",
        "aorc_tseries.csv": "oea_tseries_aorc",
        "ncei_summary.csv": "oea_summary_ncei",
        "ncei_tseries.csv": "oea_tseries_ncei",
    }

    class PdBinding:
        """pandas as the legacy cells name it, except that `read_csv` returns the fixture table the named file stands for."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The fixture table for the csv the legacy code reads; any other file is an error."""
            if str(path) not in tables:
                raise FileNotFoundError(f"the lift reads no csv named {path}")
            return fixtures[tables[str(path)]].copy()

    def record(*arguments, **keywords):
        """The `print` the cells read: keep the line and print nothing."""
        printed.append(" ".join(str(argument) for argument in arguments))

    class Sys:
        """The `sys` the legacy texts read: `exit` is passed at a register stop-marker line and raises anywhere else."""

        def exit(self, *args):
            """Return at a stop-marker line, as the run behind the kept outputs did; else exit as legacy does."""
            frame = inspect.currentframe().f_back
            name = pathlib.Path(frame.f_code.co_filename)
            where = (name.name if name.suffix == ".py" else name.parent.name + ".py", frame.f_lineno)
            if where in stop_lines:
                return None
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    unit_texts = [
        ("_b3_event_analysis.py", 228, 261),
        ("_b3_event_analysis.py", 270, 296),
        ("_utils.py", 1540, 1577),
        ("_utils.py", 2017, 2025),
        ("_utils.py", 2026, 2059),
    ]
    stop_lines = [
        ("_utils.py", 1563)
    ]  # (file, line) of every register stop marker inside those texts, passed by the recorder
    expected = sorted(
        (entry["key"].split(":")[0], entry["line"])
        for entry in tomllib.loads((root / "tests/correspondence/known_defects.toml").read_text(encoding="utf-8"))[
            "entry"
        ]
        if entry["kind"] == "stop marker"
        and any(
            entry["key"].startswith(file + ":") and first <= entry["line"] <= last for file, first, last in unit_texts
        )
    )
    if expected != sorted(stop_lines):
        raise ValueError(
            f"the register's stop markers inside the texts this lift runs are {expected}, not {sorted(stop_lines)}"
        )

    callee_labels = {
        ("_utils", "1540-1577"): (
            "stored-text:1:67673605a5435a5c38e3a5117650376841ed64ee1f9a175eb5bdc3cf5616268b",
            1540,
        ),
        ("_utils", "2017-2025"): (
            "stored-text:1:6cdad4daa1f181957c5911eeebcb7fafc93dea6c20b5bf4fab6006010dff9ba7",
            2017,
        ),
        ("_utils", "2026-2059"): (
            "stored-text:1:959ef8eaa1cfeb59d590d1feace240178ffb6ccd29636d07ecc46aa95dc2e62f",
            2026,
        ),
    }
    namespace = {"np": numpy, "pd": pandas, "stats": stats, "plt": Plt(), "sys": Sys()}
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    bindings = {
        "compare_stats": namespace["compare_stats"],
        "compute_event_timeseries_statistics": namespace["compute_event_timeseries_statistics"],
        "dir_event_slxn_validing_distributions": "plots/distributions/",
        "f_aorc_event_summaries": "aorc_summary.csv",
        "f_aorc_event_timeseries": "aorc_tseries.csv",
        "f_mrms_event_summaries": "mrms_summary.csv",
        "f_mrms_event_timeseries": "mrms_tseries.csv",
        "f_ncei_hrly_event_summaries": "ncei_summary.csv",
        "f_ncei_hrly_event_timeseries": "ncei_tseries.csv",
        "pd": PdBinding(),
        "print": record,
    }

    def pack(data, name, table):
        """Add a table to `data` as one variable per column on its own row dimension, with its index first; dates stay dates, numbers numbers, anything else text."""
        dimension = f"{name}__row"
        table = table.reset_index()
        for column in table.columns:
            values = table[column]
            kind = pandas.api.types.infer_dtype(values, skipna=True)
            if kind in ("datetime64", "datetime"):
                data[f"{name}__{column}"] = (dimension, pandas.to_datetime(values).to_numpy())
            elif kind in ("integer", "floating", "mixed-integer-float", "decimal"):
                data[f"{name}__{column}"] = (dimension, values.to_numpy(dtype=float))
            elif kind == "boolean":
                data[f"{name}__{column}"] = (dimension, values.to_numpy(dtype=bool))
            else:
                data[f"{name}__{column}"] = (dimension, values.astype(str).to_numpy())

    def reduce(finished):
        """The statistics table of each source, the figures saved and the lines printed."""
        pyplot.close("all")
        data = {"printed": ("line", numpy.asarray(printed, dtype=str)), "n_figures": ((), len(saved))}
        for name in ("mrms", "aorc", "ncei_hrly"):
            pack(data, f"stats_{name}", finished[f"df_stats_{name}"])
        for number, (path, kwargs, figure) in enumerate(saved):
            tag = f"fig{number:02d}"
            data[f"{tag}__path"] = ((), path)
            data[f"{tag}__kwargs"] = ((), kwargs)
            data[f"{tag}__suptitle"] = ((), figure["suptitle"])
            data[f"{tag}__captions"] = (f"{tag}__caption", numpy.asarray(figure["captions"], dtype=str))
            axes = figure["axes"]
            data[f"{tag}__titles"] = (f"{tag}__axis", numpy.asarray([a["title"] for a in axes], dtype=str))
            data[f"{tag}__bars"] = (f"{tag}__bar", numpy.asarray([x for a in axes for x in a["bars"]], dtype=float))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_b6_simulate_from_vincop.py:1-99",
    form="adapted",
    fixtures=[
        "orb_combined_events",
        "orb_combined_tseries",
        "ora_mrms",
        "ora_wl_6min",
        "ora_aorc",
        "orb_wlevel_resampled",
        "orb_vinecop_json",
    ],
    draws="none",
)
def declare_simulation_script_preamble(fixtures):
    """Run the simulation script's preamble cell on the invented inputs; reduce to the tables, series, dates and copulas it loads and builds.

    The cell's two star-imports of the legacy inputs and utilities modules are replaced by comments in the adapted copy
    (the runner refuses a star-import), and the eleven names they supplied are bound here: the six file paths, the two
    directories, the number of years and the `xarray` module. The cell reads its inputs from real files, so the
    declaration writes the fixture tables as the csv files the cell names, and the stored copula text as the three
    `vinecop_params_*.json` files, into a scratch directory it removes at the end; nothing else is written, and the
    committed fixtures are the source of the content. The cell imports `numpy`, `pandas`, `pyvinecopulib` and `pyplot`
    itself, so those names are the installed libraries. `np.random.RandomState(234)` builds the legacy generator and
    draws nothing.
    """
    import pathlib
    import shutil
    import tempfile

    import numpy
    import pandas
    import xarray

    workdir = tempfile.mkdtemp(prefix="observed-record-assembly-lift-")  # the run's own scratch directory
    root = pathlib.Path(workdir)
    (root / "sim").mkdir()
    (root / "vinecop").mkdir()
    folders = []  # one entry per folder the cell asked for

    class PathBinding:
        """`Path` as the legacy cell names it: a call returns a handle whose `mkdir` is recorded and made inside the scratch directory."""

        def __call__(self, path):
            """A handle for `path`."""

            class Handle:
                """A path whose `mkdir` records the request and makes the directory."""

                def mkdir(self, *args, **kwargs):
                    """Record the folder asked for and make it."""
                    folders.append(f"{path}|{sorted(kwargs.items())}")
                    pathlib.Path(path).mkdir(*args, **kwargs)

            return Handle()

    files = {
        "combined_event_summaries.csv": fixtures["orb_combined_events"],
        "combined_event_tseries.csv": fixtures["orb_combined_tseries"],
        "mrms_rainfall.csv": fixtures["ora_mrms"],
        "aorc_rainfall.csv": fixtures["ora_aorc"],
        "water_level_storm_surge.csv": fixtures["ora_wl_6min"],
    }
    for name, table in files.items():
        table.to_csv(root / name, index=True)
    for key in ("compound_events", "rain_events", "surge_events"):
        (root / "vinecop" / f"vinecop_params_{key}.json").write_text(
            str(fixtures["orb_vinecop_json"][0]), encoding="utf-8"
        )

    class XrBinding:
        """`xarray` as the legacy cell names it, except that `open_dataset` returns the fixture dataset for the one netCDF file the cell opens."""

        def __getattr__(self, name):
            """Everything but `open_dataset` is xarray's own."""
            return getattr(xarray, name)

        def open_dataset(self, path, **kwargs):
            """The fixture dataset, for the one file the cell opens; any other file is an error."""
            if not str(path).endswith("wlevel_resampled.nc"):
                raise FileNotFoundError(f"the lift opens no dataset named {path}")
            return fixtures["orb_wlevel_resampled"].copy(deep=True)

    bindings = {
        "Path": PathBinding(),
        "dir_simulate_from_copula": f"{workdir}/sim/",
        "dir_vinecop": f"{workdir}/vinecop/",
        "f_combined_event_summaries": f"{workdir}/combined_event_summaries.csv",
        "f_combined_event_tseries_csv": f"{workdir}/combined_event_tseries.csv",
        "f_mrms_rainfall": f"{workdir}/mrms_rainfall.csv",
        "f_aorc_rainfall": f"{workdir}/aorc_rainfall.csv",
        "f_water_level_storm_surge": f"{workdir}/water_level_storm_surge.csv",
        "f_6min_wlevel_resampled_to_mrms_res_nc": f"{workdir}/wlevel_resampled.nc",
        "n_years_of_weather_to_generate": 20,
        "xr": XrBinding(),
    }

    def pack(data, name, table):
        """Add a Series or DataFrame to `data` as one variable per column on its own row dimension; dates stay dates, numbers numbers, anything else text."""
        dimension = f"{name}__row"
        frame = table.to_frame() if isinstance(table, pandas.Series) else table
        frame = frame.reset_index(drop=True)
        for column in frame.columns:
            values = frame[column]
            kind = pandas.api.types.infer_dtype(values, skipna=True)
            if kind in ("datetime64", "datetime"):
                data[f"{name}__{column}"] = (dimension, pandas.to_datetime(values).to_numpy())
            elif kind in ("integer", "floating", "mixed-integer-float", "decimal"):
                data[f"{name}__{column}"] = (dimension, values.to_numpy(dtype=float))
            elif kind == "boolean":
                data[f"{name}__{column}"] = (dimension, values.to_numpy(dtype=bool))
            else:
                data[f"{name}__{column}"] = (dimension, values.astype(str).to_numpy())

    def dates(data, name, index):
        """Add a `DatetimeIndex` to `data` as its own dimension."""
        data[f"{name}__index"] = (f"{name}__date", numpy.asarray(index.to_numpy()))

    def dated(data, name, series):
        """Add a series keyed by calendar dates: the dates as timestamps, its values, and its name."""
        data[f"{name}__date"] = (f"{name}__day", pandas.to_datetime(pandas.Index(series.index)).to_numpy())
        data[f"{name}__value"] = (f"{name}__day", series.to_numpy(dtype=str))
        data[f"{name}__name"] = ((), str(series.name))

    def reduce(finished):
        """The tables, series, dates and copulas the cell leaves, and the folders it asked for."""
        data = {
            "n_years_to_simulate": ((), int(finished["n_years_to_simulate"])),
            "alpha": ((), float(finished["alpha"])),
            "random_seed": ((), int(finished["random_seed"])),
            "generator_type": ((), type(finished["random_seed_np"]).__name__),
            "folders": ("folder", numpy.asarray([f.replace(workdir, "<tmp>") for f in folders], dtype=str)),
            "event_stat_keys": ("event_stat_key", numpy.asarray(sorted(finished["dic_event_stats"]), dtype=str)),
            "summary_keys": ("summary_key", numpy.asarray(sorted(finished["dic_event_summaries"]), dtype=str)),
        }
        for name in (
            "df_all_events_summary",
            "df_surge_events",
            "df_rain_events",
            "df_combo_events",
            "df_combined_tseries",
            "df_rain_tseries",
            "df_rainfall",
            "df_6min_water_levels",
            "df_valid_dates",
        ):
            pack(data, name, finished[name])
        for name in ("s_mrms_rainfall_trgt_res_mm_per_hr", "s_aorc_rainfall_trgt_res_mm_per_hr"):
            pack(data, name, finished[name])
        for name in ("mrms_rain_dates", "aorc_rain_dates", "all_rain_dates", "wlevel_dates"):
            dated(data, name, finished[name])
        # `idx_valid_rainfall` comes from concatenating the two date-keyed series and keeps `datetime.date` elements
        data["idx_valid_rainfall__index"] = (
            "idx_valid_rainfall__date",
            pandas.to_datetime(pandas.Index(finished["idx_valid_rainfall"])).to_numpy(),
        )
        dates(data, "idx_valid_dates", finished["idx_valid_dates"])
        dataset = finished["ds_6min_water_levels_trgt_res"]
        data["dataset__date_time"] = ("dataset__step", numpy.asarray(dataset["date_time"].to_numpy()))
        for name in dataset.data_vars:
            data[f"dataset__{name}"] = ("dataset__step", numpy.asarray(dataset[name].values, dtype=float))
        for key in ("compound_events", "rain_events", "surge_events"):
            copula = finished["dic_copulas"][key]
            data[f"copula__{key}__trunc_lvl"] = ((), int(copula.trunc_lvl))
            data[f"copula__{key}__dim"] = ((), int(copula.dim))
        shutil.rmtree(workdir, ignore_errors=True)
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_b6_simulate_from_vincop.py:356-376", fixtures=["orb_annual_counts", "orb_valid_dates"], draws="none"
)
def declare_rejected_periods_cell(fixtures):
    """Run the cell that charts, for each year with many missing days, the valid dates of that year by month; reduce to the series it collects and the figures it saves.

    The cell compares the index of dates with data against `datetime.date` values. This environment's pandas refuses that
    comparison (`Invalid comparison between dtype=datetime64[ns] and date`), so the index is bound to a stand-in that
    reads a `date` operand as midnight of that day and delegates the comparison to the installed library's index; it
    carries no computation of its own. `plt` draws real figures in memory and records each save.
    """
    import datetime

    import matplotlib
    import numpy
    import pandas
    import xarray

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    saved = []  # one entry per figure the cell saved: the path, the save's keywords and what the figure holds

    def figure_record(figure):
        """What a figure holds: the title and the bar heights of its axes."""
        axes = figure.axes[0]
        bars = [b for b in axes.patches if isinstance(b, matplotlib.patches.Rectangle)]
        return {
            "title": axes.get_title(),
            "bars": [float(b.get_height()) for b in bars],
            "ticks": [t.get_text() for t in axes.get_xticklabels()],
        }

    class Plt:
        """The `plt` the cell draws with: real figures in memory, a record of each save, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, with the save's keyword arguments; write no file."""
            saved.append((str(path), repr(sorted(kwargs.items())), figure_record(pyplot.gcf())))

        def clf(self):
            """Clear the current figure, as the legacy cell does after each save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    class ValidDates:
        """The index of dates with data, as the cell compares it: a `datetime.date` operand is read as midnight of that day."""

        def __init__(self, index):
            """Hold the installed library's index."""
            self.index = index

        @staticmethod
        def _as_stamp(other):
            """`other`, with a plain date read as midnight of that day."""
            if isinstance(other, datetime.date) and not isinstance(other, datetime.datetime):
                return pandas.Timestamp(other)
            return other

        def __ge__(self, other):
            """Whether each date is on or after `other`."""
            return self.index >= self._as_stamp(other)

        def __le__(self, other):
            """Whether each date is on or before `other`."""
            return self.index <= self._as_stamp(other)

        def __getitem__(self, item):
            """The dates the mask or position selects."""
            return self.index[item]

        def __len__(self):
            """How many dates the index holds."""
            return len(self.index)

    bindings = {
        "Path": type("PathHandle", (), {"__call__": lambda self, path: self, "mkdir": lambda self, *a, **k: None})(),
        "df_obs_annual_event_counts": fixtures["orb_annual_counts"].copy(),
        "dir_plot": "sim/plots/",
        "idx_valid_dates": ValidDates(pandas.DatetimeIndex(pandas.to_datetime(fixtures["orb_valid_dates"]))),
        "pd": pandas,
        "plt": Plt(),
    }

    def pack(data, name, table):
        """Add a Series or DataFrame to `data` as one variable per column on its own row dimension, with its index first; dates stay dates, numbers numbers, anything else text."""
        dimension = f"{name}__row"
        frame = table.to_frame() if isinstance(table, pandas.Series) else table
        frame = frame.reset_index(drop=True)
        for column in frame.columns:
            values = frame[column]
            kind = pandas.api.types.infer_dtype(values, skipna=True)
            if kind in ("datetime64", "datetime"):
                data[f"{name}__{column}"] = (dimension, pandas.to_datetime(values).to_numpy())
            elif kind in ("integer", "floating", "mixed-integer-float", "decimal"):
                data[f"{name}__{column}"] = (dimension, values.to_numpy(dtype=float))
            elif kind == "boolean":
                data[f"{name}__{column}"] = (dimension, values.to_numpy(dtype=bool))
            else:
                data[f"{name}__{column}"] = (dimension, values.astype(str).to_numpy())

    def reduce(finished):
        """The collected per-period series (dates and validity) and the saved figures."""
        pyplot.close("all")
        data = {
            "n_series": ((), len(finished["lst_all_valid_dates_in_rejected_samples"])),
            "n_figures": ((), len(saved)),
        }
        for number, series in enumerate(finished["lst_all_valid_dates_in_rejected_samples"]):
            data[f"series{number}__date"] = (f"series{number}__day", numpy.asarray(series.index.to_numpy()))
            data[f"series{number}__valid"] = (f"series{number}__day", series.to_numpy(dtype=bool))
            data[f"series{number}__name"] = ((), str(series.name))
        for number, (path, kwargs, figure) in enumerate(saved):
            tag = f"fig{number:02d}"
            data[f"{tag}__path"] = ((), path)
            data[f"{tag}__kwargs"] = ((), kwargs)
            data[f"{tag}__title"] = ((), figure["title"])
            data[f"{tag}__bars"] = (f"{tag}__bar", numpy.asarray(figure["bars"], dtype=float))
            data[f"{tag}__ticks"] = (f"{tag}__tick", numpy.asarray(figure["ticks"], dtype=str))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_b6_simulate_from_vincop.py:377-383", fixtures=["orb_rejected_valid_dates"], draws="none")
def declare_rejected_period_chart_cell(fixtures):
    """Run the cell that charts the valid dates of every rejected period by month; reduce to the figure it saves.

    The cell reads the list of per-period series the previous cell collects, built here from the fixture one series per
    period. `plt` draws a real figure in memory and records the save.
    """
    import matplotlib
    import numpy
    import pandas
    import xarray

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    saved = []  # one entry per figure the cell saved: the path, the save's keywords and what the figure holds

    def figure_record(figure):
        """What a figure holds: the title and the bar heights of its axes."""
        axes = figure.axes[0]
        bars = [b for b in axes.patches if isinstance(b, matplotlib.patches.Rectangle)]
        return {
            "title": axes.get_title(),
            "bars": [float(b.get_height()) for b in bars],
            "ticks": [t.get_text() for t in axes.get_xticklabels()],
        }

    class Plt:
        """The `plt` the cell draws with: real figures in memory, a record of each save, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, with the save's keyword arguments; write no file."""
            saved.append((str(path), repr(sorted(kwargs.items())), figure_record(pyplot.gcf())))

        def clf(self):
            """Clear the current figure, as the legacy cell does after each save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    table = fixtures["orb_rejected_valid_dates"]
    collected = [
        pandas.Series(
            group["valid"].to_numpy(dtype=bool),
            index=pandas.DatetimeIndex(pandas.to_datetime(group["date"])),
            name="valid_date",
        )
        for _period, group in table.groupby("period", sort=True)
    ]
    bindings = {
        "dir_plots_ommitted_time_periods": "sim/plots/event_occurence_simulations/",
        "lst_all_valid_dates_in_rejected_samples": collected,
        "pd": pandas,
        "plt": Plt(),
    }

    def reduce(finished):
        """The one figure the cell saves: its path, the save's keywords, its title, bar heights and tick labels."""
        pyplot.close("all")
        path, kwargs, figure = saved[0]
        return xarray.Dataset(
            {
                "path": ((), path),
                "kwargs": ((), kwargs),
                "title": ((), figure["title"]),
                "bars": ("bar", numpy.asarray(figure["bars"], dtype=float)),
                "ticks": ("tick", numpy.asarray(figure["ticks"], dtype=str)),
            }
        )

    return bindings, reduce


@declares_lift(unit="_utils.py:922-990", fixtures=["ou_monte_carlo_results"], draws="none")
def declare_pvalue_distribution_plot(fixtures):
    """Run the function that charts the bootstrapped p-values of each two-sample test, saved and shown; reduce to what each figure holds.

    The function saves one figure when given a path and shows it otherwise. Its bars, titles and x limits depend only
    on the table it is given, which the fixture fixes.
    """
    import matplotlib
    import numpy
    import seaborn
    import xarray
    from matplotlib import patches as mpatches

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    saved = []  # one entry per figure the function saved or showed

    def figure_record(figure):
        """What a figure holds: its super title, and for each axes its title, labels, x limits, bar heights and legend entries."""
        axes_records = []
        for axes in figure.axes:
            legend = axes.get_legend()
            axes_records.append(
                {
                    "title": axes.get_title(),
                    "labels": [axes.get_xlabel(), axes.get_ylabel()],
                    "xlim": [float(v) for v in axes.get_xlim()],
                    "bars": [
                        float(b.get_height()) for b in axes.patches if isinstance(b, matplotlib.patches.Rectangle)
                    ],
                    "legend": [] if legend is None else [t.get_text() for t in legend.get_texts()],
                }
            )
        return {
            "suptitle": figure._suptitle.get_text() if figure._suptitle is not None else "",
            "captions": [t.get_text() for t in figure.texts],
            "axes": axes_records,
        }

    class Plt:
        """The `plt` the unit draws with: real figures in memory, a record of each save or show, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, with the save's keyword arguments; write no file."""
            saved.append((str(path), repr(sorted(kwargs.items())), figure_record(pyplot.gcf())))

        def show(self):
            """Keep what the current figure holds as shown, and display nothing."""
            saved.append(("<show>", "", figure_record(pyplot.gcf())))

        def tight_layout(self, *args, **kwargs):
            """Lay the current figure out."""
            pyplot.tight_layout(*args, **kwargs)

        def clf(self):
            """Clear the current figure, as the legacy unit does after each save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    bindings = {"np": numpy, "sns": seaborn, "mpatches": mpatches, "plt": Plt()}

    def reduce(finished):
        """The saved figure and the shown figure."""
        results = fixtures["ou_monte_carlo_results"].copy()
        finished["plot_pvalue_distributions"](
            results, 0.05, fname_savefig="plots/pvalues.png", fig_title="Comparison of tests"
        )
        finished["plot_pvalue_distributions"](results, 0.05)
        pyplot.close("all")
        data = {"n_figures": ((), len(saved)), "n_tests": ((), int(results["Test"].nunique()))}
        for number, (path, kwargs, figure) in enumerate(saved):
            tag = f"fig{number:02d}"
            data[f"{tag}__path"] = ((), path)
            data[f"{tag}__kwargs"] = ((), kwargs)
            data[f"{tag}__suptitle"] = ((), figure["suptitle"])
            data[f"{tag}__captions"] = (f"{tag}__caption", numpy.asarray(figure["captions"], dtype=str))
            data[f"{tag}__titles"] = (f"{tag}__axis", numpy.asarray([a["title"] for a in figure["axes"]], dtype=str))
            data[f"{tag}__labels"] = (
                f"{tag}__label",
                numpy.asarray([x for a in figure["axes"] for x in a["labels"]], dtype=str),
            )
            data[f"{tag}__xlims"] = (
                (f"{tag}__axis", "limit"),
                numpy.asarray([a["xlim"] for a in figure["axes"]], dtype=float),
            )
            data[f"{tag}__bars"] = (
                f"{tag}__bar",
                numpy.asarray([x for a in figure["axes"] for x in a["bars"]], dtype=float),
            )
            data[f"{tag}__legend"] = (
                f"{tag}__entry",
                numpy.asarray([x for a in figure["axes"] for x in a["legend"]], dtype=str),
            )
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_utils.py:991-1055",
    fixtures=["ou_two_samples_a", "ou_two_samples_b", "ou_monte_carlo_results"],
    draws="unseeded",
)
def declare_two_distribution_comparison(fixtures):
    """Run the function that compares two samples by a bootstrap and a histogram, for both of its branches; reduce to what does not depend on the bootstrap draw.

    The function resamples both samples with pandas `sample` and no seed (lines 1015 and 1016, `LICENSED` unseeded
    draws), so the value is not stored and what depends on the draw is packed in components named with `draw`. It calls
    `perform_two_sample_tests` and `plot_pvalue_distributions`, bound to their stored texts, and asks for a folder it
    creates, recorded. The components that do not depend on the draw are the paths, the figure and histogram titles
    but the rejection rate in the p-value panels, the axis labels and limits, the histogram bars and the means and
    percentiles the caption states.
    """
    import hashlib
    import pathlib

    import matplotlib
    import numpy
    import pandas
    import seaborn
    import xarray
    from matplotlib import patches as mpatches
    from scipy import stats

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
    saved = []  # one entry per figure the callees saved or showed
    folders = []  # one entry per folder the function asked for

    class PathBinding:
        """`Path` as the legacy function names it: a call returns a handle whose `mkdir` is recorded and creates nothing."""

        def __call__(self, path):
            """A handle for `path`."""

            class Handle:
                """A path whose `mkdir` records the request."""

                def mkdir(self, *args, **kwargs):
                    """Record the folder asked for."""
                    folders.append(f"{path}|{sorted(kwargs.items())}")

            return Handle()

    def figure_record(figure):
        """What a figure holds: its super title, and for each axes its title, labels, x limits, bar heights and legend entries."""
        axes_records = []
        for axes in figure.axes:
            legend = axes.get_legend()
            axes_records.append(
                {
                    "title": axes.get_title(),
                    "labels": [axes.get_xlabel(), axes.get_ylabel()],
                    "xlim": [float(v) for v in axes.get_xlim()],
                    "bars": [
                        float(b.get_height()) for b in axes.patches if isinstance(b, matplotlib.patches.Rectangle)
                    ],
                    "legend": [] if legend is None else [t.get_text() for t in legend.get_texts()],
                }
            )
        return {
            "suptitle": figure._suptitle.get_text() if figure._suptitle is not None else "",
            "captions": [t.get_text() for t in figure.texts],
            "axes": axes_records,
        }

    class Plt:
        """The `plt` the unit draws with: real figures in memory, a record of each save or show, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, with the save's keyword arguments; write no file."""
            saved.append((str(path), repr(sorted(kwargs.items())), figure_record(pyplot.gcf())))

        def show(self):
            """Keep what the current figure holds as shown, and display nothing."""
            saved.append(("<show>", "", figure_record(pyplot.gcf())))

        def tight_layout(self, *args, **kwargs):
            """Lay the current figure out."""
            pyplot.tight_layout(*args, **kwargs)

        def clf(self):
            """Clear the current figure, as the legacy unit does after each save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    callee_labels = {
        ("_utils", "857-921"): ("stored-text:1:3e0cbd9c540ac11b9f87e7993da1f4146b429e3ae6d54c828e9bfd189dce1c8e", 857),
        ("_utils", "922-990"): ("stored-text:1:cec2082f2179b8214be6828a5a2b22b73d52c27a5e247bf7492853a5841a587a", 922),
    }
    namespace = {
        "np": numpy,
        "pd": pandas,
        "stats": stats,
        "sns": seaborn,
        "mpatches": mpatches,
        "plt": Plt(),
        "Path": PathBinding(),
    }
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    bindings = {
        "np": numpy,
        "pd": pandas,
        "plt": Plt(),
        "sns": seaborn,
        "Path": PathBinding(),
        "perform_two_sample_tests": namespace["perform_two_sample_tests"],
        "plot_pvalue_distributions": namespace["plot_pvalue_distributions"],
    }

    def reduce(finished):
        """The two branches' paths and figures, split into what does and does not depend on the bootstrap draw."""
        first = fixtures["ou_two_samples_a"].copy()
        second = fixtures["ou_two_samples_b"].copy()
        finished["compare_2_distributions"](
            first,
            second,
            "depth_mm",
            6,
            12,
            0.05,
            bins=10,
            include_kde=True,
            dir_savefig="plots/compare/",
            figtitle_supplementary_text="(threshold = 55.0mm)",
            fig_fname_prefix="threshold_55mm",
        )
        finished["compare_2_distributions"](
            first,
            second,
            "depth_mm",
            6,
            12,
            0.05,
            bins=10,
            include_kde=False,
            do_bootstrapping=False,
        )
        pyplot.close("all")
        data = {
            "n_figures": ((), len(saved)),
            "folders": ("folder", numpy.asarray(folders, dtype=str)),
            "first_mean": ((), float(first.mean())),
            "second_mean": ((), float(second.mean())),
            "first_median": ((), float(first.median())),
            "second_median": ((), float(second.median())),
        }
        for number, (path, kwargs, figure) in enumerate(saved):
            tag = f"fig{number:02d}"
            data[f"{tag}__path"] = ((), path)
            data[f"{tag}__kwargs"] = ((), kwargs)
            data[f"{tag}__suptitle"] = ((), figure["suptitle"])
            data[f"{tag}__n_axes"] = ((), len(figure["axes"]))
            data[f"{tag}__xlabels"] = (
                f"{tag}__axis",
                numpy.asarray([a["labels"][0] for a in figure["axes"]], dtype=str),
            )
            data[f"{tag}__ylabels"] = (
                f"{tag}__axis",
                numpy.asarray([a["labels"][1] for a in figure["axes"]], dtype=str),
            )
            data[f"{tag}__xlims"] = (
                (f"{tag}__axis", "limit"),
                numpy.asarray([a["xlim"] for a in figure["axes"]], dtype=float),
            )
            if len(figure["axes"]) > 1:
                # a p-value figure: the test names do not depend on the draw, the rates and bars do
                data[f"{tag}__test_names"] = (
                    f"{tag}__axis",
                    numpy.asarray([axis["title"].split("\n")[0] for axis in figure["axes"]], dtype=str),
                )
                data[f"{tag}__draw_titles"] = (
                    f"{tag}__axis",
                    numpy.asarray([axis["title"] for axis in figure["axes"]], dtype=str),
                )
                data[f"{tag}__draw_bars"] = (
                    (f"{tag}__draw_axis", f"{tag}__draw_bar"),
                    numpy.asarray(
                        [[position, height] for position, axis in enumerate(figure["axes"]) for height in axis["bars"]],
                        dtype=float,
                    ).reshape(-1, 2),
                )
            else:
                data[f"{tag}__title"] = ((), figure["axes"][0]["title"])
                data[f"{tag}__bars"] = (f"{tag}__bar", numpy.asarray(figure["axes"][0]["bars"], dtype=float))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_utils.py:991-1055", part="draw-law", fixtures=["ou_two_samples_a", "ou_two_samples_b"], draws="unseeded"
)
def declare_two_distribution_comparison_sampling_law(fixtures):
    """Run the comparison function once with its bootstrap branch, then reduce to the law its two resampling streams follow.

    The text is the unit's own definition, so this declaration binds every name it reads (`np`, `pd`, `plt`, `sns`,
    `Path` and the two callees it calls, compiled from their stored texts) exactly as the value declaration does, and
    then calls `compare_2_distributions` once, with six resamples of twelve picks. That call draws: each resample calls
    `pandas` `sample` with `replace=True` and no `random_state` on each of the two series, so one run of this reduction
    executes twenty-four unseeded draws (twelve pairs), which is what the unit's `unseeded` cell records and what the
    draw-law test certifies. The draw and the figures it would save are discarded; nothing is written, because the
    `Path` stand-in records no folder (the call passes no `dir_savefig`) and the `plt` stand-in's `savefig` writes no
    file.

    The reduction returns the law's parameters, `rows1` and `rows2`, the number of rows each of the two calls draws
    from, read from the two series the legacy call passed: twenty-four each. A registered callable builds the same pair
    from its own `inputs` and draws two independent streams of uniform picks with replacement, one per series, so a new
    side that draws one stream and uses it for both is refused by the law's fourth test.
    """
    import hashlib
    import pathlib

    import matplotlib
    import numpy
    import pandas
    import seaborn
    from matplotlib import patches as mpatches
    from scipy import stats

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"

    class PathBinding:
        """`Path` as the legacy function names it: a call returns a handle whose `mkdir` records the folder and creates none."""

        def __init__(self):
            """Hold the folders asked for."""
            self.folders = []

        def __call__(self, path):
            """A handle for `path`."""
            folders = self.folders

            class Handle:
                """A path whose `mkdir` records the request and creates nothing."""

                def mkdir(self, *args, **kwargs):
                    """Record the folder asked for."""
                    folders.append(f"{path}|{sorted(kwargs.items())}")

            return Handle()

    class Plt:
        """The `plt` the unit draws with: real figures in memory, nothing recorded and nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Write no file."""

        def show(self):
            """Display nothing."""

        def tight_layout(self, *args, **kwargs):
            """Lay the current figure out."""
            pyplot.tight_layout(*args, **kwargs)

        def clf(self):
            """Clear the current figure."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    plt_binding = Plt()
    path_binding = PathBinding()

    callee_labels = {
        ("_utils", "857-921"): ("stored-text:1:3e0cbd9c540ac11b9f87e7993da1f4146b429e3ae6d54c828e9bfd189dce1c8e", 857),
        ("_utils", "922-990"): ("stored-text:1:cec2082f2179b8214be6828a5a2b22b73d52c27a5e247bf7492853a5841a587a", 922),
    }
    namespace = {
        "np": numpy,
        "pd": pandas,
        "stats": stats,
        "sns": seaborn,
        "mpatches": mpatches,
        "plt": Plt(),
        "Path": PathBinding(),
    }
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    bindings = {
        "np": numpy,
        "pd": pandas,
        "plt": plt_binding,
        "sns": seaborn,
        "Path": path_binding,
        "perform_two_sample_tests": namespace["perform_two_sample_tests"],
        "plot_pvalue_distributions": namespace["plot_pvalue_distributions"],
    }

    def reduce(finished):
        """Call the function once with its bootstrap branch, then return the two row counts its resamples draw from."""
        first = fixtures["ou_two_samples_a"].copy()
        second = fixtures["ou_two_samples_b"].copy()
        finished["compare_2_distributions"](first, second, "depth_mm", 6, 12, 0.05, bins=10, include_kde=False)
        pyplot.close("all")
        if path_binding.folders:
            raise ValueError(
                f"this declaration's call passes no save folder, but one was asked for: {path_binding.folders}"
            )
        return pandas.Series({"rows1": int(len(first)), "rows2": int(len(second))})

    return bindings, reduce


@declares_lift(unit="_utils.py:1990-2016", fixtures=["ou_ncei_hourly"], draws="none")
def declare_process_ncei_data(fixtures):
    """Run the function that puts an airport hourly precipitation series on a target step in millimetres per hour; reduce to the two series it returns.

    The function localizes the index to eastern standard time, converts inches to millimetres, resamples to the target
    step, converts to UTC, excludes the years it is told to, and returns the rate and the depth per step. It reads
    `mm_per_inch`, a constant of the legacy inputs script (`_inputs.py`, line 10), bound here.
    """
    import numpy
    import pandas
    import xarray

    hourly = fixtures["ou_ncei_hourly"].loc[:, ["DATE", "HPCP"]].drop_duplicates()
    series = hourly.set_index("DATE")["HPCP"]
    series.name = "HPCP"

    def pack(data, name, values):
        """Add a series to `data` with its time stamps and its name."""
        data[f"{name}__index"] = (f"{name}__time", numpy.asarray(values.index.to_numpy()))
        data[f"{name}__values"] = (f"{name}__time", values.to_numpy(dtype=float))
        data[f"{name}__name"] = ((), str(values.name))

    def reduce(finished):
        """The rate and depth series for an excluded year and for none."""
        data = {}
        for case, exclude in (("included", []), ("excluded", [2014])):
            rate, depth = finished["process_ncei_data"](series.copy(), 5, exclude)
            pack(data, f"{case}__rate", rate)
            pack(data, f"{case}__depth", depth)
        return xarray.Dataset(data)

    return {"np": numpy, "pd": pandas, "mm_per_inch": 25.4}, reduce


@declares_lift(unit="_utils.py:2017-2025", fixtures=["ou_two_samples_a", "ou_two_samples_b"], draws="none")
def declare_perform_statistical_test(fixtures):
    """Run the function that compares two samples under a given test at a given level; reduce to the p-value and the verdict for two tests and three cases.

    The test is a scipy test object passed in, so the same function is run with the rank-sum and the Cramer-von Mises
    two-sample tests, on two samples that differ and on identical samples, and at a level that forces the other branch.
    """
    import xarray
    from scipy import stats

    first = fixtures["ou_two_samples_a"].copy()
    second = fixtures["ou_two_samples_b"].copy()

    def reduce(finished):
        """The p-value and verdict for each test and case."""
        function = finished["perform_statistical_test"]
        data = {}
        for name, x, y, alpha in (
            ("ranksums_differ", first, second, 0.05),
            ("cvm_differ", first, second, 0.05),
            ("ranksums_identical", first, first, 0.05),
            ("ranksums_loose", first, second, 0.99),
        ):
            test = stats.cramervonmises_2samp if name.startswith("cvm") else stats.ranksums
            pvalue, result = function(test, x, y, alpha)
            data[f"{name}__pvalue"] = ((), float(pvalue))
            data[f"{name}__result"] = ((), str(result))
        return xarray.Dataset(data)

    return {}, reduce


@declares_lift(unit="_utils.py:2026-2059", fixtures=["ou_two_samples_a", "ou_two_samples_b"], draws="none")
def declare_compare_stats(fixtures):
    """Run the function that tests two samples and charts their histograms; reduce to the verdicts and what the figure holds.

    It calls `perform_statistical_test` (bound to its stored text) with the Cramer-von Mises two-sample test and the
    rank-sum test, and charts a histogram pair with the means and percentiles in the caption.
    """
    import hashlib
    import pathlib

    import matplotlib
    import numpy
    import pandas
    import xarray
    from scipy import stats

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
    saved = []  # one entry per figure the function saved or showed

    def figure_record(figure):
        """What a figure holds: its super title, and for each axes its title, labels, x limits, bar heights and legend entries."""
        axes_records = []
        for axes in figure.axes:
            legend = axes.get_legend()
            axes_records.append(
                {
                    "title": axes.get_title(),
                    "labels": [axes.get_xlabel(), axes.get_ylabel()],
                    "xlim": [float(v) for v in axes.get_xlim()],
                    "bars": [
                        float(b.get_height()) for b in axes.patches if isinstance(b, matplotlib.patches.Rectangle)
                    ],
                    "legend": [] if legend is None else [t.get_text() for t in legend.get_texts()],
                }
            )
        return {
            "suptitle": figure._suptitle.get_text() if figure._suptitle is not None else "",
            "captions": [t.get_text() for t in figure.texts],
            "axes": axes_records,
        }

    class Plt:
        """The `plt` the unit draws with: real figures in memory, a record of each save or show, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, with the save's keyword arguments; write no file."""
            saved.append((str(path), repr(sorted(kwargs.items())), figure_record(pyplot.gcf())))

        def show(self):
            """Keep what the current figure holds as shown, and display nothing."""
            saved.append(("<show>", "", figure_record(pyplot.gcf())))

        def tight_layout(self, *args, **kwargs):
            """Lay the current figure out."""
            pyplot.tight_layout(*args, **kwargs)

        def clf(self):
            """Clear the current figure, as the legacy unit does after each save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    callee_labels = {
        ("_utils", "2017-2025"): (
            "stored-text:1:6cdad4daa1f181957c5911eeebcb7fafc93dea6c20b5bf4fab6006010dff9ba7",
            2017,
        ),
    }
    namespace = {"np": numpy, "pd": pandas, "stats": stats, "plt": Plt()}
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    bindings = {
        "np": numpy,
        "pd": pandas,
        "stats": stats,
        "plt": Plt(),
        "perform_statistical_test": namespace["perform_statistical_test"],
    }

    def reduce(finished):
        """The two verdicts, and the data of the saved figure and of the one left on screen."""
        first = fixtures["ou_two_samples_a"].copy()
        second = fixtures["ou_two_samples_b"].copy()
        function = finished["compare_stats"]
        data = {}
        for case, plot, path in (
            ("saved", True, "plots/distributions/mrms_vs_aorc_depth_mm.png"),
            ("open", False, None),
        ):
            cmv_result, ranksum_result = function(
                first, second, "Comparison of event totals", plot=plot, fname_savefig=path
            )
            data[f"{case}__cmv_result"] = ((), str(cmv_result))
            data[f"{case}__ranksum_result"] = ((), str(ranksum_result))
        pyplot.close("all")
        data["n_figures"] = ((), len(saved))
        for number, (figure_path, kwargs, figure) in enumerate(saved):
            tag = f"fig{number:02d}"
            data[f"{tag}__path"] = ((), figure_path)
            data[f"{tag}__kwargs"] = ((), kwargs)
            data[f"{tag}__suptitle"] = ((), figure["suptitle"])
            data[f"{tag}__captions"] = (f"{tag}__caption", numpy.asarray(figure["captions"], dtype=str))
            data[f"{tag}__titles"] = (f"{tag}__axis", numpy.asarray([a["title"] for a in figure["axes"]], dtype=str))
            data[f"{tag}__bars"] = (
                (f"{tag}__axis", f"{tag}__bar"),
                numpy.asarray([a["bars"] for a in figure["axes"]], dtype=float),
            )
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_utils.py:2075-2153",
    fixtures=[
        "oea_wl_mrms_tseries",
        "oea_wl_aorc_tseries",
        "oea_wl_mrms_summary",
        "oea_wl_aorc_summary",
        "oea_partial_tseries_mrms",
        "oea_partial_tseries_aorc",
        "oea_partial_summary_mrms",
        "oea_partial_summary_aorc",
    ],
    draws="none",
)
def declare_combine_dataset_according_to_priority(fixtures):
    """Run the function that combines two sources' event series by priority, on a full-window replacement and on a partial one; reduce each returned table to the overlapped radar event's rows and the rest.

    The function's own ranges hold the register entry at line 2123 (`LICENSED`): a preferred event that an added
    lower-priority event only partly overlaps keeps its remaining steps and its summary row still describes its
    original span, where the new code removes the whole preferred event. Developer ruling U-21 (outcome 2) has this
    lift run the function on two inputs, as the cell that calls it does (row 108, chunk `4f.2`):

    - `full`: a radar event with no valid rate is replaced by a reanalysis event over its whole window, and the
      departure and the legacy code agree;
    - `partial`: a radar event whose last three of six steps have no rate is overlapped by a reanalysis event over
      exactly those three steps, so the legacy code keeps its first three steps and its summary row while the
      departure removes the event, and a second radar event later in time is untouched.

    Each returned table is split into two named components: the overlapped radar event's rows, selected by its data
    source and its original span (its first to last stamp, and in the summary table `event_start` equal to its first
    stamp), which the fixtures fix; and the rest. In the rest, `event_id` is replaced by the event's rank by start
    time, the map taken from the time series and applied to both tables, because the departure renumbers the events
    that follow it and changes nothing else about their ids. The test compares the rest and drops the overlapped rows
    by name; the registered callable applies the same split key and the same renumbering to its own tables. The
    function's `sys.exit` (a summary lookup that finds no row or several) is bound to a recorder that raises, as
    legacy does; both fixtures find exactly one row and never reach it.
    """
    import hashlib
    import pathlib

    import numpy
    import pandas
    import xarray

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"

    class Sys:
        """The `sys` the legacy text reads: `exit` raises `SystemExit`, as the legacy call does; nothing else is modelled."""

        def exit(self, *args):
            """Exit as legacy does: no register stop marker lies in this text."""
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    callee_labels = {
        ("_utils", "2075-2153"): (
            "stored-text:1:e80dd910e6746f551b11ace4b82929f8bb3db7c58232d14bbfee8a836925ca84",
            2075,
        ),
    }
    namespace = {"pd": pandas, "sys": Sys()}
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    def pack(data, name, table):
        """Add a table to `data` as one variable per column on its own row dimension; dates stay dates, numbers numbers, anything else text."""
        dimension = f"{name}__row"
        frame = table.reset_index(drop=True)
        for column in frame.columns:
            values = frame[column]
            kind = pandas.api.types.infer_dtype(values, skipna=True)
            if kind in ("datetime64", "datetime"):
                data[f"{name}__{column}"] = (dimension, pandas.to_datetime(values).to_numpy())
            elif kind in ("integer", "floating", "mixed-integer-float", "decimal"):
                data[f"{name}__{column}"] = (dimension, values.to_numpy(dtype=float))
            elif kind == "boolean":
                data[f"{name}__{column}"] = (dimension, values.to_numpy(dtype=bool))
            else:
                data[f"{name}__{column}"] = (dimension, values.astype(str).to_numpy())

    def overlapped_spans(combined_series, preferred, source, added_source):
        """The preferred events an added event that the run kept overlaps, as (source, first stamp, last stamp).

        Read from the combined series (which added events survived) and the inputs (the preferred events' original
        spans), so a registered callable can apply the same key to its own tables from its own inputs.
        """
        kept = []
        for _identifier, added_event in combined_series[combined_series["rain_data_source"] == added_source].groupby(
            "event_id"
        ):
            kept.append((added_event["date_time"].min(), added_event["date_time"].max()))
        spans = set()
        for _identifier, preferred_event in preferred.groupby("event_id"):
            if preferred_event["rain_data_source"].iloc[0] != source:
                continue
            first, last = preferred_event["date_time"].min(), preferred_event["date_time"].max()
            if any(first <= kept_last and last >= kept_first for kept_first, kept_last in kept):
                spans.add((source, first, last))
        return sorted(spans)

    def split(series, summaries, spans):
        """The overlapped preferred event's rows of each table, and the rest; the summary rows keyed by `event_start` equalling the event's first stamp."""
        in_series = numpy.zeros(len(series), dtype=bool)
        in_summaries = numpy.zeros(len(summaries), dtype=bool)
        for source, first, last in spans:
            in_series |= (
                (series["rain_data_source"] == source) & (series["date_time"] >= first) & (series["date_time"] <= last)
            )
            in_summaries |= (summaries["rain_data_source"] == source) & (summaries["event_start"] == first)
        return series[in_series], summaries[in_summaries], series[~in_series], summaries[~in_summaries]

    def renumber(series, summaries):
        """`event_id` replaced by the event's rank by start time, the map taken from the time series and applied to both tables.

        A tie in start time keeps the order the events appear in the time series, so the map is a function of the inputs alone.
        """
        starts = series.groupby("event_id")["date_time"].min().sort_values(kind="stable")
        ranks = {event_id: rank for rank, event_id in enumerate(starts.index)}
        unmapped = sorted(set(summaries["event_id"]) - set(ranks))
        if unmapped:
            raise ValueError(f"the summary rows carry event ids their time series do not: {unmapped}")
        return series.assign(event_id=series["event_id"].map(ranks)), summaries.assign(
            event_id=summaries["event_id"].map(ranks)
        )

    cases = {
        "full": (
            fixtures["oea_wl_mrms_tseries"],
            fixtures["oea_wl_aorc_tseries"],
            fixtures["oea_wl_mrms_summary"],
            fixtures["oea_wl_aorc_summary"],
        ),
        "partial": (
            fixtures["oea_partial_tseries_mrms"],
            fixtures["oea_partial_tseries_aorc"],
            fixtures["oea_partial_summary_mrms"],
            fixtures["oea_partial_summary_aorc"],
        ),
    }

    def reduce(finished):
        """For each case the overlapped event's rows and the rest of each table, the rest's ids renumbered."""
        function = finished["combine_dataset_according_to_priority"]
        data = {}
        for case in ("full", "partial"):
            series, added, summaries, added_summaries = cases[case]
            combined_series, combined_summaries = function(
                "rain_data_source",
                [series.copy(), added.copy()],
                "mm_per_hr",
                [summaries.copy(), added_summaries.copy()],
            )
            spans = overlapped_spans(combined_series, series, "mrms", "aorc")
            overlapped_series, overlapped_summaries, rest_series, rest_summaries = split(
                combined_series, combined_summaries, spans
            )
            rest_series, rest_summaries = renumber(rest_series, rest_summaries)
            pack(data, f"{case}_tseries__overlapped", overlapped_series)
            pack(data, f"{case}_summaries__overlapped", overlapped_summaries)
            pack(data, f"{case}_tseries__rest", rest_series)
            pack(data, f"{case}_summaries__rest", rest_summaries)
        return xarray.Dataset(data)

    return {"pd": pandas, "sys": Sys()}, reduce
