"""Lift declarations of the legacy units owned by combined-event-identification.

A declaration supplies the names a stored legacy text reads and reduces the finished namespace to the value a test
compares; it imports no module of this checkout, and any helper it needs sits inside it, where the lift's digest
covers it.

`_b2_event_selection.py:569-587` reports the final surge-event selection: it draws a histogram of the events' peak
surge and a bar chart of the events per year, through two plotting functions of `_utils.py` that it calls by name,
and computes the surge record's step, its length and the events' arrival rate. The text runs unchanged. The two
functions it calls are bound to their own stored legacy text (each text's label is written here as a literal and
checked before the text is compiled, so an edit to a callee changes this declaration, and with it the lift digest).
The functions draw with `plt` through pandas' plotting accessors, which need a real figure, so `plt` is a stand-in
that draws on real `matplotlib` figures with the non-interactive backend, keeps what each saved figure holds in
memory and writes no file (the two csv tables the cell writes, which no later step reads, go to a sink); any `plt` attribute the callees use that the stand-in does not provide is an error, never
a silent no-op. The value is the data the figures draw (bar positions and heights, tick and axis labels, captions)
with the numbers the cell computes.
"""

from tests.correspondence.lift import declares_lift


@declares_lift(
    unit="_b2_event_selection.py:569-587",
    fixtures=["surge_series_6min", "surge_event_summaries", "surge_event_tseries"],
)
def declare_surge_event_report(fixtures):
    """Run the surge-event report cell on the fixtures; reduce to the data its two figures draw and the rate it computes."""
    import hashlib
    from pathlib import Path

    import matplotlib
    import numpy
    import pandas
    import xarray

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    root = Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/_utils/"
    callee_labels = {
        "1578-1595": ("stored-text:1:ba41404a980520f68941da806da59c472a8582911ec73544dbb9612038d49433", 1578),
        "1596-1612": ("stored-text:1:9e9d6f5a7beddc7506e4a288778cbcf083e3042d25533bf28175515c4ebc426d", 1596),
    }
    saved = []  # one entry per figure the callees saved: the path they named and what the figure held

    def figure_contents(figure):
        """What a figure draws: its bars (left edge, width, height), tick and axis labels, and caption texts."""
        axes = figure.axes[0]
        return {
            "bars": numpy.asarray([(b.get_x(), b.get_width(), b.get_height()) for b in axes.patches], dtype=float),
            "ticks": [label.get_text() for label in axes.get_xticklabels()],
            "xlabel": axes.get_xlabel(),
            "ylabel": axes.get_ylabel(),
            "captions": [text.get_text() for text in figure.texts],
        }

    class Sink:
        """A text sink for the `to_csv` writes of the cell, whose tables no later step reads; it keeps nothing."""

        def write(self, text):
            """Accept the text and drop it."""
            return len(text)

        def flush(self):
            """Nothing is buffered."""

        def __iter__(self):
            """File-like, as pandas requires of a buffer it is handed."""
            return iter(())

    class Plt:
        """The `plt` the callees draw with: real figures drawn in memory, a record of each save, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes, so that the pandas plotting accessors the callees use can draw on them."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, and write no file."""
            saved.append((str(path), figure_contents(pyplot.gcf())))

        def clf(self):
            """Clear the current figure, as the legacy callee does after each save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    namespace = {"plt": Plt(), "pd": pandas}
    for name, (label, first) in callee_labels.items():
        data = (root / stored / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stored}{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), stored + name + ".txt", "exec"), namespace)
    bindings = {
        "pd": pandas,
        "np": numpy,
        "plot_annual_rainfall_totals": namespace["plot_annual_rainfall_totals"],
        "plot_wlevel_histogram": namespace["plot_wlevel_histogram"],
        "s_6min_surge": fixtures["surge_series_6min"],
        "df_6min_wlevel_event_summaries": fixtures["surge_event_summaries"],
        "df_6min_wlevel_event_tseries": fixtures["surge_event_tseries"],
        "surge_6min_event_n_per_year": 2.4,
        "surge_event_threshold_ft_selected": 2.5,
        "fldr_plt_selected_events": "plots/",
        "f_6min_surge_event_summaries": Sink(),
        "f_6min_surge_event_timeseries": Sink(),
    }

    def reduce(finished):
        """The data of the two saved figures (histogram, then yearly bars) and the numbers the cell computed."""
        pyplot.close("all")
        if [path for path, _ in saved] != [
            "plots/max_surge_hist.png",
            "plots/surge_event_nstorms_per_year.png",
        ]:
            raise ValueError(f"the cell saved figures {[path for path, _ in saved]}, not the two it is known to save")
        (histogram_path, histogram), (annual_path, annual) = saved
        return xarray.Dataset(
            {
                "histogram_bin_left": ("bin", histogram["bars"][:, 0]),
                "histogram_bin_width": ("bin", histogram["bars"][:, 1]),
                "histogram_bin_count": ("bin", histogram["bars"][:, 2]),
                "histogram_xlabel": ((), histogram["xlabel"]),
                "histogram_caption": ((), histogram["captions"][0]),
                "histogram_save_path": ((), histogram_path),
                "annual_bar_count": ("year", annual["bars"][:, 2]),
                "annual_bar_year": ("year", numpy.asarray(annual["ticks"])),
                "annual_ylabel": ((), annual["ylabel"]),
                "annual_caption": ((), annual["captions"][0]),
                "annual_title": ((), annual["captions"][-1]),
                "annual_save_path": ((), annual_path),
                "wlevel_return_rate_n_per_year": ((), float(finished["wlevel_return_rate_n_per_year"])),
                "tstep_wlevel": ((), numpy.timedelta64(finished["tstep_wlevel"].value, "ns")),
                "wlevel_record_length": ((), numpy.timedelta64(finished["wlevel_record_length"].value, "ns")),
            }
        )

    return bindings, reduce


@declares_lift(unit="_b2_event_selection.py:273-324", fixtures=["rain_mrms", "rain_aorc", "rain_ncei"])
def declare_final_rain_event_selection(fixtures):
    """Run the final rain-event selection cell on the fixtures; reduce to its six event tables and its thresholds and rates."""
    import hashlib
    import inspect
    import pathlib
    import tomllib
    from datetime import datetime

    import numpy
    import pandas
    import xarray

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/_utils/"
    unit_texts = [
        ("_b2_event_selection.py", 273, 324),
        ("_utils.py", 1269, 1422),
        ("_utils.py", 1540, 1577),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own included
    callees = {
        "1269-1422": ("stored-text:1:545528afe9cec9b5d0031110744a573888f752a9978c5548aef65dda320ef849", 1269),
        "1540-1577": ("stored-text:1:67673605a5435a5c38e3a5117650376841ed64ee1f9a175eb5bdc3cf5616268b", 1540),
    }  # stored text -> (its label, its first legacy line), in the order the texts are compiled
    stop_lines = [
        ("_utils.py", 1274),
        ("_utils.py", 1563),
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

    class Sys:
        """The `sys` the callee texts read: `exit` is passed at a register stop-marker line and raises anywhere else."""

        def exit(self, *args):
            """Return at a stop-marker line of a stored text, as the run behind the kept outputs did; else exit as legacy does."""
            frame = inspect.currentframe().f_back
            where = (pathlib.Path(frame.f_code.co_filename).parent.name + ".py", frame.f_lineno)
            if where in stop_lines:
                return None
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    class Sink:
        """A text sink for the `to_csv` writes of the cell; it keeps nothing."""

        def write(self, text):
            """Accept the text and drop it."""
            return len(text)

        def flush(self):
            """Nothing is buffered."""

        def __iter__(self):
            """File-like, as pandas requires of a buffer it is handed."""
            return iter(())

    def bring_in(namespace):
        """Compile each callee text under its own stored path, after checking its pinned label, into `namespace`."""
        for name, (label, first) in callees.items():
            data = (root / stored / f"{name}.txt").read_bytes()
            if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
                raise ValueError(
                    f"the stored text {stored}{name}.txt is not the text this declaration was written against"
                )
            # Blank lines first, so that the callee's line numbers are its legacy ones.
            exec(compile("\n" * (first - 1) + data.decode("utf-8"), stored + name + ".txt", "exec"), namespace)

    def pack(prefix, frame):
        """One variable per column (and index level) of `frame`, named for the output, on a row dimension of its own."""
        flat = frame.reset_index()
        out = {}
        for column in flat.columns:
            values = flat[column]
            out[f"{prefix}__{column}"] = (
                f"{prefix}__row",
                values.astype(str).to_numpy() if values.dtype == object else values.to_numpy(),
            )
        return out

    namespace = {
        "np": numpy,
        "pd": pandas,
        "datetime": datetime,
        "sys": Sys(),
        "return_bm_string": lambda *args, **kwargs: (
            None
        ),  # the discarded benchmark printer: its value is read by nothing
    }
    bring_in(namespace)

    class Folder:
        """The parent folder of a path: the cell asks it to exist, and nothing is created."""

        def mkdir(self, *args, **kwargs):
            """Create nothing."""

    class Path:
        """`Path` as the cell uses it, only to make the folder of an output file; the file is never opened."""

        def __init__(self, target):
            """Hold the target and expose a parent folder that creates nothing."""
            self.parent = Folder()

    bindings = {
        "Path": Path,
        "compute_event_timeseries_statistics": namespace["compute_event_timeseries_statistics"],
        "event_selection_threshold_or_nstorms": namespace["event_selection_threshold_or_nstorms"],
        "lst_time_durations_h_to_analyze": [1, 4, 8, 16, 24],
        "max_strm_len": pandas.Timedelta(48, "hours"),
        "min_interevent_time": pandas.Timedelta("12 hours"),
        "s_mrms_rainfall_mm_per_hr": fixtures["rain_mrms"],
        "s_aorc_rainfall_mm_per_hr": fixtures["rain_aorc"],
        "s_rainfall_ncei_hrly_mrms_res_mm_per_hr": fixtures["rain_ncei"],
        "f_mrms_event_summaries": Sink(),
        "f_mrms_event_timeseries": Sink(),
        "f_aorc_event_summaries": Sink(),
        "f_aorc_event_timeseries": Sink(),
        "f_ncei_hrly_event_summaries": Sink(),
        "f_ncei_hrly_event_timeseries": Sink(),
    }

    def reduce(finished):
        """The six event tables the cell writes and the thresholds and arrival rates it computes, as one dataset."""
        data = {}
        for prefix, name in (
            ("mrms_summary", "df_mrms_rainevent_summary"),
            ("mrms_tseries", "df_mrms_rainevent_tseries"),
            ("aorc_summary", "df_aorc_rainevent_summary"),
            ("aorc_tseries", "df_aorc_rainevent_tseries"),
            ("ncei_summary", "df_ncei_hrly_rainevent_summary"),
            ("ncei_tseries", "df_ncei_hrly_rainevent_tseries"),
        ):
            data.update(pack(prefix, finished[name]))
        for name in (
            "mrms_threshold_mm",
            "mrms_event_return_rate_n_per_year",
            "aorc_threshold_mm",
            "aorc_event_return_rate_n_per_year",
            "ncei_hrly_threshold_mm",
            "ncei_hourly_event_return_rate_n_per_year",
        ):
            data[name] = ((), float(finished[name]))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_b2_event_selection.py:553-568", fixtures=["wlevels_6min"])
def declare_final_surge_event_selection(fixtures):
    """Run the final surge-event selection cell on the fixtures; reduce to the surge events, their series and the rates it computes."""
    import hashlib
    import inspect
    import pathlib
    import tomllib

    import numpy
    import pandas
    import xarray

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/_utils/"
    unit_texts = [
        ("_b2_event_selection.py", 553, 568),
        ("_utils.py", 1423, 1539),
        ("_utils.py", 1540, 1577),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own included
    callees = {
        "1423-1539": ("stored-text:1:46a18710bf29ab15a07a10dc9a63d44de03acdab2f64d2887da9f397e53d2614", 1423),
        "1540-1577": ("stored-text:1:67673605a5435a5c38e3a5117650376841ed64ee1f9a175eb5bdc3cf5616268b", 1540),
    }  # stored text -> (its label, its first legacy line), in the order the texts are compiled
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

    class Sys:
        """The `sys` the callee texts read: `exit` is passed at a register stop-marker line and raises anywhere else."""

        def exit(self, *args):
            """Return at a stop-marker line of a stored text, as the run behind the kept outputs did; else exit as legacy does."""
            frame = inspect.currentframe().f_back
            where = (pathlib.Path(frame.f_code.co_filename).parent.name + ".py", frame.f_lineno)
            if where in stop_lines:
                return None
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    class Sink:
        """A text sink for the `to_csv` writes of the cell; it keeps nothing."""

        def write(self, text):
            """Accept the text and drop it."""
            return len(text)

        def flush(self):
            """Nothing is buffered."""

        def __iter__(self):
            """File-like, as pandas requires of a buffer it is handed."""
            return iter(())

    def bring_in(namespace):
        """Compile each callee text under its own stored path, after checking its pinned label, into `namespace`."""
        for name, (label, first) in callees.items():
            data = (root / stored / f"{name}.txt").read_bytes()
            if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
                raise ValueError(
                    f"the stored text {stored}{name}.txt is not the text this declaration was written against"
                )
            # Blank lines first, so that the callee's line numbers are its legacy ones.
            exec(compile("\n" * (first - 1) + data.decode("utf-8"), stored + name + ".txt", "exec"), namespace)

    def pack(prefix, frame):
        """One variable per column (and index level) of `frame`, named for the output, on a row dimension of its own."""
        flat = frame.reset_index()
        out = {}
        for column in flat.columns:
            values = flat[column]
            out[f"{prefix}__{column}"] = (
                f"{prefix}__row",
                values.astype(str).to_numpy() if values.dtype == object else values.to_numpy(),
            )
        return out

    namespace = {"np": numpy, "pd": pandas, "sys": Sys()}
    bring_in(namespace)
    bindings = {
        "pd": pandas,
        "compute_event_timeseries_statistics": namespace["compute_event_timeseries_statistics"],
        "surge_event_selection": namespace["surge_event_selection"],
        "df_6min_water_levels_mrms_res": fixtures["wlevels_6min"],
        "lst_time_durations_h_to_analyze": [1, 4, 8, 16, 24],
        "max_event_duration_h": 48,
        "min_interevent_time": pandas.Timedelta("12 hours"),
        "surge_threshold_to_determine_event_limits": 2,
    }

    def reduce(finished):
        """The final surge-event summaries (with their statistics) and series, the derived threshold and the arrival rates."""
        data = {}
        data.update(pack("summaries", finished["df_6min_wlevel_event_summaries"]))
        data.update(pack("tseries", finished["df_6min_wlevel_event_tseries"]))
        for name in ("_6min_surge_event_threshold", "surge_6min_event_n_per_year", "nperyear"):
            data[name] = ((), float(finished[name]))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_b2_event_selection.py:588-617",
    fixtures=[
        "wlevels_6min",
        "cmb_surge_summary",
        "cmb_surge_tseries",
        "cmb_rain_mrms_series",
        "cmb_rain_mrms_summary",
        "cmb_rain_mrms_tseries",
        "cmb_rain_aorc_series",
        "cmb_rain_aorc_summary",
        "cmb_rain_aorc_tseries",
        "cmb_rain_ncei_series",
        "cmb_rain_ncei_summary",
        "cmb_rain_ncei_tseries",
    ],
)
def declare_combined_event_pairing(fixtures):
    """Run the cell that pairs the final surge events with each source's rain events; reduce to the six combined-event tables."""
    import hashlib
    import inspect
    import pathlib
    import tomllib

    import numpy
    import pandas
    import xarray

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/_utils/"
    unit_texts = [
        ("_b2_event_selection.py", 588, 617),
        ("_utils.py", 335, 398),
        ("_utils.py", 1540, 1577),
        ("_utils.py", 1613, 1785),
        ("_utils.py", 1786, 1989),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own included
    callees = {
        "335-398": ("stored-text:1:8614a75a1efbbea4ed3172d4f136a0ca838026c7e2ddfe57371d706d4dddb70a", 335),
        "1540-1577": ("stored-text:1:67673605a5435a5c38e3a5117650376841ed64ee1f9a175eb5bdc3cf5616268b", 1540),
        "1613-1785": ("stored-text:1:c077ec874df807b81f43c7e1c2af111737f94d2e670ba2b54ca835ca57747b88", 1613),
        "1786-1989": ("stored-text:1:af744209234c86910e2a893c693c7c6429b54d171b0c4f6e6aa12fbf4d129b65", 1786),
    }  # stored text -> (its label, its first legacy line), in the order the texts are compiled
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

    class Sys:
        """The `sys` the callee texts read: `exit` is passed at a register stop-marker line and raises anywhere else."""

        def exit(self, *args):
            """Return at a stop-marker line of a stored text, as the run behind the kept outputs did; else exit as legacy does."""
            frame = inspect.currentframe().f_back
            where = (pathlib.Path(frame.f_code.co_filename).parent.name + ".py", frame.f_lineno)
            if where in stop_lines:
                return None
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    class Sink:
        """A text sink for the `to_csv` writes of the cell; it keeps nothing."""

        def write(self, text):
            """Accept the text and drop it."""
            return len(text)

        def flush(self):
            """Nothing is buffered."""

        def __iter__(self):
            """File-like, as pandas requires of a buffer it is handed."""
            return iter(())

    def bring_in(namespace):
        """Compile each callee text under its own stored path, after checking its pinned label, into `namespace`."""
        for name, (label, first) in callees.items():
            data = (root / stored / f"{name}.txt").read_bytes()
            if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
                raise ValueError(
                    f"the stored text {stored}{name}.txt is not the text this declaration was written against"
                )
            # Blank lines first, so that the callee's line numbers are its legacy ones.
            exec(compile("\n" * (first - 1) + data.decode("utf-8"), stored + name + ".txt", "exec"), namespace)

    def pack(prefix, frame):
        """One variable per column (and index level) of `frame`, named for the output, on a row dimension of its own."""
        flat = frame.reset_index()
        out = {}
        for column in flat.columns:
            values = flat[column]
            out[f"{prefix}__{column}"] = (
                f"{prefix}__row",
                values.astype(str).to_numpy() if values.dtype == object else values.to_numpy(),
            )
        return out

    namespace = {
        "np": numpy,
        "pd": pandas,
        "sys": Sys(),
        "lst_time_durations_h_to_analyze": [1, 4, 8, 16, 24],
        "timeseries_buffer_around_peaks_h": 3,
        "timeseries_buffer_before_first_rain_h": 2,
        "buffer_window_raindepth_fraction_to_trigger_tserties_extension": 0.1,
    }
    bring_in(namespace)
    bindings = {
        "determine_combined_event_statistics": namespace["determine_combined_event_statistics"],
        "df_6min_water_levels_mrms_res": fixtures["wlevels_6min"],
        "df_6min_wlevel_event_summaries": fixtures["cmb_surge_summary"],
        "df_6min_wlevel_event_tseries": fixtures["cmb_surge_tseries"],
        "df_mrms_rainevent_summary": fixtures["cmb_rain_mrms_summary"],
        "df_mrms_rainevent_tseries": fixtures["cmb_rain_mrms_tseries"],
        "s_mrms_rainfall_mm_per_hr": fixtures["cmb_rain_mrms_series"]["mm_per_hr"],
        "s_mrms_rainfall_mm": fixtures["cmb_rain_mrms_series"]["mm"],
        "df_aorc_rainevent_summary": fixtures["cmb_rain_aorc_summary"],
        "df_aorc_rainevent_tseries": fixtures["cmb_rain_aorc_tseries"],
        "s_aorc_rainfall_mm_per_hr": fixtures["cmb_rain_aorc_series"]["mm_per_hr"],
        "s_aorc_rainfall_mm": fixtures["cmb_rain_aorc_series"]["mm"],
        "df_ncei_hrly_rainevent_summary": fixtures["cmb_rain_ncei_summary"],
        "df_ncei_hrly_rainevent_tseries": fixtures["cmb_rain_ncei_tseries"],
        "s_rainfall_ncei_hrly_mrms_res_mm_per_hr": fixtures["cmb_rain_ncei_series"]["mm_per_hr"],
        "s_rainfall_ncei_hrly_mrms_res_mm": fixtures["cmb_rain_ncei_series"]["mm"],
        "f_6m_wlevel_mrms_event_summaries": Sink(),
        "f_6m_wlevel_mrms_event_timeseries": Sink(),
        "f_6m_wlevel_aorc_event_summaries": Sink(),
        "f_6m_wlevel_aorc_event_timeseries": Sink(),
        "f_6min_wlevel_hrly_ncei_event_summaries": Sink(),
        "f_6min_wlevel_hrly_ncei_event_timeseries": Sink(),
    }

    def reduce(finished):
        """The six combined-event tables the cell writes, as one dataset."""
        data = {}
        for prefix, name in (
            ("mrms_summary", "df_6min_wlevel_mrms_event_summary_stats"),
            ("mrms_tseries", "df_6min_wlevel_mrms_event_tseries"),
            ("aorc_summary", "df_6min_wlevel_aorc_event_summary_stats"),
            ("aorc_tseries", "df_6min_wlevel_aorc_event_tseries"),
            ("ncei_summary", "df_6min_wlevel_hrly_ncei_event_summary_stats"),
            ("ncei_tseries", "df_6min_wlevel_hrly_ncei_event_tseries"),
        ):
            data.update(pack(prefix, finished[name]))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_b3_event_analysis.py:24-41",
    fixtures=[
        "rb_mrms_summary",
        "rb_mrms_tseries",
        "rb_aorc_summary",
        "rb_aorc_tseries",
        "rb_ncei_summary",
        "rb_ncei_tseries",
    ],
)
def declare_read_back_combined_events(fixtures):
    """Run the cell that reads the combined-event csv files back and labels each event; reduce to its six tables.

    The cell reads six csv files through `pd.read_csv`. Here `pd` is a stand-in whose `read_csv` returns the fixture
    named by the file, copied; the fixtures are already what `read_csv` returns once the date columns are parsed, so
    `parse_dates` is accepted and ignored. The cell's one callee, `classify_events`, is bound to its stored legacy
    text. Neither the cell nor that callee reaches a register stop marker, so no `sys` is bound.
    """
    import hashlib
    import pathlib
    import types

    import xarray

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/_utils/"
    name, label, first = (
        "2060-2071",
        "stored-text:1:bc1a50ae5fa3b0464dcbf4884ae120e201747a1a0693ac749e0d1de82fed4951",
        2060,
    )
    data = (root / stored / f"{name}.txt").read_bytes()
    if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
        raise ValueError(f"the stored text {stored}{name}.txt is not the text this declaration was written against")
    namespace = {}
    # Blank lines first, so that the callee's line numbers are its legacy ones.
    exec(compile("\n" * (first - 1) + data.decode("utf-8"), stored + name + ".txt", "exec"), namespace)

    files = {
        "mrms_summaries.csv": "rb_mrms_summary",
        "mrms_timeseries.csv": "rb_mrms_tseries",
        "aorc_summaries.csv": "rb_aorc_summary",
        "aorc_timeseries.csv": "rb_aorc_tseries",
        "ncei_summaries.csv": "rb_ncei_summary",
        "ncei_timeseries.csv": "rb_ncei_tseries",
    }

    def read_csv(path, **kwargs):
        """The fixture the named file stands for, copied; a file this lift does not know is an error."""
        return fixtures[files[path]].copy()

    def pack(prefix, frame):
        """One variable per column of `frame`, named for the output, on a row dimension of its own."""
        flat = frame.reset_index(drop=True)
        return {
            f"{prefix}__{column}": (
                f"{prefix}__row",
                flat[column].astype(str).to_numpy() if flat[column].dtype == object else flat[column].to_numpy(),
            )
            for column in flat.columns
        }

    bindings = {
        "pd": types.SimpleNamespace(read_csv=read_csv),
        "classify_events": namespace["classify_events"],
        "f_6m_wlevel_mrms_event_summaries": "mrms_summaries.csv",
        "f_6m_wlevel_mrms_event_timeseries": "mrms_timeseries.csv",
        "f_6m_wlevel_aorc_event_summaries": "aorc_summaries.csv",
        "f_6m_wlevel_aorc_event_timeseries": "aorc_timeseries.csv",
        "f_6min_wlevel_hrly_ncei_event_summaries": "ncei_summaries.csv",
        "f_6min_wlevel_hrly_ncei_event_timeseries": "ncei_timeseries.csv",
    }

    def reduce(finished):
        """The six tables the cell leaves: the classified summaries and the series, for the three rainfall sources."""
        data = {}
        for prefix, table in (
            ("mrms_summary", "df_6min_wlevel_mrms_event_summary_stats"),
            ("mrms_tseries", "df_6min_wlevel_mrms_event_tseries"),
            ("aorc_summary", "df_6min_wlevel_aorc_event_summary_stats"),
            ("aorc_tseries", "df_6min_wlevel_aorc_event_tseries"),
            ("ncei_summary", "df_6min_wlevel_hrly_ncei_event_summary_stats"),
            ("ncei_tseries", "df_6min_wlevel_hrly_ncei_event_tseries"),
        ):
            data.update(pack(prefix, finished[table]))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_b3_event_analysis.py:62-97", fixtures=["rb_mrms_classified"])
def declare_event_rain_depth_chart(fixtures):
    """Run the cell that draws the radar-paired events' rain depths from 2015 on; reduce to the bars it draws and its labels.

    The cell draws with `plt.subplots` and axes methods only, so `plt` is a stand-in that creates real figures with
    the non-interactive backend and refuses any other `plt` attribute; nothing is saved. The value is the filtered
    table the chart is drawn from, each bar's left edge, width and height, the axis labels and the figure's size
    and resolution. Neither the cell nor any callee reaches a register stop marker (the cell has no callee).
    """
    import matplotlib
    import numpy
    import pandas
    import xarray

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    class Plt:
        """The `plt` the cell draws with: real figures in memory, any other attribute refused."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes, so that the cell's axes calls draw on them."""
            return pyplot.subplots(*args, **kwargs)

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    bindings = {
        "pd": pandas,
        "plt": Plt(),
        "df_6min_wlevel_mrms_event_summary_stats": fixtures["rb_mrms_classified"],
    }

    def reduce(finished):
        """The chart's source table, its bars, its labels and its figure geometry, as one dataset."""
        axes, figure = finished["ax"], finished["fig"]
        source = finished["df_6min_wlevel_mrms_event_summary_stats_no_na"].reset_index()
        data = {
            f"source__{column}": (
                "source__row",
                source[column].astype(str).to_numpy() if source[column].dtype == object else source[column].to_numpy(),
            )
            for column in source.columns
        }
        data["bar_left"] = ("bar", numpy.asarray([b.get_x() for b in axes.patches], dtype=float))
        data["bar_width"] = ("bar", numpy.asarray([b.get_width() for b in axes.patches], dtype=float))
        data["bar_height"] = ("bar", numpy.asarray([b.get_height() for b in axes.patches], dtype=float))
        data["xlabel"] = ((), axes.get_xlabel())
        data["ylabel"] = ((), axes.get_ylabel())
        data["figure_inches"] = ("size", numpy.asarray(figure.get_size_inches(), dtype=float))
        data["figure_dpi"] = ((), float(figure.get_dpi()))
        pyplot.close("all")
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_inputs.py:37-199")
def declare_event_selection_settings(fixtures):
    """Run the settings cell of the legacy inputs file; reduce to the seven settings the ledger row ports.

    The cell reads three names: `Path`, `os` and `pd`. `os` is a stand-in with `getcwd` alone, called with no argument
    and returning a fixed working directory; any other attribute, and any argument, raises. `Path` is a pure path
    class that touches no file system. The cell's file and folder names are built from that directory and are not
    ported, so they are left out of the value. The value is exactly the settings the row of `_inputs.py:37-199` names
    as ported: the summary durations, events per year, minimum gap, longest event and the surge level bounding an
    event, and the two tide settings. The cell's other numeric settings (the derived storm lengths, the radar time
    step, the hourly step, the time buffer, the water-level threshold, the compression level) are not named by the
    row, so they are not compared. The cell has no callee and reaches no register stop marker, so no `sys` is bound.
    """
    import pathlib

    import numpy
    import pandas
    import xarray

    class Os:
        """The `os` the cell reads: `getcwd` with no argument, and nothing else."""

        def getcwd(self):
            """A fixed working directory, three levels deep so that the cell's `parents[1]` exists."""
            return "/work/norfolk/stormy/run"

        def __getattr__(self, name):
            """Refuse any other attribute of `os`: a legacy use beyond `getcwd` must be seen, not stood in for."""
            raise AttributeError(f"the lift's os stand-in does not model os.{name}")

    bindings = {"Path": pathlib.PurePosixPath, "os": Os(), "pd": pandas}

    def reduce(finished):
        """The seven ported settings as one dataset: integers, and the minimum gap as a nanosecond timedelta."""

        def span(name):
            """A pandas Timedelta setting as a numpy timedelta."""
            return numpy.timedelta64(finished[name].value, "ns")

        return xarray.Dataset(
            {
                "lst_time_durations_h_to_analyze": (
                    "duration",
                    numpy.asarray(finished["lst_time_durations_h_to_analyze"]),
                ),
                "target_events_per_year": ((), finished["target_events_per_year"]),
                "max_event_duration_h": ((), finished["max_event_duration_h"]),
                "surge_threshold_to_determine_event_limits": (
                    (),
                    finished["surge_threshold_to_determine_event_limits"],
                ),
                "min_interevent_time": ((), span("min_interevent_time")),
                "t_window_h_to_shift_tidal_tseries": ((), finished["t_window_h_to_shift_tidal_tseries"]),
                "n_years_to_look_back_for_tidal_tseries": ((), finished["n_years_to_look_back_for_tidal_tseries"]),
            }
        )

    return bindings, reduce


@declares_lift(unit="_utils.py:1269-1422", fixtures=["rain_mrms"])
def declare_rain_event_selection_function(fixtures):
    """Run the legacy rain-event selection function on the invented radar rainfall; reduce to what it returns for a threshold and for a target count."""
    import inspect
    import pathlib
    import tomllib
    from datetime import datetime

    import numpy
    import pandas
    import xarray

    root = pathlib.Path(__file__).resolve().parents[2]

    unit_texts = [
        ("_utils.py", 1269, 1422)
    ]  # (file, first line, last line) of every text this lift runs, the unit's own included
    stop_lines = [
        ("_utils.py", 1274)
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

    def pack(prefix, frame):
        """One variable per column (and index level) of `frame`, named for the output, on a row dimension of its own."""
        flat = frame.reset_index()
        return {
            f"{prefix}__{column}": (
                f"{prefix}__row",
                flat[column].astype(str).to_numpy() if flat[column].dtype == object else flat[column].to_numpy(),
            )
            for column in flat.columns
        }

    bindings = {
        "np": numpy,
        "pd": pandas,
        "datetime": datetime,
        "sys": Sys(),
        "return_bm_string": lambda *args, **kwargs: (
            None
        ),  # the discarded benchmark printer: its value is read by nothing
    }

    def reduce(finished):
        """The events, series, threshold and arrival rate for the 55 mm threshold, and for a target of 60 events per year."""
        select = finished["event_selection_threshold_or_nstorms"]
        gap, longest = pandas.Timedelta("12 hours"), pandas.Timedelta(48, "hours")
        data = {}
        for prefix, kwargs in (("threshold", {"threshold": 55}), ("target", {"target_events_per_year": 60})):
            summary, series, level, rate = select(fixtures["rain_mrms"].copy(), gap, longest, "mrms", **kwargs)
            data.update(pack(prefix + "_summary", summary))
            data.update(pack(prefix + "_tseries", series))
            data[prefix + "_level_mm"] = ((), float(level))
            data[prefix + "_rate_n_per_year"] = ((), float(rate))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_utils.py:1423-1539", fixtures=["wlevels_6min"])
def declare_surge_event_selection_function(fixtures):
    """Run the legacy surge-event selection function on the invented six-minute surge; reduce to what it returns for a threshold and for a target count."""
    import pathlib

    import numpy
    import pandas
    import xarray

    pathlib.Path(__file__).resolve().parents[2]

    def pack(prefix, frame):
        """One variable per column (and index level) of `frame`, named for the output, on a row dimension of its own."""
        flat = frame.reset_index()
        return {
            f"{prefix}__{column}": (
                f"{prefix}__row",
                flat[column].astype(str).to_numpy() if flat[column].dtype == object else flat[column].to_numpy(),
            )
            for column in flat.columns
        }

    bindings = {"np": numpy, "pd": pandas}

    def reduce(finished):
        """The events, series, derived threshold and arrival rate for the 2.5 ft threshold, and for 40 events per year."""
        select = finished["surge_event_selection"]
        data = {}
        for prefix, kwargs in (("threshold", {"threshold": 2.5}), ("target", {"target_events_per_year": 40})):
            summary, series, level, rate = select(
                fixtures["wlevels_6min"]["surge_ft"], 2, 48, pandas.Timedelta("12 hours"), "6min_surge_data", **kwargs
            )
            data.update(pack(prefix + "_summary", summary))
            data.update(pack(prefix + "_tseries", series))
            data[prefix + "_level_ft"] = ((), float(level))
            data[prefix + "_rate_n_per_year"] = ((), float(rate))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_utils.py:1540-1577", fixtures=["cmb_surge_tseries", "cmb_rain_mrms_tseries"])
def declare_event_statistics_function(fixtures):
    """Run the legacy event-statistics function on the invented surge and rain event series; reduce to the three tables it returns."""
    import inspect
    import pathlib
    import tomllib

    import numpy
    import pandas
    import xarray

    root = pathlib.Path(__file__).resolve().parents[2]

    unit_texts = [
        ("_utils.py", 1540, 1577)
    ]  # (file, first line, last line) of every text this lift runs, the unit's own included
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

    def pack(prefix, frame):
        """One variable per column (and index level) of `frame`, named for the output, on a row dimension of its own."""
        flat = frame.reset_index()
        return {
            f"{prefix}__{column}": (
                f"{prefix}__row",
                flat[column].astype(str).to_numpy() if flat[column].dtype == object else flat[column].to_numpy(),
            )
            for column in flat.columns
        }

    bindings = {"np": numpy, "pd": pandas, "sys": Sys()}
    durations = [1, 4, 8, 16, 24]

    def reduce(finished):
        """The statistics of the surge events, of the rain events, and of one event given as a single-event table."""
        statistics = finished["compute_event_timeseries_statistics"]
        surge, rain = fixtures["cmb_surge_tseries"], fixtures["cmb_rain_mrms_tseries"]
        tables = {
            "surge": statistics(
                surge["surge_ft"], durations, varname="surge", idx_name_time="date_time", idx_event_id="surge_event_id"
            ),
            "rain": statistics(rain["mm_per_hour"], durations, varname="mm_per_hour", idx_name_time="date_time"),
            "single": statistics(surge.loc[[1]][["surge_ft"]].droplevel(0), durations, varname="surge", agg_stat="max"),
        }
        data = {}
        for prefix, table in tables.items():
            data.update(pack(prefix, table))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_utils.py:1578-1595", fixtures=["cmb_rain_mrms_summary", "cmb_rain_mrms_series"])
def declare_annual_totals_plot_function(fixtures):
    """Run the legacy annual event-count chart function on the invented radar rain events; reduce to the data the chart draws."""
    import pathlib

    import matplotlib
    import numpy
    import xarray

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    pathlib.Path(__file__).resolve().parents[2]

    saved = []  # one entry per figure a callee saved or showed: how it was ended and what the figure held

    def contents():
        """What the current figure draws: bars (left edge, width, height), tick and axis labels, caption texts."""
        figure = pyplot.gcf()
        axes = figure.axes[0]
        return {
            "bars": numpy.asarray([(b.get_x(), b.get_width(), b.get_height()) for b in axes.patches], dtype=float),
            "ticks": numpy.asarray([t.get_text() for t in axes.get_xticklabels()], dtype=str),
            "labels": numpy.asarray([axes.get_xlabel(), axes.get_ylabel()], dtype=str),
            "captions": numpy.asarray([t.get_text() for t in figure.texts], dtype=str),
        }

    class Plt:
        """The `plt` the callees draw with: real figures in memory, a record of each save or show, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes, so that the pandas plotting accessors the callees use can draw on them."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, and write no file."""
            saved.append((str(path), contents()))

        def show(self):
            """Keep what the current figure holds as shown, and display nothing."""
            saved.append(("<show>", contents()))

        def clf(self):
            """Clear the current figure, as the legacy callee does after each save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    def drawn(prefix, call, saves):
        """Run `call` on a fresh figure; return the data of the figure it saved or showed (`saves` = 1) or left open (0)."""
        pyplot.close("all")
        saved.clear()
        call()
        if len(saved) != saves:
            raise ValueError(f"the call saved or showed {len(saved)} figures, not the {saves} it is known to")
        where, held = saved[0] if saves else ("<left open>", contents())
        pyplot.close("all")
        return {
            f"{prefix}__where": ((), where),
            f"{prefix}__bars": ((f"{prefix}__bar", "bar_field"), held["bars"]),
            f"{prefix}__ticks": (f"{prefix}__tick", held["ticks"]),
            f"{prefix}__labels": (f"{prefix}__label", held["labels"]),
            f"{prefix}__captions": (f"{prefix}__caption", held["captions"]),
        }

    bindings = {"plt": Plt()}

    def reduce(finished):
        """The chart as saved (with a title, in mm) and as left on screen (no title, no save), as one dataset."""
        chart = finished["plot_annual_rainfall_totals"]
        summary, series = fixtures["cmb_rain_mrms_summary"], fixtures["cmb_rain_mrms_series"]["mm_per_hr"]
        data = drawn(
            "saved",
            lambda: chart(
                summary,
                series,
                55.0,
                2.4,
                fig_title="Rain events",
                fname_savefig="annual_rain.png",
                threshold_units="mm",
            ),
            1,
        )
        data.update(drawn("open", lambda: chart(summary, series, 55.0, 2.4), 0))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_utils.py:1596-1612", fixtures=["cmb_surge_summary", "cmb_surge_tseries", "wlevels_6min"])
def declare_surge_histogram_plot_function(fixtures):
    """Run the legacy surge-histogram chart function on the invented surge events; reduce to the data the chart draws."""
    import pathlib

    import matplotlib
    import numpy
    import pandas
    import xarray

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    pathlib.Path(__file__).resolve().parents[2]

    saved = []  # one entry per figure a callee saved or showed: how it was ended and what the figure held

    def contents():
        """What the current figure draws: bars (left edge, width, height), tick and axis labels, caption texts."""
        figure = pyplot.gcf()
        axes = figure.axes[0]
        return {
            "bars": numpy.asarray([(b.get_x(), b.get_width(), b.get_height()) for b in axes.patches], dtype=float),
            "ticks": numpy.asarray([t.get_text() for t in axes.get_xticklabels()], dtype=str),
            "labels": numpy.asarray([axes.get_xlabel(), axes.get_ylabel()], dtype=str),
            "captions": numpy.asarray([t.get_text() for t in figure.texts], dtype=str),
        }

    class Plt:
        """The `plt` the callees draw with: real figures in memory, a record of each save or show, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes, so that the pandas plotting accessors the callees use can draw on them."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, and write no file."""
            saved.append((str(path), contents()))

        def show(self):
            """Keep what the current figure holds as shown, and display nothing."""
            saved.append(("<show>", contents()))

        def clf(self):
            """Clear the current figure, as the legacy callee does after each save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    def drawn(prefix, call, saves):
        """Run `call` on a fresh figure; return the data of the figure it saved or showed (`saves` = 1) or left open (0)."""
        pyplot.close("all")
        saved.clear()
        call()
        if len(saved) != saves:
            raise ValueError(f"the call saved or showed {len(saved)} figures, not the {saves} it is known to")
        where, held = saved[0] if saves else ("<left open>", contents())
        pyplot.close("all")
        return {
            f"{prefix}__where": ((), where),
            f"{prefix}__bars": ((f"{prefix}__bar", "bar_field"), held["bars"]),
            f"{prefix}__ticks": (f"{prefix}__tick", held["ticks"]),
            f"{prefix}__labels": (f"{prefix}__label", held["labels"]),
            f"{prefix}__captions": (f"{prefix}__caption", held["captions"]),
        }

    bindings = {"pd": pandas, "plt": Plt()}

    def reduce(finished):
        """The histogram as saved to a file and as shown, as one dataset."""
        chart = finished["plot_wlevel_histogram"]
        summary, series = fixtures["cmb_surge_summary"], fixtures["cmb_surge_tseries"]
        surge = fixtures["wlevels_6min"]["surge_ft"]
        data = drawn("saved", lambda: chart(summary, series, 2.5, surge, 2.4, fname_save="surge_hist.png"), 1)
        data.update(drawn("shown", lambda: chart(summary, series, 2.5, surge, 2.4), 1))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_utils.py:1613-1785",
    fixtures=[
        "cmb_surge_summary",
        "cmb_surge_tseries",
        "cmb_rain_mrms_summary",
        "cmb_rain_mrms_tseries",
        "ov_surge_summary",
        "ov_surge_tseries",
        "ov_rain_summary",
        "ov_rain_tseries",
    ],
)
def declare_compound_event_times_function(fixtures):
    """Run the legacy pairing function on the invented event tables; reduce to the combined-event tables it returns for two scenarios."""
    import pathlib

    import pandas
    import xarray

    pathlib.Path(__file__).resolve().parents[2]

    def pack(prefix, frame):
        """One variable per column (and index level) of `frame`, named for the output, on a row dimension of its own."""
        flat = frame.reset_index()
        return {
            f"{prefix}__{column}": (
                f"{prefix}__row",
                flat[column].astype(str).to_numpy() if flat[column].dtype == object else flat[column].to_numpy(),
            )
            for column in flat.columns
        }

    bindings = {"pd": pandas}

    def reduce(finished):
        """The combined events for an ordinary scenario and for one whose events overlap without a shared surge peak."""
        pair = finished["determine_compound_event_start_and_end_times"]
        data = {}
        for prefix, names in (
            ("ordinary", ("cmb_surge_tseries", "cmb_surge_summary", "cmb_rain_mrms_tseries", "cmb_rain_mrms_summary")),
            ("overlap", ("ov_surge_tseries", "ov_surge_summary", "ov_rain_tseries", "ov_rain_summary")),
        ):
            data.update(pack(prefix, pair(*(fixtures[name].copy() for name in names))))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_utils.py:1786-1989",
    fixtures=[
        "wlevels_6min",
        "cmb_surge_summary",
        "cmb_surge_tseries",
        "cmb_rain_mrms_series",
        "cmb_rain_mrms_summary",
        "cmb_rain_mrms_tseries",
        "cmb_rain_ncei_series",
        "cmb_rain_ncei_summary",
        "cmb_rain_ncei_tseries",
    ],
)
def declare_combined_event_statistics_function(fixtures):
    """Run the legacy combined-event statistics function on the invented tables; reduce to the two tables it returns for two rainfall sources."""
    import hashlib
    import inspect
    import pathlib
    import tomllib

    import numpy
    import pandas
    import xarray

    root = pathlib.Path(__file__).resolve().parents[2]

    unit_texts = [
        ("_utils.py", 1786, 1989),
        ("_utils.py", 335, 398),
        ("_utils.py", 1540, 1577),
        ("_utils.py", 1613, 1785),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own included
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
    stored = "tests/correspondence/extractions/stormy/_utils/"
    callees = {
        "335-398": ("stored-text:1:8614a75a1efbbea4ed3172d4f136a0ca838026c7e2ddfe57371d706d4dddb70a", 335),
        "1540-1577": ("stored-text:1:67673605a5435a5c38e3a5117650376841ed64ee1f9a175eb5bdc3cf5616268b", 1540),
        "1613-1785": ("stored-text:1:c077ec874df807b81f43c7e1c2af111737f94d2e670ba2b54ca835ca57747b88", 1613),
    }  # stored text -> (its label, its first legacy line)

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

    def bring_in(namespace):
        """Compile each callee text under its own stored path, after checking its pinned label, into `namespace`."""
        for name, (label, first) in callees.items():
            data = (root / stored / f"{name}.txt").read_bytes()
            if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
                raise ValueError(
                    f"the stored text {stored}{name}.txt is not the text this declaration was written against"
                )
            # Blank lines first, so that the callee's line numbers are its legacy ones.
            exec(compile("\n" * (first - 1) + data.decode("utf-8"), stored + name + ".txt", "exec"), namespace)

    def pack(prefix, frame):
        """One variable per column (and index level) of `frame`, named for the output, on a row dimension of its own."""
        flat = frame.reset_index()
        return {
            f"{prefix}__{column}": (
                f"{prefix}__row",
                flat[column].astype(str).to_numpy() if flat[column].dtype == object else flat[column].to_numpy(),
            )
            for column in flat.columns
        }

    namespace = {
        "np": numpy,
        "pd": pandas,
        "sys": Sys(),
        "timeseries_buffer_around_peaks_h": 3,
        "timeseries_buffer_before_first_rain_h": 2,
        "buffer_window_raindepth_fraction_to_trigger_tserties_extension": 0.1,
    }
    bring_in(namespace)
    bindings = {
        "np": numpy,
        "pd": pandas,
        "sys": namespace["sys"],
        "lst_time_durations_h_to_analyze": [1, 4, 8, 16, 24],
        "determine_compound_event_start_and_end_times": namespace["determine_compound_event_start_and_end_times"],
        "compute_event_timeseries_statistics": namespace["compute_event_timeseries_statistics"],
        "reindex_to_buffer_significant_rain_and_surge": namespace["reindex_to_buffer_significant_rain_and_surge"],
    }

    def reduce(finished):
        """The statistics table and the combined series for the radar and the gauge rainfall, as one dataset."""
        statistics = finished["determine_combined_event_statistics"]
        data = {}
        for source in ("mrms", "ncei"):
            rain = fixtures[f"cmb_rain_{source}_series"]
            summary, series = statistics(
                fixtures["cmb_surge_tseries"].copy(),
                fixtures["cmb_surge_summary"].copy(),
                fixtures[f"cmb_rain_{source}_tseries"].copy(),
                fixtures[f"cmb_rain_{source}_summary"].copy(),
                fixtures["wlevels_6min"],
                rain["mm_per_hr"],
                rain["mm"],
            )
            data.update(pack(source + "_summary", summary))
            data.update(pack(source + "_tseries", series))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_utils.py:2060-2071", fixtures=["rb_mrms_summary", "rb_ncei_summary"])
def declare_classify_events_function(fixtures):
    """Run the legacy event-classification function on invented event summaries; reduce to the labelled tables."""
    import pathlib

    import xarray

    pathlib.Path(__file__).resolve().parents[2]

    def pack(prefix, frame):
        """One variable per column (and index level) of `frame`, named for the output, on a row dimension of its own."""
        flat = frame.reset_index()
        return {
            f"{prefix}__{column}": (
                f"{prefix}__row",
                flat[column].astype(str).to_numpy() if flat[column].dtype == object else flat[column].to_numpy(),
            )
            for column in flat.columns
        }

    bindings = {}

    def reduce(finished):
        """The summaries of two rainfall sources, each labelled compound, surge or rain by the function."""
        classify = finished["classify_events"]
        data = {}
        for source in ("mrms", "ncei"):
            data.update(pack(source, classify(fixtures[f"rb_{source}_summary"].copy())))
        return xarray.Dataset(data)

    return bindings, reduce
