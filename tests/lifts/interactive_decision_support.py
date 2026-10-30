"""Lift declarations of the legacy units owned by interactive-decision-support.

A declaration supplies the names a stored legacy text reads and reduces the finished namespace to the value a test
compares; it imports no module of this checkout, and any helper it needs sits inside it, where the lift's digest
covers it.

`__ref_ams_functions.py:215-324` defines the function that draws the four-panel figure of one fitted distribution. Its
lift runs the stored text unchanged, binds `backtransform_data` and the three functions that one calls to their stored
legacy texts, binds `plt` to a stand-in that creates real pyplot figures in memory and records what each saved figure
holds, and calls the function on three invented fits.
"""

from tests.correspondence.lift import declares_lift


@declares_lift(
    unit="__ref_ams_functions.py:215-324",
    fixtures=[
        "ids_fit_plain",
        "ids_table_plain",
        "ids_fit_transformed",
        "ids_table_transformed",
        "ids_fit_shifted",
        "ids_table_shifted",
    ],
)
def declare_fitted_distribution_figure(fixtures):
    """Run the fitted-distribution figure function on a plain, a fully transformed and a shifted fit; reduce to the data each figure holds.

    The text reads `np`, `pd`, `stats`, `sns`, `plt` and `backtransform_data`. The last is bound with its callees
    (`inverse_boxcox`, `inverse_normalize`, `unshift_data`) to the stored legacy texts under literal labels. The
    function is called three times: a three-parameter fit saved to a path, a two-parameter fit under every
    transformation (so that the infinite supports, the upper bound and the title's back-transformed bounds are
    reached) that is saved, and a shifted three-parameter fit with its location fixed to the minimum that is left
    unsaved. The text has no register stop marker and no draw.
    """
    import hashlib
    import pathlib

    import matplotlib
    import numpy
    import pandas
    import seaborn
    import xarray
    from scipy import stats

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
    saved = []  # one entry per figure the function saved: the path, the save's keywords and what the figure holds

    def figure_record(figure):
        """What a figure holds: its super title and, for each axes, titles, labels, limits, bars, lines, texts and legend."""
        axes_records = []
        for axes in figure.axes:
            legend = axes.get_legend()
            bars = [b for b in axes.patches if isinstance(b, matplotlib.patches.Rectangle)]
            lines = [
                (
                    str(line.get_label()),
                    matplotlib.colors.to_hex(line.get_color()),
                    str(line.get_linestyle()),
                    [float(v) for v in numpy.asarray(line.get_xdata(), dtype=float)],
                    [float(v) for v in numpy.asarray(line.get_ydata(), dtype=float)],
                )
                for line in axes.lines
            ]
            axes_records.append(
                {
                    "title": axes.get_title(),
                    "labels": [axes.get_xlabel(), axes.get_ylabel()],
                    "lims": [float(v) for v in axes.get_xlim()] + [float(v) for v in axes.get_ylim()],
                    "bars": [
                        (float(b.get_x()), float(b.get_width()), float(b.get_height()), float(b.get_alpha() or 0.0))
                        for b in bars
                    ],
                    "colours": [matplotlib.colors.to_hex(b.get_facecolor()) for b in bars],
                    "patch_meta": [
                        f"{type(b).__name__}|{matplotlib.colors.to_hex(b.get_facecolor(), keep_alpha=True)}"
                        f"|{matplotlib.colors.to_hex(b.get_edgecolor(), keep_alpha=True)}|{b.get_alpha()}|{b.get_fill()}"
                        for b in axes.patches
                    ],
                    "polygon_data": [
                        float(v)
                        for b in axes.patches
                        if isinstance(b, matplotlib.patches.Polygon)
                        for v in numpy.asarray(b.get_xy(), dtype=float).ravel()
                    ],
                    "line_meta": [f"{label}|{colour}|{style}|{len(x)}" for label, colour, style, x, y in lines],
                    "line_data": [v for _, _, _, x, y in lines for v in x + y],
                    "texts": [t.get_text() for t in axes.texts],
                    "legend": [] if legend is None else [t.get_text() for t in legend.get_texts()],
                }
            )
        return {
            "suptitle": figure._suptitle.get_text() if figure._suptitle is not None else "",
            "size": [float(v) for v in figure.get_size_inches()] + [float(figure.get_dpi())],
            "axes": axes_records,
        }

    class Plt:
        """The `plt` the function draws with: real figures in memory, a record of each save, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, with the save's keyword arguments; write no file."""
            saved.append((str(path), repr(sorted(kwargs.items())), figure_record(pyplot.gcf())))

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    callee_labels = {
        ("__ref_ams_functions", "31-38"): (
            "stored-text:1:19651879b8df3efb84101dfc7c6d6730a4056e76539d1b71ef9550729b5cb4b8",
            31,
        ),
        ("__ref_ams_functions", "44-47"): (
            "stored-text:1:fa9a3c07ac825aa2193af7cf461333193307ac90ac10d50dc1538a43d5bdb37a",
            44,
        ),
        ("__ref_ams_functions", "52-55"): (
            "stored-text:1:56cb15bf030fde05bc8f9108ebfc02257f55b64ee9c511985f33896f9f9abc93",
            52,
        ),
        ("__ref_ams_functions", "89-104"): (
            "stored-text:1:d9b815e4ac92d98b897eac33306bc3dfdcf2b2a259e380021052acdbeaf983c9",
            89,
        ),
    }
    namespace = {"np": numpy, "pd": pandas, "stats": stats}
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
        "sns": seaborn,
        "plt": Plt(),
        "backtransform_data": namespace["backtransform_data"],
    }

    def summary(table):
        """The one-row fit table as the Series the function reads, named for the statistic."""
        row = table.iloc[0].astype(object)
        row.name = row.pop("name")
        return row

    def reduce(finished):
        """The figures of three calls, numbered in call order, with what each returned."""
        draw = finished["plot_fitted_distribution"]
        cases = (
            ("plain", 3, "plots/plain.png"),
            ("transformed", 2, "plots/transformed.png"),
            ("shifted", 3, None),
        )
        data = {}
        for number, (case, n_params, path) in enumerate(cases):
            fig, axes = draw(summary(fixtures[f"ids_fit_{case}"]), fixtures[f"ids_table_{case}"].copy(), n_params, path)
            data[f"call{number}__axes_shape"] = (f"call{number}__dim", numpy.asarray(axes.shape, dtype=int))
            record = figure_record(fig)
            data[f"call{number}__suptitle"] = ((), record["suptitle"])
            data[f"call{number}__size"] = (f"call{number}__size_field", numpy.asarray(record["size"], dtype=float))
            for index, panel in enumerate(record["axes"]):
                name = f"call{number}__ax{index}"
                data[f"{name}__title"] = ((), panel["title"])
                data[f"{name}__labels"] = (f"{name}__label", numpy.asarray(panel["labels"], dtype=str))
                data[f"{name}__lims"] = (f"{name}__lim", numpy.asarray(panel["lims"], dtype=float))
                data[f"{name}__bars"] = (
                    (f"{name}__bar", "bar_field"),
                    numpy.asarray(panel["bars"], dtype=float).reshape(-1, 4),
                )
                data[f"{name}__colours"] = (f"{name}__bar", numpy.asarray(panel["colours"], dtype=str))
                data[f"{name}__patch_meta"] = (f"{name}__patch", numpy.asarray(panel["patch_meta"], dtype=str))
                data[f"{name}__polygon_data"] = (
                    f"{name}__polygon_point",
                    numpy.asarray(panel["polygon_data"], dtype=float),
                )
                data[f"{name}__line_meta"] = (f"{name}__line", numpy.asarray(panel["line_meta"], dtype=str))
                data[f"{name}__line_data"] = (f"{name}__line_point", numpy.asarray(panel["line_data"], dtype=float))
                data[f"{name}__texts"] = (f"{name}__text", numpy.asarray(panel["texts"], dtype=str))
                data[f"{name}__legend"] = (f"{name}__entry", numpy.asarray(panel["legend"], dtype=str))
            pyplot.close("all")
        data["n_saved"] = ((), len(saved))
        for number, (path, kwargs, record) in enumerate(saved):
            data[f"saved{number}__path"] = ((), path)
            data[f"saved{number}__kwargs"] = ((), kwargs)
            data[f"saved{number}__suptitle"] = ((), record["suptitle"])
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_b2_event_selection.py:109-153", fixtures=["ids_rain_mrms", "ids_rain_aorc", "ids_rain_ncei"])
def declare_rain_threshold_sweep(fixtures):
    """Run the cell that selects rain events at five thresholds for three rainfall sources; reduce to what it collects.

    The text runs unchanged and reads `np`, the three rainfall series, the two selection settings and
    `event_selection_threshold_or_nstorms`, which is bound to its stored legacy text. That function stops at its
    first statement with a `sys.exit` the register rules a stop marker (line 1274, `OBLIGED`), and the `sys` recorder
    passes it, as the run behind the kept outputs did. The value is, for each source and threshold, the event
    summary table, the event time-series table, the threshold and the event return rate, with the threshold array.
    """
    import hashlib
    import inspect
    import pathlib
    import tomllib
    from datetime import datetime

    import matplotlib
    import numpy
    import pandas
    import xarray

    matplotlib.use("agg")

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"

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

    callee_labels = {
        ("_utils", "1269-1422"): (
            "stored-text:1:545528afe9cec9b5d0031110744a573888f752a9978c5548aef65dda320ef849",
            1269,
        ),
    }
    unit_texts = [
        ("_utils.py", 1269, 1422),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own excepted
    stop_lines = [
        ("_utils.py", 1274),
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

    namespace = {
        "np": numpy,
        "pd": pandas,
        "sys": Sys(),
        "datetime": datetime,
        "return_bm_string": lambda *args, **kwargs: (
            None
        ),  # the discarded benchmark printer: its value is read by nothing
    }
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

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
        "s_rainfall_ncei_hrly_mrms_res_mm_per_hr": fixtures["ids_rain_ncei"].copy(),
        "s_mrms_rainfall_mm_per_hr": fixtures["ids_rain_mrms"].copy(),
        "s_aorc_rainfall_mm_per_hr": fixtures["ids_rain_aorc"].copy(),
        "min_interevent_time": pandas.Timedelta("12 hours"),
        "max_strm_len": pandas.Timedelta("48 hours"),
        "event_selection_threshold_or_nstorms": namespace["event_selection_threshold_or_nstorms"],
    }

    def reduce(finished):
        """The collected results of the sweep, packed by source and threshold."""
        data = {"thresholds": ("threshold", numpy.asarray(finished["ar_thresholds"], dtype=float))}
        data["columns"] = ("column", numpy.asarray(finished["columns"], dtype=str))
        for source in ("mrms", "aorc", "ncei_hrly"):
            for key, value in finished[f"dic_{source}_event_results"].items():
                name = f"{source}__{key}"
                if hasattr(value, "reset_index"):
                    data.update(pack(name, value))
                else:
                    data[name] = ((), float(value))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_b2_event_selection.py:154-268",
    fixtures=["ids_rain_mrms", "ids_rain_aorc", "ids_rain_ncei"],
    draws="unseeded",
)
def declare_rain_threshold_comparison(fixtures):
    """Run the cell that compares the rain sources at each threshold; reduce to the tables it builds and the figures it saves.

    The cell reads the results the sweep above it collected, which this declaration rebuilds by running the stored
    legacy text of the event-selection function over the same loop. The text itself runs unchanged: for each
    threshold it adds rolling event statistics to the summaries, counts events in every 365-day window of data,
    and compares the sources by `compare_2_distributions` (bound with `perform_two_sample_tests` and
    `plot_pvalue_distributions` to their stored texts), with three bootstrap resamples. The bootstrap draws through
    pandas `sample` with no seed (`_utils.py:1015-1016`), so the p-value figures' rates and bars depend on it: they
    are components named with `draw`. `shutil`, `os` and `Path` are recorders that remove and create nothing.
    """
    import hashlib
    import inspect
    import pathlib
    import textwrap
    import tomllib
    from datetime import datetime

    import matplotlib
    import numpy
    import pandas
    import seaborn
    import xarray
    from matplotlib import patches as mpatches
    from matplotlib.lines import Line2D
    from scipy import stats

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"

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

    saved = []  # one entry per figure the texts saved: the path, the save's keywords and what the figure holds

    def figure_record(figure):
        """What a figure holds: its super title and size and, for each axes, titles, labels, limits, every patch, lines, scatter points, texts and legend."""
        axes_records = []
        for axes in figure.axes:
            legend = axes.get_legend()
            axes_records.append(
                {
                    "title": axes.get_title(),
                    "labels": [axes.get_xlabel(), axes.get_ylabel()],
                    "lims": [float(v) for v in axes.get_xlim()] + [float(v) for v in axes.get_ylim()],
                    "patch_meta": [
                        f"{type(b).__name__}|{matplotlib.colors.to_hex(b.get_facecolor(), keep_alpha=True)}"
                        f"|{matplotlib.colors.to_hex(b.get_edgecolor(), keep_alpha=True)}|{b.get_alpha()}|{b.get_fill()}"
                        for b in axes.patches
                    ],
                    "patch_data": [
                        float(v)
                        for b in axes.patches
                        for v in (
                            [b.get_x(), b.get_y(), b.get_width(), b.get_height()]
                            if isinstance(b, matplotlib.patches.Rectangle)
                            else numpy.asarray(b.get_xy(), dtype=float).ravel()
                        )
                    ],
                    "lines": [
                        f"{line.get_label()}|{matplotlib.colors.to_hex(line.get_color())}|{line.get_linestyle()}"
                        f"|{[float(v) for v in numpy.asarray(line.get_xdata(), dtype=float)]}"
                        f"|{[float(v) for v in numpy.asarray(line.get_ydata(), dtype=float)]}"
                        for line in axes.lines
                    ],
                    "scatter": [
                        float(v)
                        for c in axes.collections
                        if type(c).__name__ == "PathCollection"
                        for v in numpy.asarray(c.get_offsets(), dtype=float).ravel()
                    ],
                    "texts": [
                        f"{t.get_text()}|{[float(v) for v in t.get_position()]}|{float(t.get_fontsize())}"
                        for t in axes.texts
                    ],
                    "legend": [] if legend is None else [t.get_text() for t in legend.get_texts()],
                }
            )
        return {
            "suptitle": figure._suptitle.get_text() if figure._suptitle is not None else "",
            "size": [float(v) for v in figure.get_size_inches()] + [float(figure.get_dpi())],
            "axes": axes_records,
        }

    def pack_figure(data, tag, figure, axes_slice=None):
        """Add one figure's record to `data` as numbered components named from `tag`."""
        data[f"{tag}__suptitle"] = ((), figure["suptitle"])
        data[f"{tag}__size"] = (f"{tag}__size_field", numpy.asarray(figure["size"], dtype=float))
        panels = figure["axes"] if axes_slice is None else figure["axes"][axes_slice]
        data[f"{tag}__n_axes"] = ((), len(panels))
        for index, panel in enumerate(panels):
            name = f"{tag}__ax{index}"
            data[f"{name}__title"] = ((), panel["title"])
            data[f"{name}__labels"] = (f"{name}__label", numpy.asarray(panel["labels"], dtype=str))
            data[f"{name}__lims"] = (f"{name}__lim", numpy.asarray(panel["lims"], dtype=float))
            data[f"{name}__patch_meta"] = (f"{name}__patch", numpy.asarray(panel["patch_meta"], dtype=str))
            data[f"{name}__patch_data"] = (f"{name}__patch_point", numpy.asarray(panel["patch_data"], dtype=float))
            data[f"{name}__lines"] = (f"{name}__line", numpy.asarray(panel["lines"], dtype=str))
            data[f"{name}__scatter"] = (f"{name}__point", numpy.asarray(panel["scatter"], dtype=float))
            data[f"{name}__texts"] = (f"{name}__text", numpy.asarray(panel["texts"], dtype=str))
            data[f"{name}__legend"] = (f"{name}__entry", numpy.asarray(panel["legend"], dtype=str))

    class Plt:
        """The `plt` the texts draw with: real figures in memory, a record of each save, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def tight_layout(self, *args, **kwargs):
            """Lay the current figure out."""
            pyplot.tight_layout(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, with the save's keyword arguments; write no file."""
            saved.append((str(path), repr(sorted(kwargs.items())), figure_record(pyplot.gcf())))

        def clf(self):
            """Clear the current figure, as the legacy texts do after a save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    asked = []  # the folders the cell asked to remove and to create, in order

    class Shutil:
        """`shutil` as the cell names it: `rmtree` records the folder and finds nothing to remove."""

        def rmtree(self, path):
            """Record the folder; raise as removing a folder that does not exist does."""
            asked.append(f"rmtree|{path}")
            raise FileNotFoundError(path)

        def __getattr__(self, name):
            """Refuse any other attribute."""
            raise AttributeError(f"the lift's shutil stand-in does not model shutil.{name}")

    class OsPath:
        """`os.path` as the cell names it: no folder exists."""

        def exists(self, path):
            """No folder exists in this run."""
            return False

    class Os:
        """`os` as the cell names it: only `os.path.exists`."""

        path = OsPath()

    class PathBinding:
        """`Path` as the cell names it: a call returns a handle whose `mkdir` is recorded and creates nothing."""

        def __call__(self, path):
            """A handle for `path`."""

            class Handle:
                """A path whose `mkdir` records the request."""

                def mkdir(self, *args, **kwargs):
                    """Record the folder asked for."""
                    asked.append(f"mkdir|{path}|{sorted(kwargs.items())}")

            return Handle()

    callee_labels = {
        ("_utils", "1269-1422"): (
            "stored-text:1:545528afe9cec9b5d0031110744a573888f752a9978c5548aef65dda320ef849",
            1269,
        ),
        ("_utils", "1540-1577"): (
            "stored-text:1:67673605a5435a5c38e3a5117650376841ed64ee1f9a175eb5bdc3cf5616268b",
            1540,
        ),
        ("_b2_event_selection", "88-94"): (
            "stored-text:1:b5bfe707673c4d519e616fec43a0bda4c16a2ff49a9992a5c7d6e44aad10ea18",
            88,
        ),
        ("_b2_event_selection", "95-108"): (
            "stored-text:1:269a20aa101e4c59a78d3319cd84c921324223e4ef35fa03948412810134c487",
            95,
        ),
        ("_utils", "857-921"): (
            "stored-text:1:3e0cbd9c540ac11b9f87e7993da1f4146b429e3ae6d54c828e9bfd189dce1c8e",
            857,
        ),
        ("_utils", "922-990"): (
            "stored-text:1:cec2082f2179b8214be6828a5a2b22b73d52c27a5e247bf7492853a5841a587a",
            922,
        ),
        ("_utils", "991-1055"): (
            "stored-text:1:3ba1b0d48bc7d0330a6f5120079e390e678349ac5cc20ebec05fad50b6e5fc2e",
            991,
        ),
    }
    unit_texts = [
        ("_utils.py", 1269, 1422),
        ("_utils.py", 1540, 1577),
        ("_b2_event_selection.py", 88, 94),
        ("_b2_event_selection.py", 95, 108),
        ("_utils.py", 857, 921),
        ("_utils.py", 922, 990),
        ("_utils.py", 991, 1055),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own excepted
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
    namespace = {
        "np": numpy,
        "pd": pandas,
        "sys": Sys(),
        "datetime": datetime,
        "return_bm_string": lambda *args, **kwargs: (
            None
        ),  # the discarded benchmark printer: its value is read by nothing
        "sns": seaborn,
        "mpatches": mpatches,
        "Line2D": Line2D,
        "textwrap": textwrap,
        "stats": stats,
        "plt": Plt(),
        "Path": PathBinding(),
    }
    namespace["t_window_d"] = 365
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

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

    rain = {
        "mrms": fixtures["ids_rain_mrms"],
        "aorc": fixtures["ids_rain_aorc"],
        "ncei": fixtures["ids_rain_ncei"],
    }
    gap, longest = pandas.Timedelta("12 hours"), pandas.Timedelta("48 hours")
    thresholds = numpy.linspace(40, 40 * 1.5, 5)

    def sweep(selection):
        """The legacy loop over the thresholds: for each source the summaries, series, threshold and return rate."""
        results = {"mrms": {}, "aorc": {}, "ncei_hrly": {}}
        for threshold in thresholds:
            for key, series, source in (
                ("ncei_hrly", rain["ncei"], "ncei_hrly"),
                ("mrms", rain["mrms"], "mrms"),
                ("aorc", rain["aorc"], "aorc"),
            ):
                summary, tseries, level, rate = selection(series.copy(), gap, longest, source, threshold=threshold)
                results[key][f"threshold_{threshold:.1f}mm_event_summary"] = summary
                results[key][f"threshold_{threshold:.1f}mm_event_tseries"] = tseries
                results[key][f"threshold_{threshold:.1f}mm"] = level
                results[key][f"threshold_{threshold:.1f}mm_event_return_rate"] = rate
        return results

    results = sweep(namespace["event_selection_threshold_or_nstorms"])
    bindings = {
        "np": numpy,
        "pd": pandas,
        "shutil": Shutil(),
        "os": Os(),
        "Path": PathBinding(),
        "ar_thresholds": thresholds,
        "idx": numpy.arange(0, len(thresholds)),
        "columns": [
            "thresholds_mm",
            "mrms_event_return_rate_n_per_year",
            "ncei_hrly_event_return_rate_n_per_year",
            "cvm_pval",
            "cvm_result",
            "ranksum_pval",
            "ranksum_result",
        ],
        "dic_mrms_event_results": results["mrms"],
        "dic_aorc_event_results": results["aorc"],
        "dic_ncei_hrly_event_results": results["ncei_hrly"],
        "dir_event_selection": "event_selection/",
        "s_mrms_rainfall_mm_per_hr": fixtures["ids_rain_mrms"].copy(),
        "s_aorc_rainfall_mm_per_hr": fixtures["ids_rain_aorc"].copy(),
        "s_rainfall_ncei_hrly_mrms_res_mm_per_hr": fixtures["ids_rain_ncei"].copy(),
        "lst_time_durations_h_to_analyze": [1, 6, 12],
        "compute_event_timeseries_statistics": namespace["compute_event_timeseries_statistics"],
        "find_dates_with_data": namespace["find_dates_with_data"],
        "generate_annual_event_occurance_df": namespace["generate_annual_event_occurance_df"],
        "event_date_column": "rain_event_start",
        "t_window_d": 365,
        "n_bootstrap": 3,
        "alpha": 0.05,
        "compare_2_distributions": namespace["compare_2_distributions"],
    }

    def reduce(finished):
        """The last threshold's joined summaries and window counts, the folders asked for, and the saved figures split into what does and does not depend on the draw."""
        pyplot.close("all")
        data = {
            "n_years_of_mrms": ((), int(finished["n_years_of_mrms"])),
            "n_years_of_aorc": ((), int(finished["n_years_of_aorc"])),
        }
        data["asked"] = ("asked_entry", numpy.asarray(asked, dtype=str))
        for name in (
            "df_mrms_rainevent_summary",
            "df_aorc_rainevent_summary",
            "df_ncei_hrly_rainevent_summary",
            "df_valid_and_occurance_aorc",
            "df_valid_and_occurance_mrms",
            "df_valid_and_occurance_ncei",
        ):
            data.update(pack(name, finished[name]))
        data["n_saved"] = ((), len(saved))
        for number, (path, kwargs, figure) in enumerate(saved):
            tag = f"fig{number:02d}"
            data[f"{tag}__path"] = ((), path)
            data[f"{tag}__kwargs"] = ((), kwargs)
            if len(figure["axes"]) > 1:
                # a bootstrap p-value figure: one panel per test; the panel names do not depend on the draw, the rates and bars do
                data[f"{tag}__suptitle"] = ((), figure["suptitle"])
                data[f"{tag}__n_axes"] = ((), len(figure["axes"]))
                data[f"{tag}__test_names"] = (
                    f"{tag}__axis",
                    numpy.asarray([a["title"].split("\n")[0] for a in figure["axes"]], dtype=str),
                )
                pack_figure(data, f"{tag}__draw", figure)
            else:
                pack_figure(data, tag, figure)
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_b2_event_selection.py:325-347",
    fixtures=["ids_rain_mrms", "ids_rain_aorc", "ids_mrms_bias_corrected"],
)
def declare_radar_against_gridded_rainfall(fixtures):
    """Run the cell that compares radar with gridded rainfall where both have events; reduce to the hourly comparison table and the two scatter plots.

    The cell reads the event time series of the radar and gridded sources for one threshold and a bias-corrected
    radar series; this declaration rebuilds the two event tables by running the stored legacy text of the
    event-selection function at the last threshold of the sweep (60 mm). The text runs unchanged and draws two
    scatter plots through the pandas plot accessor on real frames, which cannot be bound; the declaration reads the
    figures from pyplot after the run, and their points, labels and limits are the value. Nothing is written.
    """
    import hashlib
    import inspect
    import pathlib
    import tomllib
    from datetime import datetime

    import matplotlib
    import numpy
    import pandas
    import xarray

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"

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

    saved = []  # one entry per figure the texts saved: the path, the save's keywords and what the figure holds

    def figure_record(figure):
        """What a figure holds: its super title and size and, for each axes, titles, labels, limits, every patch, lines, scatter points, texts and legend."""
        axes_records = []
        for axes in figure.axes:
            legend = axes.get_legend()
            axes_records.append(
                {
                    "title": axes.get_title(),
                    "labels": [axes.get_xlabel(), axes.get_ylabel()],
                    "lims": [float(v) for v in axes.get_xlim()] + [float(v) for v in axes.get_ylim()],
                    "patch_meta": [
                        f"{type(b).__name__}|{matplotlib.colors.to_hex(b.get_facecolor(), keep_alpha=True)}"
                        f"|{matplotlib.colors.to_hex(b.get_edgecolor(), keep_alpha=True)}|{b.get_alpha()}|{b.get_fill()}"
                        for b in axes.patches
                    ],
                    "patch_data": [
                        float(v)
                        for b in axes.patches
                        for v in (
                            [b.get_x(), b.get_y(), b.get_width(), b.get_height()]
                            if isinstance(b, matplotlib.patches.Rectangle)
                            else numpy.asarray(b.get_xy(), dtype=float).ravel()
                        )
                    ],
                    "lines": [
                        f"{line.get_label()}|{matplotlib.colors.to_hex(line.get_color())}|{line.get_linestyle()}"
                        f"|{[float(v) for v in numpy.asarray(line.get_xdata(), dtype=float)]}"
                        f"|{[float(v) for v in numpy.asarray(line.get_ydata(), dtype=float)]}"
                        for line in axes.lines
                    ],
                    "scatter": [
                        float(v)
                        for c in axes.collections
                        if type(c).__name__ == "PathCollection"
                        for v in numpy.asarray(c.get_offsets(), dtype=float).ravel()
                    ],
                    "texts": [
                        f"{t.get_text()}|{[float(v) for v in t.get_position()]}|{float(t.get_fontsize())}"
                        for t in axes.texts
                    ],
                    "legend": [] if legend is None else [t.get_text() for t in legend.get_texts()],
                }
            )
        return {
            "suptitle": figure._suptitle.get_text() if figure._suptitle is not None else "",
            "size": [float(v) for v in figure.get_size_inches()] + [float(figure.get_dpi())],
            "axes": axes_records,
        }

    def pack_figure(data, tag, figure, axes_slice=None):
        """Add one figure's record to `data` as numbered components named from `tag`."""
        data[f"{tag}__suptitle"] = ((), figure["suptitle"])
        data[f"{tag}__size"] = (f"{tag}__size_field", numpy.asarray(figure["size"], dtype=float))
        panels = figure["axes"] if axes_slice is None else figure["axes"][axes_slice]
        data[f"{tag}__n_axes"] = ((), len(panels))
        for index, panel in enumerate(panels):
            name = f"{tag}__ax{index}"
            data[f"{name}__title"] = ((), panel["title"])
            data[f"{name}__labels"] = (f"{name}__label", numpy.asarray(panel["labels"], dtype=str))
            data[f"{name}__lims"] = (f"{name}__lim", numpy.asarray(panel["lims"], dtype=float))
            data[f"{name}__patch_meta"] = (f"{name}__patch", numpy.asarray(panel["patch_meta"], dtype=str))
            data[f"{name}__patch_data"] = (f"{name}__patch_point", numpy.asarray(panel["patch_data"], dtype=float))
            data[f"{name}__lines"] = (f"{name}__line", numpy.asarray(panel["lines"], dtype=str))
            data[f"{name}__scatter"] = (f"{name}__point", numpy.asarray(panel["scatter"], dtype=float))
            data[f"{name}__texts"] = (f"{name}__text", numpy.asarray(panel["texts"], dtype=str))
            data[f"{name}__legend"] = (f"{name}__entry", numpy.asarray(panel["legend"], dtype=str))

    class Plt:
        """The `plt` the texts draw with: real figures in memory, a record of each save, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def tight_layout(self, *args, **kwargs):
            """Lay the current figure out."""
            pyplot.tight_layout(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, with the save's keyword arguments; write no file."""
            saved.append((str(path), repr(sorted(kwargs.items())), figure_record(pyplot.gcf())))

        def clf(self):
            """Clear the current figure, as the legacy texts do after a save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    callee_labels = {
        ("_utils", "1269-1422"): (
            "stored-text:1:545528afe9cec9b5d0031110744a573888f752a9978c5548aef65dda320ef849",
            1269,
        ),
    }
    unit_texts = [
        ("_utils.py", 1269, 1422),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own excepted
    stop_lines = [
        ("_utils.py", 1274),
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

    namespace = {
        "np": numpy,
        "pd": pandas,
        "sys": Sys(),
        "datetime": datetime,
        "return_bm_string": lambda *args, **kwargs: (
            None
        ),  # the discarded benchmark printer: its value is read by nothing
    }
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

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

    gap, longest = pandas.Timedelta("12 hours"), pandas.Timedelta("48 hours")
    select = namespace["event_selection_threshold_or_nstorms"]
    _, mrms_tseries, _, _ = select(fixtures["ids_rain_mrms"].copy(), gap, longest, "mrms", threshold=60.0)
    _, aorc_tseries, _, _ = select(fixtures["ids_rain_aorc"].copy(), gap, longest, "aorc", threshold=60.0)
    pyplot.close("all")
    bindings = {
        "pd": pandas,
        "df_mrms_rainevent_tseries": mrms_tseries,
        "df_aorc_rainevent_tseries": aorc_tseries,
        "s_mrms_bias_corrected": fixtures["ids_mrms_bias_corrected"].copy(),
    }

    def reduce(finished):
        """The hourly comparison table and the two scatter figures, numbered in the order drawn."""
        data = pack("hourly", finished["df_mrms_aorc_rainrate_comarison_hourly"])
        data.update(pack("event_pairs", finished["df_mrms_aorc_rainrate_comarison"]))
        numbers = pyplot.get_fignums()
        data["n_figures"] = ((), len(numbers))
        for position, number in enumerate(numbers):
            pack_figure(data, f"fig{position}", figure_record(pyplot.figure(number)))
        pyplot.close("all")
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_b2_event_selection.py:412-552",
    form="adapted",
    fixtures=["ids_wlevels_6min", "ids_wlevels_hrly"],
)
def declare_surge_threshold_experiment(fixtures):
    """Run the adapted cell that compares two water level records over five surge thresholds; reduce to the tables it builds and the figures it saves.

    The adapted copy differs from the stored text in two lines: the two calls of `compute_event_timeseries_statistics`
    (lines 496 and 498) also pass `idx_name_time="date_time", idx_event_id="surge_event_id"`, as the next cell's call
    does, because the callee as committed drops every index level it is not told the names of, and the surge event
    series it is given are indexed by those two names. The text is otherwise unchanged. The callees are bound to their
    stored legacy texts: `surge_event_selection`, `compute_event_timeseries_statistics` (whose stop marker, line 1563,
    the `sys` recorder passes), `find_dates_with_data`, `generate_annual_event_occurance_df`,
    `compare_2_distributions`, `perform_two_sample_tests` and `plot_pvalue_distributions`. Every
    `compare_2_distributions` call of the cell passes `do_bootstrapping=False`, so nothing is drawn at random.
    `tqdm` returns its iterable, `shutil` and `Path` record the folders asked for and remove or create nothing, and
    `plt` records each saved figure. The value is the last threshold's joined event summaries, the window counts and
    yearly series, the folders asked for, and every saved figure.
    """
    import hashlib
    import inspect
    import pathlib
    import textwrap
    import tomllib
    from datetime import datetime

    import matplotlib
    import numpy
    import pandas
    import seaborn
    import xarray
    from matplotlib import patches as mpatches
    from matplotlib.lines import Line2D
    from scipy import stats

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"

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

    saved = []  # one entry per figure the texts saved: the path, the save's keywords and what the figure holds

    def figure_record(figure):
        """What a figure holds: its super title and size and, for each axes, titles, labels, limits, every patch, lines, scatter points, texts and legend."""
        axes_records = []
        for axes in figure.axes:
            legend = axes.get_legend()
            axes_records.append(
                {
                    "title": axes.get_title(),
                    "labels": [axes.get_xlabel(), axes.get_ylabel()],
                    "lims": [float(v) for v in axes.get_xlim()] + [float(v) for v in axes.get_ylim()],
                    "patch_meta": [
                        f"{type(b).__name__}|{matplotlib.colors.to_hex(b.get_facecolor(), keep_alpha=True)}"
                        f"|{matplotlib.colors.to_hex(b.get_edgecolor(), keep_alpha=True)}|{b.get_alpha()}|{b.get_fill()}"
                        for b in axes.patches
                    ],
                    "patch_data": [
                        float(v)
                        for b in axes.patches
                        for v in (
                            [b.get_x(), b.get_y(), b.get_width(), b.get_height()]
                            if isinstance(b, matplotlib.patches.Rectangle)
                            else numpy.asarray(b.get_xy(), dtype=float).ravel()
                        )
                    ],
                    "lines": [
                        f"{line.get_label()}|{matplotlib.colors.to_hex(line.get_color())}|{line.get_linestyle()}"
                        f"|{[float(v) for v in numpy.asarray(line.get_xdata(), dtype=float)]}"
                        f"|{[float(v) for v in numpy.asarray(line.get_ydata(), dtype=float)]}"
                        for line in axes.lines
                    ],
                    "scatter": [
                        float(v)
                        for c in axes.collections
                        if type(c).__name__ == "PathCollection"
                        for v in numpy.asarray(c.get_offsets(), dtype=float).ravel()
                    ],
                    "texts": [
                        f"{t.get_text()}|{[float(v) for v in t.get_position()]}|{float(t.get_fontsize())}"
                        for t in axes.texts
                    ],
                    "legend": [] if legend is None else [t.get_text() for t in legend.get_texts()],
                }
            )
        return {
            "suptitle": figure._suptitle.get_text() if figure._suptitle is not None else "",
            "size": [float(v) for v in figure.get_size_inches()] + [float(figure.get_dpi())],
            "axes": axes_records,
        }

    def pack_figure(data, tag, figure, axes_slice=None):
        """Add one figure's record to `data` as numbered components named from `tag`."""
        data[f"{tag}__suptitle"] = ((), figure["suptitle"])
        data[f"{tag}__size"] = (f"{tag}__size_field", numpy.asarray(figure["size"], dtype=float))
        panels = figure["axes"] if axes_slice is None else figure["axes"][axes_slice]
        data[f"{tag}__n_axes"] = ((), len(panels))
        for index, panel in enumerate(panels):
            name = f"{tag}__ax{index}"
            data[f"{name}__title"] = ((), panel["title"])
            data[f"{name}__labels"] = (f"{name}__label", numpy.asarray(panel["labels"], dtype=str))
            data[f"{name}__lims"] = (f"{name}__lim", numpy.asarray(panel["lims"], dtype=float))
            data[f"{name}__patch_meta"] = (f"{name}__patch", numpy.asarray(panel["patch_meta"], dtype=str))
            data[f"{name}__patch_data"] = (f"{name}__patch_point", numpy.asarray(panel["patch_data"], dtype=float))
            data[f"{name}__lines"] = (f"{name}__line", numpy.asarray(panel["lines"], dtype=str))
            data[f"{name}__scatter"] = (f"{name}__point", numpy.asarray(panel["scatter"], dtype=float))
            data[f"{name}__texts"] = (f"{name}__text", numpy.asarray(panel["texts"], dtype=str))
            data[f"{name}__legend"] = (f"{name}__entry", numpy.asarray(panel["legend"], dtype=str))

    class Plt:
        """The `plt` the texts draw with: real figures in memory, a record of each save, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def tight_layout(self, *args, **kwargs):
            """Lay the current figure out."""
            pyplot.tight_layout(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, with the save's keyword arguments; write no file."""
            saved.append((str(path), repr(sorted(kwargs.items())), figure_record(pyplot.gcf())))

        def clf(self):
            """Clear the current figure, as the legacy texts do after a save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    asked = []  # the folders the cell asked to remove and to create, in order

    class Shutil:
        """`shutil` as the cell names it: `rmtree` records the folder and finds nothing to remove."""

        def rmtree(self, path):
            """Record the folder; raise as removing a folder that does not exist does."""
            asked.append(f"rmtree|{path}")
            raise FileNotFoundError(path)

        def __getattr__(self, name):
            """Refuse any other attribute."""
            raise AttributeError(f"the lift's shutil stand-in does not model shutil.{name}")

    class OsPath:
        """`os.path` as the cell names it: no folder exists."""

        def exists(self, path):
            """No folder exists in this run."""
            return False

    class Os:
        """`os` as the cell names it: only `os.path.exists`."""

        path = OsPath()

    class PathBinding:
        """`Path` as the cell names it: a call returns a handle whose `mkdir` is recorded and creates nothing."""

        def __call__(self, path):
            """A handle for `path`."""

            class Handle:
                """A path whose `mkdir` records the request."""

                def mkdir(self, *args, **kwargs):
                    """Record the folder asked for."""
                    asked.append(f"mkdir|{path}|{sorted(kwargs.items())}")

            return Handle()

    callee_labels = {
        ("_utils", "1423-1539"): (
            "stored-text:1:46a18710bf29ab15a07a10dc9a63d44de03acdab2f64d2887da9f397e53d2614",
            1423,
        ),
        ("_utils", "1540-1577"): (
            "stored-text:1:67673605a5435a5c38e3a5117650376841ed64ee1f9a175eb5bdc3cf5616268b",
            1540,
        ),
        ("_b2_event_selection", "88-94"): (
            "stored-text:1:b5bfe707673c4d519e616fec43a0bda4c16a2ff49a9992a5c7d6e44aad10ea18",
            88,
        ),
        ("_b2_event_selection", "95-108"): (
            "stored-text:1:269a20aa101e4c59a78d3319cd84c921324223e4ef35fa03948412810134c487",
            95,
        ),
        ("_utils", "857-921"): (
            "stored-text:1:3e0cbd9c540ac11b9f87e7993da1f4146b429e3ae6d54c828e9bfd189dce1c8e",
            857,
        ),
        ("_utils", "922-990"): (
            "stored-text:1:cec2082f2179b8214be6828a5a2b22b73d52c27a5e247bf7492853a5841a587a",
            922,
        ),
        ("_utils", "991-1055"): (
            "stored-text:1:3ba1b0d48bc7d0330a6f5120079e390e678349ac5cc20ebec05fad50b6e5fc2e",
            991,
        ),
    }
    unit_texts = [
        ("_utils.py", 1423, 1539),
        ("_utils.py", 1540, 1577),
        ("_b2_event_selection.py", 88, 94),
        ("_b2_event_selection.py", 95, 108),
        ("_utils.py", 857, 921),
        ("_utils.py", 922, 990),
        ("_utils.py", 991, 1055),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own excepted
    stop_lines = [
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
    namespace = {
        "np": numpy,
        "pd": pandas,
        "sys": Sys(),
        "datetime": datetime,
        "return_bm_string": lambda *args, **kwargs: (
            None
        ),  # the discarded benchmark printer: its value is read by nothing
        "sns": seaborn,
        "mpatches": mpatches,
        "Line2D": Line2D,
        "textwrap": textwrap,
        "stats": stats,
        "plt": Plt(),
        "Path": PathBinding(),
    }
    namespace["t_window_d"] = 365
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

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

    wlevels_6min = fixtures["ids_wlevels_6min"]
    bindings = {
        "np": numpy,
        "pd": pandas,
        "tqdm": lambda iterable: iterable,
        "shutil": Shutil(),
        "Path": PathBinding(),
        "df_6min_water_levels_mrms_res": wlevels_6min.copy(),
        "df_hrly_water_levels": fixtures["ids_wlevels_hrly"].copy(),
        "surge_threshold_to_determine_event_limits": 1.0,
        "max_event_duration_h": 48,
        "min_interevent_time": pandas.Timedelta("12 hours"),
        "dir_event_selection": "event_selection/",
        "columns": [
            "thresholds_mm",
            "mrms_event_return_rate_n_per_year",
            "ncei_hrly_event_return_rate_n_per_year",
            "cvm_pval",
            "cvm_result",
            "ranksum_pval",
            "ranksum_result",
        ],
        "lst_time_durations_h_to_analyze": [1, 6, 12],
        "t_window_d": 365,
        "alpha": 0.05,
        "surge_event_selection": namespace["surge_event_selection"],
        "compute_event_timeseries_statistics": namespace["compute_event_timeseries_statistics"],
        "find_dates_with_data": namespace["find_dates_with_data"],
        "generate_annual_event_occurance_df": namespace["generate_annual_event_occurance_df"],
        "compare_2_distributions": namespace["compare_2_distributions"],
    }

    def reduce(finished):
        """The last threshold's joined summaries and window counts, the yearly series, the folders asked for, and every saved figure."""
        pyplot.close("all")
        data = {"n_yrs_wlevel_6min": ((), int(finished["n_yrs_wlevel_6min"]))}
        data["thresholds"] = ("threshold", numpy.asarray(finished["ar_thresholds"], dtype=float))
        data["asked"] = ("asked_entry", numpy.asarray(asked, dtype=str))
        for name in (
            "df_hrly_wlevel_event_summaries",
            "df_6min_wlevel_event_summaries",
            "df_valid_and_occurance_6min",
            "df_valid_and_occurance_hrly",
        ):
            data.update(pack(name, finished[name]))
        for name in ("n_per_year_6min", "n_per_year_hrly", "s_dataset1", "s_dataset2"):
            data.update(pack(name, finished[name].to_frame()))
        data["n_saved"] = ((), len(saved))
        for number, (path, kwargs, figure) in enumerate(saved):
            tag = f"fig{number:02d}"
            data[f"{tag}__path"] = ((), path)
            data[f"{tag}__kwargs"] = ((), kwargs)
            pack_figure(data, tag, figure)
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_b3_event_analysis.py:297-331", fixtures=["ids_combined_summaries"])
def declare_combined_summaries_quantile_correlations(fixtures):
    """Run the cell that splits the combined event summaries by type and lists each type's quantile correlations; reduce to the three subsets and the three correlation tables it builds.

    The text runs unchanged and reads `pd`, a file name and `calculate_quantile_correlations`, which is bound with
    `calculate_plotting_positions` to its stored legacy text (the register's `_utils.py:2200`, `OBLIGED`, is inside it).
    `pd.read_csv` returns the committed combined-summaries fixture; nothing is read.
    """
    import hashlib
    import pathlib
    import tomllib

    import matplotlib
    import numpy
    import pandas
    import xarray
    from scipy import stats

    matplotlib.use("agg")

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"

    class PdBinding:
        """pandas as the legacy cells name it, except that `read_csv` returns the committed combined-summaries fixture."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The fixture table for the combined event summaries file; any other file is an error."""
            if path != "combined_event_summaries.csv":
                raise FileNotFoundError(f"the lift reads no csv named {path}")
            return fixtures["ids_combined_summaries"].copy()

    callee_labels = {
        ("_utils", "2154-2197"): (
            "stored-text:1:f70f3a06fadba2194207593c15180344d390ef9934ef10ee54dbcdccd55393a7",
            2154,
        ),
        ("_utils", "2198-2283"): (
            "stored-text:1:ca6c1be6b02db3231ef9ea1e9df102acd8a7b50ed86a76280a2c4145e0579a56",
            2198,
        ),
    }
    texts = [("_utils.py", 2154, 2197), ("_utils.py", 2198, 2283)]
    stops = sorted(
        (entry["key"].split(":")[0], entry["line"])
        for entry in tomllib.loads((root / "tests/correspondence/known_defects.toml").read_text(encoding="utf-8"))[
            "entry"
        ]
        if entry["kind"] == "stop marker"
        and any(
            entry["key"].startswith(file + ":")
            for file, first, last in texts
            if first <= int(entry["key"].rsplit(":", 1)[1].split("-")[0]) <= last
        )
    )
    if stops:
        raise ValueError(f"the register holds stop markers {stops} inside the texts this lift runs; it binds none")

    def bring_in(namespace, labelled):
        """Compile each stored text under its own stored path, after checking its pinned label, into `namespace`."""
        for (stem, name), (label, first) in labelled.items():
            data = (root / stored / stem / f"{name}.txt").read_bytes()
            if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
                raise ValueError(
                    f"the stored text {stem}/{name}.txt is not the text this declaration was written against"
                )
            # Blank lines first, so that the text's line numbers are its legacy ones.
            exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    namespace = {"np": numpy, "pd": pandas, "stats": stats}
    bring_in(namespace, callee_labels)

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
        "pd": PdBinding(),
        "f_combined_event_summaries": "combined_event_summaries.csv",
        "calculate_quantile_correlations": namespace["calculate_quantile_correlations"],
    }

    def reduce(finished):
        """The three event subsets, their quantile correlation tables and the rain statistic the later cells analyse."""
        data = {"target": ((), str(finished["target_rainfall_intensity_variable"]))}
        for name in ("df_surge_events", "df_rain_events", "df_combo_events"):
            data.update(pack(name, finished[name]))
        for name in (
            "df_combined_summaries_corrs_of_interest_surge",
            "df_combined_summaries_corrs_of_interest_rain",
            "df_combined_summaries_corrs_of_interest_combo",
        ):
            data.update(pack(name, finished[name]))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_b3_event_analysis.py:332-394", fixtures=["ids_combined_summaries"])
def declare_surge_event_relationship_selection(fixtures):
    """Run the cell that picks the strongest rain-surge relationships among the surge-only events; reduce to the selected table and the last listing it analysed.

    The cell reads the surge-only correlation table that the cell above it builds, which this declaration rebuilds by
    running that cell's stored legacy text on the same fixture. The text itself runs unchanged, with
    `analyze_correlations` and `append_list_of_selected_vars` bound to their stored legacy texts.
    """
    import hashlib
    import pathlib
    import tomllib

    import matplotlib
    import numpy
    import pandas
    import xarray
    from scipy import stats

    matplotlib.use("agg")

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"

    class PdBinding:
        """pandas as the legacy cells name it, except that `read_csv` returns the committed combined-summaries fixture."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The fixture table for the combined event summaries file; any other file is an error."""
            if path != "combined_event_summaries.csv":
                raise FileNotFoundError(f"the lift reads no csv named {path}")
            return fixtures["ids_combined_summaries"].copy()

    callee_labels = {
        ("_utils", "2154-2197"): (
            "stored-text:1:f70f3a06fadba2194207593c15180344d390ef9934ef10ee54dbcdccd55393a7",
            2154,
        ),
        ("_utils", "2198-2283"): (
            "stored-text:1:ca6c1be6b02db3231ef9ea1e9df102acd8a7b50ed86a76280a2c4145e0579a56",
            2198,
        ),
        ("_utils", "2310-2322"): (
            "stored-text:1:55f6db240d9082022326db2f9e5cc56d3777e23949a6bdbc7c03a2320b8fd413",
            2310,
        ),
        ("_utils", "2323-2328"): (
            "stored-text:1:954f2d77d18a1826cc00422f31445af09a5746f2467e2eebdcbb139ee2d82fda",
            2323,
        ),
    }
    cell_labels = {
        ("_b3_event_analysis", "297-331"): (
            "stored-text:1:88d2e4fcdcf06f74b9b1a208acbc5730c31808252c47f1e77f804c6c1539e7bb",
            297,
        ),
    }
    texts = [
        ("_utils.py", 2154, 2197),
        ("_utils.py", 2198, 2283),
        ("_utils.py", 2310, 2322),
        ("_utils.py", 2323, 2328),
        ("_b3_event_analysis.py", 297, 331),
    ]
    stops = sorted(
        (entry["key"].split(":")[0], entry["line"])
        for entry in tomllib.loads((root / "tests/correspondence/known_defects.toml").read_text(encoding="utf-8"))[
            "entry"
        ]
        if entry["kind"] == "stop marker"
        and any(
            entry["key"].startswith(file + ":")
            for file, first, last in texts
            if first <= int(entry["key"].rsplit(":", 1)[1].split("-")[0]) <= last
        )
    )
    if stops:
        raise ValueError(f"the register holds stop markers {stops} inside the texts this lift runs; it binds none")

    def bring_in(namespace, labelled):
        """Compile each stored text under its own stored path, after checking its pinned label, into `namespace`."""
        for (stem, name), (label, first) in labelled.items():
            data = (root / stored / stem / f"{name}.txt").read_bytes()
            if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
                raise ValueError(
                    f"the stored text {stem}/{name}.txt is not the text this declaration was written against"
                )
            # Blank lines first, so that the text's line numbers are its legacy ones.
            exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    namespace = {"np": numpy, "pd": pandas, "stats": stats}
    bring_in(namespace, callee_labels)
    above = {"np": numpy, "pd": PdBinding(), "f_combined_event_summaries": "combined_event_summaries.csv"}
    above["calculate_quantile_correlations"] = namespace["calculate_quantile_correlations"]
    bring_in(above, cell_labels)

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
        "pd": pandas,
        "target_rainfall_intensity_variable": above["target_rainfall_intensity_variable"],
        "df_combined_summaries_corrs_of_interest_surge": above["df_combined_summaries_corrs_of_interest_surge"],
        "analyze_correlations": namespace["analyze_correlations"],
        "append_list_of_selected_vars": namespace["append_list_of_selected_vars"],
    }

    def reduce(finished):
        """The table of the selected relationships, sorted by absolute correlation, and the last correlation listing."""
        data = pack("selected", finished["df_variables_selected_surge"])
        data.update(pack("last_listing", finished["df_correlations_to_analyze"]))
        data["event_subset"] = ((), str(finished["event_subset"]))
        data["n_selections"] = ((), len(finished["lst_s_selections"]))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_b3_event_analysis.py:395-473", fixtures=["ids_combined_summaries"])
def declare_rain_event_relationship_selection(fixtures):
    """Run the cell that picks the strongest rain-surge relationships among the rain-only events; reduce to the selected table and the last listing it analysed.

    The cell reads the rain-only correlation table that the cell above the surge cell builds, rebuilt as for the surge
    cell; the text itself runs unchanged.
    """
    import hashlib
    import pathlib
    import tomllib

    import matplotlib
    import numpy
    import pandas
    import xarray
    from scipy import stats

    matplotlib.use("agg")

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"

    class PdBinding:
        """pandas as the legacy cells name it, except that `read_csv` returns the committed combined-summaries fixture."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The fixture table for the combined event summaries file; any other file is an error."""
            if path != "combined_event_summaries.csv":
                raise FileNotFoundError(f"the lift reads no csv named {path}")
            return fixtures["ids_combined_summaries"].copy()

    callee_labels = {
        ("_utils", "2154-2197"): (
            "stored-text:1:f70f3a06fadba2194207593c15180344d390ef9934ef10ee54dbcdccd55393a7",
            2154,
        ),
        ("_utils", "2198-2283"): (
            "stored-text:1:ca6c1be6b02db3231ef9ea1e9df102acd8a7b50ed86a76280a2c4145e0579a56",
            2198,
        ),
        ("_utils", "2310-2322"): (
            "stored-text:1:55f6db240d9082022326db2f9e5cc56d3777e23949a6bdbc7c03a2320b8fd413",
            2310,
        ),
        ("_utils", "2323-2328"): (
            "stored-text:1:954f2d77d18a1826cc00422f31445af09a5746f2467e2eebdcbb139ee2d82fda",
            2323,
        ),
    }
    cell_labels = {
        ("_b3_event_analysis", "297-331"): (
            "stored-text:1:88d2e4fcdcf06f74b9b1a208acbc5730c31808252c47f1e77f804c6c1539e7bb",
            297,
        ),
    }
    texts = [
        ("_utils.py", 2154, 2197),
        ("_utils.py", 2198, 2283),
        ("_utils.py", 2310, 2322),
        ("_utils.py", 2323, 2328),
        ("_b3_event_analysis.py", 297, 331),
    ]
    stops = sorted(
        (entry["key"].split(":")[0], entry["line"])
        for entry in tomllib.loads((root / "tests/correspondence/known_defects.toml").read_text(encoding="utf-8"))[
            "entry"
        ]
        if entry["kind"] == "stop marker"
        and any(
            entry["key"].startswith(file + ":")
            for file, first, last in texts
            if first <= int(entry["key"].rsplit(":", 1)[1].split("-")[0]) <= last
        )
    )
    if stops:
        raise ValueError(f"the register holds stop markers {stops} inside the texts this lift runs; it binds none")

    def bring_in(namespace, labelled):
        """Compile each stored text under its own stored path, after checking its pinned label, into `namespace`."""
        for (stem, name), (label, first) in labelled.items():
            data = (root / stored / stem / f"{name}.txt").read_bytes()
            if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
                raise ValueError(
                    f"the stored text {stem}/{name}.txt is not the text this declaration was written against"
                )
            # Blank lines first, so that the text's line numbers are its legacy ones.
            exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    namespace = {"np": numpy, "pd": pandas, "stats": stats}
    bring_in(namespace, callee_labels)
    above = {"np": numpy, "pd": PdBinding(), "f_combined_event_summaries": "combined_event_summaries.csv"}
    above["calculate_quantile_correlations"] = namespace["calculate_quantile_correlations"]
    bring_in(above, cell_labels)

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
        "pd": pandas,
        "target_rainfall_intensity_variable": above["target_rainfall_intensity_variable"],
        "df_combined_summaries_corrs_of_interest_rain": above["df_combined_summaries_corrs_of_interest_rain"],
        "analyze_correlations": namespace["analyze_correlations"],
        "append_list_of_selected_vars": namespace["append_list_of_selected_vars"],
    }

    def reduce(finished):
        """The table of the selected relationships, sorted by absolute correlation, and the last correlation listing."""
        data = pack("selected", finished["df_variables_selected_rain"])
        data.update(pack("last_listing", finished["df_correlations_to_analyze"]))
        data["event_subset"] = ((), str(finished["event_subset"]))
        data["n_selections"] = ((), len(finished["lst_s_selections"]))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_b3_event_analysis.py:474-543", fixtures=["ids_combined_summaries"])
def declare_compound_event_relationship_selection(fixtures):
    """Run the cell that picks the strongest rain-surge relationships among the compound events; reduce to the selected table and the last listing it analysed.

    The cell reads the compound-event correlation table rebuilt as for the surge cell; the text itself runs unchanged.
    """
    import hashlib
    import pathlib
    import tomllib

    import matplotlib
    import numpy
    import pandas
    import xarray
    from scipy import stats

    matplotlib.use("agg")

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"

    class PdBinding:
        """pandas as the legacy cells name it, except that `read_csv` returns the committed combined-summaries fixture."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The fixture table for the combined event summaries file; any other file is an error."""
            if path != "combined_event_summaries.csv":
                raise FileNotFoundError(f"the lift reads no csv named {path}")
            return fixtures["ids_combined_summaries"].copy()

    callee_labels = {
        ("_utils", "2154-2197"): (
            "stored-text:1:f70f3a06fadba2194207593c15180344d390ef9934ef10ee54dbcdccd55393a7",
            2154,
        ),
        ("_utils", "2198-2283"): (
            "stored-text:1:ca6c1be6b02db3231ef9ea1e9df102acd8a7b50ed86a76280a2c4145e0579a56",
            2198,
        ),
        ("_utils", "2310-2322"): (
            "stored-text:1:55f6db240d9082022326db2f9e5cc56d3777e23949a6bdbc7c03a2320b8fd413",
            2310,
        ),
        ("_utils", "2323-2328"): (
            "stored-text:1:954f2d77d18a1826cc00422f31445af09a5746f2467e2eebdcbb139ee2d82fda",
            2323,
        ),
    }
    cell_labels = {
        ("_b3_event_analysis", "297-331"): (
            "stored-text:1:88d2e4fcdcf06f74b9b1a208acbc5730c31808252c47f1e77f804c6c1539e7bb",
            297,
        ),
    }
    texts = [
        ("_utils.py", 2154, 2197),
        ("_utils.py", 2198, 2283),
        ("_utils.py", 2310, 2322),
        ("_utils.py", 2323, 2328),
        ("_b3_event_analysis.py", 297, 331),
    ]
    stops = sorted(
        (entry["key"].split(":")[0], entry["line"])
        for entry in tomllib.loads((root / "tests/correspondence/known_defects.toml").read_text(encoding="utf-8"))[
            "entry"
        ]
        if entry["kind"] == "stop marker"
        and any(
            entry["key"].startswith(file + ":")
            for file, first, last in texts
            if first <= int(entry["key"].rsplit(":", 1)[1].split("-")[0]) <= last
        )
    )
    if stops:
        raise ValueError(f"the register holds stop markers {stops} inside the texts this lift runs; it binds none")

    def bring_in(namespace, labelled):
        """Compile each stored text under its own stored path, after checking its pinned label, into `namespace`."""
        for (stem, name), (label, first) in labelled.items():
            data = (root / stored / stem / f"{name}.txt").read_bytes()
            if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
                raise ValueError(
                    f"the stored text {stem}/{name}.txt is not the text this declaration was written against"
                )
            # Blank lines first, so that the text's line numbers are its legacy ones.
            exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    namespace = {"np": numpy, "pd": pandas, "stats": stats}
    bring_in(namespace, callee_labels)
    above = {"np": numpy, "pd": PdBinding(), "f_combined_event_summaries": "combined_event_summaries.csv"}
    above["calculate_quantile_correlations"] = namespace["calculate_quantile_correlations"]
    bring_in(above, cell_labels)

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
        "pd": pandas,
        "target_rainfall_intensity_variable": above["target_rainfall_intensity_variable"],
        "df_combined_summaries_corrs_of_interest_combo": above["df_combined_summaries_corrs_of_interest_combo"],
        "analyze_correlations": namespace["analyze_correlations"],
        "append_list_of_selected_vars": namespace["append_list_of_selected_vars"],
    }

    def reduce(finished):
        """The table of the selected relationships, sorted by absolute correlation, and the last correlation listing."""
        data = pack("selected", finished["df_variables_selected_surge_and_rain"])
        data.update(pack("last_listing", finished["df_correlations_to_analyze"]))
        data["event_subset"] = ((), str(finished["event_subset"]))
        data["n_selections"] = ((), len(finished["lst_s_selections"]))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_b3_event_analysis.py:622-657", fixtures=["ids_combined_summaries"])
def declare_event_statistic_relationship_plots(fixtures):
    """Run the cell that plots the key relationships of each event type and writes the selected tables; reduce to the figures saved and the files written.

    The cell reads the three selected-relationship tables and the three event subsets that the cells above it build,
    which this declaration rebuilds by running those cells' stored legacy texts in order. The text itself runs
    unchanged: `plot_event_statistic_relationships` is bound with `calculate_plotting_positions` to its stored legacy
    text, `plt` records each saved figure (every patch, scatter point, caption and title) and writes nothing, and the
    cell's folder and its three csv files are written in a folder of the run's own, read back by the reduction and
    removed.
    """
    import hashlib
    import pathlib
    import shutil
    import tempfile
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

    class PdBinding:
        """pandas as the legacy cells name it, except that `read_csv` returns the committed combined-summaries fixture."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The fixture table for the combined event summaries file; any other file is an error."""
            if path != "combined_event_summaries.csv":
                raise FileNotFoundError(f"the lift reads no csv named {path}")
            return fixtures["ids_combined_summaries"].copy()

    callee_labels = {
        ("_utils", "2154-2197"): (
            "stored-text:1:f70f3a06fadba2194207593c15180344d390ef9934ef10ee54dbcdccd55393a7",
            2154,
        ),
        ("_utils", "2198-2283"): (
            "stored-text:1:ca6c1be6b02db3231ef9ea1e9df102acd8a7b50ed86a76280a2c4145e0579a56",
            2198,
        ),
        ("_utils", "2284-2309"): (
            "stored-text:1:93c6442010ceca169dc37b71e02efd6f745f63eb7a00f4942f651c9066747d5d",
            2284,
        ),
        ("_utils", "2310-2322"): (
            "stored-text:1:55f6db240d9082022326db2f9e5cc56d3777e23949a6bdbc7c03a2320b8fd413",
            2310,
        ),
        ("_utils", "2323-2328"): (
            "stored-text:1:954f2d77d18a1826cc00422f31445af09a5746f2467e2eebdcbb139ee2d82fda",
            2323,
        ),
    }
    cell_labels = {
        ("_b3_event_analysis", "297-331"): (
            "stored-text:1:88d2e4fcdcf06f74b9b1a208acbc5730c31808252c47f1e77f804c6c1539e7bb",
            297,
        ),
        ("_b3_event_analysis", "332-394"): (
            "stored-text:1:80017c9d0357cccc3282d02b3fea381bf399d292e738b6798953d5a304972191",
            332,
        ),
        ("_b3_event_analysis", "395-473"): (
            "stored-text:1:bfdea9439539a103b3ba6a54ad95b0c5110ce0e8fe5c6d022626ea455f87f2ce",
            395,
        ),
        ("_b3_event_analysis", "474-543"): (
            "stored-text:1:0ef90791a6e633db53ac46c6d761943faaa351e3759734225338e966588df6c0",
            474,
        ),
    }
    texts = [
        ("_utils.py", 2154, 2197),
        ("_utils.py", 2198, 2283),
        ("_utils.py", 2284, 2309),
        ("_utils.py", 2310, 2322),
        ("_utils.py", 2323, 2328),
        ("_b3_event_analysis.py", 297, 331),
        ("_b3_event_analysis.py", 332, 394),
        ("_b3_event_analysis.py", 395, 473),
        ("_b3_event_analysis.py", 474, 543),
    ]
    stops = sorted(
        (entry["key"].split(":")[0], entry["line"])
        for entry in tomllib.loads((root / "tests/correspondence/known_defects.toml").read_text(encoding="utf-8"))[
            "entry"
        ]
        if entry["kind"] == "stop marker"
        and any(
            entry["key"].startswith(file + ":")
            for file, first, last in texts
            if first <= int(entry["key"].rsplit(":", 1)[1].split("-")[0]) <= last
        )
    )
    if stops:
        raise ValueError(f"the register holds stop markers {stops} inside the texts this lift runs; it binds none")

    workdir = tempfile.mkdtemp(
        prefix="interactive-decision-support-lift-"
    )  # the run's own folder, removed by the reduction
    saved = []  # one entry per figure a callee saved: the path with the run's folder scrubbed, the keywords and the figure

    def figure_record(figure):
        """What a figure holds: its texts and size and, for each axes, titles, labels, limits, every patch, lines, scatter points and texts."""
        axes_records = []
        for axes in figure.axes:
            axes_records.append(
                {
                    "title": axes.get_title(),
                    "labels": [axes.get_xlabel(), axes.get_ylabel()],
                    "lims": [float(v) for v in axes.get_xlim()] + [float(v) for v in axes.get_ylim()],
                    "patches": [
                        f"{type(b).__name__}|{matplotlib.colors.to_hex(b.get_facecolor(), keep_alpha=True)}"
                        f"|{matplotlib.colors.to_hex(b.get_edgecolor(), keep_alpha=True)}|{b.get_alpha()}|{b.get_fill()}"
                        for b in axes.patches
                    ],
                    "scatter": [
                        float(v)
                        for c in axes.collections
                        if type(c).__name__ == "PathCollection"
                        for v in numpy.asarray(c.get_offsets(), dtype=float).ravel()
                    ],
                    "scatter_colours": [
                        matplotlib.colors.to_hex(c.get_facecolor()[0], keep_alpha=True)
                        for c in axes.collections
                        if type(c).__name__ == "PathCollection" and len(c.get_facecolor())
                    ],
                    "texts": [t.get_text() for t in axes.texts],
                }
            )
        return {
            "suptitle": figure._suptitle.get_text() if figure._suptitle is not None else "",
            "captions": [
                f"{t.get_text()}|{[float(v) for v in t.get_position()]}|{t.get_horizontalalignment()}"
                for t in figure.texts
            ],
            "size": [float(v) for v in figure.get_size_inches()] + [float(figure.get_dpi())],
            "axes": axes_records,
        }

    class Plt:
        """The `plt` the callees draw with: real figures in memory, a record of each save, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes, so that the pandas scatter accessor the callee uses can draw on them."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, with the save's keyword arguments; write no file."""
            saved.append(
                (str(path).replace(workdir, "<tmp>/"), repr(sorted(kwargs.items())), figure_record(pyplot.gcf()))
            )

        def clf(self):
            """Clear the current figure, as the callee does after each save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    def bring_in(namespace, labelled):
        """Compile each stored text under its own stored path, after checking its pinned label, into `namespace`."""
        for (stem, name), (label, first) in labelled.items():
            data = (root / stored / stem / f"{name}.txt").read_bytes()
            if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
                raise ValueError(
                    f"the stored text {stem}/{name}.txt is not the text this declaration was written against"
                )
            # Blank lines first, so that the text's line numbers are its legacy ones.
            exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    namespace = {"np": numpy, "pd": pandas, "stats": stats, "plt": Plt()}
    bring_in(namespace, callee_labels)
    above = {"np": numpy, "pd": PdBinding(), "f_combined_event_summaries": "combined_event_summaries.csv"}
    for name in ("calculate_quantile_correlations", "analyze_correlations", "append_list_of_selected_vars"):
        above[name] = namespace[name]
    for key, labelled in cell_labels.items():
        bring_in(above, {key: labelled})

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
        "dir_selecting_maringals": workdir + "/",
        "shutil": shutil,
        "Path": pathlib.Path,
        "plot_event_statistic_relationships": namespace["plot_event_statistic_relationships"],
        "df_variables_selected_surge": above["df_variables_selected_surge"],
        "df_variables_selected_rain": above["df_variables_selected_rain"],
        "df_variables_selected_surge_and_rain": above["df_variables_selected_surge_and_rain"],
        "df_surge_events": above["df_surge_events"],
        "df_rain_events": above["df_rain_events"],
        "df_combo_events": above["df_combo_events"],
    }

    def reduce(finished):
        """The figures saved, in order, and the three csv files the cell wrote, read back."""
        pyplot.close("all")
        data = {"n_saved": ((), len(saved))}
        for number, (path, kwargs, figure) in enumerate(saved):
            tag = f"fig{number:02d}"
            data[f"{tag}__path"] = ((), path)
            data[f"{tag}__kwargs"] = ((), kwargs)
            data[f"{tag}__suptitle"] = ((), figure["suptitle"])
            data[f"{tag}__captions"] = (f"{tag}__caption", numpy.asarray(figure["captions"], dtype=str))
            data[f"{tag}__size"] = (f"{tag}__size_field", numpy.asarray(figure["size"], dtype=float))
            for index, panel in enumerate(figure["axes"]):
                name = f"{tag}__ax{index}"
                data[f"{name}__title"] = ((), panel["title"])
                data[f"{name}__labels"] = (f"{name}__label", numpy.asarray(panel["labels"], dtype=str))
                data[f"{name}__lims"] = (f"{name}__lim", numpy.asarray(panel["lims"], dtype=float))
                data[f"{name}__patches"] = (f"{name}__patch", numpy.asarray(panel["patches"], dtype=str))
                data[f"{name}__scatter"] = (f"{name}__point", numpy.asarray(panel["scatter"], dtype=float))
                data[f"{name}__scatter_colours"] = (
                    f"{name}__swatch",
                    numpy.asarray(panel["scatter_colours"], dtype=str),
                )
                data[f"{name}__texts"] = (f"{name}__text", numpy.asarray(panel["texts"], dtype=str))
        for file in ("surge_event_marginals", "rain_event_marginals", "compound_event_marginals"):
            table = pandas.read_csv(f"{workdir}/{file}.csv")
            data.update(pack(file, table))
        shutil.rmtree(workdir)
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_b4_selecting_marginals.py:182-188",
    fixtures=[
        "ids_marg_summary_surge",
        "ids_marg_summary_rain",
        "ids_marg_summary_compound",
        "ids_marg_perf_surge",
        "ids_marg_perf_rain",
        "ids_marg_perf_compound",
    ],
)
def declare_all_fits_plot(fixtures):
    """Run the cell that refits and draws every candidate fit the scores admit, for each event type; reduce to the figures it saves and the folders it asks for.

    The text runs unchanged and reads the event statistics and summaries, the folder of all fits, the two score
    cutoffs and `plot_subset_of_fits`, which is bound with `fit_dist`, `plot_fitted_distribution` and the transformation
    functions to their stored legacy texts. `pd.read_csv` returns the committed candidate-fit table of the event type
    named by the file; `Path` records the folders asked for and creates none; `plt` records each saved figure. The title
    of each figure opens with the fit's AIC and test p-values, which the register's licensed sites (lines 158, 164 and
    199 of `__ref_ams_functions.py`) change, so that line is a component of its own, named with `scores`.
    """
    import hashlib
    import pathlib
    import tomllib

    import matplotlib
    import numpy
    import pandas
    import scipy
    import seaborn
    import xarray
    from scipy import stats

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
    saved = []  # one entry per figure the callees saved: the path, the save's keywords and what the figure holds
    asked = []  # the folders the callees asked to create

    def figure_record(figure):
        """What a figure holds: its super title and size and, for each axes, titles, labels, limits, every patch, lines, scatter points and texts."""
        axes_records = []
        for axes in figure.axes:
            legend = axes.get_legend()
            axes_records.append(
                {
                    "title": axes.get_title(),
                    "labels": [axes.get_xlabel(), axes.get_ylabel()],
                    "lims": [float(v) for v in axes.get_xlim()] + [float(v) for v in axes.get_ylim()],
                    "patch_meta": [
                        f"{type(b).__name__}|{matplotlib.colors.to_hex(b.get_facecolor(), keep_alpha=True)}"
                        f"|{matplotlib.colors.to_hex(b.get_edgecolor(), keep_alpha=True)}|{b.get_alpha()}|{b.get_fill()}"
                        for b in axes.patches
                    ],
                    "patch_data": [
                        float(v)
                        for b in axes.patches
                        for v in (
                            [b.get_x(), b.get_y(), b.get_width(), b.get_height()]
                            if isinstance(b, matplotlib.patches.Rectangle)
                            else numpy.asarray(b.get_xy(), dtype=float).ravel()
                        )
                    ],
                    "lines": [
                        f"{line.get_label()}|{matplotlib.colors.to_hex(line.get_color())}|{line.get_linestyle()}"
                        f"|{[float(v) for v in numpy.asarray(line.get_xdata(), dtype=float)]}"
                        f"|{[float(v) for v in numpy.asarray(line.get_ydata(), dtype=float)]}"
                        for line in axes.lines
                    ],
                    "texts": [t.get_text() for t in axes.texts],
                    "legend": [] if legend is None else [t.get_text() for t in legend.get_texts()],
                }
            )
        return {
            "suptitle": figure._suptitle.get_text() if figure._suptitle is not None else "",
            "size": [float(v) for v in figure.get_size_inches()] + [float(figure.get_dpi())],
            "axes": axes_records,
        }

    class Plt:
        """The `plt` the callees draw with: real figures in memory, a record of each save, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, with the save's keyword arguments; write no file."""
            saved.append((str(path), repr(sorted(kwargs.items())), figure_record(pyplot.gcf())))

        def clf(self):
            """Clear the current figure, as the callees do after a save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    class PathBinding:
        """`Path` as the callees name it: a call returns a handle whose `mkdir` is recorded and creates nothing."""

        def __call__(self, path):
            """A handle for `path`."""

            class Handle:
                """A path whose `mkdir` records the request."""

                def mkdir(self, *args, **kwargs):
                    """Record the folder asked for."""
                    asked.append(f"{path}|{sorted(kwargs.items())}")

            return Handle()

    class Sys:
        """The `sys` the callees read: `exit` raises `SystemExit`, as the legacy call does; nothing else is modelled."""

        def exit(self, *args):
            """Exit as legacy does: no register stop marker lies in the texts this lift runs."""
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    class PdBinding:
        """pandas as the callees name it, except that `read_csv` returns the committed candidate-fit table of the event type the file name states."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The fit table for `{event type}.csv` in the folder of all fits; any other file is an error."""
            name = str(path).rsplit("/", 1)[-1]
            for key, fixture in (
                ("surge_events", "ids_marg_perf_surge"),
                ("rain_events", "ids_marg_perf_rain"),
                ("compound_events", "ids_marg_perf_compound"),
            ):
                if name == key + ".csv":
                    return fixtures[fixture].copy()
            raise FileNotFoundError(f"the lift reads no csv named {path}")

    texts = [
        ("__ref_ams_functions.py", 23, 30),
        ("__ref_ams_functions.py", 31, 38),
        ("__ref_ams_functions.py", 39, 43),
        ("__ref_ams_functions.py", 44, 47),
        ("__ref_ams_functions.py", 48, 51),
        ("__ref_ams_functions.py", 52, 55),
        ("__ref_ams_functions.py", 71, 88),
        ("__ref_ams_functions.py", 89, 104),
        ("__ref_ams_functions.py", 105, 107),
        ("__ref_ams_functions.py", 108, 110),
        ("__ref_ams_functions.py", 111, 214),
        ("__ref_ams_functions.py", 215, 324),
        ("_utils.py", 1065, 1197),
        ("_utils.py", 1239, 1256),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own excepted
    stops = sorted(
        (entry["key"].split(":")[0], entry["line"])
        for entry in tomllib.loads((root / "tests/correspondence/known_defects.toml").read_text(encoding="utf-8"))[
            "entry"
        ]
        if entry["kind"] == "stop marker"
        and any(entry["key"].startswith(file + ":") and first <= entry["line"] <= last for file, first, last in texts)
    )
    if stops:
        raise ValueError(f"the register holds stop markers {stops} inside the texts this lift runs; it binds none")

    callee_labels = {
        ("__ref_ams_functions", "23-30"): (
            "stored-text:1:861c7216b43ad1b88707d69279558cd3115d5120b67182a1a1488a2149e47460",
            23,
        ),
        ("__ref_ams_functions", "31-38"): (
            "stored-text:1:19651879b8df3efb84101dfc7c6d6730a4056e76539d1b71ef9550729b5cb4b8",
            31,
        ),
        ("__ref_ams_functions", "39-43"): (
            "stored-text:1:fcbc03806cb3ee0268fb3d4a79d2492f6a3a36052e0828d298cbf889a9c1f56c",
            39,
        ),
        ("__ref_ams_functions", "44-47"): (
            "stored-text:1:fa9a3c07ac825aa2193af7cf461333193307ac90ac10d50dc1538a43d5bdb37a",
            44,
        ),
        ("__ref_ams_functions", "48-51"): (
            "stored-text:1:70049577e51fc9dc45e73f05b2998a6045353ac250849c58c3f8e7ceceb31a4d",
            48,
        ),
        ("__ref_ams_functions", "52-55"): (
            "stored-text:1:56cb15bf030fde05bc8f9108ebfc02257f55b64ee9c511985f33896f9f9abc93",
            52,
        ),
        ("__ref_ams_functions", "71-88"): (
            "stored-text:1:de1c0df7f005101b0b132b9ace3f9359b148f12e14632440e3cf5357e4b71c3e",
            71,
        ),
        ("__ref_ams_functions", "89-104"): (
            "stored-text:1:d9b815e4ac92d98b897eac33306bc3dfdcf2b2a259e380021052acdbeaf983c9",
            89,
        ),
        ("__ref_ams_functions", "105-107"): (
            "stored-text:1:41adab61721685701b64e8c2c48d958b6af414a4257781a4f9f85a0d196cd51e",
            105,
        ),
        ("__ref_ams_functions", "108-110"): (
            "stored-text:1:c510f905fc8cbe2d018c67909bbdac27f7451ba01b1b57401844e5f06a891231",
            108,
        ),
        ("__ref_ams_functions", "111-214"): (
            "stored-text:1:325dca49eeee8ebd6f4b03561fe04f7c522471a84230dc21b0cfc33cb717429b",
            111,
        ),
        ("__ref_ams_functions", "215-324"): (
            "stored-text:1:174610d747c5455adc3ef4015275572301278a5e28aa4ab1b46de5c89010ab38",
            215,
        ),
        ("_utils", "1065-1197"): (
            "stored-text:1:32479c81ba58951062e0b726bc65eb9ca849bbd89dd11c185ea62771c2583902",
            1065,
        ),
        ("_utils", "1239-1256"): (
            "stored-text:1:8776c74a18b7ecc0f0c3595ba1fade379fe84abf934ea79a9c83d77ebb8186bf",
            1239,
        ),
    }
    namespace = {
        "np": numpy,
        "pd": PdBinding(),
        "scipy": scipy,
        "stats": stats,
        "sns": seaborn,
        "sys": Sys(),
        "plt": Plt(),
        "Path": PathBinding(),
        "dir_fitting_maringals": "fitting/",
    }
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    summaries = {
        "compound_events": fixtures["ids_marg_summary_compound"].copy(),
        "rain_events": fixtures["ids_marg_summary_rain"].copy(),
        "surge_events": fixtures["ids_marg_summary_surge"].copy(),
    }
    bindings = {
        "dic_event_summaries": summaries,
        "dic_event_stats": {key: list(table.columns) for key, table in summaries.items()},
        "dir_plots_all_fitted_marginals": "fitted_distributions_all/",
        "cvm_cutoff": 0.2,
        "ks_cutoff": 0.2,
        "plot_subset_of_fits": namespace["plot_subset_of_fits"],
    }

    def reduce(finished):
        """The figures saved, in order, with the score line of each title set apart, and the folders asked for."""
        pyplot.close("all")
        data = {"asked": ("asked_entry", numpy.asarray(asked, dtype=str)), "n_saved": ((), len(saved))}
        for number, (path, kwargs, figure) in enumerate(saved):
            tag = f"fig{number:02d}"
            data[f"{tag}__path"] = ((), path)
            data[f"{tag}__kwargs"] = ((), kwargs)
            # the first line of the title states the fit's AIC and test p-values, which the licensed sites at
            # `__ref_ams_functions.py` lines 158, 164 and 199 change; the rest states the support and transformations
            first, _, rest = figure["suptitle"].partition("\n")
            data[f"{tag}__scores_line"] = ((), first)
            data[f"{tag}__suptitle_rest"] = ((), rest)
            data[f"{tag}__size"] = (f"{tag}__size_field", numpy.asarray(figure["size"], dtype=float))
            for index, panel in enumerate(figure["axes"]):
                name = f"{tag}__ax{index}"
                data[f"{name}__title"] = ((), panel["title"])
                data[f"{name}__labels"] = (f"{name}__label", numpy.asarray(panel["labels"], dtype=str))
                data[f"{name}__lims"] = (f"{name}__lim", numpy.asarray(panel["lims"], dtype=float))
                data[f"{name}__patch_meta"] = (f"{name}__patch", numpy.asarray(panel["patch_meta"], dtype=str))
                data[f"{name}__patch_data"] = (f"{name}__patch_point", numpy.asarray(panel["patch_data"], dtype=float))
                data[f"{name}__lines"] = (f"{name}__line", numpy.asarray(panel["lines"], dtype=str))
                data[f"{name}__texts"] = (f"{name}__text", numpy.asarray(panel["texts"], dtype=str))
                data[f"{name}__legend"] = (f"{name}__entry", numpy.asarray(panel["legend"], dtype=str))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_b4_selecting_marginals.py:189-208",
    fixtures=[
        "ids_marg_summary_surge",
        "ids_marg_summary_rain",
        "ids_marg_summary_compound",
        "ids_marg_perf_surge",
        "ids_marg_perf_rain",
        "ids_marg_perf_compound",
    ],
)
def declare_coarse_surge_fit_selection(fixtures):
    """Run the cell that redraws the hand-picked surge-event fits by file name and checks every statistic is covered; reduce to the figures saved, the folders asked for and the statistics accounted for.

    The names the cell lists are matched against the names the candidate-fit table generates; a name that matches no
    row draws nothing. The callees and bindings are those of the cell that draws every fit; the title's score line is a
    component of its own, named with `scores`.
    """
    import hashlib
    import pathlib
    import tomllib

    import matplotlib
    import numpy
    import pandas
    import scipy
    import seaborn
    import xarray
    from scipy import stats

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
    saved = []  # one entry per figure the callees saved: the path, the save's keywords and what the figure holds
    asked = []  # the folders the callees asked to create

    def figure_record(figure):
        """What a figure holds: its super title and size and, for each axes, titles, labels, limits, every patch, lines, scatter points and texts."""
        axes_records = []
        for axes in figure.axes:
            legend = axes.get_legend()
            axes_records.append(
                {
                    "title": axes.get_title(),
                    "labels": [axes.get_xlabel(), axes.get_ylabel()],
                    "lims": [float(v) for v in axes.get_xlim()] + [float(v) for v in axes.get_ylim()],
                    "patch_meta": [
                        f"{type(b).__name__}|{matplotlib.colors.to_hex(b.get_facecolor(), keep_alpha=True)}"
                        f"|{matplotlib.colors.to_hex(b.get_edgecolor(), keep_alpha=True)}|{b.get_alpha()}|{b.get_fill()}"
                        for b in axes.patches
                    ],
                    "patch_data": [
                        float(v)
                        for b in axes.patches
                        for v in (
                            [b.get_x(), b.get_y(), b.get_width(), b.get_height()]
                            if isinstance(b, matplotlib.patches.Rectangle)
                            else numpy.asarray(b.get_xy(), dtype=float).ravel()
                        )
                    ],
                    "lines": [
                        f"{line.get_label()}|{matplotlib.colors.to_hex(line.get_color())}|{line.get_linestyle()}"
                        f"|{[float(v) for v in numpy.asarray(line.get_xdata(), dtype=float)]}"
                        f"|{[float(v) for v in numpy.asarray(line.get_ydata(), dtype=float)]}"
                        for line in axes.lines
                    ],
                    "texts": [t.get_text() for t in axes.texts],
                    "legend": [] if legend is None else [t.get_text() for t in legend.get_texts()],
                }
            )
        return {
            "suptitle": figure._suptitle.get_text() if figure._suptitle is not None else "",
            "size": [float(v) for v in figure.get_size_inches()] + [float(figure.get_dpi())],
            "axes": axes_records,
        }

    class Plt:
        """The `plt` the callees draw with: real figures in memory, a record of each save, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, with the save's keyword arguments; write no file."""
            saved.append((str(path), repr(sorted(kwargs.items())), figure_record(pyplot.gcf())))

        def clf(self):
            """Clear the current figure, as the callees do after a save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    class PathBinding:
        """`Path` as the callees name it: a call returns a handle whose `mkdir` is recorded and creates nothing."""

        def __call__(self, path):
            """A handle for `path`."""

            class Handle:
                """A path whose `mkdir` records the request."""

                def mkdir(self, *args, **kwargs):
                    """Record the folder asked for."""
                    asked.append(f"{path}|{sorted(kwargs.items())}")

            return Handle()

    class Sys:
        """The `sys` the callees read: `exit` raises `SystemExit`, as the legacy call does; nothing else is modelled."""

        def exit(self, *args):
            """Exit as legacy does: no register stop marker lies in the texts this lift runs."""
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    class PdBinding:
        """pandas as the callees name it, except that `read_csv` returns the committed candidate-fit table of the event type the file name states."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The fit table for `{event type}.csv` in the folder of all fits; any other file is an error."""
            name = str(path).rsplit("/", 1)[-1]
            for key, fixture in (
                ("surge_events", "ids_marg_perf_surge"),
                ("rain_events", "ids_marg_perf_rain"),
                ("compound_events", "ids_marg_perf_compound"),
            ):
                if name == key + ".csv":
                    return fixtures[fixture].copy()
            raise FileNotFoundError(f"the lift reads no csv named {path}")

    texts = [
        ("__ref_ams_functions.py", 23, 30),
        ("__ref_ams_functions.py", 31, 38),
        ("__ref_ams_functions.py", 39, 43),
        ("__ref_ams_functions.py", 44, 47),
        ("__ref_ams_functions.py", 48, 51),
        ("__ref_ams_functions.py", 52, 55),
        ("__ref_ams_functions.py", 71, 88),
        ("__ref_ams_functions.py", 89, 104),
        ("__ref_ams_functions.py", 105, 107),
        ("__ref_ams_functions.py", 108, 110),
        ("__ref_ams_functions.py", 111, 214),
        ("__ref_ams_functions.py", 215, 324),
        ("_utils.py", 1065, 1197),
        ("_utils.py", 1239, 1256),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own excepted
    stops = sorted(
        (entry["key"].split(":")[0], entry["line"])
        for entry in tomllib.loads((root / "tests/correspondence/known_defects.toml").read_text(encoding="utf-8"))[
            "entry"
        ]
        if entry["kind"] == "stop marker"
        and any(entry["key"].startswith(file + ":") and first <= entry["line"] <= last for file, first, last in texts)
    )
    if stops:
        raise ValueError(f"the register holds stop markers {stops} inside the texts this lift runs; it binds none")

    callee_labels = {
        ("__ref_ams_functions", "23-30"): (
            "stored-text:1:861c7216b43ad1b88707d69279558cd3115d5120b67182a1a1488a2149e47460",
            23,
        ),
        ("__ref_ams_functions", "31-38"): (
            "stored-text:1:19651879b8df3efb84101dfc7c6d6730a4056e76539d1b71ef9550729b5cb4b8",
            31,
        ),
        ("__ref_ams_functions", "39-43"): (
            "stored-text:1:fcbc03806cb3ee0268fb3d4a79d2492f6a3a36052e0828d298cbf889a9c1f56c",
            39,
        ),
        ("__ref_ams_functions", "44-47"): (
            "stored-text:1:fa9a3c07ac825aa2193af7cf461333193307ac90ac10d50dc1538a43d5bdb37a",
            44,
        ),
        ("__ref_ams_functions", "48-51"): (
            "stored-text:1:70049577e51fc9dc45e73f05b2998a6045353ac250849c58c3f8e7ceceb31a4d",
            48,
        ),
        ("__ref_ams_functions", "52-55"): (
            "stored-text:1:56cb15bf030fde05bc8f9108ebfc02257f55b64ee9c511985f33896f9f9abc93",
            52,
        ),
        ("__ref_ams_functions", "71-88"): (
            "stored-text:1:de1c0df7f005101b0b132b9ace3f9359b148f12e14632440e3cf5357e4b71c3e",
            71,
        ),
        ("__ref_ams_functions", "89-104"): (
            "stored-text:1:d9b815e4ac92d98b897eac33306bc3dfdcf2b2a259e380021052acdbeaf983c9",
            89,
        ),
        ("__ref_ams_functions", "105-107"): (
            "stored-text:1:41adab61721685701b64e8c2c48d958b6af414a4257781a4f9f85a0d196cd51e",
            105,
        ),
        ("__ref_ams_functions", "108-110"): (
            "stored-text:1:c510f905fc8cbe2d018c67909bbdac27f7451ba01b1b57401844e5f06a891231",
            108,
        ),
        ("__ref_ams_functions", "111-214"): (
            "stored-text:1:325dca49eeee8ebd6f4b03561fe04f7c522471a84230dc21b0cfc33cb717429b",
            111,
        ),
        ("__ref_ams_functions", "215-324"): (
            "stored-text:1:174610d747c5455adc3ef4015275572301278a5e28aa4ab1b46de5c89010ab38",
            215,
        ),
        ("_utils", "1065-1197"): (
            "stored-text:1:32479c81ba58951062e0b726bc65eb9ca849bbd89dd11c185ea62771c2583902",
            1065,
        ),
        ("_utils", "1239-1256"): (
            "stored-text:1:8776c74a18b7ecc0f0c3595ba1fade379fe84abf934ea79a9c83d77ebb8186bf",
            1239,
        ),
    }
    namespace = {
        "np": numpy,
        "pd": PdBinding(),
        "scipy": scipy,
        "stats": stats,
        "sns": seaborn,
        "sys": Sys(),
        "plt": Plt(),
        "Path": PathBinding(),
        "dir_fitting_maringals": "fitting/",
    }
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    summaries = {
        "compound_events": fixtures["ids_marg_summary_compound"].copy(),
        "rain_events": fixtures["ids_marg_summary_rain"].copy(),
        "surge_events": fixtures["ids_marg_summary_surge"].copy(),
    }
    bindings = {
        "dic_event_summaries": summaries,
        "dic_event_stats": {key: list(table.columns) for key, table in summaries.items()},
        "dir_plots_all_fitted_marginals": "fitted_distributions_all/",
        "cvm_cutoff": 0.2,
        "ks_cutoff": 0.2,
        "reproduce_select_plots_and_verify_all_vars_accounted_for": namespace[
            "reproduce_select_plots_and_verify_all_vars_accounted_for"
        ],
    }

    def reduce(finished):
        """The figures saved, in order, with the score line of each title set apart, and the folders asked for and the variables accounted for."""
        pyplot.close("all")
        data = {"asked": ("asked_entry", numpy.asarray(asked, dtype=str)), "n_saved": ((), len(saved))}
        for number, (path, kwargs, figure) in enumerate(saved):
            tag = f"fig{number:02d}"
            data[f"{tag}__path"] = ((), path)
            data[f"{tag}__kwargs"] = ((), kwargs)
            # the first line of the title states the fit's AIC and test p-values, which the licensed sites at
            # `__ref_ams_functions.py` lines 158, 164 and 199 change; the rest states the support and transformations
            first, _, rest = figure["suptitle"].partition("\n")
            data[f"{tag}__scores_line"] = ((), first)
            data[f"{tag}__suptitle_rest"] = ((), rest)
            data[f"{tag}__size"] = (f"{tag}__size_field", numpy.asarray(figure["size"], dtype=float))
            for index, panel in enumerate(figure["axes"]):
                name = f"{tag}__ax{index}"
                data[f"{name}__title"] = ((), panel["title"])
                data[f"{name}__labels"] = (f"{name}__label", numpy.asarray(panel["labels"], dtype=str))
                data[f"{name}__lims"] = (f"{name}__lim", numpy.asarray(panel["lims"], dtype=float))
                data[f"{name}__patch_meta"] = (f"{name}__patch", numpy.asarray(panel["patch_meta"], dtype=str))
                data[f"{name}__patch_data"] = (f"{name}__patch_point", numpy.asarray(panel["patch_data"], dtype=float))
                data[f"{name}__lines"] = (f"{name}__line", numpy.asarray(panel["lines"], dtype=str))
                data[f"{name}__texts"] = (f"{name}__text", numpy.asarray(panel["texts"], dtype=str))
                data[f"{name}__legend"] = (f"{name}__entry", numpy.asarray(panel["legend"], dtype=str))
        data["which_event_key"] = ((), str(finished["which_event_key"]))
        accounted = finished["s_vars_accounted_for"]
        data["accounted_vars"] = ("accounted_var", numpy.asarray(accounted.index, dtype=str))
        data["accounted_flags"] = ("accounted_var", accounted.to_numpy(dtype=bool))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_b4_selecting_marginals.py:209-221",
    fixtures=[
        "ids_marg_summary_surge",
        "ids_marg_summary_rain",
        "ids_marg_summary_compound",
        "ids_marg_perf_surge",
        "ids_marg_perf_rain",
        "ids_marg_perf_compound",
    ],
)
def declare_coarse_rain_fit_selection(fixtures):
    """Run the cell that redraws the hand-picked rain-event fits by file name and checks every statistic is covered; reduce to the figures saved, the folders asked for and the statistics accounted for.

    As the surge cell, for the rain-only events.
    """
    import hashlib
    import pathlib
    import tomllib

    import matplotlib
    import numpy
    import pandas
    import scipy
    import seaborn
    import xarray
    from scipy import stats

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
    saved = []  # one entry per figure the callees saved: the path, the save's keywords and what the figure holds
    asked = []  # the folders the callees asked to create

    def figure_record(figure):
        """What a figure holds: its super title and size and, for each axes, titles, labels, limits, every patch, lines, scatter points and texts."""
        axes_records = []
        for axes in figure.axes:
            legend = axes.get_legend()
            axes_records.append(
                {
                    "title": axes.get_title(),
                    "labels": [axes.get_xlabel(), axes.get_ylabel()],
                    "lims": [float(v) for v in axes.get_xlim()] + [float(v) for v in axes.get_ylim()],
                    "patch_meta": [
                        f"{type(b).__name__}|{matplotlib.colors.to_hex(b.get_facecolor(), keep_alpha=True)}"
                        f"|{matplotlib.colors.to_hex(b.get_edgecolor(), keep_alpha=True)}|{b.get_alpha()}|{b.get_fill()}"
                        for b in axes.patches
                    ],
                    "patch_data": [
                        float(v)
                        for b in axes.patches
                        for v in (
                            [b.get_x(), b.get_y(), b.get_width(), b.get_height()]
                            if isinstance(b, matplotlib.patches.Rectangle)
                            else numpy.asarray(b.get_xy(), dtype=float).ravel()
                        )
                    ],
                    "lines": [
                        f"{line.get_label()}|{matplotlib.colors.to_hex(line.get_color())}|{line.get_linestyle()}"
                        f"|{[float(v) for v in numpy.asarray(line.get_xdata(), dtype=float)]}"
                        f"|{[float(v) for v in numpy.asarray(line.get_ydata(), dtype=float)]}"
                        for line in axes.lines
                    ],
                    "texts": [t.get_text() for t in axes.texts],
                    "legend": [] if legend is None else [t.get_text() for t in legend.get_texts()],
                }
            )
        return {
            "suptitle": figure._suptitle.get_text() if figure._suptitle is not None else "",
            "size": [float(v) for v in figure.get_size_inches()] + [float(figure.get_dpi())],
            "axes": axes_records,
        }

    class Plt:
        """The `plt` the callees draw with: real figures in memory, a record of each save, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, with the save's keyword arguments; write no file."""
            saved.append((str(path), repr(sorted(kwargs.items())), figure_record(pyplot.gcf())))

        def clf(self):
            """Clear the current figure, as the callees do after a save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    class PathBinding:
        """`Path` as the callees name it: a call returns a handle whose `mkdir` is recorded and creates nothing."""

        def __call__(self, path):
            """A handle for `path`."""

            class Handle:
                """A path whose `mkdir` records the request."""

                def mkdir(self, *args, **kwargs):
                    """Record the folder asked for."""
                    asked.append(f"{path}|{sorted(kwargs.items())}")

            return Handle()

    class Sys:
        """The `sys` the callees read: `exit` raises `SystemExit`, as the legacy call does; nothing else is modelled."""

        def exit(self, *args):
            """Exit as legacy does: no register stop marker lies in the texts this lift runs."""
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    class PdBinding:
        """pandas as the callees name it, except that `read_csv` returns the committed candidate-fit table of the event type the file name states."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The fit table for `{event type}.csv` in the folder of all fits; any other file is an error."""
            name = str(path).rsplit("/", 1)[-1]
            for key, fixture in (
                ("surge_events", "ids_marg_perf_surge"),
                ("rain_events", "ids_marg_perf_rain"),
                ("compound_events", "ids_marg_perf_compound"),
            ):
                if name == key + ".csv":
                    return fixtures[fixture].copy()
            raise FileNotFoundError(f"the lift reads no csv named {path}")

    texts = [
        ("__ref_ams_functions.py", 23, 30),
        ("__ref_ams_functions.py", 31, 38),
        ("__ref_ams_functions.py", 39, 43),
        ("__ref_ams_functions.py", 44, 47),
        ("__ref_ams_functions.py", 48, 51),
        ("__ref_ams_functions.py", 52, 55),
        ("__ref_ams_functions.py", 71, 88),
        ("__ref_ams_functions.py", 89, 104),
        ("__ref_ams_functions.py", 105, 107),
        ("__ref_ams_functions.py", 108, 110),
        ("__ref_ams_functions.py", 111, 214),
        ("__ref_ams_functions.py", 215, 324),
        ("_utils.py", 1065, 1197),
        ("_utils.py", 1239, 1256),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own excepted
    stops = sorted(
        (entry["key"].split(":")[0], entry["line"])
        for entry in tomllib.loads((root / "tests/correspondence/known_defects.toml").read_text(encoding="utf-8"))[
            "entry"
        ]
        if entry["kind"] == "stop marker"
        and any(entry["key"].startswith(file + ":") and first <= entry["line"] <= last for file, first, last in texts)
    )
    if stops:
        raise ValueError(f"the register holds stop markers {stops} inside the texts this lift runs; it binds none")

    callee_labels = {
        ("__ref_ams_functions", "23-30"): (
            "stored-text:1:861c7216b43ad1b88707d69279558cd3115d5120b67182a1a1488a2149e47460",
            23,
        ),
        ("__ref_ams_functions", "31-38"): (
            "stored-text:1:19651879b8df3efb84101dfc7c6d6730a4056e76539d1b71ef9550729b5cb4b8",
            31,
        ),
        ("__ref_ams_functions", "39-43"): (
            "stored-text:1:fcbc03806cb3ee0268fb3d4a79d2492f6a3a36052e0828d298cbf889a9c1f56c",
            39,
        ),
        ("__ref_ams_functions", "44-47"): (
            "stored-text:1:fa9a3c07ac825aa2193af7cf461333193307ac90ac10d50dc1538a43d5bdb37a",
            44,
        ),
        ("__ref_ams_functions", "48-51"): (
            "stored-text:1:70049577e51fc9dc45e73f05b2998a6045353ac250849c58c3f8e7ceceb31a4d",
            48,
        ),
        ("__ref_ams_functions", "52-55"): (
            "stored-text:1:56cb15bf030fde05bc8f9108ebfc02257f55b64ee9c511985f33896f9f9abc93",
            52,
        ),
        ("__ref_ams_functions", "71-88"): (
            "stored-text:1:de1c0df7f005101b0b132b9ace3f9359b148f12e14632440e3cf5357e4b71c3e",
            71,
        ),
        ("__ref_ams_functions", "89-104"): (
            "stored-text:1:d9b815e4ac92d98b897eac33306bc3dfdcf2b2a259e380021052acdbeaf983c9",
            89,
        ),
        ("__ref_ams_functions", "105-107"): (
            "stored-text:1:41adab61721685701b64e8c2c48d958b6af414a4257781a4f9f85a0d196cd51e",
            105,
        ),
        ("__ref_ams_functions", "108-110"): (
            "stored-text:1:c510f905fc8cbe2d018c67909bbdac27f7451ba01b1b57401844e5f06a891231",
            108,
        ),
        ("__ref_ams_functions", "111-214"): (
            "stored-text:1:325dca49eeee8ebd6f4b03561fe04f7c522471a84230dc21b0cfc33cb717429b",
            111,
        ),
        ("__ref_ams_functions", "215-324"): (
            "stored-text:1:174610d747c5455adc3ef4015275572301278a5e28aa4ab1b46de5c89010ab38",
            215,
        ),
        ("_utils", "1065-1197"): (
            "stored-text:1:32479c81ba58951062e0b726bc65eb9ca849bbd89dd11c185ea62771c2583902",
            1065,
        ),
        ("_utils", "1239-1256"): (
            "stored-text:1:8776c74a18b7ecc0f0c3595ba1fade379fe84abf934ea79a9c83d77ebb8186bf",
            1239,
        ),
    }
    namespace = {
        "np": numpy,
        "pd": PdBinding(),
        "scipy": scipy,
        "stats": stats,
        "sns": seaborn,
        "sys": Sys(),
        "plt": Plt(),
        "Path": PathBinding(),
        "dir_fitting_maringals": "fitting/",
    }
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    summaries = {
        "compound_events": fixtures["ids_marg_summary_compound"].copy(),
        "rain_events": fixtures["ids_marg_summary_rain"].copy(),
        "surge_events": fixtures["ids_marg_summary_surge"].copy(),
    }
    bindings = {
        "dic_event_summaries": summaries,
        "dic_event_stats": {key: list(table.columns) for key, table in summaries.items()},
        "dir_plots_all_fitted_marginals": "fitted_distributions_all/",
        "cvm_cutoff": 0.2,
        "ks_cutoff": 0.2,
        "reproduce_select_plots_and_verify_all_vars_accounted_for": namespace[
            "reproduce_select_plots_and_verify_all_vars_accounted_for"
        ],
    }

    def reduce(finished):
        """The figures saved, in order, with the score line of each title set apart, and the folders asked for and the variables accounted for."""
        pyplot.close("all")
        data = {"asked": ("asked_entry", numpy.asarray(asked, dtype=str)), "n_saved": ((), len(saved))}
        for number, (path, kwargs, figure) in enumerate(saved):
            tag = f"fig{number:02d}"
            data[f"{tag}__path"] = ((), path)
            data[f"{tag}__kwargs"] = ((), kwargs)
            # the first line of the title states the fit's AIC and test p-values, which the licensed sites at
            # `__ref_ams_functions.py` lines 158, 164 and 199 change; the rest states the support and transformations
            first, _, rest = figure["suptitle"].partition("\n")
            data[f"{tag}__scores_line"] = ((), first)
            data[f"{tag}__suptitle_rest"] = ((), rest)
            data[f"{tag}__size"] = (f"{tag}__size_field", numpy.asarray(figure["size"], dtype=float))
            for index, panel in enumerate(figure["axes"]):
                name = f"{tag}__ax{index}"
                data[f"{name}__title"] = ((), panel["title"])
                data[f"{name}__labels"] = (f"{name}__label", numpy.asarray(panel["labels"], dtype=str))
                data[f"{name}__lims"] = (f"{name}__lim", numpy.asarray(panel["lims"], dtype=float))
                data[f"{name}__patch_meta"] = (f"{name}__patch", numpy.asarray(panel["patch_meta"], dtype=str))
                data[f"{name}__patch_data"] = (f"{name}__patch_point", numpy.asarray(panel["patch_data"], dtype=float))
                data[f"{name}__lines"] = (f"{name}__line", numpy.asarray(panel["lines"], dtype=str))
                data[f"{name}__texts"] = (f"{name}__text", numpy.asarray(panel["texts"], dtype=str))
                data[f"{name}__legend"] = (f"{name}__entry", numpy.asarray(panel["legend"], dtype=str))
        data["which_event_key"] = ((), str(finished["which_event_key"]))
        accounted = finished["s_vars_accounted_for"]
        data["accounted_vars"] = ("accounted_var", numpy.asarray(accounted.index, dtype=str))
        data["accounted_flags"] = ("accounted_var", accounted.to_numpy(dtype=bool))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_b4_selecting_marginals.py:222-249",
    fixtures=[
        "ids_marg_summary_surge",
        "ids_marg_summary_rain",
        "ids_marg_summary_compound",
        "ids_marg_perf_surge",
        "ids_marg_perf_rain",
        "ids_marg_perf_compound",
    ],
)
def declare_coarse_compound_fit_selection(fixtures):
    """Run the cell that redraws the hand-picked compound-event fits by file name and checks every statistic is covered; reduce to the figures saved, the folders asked for and the statistics accounted for.

    As the surge cell, for the compound events.
    """
    import hashlib
    import pathlib
    import tomllib

    import matplotlib
    import numpy
    import pandas
    import scipy
    import seaborn
    import xarray
    from scipy import stats

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
    saved = []  # one entry per figure the callees saved: the path, the save's keywords and what the figure holds
    asked = []  # the folders the callees asked to create

    def figure_record(figure):
        """What a figure holds: its super title and size and, for each axes, titles, labels, limits, every patch, lines, scatter points and texts."""
        axes_records = []
        for axes in figure.axes:
            legend = axes.get_legend()
            axes_records.append(
                {
                    "title": axes.get_title(),
                    "labels": [axes.get_xlabel(), axes.get_ylabel()],
                    "lims": [float(v) for v in axes.get_xlim()] + [float(v) for v in axes.get_ylim()],
                    "patch_meta": [
                        f"{type(b).__name__}|{matplotlib.colors.to_hex(b.get_facecolor(), keep_alpha=True)}"
                        f"|{matplotlib.colors.to_hex(b.get_edgecolor(), keep_alpha=True)}|{b.get_alpha()}|{b.get_fill()}"
                        for b in axes.patches
                    ],
                    "patch_data": [
                        float(v)
                        for b in axes.patches
                        for v in (
                            [b.get_x(), b.get_y(), b.get_width(), b.get_height()]
                            if isinstance(b, matplotlib.patches.Rectangle)
                            else numpy.asarray(b.get_xy(), dtype=float).ravel()
                        )
                    ],
                    "lines": [
                        f"{line.get_label()}|{matplotlib.colors.to_hex(line.get_color())}|{line.get_linestyle()}"
                        f"|{[float(v) for v in numpy.asarray(line.get_xdata(), dtype=float)]}"
                        f"|{[float(v) for v in numpy.asarray(line.get_ydata(), dtype=float)]}"
                        for line in axes.lines
                    ],
                    "texts": [t.get_text() for t in axes.texts],
                    "legend": [] if legend is None else [t.get_text() for t in legend.get_texts()],
                }
            )
        return {
            "suptitle": figure._suptitle.get_text() if figure._suptitle is not None else "",
            "size": [float(v) for v in figure.get_size_inches()] + [float(figure.get_dpi())],
            "axes": axes_records,
        }

    class Plt:
        """The `plt` the callees draw with: real figures in memory, a record of each save, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, with the save's keyword arguments; write no file."""
            saved.append((str(path), repr(sorted(kwargs.items())), figure_record(pyplot.gcf())))

        def clf(self):
            """Clear the current figure, as the callees do after a save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    class PathBinding:
        """`Path` as the callees name it: a call returns a handle whose `mkdir` is recorded and creates nothing."""

        def __call__(self, path):
            """A handle for `path`."""

            class Handle:
                """A path whose `mkdir` records the request."""

                def mkdir(self, *args, **kwargs):
                    """Record the folder asked for."""
                    asked.append(f"{path}|{sorted(kwargs.items())}")

            return Handle()

    class Sys:
        """The `sys` the callees read: `exit` raises `SystemExit`, as the legacy call does; nothing else is modelled."""

        def exit(self, *args):
            """Exit as legacy does: no register stop marker lies in the texts this lift runs."""
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    class PdBinding:
        """pandas as the callees name it, except that `read_csv` returns the committed candidate-fit table of the event type the file name states."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The fit table for `{event type}.csv` in the folder of all fits; any other file is an error."""
            name = str(path).rsplit("/", 1)[-1]
            for key, fixture in (
                ("surge_events", "ids_marg_perf_surge"),
                ("rain_events", "ids_marg_perf_rain"),
                ("compound_events", "ids_marg_perf_compound"),
            ):
                if name == key + ".csv":
                    return fixtures[fixture].copy()
            raise FileNotFoundError(f"the lift reads no csv named {path}")

    texts = [
        ("__ref_ams_functions.py", 23, 30),
        ("__ref_ams_functions.py", 31, 38),
        ("__ref_ams_functions.py", 39, 43),
        ("__ref_ams_functions.py", 44, 47),
        ("__ref_ams_functions.py", 48, 51),
        ("__ref_ams_functions.py", 52, 55),
        ("__ref_ams_functions.py", 71, 88),
        ("__ref_ams_functions.py", 89, 104),
        ("__ref_ams_functions.py", 105, 107),
        ("__ref_ams_functions.py", 108, 110),
        ("__ref_ams_functions.py", 111, 214),
        ("__ref_ams_functions.py", 215, 324),
        ("_utils.py", 1065, 1197),
        ("_utils.py", 1239, 1256),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own excepted
    stops = sorted(
        (entry["key"].split(":")[0], entry["line"])
        for entry in tomllib.loads((root / "tests/correspondence/known_defects.toml").read_text(encoding="utf-8"))[
            "entry"
        ]
        if entry["kind"] == "stop marker"
        and any(entry["key"].startswith(file + ":") and first <= entry["line"] <= last for file, first, last in texts)
    )
    if stops:
        raise ValueError(f"the register holds stop markers {stops} inside the texts this lift runs; it binds none")

    callee_labels = {
        ("__ref_ams_functions", "23-30"): (
            "stored-text:1:861c7216b43ad1b88707d69279558cd3115d5120b67182a1a1488a2149e47460",
            23,
        ),
        ("__ref_ams_functions", "31-38"): (
            "stored-text:1:19651879b8df3efb84101dfc7c6d6730a4056e76539d1b71ef9550729b5cb4b8",
            31,
        ),
        ("__ref_ams_functions", "39-43"): (
            "stored-text:1:fcbc03806cb3ee0268fb3d4a79d2492f6a3a36052e0828d298cbf889a9c1f56c",
            39,
        ),
        ("__ref_ams_functions", "44-47"): (
            "stored-text:1:fa9a3c07ac825aa2193af7cf461333193307ac90ac10d50dc1538a43d5bdb37a",
            44,
        ),
        ("__ref_ams_functions", "48-51"): (
            "stored-text:1:70049577e51fc9dc45e73f05b2998a6045353ac250849c58c3f8e7ceceb31a4d",
            48,
        ),
        ("__ref_ams_functions", "52-55"): (
            "stored-text:1:56cb15bf030fde05bc8f9108ebfc02257f55b64ee9c511985f33896f9f9abc93",
            52,
        ),
        ("__ref_ams_functions", "71-88"): (
            "stored-text:1:de1c0df7f005101b0b132b9ace3f9359b148f12e14632440e3cf5357e4b71c3e",
            71,
        ),
        ("__ref_ams_functions", "89-104"): (
            "stored-text:1:d9b815e4ac92d98b897eac33306bc3dfdcf2b2a259e380021052acdbeaf983c9",
            89,
        ),
        ("__ref_ams_functions", "105-107"): (
            "stored-text:1:41adab61721685701b64e8c2c48d958b6af414a4257781a4f9f85a0d196cd51e",
            105,
        ),
        ("__ref_ams_functions", "108-110"): (
            "stored-text:1:c510f905fc8cbe2d018c67909bbdac27f7451ba01b1b57401844e5f06a891231",
            108,
        ),
        ("__ref_ams_functions", "111-214"): (
            "stored-text:1:325dca49eeee8ebd6f4b03561fe04f7c522471a84230dc21b0cfc33cb717429b",
            111,
        ),
        ("__ref_ams_functions", "215-324"): (
            "stored-text:1:174610d747c5455adc3ef4015275572301278a5e28aa4ab1b46de5c89010ab38",
            215,
        ),
        ("_utils", "1065-1197"): (
            "stored-text:1:32479c81ba58951062e0b726bc65eb9ca849bbd89dd11c185ea62771c2583902",
            1065,
        ),
        ("_utils", "1239-1256"): (
            "stored-text:1:8776c74a18b7ecc0f0c3595ba1fade379fe84abf934ea79a9c83d77ebb8186bf",
            1239,
        ),
    }
    namespace = {
        "np": numpy,
        "pd": PdBinding(),
        "scipy": scipy,
        "stats": stats,
        "sns": seaborn,
        "sys": Sys(),
        "plt": Plt(),
        "Path": PathBinding(),
        "dir_fitting_maringals": "fitting/",
    }
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    summaries = {
        "compound_events": fixtures["ids_marg_summary_compound"].copy(),
        "rain_events": fixtures["ids_marg_summary_rain"].copy(),
        "surge_events": fixtures["ids_marg_summary_surge"].copy(),
    }
    bindings = {
        "dic_event_summaries": summaries,
        "dic_event_stats": {key: list(table.columns) for key, table in summaries.items()},
        "dir_plots_all_fitted_marginals": "fitted_distributions_all/",
        "cvm_cutoff": 0.2,
        "ks_cutoff": 0.2,
        "reproduce_select_plots_and_verify_all_vars_accounted_for": namespace[
            "reproduce_select_plots_and_verify_all_vars_accounted_for"
        ],
    }

    def reduce(finished):
        """The figures saved, in order, with the score line of each title set apart, and the folders asked for and the variables accounted for."""
        pyplot.close("all")
        data = {"asked": ("asked_entry", numpy.asarray(asked, dtype=str)), "n_saved": ((), len(saved))}
        for number, (path, kwargs, figure) in enumerate(saved):
            tag = f"fig{number:02d}"
            data[f"{tag}__path"] = ((), path)
            data[f"{tag}__kwargs"] = ((), kwargs)
            # the first line of the title states the fit's AIC and test p-values, which the licensed sites at
            # `__ref_ams_functions.py` lines 158, 164 and 199 change; the rest states the support and transformations
            first, _, rest = figure["suptitle"].partition("\n")
            data[f"{tag}__scores_line"] = ((), first)
            data[f"{tag}__suptitle_rest"] = ((), rest)
            data[f"{tag}__size"] = (f"{tag}__size_field", numpy.asarray(figure["size"], dtype=float))
            for index, panel in enumerate(figure["axes"]):
                name = f"{tag}__ax{index}"
                data[f"{name}__title"] = ((), panel["title"])
                data[f"{name}__labels"] = (f"{name}__label", numpy.asarray(panel["labels"], dtype=str))
                data[f"{name}__lims"] = (f"{name}__lim", numpy.asarray(panel["lims"], dtype=float))
                data[f"{name}__patch_meta"] = (f"{name}__patch", numpy.asarray(panel["patch_meta"], dtype=str))
                data[f"{name}__patch_data"] = (f"{name}__patch_point", numpy.asarray(panel["patch_data"], dtype=float))
                data[f"{name}__lines"] = (f"{name}__line", numpy.asarray(panel["lines"], dtype=str))
                data[f"{name}__texts"] = (f"{name}__text", numpy.asarray(panel["texts"], dtype=str))
                data[f"{name}__legend"] = (f"{name}__entry", numpy.asarray(panel["legend"], dtype=str))
        data["which_event_key"] = ((), str(finished["which_event_key"]))
        accounted = finished["s_vars_accounted_for"]
        data["accounted_vars"] = ("accounted_var", numpy.asarray(accounted.index, dtype=str))
        data["accounted_flags"] = ("accounted_var", accounted.to_numpy(dtype=bool))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_b5_fit_vine_copulas.py:241-252", fixtures=["ids_sensitivity_pvalues"])
def declare_best_control_settings_selection(fixtures):
    """Run the cell that keeps the vine copula settings whose p-values all lie between 0.05 and 0.95; reduce to the table it keeps and the table it saves.

    The text runs unchanged and reads `pd`, a folder name and the settings' event types; `pd.read_csv` returns the
    committed table of settings for the one event type the cell does not skip, and the cell's csv is written in a
    folder of the run's own, read back by the reduction and removed. The text has no callee, no draw and no register
    entry.
    """
    import pathlib
    import shutil
    import tempfile

    import pandas
    import xarray

    workdir = tempfile.mkdtemp(
        prefix="interactive-decision-support-lift-"
    )  # the run's own folder, removed by the reduction
    folder = workdir + "/plots_rain_events_control_testing/"
    pathlib.Path(folder).mkdir(parents=True)

    class PdBinding:
        """pandas as the legacy cell names it, except that `read_csv` returns the committed table of settings."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The settings table for the sensitivity-results csv of the rain-only events; any other file is an error."""
            if str(path) != folder + "copula_fit_sensitivity_analysis_results.csv":
                raise FileNotFoundError(f"the lift reads no csv named {path}")
            return fixtures["ids_sensitivity_pvalues"].copy()

    bindings = {
        "pd": PdBinding(),
        "dir_vinecop": workdir + "/",
        "dic_event_summaries": {"surge_events": None, "rain_events": None, "compound_events": None},
    }

    def reduce(finished):
        """The settings kept, as the cell holds them and as it saved them."""
        kept = finished["df_controls_and_results_best"]
        saved = pandas.read_csv(folder + "copula_fit_sensitivity_analysis_results_best.csv", index_col=0)
        shutil.rmtree(workdir)
        data = {"alpha": ((), float(finished["cntrl_selection_alpha_alt"]))}
        for name, frame in (("kept", kept), ("saved", saved)):
            flat = frame.reset_index()
            for column in flat.columns:
                data[f"{name}__{column}"] = (
                    f"{name}__row",
                    flat[column].astype(str).to_numpy() if flat[column].dtype == object else flat[column].to_numpy(),
                )
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_b5_fit_vine_copulas.py:253-291",
    form="adapted",
    fixtures=[
        "ids_cdf_long_surge",
        "ids_cdf_long_rain",
        "ids_cdf_long_compound",
        "ids_vinecop_json",
        "ids_sensitivity_results",
        "seed",
    ],
    draws="seeded",
)
def declare_short_listed_control_figures(fixtures):
    """Run the adapted cell that draws the goodness-of-fit figures of one short-listed control setting of each event type at sample sizes of 50; reduce to the figures it saves and what they hold.

    The adapted copy differs from the stored text in six lines, and only in values the cell assigns: the sample sizes
    of the density and marginal-CDF comparison draws, of the tau test and of the Cramer-von Mises test (1000, 500 and
    500) are 50, and each event type's short-list keeps its first entry (`[0]`, `[7]`, `[1]` for `[0, 37, 1]`,
    `[7,5,1]`, `[1,3,4,0]`). The full cell takes about 750 s a run. The rest of the text runs unchanged. The library's `cdf` and `simulate` are bound through a stand-in over the installed
    library: `cdf` takes the committed fixture `seed` itself, and every `simulate` call whose legacy call names no
    seeds takes the next seed of one generator built from it (`numpy.random.default_rng(seed)`, one
    `integers(1, 2**31 - 1)` per call, in the legacy call order), so every result is repeatable and the new side
    builds the same generator from `inputs["seed"]`. The cell passes a fitted copula to every test, so the pandas
    bootstrap of `_utils.py:733` is not reached. The callees are bound to their stored legacy texts and `sys` passes
    the register's two stop markers (lines 48 and 78 of `_b5_fit_vine_copulas.py`) as the run behind the kept outputs
    did. The saved fit is written to a folder of the run's own, read back as a copula, and the folder removed.
    """
    import hashlib
    import inspect
    import pathlib
    import shutil
    import tempfile
    import textwrap
    import tomllib
    import types

    import matplotlib
    import numpy
    import pandas
    import pyvinecopulib
    import seaborn
    import xarray
    from matplotlib import patches as mpatches
    from matplotlib.lines import Line2D
    from scipy import stats

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
    workdir = tempfile.mkdtemp(
        prefix="interactive-decision-support-lift-"
    )  # the run's own folder, removed by the reduction
    tmp_prefix = workdir + "/"
    cdf_tables = {
        "surge_events": "ids_cdf_long_surge",
        "rain_events": "ids_cdf_long_rain",
        "compound_events": "ids_cdf_long_compound",
    }
    fit_path = tmp_prefix + "selected_fit.json"
    pathlib.Path(fit_path).write_text(str(fixtures["ids_vinecop_json"][0]), encoding="utf-8")

    class PdBinding:
        """pandas as the legacy cell names it, except that `read_csv` returns the committed table the named file stands for."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The fitted-probability table of an event type, or the table of control settings; any other file is an error."""
            name = str(path).rsplit("/", 1)[-1]
            if name.endswith("_empirical_vs_fitted.csv"):
                return fixtures[cdf_tables[name[: -len("_empirical_vs_fitted.csv")]]].copy()
            if name == "copula_fit_sensitivity_analysis_results.csv":
                table = fixtures["ids_sensitivity_results"].copy()
                table["f_vinecop_params"] = fit_path
                return table
            raise FileNotFoundError(f"the lift reads no csv named {path}")

    seed = int(
        fixtures["seed"]
    )  # the one seed, a committed fixture; the new side receives the same value as inputs["seed"]
    seed_stream = numpy.random.default_rng(
        seed
    )  # one seed per simulate call whose legacy call names none, in call order

    class Vine:
        """A fitted vine copula as the legacy calls use it, over the installed library's array interface."""

        def __init__(self, fitted):
            """Hold the library's own fitted copula."""
            self.fitted = fitted

        @property
        def trunc_lvl(self):
            """The truncation level of the fit."""
            return self.fitted.trunc_lvl

        def cdf(self, u, **kwargs):
            """The copula's probabilities at the rows of `u`, with the fixture's seed, on the array form the library takes."""
            return self.fitted.cdf(numpy.asfortranarray(numpy.asarray(u, dtype=float)), seeds=[seed])

        def simulate(self, n, seeds=None):
            """`n` draws as an array; the seeds the legacy call names, else the next seed of the stream built from the fixture's seed."""
            if seeds is None:
                seeds = [int(seed_stream.integers(1, 2**31 - 1))]
            return self.fitted.simulate(n=n, seeds=list(seeds))

    def vinecop(source):
        """A copula loaded from the json file named by `source`."""
        return Vine(pyvinecopulib.Vinecop.from_file(str(source)))

    pv_binding = types.SimpleNamespace(Vinecop=vinecop)

    saved = []  # one entry per figure the callees saved: the path with the run's folder scrubbed and what the figure holds

    def figure_record(figure):
        """What a figure holds: for each axes its labels, dashed observed-statistic lines, titles, every patch, other lines and texts."""
        axes_records = []
        for axes in figure.axes:
            axes_records.append(
                {
                    "xlabel": axes.get_xlabel(),
                    "ylabel": axes.get_ylabel(),
                    "title": axes.get_title(),
                    "observed": sorted(
                        float(line.get_xdata()[0]) for line in axes.lines if line.get_linestyle() == "--"
                    ),
                    "patches": [
                        f"{type(b).__name__}|{matplotlib.colors.to_hex(b.get_facecolor(), keep_alpha=True)}|{b.get_alpha()}"
                        f"|{[float(v) for v in (b.get_x(), b.get_y(), b.get_width(), b.get_height())]}"
                        if isinstance(b, matplotlib.patches.Rectangle)
                        else f"{type(b).__name__}|{[float(v) for v in numpy.asarray(b.get_path().vertices, dtype=float).ravel()]}"
                        for b in axes.patches
                    ],
                    "texts": [t.get_text() for t in axes.texts],
                }
            )
        return axes_records

    class Plt:
        """The `plt` the callees draw with: real figures drawn in memory, a record of each save, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, and write no file."""
            saved.append((str(path).replace(tmp_prefix, "<tmp>/"), figure_record(pyplot.gcf())))

        def clf(self):
            """Clear the current figure, as the legacy callee does after each save."""
            pyplot.clf()

        def tight_layout(self, *args, **kwargs):
            """Lay the current figure out."""
            pyplot.tight_layout(*args, **kwargs)

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    unit_texts = [
        ("_b5_fit_vine_copulas.py", 38, 57),
        ("_b5_fit_vine_copulas.py", 58, 71),
        ("_b5_fit_vine_copulas.py", 72, 75),
        ("_b5_fit_vine_copulas.py", 76, 96),
        ("_utils.py", 421, 434),
        ("_utils.py", 597, 604),
        ("_utils.py", 605, 660),
        ("_utils.py", 661, 717),
        ("_utils.py", 718, 746),
        ("_utils.py", 747, 856),
        ("_utils.py", 857, 921),
        ("_utils.py", 922, 990),
        ("_b5_fit_vine_copulas.py", 97, 119),
        ("_utils.py", 410, 420),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own excepted
    stop_lines = [
        ("_b5_fit_vine_copulas.py", 48),
        ("_b5_fit_vine_copulas.py", 78),
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

    callee_labels = {
        ("_b5_fit_vine_copulas", "38-57"): (
            "stored-text:1:cf07650fab513633921570bdc6cb5908f69b30151834e26e862639c7cdcf4025",
            38,
        ),
        ("_b5_fit_vine_copulas", "58-71"): (
            "stored-text:1:8be614a46a8213b16672ec7a4dcf588adc63b56fbc661a7a1ceadd8f9ee3258c",
            58,
        ),
        ("_b5_fit_vine_copulas", "72-75"): (
            "stored-text:1:ccbb3f556df6d718de11f1980cca6eb116ca913821018a2816c5155fc59a1b8f",
            72,
        ),
        ("_b5_fit_vine_copulas", "76-96"): (
            "stored-text:1:4478de3f0d6a992d5dbb2571eba1f9d5efdc2c516d9402ef821f77bf4fe44b65",
            76,
        ),
        ("_utils", "421-434"): (
            "stored-text:1:4ef6e81b8733a313a33f666ed883055d16ca97779e8c7c13d1f63dfdb23def9d",
            421,
        ),
        ("_utils", "597-604"): (
            "stored-text:1:efef30214c4a04554a2932afb505cf3fe056333ff4b223b46bd16ee7581d7394",
            597,
        ),
        ("_utils", "605-660"): (
            "stored-text:1:f7cf4b0fef56d6956864824f37f042b5c47a53dea922996f83e8a55b2051e89b",
            605,
        ),
        ("_utils", "661-717"): (
            "stored-text:1:7a6467a5addf894a7aa686d1890eb41a333984baa05b02227784168e53f320fe",
            661,
        ),
        ("_utils", "718-746"): (
            "stored-text:1:303748bf356cb7c6339e2c0ec17bbbbd763b791fef286d81c2c660b3fa97ebbb",
            718,
        ),
        ("_utils", "747-856"): (
            "stored-text:1:9c39c8cfd602394a12629dd249401f6bfee7d305688b358193bc5d54d06b1ddd",
            747,
        ),
        ("_utils", "857-921"): (
            "stored-text:1:3e0cbd9c540ac11b9f87e7993da1f4146b429e3ae6d54c828e9bfd189dce1c8e",
            857,
        ),
        ("_utils", "922-990"): (
            "stored-text:1:cec2082f2179b8214be6828a5a2b22b73d52c27a5e247bf7492853a5841a587a",
            922,
        ),
        ("_b5_fit_vine_copulas", "97-119"): (
            "stored-text:1:1cee0038b11a5b982bcf6f10e6898be1513131fed3a5f8c17b3ae26ea978bd8d",
            97,
        ),
        ("_utils", "410-420"): (
            "stored-text:1:c0f0f53e259dcbcf9ef15a91817aea99299930c746892cafe6700f9a1424baff",
            410,
        ),
    }
    namespace = {
        "np": numpy,
        "pd": PdBinding(),
        "dir_vinecop": tmp_prefix,
        "sys": Sys(),
        "pv": pv_binding,
        "plt": Plt(),
        "sns": seaborn,
        "mpatches": mpatches,
        "Line2D": Line2D,
        "textwrap": textwrap,
        "kendalltau": stats.kendalltau,
        "gaussian_kde": stats.gaussian_kde,
        "stats": stats,
        "dir_fitting_maringals": "marginals/",
    }
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    bindings = {
        "pd": PdBinding(),
        "pv": pv_binding,
        "shutil": shutil,
        "Path": pathlib.Path,
        "dic_event_summaries": {"surge_events": None, "rain_events": None, "compound_events": None},
        "dir_vinecop": tmp_prefix,
        "format_fitted_cdf_values_for_fitting_copula": namespace["format_fitted_cdf_values_for_fitting_copula"],
        "create_vinecop_gof_plots": namespace["create_vinecop_gof_plots"],
    }

    def reduce(finished):
        """The figures saved, in order: paths, labels, observed lines, patches and texts compared; the titles that state a test's result and rate set apart."""
        pyplot.close("all")
        data = {"n_saved": ((), len(saved))}
        for number, (path, panels) in enumerate(saved):
            tag = f"fig{number:03d}"
            data[f"{tag}__path"] = ((), path)
            data[f"{tag}__n_axes"] = ((), len(panels))
            data[f"{tag}__xlabels"] = (f"{tag}__axis", numpy.asarray([p["xlabel"] for p in panels], dtype=str))
            data[f"{tag}__ylabels"] = (f"{tag}__axis", numpy.asarray([p["ylabel"] for p in panels], dtype=str))
            data[f"{tag}__observed"] = (
                f"{tag}__line",
                numpy.asarray([v for p in panels for v in p["observed"]], dtype=float),
            )
            titles = [p["title"] for p in panels]
            groups = {
                "titles_observed": [t for t in titles if t.startswith("Observed Data")],
                "titles_simulated": [t for t in titles if t.startswith("Simulated Data")],
                "titles_tests": [t for t in titles if not t.startswith(("Observed Data", "Simulated Data"))],
            }
            for group, members in groups.items():
                data[f"{tag}__{group}"] = (f"{tag}__{group}_axis", numpy.asarray(members, dtype=str))
            for index, panel in enumerate(panels):
                name = f"{tag}__ax{index}"
                data[f"{name}__patches"] = (f"{name}__patch", numpy.asarray(panel["patches"], dtype=str))
                data[f"{name}__texts"] = (f"{name}__text", numpy.asarray(panel["texts"], dtype=str))
        shutil.rmtree(workdir, ignore_errors=True)
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_utils.py:1065-1197",
    fixtures=[
        "ids_marg_summary_surge",
        "ids_marg_summary_rain",
        "ids_marg_summary_compound",
        "ids_marg_perf_surge",
        "ids_marg_perf_rain",
        "ids_marg_perf_compound",
    ],
)
def declare_subset_of_fits_plotter(fixtures):
    """Run the function that refits and draws the candidate fits of each event type, in the mode that draws all of them and in the mode that draws one named fit and returns it; reduce to the figures saved, the folders asked for and what the named-fit call returns.

    The text defines `plot_subset_of_fits`; the reduction calls it once for every admitted fit and once by file name. Its
    callees are bound to stored legacy texts (`fit_dist`, `plot_fitted_distribution` and the transformation functions);
    `pd.read_csv` returns the committed candidate-fit table of the event type named; `shutil`, `Path` and `plt` record
    what they are asked and write nothing. No fixed location is asked of a gamma or lognormal fit.
    """
    import hashlib
    import pathlib
    import tomllib

    import matplotlib
    import numpy
    import pandas
    import scipy
    import seaborn
    import xarray
    from scipy import stats

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"

    saved = []  # one entry per figure the texts saved: the path, the save's keywords and what the figure holds
    asked = []  # the folders the texts asked to remove and to create

    def figure_record(figure):
        """What a figure holds: its texts and size and, for each axes, titles, labels, limits, every patch, lines, scatter points and texts."""
        axes_records = []
        for axes in figure.axes:
            legend = axes.get_legend()
            axes_records.append(
                {
                    "title": axes.get_title(),
                    "labels": [axes.get_xlabel(), axes.get_ylabel()],
                    "lims": [float(v) for v in axes.get_xlim()] + [float(v) for v in axes.get_ylim()],
                    "patch_meta": [
                        f"{type(b).__name__}|{matplotlib.colors.to_hex(b.get_facecolor(), keep_alpha=True)}"
                        f"|{matplotlib.colors.to_hex(b.get_edgecolor(), keep_alpha=True)}|{b.get_alpha()}|{b.get_fill()}"
                        for b in axes.patches
                    ],
                    "patch_data": [
                        float(v)
                        for b in axes.patches
                        for v in (
                            [b.get_x(), b.get_y(), b.get_width(), b.get_height()]
                            if isinstance(b, matplotlib.patches.Rectangle)
                            else numpy.asarray(b.get_path().vertices, dtype=float).ravel()
                        )
                    ],
                    "lines": [
                        f"{line.get_label()}|{matplotlib.colors.to_hex(line.get_color())}|{line.get_linestyle()}"
                        f"|{[float(v) for v in numpy.asarray(line.get_xdata(), dtype=float)]}"
                        f"|{[float(v) for v in numpy.asarray(line.get_ydata(), dtype=float)]}"
                        for line in axes.lines
                    ],
                    "scatter": [
                        float(v)
                        for c in axes.collections
                        if type(c).__name__ == "PathCollection"
                        for v in numpy.asarray(c.get_offsets(), dtype=float).ravel()
                    ],
                    "texts": [t.get_text() for t in axes.texts],
                    "legend": [] if legend is None else [t.get_text() for t in legend.get_texts()],
                }
            )
        return {
            "suptitle": figure._suptitle.get_text() if figure._suptitle is not None else "",
            "captions": [
                f"{t.get_text()}|{[float(v) for v in t.get_position()]}|{t.get_horizontalalignment()}"
                for t in figure.texts
            ],
            "size": [float(v) for v in figure.get_size_inches()] + [float(figure.get_dpi())],
            "axes": axes_records,
        }

    def pack_figures(data):
        """Add every saved figure to `data` as numbered components."""
        data["asked"] = ("asked_entry", numpy.asarray(asked, dtype=str))
        data["n_saved"] = ((), len(saved))
        for number, (path, kwargs, figure) in enumerate(saved):
            tag = f"fig{number:02d}"
            data[f"{tag}__path"] = ((), path)
            data[f"{tag}__kwargs"] = ((), kwargs)
            # the first line of the title states the fit's AIC and test p-values, which the licensed sites at
            # `__ref_ams_functions.py` lines 158, 164 and 199 change; the rest states the support and transformations
            first, _, rest = figure["suptitle"].partition("\n")
            data[f"{tag}__scores_line"] = ((), first)
            data[f"{tag}__suptitle_rest"] = ((), rest)
            data[f"{tag}__captions"] = (f"{tag}__caption", numpy.asarray(figure["captions"], dtype=str))
            data[f"{tag}__size"] = (f"{tag}__size_field", numpy.asarray(figure["size"], dtype=float))
            for index, panel in enumerate(figure["axes"]):
                name = f"{tag}__ax{index}"
                data[f"{name}__title"] = ((), panel["title"])
                data[f"{name}__labels"] = (f"{name}__label", numpy.asarray(panel["labels"], dtype=str))
                data[f"{name}__lims"] = (f"{name}__lim", numpy.asarray(panel["lims"], dtype=float))
                data[f"{name}__patch_meta"] = (f"{name}__patch", numpy.asarray(panel["patch_meta"], dtype=str))
                data[f"{name}__patch_data"] = (f"{name}__patch_point", numpy.asarray(panel["patch_data"], dtype=float))
                data[f"{name}__lines"] = (f"{name}__line", numpy.asarray(panel["lines"], dtype=str))
                data[f"{name}__scatter"] = (f"{name}__point", numpy.asarray(panel["scatter"], dtype=float))
                data[f"{name}__texts"] = (f"{name}__text", numpy.asarray(panel["texts"], dtype=str))
                data[f"{name}__legend"] = (f"{name}__entry", numpy.asarray(panel["legend"], dtype=str))

    class Plt:
        """The `plt` the texts draw with: real figures in memory, a record of each save, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, with the save's keyword arguments; write no file."""
            saved.append((str(path), repr(sorted(kwargs.items())), figure_record(pyplot.gcf())))

        def clf(self):
            """Clear the current figure, as the texts do after a save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    class PathBinding:
        """`Path` as the texts name it: a call returns a handle whose `mkdir` is recorded and creates nothing."""

        def __call__(self, path):
            """A handle for `path`."""

            class Handle:
                """A path whose `mkdir` records the request."""

                def mkdir(self, *args, **kwargs):
                    """Record the folder asked for."""
                    asked.append(f"mkdir|{path}|{sorted(kwargs.items())}")

            return Handle()

    class Shutil:
        """`shutil` as the texts name it: `rmtree` records the folder and finds nothing to remove."""

        def rmtree(self, path):
            """Record the folder; raise as removing a folder that does not exist does."""
            asked.append(f"rmtree|{path}")
            raise FileNotFoundError(path)

    class Sys:
        """The `sys` the texts read: `exit` raises `SystemExit`, as the legacy call does; nothing else is modelled."""

        def exit(self, *args):
            """Exit as legacy does: no register stop marker lies in the texts this lift runs."""
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    class PdBinding:
        """pandas as the texts name it, except that `read_csv` returns the committed candidate-fit table of the event type the file names."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The fit table for `{event type}.csv`; any other file is an error."""
            name = str(path).rsplit("/", 1)[-1]
            for key, fixture in (
                ("surge_events", "ids_marg_perf_surge"),
                ("rain_events", "ids_marg_perf_rain"),
                ("compound_events", "ids_marg_perf_compound"),
            ):
                if name == key + ".csv":
                    return fixtures[fixture].copy()
            raise FileNotFoundError(f"the lift reads no csv named {path}")

    namespace = {
        "np": numpy,
        "pd": PdBinding(),
        "scipy": scipy,
        "stats": stats,
        "sns": seaborn,
        "sys": Sys(),
        "plt": Plt(),
        "Path": PathBinding(),
        "shutil": Shutil(),
        "dir_fitting_maringals": "fitting/",
    }

    texts = [
        ("__ref_ams_functions.py", 23, 30),
        ("__ref_ams_functions.py", 31, 38),
        ("__ref_ams_functions.py", 39, 43),
        ("__ref_ams_functions.py", 44, 47),
        ("__ref_ams_functions.py", 48, 51),
        ("__ref_ams_functions.py", 52, 55),
        ("__ref_ams_functions.py", 71, 88),
        ("__ref_ams_functions.py", 89, 104),
        ("__ref_ams_functions.py", 105, 107),
        ("__ref_ams_functions.py", 108, 110),
        ("__ref_ams_functions.py", 111, 214),
        ("__ref_ams_functions.py", 215, 324),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own excepted
    stops = sorted(
        (entry["key"].split(":")[0], entry["line"])
        for entry in tomllib.loads((root / "tests/correspondence/known_defects.toml").read_text(encoding="utf-8"))[
            "entry"
        ]
        if entry["kind"] == "stop marker"
        and any(entry["key"].startswith(file + ":") and first <= entry["line"] <= last for file, first, last in texts)
    )
    if stops:
        raise ValueError(f"the register holds stop markers {stops} inside the texts this lift runs; it binds none")

    callee_labels = {
        ("__ref_ams_functions", "23-30"): (
            "stored-text:1:861c7216b43ad1b88707d69279558cd3115d5120b67182a1a1488a2149e47460",
            23,
        ),
        ("__ref_ams_functions", "31-38"): (
            "stored-text:1:19651879b8df3efb84101dfc7c6d6730a4056e76539d1b71ef9550729b5cb4b8",
            31,
        ),
        ("__ref_ams_functions", "39-43"): (
            "stored-text:1:fcbc03806cb3ee0268fb3d4a79d2492f6a3a36052e0828d298cbf889a9c1f56c",
            39,
        ),
        ("__ref_ams_functions", "44-47"): (
            "stored-text:1:fa9a3c07ac825aa2193af7cf461333193307ac90ac10d50dc1538a43d5bdb37a",
            44,
        ),
        ("__ref_ams_functions", "48-51"): (
            "stored-text:1:70049577e51fc9dc45e73f05b2998a6045353ac250849c58c3f8e7ceceb31a4d",
            48,
        ),
        ("__ref_ams_functions", "52-55"): (
            "stored-text:1:56cb15bf030fde05bc8f9108ebfc02257f55b64ee9c511985f33896f9f9abc93",
            52,
        ),
        ("__ref_ams_functions", "71-88"): (
            "stored-text:1:de1c0df7f005101b0b132b9ace3f9359b148f12e14632440e3cf5357e4b71c3e",
            71,
        ),
        ("__ref_ams_functions", "89-104"): (
            "stored-text:1:d9b815e4ac92d98b897eac33306bc3dfdcf2b2a259e380021052acdbeaf983c9",
            89,
        ),
        ("__ref_ams_functions", "105-107"): (
            "stored-text:1:41adab61721685701b64e8c2c48d958b6af414a4257781a4f9f85a0d196cd51e",
            105,
        ),
        ("__ref_ams_functions", "108-110"): (
            "stored-text:1:c510f905fc8cbe2d018c67909bbdac27f7451ba01b1b57401844e5f06a891231",
            108,
        ),
        ("__ref_ams_functions", "111-214"): (
            "stored-text:1:325dca49eeee8ebd6f4b03561fe04f7c522471a84230dc21b0cfc33cb717429b",
            111,
        ),
        ("__ref_ams_functions", "215-324"): (
            "stored-text:1:174610d747c5455adc3ef4015275572301278a5e28aa4ab1b46de5c89010ab38",
            215,
        ),
    }
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)
    summaries = {
        "compound_events": fixtures["ids_marg_summary_compound"].copy(),
        "rain_events": fixtures["ids_marg_summary_rain"].copy(),
        "surge_events": fixtures["ids_marg_summary_surge"].copy(),
    }
    stats_of = {key: list(table.columns) for key, table in summaries.items()}

    bindings = {
        "np": numpy,
        "pd": PdBinding(),
        "stats": stats,
        "sys": Sys(),
        "plt": Plt(),
        "Path": PathBinding(),
        "dir_fitting_maringals": "fitting/",
        "fit_dist": namespace["fit_dist"],
    }

    def reduce(finished):
        """The figures of the all-fits call and of the named-fit call, the folders asked for, and the fit the named call returns."""
        pyplot.close("all")
        plot = finished["plot_subset_of_fits"]
        plot(summaries, stats_of, "fitted_distributions_all/", 0.2, 0.2, mode="all")
        data = {"n_saved_all": ((), len(saved))}
        pack_figures(data)
        before = len(saved)
        fit, table = plot(
            summaries,
            stats_of,
            "fitted_distributions_all/",
            0.2,
            0.2,
            mode="from_fname",
            which_event_key="rain_events",
            fname_of_interest="precip_depth_mm_cvm-0.96_notrns_dist-weibull_min_fixed_loc",
            subdir_name="slxn_final",
            return_fit=True,
        )
        pyplot.close("all")
        data["named_figures"] = ((), len(saved) - before)
        scores = {"aic", "cvm_pval", "ks_pval"}  # the scores the licensed sites at lines 158, 164 and 199 change
        kept = [name for name in fit.index if name not in scores]
        data["fit_index"] = ("fit_field", numpy.asarray(kept, dtype=str))
        data["fit_values"] = ("fit_field", numpy.asarray([str(fit[name]) for name in kept], dtype=str))
        data["fit_scores"] = ("fit_score", numpy.asarray([float(fit[name]) for name in sorted(scores)], dtype=float))
        data["table_columns"] = ("table_column", numpy.asarray(table.columns, dtype=str))
        for column in table.columns:
            data[f"table__{column}"] = ("table_row", table[column].to_numpy())
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_utils.py:1239-1256",
    fixtures=[
        "ids_marg_summary_surge",
        "ids_marg_summary_rain",
        "ids_marg_summary_compound",
        "ids_marg_perf_surge",
        "ids_marg_perf_rain",
        "ids_marg_perf_compound",
    ],
)
def declare_selection_coverage_check(fixtures):
    """Run the function that redraws hand-picked fits by file name and reports which statistics they cover; reduce to the series it returns and the figures it saves.

    The text defines `reproduce_select_plots_and_verify_all_vars_accounted_for`; the reduction calls it for two lists of
    file names of the surge-only events (one covering every statistic, one leaving two uncovered, which reaches the
    warning branch) and one list of the rain-only events with a name that matches no fit. `plot_subset_of_fits` and the
    fit functions it calls are bound to their stored legacy texts.
    """
    import hashlib
    import pathlib
    import tomllib

    import matplotlib
    import numpy
    import pandas
    import scipy
    import seaborn
    import xarray
    from scipy import stats

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"

    saved = []  # one entry per figure the texts saved: the path, the save's keywords and what the figure holds
    asked = []  # the folders the texts asked to remove and to create

    def figure_record(figure):
        """What a figure holds: its texts and size and, for each axes, titles, labels, limits, every patch, lines, scatter points and texts."""
        axes_records = []
        for axes in figure.axes:
            legend = axes.get_legend()
            axes_records.append(
                {
                    "title": axes.get_title(),
                    "labels": [axes.get_xlabel(), axes.get_ylabel()],
                    "lims": [float(v) for v in axes.get_xlim()] + [float(v) for v in axes.get_ylim()],
                    "patch_meta": [
                        f"{type(b).__name__}|{matplotlib.colors.to_hex(b.get_facecolor(), keep_alpha=True)}"
                        f"|{matplotlib.colors.to_hex(b.get_edgecolor(), keep_alpha=True)}|{b.get_alpha()}|{b.get_fill()}"
                        for b in axes.patches
                    ],
                    "patch_data": [
                        float(v)
                        for b in axes.patches
                        for v in (
                            [b.get_x(), b.get_y(), b.get_width(), b.get_height()]
                            if isinstance(b, matplotlib.patches.Rectangle)
                            else numpy.asarray(b.get_path().vertices, dtype=float).ravel()
                        )
                    ],
                    "lines": [
                        f"{line.get_label()}|{matplotlib.colors.to_hex(line.get_color())}|{line.get_linestyle()}"
                        f"|{[float(v) for v in numpy.asarray(line.get_xdata(), dtype=float)]}"
                        f"|{[float(v) for v in numpy.asarray(line.get_ydata(), dtype=float)]}"
                        for line in axes.lines
                    ],
                    "scatter": [
                        float(v)
                        for c in axes.collections
                        if type(c).__name__ == "PathCollection"
                        for v in numpy.asarray(c.get_offsets(), dtype=float).ravel()
                    ],
                    "texts": [t.get_text() for t in axes.texts],
                    "legend": [] if legend is None else [t.get_text() for t in legend.get_texts()],
                }
            )
        return {
            "suptitle": figure._suptitle.get_text() if figure._suptitle is not None else "",
            "captions": [
                f"{t.get_text()}|{[float(v) for v in t.get_position()]}|{t.get_horizontalalignment()}"
                for t in figure.texts
            ],
            "size": [float(v) for v in figure.get_size_inches()] + [float(figure.get_dpi())],
            "axes": axes_records,
        }

    def pack_figures(data):
        """Add every saved figure to `data` as numbered components."""
        data["asked"] = ("asked_entry", numpy.asarray(asked, dtype=str))
        data["n_saved"] = ((), len(saved))
        for number, (path, kwargs, figure) in enumerate(saved):
            tag = f"fig{number:02d}"
            data[f"{tag}__path"] = ((), path)
            data[f"{tag}__kwargs"] = ((), kwargs)
            # the first line of the title states the fit's AIC and test p-values, which the licensed sites at
            # `__ref_ams_functions.py` lines 158, 164 and 199 change; the rest states the support and transformations
            first, _, rest = figure["suptitle"].partition("\n")
            data[f"{tag}__scores_line"] = ((), first)
            data[f"{tag}__suptitle_rest"] = ((), rest)
            data[f"{tag}__captions"] = (f"{tag}__caption", numpy.asarray(figure["captions"], dtype=str))
            data[f"{tag}__size"] = (f"{tag}__size_field", numpy.asarray(figure["size"], dtype=float))
            for index, panel in enumerate(figure["axes"]):
                name = f"{tag}__ax{index}"
                data[f"{name}__title"] = ((), panel["title"])
                data[f"{name}__labels"] = (f"{name}__label", numpy.asarray(panel["labels"], dtype=str))
                data[f"{name}__lims"] = (f"{name}__lim", numpy.asarray(panel["lims"], dtype=float))
                data[f"{name}__patch_meta"] = (f"{name}__patch", numpy.asarray(panel["patch_meta"], dtype=str))
                data[f"{name}__patch_data"] = (f"{name}__patch_point", numpy.asarray(panel["patch_data"], dtype=float))
                data[f"{name}__lines"] = (f"{name}__line", numpy.asarray(panel["lines"], dtype=str))
                data[f"{name}__scatter"] = (f"{name}__point", numpy.asarray(panel["scatter"], dtype=float))
                data[f"{name}__texts"] = (f"{name}__text", numpy.asarray(panel["texts"], dtype=str))
                data[f"{name}__legend"] = (f"{name}__entry", numpy.asarray(panel["legend"], dtype=str))

    class Plt:
        """The `plt` the texts draw with: real figures in memory, a record of each save, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, with the save's keyword arguments; write no file."""
            saved.append((str(path), repr(sorted(kwargs.items())), figure_record(pyplot.gcf())))

        def clf(self):
            """Clear the current figure, as the texts do after a save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    class PathBinding:
        """`Path` as the texts name it: a call returns a handle whose `mkdir` is recorded and creates nothing."""

        def __call__(self, path):
            """A handle for `path`."""

            class Handle:
                """A path whose `mkdir` records the request."""

                def mkdir(self, *args, **kwargs):
                    """Record the folder asked for."""
                    asked.append(f"mkdir|{path}|{sorted(kwargs.items())}")

            return Handle()

    class Shutil:
        """`shutil` as the texts name it: `rmtree` records the folder and finds nothing to remove."""

        def rmtree(self, path):
            """Record the folder; raise as removing a folder that does not exist does."""
            asked.append(f"rmtree|{path}")
            raise FileNotFoundError(path)

    class Sys:
        """The `sys` the texts read: `exit` raises `SystemExit`, as the legacy call does; nothing else is modelled."""

        def exit(self, *args):
            """Exit as legacy does: no register stop marker lies in the texts this lift runs."""
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    class PdBinding:
        """pandas as the texts name it, except that `read_csv` returns the committed candidate-fit table of the event type the file names."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The fit table for `{event type}.csv`; any other file is an error."""
            name = str(path).rsplit("/", 1)[-1]
            for key, fixture in (
                ("surge_events", "ids_marg_perf_surge"),
                ("rain_events", "ids_marg_perf_rain"),
                ("compound_events", "ids_marg_perf_compound"),
            ):
                if name == key + ".csv":
                    return fixtures[fixture].copy()
            raise FileNotFoundError(f"the lift reads no csv named {path}")

    namespace = {
        "np": numpy,
        "pd": PdBinding(),
        "scipy": scipy,
        "stats": stats,
        "sns": seaborn,
        "sys": Sys(),
        "plt": Plt(),
        "Path": PathBinding(),
        "shutil": Shutil(),
        "dir_fitting_maringals": "fitting/",
    }

    texts = [
        ("__ref_ams_functions.py", 23, 30),
        ("__ref_ams_functions.py", 31, 38),
        ("__ref_ams_functions.py", 39, 43),
        ("__ref_ams_functions.py", 44, 47),
        ("__ref_ams_functions.py", 48, 51),
        ("__ref_ams_functions.py", 52, 55),
        ("__ref_ams_functions.py", 71, 88),
        ("__ref_ams_functions.py", 89, 104),
        ("__ref_ams_functions.py", 105, 107),
        ("__ref_ams_functions.py", 108, 110),
        ("__ref_ams_functions.py", 111, 214),
        ("__ref_ams_functions.py", 215, 324),
        ("_utils.py", 1065, 1197),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own excepted
    stops = sorted(
        (entry["key"].split(":")[0], entry["line"])
        for entry in tomllib.loads((root / "tests/correspondence/known_defects.toml").read_text(encoding="utf-8"))[
            "entry"
        ]
        if entry["kind"] == "stop marker"
        and any(entry["key"].startswith(file + ":") and first <= entry["line"] <= last for file, first, last in texts)
    )
    if stops:
        raise ValueError(f"the register holds stop markers {stops} inside the texts this lift runs; it binds none")

    callee_labels = {
        ("__ref_ams_functions", "23-30"): (
            "stored-text:1:861c7216b43ad1b88707d69279558cd3115d5120b67182a1a1488a2149e47460",
            23,
        ),
        ("__ref_ams_functions", "31-38"): (
            "stored-text:1:19651879b8df3efb84101dfc7c6d6730a4056e76539d1b71ef9550729b5cb4b8",
            31,
        ),
        ("__ref_ams_functions", "39-43"): (
            "stored-text:1:fcbc03806cb3ee0268fb3d4a79d2492f6a3a36052e0828d298cbf889a9c1f56c",
            39,
        ),
        ("__ref_ams_functions", "44-47"): (
            "stored-text:1:fa9a3c07ac825aa2193af7cf461333193307ac90ac10d50dc1538a43d5bdb37a",
            44,
        ),
        ("__ref_ams_functions", "48-51"): (
            "stored-text:1:70049577e51fc9dc45e73f05b2998a6045353ac250849c58c3f8e7ceceb31a4d",
            48,
        ),
        ("__ref_ams_functions", "52-55"): (
            "stored-text:1:56cb15bf030fde05bc8f9108ebfc02257f55b64ee9c511985f33896f9f9abc93",
            52,
        ),
        ("__ref_ams_functions", "71-88"): (
            "stored-text:1:de1c0df7f005101b0b132b9ace3f9359b148f12e14632440e3cf5357e4b71c3e",
            71,
        ),
        ("__ref_ams_functions", "89-104"): (
            "stored-text:1:d9b815e4ac92d98b897eac33306bc3dfdcf2b2a259e380021052acdbeaf983c9",
            89,
        ),
        ("__ref_ams_functions", "105-107"): (
            "stored-text:1:41adab61721685701b64e8c2c48d958b6af414a4257781a4f9f85a0d196cd51e",
            105,
        ),
        ("__ref_ams_functions", "108-110"): (
            "stored-text:1:c510f905fc8cbe2d018c67909bbdac27f7451ba01b1b57401844e5f06a891231",
            108,
        ),
        ("__ref_ams_functions", "111-214"): (
            "stored-text:1:325dca49eeee8ebd6f4b03561fe04f7c522471a84230dc21b0cfc33cb717429b",
            111,
        ),
        ("__ref_ams_functions", "215-324"): (
            "stored-text:1:174610d747c5455adc3ef4015275572301278a5e28aa4ab1b46de5c89010ab38",
            215,
        ),
        ("_utils", "1065-1197"): (
            "stored-text:1:32479c81ba58951062e0b726bc65eb9ca849bbd89dd11c185ea62771c2583902",
            1065,
        ),
    }
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)
    summaries = {
        "compound_events": fixtures["ids_marg_summary_compound"].copy(),
        "rain_events": fixtures["ids_marg_summary_rain"].copy(),
        "surge_events": fixtures["ids_marg_summary_surge"].copy(),
    }
    stats_of = {key: list(table.columns) for key, table in summaries.items()}

    bindings = {"pd": pandas, "plot_subset_of_fits": namespace["plot_subset_of_fits"]}

    def reduce(finished):
        """The coverage series of three calls and the figures the calls save."""
        pyplot.close("all")
        check = finished["reproduce_select_plots_and_verify_all_vars_accounted_for"]
        calls = (
            (
                "surge_events",
                [
                    "surge_peak_ft_cvm-0.89_trns_1_shift_dist-gamma",
                    "precip_depth_mm_cvm-0.72_trns_1_ub_dist-genpareto",
                    "max_mean_1hrsurge_peak_after_4hrrain_peak_h_cvm-0.85_notrns_dist-gumbel_l",
                    "max_4hr_mean_mm_per_hr_cvm-0.91_notrns_dist-genpareto_fixed_loc",
                ],
            ),
            (
                "surge_events",
                [
                    "surge_peak_ft_cvm-0.86_notrns_dist-weibull_min_fixed_loc",
                    "precip_depth_mm_cvm-0.38_trns_1_ub_dist-genpareto_fixed_loc",
                ],
            ),
            (
                "rain_events",
                [
                    "surge_peak_ft_cvm-0.96_trns_1_ub_dist-exponpow",
                    "no_such_fit_dist-t",
                    "max_4hr_mean_mm_per_hr_cvm-0.96_notrns_dist-chi",
                ],
            ),
        )
        data = {}
        for number, (key, names) in enumerate(calls):
            covered = (
                check(stats_of, key, names, summaries, "fitted_distributions_all/", 0.2, 0.2)
                if False
                else check(
                    dic_event_stats=stats_of,
                    which_event_key=key,
                    lst_fname_of_interest=names,
                    dic_event_summaries=summaries,
                    dir_plots_all_fitted_marginals="fitted_distributions_all/",
                    cvm_cutoff=0.2,
                    ks_cutoff=0.2,
                )
            )
            data[f"call{number}__vars"] = (f"call{number}__var", numpy.asarray(covered.index, dtype=str))
            data[f"call{number}__covered"] = (f"call{number}__var", covered.to_numpy(dtype=bool))
        pyplot.close("all")
        pack_figures(data)
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_utils.py:2154-2197", fixtures=["ids_combined_summaries"])
def declare_plotting_positions_function(fixtures):
    """Run the function that computes the plotting positions of each column of an event table after dropping its missing values; reduce to the table it returns for a table with missing values and an all-missing column."""
    import numpy
    import pandas

    table = fixtures["ids_combined_summaries"]
    numeric = table[table["event_type"] == "surge"][
        ["surge_peak_ft", "precip_depth_mm", "max_4hr_mean_mm_per_hr", "never_measured_mm"]
    ].reset_index(drop=True)
    numeric.loc[[2, 9], "precip_depth_mm"] = numpy.nan
    numeric.loc[5, "surge_peak_ft"] = numpy.nan

    def reduce(finished):
        """The plotting positions of each column, on each column's own non-missing events."""
        return finished["calculate_plotting_positions"](numeric.copy())

    from scipy import stats

    return {"pd": pandas, "stats": stats}, reduce


@declares_lift(unit="_utils.py:2198-2283", fixtures=["ids_combined_summaries"])
def declare_quantile_correlations_function(fixtures):
    """Run the function that lists the correlations of surge-related with rain-related statistics from the plotting positions of an event table; reduce to the listing it returns for the events of each type.

    `calculate_plotting_positions` is bound to its stored legacy text (the register's `_utils.py:2200`, `OBLIGED`, lies in
    the unit's own text).
    """
    import hashlib
    import pathlib
    import tomllib

    import matplotlib
    import numpy
    import pandas
    import xarray
    from scipy import stats

    matplotlib.use("agg")

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
    namespace = {"np": numpy, "pd": pandas, "stats": stats}

    texts = [
        ("_utils.py", 2154, 2197),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own excepted
    stops = sorted(
        (entry["key"].split(":")[0], entry["line"])
        for entry in tomllib.loads((root / "tests/correspondence/known_defects.toml").read_text(encoding="utf-8"))[
            "entry"
        ]
        if entry["kind"] == "stop marker"
        and any(entry["key"].startswith(file + ":") and first <= entry["line"] <= last for file, first, last in texts)
    )
    if stops:
        raise ValueError(f"the register holds stop markers {stops} inside the texts this lift runs; it binds none")

    callee_labels = {
        ("_utils", "2154-2197"): (
            "stored-text:1:f70f3a06fadba2194207593c15180344d390ef9934ef10ee54dbcdccd55393a7",
            2154,
        ),
    }
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    table = fixtures["ids_combined_summaries"]
    drop = ["compound", "rain_event", "surge_event", "rain_data_source", "wlevel_data_source", "event_type"]

    def reduce(finished):
        """The listing for the surge-only, rain-only and compound events, as named components."""
        calc = finished["calculate_quantile_correlations"]
        data = {}
        for kind in ("surge", "rain", "compound"):
            listing = calc(table[table["event_type"] == kind].drop(columns=drop)).reset_index(drop=True)
            for column in listing.columns:
                data[f"{kind}__{column}"] = (f"{kind}__row", listing[column].to_numpy())
        return xarray.Dataset(data)

    return {"pd": pandas, "calculate_plotting_positions": namespace["calculate_plotting_positions"]}, reduce


@declares_lift(unit="_utils.py:2284-2309", fixtures=["ids_combined_summaries", "ids_corr_listing"])
def declare_relationship_plotter(fixtures):
    """Run the function that plots the chosen relationships of an event table in plotting-position and real space; reduce to the figures it saves.

    The reduction calls it twice on the first three pairs of the listing: with a figure title and no file names (the
    figures stay open and are read), and with per-figure titles and file names (saved and cleared). `plt` records saves;
    `calculate_plotting_positions` is bound to its stored legacy text.
    """
    import hashlib
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

    saved = []  # one entry per figure the texts saved: the path, the save's keywords and what the figure holds
    asked = []  # the folders the texts asked to remove and to create

    def figure_record(figure):
        """What a figure holds: its texts and size and, for each axes, titles, labels, limits, every patch, lines, scatter points and texts."""
        axes_records = []
        for axes in figure.axes:
            legend = axes.get_legend()
            axes_records.append(
                {
                    "title": axes.get_title(),
                    "labels": [axes.get_xlabel(), axes.get_ylabel()],
                    "lims": [float(v) for v in axes.get_xlim()] + [float(v) for v in axes.get_ylim()],
                    "patch_meta": [
                        f"{type(b).__name__}|{matplotlib.colors.to_hex(b.get_facecolor(), keep_alpha=True)}"
                        f"|{matplotlib.colors.to_hex(b.get_edgecolor(), keep_alpha=True)}|{b.get_alpha()}|{b.get_fill()}"
                        for b in axes.patches
                    ],
                    "patch_data": [
                        float(v)
                        for b in axes.patches
                        for v in (
                            [b.get_x(), b.get_y(), b.get_width(), b.get_height()]
                            if isinstance(b, matplotlib.patches.Rectangle)
                            else numpy.asarray(b.get_path().vertices, dtype=float).ravel()
                        )
                    ],
                    "lines": [
                        f"{line.get_label()}|{matplotlib.colors.to_hex(line.get_color())}|{line.get_linestyle()}"
                        f"|{[float(v) for v in numpy.asarray(line.get_xdata(), dtype=float)]}"
                        f"|{[float(v) for v in numpy.asarray(line.get_ydata(), dtype=float)]}"
                        for line in axes.lines
                    ],
                    "scatter": [
                        float(v)
                        for c in axes.collections
                        if type(c).__name__ == "PathCollection"
                        for v in numpy.asarray(c.get_offsets(), dtype=float).ravel()
                    ],
                    "texts": [t.get_text() for t in axes.texts],
                    "legend": [] if legend is None else [t.get_text() for t in legend.get_texts()],
                }
            )
        return {
            "suptitle": figure._suptitle.get_text() if figure._suptitle is not None else "",
            "captions": [
                f"{t.get_text()}|{[float(v) for v in t.get_position()]}|{t.get_horizontalalignment()}"
                for t in figure.texts
            ],
            "size": [float(v) for v in figure.get_size_inches()] + [float(figure.get_dpi())],
            "axes": axes_records,
        }

    def pack_figures(data):
        """Add every saved figure to `data` as numbered components."""
        data["asked"] = ("asked_entry", numpy.asarray(asked, dtype=str))
        data["n_saved"] = ((), len(saved))
        for number, (path, kwargs, figure) in enumerate(saved):
            tag = f"fig{number:02d}"
            data[f"{tag}__path"] = ((), path)
            data[f"{tag}__kwargs"] = ((), kwargs)
            data[f"{tag}__suptitle"] = ((), figure["suptitle"])
            data[f"{tag}__captions"] = (f"{tag}__caption", numpy.asarray(figure["captions"], dtype=str))
            data[f"{tag}__size"] = (f"{tag}__size_field", numpy.asarray(figure["size"], dtype=float))
            for index, panel in enumerate(figure["axes"]):
                name = f"{tag}__ax{index}"
                data[f"{name}__title"] = ((), panel["title"])
                data[f"{name}__labels"] = (f"{name}__label", numpy.asarray(panel["labels"], dtype=str))
                data[f"{name}__lims"] = (f"{name}__lim", numpy.asarray(panel["lims"], dtype=float))
                data[f"{name}__patch_meta"] = (f"{name}__patch", numpy.asarray(panel["patch_meta"], dtype=str))
                data[f"{name}__patch_data"] = (f"{name}__patch_point", numpy.asarray(panel["patch_data"], dtype=float))
                data[f"{name}__lines"] = (f"{name}__line", numpy.asarray(panel["lines"], dtype=str))
                data[f"{name}__scatter"] = (f"{name}__point", numpy.asarray(panel["scatter"], dtype=float))
                data[f"{name}__texts"] = (f"{name}__text", numpy.asarray(panel["texts"], dtype=str))
                data[f"{name}__legend"] = (f"{name}__entry", numpy.asarray(panel["legend"], dtype=str))

    class Plt:
        """The `plt` the texts draw with: real figures in memory, a record of each save, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, with the save's keyword arguments; write no file."""
            saved.append((str(path), repr(sorted(kwargs.items())), figure_record(pyplot.gcf())))

        def clf(self):
            """Clear the current figure, as the texts do after a save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    class PathBinding:
        """`Path` as the texts name it: a call returns a handle whose `mkdir` is recorded and creates nothing."""

        def __call__(self, path):
            """A handle for `path`."""

            class Handle:
                """A path whose `mkdir` records the request."""

                def mkdir(self, *args, **kwargs):
                    """Record the folder asked for."""
                    asked.append(f"mkdir|{path}|{sorted(kwargs.items())}")

            return Handle()

    class Shutil:
        """`shutil` as the texts name it: `rmtree` records the folder and finds nothing to remove."""

        def rmtree(self, path):
            """Record the folder; raise as removing a folder that does not exist does."""
            asked.append(f"rmtree|{path}")
            raise FileNotFoundError(path)

    class Sys:
        """The `sys` the texts read: `exit` raises `SystemExit`, as the legacy call does; nothing else is modelled."""

        def exit(self, *args):
            """Exit as legacy does: no register stop marker lies in the texts this lift runs."""
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    class PdBinding:
        """pandas as the texts name it, except that `read_csv` returns the committed candidate-fit table of the event type the file names."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The fit table for `{event type}.csv`; any other file is an error."""
            name = str(path).rsplit("/", 1)[-1]
            for key, fixture in (
                ("surge_events", "ids_marg_perf_surge"),
                ("rain_events", "ids_marg_perf_rain"),
                ("compound_events", "ids_marg_perf_compound"),
            ):
                if name == key + ".csv":
                    return fixtures[fixture].copy()
            raise FileNotFoundError(f"the lift reads no csv named {path}")

    namespace = {"np": numpy, "pd": pandas, "stats": stats}

    texts = [
        ("_utils.py", 2154, 2197),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own excepted
    stops = sorted(
        (entry["key"].split(":")[0], entry["line"])
        for entry in tomllib.loads((root / "tests/correspondence/known_defects.toml").read_text(encoding="utf-8"))[
            "entry"
        ]
        if entry["kind"] == "stop marker"
        and any(entry["key"].startswith(file + ":") and first <= entry["line"] <= last for file, first, last in texts)
    )
    if stops:
        raise ValueError(f"the register holds stop markers {stops} inside the texts this lift runs; it binds none")

    callee_labels = {
        ("_utils", "2154-2197"): (
            "stored-text:1:f70f3a06fadba2194207593c15180344d390ef9934ef10ee54dbcdccd55393a7",
            2154,
        ),
    }
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    table = fixtures["ids_combined_summaries"]
    events = table[table["event_type"] == "surge"][
        ["surge_peak_ft", "max_16hr_mean_surge", "precip_depth_mm", "max_4hr_mean_mm_per_hr"]
    ].reset_index(drop=True)
    selected = fixtures["ids_corr_listing"].iloc[[0, 1, 4]].set_index(["var1", "var2"])
    selected = selected[selected.index.get_level_values("var1").isin(events.columns)]
    selected = selected[selected.index.get_level_values("var2").isin(events.columns)]
    bindings = {"plt": Plt(), "calculate_plotting_positions": namespace["calculate_plotting_positions"]}

    def reduce(finished):
        """The figures of the two calls."""
        plot = finished["plot_event_statistic_relationships"]
        plot(selected, events, fig_title="surge_events")
        data = {}
        for number in pyplot.get_fignums():
            figure_data = figure_record(pyplot.figure(number))
            saved.append((f"open{number}", "", figure_data))
        pyplot.close("all")
        titles = pandas.Series([f"relationship {index}" for index in range(len(selected))])
        names = pandas.Series([f"plots/relationship_{index}.png" for index in range(len(selected))])
        plot(selected, events, s_fig_titles=titles, s_savefig_fnames=names)
        pyplot.close("all")
        data["n_pairs"] = ((), len(selected))
        pack_figures(data)
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_utils.py:2310-2322", fixtures=["ids_corr_listing"])
def declare_correlation_filter_function(fixtures):
    """Run the function that selects the listed correlations naming one statistic and one kind of statistic, with and without a name to filter out; reduce to the tables it returns."""
    import xarray

    listing = fixtures["ids_corr_listing"]

    def reduce(finished):
        """Four selections: surge statistics of the target intensity without delays, delay statistics, rain statistics of the surge peak, and a selection filtering out a name."""
        analyze = finished["analyze_correlations"]
        calls = (
            ("surge_no_after", ("max_4hr_mean_mm_per_hr", "surge", "after")),
            ("after_only", ("max_4hr_mean_mm_per_hr", "after")),
            ("mm_of_peak", ("surge_peak_ft", "mm")),
            ("depth_no_delay", ("precip_depth_mm", "surge", "after")),
        )
        data = {}
        for name, (variable, kind, *filtered) in calls:
            table = analyze(listing, variable, kind, *filtered).reset_index(drop=True)
            for column in table.columns:
                data[f"{name}__{column}"] = (f"{name}__row", table[column].to_numpy())
        return xarray.Dataset(data)

    return {}, reduce


@declares_lift(unit="_utils.py:2323-2328", fixtures=["ids_corr_listing"])
def declare_selected_variables_appender(fixtures):
    """Run the function that appends the row of one chosen variable pair to a list; reduce to the list after two appends."""
    import pandas
    import xarray

    listing = fixtures["ids_corr_listing"]

    def reduce(finished):
        """The rows appended for two pairs, as one table."""
        append = finished["append_list_of_selected_vars"]
        rows = []
        rows = append("surge_peak_ft", "precip_depth_mm", rows, listing)
        rows = append("max_16hr_mean_surge", "max_4hr_mean_mm_per_hr", rows, listing)
        table = pandas.concat(rows, axis=1).T
        table.index.names = ["var1", "var2"]
        flat = table.reset_index()
        return xarray.Dataset({column: ("row", flat[column].to_numpy()) for column in flat.columns})

    return {}, reduce
