"""Lift declarations of the legacy units owned by event-occurrence-modelling.

A declaration supplies the names a stored legacy text reads and reduces the finished namespace to the value a test
compares; it imports no module of this checkout, and any helper it needs sits inside it, where the lift's digest
covers it.

`_b2_event_selection.py:95-108` defines one function, which counts the events starting in each rolling window of a
record. Its lift runs the stored text unchanged and its reduction calls the function, as the legacy script does, on the
invented day table and event table.
"""

from tests.correspondence.lift import declares_lift


@declares_lift(
    unit="_b2_event_selection.py:95-108", fixtures=["occ_valid_dates", "occ_rain_events", "occ_surge_events"]
)
def declare_annual_event_occurrence(fixtures):
    """Run the legacy occurrence-count function on the invented day and event tables; reduce to the tables it returns.

    The text reads two names: `pd` (real pandas) and `t_window_d`, the 365-day window the legacy script sets in the
    cell above the function. The function is called once per event table — the run calls it with the rain event table
    and the water-level event table in turn — so each call takes that table and its own start column. The text has no
    callee and reaches no register stop marker, so no `sys` is bound.
    """
    import pandas
    import xarray

    bindings = {"pd": pandas, "t_window_d": 365}

    def reduce(finished):
        """The windows kept for the rain starts and for the surge starts, as one dataset."""
        count = finished["generate_annual_event_occurance_df"]
        data = {}
        calls = (("rain_event_start", fixtures["occ_rain_events"]), ("surge_event_start", fixtures["occ_surge_events"]))
        for column, table in calls:
            frame = count(fixtures["occ_valid_dates"].copy(), table.copy(), column).reset_index(names="date")
            for name in frame.columns:
                data[f"{column}__{name}"] = (f"{column}__row", frame[name].to_numpy())
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_b3_event_analysis.py:98-134", fixtures=["pie_mrms_summary", "pie_aorc_summary", "pie_ncei_summary"]
)
def declare_event_type_pie_charts(fixtures):
    """Run the cell that counts the event types for each rainfall source and draws a pie chart of each; reduce to the data drawn.

    The text runs unchanged and reads `plt`, a folder name and the three classified summary tables. `plt` is a
    stand-in that creates real pyplot figures under the non-interactive backend, records what each saved figure holds
    (the pie's wedge angles, its label texts, the axis label and the figure's captions, with the keyword arguments of the
    save) and refuses any other `plt` attribute; nothing is written. The value is the three saved figures' data, the
    gauge table's `data_source` column the cell adds, and the counts the cell leaves. The text has no callee and
    reaches no register stop marker, so no `sys` is bound, and no library call in it breaks under the installed versions.
    """
    import matplotlib
    import numpy
    import xarray

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    saved = []

    class Plt:
        """The `plt` the cell draws with: real figures in memory, a record of each save, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes, so that the pandas pie accessor the cell uses can draw on them."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, with the save's keyword arguments."""
            figure = pyplot.gcf()
            axes = figure.axes[0]
            saved.append(
                {
                    "path": str(path),
                    "kwargs": repr(sorted(kwargs.items())),
                    "wedges": numpy.asarray([(w.theta1, w.theta2) for w in axes.patches], dtype=float),
                    "labels": numpy.asarray([t.get_text() for t in axes.texts], dtype=str),
                    "ylabel": axes.get_ylabel(),
                    "captions": numpy.asarray([t.get_text() for t in figure.texts], dtype=str),
                }
            )

        def clf(self):
            """Clear the current figure, as the cell does after each save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    bindings = {
        "plt": Plt(),
        "dir_compound_events_stats_plots": "plots/",
        "df_6min_wlevel_mrms_event_summary_stats": fixtures["pie_mrms_summary"],
        "df_6min_wlevel_aorc_event_summary_stats": fixtures["pie_aorc_summary"],
        "df_6min_wlevel_hrly_ncei_event_summary_stats": fixtures["pie_ncei_summary"],
    }

    def reduce(finished):
        """The three pie charts as saved, the gauge table's added source column and the counts the cell leaves."""
        pyplot.close("all")
        if len(saved) != 3:
            raise ValueError(f"the cell saved {len(saved)} figures, not the three it is known to")
        data = {}
        for number, figure in enumerate(saved):
            prefix = f"pie{number}"
            data[f"{prefix}__path"] = ((), figure["path"])
            data[f"{prefix}__kwargs"] = ((), figure["kwargs"])
            data[f"{prefix}__wedges"] = ((f"{prefix}__wedge", "angle"), figure["wedges"])
            data[f"{prefix}__labels"] = (f"{prefix}__label", figure["labels"])
            data[f"{prefix}__ylabel"] = ((), figure["ylabel"])
            data[f"{prefix}__captions"] = (f"{prefix}__caption", figure["captions"])
        source = finished["df_6min_wlevel_hrly_ncei_event_summary_stats"]["data_source"]
        data["ncei_data_source"] = ("ncei_row", numpy.asarray(source.astype(str).tolist(), dtype=str))
        data["n_valid_events"] = ((), int(finished["n_valid_events"]))
        data["n_compound"] = ((), int(finished["n_compound"]))
        data["frac_overlapping"] = ((), float(finished["frac_overlapping"]))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_b6_simulate_from_vincop.py:100-216", fixtures=["sim_valid_dates", "sim_events"])
def declare_occurrence_counts_function(fixtures):
    """Run the legacy function that counts events per observed year; reduce to the tables it returns for a calendar-year and a mid-year start."""
    import numpy
    import pandas
    import xarray

    def pack(prefix, frame):
        """One variable per column (and index level) of `frame`, named for the output, on a row dimension of its own."""
        flat = frame.reset_index()
        return {
            f"{prefix}__{column}": (
                f"{prefix}__row",
                numpy.asarray(flat[column].astype(str).tolist(), dtype=str)
                if flat[column].dtype == object
                else flat[column].to_numpy(),
            )
            for column in flat.columns
        }

    def valid_dates():
        """The table of days with data, indexed by date objects, as the legacy script builds it."""
        frame = fixtures["sim_valid_dates"].copy()
        frame.index = frame.index.date
        return frame

    def events():
        """The observed events table, a copy."""
        return fixtures["sim_events"].copy()

    bindings = {"np": numpy, "pd": pandas}

    def reduce(finished):
        """The yearly count tables for two fixed start dates."""
        count = finished["create_df_of_obs_event_occurences"]
        days = valid_dates()
        both = days.dropna().index
        data = {}
        for prefix, start in (("calendar", "01-01"), ("midyear", "07-01")):
            data.update(pack(prefix, count(both, events(), valid_dates(), start)))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_b6_simulate_from_vincop.py:217-233", fixtures=["sim_valid_dates", "sim_events"])
def declare_observed_counts_cell(fixtures):
    """Run the cell that builds the observed yearly counts; reduce to the three tables it leaves and the valid dates it uses."""
    import hashlib
    import pathlib

    import numpy
    import pandas
    import xarray

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"

    def bring_in(namespace, callees):
        """Compile each callee text under its own stored path, after checking its pinned label, into `namespace`."""
        for path, (label, first) in callees.items():
            data = (root / stored / f"{path}.txt").read_bytes()
            if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
                raise ValueError(
                    f"the stored text {stored}{path}.txt is not the text this declaration was written against"
                )
            # Blank lines first, so that the callee's line numbers are its legacy ones.
            exec(compile("\n" * (first - 1) + data.decode("utf-8"), stored + path + ".txt", "exec"), namespace)

    def pack(prefix, frame):
        """One variable per column (and index level) of `frame`, named for the output, on a row dimension of its own."""
        flat = frame.reset_index()
        return {
            f"{prefix}__{column}": (
                f"{prefix}__row",
                numpy.asarray(flat[column].astype(str).tolist(), dtype=str)
                if flat[column].dtype == object
                else flat[column].to_numpy(),
            )
            for column in flat.columns
        }

    def valid_dates():
        """The table of days with data, indexed by date objects, as the legacy script builds it."""
        frame = fixtures["sim_valid_dates"].copy()
        frame.index = frame.index.date
        return frame

    def events():
        """The observed events table, a copy."""
        return fixtures["sim_events"].copy()

    namespace = {"np": numpy, "pd": pandas}
    bring_in(
        namespace,
        {
            "_b6_simulate_from_vincop/100-216": (
                "stored-text:1:6e030d881186fb90f35c5449431191bf7b187681e87773590d46b9a529118d55",
                100,
            )
        },
    )
    days = valid_dates()
    bindings = {
        "pd": pandas,
        "all_rain_dates": days["all_rain_data"].dropna(),
        "wlevel_dates": days["wlevel_data"].dropna(),
        "create_df_of_obs_event_occurences": namespace["create_df_of_obs_event_occurences"],
        "df_all_events_summary": events(),
        "df_valid_dates": days,
    }

    def reduce(finished):
        """The yearly counts, those kept for fitting, the occurrence table and the valid dates."""
        data = {}
        for name in ("df_obs_annual_event_counts", "df_obs_annual_event_counts_for_fitting", "df_occurences_obs"):
            data.update(pack(name, finished[name]))
        data["idx_valid_dates"] = ("date", numpy.asarray([str(d) for d in finished["idx_valid_dates"]], dtype=str))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_b6_simulate_from_vincop.py:384-392", fixtures=["sim_counts", "sim_valid_dates"])
def declare_poisson_lambda_function(fixtures):
    """Run the legacy function that estimates a Poisson rate; reduce to the rate it returns for each event type."""
    import pandas  # noqa: F401
    import xarray

    bindings = {}

    def reduce(finished):
        """The estimated rate for each of the three event types."""
        estimate = finished["estimate_poisson_lambda"]
        idx = fixtures["sim_valid_dates"].dropna().index.date
        return xarray.Dataset(
            {
                kind: ((), float(estimate(fixtures["sim_counts"].copy(), idx, kind)))
                for kind in ("compound", "rain", "surge")
            }
        )

    return bindings, reduce


@declares_lift(unit="_b6_simulate_from_vincop.py:399-455", fixtures=["sim_counts", "sim_valid_dates"])
def declare_poisson_rates_cell(fixtures):
    """Run the cell that estimates the three Poisson rates; reduce to the three rates."""
    import hashlib
    import pathlib

    import xarray

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"

    def bring_in(namespace, callees):
        """Compile each callee text under its own stored path, after checking its pinned label, into `namespace`."""
        for path, (label, first) in callees.items():
            data = (root / stored / f"{path}.txt").read_bytes()
            if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
                raise ValueError(
                    f"the stored text {stored}{path}.txt is not the text this declaration was written against"
                )
            # Blank lines first, so that the callee's line numbers are its legacy ones.
            exec(compile("\n" * (first - 1) + data.decode("utf-8"), stored + path + ".txt", "exec"), namespace)

    namespace = {}
    bring_in(
        namespace,
        {
            "_b6_simulate_from_vincop/384-392": (
                "stored-text:1:2e984652da2ca000b65fe7240ce7581dc0f5c560a9d42ec0ed6918ead79abd01",
                384,
            )
        },
    )
    bindings = {
        "df_obs_annual_event_counts_for_fitting": fixtures["sim_counts"],
        "estimate_poisson_lambda": namespace["estimate_poisson_lambda"],
        "idx_valid_dates": fixtures["sim_valid_dates"].dropna().index.date,
    }

    def reduce(finished):
        """The three rates the cell leaves."""
        return xarray.Dataset(
            {name: ((), float(finished[name])) for name in ("lbda_compound", "lbda_rain", "lbda_surge")}
        )

    return bindings, reduce


@declares_lift(unit="_b6_simulate_from_vincop.py:456-478", fixtures=["sim_events"])
def declare_monthly_event_counts_chart(fixtures):
    """Run the cell that charts the events by month for each type; reduce to the data the chart draws."""

    import matplotlib
    import numpy
    import pandas
    import xarray

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    saved = []  # one entry per figure a callee or the text saved or showed

    def figure_data(figure):
        """What a figure draws: for each axes its bars (left edge, width, height), lines, texts, title and labels."""
        axes_data = []
        for axes in figure.axes:
            axes_data.append(
                {
                    "bars": numpy.asarray(
                        [(b.get_x(), b.get_width(), b.get_height()) for b in axes.patches], dtype=float
                    ).reshape(-1, 3),
                    "lines": numpy.asarray([len(line.get_xdata()) for line in axes.lines], dtype=float),
                    "texts": numpy.asarray([t.get_text() for t in axes.texts], dtype=str),
                    "labels": numpy.asarray([axes.get_title(), axes.get_xlabel(), axes.get_ylabel()], dtype=str),
                }
            )
        return {"axes": axes_data, "captions": numpy.asarray([t.get_text() for t in figure.texts], dtype=str)}

    class Plt:
        """The `plt` the text and its callees draw with: real figures in memory, a record of each save, nothing written."""

        def figure(self, *args, **kwargs):
            """A real figure."""
            return pyplot.figure(*args, **kwargs)

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def gca(self):
            """The current axes."""
            return pyplot.gca()

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named and the save's keyword arguments."""
            saved.append((str(path), repr(sorted(kwargs.items())), figure_data(pyplot.gcf())))

        def clf(self):
            """Clear the current figure, as the legacy code does after each save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    def drawn():
        """Every saved figure as dataset variables, numbered in the order saved."""
        pyplot.close("all")
        data = {}
        for number, (path, kwargs, figure) in enumerate(saved):
            prefix = f"fig{number}"
            data[f"{prefix}__path"] = ((), path)
            data[f"{prefix}__kwargs"] = ((), kwargs)
            data[f"{prefix}__captions"] = (f"{prefix}__caption", figure["captions"])
            for index, axes in enumerate(figure["axes"]):
                name = f"{prefix}__ax{index}"
                data[f"{name}__bars"] = ((f"{name}__bar", "bar_field"), axes["bars"])
                data[f"{name}__lines"] = (f"{name}__line", axes["lines"])
                data[f"{name}__texts"] = (f"{name}__text", axes["texts"])
                data[f"{name}__labels"] = (f"{name}__label", axes["labels"])
        return data

    events = fixtures["sim_events"].copy()
    events["event_type"] = events["event_type"].replace({"rain": "rain", "surge": "surge", "compound": "compound"})
    bindings = {
        "pd": pandas,
        "plt": Plt(),
        "df_all_events_summary": events,
        "dir_plots_ommitted_time_periods": "plots/",
    }

    def reduce(finished):
        """The monthly count chart as saved."""
        return xarray.Dataset(drawn())

    return bindings, reduce


@declares_lift(
    unit="_b6_simulate_from_vincop.py:479-488",
    form="adapted",
    draws="seeded",
    fixtures=["sim_observed_occurrences", "seed"],
)
def declare_bootstrapped_occurrences_cell(fixtures):
    """Run the adapted cell that bootstraps event counts and charts them; reduce to the sampled table, the chart data and the counts written."""
    import hashlib
    import pathlib

    import matplotlib
    import numpy
    import xarray

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"

    def bring_in(namespace, callees):
        """Compile each callee text under its own stored path, after checking its pinned label, into `namespace`."""
        for path, (label, first) in callees.items():
            data = (root / stored / f"{path}.txt").read_bytes()
            if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
                raise ValueError(
                    f"the stored text {stored}{path}.txt is not the text this declaration was written against"
                )
            # Blank lines first, so that the callee's line numbers are its legacy ones.
            exec(compile("\n" * (first - 1) + data.decode("utf-8"), stored + path + ".txt", "exec"), namespace)

    def pack(prefix, frame):
        """One variable per column (and index level) of `frame`, named for the output, on a row dimension of its own."""
        flat = frame.reset_index()
        return {
            f"{prefix}__{column}": (
                f"{prefix}__row",
                numpy.asarray(flat[column].astype(str).tolist(), dtype=str)
                if flat[column].dtype == object
                else flat[column].to_numpy(),
            )
            for column in flat.columns
        }

    saved = []  # one entry per figure a callee or the text saved or showed

    def figure_data(figure):
        """What a figure draws: for each axes its bars (left edge, width, height), lines, texts, title and labels."""
        axes_data = []
        for axes in figure.axes:
            axes_data.append(
                {
                    "bars": numpy.asarray(
                        [(b.get_x(), b.get_width(), b.get_height()) for b in axes.patches], dtype=float
                    ).reshape(-1, 3),
                    "lines": numpy.asarray([len(line.get_xdata()) for line in axes.lines], dtype=float),
                    "texts": numpy.asarray([t.get_text() for t in axes.texts], dtype=str),
                    "labels": numpy.asarray([axes.get_title(), axes.get_xlabel(), axes.get_ylabel()], dtype=str),
                }
            )
        return {"axes": axes_data, "captions": numpy.asarray([t.get_text() for t in figure.texts], dtype=str)}

    class Plt:
        """The `plt` the text and its callees draw with: real figures in memory, a record of each save, nothing written."""

        def figure(self, *args, **kwargs):
            """A real figure."""
            return pyplot.figure(*args, **kwargs)

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def gca(self):
            """The current axes."""
            return pyplot.gca()

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named and the save's keyword arguments."""
            saved.append((str(path), repr(sorted(kwargs.items())), figure_data(pyplot.gcf())))

        def clf(self):
            """Clear the current figure, as the legacy code does after each save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    def drawn():
        """Every saved figure as dataset variables, numbered in the order saved."""
        pyplot.close("all")
        data = {}
        for number, (path, kwargs, figure) in enumerate(saved):
            prefix = f"fig{number}"
            data[f"{prefix}__path"] = ((), path)
            data[f"{prefix}__kwargs"] = ((), kwargs)
            data[f"{prefix}__captions"] = (f"{prefix}__caption", figure["captions"])
            for index, axes in enumerate(figure["axes"]):
                name = f"{prefix}__ax{index}"
                data[f"{name}__bars"] = ((f"{name}__bar", "bar_field"), axes["bars"])
                data[f"{name}__lines"] = (f"{name}__line", axes["lines"])
                data[f"{name}__texts"] = (f"{name}__text", axes["texts"])
                data[f"{name}__labels"] = (f"{name}__label", axes["labels"])
        return data

    namespace = {"np": numpy, "plt": Plt()}
    bring_in(
        namespace,
        {"_utils/467-573": ("stored-text:1:6a84e2d13eb2383eed8893a9b7a9e1c934f66df3655c37d2f0f6d120785ba04e", 467)},
    )
    bindings = {
        "compare_sim_vs_obs_event_occurances": namespace["compare_sim_vs_obs_event_occurances"],
        "df_occurences_obs": fixtures["sim_observed_occurrences"].copy(),
        "dir_plot": "plots/",
        "n_years_of_weather_to_generate": 20,
        "n_years_to_simulate": 30,
        "random_seed": int(fixtures["seed"]),
    }

    def reduce(finished):
        """The sampled counts, the counts the cell writes and the chart the callee draws."""
        data = {}
        data.update(pack("sampled", finished["df_occurences_sim"]))
        data.update(pack("written", finished["written_event_counts"]))
        data.update(drawn())
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_b6_simulate_from_vincop.py:479-488",
    form="adapted",
    part="draw-law",
    draws="seeded",
    fixtures=["sim_observed_occurrences"],
)
def declare_bootstrap_law_parameters(fixtures):
    """Run the adapted bootstrap cell and reduce to the parameter of the law its draw follows: the row count of the observed table."""
    import hashlib
    import pathlib

    import matplotlib
    import numpy
    import pandas

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"

    def bring_in(namespace, callees):
        """Compile each callee text under its own stored path, after checking its pinned label, into `namespace`."""
        for path, (label, first) in callees.items():
            data = (root / stored / f"{path}.txt").read_bytes()
            if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
                raise ValueError(
                    f"the stored text {stored}{path}.txt is not the text this declaration was written against"
                )
            # Blank lines first, so that the callee's line numbers are its legacy ones.
            exec(compile("\n" * (first - 1) + data.decode("utf-8"), stored + path + ".txt", "exec"), namespace)

    saved = []  # one entry per figure a callee or the text saved or showed

    def figure_data(figure):
        """What a figure draws: for each axes its bars (left edge, width, height), lines, texts, title and labels."""
        axes_data = []
        for axes in figure.axes:
            axes_data.append(
                {
                    "bars": numpy.asarray(
                        [(b.get_x(), b.get_width(), b.get_height()) for b in axes.patches], dtype=float
                    ).reshape(-1, 3),
                    "lines": numpy.asarray([len(line.get_xdata()) for line in axes.lines], dtype=float),
                    "texts": numpy.asarray([t.get_text() for t in axes.texts], dtype=str),
                    "labels": numpy.asarray([axes.get_title(), axes.get_xlabel(), axes.get_ylabel()], dtype=str),
                }
            )
        return {"axes": axes_data, "captions": numpy.asarray([t.get_text() for t in figure.texts], dtype=str)}

    class Plt:
        """The `plt` the text and its callees draw with: real figures in memory, a record of each save, nothing written."""

        def figure(self, *args, **kwargs):
            """A real figure."""
            return pyplot.figure(*args, **kwargs)

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def gca(self):
            """The current axes."""
            return pyplot.gca()

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named and the save's keyword arguments."""
            saved.append((str(path), repr(sorted(kwargs.items())), figure_data(pyplot.gcf())))

        def clf(self):
            """Clear the current figure, as the legacy code does after each save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    def drawn():
        """Every saved figure as dataset variables, numbered in the order saved."""
        pyplot.close("all")
        data = {}
        for number, (path, kwargs, figure) in enumerate(saved):
            prefix = f"fig{number}"
            data[f"{prefix}__path"] = ((), path)
            data[f"{prefix}__kwargs"] = ((), kwargs)
            data[f"{prefix}__captions"] = (f"{prefix}__caption", figure["captions"])
            for index, axes in enumerate(figure["axes"]):
                name = f"{prefix}__ax{index}"
                data[f"{name}__bars"] = ((f"{name}__bar", "bar_field"), axes["bars"])
                data[f"{name}__lines"] = (f"{name}__line", axes["lines"])
                data[f"{name}__texts"] = (f"{name}__text", axes["texts"])
                data[f"{name}__labels"] = (f"{name}__label", axes["labels"])
        return data

    namespace = {"np": numpy, "plt": Plt()}
    bring_in(
        namespace,
        {"_utils/467-573": ("stored-text:1:6a84e2d13eb2383eed8893a9b7a9e1c934f66df3655c37d2f0f6d120785ba04e", 467)},
    )
    bindings = {
        "compare_sim_vs_obs_event_occurances": namespace["compare_sim_vs_obs_event_occurances"],
        "df_occurences_obs": fixtures["sim_observed_occurrences"].copy(),
        "dir_plot": "plots/",
        "n_years_of_weather_to_generate": 20,
        "n_years_to_simulate": 30,
        "random_seed": 24,
    }

    def reduce(finished):
        """The law's parameter: `sample(replace=True)` draws rows uniformly, so the row count of the table sampled from."""
        pyplot.close("all")
        return pandas.Series({"rows": len(finished["df_occurences_obs"])})

    return bindings, reduce


@declares_lift(
    unit="_b6_simulate_from_vincop.py:627-745",
    form="adapted",
    fixtures=[
        "sim_occurrences",
        "sim_obs_surge",
        "sim_obs_rain",
        "sim_obs_compound",
        "sim_sim_surge",
        "sim_sim_rain",
        "sim_sim_compound",
        "sim_cdf_surge",
        "sim_cdf_rain",
        "sim_cdf_compound",
    ],
)
def declare_simulated_event_distribution_cell(fixtures):
    """Run the adapted cell that distributes simulated events over years and compares them with the observed ones; reduce to what it leaves."""
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

    def bring_in(namespace, callees):
        """Compile each callee text under its own stored path, after checking its pinned label, into `namespace`."""
        for path, (label, first) in callees.items():
            data = (root / stored / f"{path}.txt").read_bytes()
            if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
                raise ValueError(
                    f"the stored text {stored}{path}.txt is not the text this declaration was written against"
                )
            # Blank lines first, so that the callee's line numbers are its legacy ones.
            exec(compile("\n" * (first - 1) + data.decode("utf-8"), stored + path + ".txt", "exec"), namespace)

    def pack(prefix, frame):
        """One variable per column (and index level) of `frame`, named for the output, on a row dimension of its own."""
        flat = frame.reset_index()
        return {
            f"{prefix}__{column}": (
                f"{prefix}__row",
                numpy.asarray(flat[column].astype(str).tolist(), dtype=str)
                if flat[column].dtype == object
                else flat[column].to_numpy(),
            )
            for column in flat.columns
        }

    saved = []  # one entry per figure a callee or the text saved or showed

    def figure_data(figure):
        """What a figure draws: for each axes its bars (left edge, width, height), lines, texts, title and labels."""
        axes_data = []
        for axes in figure.axes:
            axes_data.append(
                {
                    "bars": numpy.asarray(
                        [(b.get_x(), b.get_width(), b.get_height()) for b in axes.patches], dtype=float
                    ).reshape(-1, 3),
                    "lines": numpy.asarray([len(line.get_xdata()) for line in axes.lines], dtype=float),
                    "texts": numpy.asarray([t.get_text() for t in axes.texts], dtype=str),
                    "labels": numpy.asarray([axes.get_title(), axes.get_xlabel(), axes.get_ylabel()], dtype=str),
                }
            )
        return {"axes": axes_data, "captions": numpy.asarray([t.get_text() for t in figure.texts], dtype=str)}

    class Plt:
        """The `plt` the text and its callees draw with: real figures in memory, a record of each save, nothing written."""

        def figure(self, *args, **kwargs):
            """A real figure."""
            return pyplot.figure(*args, **kwargs)

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def gca(self):
            """The current axes."""
            return pyplot.gca()

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named and the save's keyword arguments."""
            saved.append((str(path), repr(sorted(kwargs.items())), figure_data(pyplot.gcf())))

        def clf(self):
            """Clear the current figure, as the legacy code does after each save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    def drawn():
        """Every saved figure as dataset variables, numbered in the order saved."""
        pyplot.close("all")
        data = {}
        for number, (path, kwargs, figure) in enumerate(saved):
            prefix = f"fig{number}"
            data[f"{prefix}__path"] = ((), path)
            data[f"{prefix}__kwargs"] = ((), kwargs)
            data[f"{prefix}__captions"] = (f"{prefix}__caption", figure["captions"])
            for index, axes in enumerate(figure["axes"]):
                name = f"{prefix}__ax{index}"
                data[f"{name}__bars"] = ((f"{name}__bar", "bar_field"), axes["bars"])
                data[f"{name}__lines"] = (f"{name}__line", axes["lines"])
                data[f"{name}__texts"] = (f"{name}__text", axes["texts"])
                data[f"{name}__labels"] = (f"{name}__label", axes["labels"])
        return data

    class Reader:
        """The `pd` the cdf callee reads files with: `read_csv` of one path and nothing else, returning the fixture."""

        def read_csv(self, path):
            """The fixture the named file stands for, copied; any other argument or file is an error."""
            name = path.rsplit("/", 1)[-1]
            table = {
                "surge_events": "sim_cdf_surge",
                "rain_events": "sim_cdf_rain",
                "compound_events": "sim_cdf_compound",
            }
            return fixtures[table[name[: -len("_empirical_vs_fitted.csv")]]].copy()

        def __getattr__(self, name):
            """Refuse any other attribute of `pd` in that callee's namespace."""
            raise AttributeError(f"the lift's pd reader does not model pd.{name}")

    reading = {"pd": Reader(), "dir_fitting_maringals": "fit/"}
    reading_namespace = dict(reading)
    bring_in(
        reading_namespace,
        {"_utils/410-420": ("stored-text:1:c0f0f53e259dcbcf9ef15a91817aea99299930c746892cafe6700f9a1424baff", 410)},
    )
    testing_namespace = {"np": numpy, "pd": pandas, "stats": stats}
    bring_in(
        testing_namespace,
        {"_utils/857-921": ("stored-text:1:3e0cbd9c540ac11b9f87e7993da1f4146b429e3ae6d54c828e9bfd189dce1c8e", 857)},
    )
    written_sims = {}
    bindings = {
        "np": numpy,
        "plt": Plt(),
        "sns": seaborn,
        "df_occurences_sim": fixtures["sim_occurrences"].copy(),
        "dic_event_summaries": {
            "surge_events": fixtures["sim_obs_surge"].copy(),
            "rain_events": fixtures["sim_obs_rain"].copy(),
            "compound_events": fixtures["sim_obs_compound"].copy(),
        },
        "dic_simulations": {
            "surge_events": fixtures["sim_sim_surge"].copy(),
            "rain_events": fixtures["sim_sim_rain"].copy(),
            "compound_events": fixtures["sim_sim_compound"].copy(),
        },
        "dir_plot": "plots/",
        "format_fitted_cdf_values_for_fitting_copula": reading_namespace["format_fitted_cdf_values_for_fitting_copula"],
        "perform_two_sample_tests": testing_namespace["perform_two_sample_tests"],
        "rain_event_threshold": 40.0,
        "surge_event_threshold": 2.5,
        "written_sims": written_sims,
    }

    def reduce(finished):
        """The simulated tables as the cell leaves and writes them, the charts it saves and the event counter."""
        data = {}
        for key, frame in finished["written_sims"].items():
            data.update(pack("written_" + key, frame))
        data["event_id"] = ((), int(finished["event_id"]))
        data.update(drawn())
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_utils.py:467-573", fixtures=["sim_occurrences", "sim_observed_occurrences"])
def declare_simulated_vs_observed_occurrence_histograms(fixtures):
    """Run the legacy occurrence-histogram function in its three output modes; reduce to the data each figure draws.

    The text defines one function and reads `np` (real numpy) and `plt`. `plt` is a stand-in that creates real
    pyplot figures under the non-interactive backend, models only the four calls the function makes (`figure`, `gca`,
    `savefig`, `clf`) plus `show`, refuses any other attribute, and records what each saved or shown figure holds:
    its size and resolution, and for every axes its bars (left edge, width, height, face colour), limits, texts with
    position, size and alignment, labels, title and legend entries, together with the figure's own texts. Nothing is
    written and no window opens. The reduction calls the function three times on copies of the same two invented
    tables: saved with no sample-size note and no caption, saved with both, and shown rather than saved (the three
    branches of the function's `savefig_filename` and `plot_n_count` switches). The text has no callee and reaches
    no register stop marker, so no `sys` is bound, and its `import textwrap` is the standard library's.
    """
    import matplotlib
    import numpy
    import xarray

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    recorded = []  # one entry per figure the function saved or showed, in call order

    def figure_data(figure):
        """What a figure draws: size, resolution, its own texts, and for each axes its bars, limits, texts and labels."""
        axes_data = []
        for axes in figure.axes:
            legend = axes.get_legend()
            axes_data.append(
                {
                    "bars": numpy.asarray(
                        [(b.get_x(), b.get_width(), b.get_height()) for b in axes.patches], dtype=float
                    ).reshape(-1, 3),
                    "colours": numpy.asarray(
                        [matplotlib.colors.to_hex(b.get_facecolor()) for b in axes.patches], dtype=str
                    ),
                    "xlim": numpy.asarray(axes.get_xlim(), dtype=float),
                    "texts": numpy.asarray(
                        [
                            f"{t.get_text()}|{[float(v) for v in t.get_position()]}|{float(t.get_fontsize())}"
                            f"|{t.get_horizontalalignment()}|{t.get_verticalalignment()}"
                            for t in axes.texts
                        ],
                        dtype=str,
                    ),
                    "labels": numpy.asarray([axes.get_title(), axes.get_xlabel(), axes.get_ylabel()], dtype=str),
                    "legend": numpy.asarray(
                        [] if legend is None else [t.get_text() for t in legend.get_texts()], dtype=str
                    ),
                }
            )
        return {
            "size": numpy.asarray([*figure.get_size_inches(), figure.get_dpi()], dtype=float),
            "captions": numpy.asarray(
                [
                    f"{t.get_text()}|{[float(v) for v in t.get_position()]}|{float(t.get_fontsize())}"
                    for t in figure.texts
                ],
                dtype=str,
            ),
            "axes": axes_data,
        }

    class Plt:
        """The `plt` the function draws with: real figures in memory, a record of each save or show, nothing written."""

        def figure(self, *args, **kwargs):
            """A real figure, so that the function's grid layout and histograms draw on real axes."""
            return pyplot.figure(*args, **kwargs)

        def gca(self):
            """The current axes, which the function reads the transform of when it places a sample-size note."""
            return pyplot.gca()

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, with the save's keyword arguments."""
            recorded.append(("save", str(path), repr(sorted(kwargs.items())), figure_data(pyplot.gcf())))

        def show(self):
            """Keep what the current figure holds; no window opens."""
            recorded.append(("show", "", "", figure_data(pyplot.gcf())))

        def clf(self):
            """Clear the current figure, as the function does after a save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    bindings = {"np": numpy, "plt": Plt()}

    def reduce(finished):
        """The three figures as the function leaves them, numbered in call order."""
        draw = finished["compare_sim_vs_obs_event_occurances"]
        calls = (
            {"savefig_filename": "plots/occurrences_plain.png"},
            {
                "txt_fig_caption": "Simulated and observed annual event occurrences for the invented record.",
                "savefig_filename": "plots/occurrences_counted.png",
                "plot_n_count": True,
            },
            {},
        )
        for keywords in calls:
            draw(fixtures["sim_occurrences"].copy(), fixtures["sim_observed_occurrences"].copy(), **keywords)
            pyplot.close("all")
        if len(recorded) != len(calls):
            raise ValueError(f"the function drew {len(recorded)} figures, not the {len(calls)} it is called for")
        data = {}
        for number, (mode, path, kwargs, figure) in enumerate(recorded):
            prefix = f"fig{number}"
            data[f"{prefix}__mode"] = ((), mode)
            data[f"{prefix}__path"] = ((), path)
            data[f"{prefix}__kwargs"] = ((), kwargs)
            data[f"{prefix}__size"] = (f"{prefix}__size_field", figure["size"])
            data[f"{prefix}__captions"] = (f"{prefix}__caption", figure["captions"])
            for index, axes in enumerate(figure["axes"]):
                name = f"{prefix}__ax{index}"
                data[f"{name}__bars"] = ((f"{name}__bar", "bar_field"), axes["bars"])
                data[f"{name}__colours"] = (f"{name}__bar", axes["colours"])
                data[f"{name}__xlim"] = (f"{name}__limit", axes["xlim"])
                data[f"{name}__texts"] = (f"{name}__text", axes["texts"])
                data[f"{name}__labels"] = (f"{name}__label", axes["labels"])
                data[f"{name}__legend"] = (f"{name}__legend_entry", axes["legend"])
        return xarray.Dataset(data)

    return bindings, reduce
