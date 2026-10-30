"""Lift declarations of the legacy units owned by generator-validation.

A declaration supplies the names a stored legacy text reads and reduces the finished namespace to the value a test
compares; it imports no module of this checkout, and any helper it needs sits inside it, where the lift's digest
covers it, so each declaration repeats the kits it needs.

`_b6_simulate_from_vincop.py:597-626` and `:746-770` are two cells of the legacy simulation script that compare the
simulated events with the observed ones. Each runs the stored text unchanged and calls functions of `_utils.py` that
the declaration binds to their stored legacy text (the callees), and each reaches a draw only through those callees:
the pandas bootstrap of `_utils.py:1015-1016` and of `_utils.py:733`, which read numpy's global state and name no
seed. The lifts do not seed it, they declare `unseeded`, and they pack what depends on the draws as components of
their own (named with `draw` in the name) next to the components that do not, so that the test can compare the
second group exactly and drop the first by name.

`plt` is a stand-in that creates real pyplot figures under the non-interactive backend and keeps what each saved
figure holds; `Path` is a stand-in that records the folders the legacy cells ask to create and creates none; `pd`
is pandas except that `read_csv` returns the committed fixture the named file stands for. Nothing is written.

The six validation helpers of `_utils.py` (lines 597-921) are each a function the lift defines by running the stored
text unchanged and calls on committed fixtures. The two that reach the vine library's `simulate` (718-746 and 747-856)
run it through a stand-in that gives each call whose legacy call names no seeds the next seed of one generator built
from the committed fixture `seed`, in call order, and run the pandas bootstrap unseeded beside it; the components that
depend on the bootstrap are named with `draw`.
"""

from tests.correspondence.lift import declares_lift


@declares_lift(
    unit="_b6_simulate_from_vincop.py:597-626",
    fixtures=["gv_sim_realspace", "gv_obs_summaries"],
    draws="unseeded",
)
def declare_simulated_vs_observed_distribution_tests(fixtures):
    """Run the cell that compares each statistic's simulated and observed distribution for each copula source; reduce to the tables it builds and the figures it saves.

    The text loops over the copula sources of the simulated table, keeps the statistics a source's simulated rows all
    have, and calls `compare_2_distributions` (bound to its stored text, with `perform_two_sample_tests` and
    `plot_pvalue_distributions`) for each: 1000 bootstrap resamples of both samples (the draw, through pandas
    `sample` with no seed), the p-value figure of the bootstrap, and a histogram of the full samples with the
    full-sample tests. The value holds the per-source simulated tables, the folders asked for, and for each saved
    figure its path, the keyword arguments of the save and what does not depend on the draw (axis counts, labels,
    test names, and for a histogram its bars, lines, texts and legend); the bars of the p-value figures and the
    rates in their titles depend on the draw and are components of their own, named with `draw`. The text reads no
    `sys` except through `plot_obs_vs_montecarlo_simulated_statstic`, which this cell does not call, and
    no register stop marker lies in the texts it runs.
    """
    import hashlib
    import pathlib
    import textwrap
    import tomllib

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

    saved = []  # one entry per figure the callees saved: the path they named, the save's keywords and what the figure holds
    folders = []  # one entry per folder the cell asked for

    def figure_record(figure):
        """What a figure holds: size of the axes list, the super title, and for each axes its title, labels, limits, bars, lines, texts and legend."""
        suptitle = figure._suptitle.get_text() if figure._suptitle is not None else ""
        axes_records = []
        for axes in figure.axes:
            legend = axes.get_legend()
            axes_records.append(
                {
                    "title": axes.get_title(),
                    "xlabel": axes.get_xlabel(),
                    "ylabel": axes.get_ylabel(),
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
        return {"suptitle": suptitle, "axes": axes_records}

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
        """`Path` as the legacy cells name it: a call returns a handle whose `mkdir` is recorded and creates nothing."""

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

    unit_texts = [("_utils.py", 857, 921), ("_utils.py", 922, 990), ("_utils.py", 991, 1055)]
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
    if expected:
        raise ValueError(f"the register holds stop markers {expected} inside the texts this lift runs; it binds none")

    callee_labels = {
        ("_utils", "857-921"): ("stored-text:1:3e0cbd9c540ac11b9f87e7993da1f4146b429e3ae6d54c828e9bfd189dce1c8e", 857),
        ("_utils", "922-990"): ("stored-text:1:cec2082f2179b8214be6828a5a2b22b73d52c27a5e247bf7492853a5841a587a", 922),
        ("_utils", "991-1055"): ("stored-text:1:3ba1b0d48bc7d0330a6f5120079e390e678349ac5cc20ebec05fad50b6e5fc2e", 991),
    }
    namespace = {
        "np": numpy,
        "pd": pandas,
        "sys": Sys(),
        "plt": Plt(),
        "sns": seaborn,
        "mpatches": mpatches,
        "Line2D": Line2D,
        "textwrap": textwrap,
        "stats": stats,
        "Path": PathBinding(),
    }
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    observed = fixtures["gv_obs_summaries"]
    summaries = {
        event_type: group.drop(columns=["event_type", "event_id"]).reset_index(drop=True)
        for event_type, group in observed.groupby("event_type", sort=False)
    }
    bindings = {
        "df_simulated_realspace": fixtures["gv_sim_realspace"].copy(),
        "dic_event_summaries": summaries,
        "dir_plot": "plots/",
        "compare_2_distributions": namespace["compare_2_distributions"],
    }

    def reduce(finished):
        """The per-source simulated tables, the folders asked for, and the saved figures split into what does and does not depend on the draw."""
        pyplot.close("all")
        data = {}
        for event_type, table in finished["dic_simulations"].items():
            for column in table.columns:
                if column == "copula_source":
                    data[f"sim_{event_type}__{column}"] = (f"sim_{event_type}__row", table[column].to_numpy(dtype=str))
                else:
                    data[f"sim_{event_type}__{column}"] = (f"sim_{event_type}__row", table[column].to_numpy())
        data["folders"] = ("folder", numpy.asarray(folders, dtype=str))
        data["n_figures"] = ((), len(saved))
        for number, (path, kwargs, figure) in enumerate(saved):
            tag = f"fig{number:02d}"
            data[f"{tag}__path"] = ((), path)
            data[f"{tag}__kwargs"] = ((), kwargs)
            data[f"{tag}__suptitle"] = ((), figure["suptitle"])
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
                data[f"{tag}__bars"] = (
                    (f"{tag}__bar", "bar_field"),
                    numpy.asarray(only["bars"], dtype=float).reshape(-1, 3),
                )
                data[f"{tag}__lines"] = (
                    (f"{tag}__line", f"{tag}__point"),
                    numpy.asarray(only["lines"], dtype=float).reshape(len(only["lines"]), -1),
                )
                data[f"{tag}__texts"] = (f"{tag}__text", numpy.asarray(only["texts"], dtype=str))
                data[f"{tag}__legend"] = (f"{tag}__entry", numpy.asarray(only["legend"], dtype=str))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_b6_simulate_from_vincop.py:746-770",
    fixtures=["gv_sim_realspace", "gv_obs_summaries", "gv_fitted_cdf_long"],
    draws="unseeded",
)
def declare_pairwise_realspace_comparison(fixtures):
    """Run the cell that tests, for each pair of statistics of each copula source it does not skip, whether the simulated events reproduce the observed rank correlation; reduce to the results table and the figures it saves.

    The text loops over the sources, reads the fitted-probability table to learn each source's statistics
    (`format_fitted_cdf_values_for_fitting_copula`, with `read_csv` returning the fixture), and calls
    `evaluate_copula_fit_based_on_comparing_kendalltaus_for_each_variable_pair` with `cop_fitted=None`, so every
    pair is tested by 1000 bootstrap resamples of the simulated table (the draw: pandas `sample`, no seed, at
    `_utils.py:733`) and drawn as three panels. Every callee is bound to its stored text. The value holds, for the
    last source tested, the pair names and observed Kendall's tau; the test's result, p-value and simulated tau
    bounds depend on the draw and are components of their own, named with `draw`. For each saved figure the value
    holds the path, the keyword arguments of the save, the axis labels, the observed-statistic lines and the titles of
    the observed and simulated panels; the titles that state a test's result and the percentile text depend on the
    draw and are components of their own. No register stop marker lies in the texts it runs.
    """
    import hashlib
    import pathlib
    import textwrap
    import tomllib

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

    saved = []  # one entry per figure the callees saved: the path they named, the save's keywords and what the figure drew
    folders = []  # one entry per folder the cell asked for

    def figure_contents(figure):
        """What a figure drew: axis labels, dashed observed-statistic lines, panel titles, and the tau histogram's bars and texts."""
        axes = figure.axes
        return {
            "xlabels": [ax.get_xlabel() for ax in axes],
            "ylabels": [ax.get_ylabel() for ax in axes],
            "observed": sorted(
                float(line.get_xdata()[0]) for ax in axes for line in ax.lines if line.get_linestyle() == "--"
            ),
            "titles": [ax.get_title() for ax in axes],
            "bars": [
                (float(b.get_x()), float(b.get_width()), float(b.get_height()))
                for ax in axes
                for b in ax.patches
                if isinstance(b, matplotlib.patches.Rectangle)
            ],
            "texts": [t.get_text() for ax in axes for t in ax.texts],
        }

    class Plt:
        """The `plt` the callees draw with: real figures in memory, a record of each save, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def tight_layout(self, *args, **kwargs):
            """Lay the current figure out."""
            pyplot.tight_layout(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure drew under the path named, with the save's keyword arguments, and write no file."""
            saved.append((str(path), repr(sorted(kwargs.items())), figure_contents(pyplot.gcf())))

        def clf(self):
            """Clear the current figure, as the legacy callee does after each save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    class PathBinding:
        """`Path` as the legacy cells name it: a call returns a handle whose `mkdir` is recorded and creates nothing."""

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

    class PdBinding:
        """pandas as the legacy cells name it, except that `read_csv` returns the fixture the named file stands for."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The fitted-probability table of the event type the file name states; any other file is an error."""
            name = str(path).rsplit("/", 1)[-1]
            suffix = "_empirical_vs_fitted.csv"
            if not name.endswith(suffix):
                raise FileNotFoundError(f"the lift reads no csv named {path}")
            table = fixtures["gv_fitted_cdf_long"]
            part = table[table["event_type"] == name[: -len(suffix)]]
            return part.drop(columns=["event_type"]).reset_index(drop=True)

    unit_texts = [
        ("_utils.py", 410, 420),
        ("_utils.py", 421, 434),
        ("_utils.py", 597, 604),
        ("_utils.py", 605, 660),
        ("_utils.py", 661, 717),
        ("_utils.py", 718, 746),
        ("_utils.py", 747, 856),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own excepted
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
    if expected:
        raise ValueError(f"the register holds stop markers {expected} inside the texts this lift runs; it binds none")

    callee_labels = {
        ("_utils", "410-420"): ("stored-text:1:c0f0f53e259dcbcf9ef15a91817aea99299930c746892cafe6700f9a1424baff", 410),
        ("_utils", "421-434"): ("stored-text:1:4ef6e81b8733a313a33f666ed883055d16ca97779e8c7c13d1f63dfdb23def9d", 421),
        ("_utils", "597-604"): ("stored-text:1:efef30214c4a04554a2932afb505cf3fe056333ff4b223b46bd16ee7581d7394", 597),
        ("_utils", "605-660"): ("stored-text:1:f7cf4b0fef56d6956864824f37f042b5c47a53dea922996f83e8a55b2051e89b", 605),
        ("_utils", "661-717"): ("stored-text:1:7a6467a5addf894a7aa686d1890eb41a333984baa05b02227784168e53f320fe", 661),
        ("_utils", "718-746"): ("stored-text:1:303748bf356cb7c6339e2c0ec17bbbbd763b791fef286d81c2c660b3fa97ebbb", 718),
        ("_utils", "747-856"): ("stored-text:1:9c39c8cfd602394a12629dd249401f6bfee7d305688b358193bc5d54d06b1ddd", 747),
    }
    namespace = {
        "np": numpy,
        "pd": PdBinding(),
        "sys": Sys(),
        "plt": Plt(),
        "sns": seaborn,
        "mpatches": mpatches,
        "Line2D": Line2D,
        "textwrap": textwrap,
        "kendalltau": stats.kendalltau,
        "gaussian_kde": stats.gaussian_kde,
        "stats": stats,
        "Path": PathBinding(),
        "dir_fitting_maringals": "marginals/",
    }
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    observed = fixtures["gv_obs_summaries"]
    summaries = {
        event_type: group.drop(columns=["event_type", "event_id"]).reset_index(drop=True)
        for event_type, group in observed.groupby("event_type", sort=False)
    }
    bindings = {
        "Path": PathBinding(),
        "dic_copulas": {"rain": None, "surge": None, "compound": None},
        "lst_sim_to_skip": ["surge"],
        "df_simulated_realspace": fixtures["gv_sim_realspace"].copy(),
        "dic_event_summaries": summaries,
        "dir_plot": "plots/",
        "alpha": 0.05,
        "format_fitted_cdf_values_for_fitting_copula": namespace["format_fitted_cdf_values_for_fitting_copula"],
        "evaluate_copula_fit_based_on_comparing_kendalltaus_for_each_variable_pair": namespace[
            "evaluate_copula_fit_based_on_comparing_kendalltaus_for_each_variable_pair"
        ],
    }

    def reduce(finished):
        """The last source's pair table, the folders asked for, and the saved figures split into what does and does not depend on the draw."""
        pyplot.close("all")
        results = finished["df_mc_results_combined"].reset_index(drop=True)
        data = {
            "result_var1": ("pair", results["var1"].to_numpy(dtype=str)),
            "result_var2": ("pair", results["var2"].to_numpy(dtype=str)),
            "result_obs_kendalltau": ("pair", results["obs_kendalltau"].to_numpy(dtype=float)),
            "result_draw_mc_result": ("pair", results["mc_result"].to_numpy(dtype=str)),
            "result_draw_mc_pvalue": ("pair", results["mc_pvalue"].to_numpy(dtype=float)),
            "result_draw_lowerbound": ("pair", results["lowerbound_sim_tau"].to_numpy(dtype=float)),
            "result_draw_upperbound": ("pair", results["upperbound_sim_tau"].to_numpy(dtype=float)),
            "folders": ("folder", numpy.asarray(folders, dtype=str)),
            "n_figures": ((), len(saved)),
        }
        for number, (path, kwargs, contents) in enumerate(saved):
            tag = f"fig{number:02d}"
            data[f"{tag}__path"] = ((), path)
            data[f"{tag}__kwargs"] = ((), kwargs)
            data[f"{tag}__xlabels"] = (f"{tag}__axis", numpy.asarray(contents["xlabels"], dtype=str))
            data[f"{tag}__ylabels"] = (f"{tag}__axis", numpy.asarray(contents["ylabels"], dtype=str))
            data[f"{tag}__observed"] = (f"{tag}__line", numpy.asarray(contents["observed"], dtype=float))
            titles = contents["titles"]
            groups = {
                "titles_observed": [t for t in titles if t.startswith("Observed Data")],
                "titles_simulated": [t for t in titles if t.startswith("Simulated Data")],
                "draw_titles_tests": [t for t in titles if not t.startswith(("Observed Data", "Simulated Data"))],
            }
            for group, members in groups.items():
                data[f"{tag}__{group}"] = (f"{tag}__{group}_axis", numpy.asarray(members, dtype=str))
            data[f"{tag}__draw_bars"] = (
                (f"{tag}__draw_bar", "bar_field"),
                numpy.asarray(contents["bars"], dtype=float).reshape(-1, 3),
            )
            data[f"{tag}__draw_texts"] = (f"{tag}__draw_text", numpy.asarray(contents["texts"], dtype=str))
        return xarray.Dataset(data)

    return bindings, reduce


# ---- the validation helpers of `_utils.py` (4c.2) ----


@declares_lift(unit="_utils.py:597-604", fixtures=["gv_samples"])
def declare_quantile_of_nearest_value(fixtures):
    """Run the quantile lookup on a sample for values inside the sample, on a member, below and above it, and for a series with a tie; reduce to the quantiles it returns."""
    import pandas

    sample = fixtures["gv_samples"]["sample1"]
    tied = pandas.Series([1.0, 2.0, 2.0, 3.0, 5.0])
    probes = (
        ("inside", sample, float(sample.median())),
        ("member", sample, float(sample.iloc[7])),
        ("below", sample, float(sample.min()) - 1.0),
        ("above", sample, float(sample.max()) + 1.0),
        ("tied", tied, 2.0),
        ("between_tied", tied, 3.9),
    )

    def reduce(finished):
        """One quantile per probe, named by the probe."""
        lookup = finished["get_quantile_from_value"]
        return pandas.Series({name: float(lookup(series, value)) for name, series, value in probes}, dtype=float)

    return {}, reduce


@declares_lift(unit="_utils.py:605-660", fixtures=["gv_copula_obs", "gv_copula_sim"])
def declare_common_density_percentiles(fixtures):
    """Run the common-density-percentile function on the invented observed and simulated tables for two percentiles; reduce to the three numbers each call returns."""
    import numpy
    import pandas

    def reduce(finished):
        """The common maximum density and the two difference percentiles, for a high and a low percentile."""
        compute = finished["compute_common_percentiles"]
        values = {}
        for percentile in (98, 80):
            result = compute(fixtures["gv_copula_obs"], fixtures["gv_copula_sim"], 20, percentile)
            for key in ("z_max", "diff_min", "diff_max"):
                values[f"{key}_{percentile}"] = float(result[key])
        return pandas.Series(values, dtype=float)

    return {"np": numpy}, reduce


@declares_lift(unit="_utils.py:661-717", fixtures=["gv_samples"])
def declare_statistic_distribution_figure(fixtures):
    """Run the Monte Carlo statistic histogram on a sample of taus for monte-carlo and bootstrap labels and every percentile suffix; reduce to the data each figure holds."""
    import pathlib
    import textwrap
    import tomllib

    import matplotlib
    import numpy
    import seaborn
    import xarray
    from matplotlib import patches as mpatches
    from matplotlib.lines import Line2D

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    root = pathlib.Path(__file__).resolve().parents[2]
    saved = []  # one entry per figure the texts saved: the path, the save's keywords and what the figure holds

    def figure_record(figure):
        """What a figure holds: its size, texts and super title and, for each axes, titles, labels, limits, bars, lines, texts, legend and scatter points."""
        axes_records = []
        for axes in figure.axes:
            legend = axes.get_legend()
            bars = [b for b in axes.patches if isinstance(b, matplotlib.patches.Rectangle)]
            axes_records.append(
                {
                    "title": axes.get_title(),
                    "xlabel": axes.get_xlabel(),
                    "ylabel": axes.get_ylabel(),
                    "xlim": [float(v) for v in axes.get_xlim()],
                    "ylim": [float(v) for v in axes.get_ylim()],
                    "bars": [
                        (float(b.get_x()), float(b.get_width()), float(b.get_height()), float(b.get_alpha() or 0.0))
                        for b in bars
                    ],
                    "colours": [matplotlib.colors.to_hex(b.get_facecolor()) for b in bars],
                    "lines": [(line.get_linestyle(), float(line.get_xdata()[0])) for line in axes.lines],
                    "texts": [
                        f"{t.get_text()}|{[float(v) for v in t.get_position()]}|{float(t.get_fontsize())}"
                        for t in axes.texts
                    ],
                    "legend": [] if legend is None else [t.get_text() for t in legend.get_texts()],
                    "scatter": [
                        float(v)
                        for c in axes.collections
                        if type(c).__name__ == "PathCollection"
                        for v in numpy.asarray(c.get_offsets(), dtype=float).ravel()
                    ],
                    "n_collections": len(axes.collections),
                }
            )
        return {
            "suptitle": figure._suptitle.get_text() if figure._suptitle is not None else "",
            "size": [float(v) for v in figure.get_size_inches()] + [float(figure.get_dpi())],
            "axes": axes_records,
        }

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

    def pack_figure(data, tag, figure, prefix=""):
        """Add one figure's record to `data` as numbered components; `prefix` names a draw-dependent group."""
        data[f"{tag}__{prefix}suptitle"] = ((), figure["suptitle"])
        data[f"{tag}__{prefix}size"] = (f"{tag}__size_field", numpy.asarray(figure["size"], dtype=float))
        for index, axes in enumerate(figure["axes"]):
            name = f"{tag}__ax{index}"
            data[f"{name}__title"] = ((), axes["title"])
            data[f"{name}__labels"] = (f"{name}__label", numpy.asarray([axes["xlabel"], axes["ylabel"]], dtype=str))
            data[f"{name}__lims"] = (
                (f"{name}__lim", "bound"),
                numpy.asarray([axes["xlim"], axes["ylim"]], dtype=float),
            )
            data[f"{name}__bars"] = (
                (f"{name}__bar", "bar_field"),
                numpy.asarray(axes["bars"], dtype=float).reshape(-1, 4),
            )
            data[f"{name}__colours"] = (f"{name}__bar", numpy.asarray(axes["colours"], dtype=str))
            data[f"{name}__lines"] = (f"{name}__line", numpy.asarray([f"{s}|{x}" for s, x in axes["lines"]], dtype=str))
            data[f"{name}__texts"] = (f"{name}__text", numpy.asarray(axes["texts"], dtype=str))
            data[f"{name}__legend"] = (f"{name}__entry", numpy.asarray(axes["legend"], dtype=str))
            data[f"{name}__scatter"] = (f"{name}__point", numpy.asarray(axes["scatter"], dtype=float))
            data[f"{name}__n_collections"] = ((), axes["n_collections"])

    class Sys:
        """The `sys` the legacy texts read: `exit` raises `SystemExit`, as the legacy call does; nothing else is modelled."""

        def exit(self, *args):
            """Exit as legacy does: no register stop marker lies in the texts this lift runs."""
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    unit_texts = [
        ("_utils.py", 661, 717),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own included
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
    if expected:
        raise ValueError(f"the register holds stop markers {expected} inside the texts this lift runs; it binds none")

    namespace = {
        "sys": Sys(),
        "plt": Plt(),
        "sns": seaborn,
        "mpatches": mpatches,
        "Line2D": Line2D,
        "textwrap": textwrap,
    }
    bindings = dict(namespace)

    def reduce(finished):
        """The figures of six calls: one for each percentile suffix and sim_type kept open, one saved and cleared."""
        draw = finished["plot_obs_vs_montecarlo_simulated_statstic"]
        taus = fixtures["gv_samples"]["taus"]
        # percentile 21 -> "st", 32 -> "nd", 43 -> "rd", 55 -> "th", 12 -> "nd" (the `== 12` branch is unreachable)
        calls = (
            ("monte_carlo", 0.21, "fail to reject"),
            ("bootstrap", 0.32, "fail to reject"),
            ("monte_carlo", 0.43, "fail to reject"),
            ("bootstrap", 0.55, "fail to reject"),
            ("monte_carlo", 0.12, "reject"),
        )
        data = {}
        for number, (sim_type, pvalue, result) in enumerate(calls):
            fig, ax = draw(
                varname_test="Kendall Tau",
                s_mc_simulated_stat=taus,
                val_obs_stat=float(taus.iloc[3]),
                pvalue=pvalue,
                string_two_sided_test_result=result,
                alpha=0.05,
                n_obs=30,
                n_mc_sims=len(taus),
                fig_maintitle="observed vs. stochastically simulated rain events",
                clearfigs=False,
                sim_type=sim_type,
            )
            pack_figure(data, f"open{number}", figure_record(fig))
            pyplot.close("all")
        draw(
            varname_test="Kendall Tau",
            s_mc_simulated_stat=taus,
            val_obs_stat=float(taus.iloc[5]),
            pvalue=0.5,
            string_two_sided_test_result="fail to reject",
            alpha=0.1,
            n_obs=30,
            n_mc_sims=len(taus),
            fig_maintitle="saved",
            f_savefig="plots/tau.png",
            sim_type="monte_carlo",
        )
        pyplot.close("all")
        data["n_saved"] = ((), len(saved))
        for number, (path, kwargs, figure) in enumerate(saved):
            data[f"saved{number}__path"] = ((), path)
            data[f"saved{number}__kwargs"] = ((), kwargs)
            pack_figure(data, f"saved{number}", figure)
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_utils.py:718-746",
    fixtures=["gv_copula_obs", "gv_copula_sim", "gv_vinecop_json", "seed"],
    draws="unseeded",
)
def declare_kendall_tau_monte_carlo_test(fixtures):
    """Run the Kendall-tau Monte Carlo test from a fitted vine copula (seeded) and by bootstrapping a simulated table (unseeded); reduce to the results of both.

    The copula calls go through a stand-in that gives every `simulate` call whose legacy call names no seeds the next
    seed of one generator built from the committed `seed` fixture, in call order, so their results are repeatable and
    compared exactly. The bootstrap calls draw through pandas `sample` with no seed (`_utils.py:733`): their observed
    tau is compared, and their result, p-value and simulated taus are components named with `draw`, dropped by the
    test. The copula calls use several pairs and significance levels so the result's three branches are reached.
    """
    import hashlib
    import pathlib
    import tomllib

    import matplotlib
    import numpy
    import pandas
    import pyvinecopulib
    import xarray
    from scipy import stats

    matplotlib.use("agg")

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
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

        def simulate(self, n, seeds=None):
            """`n` draws as an array; the seeds the legacy call names, else the next seed of the stream built from the fixture's seed."""
            if seeds is None:
                seeds = [int(seed_stream.integers(1, 2**31 - 1))]
            return self.fitted.simulate(n=n, seeds=list(seeds))

    vine = Vine(pyvinecopulib.Vinecop.from_json(str(fixtures["gv_vinecop_json"][0])))

    class Sys:
        """The `sys` the legacy texts read: `exit` raises `SystemExit`, as the legacy call does; nothing else is modelled."""

        def exit(self, *args):
            """Exit as legacy does: no register stop marker lies in the texts this lift runs."""
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    unit_texts = [
        ("_utils.py", 421, 434),
        ("_utils.py", 597, 604),
        ("_utils.py", 718, 746),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own included
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
    if expected:
        raise ValueError(f"the register holds stop markers {expected} inside the texts this lift runs; it binds none")
    callee_labels = {
        ("_utils", "421-434"): ("stored-text:1:4ef6e81b8733a313a33f666ed883055d16ca97779e8c7c13d1f63dfdb23def9d", 421),
        ("_utils", "597-604"): ("stored-text:1:efef30214c4a04554a2932afb505cf3fe056333ff4b223b46bd16ee7581d7394", 597),
    }
    namespace = {"np": numpy, "pd": pandas, "sys": Sys(), "kendalltau": stats.kendalltau}
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    bindings = {
        "pd": pandas,
        "kendalltau": stats.kendalltau,
        "simulate_from_copula": namespace["simulate_from_copula"],
        "get_quantile_from_value": namespace["get_quantile_from_value"],
    }

    def reduce(finished):
        """The results of three copula calls and one bootstrap call, as named components."""
        test = finished["monte_carlo_tau_test"]
        obs, sim = fixtures["gv_copula_obs"], fixtures["gv_copula_sim"]
        data = {}
        for number, (first, second, alpha) in enumerate(
            ((0, 1, 0.6), (0, 1, 0.0), (0, 1, 1.0), (1, 2, 0.3), (0, 2, 0.3), (0, 2, 1.0))
        ):
            result, pvalue, taus, tau_obs = test(
                df_obs=obs, col1_iloc=first, col2_iloc=second, cop_fitted=vine, n_samples=30, alpha=alpha
            )
            data[f"copula{number}__result"] = ((), result)
            data[f"copula{number}__pvalue"] = ((), float(pvalue))
            data[f"copula{number}__taus"] = (f"copula{number}__sample", taus.to_numpy(dtype=float))
            data[f"copula{number}__tau_obs"] = ((), float(tau_obs))
        result, pvalue, taus, tau_obs = test(
            df_obs=obs, col1_iloc=0, col2_iloc=2, df_simulated=sim, n_samples=30, alpha=0.6
        )
        data["bootstrap__tau_obs"] = ((), float(tau_obs))
        data["bootstrap__draw_result"] = ((), result)
        data["bootstrap__draw_pvalue"] = ((), float(pvalue))
        data["bootstrap__draw_taus"] = ("bootstrap__draw_sample", taus.to_numpy(dtype=float))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_utils.py:718-746",
    part="draw-law",
    fixtures=["gv_copula_obs", "gv_copula_sim"],
    draws="unseeded",
)
def declare_kendall_tau_bootstrap_law_parameters(fixtures):
    """Run the Kendall-tau Monte Carlo test once by bootstrapping and reduce to the parameter of the law its draw follows: the row count of the table resampled."""
    import hashlib
    import pathlib

    import numpy
    import pandas
    from scipy import stats

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
    callee_labels = {
        ("_utils", "421-434"): ("stored-text:1:4ef6e81b8733a313a33f666ed883055d16ca97779e8c7c13d1f63dfdb23def9d", 421),
        ("_utils", "597-604"): ("stored-text:1:efef30214c4a04554a2932afb505cf3fe056333ff4b223b46bd16ee7581d7394", 597),
    }
    namespace = {"np": numpy, "pd": pandas}
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    bindings = {
        "pd": pandas,
        "kendalltau": stats.kendalltau,
        "simulate_from_copula": namespace["simulate_from_copula"],
        "get_quantile_from_value": namespace["get_quantile_from_value"],
    }

    def reduce(finished):
        """`sample(replace=True)` picks rows uniformly, so the law's parameter is the row count of the simulated table."""
        finished["monte_carlo_tau_test"](
            df_obs=fixtures["gv_copula_obs"],
            col1_iloc=0,
            col2_iloc=1,
            df_simulated=fixtures["gv_copula_sim"],
            n_samples=1,
        )
        return pandas.Series({"rows": len(fixtures["gv_copula_sim"])})

    return bindings, reduce


@declares_lift(
    unit="_utils.py:747-856",
    fixtures=["gv_copula_obs", "gv_copula_sim", "gv_vinecop_json", "seed"],
    draws="unseeded",
)
def declare_copula_fit_kendall_tau_evaluation(fixtures):
    """Run the per-pair Kendall-tau evaluation with a fitted vine copula (seeded) and with a simulated table to bootstrap (unseeded); reduce to the results table and the figures of both.

    With a copula, the evaluator simulates a table from it and runs the Monte Carlo test for every pair; the stand-in
    gives each `simulate` call the next seed of one generator built from the committed `seed` fixture, in call order,
    so those results and figures are repeatable and compared exactly. With a simulated table instead, every pair is
    bootstrapped through pandas `sample` with no seed (`_utils.py:733`): the pair names and the observed tau are
    compared, and the result, p-value, simulated-tau bounds, histogram bars and texts are components named with `draw`,
    dropped by the test. The callees are bound to their stored texts.
    """
    import hashlib
    import pathlib
    import textwrap
    import tomllib

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

        def simulate(self, n, seeds=None):
            """`n` draws as an array; the seeds the legacy call names, else the next seed of the stream built from the fixture's seed."""
            if seeds is None:
                seeds = [int(seed_stream.integers(1, 2**31 - 1))]
            return self.fitted.simulate(n=n, seeds=list(seeds))

    vine = Vine(pyvinecopulib.Vinecop.from_json(str(fixtures["gv_vinecop_json"][0])))
    saved = []  # one entry per figure the texts saved: the path, the save's keywords and what the figure holds

    def figure_record(figure):
        """What a figure holds: its size, texts and super title and, for each axes, titles, labels, limits, bars, lines, texts, legend and scatter points."""
        axes_records = []
        for axes in figure.axes:
            legend = axes.get_legend()
            bars = [b for b in axes.patches if isinstance(b, matplotlib.patches.Rectangle)]
            axes_records.append(
                {
                    "title": axes.get_title(),
                    "xlabel": axes.get_xlabel(),
                    "ylabel": axes.get_ylabel(),
                    "xlim": [float(v) for v in axes.get_xlim()],
                    "ylim": [float(v) for v in axes.get_ylim()],
                    "bars": [
                        (float(b.get_x()), float(b.get_width()), float(b.get_height()), float(b.get_alpha() or 0.0))
                        for b in bars
                    ],
                    "colours": [matplotlib.colors.to_hex(b.get_facecolor()) for b in bars],
                    "lines": [(line.get_linestyle(), float(line.get_xdata()[0])) for line in axes.lines],
                    "texts": [
                        f"{t.get_text()}|{[float(v) for v in t.get_position()]}|{float(t.get_fontsize())}"
                        for t in axes.texts
                    ],
                    "legend": [] if legend is None else [t.get_text() for t in legend.get_texts()],
                    "scatter": [
                        float(v)
                        for c in axes.collections
                        if type(c).__name__ == "PathCollection"
                        for v in numpy.asarray(c.get_offsets(), dtype=float).ravel()
                    ],
                    "n_collections": len(axes.collections),
                }
            )
        return {
            "suptitle": figure._suptitle.get_text() if figure._suptitle is not None else "",
            "size": [float(v) for v in figure.get_size_inches()] + [float(figure.get_dpi())],
            "axes": axes_records,
        }

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

    def pack_figure(data, tag, figure, prefix=""):
        """Add one figure's record to `data` as numbered components; `prefix` names a draw-dependent group."""
        data[f"{tag}__{prefix}suptitle"] = ((), figure["suptitle"])
        data[f"{tag}__{prefix}size"] = (f"{tag}__size_field", numpy.asarray(figure["size"], dtype=float))
        for index, axes in enumerate(figure["axes"]):
            name = f"{tag}__ax{index}"
            data[f"{name}__title"] = ((), axes["title"])
            data[f"{name}__labels"] = (f"{name}__label", numpy.asarray([axes["xlabel"], axes["ylabel"]], dtype=str))
            data[f"{name}__lims"] = (
                (f"{name}__lim", "bound"),
                numpy.asarray([axes["xlim"], axes["ylim"]], dtype=float),
            )
            data[f"{name}__bars"] = (
                (f"{name}__bar", "bar_field"),
                numpy.asarray(axes["bars"], dtype=float).reshape(-1, 4),
            )
            data[f"{name}__colours"] = (f"{name}__bar", numpy.asarray(axes["colours"], dtype=str))
            data[f"{name}__lines"] = (f"{name}__line", numpy.asarray([f"{s}|{x}" for s, x in axes["lines"]], dtype=str))
            data[f"{name}__texts"] = (f"{name}__text", numpy.asarray(axes["texts"], dtype=str))
            data[f"{name}__legend"] = (f"{name}__entry", numpy.asarray(axes["legend"], dtype=str))
            data[f"{name}__scatter"] = (f"{name}__point", numpy.asarray(axes["scatter"], dtype=float))
            data[f"{name}__n_collections"] = ((), axes["n_collections"])

    class Sys:
        """The `sys` the legacy texts read: `exit` raises `SystemExit`, as the legacy call does; nothing else is modelled."""

        def exit(self, *args):
            """Exit as legacy does: no register stop marker lies in the texts this lift runs."""
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    unit_texts = [
        ("_utils.py", 421, 434),
        ("_utils.py", 597, 604),
        ("_utils.py", 605, 660),
        ("_utils.py", 661, 717),
        ("_utils.py", 718, 746),
        ("_utils.py", 747, 856),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own included
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
    if expected:
        raise ValueError(f"the register holds stop markers {expected} inside the texts this lift runs; it binds none")
    callee_labels = {
        ("_utils", "421-434"): ("stored-text:1:4ef6e81b8733a313a33f666ed883055d16ca97779e8c7c13d1f63dfdb23def9d", 421),
        ("_utils", "597-604"): ("stored-text:1:efef30214c4a04554a2932afb505cf3fe056333ff4b223b46bd16ee7581d7394", 597),
        ("_utils", "605-660"): ("stored-text:1:f7cf4b0fef56d6956864824f37f042b5c47a53dea922996f83e8a55b2051e89b", 605),
        ("_utils", "661-717"): ("stored-text:1:7a6467a5addf894a7aa686d1890eb41a333984baa05b02227784168e53f320fe", 661),
        ("_utils", "718-746"): ("stored-text:1:303748bf356cb7c6339e2c0ec17bbbbd763b791fef286d81c2c660b3fa97ebbb", 718),
    }
    namespace = {
        "np": numpy,
        "pd": pandas,
        "sys": Sys(),
        "plt": Plt(),
        "sns": seaborn,
        "mpatches": mpatches,
        "Line2D": Line2D,
        "textwrap": textwrap,
        "stats": stats,
        "kendalltau": stats.kendalltau,
        "gaussian_kde": stats.gaussian_kde,
    }
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    bindings = {
        "pd": pandas,
        "np": numpy,
        "plt": namespace["plt"],
        "kendalltau": stats.kendalltau,
        "gaussian_kde": stats.gaussian_kde,
        "simulate_from_copula": namespace["simulate_from_copula"],
        "monte_carlo_tau_test": namespace["monte_carlo_tau_test"],
        "compute_common_percentiles": namespace["compute_common_percentiles"],
        "plot_obs_vs_montecarlo_simulated_statstic": namespace["plot_obs_vs_montecarlo_simulated_statstic"],
    }

    def reduce(finished):
        """The results table and saved figures of the copula run and of the bootstrap run."""
        evaluate = finished["evaluate_copula_fit_based_on_comparing_kendalltaus_for_each_variable_pair"]
        obs, sim = fixtures["gv_copula_obs"], fixtures["gv_copula_sim"]
        data = {}
        runs = (
            ("copula", {"cop_fitted": vine, "n_cop_sim_for_dnsty_plts": 200}, ""),
            ("bootstrap", {"df_simulated": sim}, "draw_"),
        )
        for label, extra, draw_prefix in runs:
            del saved[:]
            results = evaluate(
                df_obs=obs,
                alpha=0.05,
                n_for_montecarlo_tau_test=6,
                plot=True,
                fldr_plots="plots/",
                fig_main_title="observed vs. stochastically simulated",
                **extra,
            ).reset_index(drop=True)
            pyplot.close("all")
            data[f"{label}__var1"] = (f"{label}__pair", results["var1"].to_numpy(dtype=str))
            data[f"{label}__var2"] = (f"{label}__pair", results["var2"].to_numpy(dtype=str))
            data[f"{label}__obs_kendalltau"] = (f"{label}__pair", results["obs_kendalltau"].to_numpy(dtype=float))
            data[f"{label}__{draw_prefix}mc_result"] = (f"{label}__pair", results["mc_result"].to_numpy(dtype=str))
            data[f"{label}__{draw_prefix}mc_pvalue"] = (f"{label}__pair", results["mc_pvalue"].to_numpy(dtype=float))
            data[f"{label}__{draw_prefix}lowerbound"] = (
                f"{label}__pair",
                results["lowerbound_sim_tau"].to_numpy(dtype=float),
            )
            data[f"{label}__{draw_prefix}upperbound"] = (
                f"{label}__pair",
                results["upperbound_sim_tau"].to_numpy(dtype=float),
            )
            data[f"{label}__n_figures"] = ((), len(saved))
            for number, (path, kwargs, figure) in enumerate(saved):
                tag = f"{label}_fig{number}"
                data[f"{tag}__path"] = ((), path)
                data[f"{tag}__kwargs"] = ((), kwargs)
                if draw_prefix:
                    # the third panel, the histogram of the bootstrapped taus with its result title, is the one that depends on the draw
                    panels = figure["axes"]
                    fixed = {"suptitle": figure["suptitle"], "size": figure["size"], "axes": panels[:2] + panels[3:]}
                    moving = {"suptitle": "", "size": figure["size"], "axes": panels[2:3]}
                    pack_figure(data, f"{tag}__fixed", fixed)
                    pack_figure(data, f"{tag}__draw", moving)
                else:
                    pack_figure(data, tag, figure)
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_utils.py:857-921", fixtures=["gv_samples"])
def declare_two_sample_test_battery(fixtures):
    """Run the two-sample test battery on the two invented samples at two significance levels; reduce to the tables it returns."""
    import numpy
    import pandas
    from scipy import stats

    def reduce(finished):
        """The table of tests, p-values and conclusions for each level, one after the other with the level named."""
        battery = finished["perform_two_sample_tests"]
        sample1, sample2 = fixtures["gv_samples"]["sample1"], fixtures["gv_samples"]["sample2"]
        parts = []
        for alpha in (0.05, 0.5):
            table = battery(sample1, sample2, alpha=alpha)
            table.insert(0, "alpha", alpha)
            parts.append(table)
        return pandas.concat(parts, ignore_index=True)

    return {"np": numpy, "stats": stats, "pd": pandas}, reduce
