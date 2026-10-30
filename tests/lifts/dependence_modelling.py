"""Lift declarations of the legacy units owned by dependence-modelling.

A declaration supplies the names a stored legacy text reads and reduces the finished namespace to the value a test
compares; it imports no module of this checkout, and any helper it needs sits inside it, where the lift's digest
covers it. This file is written by a generator, so each declaration repeats the kits it needs.

The legacy script targets an older release of the vine-copula library than the one installed: a fitted copula's
`cdf` and `select` take arrays and not tables, `to_json` returns text, and a copula is built by `from_data` or
`from_file`. The lifts that fit, load or evaluate a copula bind `pv` and the copula they pass to a stand-in over
the installed library that accepts the legacy calls; the legacy text is unchanged. The library's `cdf` is quasi-Monte
Carlo and unseeded unless seeds are given, and the legacy calls of `simulate` name none. The stand-in therefore passes
the committed fixture `seed` itself to every `cdf` call, and to every `simulate` call a seed drawn from one generator
built from it (`numpy.random.default_rng(seed)`, one `integers(1, 2**31 - 1)` per call, in the order the legacy text
calls): the lift's value is repeatable, the repeated simulations are independent, and the new side receives the same
value as `inputs["seed"]` and builds the same generator.
"""

from tests.correspondence.lift import declares_lift


@declares_lift(unit="_b5_fit_vine_copulas.py:58-71", fixtures=["obs"])
def declare_empirical_multivariate_cdf(fixtures):
    """Run the empirical joint-probability function on the invented observations; reduce to the series it returns."""
    import pandas

    return {"pd": pandas}, lambda finished: finished["empirical_multivariate_cdf"](fixtures["obs"])


@declares_lift(unit="_b5_fit_vine_copulas.py:72-75", fixtures=["obs"])
def declare_cramer_von_mises_criterion(fixtures):
    """Run the distance function on two columns of the invented observations; reduce to the distance it returns."""
    obs = fixtures["obs"]

    return {}, lambda finished: float(finished["compute_cvm_criterion"](obs.iloc[:, 0], obs.iloc[:, 1]))


@declares_lift(unit="_b5_fit_vine_copulas.py:120-127", fixtures=["obs", "vinecop_json", "seed"], draws="seeded")
def declare_observed_cvm_statistic(fixtures):
    """Run the observed-distance function on the invented observations and fitted copula; reduce to the distance."""
    import hashlib
    import pathlib

    import numpy
    import pandas
    import pyvinecopulib

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

        def select(self, data=None, controls=None):
            """Select the structure and pair copulas for `data` in place."""
            self.fitted.select(numpy.asfortranarray(numpy.asarray(data, dtype=float)), controls)

        def to_json(self, path=None):
            """The copula as json text, written to `path` too when one is given."""
            text = self.fitted.to_json()
            if path is not None:
                pathlib.Path(path).write_text(text, encoding="utf-8")
            return text

    def vinecop(source=None, data=None, controls=None):
        """A copula fitted to `data` under `controls`, or loaded from the json file named by `source`."""
        if data is not None:
            return Vine(
                pyvinecopulib.Vinecop.from_data(numpy.asfortranarray(numpy.asarray(data, dtype=float)), controls)
            )
        return Vine(pyvinecopulib.Vinecop.from_file(str(source)))

    callee_labels = {
        ("_b5_fit_vine_copulas", "58-71"): (
            "stored-text:1:8be614a46a8213b16672ec7a4dcf588adc63b56fbc661a7a1ceadd8f9ee3258c",
            58,
        ),
        ("_b5_fit_vine_copulas", "72-75"): (
            "stored-text:1:ccbb3f556df6d718de11f1980cca6eb116ca913821018a2816c5155fc59a1b8f",
            72,
        ),
    }
    namespace = {"np": numpy, "pd": pandas}
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)
    vine = Vine(pyvinecopulib.Vinecop.from_json(str(fixtures["vinecop_json"][0])))
    bindings = {
        "pd": pandas,
        "empirical_multivariate_cdf": namespace["empirical_multivariate_cdf"],
        "compute_cvm_criterion": namespace["compute_cvm_criterion"],
    }

    return bindings, lambda finished: float(finished["compute_obs_cvm_stat"](fixtures["obs"], vine))


@declares_lift(unit="_b5_fit_vine_copulas.py:76-96", fixtures=["obs", "vinecop_json", "seed"], draws="seeded")
def declare_monte_carlo_cvm_test(fixtures):
    """Run the Monte Carlo distance test on the invented observations and fitted copula; reduce to the observed distance and the test's result, p-value and simulated distances."""
    import hashlib
    import inspect
    import pathlib
    import tomllib

    import numpy
    import pandas
    import pyvinecopulib
    import xarray

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

        def select(self, data=None, controls=None):
            """Select the structure and pair copulas for `data` in place."""
            self.fitted.select(numpy.asfortranarray(numpy.asarray(data, dtype=float)), controls)

        def to_json(self, path=None):
            """The copula as json text, written to `path` too when one is given."""
            text = self.fitted.to_json()
            if path is not None:
                pathlib.Path(path).write_text(text, encoding="utf-8")
            return text

    def vinecop(source=None, data=None, controls=None):
        """A copula fitted to `data` under `controls`, or loaded from the json file named by `source`."""
        if data is not None:
            return Vine(
                pyvinecopulib.Vinecop.from_data(numpy.asfortranarray(numpy.asarray(data, dtype=float)), controls)
            )
        return Vine(pyvinecopulib.Vinecop.from_file(str(source)))

    unit_texts = [
        ("_b5_fit_vine_copulas.py", 76, 96),
        ("_b5_fit_vine_copulas.py", 58, 71),
        ("_b5_fit_vine_copulas.py", 72, 75),
        ("_utils.py", 421, 434),
        ("_utils.py", 597, 604),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own included
    stop_lines = [
        ("_b5_fit_vine_copulas.py", 78)
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
        ("_b5_fit_vine_copulas", "58-71"): (
            "stored-text:1:8be614a46a8213b16672ec7a4dcf588adc63b56fbc661a7a1ceadd8f9ee3258c",
            58,
        ),
        ("_b5_fit_vine_copulas", "72-75"): (
            "stored-text:1:ccbb3f556df6d718de11f1980cca6eb116ca913821018a2816c5155fc59a1b8f",
            72,
        ),
        ("_utils", "421-434"): ("stored-text:1:4ef6e81b8733a313a33f666ed883055d16ca97779e8c7c13d1f63dfdb23def9d", 421),
        ("_utils", "597-604"): ("stored-text:1:efef30214c4a04554a2932afb505cf3fe056333ff4b223b46bd16ee7581d7394", 597),
    }
    namespace = {"np": numpy, "pd": pandas, "sys": Sys()}
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)
    vine = Vine(pyvinecopulib.Vinecop.from_json(str(fixtures["vinecop_json"][0])))
    bindings = {
        "np": numpy,
        "pd": pandas,
        "sys": namespace["sys"],
        "empirical_multivariate_cdf": namespace["empirical_multivariate_cdf"],
        "compute_cvm_criterion": namespace["compute_cvm_criterion"],
        "simulate_from_copula": namespace["simulate_from_copula"],
        "get_quantile_from_value": namespace["get_quantile_from_value"],
    }

    def reduce(finished):
        """The observed distance, and the test's result, position and simulated distances."""
        result, pvalue, observed, simulated = finished["compare_emp_cvm_criterion_to_dist_from_copula_sims"](
            8, fixtures["obs"], vine, 0.05
        )
        return xarray.Dataset(
            {
                "cvm_crit_obs": ((), float(observed)),
                "mc_result": ((), str(result)),
                "pvalue": ((), float(pvalue)),
                "s_cvm_crit_sim": ("simulation", simulated.to_numpy(dtype=float)),
            }
        )

    return bindings, reduce


@declares_lift(unit="_b5_fit_vine_copulas.py:97-119", fixtures=["obs", "vinecop_json", "seed"], draws="seeded")
def declare_goodness_of_fit_figures(fixtures):
    """Run the figure-drawing function on the invented observations and fitted copula; reduce to the figures it saves and what they drew."""
    import hashlib
    import inspect
    import pathlib
    import textwrap
    import tomllib
    import types

    import matplotlib
    import numpy
    import pandas
    import pyvinecopulib
    import seaborn
    from matplotlib import patches as mpatches
    from matplotlib.lines import Line2D
    from scipy import stats

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot
    import xarray

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
    tmp_prefix = ""
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

        def select(self, data=None, controls=None):
            """Select the structure and pair copulas for `data` in place."""
            self.fitted.select(numpy.asfortranarray(numpy.asarray(data, dtype=float)), controls)

        def to_json(self, path=None):
            """The copula as json text, written to `path` too when one is given."""
            text = self.fitted.to_json()
            if path is not None:
                pathlib.Path(path).write_text(text, encoding="utf-8")
            return text

    def vinecop(source=None, data=None, controls=None):
        """A copula fitted to `data` under `controls`, or loaded from the json file named by `source`."""
        if data is not None:
            return Vine(
                pyvinecopulib.Vinecop.from_data(numpy.asfortranarray(numpy.asarray(data, dtype=float)), controls)
            )
        return Vine(pyvinecopulib.Vinecop.from_file(str(source)))

    pv_binding = types.SimpleNamespace(FitControlsVinecop=pyvinecopulib.FitControlsVinecop, Vinecop=vinecop)

    saved = []  # one entry per figure the callees saved: the path they named and what the figure drew

    def figure_contents(figure):
        """What a figure drew that does not depend on a random draw: axis labels, dashed observed-statistic lines, titles of the first panel."""
        axes = figure.axes
        return {
            "xlabels": [ax.get_xlabel() for ax in axes],
            "ylabels": [ax.get_ylabel() for ax in axes],
            "observed": sorted(
                float(line.get_xdata()[0]) for ax in axes for line in ax.lines if line.get_linestyle() == "--"
            ),
            "titles": [ax.get_title() for ax in axes],
        }

    class Plt:
        """The `plt` the callees draw with: real figures drawn in memory, a record of each save, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure drew under the path named, and write no file."""
            saved.append(
                (str(path).replace(tmp_prefix, "<tmp>/") if tmp_prefix else str(path), figure_contents(pyplot.gcf()))
            )

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
    ]  # (file, first line, last line) of every text this lift runs, the unit's own included
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
        ("_utils", "421-434"): ("stored-text:1:4ef6e81b8733a313a33f666ed883055d16ca97779e8c7c13d1f63dfdb23def9d", 421),
        ("_utils", "597-604"): ("stored-text:1:efef30214c4a04554a2932afb505cf3fe056333ff4b223b46bd16ee7581d7394", 597),
        ("_utils", "605-660"): ("stored-text:1:f7cf4b0fef56d6956864824f37f042b5c47a53dea922996f83e8a55b2051e89b", 605),
        ("_utils", "661-717"): ("stored-text:1:7a6467a5addf894a7aa686d1890eb41a333984baa05b02227784168e53f320fe", 661),
        ("_utils", "718-746"): ("stored-text:1:303748bf356cb7c6339e2c0ec17bbbbd763b791fef286d81c2c660b3fa97ebbb", 718),
        ("_utils", "747-856"): ("stored-text:1:9c39c8cfd602394a12629dd249401f6bfee7d305688b358193bc5d54d06b1ddd", 747),
        ("_utils", "857-921"): ("stored-text:1:3e0cbd9c540ac11b9f87e7993da1f4146b429e3ae6d54c828e9bfd189dce1c8e", 857),
        ("_utils", "922-990"): ("stored-text:1:cec2082f2179b8214be6828a5a2b22b73d52c27a5e247bf7492853a5841a587a", 922),
    }
    namespace = {
        "np": numpy,
        "pd": pandas,
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

    def pack_figures(figure_records):
        """One variable group per saved figure: path, axis labels, dashed observed-statistic lines and the panel titles in three groups.

        The titles of the observed panels and of the simulated panels ("Simulated Data (n=...)") depend on nothing but the
        data and the seeded vine draws. The other titles state a test's result and rate, which the licensed stop markers
        change and the pandas draws of the bootstrap test decide, so they form a component of their own.
        """
        data = {}
        for i, (path, contents) in enumerate(figure_records):
            tag = f"fig{i:02d}"
            data[f"{tag}_path"] = ((), path)
            data[f"{tag}_xlabels"] = (f"{tag}_axis", numpy.asarray(contents["xlabels"], dtype=str))
            data[f"{tag}_ylabels"] = (f"{tag}_axis", numpy.asarray(contents["ylabels"], dtype=str))
            data[f"{tag}_observed"] = (f"{tag}_line", numpy.asarray(contents["observed"], dtype=float))
            titles = contents["titles"]
            groups = {
                "titles_observed": [t for t in titles if t.startswith("Observed Data")],
                "titles_simulated": [t for t in titles if t.startswith("Simulated Data")],
                "titles_tests": [t for t in titles if not t.startswith(("Observed Data", "Simulated Data"))],
            }
            for group, members in groups.items():
                data[f"{tag}_{group}"] = (f"{tag}_{group}_axis", numpy.asarray(members, dtype=str))
        return data

    vine = Vine(pyvinecopulib.Vinecop.from_json(str(fixtures["vinecop_json"][0])))
    bindings = {
        "compare_emp_cvm_criterion_to_dist_from_copula_sims": namespace[
            "compare_emp_cvm_criterion_to_dist_from_copula_sims"
        ],
        "simulate_from_copula": namespace["simulate_from_copula"],
        "plot_obs_vs_montecarlo_simulated_statstic": namespace["plot_obs_vs_montecarlo_simulated_statstic"],
        "evaluate_copula_fit_based_on_comparing_simulated_marginals_with_obervations": namespace[
            "evaluate_copula_fit_based_on_comparing_simulated_marginals_with_obervations"
        ],
        "evaluate_copula_fit_based_on_comparing_kendalltaus_for_each_variable_pair": namespace[
            "evaluate_copula_fit_based_on_comparing_kendalltaus_for_each_variable_pair"
        ],
    }

    def reduce(finished):
        """The saved figures, in order, and what each drew."""
        finished["create_vinecop_gof_plots"](
            df_obs=fixtures["obs"],
            cop_fitted=vine,
            n_for_density_plot=60,
            n_for_marginal_cdf_comparison=2,
            n_for_montecarlo_tau_test=6,
            n_for_montecarlo_cvm_test=6,
            alpha=0.05,
            fldr_plots="plots/",
            event_key="rain_events",
        )
        pyplot.close("all")
        return xarray.Dataset(pack_figures(saved))

    return bindings, reduce


@declares_lift(
    unit="_b5_fit_vine_copulas.py:297-328",
    fixtures=["obs", "vinecop_json", "fitted_cdf_long", "sensitivity_results", "seed"],
    draws="seeded",
)
def declare_final_copula_and_figures(fixtures):
    """Run the final-copula cell on the invented fitted copula and tables; reduce to the copy of the saved fit and the figures it saves."""
    import hashlib
    import inspect
    import pathlib
    import textwrap
    import tomllib
    import types

    import matplotlib
    import numpy
    import pandas
    import pyvinecopulib
    import seaborn
    from matplotlib import patches as mpatches
    from matplotlib.lines import Line2D
    from scipy import stats

    matplotlib.use("agg")
    import shutil
    import tempfile

    import matplotlib.pyplot as pyplot
    import xarray

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
    workdir = tempfile.mkdtemp(
        prefix="dependence-modelling-lift-"
    )  # the run's own scratch directory, removed by the reduction
    tmp_prefix = workdir + "/"

    class PdBinding:
        """pandas as the legacy cells name it, except that `read_csv` returns the fixture the named file stands for."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The fixture table for the csv the legacy code reads; any other file is an error."""
            if str(path).endswith("_empirical_vs_fitted.csv"):
                return fixtures["fitted_cdf_long"].copy()
            if str(path).endswith("copula_fit_sensitivity_analysis_results.csv"):
                table = fixtures["sensitivity_results"].copy()
                table["f_vinecop_params"] = saved_fit_path
                return table
            raise FileNotFoundError(f"the lift reads no csv named {path}")

    pd_binding = PdBinding()
    saved_fit_path = tmp_prefix + "selected_fit.json"
    pathlib.Path(saved_fit_path).write_text(str(fixtures["vinecop_json"][0]), encoding="utf-8")
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

        def select(self, data=None, controls=None):
            """Select the structure and pair copulas for `data` in place."""
            self.fitted.select(numpy.asfortranarray(numpy.asarray(data, dtype=float)), controls)

        def to_json(self, path=None):
            """The copula as json text, written to `path` too when one is given."""
            text = self.fitted.to_json()
            if path is not None:
                pathlib.Path(path).write_text(text, encoding="utf-8")
            return text

    def vinecop(source=None, data=None, controls=None):
        """A copula fitted to `data` under `controls`, or loaded from the json file named by `source`."""
        if data is not None:
            return Vine(
                pyvinecopulib.Vinecop.from_data(numpy.asfortranarray(numpy.asarray(data, dtype=float)), controls)
            )
        return Vine(pyvinecopulib.Vinecop.from_file(str(source)))

    pv_binding = types.SimpleNamespace(FitControlsVinecop=pyvinecopulib.FitControlsVinecop, Vinecop=vinecop)

    saved = []  # one entry per figure the callees saved: the path they named and what the figure drew

    def figure_contents(figure):
        """What a figure drew that does not depend on a random draw: axis labels, dashed observed-statistic lines, titles of the first panel."""
        axes = figure.axes
        return {
            "xlabels": [ax.get_xlabel() for ax in axes],
            "ylabels": [ax.get_ylabel() for ax in axes],
            "observed": sorted(
                float(line.get_xdata()[0]) for ax in axes for line in ax.lines if line.get_linestyle() == "--"
            ),
            "titles": [ax.get_title() for ax in axes],
        }

    class Plt:
        """The `plt` the callees draw with: real figures drawn in memory, a record of each save, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure drew under the path named, and write no file."""
            saved.append(
                (str(path).replace(tmp_prefix, "<tmp>/") if tmp_prefix else str(path), figure_contents(pyplot.gcf()))
            )

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
        ("_b5_fit_vine_copulas.py", 297, 328),
        ("_utils.py", 410, 420),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own included
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
        ("_utils", "421-434"): ("stored-text:1:4ef6e81b8733a313a33f666ed883055d16ca97779e8c7c13d1f63dfdb23def9d", 421),
        ("_utils", "597-604"): ("stored-text:1:efef30214c4a04554a2932afb505cf3fe056333ff4b223b46bd16ee7581d7394", 597),
        ("_utils", "605-660"): ("stored-text:1:f7cf4b0fef56d6956864824f37f042b5c47a53dea922996f83e8a55b2051e89b", 605),
        ("_utils", "661-717"): ("stored-text:1:7a6467a5addf894a7aa686d1890eb41a333984baa05b02227784168e53f320fe", 661),
        ("_utils", "718-746"): ("stored-text:1:303748bf356cb7c6339e2c0ec17bbbbd763b791fef286d81c2c660b3fa97ebbb", 718),
        ("_utils", "747-856"): ("stored-text:1:9c39c8cfd602394a12629dd249401f6bfee7d305688b358193bc5d54d06b1ddd", 747),
        ("_utils", "857-921"): ("stored-text:1:3e0cbd9c540ac11b9f87e7993da1f4146b429e3ae6d54c828e9bfd189dce1c8e", 857),
        ("_utils", "922-990"): ("stored-text:1:cec2082f2179b8214be6828a5a2b22b73d52c27a5e247bf7492853a5841a587a", 922),
        ("_b5_fit_vine_copulas", "97-119"): (
            "stored-text:1:1cee0038b11a5b982bcf6f10e6898be1513131fed3a5f8c17b3ae26ea978bd8d",
            97,
        ),
        ("_utils", "410-420"): ("stored-text:1:c0f0f53e259dcbcf9ef15a91817aea99299930c746892cafe6700f9a1424baff", 410),
    }
    namespace = {
        "np": numpy,
        "pd": pd_binding,
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

    def pack_figures(figure_records):
        """One variable group per saved figure: path, axis labels, dashed observed-statistic lines and the panel titles in three groups.

        The titles of the observed panels and of the simulated panels ("Simulated Data (n=...)") depend on nothing but the
        data and the seeded vine draws. The other titles state a test's result and rate, which the licensed stop markers
        change and the pandas draws of the bootstrap test decide, so they form a component of their own.
        """
        data = {}
        for i, (path, contents) in enumerate(figure_records):
            tag = f"fig{i:02d}"
            data[f"{tag}_path"] = ((), path)
            data[f"{tag}_xlabels"] = (f"{tag}_axis", numpy.asarray(contents["xlabels"], dtype=str))
            data[f"{tag}_ylabels"] = (f"{tag}_axis", numpy.asarray(contents["ylabels"], dtype=str))
            data[f"{tag}_observed"] = (f"{tag}_line", numpy.asarray(contents["observed"], dtype=float))
            titles = contents["titles"]
            groups = {
                "titles_observed": [t for t in titles if t.startswith("Observed Data")],
                "titles_simulated": [t for t in titles if t.startswith("Simulated Data")],
                "titles_tests": [t for t in titles if not t.startswith(("Observed Data", "Simulated Data"))],
            }
            for group, members in groups.items():
                data[f"{tag}_{group}"] = (f"{tag}_{group}_axis", numpy.asarray(members, dtype=str))
        return data

    bindings = {
        "format_fitted_cdf_values_for_fitting_copula": namespace["format_fitted_cdf_values_for_fitting_copula"],
        "create_vinecop_gof_plots": namespace["create_vinecop_gof_plots"],
    }
    bindings.update(
        {
            "pd": pd_binding,
            "pv": pv_binding,
            "shutil": shutil,
            "Path": pathlib.Path,
            "dic_event_summaries": {
                "surge_events": None,
                "rain_events": None,
                "compound_events": None,
                "other_events": None,
            },
            "dir_vinecop": tmp_prefix,
            "control_idx": 1,
        }
    )

    def reduce(finished):
        """The saved fit's copy (its text), the figures saved, and what they drew."""
        copied = pathlib.Path(tmp_prefix + "vinecop_params_other_events.json").read_text(encoding="utf-8")
        pyplot.close("all")
        data = pack_figures(saved)
        data["copied_fit_matches_saved_fit"] = ((), copied == str(fixtures["vinecop_json"][0]))
        shutil.rmtree(workdir, ignore_errors=True)
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_b5_fit_vine_copulas.py:128-240", fixtures=["fitted_cdf_long", "seed"], draws="seeded")
def declare_fit_control_sensitivity(fixtures):
    """Run the fit-sensitivity cell on the invented tables; reduce to the table of tested settings it writes and the figures it saves."""
    import hashlib
    import inspect
    import pathlib
    import textwrap
    import tomllib
    import types

    import matplotlib
    import numpy
    import pandas
    import pyvinecopulib
    import seaborn
    from matplotlib import patches as mpatches
    from matplotlib.lines import Line2D
    from scipy import stats

    matplotlib.use("agg")
    import shutil
    import tempfile

    import matplotlib.pyplot as pyplot
    import xarray

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
    workdir = tempfile.mkdtemp(
        prefix="dependence-modelling-lift-"
    )  # the run's own scratch directory, removed by the reduction
    tmp_prefix = workdir + "/"

    class PdBinding:
        """pandas as the legacy cells name it, except that `read_csv` returns the fixture the named file stands for."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The fixture table for the csv the legacy code reads; any other file is an error."""
            if str(path).endswith("_empirical_vs_fitted.csv"):
                return fixtures["fitted_cdf_long"].copy()
            raise FileNotFoundError(f"the lift reads no csv named {path}")

    pd_binding = PdBinding()
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

        def select(self, data=None, controls=None):
            """Select the structure and pair copulas for `data` in place."""
            self.fitted.select(numpy.asfortranarray(numpy.asarray(data, dtype=float)), controls)

        def to_json(self, path=None):
            """The copula as json text, written to `path` too when one is given."""
            text = self.fitted.to_json()
            if path is not None:
                pathlib.Path(path).write_text(text, encoding="utf-8")
            return text

    def vinecop(source=None, data=None, controls=None):
        """A copula fitted to `data` under `controls`, or loaded from the json file named by `source`."""
        if data is not None:
            return Vine(
                pyvinecopulib.Vinecop.from_data(numpy.asfortranarray(numpy.asarray(data, dtype=float)), controls)
            )
        return Vine(pyvinecopulib.Vinecop.from_file(str(source)))

    pv_binding = types.SimpleNamespace(FitControlsVinecop=pyvinecopulib.FitControlsVinecop, Vinecop=vinecop)

    saved = []  # one entry per figure the callees saved: the path they named and what the figure drew

    def figure_contents(figure):
        """What a figure drew that does not depend on a random draw: axis labels, dashed observed-statistic lines, titles of the first panel."""
        axes = figure.axes
        return {
            "xlabels": [ax.get_xlabel() for ax in axes],
            "ylabels": [ax.get_ylabel() for ax in axes],
            "observed": sorted(
                float(line.get_xdata()[0]) for ax in axes for line in ax.lines if line.get_linestyle() == "--"
            ),
            "titles": [ax.get_title() for ax in axes],
        }

    class Plt:
        """The `plt` the callees draw with: real figures drawn in memory, a record of each save, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure drew under the path named, and write no file."""
            saved.append(
                (str(path).replace(tmp_prefix, "<tmp>/") if tmp_prefix else str(path), figure_contents(pyplot.gcf()))
            )

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
        ("_b5_fit_vine_copulas.py", 128, 240),
        ("_b5_fit_vine_copulas.py", 58, 71),
        ("_b5_fit_vine_copulas.py", 72, 75),
        ("_b5_fit_vine_copulas.py", 76, 96),
        ("_b5_fit_vine_copulas.py", 120, 127),
        ("_utils.py", 410, 420),
        ("_utils.py", 421, 434),
        ("_utils.py", 597, 604),
        ("_utils.py", 605, 660),
        ("_utils.py", 661, 717),
        ("_utils.py", 718, 746),
        ("_utils.py", 747, 856),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own included
    stop_lines = [
        ("_b5_fit_vine_copulas.py", 78)
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
        ("_b5_fit_vine_copulas", "120-127"): (
            "stored-text:1:d6ec7761d6d47ba44e6fd879ffc0b5d03b723e3f0d79bf4df82afed629be00c0",
            120,
        ),
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
        "pd": pd_binding,
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

    def pack_figures(figure_records):
        """One variable group per saved figure: path, axis labels, dashed observed-statistic lines and the panel titles in three groups.

        The titles of the observed panels and of the simulated panels ("Simulated Data (n=...)") depend on nothing but the
        data and the seeded vine draws. The other titles state a test's result and rate, which the licensed stop markers
        change and the pandas draws of the bootstrap test decide, so they form a component of their own.
        """
        data = {}
        for i, (path, contents) in enumerate(figure_records):
            tag = f"fig{i:02d}"
            data[f"{tag}_path"] = ((), path)
            data[f"{tag}_xlabels"] = (f"{tag}_axis", numpy.asarray(contents["xlabels"], dtype=str))
            data[f"{tag}_ylabels"] = (f"{tag}_axis", numpy.asarray(contents["ylabels"], dtype=str))
            data[f"{tag}_observed"] = (f"{tag}_line", numpy.asarray(contents["observed"], dtype=float))
            titles = contents["titles"]
            groups = {
                "titles_observed": [t for t in titles if t.startswith("Observed Data")],
                "titles_simulated": [t for t in titles if t.startswith("Simulated Data")],
                "titles_tests": [t for t in titles if not t.startswith(("Observed Data", "Simulated Data"))],
            }
            for group, members in groups.items():
                data[f"{tag}_{group}"] = (f"{tag}_{group}_axis", numpy.asarray(members, dtype=str))
        return data

    bindings = {
        "format_fitted_cdf_values_for_fitting_copula": namespace["format_fitted_cdf_values_for_fitting_copula"],
        "compare_emp_cvm_criterion_to_dist_from_copula_sims": namespace[
            "compare_emp_cvm_criterion_to_dist_from_copula_sims"
        ],
        "compute_obs_cvm_stat": namespace["compute_obs_cvm_stat"],
        "evaluate_copula_fit_based_on_comparing_kendalltaus_for_each_variable_pair": namespace[
            "evaluate_copula_fit_based_on_comparing_kendalltaus_for_each_variable_pair"
        ],
        "plot_obs_vs_montecarlo_simulated_statstic": namespace["plot_obs_vs_montecarlo_simulated_statstic"],
    }
    bindings.update(
        {
            "pd": pd_binding,
            "np": numpy,
            "pv": pv_binding,
            "shutil": shutil,
            "Path": pathlib.Path,
            "dic_event_summaries": {
                "surge_events": None,
                "rain_events": None,
                "compound_events": None,
                "other_events": None,
            },
            "dir_vinecop": tmp_prefix,
        }
    )

    def reduce(finished):
        """The written table, ordered by setting, the figures saved."""
        table = pandas.read_csv(
            tmp_prefix + "plots_other_events_control_testing/copula_fit_sensitivity_analysis_results.csv",
            index_col="control_idx",
        ).sort_index()
        pyplot.close("all")
        data = {"control_idx": ("control", table.index.to_numpy())}
        for column in (
            "selection_criterion",
            "parametric_method",
            "tree_criterion",
            "select_trunc_lvl",
            "problems",
            "result",
        ):
            data[column] = ("control", table[column].astype(str).to_numpy())
        for column in (
            "trunc_lvl",
            "cvm_obs",
            "cvm_obs_before_exporting",
            "cvm_obs_after_loading_from_json",
            "nsims",
            "pvalue",
        ):
            data[column] = ("control", table[column].astype(float).to_numpy())
        figures = []
        for path, _contents in saved:
            name = path.rsplit("/", 1)[-1]
            index, rest = name[len("cntrl_idx") :].split("_", 1)
            figures.append((int(index), rest.split("_p", 1)[1].split("_", 1)[1]))
        figures.sort()
        data["figure_control_idx"] = ("figure", numpy.asarray([i for i, _ in figures], dtype=int))
        data["figure_settings"] = ("figure", numpy.asarray([s for _, s in figures], dtype=str))
        shutil.rmtree(workdir, ignore_errors=True)
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_b6_simulate_from_vincop.py:489-596",
    form="adapted",
    draws="unseeded",
    fixtures=[
        "b6_surge_events",
        "b6_rain_events",
        "b6_occurrences",
        "b6_cdf_long_surge",
        "b6_cdf_long_rain",
        "b6_cdf_long_compound",
        "b6_marginals_surge",
        "b6_marginals_rain",
        "b6_marginals_compound",
        "b6_vinecop_surge",
        "b6_vinecop_rain",
        "b6_vinecop_compound",
        "seed",
    ],
)
def declare_simulation_cell(fixtures):
    """Run the adapted simulation cell over the three event types; reduce to what it computes that no unseeded draw decides."""
    import hashlib
    import pathlib

    import numpy
    import pandas
    import pyvinecopulib
    import xarray
    from scipy import stats

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
    tables = {
        "surge_events_distributions.csv": fixtures["b6_marginals_surge"],
        "rain_events_distributions.csv": fixtures["b6_marginals_rain"],
        "compound_events_distributions.csv": fixtures["b6_marginals_compound"],
    }

    class PdBinding:
        """pandas as the legacy cell names it, except that `read_csv` returns the fixture table the named file stands for."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The fixture table for the csv the legacy code reads; any other file is an error."""
            name = str(path).rsplit("/", 1)[-1]
            if name.endswith("_empirical_vs_fitted.csv"):
                event_type = name[: -len("_empirical_vs_fitted.csv")].replace("_events", "")
                return fixtures[f"b6_cdf_long_{event_type}"].copy()
            if name in tables:
                return tables[name].copy()
            raise FileNotFoundError(f"the lift reads no csv named {path}")

    pd_binding = PdBinding()
    marginals_dir = "marginals/"
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
        ("_utils", "410-420"): ("stored-text:1:c0f0f53e259dcbcf9ef15a91817aea99299930c746892cafe6700f9a1424baff", 410),
        ("_utils", "421-434"): ("stored-text:1:4ef6e81b8733a313a33f666ed883055d16ca97779e8c7c13d1f63dfdb23def9d", 421),
    }
    namespace = {"np": numpy, "pd": pd_binding, "dir_fitting_maringals": marginals_dir}
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)
    event_types = ("surge_events", "rain_events", "compound_events")
    copula_json = {
        "surge_events": str(fixtures["b6_vinecop_surge"][0]),
        "rain_events": str(fixtures["b6_vinecop_rain"][0]),
        "compound_events": str(fixtures["b6_vinecop_compound"][0]),
    }
    bindings = {
        "df_surge_events": fixtures["b6_surge_events"],
        "df_rain_events": fixtures["b6_rain_events"],
        "df_occurences_sim": fixtures["b6_occurrences"],
        "n_years_to_simulate": len(fixtures["b6_occurrences"]),
        "dir_fitting_maringals": marginals_dir,
        "dic_copulas": {key: pyvinecopulib.Vinecop.from_json(copula_json[key]) for key in event_types},
        "dic_event_summaries": dict.fromkeys(event_types),
        "random_seed": int(fixtures["seed"]),  # the one seed: the legacy call names it, so every simulate call keeps it
        "pd": pd_binding,
        "np": numpy,
        "stats": stats,
        "simulate_from_copula": namespace["simulate_from_copula"],
        "format_fitted_cdf_values_for_fitting_copula": namespace["format_fitted_cdf_values_for_fitting_copula"],
        "backtransform_data": namespace["backtransform_data"],
    }

    def reduce(finished):
        """The thresholds and counts, the fraction invalid and the whole over-simulation of each event type, the returned tables' shape, and the four checks."""
        data = {
            "surge_event_threshold": ((), float(finished["surge_event_threshold"])),
            "rain_event_threshold": ((), float(finished["rain_event_threshold"])),
        }
        for name in ("n_surge_events", "n_rain_events", "n_combo_events", "n_total_events"):
            data[name] = ((), int(finished[name]))
        for key in event_types:
            data[f"frac_invalid_{key}"] = ((), float(finished["dic_frac_invalid"][key]))
            frame = finished["dic_full_over_sim"][key]
            dimension = f"over_sim_{key}"
            for column in frame.columns:
                if column in ("rain_event", "surge_event", "compound_event"):
                    data[f"{dimension}__{column}"] = (dimension, frame[column].to_numpy(dtype=bool))
                elif column == "copula_source":
                    data[f"{dimension}__{column}"] = (dimension, frame[column].to_numpy(dtype=str))
                else:
                    data[f"{dimension}__{column}"] = (dimension, frame[column].to_numpy(dtype=float))
        for key, returned in zip(event_types, finished["lst_df_simulated_realspace"], strict=True):
            data[f"n_returned_{key}"] = ((), int(len(returned)))
            data[f"returned_source_{key}"] = ((), str(returned["copula_source"].iloc[0]))
            data[f"returned_columns_{key}"] = (
                f"returned_columns_{key}_axis",
                numpy.asarray(list(returned.columns), dtype=str),
            )
        for name in ("chk1", "chk2", "chk3", "chk4"):
            data[name] = ((), bool(finished[name]))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_b6_simulate_from_vincop.py:489-596",
    form="adapted",
    part="draw-law",
    draws="unseeded",
    fixtures=[
        "b6_surge_events",
        "b6_rain_events",
        "b6_occurrences",
        "b6_cdf_long_surge",
        "b6_cdf_long_rain",
        "b6_cdf_long_compound",
        "b6_marginals_surge",
        "b6_marginals_rain",
        "b6_marginals_compound",
        "b6_vinecop_surge",
        "b6_vinecop_rain",
        "b6_vinecop_compound",
    ],
)
def declare_simulation_cell_sampling_law(fixtures):
    """Run the adapted simulation cell over the compound event type alone; reduce to the parameters of the law its sampling follows."""
    import hashlib
    import pathlib

    import numpy
    import pandas
    import pyvinecopulib
    from scipy import stats

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
    tables = {
        "surge_events_distributions.csv": fixtures["b6_marginals_surge"],
        "rain_events_distributions.csv": fixtures["b6_marginals_rain"],
        "compound_events_distributions.csv": fixtures["b6_marginals_compound"],
    }

    class PdBinding:
        """pandas as the legacy cell names it, except that `read_csv` returns the fixture table the named file stands for."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The fixture table for the csv the legacy code reads; any other file is an error."""
            name = str(path).rsplit("/", 1)[-1]
            if name.endswith("_empirical_vs_fitted.csv"):
                event_type = name[: -len("_empirical_vs_fitted.csv")].replace("_events", "")
                return fixtures[f"b6_cdf_long_{event_type}"].copy()
            if name in tables:
                return tables[name].copy()
            raise FileNotFoundError(f"the lift reads no csv named {path}")

    pd_binding = PdBinding()
    marginals_dir = "marginals/"
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
        ("_utils", "410-420"): ("stored-text:1:c0f0f53e259dcbcf9ef15a91817aea99299930c746892cafe6700f9a1424baff", 410),
        ("_utils", "421-434"): ("stored-text:1:4ef6e81b8733a313a33f666ed883055d16ca97779e8c7c13d1f63dfdb23def9d", 421),
    }
    namespace = {"np": numpy, "pd": pd_binding, "dir_fitting_maringals": marginals_dir}
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)
    event_types = ("compound_events",)
    copula_json = {
        "surge_events": str(fixtures["b6_vinecop_surge"][0]),
        "rain_events": str(fixtures["b6_vinecop_rain"][0]),
        "compound_events": str(fixtures["b6_vinecop_compound"][0]),
    }
    bindings = {
        "df_surge_events": fixtures["b6_surge_events"],
        "df_rain_events": fixtures["b6_rain_events"],
        "df_occurences_sim": fixtures["b6_occurrences"],
        "n_years_to_simulate": len(fixtures["b6_occurrences"]),
        "dir_fitting_maringals": marginals_dir,
        "dic_copulas": {key: pyvinecopulib.Vinecop.from_json(copula_json[key]) for key in event_types},
        "dic_event_summaries": dict.fromkeys(event_types),
        # the legacy script's own constant (its line 22): the pool is twice the count for any seed, so this lift needs no test seed
        "random_seed": 24,
        "pd": pd_binding,
        "np": numpy,
        "stats": stats,
        "simulate_from_copula": namespace["simulate_from_copula"],
        "format_fitted_cdf_values_for_fitting_copula": namespace["format_fitted_cdf_values_for_fitting_copula"],
        "backtransform_data": namespace["backtransform_data"],
    }

    def reduce(finished):
        """The parameters of the law the compound sampling follows: the rows it draws from, after the filter, and the rows it draws."""
        return pandas.Series(
            {"pool": len(finished["df_realspace_valid"]), "k": int(finished["dic_n_to_simulate"]["compound_events"])}
        )

    return bindings, reduce


@declares_lift(unit="_utils.py:421-434", draws="seeded", fixtures=["obs", "vinecop_json", "seed"])
def declare_simulate_from_copula(fixtures):
    """Run the copula-sampling function on the invented copula with no seeds, a list of seeds and one seed; reduce to the three tables."""
    import numpy
    import pandas
    import pyvinecopulib
    import xarray

    seed = int(
        fixtures["seed"]
    )  # the one seed, a committed fixture; the new side receives the same value as inputs["seed"]
    seed_stream = numpy.random.default_rng(
        seed
    )  # one seed per simulate call whose legacy call names none, in call order

    class Vine:
        """A fitted vine copula as the legacy call uses it, over the installed library's array interface."""

        def __init__(self, fitted):
            """Hold the library's own fitted copula."""
            self.fitted = fitted

        def simulate(self, n, seeds=None):
            """`n` draws as an array; the seeds the legacy call names, else the next seed of the stream built from the fixture's seed."""
            if seeds is None:
                seeds = [int(seed_stream.integers(1, 2**31 - 1))]
            return self.fitted.simulate(n=n, seeds=list(seeds))

    vine = Vine(pyvinecopulib.Vinecop.from_json(str(fixtures["vinecop_json"][0])))
    obs = fixtures["obs"]

    def reduce(finished):
        """The unseeded call twice (two different seeds of the stream), a call naming a list of seeds, and one naming a single seed."""
        calls = {
            "unseeded_first": finished["simulate_from_copula"](df_obs=obs, n=12, cop_fitted=vine),
            "unseeded_second": finished["simulate_from_copula"](df_obs=obs, n=12, cop_fitted=vine),
            "seed_list": finished["simulate_from_copula"](df_obs=obs, n=12, cop_fitted=vine, seeds=[7, 11]),
            "seed_scalar": finished["simulate_from_copula"](df_obs=obs, n=12, cop_fitted=vine, seeds=7),
        }
        data = {}
        for name, frame in calls.items():
            data[f"{name}_columns"] = (f"{name}_axis", numpy.asarray(list(frame.columns), dtype=str))
            for column in frame.columns:
                data[f"{name}__{column}"] = (name, frame[column].to_numpy(dtype=float))
        return xarray.Dataset(data)

    return {"pd": pandas}, reduce


@declares_lift(unit="_utils.py:421-434", part="draw-law", draws="none", fixtures=["obs", "vinecop_json"])
def declare_simulate_from_copula_law(fixtures):
    """Run the copula-sampling function's definition; reduce to the parameters of the law its draws follow: the copula's json text and the columns."""
    import pandas

    obs = fixtures["obs"]
    copula_json = str(fixtures["vinecop_json"][0])

    def reduce(finished):
        """The vine copula's json text and the column names the function gives the sample; the function itself draws nothing here."""
        return pandas.Series({"vine_json": copula_json, "columns": list(obs.columns)})

    return {"pd": pandas}, reduce
