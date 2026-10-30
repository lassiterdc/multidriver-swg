"""Lift declarations of the legacy marginal-fitting helpers owned by marginal-distribution-fitting.

A declaration supplies the names a stored legacy text reads and reduces the finished namespace to the value a test
compares; it imports no module of this checkout, and any helper it needs sits inside it, where the lift's digest
covers it. This file is written by a generator, so each declaration repeats the kit it needs. Every unit here is a
function of its arguments and draws nothing: the helpers transform a series, and the fitting function optimises
deterministically. A function that calls another legacy function by name runs that function's own stored text, bound
by the declaration after its stored-text label is checked.
"""

from tests.correspondence.lift import declares_lift


@declares_lift(unit="__ref_ams_functions.py:23-30", fixtures=["series_positive", "series_mixed"], draws="none")
def declare_boxcox_transformation(fixtures):
    """Run the Box-Cox helper on a positive series and on one with non-positive values; reduce to what it returns and to the series it shifts in place."""
    import numpy
    import xarray
    from scipy import stats

    def reduce(finished):
        """The transformed values, the power and the shift for each series, and each series after the call (the helper adds the shift to it in place)."""
        data = {}
        for case in ("positive", "mixed"):
            series = fixtures[f"series_{case}"].copy()
            transformed, lmbda, shift = finished["boxcox_transformation"](series)
            data[f"{case}_transformed"] = (case, numpy.asarray(transformed, dtype=float))
            data[f"{case}_lambda"] = ((), float(lmbda))
            data[f"{case}_shift"] = ((), float(shift))
            data[f"{case}_series_after"] = (f"{case}_after", numpy.asarray(series, dtype=float))
        return xarray.Dataset(data)

    return {"stats": stats}, reduce


@declares_lift(unit="__ref_ams_functions.py:31-38", fixtures=["series_positive"], draws="none")
def declare_inverse_boxcox(fixtures):
    """Run the inverse Box-Cox helper on a series with a zero power and with a non-zero power; reduce to the two results."""
    import numpy
    import xarray

    values = fixtures["series_positive"].copy() / 10.0

    def reduce(finished):
        """The values returned for a zero power and for a non-zero power, each with a shift."""
        zero = finished["inverse_boxcox"](values, 0, 0.25)
        nonzero = finished["inverse_boxcox"](values, 0.5, 0.25)
        return xarray.Dataset(
            {
                "zero_power": ("value", numpy.asarray(zero, dtype=float)),
                "nonzero_power": ("value", numpy.asarray(nonzero, dtype=float)),
            }
        )

    return {"np": numpy}, reduce


@declares_lift(unit="__ref_ams_functions.py:48-51", fixtures=["series_positive"], draws="none")
def declare_shift_data(fixtures):
    """Run the helper `shift_data` on a series with a non-zero and a zero shift; reduce to the two results."""
    import numpy
    import xarray

    values = fixtures["series_positive"].copy()

    def reduce(finished):
        """The series for a shift of 2.5 and for a shift of zero."""
        shifted = finished["shift_data"](values, 2.5)
        unchanged = finished["shift_data"](values, 0)
        return xarray.Dataset(
            {
                "shift_2_5": ("value", numpy.asarray(shifted, dtype=float)),
                "shift_0": ("value", numpy.asarray(unchanged, dtype=float)),
            }
        )

    return {}, reduce


@declares_lift(unit="__ref_ams_functions.py:52-55", fixtures=["series_positive"], draws="none")
def declare_unshift_data(fixtures):
    """Run the helper `unshift_data` on a series with a non-zero and a zero shift; reduce to the two results."""
    import numpy
    import xarray

    values = fixtures["series_positive"].copy()

    def reduce(finished):
        """The series for a shift of 2.5 and for a shift of zero."""
        shifted = finished["unshift_data"](values, 2.5)
        unchanged = finished["unshift_data"](values, 0)
        return xarray.Dataset(
            {
                "shift_2_5": ("value", numpy.asarray(shifted, dtype=float)),
                "shift_0": ("value", numpy.asarray(unchanged, dtype=float)),
            }
        )

    return {}, reduce


@declares_lift(unit="__ref_ams_functions.py:56-70", fixtures=["series_positive"], draws="none")
def declare_transform_given_transformations(fixtures):
    """Run the helper that applies a fit's recorded transformations on a series, for a fit with an upper bound, a shift, a rescaling and a Box-Cox power and for one with a shift only; reduce to the two results."""
    import hashlib
    import pathlib

    import numpy
    import pandas
    import xarray
    from scipy import stats

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
    callee_labels = {
        ("__ref_ams_functions", "48-51"): (
            "stored-text:1:70049577e51fc9dc45e73f05b2998a6045353ac250849c58c3f8e7ceceb31a4d",
            48,
        ),
    }
    namespace = {"np": numpy, "pd": pandas, "stats": stats}
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    shift_data = namespace["shift_data"]
    values = fixtures["series_positive"].copy()
    names = ["upper_bound", "scalar_shift", "normalize", "mean", "std", "boxcox", "lambda", "scalar_shift_for_boxcox"]
    nan = float("nan")
    full = pandas.Series([80.0, 1.5, True, 20.0, 12.0, True, 0.3, 0.2], index=names, dtype=object)
    shift_only = pandas.Series([nan, 1.5, False, nan, nan, False, nan, nan], index=names, dtype=object)

    def reduce(finished):
        """The values transformed under each fit's recorded transformations."""
        return xarray.Dataset(
            {
                "full": (
                    "value",
                    numpy.asarray(finished["transform_data_given_transformations"](values, full), dtype=float),
                ),
                "shift_only": (
                    "value",
                    numpy.asarray(finished["transform_data_given_transformations"](values, shift_only), dtype=float),
                ),
            }
        )

    return {"np": numpy, "shift_data": shift_data, "stats": stats}, reduce


@declares_lift(unit="__ref_ams_functions.py:71-88", fixtures=["series_positive", "series_mixed"], draws="none")
def declare_transform_data_for_fitting(fixtures):
    """Run the helper that transforms a series before fitting, with no transformation, with an upper bound, a shift and a rescaling, and with a Box-Cox power on a series with non-positive values; reduce to what it returns each time."""
    import hashlib
    import pathlib

    import numpy
    import pandas
    import xarray
    from scipy import stats

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
    callee_labels = {
        ("__ref_ams_functions", "23-30"): (
            "stored-text:1:861c7216b43ad1b88707d69279558cd3115d5120b67182a1a1488a2149e47460",
            23,
        ),
        ("__ref_ams_functions", "39-43"): (
            "stored-text:1:fcbc03806cb3ee0268fb3d4a79d2492f6a3a36052e0828d298cbf889a9c1f56c",
            39,
        ),
        ("__ref_ams_functions", "48-51"): (
            "stored-text:1:70049577e51fc9dc45e73f05b2998a6045353ac250849c58c3f8e7ceceb31a4d",
            48,
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
        "pd": pandas,
        "np": numpy,
        "boxcox_transformation": namespace["boxcox_transformation"],
        "normalize_data": namespace["normalize_data"],
        "shift_data": namespace["shift_data"],
    }
    cases = {
        "plain": ("series_positive", {}),
        "bounded": ("series_positive", {"upper_bound": 80.0, "scalar_shift": 1.5, "normalize": True}),
        "boxcox_mixed": ("series_mixed", {"boxcox": True}),
    }

    def reduce(finished):
        """For each case the transformed series, the Box-Cox shift and power, and the mean and deviation of the rescaling."""
        data = {}
        for case, (name, arguments) in cases.items():
            series, shift, lmbda, mean, std = finished["transform_data_for_fitting"](fixtures[name].copy(), **arguments)
            data[f"{case}_series"] = (case, numpy.asarray(series, dtype=float))
            data[f"{case}_scalar_shift_for_boxcox"] = ((), float(shift))
            data[f"{case}_lambda"] = ((), float(lmbda))
            data[f"{case}_mean"] = ((), float(mean))
            data[f"{case}_std"] = ((), float(std))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="__ref_ams_functions.py:89-104", fixtures=["series_positive"], draws="none")
def declare_backtransform_data(fixtures):
    """Run the helper that reverses a fit's transformations, for a fit with every transformation and for one with a shift only; reduce to the two results."""
    import hashlib
    import pathlib

    import numpy
    import pandas
    import xarray
    from scipy import stats

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
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
    }
    namespace = {"np": numpy, "pd": pandas, "stats": stats}
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    bindings = {
        "pd": pandas,
        "np": numpy,
        "inverse_boxcox": namespace["inverse_boxcox"],
        "inverse_normalize": namespace["inverse_normalize"],
        "unshift_data": namespace["unshift_data"],
    }
    values = fixtures["series_positive"].copy() / 10.0
    names = ["boxcox", "lambda", "scalar_shift_for_boxcox", "normalize", "mean", "std", "scalar_shift", "upper_bound"]
    nan = float("nan")
    full = pandas.Series([True, 0.4, 0.2, True, 0.1, 1.2, 1.0, 70.0], index=names, dtype=object)
    shift_only = pandas.Series([False, nan, nan, False, nan, nan, 0.5, nan], index=names, dtype=object)

    def reduce(finished):
        """The values returned to the units of the event statistic under each fit."""
        return xarray.Dataset(
            {
                "full": ("value", numpy.asarray(finished["backtransform_data"](values, full), dtype=float)),
                "shift_only": ("value", numpy.asarray(finished["backtransform_data"](values, shift_only), dtype=float)),
            }
        )

    return bindings, reduce


@declares_lift(unit="__ref_ams_functions.py:105-107", fixtures=["series_positive"], draws="none")
def declare_mean_squared_relative_difference(fixtures):
    """Run the goodness-of-fit score `comp_msdi` on a series and a perturbed copy of it; reduce to the score."""
    import xarray

    empirical = fixtures["series_positive"].copy()
    fitted = empirical * 1.05 - 0.3

    def reduce(finished):
        """The score of the perturbed series against the series, and of the series against itself."""
        return xarray.Dataset(
            {
                "perturbed": ((), float(finished["comp_msdi"](empirical, fitted))),
                "identical": ((), float(finished["comp_msdi"](empirical, empirical))),
            }
        )

    return {}, reduce


@declares_lift(unit="__ref_ams_functions.py:108-110", fixtures=["series_positive"], draws="none")
def declare_mean_absolute_relative_difference(fixtures):
    """Run the goodness-of-fit score `comp_madi` on a series and a perturbed copy of it; reduce to the score."""
    import xarray

    empirical = fixtures["series_positive"].copy()
    fitted = empirical * 1.05 - 0.3

    def reduce(finished):
        """The score of the perturbed series against the series, and of the series against itself."""
        return xarray.Dataset(
            {
                "perturbed": ((), float(finished["comp_madi"](empirical, fitted))),
                "identical": ((), float(finished["comp_madi"](empirical, empirical))),
            }
        )

    return {}, reduce


@declares_lift(unit="__ref_ams_functions.py:111-214", fixtures=["series_positive"], draws="none")
def declare_fit_dist(fixtures):
    """Run the distribution-fitting function for several candidate distributions and transformation sets, including fits it rejects; reduce to the fit record and the table of empirical and fitted values of each."""
    import hashlib
    import pathlib

    import numpy
    import pandas
    import scipy.stats
    import xarray
    from scipy import stats

    class Refuse:
        """The plotting names the figure function reads: it is bound, never called here, and refuses any use."""

        def __getattr__(self, name):
            """Refuse every attribute: a plotting call this lift does not model must not pass unseen."""
            raise AttributeError(f"the lift's plotting stand-in does not model {name}")

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
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
        ("__ref_ams_functions", "215-324"): (
            "stored-text:1:174610d747c5455adc3ef4015275572301278a5e28aa4ab1b46de5c89010ab38",
            215,
        ),
    }
    namespace = {"np": numpy, "pd": pandas, "stats": stats, "plt": Refuse(), "sns": Refuse()}
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    bindings = {
        "pd": pandas,
        "np": numpy,
        "scipy": scipy,
        "stats": stats,
        "backtransform_data": namespace["backtransform_data"],
        "comp_madi": namespace["comp_madi"],
        "comp_msdi": namespace["comp_msdi"],
        "plot_fitted_distribution": namespace["plot_fitted_distribution"],
        "transform_data_for_fitting": namespace["transform_data_for_fitting"],
    }
    nan = float("nan")
    series = fixtures["series_positive"].copy()
    top = float(series.max()) + 20.0
    cases = {
        "norm": (stats.norm, 2, nan, False, 0, False, False),
        "gamma": (stats.gamma, 3, nan, False, 0, False, False),
        "genextreme_fixed_loc": (stats.genextreme, 3, nan, True, 0, False, False),
        "weibull_fixed_loc": (stats.weibull_min, 3, nan, True, 0, False, False),
        "lognorm_all": (stats.lognorm, 3, nan, False, 2.0, True, True),
        "gamma_bounded": (stats.gamma, 3, top, False, 0, False, False),
        "norm_bounded": (stats.norm, 2, top, False, 0, False, False),
    }

    def reduce(finished):
        """Per case: whether the function rejected the fit, its record (text, counts, scores, parameters) and the table of values."""
        data = {}
        for case, (fx, n_params, upper_bound, fix_loc, shift, normalize, boxcox) in cases.items():
            s_fit, table = finished["fit_dist"](
                series.copy(), fx, n_params, upper_bound, fix_loc, shift, normalize, boxcox
            )
            data[f"{case}_rejected"] = ((), s_fit is None)
            if s_fit is None:
                continue
            for key in ("data", "distribution"):
                data[f"{case}_{key}"] = ((), str(s_fit[key]))
            for key in ("fix_loc_to_min", "normalize", "boxcox", "any_transformation"):
                data[f"{case}_{key}"] = ((), bool(s_fit[key]))
            for key in (
                "n_params",
                "shape",
                "loc",
                "scale",
                "cvm_pval",
                "ks_pval",
                "aic",
                "msdi",
                "madi",
                "scalar_shift",
                "upper_bound",
                "scalar_shift_for_boxcox",
                "lambda",
                "mean",
                "std",
                "support_lb",
                "support_ub",
            ):
                data[f"{case}_{key}"] = ((), float(s_fit[key]))
            for column in table.columns:
                data[f"{case}_table_{column}"] = (f"{case}_row", numpy.asarray(table[column], dtype=float))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="__ref_ams_functions.py:325-387", fixtures=[], draws="none")
def declare_candidate_distributions(fixtures):
    """Run the cell that defines the candidate distributions; reduce to each candidate's name and parameter count, in order."""
    import numpy
    import xarray
    from scipy import stats

    names = {
        "genextreme": stats.genextreme,
        "weibull_min": stats.weibull_min,
        "weibull_max": stats.weibull_max,
        "gumbel_r": stats.gumbel_r,
        "gumbel_l": stats.gumbel_l,
        "norm": stats.norm,
        "lognorm": stats.lognorm,
        "t": stats.t,
        "genpareto": stats.genpareto,
        "chi": stats.chi,
        "gamma": stats.gamma,
        "pearson3": stats.pearson3,
        "exponpow": stats.exponpow,
    }

    def reduce(finished):
        """The name and parameter count of each candidate, in the order the cell lists them."""
        candidates = finished["fxs"]
        return xarray.Dataset(
            {
                "distribution": ("candidate", numpy.asarray([c["args"]["fx"].name for c in candidates], dtype=str)),
                "n_params": ("candidate", numpy.asarray([c["args"]["n_params"] for c in candidates], dtype=int)),
            }
        )

    return names, reduce


@declares_lift(
    unit="_b4_selecting_marginals.py:23-65",
    fixtures=["msel_combined_events", "msel_stats_compound", "msel_stats_rain", "msel_stats_surge", "msel_stats_other"],
    draws="none",
)
def declare_event_statistics_loading_cell(fixtures):
    """Run the cell that loads the combined event summaries and the statistics each event type kept; reduce to what it leaves.

    The cell reads one csv of event summaries and, for every csv in a folder, the pairs of statistics kept; it names
    the event type from the file name (a file it does not recognize is reported), and asks for one output folder.
    """
    import numpy
    import pandas
    import xarray

    folders = []  # one entry per folder the cell asked for
    printed = []  # the lines the cell prints

    class PdBinding:
        """pandas as the legacy cell names it, except that `read_csv` returns the fixture table the named file stands for."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The fixture table for the csv the legacy code reads; any other file is an error."""
            tables = {
                "combined_event_summaries.csv": "msel_combined_events",
                "stats/compound_events_pairs.csv": "msel_stats_compound",
                "stats/rain_event_pairs.csv": "msel_stats_rain",
                "stats/surge_event_pairs.csv": "msel_stats_surge",
                "stats/other_pairs.csv": "msel_stats_other",
            }
            if str(path) not in tables:
                raise FileNotFoundError(f"the lift reads no csv named {path}")
            return fixtures[tables[str(path)]].copy()

    def glob(pattern):
        """The files of the folder, in the order the lift fixes (the legacy call asks the file system)."""
        if pattern != "stats/*.csv":
            raise FileNotFoundError(f"the lift lists no folder matching {pattern}")
        return [
            "stats/rain_event_pairs.csv",
            "stats/other_pairs.csv",
            "stats/compound_events_pairs.csv",
            "stats/surge_event_pairs.csv",
        ]

    class PathBinding:
        """`Path` as the legacy code names it: a call returns a handle whose `mkdir` is recorded and creates nothing."""

        def __call__(self, path):
            """A handle for `path`."""

            class Handle:
                """A path whose `mkdir` records the request."""

                def mkdir(self, *args, **kwargs):
                    """Record the folder asked for."""
                    folders.append(f"{path}|{sorted(kwargs.items())}")

            return Handle()

    def record(*arguments, **keywords):
        """The `print` the cell reads: keep the line and print nothing."""
        printed.append(" ".join(str(argument) for argument in arguments))

    bindings = {
        "Path": PathBinding(),
        "dir_fitting_maringals": "marginals/",
        "dir_selecting_maringals": "stats/",
        "f_combined_event_summaries": "combined_event_summaries.csv",
        "glob": glob,
        "pd": PdBinding(),
        "print": record,
        "threshold_varname_rain_events": "precip_depth_mm",
        "threshold_varname_surge_events": "surge_peak_ft",
    }

    def reduce(finished):
        """The event tables by type, the thresholds, the statistics each type kept, the folders asked for and the lines printed."""
        data = {
            "surge_threshold_ft": ((), float(finished["surge_threshold_ft"])),
            "rain_threshold_mm": ((), float(finished["rain_threshold_mm"])),
            "folders": ("folder", numpy.asarray(folders, dtype=str)),
            "printed": ("line", numpy.asarray(printed, dtype=str)),
        }
        for key, table in finished["dic_event_summaries"].items():
            for column in table.columns:
                values = table[column]
                if values.dtype.kind == "M":
                    data[f"{key}__{column}"] = (f"{key}__row", values.to_numpy())
                elif values.dtype.kind in "fiu":
                    data[f"{key}__{column}"] = (f"{key}__row", values.to_numpy(dtype=float))
                else:
                    data[f"{key}__{column}"] = (f"{key}__row", values.astype(str).to_numpy())
        for key, statistics in finished["dic_event_stats"].items():
            data[f"stats__{key}"] = (f"stats__{key}__item", numpy.asarray(statistics, dtype=str))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_b4_selecting_marginals.py:66-181",
    fixtures=["msel_surge_events", "msel_rain_events", "msel_compound_events"],
    draws="none",
)
def declare_all_marginal_fits_cell(fixtures):
    """Run the cell that fits every candidate distribution under every transformation to two statistics of each event type; reduce to the tables of fits and of failed fits it writes.

    The cell's first statement is an unconditional stop that the register rules `OBLIGED`: the run behind the kept
    outputs passed it, so the `sys` recorder returns at that line (and raises at any other). The fits that raise inside
    `fit_dist` (a gamma or lognormal fit with its location fixed at the minimum, whose data sit on the open bound) are
    caught by the cell and recorded in its failed-fits table with the error text; the text is packed apart from the
    kind of problem, so that a test can compare the kind and leave the library's wording out.
    """
    import hashlib
    import inspect
    import pathlib
    import shutil
    import tempfile
    import tomllib

    import numpy
    import pandas
    import scipy.stats
    import xarray
    from scipy import stats

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
    workdir = tempfile.mkdtemp(
        prefix="marginal-fitting-lift-"
    )  # the run's own scratch directory, removed by the reduction
    plots_dir = workdir + "/"

    class Refuse:
        """The plotting names the figure function reads: it is bound, never called here (no fit asks for a plot), and refuses any use."""

        def __getattr__(self, name):
            """Refuse every attribute: a plotting call this lift does not model must not pass unseen."""
            raise AttributeError(f"the lift's plotting stand-in does not model {name}")

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
        ("_b4_selecting_marginals.py", 66, 181),
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
        ("__ref_ams_functions.py", 325, 387),
    ]  # (file, first line, last line) of every text this lift runs
    stop_lines = [
        ("_b4_selecting_marginals.py", 67)
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
        ("__ref_ams_functions", "325-387"): (
            "stored-text:1:62de10e8b8f60dcc5208d5944d4c3eddceb88b5c2c54dee3eee2ef20c093a171",
            325,
        ),
    }
    namespace = {"np": numpy, "pd": pandas, "stats": stats, "scipy": scipy, "plt": Refuse(), "sns": Refuse()}
    for candidate in (
        "genextreme",
        "weibull_min",
        "weibull_max",
        "gumbel_r",
        "gumbel_l",
        "norm",
        "lognorm",
        "t",
        "genpareto",
        "chi",
        "gamma",
        "pearson3",
        "exponpow",
    ):  # the distributions the candidate cell (lines 325-387) names, as the legacy script's star-import supplied them
        namespace[candidate] = getattr(stats, candidate)
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    names = ["gev", "weibull_min_dist", "weibull_max_dist", "gumbel_right", "gumbel_left", "norm_dist", "lognormal"]
    names += ["student_t", "genpareto_dist", "chi_dist", "gamma_dist", "p3", "sep"]
    bindings = {name: namespace[name] for name in names}
    bindings.update(
        {
            "fit_dist": namespace["fit_dist"],
            "np": numpy,
            "pd": pandas,
            "sys": Sys(),
            "tqdm": lambda items: items,  # the progress bar the cell wraps its loop in
            "dic_event_summaries": {
                "compound_events": fixtures["msel_compound_events"].copy(),
                "rain_events": fixtures["msel_rain_events"].copy(),
                "surge_events": fixtures["msel_surge_events"].copy(),
            },
            "dic_event_stats": {
                "compound_events": ["precip_depth_mm", "surge_peak_ft"],
                "rain_events": ["precip_depth_mm", "surge_peak_ft"],
                "surge_events": ["precip_depth_mm", "surge_peak_ft"],
            },
            "dir_plots_all_fitted_marginals": plots_dir,
            "rain_threshold_mm": 12.0,
            "surge_threshold_ft": 2.0,
            "threshold_varname_rain_events": "precip_depth_mm",
            "threshold_varname_surge_events": "surge_peak_ft",
        }
    )
    kinds = {"": "none", "return None conditional triggered during distribution fitting": "returned none"}

    def pack(data, name, table, drop=()):
        """Add a table to `data` as one variable per column on its own row dimension: numbers and flags as they are, text as text."""
        dimension = f"{name}__row"
        for column in table.columns:
            if column in drop:
                continue
            values = table[column]
            if values.dtype == bool:
                data[f"{name}__{column}"] = (dimension, values.to_numpy(dtype=bool))
            elif values.dtype.kind in "fiu":
                data[f"{name}__{column}"] = (dimension, values.to_numpy(dtype=float))
            else:
                data[f"{name}__{column}"] = (dimension, values.astype(str).to_numpy())

    def reduce(finished):
        """For each event type, the table of successful fits and the table of failed fits (with the problem's kind and its text apart)."""
        data = {}
        for key in ("compound_events", "rain_events", "surge_events"):
            performance = pandas.read_csv(f"{plots_dir}{key}.csv")
            pack(data, f"fits_{key}", performance)
            failed = pandas.read_csv(f"{plots_dir}{key}_failed.csv")
            failed["problem_kind"] = [kinds.get(p, "error") for p in failed["problem"].fillna("")]
            failed["problem_text"] = failed["problem"].fillna("")
            pack(data, f"failed_{key}", failed.drop(columns=["problem"]))
        shutil.rmtree(workdir, ignore_errors=True)
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_b4_selecting_marginals.py:268-316",
    fixtures=[
        "msel_surge_events",
        "msel_rain_events",
        "msel_compound_events",
        "msel_perf_surge_events",
        "msel_perf_rain_events",
        "msel_perf_compound_events",
    ],
    draws="none",
)
def declare_final_marginal_selection_cell(fixtures):
    """Run the cell that plots and writes the chosen marginal fit of each statistic for each event type; reduce to the fit and empirical-versus-fitted tables it writes and the figures it saves.

    For each event type the cell names four fits by file name; `final_selection_plot_and_write_csv` (bound with
    `plot_subset_of_fits`, `fit_dist` and the transformation helpers to their stored texts) reads the type's table of fit
    performance, finds the rows whose generated name matches, refits them with a plot, and writes two tables. The lift
    binds the performance tables to committed fixtures built so that every named fit is present.
    """
    import hashlib
    import pathlib
    import shutil
    import tempfile

    import matplotlib
    import numpy
    import pandas
    import scipy.stats
    import seaborn
    import xarray
    from scipy import stats

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
    workdir = tempfile.mkdtemp(
        prefix="marginal-fitting-lift-"
    )  # the run's own scratch directory, removed by the reduction
    fitting_dir = workdir + "/"
    saved = []  # one entry per figure the callees saved: the path, the save's keywords and what the figure holds
    folders = []  # one entry per folder the callees asked for
    printed = []  # the lines the callees print

    def figure_record(figure):
        """What a figure holds: its super title and, for each axes, its title, labels, limits, bars and lines."""
        axes_records = []
        for axes in figure.axes:
            bars = [b for b in axes.patches if isinstance(b, matplotlib.patches.Rectangle)]
            axes_records.append(
                {
                    "title": axes.get_title(),
                    "labels": [axes.get_xlabel(), axes.get_ylabel()],
                    "lims": [float(v) for v in axes.get_xlim()] + [float(v) for v in axes.get_ylim()],
                    "bars": [v for b in bars for v in (float(b.get_x()), float(b.get_width()), float(b.get_height()))],
                    "line_meta": [
                        f"{line.get_label()}|{line.get_linestyle()}|{len(line.get_xdata())}" for line in axes.lines
                    ],
                    "line_data": [
                        float(v)
                        for line in axes.lines
                        for v in list(numpy.asarray(line.get_xdata(), dtype=float))
                        + list(numpy.asarray(line.get_ydata(), dtype=float))
                    ],
                    "legend": []
                    if axes.get_legend() is None
                    else [t.get_text() for t in axes.get_legend().get_texts()],
                }
            )
        return {"suptitle": figure._suptitle.get_text() if figure._suptitle is not None else "", "axes": axes_records}

    class Plt:
        """The `plt` the callees draw with: real figures in memory, a record of each save, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, with the save's keyword arguments; write no file."""
            saved.append(
                (str(path).replace(fitting_dir, "<tmp>/"), repr(sorted(kwargs.items())), figure_record(pyplot.gcf()))
            )

        def clf(self):
            """Clear the current figure, as the legacy callee does after each save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    class PathBinding:
        """`Path` as the legacy code names it: a call returns a handle whose `mkdir` is recorded and creates nothing."""

        def __call__(self, path):
            """A handle for `path`."""

            class Handle:
                """A path whose `mkdir` records the request."""

                def mkdir(self, *args, **kwargs):
                    """Record the folder asked for."""
                    folders.append(f"{str(path).replace(fitting_dir, '<tmp>/')}|{sorted(kwargs.items())}")

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
        """pandas as the legacy callees name it, except that `read_csv` returns the fixture table of fit performance the named file stands for."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The performance table of the event type the file name carries; any other file is an error."""
            for key in ("surge_events", "rain_events", "compound_events"):
                if str(path) == f"all_marginals/{key}.csv":
                    return fixtures[f"msel_perf_{key}"].copy()
            raise FileNotFoundError(f"the lift reads no csv named {path}")

    def record(*arguments, **keywords):
        """The `print` the callees read: keep the line and print nothing."""
        printed.append(" ".join(str(argument) for argument in arguments))

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
        ("_utils", "1198-1238"): (
            "stored-text:1:f700014259fd05c2b5b331ae91e4c7ae655225bd0d2b4836dc4c54235d1df8b8",
            1198,
        ),
    }
    namespace = {
        "np": numpy,
        "pd": PdBinding(),
        "stats": stats,
        "scipy": scipy,
        "plt": Plt(),
        "sns": seaborn,
        "sys": Sys(),
        "Path": PathBinding(),
        "print": record,
        "dir_fitting_maringals": fitting_dir,
        "f_combined_event_summaries": "combined_event_summaries.csv",
    }
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    bindings = {
        "cvm_cutoff": 0.2,
        "dic_event_stats": {
            "surge_events": [
                "surge_peak_ft",
                "precip_depth_mm",
                "max_mean_1hrsurge_peak_after_4hrrain_peak_h",
                "max_4hr_mean_mm_per_hr",
            ],
            "rain_events": [
                "surge_peak_ft",
                "precip_depth_mm",
                "max_mean_1hrsurge_peak_after_16hrrain_peak_h",
                "max_4hr_mean_mm_per_hr",
            ],
            "compound_events": [
                "surge_peak_ft",
                "precip_depth_mm",
                "max_mean_16hrsurge_peak_after_24hrrain_peak_h",
                "max_4hr_mean_mm_per_hr",
            ],
        },
        "dic_event_summaries": {
            "surge_events": fixtures["msel_surge_events"].copy(),
            "rain_events": fixtures["msel_rain_events"].copy(),
            "compound_events": fixtures["msel_compound_events"].copy(),
        },
        "dir_plots_all_fitted_marginals": "all_marginals/",
        "final_selection_plot_and_write_csv": namespace["final_selection_plot_and_write_csv"],
        "ks_cutoff": 0.2,
        "print": record,
    }

    def pack(data, name, table, drop=()):
        """Add a table to `data` as one variable per column on its own row dimension: numbers and flags as they are, text as text."""
        dimension = f"{name}__row"
        for column in table.columns:
            if column in drop:
                continue
            values = table[column]
            if values.dtype == bool:
                data[f"{name}__{column}"] = (dimension, values.to_numpy(dtype=bool))
            elif values.dtype.kind in "fiu":
                data[f"{name}__{column}"] = (dimension, values.to_numpy(dtype=float))
            else:
                data[f"{name}__{column}"] = (dimension, values.astype(str).to_numpy())

    def reduce(finished):
        """The two tables written for each event type, the figures saved (path, keywords, title, panels), the folders asked for and the lines printed."""
        pyplot.close("all")
        data = {
            "folders": ("folder", numpy.asarray(folders, dtype=str)),
            "printed": ("line", numpy.asarray(printed, dtype=str)),
        }
        for key in ("surge_events", "rain_events", "compound_events"):
            pack(data, f"fits_{key}", pandas.read_csv(f"{fitting_dir}{key}_distributions.csv"))
            pack(data, f"emp_vs_fit_{key}", pandas.read_csv(f"{fitting_dir}{key}_empirical_vs_fitted.csv"))
        data["n_figures"] = ((), len(saved))
        for number, (path, kwargs, figure) in enumerate(saved):
            tag = f"fig{number:02d}"
            data[f"{tag}__path"] = ((), path)
            data[f"{tag}__kwargs"] = ((), kwargs)
            data[f"{tag}__suptitle"] = ((), figure["suptitle"])
            axes = figure["axes"]
            data[f"{tag}__titles"] = (f"{tag}__axis", numpy.asarray([a["title"] for a in axes], dtype=str))
            data[f"{tag}__labels"] = (f"{tag}__label", numpy.asarray([x for a in axes for x in a["labels"]], dtype=str))
            data[f"{tag}__lims"] = (f"{tag}__lim", numpy.asarray([x for a in axes for x in a["lims"]], dtype=float))
            data[f"{tag}__bars"] = (f"{tag}__bar", numpy.asarray([x for a in axes for x in a["bars"]], dtype=float))
            data[f"{tag}__line_meta"] = (
                f"{tag}__linem",
                numpy.asarray([x for a in axes for x in a["line_meta"]], dtype=str),
            )
            data[f"{tag}__line_data"] = (
                f"{tag}__lined",
                numpy.asarray([x for a in axes for x in a["line_data"]], dtype=float),
            )
            data[f"{tag}__legend"] = (f"{tag}__entry", numpy.asarray([x for a in axes for x in a["legend"]], dtype=str))
        shutil.rmtree(workdir, ignore_errors=True)
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_utils.py:410-420", fixtures=["msel_cdf_long"], draws="none")
def declare_format_fitted_cdf_values(fixtures):
    """Run the function that pivots the long table of fitted probabilities into one column per variable and drops incomplete observations; reduce to the tables it returns and the lines it prints."""
    import numpy
    import pandas
    import xarray

    printed = []  # the lines the function prints when it drops observations

    class PdBinding:
        """pandas as the legacy function names it, except that `read_csv` returns the fixture table the named file stands for."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The long table of fitted probabilities for the one csv the function reads; any other file is an error."""
            if str(path) != "marginals/rain_events_empirical_vs_fitted.csv":
                raise FileNotFoundError(f"the lift reads no csv named {path}")
            return fixtures["msel_cdf_long"].copy()

    def record(*arguments, **keywords):
        """The `print` the function reads: keep the line and print nothing."""
        printed.append(" ".join(str(argument) for argument in arguments))

    bindings = {"pd": PdBinding(), "dir_fitting_maringals": "marginals/", "print": record}

    def reduce(finished):
        """The table returned with the default (verbose) and with verbose off, and the lines printed."""
        data = {}
        for name, table in (
            ("verbose", finished["format_fitted_cdf_values_for_fitting_copula"]("rain_events")),
            ("quiet", finished["format_fitted_cdf_values_for_fitting_copula"]("rain_events", verbose=False)),
        ):
            data[f"{name}__index"] = (f"{name}__row", numpy.asarray(table.index.to_numpy(), dtype=float))
            data[f"{name}__columns"] = (f"{name}__column", numpy.asarray(list(table.columns), dtype=str))
            for column in table.columns:
                data[f"{name}__{column}"] = (f"{name}__row", table[column].to_numpy(dtype=float))
        data["printed"] = ("line", numpy.asarray(printed, dtype=str))
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_utils.py:1198-1238",
    fixtures=[
        "msel_surge_events",
        "msel_rain_events",
        "msel_perf_surge_events",
        "msel_perf_rain_events",
    ],
    draws="none",
)
def declare_final_selection_plot_and_write_function(fixtures):
    """Run the function that plots the chosen fits of an event type and writes their tables, for a type whose four statistics are all named and one whose are not; reduce to the tables it writes, the figures it saves and the lines it prints.

    The function reads the type's table of fit performance, refits the named fits through `plot_subset_of_fits`
    (bound, with `fit_dist` and the transformation helpers, to their stored texts) and writes two tables. The
    fixtures name only fits that exist and that `fit_dist` accepts: no fixed-location gamma or lognormal fit is
    asked for, so no fit raises (the Fixtures bullet, ruling U-20).
    """
    import hashlib
    import pathlib
    import shutil
    import tempfile

    import matplotlib
    import numpy
    import pandas
    import scipy.stats
    import seaborn
    import xarray
    from scipy import stats

    matplotlib.use("agg")
    import matplotlib.pyplot as pyplot

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
    workdir = tempfile.mkdtemp(
        prefix="marginal-fitting-lift-"
    )  # the run's own scratch directory, removed by the reduction
    fitting_dir = workdir + "/"
    saved = []  # one entry per figure the callees saved: the path, the save's keywords and what the figure holds
    folders = []  # one entry per folder the callees asked for
    printed = []  # the lines printed

    def figure_record(figure):
        """What a figure holds: its super title and, for each axes, its title, labels, limits, bars and lines."""
        axes_records = []
        for axes in figure.axes:
            bars = [b for b in axes.patches if isinstance(b, matplotlib.patches.Rectangle)]
            axes_records.append(
                {
                    "title": axes.get_title(),
                    "labels": [axes.get_xlabel(), axes.get_ylabel()],
                    "lims": [float(v) for v in axes.get_xlim()] + [float(v) for v in axes.get_ylim()],
                    "bars": [v for b in bars for v in (float(b.get_x()), float(b.get_width()), float(b.get_height()))],
                    "line_meta": [
                        f"{line.get_label()}|{line.get_linestyle()}|{len(line.get_xdata())}" for line in axes.lines
                    ],
                    "line_data": [
                        float(v)
                        for line in axes.lines
                        for v in list(numpy.asarray(line.get_xdata(), dtype=float))
                        + list(numpy.asarray(line.get_ydata(), dtype=float))
                    ],
                    "legend": []
                    if axes.get_legend() is None
                    else [t.get_text() for t in axes.get_legend().get_texts()],
                }
            )
        return {"suptitle": figure._suptitle.get_text() if figure._suptitle is not None else "", "axes": axes_records}

    class Plt:
        """The `plt` the callees draw with: real figures in memory, a record of each save, nothing written."""

        def subplots(self, *args, **kwargs):
            """A real figure and axes."""
            return pyplot.subplots(*args, **kwargs)

        def savefig(self, path, **kwargs):
            """Keep what the current figure holds under the path named, with the save's keyword arguments; write no file."""
            saved.append(
                (str(path).replace(fitting_dir, "<tmp>/"), repr(sorted(kwargs.items())), figure_record(pyplot.gcf()))
            )

        def clf(self):
            """Clear the current figure, as the legacy callee does after each save."""
            pyplot.clf()

        def __getattr__(self, name):
            """Refuse any other attribute: a drawing call this stand-in does not model must not pass unseen."""
            raise AttributeError(f"the lift's plt stand-in does not model plt.{name}")

    class PathBinding:
        """`Path` as the legacy code names it: a call returns a handle whose `mkdir` is recorded and creates nothing."""

        def __call__(self, path):
            """A handle for `path`."""

            class Handle:
                """A path whose `mkdir` records the request."""

                def mkdir(self, *args, **kwargs):
                    """Record the folder asked for."""
                    folders.append(f"{str(path).replace(fitting_dir, '<tmp>/')}|{sorted(kwargs.items())}")

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
        """pandas as the legacy texts name it, except that `read_csv` returns the fixture table of fit performance the named file stands for."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The performance table of the event type the file name carries; any other file is an error."""
            for key in ("surge_events", "rain_events"):
                if str(path) == f"all_marginals/{key}.csv":
                    return fixtures[f"msel_perf_{key}"].copy()
            raise FileNotFoundError(f"the lift reads no csv named {path}")

    def record(*arguments, **keywords):
        """The `print` the legacy texts read: keep the line and print nothing."""
        printed.append(" ".join(str(argument) for argument in arguments))

    pd_binding = PdBinding()
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
    namespace = {
        "np": numpy,
        "pd": pd_binding,
        "stats": stats,
        "scipy": scipy,
        "plt": Plt(),
        "sns": seaborn,
        "sys": Sys(),
        "Path": PathBinding(),
        "print": record,
        "dir_fitting_maringals": fitting_dir,
        "f_combined_event_summaries": "combined_event_summaries.csv",
    }
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    bindings = {
        "np": numpy,
        "pd": pd_binding,
        "dir_fitting_maringals": fitting_dir,
        "f_combined_event_summaries": "combined_event_summaries.csv",
        "plot_subset_of_fits": namespace["plot_subset_of_fits"],
        "print": record,
    }
    surge_statistics = [
        "surge_peak_ft",
        "precip_depth_mm",
        "max_mean_1hrsurge_peak_after_4hrrain_peak_h",
        "max_4hr_mean_mm_per_hr",
    ]
    rain_statistics = [
        "surge_peak_ft",
        "precip_depth_mm",
        "max_mean_1hrsurge_peak_after_16hrrain_peak_h",
        "max_4hr_mean_mm_per_hr",
    ]
    summaries = {
        "surge_events": fixtures["msel_surge_events"].copy(),
        "rain_events": fixtures["msel_rain_events"].copy(),
    }
    statistics = {"surge_events": surge_statistics, "rain_events": rain_statistics}
    chosen = {
        "surge_events": [
            "surge_peak_ft_cvm-0.86_notrns_dist-weibull_min_fixed_loc",
            "precip_depth_mm_cvm-0.72_trns_1_ub_dist-genpareto",
            "max_mean_1hrsurge_peak_after_4hrrain_peak_h_cvm-0.85_notrns_dist-gumbel_l",
            "max_4hr_mean_mm_per_hr_cvm-0.91_notrns_dist-genpareto_fixed_loc",
        ],
        "rain_events": [  # the fourth statistic is left unmodelled, so that the function warns
            "surge_peak_ft_cvm-0.96_trns_1_ub_dist-exponpow",
            "precip_depth_mm_cvm-0.96_notrns_dist-weibull_min_fixed_loc",
            "max_mean_1hrsurge_peak_after_16hrrain_peak_h_cvm-0.98_notrns_dist-t",
        ],
    }

    def pack(data, name, table, drop=()):
        """Add a table to `data` as one variable per column on its own row dimension: numbers and flags as they are, text as text."""
        dimension = f"{name}__row"
        for column in table.columns:
            if column in drop:
                continue
            values = table[column]
            if values.dtype == bool:
                data[f"{name}__{column}"] = (dimension, values.to_numpy(dtype=bool))
            elif values.dtype.kind in "fiu":
                data[f"{name}__{column}"] = (dimension, values.to_numpy(dtype=float))
            else:
                data[f"{name}__{column}"] = (dimension, values.astype(str).to_numpy())

    def reduce(finished):
        """The two tables written for each event type, the figures saved, the folders asked for and the lines printed."""
        function = finished["final_selection_plot_and_write_csv"]
        for key in ("surge_events", "rain_events"):
            function(chosen[key], key, statistics, summaries, "all_marginals/", 0.2, 0.2)
        pyplot.close("all")
        data = {
            "folders": ("folder", numpy.asarray(folders, dtype=str)),
            "printed": ("line", numpy.asarray(printed, dtype=str)),
        }
        for key in ("surge_events", "rain_events"):
            pack(data, f"fits_{key}", pandas.read_csv(f"{fitting_dir}{key}_distributions.csv"))
            pack(data, f"emp_vs_fit_{key}", pandas.read_csv(f"{fitting_dir}{key}_empirical_vs_fitted.csv"))
        data["n_figures"] = ((), len(saved))
        for number, (path, kwargs, figure) in enumerate(saved):
            tag = f"fig{number:02d}"
            data[f"{tag}__path"] = ((), path)
            data[f"{tag}__kwargs"] = ((), kwargs)
            data[f"{tag}__suptitle"] = ((), figure["suptitle"])
            axes = figure["axes"]
            data[f"{tag}__titles"] = (f"{tag}__axis", numpy.asarray([a["title"] for a in axes], dtype=str))
            data[f"{tag}__labels"] = (f"{tag}__label", numpy.asarray([x for a in axes for x in a["labels"]], dtype=str))
            data[f"{tag}__lims"] = (f"{tag}__lim", numpy.asarray([x for a in axes for x in a["lims"]], dtype=float))
            data[f"{tag}__bars"] = (f"{tag}__bar", numpy.asarray([x for a in axes for x in a["bars"]], dtype=float))
            data[f"{tag}__line_meta"] = (
                f"{tag}__linem",
                numpy.asarray([x for a in axes for x in a["line_meta"]], dtype=str),
            )
            data[f"{tag}__line_data"] = (
                f"{tag}__lined",
                numpy.asarray([x for a in axes for x in a["line_data"]], dtype=float),
            )
            data[f"{tag}__legend"] = (f"{tag}__entry", numpy.asarray([x for a in axes for x in a["legend"]], dtype=str))
        shutil.rmtree(workdir, ignore_errors=True)
        return xarray.Dataset(data)

    return bindings, reduce
