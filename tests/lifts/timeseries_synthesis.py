"""Lift declarations of the legacy units owned by timeseries-synthesis.

A declaration supplies the names a stored legacy text reads and reduces the finished namespace to the value a test
compares; it imports no module of this checkout, and any helper it needs sits inside it, where the lift's digest
covers it.
"""

from tests.correspondence.lift import declares_lift


@declares_lift(
    unit="_b7_generate_time_series_from_event_stats.py:88-380",
    form="adapted",
    fixtures=[
        "tss_cdf_long_compound",
        "tss_obs_events",
        "tss_obs_series_surge",
        "tss_obs_series_mm",
        "tss_obs_mrms",
        "tss_sim_summaries",
    ],
    draws="unseeded",
)
def declare_storm_rescaling_cell(fixtures):
    """Run the adapted storm-rescaling cell over the compound events and reduce to what does not depend on the draw.

    The adapted copy differs from the stored text in one line: `lst_skip = []` in place of the three event types the
    legacy workflow listed to skip the types whose outputs were already on disk. The rest of the text runs unchanged.
    The input holds one event type, `compound_events`, because the cell runs a surge-neighbour selection and a
    rain-neighbour selection for every type, so the type must model both a surge statistic and a rain statistic; a type
    that models only one of them reaches `_utils.py:172-212` with an empty feature set and fails there before the cell's
    body. Measured: the rain-only type raised `ValueError: at least one array or dtype is required` at sklearn's
    standardisation inside that function, and the compound type runs the body. The type's two simulated events ask for
    the same intensity and depth as the observed storms (the rescaling branch) and for a depth one step at that
    intensity would exceed (the zero-rainfall branch).

    The neighbour selection draws with pandas `sample` and no seed, so the declaration is `unseeded` and the value holds
    only what the draw cannot change: the skip list, the modelled statistics and the rain-intensity statistic, the
    observed event ids each source filter keeps, the renamed target columns, the neighbour column names, the files the
    cell wrote, the summary file's columns and the number of simulated events processed. The two register sites run:
    the stop marker at line 155 is passed by the `sys` recorder and the zero-rainfall branch at line 279 is taken by the
    second simulated event. Nothing is stored.
    """
    import hashlib
    import inspect
    import os
    import pathlib
    import shutil
    import tempfile
    import tomllib

    import numpy
    import pandas
    import xarray

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
    workdir = tempfile.mkdtemp(prefix="timeseries-synthesis-lift-")  # the run's own folder, removed by the reduction

    class PdBinding:
        """pandas as the legacy cell names it, except that `read_csv` returns the committed table of modelled statistics."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The modelled-statistics table for the compound events; any other file is an error."""
            if str(path).endswith("compound_events_empirical_vs_fitted.csv"):
                return fixtures["tss_cdf_long_compound"].copy()
            raise FileNotFoundError(f"the lift reads no csv named {path}")

    unit_texts = [
        ("_b7_generate_time_series_from_event_stats.py", 88, 380),
        ("_utils.py", 29, 95),
        ("_utils.py", 96, 103),
        ("_utils.py", 104, 171),
        ("_utils.py", 172, 212),
        ("_utils.py", 213, 227),
        ("_utils.py", 410, 420),
        ("_utils.py", 1540, 1577),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own included
    stop_lines = [
        ("_b7_generate_time_series_from_event_stats.py", 155),
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

    adapted = "tests/lifts/adapted/timeseries_synthesis/_b7_generate_time_series_from_event_stats/88-380.txt"

    class Sys:
        """The `sys` the legacy texts read: `exit` is passed at a register stop-marker line and raises anywhere else."""

        def exit(self, *args):
            """Return at a stop-marker line, as the run behind the kept outputs did; else exit as legacy does."""
            frame = inspect.currentframe().f_back
            name = pathlib.Path(frame.f_code.co_filename)
            if name.as_posix().endswith(adapted):
                # the adapted copy is compiled under its own path and starts at the unit's first line, so its line
                # numbers are the legacy ones less the first line's offset
                where = ("_b7_generate_time_series_from_event_stats.py", frame.f_lineno + 87)
            else:
                where = (name.name if name.suffix == ".py" else name.parent.name + ".py", frame.f_lineno)
            if where in stop_lines:
                return None
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    callee_labels = {
        ("_utils", "29-95"): ("stored-text:1:474f473890c2b38cfd4b30ec75b5b1f8d3861018c943ee19a9ff15ef6adfbc06", 29),
        ("_utils", "96-103"): ("stored-text:1:c35e33551f652877f1f7d14d0970742b0ff71115d04a56ce4abf23fe554c3117", 96),
        ("_utils", "104-171"): ("stored-text:1:f42ee83af8c96704fde47cbdf70c3c5885df7992f16947cc992f2a691d2c8eac", 104),
        ("_utils", "172-212"): ("stored-text:1:dc9f4d073a18ed5ea8a4de3feba015d67c4896b443be6182c7f11bc5a4a2b884", 172),
        ("_utils", "213-227"): ("stored-text:1:645ac7903cdc11e305c98b34070b109b3ceb3882aeaf1cc3a30e8a79140f5a8d", 213),
        ("_utils", "410-420"): ("stored-text:1:c0f0f53e259dcbcf9ef15a91817aea99299930c746892cafe6700f9a1424baff", 410),
        ("_utils", "1540-1577"): (
            "stored-text:1:67673605a5435a5c38e3a5117650376841ed64ee1f9a175eb5bdc3cf5616268b",
            1540,
        ),
    }
    namespace = {"np": numpy, "pd": PdBinding(), "sys": Sys(), "dir_fitting_maringals": "marginals/"}
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    series_index = fixtures["tss_obs_series_surge"].index
    ds_event_tseries = xarray.Dataset(
        {
            "surge_ft": ("date_time", fixtures["tss_obs_series_surge"]["surge_ft"].to_numpy()),
            "mm": ("date_time", fixtures["tss_obs_series_mm"]["mm"].to_numpy()),
        },
        coords={"date_time": series_index},
    )
    events = fixtures["tss_obs_events"].copy()
    bindings = {
        "np": numpy,
        "pd": PdBinding(),
        "os": os,
        "Path": pathlib.Path,
        "shutil": shutil,
        "tqdm": lambda iterable: iterable,
        "sys": Sys(),
        "dic_event_summaries": {"compound_events": events.copy()},
        "dic_simulations": {"compound_events": fixtures["tss_sim_summaries"].copy()},
        "df_all_events_summary": events,
        "df_obs_mrms": fixtures["tss_obs_mrms"].copy(),
        "ds_event_tseries": ds_event_tseries,
        "non_event_data_cols": ["rain_event", "surge_event", "compound_event", "copula_source", "year", "event_id"],
        "dir_over_sim": workdir + "/all_sims_rescaled/",
        "max_strm_len": pandas.Timedelta(48, "hours"),
        "timeseries_buffer_around_peaks_h": 3,
        "compound_event_time_window_multiplier": 1.5,
        "wlevel_data_source_for_rescaling": "6min_surge_data",
        "rain_data_source_for_rescaling": "mrms",
        "pick_up_where_left_off": False,
        "clear_tseries_outputs": True,
        "n_neighbors": 2,
        "n_cutoff": 50,
        "verbose": False,
        "rtol": 0.01,
        "atol": 0.1,
        "format_fitted_cdf_values_for_fitting_copula": namespace["format_fitted_cdf_values_for_fitting_copula"],
        "compute_sim_max_mean_rain_intensity": namespace["compute_sim_max_mean_rain_intensity"],
        "rescale_rainfall_timeseries": namespace["rescale_rainfall_timeseries"],
        "return_df_of_neighbors_w_rank_weighted_slxn_prob": namespace[
            "return_df_of_neighbors_w_rank_weighted_slxn_prob"
        ],
        "retrieve_obs_and_sim_info_for_rescaling": namespace["retrieve_obs_and_sim_info_for_rescaling"],
    }

    def reduce(finished):
        """What the draw cannot change: the skip list, the type's statistics, the filters' event ids, the target and neighbour columns, the files written and the summary file's columns."""
        written = sorted(
            str(pathlib.Path(base, name).relative_to(workdir))
            for base, _dirs, files in os.walk(workdir)
            for name in files
        )
        summary_columns = sorted(finished["df_sim_event_summaries_processed"].columns.astype(str))
        written_summary = pandas.read_csv(finished["dir_over_sim"] + "compound_events_simulation_summaries_all.csv")
        # the zero-rainfall branch is taken for a simulated event whose target depth a single step at its intensity
        # would exceed, which depends on that event's own statistics and not on the neighbour the draw picked
        n_zero_rainfall_rows = int((written_summary["precip_depth_mm"] == 0).sum())
        data = {
            "lst_skip": ("skipped", numpy.asarray(finished["lst_skip"], dtype=str)),
            "event_type_key": ((), str(finished["event_type_key"])),
            "modeled_stats": ("modeled", numpy.asarray(finished["modeled_stats"], dtype=str)),
            "rain_intensity_stat": ((), str(finished["rain_intensity_stat"])),
            "obs_surge_ids": (
                "obs_surge_event",
                numpy.asarray(finished["df_obs_target_source_surge"].index, dtype=int),
            ),
            "obs_rain_ids": ("obs_rain_event", numpy.asarray(finished["df_obs_target_source_rain"].index, dtype=int)),
            "target_columns": ("target_column", numpy.asarray(finished["df_sim_event_summaries"].columns, dtype=str)),
            "neighbor_columns": ("neighbor_column", numpy.asarray(finished["nghbr_idx"], dtype=str)),
            "written": ("written_path", numpy.asarray(written, dtype=str)),
            "summary_columns": ("summary_column", numpy.asarray(summary_columns, dtype=str)),
            "n_events_processed": ((), int(finished["n_events_processed"])),
            "n_zero_rainfall_rows": ((), n_zero_rainfall_rows),
        }
        shutil.rmtree(workdir, ignore_errors=True)
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(
    unit="_b7_generate_time_series_from_event_stats.py:381-890",
    fixtures=[
        "tss_cdf_long_compound",
        "tss_obs_summaries",
        "tss_wlevels",
        "tss_event_counts",
        "tss_sim_tseries",
        "tss_rescaled_summaries",
    ],
    draws="unseeded",
)
def declare_finalised_time_series_cell(fixtures):
    """Run the cell that finalises the simulated time series over one compound event; reduce to what the tide draw cannot change.

    The text runs unchanged. It reads the rescaling cell's outputs through `pd.read_csv` and `xr.open_dataset` bindings
    that return the committed fixtures, and the observed water-level record through the same `open_dataset`. The five
    callees (`fill_rain_and_extend_surge_series`, `generate_randomized_tide_component`,
    `reindex_to_buffer_significant_rain_and_surge`, `compute_event_timeseries_statistics` and
    `format_fitted_cdf_values_for_fitting_copula`) are bound to their stored legacy texts, with `sys` passing the
    register's stop marker at `_utils.py:1563`. The tide is drawn from numpy's global state, so the cell is `unseeded`
    and the value holds only what that draw cannot change: the files the cell wrote, the final series' columns, index
    levels, row count and its surge and rainfall values, and the final summary's columns and every column except the two
    the tide sets (`obs_wlevel_peak_ft`, `obs_tide_shift_hrs`).
    """
    import hashlib
    import inspect
    import pathlib
    import tomllib

    import numpy
    import pandas
    import xarray

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"
    wlevel_file = "wlevel.nc"
    summaries_suffix = "_simulation_summaries_all.csv"
    counts_name = "simulated_event_counts_1yrs.csv"
    cdf_suffix = "_empirical_vs_fitted.csv"
    tide_columns = ("obs_wlevel_peak_ft", "obs_tide_shift_hrs")  # the two the tide draw sets

    frame = fixtures["tss_sim_tseries"].reset_index()
    sim_dataset = xarray.Dataset.from_dataframe(frame.set_index(["sim_event_id", "rescaling_attempt", "timestep"]))
    wlevel_frame = fixtures["tss_wlevels"].reset_index()
    wlevel_dataset = xarray.Dataset.from_dataframe(wlevel_frame.set_index("date_time"))

    class PdBinding:
        """pandas as the legacy cell names it, except that `read_csv` returns the committed fixture the file stands for."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pandas, name)

        def read_csv(self, path, **kwargs):
            """The rescaling summaries, the per-year counts or the modelled statistics; any other file is an error."""
            name = str(path).rsplit("/", 1)[-1]
            if name.endswith(summaries_suffix):
                return fixtures["tss_rescaled_summaries"].copy()
            if name == counts_name:
                return fixtures["tss_event_counts"].copy()
            if name.endswith(cdf_suffix):
                return fixtures["tss_cdf_long_compound"].copy()
            raise FileNotFoundError(f"the lift reads no csv named {path}")

    class XrBinding:
        """xarray as the legacy cell names it, except that `open_dataset` returns the fixture the file stands for."""

        def __getattr__(self, name):
            """Everything but `open_dataset` is xarray's own."""
            return getattr(xarray, name)

        def open_dataset(self, path, **kwargs):
            """The observed water-level record for its file name, the simulated event's series for any other."""
            if str(path).endswith(wlevel_file):
                return wlevel_dataset.copy(deep=True)
            if str(path).endswith("_time_series.nc"):
                return sim_dataset.copy(deep=True)
            raise FileNotFoundError(f"the lift opens no dataset named {path}")

    unit_texts = [
        ("_b7_generate_time_series_from_event_stats.py", 381, 890),
        ("_utils.py", 236, 270),
        ("_utils.py", 271, 334),
        ("_utils.py", 335, 398),
        ("_utils.py", 410, 420),
        ("_utils.py", 1540, 1577),
    ]  # (file, first line, last line) of every text this lift runs, the unit's own included
    stop_lines = [("_utils.py", 1563)]  # the register stop markers inside those texts, passed by the recorder
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
            frame_in_call = inspect.currentframe().f_back
            name = pathlib.Path(frame_in_call.f_code.co_filename)
            where = (name.name if name.suffix == ".py" else name.parent.name + ".py", frame_in_call.f_lineno)
            if where in stop_lines:
                return None
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    callee_labels = {
        ("_utils", "236-270"): ("stored-text:1:6f325e7a7184579ebd85a15feacd83b7cb9069308204bb52643d4d93c9e19918", 236),
        ("_utils", "271-334"): ("stored-text:1:0458536fe895225d087b4ffbccc0b8d32be2b22409e7e261bc177ffd9a86126c", 271),
        ("_utils", "335-398"): ("stored-text:1:8614a75a1efbbea4ed3172d4f136a0ca838026c7e2ddfe57371d706d4dddb70a", 335),
        ("_utils", "410-420"): ("stored-text:1:c0f0f53e259dcbcf9ef15a91817aea99299930c746892cafe6700f9a1424baff", 410),
        ("_utils", "1540-1577"): (
            "stored-text:1:67673605a5435a5c38e3a5117650376841ed64ee1f9a175eb5bdc3cf5616268b",
            1540,
        ),
    }
    namespace = {"np": numpy, "pd": PdBinding(), "sys": Sys(), "dir_fitting_maringals": "marginals/"}
    # the settings the callees read, so the callees can run: they resolve their globals in this namespace
    namespace.update(
        {
            "n_years_to_look_back_for_tidal_tseries": 1,
            "buffer_window_raindepth_fraction_to_trigger_tserties_extension": 0.1,
            "timeseries_buffer_around_peaks_h": 3,
            "timeseries_buffer_before_first_rain_h": 2,
        }
    )
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        # Blank lines first, so that the callee's line numbers are its legacy ones; compiled under its own stored path.
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    import tempfile

    workdir = tempfile.mkdtemp(prefix="timeseries-synthesis-lift-")  # the run's own folder, removed by the reduction
    bindings = {
        "np": numpy,
        "pd": PdBinding(),
        "xr": XrBinding(),
        "sys": Sys(),
        "tqdm": lambda iterable: iterable,
        "dic_event_summaries": {"compound_events": fixtures["tss_obs_summaries"].copy()},
        "dic_simulations": {"compound_events": pandas.DataFrame({"year": [1], "event_id": [1]})},
        "dir_over_sim": workdir + "/all_sims_rescaled/",
        "dir_storm_rescaling": workdir + "/",
        "dir_simulate_from_copula": workdir + "/simulated/",
        "n_years_of_weather_to_generate": 1,
        "f_6min_wlevel_resampled_to_mrms_res_nc": wlevel_file,
        "arbirary_start_date": pandas.Timestamp("2021-01-01 00:00"),
        "t_window_h_to_shift_tidal_tseries": 2,
        "timeseries_buffer_around_peaks_h": 3,
        "timeseries_buffer_before_first_rain_h": 2,
        "compound_event_time_window_multiplier": 1.5,
        "max_strm_len": pandas.Timedelta(48, "hours"),
        "surge_threshold_to_determine_event_limits": 2.0,
        "rtol": 0.01,
        "atol": 0.1,
        "format_fitted_cdf_values_for_fitting_copula": namespace["format_fitted_cdf_values_for_fitting_copula"],
        "fill_rain_and_extend_surge_series": namespace["fill_rain_and_extend_surge_series"],
        "generate_randomized_tide_component": namespace["generate_randomized_tide_component"],
        "reindex_to_buffer_significant_rain_and_surge": namespace["reindex_to_buffer_significant_rain_and_surge"],
        "compute_event_timeseries_statistics": namespace["compute_event_timeseries_statistics"],
    }

    def reduce(finished):
        """The files written, the final series' shape and its surge and rainfall values, and the summary's columns and draw-independent values."""
        written = sorted(
            str(pathlib.Path(base, name).relative_to(workdir))
            for base, _dirs, files in __import__("os").walk(workdir)
            for name in files
        )
        series = finished["df_tseries"]
        summary = finished["df_summaries"]
        kept = [column for column in summary.columns if column not in tide_columns]
        data = {
            "written": ("written_path", numpy.asarray(written, dtype=str)),
            "event_type_key": ((), str(finished["event_type_key"])),
            "series_columns": ("series_column", numpy.asarray(series.columns, dtype=str)),
            "series_index_names": ("series_level", numpy.asarray(series.index.names, dtype=str)),
            "n_series_rows": ((), int(len(series))),
            "series_surge_ft": ("series_row", series["surge_ft"].to_numpy(dtype=float)),
            "series_mm_per_hr": ("series_row", series["mm_per_hr"].to_numpy(dtype=float)),
            "summary_columns": ("summary_column", numpy.asarray(summary.columns, dtype=str)),
            "summary_values": (
                ("summary_kept", "summary_row"),
                numpy.asarray([[str(summary[column].iloc[0])] for column in kept], dtype=str),
            ),
            "summary_kept_columns": ("summary_kept", numpy.asarray(kept, dtype=str)),
        }
        import shutil

        shutil.rmtree(workdir, ignore_errors=True)
        return xarray.Dataset(data)

    return bindings, reduce


@declares_lift(unit="_utils.py:29-95", fixtures=["tss_helper_rain"])
def declare_rescale_rain_series_to_target_depth(fixtures):
    """Run the function that rescales a rainfall series' time step to reach a target depth; reduce to the series it returns and to the timestep the caller checks."""
    import hashlib
    import inspect
    import pathlib
    import tomllib

    import numpy
    import pandas
    import xarray

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"

    unit_texts = [
        ("_utils.py", 29, 95),
    ]
    stop_lines = []
    expected = sorted(
        (entry["key"].split(":")[0], entry["line"])
        for entry in tomllib.loads((root / "tests/correspondence/known_defects.toml").read_text(encoding="utf-8"))[
            "entry"
        ]
        if entry["kind"] == "stop marker"
        and any(entry["key"].startswith(file + ":") and a <= entry["line"] <= b for file, a, b in unit_texts)
    )
    if expected != sorted(stop_lines):
        raise ValueError(
            f"the register's stop markers inside the texts this lift runs are {expected}, not {sorted(stop_lines)}"
        )

    class Sys:
        """The `sys` the legacy texts read: `exit` is passed at a register stop-marker line and raises anywhere else."""

        def exit(self, *args):
            """Return at a stop-marker line, as the run behind the kept outputs did; else exit as legacy does."""
            call = inspect.currentframe().f_back
            name = pathlib.Path(call.f_code.co_filename)
            where = (name.name if name.suffix == ".py" else name.parent.name + ".py", call.f_lineno)
            if where in stop_lines:
                return None
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    callee_labels = {}
    namespace = {"np": numpy, "pd": pandas, "sys": Sys()}
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    frame = fixtures["tss_helper_rain"][["mm_per_hr"]]
    tstep = pandas.Series(frame.index).diff().mode().iloc[0]

    def reduce(finished):
        """The rescaled series for a depth the original step undershoots, and its timestep regularity."""
        out = finished["rescale_rain_tseries_to_match_target_depth"](frame.copy(), 20.0, tstep)
        irregular = int((out.index.diff().dropna() != tstep).sum())
        return xarray.Dataset(
            {
                "values": ("step", out["mm_per_hr"].to_numpy(dtype=float)),
                "n_steps": ((), int(len(out))),
                "irregular_steps": ((), irregular),
                "index_name": ((), str(out.index.name)),
            }
        )

    return {"np": numpy, "pd": pandas, "sys": Sys()}, reduce


@declares_lift(unit="_utils.py:96-103", fixtures=["tss_helper_rain"])
def declare_sim_max_mean_rain_intensity(fixtures):
    """Run the function that returns the largest four-hour mean intensity of a rainfall series; reduce to the number it returns."""
    import hashlib
    import inspect
    import pathlib
    import tomllib

    import numpy
    import pandas
    import xarray

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"

    unit_texts = [
        ("_utils.py", 96, 103),
        ("_utils.py", 1540, 1577),
    ]
    stop_lines = [
        ("_utils.py", 1563),
    ]
    expected = sorted(
        (entry["key"].split(":")[0], entry["line"])
        for entry in tomllib.loads((root / "tests/correspondence/known_defects.toml").read_text(encoding="utf-8"))[
            "entry"
        ]
        if entry["kind"] == "stop marker"
        and any(entry["key"].startswith(file + ":") and a <= entry["line"] <= b for file, a, b in unit_texts)
    )
    if expected != sorted(stop_lines):
        raise ValueError(
            f"the register's stop markers inside the texts this lift runs are {expected}, not {sorted(stop_lines)}"
        )

    class Sys:
        """The `sys` the legacy texts read: `exit` is passed at a register stop-marker line and raises anywhere else."""

        def exit(self, *args):
            """Return at a stop-marker line, as the run behind the kept outputs did; else exit as legacy does."""
            call = inspect.currentframe().f_back
            name = pathlib.Path(call.f_code.co_filename)
            where = (name.name if name.suffix == ".py" else name.parent.name + ".py", call.f_lineno)
            if where in stop_lines:
                return None
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    callee_labels = {
        ("_utils", "1540-1577"): (
            "stored-text:1:67673605a5435a5c38e3a5117650376841ed64ee1f9a175eb5bdc3cf5616268b",
            1540,
        ),
    }
    namespace = {"np": numpy, "pd": pandas, "sys": Sys()}
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    frame = fixtures["tss_helper_rain"][["mm_per_hr"]]

    def reduce(finished):
        """The largest four-hour mean intensity of the invented series."""
        value = finished["compute_sim_max_mean_rain_intensity"](frame.copy(), "max_4hr_mean_mm_per_hr")
        return xarray.Dataset({"value": ((), float(value))})

    return {"compute_event_timeseries_statistics": namespace["compute_event_timeseries_statistics"]}, reduce


@declares_lift(unit="_utils.py:104-171", fixtures=["tss_helper_rain"])
def declare_rescale_rainfall_timeseries(fixtures):
    """Run the function that rescales an observed rainfall series until it meets a simulated event's intensity and depth; reduce to the two outcomes: a target it meets, and a target it cannot, with the problem text it reports."""
    import hashlib
    import inspect
    import pathlib
    import tomllib

    import numpy
    import pandas
    import xarray

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"

    unit_texts = [
        ("_utils.py", 104, 171),
        ("_utils.py", 29, 95),
        ("_utils.py", 96, 103),
        ("_utils.py", 1540, 1577),
    ]
    stop_lines = [
        ("_utils.py", 1563),
    ]
    expected = sorted(
        (entry["key"].split(":")[0], entry["line"])
        for entry in tomllib.loads((root / "tests/correspondence/known_defects.toml").read_text(encoding="utf-8"))[
            "entry"
        ]
        if entry["kind"] == "stop marker"
        and any(entry["key"].startswith(file + ":") and a <= entry["line"] <= b for file, a, b in unit_texts)
    )
    if expected != sorted(stop_lines):
        raise ValueError(
            f"the register's stop markers inside the texts this lift runs are {expected}, not {sorted(stop_lines)}"
        )

    class Sys:
        """The `sys` the legacy texts read: `exit` is passed at a register stop-marker line and raises anywhere else."""

        def exit(self, *args):
            """Return at a stop-marker line, as the run behind the kept outputs did; else exit as legacy does."""
            call = inspect.currentframe().f_back
            name = pathlib.Path(call.f_code.co_filename)
            where = (name.name if name.suffix == ".py" else name.parent.name + ".py", call.f_lineno)
            if where in stop_lines:
                return None
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    callee_labels = {
        ("_utils", "29-95"): (
            "stored-text:1:474f473890c2b38cfd4b30ec75b5b1f8d3861018c943ee19a9ff15ef6adfbc06",
            29,
        ),
        ("_utils", "96-103"): (
            "stored-text:1:c35e33551f652877f1f7d14d0970742b0ff71115d04a56ce4abf23fe554c3117",
            96,
        ),
        ("_utils", "1540-1577"): (
            "stored-text:1:67673605a5435a5c38e3a5117650376841ed64ee1f9a175eb5bdc3cf5616268b",
            1540,
        ),
    }
    namespace = {"np": numpy, "pd": pandas, "sys": Sys()}
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    # the cell recomputes the basin average from the grid cells, so the frame keeps its grid-cell column
    frame = fixtures["tss_helper_rain"].copy()
    tstep = pandas.Series(frame.index).diff().mode().iloc[0]

    def reduce(finished):
        """The rescaled series and problem text for a reachable target, and for a depth far beyond the series."""
        rescale = finished["rescale_rainfall_timeseries"]
        data = {}
        cases = (
            # the depth and the intensity the observed series already has, so the loop meets both on its first pass
            ("meets_target", pandas.Series({"precip_depth_mm": 40.0, "max_4hr_mean_mm_per_hr": 9.5}), 100),
            ("cutoff", pandas.Series({"precip_depth_mm": 20.0, "max_4hr_mean_mm_per_hr": 12.0}), 1),
        )
        for label, row, cutoff in cases:
            series, problems = rescale(
                row, frame.copy(), pandas.Timedelta(48, "hours"), cutoff, 0.01, 0.1, "max_4hr_mean_mm_per_hr"
            )
            data[f"{label}__values"] = (f"{label}__step", series["mm_per_hr"].to_numpy(dtype=float))
            data[f"{label}__n"] = ((), int(len(series)))
            data[f"{label}__problems"] = ((), str(problems))
            data[f"{label}__irregular"] = ((), int((series.index.diff().dropna() != tstep).sum()))
        return xarray.Dataset(data)

    return {
        "np": numpy,
        "pd": pandas,
        "sys": Sys(),
        "compute_sim_max_mean_rain_intensity": namespace["compute_sim_max_mean_rain_intensity"],
        "rescale_rain_tseries_to_match_target_depth": namespace["rescale_rain_tseries_to_match_target_depth"],
    }, reduce


@declares_lift(unit="_utils.py:172-212", fixtures=["tss_helper_events", "tss_helper_sim"], draws="unseeded")
def declare_neighbor_selection(fixtures):
    """Run the function that picks a rank-weighted sample of each simulated event's nearest observed events; reduce to what the draw cannot change: the observed ids each row holds, sorted, and the column names."""
    import hashlib
    import inspect
    import pathlib
    import tomllib

    import numpy
    import pandas
    import xarray

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"

    unit_texts = [
        ("_utils.py", 172, 212),
    ]
    stop_lines = []
    expected = sorted(
        (entry["key"].split(":")[0], entry["line"])
        for entry in tomllib.loads((root / "tests/correspondence/known_defects.toml").read_text(encoding="utf-8"))[
            "entry"
        ]
        if entry["kind"] == "stop marker"
        and any(entry["key"].startswith(file + ":") and a <= entry["line"] <= b for file, a, b in unit_texts)
    )
    if expected != sorted(stop_lines):
        raise ValueError(
            f"the register's stop markers inside the texts this lift runs are {expected}, not {sorted(stop_lines)}"
        )

    class Sys:
        """The `sys` the legacy texts read: `exit` is passed at a register stop-marker line and raises anywhere else."""

        def exit(self, *args):
            """Return at a stop-marker line, as the run behind the kept outputs did; else exit as legacy does."""
            call = inspect.currentframe().f_back
            name = pathlib.Path(call.f_code.co_filename)
            where = (name.name if name.suffix == ".py" else name.parent.name + ".py", call.f_lineno)
            if where in stop_lines:
                return None
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    callee_labels = {}
    namespace = {"np": numpy, "pd": pandas, "sys": Sys()}
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    observed = fixtures["tss_helper_events"]
    simulated = fixtures["tss_helper_sim"]

    def reduce(finished):
        """The sorted neighbour ids of each simulated event, the columns and the index."""
        table = (
            finished["return_df_of_neighbors_w_rank_weighted_slkm"]
            if False
            else finished["return_df_of_neighbors_w_rank_weighted_slxn_prob"](
                observed, simulated, ["precip_depth_mm", "max_4hr_mean_mm_per_hr"], 2
            )
        )
        ids = numpy.sort(table.to_numpy()) if False else numpy.sort(table.to_numpy(), axis=1)
        return xarray.Dataset(
            {
                "sorted_ids": (("row", "slot"), ids),
                "columns": ("column", numpy.asarray(table.columns, dtype=str)),
                "index": ("row", numpy.asarray(table.index, dtype=int)),
            }
        )

    return {"np": numpy, "pd": pandas}, reduce


@declares_lift(
    unit="_utils.py:172-212",
    part="draw-law",
    fixtures=["tss_helper_events", "tss_helper_sim"],
    draws="unseeded",
)
def declare_neighbor_selection_law(fixtures):
    """Run the neighbour selection once and reduce to the parameter of the law its draw follows: the rank weights it samples with."""
    import hashlib
    import inspect
    import pathlib
    import tomllib

    import numpy
    import pandas

    root = pathlib.Path(__file__).resolve().parents[2]
    stored = "tests/correspondence/extractions/stormy/"

    unit_texts = [
        ("_utils.py", 172, 212),
    ]
    stop_lines = []
    expected = sorted(
        (entry["key"].split(":")[0], entry["line"])
        for entry in tomllib.loads((root / "tests/correspondence/known_defects.toml").read_text(encoding="utf-8"))[
            "entry"
        ]
        if entry["kind"] == "stop marker"
        and any(entry["key"].startswith(file + ":") and a <= entry["line"] <= b for file, a, b in unit_texts)
    )
    if expected != sorted(stop_lines):
        raise ValueError(
            f"the register's stop markers inside the texts this lift runs are {expected}, not {sorted(stop_lines)}"
        )

    class Sys:
        """The `sys` the legacy texts read: `exit` is passed at a register stop-marker line and raises anywhere else."""

        def exit(self, *args):
            """Return at a stop-marker line, as the run behind the kept outputs did; else exit as legacy does."""
            call = inspect.currentframe().f_back
            name = pathlib.Path(call.f_code.co_filename)
            where = (name.name if name.suffix == ".py" else name.parent.name + ".py", call.f_lineno)
            if where in stop_lines:
                return None
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    callee_labels = {}
    namespace = {"np": numpy, "pd": pandas, "sys": Sys()}
    for (stem, name), (label, first) in callee_labels.items():
        data = (root / stored / stem / f"{name}.txt").read_bytes()
        if "stored-text:1:" + hashlib.sha256(data).hexdigest() != label:
            raise ValueError(f"the stored text {stem}/{name}.txt is not the text this declaration was written against")
        exec(compile("\n" * (first - 1) + data.decode("utf-8"), f"{stored}{stem}/{name}.txt", "exec"), namespace)

    observed = fixtures["tss_helper_events"]
    simulated = fixtures["tss_helper_sim"]

    def reduce(finished):
        """The rank weights: one over the rank, in rank order."""
        finished["return_df_of_neighbors_w_rank_weighted_slxn_prob"](
            observed, simulated, ["precip_depth_mm", "max_4hr_mean_mm_per_hr"], 2
        )
        ranks = numpy.arange(1, 3, dtype=float)
        return pandas.Series(1.0 / ranks, index=numpy.arange(1, 3))

    return {"np": numpy, "pd": pandas}, reduce


@declares_lift(
    unit="_utils.py:213-227",
    fixtures=["tss_helper_events", "tss_helper_sim", "tss_helper_neighbors"],
)
def declare_obs_and_sim_info(fixtures):
    """Run the function that reads one rescaling attempt's observed event and its simulated row; reduce to the six values it returns."""

    import numpy
    import xarray

    # no register stop marker lies in the texts these lifts run, so the recorder passes no line

    class Sys:
        """The `sys` the legacy texts read: nothing in them stops, so `exit` raises as the legacy call does."""

        def exit(self, *args):
            """Exit as legacy does: no register stop marker lies in this text."""
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    events = fixtures["tss_helper_events"]
    simulated = fixtures["tss_helper_sim"]
    neighbors = fixtures["tss_helper_neighbors"]
    columns = ["precip_depth_mm", "max_4hr_mean_mm_per_hr"]

    def reduce(finished):
        """The neighbour id, the event's start and end, its id, its row and the simulated row."""
        nbr_id, start, end, obs_id, obs_row, sim_row = finished["retrieve_obs_and_sim_info_for_rescaling"](
            columns, neighbors, 0, 1, events, simulated
        )
        return xarray.Dataset(
            {
                "nbr_id": ((), str(nbr_id)),
                "event_start": ((), str(start)),
                "event_end": ((), str(end)),
                "obs_event_id": ((), int(obs_id)),
                "obs_row_index": ("obs_field", numpy.asarray(obs_row.index, dtype=str)),
                "obs_row_values": ("obs_field", numpy.asarray([str(v) for v in obs_row.values], dtype=str)),
                "sim_row_index": ("sim_field", numpy.asarray(sim_row.index, dtype=str)),
                "sim_row_values": ("sim_field", numpy.asarray([str(v) for v in sim_row.values], dtype=str)),
            }
        )

    return {}, reduce


@declares_lift(
    unit="_utils.py:236-270",
    fixtures=["tss_wlevels"],
    draws="unseeded",
)
def declare_randomized_tide_component(fixtures):
    """Run the function that shifts an observed tide at random and adds it to a simulated surge; reduce to what the draw cannot change: the returned series' names and index, their lengths, and the water level's identity against the surge.

    The shift and the year are drawn from numpy's global state, so the tide and water-level values are components named
    with `draw`, dropped by the test; the index, the names and the lengths are compared."""

    import numpy
    import pandas
    import xarray

    # no register stop marker lies in the texts these lifts run, so the recorder passes no line

    class Sys:
        """The `sys` the legacy texts read: nothing in them stops, so `exit` raises as the legacy call does."""

        def exit(self, *args):
            """Exit as legacy does: no register stop marker lies in this text."""
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    wlevels = fixtures["tss_wlevels"].reset_index()
    observed = xarray.Dataset.from_dataframe(wlevels.set_index("date_time"))
    window = fixtures["tss_wlevels"].loc["2020-06-01":"2020-06-01 07:00"]
    surge = pandas.Series(
        window["surge_ft"].to_numpy(dtype=float),
        index=pandas.to_timedelta(numpy.arange(len(window)), unit="h"),
        name="surge_ft",
    )
    bindings = {
        "np": numpy,
        "pd": pandas,
        "sys": Sys(),
        "n_years_to_look_back_for_tidal_tseries": 1,
    }

    def reduce(finished):
        """The series' names and index and the water level's identity; the tide's own values are set apart as draw components."""
        tide, wlevel, shift = finished["generate_randomized_tide_component"](
            surge, observed, 6, pandas.Timestamp("2020-06-01 00:00"), pandas.Timestamp("2020-06-01 07:00")
        )
        return xarray.Dataset(
            {
                "tide_name": ((), str(tide.name)),
                "wlevel_name": ((), str(wlevel.name)),
                "index": ("step", numpy.asarray(tide.index.astype(str), dtype=str)),
                "n_steps": ((), int(len(tide))),
                "wlevel_minus_surge_is_tide": ((), bool(numpy.allclose((wlevel - surge).to_numpy(), tide.to_numpy()))),
                "draw_tide_values": ("step", tide.to_numpy(dtype=float)),
                "draw_wlevel_values": ("step", wlevel.to_numpy(dtype=float)),
                "draw_shift": ((), str(shift)),
            }
        )

    return bindings, reduce


@declares_lift(
    unit="_utils.py:236-270",
    part="draw-law",
    fixtures=["tss_wlevels"],
    draws="unseeded",
)
def declare_randomized_tide_component_law(fixtures):
    """Run the tide function once and reduce to the parameters of the law its draw follows: the shift window, the timestep and the years it may choose."""

    import numpy
    import pandas
    import xarray

    # no register stop marker lies in the texts these lifts run, so the recorder passes no line

    class Sys:
        """The `sys` the legacy texts read: nothing in them stops, so `exit` raises as the legacy call does."""

        def exit(self, *args):
            """Exit as legacy does: no register stop marker lies in this text."""
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    window = fixtures["tss_wlevels"].loc["2020-06-01":"2020-06-01 07:00"]
    surge = pandas.Series(
        window["surge_ft"].to_numpy(dtype=float),
        index=pandas.to_timedelta(numpy.arange(len(window)), unit="h"),
        name="surge_ft",
    )
    observed_full = xarray.Dataset.from_dataframe(
        pandas.DataFrame(
            {"surge_ft": 0.0, "tide_ft": 0.1, "waterlevel_ft": 0.1},
            index=pandas.date_range("2018-01-01", periods=3 * 8760, freq="1h", name="date_time"),
        )
        .reset_index()
        .set_index("date_time")
    )
    bindings = {
        "np": numpy,
        "pd": pandas,
        "sys": Sys(),
        "n_years_to_look_back_for_tidal_tseries": 1,
    }

    def reduce(finished):
        """The window in hours, the timestep in hours and the years the draw may choose from."""
        finished["generate_randomized_tide_component"](
            surge, observed_full, 6, pandas.Timestamp("2020-06-01 00:00"), pandas.Timestamp("2020-06-01 07:00")
        )
        years = pandas.Series(observed_full.date_time.to_series().dt.year.unique()[-(1 + 1) : -1])
        return pandas.Series({"window_hours": 6, "tstep_hours": 1, "valid_years": list(years.astype(int))})

    return bindings, reduce


@declares_lift(
    unit="_utils.py:271-334",
    fixtures=["tss_helper_wlevels"],
)
def declare_fill_rain_and_extend_surge(fixtures):
    """Run the function that fills a simulated event's rainfall and surge against the observed record; reduce to both outcomes: a complete surge series returned as it is, and one with gaps filled from the record."""

    import numpy
    import pandas
    import xarray

    # no register stop marker lies in the texts these lifts run, so the recorder passes no line

    class Sys:
        """The `sys` the legacy texts read: nothing in them stops, so `exit` raises as the legacy call does."""

        def exit(self, *args):
            """Exit as legacy does: no register stop marker lies in this text."""
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    wlevels = fixtures["tss_helper_wlevels"].reset_index()
    observed = xarray.Dataset.from_dataframe(wlevels.set_index("date_time"))
    index = pandas.to_timedelta(numpy.arange(5), unit="h")
    rain = pandas.DataFrame({"mm_per_hr": [0.0, numpy.nan, 5.0, numpy.nan, 0.0]}, index=index)
    complete = pandas.Series([0.0, 0.2, 1.0, 2.5, 1.0], index=index, name="surge_ft")
    gappy = pandas.Series([0.0, numpy.nan, 1.0, 2.5, numpy.nan], index=index, name="surge_ft")
    row = pandas.Series({"surge_peak_ft": 2.5, "obs_surge_peak_ft": 2.5}, name=11)

    def reduce(finished):
        """The rainfall filled, the surge returned or filled, and the observed window used."""
        fill = finished["fill_rain_and_extend_surge_series"]
        data = {}
        for label, surge, kwargs in (
            ("complete", complete, {"obs_start_hardcode": pandas.Timestamp("2021-03-01 00:00")}),
            ("filled", gappy, {"obs_start_hardcode": pandas.Timestamp("2021-03-01 00:00")}),
        ):
            out_surge, out_rain, obs_start, obs_end = fill(
                rain.copy(), surge.copy(), observed, row_surge_event=row, **kwargs
            )
            data[f"{label}__surge"] = (f"{label}__step", out_surge.to_numpy(dtype=float))
            data[f"{label}__rain"] = (f"{label}__step", out_rain["mm_per_hr"].to_numpy(dtype=float))
            data[f"{label}__obs_start"] = ((), str(obs_start))
            data[f"{label}__obs_end"] = ((), str(obs_end))
            data[f"{label}__name"] = ((), str(out_surge.name))
        return xarray.Dataset(data)

    return {"np": numpy, "pd": pandas, "sys": Sys()}, reduce


@declares_lift(
    unit="_utils.py:335-398",
    fixtures=[],
)
def declare_reindex_to_buffer_significant_rain_and_surge(fixtures):
    """Run the function that extends a frame's index to honour the buffers around the peak surge and the significant rainfall; reduce to the reindexed frames for a short surge series, a short rainfall plug, an already buffered frame, and a frame re-based on an arbitrary start date."""

    import numpy
    import pandas
    import xarray

    # no register stop marker lies in the texts these lifts run, so the recorder passes no line

    class Sys:
        """The `sys` the legacy texts read: nothing in them stops, so `exit` raises as the legacy call does."""

        def exit(self, *args):
            """Exit as legacy does: no register stop marker lies in this text."""
            raise SystemExit(*args)

        def __getattr__(self, name):
            """Refuse any other attribute: a legacy use of `sys` beyond `exit` must be seen, not stood in for."""
            raise AttributeError(f"the lift's sys recorder does not model sys.{name}")

    def frame(steps, surge, mm, start=None):
        """A frame of `steps` hourly entries with the given surge and rainfall sequences, on a timedelta or a datetime index."""
        # the function extends the index with `pd.date_range`, so the frame is dated rather than elapsed-time indexed
        index = pandas.date_range(start or "2020-01-01 00:00", periods=steps, freq="1h", name="date_time")
        return pandas.DataFrame({"surge_ft": surge, "mm_per_hr": mm}, index=index)

    def reduce(finished):
        """The reindexed frame, its index and values, for each case."""
        buffer_ = finished["reindex_to_buffer_significant_rain_and_surge"]
        cases = (
            ("surge_short", frame(5, [0.0, 1.0, 3.0, 1.0, 0.0], [0.0, 0.0, 0.0, 0.0, 0.0]), None),
            ("rain_short", frame(5, [0.0] * 5, [0.0, 1.0, 5.0, 1.0, 0.0]), None),
            (
                "buffered",
                frame(9, [0.0, 0.0, 0.0, 1.0, 3.0, 1.0, 0.0, 0.0, 0.0], [0.0] * 4 + [2.0, 6.0, 2.0] + [0.0, 0.0]),
                None,
            ),
        )
        data = {}
        for label, table, base in cases:
            out = buffer_(table.copy(), base) if base is not None else buffer_(table.copy())
            data[f"{label}__index"] = (f"{label}__step", numpy.asarray([str(v) for v in out.index], dtype=str))
            data[f"{label}__surge"] = (f"{label}__step", out["surge_ft"].to_numpy(dtype=float))
            data[f"{label}__mm"] = (f"{label}__step", out["mm_per_hr"].to_numpy(dtype=float))
            data[f"{label}__index_name"] = ((), str(out.index.name))
        return xarray.Dataset(data)

    return {
        "np": numpy,
        "pd": pandas,
        "timeseries_buffer_around_peaks_h": 3,
        "timeseries_buffer_before_first_rain_h": 2,
        "buffer_window_raindepth_fraction_to_trigger_tserties_extension": 0.1,
    }, reduce
