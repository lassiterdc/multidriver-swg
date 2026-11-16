r"""The four series-assembly equations of the SWG supplement, each checked against the code that runs it.

Every function here is one equation of the supplementary manuscript's series-assembly section, named by the name the
equation register proposes for it and carrying that equation's id in its conformance marker. Each carries its equation's
implementation unit, entry, literal inputs, compared value and licence, as the approved criterion's equation table
states them. The observed side is the
legacy unit that executes the published mathematics today, reached as its stored extraction through the lift harness and
run by `equation_check.implementation` (nothing under `src/` implements these equations yet). The reference side is the
manuscript's formula, computed in the test's own body over literal inputs. One `check` call compares them, so a
disagreement arrives as a `ConformanceDivergence` carrying the ledger's binding for that row.

Three of the four diverge, and each divergence is the register's own record rather than a surprise. E-20's cell adds
`PS_g - PS_h` where the manuscript writes `PS_h - PS_g`, so the simulated surge peak lands on the target and the
manuscript's own sign misses it by twice the peak difference. E-28's significant-rainfall windows are non-overlapping
and anchored at the series end, which the manuscript's text does not state, so the last buffered timestep moves with the
record's length rather than with the rainfall. E-31's year slice is `[-(n + 1):-1]`, which drops the most recent
complete year the manuscript's set includes. E-26 is green: the shifted series' surge-after-rain peak lag is the target
exactly when the target is a whole number of time steps, which is the case its literal inputs carry.

What this module does NOT establish: that the class a reader assigns to each divergence (D1..D9) is right. A divergence
record carries the two values and the ledger's binding; class assignment is `taxonomy.classify` over an authored
diagnosis, and no test here authors one. Nor does it establish that the literal inputs span each equation's domain:
each row names the case it discriminates, and that case is the divergence it exists to see.
"""

from __future__ import annotations

import io
import pathlib

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from tests.conformance.equation_check import check, implementation, seeded_global_numpy
from tests.correspondence import compare

# The legacy units these equations run — the register's implementation unit for each: E-20's storm-rescaling cell and
# E-26's finalising cell are the two halves of the series-assembly script, and E-28's buffering function and E-31's
# tide randomiser are the `_utils` helpers both cells call.
STORM_RESCALING_CELL = "_b7_generate_time_series_from_event_stats.py:88-380"
FINALISING_CELL = "_b7_generate_time_series_from_event_stats.py:381-890"
BUFFERING_FUNCTION = "_utils.py:335-398"
TIDE_FUNCTION = "_utils.py:236-270"

# The comparison policy these tests take by default, the equivalence family's own: numbers within `1e-9 * |reference|`,
# and every other kind exact.
TOLERANCE = compare.Tolerance(rtol=1e-9, atol=0.0)

# The one seed the stochastic units set. Their texts draw through `np.random.*` and through `DataFrame.sample` with no
# generator argument, so numpy's global state is the only channel that steers them.
SEED = 20240116


@pytest.mark.conformance(unit="E-20", part="test_eq20_additive_surge_rescaling", system="publication-math-conformance")
def test_eq20_additive_surge_rescaling(request: pytest.FixtureRequest, tmp_path: pathlib.Path) -> None:
    r"""E-20: the simulated surge series is the observed one with the peak difference added on.

    The register's span for this equation, verbatim:

    \vec{s}_{s} = \vec{s}_{h} + (\text{PS}_{h} - \text{PS}_{g})

    Discriminating inputs. One observed surge event of five hourly steps whose peak is 5.0 ft, and one simulated event
    that asks for a peak of 3.0 ft. The two forms then differ by 4.0 ft at every step (`+2` against `-2`), so the sign
    alone separates them and no other input can absorb it. A simulated peak equal to the observed one would make the
    two forms identical and the test vacuous, which is why the literal peaks are chosen apart.

    The value compared is the cell's own written surge series, not a value the cell reports about itself: the reduce
    reads the simulated-event time series the cell leaves in its namespace, so a cell that computed the manuscript's
    form and then reported the target peak would still be caught.

    The cell runs one event type, `compound_events`, because it reselects neighbours for a surge statistic and for a
    rain statistic on every type. The rainfall side is inert by construction — the observed event carries no rain, so
    the cell takes its zero-rainfall branch — and the surge side carries this row's whole subject.

    One neighbour is bound (`n_neighbors = 1`), so the rank-weighted draw over the nearest neighbours has one candidate
    per attempt and the test does not depend on which neighbour a draw would pick. numpy's global state is seeded anyway
    because the draw still passes through it.
    """
    observed_start = pd.Timestamp("2020-03-01 00:00")
    observed_end = pd.Timestamp("2020-03-01 04:00")
    observed_index = pd.date_range(observed_start, observed_end, freq="1h", name="date_time")
    # The observed event's surge, whose peak IS the manuscript's `PS_h` because the cell refuses a summary that
    # disagrees with the series it reads (it exits at `_b7:221-223` otherwise).
    observed_surge = [0.0, 1.0, 5.0, 1.0, 0.0]
    observed_peak = max(observed_surge)  # `PS_h`
    simulated_peak = 3.0  # `PS_g`, deliberately not `PS_h`
    event_id = 7

    event_series = xr.Dataset(
        {
            "surge_ft": ("date_time", np.asarray(observed_surge, dtype=float)),
            "mm": ("date_time", np.zeros(len(observed_index), dtype=float)),
        },
        coords={"date_time": observed_index},
    )
    observed_events = pd.DataFrame(
        {
            "event_type": ["compound"],
            "event_start": [observed_start],
            "event_end": [observed_end],
            "surge_peak_ft": [observed_peak],
            "precip_depth_mm": [0.0],
            "max_4hr_mean_mm_per_hr": [0.0],
            "rain_data_source": ["mrms"],
            "wlevel_data_source": ["6min_surge_data"],
        },
        index=pd.Index([event_id], name="event_id"),
    )
    observed_mrms = pd.DataFrame({"rain_mean": np.zeros(len(observed_index), dtype=float)}, index=observed_index)
    # `format_fitted_cdf_values_for_fitting_copula` names the modelled statistics from its own table, so this frame
    # only has to carry the columns the cell's `surge_peak_ft` statistic is selected by.
    simulated_events = pd.DataFrame(
        {
            "year": [1],
            "event_id": [1],
            "rain_event": [True],
            "surge_event": [True],
            "compound_event": [True],
            "copula_source": ["compound"],
            "precip_depth_mm": [0.0],
            "max_4hr_mean_mm_per_hr": [1.0],
            "surge_peak_ft": [simulated_peak],
        },
        index=pd.Index([0], name="sim_event_id"),
    )

    def reduce(finished):
        """The simulated event's surge series, as the cell wrote it into its own time-series dataset."""
        frame = finished["ds_sim_event_tseries"].to_dataframe()
        return frame["surge_ft"].to_numpy(dtype=float)

    with seeded_global_numpy(SEED):
        observed = implementation(
            STORM_RESCALING_CELL,
            None,
            reduce,
            {
                "dic_event_summaries": {"compound_events": observed_events[["event_start", "event_end"]].copy()},
                "dic_simulations": {"compound_events": simulated_events.copy()},
                "df_all_events_summary": observed_events.copy(),
                "df_obs_mrms": observed_mrms.copy(),
                "ds_event_tseries": event_series,
                "dir_over_sim": f"{tmp_path}/all_sims_rescaled/",
                "n_neighbors": 1,
            },
        )

    expected = np.asarray(observed_surge, dtype=float) + (observed_peak - simulated_peak)

    check(request, observed, expected, TOLERANCE)


# E-26's literal inputs. The cell reads three tables and one dataset: the per-year simulated event counts, the rescaled
# per-event summaries, the modelled statistics' names, and the rescaling cell's written time series. Each is supplied
# literally below, so the shifted series the cell returns depends on no committed fixture.
#
# The surge's index and the rainfall's are the same before the shift and different after it, so the two series' union
# has gaps once the cell has moved the surge; the cell fills them from the observed water-level record, which is why
# that record is built from the same literals as the simulated surge.
LAG_TARGET_H = 2.0  # a whole number of 1-hour time steps, so the cell's `round(tstep)` lands on it exactly
SURGE_STEPS = pd.to_timedelta(np.arange(-2, 9), unit="h")
# The surge series in the alignment the cell ends up giving it: its peak is at +5 h. The dataset carries it two steps
# earlier (-2 h .. +8 h), because the cell moves it forward by the target lag before the comparison.
SHIFTED_SURGE = [0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 3.0, 5.0, 3.0, 1.0, 0.0]
# The rainfall series, whose peak is at +5 h too: after the surge moves by the target lag the two peaks are
# `LAG_TARGET_H` apart, which is what the test reads back.
SIMULATED_RAIN = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 20.0, 5.0, 0.0, 0.0]
OBSERVED_EVENT_START = pd.Timestamp("2020-06-01 00:00")
OBSERVED_EVENT_END = pd.Timestamp("2020-06-01 22:00")
SIMULATED_PEAK_FT = 5.0
OBSERVED_PEAK_FT = 5.0


def _water_levels() -> xr.Dataset:
    """The observed record the cell's filling and tide steps read: hourly 2018-2020, one surge event, a 24-hour tide."""
    index = pd.date_range("2018-01-01", "2020-12-31 23:00", freq="1h", name="date_time")
    hours = np.arange(len(index), dtype=float)
    # One observed surge event, whose window is `OBSERVED_EVENT_START` .. +10 h and whose values are the shifted
    # series' own: the cell compares the simulated surge against this window plus the peak adjustment, so the two
    # sides agree only when the record is built from the same literals.
    surge = np.zeros(len(index), dtype=float)
    window = index.get_indexer(pd.date_range(OBSERVED_EVENT_START, periods=len(SHIFTED_SURGE), freq="1h"))
    surge[window] = np.asarray(SHIFTED_SURGE, dtype=float)
    tide = 0.2 * np.sin(2.0 * np.pi * hours / 24.0)
    return xr.Dataset(
        {
            "surge_ft": ("date_time", surge),
            "tide_ft": ("date_time", tide),
            "waterlevel_ft": ("date_time", surge + tide),
        },
        coords={"date_time": index},
    )


@pytest.mark.conformance(unit="E-26", part="test_eq26_lag_shift", system="publication-math-conformance")
def test_eq26_lag_shift(request: pytest.FixtureRequest, tmp_path: pathlib.Path) -> None:
    r"""E-26: the surge series is shifted so the surge peak sits the generated time delay after the rain peak.

    The register's span for this equation, verbatim:

    Shift the time series to match the generated time delay statistic between $PR_{g}$ and $PS_{g}$.

    Discriminating inputs. One combined event whose target lag is 2 h, a whole number of the series' 1-hour steps, so
    the cell's `round(tstep)` lands on the target exactly and a wrong shift is a whole step out rather than a rounding
    of a fraction. The simulated surge's index carries the two steps the shift will move it by, so the surge and the
    rainfall stop covering one another once the cell has moved the surge and the cell's filling step completes the
    series from the observed record it is handed. The two peaks start level in those inputs, so the cell has to move
    the surge by the full target rather than by a difference the shift happens to make small.

    The value compared is the peak lag read back off the returned series, not the lag the cell reports in its summary
    table. The cell writes the achieved lag into its own summary, so a test that read it there would compare the code
    with itself; the reference computes the lag from the two peaks of the series the cell returned.

    The register's record for this row is that window durations are recovered by parsing the column name (`_b7:532-533`).
    The target column below is the instantaneous-peak form, `surge_peak_after_rain_peak_h_target`, which the cell
    handles without parsing a duration; the max-mean form is not this row's subject and its window would have to be
    authored at the same time as the two durations it names.

    This row is the family's series-assembly green: the register records no departure for the instantaneous-peak branch,
    and the literal lag is one the cell can reach exactly.
    """
    simulated_event = xr.Dataset.from_dataframe(
        pd.DataFrame(
            {
                "surge_ft": np.asarray(SHIFTED_SURGE, dtype=float),
                "mm_per_hr": np.asarray(SIMULATED_RAIN, dtype=float),
            },
            index=pd.MultiIndex.from_arrays(
                [np.zeros(len(SHIFTED_SURGE), dtype=int), np.zeros(len(SHIFTED_SURGE), dtype=int), SURGE_STEPS],
                names=["sim_event_id", "rescaling_attempt", "timestep"],
            ),
        )
    )
    # The rescaled summaries the cell filters, in the columns it reads: the three modelled statistics with and without
    # the `_target` suffix, the two problem flags, the observed window, and the lag target.
    summaries = pd.DataFrame(
        {
            "year": [1],
            "event_id": [1],
            "compound_event": [True],
            "copula_source": ["compound"],
            "surge_peak_ft_target": [SIMULATED_PEAK_FT],
            "precip_depth_mm_target": [float(np.sum(SIMULATED_RAIN))],
            "max_4hr_mean_mm_per_hr_target": [float(np.max(SIMULATED_RAIN))],
            "surge_peak_after_rain_peak_h_target": [LAG_TARGET_H],
            "surge_peak_ft": [SIMULATED_PEAK_FT],
            "precip_depth_mm": [float(np.sum(SIMULATED_RAIN))],
            "max_4hr_mean_mm_per_hr": [float(np.max(SIMULATED_RAIN))],
            "surge_rescaling_problems": [np.nan],
            "rain_rescaling_problems": [np.nan],
            "obs_event_start_surge": [OBSERVED_EVENT_START],
            "obs_event_end_surge": [OBSERVED_EVENT_START + pd.Timedelta(len(SHIFTED_SURGE) - 1, "h")],
            "obs_event_start_rain": [OBSERVED_EVENT_START],
            "obs_event_end_rain": [OBSERVED_EVENT_START + pd.Timedelta(len(SHIFTED_SURGE) - 1, "h")],
            "obs_surge_peak_ft": [OBSERVED_PEAK_FT],
            "obs_precip_depth_mm": [float(np.sum(SIMULATED_RAIN))],
            "obs_rain_intensity_stat": [float(np.max(SIMULATED_RAIN))],
            "sim_surge_duration": [pd.Timedelta(len(SHIFTED_SURGE) - 1, "h")],
            "sim_rain_duration": [pd.Timedelta(len(SHIFTED_SURGE) - 1, "h")],
        },
        index=pd.MultiIndex.from_tuples([(0, 0)], names=["sim_event_id", "rescaling_attempt"]),
    )
    counts = pd.DataFrame({"any": [1], "rain": [0], "surge": [0], "compound": [1]}, index=pd.Index([1], name="year"))
    # `format_fitted_cdf_values_for_fitting_copula` pivots this long table into the modelled statistics' names; the
    # four modelled statistics are the cell's own column names for this event type.
    modelled_statistics = pd.DataFrame(
        {
            "observation_id": np.tile(np.arange(1, len(SHIFTED_SURGE) + 1), 4),
            "data": np.repeat(
                ["surge_peak_ft", "precip_depth_mm", "max_4hr_mean_mm_per_hr", "surge_peak_after_rain_peak_h"],
                len(SHIFTED_SURGE),
            ),
            "cdf_fit": np.linspace(0.05, 0.95, 4 * len(SHIFTED_SURGE)),
        }
    )
    water_levels = _water_levels()

    class PdBinding:
        """pandas as the cell names it, except that `read_csv` answers with these literal tables."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pd, name)

        def read_csv(self, path, **kwargs):
            """The literal table the named file stands for, read back through pandas so the cell's own converters apply."""
            name = str(path).rsplit("/", 1)[-1]
            if name.endswith("_simulation_summaries_all.csv"):
                table = summaries
            elif name.startswith("simulated_event_counts_"):
                table = counts
            elif name.endswith("_empirical_vs_fitted.csv"):
                table = modelled_statistics
            else:
                raise FileNotFoundError(f"the test binds no csv named {path}")
            # Written through pandas and read back through pandas, so the index columns, the `converters` and the
            # `parse_dates` the cell passes are the ones a real file would see.
            return pd.read_csv(io.StringIO(table.to_csv()), **kwargs)

    class XrBinding:
        """xarray as the cell names it, except that `open_dataset` answers with these literal datasets."""

        def __getattr__(self, name):
            """Everything but `open_dataset` is xarray's own."""
            return getattr(xr, name)

        def open_dataset(self, path, **kwargs):
            """The observed record for its file name, the rescaling cell's series for any other."""
            if str(path).endswith("wlevel.nc"):
                return water_levels
            if str(path).endswith("_time_series.nc"):
                return simulated_event
            raise FileNotFoundError(f"the test opens no dataset named {path}")

    def reduce(finished):
        """The returned series' surge peak lag behind its rain peak, in hours."""
        series = finished["df_tseries"]
        surge_peak = series["surge_ft"].idxmax()
        rain_peak = series["mm_per_hr"].idxmax()
        return float((surge_peak[-1] - rain_peak[-1]) / np.timedelta64(1, "h"))

    with seeded_global_numpy(SEED):
        observed = implementation(
            FINALISING_CELL,
            None,
            reduce,
            {
                "pd": PdBinding(),
                "xr": XrBinding(),
                "dic_event_summaries": {
                    "compound_events": pd.DataFrame(
                        {"event_start": [OBSERVED_EVENT_START], "event_end": [OBSERVED_EVENT_END]},
                        index=pd.Index([7], name="event_id"),
                    )
                },
                "dic_simulations": {"compound_events": pd.DataFrame({"year": [1], "event_id": [1]})},
                "f_6min_wlevel_resampled_to_mrms_res_nc": "wlevel.nc",
                "dir_over_sim": f"{tmp_path}/all_sims_rescaled/",
                "dir_storm_rescaling": f"{tmp_path}/",
                "dir_simulate_from_copula": f"{tmp_path}/simulated/",
            },
        )

    check(request, observed, LAG_TARGET_H, TOLERANCE)


# E-28's literal inputs. Both cases are 12 hourly steps, so a buffer of whole hours is a whole number of steps.
BUFFER_STEPS = 12
BUFFER_PEAKS_H = 3
BUFFER_BEFORE_FIRST_RAIN_H = 2
BUFFER_RAINDEPTH_FRACTION = 0.1


def _buffer_frame(surge: list[float], rainfall: list[float]) -> pd.DataFrame:
    """A 12-step hourly frame of the two columns the buffering function reads."""
    index = pd.date_range("2020-01-01 00:00", periods=BUFFER_STEPS, freq="1h", name="date_time")
    return pd.DataFrame({"surge_ft": surge, "mm_per_hr": rainfall}, index=index)


@pytest.mark.conformance(unit="E-28", part="test_eq28_series_buffering", system="publication-math-conformance")
def test_eq28_series_buffering(request: pytest.FixtureRequest) -> None:
    r"""E-28: the series is extended by the peak and rainfall buffers the manuscript states.

    The register's span for this equation, verbatim:

    Ensure that each time series extends $D^{\text{buffer}}_{\text{peaks}}$-hours before and after the peak surge and after the last $D^{\text{buffer}}_{\text{peaks}}$-hour period of rainfall producing a fraction of the total rainfall greater than or equal to $R_{\text{frac}}$. Also ensure that each time series extends $D^{\text{buffer}}_{\text{pre}}$-hours before the first timestep with rainfall.

    Discriminating inputs. Two cases, each compared in one value. The first puts the significant rainfall in the last
    quarter of the record, so the periods the rainfall is grouped into decide where the buffer ends: the manuscript
    groups the record's `D_peaks`-hour periods from its first timestep, and the code groups them from its end, so the
    two forms name different periods for the same rainfall and the returned last timestep moves by a step. The second
    case keeps the rainfall clear of that trailing window and puts the surge peak near the record's end and the first
    rainfall one step in, so the peak and pre-rain buffers are exercised and both forms agree; it is the control that a
    value carrying the buffering is representable and that the first case's red is the grouping alone.

    The reading this equation needs. The manuscript does not say where the periods are anchored, so the reference takes
    the conventional reading — the record's own `D_peaks`-hour periods counted from its first timestep — and extends
    the series to the end of the last of them plus the buffer, which is what the code's own arithmetic does with the
    period it names. Anchoring is the only choice the two sides make differently, so the disagreement is attributable
    to the register's recorded departure and not to a second reading.

    The three buffer parameters are literal overrides on the declaration's own bindings, so no value here depends on a
    setting the cell would otherwise inherit.
    """
    cases = {
        # The burst sits wholly inside the record's last `D_peaks`-hour period and its first wet step is a whole number
        # of periods from the record's first timestep, so the first-wet-step and record-start readings name the same
        # period and only the end-anchored grid names another. The end-anchored grid puts the burst's last step in a
        # period of its own, which ends one hour later than the manuscript's.
        "late_rainfall": _buffer_frame(
            [0.0, 0.0, 0.0, 0.0, 0.0, 3.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 6.0, 4.0, 0.0],
        ),
        "early_rainfall_late_peak": _buffer_frame(
            [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3.0, 0.0],
            [0.0, 2.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        ),
    }

    def reduce(finished):
        """The returned index's first and last timestep for each case."""
        buffered = finished["reindex_to_buffer_significant_rain_and_surge"]
        data = {}
        for label, table in cases.items():
            result = buffered(table.copy())
            data[f"{label}__bounds"] = (
                ("case", "bound"),
                np.asarray([[result.index.min(), result.index.max()]], dtype="datetime64[ns]"),
            )
        return xr.Dataset(data)

    observed = implementation(
        BUFFERING_FUNCTION,
        None,
        reduce,
        {
            "timeseries_buffer_around_peaks_h": BUFFER_PEAKS_H,
            "timeseries_buffer_before_first_rain_h": BUFFER_BEFORE_FIRST_RAIN_H,
            "buffer_window_raindepth_fraction_to_trigger_tserties_extension": BUFFER_RAINDEPTH_FRACTION,
        },
    )

    def expected_bounds(table: pd.DataFrame) -> tuple[pd.Timestamp, pd.Timestamp]:
        """The manuscript's three buffers: peak +/- `D_peaks`, the last significant period's end + `D_peaks`, first rain - `D_pre`."""
        index = table.index
        tstep = pd.Series(index).diff().mode().iloc[0]
        d_peaks, d_pre = pd.Timedelta(BUFFER_PEAKS_H, "h"), pd.Timedelta(BUFFER_BEFORE_FIRST_RAIN_H, "h")
        first, last = index.min(), index.max()
        peak = table["surge_ft"].dropna().idxmax()
        if (last - peak) < d_peaks:
            last = peak + d_peaks
        if (peak - first) < d_peaks:
            first = peak - d_peaks
        depths = table["mm_per_hr"].dropna() * (tstep / np.timedelta64(1, "h"))
        nonzero = depths[depths > 0].index
        # The record's own `D_peaks`-hour periods, counted from its FIRST TIMESTEP — the whole record is resampled and
        # not the wet range, because anchoring the periods at the first wet step is a third reading the manuscript's
        # text does not name and one that agrees with the code on this input for the wrong reason.
        periods = depths.resample(d_peaks, origin="start", closed="left").sum()
        significant = periods[periods / depths.sum() >= BUFFER_RAINDEPTH_FRACTION]
        end_of_last_significant_period = significant.index.max() + d_peaks
        if (last - end_of_last_significant_period) < d_peaks:
            last = end_of_last_significant_period + d_peaks
        if (nonzero.min() - first) < d_pre:
            first = nonzero.min() - d_pre
        return first, last

    expected = xr.Dataset(
        {
            f"{label}__bounds": (("case", "bound"), np.asarray([list(expected_bounds(table))], dtype="datetime64[ns]"))
            for label, table in cases.items()
        }
    )

    check(request, observed, expected, compare.Exact())


# E-31's literal inputs. The record spans three complete water-level years, so the manuscript's set of
# `n_tide yrs + 1` years is `{2018, 2019, 2020}` and the code's slice can only drop the most recent one.
TIDE_YEARS_RECORDED = list(range(2018, 2021))
TIDE_YEARS_TO_LOOK_BACK = 2
TIDE_WINDOW_H = 6
TIDE_DRAWS = 40


@pytest.mark.conformance(unit="E-31", part="test_eq31_tide_randomization", system="publication-math-conformance")
def test_eq31_tide_randomization(request: pytest.FixtureRequest) -> None:
    r"""E-31: the year and the phase of the astronomical tide are drawn from the manuscript's two sets.

    The register's span for this equation, verbatim:

    Apply an astronomical tide time series selected within a continuous uniformly distributed $h$-hour window centered on the original observed event, $U \sim \left( -\frac{h}{2}, \frac{h}{2} \right)$. Modify the year of the time series by drawing from a discrete uniform random variable defined by the most recent complete year of water level observations, $y_{r}$,. and a user-defined most recent number of years to draw from, $n_{\text{tide yrs}}$: $U \sim \{y_{r}-n_{\text{tide yrs}},\dots, y_{r}\}$

    Discriminating inputs. The record spans exactly the years the two sets disagree on: `y_r = 2020`, `n = 2`, so the
    manuscript's set is `{2018, 2019, 2020}` and the code's slice `[-(n + 1):-1]` names `{2018, 2019}`, excluding the
    most recent complete year. Forty draws are taken, so a population of two years is visited many times over and the
    observed set is that population rather than a sample of it; the seed fixes which draws those are.

    Both of the row's draws are observed. The year is read from the draw itself: the implementation's `np` binding
    records every `np.random.choice` and every `np.random.uniform` the legacy text makes, so the test sees the
    distribution's outcome rather than a value the function reports about it. The phase shift is the second component,
    and it is compared as the boolean the manuscript's `U ~ (-h/2, h/2)` states.

    A function call draws one year, so the value is built from repeated calls on one literal water-level record. Every
    window the drawn year could name exists in that record, which is why the retry that drops an unusable year on its
    own path never fires: the compared set is the draw's population, not the retry's exhaustion.
    """
    index = pd.date_range("2018-01-01", "2020-12-31 23:00", freq="1h", name="date_time")
    hours = np.arange(len(index), dtype=float)
    tide = 0.2 * np.sin(2.0 * np.pi * hours / 24.0)
    water_levels = xr.Dataset(
        {
            "surge_ft": ("date_time", np.zeros(len(index), dtype=float)),
            "tide_ft": ("date_time", tide),
            "waterlevel_ft": ("date_time", tide),
        },
        coords={"date_time": index},
    )
    surge = pd.Series(np.zeros(6, dtype=float), index=pd.to_timedelta(np.arange(6), unit="h"), name="surge_ft")
    observed_start = pd.Timestamp("2020-06-01 00:00")
    observed_end = pd.Timestamp("2020-06-01 05:00")

    draws: list[tuple[str, float]] = []

    class RecordingRandom:
        """numpy's own `random` module, recording the two draws the legacy text makes."""

        def uniform(self, low, high, size=None):
            """Draw the phase shift, record it, and hand it on."""
            value = np.random.uniform(low=low, high=high, size=size)
            draws.append(("uniform", float(value)))
            return value

        def choice(self, a, *args, **kwargs):
            """Draw the year, record it, and hand it on."""
            value = np.random.choice(a, *args, **kwargs)
            draws.append(("choice", float(value)))
            return value

        def __getattr__(self, name):
            """Every other draw or generator call is numpy's own."""
            return getattr(np.random, name)

    class NpBinding:
        """numpy as the legacy text names it, with `random` recorded."""

        def __init__(self):
            """Hold the recording `random` stand-in."""
            self.random = RecordingRandom()

        def __getattr__(self, name):
            """Everything but `random` is numpy's own."""
            return getattr(np, name)

    def reduce(finished):
        """The set of years the repeated calls drew, and whether every phase shift stayed inside the window."""
        function = finished["generate_randomized_tide_component"]
        for _ in range(TIDE_DRAWS):
            function(surge, water_levels, TIDE_WINDOW_H, observed_start, observed_end)
        years = [int(value) for kind, value in draws if kind == "choice"]
        shifts = [value for kind, value in draws if kind == "uniform"]
        return xr.Dataset(
            {
                "years": ("year", np.asarray(sorted(set(years)), dtype=int)),
                "every_shift_within_half_window": (
                    (),
                    bool(all(abs(shift) <= TIDE_WINDOW_H / 2 for shift in shifts)),
                ),
            }
        )

    with seeded_global_numpy(SEED):
        observed = implementation(
            TIDE_FUNCTION,
            None,
            reduce,
            {"np": NpBinding(), "n_years_to_look_back_for_tidal_tseries": TIDE_YEARS_TO_LOOK_BACK},
        )

    most_recent_year = max(TIDE_YEARS_RECORDED)
    expected = xr.Dataset(
        {
            "years": (
                "year",
                np.asarray(
                    sorted(
                        range(
                            most_recent_year - TIDE_YEARS_TO_LOOK_BACK,
                            most_recent_year + 1,
                        )
                    ),
                    dtype=int,
                ),
            ),
            "every_shift_within_half_window": ((), True),
        }
    )

    check(request, observed, expected, compare.Exact())
