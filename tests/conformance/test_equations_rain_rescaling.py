r"""The six rain-rescaling equations of the SWG supplement, each checked against the code that runs it.

Every function here is one equation of the supplementary manuscript's rain-rescaling stretch — E-04's
watershed mean intensity, and the five steps E-21..E-25 by which a generated event's rainfall time series
is dilated to its target depth and rescaled back to the original timestep — named by the name the register
proposes for it and carrying that equation's id in its conformance marker. The observed side is the legacy
unit that executes the published mathematics today, reached as its stored extraction through the lift
harness and run by `equation_check.implementation` (nothing under `src/` implements these equations yet).
The reference side is the manuscript's formula, computed in the test's own body over literal inputs. One
`check` call compares them, so a disagreement arrives as a `ConformanceDivergence` carrying the ledger's
binding for that row.

Four of the six diverge, and each divergence is the register's own record rather than a surprise: E-04
identifies the grid cells by column negation, so a column that is not a rainfall field enters the
watershed mean; E-23 weights each resampled window by overlap duration where the manuscript says "the
average"; E-24 asks `np.isclose`, whose comparison is `<=`, where the manuscript writes a strict `<`; and
E-25 stops at `n_cutoff` attempts and returns a series that does not meet the test it exists to meet.
E-21 and E-22 are green: the intensity multiplier is applied to exactly the vector the manuscript names,
and the dilated time step is the depth ratio the manuscript defines.

The constant series E-21 and E-24 use is a deliberate choice and not a convenience. E-21's `PR_h` and the
code's four-hour-mean statistic are the same number on a constant series, so the multiplier carries one
quantity and not two; and E-24's depth arithmetic is the same under the manuscript's plain average and under
the code's overlap-weighted one, so its red is about the stopping test and not a second copy of E-23's
divergence. E-25 uses a varying series instead, because a constant series' resampled depth is a fixed point
below any target it misses, so the cap's presence would not change the series the caller receives and the
row could not see the departure it records.

Two readings belong to the rows rather than to the manuscript's prose, and are recorded where each is used:
the number of windows a dilated series is written back on (the stored text's convention at `_utils:52`,
which the rows for E-22, E-24 and E-25 fix; the manuscript's E-23 sentence states no count), and E-24's
application of the printed inequality to the series' depth (the stored loop applies the same tolerance test
to the depth and to the peak intensity, so the `<`/`<=` departure is present on both quantities).

What this module does NOT establish: that the class a reader assigns to each divergence (D1..D9) is right.
A divergence record carries the two values and the ledger's binding; class assignment is
`taxonomy.classify` over an authored diagnosis, and no test here authors one. Nor does it establish that
the literal inputs span each equation's domain: each row names the case it discriminates, and that case is
the divergence it exists to see.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from tests.conformance import equation_check
from tests.correspondence import compare

# The system slug every function's marker names is written as a literal at each decorator rather than
# through a module constant, because `tests.correspondence.inventory` reads markers from the source without
# importing it, and a name where the value should be is a decorator it refuses to list.

# The two legacy units these six equations run. E-22 and E-23 are the dilation helper; E-04, E-21, E-24 and
# E-25 are the rescaling loop, whose first act in each pass is the watershed mean E-04 states (`_utils:117`
# picks the columns, `_utils:125-138` averages them), so E-04 has no unit of its own.
DILATION_UNIT = "_utils.py:29-95"
RESCALING_UNIT = "_utils.py:104-171"

# The intensity statistic the legacy loop recomputes and tests, under the name `_utils:1540-1577` gives it:
# the largest four-hour mean intensity. The manuscript calls the same quantity the peak intensity `PR`.
INTENSITY_STAT = "max_4hr_mean_mm_per_hr"

# The original time step every literal series here carries, and the window the manuscript averages over.
TSTEP = pd.Timedelta("1h")

# The columns E-04's sum runs over. The third column of that frame is not a rainfall field, which is the
# point of the E-04 case: the code's negation cannot tell the two apart.
GRID_CELLS = ("grid_cell_a", "grid_cell_b")

# The comparison policy these tests take by default, the equivalence family's own: numbers within
# `1e-9 * |reference|`, and every other kind exact. A text decision carried beside a number in one value is
# still decided exactly, so the one-`check`-call convention holds on a row whose value has two components.
TOLERANCE = compare.Tolerance(rtol=1e-9, atol=0.0)


def _hourly(columns: Mapping[str, Sequence[float]]) -> pd.DataFrame:
    """A frame of hourly steps whose index is the time the legacy text rolls over.

    The rolling statistic the loop computes needs a time index rather than a range index, and every
    literal series here is hourly, which is the step the stored fixture uses.
    """
    length = len(next(iter(columns.values())))
    index = pd.date_range("2020-01-01 00:00", periods=length, freq=TSTEP, name="date_time")
    return pd.DataFrame({name: np.asarray(values, dtype=float) for name, values in columns.items()}, index=index)


@pytest.mark.conformance(unit="E-04", part="test_eq04_watershed_mean_intensity", system="publication-math-conformance")
def test_eq04_watershed_mean_intensity(request: pytest.FixtureRequest) -> None:
    r"""E-04: the watershed mean intensity, `\bar P(t) = (1/n_g)\sum_{i=1}^{n_g} P(x,y,t)`.

    The register records the divergence: `_utils:117` identifies the grid-cell columns by NEGATION — every
    column whose name does not contain `mm_per_hr` counts as a grid cell — so a column that is not a
    rainfall field at all enters the watershed mean, and the series the loop returns is an average over it.

    The case that sees it is a frame whose two genuine grid-cell columns sum to a constant 4.0 mm/h at every
    step, beside a third column of 10.0 that is not rainfall at all. The code's mean over the three is
    4.0 mm/h where the manuscript's mean over the two grid cells is 2.0 mm/h, so the extra column moves the
    series the loop returns. A frame whose extra column happened to equal the grid cells' mean could not see
    the divergence, and the test would be a vacuous green.

    The reference is the equation's identity read at the state the loop RETURNS: the returned `mm_per_hr`
    against the mean of the returned grid-cell columns. It is deliberately not the mean of the input cells,
    because the loop's first act multiplies every column by the ratio of the target intensity to the
    series' own four-hour mean; the returned cells are the input cells rescaled, and only the returned pair
    makes the identity a statement about `n_g` alone. On a correct grid-cell mean the returned pair agrees
    whatever the multiplier is; on the code's negation the pair disagrees at every step.

    The target is the frame's own statistic, so the loop accepts on its first pass and the returned series
    is the mean the code computed rather than a rescaling artefact: the target intensity is the frame's own
    four-hour-mean maximum (4.0 mm/h, taken from the column the code has already averaged over the three)
    and the target depth is the frame's own depth (24.0 mm), which makes the multiplier 1.0 and the dilation
    1.0 h. The loop's other two exits are pushed out of reach with a maximum storm duration far beyond the
    literal series and the default attempt cap.
    """
    frame = _hourly(
        {
            "grid_cell_a": [1.0, 2.0, 3.0, 2.0, 1.0, 2.0],
            "grid_cell_b": [3.0, 2.0, 1.0, 2.0, 3.0, 2.0],
            "temperature_c": [10.0] * 6,
            "mm_per_hr": [4.0] * 6,
        }
    )
    target_intensity = 4.0
    target_depth = 24.0

    def reduce(finished: Mapping[str, Any]) -> Any:
        """Run the legacy loop on the literal frame and reduce to the series it returns."""
        series, _problems = finished["rescale_rainfall_timeseries"](
            pd.Series({"precip_depth_mm": target_depth, INTENSITY_STAT: target_intensity}),
            frame.copy(),
            pd.Timedelta("48h"),
            50,
            1e-9,
            1e-12,
            INTENSITY_STAT,
        )
        return series

    series = equation_check.implementation(RESCALING_UNIT, None, reduce)
    observed = series["mm_per_hr"].to_numpy(dtype=float)
    expected = series[list(GRID_CELLS)].mean(axis=1).to_numpy(dtype=float)

    equation_check.check(request, observed, expected, TOLERANCE)


@pytest.mark.conformance(unit="E-21", part="test_eq21_intensity_multiplier", system="publication-math-conformance")
def test_eq21_intensity_multiplier(request: pytest.FixtureRequest) -> None:
    r"""E-21: the intensity multiplier, `\vec r_g = \vec r_h PR_g/PR_h`.

    The register records no defect for this row: `_utils:122-123` scales every grid-cell column by the
    ratio of the simulated event's peak intensity to the observed series' own, which is the vector the
    equation names.

    The literal series is constant, which is what makes the ratio unambiguous: on a constant series the
    pointwise peak the manuscript writes and the four-hour mean the code measures are the same number, so
    `PR_h = 2.0` and `PR_g = 3.0` fix the multiplier at 1.5 rather than at a ratio of two different
    statistics. The target depth is `PR_g/PR_h` times the observed series' depth, so the dilation is exactly
    1.0 h and the multiplier alone shapes the returned series — which is this row's subject and not E-22's.

    The observed value is the returned `mm_per_hr`, rendered as a float array; the reference is the
    observed series times the ratio, computed over the literals.
    """
    observed_series = np.full(5, 2.0)
    peak_observed = 2.0
    peak_simulated = 3.0
    target_depth = float((peak_simulated / peak_observed) * observed_series.sum())
    frame = _hourly({"grid_cell_a": observed_series, "grid_cell_b": observed_series, "mm_per_hr": observed_series})

    def reduce(finished: Mapping[str, Any]) -> Any:
        """Run the legacy loop on the literal frame and reduce to the series it returns."""
        series, _problems = finished["rescale_rainfall_timeseries"](
            pd.Series({"precip_depth_mm": target_depth, INTENSITY_STAT: peak_simulated}),
            frame.copy(),
            pd.Timedelta("48h"),
            50,
            1e-9,
            1e-12,
            INTENSITY_STAT,
        )
        return series

    series = equation_check.implementation(RESCALING_UNIT, None, reduce)
    observed = series["mm_per_hr"].to_numpy(dtype=float)
    expected = observed_series * peak_simulated / peak_observed

    equation_check.check(request, observed, expected, TOLERANCE)


@pytest.mark.conformance(unit="E-22", part="test_eq22_timestep_dilation", system="publication-math-conformance")
def test_eq22_timestep_dilation(request: pytest.FixtureRequest) -> None:
    r"""E-22: the dilated time step, `\Delta t' = TD_g/\sum_t r_{g,t}`.

    The register records no defect for this row: `_utils:44` divides the target depth by the series' own
    sum, and `_utils:45-47` re-indexes the series at that step. The step is never returned as a number, so
    the returned series' LENGTH is the only carrier of it: a series of `n` dilated steps starting at zero,
    written back on the original grid, holds `\lfloor (n-1)\Delta t'/\Delta t \rfloor + 1` windows.

    That count is the stored text's own convention (`_utils:52` stops the grid at the start of the last
    dilated step) and the row for this equation fixes it as the reference; the manuscript's sentence states
    no count, and a reading that covered the whole dilated span (`n\Delta t'`) would carry one window more
    and make this row red on the length alone. The reading is recorded here so the red or green this row
    reports is read as the dilated STEP, which is the equation's subject, rather than as the count.

    The literal series is twelve steps of 2.0 mm/h and the target depth is twice its sum, so `\Delta t'`
    is 2.0 h against a 1.0 h window and the returned length is `\lfloor 11 \times 2.0/1.0 \rfloor + 1 = 23`.
    Twelve steps is what makes the length discriminating: a five-per-cent error in `\Delta t'` (2.1 h) gives
    24 at twelve steps, where at four steps it would give 7 against 7 and the test could not see it. The
    values are 2.0 at every window whatever `\Delta t'` is — a constant dilated series has the same plain
    average in every window — so they are compared beside the length for completeness and carry nothing
    about the step, and the length is an integer, which no tolerance can loosen into a different integer.

    The observed value is the returned length beside the returned values, rendered as one `Dataset`; the
    reference is the manuscript's step over the same literals.
    """
    values = np.full(12, 2.0)
    target_depth = 2.0 * float(values.sum())
    frame = _hourly({"mm_per_hr": values})
    # The manuscript's step b, over the literals: the dilated step is the target depth over the series' sum.
    delta_t_prime = pd.Timedelta(hours=target_depth / float(values.sum()))

    def reduce(finished: Mapping[str, Any]) -> Any:
        """Run the legacy dilation on the literal series and reduce to its length and values."""
        series = finished["rescale_rain_tseries_to_match_target_depth"](frame.copy(), target_depth, TSTEP)
        return series

    series = equation_check.implementation(DILATION_UNIT, None, reduce)
    observed_length = int(len(series))
    observed_values = series["mm_per_hr"].to_numpy(dtype=float)

    # The manuscript's steps b and c over the literals: `\lfloor (n-1)\Delta t'/\Delta t \rfloor + 1`
    # windows, each carrying the dilated values' plain average — 2.0 in every window, because the series
    # is constant.
    expected_length = int((len(values) - 1) * delta_t_prime // TSTEP) + 1
    expected_values = np.full(expected_length, float(values[0]))

    def rendered(length: int, window_values: np.ndarray) -> xr.Dataset:
        """One value holding the window count, an integer, beside the values each window carries."""
        return xr.Dataset({"n_steps": ((), length), "values": (("step",), window_values)})

    equation_check.check(
        request,
        rendered(observed_length, observed_values),
        rendered(expected_length, expected_values),
        TOLERANCE,
    )


@pytest.mark.conformance(unit="E-23", part="test_eq23_overlap_weighted_resample", system="publication-math-conformance")
def test_eq23_overlap_weighted_resample(request: pytest.FixtureRequest) -> None:
    r"""E-23: resample back to the original timestep "by taking the average of `\vec r_g` for each `\Delta t` window".

    The register records the divergence: `_utils:64-89` sums each dilated step's value weighted by the
    DURATION of its overlap with the window, which is not the plain average the sentence asks for once a
    dilated step straddles a window boundary and the neighbouring steps carry different values.

    The case that sees it is a nine-step series of distinct values dilated to `\Delta t' = 1.25 h`,
    so that the quarter-hour boundaries fall inside the hourly windows: window 1 holds 0.25 of one step and
    0.75 of the next, where the durations weight the two at 1:3 and the manuscript's average weights them at
    1:1. An integer `\Delta t'` would align the steps with the windows and a constant series would give the
    same number under both readings; either would make this a vacuous green. Windows that hold a single
    dilated step are also present, so the difference is read against windows where both forms agree.

    The observed value is the returned `mm_per_hr`, rendered as a float array; the reference is the plain
    mean of the dilated values overlapping each window, from the literals. The windows are the grid the row
    for E-22 fixes — the stored text's count, since the manuscript's sentence states none — so the
    comparison is between two series written on the same grid and the difference it reports is the weighting
    alone.
    """
    values = np.asarray([4.0, 1.0, 3.0, 2.0, 5.0, 1.5, 4.5, 2.5, 3.5], dtype=float)
    target_depth = 33.75
    frame = _hourly({"mm_per_hr": values})
    delta_t_prime = pd.Timedelta(hours=target_depth / float(values.sum()))

    def reduce(finished: Mapping[str, Any]) -> Any:
        """Run the legacy dilation on the literal series and reduce to the resampled values."""
        series = finished["rescale_rain_tseries_to_match_target_depth"](frame.copy(), target_depth, TSTEP)
        return series["mm_per_hr"].to_numpy(dtype=float)

    observed = equation_check.implementation(DILATION_UNIT, None, reduce)

    # The manuscript's steps b and c over the literals: the windows are the original grid from zero to the
    # start of the last dilated step, and each one carries the PLAIN mean of the dilated values it overlaps
    # for a positive duration. A step that only touches a window at its boundary contributes nothing to it.
    n_windows = int((len(values) - 1) * delta_t_prime // TSTEP) + 1
    expected = np.asarray(
        [
            float(
                np.mean(
                    [
                        float(values[step])
                        for step in range(len(values))
                        if min(step * delta_t_prime + delta_t_prime, (window + 1) * TSTEP)
                        > max(step * delta_t_prime, window * TSTEP)
                    ]
                )
            )
            for window in range(n_windows)
        ],
        dtype=float,
    )

    equation_check.check(request, observed, expected, TOLERANCE)


@pytest.mark.conformance(unit="E-24", part="test_eq24_isclose_tolerance", system="publication-math-conformance")
def test_eq24_isclose_tolerance(request: pytest.FixtureRequest) -> None:
    r"""E-24: the depth test `|\hat{PR}_g - PR_g| < \text{tol}_r|\cdot|PR_g| + \text{tol}_a`.

    The register records no defect for this row; the divergence is inferred from the text: `_utils:154`
    asks `np.isclose`, whose comparison is `<=`, where the manuscript writes a strict `<`. At EQUALITY the
    two forms therefore part: the code accepts and the manuscript's test has not passed.

    The case that sees it is built so the depth difference meets a literal `atol` exactly. A
    four-step constant series of 2.0 mm/h has a depth of 8.0 mm; the target depth is a literal 10.0 mm, so
    `\Delta t'` is 1.25 h and the series written back on the hourly grid — the window count of the stored
    text's convention, as E-22 records — carries four windows of 2.0 mm/h, a depth of 8.0 mm. The difference
    is `|8.0 - 10.0| = 2.0`, exactly the `atol` with `rtol = 0`, so the code's `<=` accepts while
    `2.0 < 2.0` refuses. The constant series is what keeps this row's red its own: under the manuscript's
    plain average and the code's overlap-weighted one the depth is the same 8.0, so the difference read here
    is not E-23's divergence wearing E-24's id.

    The printed inequality is written for the difference between the achieved and the target peak
    intensity, and it is read here on the series' depth: the stored loop runs the same `rtol`, `atol` and
    `np.isclose` on the depth and on the peak intensity alike, so the `<`/`<=` departure is present on both
    quantities and the depth is the one whose difference the literals make meet the `atol` exactly.

    The value carries the depth the loop measured beside its decision, because the two are read together:
    if the depth were not 8.0 the equality the row turns on would not be the one compared, and the
    reference's depth is the manuscript's own steps over the literals. The decision itself is the code's
    observable — its returned problem text, empty exactly when the loop accepted rather than broke out.
    """
    values = np.full(4, 2.0)
    target_depth = 10.0
    target_intensity = 2.0
    rtol = 0.0
    atol = 2.0
    # The loop rebuilds `mm_per_hr` as the mean of the grid-cell columns on every pass, so the frame
    # carries the cell the series comes from: a frame of `mm_per_hr` alone would average no columns and
    # the statistic would be NaN rather than this row's subject.
    frame = _hourly({"grid_cell_a": values, "mm_per_hr": values})
    # The manuscript's step b over the literals.
    delta_t_prime = pd.Timedelta(hours=target_depth / float(values.sum()))

    def reduce(finished: Mapping[str, Any]) -> Any:
        """Run the legacy loop on the literal frame and reduce to its decision and its series."""
        series, problems = finished["rescale_rainfall_timeseries"](
            pd.Series({"precip_depth_mm": target_depth, INTENSITY_STAT: target_intensity}),
            frame.copy(),
            pd.Timedelta("48h"),
            50,
            rtol,
            atol,
            INTENSITY_STAT,
        )
        return problems, series

    problems, series = equation_check.implementation(RESCALING_UNIT, None, reduce)
    observed = (
        "accepted" if problems == "" else "refused",
        float(series["mm_per_hr"].sum() * (TSTEP / pd.Timedelta("1h"))),
    )

    # The manuscript's steps b and c over the literals: the windows are the original grid up to the start of
    # the last dilated step, and each carries the plain average 2.0 of the constant series, so the resampled
    # depth is that many windows times one hour.
    n_windows = int((len(values) - 1) * delta_t_prime // TSTEP) + 1
    depth_by_paper = float(n_windows * values[0] * (TSTEP / pd.Timedelta("1h")))
    expected = (
        "accepted" if abs(depth_by_paper - target_depth) < rtol * abs(target_depth) + atol else "refused",
        depth_by_paper,
    )

    def rendered(decision: str, depth: float) -> xr.Dataset:
        """One value holding the loop's decision, which is text, beside the depth the test weighs."""
        return xr.Dataset({"decision": ((), decision), "depth_mm": ((), depth)})

    equation_check.check(request, rendered(*observed), rendered(*expected), compare.Exact())


@pytest.mark.conformance(unit="E-25", part="test_eq25_rescaling_fixed_point", system="publication-math-conformance")
def test_eq25_rescaling_fixed_point(request: pytest.FixtureRequest) -> None:
    r"""E-25: "Until the test is passed, update the peak intensity with `\vec r_g = \vec r_g PR_g/\hat{PR}_g`".

    The register records the divergence: the manuscript states no termination other than success, while
    `_utils:143-148` stops after `n_cutoff` attempts and `_utils:149-153` stops when the series reaches
    twice the maximum observed storm length, and both exits return the event with a problem text.

    The case that sees the cap is one the loop meets on a later attempt and not on its first. The six-step
    series rises to a single 6.0 mm/h step in its fourth hour, the target depth is 20.0 mm and the target
    intensity 3.0 mm/h. Its first pass returns a series 1.0 mm short of the target — the dilation leaves a
    tail the window grid does not cover, and the depth test refuses at a relative tolerance of `1e-9` — so
    at one attempt the loop returns that short series with its cutoff text, while the same loop with the
    default cap reaches 19.999999986 mm against 20.0 mm, inside the tolerance, before its cap is reached.
    The cap's presence therefore changes the series the caller receives, which is what this row's red must
    show.

    A constant series cannot make that showing, and is not used here for that reason: its resampled depth is
    a fixed point below any target it misses, so one attempt and fifty return the same unmet series, and the
    red would be a departure the register does not name. E-24's constant series is safe for E-24 because that
    row reads the equality the tolerance test admits, not the loop's termination.

    The returned depth is the sum of the series the code writes back, whose window count is the stored text's
    own convention (`_utils:52`; the manuscript's sentence states no count). The verdict does not turn on
    that count: at one attempt the shortfall is 1.0 mm, five per cent of the target, which no window count
    brings inside the tolerance, and the converged series meets it on either count.

    The observed value is whether the returned series meets the test — the manuscript's inequality applied
    to the returned depth — and the reference is `meets`, because a loop that stops only on success never
    returns a series that does not meet it.
    """
    values = np.asarray([2.0, 2.0, 2.0, 6.0, 2.0, 2.0], dtype=float)
    target_depth = 20.0
    target_intensity = 3.0
    rtol = 1e-9
    atol = 0.0
    # As in E-24, the loop rebuilds `mm_per_hr` from the grid-cell columns, so the frame carries the cell
    # the series comes from; a frame of `mm_per_hr` alone would average no columns.
    frame = _hourly({"grid_cell_a": values, "mm_per_hr": values})

    def reduce(finished: Mapping[str, Any]) -> Any:
        """Run the legacy loop at a cap of one attempt and reduce to its returned series."""
        series, _problems = finished["rescale_rainfall_timeseries"](
            pd.Series({"precip_depth_mm": target_depth, INTENSITY_STAT: target_intensity}),
            frame.copy(),
            pd.Timedelta("48h"),
            1,
            rtol,
            atol,
            INTENSITY_STAT,
        )
        return series

    series = equation_check.implementation(RESCALING_UNIT, None, reduce)
    returned_depth = float(series["mm_per_hr"].sum() * (TSTEP / pd.Timedelta("1h")))
    observed = "meets" if abs(returned_depth - target_depth) < rtol * abs(target_depth) + atol else "does not meet"
    expected = "meets"

    def rendered(outcome: str) -> xr.Dataset:
        """One value holding whether the returned series meets the test the manuscript iterates to."""
        return xr.Dataset({"outcome": ((), outcome)})

    equation_check.check(request, rendered(observed), rendered(expected), compare.Exact())
