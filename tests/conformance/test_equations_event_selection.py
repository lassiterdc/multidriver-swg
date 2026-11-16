r"""Conformance tests of the three event-selection equations, E-05, E-08 and E-09.

Each test runs the legacy unit that implements its equation through `equation_check.implementation`,
computes the value the MANUSCRIPT's formula gives over the same literal inputs in its own body, and
compares the two through `check`. Nothing here reaches a legacy text outside the lift harness, and
nothing here reads an expected value out of the code: the reference is the equation, transcribed.

All three rows are **DIVERGENT** in the register, so all three tests are red on build, and each red is
a `ConformanceDivergence` naming the equation — a recorded paper/code disagreement, not a defect of
this file (AD-7). A green here would mean one of the three recorded divergences is not observable
through its own entry point, which is the outcome these tests exist to prevent.

What this does NOT establish: which artifact is authoritative where the paper and the code disagree.
A divergence records the disagreement; the class assignment stays with the classifier over an authored
`Diagnosis`.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd
import pytest

from tests.conformance import equation_check
from tests.correspondence import compare

# The three units `tests/lifts/combined_event_identification.py` declares, each with `part=None`, so
# the pair (unit, part) names exactly one declaration per equation.
RAIN_SELECTION_UNIT = "_utils.py:1269-1422"
SURGE_SELECTION_UNIT = "_utils.py:1423-1539"
COMPOUND_PAIRING_UNIT = "_utils.py:1613-1785"


@pytest.mark.conformance(unit="E-05", part="test_eq05_moving_depth_sum", system="publication-math-conformance")
def test_eq05_moving_depth_sum(request: pytest.FixtureRequest) -> None:
    r"""The moving rainfall depth sum, `S_{R}(t_{s})=\sum_{t=t_{s}-D_{thresh}}^{t_{s}} \bar{P}(t)\Delta t`.

    The register records the divergence: `_utils:1332` sets the window from `min_interevent_time`,
    not from `D_{\text{thresh}}`; the author acknowledges it at `_utils:1274`, the stop the lift's
    `sys` recorder passes because `tests/correspondence/known_defects.toml` rules it `OBLIGED`. The
    function's signature accepts `threshold_duration` and never reads it.

    The discriminating case (AD-6) is a series whose two bursts rank differently under the two
    windows: burst A is 5.0 mm at one hourly step (2020-01-02 16:00) and burst B is 12 mm spread over
    six steps from 2020-01-05 04:00. Over the paper's `D_{thresh} = 1 h` the largest sum is burst A's
    5.0 mm, so the paper's first event starts at burst A; over the code's 12-hour window it is burst
    B's 12 mm, so the code's first event starts at burst B. A series on which both windows ranked the
    bursts alike could not see the recorded divergence, and would be a vacuous green.

    The summation bound is printed literally as `t = t_{s} - D_{thresh}` to `t_{s}`, and reads as the
    CLOSED interval by DR-3's option 1 (literally where the literal reading is defined): an hourly
    step and `D_{thresh} = 1 h` make `S_R(t_s)` the sum of the two steps at `t_s - 1 h` and `t_s`.
    Both readings give the same verdict on this series — burst A leads under either — so the reading
    decides the arithmetic and not the outcome.

    The observed value is the first row of the returned summary's `rain_event_start`, rendered as a
    timestamp array (AD-3); a frame-local read would be an artefact of this run rather than an
    observable of the function, so the returned summary is the only route. Threshold 6.0 mm also
    selects burst A on the second iteration, because the loop's stop test reads the rolling sum
    computed before the previous event was removed; that second row is not what this test compares.
    """
    # One burst of 5.0 mm at 2020-01-02 16:00 and one of 12 mm spread over six hours from
    # 2020-01-05 04:00, on a series long enough that neither burst lies inside the first
    # min_interevent_time of the record: a burst at step 10 raises `ValueError` inside the legacy
    # text at `_utils.py:1378`, because the rolling sum finds no zero-rainfall gap before it.
    series = pd.Series(
        np.zeros(160, dtype=float),
        index=pd.date_range("2020-01-01 00:00", periods=160, freq="1h"),
    )
    series.iloc[40] = 5.0
    series.iloc[100:106] = 2.0

    min_interevent_time = pd.Timedelta("12h")
    max_strm_len = pd.Timedelta("24h")
    threshold_duration = pd.Timedelta("1h")

    def reduce(finished: Mapping[str, Any]) -> Any:
        """Run the legacy selection on the literal series and reduce to its event summary."""
        summary, _tseries, _threshold_mm, _rate = finished["event_selection_threshold_or_nstorms"](
            series.copy(),
            min_interevent_time,
            max_strm_len,
            "x",
            threshold=6.0,
            threshold_duration=threshold_duration,
        )
        return summary

    summary = equation_check.implementation(RAIN_SELECTION_UNIT, None, reduce)
    observed = np.asarray([summary["rain_event_start"].iloc[0]], dtype="datetime64[ns]")

    # The manuscript's moving sum over the literal series and the literal D_thresh, and the first
    # step of the wet run whose window it weighs most heavily: the event the paper selects first.
    window_mm = series.rolling(threshold_duration, closed="both").sum()
    heaviest_tstep = window_mm.idxmax()
    wet_steps = set(np.flatnonzero(series.to_numpy() > 0.0).tolist())
    run_start = max(step for step in wet_steps if step <= series.index.get_loc(heaviest_tstep))
    while run_start - 1 in wet_steps:
        run_start -= 1
    expected = np.asarray([series.index[run_start]], dtype="datetime64[ns]")

    equation_check.check(request, observed, expected, compare.Exact())


@pytest.mark.conformance(unit="E-08", part="test_eq08_surge_window_expansion", system="publication-math-conformance")
def test_eq08_surge_window_expansion(request: pytest.FixtureRequest) -> None:
    r"""The surge event's window expansion: `\text{if } s(t_{0}-\Delta t) >s(t_{1}+\Delta t), \text{ }t_{0} \overset{\text{set}}{=} t_{0}-\Delta t \\ \text{else } t_{1}\overset{\text{set}}{=}t_{1}+\Delta t`.

    The register records the divergence: `_utils:1477-1481` compares the ONE-HOUR MEANS either side of
    the peak, not the pointwise values the equation writes, and it does so on the PEAK's hours rather
    than on the event's own bounds.

    The discriminating case (AD-6) is a six-minute series whose event exceeds the maximum duration,
    with the two rankings in opposition from the first expansion step to the last. The ten steps
    before the peak are seven of 2.5 ft and then 4.0, 5.0 and 6.0 ft (mean 3.25 ft); the ten steps
    after it are 3.0 ft and then nine of 6.0 ft (mean 5.70 ft). A rule that compares the two
    neighbouring HOURS therefore sends the window RIGHT at every step and ends one hour to the right of
    the peak; the legacy text's eleven-step hours (3.86 ft against 6.09 ft) rank the same way. The
    POINTWISE neighbours compared at the peak are 6.0 ft against 3.0 ft, which send it LEFT, and it
    stays left while the values just outside it (5.0, 4.0) still exceed the right-hand 3.0, before the
    flat 2.5 turns it right for the last seven steps. The two rules therefore end THREE steps left and
    TEN steps right of the peak respectively: different windows, in the start and the end alike, and
    not a rounding step apart.

    The step the equation expands by is the series' own timestep (`\Delta t` = 6 minutes), and the
    surrounding procedure stops the expansion when the event reaches the maximum duration, which is
    the literal parameter `max_event_duration_h`; the register's span covers the comparison alone, so
    the reference applies the manuscript's comparison at every step of that literal cutoff.

    The observed value is the first row of the returned summary's start and end (the selection records
    one event here), rendered as a timestamp array (AD-3).
    """
    tstep = pd.Timedelta("6min")
    event_limit_ft = 2.0
    max_event_duration_h = 1.0
    peak_step = 150
    # The event runs 2020-01-01 14:00 to 17:00, three hours above the limit threshold, so the code
    # enters the maximum-duration branch. The ten steps either side of the peak are laid out so the
    # hourly means and the pointwise neighbours rank the two sides differently: 3.25 ft against
    # 5.70 ft over the ten-step hours, and 6.0 ft against 3.0 ft at the first pair of neighbours.
    series = pd.Series(
        np.zeros(300, dtype=float),
        index=pd.date_range("2020-01-01 00:00", periods=300, freq=tstep, name="date_time"),
    )
    series.iloc[140:147] = 2.5
    series.iloc[147] = 4.0
    series.iloc[148] = 5.0
    series.iloc[149] = 6.0
    series.iloc[peak_step] = 10.0
    series.iloc[151] = 3.0
    series.iloc[152:171] = 6.0

    def reduce(finished: Mapping[str, Any]) -> Any:
        """Run the legacy surge selection on the literal series and reduce to its event summary."""
        summary, _tseries, _threshold_ft, _rate = finished["surge_event_selection"](
            series.copy(),
            event_limit_ft,
            max_event_duration_h,
            pd.Timedelta("1h"),
            "x",
            threshold=5.0,
        )
        return summary

    summary = equation_check.implementation(SURGE_SELECTION_UNIT, None, reduce)
    observed = np.asarray(
        [summary["surge_event_start"].iloc[0], summary["surge_event_end"].iloc[0]], dtype="datetime64[ns]"
    )

    # The manuscript's rule at the peak, expanded one timestep at a time on the side the compared
    # values favour until the event reaches the maximum duration.
    start = end = series.index[peak_step]
    while end - start < pd.Timedelta(hours=max_event_duration_h):
        if series.loc[start - tstep] > series.loc[end + tstep]:
            start -= tstep
        else:
            end += tstep
    expected = np.asarray([start, end], dtype="datetime64[ns]")

    equation_check.check(request, observed, expected, compare.Exact())


@pytest.mark.conformance(unit="E-09", part="test_eq09_combined_pairing", system="publication-math-conformance")
def test_eq09_combined_pairing(request: pytest.FixtureRequest) -> None:
    r"""The combined-event rule: "If the peak surge of a surge event occurs during a rain event, create a new event by combining the two time series and classify the combined time series as a combined event."

    The register records the divergence: `_utils:1633-1640` triggers on ANY span overlap; the paper's
    rule survives only in the docstring at `_utils:1617`, and a third, superseded 12-hour-moving-window
    rule sits commented at `_b2:589-590`. The code's own peak test is inside the overlap branch, so a
    rain event that overlaps a surge event whose peak lies outside it still yields a "combined" row —
    the rain event's own bounds, with no surge event actually appended to the combination — because
    the row is appended outside that branch.

    The discriminating case (AD-6) is therefore two rain events and two surge events: the first pair
    overlaps with the surge peak OUTSIDE the rain event, where the paper combines nothing and the code
    reports a combination; the second pair overlaps with the surge peak INSIDE the rain event, where
    both combine. The second pair is the control the first one is read against: it shows the paper's
    rule is not simply "never combine", and its presence in both values is what makes the extra pair
    the whole of the difference.

    The observed value is the set of combined events read off the returned summary's rows whose
    `compound` flag is set, as `(start, end)` pairs rendered by AD-3's rule for pairs. The reference
    is the manuscript's rule over the literal tables: a pair for every rain event whose span contains
    a surge event's `max_surge_tstep`, with the span of the union of the two events.
    """
    # Two rain events and two surge events, hourly. The first pair overlaps 2020-01-01 06:00-12:00
    # with the surge peak at 15:00, OUTSIDE the rain event; the second overlaps 2020-01-02 06:00-12:00
    # with the surge peak at 09:00, INSIDE it.
    rain_summary = pd.DataFrame(
        {
            "rain_event_start": [pd.Timestamp("2020-01-01 00:00"), pd.Timestamp("2020-01-02 00:00")],
            "rain_event_end": [pd.Timestamp("2020-01-01 12:00"), pd.Timestamp("2020-01-02 12:00")],
            "rain_data_source": ["x", "x"],
        },
        index=pd.Index([1, 2], name="event_id"),
    )
    surge_summary = pd.DataFrame(
        {
            "surge_event_start": [pd.Timestamp("2020-01-01 06:00"), pd.Timestamp("2020-01-02 06:00")],
            "surge_event_end": [pd.Timestamp("2020-01-01 18:00"), pd.Timestamp("2020-01-02 18:00")],
            "duration": [pd.Timedelta("12h"), pd.Timedelta("12h")],
            "max_surge_ft": [3.0, 4.0],
            "max_surge_tstep": [pd.Timestamp("2020-01-01 15:00"), pd.Timestamp("2020-01-02 09:00")],
            "wlevel_data_source": ["x", "x"],
        },
        index=pd.Index([1, 2], name="surge_event_id"),
    )
    rain_tseries = pd.concat(
        [
            _event_tseries(1, "2020-01-01 00:00", "2020-01-01 12:00", "event_id"),
            _event_tseries(2, "2020-01-02 00:00", "2020-01-02 12:00", "event_id"),
        ]
    )
    surge_tseries = pd.concat(
        [
            _event_tseries(1, "2020-01-01 06:00", "2020-01-01 18:00", "surge_event_id"),
            _event_tseries(2, "2020-01-02 06:00", "2020-01-02 18:00", "surge_event_id"),
        ]
    )

    def reduce(finished: Mapping[str, Any]) -> Any:
        """Run the legacy pairing on the literal tables and reduce to its combined-event summary."""
        return finished["determine_compound_event_start_and_end_times"](
            surge_tseries.copy(), surge_summary.copy(), rain_tseries.copy(), rain_summary.copy()
        )

    frame = equation_check.implementation(COMPOUND_PAIRING_UNIT, None, reduce)
    # The returned summary carries no event ids, only each event's span and its three flags, so a
    # combined event is read off as the span of a row whose `compound` flag is set.
    observed = np.asarray(
        sorted((row.event_start, row.event_end) for _, row in frame.iterrows() if bool(row["compound"]))
    )

    combined: set[tuple[pd.Timestamp, pd.Timestamp]] = set()
    for _, rain in rain_summary.iterrows():
        for _, surge in surge_summary.iterrows():
            if rain.rain_event_start <= surge.max_surge_tstep <= rain.rain_event_end:
                combined.add(
                    (
                        min(rain.rain_event_start, surge.surge_event_start),
                        max(rain.rain_event_end, surge.surge_event_end),
                    )
                )
    expected = np.asarray(sorted(combined))

    equation_check.check(request, observed, expected, compare.Exact())


def _event_tseries(event_id: int, start: str, end: str, id_name: str) -> pd.DataFrame:
    """One event's hourly series under a two-level index of the event id and the timestamp.

    The code reads only the index of a time series: which event each timestamp belongs to and when it
    is. The value column carries a constant, so a reader does not mistake it for an input to the rule.
    """
    times = pd.date_range(start, end, freq="1h")
    return pd.DataFrame(
        {"value": np.full(len(times), 1.0)},
        index=pd.MultiIndex.from_product([[event_id], times], names=[id_name, "date_time"]),
    )
