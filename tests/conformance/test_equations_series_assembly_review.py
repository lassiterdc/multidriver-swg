r"""Companion checks of the four series-assembly equation tests in `test_equations_series_assembly.py`.

An equation test of this family is red on build by design, and a red alone cannot tell a test that found the recorded
paper/code departure from a test whose own reference or plumbing is wrong: both end in a `ConformanceDivergence`.
Two failures break the meaning of a red without anyone noticing:

- a test can be red for a reason no change to the code removes, because its reference reads a quantity the equation
  does not state, so the divergence it records can never be closed;
- a test can stay green on a wrong implementation, because its literal input does not separate the manuscript's form
  from the wrong one, so a later change that breaks the code goes unseen.

The checks below separate these by giving each equation test a second implementation to run against. Each is written
here from the manuscript's formula and shares no text with the legacy unit or with the test's own reference. The
equation tests obtain their observed value by calling the `implementation` name their module binds, so a replacement
bound to that name builds the namespace the test's own reduction reads from the literal overrides the test passes. One
replacement computes the printed formula and must turn the test green; several each compute one wrong form and must
turn it red with a `ConformanceDivergence`, the disagreement landing on exactly the part that form gets wrong. The
stored legacy text is also run unchanged, which fixes the outcome of every row: three diverge and one agrees.

What these checks establish, by row:

- E-20 is green against `s_h + (PS_h - PS_g)` and red on the code's `s_h + (PS_g - PS_h)`, on an unchanged series and on
  a rescaled one. Against the stored text the observed and expected series differ by exactly twice the peak
  difference at every step, so the red is the sign and nothing else.
- E-26 is green against a shift that puts the surge peak the target lag after the rain peak, whatever whole-hour
  target the literals carry, and red against no shift, a shift of the wrong sign and shifts one hour either side of
  the target. The stored text reaches each of several targets, so its green is not a coincidence of one literal.
- E-28 is green against the manuscript's three buffers with the significant-rainfall periods counted from either start,
  the record's first timestep or its first rainfall step, and red against the legacy end-anchored periods, or with any
  one buffer left out. Against the stored text the disagreement is one value, the last time step of the late-rainfall
  case, by exactly one hour; the other case agrees.
- E-31 is green against a draw from `{y_r - n, ..., y_r}` and red against the code's slice, a pool one year too long, a
  pool of the last year only, and a phase shift that leaves `±h/2`. Against the stored text the year sets differ and
  the phase flag agrees.
- Each equation test seeds numpy's global stream where its unit draws, repeats exactly, and leaves the stream as it
  found it.
- The four tests keep the structure the family relies on: one conformance marker, with string-literal keywords, on
  the function that names its ledger row; no skip or expected-failure marker; no handler that could turn a refusal
  into a pass; exactly one final `check` call; the register's span quoted in the docstring; and no file written
  outside the directory the test is handed.

These carry no family marker, so they enter no inventory count. They guard
`tests/conformance/test_equations_series_assembly.py`.

Not established: that the manuscript's wording of E-28 fixes where the significant-rainfall periods are anchored. It
does not, so the equation test is held to be green under each reading that counts the periods from a start, and red
only under the reading its register entry records, the end. Nor is it established that a phase shift drawn from a
narrower range than `±h/2` is refused: the equation test reads the shift only as inside the window.
"""

from __future__ import annotations

import ast
import inspect
import os
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from tests.conformance import bindings, equation_check, register, test_equations_series_assembly
from tests.correspondence import lift

SUBJECT = "tests.conformance.test_equations_series_assembly"
SYSTEM = "publication-math-conformance"

# The four rows the module owns: equation id, the function the ledger names for it, and the unit constant it runs.
ROWS = {
    "E-20": ("test_eq20_additive_surge_rescaling", "STORM_RESCALING_CELL"),
    "E-26": ("test_eq26_lag_shift", "FINALISING_CELL"),
    "E-28": ("test_eq28_series_buffering", "BUFFERING_FUNCTION"),
    "E-31": ("test_eq31_tide_randomization", "TIDE_FUNCTION"),
}
RED_ROWS = ("E-20", "E-28", "E-31")
# The legacy unit each constant must name, written out here so a drifted constant is seen.
UNITS = {
    "STORM_RESCALING_CELL": "_b7_generate_time_series_from_event_stats.py:88-380",
    "FINALISING_CELL": "_b7_generate_time_series_from_event_stats.py:381-890",
    "BUFFERING_FUNCTION": "_utils.py:335-398",
    "TIDE_FUNCTION": "_utils.py:236-270",
}
EXACT_ROWS = ("E-28", "E-31")
TOLERANCE_ROWS = ("E-20", "E-26")
SEEDED_ROWS = ("E-20", "E-26", "E-31")
HOUR = np.timedelta64(1, "h")


def _subject() -> ModuleType:
    """The module under review, read from `sys.modules` so a probe that substitutes a variant of it is seen.

    The module is also imported by name at the top of this file, so the selection of tests to run after a change to it
    finds this file by its import and not only by its declared path.
    """
    return sys.modules.get(SUBJECT, test_equations_series_assembly)


def _function(equation_id: str) -> Callable[..., None]:
    """The equation test function for `equation_id`."""
    return getattr(_subject(), ROWS[equation_id][0])


def _request_for(test: Callable[..., Any]) -> object:
    """A stand-in for pytest's request whose node answers with the test's own `conformance` marker.

    Reading the marker off the test function, rather than writing the id here, keeps the equation id the one the
    decorator states, which is the id `check` attributes a red to.
    """
    marker = next(each for each in test.pytestmark if each.name == "conformance")  # type: ignore[attr-defined]

    def get_closest_marker(name: str) -> object | None:
        """Return the test's conformance marker for that name and nothing for any other."""
        return marker if name == "conformance" else None

    return SimpleNamespace(node=SimpleNamespace(get_closest_marker=get_closest_marker))


def _call(equation_id: str, tmp_path: Path) -> None:
    """Call the equation test as pytest would, handing it a temporary directory when it asks for one."""
    test = _function(equation_id)
    arguments: list[Any] = [_request_for(test)]
    if "tmp_path" in inspect.signature(test).parameters:
        arguments.append(tmp_path)
    test(*arguments)


def _divergence(equation_id: str, tmp_path: Path) -> equation_check.ConformanceFailure:
    """The failure record the equation test's `ConformanceDivergence` carries; any other outcome fails here."""
    with pytest.raises(equation_check.ConformanceDivergence) as refused:
        _call(equation_id, tmp_path)
    failure = refused.value.failure
    assert failure.equation_id == equation_id
    assert failure.diverged is True
    return failure


def _recording_implementation(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str | None, dict[str, Any]]]:
    """Bind a recorder in front of the real helper and return its list of calls: unit, part and overrides."""
    subject = _subject()
    real = subject.implementation
    calls: list[tuple[str, str | None, dict[str, Any]]] = []

    def recording(
        unit: str, part: str | None, reduce: Callable[[Any], Any], overrides: Mapping[str, Any] | None = None
    ):
        """Note the call, then run the real helper."""
        calls.append((unit, part, dict(overrides or {})))
        return real(unit, part, reduce, overrides)

    monkeypatch.setattr(subject, "implementation", recording)
    return calls


def _replace_implementation(
    monkeypatch: pytest.MonkeyPatch,
    unit_constant: str,
    build: Callable[[dict[str, Any]], dict[str, Any]],
) -> list[dict[str, Any]]:
    """Bind a second implementation: the call's unit is checked, and its reduction reads a namespace `build` makes.

    `build` receives the overrides the equation test passed and returns the namespace its reduction reads. The list
    returned collects those overrides, so a check can read the literals the test chose.
    """
    subject = _subject()
    seen: list[dict[str, Any]] = []

    def replacement(
        unit: str, part: str | None, reduce: Callable[[Any], Any], overrides: Mapping[str, Any] | None = None
    ) -> Any:
        """Run the test's reduction over the namespace the second implementation builds."""
        assert unit == UNITS[unit_constant]
        assert part is None
        literals = dict(overrides or {})
        seen.append(literals)
        return reduce(build(literals))

    monkeypatch.setattr(subject, "implementation", replacement)
    return seen


# ---------------------------------------------------------------------------------------------------------------
# E-20: the simulated surge series is the observed one with the peak difference added on.
# ---------------------------------------------------------------------------------------------------------------

SurgeForm = Callable[[np.ndarray, float, float], np.ndarray]


def _surge_literals(literals: dict[str, Any]) -> tuple[np.ndarray, float, float]:
    """The observed surge series, the observed peak and the simulated peak, as the test's literals state them."""
    series = literals["ds_event_tseries"]["surge_ft"].to_numpy().astype(float)
    observed_peak = float(literals["df_all_events_summary"]["surge_peak_ft"].iloc[0])
    simulated_peak = float(literals["dic_simulations"]["compound_events"]["surge_peak_ft"].iloc[0])
    return series, observed_peak, simulated_peak


def _surge_namespace(form: SurgeForm) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """A namespace builder: the rescaled series the cell would write, with `form` applied to the observed one.

    `form` receives the observed surge series, the observed peak and the simulated peak, both read from the literals
    the test supplied, and returns the series a rescaling would give.
    """

    def build(literals: dict[str, Any]) -> dict[str, Any]:
        """The cell's namespace for one rescaled surge series."""
        series, observed_peak, simulated_peak = _surge_literals(literals)
        assert observed_peak == series.max()
        assert observed_peak != simulated_peak
        return {
            "ds_sim_event_tseries": xr.Dataset({"surge_ft": ("timestep", form(series, observed_peak, simulated_peak))})
        }

    return build


def _manuscript_surge(series: np.ndarray, observed_peak: float, simulated_peak: float) -> np.ndarray:
    """The printed form: the observed series plus the observed peak less the simulated one."""
    return series + (observed_peak - simulated_peak)


def _legacy_signed_surge(series: np.ndarray, observed_peak: float, simulated_peak: float) -> np.ndarray:
    """The code's form: the observed series plus the simulated peak less the observed one."""
    return series + (simulated_peak - observed_peak)


def _unchanged_surge(series: np.ndarray, observed_peak: float, simulated_peak: float) -> np.ndarray:
    """No rescaling at all."""
    return series.copy()


def _multiplicative_surge(series: np.ndarray, observed_peak: float, simulated_peak: float) -> np.ndarray:
    """A rescaling by the ratio of the peaks in place of their difference."""
    return series * simulated_peak / observed_peak


SURGE_WRONG_FORMS: dict[str, SurgeForm] = {
    "legacy-sign": _legacy_signed_surge,
    "unchanged": _unchanged_surge,
    "ratio-of-peaks": _multiplicative_surge,
}


def test_e20_passes_against_the_printed_form(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Property: with a second implementation that adds `PS_h - PS_g` to the observed surge series behind the call,
    the E-20 test returns without raising, so its red on the stored text is the text's disagreement and not the
    test's own error.

    Why: a test red under every implementation reports a divergence whatever the code does.
    Kills: a reference that reads the code's sign; a series compared in another layout from the one the cell writes.
    Class: NEW CAPABILITY. The plausible wrong implementation is an E-20 reference computed with the code's sign; this
    test is red under it, because the printed form then disagrees with the reference.
    Second correct implementation: any rescaling that returns the observed series plus the peak difference, by
    addition, by `np.add` or through a differently named local.
    """
    seen = _replace_implementation(monkeypatch, "STORM_RESCALING_CELL", _surge_namespace(_manuscript_surge))
    _call("E-20", tmp_path)
    assert len(seen) == 1


@pytest.mark.parametrize("form_name", SURGE_WRONG_FORMS)
def test_e20_is_red_on_each_wrong_form(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, form_name: str) -> None:
    """Property: with a wrong rescaling behind the call, the E-20 test raises `ConformanceDivergence` for E-20, and
    the observed value is the wrong form's own series.

    Why: an input that cannot separate the printed form from a wrong one is a vacuous green once code adopts it.
    Kills: peaks that coincide; a comparison on the peak alone; a series read from a summary the cell writes about
    itself in place of the series it wrote.
    Class: NEW CAPABILITY. The plausible wrong implementation is a pair of literal peaks that coincide, under which
    every wrong form here equals the printed one; the premise assertion on the peaks is red under it.
    Second correct implementation: another pair of distinct literal peaks, with another observed series.
    """
    form = SURGE_WRONG_FORMS[form_name]
    seen = _replace_implementation(monkeypatch, "STORM_RESCALING_CELL", _surge_namespace(form))
    failure = _divergence("E-20", tmp_path)
    literals = seen[0]
    series, observed_peak, simulated_peak = _surge_literals(literals)
    np.testing.assert_allclose(failure.observed, form(series, observed_peak, simulated_peak))
    np.testing.assert_allclose(failure.expected, _manuscript_surge(series, observed_peak, simulated_peak))


def test_e20_stored_text_differs_from_the_printed_form_by_twice_the_peak_difference_at_every_step(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Property: run against the stored text, the E-20 test raises `ConformanceDivergence`; the observed series is
    the literal observed series moved so its peak is the simulated peak, the expected series is the printed form, and
    the two differ by `2 (PS_g - PS_h)` at every step.

    Why: a red whose gap moves with the step, or is not twice the peak difference, has a cause other than the sign.
    Kills: a reference with a second difference from the code; a gap that is the peak difference once.
    Class: NEW CAPABILITY. The plausible wrong implementation is a reference with the code's sign, under which the
    stored text agrees and nothing is raised.
    Second correct implementation: other literal peaks and another observed series, with the sign left as printed.
    """
    calls = _recording_implementation(monkeypatch)
    failure = _divergence("E-20", tmp_path)
    literals = calls[0][2]
    series, observed_peak, simulated_peak = _surge_literals(literals)
    assert observed_peak != simulated_peak
    np.testing.assert_allclose(failure.observed, series + (simulated_peak - observed_peak))
    np.testing.assert_allclose(failure.expected, series + (observed_peak - simulated_peak))
    np.testing.assert_allclose(failure.observed - failure.expected, 2.0 * (simulated_peak - observed_peak))
    assert failure.observed.max() == simulated_peak


# ---------------------------------------------------------------------------------------------------------------
# E-26: the surge series is shifted so its peak sits the generated time delay after the rain peak.
# ---------------------------------------------------------------------------------------------------------------


def _lag_namespace(achieved_lag: Callable[[float], float]) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """A namespace builder: the series the cell would return, its surge shifted to `achieved_lag(target)` hours.

    The target is read from the summary table the test supplies and the series from the dataset it supplies, each
    through the `pandas` and `xarray` names the test binds, so a change to either literal reaches this builder.
    """

    def build(literals: dict[str, Any]) -> dict[str, Any]:
        """The cell's namespace for one finalised event."""
        summaries = literals["pd"].read_csv("event_simulation_summaries_all.csv")
        target = float(summaries["surge_peak_after_rain_peak_h_target"].iloc[0])
        frame = literals["xr"].open_dataset("event_time_series.nc").to_dataframe()
        timesteps = frame.index.get_level_values(-1)
        step_hours = float((timesteps[1] - timesteps[0]) / HOUR)
        surge = frame["surge_ft"].to_numpy(dtype=float)
        rain = frame["mm_per_hr"].to_numpy(dtype=float)
        wanted = int(np.argmax(rain)) + round(achieved_lag(target) / step_hours)
        shift = wanted - int(np.argmax(surge))
        shifted = np.zeros_like(surge)
        for position, value in enumerate(surge):
            if 0 <= position + shift < len(surge):
                shifted[position + shift] = value
        return {"df_tseries": frame.assign(surge_ft=shifted)}

    return build


LAG_WRONG_FORMS: dict[str, tuple[Callable[[float], float], float]] = {
    "no-shift": (lambda target: 0.0, 0.0),
    "wrong-sign": (lambda target: -target, -2.0),
    "one-hour-short": (lambda target: target - 1.0, 1.0),
    "one-hour-long": (lambda target: target + 1.0, 3.0),
}


def test_e26_passes_against_a_shift_to_the_target_lag(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Property: with a second implementation that moves the surge peak to the target lag after the rain peak behind
    the call, the E-26 test returns without raising.

    Why: a test red under every implementation reports a divergence whatever the code does.
    Kills: a peak lag read in the wrong direction; a lag in steps compared with a target in hours.
    Class: NEW CAPABILITY. The plausible wrong implementation is a reference that reads the lag in the other
    direction, under which the faithful shift is refused.
    Second correct implementation: a shift that moves the rain peak instead of the surge peak to the same lag.
    """
    seen = _replace_implementation(monkeypatch, "FINALISING_CELL", _lag_namespace(lambda target: target))
    _call("E-26", tmp_path)
    assert len(seen) == 1


@pytest.mark.parametrize("target", [1.0, 3.0])
def test_e26_follows_the_target_the_literals_carry(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, target: float
) -> None:
    """Property: with the module's target lag changed to another whole number of hours, a second implementation that
    reaches the target still turns the E-26 test green, and one that reaches the original two hours turns it red.

    Why: a reference fixed at two hours rather than read from the literals agrees with the stored text by chance.
    Kills: an expected value that does not move with the target; a target held in two places that can drift apart.
    Class: NEW CAPABILITY. The plausible wrong implementation is an expected value fixed at two hours; it is red under
    a target of one or three hours, where the faithful shift is refused.
    Second correct implementation: a reference that reads the target from the summary the cell reads.
    """
    monkeypatch.setattr(_subject(), "LAG_TARGET_H", target)
    _replace_implementation(monkeypatch, "FINALISING_CELL", _lag_namespace(lambda requested: requested))
    _call("E-26", tmp_path)
    _replace_implementation(monkeypatch, "FINALISING_CELL", _lag_namespace(lambda requested: 2.0))
    with pytest.raises(equation_check.ConformanceDivergence):
        _call("E-26", tmp_path)


@pytest.mark.parametrize("form_name", LAG_WRONG_FORMS)
def test_e26_is_red_on_each_wrong_shift(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, form_name: str) -> None:
    """Property: with a surge that is not shifted to the target behind the call, the E-26 test raises
    `ConformanceDivergence` for E-26, and the observed value is the lag the series really has, read off the series.

    Why: a test that read a lag the cell reports about itself would agree with the code whatever the series holds.
    Kills: an observed value taken from a summary; a lag read from the wrong peaks; a tolerance that admits a step.
    Class: NEW CAPABILITY. The plausible wrong implementation is an observed value that is the target itself rather
    than a lag read off the series; the four wrong shifts then raise nothing, so each is red under it.
    Second correct implementation: a lag read from the returned series by any other route, such as a cross-correlation.
    """
    achieved, reached = LAG_WRONG_FORMS[form_name]
    _replace_implementation(monkeypatch, "FINALISING_CELL", _lag_namespace(achieved))
    failure = _divergence("E-26", tmp_path)
    assert failure.observed == pytest.approx(reached)
    assert failure.expected == pytest.approx(_subject().LAG_TARGET_H)


def test_e26_literals_start_the_two_peaks_level_and_the_target_is_a_whole_step() -> None:
    """Property: the literal surge and rainfall series peak at the same step before any shift, the target lag is a
    nonzero whole number of the series' hourly steps, and the surge's index leaves room for the shift.

    Why: with peaks that already differ by the target, no shift is tested; with a fractional target the cell's rounding
    is tested in place of the shift.
    Kills: an edit to either series that moves one peak; a target of 2.5 h on an hourly series.
    Class: NEW CAPABILITY, and a fitness function over the literals. The plausible wrong implementation is a rainfall
    series whose peak differs from the surge's before any shift; this test is red under it.
    Second correct implementation: other series with level peaks and a whole-hour target.
    """
    subject = _subject()
    assert int(np.argmax(subject.SHIFTED_SURGE)) == int(np.argmax(subject.SIMULATED_RAIN))
    assert subject.LAG_TARGET_H != 0
    assert float(subject.LAG_TARGET_H).is_integer()
    steps = np.asarray(subject.SURGE_STEPS / HOUR)
    assert (np.diff(steps) == 1.0).all()
    peak = int(np.argmax(subject.SHIFTED_SURGE))
    assert peak + int(subject.LAG_TARGET_H) < len(subject.SHIFTED_SURGE)


@pytest.mark.parametrize("target", [1.0, 3.0, 4.0])
def test_e26_stored_text_reaches_each_whole_hour_target(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, target: float
) -> None:
    """Property: with the target lag changed to 1, 3 or 4 hours, the stored text still leaves the surge peak that many
    hours after the rain peak, so the E-26 test returns without raising.

    Why: a green on one literal can be a coincidence of that literal; the target moving the achieved lag shows the
    returned series is what the test reads.
    Kills: a cell that leaves the series as it found it and agrees with a target of 2 by construction.
    Class: NEW CAPABILITY. The plausible wrong implementation is an expected value fixed at two hours; it is red under
    a target of one, three or four hours, which the stored text reaches.
    Second correct implementation: the stored text replaced by an implementation that reaches any whole-hour target.
    """
    monkeypatch.setattr(_subject(), "LAG_TARGET_H", target)
    _call("E-26", tmp_path)


# ---------------------------------------------------------------------------------------------------------------
# E-28: the series is extended by the peak buffer, the rainfall buffer and the pre-rain buffer.
# ---------------------------------------------------------------------------------------------------------------

PEAKS_KEY = "timeseries_buffer_around_peaks_h"
PRE_KEY = "timeseries_buffer_before_first_rain_h"
FRACTION_KEY = "buffer_window_raindepth_fraction_to_trigger_tserties_extension"


def _buffered_frame(
    frame: pd.DataFrame,
    *,
    peaks_h: float,
    pre_h: float,
    fraction: float,
    anchor: str,
    peak_buffer: bool = True,
    rain_buffer: bool = True,
    pre_buffer: bool = True,
) -> pd.DataFrame:
    """The frame reindexed to the manuscript's buffers, written with plain period arithmetic.

    The rainfall record is cut to its first and last wet step and divided into `D_peaks`-hour periods that start at
    `anchor`: the "first" wet step, the "last" wet step, or the "record"'s first timestep. A period is significant when
    it holds at least `fraction` of the total
    depth. The buffers are the manuscript's three: `D_peaks` either side of the peak surge, `D_peaks` after the end of
    the last significant period, and `D_pre` before the first wet step. Each is applied only when the series does not
    already reach it. A switch leaves one buffer out.
    """
    period = pd.Timedelta(peaks_h, "h")
    lead = pd.Timedelta(pre_h, "h")
    step = pd.Series(frame.index).diff().mode().iloc[0]
    first, last = frame.index.min(), frame.index.max()
    if peak_buffer:
        peak = frame["surge_ft"].idxmax()
        first, last = min(first, peak - period), max(last, peak + period)
    depth = frame["mm_per_hr"] * (step / pd.Timedelta(1, "h"))
    wet = depth[depth > 0].index
    wet_first, wet_last = wet.min(), wet.max()
    if rain_buffer:
        origins = {"first": wet_first, "last": wet_last, "record": frame.index.min()}
        origin = origins[anchor]
        per_period: dict[int, float] = {}
        for moment, amount in depth.loc[wet_first:wet_last].items():
            index = (moment - origin) // period
            per_period[index] = per_period.get(index, 0.0) + float(amount)
        total = sum(per_period.values())
        significant_ends = [
            origin + (index + 1) * period for index, amount in per_period.items() if amount / total >= fraction
        ]
        last = max(last, max(significant_ends) + period)
    if pre_buffer:
        first = min(first, wet_first - lead)
    return frame.reindex(pd.date_range(first, last, freq=step))


def _buffer_namespace(**switches: Any) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """A namespace builder holding the buffering function, written from the manuscript, with `switches` applied."""

    def build(literals: dict[str, Any]) -> dict[str, Any]:
        """The buffering function's namespace, its three parameters read from the overrides the test passed."""

        def reindex_to_buffer_significant_rain_and_surge(frame: pd.DataFrame) -> pd.DataFrame:
            """The frame reindexed to the buffers."""
            return _buffered_frame(
                frame,
                peaks_h=literals[PEAKS_KEY],
                pre_h=literals[PRE_KEY],
                fraction=literals[FRACTION_KEY],
                **{"anchor": "first", **switches},
            )

        return {"reindex_to_buffer_significant_rain_and_surge": reindex_to_buffer_significant_rain_and_surge}

    return build


def _disagreeing_bounds(failure: equation_check.ConformanceFailure) -> set[tuple[str, str]]:
    """The (case, bound) pairs on which the observed value's first or last time step differs from the expected one."""
    pairs: set[tuple[str, str]] = set()
    for name in failure.expected.data_vars:
        differs = failure.observed[name].to_numpy() != failure.expected[name].to_numpy()
        for bound, moved in zip(("first", "last"), differs[0], strict=True):
            if moved:
                pairs.add((str(name).removesuffix("__bounds"), bound))
    return pairs


BUFFER_WRONG_FORMS: dict[str, tuple[dict[str, Any], set[tuple[str, str]]]] = {
    "periods-anchored-at-the-end": ({"anchor": "last"}, {("late_rainfall", "last")}),
    "no-rainfall-buffer": ({"rain_buffer": False}, {("late_rainfall", "last")}),
    "no-peak-buffer": ({"peak_buffer": False}, {("early_rainfall_late_peak", "last")}),
    "no-pre-rain-buffer": ({"pre_buffer": False}, {("early_rainfall_late_peak", "first")}),
}


@pytest.mark.parametrize("anchor", ["first", "record"], ids=["first-wet-step", "record-start"])
def test_e28_passes_against_the_three_buffers_with_periods_counted_from_either_start(
    monkeypatch: pytest.MonkeyPatch, anchor: str
) -> None:
    """Property: with a second implementation that applies the manuscript's three buffers behind the call, counting the
    significant-rainfall periods from the first wet step or from the record's first timestep, the E-28 test returns
    without raising.

    Why: the manuscript names no anchor, so a red that holds for one start-anchored reading and not for the other
    asserts the reference's reading in place of the register's recorded departure, which is the end anchoring; a
    correct implementation that counted from the other start would be refused.
    Kills: a late-rainfall case whose first wet step is off the record's `D_peaks` grid, where the two readings name
    different periods and only one of them agrees with the code.
    Class: NEW CAPABILITY. The plausible wrong implementation is a late-rainfall case whose first wet step is not a
    whole number of `D_peaks` periods from the record's first timestep; the record-start reading is red under it.
    Second correct implementation: either reading, or one counting the periods by pandas resampling with the origin at
    the start, on a case whose first wet step lies on the record's `D_peaks` grid.
    """
    seen = _replace_implementation(monkeypatch, "BUFFERING_FUNCTION", _buffer_namespace(anchor=anchor))
    _call("E-28", Path("."))
    assert len(seen) == 1


@pytest.mark.parametrize("form_name", BUFFER_WRONG_FORMS)
def test_e28_is_red_on_each_wrong_form_and_only_where_that_form_is_wrong(
    monkeypatch: pytest.MonkeyPatch, form_name: str
) -> None:
    """Property: with the periods anchored at the end, or with one of the three buffers left out, behind the call, the
    E-28 test raises `ConformanceDivergence` for E-28, and the first or last time step that disagrees is exactly the
    one that form gets wrong.

    Why: each case is meant to carry one value that the form moves; a form that moved another value would mean the
    cases are not separate.
    Kills: two cases that react to every buffer alike; a control that is not a control.
    Class: NEW CAPABILITY. The plausible wrong implementation is a literal case whose peak buffer is one hour, so that
    three of the four wrong forms agree with the reference; each of them is red under it.
    Second correct implementation: other whole-hour buffer parameters with literal frames of the same two shapes.
    """
    switches, wrong_at = BUFFER_WRONG_FORMS[form_name]
    _replace_implementation(monkeypatch, "BUFFERING_FUNCTION", _buffer_namespace(**switches))
    failure = _divergence("E-28", Path("."))
    assert _disagreeing_bounds(failure) == wrong_at


def test_e28_stored_text_differs_in_one_value_by_one_hour() -> None:
    """Property: run against the stored text, the E-28 test raises `ConformanceDivergence`, the one time step on which
    the two sides differ is the last of the late-rainfall case, the code's is one hour later, and the other case
    agrees on both its time steps, and each case's expected bounds extend its series.

    Why: a red that moves a second value has a second cause, and a case whose expected bounds equal its series' own
    ends would test no buffer.
    Kills: a reference that differs in the pre-rain or peak buffer; a control with no extension in it.
    Class: NEW CAPABILITY. The plausible wrong implementation is a reference anchored at the end of the series, under
    which the stored text agrees and nothing is raised.
    Second correct implementation: the same two cases at another start date.
    """
    failure = _divergence("E-28", Path("."))
    assert _disagreeing_bounds(failure) == {("late_rainfall", "last")}
    observed = failure.observed["late_rainfall__bounds"].to_numpy()[0]
    expected = failure.expected["late_rainfall__bounds"].to_numpy()[0]
    assert observed[1] - expected[1] == HOUR
    subject = _subject()
    quiet = subject._buffer_frame([0.0] * subject.BUFFER_STEPS, [0.0] * subject.BUFFER_STEPS).index
    for case in ("late_rainfall", "early_rainfall_late_peak"):
        first, last = failure.expected[f"{case}__bounds"].to_numpy()[0]
        assert first <= quiet.min().to_datetime64()
        assert last >= quiet.max().to_datetime64()
        assert (first, last) != (quiet.min().to_datetime64(), quiet.max().to_datetime64())


# ---------------------------------------------------------------------------------------------------------------
# E-31: the year and the phase of the astronomical tide are drawn from the manuscript's two sets.
# ---------------------------------------------------------------------------------------------------------------

Pool = Callable[[list[int], int], list[int]]


def _tide_namespace(pool: Pool, shift_scale: float = 1.0) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """A namespace builder: a tide function that draws its year from `pool` and its shift from the scaled window.

    It draws through the `np` the test binds, so the test's recording of the two draws is what it reads.
    """

    def build(literals: dict[str, Any]) -> dict[str, Any]:
        """The tide function's namespace, `n` and the numpy binding read from the overrides the test passed."""
        recording = literals["np"]
        n_back = literals["n_years_to_look_back_for_tidal_tseries"]

        def generate_randomized_tide_component(surge, water_levels, window_h, obs_start, obs_end):
            """Draw the phase shift and the year, as the manuscript's two uniform sets say."""
            years = sorted({int(year) for year in water_levels.date_time.to_series().dt.year})
            recording.random.uniform(low=-shift_scale * window_h / 2, high=shift_scale * window_h / 2, size=None)
            recording.random.choice(pool(years, n_back))

        return {"generate_randomized_tide_component": generate_randomized_tide_component}

    return build


def _printed_pool(years: list[int], n_back: int) -> list[int]:
    """The manuscript's set: the most recent complete year and the `n` before it."""
    return list(range(years[-1] - n_back, years[-1] + 1))


def _legacy_slice_pool(years: list[int], n_back: int) -> list[int]:
    """The code's set: the same slice with the most recent year dropped."""
    return years[-(n_back + 1) : -1]


def _one_year_too_long_pool(years: list[int], n_back: int) -> list[int]:
    """The manuscript's set with one more earlier year."""
    return list(range(years[-1] - n_back - 1, years[-1] + 1))


def _most_recent_only_pool(years: list[int], n_back: int) -> list[int]:
    """The most recent year alone."""
    return [years[-1]]


TIDE_WRONG_FORMS: dict[str, tuple[Pool, float, bool]] = {
    # name: (pool of years, shift scale, whether the years are expected to disagree)
    "legacy-year-slice": (_legacy_slice_pool, 1.0, True),
    "one-year-too-long": (_one_year_too_long_pool, 1.0, True),
    "most-recent-year-only": (_most_recent_only_pool, 1.0, True),
    "shift-twice-the-window": (_printed_pool, 2.0, False),
}


@pytest.mark.parametrize("n_back", [1, 2])
def test_e31_passes_against_a_draw_from_the_printed_set(monkeypatch: pytest.MonkeyPatch, n_back: int) -> None:
    """Property: with a second implementation that draws its year from `{y_r - n, ..., y_r}` and its shift from
    `(-h/2, h/2)` behind the call, the E-31 test returns without raising, for `n` of one and of two years.

    Why: a test red under every implementation reports a divergence whatever the code does, and an `n` that does not
    reach the reference shows the reference ignores the literal it is stated against.
    Kills: an expected set fixed at three years; a year set read from the code's pool.
    Class: NEW CAPABILITY. The plausible wrong implementation is an expected set fixed at three years, which a one-year
    look-back refuses under the faithful draw.
    Second correct implementation: a draw by `np.random.randint` over the same years.
    """
    monkeypatch.setattr(_subject(), "TIDE_YEARS_TO_LOOK_BACK", n_back)
    seen = _replace_implementation(monkeypatch, "TIDE_FUNCTION", _tide_namespace(_printed_pool))
    _call("E-31", Path("."))
    assert seen[0]["n_years_to_look_back_for_tidal_tseries"] == n_back


@pytest.mark.parametrize("form_name", TIDE_WRONG_FORMS)
def test_e31_is_red_on_each_wrong_draw_and_only_on_the_part_that_draw_gets_wrong(
    monkeypatch: pytest.MonkeyPatch, form_name: str
) -> None:
    """Property: with a draw from a wrong set of years, or a phase shift that leaves the window, behind the call, the
    E-31 test raises `ConformanceDivergence` for E-31, and the disagreement is in the years for a wrong set and in
    the phase flag for a wrong shift, never in both.

    Why: the two draws are one value in two parts; a form that moved the part it did not touch would show the parts
    are not independent.
    Kills: a years set built from the flag; a flag read from the years; a window flag that is a constant.
    Class: NEW CAPABILITY. The plausible wrong implementation is a window flag fixed at true, under which a shift of
    twice the window is not refused; it is red under that shift.
    Second correct implementation: a draw that also records the shift's sign, with the flag computed from it.
    """
    pool, shift_scale, years_disagree = TIDE_WRONG_FORMS[form_name]
    _replace_implementation(monkeypatch, "TIDE_FUNCTION", _tide_namespace(pool, shift_scale))
    failure = _divergence("E-31", Path("."))
    observed_years = failure.observed["years"].to_numpy()
    expected_years = failure.expected["years"].to_numpy()
    years_equal = observed_years.shape == expected_years.shape and bool((observed_years == expected_years).all())
    flag_equal = bool(
        failure.observed["every_shift_within_half_window"] == failure.expected["every_shift_within_half_window"]
    )
    assert years_equal is not years_disagree
    assert flag_equal is years_disagree


def test_e31_stored_text_draws_all_but_the_most_recent_year() -> None:
    """Property: run against the stored text, the E-31 test raises `ConformanceDivergence`; the years the text draws
    are the manuscript's set without its most recent year, and the phase flag agrees.

    Why: the red is meant to be the slice's exclusion of `y_r`; a gap in the flag or in another year would be a second
    cause.
    Kills: a reference that differs in a year other than `y_r`; a window flag that disagrees for an unrelated reason.
    Class: NEW CAPABILITY. The plausible wrong implementation is an expected set without its most recent year, under
    which the stored text agrees and nothing is raised.
    Second correct implementation: a record of other years with the look-back changed to match.
    """
    subject = _subject()
    failure = _divergence("E-31", Path("."))
    most_recent = max(subject.TIDE_YEARS_RECORDED)
    printed = list(range(most_recent - subject.TIDE_YEARS_TO_LOOK_BACK, most_recent + 1))
    assert failure.expected["years"].to_numpy().tolist() == printed
    assert failure.observed["years"].to_numpy().tolist() == printed[:-1]
    assert bool(failure.observed["every_shift_within_half_window"]) is True
    assert bool(failure.expected["every_shift_within_half_window"]) is True


def test_e31_literals_make_the_two_sets_differ_and_the_record_covers_them() -> None:
    """Property: the literal record spans exactly the `n + 1` years the manuscript's set names, so the code's slice can
    only drop the most recent, and the forty draws outnumber the years by far.

    Why: a record with more years than the set would give the slice a year to draw in place of the one it drops, and
    too few draws would leave a year of the set unseen by chance.
    Kills: a record extended past `y_r`; an `n` that leaves the slice one year; a handful of draws.
    Class: NEW CAPABILITY, and a fitness function over the literals. The plausible wrong implementation is a record
    with a fourth year, under which the code's slice still draws the manuscript's set.
    Second correct implementation: a record of two or four years with the look-back changed to match.
    """
    subject = _subject()
    assert len(subject.TIDE_YEARS_RECORDED) == subject.TIDE_YEARS_TO_LOOK_BACK + 1
    assert list(subject.TIDE_YEARS_RECORDED) == sorted(subject.TIDE_YEARS_RECORDED)
    assert len(subject.TIDE_YEARS_RECORDED) * 10 <= subject.TIDE_DRAWS


# ---------------------------------------------------------------------------------------------------------------
# Every equation: the stored text's outcome, repeatability, the random stream, written files.
# ---------------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("equation_id", RED_ROWS)
def test_each_diverging_row_is_red_with_a_divergence_carrying_the_ledger_binding(
    equation_id: str, tmp_path: Path
) -> None:
    """Property: run unpatched against the stored text, E-20, E-28 and E-31 each raise `ConformanceDivergence` for
    their own id and raise nothing else; the failure's binding and reachability are the ledger row's, and it holds two
    values that differ.

    Why: a red is admitted exactly when it is a `ConformanceDivergence`; a red whose cause is the plumbing would
    record a divergence the text does not have.
    Kills: a `TypeError` from an unrendered value; a `SystemExit` from an unpassed stop; a green where the register
    records a departure.
    Class: NEW CAPABILITY. The plausible wrong implementation is an equation test that agrees with the stored text,
    so that nothing is raised; this test is red under it.
    Second correct implementation: other literal inputs for the same three rows that still leave each red.
    Flip condition: an owning system lands an implementation that the stored text no longer stands in for; these rows
    then assert a red the flipped equation no longer owes.
    """
    failure = _divergence(equation_id, tmp_path)
    row = bindings.load()[equation_id]
    assert failure.binding == bindings.binding_of(row)
    assert failure.reachable == bindings.reachable_of(row)


def test_the_one_agreeing_row_returns_without_raising(tmp_path: Path) -> None:
    """Property: run unpatched against the stored text, E-26 returns without raising.

    Why: the register records no departure for the instantaneous-peak branch, so a red here would be an input the
    cell cannot take or a reference that is wrong.
    Kills: a red from a shift the cell rounds; a lag read from the wrong level of the index.
    Class: NEW CAPABILITY. The plausible wrong implementation is a lag read in the other direction, under which the
    stored text is refused; this test is red under it.
    Second correct implementation: another whole-hour target the cell reaches.
    Flip condition: the same as for the diverging rows, read the other way.
    """
    _call("E-26", tmp_path)


@pytest.mark.parametrize("equation_id", SEEDED_ROWS)
def test_each_drawing_row_repeats_and_leaves_numpys_global_stream_as_it_found_it(
    equation_id: str, tmp_path: Path
) -> None:
    """Property: with numpy's global generator in a known state, running the equation test leaves that state
    unchanged, and a second run gives the same observed value as the first.

    Why: the legacy texts draw through numpy's global state; a test that left it moved would change what a later test
    draws, and one that did not seed it would give a different value each run.
    Kills: a seed that is not restored; a seed removed from the call.
    Class: NEW CAPABILITY. The plausible wrong implementation is an equation test that does not seed numpy's global
    generator, so that two runs differ; this test is red under it.
    Second correct implementation: the same test with another seed value.
    """
    state = np.random.get_state()
    np.random.seed(11)
    before = np.random.get_state()
    values = []
    try:
        for attempt in range(2):
            try:
                handed = tmp_path / f"run{attempt}"
                handed.mkdir()
                _call(equation_id, handed)
            except equation_check.ConformanceDivergence as divergence:
                values.append(divergence.failure.observed)
            else:
                values.append(None)
            after = np.random.get_state()
            assert after[0] == before[0]
            np.testing.assert_array_equal(after[1], before[1])
            assert after[2:] == before[2:]
    finally:
        np.random.set_state(state)
    if equation_id in RED_ROWS:
        assert all(each is not None for each in values)
        if isinstance(values[0], xr.Dataset):
            xr.testing.assert_identical(values[0], values[1])
        else:
            np.testing.assert_array_equal(values[0], values[1])
    else:
        assert values == [None, None]


def test_the_equation_tests_write_only_inside_the_directory_they_are_handed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Property: with the working directory an empty temporary directory, running the four equation tests leaves
    that directory empty and puts every file the cells wrote under the directory the tests were handed, with the same
    outcomes as from the checkout.

    Why: the cells write their outputs through paths bound as literals; a path left at its declared value would write
    into the checkout, and a test that did so would leave the tree changed by a run.
    Kills: a directory binding left out; a relative path; an outcome that depends on where a run starts.
    Class: NEW CAPABILITY. The plausible wrong implementation is a directory binding that is relative, under which the
    run writes into the working directory and the listing is not empty.
    Second correct implementation: tests that write under another handed directory.
    """
    working = tmp_path / "working"
    handed = tmp_path / "handed"
    working.mkdir()
    handed.mkdir()
    monkeypatch.chdir(working)
    outcomes = {}
    for equation_id in ROWS:
        (handed / equation_id).mkdir()
        try:
            _call(equation_id, handed / equation_id)
        except equation_check.ConformanceDivergence:
            outcomes[equation_id] = "red"
        else:
            outcomes[equation_id] = "green"
    assert outcomes == {"E-20": "red", "E-26": "green", "E-28": "red", "E-31": "red"}
    assert os.listdir(working) == []
    assert any((handed / "E-20").rglob("*"))
    assert any((handed / "E-26").rglob("*"))


# ---------------------------------------------------------------------------------------------------------------
# The structure of the four functions.
# ---------------------------------------------------------------------------------------------------------------


def _functions() -> dict[str, ast.FunctionDef]:
    """The four equation test functions of the tested file, parsed from its source, by equation id."""
    module = ast.parse(Path(_subject().__file__).read_text(encoding="utf-8"))
    by_name = {each.name: each for each in module.body if isinstance(each, ast.FunctionDef)}
    return {equation_id: by_name[name] for equation_id, (name, _unit) in ROWS.items()}


def _calls_at(node: ast.AST, name: str) -> list[ast.Call]:
    """Every call of the bare name `name` at or below `node`."""
    return [
        each
        for each in ast.walk(node)
        if isinstance(each, ast.Call) and isinstance(each.func, ast.Name) and each.func.id == name
    ]


def _body_nodes(function: ast.FunctionDef) -> list[ast.AST]:
    """Every node in the function's body, nested functions included; its decorators and annotations are left out."""
    return [node for statement in function.body for node in ast.walk(statement)]


def _named_calls(function: ast.FunctionDef, name: str) -> list[ast.Call]:
    """Every call of the bare name `name` in the function's body."""
    return [call for statement in function.body for call in _calls_at(statement, name)]


def test_the_files_conformance_functions_are_the_four_ledger_rows_and_carry_no_skip_or_xfail() -> None:
    """Property: the module's conformance-marked functions are exactly the four named in the ledger, each with one
    decorator-level marker whose `unit` is the id, whose `part` is its own name and whose `system` is the system slug,
    every keyword a string literal in the source, and no skip, no expected-failure and no parametrize marker.

    Why: the test name and the decorator site are fixed by the ledger, a marker whose value is a module constant
    cannot be read by the static inventory, and a red hidden behind a skip or an expected failure is a recorded
    divergence lost.
    Kills: `pytestmark = ...` at module level; a renamed function; a marker keyword that names a constant; an added
    `@pytest.mark.xfail`; a fifth conformance function.
    Class: NEW CAPABILITY, and a fitness function over the file's decorators, stated without the equations' values.
    The plausible wrong implementation is a marker keyword that names a constant, or an added expected-failure marker.
    Second correct implementation: the same four functions with other helpers beside them in the module.
    """
    subject = _subject()
    carried = {
        name: each
        for name, each in vars(subject).items()
        if callable(each) and any(mark.name == "conformance" for mark in getattr(each, "pytestmark", []))
    }
    assert not hasattr(subject, "pytestmark")
    assert sorted(carried) == sorted(name for name, _unit in ROWS.values())
    ledger = bindings.load()
    for equation_id, (name, _unit) in ROWS.items():
        assert ledger[equation_id]["binding"] == "TEST"
        assert ledger[equation_id]["test"] == name
        marks = carried[name].pytestmark
        assert [mark.name for mark in marks] == ["conformance"]
        assert marks[0].kwargs == {"unit": equation_id, "part": name, "system": SYSTEM}
        for decorator in _functions()[equation_id].decorator_list:
            assert isinstance(decorator, ast.Call)
            assert all(
                isinstance(keyword.value, ast.Constant) and isinstance(keyword.value.value, str)
                for keyword in decorator.keywords
            )


def test_each_equation_test_runs_one_stored_unit_and_ends_in_one_check_under_its_licence() -> None:
    """Property: in each equation test, exactly one `implementation` call, whose unit is a module constant naming the
    legacy unit the equation's row lists and one lift declaration; exactly one `check` call, the last statement of
    the function, handed four arguments; the licence `Tolerance(rtol=1e-9, atol=0)` for E-20 and E-26 and `Exact()`
    for E-28 and E-31; and no assertion, handler, lift engine, skip, or other comparison in the body.

    Why: a comparison that does not end in `check` cannot produce a `ConformanceDivergence`, a run of a legacy text
    outside the helper escapes the stop-marker recorder the declaration binds, and a looser licence would admit a
    gap the rows' values carry.
    Kills: a bare `assert`; a second `check`; a `try` around the call; `pytest.skip` in the body; a unit string typed
    in place of the constant; `Exact` on a tolerance row.
    Class: NEW CAPABILITY, and a fitness function over the four function bodies, stated without the equations' values.
    The plausible wrong implementation is an equation test that ends in a bare `assert`, a second `check`, or a
    different licence.
    Second correct implementation: the same calls through differently named locals, or in another order.
    """
    subject = _subject()
    assert subject.check is equation_check.check
    assert subject.implementation is equation_check.implementation
    assert subject.seeded_global_numpy is equation_check.seeded_global_numpy
    declared = lift.discover()
    for equation_id, function in _functions().items():
        _name, constant = ROWS[equation_id]
        calls = _named_calls(function, "implementation")
        assert len(calls) == 1
        assert isinstance(calls[0].args[0], ast.Name)
        assert calls[0].args[0].id == constant
        assert getattr(subject, constant) == UNITS[constant]
        assert [each.part for each in declared if each.unit == UNITS[constant]].count(None) == 1
        assert isinstance(calls[0].args[1], ast.Constant) and calls[0].args[1].value is None

        checks = _named_calls(function, "check")
        assert len(checks) == 1
        final = function.body[-1]
        assert isinstance(final, ast.Expr)
        assert final.value is checks[0]
        assert len(checks[0].args) == 4
        licence = checks[0].args[3]
        if equation_id in TOLERANCE_ROWS:
            assert isinstance(licence, ast.Name)
            assert licence.id == "TOLERANCE"
            assert subject.TOLERANCE.rtol == 1e-9
            assert subject.TOLERANCE.atol == 0.0
        else:
            assert equation_id in EXACT_ROWS
            assert isinstance(licence, ast.Call)
            assert isinstance(licence.func, ast.Attribute)
            assert licence.func.attr == "Exact"
            assert not licence.args
            assert not licence.keywords

        forbidden = (ast.Assert, ast.Try, ast.ExceptHandler)
        assert not [each for each in _body_nodes(function) if isinstance(each, forbidden)]
        names = {each.id for each in _body_nodes(function) if isinstance(each, ast.Name)}
        assert not names & {"lift", "run_lift", "pytest", "equation_check"}
        compare_attributes = {
            each.attr
            for each in _body_nodes(function)
            if isinstance(each, ast.Attribute) and isinstance(each.value, ast.Name) and each.value.id == "compare"
        }
        assert compare_attributes <= {"Exact"}


def test_each_drawing_equation_runs_its_stored_unit_inside_a_seeded_block() -> None:
    """Property: in E-20, E-26 and E-31 the `implementation` call sits inside a `with seeded_global_numpy(SEED)` block
    and `SEED` is an integer; E-28, which draws nothing, has no seeded block.

    Why: a draw outside the block uses a stream no test steers, so the red or green changes from run to run.
    Kills: a seed moved after the call; a seed that is a float or a name that is never bound; a seed on a row whose
    unit draws nothing.
    Class: NEW CAPABILITY, and a fitness function over the function bodies. The plausible wrong implementation is a
    drawing row with its seed removed or moved off the call; it is red under both.
    Second correct implementation: a seeded block that wraps more statements than the call.
    """
    subject = _subject()
    assert isinstance(subject.SEED, int)
    for equation_id, function in _functions().items():
        blocks = [
            each
            for each in _body_nodes(function)
            if isinstance(each, ast.With)
            and any(
                isinstance(item.context_expr, ast.Call)
                and isinstance(item.context_expr.func, ast.Name)
                and item.context_expr.func.id == "seeded_global_numpy"
                and [getattr(argument, "id", None) for argument in item.context_expr.args] == ["SEED"]
                for item in each.items
            )
        ]
        if equation_id in SEEDED_ROWS:
            assert len(blocks) == 1
            inside = [call for statement in blocks[0].body for call in _calls_at(statement, "implementation")]
            assert len(inside) == 1
        else:
            assert blocks == []


def test_each_docstring_quotes_the_registers_span_for_its_equation() -> None:
    """Property: each equation test's docstring contains, verbatim, the `verbatim_span` the register holds for the id
    in its marker.

    Why: the span is the manuscript text the test implements; a docstring that quotes another text, or a paraphrase,
    leaves a reader unable to tell what the reference was written from.
    Kills: a span edited to match a reading; a docstring left from another row; a span shortened.
    Class: NEW CAPABILITY. The plausible wrong implementation is a docstring that quotes another row's span or a
    shortened span; it is red under both.
    Second correct implementation: the same docstring with other prose around the quoted span.
    """
    for equation_id in ROWS:
        span = register.entry(equation_id)["verbatim_span"]
        assert span in (_function(equation_id).__doc__ or "")
