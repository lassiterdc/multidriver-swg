"""Review tests for the rain-rescaling equation tests: E-04, E-21, E-22, E-23, E-24 and E-25 in `test_equations_rain_rescaling.py`.

Four of the six equation tests are red on build by design, and a red alone cannot tell a test that found the
recorded paper/code departure from a test whose own reference, literal inputs or plumbing is wrong: both end
in the same `ConformanceDivergence`. The tests below separate the two by giving each equation test a SECOND
implementation to run against, written here from the manuscript's text and sharing no text with the legacy
unit or with that test's own reference. A second implementation that follows the manuscript must turn the
test green; a form that departs from it as the register records must keep it red; the stored legacy text
must leave the four recorded rows red with a divergence and the other two green. The two rows that stay green
on the stored text get a must-fire control, a wrong form that turns each of them red.

Where the recorded departure is a single rule of the legacy text (E-24's `<=`, E-25's cap), the second
implementation is that very text with the one rule changed (`np.isclose` made strict for the call; the cap
argument raised), so the test is shown to be red because of that rule and for no other reason.

The second implementations enter through `lift.run_lift`, the one engine every equation test's call to
`equation_check.implementation` must reach. The declaration lookup, the override refusal and the test's own
reduction all still run; only the compiled legacy text is replaced by a namespace of these functions.

These carry no family marker, so they enter no inventory count; they are companion tests, and this file guards
`tests/conformance/test_equations_rain_rescaling.py`. No test here pins an absolute count or a stored blob.

Not established: that the paper's value is the value these second implementations compute. They are one
reading of the manuscript's text. Two readings are shared with the tested file and so are not checked by it:
the number of windows a dilated series is written back on (the legacy text stops at the start of the last
dilated step, `floor((n - 1) * dt' / dt) + 1` windows, and the manuscript does not state a count), and the
reading of E-24's test on the series' depth. Both come from the rows the tested file was written against.
"""

from __future__ import annotations

import ast
import sys
from collections.abc import Callable, Mapping
from fractions import Fraction
from math import ceil, floor
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest

from tests.conformance import bindings, equation_check, test_equations_rain_rescaling
from tests.correspondence import lift

SUBJECT = "tests.conformance.test_equations_rain_rescaling"
SYSTEM = "publication-math-conformance"

# The six rows this file owns, with the function name the ledger and the manuscript's bound-test column give
# each. The domain is this file's own rows; every other row belongs to another file.
EQUATION_TESTS = {
    "E-04": "test_eq04_watershed_mean_intensity",
    "E-21": "test_eq21_intensity_multiplier",
    "E-22": "test_eq22_timestep_dilation",
    "E-23": "test_eq23_overlap_weighted_resample",
    "E-24": "test_eq24_isclose_tolerance",
    "E-25": "test_eq25_rescaling_fixed_point",
}
# Which of them the stored legacy text leaves red with a divergence, and which it leaves green.
RED_ON_LEGACY = ("E-04", "E-23", "E-24", "E-25")
GREEN_ON_LEGACY = ("E-21", "E-22")

LOOP = "rescale_rainfall_timeseries"
DILATION = "rescale_rain_tseries_to_match_target_depth"
# The attempt cap the legacy loop is called with when its caller does not lower it.
DEFAULT_CAP = 50


def _subject() -> ModuleType:
    """The module under review, read from `sys.modules` so a probe that substitutes a variant of it is seen.

    The module is also imported by name at the top of this file, so the selection of tests to run after a
    change to it finds this file by its import and not only by its declared path.
    """
    return sys.modules.get(SUBJECT, test_equations_rain_rescaling)


def _request_for(test: Callable[..., Any]) -> object:
    """A stand-in for pytest's request whose node answers with the test's own `conformance` marker.

    Reading the marker off the test function, rather than writing the id here, keeps the equation id the
    one the decorator states, which is the id `check` attributes a red to.
    """
    marker = next(each for each in test.pytestmark if each.name == "conformance")  # type: ignore[attr-defined]

    def get_closest_marker(name: str) -> object | None:
        """Return the test's conformance marker for that name and nothing for any other."""
        return marker if name == "conformance" else None

    return SimpleNamespace(node=SimpleNamespace(get_closest_marker=get_closest_marker))


def _run_against(monkeypatch: pytest.MonkeyPatch, namespace: Mapping[str, Any]) -> None:
    """Make every `lift.run_lift` call hand the test's reduction `namespace` in place of the legacy text's.

    The declaration's own bindings are built and discarded, so the helper's unit lookup and its override
    check still run on the real declaration; only the execution of the stored legacy text is replaced. The
    declaration's fixtures are loaded the way the engine loads them, because the rescaling units' bindings
    are built from stored frames and refuse to build without them.
    """

    def run_lift(declaration: lift.LiftDeclaration) -> Any:
        """Build the declaration's bindings as the engine would, then reduce over the replacement namespace."""
        _bindings, reduce = declaration.function(lift._load(declaration))
        return reduce(namespace)

    monkeypatch.setattr(lift, "run_lift", run_lift)


def _legacy(unit: str, name: str) -> Callable[..., Any]:
    """The stored legacy function `name` of `unit`, obtained through the helper before any substitution."""
    return equation_check.implementation(unit, None, lambda finished: finished[name])


def _call(equation_id: str) -> None:
    """Call the equation's test as pytest calls it: with a request carrying the test's own marker."""
    test = getattr(_subject(), EQUATION_TESTS[equation_id])
    test(_request_for(test))


def _refused(equation_id: str) -> equation_check.ConformanceDivergence:
    """Call the equation's test and return the divergence it raises, failing if it raises nothing."""
    with pytest.raises(equation_check.ConformanceDivergence) as refused:
        _call(equation_id)
    assert refused.value.failure.equation_id == equation_id
    assert refused.value.failure.diverged is True
    return refused.value


# --- the second implementations -------------------------------------------------------------------------


def _dilate_and_average(frame: pd.DataFrame, target_depth: float, tstep: pd.Timedelta, *, scale: float = 1.0):
    """The manuscript's steps b and c on a one-column series: dilate to the target depth, average per window.

    The dilated step is the target depth over the series' sum (times `scale`, which only a deliberately wrong
    form changes). Dilated step `k` covers `[k*dt', (k+1)*dt')`. The series is written back on windows of
    `tstep` from zero, one for each window that starts at or before the start of the last dilated step, and
    each window carries the PLAIN mean of the dilated values whose steps overlap it for a positive duration.
    The overlap is found from the index range of those steps, in exact rational arithmetic, so no float
    comparison decides which step a boundary belongs to.
    """
    values = frame["mm_per_hr"].to_numpy(dtype=float)
    dilated = Fraction(target_depth) / Fraction(float(values.sum())) * Fraction(scale)
    window = Fraction(tstep / pd.Timedelta("1h"))
    n_windows = floor((len(values) - 1) * dilated / window) + 1
    averaged = []
    for index in range(n_windows):
        first = floor(index * window / dilated)
        last = ceil((index + 1) * window / dilated) - 1
        covered = values[max(first, 0) : min(last, len(values) - 1) + 1]
        averaged.append(float(covered.mean()))
    steps = pd.timedelta_range(start="0 hours", periods=n_windows, freq=tstep, name=frame.index.name)
    return pd.DataFrame({"mm_per_hr": averaged}, index=steps)


def _cell_columns(frame: pd.DataFrame, *, every_other_column: bool) -> list[str]:
    """The columns a watershed mean runs over.

    The manuscript's mean is over the grid cells, which these literal frames name `grid_cell_*`. The legacy
    rule takes every column that is not the mean's own, which is what `every_other_column` selects.
    """
    if every_other_column:
        return [each for each in frame.columns if "mm_per_hr" not in each]
    return [each for each in frame.columns if each.startswith("grid_cell")]


def _rescale_in_one_pass(*, every_other_column: bool = False, inverse_ratio: bool = False) -> Callable[..., Any]:
    """A rescaling that applies the manuscript's multiplier once and returns, with no dilation.

    The manuscript's steps are: take the watershed mean over the cells, multiply the cells by `PR_g/PR_h`
    where `PR_h` is the peak of that mean's four-hour mean, and take the mean again. The dilation is the
    identity wherever the target depth equals the multiplied series' depth, which is what the literal frames
    of E-04 and E-21 arrange, so this form is a second implementation for those two rows only.
    """

    def rescale(row, frame, _max_duration, _n_cutoff, _rtol, _atol, intensity_stat):
        """Return the multiplied frame and an empty problem text, as the legacy loop does on success."""
        cells = _cell_columns(frame, every_other_column=every_other_column)
        peak = frame[cells].mean(axis=1).rolling("4h").mean().max()
        ratio = row[intensity_stat] / peak
        multiplier = 1.0 / ratio if inverse_ratio else ratio
        scaled = frame.copy()
        scaled[cells] = frame[cells] * multiplier
        scaled["mm_per_hr"] = scaled[cells].mean(axis=1)
        return scaled, ""

    return rescale


def _strict_isclose(left: Any, right: Any, rtol: float = 1e-5, atol: float = 1e-8, equal_nan: bool = False) -> Any:
    """`np.isclose` with the manuscript's strict inequality: `|left - right| < atol + rtol * |right|`."""
    return abs(left - right) < atol + rtol * abs(right)


def _legacy_loop_with(monkeypatch: pytest.MonkeyPatch, legacy: Callable[..., Any], *, strict: bool, cap: int | None):
    """The stored legacy loop with one rule changed for the call: a strict tolerance test, a raised cap, or both."""

    def loop(row, frame, max_duration, n_cutoff, rtol, atol, intensity_stat):
        """Run the stored loop; the changed rule is in force only inside this call."""
        with monkeypatch.context() as inner:
            if strict:
                inner.setattr(np, "isclose", _strict_isclose)
            return legacy(row, frame, max_duration, n_cutoff if cap is None else cap, rtol, atol, intensity_stat)

    return loop


# --- the stored text -----------------------------------------------------------------------------------


@pytest.mark.parametrize("equation_id", RED_ON_LEGACY)
def test_the_four_recorded_rows_are_red_with_a_conformance_divergence_on_the_stored_legacy_text(
    equation_id: str,
) -> None:
    """Property: run unpatched, against the stored legacy text, each of E-04, E-23, E-24 and E-25 raises
    `ConformanceDivergence` for its own equation id, and raises nothing else.

    Why: a red is admitted exactly when it is a `ConformanceDivergence`, and a red of any other kind (an
    unrunnable lift, a `SystemExit` from the legacy text's own exits, a `TypeError` from a value no licence
    reads) is a defect that a classifier would be handed as a divergence.
    Kills: a literal input that makes the legacy text stop inside itself; a bare `assert` in place of `check`.
    """
    _refused(equation_id)


@pytest.mark.parametrize("equation_id", GREEN_ON_LEGACY)
def test_the_two_rows_with_no_recorded_departure_are_green_on_the_stored_legacy_text(equation_id: str) -> None:
    """Property: run unpatched, against the stored legacy text, E-21 and E-22 return without raising.

    Why: the register records no departure for either, so a red here would be a divergence the register does
    not know about, or a test whose reference is wrong.
    Kills: a reference that carries a convention the legacy text does not (a different window count for E-22,
    a different statistic for E-21's ratio).
    """
    _call(equation_id)


# --- E-04 -----------------------------------------------------------------------------------------------


def test_e04_is_green_against_a_mean_taken_over_the_grid_cells_alone(monkeypatch: pytest.MonkeyPatch) -> None:
    """Property: with an implementation whose watershed mean runs over the grid cells only, the E-04 test
    returns without raising, so its red on the stored text is the third column's entry into the mean and
    not a reference that no implementation could meet.

    Why: the equation is an identity over the cells the implementation returns, `mm_per_hr` equals their
    mean. A reference that is the mean of the INPUT cells equals the returned series only where the
    multiplier is one, and the literal target here makes it two, so such a reference is red on the manuscript's
    own mean, and its red would stay after the code is corrected.
    Kills: an expected value that is the input cells' mean; an expected value that leaves out the multiplier
    the target imposes.
    """
    _run_against(monkeypatch, {LOOP: _rescale_in_one_pass()})
    _call("E-04")


def test_e04_is_red_against_a_mean_that_takes_every_column_that_is_not_the_rainfall_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Property: with the legacy rule behind the call (every column but the mean's own enters the mean), the
    E-04 test raises `ConformanceDivergence` for its own id.

    Why: an input on which the extra column is invisible is a vacuous green once the code adopts the rule.
    Kills: a frame without a non-rainfall column; a returned-series comparison that the loop's rescaling to the
    target peak makes blind to the extra column.
    """
    _run_against(monkeypatch, {LOOP: _rescale_in_one_pass(every_other_column=True)})
    _refused("E-04")


# --- E-21 -----------------------------------------------------------------------------------------------


def test_e21_is_green_against_the_manuscripts_multiplier(monkeypatch: pytest.MonkeyPatch) -> None:
    """Property: with a multiplier of `PR_g / PR_h`, the E-21 test returns without raising."""
    _run_against(monkeypatch, {LOOP: _rescale_in_one_pass()})
    _call("E-21")


def test_e21_is_red_against_the_inverted_multiplier(monkeypatch: pytest.MonkeyPatch) -> None:
    """Property: with the multiplier inverted (`PR_h / PR_g`), the E-21 test raises `ConformanceDivergence`.

    Why: a green row needs a control that can turn it red, or the stored text's green says nothing about
    the multiplier. The legacy loop iterates until the target is met, so on a constant series it reaches the
    same series by any multiplicative path; the control therefore runs a form that applies its multiplier
    once.
    Kills: a reference that equals the target series whatever the ratio; a literal with `PR_g = PR_h`.
    """
    _run_against(monkeypatch, {LOOP: _rescale_in_one_pass(inverse_ratio=True)})
    _refused("E-21")


# --- E-22 and E-23 --------------------------------------------------------------------------------------


def test_e22_is_green_against_the_manuscripts_dilated_step(monkeypatch: pytest.MonkeyPatch) -> None:
    """Property: with the dilated step the manuscript defines, the E-22 test returns without raising."""
    _run_against(monkeypatch, {DILATION: lambda frame, depth, tstep: _dilate_and_average(frame, depth, tstep)})
    _call("E-22")


def test_e22_is_red_against_a_dilated_step_that_is_five_per_cent_too_long(monkeypatch: pytest.MonkeyPatch) -> None:
    """Property: with the dilated step 5 per cent too long (2.1 h for 2.0 h), the E-22 test raises
    `ConformanceDivergence`, so the output's length is a carrier of the step the test can read.

    Why: the step is never returned as a number and the values are 2.0 at every window whatever the step is,
    so a series short enough that both steps give the same length would leave the row blind to its own subject.
    Kills: a literal series of four steps, where 2.0 h and 2.1 h both give seven windows.
    """
    _run_against(
        monkeypatch,
        {DILATION: lambda frame, depth, tstep: _dilate_and_average(frame, depth, tstep, scale=1.05)},
    )
    _refused("E-22")


def test_e23_is_green_against_the_plain_average_over_each_window(monkeypatch: pytest.MonkeyPatch) -> None:
    """Property: with a resampling that takes the plain average of the dilated values in each window, the
    E-23 test returns without raising, so its red on the stored text is the overlap weighting.

    Kills: a reference that differs from the plain average at windows where only one dilated step lies (the
    two forms agree there), or at a boundary a dilated step only touches.
    """
    _run_against(monkeypatch, {DILATION: lambda frame, depth, tstep: _dilate_and_average(frame, depth, tstep)})
    _call("E-23")


# --- E-24 and E-25 --------------------------------------------------------------------------------------


def test_e24_is_green_against_the_stored_loop_with_a_strict_tolerance_test(monkeypatch: pytest.MonkeyPatch) -> None:
    """Property: with the stored loop changed in one rule, `np.isclose`'s `<=` made the manuscript's `<`, the
    E-24 test returns without raising, so its red on the stored text is that one rule and nothing else.

    Why: the test's value carries the depth beside the decision. If any other difference separated the
    stored loop from the paper on this input, the stand-in would stay red.
    """
    legacy = _legacy(_subject().RESCALING_UNIT, LOOP)
    _run_against(monkeypatch, {LOOP: _legacy_loop_with(monkeypatch, legacy, strict=True, cap=None)})
    _call("E-24")


def test_e25_is_green_against_the_stored_loop_with_the_attempt_cap_lifted(monkeypatch: pytest.MonkeyPatch) -> None:
    """Property: with the stored loop called at a cap of 50 where the test asks for 1, the E-25 test returns
    without raising, so its red on the stored text is the cap and nothing else.

    Why: the register records the cap as the departure. A target the loop cannot meet at any cap leaves the
    returned series unmet whatever the cap is, so that input shows a departure the register does not name
    (and the red would stay when the cap is gone). A target the loop meets on a later attempt shows the cap.
    Kills: a constant series whose resampled depth is a fixed point below the target (it returns 8.0 for a
    target of 10.0 at a cap of 1, 2 or 50).
    """
    legacy = _legacy(_subject().RESCALING_UNIT, LOOP)
    _run_against(monkeypatch, {LOOP: _legacy_loop_with(monkeypatch, legacy, strict=False, cap=DEFAULT_CAP)})
    _call("E-25")


# --- the file's own shape -------------------------------------------------------------------------------


def test_the_files_conformance_functions_are_the_six_ledger_rows_and_carry_no_skip_or_xfail() -> None:
    """Property: the module's conformance-marked functions are exactly E-04, E-21, E-22, E-23, E-24 and E-25,
    each named as the ledger names it, each carrying one decorator-level marker whose `unit` is its id, whose
    `part` is its own name and whose `system` is the system slug, and none carrying a skip, an xfail or a
    parametrize.

    Why: the test names and the decorator site are fixed by the ledger, and a red hidden behind xfail or skip
    is forbidden.
    Kills: `pytestmark = ...` at module level; a function renamed from the ledger's name; an added
    `@pytest.mark.xfail`; a second conformance function for an id.
    """
    subject = _subject()
    carried = {
        name: each
        for name, each in vars(subject).items()
        if callable(each) and any(mark.name == "conformance" for mark in getattr(each, "pytestmark", []))
    }
    assert not hasattr(subject, "pytestmark")
    assert sorted(carried) == sorted(EQUATION_TESTS.values())
    ledger = bindings.load()
    for equation_id, name in EQUATION_TESTS.items():
        assert ledger[equation_id]["binding"] == "TEST"
        assert ledger[equation_id]["test"] == name
        marks = carried[name].pytestmark
        assert [mark.name for mark in marks] == ["conformance"]
        assert marks[0].kwargs == {"unit": equation_id, "part": name, "system": SYSTEM}


def _own_statements(body: list[ast.stmt]):
    """Yield every statement of a function body, descending into blocks but not into nested functions."""
    for statement in body:
        yield statement
        if isinstance(statement, ast.FunctionDef):
            continue
        for block in ("body", "orelse", "finalbody"):
            yield from _own_statements(getattr(statement, block, []))


def _names(node: ast.AST, context: type) -> set[str]:
    """The names under `node` that are read (`ast.Load`) or bound (`ast.Store`)."""
    return {each.id for each in ast.walk(node) if isinstance(each, ast.Name) and isinstance(each.ctx, context)}


def _written_bases(target: ast.AST) -> set[str]:
    """The names an assignment target changes: a name itself, or the base name of an attribute or subscript."""
    if isinstance(target, ast.Name):
        return {target.id}
    if isinstance(target, ast.Attribute | ast.Subscript):
        return _written_bases(target.value)
    if isinstance(target, ast.Tuple | ast.List):
        return set().union(*(_written_bases(each) for each in target.elts))
    return set()


def _dependencies(function: ast.FunctionDef) -> dict[str, set[str]]:
    """For each name the function binds, the names read by every statement that binds it or controls one."""
    deps: dict[str, set[str]] = {}

    def visit(statements: list[ast.stmt], control: set[str]) -> None:
        for statement in statements:
            if isinstance(statement, ast.FunctionDef):
                continue
            inner = control
            if isinstance(statement, ast.Assign):
                reads = _names(statement.value, ast.Load)
                for target in statement.targets:
                    for base in _written_bases(target):
                        deps.setdefault(base, set()).update(reads | _names(target, ast.Load) | control)
            elif isinstance(statement, ast.AugAssign):
                for base in _written_bases(statement.target):
                    deps.setdefault(base, set()).update(
                        _names(statement.value, ast.Load) | _names(statement.target, ast.Load) | {base} | control
                    )
            elif isinstance(statement, ast.For):
                reads = _names(statement.iter, ast.Load)
                for base in _written_bases(statement.target):
                    deps.setdefault(base, set()).update(reads | control)
                inner = control | reads
            elif isinstance(statement, ast.If | ast.While):
                inner = control | _names(statement.test, ast.Load)
            for block in ("body", "orelse", "finalbody"):
                visit(getattr(statement, block, []), inner)

    visit(function.body, set())
    return deps


def _is_equation_check_call(node: ast.AST, attribute: str) -> bool:
    """Whether `node` is a call of `equation_check.{attribute}(...)`."""
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == attribute
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "equation_check"
    )


def _equation_function(equation_id: str) -> ast.FunctionDef:
    """The syntax tree of the module's equation test for `equation_id`."""
    module = ast.parse(Path(_subject().__file__).read_text(encoding="utf-8"))
    return next(
        each for each in module.body if isinstance(each, ast.FunctionDef) and each.name == EQUATION_TESTS[equation_id]
    )


@pytest.mark.parametrize("equation_id", EQUATION_TESTS)
def test_an_equation_test_has_one_implementation_call_and_ends_in_one_check_call(equation_id: str) -> None:
    """Property: in each equation test, exactly one `equation_check.implementation` call and exactly one
    `equation_check.check` call, the check call last and given four arguments; the observed operand is a name
    that depends on the implementation's result; and the body reaches no lift engine and no comparison other
    than the licence it passes to `check`.

    Why: a test that runs a legacy text itself, or calls `check` twice, bypasses the single attribution the
    helper gives a red.
    Kills: a call to `lift.run_lift` or `compare.assert_equivalent` in the test body; a second `check` call.
    """
    function = _equation_function(equation_id)
    own = list(_own_statements(function.body))
    calls = [each for statement in own for each in ast.walk(statement) if isinstance(each, ast.Call)]
    implementation_calls = [each for each in calls if _is_equation_check_call(each, "implementation")]
    check_calls = [each for each in calls if _is_equation_check_call(each, "check")]
    assert len(implementation_calls) == 1
    assert len(check_calls) == 1
    final = function.body[-1]
    assert isinstance(final, ast.Expr)
    assert final.value is check_calls[0]
    assert len(check_calls[0].args) == 4
    roots = {
        base
        for statement in own
        if isinstance(statement, ast.Assign) and statement.value is implementation_calls[0]
        for target in statement.targets
        for base in _written_bases(target)
    }
    assert roots, "the implementation's result is not bound to a name this walk can follow"
    deps = _dependencies(function)
    tainted = set(roots)
    grew = True
    while grew:
        grew = False
        for name, reads in deps.items():
            if name not in tainted and reads & tainted:
                tainted.add(name)
                grew = True
    observed_operand = check_calls[0].args[1]
    assert _names(observed_operand, ast.Load) & tainted

    licence_names = _names(check_calls[0].args[3], ast.Load)
    in_function = [each for each in ast.walk(function) if isinstance(each, ast.Name)]
    assert not [each.id for each in in_function if each.id in {"lift", "run_lift"}]
    assert sum(1 for each in in_function if each.id == "compare") == (1 if "compare" in licence_names else 0)
    comparison_attributes = {
        each.attr
        for each in ast.walk(function)
        if isinstance(each, ast.Attribute) and isinstance(each.value, ast.Name) and each.value.id == "compare"
    }
    assert comparison_attributes <= {"Exact", "Tolerance"}


@pytest.mark.parametrize("equation_id", ["E-21", "E-22", "E-23", "E-24", "E-25"])
def test_an_equation_test_computes_its_expected_value_without_the_implementations_result(equation_id: str) -> None:
    """Property: in E-21, E-22, E-23, E-24 and E-25, the expected operand of `check` depends on no name bound
    from the implementation's result.

    Why: an expected value that depends on the observed one makes every comparison agree, and a recorded
    divergence is then lost without any test going red. E-04 is not in this list: its equation is an identity
    over the cells the implementation returns, so its reference reads those cells (and not the returned
    `mm_per_hr`) by construction.
    Kills: `expected = observed`; `expected = series["mm_per_hr"]`.
    """
    function = _equation_function(equation_id)
    own = list(_own_statements(function.body))
    calls = [each for statement in own for each in ast.walk(statement) if isinstance(each, ast.Call)]
    implementation_call = next(each for each in calls if _is_equation_check_call(each, "implementation"))
    check_call = next(each for each in calls if _is_equation_check_call(each, "check"))
    roots = {
        base
        for statement in own
        if isinstance(statement, ast.Assign) and statement.value is implementation_call
        for target in statement.targets
        for base in _written_bases(target)
    }
    deps = _dependencies(function)
    tainted = set(roots)
    grew = True
    while grew:
        grew = False
        for name, reads in deps.items():
            if name not in tainted and reads & tainted:
                tainted.add(name)
                grew = True
    assert not _names(check_call.args[2], ast.Load) & tainted
