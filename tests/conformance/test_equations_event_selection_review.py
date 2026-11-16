"""Review tests for the event-selection equation tests: E-05, E-08 and E-09 in `test_equations_event_selection.py`.

An equation test of this family is red on build by design, and a red alone cannot tell a test that found
the recorded paper/code divergence from a test whose own reference or plumbing is wrong: both end in the
same `ConformanceDivergence`. The tests below separate the two by giving each equation test a SECOND
implementation to run against, written here from the manuscript and sharing no text with the legacy unit
or with that test's own reference. A faithful second implementation must turn the test green; the divergent
form the register records must turn it red; the stored legacy text must leave it red with a divergence.
They also pin the file's own shape: which functions carry a conformance marker and that no test's expected
value is computed from the value the implementation returned.

The second implementations enter through `lift.run_lift`, the one engine every equation test's call to
`equation_check.implementation` must reach. The declaration lookup, the override refusal and the test's own
reduction all still run; only the compiled legacy text is replaced by a namespace of these functions.

These carry no family marker, so they enter no inventory count; they are companion tests and this file guards
`tests/conformance/test_equations_event_selection.py` (a module test of it on promotion; no test here pins
an absolute count or a stored blob).

Not established: that the paper's value is the value these second implementations compute. They are one
reading of the manuscript's text, and where that text is silent or ambiguous (the surge expansion's stop
rule, the rain event's bounding) they take the plainest reading the tested file's literal inputs cannot tell apart.
"""

from __future__ import annotations

import ast
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest

from tests.conformance import bindings, equation_check, test_equations_event_selection
from tests.correspondence import lift

SUBJECT = "tests.conformance.test_equations_event_selection"
SYSTEM = "publication-math-conformance"

# The three rows this file owns, with the function name the ledger and the manuscript's bound-test column
# give each. The domain is this file's own rows; every other row belongs to another file.
EQUATION_TESTS = {
    "E-05": "test_eq05_moving_depth_sum",
    "E-08": "test_eq08_surge_window_expansion",
    "E-09": "test_eq09_combined_pairing",
}
# The legacy function each equation test calls, by the name its reduction reads from the finished namespace.
LEGACY_FUNCTION = {
    "E-05": "event_selection_threshold_or_nstorms",
    "E-08": "surge_event_selection",
    "E-09": "determine_compound_event_start_and_end_times",
}


def _subject() -> ModuleType:
    """The module under review, read from `sys.modules` so a probe that substitutes a variant of it is seen.

    The module is also imported by name at the top of this file, so the selection of tests to run after a
    change to it finds this file by its import and not only by its declared path.
    """
    return sys.modules.get(SUBJECT, test_equations_event_selection)


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


def _run_against(monkeypatch: pytest.MonkeyPatch, equation_id: str, implementation: Callable[..., Any]) -> None:
    """Make every `lift.run_lift` call hand the test's reduction a namespace holding `implementation`.

    The declaration's own bindings are built and discarded, so the helper's unit lookup and its override
    check still run on the real declaration; only the execution of the stored legacy text is replaced.
    """
    namespace: Mapping[str, Any] = {LEGACY_FUNCTION[equation_id]: implementation}

    def run_lift(declaration: lift.LiftDeclaration) -> Any:
        """Build the declaration's bindings as the engine would, then reduce over the replacement namespace."""
        _bindings, reduce = declaration.function({})
        return reduce(namespace)

    monkeypatch.setattr(lift, "run_lift", run_lift)


def _rain_selection(window_of: Callable[[pd.Timedelta, pd.Timedelta | None], pd.Timedelta]) -> Callable[..., Any]:
    """A rain-event selection whose moving-sum window is `window_of(min_interevent_time, threshold_duration)`.

    The first event is bounded outward from the end of the heaviest window to the nearest gaps of at least
    `min_interevent_time` with no rain, as the manuscript's step 5 reads. `window_of` is the one thing the
    paper and the legacy text disagree on, so the faithful form passes `threshold_duration` and the divergent
    form passes `min_interevent_time`.
    """

    def select(
        rainfall: pd.Series,
        min_interevent_time: pd.Timedelta,
        max_strm_len: pd.Timedelta,
        str_data_source: str,
        target_events_per_year: float | None = None,
        threshold: float | None = None,
        threshold_duration: pd.Timedelta | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame, float | None, float]:
        """Return the summary of the first event the window rule selects, in the legacy function's shape."""
        times = rainfall.index
        step = times[1] - times[0]
        window = window_of(min_interevent_time, threshold_duration)
        depth = np.array([rainfall[(times >= each - window) & (times <= each)].sum() for each in times])
        wet = times[rainfall.to_numpy() > 0.0]
        start = wet[wet <= times[int(depth.argmax())]].max()
        end = start
        while len(wet[wet < start]) and start - wet[wet < start].max() - step < min_interevent_time:
            start = wet[wet < start].max()
        while len(wet[wet > end]) and wet[wet > end].min() - end - step < min_interevent_time:
            end = wet[wet > end].min()
        summary = pd.DataFrame({"rain_event_start": [start], "rain_event_end": [end]})
        return summary, pd.DataFrame(), threshold, 0.0

    return select


def _surge_selection(*, compare_one_hour_means: bool) -> Callable[..., Any]:
    """A surge-event selection that rebuilds the peak's window one timestep at a time, as the manuscript's step 3 reads.

    With `compare_one_hour_means` false it compares the two neighbouring values, as the equation writes. With
    it true it compares the mean of the hour before the window with the mean of the hour after it, which is the
    form the register records for the legacy text; the step is still one timestep so that comparison is the
    only difference between the two forms.
    """

    def select(
        s_surge: pd.Series,
        surge_threshold_to_determine_event_limits: float,
        max_event_duration_h: float,
        min_interevent_time: pd.Timedelta,
        str_data_source: str,
        target_events_per_year: float | None = None,
        threshold: float | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame, float, float]:
        """Return the summary of the event around the series' peak, in the legacy function's shape."""
        step = s_surge.index[1] - s_surge.index[0]
        hour = pd.Timedelta("1h")
        longest = pd.Timedelta(hours=max_event_duration_h)
        peak = s_surge.idxmax()
        below = s_surge[s_surge <= surge_threshold_to_determine_event_limits]
        start, end = below[:peak].index[-1], below[peak:].index[0]
        if end - start > longest:
            start = end = peak
            while end - start < longest:
                if compare_one_hour_means:
                    before = s_surge.loc[start - hour : start - step].mean()
                    after = s_surge.loc[end + step : end + hour].mean()
                else:
                    before, after = s_surge.loc[start - step], s_surge.loc[end + step]
                if before > after:
                    start -= step
                else:
                    end += step
        summary = pd.DataFrame({"surge_event_start": [start], "surge_event_end": [end]})
        return summary, pd.DataFrame(), float(s_surge.loc[peak]), 0.0

    return select


def _pairing(*, combine_on_any_overlap: bool) -> Callable[..., pd.DataFrame]:
    """A combined-event pairing that combines a rain and a surge event by the rule `combine_on_any_overlap` names.

    The faithful rule combines a pair when the surge event's peak lies inside the rain event, as the manuscript's
    step 1 reads. The divergent rule combines a pair when the two spans overlap at all, which is the trigger the
    register records for the legacy text. Every event left unpaired is returned alone and flagged not compound.
    """

    def pair(
        surge_tseries: pd.DataFrame,
        surge_summary: pd.DataFrame,
        rain_tseries: pd.DataFrame,
        rain_summary: pd.DataFrame,
    ) -> pd.DataFrame:
        """Return one row per combined event and one per event left alone, in the legacy function's columns."""
        rows: list[tuple[pd.Timestamp, pd.Timestamp, bool]] = []
        paired_rain: set[Any] = set()
        paired_surge: set[Any] = set()
        for surge_id, surge in surge_summary.iterrows():
            for rain_id, rain in rain_summary.iterrows():
                if combine_on_any_overlap:
                    joined = (
                        rain.rain_event_start <= surge.surge_event_end
                        and surge.surge_event_start <= rain.rain_event_end
                    )
                else:
                    joined = rain.rain_event_start <= surge.max_surge_tstep <= rain.rain_event_end
                if joined:
                    paired_rain.add(rain_id)
                    paired_surge.add(surge_id)
                    rows.append(
                        (
                            min(rain.rain_event_start, surge.surge_event_start),
                            max(rain.rain_event_end, surge.surge_event_end),
                            True,
                        )
                    )
        rows += [
            (r.rain_event_start, r.rain_event_end, False) for i, r in rain_summary.iterrows() if i not in paired_rain
        ]
        rows += [
            (s.surge_event_start, s.surge_event_end, False)
            for i, s in surge_summary.iterrows()
            if i not in paired_surge
        ]
        return pd.DataFrame(rows, columns=["event_start", "event_end", "compound"])

    return pair


FAITHFUL = {
    "E-05": _rain_selection(lambda min_interevent_time, threshold_duration: threshold_duration),
    "E-08": _surge_selection(compare_one_hour_means=False),
    "E-09": _pairing(combine_on_any_overlap=False),
}
DIVERGENT = {
    "E-05": _rain_selection(lambda min_interevent_time, threshold_duration: min_interevent_time),
    "E-08": _surge_selection(compare_one_hour_means=True),
    "E-09": _pairing(combine_on_any_overlap=True),
}


@pytest.mark.parametrize("equation_id", EQUATION_TESTS)
def test_an_equation_test_passes_against_an_implementation_that_follows_the_manuscript(
    monkeypatch: pytest.MonkeyPatch, equation_id: str
) -> None:
    """Property: with a paper-faithful implementation behind the call, the equation's test returns without
    raising, so its red on the stored text is the text's disagreement and not the test's own error.

    Class: each of the three equations of this file, one test per row. NEW CAPABILITY: the plausible wrong
    implementation of the tested equation test is one whose reference reads the legacy rule (E-05's window from the
    inter-event time, E-08's hourly means, E-09's any-overlap trigger), or whose reduction mis-reads the
    returned value; it is red here because the faithful value then differs from its expected one.
    Reachability: the call goes through the real `equation_check.implementation`, so the test's literal
    inputs, its reduction and its rendering all run; only the compiled legacy text is replaced.
    Both-states anchor: the oracle is that no exception is raised on an implementation that follows the
    manuscript; it turns on the test's reference and reduction, which exist before and after any change to
    the legacy text.
    Why: a test that is red under every implementation, faithful or not, reports a divergence on every
    equation whatever the code does, so the red carries no information about the code.
    Kills: a reference that equals the legacy rule; a reduction that reads the wrong row or column of the
    returned summary; a rendering that makes equal values compare unequal.
    Second correct implementation: any implementation of the equation that follows the manuscript, which is
    what the faithful functions above are; they share no text with the tested file's reference.
    """
    subject = _subject()
    test = getattr(subject, EQUATION_TESTS[equation_id])
    _run_against(monkeypatch, equation_id, FAITHFUL[equation_id])
    test(_request_for(test))


@pytest.mark.parametrize("equation_id", EQUATION_TESTS)
def test_an_equation_test_is_red_against_the_divergent_form_its_register_row_records(
    monkeypatch: pytest.MonkeyPatch, equation_id: str
) -> None:
    """Property: with the divergent form the register records behind the call, the equation's test raises
    `ConformanceDivergence` carrying its own equation id, so the test's literal inputs can see that form.

    Class: each of the three equations of this file. NEW CAPABILITY: the plausible wrong implementation of
    the tested equation test is one whose inputs the two forms treat alike (a series on which both windows rank the
    bursts the same way, tables on which a peak-outside overlap never occurs), which stays green and records
    no divergence; it is red here because the test returns instead of raising.
    Reachability: each divergent form differs from the faithful one only in the recorded rule, so a literal
    input class that separates them is the whole of what this test needs the tested file's inputs to contain.
    Both-states anchor: the oracle is the exception and its equation id, a property of the comparison and not
    of any wording either side introduces; the paired test above shows the same test green on the faithful form.
    Why: an input that cannot separate the paper's form from the recorded divergent one is a vacuous green
    once an implementation adopts the divergence, which is the failure these inputs exist to prevent.
    Kills: replacing the discriminating inputs by a single burst, an event inside its maximum duration, or
    pairs whose surge peaks all lie inside their rain events.
    Second correct implementation: a test whose literal inputs differ from the tested file's in value but still
    separate the two forms, which passes the paired test above and this one.
    """
    subject = _subject()
    test = getattr(subject, EQUATION_TESTS[equation_id])
    _run_against(monkeypatch, equation_id, DIVERGENT[equation_id])
    with pytest.raises(equation_check.ConformanceDivergence) as refused:
        test(_request_for(test))
    assert refused.value.failure.equation_id == equation_id
    assert refused.value.failure.diverged is True


@pytest.mark.parametrize("equation_id", EQUATION_TESTS)
def test_an_equation_test_is_red_with_a_conformance_divergence_on_the_stored_legacy_text(equation_id: str) -> None:
    """Property: run unpatched, against the stored legacy text, each equation's test raises
    `ConformanceDivergence` for its own equation id, and raises nothing else.

    Class: each of the three equations of this file; each is expected to be red against the stored text, and a
    red is the admissible state. NEW CAPABILITY: the plausible wrong
    implementation is a reference that happens to equal what the legacy text returns (a green, so a recorded
    divergence is lost) or one whose run ends in an unrunnable lift, a `TypeError` or a `SystemExit` (a red
    that is not a divergence); both are red here, the first because nothing is raised.
    Reachability: the test is called as pytest calls it, with its own marker, through the real discovery and
    the real stored extraction.
    Both-states anchor: the oracle is the exception type and its equation id; the paired tests above show the
    same functions green and red on implementations that follow and depart from the manuscript.
    Why: a red is admitted exactly when it is a `ConformanceDivergence`, and a red of any other kind is a
    defect of the run that a classifier would be handed as a divergence.
    Kills: a reference computed from the returned value; a literal input that makes the legacy text raise
    inside itself; a bare `assert` in place of `check`.
    Second correct implementation: any test of the same equation that ends in `equation_check.check` and
    compares a manuscript-derived value with the legacy one, whatever its literals.
    Flip condition: an owning system lands a `src/` implementation that the stored text no longer stands in
    for (a later change to the equation's implementation); this test then asserts a red the flipped equation no longer owes.
    """
    subject = _subject()
    test = getattr(subject, EQUATION_TESTS[equation_id])
    with pytest.raises(equation_check.ConformanceDivergence) as refused:
        test(_request_for(test))
    assert refused.value.failure.equation_id == equation_id
    assert refused.value.failure.diverged is True


def test_the_files_conformance_functions_are_the_three_ledger_rows_and_carry_no_skip_or_xfail() -> None:
    """Property: the module's conformance-marked functions are exactly E-05, E-08 and E-09, each named as the
    ledger names it, each carrying one decorator-level marker whose `unit` is its id, whose `part` is its own
    name and whose `system` is the system slug, and none carrying a skip, an xfail or a parametrize.

    Class: the module's functions carrying a `conformance` marker. The domain is this file's three rows and
    the allowlist is E-05, E-08, E-09; another file's rows are not this module's to carry. NEW CAPABILITY: the plausible
    wrong implementation
    marks a test at module level, names it by a typo, or xfails it so its divergence is not red.
    Reachability: the static inventory reads only a function's own decorator, so a module-level marker or a
    name that disagrees with the ledger is a test the coverage check cannot see.
    Both-states anchor: the oracle is the ledger's `test` key and the marker's own keywords; neither moves
    with the wording of any docstring.
    Why: the test names and the decorator site are fixed by the ledger, and a red hidden behind xfail or skip is forbidden.
    Kills: `pytestmark = ...` at module level; a function renamed from the ledger's name; an added
    `@pytest.mark.xfail`; a second conformance function for an id.
    Second correct implementation: the same three functions in a different order or with other helpers
    between them.
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
    """For each name the function binds, the names read by every statement that binds it or controls one.

    A name assigned inside an `if` or a loop also depends on the names read by that block's test or iterable,
    because which branch ran decided the value.
    """
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


@pytest.mark.parametrize("equation_id", EQUATION_TESTS)
def test_an_equation_test_computes_its_expected_value_without_the_implementations_result(equation_id: str) -> None:
    """Property: in each equation test, exactly one `equation_check.implementation` call and exactly one
    `equation_check.check` call, the check call last; the expected operand of `check` is a name that depends
    on no name bound from the implementation's result; and the body reaches no lift engine and no comparison
    other than the licence it passes to `check`.

    Class: the module-level equation test functions of this file (a FITNESS FUNCTION over their source; the
    property is stated without reference to any equation). NEW CAPABILITY: the plausible wrong implementation
    derives the expected value from the returned summary (a reference that is the code's own answer, which
    is green and says nothing) or runs a legacy text itself instead of through the helper; both are red here.
    Reachability: each equation test's reference is written in its own body from the manuscript's
    formula over its literal parameters, "never through a legacy text and never through `compare`".
    Both-states anchor: the oracle is a dependency walk over the function's own assignments, so it does not
    turn on any literal, name or comment the change introduces.
    Why: an expected value that depends on the observed one makes every comparison agree, and a recorded
    divergence is then lost without any test going red.
    Kills: `expected = observed`; `expected = np.asarray([summary["rain_event_start"].iloc[0]])`; a call to
    `lift.run_lift` or `compare.assert_equivalent` in the test body; a second `check` call.
    Second correct implementation: a test whose reference is built by a module-level helper that takes only
    the literal inputs, or whose observed value arrives through a differently named local; the walk follows
    names and not spellings. A test whose observed value is obtained by a module-level helper that itself
    calls the implementation would fail the call count, which this file's tests keep in the function.
    """
    path = Path(_subject().__file__)
    module = ast.parse(path.read_text(encoding="utf-8"))
    function = next(
        each for each in module.body if isinstance(each, ast.FunctionDef) and each.name == EQUATION_TESTS[equation_id]
    )
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
    observed_name, expected_name = check_calls[0].args[1], check_calls[0].args[2]
    assert isinstance(observed_name, ast.Name)
    assert isinstance(expected_name, ast.Name)

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
    assert observed_name.id in tainted
    assert expected_name.id not in tainted

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
