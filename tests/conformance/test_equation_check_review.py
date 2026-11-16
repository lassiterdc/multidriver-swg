"""Review tests for the equation-check helper: refusal order, applied overrides, licence use, seeding and markers.

These tests accompany `equation_check.py` and `test_equation_check.py`. The helper is the one place every
equation test reaches its implementation and its verdict, so a defect here moves all of them at once and
shows up as a red or a green in a file nobody touched. The tests below grade nine classes the helper's own
suite does not reach: the order of the refusal against the comparison, an override the text actually reads,
the licence's tolerance and the side it is measured from, the seed's value and the restore on an exiting
exception, the markers of the helper's own tests, the divergence's message and cause, the discovery cache,
and a marker that names no unit.

These carry no family marker, so they enter no inventory count; they are chunk tests and this file guards
`tests/conformance/equation_check.py` (a module test of it on promotion; its tests pin no absolute count
and no stored blob). Unexecuted hypothesis: no run of any test here is recorded in this file; each claim
below about what a wrong implementation does is a read of `equation_check.py`.

Not established: that any equation's expected value is right, which is the calling test's business.
"""

from __future__ import annotations

import ast
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from tests.conformance import bindings
from tests.conformance.equation_check import (
    ConformanceDivergence,
    NotAnEquationMarker,
    UnboundEquation,
    UnknownLiftUnit,
    check,
    implementation,
    seeded_global_numpy,
)
from tests.correspondence import compare, inventory, lift

SYSTEM = "publication-math-conformance"
TAU_UNIT = "_utils.py:718-746"
HELPER_TESTS = "tests/conformance/test_equation_check.py"


@pytest.fixture(autouse=True)
def _numpy_state_preserved():
    """Hand numpy's global stream back as this test found it, so a seed set here leaks into no later test."""
    state = np.random.get_state()
    yield
    np.random.set_state(state)


def _request(kwargs: dict | None) -> object:
    """A stand-in for pytest's request whose node answers the one call the helper makes.

    `kwargs` is the conformance marker's keyword dictionary, or None for a test that carries no marker.
    """
    marker = None if kwargs is None else SimpleNamespace(kwargs=kwargs)

    def get_closest_marker(name):
        """Return the conformance marker the helper asks for, and None for any other name."""
        return marker if name == "conformance" else None

    return SimpleNamespace(node=SimpleNamespace(get_closest_marker=get_closest_marker))


def _for(unit: str) -> object:
    """A stand-in request whose marker names `unit` as the equation id."""
    return _request({"unit": unit, "part": "probe", "system": SYSTEM})


def test_a_row_the_ledger_does_not_bind_to_a_test_is_refused_even_when_the_values_diverge():
    """Property: for every ledger row whose binding is not `TEST`, `check` raises `UnboundEquation` and
    never `ConformanceDivergence`, on values the licence refuses; the same values on a `TEST` row diverge.

    Class: every non-`TEST` row of the ledger (cross-repository, unreachable, no manuscript text), read from
    the ledger and not listed here. NEW CAPABILITY: the plausible wrong implementation compares first and
    refuses a non-`TEST` row only after an agreeing comparison, so a diverging value on an equation no test
    computes becomes a recorded divergence the ledger says is not owed; red under it because the raised
    error is the divergence. The helper's own test of this refusal passes equal values, on which that
    implementation also raises `UnboundEquation`, so only a diverging pair separates the two orders.
    Unexecuted hypothesis.
    Reachability: a marker may name any ledger id, and `check` is called with whatever the test computed.
    Both-states anchor: the oracle is the exception type on a fixed diverging pair; the control arm shows the
    same pair does diverge on a `TEST` row, so the refusal is not a quirk of the values.
    Why: a red on an equation the ledger classes as another repository's or as unreachable is not evidence
    about this repository, and the classifier would be handed a record for it.
    Kills: moving the binding test after the comparison; refusing the row only when the values agree.
    Second correct implementation: a predicate shared with the coverage check, called before the comparison.
    """
    unbound = [equation_id for equation_id, row in bindings.load().items() if row["binding"] != "TEST"]
    assert unbound, "the control read found no non-TEST row in the ledger"
    with pytest.raises(ConformanceDivergence):
        check(_for("E-04"), 1.0, 2.0, compare.Exact())
    for equation_id in unbound:
        with pytest.raises(UnboundEquation):
            check(_for(equation_id), 1.0, 2.0, compare.Exact())


def test_an_override_the_declaration_binds_reaches_the_text_that_reads_it():
    """Property: a binding named in `overrides` replaces the declaration's own binding in the run, so a
    stand-in `kendalltau` returning 0.25 makes the text's observed tau 0.25, not the legacy value.

    Class: every key the declaration binds and its text reads, here `kendalltau`. NEW CAPABILITY: the
    plausible wrong implementation checks the override keys for membership and then returns the declared
    bindings unchanged, or applies them to a copy that is dropped, so a cell unit's literal inputs never
    arrive; red under it because the tau is the legacy `0.8888888888888888`, which the stand-in cannot
    produce. The helper's own suite refuses an unknown key and never supplies a known one.
    Reachability: a cell unit's inputs are bindings, so an override is the ordinary way a test supplies its
    literals; `kendalltau` is a binding of this declaration and its text reads it, so the run reaches it.
    Both-states anchor: the oracle is a value the real binding cannot return on this frame, true of any
    working helper and of any repair of a broken one.
    Why: AD-1 says a cell unit's inputs reach it through `overrides`, and the cell units owe their tests
    that route.
    Kills: `return declared, reduce`; merging the overrides into a copy that is not returned.
    Second correct implementation: apply the overrides in the reduction's namespace after the declaration's
    function returns; the comparison is on the value the text computed.
    """
    frame = pd.DataFrame({"x": [1, 2, 2, 3, 4], "y": [1, 3, 2, 3, 5]})

    def reduce(finished):
        """The observed tau of the Monte Carlo test on the frame, whichever `kendalltau` the text was given."""
        _result, _pvalue, _taus, tau_obs = finished["monte_carlo_tau_test"](
            df_obs=frame, col1_iloc=0, col2_iloc=1, df_simulated=frame, n_samples=2
        )
        return float(tau_obs)

    def stand_in(left, right):
        """A `kendalltau` that ignores its data and returns a fixed pair no real tau of this frame equals."""
        return 0.25, 0.0

    observed = implementation(TAU_UNIT, None, reduce, {"kendalltau": stand_in})
    assert observed == 0.25


def test_the_licence_the_caller_passes_decides_and_the_observed_side_is_measured_against_the_expected():
    """Property: `check` judges under the licence it is given, measuring the observed value against the
    expected one: with a relative tolerance of 0.1, 10.9 against an expected 10.0 is admitted and 11.05
    against an expected 10.0 diverges.

    Class: every pair on which the two licences, or the two orders of the operands under one licence,
    reach different verdicts; here one pair of each kind. NEW CAPABILITY: the plausible wrong implementation
    swaps the operands, which `Exact` cannot see because it is symmetric and which every tolerance-licensed
    equation test would then judge from the wrong side, or fixes the licence it compares under; red under
    the swap on the second pair (the tolerance is 0.1 times the reference: a difference of 1.05 exceeds it
    from 10.0 and not from 11.05) and under a fixed `Exact` on the first.
    Reachability: the tolerance licence is the one most equation tests use, `assert_equivalent` takes the
    value first and the reference second, and the tolerance is measured from the reference alone.
    Both-states anchor: the oracle is each verdict on a fixed pair, true of any correct helper.
    Why: the order moves every tolerance-licensed verdict near the boundary and the record would still carry
    the observed and expected values the right way round, hiding the swap.
    Kills: `compare.assert_equivalent(expected, observed, licence)`; `compare.Exact()` in place of `licence`.
    Second correct implementation: `licence.admits(expected, observed)` called directly.
    """
    licence = compare.Tolerance(rtol=0.1, atol=0.0)
    assert check(_for("E-04"), 10.9, 10.0, licence) is None
    with pytest.raises(ConformanceDivergence) as raised:
        check(_for("E-04"), 11.05, 10.0, licence)
    assert raised.value.failure.observed == 11.05
    assert raised.value.failure.expected == 10.0


def test_the_seed_value_decides_the_draws_of_both_channels_the_legacy_texts_use():
    """Property: inside `seeded_global_numpy(seed)` two runs of the same seed draw alike and two different
    seeds draw differently, through `numpy.random` and through `DataFrame.sample` with no generator.

    Class: every seed value, with the two channels the legacy texts draw through. NEW CAPABILITY: the plausible
    wrong implementation ignores its argument and seeds a constant, or seeds a generator the texts do not draw
    from; red under the constant because the two seeds then draw alike, and under a private generator because
    the same seed then draws differently. The helper's own test calls it with one seed only and with `numpy.random`
    only.
    Reachability: the legacy calls name no generator, so a pandas `sample` with `random_state=None` draws
    from numpy's global state, which is the channel the helper exists to steer.
    Both-states anchor: the oracle is the equality of a repeated seed and the inequality of two seeds, true of
    any correct helper whatever the mechanism.
    Why: a test that seeds a constant would pass every repeatability check and silently ignore the value its
    author wrote.
    Kills: `np.random.seed(0)`; `np.random.default_rng(seed)`; seeding Python's `random`.
    Second correct implementation: `np.random.set_state(np.random.RandomState(seed).get_state())`.
    """
    table = pd.DataFrame({"v": list(range(60))})

    def draws(seed):
        """The numpy draws and the pandas sample drawn inside one seeded block."""
        with seeded_global_numpy(seed):
            return np.random.random(4).tolist(), table.sample(n=6, replace=True)["v"].tolist()

    assert draws(1) == draws(1)
    assert draws(1) != draws(2)


def test_the_outer_stream_is_restored_when_the_seeded_block_raises():
    """Property: a block that exits by an exception still hands numpy's stream back where it found it.

    Class: every exit of the block, here a raised exception after a draw. NEW CAPABILITY: the plausible wrong
    implementation restores the state after the `yield` with no `finally`, so a block that raises leaves the
    seeded stream behind; red under it at the restore assertion. The helper's own test leaves the block only
    by falling off its end.
    Reachability: a conformance test is red exactly when a `ConformanceDivergence` leaves it, and a test that
    checks inside the block exits by that exception.
    Both-states anchor: the outer sequence is compared with itself, not with a value this helper produces.
    Why: a red that leaks its seed changes what every later test draws, so a flaky-looking red appears in a
    file the change did not touch.
    Kills: removing the `try`/`finally`; restoring only on a normal exit.
    Second correct implementation: `contextlib.ExitStack` with a restoring callback.
    """
    np.random.seed(5)
    baseline = [np.random.random() for _ in range(3)]
    np.random.seed(5)
    with pytest.raises(RuntimeError), seeded_global_numpy(9):
        np.random.random()
        raise RuntimeError("probe")
    after = [np.random.random() for _ in range(3)]
    assert after == baseline


def test_the_helper_tests_carry_the_marker_the_family_specifies_each():
    """Property: every `test_` function of `test_equation_check.py` is listed once, as
    `conformance(unit="equation-check", part="{its own name}", system="publication-math-conformance")`.

    Class: the functions of one file, enumerated from that file's own syntax tree. NEW CAPABILITY: the
    plausible wrong implementation is a marker whose `unit` is a near spelling (`equation_check`), whose
    `system` is another system's, or that is unreadable, which lists nothing; red under each. A stale `part`
    is also caught by the inventory's duplicate refusal, a `unit` or `system` typo by nothing else, since a
    unit that is not equation-like is not checked by the ledger.
    Reachability: a decorator copied from a neighbouring test keeps whatever unit and system it had.
    Both-states anchor: the expected tuples come from the function names in the file, not from the marker
    text, so a repair that changes a marker leaves the oracle where it was.
    Why: the family selects, counts and attributes its tests by these three words.
    Kills: a different `unit`, `part` or `system` on any one function.
    Second correct implementation: a helper stamping the marker on each function by name; only the listed
    tuple is compared.
    """
    entries_listed, _ = inventory.listing(inventory.REPO_ROOT)
    source = (inventory.REPO_ROOT / HELPER_TESTS).read_text(encoding="utf-8")
    declared = sorted(
        node.name
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.FunctionDef) and node.name.startswith("test_")
    )
    listed = sorted(
        (entry["function"], entry["unit"], entry["part"], entry["system"])
        for entry in entries_listed
        if entry["file"] == HELPER_TESTS
    )
    assert declared, "the control read found no test function in the file"
    assert listed == [(name, "equation-check", name, SYSTEM) for name in declared]


def test_a_divergence_names_the_equation_and_the_licence_and_chains_the_refused_comparison():
    """Property: the message of a `ConformanceDivergence` names the equation id and the licence's class, and
    its `__cause__` is the `AssertionError` the comparison raised.

    Class: every refused comparison; here one under `Tolerance`, so the licence named is not the default.
    NEW CAPABILITY: the plausible wrong implementation raises the divergence with no `from`, or with a message
    that omits the licence, so the reader of a red cannot tell which licence refused the value; red under both.
    The helper's own test reads only the id.
    Reachability: any refused comparison reaches the raise.
    Both-states anchor: the oracle is two substrings and the cause's type, true of any wording.
    Why: AD-3 requires a message naming the id and the licence, and the chained comparison is where pytest
    shows both operands of the refused assert.
    Kills: dropping `from refused`; a message with the id only.
    Second correct implementation: `type(licence).__name__` in the message; both substrings are present.
    """
    with pytest.raises(ConformanceDivergence) as raised:
        check(_for("E-04"), 3.0, 2.0, compare.Tolerance(rtol=0.0, atol=0.5))
    message = str(raised.value)
    assert "E-04" in message and "Tolerance" in message, message
    cause = raised.value.__cause__
    assert isinstance(cause, AssertionError) and not isinstance(cause, ConformanceDivergence), repr(cause)


def test_the_declarations_are_discovered_at_most_once_across_repeated_calls(monkeypatch):
    """Property: two calls of `implementation` trigger at most one `lift.discover()` between them.

    Class: every call count above one. NEW CAPABILITY: the plausible wrong implementation calls
    `lift.discover()` on every request, which executes every lift module under `tests/lifts/` each time and
    makes a refused module report itself once per equation test; red under it because the spy then counts
    two. Unexecuted hypothesis: the spy replaces the module attribute `equation_check` calls through.
    Reachability: the refusal of an unknown part is reached after discovery and before any run, so the calls
    are cheap whatever the cache holds.
    Both-states anchor: the cache may already be warm from an earlier test, so the bound is `<= 1`; a correct
    helper counts 0 or 1 and a helper with no cache counts 2 in either state.
    Why: the chunk specifies discovery once per session, and the cost and the repeated refusal are why.
    Kills: removing the cache; resetting it on every call.
    Second correct implementation: `functools.cache` on the discovery function.
    """
    calls: list[int] = []
    real = lift.discover

    def spy(*args, **kwargs):
        """Count one discovery and run the real one."""
        calls.append(1)
        return real(*args, **kwargs)

    monkeypatch.setattr(lift, "discover", spy)
    for _ in range(2):
        with pytest.raises(UnknownLiftUnit):
            implementation(TAU_UNIT, "no-such-part", lambda finished: None)
    assert len(calls) <= 1, len(calls)


def test_a_marker_that_carries_no_unit_is_the_helper_refusal_and_not_a_key_error():
    """Property: a conformance marker with no `unit` keyword raises `NotAnEquationMarker` naming `unit`.

    Class: every marker keyword dictionary the static inventory lists without an equation id; a marker with
    only `part` and `system` is readable there and lists `unit` as None. NEW CAPABILITY: the plausible wrong
    implementation indexes `marker.kwargs["unit"]`, so the red is a `KeyError` that names no authoring defect;
    red under it. The helper's own suite supplies a marker with a unit in every arm, though its docstring
    claims this kill.
    Reachability: the inventory reads `values.get(k)` for each keyword and does not refuse an absent one.
    Both-states anchor: the oracle is the exception type and the word `unit`, true of any wording.
    Why: the message is the only place the authoring defect can be named for the author of the test.
    Kills: `marker.kwargs["unit"]`.
    Second correct implementation: `marker.kwargs.get("unit")` followed by the same refusal.
    """
    request = _request({"part": "probe", "system": SYSTEM})
    with pytest.raises(NotAnEquationMarker) as raised:
        check(request, 1.0, 1.0, compare.Exact())
    assert "unit" in str(raised.value), str(raised.value)
