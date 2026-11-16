"""Prove the shared equation-check helper: the red it raises, the value it runs, and the seed it steers.

Every equation test in this family ends in one `check` call and begins with one `implementation` call, so
a defect here is a defect in all of them at once. Three properties matter and none is visible from a green
suite: a disagreement must arrive as a `ConformanceDivergence` that a classifier accepts rather than as a
bare assertion, a test-authoring TypeError must NOT arrive as one, and the implementation adapter must
return the legacy value the harness actually computes — not the value its own declaration's reduction would
have computed instead.

The reduction is the crux. A lift declaration carries a reduction written for the equivalence tests, which
runs its own fixed set of calls. The adapter must REPLACE that reduction with the caller's, or a conformance
test would compare whatever the equivalence declaration happened to ask for. The tied five-row frame below
is the discriminating input: the declaration's own reduction returns a whole dataset, while a caller's
reduction over the same unit returns one tau.

What this does NOT establish: that any equation's expected value is right. The reference value is the
calling test's business; this file pins only the plumbing between an implementation and a comparison.
"""

from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from multidriver_swg.conformance.taxonomy import (
    Binding,
    ConformanceFailure,
    Diagnosis,
    DivergenceClass,
    classify,
)
from tests.conformance.equation_check import (
    ConformanceDivergence,
    NotAnEquationMarker,
    UnboundEquation,
    UnknownLiftUnit,
    UnknownOverride,
    check,
    implementation,
    seeded_global_numpy,
)
from tests.correspondence import compare

SYSTEM = "publication-math-conformance"
TAU_UNIT = "_utils.py:718-746"


def _request(unit: str | None) -> object:
    """A stand-in for pytest's request whose node answers the one call the helper makes.

    A real calling test cannot carry an equation id: this file's own marker must stay `equation-check`, and
    a marker naming an equation would claim to be that equation's conformance test. So the stand-in
    supplies exactly the attribute the helper reads, and no more.
    """
    marker = None if unit is None else SimpleNamespace(kwargs={"unit": unit, "part": "probe", "system": SYSTEM})

    def get_closest_marker(name: str):
        """Return the conformance marker the helper asks for, and None for any other name."""
        return marker if name == "conformance" else None

    return SimpleNamespace(node=SimpleNamespace(get_closest_marker=get_closest_marker))


@pytest.mark.conformance(
    unit="equation-check",
    part="test_a_divergence_carries_a_record_the_classifier_accepts",
    system="publication-math-conformance",
)
def test_a_divergence_carries_a_record_the_classifier_accepts() -> None:
    """Property: a refused comparison raises `ConformanceDivergence` whose `failure` carries the equation
    id, both values, the ledger's binding and reachability, and a matching `Diagnosis` classifies it.

    Class: NEW CAPABILITY — the plausible wrong implementation raises a bare `AssertionError` (or returns a
    boolean), so the red carries no record and the classifier has nothing to consume; red here at the
    `failure` attribute and at `classify`. Why: the divergence record is the only artifact that lets a
    reader attribute a red to an equation and a binding without re-reading the ledger, and `classify`
    refuses a failure whose `diverged` flag is not True — this arm proves the flagged record is one it
    accepts. Reachability: `compare.Exact().outside(reference)` is a stand-in the licence turns down, so
    the comparison reaches its assertion by construction. Both-states anchor: the oracle is the record's
    fields and the classifier's verdict, present in both the current and a repaired helper. Invocation
    isolated: the request stand-in and the ledger load from the package, not from the working directory.
    Kills: dropping the `failure` attribute; constructing `ConformanceFailure` without the ledger's
    binding; catching `AssertionError` and re-raising something else. Second correct implementation: build
    the failure first and raise `ConformanceDivergence(message, failure)` from it, exactly as here, or
    raise a subclass that sets `failure` in `__post_init__` — the assertions read the record, not the
    construction path.
    """
    expected = np.array([0.5, 0.25, 0.125])
    observed = compare.Exact().outside(expected)
    with pytest.raises(ConformanceDivergence) as raised:
        check(_request("E-04"), observed, expected, compare.Exact())
    failure = raised.value.failure
    assert isinstance(failure, ConformanceFailure)
    assert failure.equation_id == "E-04"
    assert failure.binding is Binding.BOUND
    assert failure.reachable is True
    assert failure.diverged is True
    assert failure.observed is observed and failure.expected is expected
    assert "E-04" in str(raised.value)
    diagnosis = Diagnosis(
        equation_id="E-04", divergence=DivergenceClass.D7, evidence="probe: exact refused a moved array"
    )
    assert classify(failure, diagnosis) is DivergenceClass.D7


@pytest.mark.conformance(
    unit="equation-check",
    part="test_an_agreement_returns_none_and_raises_nothing",
    system="publication-math-conformance",
)
def test_an_agreement_returns_none_and_raises_nothing() -> None:
    """Property: a comparison the licence admits returns `None`.

    Class: NEW CAPABILITY — the plausible wrong implementation returns the comparison's boolean, so a
    caller that trusts a truthy return reads an admitted value as a red; red here because the return is
    `None`. Why: the helper's contract is raise-or-return, and a returned value invites `if check(...)`,
    which inverts the meaning of the only signal the family has. Reachability: an equal scalar is the
    ordinary admitted input. Both-states anchor: the oracle is the return value, present in both versions.
    Invocation isolated: no file is read outside the package's own data. Kills: `return licence.admits(...)`.
    Second correct implementation: `compare.assert_equivalent` followed by an explicit `return None`.
    """
    assert check(_request("E-04"), 2.0, 2.0, compare.Exact()) is None


@pytest.mark.conformance(
    unit="equation-check", part="test_a_type_error_propagates_unwrapped", system="publication-math-conformance"
)
def test_a_type_error_propagates_unwrapped() -> None:
    """Property: a value kind no licence reads raises `TypeError`, not `ConformanceDivergence`.

    Class: NEW CAPABILITY — the plausible wrong implementation catches `Exception` (or bare `AssertionError`
    plus `TypeError`) and reports it as a divergence; red here because the raised error is then the wrapper,
    and the classifier would record a paper/code disagreement that never happened. Why: a string or a set
    handed to the comparison is a defect of the TEST, and the family's rule is that only a refused
    comparison is a divergence. Reachability: `str` is refused by the comparison surface's kind check.
    Both-states anchor: the exception's type, present in both versions. Invocation isolated: no file is
    read. Kills: `except (AssertionError, TypeError)`. Second correct implementation: a pre-check that
    raises `TypeError` itself, or nothing at all — the surface already raises it.
    """
    with pytest.raises(TypeError) as raised:
        check(_request("E-04"), "a string", np.array([1.0]), compare.Exact())
    assert not isinstance(raised.value, ConformanceDivergence)


@pytest.mark.conformance(
    unit="equation-check", part="test_a_non_test_row_is_refused_before_comparing", system="publication-math-conformance"
)
def test_a_non_test_row_is_refused_before_comparing() -> None:
    """Property: a marker naming an equation whose ledger binding is not `TEST` raises `UnboundEquation`
    naming the id and the binding, and no comparison runs.

    Class: NEW CAPABILITY — the plausible wrong implementation compares first and looks the row up only when
    building the record, so a cross-repository or unreachable equation produces a divergence the ledger says
    is not owed; red here because the refusal never arrives. Why: three of the ledger's four bindings name
    equations no test computes, and a red on one of them is not evidence about this repository.
    Reachability: `E-06` is bound `UNREACHABLE` in the checked-in ledger. Both-states anchor: the exception
    and its message, present in both versions. Invocation isolated: the ledger loads from the package.
    Kills: dropping the binding test; testing it after the comparison. Second correct implementation: look
    the row up and refuse through a small predicate shared with the coverage check.
    """
    with pytest.raises(UnboundEquation) as raised:
        check(_request("E-06"), 1.0, 1.0, compare.Exact())
    message = str(raised.value)
    assert "E-06" in message and "UNREACHABLE" in message, message


@pytest.mark.conformance(
    unit="equation-check",
    part="test_a_marker_outside_the_ledger_or_absent_is_refused",
    system="publication-math-conformance",
)
def test_a_marker_outside_the_ledger_or_absent_is_refused() -> None:
    """Property: a marker naming an id the ledger lacks, and a test carrying no marker at all, both raise
    `NotAnEquationMarker` naming what was found.

    Class: NEW CAPABILITY — the plausible wrong implementation treats a missing row as an ordinary lookup
    miss and raises `KeyError`, or defaults the id to the empty string; red here because neither refusal is
    the family's exception or names the id. Why: a typo in the decorator is the ordinary authoring defect
    and the message is the only place it can be named, since the ledger never sees the marker.
    Reachability: `E-99` is outside the register's range, and a node with no marker reaches the other arm.
    Both-states anchor: the exception type and the id in its message, present in both versions. Invocation
    isolated: no file is read beyond the package's ledger. Kills: `rows[equation_id]`; `marker.kwargs["unit"]`
    with no default. Second correct implementation: one refusal class for both arms, or two, as here.
    """
    with pytest.raises(NotAnEquationMarker) as raised:
        check(_request("E-99"), 1.0, 1.0, compare.Exact())
    assert "E-99" in str(raised.value), str(raised.value)
    with pytest.raises(NotAnEquationMarker) as raised:
        check(_request(None), 1.0, 1.0, compare.Exact())
    assert "conformance marker" in str(raised.value), str(raised.value)


@pytest.mark.conformance(
    unit="equation-check",
    part="test_the_helper_returns_the_legacy_tau_on_the_tied_frame",
    system="publication-math-conformance",
)
def test_the_helper_returns_the_legacy_tau_on_the_tied_frame() -> None:
    """Property: `implementation` runs the stored text of the Kendall-tau test with the CALLER'S reduction
    and returns the legacy tau `0.8888888888888888` on a five-row frame with ties in both columns.

    Class: NEW CAPABILITY — the plausible wrong implementation keeps the declaration's own reduction, which
    returns a dataset of copula and bootstrap calls; red here because the return is not a float, and red
    under a reduction that is ignored because the value is then the declaration's dataset. Why: the value
    is the measured behaviour of the stored text on this exact input, so it discriminates "the adapter ran
    the unit" from "the adapter ran something adjacent". Reachability: the frame has ties, which is the
    input the tie-corrected tau differs on. Both-states anchor: the oracle is the legacy value measured
    independently of this helper, not a value read back from it. Invocation isolated: the declaration is
    discovered from the package root and the stored text from the committed corpus. Kills: reusing
    `declaration.reduce`; running a different unit; seeding the bootstrap. Second correct implementation: a
    `functools.partial` reduction, or a closure that returns an array of one — the comparison is on the
    float, not on how the callable is written.
    """
    frame = pd.DataFrame({"x": [1, 2, 2, 3, 4], "y": [1, 3, 2, 3, 5]})

    def reduce(finished):
        """The first row's tau from the legacy Monte Carlo test, bootstrapped against the same frame."""
        result, pvalue, taus, tau_obs = finished["monte_carlo_tau_test"](
            df_obs=frame, col1_iloc=0, col2_iloc=1, df_simulated=frame, n_samples=2
        )
        assert result is not None and pvalue is not None and taus is not None
        return float(tau_obs)

    observed = implementation(TAU_UNIT, None, reduce)
    # The type test is load-bearing: the declaration's own reduction returns a dataset, and a dataset is
    # truthy, so `assert observed == 0.888...` alone would pass on a value this call never produced.
    assert isinstance(observed, float), type(observed).__name__
    assert observed == 0.8888888888888888


@pytest.mark.conformance(
    unit="equation-check", part="test_an_unknown_override_key_is_refused", system="publication-math-conformance"
)
def test_an_unknown_override_key_is_refused() -> None:
    """Property: an override naming a key the declaration does not bind raises `UnknownOverride` naming the
    unit, the part and the key.

    Class: NEW CAPABILITY — the plausible wrong implementation merges the overrides without checking, so the
    key either reaches the runtime (which reports it as a binding the text never reads, a different and
    misleading refusal) or vanishes into a namespace the text cannot see; red here because neither names the
    key against the declaration. Why: a cell unit's inputs are bindings, so an override is the ordinary way
    a test supplies its literals and a typo in one is the ordinary defect. Reachability: `not_a_binding` is
    a key no declaration binds. Both-states anchor: the exception and the three names in its message,
    present in both versions. Invocation isolated: discovery reads the package's own lift modules. Kills:
    merging without a membership test. Second correct implementation: compare against the bound names before
    calling the declaration, or inside the wrapped function as here.
    """
    with pytest.raises(UnknownOverride) as raised:
        implementation(TAU_UNIT, None, lambda finished: None, {"not_a_binding": 1})
    message = str(raised.value)
    assert TAU_UNIT in message and "not_a_binding" in message, message


@pytest.mark.conformance(
    unit="equation-check",
    part="test_a_unit_and_part_no_declaration_carries_is_refused",
    system="publication-math-conformance",
)
def test_a_unit_and_part_no_declaration_carries_is_refused() -> None:
    """Property: a unit/part pair no declaration carries raises `UnknownLiftUnit` naming the unit and the
    part, rather than running some other declaration or returning `None`.

    Class: NEW CAPABILITY — the plausible wrong implementation matches on the unit alone, so a request for a
    named part silently runs the unit's unnamed declaration; red here because the refusal never arrives and
    the returned value would be another reduction's. Why: one unit can carry several declarations (its own
    and the components a test reduces), and running the wrong one would compare the wrong behaviour.
    Reachability: `no-such-part` is a part no declaration of this unit carries. Both-states anchor: the
    exception type and the two names, present in both versions. Invocation isolated: discovery reads the
    package's lift modules. Kills: `next(d for d in declarations if d.unit == unit)`; returning `None` on a
    miss. Second correct implementation: an index keyed by the pair, with a refusal on a missing key.
    """
    with pytest.raises(UnknownLiftUnit) as raised:
        implementation(TAU_UNIT, "no-such-part", lambda finished: None)
    message = str(raised.value)
    assert TAU_UNIT in message and "no-such-part" in message, message


@pytest.mark.conformance(
    unit="equation-check",
    part="test_seeding_steers_the_block_and_restores_the_stream",
    system="publication-math-conformance",
)
def test_seeding_steers_the_block_and_restores_the_stream() -> None:
    """Property: inside `seeded_global_numpy(seed)` the draws are the seed's, and after the block the outer
    stream continues exactly where the block found it.

    Class: NEW CAPABILITY — the plausible wrong implementation seeds without saving, so every later test
    that draws sees a different stream and a red appears in a file nobody touched; red here at the
    restore assertion. Why: the legacy texts draw through numpy's global state and name no generator, so the
    only way to make them repeatable is to seed that state — and the only way to keep that from leaking is
    to restore it. Reachability: two draws before the block fix the outer stream's position.
    Both-states anchor: the outer sequence is compared with itself, not with a value this helper produces.
    Invocation isolated: no file is read. Kills: `np.random.seed(seed)` with no save/restore.
    Second correct implementation: save `get_state()` and restore, or use a `numpy.random.seed`/state
    context written inline in the test — the assertions read the streams, not the mechanism.
    """
    np.random.seed(5)
    baseline = [np.random.random() for _ in range(3)]
    np.random.seed(5)
    with seeded_global_numpy(9):
        inside = [np.random.random() for _ in range(2)]
    after = [np.random.random() for _ in range(3)]
    with seeded_global_numpy(9):
        again = [np.random.random() for _ in range(2)]
    assert inside == again, "the seed does not decide the block's draws"
    assert after == baseline, "the block did not restore the stream it found"
    assert inside != baseline[:2], "the block drew the outer sequence, so it never seeded at all"
