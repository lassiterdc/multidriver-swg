"""Assert the published `Status` cannot contradict itself, and that the BLOCKING SET is its own predicate.

`Status` is the interface a downstream attribution reader consumes, so its value is that a
contradictory status cannot exist to be read later. That property lives entirely in construction
refusals and in one derivable function, neither of which a type checker can see: a `DIVERGENT`
verdict with nothing named reads as an undifferentiated red, and a `D8` in a defect list reads as a
second home for a fact the binding already holds. This module pins the four refusals, the predicate,
and the single binding a manuscript span actually decides.

Every refusal is driven beside a must-fire control that differs only in the field the rule reads, so
a rule that refuses everything is distinguishable from one that refuses nothing. The BLOCKING SET is
driven over the whole 3x3 verdict-by-binding population with each expectation COMPUTED from the two
declared frozensets rather than transcribed, so the table cannot certify a predicate that is wrong.

What this does NOT establish: that a `D1`/`D2` in a defect list names an author. That attestation is
enforced by `classify` over a `Diagnosis`; a `Status` built directly carries no author, and nothing
here can tell it from one that came through the classifier.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from multidriver_swg.conformance import (
    BLOCKING_BINDINGS,
    BLOCKING_VERDICTS,
    DERIVED_IN_STATUS,
    Binding,
    ConformingWithDefects,
    DerivedClassInDefectList,
    DivergenceClass,
    NotADivergenceClass,
    NotAFailure,
    Status,
    StatusVocabularyError,
    UndiagnosedDivergence,
    UnevidencedDiagnosis,
    Verdict,
    is_blocking,
    manuscript_binding,
)
from tests.conformance.register import entries

# The shape names these two populations by identifier rather than deriving them from the register,
# and the identity list is the point: an entry that changes group is exactly the drift an agreement
# assertion over all 36 entries cannot see, because it would agree with itself wherever the entry moved.
# Measured 2026-10-29 against register.json at commit a57916d, its own landing commit (blob 7f4146f2,
# byte-identical through 5e99ce8): three entries carry an empty verbatim_span, and six name another
# repository. Re-date this comment and the literal together when the register is revised.
NO_MANUSCRIPT_TEXT = frozenset({"E-34", "E-35", "E-36"})
CROSS_REPO = ("E-01", "E-02", "E-03", "E-07", "E-32", "E-33")


def test_a_well_formed_status_constructs_and_is_frozen() -> None:
    """Property: a `DIVERGENT`/`BOUND` status carrying one real defect constructs, and cannot be mutated.

    Class: NEW CAPABILITY — the plausible wrong implementation is a mutable dataclass, which admits a
    status a later reader can change under a consumer that already read it; red under it because the
    assignment succeeds instead of raising.
    Reachability: every consumer of the published interface reaches construction, and every status in
    a report reaches the frozen claim; the input is the ordinary well-formed shape.
    Both-states anchor: the assertion is the raised `FrozenInstanceError` and the constructed field
    values, both of which exist without this module.
    Invocation isolation (B4-4, arm 1): no file, path or environment input reaches this assertion.
    Kills: dropping `frozen=True` from the dataclass.
    A second correct implementation: a status built with `dataclasses.replace` from a default instance
    still passes, because the assertions read construction success and the refusal on mutation rather
    than the construction call.
    """
    status = Status(
        equation_id="E-03",
        verdict=Verdict.DIVERGENT,
        binding=Binding.BOUND,
        defect_list=(DivergenceClass.D3,),
    )
    assert status.defect_list == (DivergenceClass.D3,)
    with pytest.raises(FrozenInstanceError):
        status.equation_id = "E-04"  # type: ignore[misc]  (assigning on a frozen dataclass is the assertion)


@pytest.mark.parametrize("derived", sorted(DERIVED_IN_STATUS))
def test_the_derived_classes_are_refused_in_the_defect_list(derived: DivergenceClass) -> None:
    """Property: every class the binding or the verdict already carries is refused inside `defect_list`,
    while a class outside that set is admitted.

    Class: NEW CAPABILITY — the plausible wrong implementation admits all nine classes to the list, so
    a status can say `CROSS_REPO` and carry a `D8` defect, or say `UNREACHABLE` and carry a `D9`; the
    two homes are individually plausible and can then disagree. Red under it, because the refusal arm
    constructs instead of raising.
    Reachability: every status recording a cross-repo or unreachable equation reaches this branch, and
    those are exactly the rows a downstream attribution reader blocks on.
    Both-states anchor: the raised type and the admitted construction, both present in the module.
    Invocation isolation (B4-4, arm 1): no file, path or environment input reaches this assertion.
    Kills: an admission rule for `D8`/`D9`, which is the redundancy the facet split exists to remove.
    A second correct implementation: a refusal keyed on a literal pair rather than on the imported set
    still passes, because the parametrization drives exactly the set's two members.
    """
    with pytest.raises(DerivedClassInDefectList):
        Status("E-03", Verdict.DIVERGENT, Binding.BOUND, (derived,))
    admitted = Status("E-03", Verdict.DIVERGENT, Binding.BOUND, (DivergenceClass.D7,))
    assert admitted.defect_list == (DivergenceClass.D7,)


@pytest.mark.parametrize("not_a_class", ["D3", "the code returns the sample estimator"])
def test_a_plain_string_in_the_defect_list_is_refused(not_a_class: str) -> None:
    """Property: a string that is not a vocabulary member is refused, and a real member is admitted.

    Class: NEW CAPABILITY — the plausible wrong implementation tests `isinstance(entry, str)`, which
    every member of the `StrEnum` vocabulary satisfies and so does the wire token `"D3"`. Under it the
    refusal never fires for the value most likely to arrive; red here, and the `"D3"` arm is what
    makes that failure legible.
    Reachability: a consumer rebuilding a status from serialized data reaches this branch with a bare
    string, which is the ordinary way a non-member arrives.
    Both-states anchor: the raised type and the admitted member construction, both present in the module.
    Invocation isolation (B4-4, arm 1): no file, path or environment input reaches this assertion.
    Kills: a string-type test standing in for an enumeration-membership test.
    A second correct implementation: a refusal written as `if entry not in DivergenceClass` still
    passes, because neither arm is a member and the admitted arm is.
    """
    with pytest.raises(NotADivergenceClass):
        Status("E-03", Verdict.DIVERGENT, Binding.BOUND, (not_a_class,))
    admitted = Status("E-03", Verdict.DIVERGENT, Binding.BOUND, (DivergenceClass.D3,))
    assert admitted.defect_list == (DivergenceClass.D3,)


def test_a_conforming_verdict_carrying_a_defect_is_refused() -> None:
    """Property: `CONFORMING` with a non-empty defect list is refused, and `CONFORMING` with an empty
    list constructs.

    Class: NEW CAPABILITY — the plausible wrong implementation never reads the two fields together, so
    a status can announce conformance and name a defect in the same breath; red under it, because the
    refusal arm constructs instead of raising.
    Reachability: every conforming row reaches this branch, and a report that lists conforming
    equations is the ordinary case rather than a corner.
    Both-states anchor: the raised type and the admitted construction, both present in the module.
    Invocation isolation (B4-4, arm 1): no file, path or environment input reaches this assertion.
    Kills: a shape that lets the verdict and the list describe different equations.
    A second correct implementation: a refusal testing `bool(self.defect_list)` rather than truthiness
    of the tuple still passes, because the fixture is a one-element tuple either way.
    """
    with pytest.raises(ConformingWithDefects):
        Status("E-03", Verdict.CONFORMING, Binding.BOUND, (DivergenceClass.D3,))
    admitted = Status("E-03", Verdict.CONFORMING, Binding.BOUND)
    assert admitted.defect_list == ()


def test_a_divergent_verdict_without_a_defect_is_refused() -> None:
    """Property: `DIVERGENT` with an empty defect list is refused, and `DIVERGENT` naming a defect constructs.

    Class: NEW CAPABILITY — the plausible wrong implementation admits the empty list, which is exactly
    the undifferentiated red the whole vocabulary exists to rule out: a caller learns that something
    diverged and nothing about what. Red under it, because the refusal arm constructs.
    Reachability: every divergent row reaches this branch, and a divergence with no recorded class is
    the state a partially populated report produces.
    Both-states anchor: the raised type and the admitted construction, both present in the module.
    Invocation isolation (B4-4, arm 1): no file, path or environment input reaches this assertion.
    Kills: an undiagnosed divergence, which reads as diagnosed to a consumer that only checks the verdict.
    A second correct implementation: a refusal testing `len(self.defect_list) == 0` still passes,
    because the fixture's list is empty either way.
    """
    with pytest.raises(UndiagnosedDivergence):
        Status("E-03", Verdict.DIVERGENT, Binding.BOUND, ())
    admitted = Status("E-03", Verdict.DIVERGENT, Binding.BOUND, (DivergenceClass.D3,))
    assert admitted.defect_list == (DivergenceClass.D3,)


def test_an_empty_iterable_never_yields_a_divergent_status() -> None:
    """Property: `DIVERGENT` built from an iterable that yields NOTHING refuses, rather than coming into
    existence naming no defect.

    Class: NEW CAPABILITY — the plausible wrong implementation reads the CALLER's value in the verdict
    check after the membership loop has already consumed it. An exhausted generator is still TRUTHY, so
    `not self.defect_list` is `False`, the refusal does not fire, and the stored list is empty: a
    `DIVERGENT` status exists naming nothing. Red under it, measured by the per-hunk reversion that
    restores `self.defect_list` in that check.
    Reachability: a caller assembling the list by comprehension over a maybe-empty source hands exactly
    this value, and it is the zero-yield member of the input class the non-empty arm beside it drives.
    Both-states anchor: the raised type and the admitted empty-list construction, both fixed by the
    module's own refusals.
    Invocation isolation (B4-4, arm 1): no file, path or environment input reaches this assertion.
    Kills: any construction path on which the undifferentiated red becomes reachable through a one-shot
    iterable — the same invariant the non-empty arm pins, at the input that arm cannot reach.
    A second correct implementation: a producer that materializes with `list()` and tests that list
    still passes, because the assertion reads the refusal and never the producer's local name.
    """
    with pytest.raises(UndiagnosedDivergence):
        Status("E-03", Verdict.DIVERGENT, Binding.BOUND, iter(()))
    with pytest.raises(UndiagnosedDivergence):
        Status("E-03", Verdict.DIVERGENT, Binding.BOUND, iter([]))


def test_a_conforming_status_accepts_an_empty_iterable() -> None:
    """Property: `CONFORMING` built from an iterable that yields nothing CONSTRUCTS with an empty list,
    rather than being refused for carrying defects it does not have.

    Class: NEW CAPABILITY — the plausible wrong implementation reads the caller's value in the conforming
    check, and an exhausted generator is TRUTHY, so it refuses a conforming status whose source happened
    to yield nothing. Red under it: the construction raises `ConformingWithDefects` naming zero defects,
    which is the refusal firing on its own opposite.
    Reachability: the same input class the divergent arm drives, on the verdict whose refusal is
    inherently about emptiness — so the two arms together show the check reads the MATERIALIZED list
    rather than the caller's truthiness.
    Both-states anchor: the constructed status and its empty tuple, both fixed by the module.
    Invocation isolation (B4-4, arm 1): no file, path or environment input reaches this assertion.
    Kills: a conforming check that reads a truthy-but-exhausted iterable as "carries defects".
    A second correct implementation: a producer that materializes with `list()` still passes, because
    the assertion reads the constructed field.
    """
    status = Status("E-03", Verdict.CONFORMING, Binding.BOUND, iter(()))
    assert status.defect_list == ()
    listed = Status("E-03", Verdict.CONFORMING, Binding.BOUND, [])
    assert listed.defect_list == ()


@pytest.mark.parametrize(("verdict", "binding"), [(v, b) for v in Verdict for b in Binding])
def test_the_blocking_set_is_the_declared_predicate(verdict: Verdict, binding: Binding) -> None:
    """Property: over every verdict-by-binding pair, `is_blocking` answers the value computed from the
    two declared frozensets, and the `CONFORMING`/`BOUND` row answers `False`.

    Class: NEW CAPABILITY — the plausible wrong implementation names only the verdict arm, so a
    conforming equation bound to another repository reads as non-blocking and is never attributed;
    the always-true implementation is the other one, and the `False` control below is what catches it.
    Both are red here: the first on the `CONFORMING`/`CROSS_REPO` row, the second on
    `CONFORMING`/`BOUND`.
    Reachability: every status a consumer holds reaches this predicate, and the population is the full
    cross product, so both arms of the `or` are exercised and the single `False` row is present.
    Both-states anchor: the expectation is recomputed here from the frozensets themselves, so a
    transcription error in this table cannot certify a wrong predicate.
    Invocation isolation (B4-4, arm 1): no file, path or environment input reaches this assertion.
    Kills: a one-armed predicate, and an always-true one.
    A second correct implementation: a predicate combining two set lookups as a union rather than an
    `or` still passes, because the expectation is computed from the same two sets.
    """
    defect_list = (DivergenceClass.D3,) if verdict is Verdict.DIVERGENT else ()
    status = Status("E-03", verdict, binding, defect_list)
    expected = verdict in BLOCKING_VERDICTS or binding in BLOCKING_BINDINGS
    assert is_blocking(status) is expected, (
        f"{verdict}/{binding} answered {is_blocking(status)}, computed {expected} from "
        f"{sorted(BLOCKING_VERDICTS)} and {sorted(BLOCKING_BINDINGS)}"
    )
    if verdict is Verdict.CONFORMING and binding is Binding.BOUND:
        assert is_blocking(status) is False, (
            "the CONFORMING/BOUND row is the always-true control: a predicate that blocks it blocks everything"
        )


def test_manuscript_binding_answers_only_the_derivable_case() -> None:
    """Property: `manuscript_binding` returns `NO_MANUSCRIPT_TEXT` exactly when the span is empty over
    all 36 real register entries, the resulting set is exactly `{E-34, E-35, E-36}`, and all six
    cross-repo entries return `None` rather than `BOUND`.

    Class: NEW CAPABILITY — the plausible wrong implementation returns `BOUND` for a non-empty span,
    which is the default that looks computed. Under it the six cross-repo equations, whose spans are
    all NON-empty, receive a non-blocking binding, so exactly the equations the BLOCKING SET exists to
    catch are the ones it misses. Red here on the cross-repo arm.
    Reachability: every register entry reaches this function, and the cross-repo population is the one
    where a wrong answer propagates rather than merely being untidy.
    Both-states anchor: the first assertion compares the function against the register's own
    `verbatim_span` field, which exists independently of this module and of this test.
    Invocation isolation (B4-4, arm 1): the input is the in-repo register; no environment variable or
    checkout location reaches the assertion.
    Kills: a `Binding`-typed return that defaults a non-empty span to `BOUND`.
    A second correct implementation: returning `NO_MANUSCRIPT_TEXT` on a falsy span and `None`
    otherwise still passes, because the register's spans are non-empty strings rather than other
    falsy values, and the synthetic control below is the empty string itself.
    """
    register = entries()
    assert len(register) == 36, f"the register carries {len(register)} entries, not 36"

    for equation_id, entry in register.items():
        decided = manuscript_binding(entry["verbatim_span"])
        assert (decided is None) == (entry["verbatim_span"] != ""), (
            f"{equation_id}: a span of length {len(entry['verbatim_span'])} decided {decided}, "
            "which disagrees with the register's own field"
        )

    computed = {
        equation_id for equation_id, entry in register.items() if manuscript_binding(entry["verbatim_span"]) is not None
    }
    assert computed == NO_MANUSCRIPT_TEXT, f"the derivable population is {sorted(computed)}"

    for equation_id in CROSS_REPO:
        assert manuscript_binding(register[equation_id]["verbatim_span"]) is None, (
            f"{equation_id} is cross-repo, so its non-empty span must not be read as a binding"
        )

    assert manuscript_binding("") is Binding.NO_MANUSCRIPT_TEXT
    assert manuscript_binding("x = 1") is None, "the must-fire control: a synthetic non-empty span decides nothing"


def test_every_status_refusal_is_a_member_of_the_family() -> None:
    """Property: each of the four construction refusals escapes as a member of `StatusVocabularyError`
    AND of its own named subclass, and the two exception families are unrelated to each other.

    Class: NEW CAPABILITY — the plausible wrong implementation re-parents a subclass to a bare
    `Exception`, so a consumer catching the family silently stops catching that refusal. Red here,
    because the family arm raises nothing to catch.
    Reachability: every refusal a consumer can meet passes through this table, and a catch site is the
    one thing a consumer does against this interface that the plan's own tests otherwise provoke only
    from the inside.
    Both-states anchor: each arm asserts family membership and subclass identity, both fixed by the
    module's own class declarations.
    Invocation isolation (B4-4, arm 1): no file, path or environment input reaches this assertion.
    Kills: a subclass detached from its family, and the two families accidentally unified.
    A second correct implementation: a base declared as a `ValueError` subclass with the same four
    children still passes, because the assertions name `StatusVocabularyError` rather than `Exception`.
    """
    cases = (
        (NotADivergenceClass, lambda: Status("E-03", Verdict.DIVERGENT, Binding.BOUND, ("D3",))),
        (
            DerivedClassInDefectList,
            lambda: Status("E-03", Verdict.DIVERGENT, Binding.BOUND, (DivergenceClass.D8,)),
        ),
        (
            ConformingWithDefects,
            lambda: Status("E-03", Verdict.CONFORMING, Binding.BOUND, (DivergenceClass.D3,)),
        ),
        (UndiagnosedDivergence, lambda: Status("E-03", Verdict.DIVERGENT, Binding.BOUND, ())),
    )
    for expected, construct in cases:
        with pytest.raises(StatusVocabularyError) as caught:
            construct()
        assert isinstance(caught.value, expected), (
            f"{expected.__name__} escaped as {type(caught.value).__name__}, which is in the family but is not the "
            "named subclass"
        )

    assert not issubclass(NotAFailure, StatusVocabularyError), (
        "a taxonomy refusal catchable as a status-vocabulary refusal would blur two distinct catch sites"
    )
    assert not issubclass(UnevidencedDiagnosis, StatusVocabularyError)
