"""Assert the divergence vocabulary is closed, and that the classifier refuses every unusable record.

The classifier's whole value is that a red arrives at its caller carrying exactly one class. That
value depends on two things a type checker cannot see: that the class vocabulary is still the closed
nine the design declares, and that every path which would return an undifferentiated (or silently
mis-attributed) answer is refused rather than defaulted. This module pins both, and pins them per
refusal rather than as one lump, so a rule that stops firing names itself.

Each refusal is driven together with a must-fire control that differs only in the field the rule
reads. A refusal with no control cannot be told from a function that refuses everything, and a
control with no refusal cannot be told from a rule that never fires.

What this does NOT establish: that a recorded class is CORRECT. The class is a ruling about which
artifact is authoritative, and no arithmetic over the two reported values recovers it. These tests
establish that a class was recorded and that the record is internally consistent — never that the
ruling was right.
"""

from __future__ import annotations

from dataclasses import replace

import numpy
import pytest

import multidriver_swg.conformance as conformance_package
from multidriver_swg.conformance import (
    AUTHOR_JUDGED,
    DERIVED_IN_STATUS,
    AuthorJudgementMissing,
    Binding,
    ClassConflict,
    ConformanceFailure,
    ConformanceTaxonomyError,
    Diagnosis,
    DiagnosisMismatch,
    DivergenceClass,
    NotAFailure,
    Status,
    UnevidencedDiagnosis,
    Verdict,
    classify,
)


def _a_failure(
    *,
    equation_id: str = "E-03",
    binding: Binding = Binding.BOUND,
    reachable: bool = True,
    diverged: bool = True,
) -> ConformanceFailure:
    """A red for `E-03` whose reported values are ARRAYS, which is the ordinary case a conformance test produces.

    The array-valued `observed`/`expected` are deliberate and not decorative: an implementation that
    decided divergence by comparing the two values would raise on them (`bool()` of a multi-element
    array) instead of returning a class, so a comparison creeping back into the classifier is caught
    here rather than in production.
    """
    return ConformanceFailure(
        equation_id=equation_id,
        observed=numpy.array([0.25, 0.75]),
        expected=numpy.array([0.25, 0.50]),
        binding=binding,
        reachable=reachable,
        diverged=diverged,
    )


def _a_diagnosis(
    *,
    equation_id: str = "E-03",
    divergence: DivergenceClass = DivergenceClass.D3,
    evidence: str = "the code returns the sample estimator the paper writes in § 3.2",
    ruled_by: str | None = None,
) -> Diagnosis:
    """A well-formed ruling for `E-03` unless a keyword overrides one field, so a control differs in exactly that field.

    The default class is `D3`, the code-side class, because it is the one class a caller reaches with
    no binding, reachability or author condition to satisfy — so a defect in any OTHER rule shows up
    as that rule misfiring rather than as this fixture failing for an unrelated reason.
    """
    return Diagnosis(
        equation_id=equation_id,
        divergence=divergence,
        evidence=evidence,
        ruled_by=ruled_by,
    )


def test_the_class_vocabulary_is_exactly_nine() -> None:
    """Property: `DivergenceClass` carries exactly the nine members `D1` through `D9`, in order.

    Class: NEW CAPABILITY — the plausible wrong implementation is a vocabulary that gained a tenth
    member (a "default" or "undetermined" class, the shape's own named hazard) or lost one, either of
    which reads downstream as a closed taxonomy while admitting a value the design does not define.
    Red under both, because both the count and the name sequence are asserted.
    Reachability: any edit to the enum reaches this assertion; the input is the real module member
    set, not a copy of it.
    Both-states anchor: the assertion reads `len()` and the member names of the enumeration itself, a
    property that exists whatever this module's own text says.
    Invocation isolation (B4-4, arm 1): the enumeration is defined in the package source with no
    file, path or environment input, so the same invocation returns the same verdict from any
    checkout the run resolves from.
    Kills: a class added, renamed or removed without the closed-domain declaration moving with it.
    A second correct implementation: a vocabulary written as an explicit `StrEnum` with per-member
    string values rather than relying on auto-numbering still passes, because the assertions read
    member names and values rather than the declaration form.
    """
    assert len(DivergenceClass) == 9, f"the class vocabulary has {len(DivergenceClass)} members, not 9"
    assert [member.name for member in DivergenceClass] == [f"D{number}" for number in range(1, 10)]
    assert [member.value for member in DivergenceClass] == [f"D{number}" for number in range(1, 10)]


def test_the_status_field_vocabularies_are_exactly_three_each() -> None:
    """Property: `Verdict` and `Binding` each carry exactly three members, with the declared wire values.

    Class: NEW CAPABILITY — the plausible wrong implementation is a field vocabulary that grew a
    fourth value to cover a case the design has not settled (the over-constraint the shape's own
    ruling forbids in the opposite direction), or that spells its wire token differently from the
    manuscript's own prose. Red under either, because both the count and the ordered value list are
    asserted.
    Reachability: any edit to either enumeration reaches this assertion; the input is the real
    module member set.
    Both-states anchor: the assertion reads the enumerations' own members and values, not any
    message or helper this module introduces.
    Invocation isolation (B4-4, arm 1): as the previous test — no tree input reaches this assertion.
    Kills: a status vocabulary silently widened, or a wire spelling changed on one side only.
    A second correct implementation: declaring the members without explicit values, letting `StrEnum`
    take the name as the value, still passes for `Verdict` (whose spellings equal its names) and is
    correctly caught for `Binding` (whose spellings do not).
    """
    assert [member.value for member in Verdict] == ["CONFORMING", "DIVERGENT", "UNREACHABLE"], (
        f"the verdict vocabulary is {[member.value for member in Verdict]}, not the declared three"
    )
    assert [member.value for member in Binding] == ["BOUND", "CROSS-REPO", "NO-MANUSCRIPT-TEXT"], (
        f"the binding vocabulary is {[member.value for member in Binding]}, not the declared three"
    )
    assert len(Verdict) == 3, f"the verdict vocabulary has {len(Verdict)} members, not 3"
    assert len(Binding) == 3, f"the binding vocabulary has {len(Binding)} members, not 3"


def test_the_enumerations_render_their_wire_values() -> None:
    """Property: every member of the three enumerations renders as its wire token, under `str()` and an f-string.

    Class: NEW CAPABILITY — the plausible wrong implementation is `class Binding(str, Enum)` rather
    than `StrEnum`. The two are indistinguishable by equality (both compare equal to the wire string)
    and differ only in what they RENDER: under the mixin form `str(member)` and `f"{member}"` produce
    `Binding.CROSS_REPO` while the wire value is `CROSS-REPO`. Red under the mixin form, which is why
    this test exists rather than being folded into a value-equality check that the wrong form passes.
    Reachability: any member of any of the three enumerations reaches this assertion, and the mixin
    form is the realistic regression because it is the older, more familiar spelling.
    Both-states anchor: the assertion compares the RENDERED string with `member.value`, a pairing
    that exists in the module regardless of this test.
    Invocation isolation (B4-4, arm 1): no tree input reaches this assertion.
    Kills: substituting the `(str, Enum)` mixin form for `StrEnum`, which would make every prose
    rendering of a published value disagree with the value itself.
    A second correct implementation: a module that spells each member's value out explicitly instead
    of relying on `StrEnum`'s name-as-value default still passes, because the assertion reads the
    rendered result and never the declaration style.
    """
    assert str(Binding.CROSS_REPO) == "CROSS-REPO", f"str() rendered {str(Binding.CROSS_REPO)!r}"
    assert str(Binding.NO_MANUSCRIPT_TEXT) == "NO-MANUSCRIPT-TEXT"
    for enumeration in (DivergenceClass, Verdict, Binding):
        for member in enumeration:
            assert f"{member}" == member.value, f"{member!r} rendered as {f'{member}'!r}, not its wire value"
            assert isinstance(member, str)


def test_a_well_formed_diagnosis_is_returned_unchanged() -> None:
    """Property: `classify` returns the class the diagnosis records, as the same enumeration member.

    Class: NEW CAPABILITY — the plausible wrong implementation is a classifier that computes a class
    from the reported values instead of returning the recorded one, which is the whole failure the
    record exists to prevent. The fixture records `D6`, and a value-comparing classifier can only
    ever reach `D1`, `D2` or `D3` from two numbers, so it reds here. Red under it, measured in the
    build return — and the first draft of this test used `D3` and did NOT discriminate, because an
    inference-based classifier lands on `D3` for these values as well.
    Reachability: every classify call reaches this assertion; the input is a well-formed `D6` record
    against a bound, reachable, diverged failure.
    Both-states anchor: the assertion is member identity between the record's field and the return
    value, a relation that exists whether or not this test does.
    Invocation isolation (B4-4, arm 1): no tree input reaches this assertion.
    Kills: a classifier that infers, or that returns a copy or a plain string rather than the member.
    A second correct implementation: a classifier that validates and then returns
    `DivergenceClass(diagnosis.divergence)` still passes, because construction from the wire value
    yields the same member identity.
    """
    diagnosis = _a_diagnosis(divergence=DivergenceClass.D6)
    returned = classify(_a_failure(), diagnosis)
    assert returned is diagnosis.divergence, (
        f"the classifier returned {returned} for a record that states {diagnosis.divergence}; "
        "a class computed from the reported values would land on D1, D2 or D3"
    )


def test_a_diagnosis_keyed_to_another_equation_is_refused() -> None:
    """Property: a record authored for `E-02` handed a failure for `E-04` raises `DiagnosisMismatch`,
    while the same record against an `E-02` failure is admitted.

    Class: NEW CAPABILITY — the plausible wrong implementation is a classifier that never compares
    the two equation identifiers, so a per-equation record silently applies to whichever failure it
    was handed. Red under it, because the refusal arm returns a class instead of raising.
    Reachability: any mis-keyed record reaches this branch, and a per-equation record is exactly the
    shape that makes the mistake possible.
    Both-states anchor: the assertion is the raised exception type on one arm and the returned member
    on the other, both of which exist independently of this test.
    Invocation isolation (B4-4, arm 1): no tree input reaches this assertion.
    Kills: a recorded ruling applied to the wrong equation, which is silent mis-attribution rather
    than a wrong answer.
    A second correct implementation: a classifier that keys the check on the record's own identifier
    looked up in a register rather than on field equality still passes, because the two identifiers
    are the same input either way.
    """
    mismatched = _a_diagnosis(equation_id="E-02", divergence=DivergenceClass.D4)
    with pytest.raises(DiagnosisMismatch):
        classify(_a_failure(equation_id="E-04"), mismatched)
    assert classify(_a_failure(equation_id="E-02"), mismatched) is DivergenceClass.D4


def test_the_cross_repo_class_and_binding_must_agree_in_both_directions() -> None:
    """Property: `D8` on a bound failure raises `ClassConflict`, `D3` on a cross-repo failure raises
    `ClassConflict`, and `D8` on a cross-repo failure is admitted.

    Class: NEW CAPABILITY — the plausible wrong implementation enforces only one direction: `D8`
    requires `CROSS_REPO`, but nothing requires `CROSS_REPO` to be classified `D8`. That one-way rule
    passes the first assertion and fails the second, which is why both directions are driven.
    Red under it, measured in the build return.
    Reachability: every cross-repo equation reaches the second arm and every non-cross-repo equation
    reaches the first, so the two arms together cover the whole population rather than a corner.
    Both-states anchor: the assertions are the raised type and the returned member, both present in
    the module whatever this test says.
    Invocation isolation (B4-4, arm 1): no tree input reaches this assertion.
    Kills: a one-way rule, which is the form a half-implemented consistency check actually takes.
    A second correct implementation: a classifier that reads the pair as a lookup table keyed on
    (divergence, binding) rather than as two `if` statements still passes, because it refuses the
    same two combinations.
    """
    with pytest.raises(ClassConflict):
        classify(_a_failure(binding=Binding.BOUND), _a_diagnosis(divergence=DivergenceClass.D8))
    with pytest.raises(ClassConflict):
        classify(_a_failure(binding=Binding.CROSS_REPO), _a_diagnosis(divergence=DivergenceClass.D3))
    admitted = classify(_a_failure(binding=Binding.CROSS_REPO), _a_diagnosis(divergence=DivergenceClass.D8))
    assert admitted is DivergenceClass.D8


def test_the_unreachable_class_and_reachability_must_agree_in_both_directions() -> None:
    """Property: `D9` on a reachable failure raises `ClassConflict`, `D3` on an unreachable failure
    raises `ClassConflict`, and `D9` on an unreachable failure is admitted.

    Class: NEW CAPABILITY — as the cross-repo rule, the plausible wrong implementation is the one-way
    form: `D9` requires unreachability, but nothing then forces an unreachable failure to be
    classified `D9`. Red under it on the second assertion.
    Reachability: the four reachable-by-class combinations are exactly the population, and the two
    refusal arms cover the two that conflict; the admitted arm is the third. The fourth (a
    cross-repo AND unreachable record) is refused as a consequence of the two rules meeting, not by a
    rule of its own, and is deliberately not driven here.
    Both-states anchor: the raised type and the returned member, both present in the module.
    Invocation isolation (B4-4, arm 1): no tree input reaches this assertion.
    Kills: a one-way reachability rule, and a classifier that reads `reachable` as truthy text.
    A second correct implementation: a classifier comparing `failure.reachable is False` rather than
    testing truthiness still passes, because the field is a real `bool` in every fixture here.
    """
    with pytest.raises(ClassConflict):
        classify(_a_failure(reachable=True), _a_diagnosis(divergence=DivergenceClass.D9))
    with pytest.raises(ClassConflict):
        classify(_a_failure(reachable=False), _a_diagnosis(divergence=DivergenceClass.D3))
    admitted = classify(_a_failure(reachable=False), _a_diagnosis(divergence=DivergenceClass.D9))
    assert admitted is DivergenceClass.D9


@pytest.mark.parametrize("paper_side", sorted(AUTHOR_JUDGED))
def test_a_paper_side_class_needs_a_named_author(paper_side: DivergenceClass) -> None:
    """Property: every class in `AUTHOR_JUDGED` refuses with `AuthorJudgementMissing` when `ruled_by`
    is absent, and is admitted when it names a party.

    Class: NEW CAPABILITY — the plausible wrong implementation is a classifier that never reads
    `ruled_by`, which makes the two paper-side classes differ from the other seven only by a docstring
    claim. Red under it, because the refusal arm returns a class instead of raising.
    Reachability: every member of `AUTHOR_JUDGED` is driven, and the parametrization is taken from the
    set the refusal itself reads rather than from a literal — so adding a class to that set extends
    this test rather than leaving it behind.
    Both-states anchor: the assertion reads the set the module declares and the exception/return
    types, all present without this test.
    Invocation isolation (B4-4, arm 1): no tree input reaches this assertion.
    Kills: an attestation rule that exists only in prose, and a `ruled_by` check that fires on the
    wrong population.
    A second correct implementation: a classifier testing `diagnosis.ruled_by in (None, "")` instead
    of a strip-based blank test still passes, because the absent arm passes `None` and the admitted
    arm passes a real name.
    """
    assert frozenset({DivergenceClass.D1, DivergenceClass.D2}) == AUTHOR_JUDGED, (
        "the author-judged population moved; every class in it owes a required `ruled_by`"
    )
    with pytest.raises(AuthorJudgementMissing):
        classify(_a_failure(), _a_diagnosis(divergence=paper_side, ruled_by=None))
    with pytest.raises(AuthorJudgementMissing):
        classify(_a_failure(), _a_diagnosis(divergence=paper_side, ruled_by="   "))
    admitted = classify(_a_failure(), _a_diagnosis(divergence=paper_side, ruled_by="D. Lassiter"))
    assert admitted is paper_side


def test_a_green_is_not_classifiable() -> None:
    """Property: a failure carrying `diverged` `False` raises `NotAFailure`, while an otherwise
    identical failure carrying `diverged` `True` is classified.

    Class: NEW CAPABILITY — the plausible wrong implementation decides divergence by comparing
    `observed` with `expected`. Under it this fixture's array values make `bool()` raise, so the
    refusal arm reports a `ValueError` instead of `NotAFailure`; there is no implementation that
    green-lights a green silently, which is exactly why `diverged` is a field.
    Reachability: every green comparison reaches this branch, and the fixtures are arrays because
    that is the ordinary case the comparison surface produces.
    Both-states anchor: the two arms are the SAME record differing only in `diverged`, asserted below
    field by field, so the refusal can only be reading that flag.
    Invocation isolation (B4-4, arm 1): no tree input reaches this assertion.
    Kills: a classifier that compares values, or one that classifies a green as a vacuous pass.
    A second correct implementation: a classifier testing `failure.diverged is False` rather than
    truthiness still passes, because the fixture's flag is a real `bool`.
    """
    red = _a_failure()
    green = replace(red, diverged=False)
    assert green.observed is red.observed and green.expected is red.expected
    assert (green.equation_id, green.binding, green.reachable) == (red.equation_id, red.binding, red.reachable)
    assert green.diverged is False and red.diverged is True

    with pytest.raises(NotAFailure):
        classify(green, _a_diagnosis())
    assert classify(red, _a_diagnosis()) is DivergenceClass.D3


def test_an_unevidenced_diagnosis_is_refused() -> None:
    """Property: a record whose evidence is empty or whitespace-only raises `UnevidencedDiagnosis`,
    while the same record carrying a real note is admitted.

    Class: NEW CAPABILITY — the plausible wrong implementation is a classifier that never reads
    `evidence`, so a ruling with nothing behind it classifies exactly as one with a citation. Red
    under it, because the refusal arm returns a class instead of raising.
    Reachability: every record reaches this branch, and a whitespace-only note is the realistic
    accidental case rather than a manufactured one.
    Both-states anchor: the raised type and the returned member, both present in the module.
    Invocation isolation (B4-4, arm 1): no tree input reaches this assertion.
    Kills: an empty evidence string accepted as if it were a citation.
    A second correct implementation: a classifier testing `not diagnosis.evidence.strip()` directly
    still passes, because the fixtures hold real strings and the assertion is the raised type.
    """
    with pytest.raises(UnevidencedDiagnosis):
        classify(_a_failure(), _a_diagnosis(evidence=""))
    with pytest.raises(UnevidencedDiagnosis):
        classify(_a_failure(), _a_diagnosis(evidence="  \n\t "))
    assert classify(_a_failure(), _a_diagnosis(evidence="the code returns the sample estimator")) is DivergenceClass.D3


def test_the_refusals_fire_in_the_order_the_decision_lists_them() -> None:
    """Property: where one record violates more than one refusal condition at once, the EARLIEST
    condition the decision lists is the one that fires, so a caller hears the first thing wrong.

    Class: NEW CAPABILITY — the plausible wrong implementation orders the checks by how naturally
    each reads rather than by the decision's own list: evidence first because it is the cheapest, or
    the binding conflict before the identifier comparison because it is the loudest. Every such
    ordering passes every single-defect test in this module, because a record with ONE defect is
    reached by every ordering; only a record carrying TWO tells them apart. Red under each ordering
    driven below, measured in the reviewer return.
    Reachability: the four arms are four two-defect records, one per adjacent pair of refusals, so
    each of the four boundaries of the declared order is reached. All four are realistic rather than
    manufactured: a mis-keyed record that also carries no evidence is what a copy-pasted ruling looks
    like, and a green handed to a classifier is the vacuous-pass case the shape names.
    Both-states anchor: each assertion reads the raised exception type, which exists in the module
    whatever this test says, and each arm differs from a fixture already in this module by exactly one
    additional defect — so the assertion can only be reading the ORDER and not the defect itself.
    Invocation isolation (B4-4, arm 1): every failure and record is constructed in-process with no
    file, path or environment input, so the same invocation returns the same verdict from any
    checkout the run resolves from.
    Kills: a reordered classifier, which is the exact shape a later "cheap checks first" refactor
    takes, and which no single-defect test can see.
    A second correct implementation: a classifier that evaluates all five predicates into a list and
    raises the first by rank still passes, because it refuses the same member on every arm.
    """
    # Refusal 1 outranks refusal 3: a green is not classifiable at all, whatever its binding says.
    with pytest.raises(NotAFailure):
        classify(_a_failure(diverged=False), _a_diagnosis(divergence=DivergenceClass.D8))

    # Refusal 2 outranks refusal 3: a record keyed to another equation is mis-attribution first,
    # and its class is then never examined.
    with pytest.raises(DiagnosisMismatch):
        classify(
            _a_failure(equation_id="E-04"),
            _a_diagnosis(equation_id="E-02", divergence=DivergenceClass.D8),
        )

    # Refusal 3 outranks refusal 4: the class contradicting the binding is reported before the
    # missing author attestation on a class that would otherwise owe one.
    with pytest.raises(ClassConflict):
        classify(
            _a_failure(binding=Binding.CROSS_REPO),
            _a_diagnosis(divergence=DivergenceClass.D1, ruled_by=None),
        )

    # Refusal 4 outranks refusal 5: an unattested paper-side ruling is reported before its missing
    # evidence, so the caller is not sent to look for a citation before it knows who ruled.
    with pytest.raises(AuthorJudgementMissing):
        classify(
            _a_failure(),
            _a_diagnosis(divergence=DivergenceClass.D2, evidence="", ruled_by=None),
        )


def test_the_derived_in_status_population_is_the_declared_pair() -> None:
    """Property: `DERIVED_IN_STATUS` is exactly the classes `D8` and `D9`, the two the published
    `Status` carries by its binding and its verdict and therefore refuses inside a defect list.

    Class: NEW CAPABILITY — the plausible wrong implementation declares the pair with one member
    dropped, which is what happens when the set is written from memory of the facet split rather
    than from the shape's own enumeration. Red under it, because the assertion is over the set's
    full membership rather than over the member the neighbouring chunk happens to drive.
    Reachability: the set is a module constant with no input, so every run reads the same value; the
    defect this test reaches is a declaration that differs from the shape, which no call site can
    observe because nothing in this work chunk reads `DERIVED_IN_STATUS` — its only consumer is the
    defect-list refusal the NEXT chunk lands, and that refusal's own test drives one derived member
    and admits one ordinary member, so an omission of the other derived member is invisible to it.
    Both-states anchor: the assertion compares the module's own frozen set against the pair the shape
    declares, a relation that holds whether or not this test exists.
    Invocation isolation (B4-4, arm 1): no tree input reaches this assertion.
    Kills: a set declared from memory, and a set edited to serve a test that only ever asks about
    one of its members.
    A second correct implementation: a set declared as a `frozenset` over the members, over their
    wire strings, or built by comprehension from the class vocabulary all still pass, because the
    assertion reads the frozenset of members rather than the declaration form.
    """
    assert frozenset(DERIVED_IN_STATUS) == frozenset({DivergenceClass.D8, DivergenceClass.D9}), (
        f"the derived-in-status population is {sorted(DERIVED_IN_STATUS)}, not the declared pair D8 and D9"
    )


def test_a_caller_catching_the_family_hears_every_refusal_and_which_one() -> None:
    """Property: each of the five declared refusals escapes as an instance of
    `ConformanceTaxonomyError` AND of its own named subclass, so a caller that catches the family
    hears every refusal and the member still says which one fired.

    Class: NEW CAPABILITY — the plausible wrong implementation leaves one subclass inheriting
    `Exception` directly, dropping the family base: five refusals that all still subclass
    `Exception`, so nothing at the raise site changes. Every other test in this module passes under
    it, because each of them catches the NAMED subclass; only a catch on the family sees the loss,
    and a consumer written against the family is the operation the family exists for. Red under it,
    measured in the reviewer return.
    Reachability: all five refusal conditions are driven, so every member of the family is reached
    at least once, and the population is the module's declared family rather than a sample of it.
    Both-states anchor: the assertions read the exception type actually raised and compare it with
    the family and with the named class, both of which exist in the module independently of this
    test; the fixtures are the same one-defect records the tests above use.
    Invocation isolation (B4-4, arm 1): no tree input reaches this assertion.
    Kills: a refusal that loses its place in the family, and a refusal whose raised member is not the
    one the condition names — the two ways a caller's `except` clause silently misses a case.
    A second correct implementation: a classifier that raises from a table of (condition, exception)
    pairs rather than from one `raise` per branch still passes, because the assertion reads what was
    raised rather than where it was raised.
    """
    refusals = (
        (NotAFailure, _a_failure(diverged=False), _a_diagnosis()),
        (
            DiagnosisMismatch,
            _a_failure(equation_id="E-04"),
            _a_diagnosis(equation_id="E-02", divergence=DivergenceClass.D4),
        ),
        (ClassConflict, _a_failure(binding=Binding.CROSS_REPO), _a_diagnosis(divergence=DivergenceClass.D3)),
        (AuthorJudgementMissing, _a_failure(), _a_diagnosis(divergence=DivergenceClass.D1, ruled_by=None)),
        (UnevidencedDiagnosis, _a_failure(), _a_diagnosis(evidence="")),
    )
    for expected, failure, diagnosis in refusals:
        with pytest.raises(ConformanceTaxonomyError) as caught:
            classify(failure, diagnosis)
        assert isinstance(caught.value, expected), (
            f"a {expected.__name__} condition raised {type(caught.value).__name__}, "
            "so a caller catching the family hears the wrong refusal"
        )


def test_the_package_re_exports_every_name_this_chunk_publishes() -> None:
    """Property: each of the fourteen names the shape assigns to this work chunk is listed in
    `multidriver_swg.conformance.__all__` and resolves as an attribute of the package.

    Class: NEW CAPABILITY — the plausible wrong implementation imports a name into the package module
    without listing it in `__all__`, which is the thing a hand-typed list loses first. The name then
    works under an explicit `from ... import name` and vanishes from `import *` and from every
    consumer that enumerates the surface, so the interface is narrower than it reads.
    Reachability: the enumeration is the shape's own list of fourteen names, all of which the chunk
    is required to publish; the assertion is reached by any edit to `__all__`, which this chunk and
    the next both make.
    Both-states anchor: the assertion reads the package's own `__all__` and `getattr` results, which
    exist whatever this test says, and never a list this test carries.
    Invocation isolation (B4-4, arm 1): no tree input reaches this assertion beyond the import itself.
    Kills: a name bound at module level and omitted from `__all__`, and a name listed in `__all__`
    that no longer resolves.
    A second correct implementation: a package declaring `__all__` as a tuple, in a different order,
    or already carrying the NEXT chunk's names all still pass, because the assertion is a SUBSET one
    and deliberately not equality — an equality pin here would red on the next chunk's correct
    landing rather than on a defect, which is the allowlist rule this shape states.
    """
    published = (
        "DivergenceClass",
        "Verdict",
        "Binding",
        "AUTHOR_JUDGED",
        "DERIVED_IN_STATUS",
        "ConformanceFailure",
        "Diagnosis",
        "classify",
        "ConformanceTaxonomyError",
        "NotAFailure",
        "DiagnosisMismatch",
        "ClassConflict",
        "AuthorJudgementMissing",
        "UnevidencedDiagnosis",
    )
    missing = [name for name in published if name not in conformance_package.__all__]
    assert not missing, f"the package binds {missing} at module level but omits them from __all__"
    unresolved = [name for name in published if not hasattr(conformance_package, name)]
    assert not unresolved, f"__all__ promises {unresolved}, which do not resolve on the package"


def test_the_package_re_exports_every_name_item_two_publishes() -> None:
    """Property: each of the ten names the status-vocabulary item adds to the package surface —
    `Status`, its family base and four refusals, `manuscript_binding`, and the three BLOCKING SET
    names — is listed in `multidriver_swg.conformance.__all__` and resolves on the package.

    Class: NEW CAPABILITY — the plausible wrong implementation binds a name at module level and omits
    it from `__all__`, which is what a hand-typed list loses first when it is rewritten end to end;
    the name then works under an explicit `from ... import name` and vanishes from `import *` and from
    any consumer that enumerates the surface. Red under it, measured in the reviewer return.
    Reachability: the population is the shape's own list for this item, all ten of which the chunk is
    required to publish, and the second chunk rewrote that list wholesale — which is exactly the edit
    in which a member drops.
    Both-states anchor: the assertion reads the package's own `__all__` and its `getattr` results,
    both of which exist whatever this test says, and never a list this test carries as its authority.
    Invocation isolation (B4-4, arm 1): no file, path or environment input reaches this assertion
    beyond the import itself.
    Kills: a name bound at module level and omitted from `__all__`, and a name listed there that no
    longer resolves.
    A second correct implementation: a package declaring `__all__` as a tuple, in a different order, or
    already carrying a later chunk's names still passes, because the assertion is a SUBSET one and
    deliberately not equality — an equality pin would red on a later chunk's correct landing rather
    than on a defect.
    One class, two tests rather than a restatement: this population is the ten names item 2 adds and
    is disjoint from the fourteen the sibling test above ranges over, so the two together enumerate
    the twenty-four-name surface without either repeating the other.
    """
    published = (
        "Status",
        "StatusVocabularyError",
        "NotADivergenceClass",
        "DerivedClassInDefectList",
        "ConformingWithDefects",
        "UndiagnosedDivergence",
        "manuscript_binding",
        "BLOCKING_VERDICTS",
        "BLOCKING_BINDINGS",
        "is_blocking",
    )
    missing = [name for name in published if name not in conformance_package.__all__]
    assert not missing, f"the package binds {missing} at module level but omits them from __all__"
    unresolved = [name for name in published if not hasattr(conformance_package, name)]
    assert not unresolved, f"__all__ promises {unresolved}, which do not resolve on the package"


def test_a_divergent_status_always_names_a_defect_whatever_the_iterable() -> None:
    """Property: a `DIVERGENT` status built from an iterable that yields at least one class either
    refuses, or carries that class in its defect list — it never comes into existence naming nothing.

    Class: NEW CAPABILITY — the plausible wrong implementation runs the membership test over the
    caller's iterable and coerces it afterwards, which CONSUMES a one-shot iterable; the truthiness
    check that follows then reads a still-truthy exhausted generator and admits it, so the status
    exists with an empty tuple. Red under it, measured in the reviewer return.
    Reachability: a caller assembling the list by comprehension or by `map` hands the field exactly
    this kind of value, and `Status`'s own note on coercion (`tuple("D3")` is `("D", "3")`)
    establishes that a non-tuple arriving at this field is contemplated rather than treated as a type
    error nobody meets.
    Both-states anchor: the assertion reads `Status`'s published invariant — a `DIVERGENT` verdict
    names at least one defect — which the module states independently of this test, and it admits a
    refusal as an equally correct outcome, so it pins no implementation and no refusal type.
    Invocation isolation (B4-4, arm 1): no file, path or environment input reaches this assertion.
    Kills: any construction path on which the undifferentiated red the interface forbids becomes
    constructible, which is the one state the whole refusal set exists to make unreachable.
    A second correct implementation: an implementation that materializes the iterable before its
    checks, and one that refuses a non-tuple outright, both pass — the assertion is that the forbidden
    state cannot exist, never which remedy the implementation chose.
    """
    constructed: Status | None = None
    try:
        constructed = Status("E-03", Verdict.DIVERGENT, Binding.BOUND, iter((DivergenceClass.D3,)))
    except Exception:  # noqa: BLE001  (any refusal is an admissible outcome; the invariant is the subject)
        return
    assert constructed.defect_list, (
        "a DIVERGENT status came into existence with an empty defect list — the undifferentiated red this "
        "interface forbids — built from an iterable that yields DivergenceClass.D3"
    )
