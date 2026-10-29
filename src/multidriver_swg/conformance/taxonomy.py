"""State the divergence vocabulary: the nine classes, the two Status fields, and the classifier.

Every conformance test that goes red hands its failure to exactly one class stated here, and each
class names WHICH artifact the project treats as authoritative and must therefore change. The
vocabulary is closed at nine. `D1` and `D2` put the manuscript or the methodology in the wrong;
`D3`–`D5` put the code or the paper's coverage in the wrong; `D6` and `D7` name a value or an
estimator that drifted; and `D8`/`D9` are not judgements about the pair at all — they say the
implementation lives in another repository, or cannot be reached.

Implements the `nine-class-classifier` scope item: the closed `D1`–`D9` class vocabulary, the two
closed Status field vocabularies, and the instrument that turns a red into exactly one class.

WHY THE CLASSIFIER VALIDATES A RECORDED DIAGNOSIS INSTEAD OF THE TWO VALUES. A red states that the
code disagrees with the paper. It does not state which of the two is authoritative, and that is
exactly what separates `D1`, `D2` and `D3` — three different rulings over the SAME numeric fact. A
function computing the class from `observed` and `expected` would therefore assert a judgement it
never made and dress it as arithmetic. So `classify` checks an authored record against the failure's
recorded binding and reachability, and returns the class the record already carries.

WHY `diverged` IS A FIELD AND NOT A COMPARISON. A conformance test compares through a licensed
comparison surface, so the values it reports are routinely arrays and routinely compared under a
tolerance. `observed == expected` on an array returns an array and `bool()` of it raises, so an
implementation that compared the two here would raise on the ordinary case rather than refuse a
green. Whether a pair agrees is the comparison surface's decision; this module never reads
`observed` or `expected`, and carries them only so a report can print them.

Not established: that the class an author recorded is CORRECT. Nothing here can establish that,
because the class is a ruling about which artifact is authoritative. What this module establishes is
that a class was recorded, that it is consistent with the recorded binding and reachability, that a
`D1`/`D2` ruling names the party who made it, and that no red reaches a caller without exactly one
class.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class DivergenceClass(StrEnum):
    """The closed nine-class vocabulary; each member names the artifact its class says must change.

    `D1` and `D2` are AUTHOR JUDGEMENTS that no machine may separate (see `AUTHOR_JUDGED`). `D8` and
    `D9` are DERIVED from the recorded binding and reachability rather than authored (see
    `DERIVED_IN_STATUS`). The nine names below are the divergence taxonomy's own headings, quoted
    verbatim; the artifact column beside each is DERIVED from that heading's own parenthetical and
    the body beneath it, in a taxonomy that states the partition as an axis and tabulates no artifact
    per class, so the column is a reading of it and not a quotation from it:

        D1  Transcription error (the paper states something the author did not intend)          -> the manuscript
        D2  Methodology flaw (the paper faithfully records logic that is wrong)                -> the methodology
        D3  Code defect (the paper is right, the code is not)                                  -> the code
        D4  Paper omits code (the code implements a substantive method the paper does not state) -> the manuscript
        D5  Code omits paper (the paper states something the code cannot express)              -> the code
        D6  Parameter-value drift                                                              -> the value's single home
        D7  Estimator substitution (the paper writes a formula, the code calls a routine computing a different estimator)
                                                                                               -> the estimator, on one side
        D8  Cross-repo binding (the equation's implementation is not in this repository)       -> the binding
        D9  Unreachable implementation                                                         -> the code's reachability
    """

    D1 = "D1"
    D2 = "D2"
    D3 = "D3"
    D4 = "D4"
    D5 = "D5"
    D6 = "D6"
    D7 = "D7"
    D8 = "D8"
    D9 = "D9"


class Verdict(StrEnum):
    """The conformance field's closed vocabulary: the code conforms, diverges, or cannot be reached.

    `UNREACHABLE` sits here, as the third value of one fact, rather than in a field of its own. Two
    encodings of one fact are two things that can disagree, and a published interface that let them
    disagree would be the single mixed slot this vocabulary exists to split.
    """

    CONFORMING = "CONFORMING"
    DIVERGENT = "DIVERGENT"
    UNREACHABLE = "UNREACHABLE"


class Binding(StrEnum):
    """The binding field's closed vocabulary: where the equation's implementation is stated to live.

    Deliberately hyphenated wire values for the two spellings the manuscript's own prose uses, so a
    value read off the published interface and a value written in prose are the same token rather
    than two forms a reader has to reconcile.
    """

    BOUND = "BOUND"
    CROSS_REPO = "CROSS-REPO"
    NO_MANUSCRIPT_TEXT = "NO-MANUSCRIPT-TEXT"


# The classes a machine may not separate: `D1` and `D2` differ only by which side's intent is wrong,
# which no arithmetic over two values can see. A refusal reads this set rather than a literal pair,
# so extending the author-judged population is one edit and the enforcement is already wired.
AUTHOR_JUDGED = frozenset({DivergenceClass.D1, DivergenceClass.D2})

# The classes the published `Status` carries by its binding and its verdict, and therefore refuses
# inside a defect list: one fact with two homes is a fact that can be stated two ways.
DERIVED_IN_STATUS = frozenset({DivergenceClass.D8, DivergenceClass.D9})


@dataclass(frozen=True)
class ConformanceFailure:
    """One red conformance test: the values it reported, and the facts a recorded class is checked against.

    `observed` and `expected` are typed `object` on purpose. They are carried for the report and are
    never compared here, so a narrower annotation would promise a comparison this module does not
    perform, and a `float` annotation would forbid the array-valued equations the comparison surface
    exists for. `diverged` is the caller's verdict over that pair, decided through the licensed
    comparison surface; this dataclass records it and does not re-derive it.
    """

    equation_id: str
    observed: object
    expected: object
    binding: Binding
    reachable: bool
    diverged: bool


@dataclass(frozen=True)
class Diagnosis:
    """One authored ruling on a red: its class, the evidence for it, and who ruled when it is a judgement.

    `ruled_by` is required in substance — not by construction — exactly when the class is `D1` or
    `D2`, and `classify` is what refuses a record that leaves it out. It defaults to `None` here
    because the record has to be CONSTRUCTIBLE in order to be refused: a construction-time rule
    would make the missing attestation unreachable rather than refused, and an unreachable refusal
    is not a check.
    """

    equation_id: str
    divergence: DivergenceClass
    evidence: str
    ruled_by: str | None = None


class ConformanceTaxonomyError(Exception):
    """Base class for every refusal `classify` raises, so a caller can catch the family and act on the member.

    A named base rather than a bare exception keeps "this record cannot be classified" distinguishable
    from any other failure a caller might see at the same site, and the five named subclasses keep the
    reasons distinguishable from each other — a caller that must react differently to a mis-keyed
    record than to a missing author cannot say so against one undifferentiated error.
    """


class NotAFailure(ConformanceTaxonomyError):  # noqa: N818  (the name the refusals are written against)
    """The comparison reported agreement, so there is no divergence to classify."""


class DiagnosisMismatch(ConformanceTaxonomyError):  # noqa: N818  (the name the refusals are written against)
    """The record was authored for a different equation than the failure it was handed to."""


class ClassConflict(ConformanceTaxonomyError):  # noqa: N818  (the name the refusals are written against)
    """The recorded class contradicts the recorded binding or reachability, in one direction or the other."""


class AuthorJudgementMissing(ConformanceTaxonomyError):  # noqa: N818  (the name the refusals are written against)
    """A `D1` or `D2` ruling carries no non-blank name of the party who made it."""


class UnevidencedDiagnosis(ConformanceTaxonomyError):  # noqa: N818  (the name the refusals are written against)
    """The record carries no non-blank evidence, so a reader cannot check the ruling against what was seen."""


def classify(failure: ConformanceFailure, diagnosis: Diagnosis) -> DivergenceClass:
    """Return the one class `diagnosis` records, after refusing every record that cannot be relied on.

    The refusals run in a fixed order, so a caller diagnosing a multi-defect record learns the
    earliest thing wrong with it rather than an arbitrary one. Each names the equation it concerns.

    Deliberately NOT a comparison of `observed` with `expected`: see this module's docstring for why
    that computation would either raise on the array-valued ordinary case or assert a judgement the
    function never made.
    """
    if not failure.diverged:
        raise NotAFailure(
            f"{failure.equation_id}: the comparison reported agreement, so there is no divergence to classify; "
            "classifying a green would dress a vacuous pass as a diagnosis"
        )
    if diagnosis.equation_id != failure.equation_id:
        raise DiagnosisMismatch(
            f"the diagnosis is recorded for {diagnosis.equation_id} but the failure is {failure.equation_id}; "
            "applying one equation's ruling to another is silent mis-attribution"
        )
    _refuse_derived_class_conflict(failure, diagnosis)
    if diagnosis.divergence in AUTHOR_JUDGED and _blank(diagnosis.ruled_by):
        raise AuthorJudgementMissing(
            f"{diagnosis.equation_id}: {diagnosis.divergence} is a paper-side ruling, so it must name the party who "
            "made it; a class no machine may separate cannot be recorded anonymously"
        )
    if _blank(diagnosis.evidence):
        raise UnevidencedDiagnosis(
            f"{diagnosis.equation_id}: the diagnosis carries no evidence, so a reader cannot check the ruling "
            "against what was observed"
        )
    return diagnosis.divergence


def _refuse_derived_class_conflict(failure: ConformanceFailure, diagnosis: Diagnosis) -> None:
    """Refuse a class the recorded binding or reachability contradicts, checked in BOTH directions.

    One direction alone is a one-way rule that discriminates nothing. `D8` requiring `CROSS_REPO`
    still lets every cross-repo equation be filed under some other class; `CROSS_REPO` requiring
    `D8` still lets a bound equation claim the implementation lives elsewhere. The same holds for
    `D9` against `reachable`, which is why each pair is two refusals rather than one.

    A consequence worth knowing rather than discovering: an equation whose implementation is both
    cross-repo AND unreachable cannot be classified at all here, because one record carries one class
    and the two rules then require two different ones. It is refused as `ClassConflict` rather than
    silently assigned whichever rule ran first.
    """
    if diagnosis.divergence is DivergenceClass.D8 and failure.binding is not Binding.CROSS_REPO:
        raise ClassConflict(
            f"{failure.equation_id}: class D8 says the implementation lives in another repository, but the recorded "
            f"binding is {failure.binding}"
        )
    if failure.binding is Binding.CROSS_REPO and diagnosis.divergence is not DivergenceClass.D8:
        raise ClassConflict(
            f"{failure.equation_id}: the recorded binding is {Binding.CROSS_REPO}, so the class is D8, "
            f"not {diagnosis.divergence}"
        )
    if diagnosis.divergence is DivergenceClass.D9 and failure.reachable:
        raise ClassConflict(
            f"{failure.equation_id}: class D9 says the implementation cannot be reached, but the failure records the "
            "implementation as reachable"
        )
    if not failure.reachable and diagnosis.divergence is not DivergenceClass.D9:
        raise ClassConflict(
            f"{failure.equation_id}: the implementation is recorded as unreachable, so the class is D9, "
            f"not {diagnosis.divergence}"
        )


def _blank(value: object) -> bool:
    """Whether `value` carries nothing a reader could use: not a string, or only whitespace."""
    return not (isinstance(value, str) and value.strip())
