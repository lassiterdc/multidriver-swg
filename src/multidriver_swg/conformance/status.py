"""Publish `Status`: an equation's conformance verdict, where its implementation lives, and its defects.

A reader of one conformance result needs three separate things about one equation: whether the
implementation conforms, where the implementation is stated to live, and which named divergences are
recorded against it. Crushing those into one field is the failure this shape exists to prevent — a
single "diverges" value cannot also say whether the divergence sits in another repository, so a
reader who needs the second goes elsewhere for it and may not come back. `Status` gives each facet
its own home, and refuses the states in which the homes disagree.

Implements the `status-vocabulary` scope item, and publishes the BLOCKING SET that a downstream
attribution reader consumes as a declared interface rather than re-deriving from the producer's
values.

WHY REACHABILITY IS NOT A THIRD FIELD. `Verdict.UNREACHABLE` already IS the reachability answer, as
the third value of one fact. A `reachable: bool` beside it would encode one fact twice, and two
encodings can disagree — the exact failure this vocabulary exists to remove, arriving inside the
published interface. `D9` lives in the verdict for the same reason, and `D8` lives in the binding.

WHY `D8` AND `D9` ARE REFUSED INSIDE THE DEFECT LIST. Both are already carried, by the binding and
by the verdict respectively, so admitting them to the list would give one fact two homes that can
drift apart. The refusal is keyed on `DERIVED_IN_STATUS` rather than on a literal pair, so widening
the derived population is one edit in `taxonomy` and the enforcement here follows it.

Deliberately NOT enforced: the JOINT combinations of the two fields. `UNREACHABLE` together with
`CROSS_REPO` is accepted, because neither value vocabulary fixes joint admissibility and a constraint
the design has not been fleshed out to hold would be over-constraint wearing validation's clothes.
The combination is named here so a reader meets the decision rather than inferring an oversight.

Not established: that a `D1` or `D2` appearing in a defect list carries an author. The attestation is
a field of `Diagnosis` and is enforced by `classify`, which is the only instrument that turns a record
into a class. A `Status` constructed directly with a paper-side class carries no author, and no
construction-time rule here can tell it from one that came through the classifier. Re-attesting on
the published interface would duplicate an obligation the classifier already enforces, and refusing
the two paper-side classes outright would delete the population the defect list exists to publish.
"""

from __future__ import annotations

from dataclasses import dataclass

from multidriver_swg.conformance.taxonomy import DERIVED_IN_STATUS, Binding, DivergenceClass, Verdict


class StatusVocabularyError(Exception):
    """Base class for every refusal `Status` raises at construction, so a caller can catch the family.

    A named base keeps "this status cannot exist" distinguishable from any other failure a consumer
    might meet at the same call, and the four named subclasses keep the reasons distinguishable from
    each other — a consumer that reacts differently to a derived-class slip than to a conforming
    verdict carrying defects cannot say so against one undifferentiated error.
    """


class NotADivergenceClass(StatusVocabularyError):  # noqa: N818  (the name the refusals are written against)
    """A defect-list entry is not a member of the class vocabulary."""


class DerivedClassInDefectList(StatusVocabularyError):  # noqa: N818  (the name the refusals are written against)
    """A defect-list entry is a class the binding or the verdict already carries."""


class ConformingWithDefects(StatusVocabularyError):  # noqa: N818  (the name the refusals are written against)
    """A conforming verdict carries a non-empty defect list, so the two fields contradict each other."""


class UndiagnosedDivergence(StatusVocabularyError):  # noqa: N818  (the name the refusals are written against)
    """A divergent verdict carries no defect, which is the undifferentiated red the vocabulary forbids."""


@dataclass(frozen=True)
class Status:
    """One equation's conformance verdict, its binding, and the divergence classes recorded against it.

    `defect_list` is a tuple because one equation can carry more than one recorded defect while its
    CLASSIFICATION is single — the classifier returns exactly one class, and this list is the separate,
    finer granularity the shape keeps beside it. The list may be empty for a conforming or an
    unreachable verdict; it may not be empty for a divergent one, and it may not contain `D8` or `D9`,
    whose facts the other two fields already hold.
    """

    equation_id: str
    verdict: Verdict
    binding: Binding
    defect_list: tuple[DivergenceClass, ...] = ()

    def __post_init__(self) -> None:
        """Refuse the four states in which the fields disagree, then freeze the defect list as a tuple.

        The membership test is `isinstance(entry, DivergenceClass)` and never `isinstance(entry, str)`:
        every member of the class vocabulary IS a `str`, so a string test admits the very values this
        refusal exists to catch. The coercion into the FIELD runs only AFTER the membership test,
        because `tuple("D3")` is `("D", "3")`, and coercing first would store a two-entry list nobody
        wrote.

        The caller's value is materialized ONCE, into a local, and every check below reads that local.
        The alternative — testing membership over the caller's value and reading it again for the
        verdict checks — passes every refusal and still admits a status it forbids: a one-shot
        iterable is consumed by the loop, and an exhausted generator is still TRUTHY, so
        `Status(..., DIVERGENT, ..., iter([D3]))` would come into existence naming no defect, which is
        the one state `UndiagnosedDivergence` exists to make unreachable. Reading one materialization
        instead of the caller's value is what closes that path, and it is why the two verdict checks
        are written against `materialized` rather than against `self.defect_list`.
        """
        materialized: list[DivergenceClass] = []
        for entry in self.defect_list:
            if not isinstance(entry, DivergenceClass):
                raise NotADivergenceClass(
                    f"{self.equation_id}: the defect list carries {entry!r}, which is not a DivergenceClass; every "
                    "member is itself a str, so only a membership test of the enumeration reads the right property"
                )
            if entry in DERIVED_IN_STATUS:
                raise DerivedClassInDefectList(
                    f"{self.equation_id}: the defect list carries {entry}, whose fact the binding or the verdict "
                    "already expresses; one fact with two homes is a fact that can be stated two ways"
                )
            materialized.append(entry)
        if self.verdict is Verdict.CONFORMING and materialized:
            raise ConformingWithDefects(
                f"{self.equation_id}: the verdict is {Verdict.CONFORMING} but the defect list names "
                f"{len(materialized)} defect(s); a conforming equation has none to name"
            )
        if self.verdict is Verdict.DIVERGENT and not materialized:
            raise UndiagnosedDivergence(
                f"{self.equation_id}: the verdict is {Verdict.DIVERGENT} but no defect is named; a divergence that "
                "names nothing is the undifferentiated red this vocabulary exists to rule out"
            )
        object.__setattr__(self, "defect_list", tuple(materialized))


# The BLOCKING SET, as VALUES beside the predicate that reads them, so a consumer reads the declared
# set rather than re-deriving it from the producer's fields — which is what "declared interface"
# means for a set, and what stops a consumer from making a producer's field well-formed from inside
# itself.
BLOCKING_VERDICTS = frozenset({Verdict.DIVERGENT, Verdict.UNREACHABLE})
BLOCKING_BINDINGS = frozenset({Binding.CROSS_REPO})


def is_blocking(status: Status) -> bool:
    """Whether `status` puts its equation in the BLOCKING SET: divergent, unreachable, or cross-repo.

    `CONFORMING`/`BOUND` is the row that must answer `False`, so an always-true predicate is caught
    rather than flattered by a table that only names the cases it already blocks.
    """
    return status.verdict in BLOCKING_VERDICTS or status.binding in BLOCKING_BINDINGS


def manuscript_binding(verbatim_span: str) -> Binding | None:
    """The one binding a manuscript span decides, or `None` where the span does not decide the binding.

    An empty span is the single derivable case: an entry with no span records an equation the
    manuscript never states, so its binding is `NO_MANUSCRIPT_TEXT` and nothing else could be.

    `None` rather than `BOUND` for a non-empty span, and the return TYPE is the point rather than a
    detail. Six of the register's thirty-six entries are cross-repository and every one of them carries
    a NON-empty span, so answering `BOUND` would hand exactly the equations that block a verdict a
    non-blocking binding — and a `Binding`-typed return would let a caller read that default as
    computed. `None` says what is true: the span does not settle it.
    """
    return Binding.NO_MANUSCRIPT_TEXT if verbatim_span == "" else None
