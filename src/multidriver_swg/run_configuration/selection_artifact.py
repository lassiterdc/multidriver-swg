"""Refuse a per-run selection document that is not the artifact, and name the marginal fits it holds.

A run's modelling starts with a human choice: which summary statistic is modelled, on which population
of events, under which marginal distribution family, and through which transformation. That choice is
not recoverable from the run's inputs, so a run that does not hold it in a checkable form can be given
the same inputs twice and produce two different models, with nothing in its output saying which was
chosen. This module owns the form of the document that holds the selection, and it does not produce
one: the document is written by a recorded interactive session, and a consumer's only route to a
selection is through the form this module accepts.

The rulings this module implements, in their own words, quoted verbatim so a later reader can check the
code against the ruling rather than against a paraphrase of it:

    "The per-run human selection is held as a single versioned, provenance-stamped document whose identifiers follow parent `ADR-n`'s grammar, and systems 4, 5, 6, 8 and 10 consume it from that one document."

    "a fit identifier is a function of (event class, statistic, distribution family, transformation chain) and MUST NOT embed a goodness-of-fit value"

The second sentence is a hard constraint rather than a preference, and the failure it names is silent in
the direction that matters: an identifier carrying its own metric changes whenever the record is
re-fitted, so a curated list of such identifiers goes on matching nothing while every check the package
has stays green. `FitIdentifier` therefore has exactly the ruling's four fields and no field a metric
could live in, and its parse admits exactly four components under a charset that refuses a metric's
surface spellings — a fifth dotted segment, `=`, a space, and a parenthesis. The third layer is the
document's, and it is a different KIND of guarantee rather than a wider one: every `selection` entry must
be a bare string, so the document offers no table field for a metric to live in, and an entry written as
a TOML table is refused. Three mechanisms for one constraint, and none is another's backstop — the type
stops a metric being carried as a field, the string-entry rule stops the document growing a per-entry
table, and the parse refuses the metric's surface spellings.

What none of the three mechanisms closes is stated here rather than left for a reader to assume. A metric SMUGGLED
INSIDE a legal segment is admitted, because `gamma_ks003` is a run of letters, digits and `_` and is a
legal `distribution_family`. The chain is the widest position of all: its steps are joined by `+`, so
`compound.rainfall_depth.gamma.identity+ks0_03` is four dotted components, a two-step chain, and a legal
spelling carrying a Kolmogorov-Smirnov statistic. That residual is the emission surface's discipline and
not a defect a charset could close — refusing digit-bearing steps would refuse `log10` while a metric
spelled `kstest` passed anyway.

A second bound is not a residual and a charset cannot reach it, so it is stated here. Two fits that differ
only in a transformation's FITTED PARAMETER are ONE identifier: the chain is a tuple of step NAMES, so
`log` under one Box-Cox lambda and `log` under another spell the same four components and compare equal.
The parameter belongs to the chosen transformation rather than being a fifth axis of the selection — it
changes on every re-fit, which is the failure the no-metric constraint already names — so this module
neither holds it nor refuses that collision, and a consumer that needs the fitted value reads it from the
fit record. That is a limit of the GRAMMAR rather than a defect of the charset, and it is why the four
components name every axis of the marginal selection except this one.

The refusals are the mechanism, not an error-reporting detail. A refusal owes its reader the recovery,
which for a malformed identifier is a one-line rewrite of the spelling, so each message names the entry,
the defect, and the spelling to write instead.

`SelectionArtifactError` is the root of every refusal in this module, so a caller treating any refusal
as one class catches the root instead of enumerating leaves. It derives from `Exception` directly, and
deliberately NOT from `KeyError`: `KeyError.__str__` renders its message inside repr quotes, and a
refusal's message is the reader's whole recovery path, so quoting it would deliver the actionable part
as a quoted literal.

The three leaves each separate a defect by WHO can fix it, because that is what a reader acts on.
`UnsupportedSelectionVersionError` is a document regenerated against the matching code version, while
`UnstampedSelectionError` and `UndescribedDependenceError` are the emission surface's omissions — kept
apart from each other so that a reader is told which of that surface's obligations was missed rather
than that something is missing. A leaf is a separate class only where the recovery differs; where it
does not, the root is raised.

`FitIdentifier` is the marginal-fit name: a function of event class, statistic, distribution family and
transformation chain, in that order, which is the ruling's own order and is stated once in
`IDENTIFIER_COMPONENTS` so the parser, the render and the tests cannot drift on it. The first three
components are one segment each; the chain is a tuple of one or more steps, `('identity',)` naming the
no-transformation chain. It is a tuple rather than a joined string because a chain is ORDERED — one
opaque string over its steps would let two orderings that mean different things collapse into one
identity. `parse` is the one place a dotted spelling becomes an identifier, so no consumer needs a parse
of its own, and `__str__` renders the spelling `parse` accepts, so every identifier round-trips through
text.

The DOCUMENT's own contract is four top-level keys and one version literal. `SELECTION_RULE` names the
form the document was written against, and it is compared as a LITERAL rather than parsed and refused
rather than migrated: the header's whole job is to make a later change to the document's fields loud, so
a header this code does not recognise names a document written against another form, and reading it
optimistically is exactly the silent misread the header exists to prevent. The four keys are `rule`,
`selection`, `dependence` and `stamp`, and a fifth is refused because an unnamed key is a field its
writer believes is live while nothing reads it. The closed set is drawn over the keys a CONFORMING
DOCUMENT carries, which is a separate question from whether a given key's CONTENTS are checked: an
unnamed-key refusal asks only whether the writer used a key the form names, so a named key whose own
refusal lives in another helper of this module is admitted here. The `selection` array is refused when it is absent, empty, or
carries a non-text entry, and the entries are then parsed in the document's order into identifiers, with
a repeated identifier refused rather than merged.

The MULTIVARIATE half is HELD here rather than named by an identifier, and that is the closure clause's
other bound. `ADR-n`'s identifier has one `statistic` slot, so a joint fit — a vine's structure, the
ordered marginals it is joint over, and one pair-copula family per edge position — is not a function of
one statistic and is therefore not an identifier at all. It is a `dependence` entry instead: one per
event class, whose marginals are REFERENCES to the identifiers the `selection` array already holds, so a
joint model and the margins it joins are validated as one object and a reference to a fit the document
does not hold cannot be written. Its family field carries exactly d(d-1)/2 names, the edge count of any
vine on d variables, and a uniform selection is expressed by REPEATING a name — never by a short tuple,
and never refused as a duplicate, because a short tuple would be ambiguous between one family for every
edge and a prefix of the edges. What the block does NOT decide is which EDGE a position names: for three
variables a C-vine and a D-vine enumerate the same positions over different pairs, so that reading is the
consumer's, applied against the structure kind it reads. The block carries no fitted copula parameter and
no selection metric, for the reason the marginal half carries none: a value that changes on a re-fit is
not part of a selection.

The STAMP is the document's third part — the run's provenance record carried INSIDE the selection rather
than beside it, so the artifact is auditable and versioned rather than merely versioned. Its three
sections are frozen at every depth before the record is assembled, because a read-only proxy at the top
level alone admits a write into a nested table and an append to an array: the proxy holds the parsed
document's own objects, so those two writes reach the document as well as the record.

Deliberately NOT a list of event classes, statistics, distribution families or chains: that vocabulary
is the emission surface's, and a toolkit-side enumeration would be a second source that drifts when the
emitter gains a value — and would make every new family a toolkit release. So an identifier naming a
family no emitter would offer, such as `atlantis.junk.gamma.identity`, is WELL-FORMED here, and whether
a run may select it is its consumer's judgement.

Not established: that the emission surface writes identifiers in the spelling this module accepts, that
it writes a dependence block whose family positions follow the consumer's convention, or that a named
family or chain denotes the model its consumer expects. This module checks the form of a selection and
nothing about its substance.
"""

from __future__ import annotations

import re
import tomllib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

from multidriver_swg.run_configuration.provenance import (
    ImplementationIdentity,
    ProvenanceRecord,
    capture,
)


class SelectionArtifactError(Exception):
    """The root of every refusal this module raises.

    A caller that wants any refusal as one class catches this rather than enumerating the leaves, which
    is why the leaves below derive from it rather than from separate stdlib errors. Raised directly
    where no leaf's recovery fits — a malformed identifier is rewritten in place, which is none of the
    three leaves' acts.
    """


class UnsupportedSelectionVersionError(SelectionArtifactError):
    """The document's version header is absent, or names a version this code does not read.

    A leaf because the reader's recovery differs from a malformed document's: an unsupported document
    is regenerated against the matching code version, while a malformed one is fixed where it sits, and
    those are different parties' acts.
    """


class UnstampedSelectionError(SelectionArtifactError):
    """The document carries no usable provenance stamp: absent, of the wrong shape, or empty of content.

    A leaf on the same ground: the defect is the emission surface's omission, and a reader told only
    that the document was refused would not know whom to send it back to.
    """


class UndescribedDependenceError(SelectionArtifactError):
    """The document carries no usable dependence block: absent, of the wrong shape, empty, or dangling.

    A leaf on the same ground as the stamp's, and deliberately kept SEPARATE from it: both are omissions
    the emission surface can make, and a reader told only that something is missing cannot tell which of
    that surface's obligations to fix.
    """


IDENTIFIER_COMPONENTS = ("event_class", "statistic", "distribution_family", "transformation_chain")

_SEGMENT_COUNT = len(IDENTIFIER_COMPONENTS)

_SEGMENT_PATTERN = re.compile(r"[A-Za-z0-9_-]+")

_CHAIN_STEP_SEPARATOR = "+"


def _is_segment(value: object) -> bool:
    """Whether `value` is a non-empty run of letters, digits, `_` and `-`, which is what a name component is."""
    return isinstance(value, str) and _SEGMENT_PATTERN.fullmatch(value) is not None


def _require_component(role: str, segment: object) -> None:
    """Refuse a one-segment component that names no value the grammar can address.

    The charset IS the surface-form half of the no-metric rule. It refuses every spelling a metric's
    value takes on the way in — `=`, a space, a parenthesis, and the `.` and `+` that would re-partition
    the spelling into more or fewer components — so a value that passed it carries no metric except one
    ENCODED inside a legal name, which is the residual the module docstring states.
    """
    if not _is_segment(segment):
        raise SelectionArtifactError(
            f"{role} {segment!r} is not a valid fit-identifier component: each of a fit identifier's "
            "first three components is one segment, a non-empty run of letters, digits, '_' and '-', so "
            "a component carries no separator, no dot, no '=', no space, no parenthesis and no '+'; "
            "rewrite the identifier in that spelling"
        )


def _require_step(step: object) -> None:
    """Refuse a chain step that names no value the grammar can address, on the same charset as a component.

    The `+` exclusion is load-bearing rather than incidental: `+` is the separator the render inserts
    and the parse splits on, so a step carrying one would come back as two steps and the identifier
    would denote a chain its author did not write.
    """
    if not _is_segment(step):
        raise SelectionArtifactError(
            f"transformation step {step!r} is not a valid step name: every step is a non-empty run of "
            "letters, digits, '_' and '-', and '+' is the chain's separator, so a step carrying one "
            "would split into two steps on the way back in; name the step without it"
        )


def _require_chain(chain: object) -> None:
    """Refuse a chain that is not a tuple of steps, that is empty, or that carries a repeated step.

    The container check runs first because both of its consequences are silent. A `str` is iterable, so
    a chain written as a plain string is read one step per letter and renders as a legal identifier for
    a fit nobody selected; a `list` leaves the frozen identifier unhashable, so the membership checks a
    consumer runs on it raise `TypeError` long after the identifier was admitted.
    """
    if not isinstance(chain, tuple):
        raise SelectionArtifactError(
            f"transformation_chain {chain!r} is {type(chain).__name__} rather than a tuple: a chain is "
            "an ordered TUPLE of steps, and the container kind matters twice over — a string iterates "
            "its characters, so a chain written as a plain string becomes one step per letter and "
            "renders as a legal identifier for a fit nobody selected, and a list is mutable, so an "
            "identifier that passed every check could be changed afterwards and is not hashable for the "
            "membership checks a consumer runs; write it as a tuple, ('identity',) for the "
            "no-transformation chain"
        )

    if not chain:
        raise SelectionArtifactError(
            "transformation_chain is empty: a chain names at least one step, with ('identity',) naming "
            "the no-transformation chain, and a chain of no steps renders the identifier with nothing "
            "after its third separator, so the spelling parses back to an empty step and the identifier "
            "would not round-trip through its own text"
        )

    for step in chain:
        _require_step(step)

    # An index walk rather than `zip(chain, chain[1:])`: the two operand lengths differ by one by
    # construction, so `strict=True` would raise on every well-formed chain and `strict=False` would be
    # the silent-truncation form the repo's lint set refuses.
    for position in range(1, len(chain)):
        step = chain[position]
        if chain[position - 1] == step:
            raise SelectionArtifactError(
                f"transformation_chain {chain!r} repeats the step {step!r} at positions "
                f"{position - 1} and {position}: an immediately repeated step is what a duplicated "
                "paste produces, and a chain that applies one step twice is a different transformation "
                "that deserves a step name of its own, so one spelling keeps denoting one chain"
            )


@dataclass(frozen=True)
class FitIdentifier:
    """One marginal fit's name: event class, statistic, distribution family, transformation chain.

    A frozen dataclass of exactly the four components `IDENTIFIER_COMPONENTS` states, in that order, and
    no field a goodness-of-fit value could live in. The first three are one segment each, a non-empty
    run of letters, digits, `_` and `-`; the fourth is a tuple of one or more steps, with `('identity',)`
    naming the no-transformation chain.

    WHY THE CHAIN IS A TUPLE AND A COMPONENT IS NOT. A chain is ordered, and a single opaque string over
    its steps would let two orderings that mean different things collapse into one identity — the
    silent-match failure the no-metric constraint exists to prevent, arriving from the other side. So
    the steps are held apart, and only the render joins them.

    Deliberately NOT accepting a bare string as the chain: a `str` is iterable, so a chain written as
    `'identity'` would build eight one-letter steps and render `i+d+e+n+t+i+t+y` — a well-formed
    identifier for a fit nobody selected. Refusing the container rather than coercing it is what turns
    that from a silent wrong value into a message. Deliberately NOT accepting a `list` either, for the
    reason `_require_chain` records: a mutable chain makes the frozen identifier unhashable.

    Frozen, and therefore hashable, because a consumer holds identifiers in tuples and compares them for
    membership: a mutable identifier could change after the check that admitted it.

    Raises `SelectionArtifactError` rather than a leaf, because none of the three leaves' recoveries fits
    a malformed spelling — the recovery is a one-line rewrite of the identifier, which the message
    names.

    Not established: that a component names something a run may select. The module holds no list of
    event classes, statistics, families or chains, so `atlantis.junk.gamma.identity` constructs here,
    and what it denotes is its consumer's judgement.
    """

    event_class: str
    statistic: str
    distribution_family: str
    transformation_chain: tuple[str, ...]

    def __post_init__(self) -> None:
        """Refuse an identifier whose components no dotted spelling could denote."""
        _require_component("event_class", self.event_class)
        _require_component("statistic", self.statistic)
        _require_component("distribution_family", self.distribution_family)
        _require_chain(self.transformation_chain)

    @classmethod
    def parse(cls, key: str) -> FitIdentifier:
        """Build an identifier from its dotted spelling; the one place a spelling becomes a value.

        A document's `selection` entry is handed here rather than split by its reader, so no consumer
        re-derives the grammar and no un-parsed spelling reaches one. A spelling that does not split into
        exactly four segments is refused before the per-component refusals run, because three segments
        and five segments are one defect and the count is the fact a reader needs to recover. The fourth
        segment is split on the chain's separator and the components are checked by construction, which
        is where an empty step and an immediately repeated one are refused.
        """
        segments = key.split(".")
        if len(segments) != _SEGMENT_COUNT:
            raise SelectionArtifactError(
                f"selection entry {key!r} splits into {len(segments)} dotted segments: a fit identifier "
                "is 'event_class.statistic.distribution_family.transformation_chain', exactly four "
                "segments, where the fourth names one or more '+' joined transformation steps; rewrite "
                "the entry in that spelling"
            )
        event_class, statistic, distribution_family, chain = segments
        return cls(
            event_class=event_class,
            statistic=statistic,
            distribution_family=distribution_family,
            transformation_chain=tuple(chain.split(_CHAIN_STEP_SEPARATOR)),
        )

    def __str__(self) -> str:
        """Render the canonical dotted spelling `parse` accepts, so an identifier round-trips through text."""
        chain = _CHAIN_STEP_SEPARATOR.join(self.transformation_chain)
        return ".".join((self.event_class, self.statistic, self.distribution_family, chain))


# The document-contract half starts here, after the identifier type because it cannot start before it: a
# document's `selection` entry is parsed INTO a `FitIdentifier`, so the type that refuses a malformed
# spelling must exist before the helpers that read a document can delegate to it.
SELECTION_RULE = "modelled-selection:1"

# The four top-level keys the form names, and the closed set the unnamed-key refusal is drawn over. The
# set is closed over the keys a CONFORMING DOCUMENT carries, which is a separate question from whether a
# key's CONTENTS are checked: refusing an UNNAMED key asks only whether the writer used a key the form
# names, so a named key whose own refusal lives in another helper of this module is admitted here. A set
# drawn instead over the keys some ONE helper reads would refuse every document the emitter wrote for a
# field that helper does not read.
_DOCUMENT_KEYS = ("rule", "selection", "dependence", "stamp")

# The stamp's three sections, and the three fields an identity carries. Both are closed sets in the
# document's form, so both are stated once here and read by the refusal that names them.
_STAMP_SECTIONS = ("inputs", "seeds", "identities")

_IDENTITY_FIELDS = ("system", "implementation", "code_version")

# The four fields a dependence entry carries. Closed for the same reason, and load-bearing beyond
# tidiness: because `theta` is not one of them, a fitted copula PARAMETER is unrepresentable in this
# document rather than merely discouraged.
_DEPENDENCE_FIELDS = ("event_class", "marginals", "structure", "copula_families")


def _require_named_keys(document: Mapping[str, object]) -> None:
    """Refuse a document carrying a top-level key the form does not name.

    The check ranges over the WHOLE document rather than over the keys some ONE helper reads, because an
    unnamed key is a field its writer believes is live while nothing does: a `goodness_of_fit` table
    beside `selection` would be written, maintained and read by nobody, and its author would never learn
    that the number they computed reached no consumer. What each NAMED key must contain is a different
    question and belongs to the helper that owns that key.
    """
    for key in document:
        if key not in _DOCUMENT_KEYS:
            raise SelectionArtifactError(
                f"selection document carries the top-level key {key!r}: a selection document is exactly "
                f"{', '.join(repr(name) for name in _DOCUMENT_KEYS)}, so a key this form does not name is "
                "a field its writer believes is live while nothing reads it; remove the key, or move its "
                "value under a key the form names"
            )


def _require_rule(document: Mapping[str, object]) -> str:
    """Return the document's version literal, refused if it is absent or is not `SELECTION_RULE`.

    Compared as a literal and not parsed, and refused rather than migrated, for one reason: the header's
    whole job is to make a later change to the document's FIELDS loud, so a header this code does not
    recognise names a document written against another form, and reading it optimistically is exactly
    the silent misread the header exists to prevent. Deliberately NOT a tolerant read that accepts any
    `modelled-selection:N` and warns on an unexpected one: a warning emitted from a library reaches no
    reader who acts on it, so it is a silent pass wearing a warning's clothes.

    Returns the constant rather than the document's own value. The two are equal at the only point that
    matters — after the check — and the document's value is typed `object`, so returning it would carry
    an unproven type into a field the artifact publishes.
    """
    rule = document.get("rule")
    if rule != SELECTION_RULE:
        raise UnsupportedSelectionVersionError(
            f"selection document's rule is {rule!r}: a selection document must declare rule = "
            f"{SELECTION_RULE!r}, the version header that makes a later change to the document's fields "
            "loud rather than silently misread by an older reader; an ABSENT rule is the emission "
            "surface's omission, while a DIFFERENT one means the document was written against another "
            "form and is regenerated rather than edited"
        )
    return SELECTION_RULE


def _require_selection(document: Mapping[str, object]) -> list[str]:
    """Return the document's `selection` entries, refused if the array is absent, empty, or not all text.

    Three refusals in one helper, because each is a property of the same array and no caller can act on
    one of them separately. ABSENCE is refused rather than defaulted to an empty array: a default would
    make a document with no selection indistinguishable from one that selects nothing, and the two need
    different repairs. EMPTINESS is refused on its own ground, because a load would otherwise certify
    that the run has a selection when it has none. And a non-text ENTRY is refused HERE rather than at
    the parse, because a TOML table entry is the one place on disk a goodness-of-fit value could live:
    an entry is a dotted spelling of one fit, and the document offering no table field for a metric is
    what makes a metric unrepresentable in it rather than merely discouraged.
    """
    declared = document.get("selection")
    if not isinstance(declared, list):
        raise SelectionArtifactError(
            f"selection document's 'selection' is {declared!r}: 'selection' must be an array of dotted "
            "fit-identifier spellings, because without that array there is no selection for the artifact "
            "to hold and no entry for a consumer to read"
        )
    if not declared:
        raise SelectionArtifactError(
            "selection document's 'selection' is empty: a document that selects nothing is not a "
            "selection, and loading it would certify that the run has one; name at least one fit, or do "
            "not emit the document"
        )
    for index, entry in enumerate(declared):
        if not isinstance(entry, str):
            raise SelectionArtifactError(
                f"selection document's 'selection' entry {index} is {entry!r}: every entry is a dotted "
                "spelling of ONE fit, and a table entry is where a goodness-of-fit value or a fitted "
                "parameter would live, which is what this form refuses to carry; write the entry as a "
                "string"
            )
    return declared


def _parse_selection(entries: Iterable[str]) -> tuple[FitIdentifier, ...]:
    """Return `entries` parsed into identifiers in the document's order, refusing a repeated identifier.

    The parse is DELEGATED to `FitIdentifier.parse` rather than restated, so a spelling this helper
    accepts and a spelling a consumer's own `FitIdentifier` accepts cannot drift apart; the refusal's
    message is `parse`'s, which already names the entry and the defect a reader recovers from.

    A repeat is refused rather than merged, and the message names BOTH readings because the two are one
    signal here. A duplicated paste is fixed by deleting one entry. Two legitimately different fits that
    share all four components cannot be separated by this grammar AT ALL — a hazard-scenario definition,
    an estimation method and a transformation's fitted parameter are none of them components of a
    marginal fit — so a reader who assumes the first cause and has the second edits the document and
    silently loses a fit. That is why the message is a halt with two readings rather than a hint.

    Takes the entries rather than the document, so it is exercisable without the document's other keys;
    the entries are the text values `_require_selection` returned, and a non-text value reaching here is
    a caller error rather than a document defect.
    """
    parsed: dict[FitIdentifier, None] = {}
    for entry in entries:
        identifier = FitIdentifier.parse(entry)
        if identifier in parsed:
            raise SelectionArtifactError(
                f"selection document names the fit identifier {str(identifier)!r} twice: a selection is "
                "one fit per identifier, so a repeat is EITHER a duplicated entry, fixed by deleting "
                "one, OR two legitimately different fits that share all four components — differing in a "
                "hazard-scenario definition, an estimation method or a transformation's fitted parameter "
                "— which this grammar cannot tell apart and cannot carry at all; delete the repeat if it "
                "is one, and if it is not, the two fits cannot both be recorded here"
            )
        parsed[identifier] = None
    return tuple(parsed)


@dataclass(frozen=True)
class DependenceSelection:
    """The joint model for ONE event class: its ordered marginals, its structure, and one family per edge.

    Four fields and no fifth. `event_class` and `structure` are each one charset-checked segment;
    `marginals` is an ORDERED tuple of at least two fit identifiers; `copula_families` is an ordered
    tuple of exactly d(d-1)/2 family names, where d is the marginal count — the edge count of any regular
    vine on d variables, which is the one arithmetic this type performs. A uniform selection is expressed
    by REPETITION rather than by a short tuple, so nothing here refuses a repeated family: a short tuple
    would be ambiguous between "one family for all edges" and "a prefix of the edges", and a duplicate
    check would make the encoding it is meant to express unwritable.

    Deliberately NOT an identifier: no rendered spelling and no parser. A joint fit is not a fit of one
    statistic, and giving this type a dotted spelling would invite the reserved-token encoding of a
    joint model inside the marginal `selection` array — a route the selection's own form refuses,
    because an entry there is a dotted spelling of exactly one marginal fit.

    WHY THE MARGINALS ARE ORDERED AND WHY THEY ARE VALUES. A vine's structure is not a set: which
    marginal comes first determines which pairs exist in the first tree, so an implicit order would make
    the model's meaning depend on the order of a list its emitter controls for other reasons. And they
    are `FitIdentifier` values rather than strings, so the parse that admitted them is the parse a
    consumer receives, and a reference to a fit the document does not hold cannot be represented here at
    all — the constructor that builds these checks membership against the document's own collection.

    Not established: which EDGE a position names. For d of 3 a C-vine and a D-vine enumerate the same
    three positions over DIFFERENT pairs, so the position-to-edge reading depends on the structure kind,
    which this module holds no vocabulary for; it is the emission surface's convention, applied by the
    consumer that reads both fields.
    """

    event_class: str
    marginals: tuple[FitIdentifier, ...]
    structure: str
    copula_families: tuple[str, ...]

    def __post_init__(self) -> None:
        """Refuse a dependence selection whose own fields cannot denote one joint model."""
        if not _is_segment(self.event_class):
            raise SelectionArtifactError(
                f"dependence entry's event_class {self.event_class!r} is not a valid name: it takes the "
                "same charset as a fit identifier's components, a non-empty run of letters, digits, '_' "
                "and '-', so it can carry no separator, no dot, no '=' and no space"
            )

        if not _is_segment(self.structure):
            raise SelectionArtifactError(
                f"dependence entry's structure {self.structure!r} is not a valid name: a structure is one "
                "charset-checked segment, a non-empty run of letters, digits, '_' and '-', so a dotted or "
                "matrix-shaped spelling is refused here rather than misread as a name"
            )

        for position, family in enumerate(self.copula_families):
            if not _is_segment(family):
                raise SelectionArtifactError(
                    f"dependence entry's copula_families position {position} is {family!r}, which is not a "
                    "valid name: every family is one charset-checked segment, so a value carrying '=', a "
                    "dot or a space cannot ride there"
                )

        if len(self.marginals) < 2:
            raise SelectionArtifactError(
                f"dependence entry's marginals holds {len(self.marginals)} identifier(s): a joint model "
                "needs at least two quantities, because a model over one quantity is not joint"
            )

        for position, marginal in enumerate(self.marginals):
            if not isinstance(marginal, FitIdentifier):
                raise SelectionArtifactError(
                    f"dependence entry's marginals position {position} is {marginal!r}, which is not a fit "
                    "identifier: a marginal is a VALUE the document's own parse produced, because a "
                    "consumer reads the identifier's components off it and a bare spelling would fail "
                    "there instead of here"
                )
            if marginal in self.marginals[:position]:
                raise SelectionArtifactError(
                    f"dependence entry's marginals names {str(marginal)!r} more than once: one quantity "
                    "cannot occupy two positions of one joint model, so the second entry is either a "
                    "paste error or an ordering that cannot be read"
                )

        required = len(self.marginals) * (len(self.marginals) - 1) // 2
        if len(self.copula_families) != required:
            raise SelectionArtifactError(
                f"dependence entry's copula_families holds {len(self.copula_families)} family name(s) for "
                f"{len(self.marginals)} marginals: a vine on d variables has exactly d(d-1)/2 edges, so "
                f"this entry must carry {required}. A single family does not stand for every edge — "
                "express a uniform selection by REPEATING it once per position"
            )


def _freeze(value: object) -> object:
    """Return `value` with every table frozen to a read-only proxy and every array frozen to a tuple.

    A top-level proxy is NOT a read-only guarantee, and that is why this exists rather than a single
    `MappingProxyType` at the call site: the proxy refuses a top-level assignment and admits both a write
    into a nested table and an append to an array, because it holds the parsed document's own objects
    rather than copies. Freezing at every depth is what makes the artifact's stamp read-only at the depth
    a consumer actually reads.

    Deliberately NOT walking anything but tables and arrays: no cycle handling, no arbitrary-object
    traversal, no copying of scalars. The document this runs on is TOML-parsed, where every array is a
    list and every table a dict, so those two cases are the whole of the input — a hand-built document
    carrying a tuple of tables is not re-walked, and that is a gap this function does not close.
    """
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


@dataclass(frozen=True, init=False)
class SelectionArtifact:
    """One validated selection document: its version, its marginal fits, its joint models and its stamp.

    THE DOCUMENT IS THE CONSTRUCTOR OPERAND, deliberately. A constructor taking `rule`, `fits`,
    `dependence` and `stamp` as separate fields cannot see a top-level key the form does not name, and
    cannot tell an ABSENT key from a WRONG one — so those refusals would be reachable only through
    `load_selection` while this type is re-exported on the public surface. Taking the document makes
    every refusal but the TOML-parse one a construction check.

    Deliberately NOT retaining the document as a field: a retained mapping keeps the caller's own object,
    so a caller that edits it afterwards would read a value its validated `rule` disagrees with. Not
    hashable, and that follows from the read-only guarantee rather than being chosen: the stamp's
    mappings are read-only proxies, and a proxy is unhashable, so an artifact is a value a consumer holds
    and passes rather than one it collects in a set or uses as a dictionary key.

    Every refusal runs here, in one act, in the order the form's table states except that the four
    document-contract helpers run first: an unnamed key, an absent or wrong `rule`, and a malformed
    `selection` are answered before anything downstream reads the values they would have fixed.

    Not established: that the stamp names every system the run exercised. `require_complete` needs the
    exercised set as an argument and a document constructor has no workflow graph to derive one from, so
    what is checked here is presence, shape and non-emptiness — strictly weaker than completeness.
    """

    rule: str
    fits: tuple[FitIdentifier, ...]
    dependence: tuple[DependenceSelection, ...]
    stamp: ProvenanceRecord

    def __init__(self, document: Mapping[str, object]) -> None:
        """Validate `document` in one act and freeze the four values this artifact carries."""
        _require_named_keys(document)
        rule = _require_rule(document)
        fits = _parse_selection(_require_selection(document))

        # The stamp's rows. Presence and shape first, then content, then the record itself — because a
        # record assembled from a table that failed either of the first two would carry values nothing
        # validated, and `capture` is the record's only assembler so there is no second place to check.
        stamp = document.get("stamp")
        if not isinstance(stamp, dict):
            raise UnstampedSelectionError(
                f"selection document's 'stamp' is {stamp!r}: the artifact is provenance-stamped or it is "
                "not this artifact, so a run reading it could not say which implementation produced the "
                "selection; the emission surface owes a [stamp] table"
            )
        for key in stamp:
            if key not in _STAMP_SECTIONS:
                raise UnstampedSelectionError(
                    f"selection document's 'stamp' carries the section {key!r}: a stamp is exactly "
                    f"{', '.join(repr(name) for name in _STAMP_SECTIONS)} and nothing else, so a section "
                    "this form does not name is a field its writer believes is live while nothing reads it"
                )
        for section in _STAMP_SECTIONS:
            if not isinstance(stamp.get(section), dict):
                raise UnstampedSelectionError(
                    f"selection document's 'stamp' section {section!r} is {stamp.get(section)!r}: a stamp "
                    f"carries {', '.join(repr(name) for name in _STAMP_SECTIONS)} as three tables, and an "
                    "absent or non-table section is the emission surface's omission"
                )

        raw_seeds = stamp["seeds"]
        for name, value in raw_seeds.items():
            if not isinstance(value, int) or isinstance(value, bool):
                raise SelectionArtifactError(
                    f"selection document's 'stamp' seeds value for {name!r} is {value!r}: a seed is the "
                    "INTEGER that seeds the run's generator, which is the value a rerun reproduces, so a "
                    "string, a float or a boolean names no entropy a later run could replay"
                )

        raw_identities = stamp["identities"]
        identities: dict[str, ImplementationIdentity] = {}
        for system, entry in raw_identities.items():
            if not isinstance(entry, dict) or set(entry) != set(_IDENTITY_FIELDS):
                raise SelectionArtifactError(
                    f"selection document's 'stamp' identities entry {system!r} is {entry!r}: an identity "
                    f"is a table of exactly {', '.join(repr(name) for name in _IDENTITY_FIELDS)}, each "
                    "text, because a missing or extra field is an identity nothing can compare"
                )
            for field_name in _IDENTITY_FIELDS:
                if not isinstance(entry[field_name], str):
                    raise SelectionArtifactError(
                        f"selection document's 'stamp' identities entry {system!r} carries the "
                        f"non-text {field_name} {entry[field_name]!r}: every identity field is text"
                    )
            if entry["system"] != system:
                raise SelectionArtifactError(
                    f"selection document's 'stamp' identities entry is keyed {system!r} but declares "
                    f"system = {entry['system']!r}: the record is keyed by system name and the identity "
                    "carries its own name, so the two are one fact stated twice and a disagreement means "
                    "one of them is wrong"
                )
            identities[system] = ImplementationIdentity(
                system=entry["system"],
                implementation=entry["implementation"],
                code_version=entry["code_version"],
            )

        if not raw_seeds or not identities:
            raise UnstampedSelectionError(
                f"selection document's 'stamp' names no "
                f"{'seeds and no' if not raw_seeds and not identities else ('seeds' if not raw_seeds else 'identities')}"
                ": a stamp with three shape-valid but empty tables names no producer and no entropy, so "
                "the document would be provenance-stamped in form and unaccountable in content"
            )

        # The dependence block's rows. Presence and shape, then each entry's keys, then the reference
        # check against the collection parsed above, then per-event-class uniqueness, and last the
        # construction whose own `__post_init__` owns the charset and arity refusals.
        declared = document.get("dependence")
        if not isinstance(declared, list) or not declared:
            raise UndescribedDependenceError(
                f"selection document's 'dependence' is {declared!r}: the artifact holds the multivariate "
                "selection as a non-empty array of tables, one per event class, and an absent, empty or "
                "non-array block is the emission surface's omission rather than an absent joint model"
            )
        dependence: list[DependenceSelection] = []
        for index, entry in enumerate(declared):
            if not isinstance(entry, dict):
                raise UndescribedDependenceError(
                    f"selection document's 'dependence' entry {index} is {entry!r}: every entry is a table "
                    f"of exactly {', '.join(repr(name) for name in _DEPENDENCE_FIELDS)}"
                )
            for key in entry:
                if key not in _DEPENDENCE_FIELDS:
                    raise UndescribedDependenceError(
                        f"selection document's 'dependence' entry {index} carries the key {key!r}: an "
                        f"entry is exactly {', '.join(repr(name) for name in _DEPENDENCE_FIELDS)}, and a "
                        "fifth key is where a fitted copula parameter would live — which is exactly what "
                        "this form refuses to carry"
                    )
            for field_name in _DEPENDENCE_FIELDS:
                if field_name not in entry:
                    raise UndescribedDependenceError(
                        f"selection document's 'dependence' entry {index} omits {field_name!r}: an entry is "
                        f"exactly {', '.join(repr(name) for name in _DEPENDENCE_FIELDS)}, and the omitted "
                        "field is a bound the entry exists to carry"
                    )

            event_class = entry["event_class"]
            raw_marginals = entry["marginals"]
            raw_families = entry["copula_families"]
            if not isinstance(raw_marginals, list):
                raise SelectionArtifactError(
                    f"selection document's 'dependence' entry {index} marginals is {raw_marginals!r}: "
                    "marginals is an ARRAY of identified fits, in the order the joint model reads them"
                )
            if not isinstance(raw_families, list):
                raise SelectionArtifactError(
                    f"selection document's 'dependence' entry {index} copula_families is {raw_families!r}: "
                    "copula_families is an ARRAY with one family name per edge position"
                )

            marginals: list[FitIdentifier] = []
            for position, spelling in enumerate(raw_marginals):
                # Text is THIS layer's question, exactly as it is for a `selection` entry, and the check
                # runs here rather than inside `parse` for the same reason that entry's is checked before
                # its own parse: a spelling-grammar function is handed text by contract, so a non-text
                # value reaching it is a document defect. Without this the value would reach `parse`,
                # whose first statement splits it, and an AttributeError would escape the module's
                # hierarchy from the public read interface.
                if not isinstance(spelling, str):
                    raise SelectionArtifactError(
                        f"selection document's 'dependence' entry {index} marginals position {position} "
                        f"is {spelling!r}, which is not a fit-identifier spelling: a marginal is a dotted "
                        "spelling of one fit, and a table entry is where a goodness-of-fit value or a "
                        "fitted parameter would live — which this form refuses to carry; write the marginal "
                        "as a string"
                    )
                try:
                    marginal = FitIdentifier.parse(spelling)
                except SelectionArtifactError as error:
                    raise SelectionArtifactError(
                        f"selection document's 'dependence' entry {index} marginals position {position} "
                        f"is {spelling!r}, which is not a fit identifier: {error}"
                    ) from error
                if marginal not in fits or marginal.event_class != event_class:
                    raise SelectionArtifactError(
                        f"selection document's 'dependence' entry {index} names the marginal "
                        f"{str(marginal)!r}, which this document's 'selection' does not hold for event "
                        f"class {event_class!r}: a reference to a fit the document does not hold is a "
                        "dangling name, and one held under ANOTHER event class would make the joint model "
                        "span populations"
                    )
                marginals.append(marginal)

            if any(existing.event_class == event_class for existing in dependence):
                raise SelectionArtifactError(
                    f"selection document's 'dependence' names event class {event_class!r} more than once: "
                    "the artifact holds THE joint model for an event class, and alternative structures "
                    "compared in one run are not representable — so a second entry is either a paste "
                    "error or a comparison this form cannot record"
                )

            dependence.append(
                DependenceSelection(
                    event_class=event_class,
                    marginals=tuple(marginals),
                    structure=entry["structure"],
                    copula_families=tuple(raw_families),
                )
            )

        stamp_record = capture(
            inputs=_freeze(stamp["inputs"]),
            seeds=_freeze(raw_seeds),
            identities=MappingProxyType(identities),
        )

        object.__setattr__(self, "rule", rule)
        object.__setattr__(self, "fits", fits)
        object.__setattr__(self, "dependence", tuple(dependence))
        object.__setattr__(self, "stamp", stamp_record)


def load_selection(document_path: str | Path) -> SelectionArtifact:
    """Read the selection document at `document_path` and return it validated.

    The argument is REQUIRED and there is no default, for the reason `load_bindings` gives about its own
    path: a default would be a toolkit-side well-known location, so an unconfigured run would read
    whatever ships in the wheel instead of failing loudly.

    A document that is not valid TOML is refused HERE, because the object exists only once parsing has
    succeeded. Every other refusal is `SelectionArtifact`'s own and is re-raised with this path attached,
    under the SAME class, so a caller that catches `UnstampedSelectionError` still catches it after
    loading. Deliberately NOT collapsing the leaves into the root the way `load_bindings` does: the four
    classes are separated precisely because their recoveries differ, and a loader that flattened them
    would undo that at the one boundary every consumer crosses.

    Deliberately NOT wrapped: an unreadable path. `Path.read_text` raises the OS error, which already
    names the file to fix, and this document's refusal set has no row for it.
    """
    path = Path(document_path)
    try:
        document = tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as error:
        raise SelectionArtifactError(
            f"selection document {path} is not valid TOML, so there is nothing to read: {error}"
        ) from error

    try:
        return SelectionArtifact(document)
    except SelectionArtifactError as error:
        raise type(error)(f"selection document {path}: {error}") from error
