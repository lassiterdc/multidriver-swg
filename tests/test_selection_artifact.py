"""Check the selection artifact's identifier grammar: the four components, the canonical render, and every refusal the parse owns.

A fit identifier is the name a run's marginal selection is stored under, so the grammar is what a
metric cannot ride in and what a later reader recovers an identifier from. Each check here is written
against a form of the defect it forbids — a fifth component appended as its own segment, a metric's
surface spellings inside a component, a chain written as a bare string, a repeated step — because a
check that cannot fail for the reason it claims constrains nothing.

The second half of this module checks the DOCUMENT's contract — the version literal, the four named
top-level keys, and the `selection` array's own refusals — through the four helpers directly, because the
helpers are exercisable on their own, and a helper reached only through the artifact would be tested
through a surface whose own refusals can mask which of the two fired.

Invocation isolation. The identifier tests are NEW CAPABILITY over a module the package did not carry
before it landed, and the document-contract tests are NEW CAPABILITY over names that module did not carry
before this change; in neither case does the pre-change tree hold the callable whose behaviour a test
reds on, so the tree-substitution arm is unreachable and the discrimination arm applies. Each test's
`Killed by:` names a change to what the code DOES on that test's input, and that is the discrimination
this suite is run forward on.

The names are imported from `multidriver_swg.run_configuration.selection_artifact` rather than through
the package: the package's re-export is a separate surface with its own test, and a module that imported
through the package would be asserting that surface here as well.
"""

from __future__ import annotations

import ast
import dataclasses
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pytest

import multidriver_swg.run_configuration as run_configuration
import multidriver_swg.run_configuration.selection_artifact as selection_artifact
from multidriver_swg.run_configuration.provenance import ImplementationIdentity, ProvenanceRecord
from multidriver_swg.run_configuration.selection_artifact import (
    IDENTIFIER_COMPONENTS,
    SELECTION_RULE,
    DependenceSelection,
    FitIdentifier,
    SelectionArtifact,
    SelectionArtifactError,
    UndescribedDependenceError,
    UnstampedSelectionError,
    UnsupportedSelectionVersionError,
    load_selection,
)

# The two statements the module is the deliverable for, quoted from the ruling rather than paraphrased:
# the checks below assert these exact sentences appear in the module's docstring.
SELECTION_IS_ONE_DOCUMENT = (
    "The per-run human selection is held as a single versioned, provenance-stamped document whose "
    "identifiers follow parent `ADR-n`'s grammar, and systems 4, 5, 6, 8 and 10 consume it from that "
    "one document."
)
NO_GOODNESS_OF_FIT = (
    "a fit identifier is a function of (event class, statistic, distribution family, transformation "
    "chain) and MUST NOT embed a goodness-of-fit value"
)


def _identifier(**overrides: Any) -> FitIdentifier:
    """One well-formed identifier, so a test states only the component it is proving something about."""
    fields: dict[str, Any] = {
        "event_class": "compound",
        "statistic": "rainfall_depth",
        "distribution_family": "gamma",
        "transformation_chain": ("identity",),
    }
    fields.update(overrides)
    return FitIdentifier(**fields)


def _document(**overrides: Any) -> dict[str, Any]:
    """One well-formed document, so a test states only the key it is proving something about.

    All FOUR top-level keys are present: a fixture carrying only the keys the helper under test reads
    would prove nothing about whether a conforming document that also carries the other two is admitted.
    """
    document: dict[str, Any] = {
        "rule": "modelled-selection:1",
        "selection": ["compound.rainfall_depth.gamma.identity"],
        "dependence": [],
        "stamp": {},
    }
    document.update(overrides)
    return document


def test_the_identifier_carries_exactly_the_four_grammar_components():
    """Property: `FitIdentifier` has exactly `event_class`, `statistic`, `distribution_family` and `transformation_chain`, in that order, equal to `IDENTIFIER_COMPONENTS`; it constructs with all four; and assigning to a field raises `FrozenInstanceError`.

    Class: NEW CAPABILITY. The plausible wrong implementation is an identifier carrying the metric it
    was accepted on — a fifth `goodness_of_fit` field, or a `dict`-shaped value with no fixed field set
    at all — which is exactly what the ruling's hard constraint forbids: a re-fit changes the metric, so
    every stored identifier stops matching and nothing says why. The check is red under it at the
    field-name arm.
    Reachability: the input class is any construction of an identifier, and the admit arm below
    constructs one with all four components and reads each back, so the frozen arm is shown to be
    reading a type that constructs rather than one nothing can build.
    Both-states anchor: the assertions turn on `dataclasses.fields` and on whether assignment raises,
    both properties of the declaration rather than wording this test introduces.
    Killed by: adding a field, renaming one, dropping one, reordering them, or unfreezing the class.
    Second correct implementation: a frozen dataclass declared with `slots=True`, or one built through
    `dataclasses.make_dataclass(..., frozen=True)`, in the same field order — both expose the same
    `dataclasses.fields` names and both raise `FrozenInstanceError`. A `NamedTuple` is not one: it
    passes the fields arm and raises `AttributeError` on assignment, and the frozen arm is kept for
    precisely that discrimination.
    """

    assert IDENTIFIER_COMPONENTS, "the component tuple is empty, so the field-name arm compares nothing"
    assert tuple(field.name for field in dataclasses.fields(FitIdentifier)) == IDENTIFIER_COMPONENTS
    assert IDENTIFIER_COMPONENTS == (
        "event_class",
        "statistic",
        "distribution_family",
        "transformation_chain",
    ), IDENTIFIER_COMPONENTS

    identifier = _identifier()
    assert identifier.event_class == "compound"
    assert identifier.statistic == "rainfall_depth"
    assert identifier.distribution_family == "gamma"
    assert identifier.transformation_chain == ("identity",)

    with pytest.raises(dataclasses.FrozenInstanceError):
        identifier.statistic = "rainfall_intensity"


@pytest.mark.parametrize(
    ("chain", "spelling"),
    [
        (("identity",), "compound.rainfall_depth.gamma.identity"),
        (("log", "identity"), "compound.rainfall_depth.gamma.log+identity"),
        (("identity", "log", "identity"), "compound.rainfall_depth.gamma.identity+log+identity"),
    ],
)
def test_the_canonical_render_round_trips_through_the_parse(chain: tuple[str, ...], spelling: str):
    """Property: `str` renders `event_class.statistic.distribution_family.step+step`, `parse` reads that spelling back to an equal identifier, and the render of a parsed identifier is the spelling it came from.

    Class: NEW CAPABILITY. The plausible wrong implementation is a render that joins the chain with the
    same separator as the components, so a two-step chain renders five dotted segments and the parse
    refuses the identifier the render just produced; or one that drops the chain, so every chain
    collapses to one name. The check is red under the first at the exact-spelling arm and under the
    second at both the spelling and the round-trip arm.
    Reachability: the input class is any identifier, and three members are exercised — a single-step
    chain, a two-step chain, and a three-step chain whose repeated step is NOT adjacent — so the render
    is shown to join, to separate, and to leave a non-adjacent repeat alone.
    Both-states anchor: the round-trip assertion turns on equality of the identifier, a property
    independent of how the render is written, not on wording this test introduces.
    Killed by: joining the chain with `.`; dropping the chain from the render; rendering the components
    in a different order; or refusing every repeated step rather than only an adjacent one.
    Second correct implementation: `f"{a}.{b}.{c}.{'+'.join(chain)}"` and `".".join((...))` produce the
    same text, and a parse that splits the fourth segment with `str.split` or with `re.split` produces
    the same tuple — the test asserts the rendered text and the parsed value, never the expression.
    """

    identifier = _identifier(transformation_chain=chain)

    assert str(identifier) == spelling
    assert FitIdentifier.parse(spelling) == identifier
    assert str(FitIdentifier.parse(spelling)) == spelling


@pytest.mark.parametrize(
    ("entry", "segments"),
    [
        ("gamma.identity", 2),
        ("rainfall_depth.gamma.identity", 3),
        ("compound.rainfall_depth.gamma.identity.extra", 5),
        ("compound.rainfall_depth.gamma.identity.extra.more", 6),
    ],
)
def test_a_spelling_that_is_not_four_dotted_segments_is_refused(entry: str, segments: int):
    """Property: `FitIdentifier.parse` raises `SelectionArtifactError` naming the entry and the segment count, for every spelling that does not split into exactly four dotted segments.

    Class: NEW CAPABILITY. The plausible wrong implementation is a parser accepting any segment count,
    so a FIFTH component — the form a goodness-of-fit value takes when it is appended as its own
    segment — is admitted and silently becomes part of the identifier. The check is red under it at the
    count arm.
    Reachability: the input class is any spelling that is not four segments, and four members of it are
    exercised — two, three, five and six segments — of which the five-segment member is the
    metric-as-fifth-component case. The admit arm is the round-trip test's three spellings, which show a
    four-segment spelling parses.
    Both-states anchor: the assertions turn on whether the refusal fires and on the count in its
    message, both consequences of the grammar rather than wording this test introduces.
    Killed by: accepting a variable segment count; splitting on the wrong separator; or dropping the
    count from the message, which is the one fact a reader recovers from.
    Second correct implementation: a parser comparing the length of the `split` result, or one
    normalizing to a tuple first and checking its arity — the test asserts the raised class, the entry
    and the count, never the wording around them.
    """

    with pytest.raises(SelectionArtifactError) as refused:
        FitIdentifier.parse(entry)

    message = str(refused.value)
    assert entry in message, message
    assert str(segments) in message, message


@pytest.mark.parametrize(
    "overrides",
    [
        {"event_class": "com/pound"},
        {"statistic": "rain fall"},
        {"distribution_family": "gamma(ks=0.03)"},
        {"distribution_family": "gamma.identity"},
        {"transformation_chain": ("ks=0.03",)},
        {"transformation_chain": ("identity.x",)},
    ],
)
def test_a_component_outside_the_identifier_charset_is_refused(overrides: dict[str, Any]):
    """Property: a component or a chain step outside the identifier charset raises `SelectionArtifactError` naming the offending value, and a conforming value at every one of those positions is admitted.

    Class: NEW CAPABILITY. The plausible wrong implementation is a parse that checks the component
    COUNT alone, which admits every spelling a metric's value takes on the way in —
    `family(ks=0.03)` — and admits a dot inside a component, which would re-partition the identifier
    into more segments than its author wrote. The check is red under it at the charset arm.
    Reachability: the input class is any component value at any position, and six members are
    exercised — the three single segments, a dot inside a component, and a `=` and a dot inside a chain
    step — with the control below admitting a conforming value at every one of those positions, so the
    refusal is attributable to the offending value and not to a type that refuses everything.
    Both-states anchor: the assertion turns on whether the refusal fires for a value the control admits
    at the same position, a property of the value, not on wording this test introduces.
    Killed by: dropping the per-component and per-step charset check; widening the pattern to admit a
    dot or `=`; or refusing only the `event_class` position.
    Second correct implementation: one predicate applied to all four positions, or four separate checks
    with the same pattern — the test asserts the raised class and the offending value in the message,
    never where the check lives.
    """

    # The control: a conforming value at every position these members occupy.
    _identifier()

    with pytest.raises(SelectionArtifactError) as refused:
        _identifier(**overrides)

    offending = next(iter(overrides.values()))
    if isinstance(offending, tuple):
        offending = offending[0]
    assert repr(offending) in str(refused.value), str(refused.value)


@pytest.mark.parametrize("chain", ["identity", ["identity"]])
def test_a_chain_that_is_not_a_tuple_is_refused(chain: Any):
    """Property: a `transformation_chain` given as anything but a tuple raises `SelectionArtifactError`, and the same steps given as a tuple are admitted.

    Class: NEW CAPABILITY. The plausible wrong implementation is a chain accepted as whatever iterable
    arrives, which fails silently in two directions: a string iterates its characters, so `'identity'`
    becomes eight one-letter steps and renders `i+d+e+n+t+i+t+y` — a legal identifier for a fit nobody
    selected; and a list leaves the frozen identifier unhashable, so the membership checks a consumer
    runs raise `TypeError` long after the identifier was admitted.
    Reachability: the input class is any chain value, and two members are exercised — a string and a
    list carrying the same single step — with the tuple form of that same step admitted by the arm
    below, so the refusal is attributable to the container kind and not to the step name.
    Both-states anchor: the assertion turns on the container kind of the value, which the admit arm
    holds fixed while the steps stay equal, not on wording this test introduces.
    Killed by: dropping the container guard; coercing with `tuple(chain)`, which makes the string case
    render eight steps; or accepting any `Sequence`.
    Second correct implementation: `not isinstance(chain, tuple)` or `isinstance(chain, str | list)` —
    the first also refuses a generator, the second admits any other iterable, and the test asserts
    nothing about those, so both pass.
    """

    with pytest.raises(SelectionArtifactError) as refused:
        _identifier(transformation_chain=chain)

    assert repr(chain) in str(refused.value), str(refused.value)

    admitted = _identifier(transformation_chain=("identity",))
    assert admitted.transformation_chain == ("identity",)


def test_a_chain_must_carry_one_step_and_may_not_repeat_an_adjacent_one():
    """Property: an empty chain, an empty step and an immediately repeated step each raise `SelectionArtifactError`, while a single step, two different steps and a NON-adjacent repeat are admitted.

    Class: NEW CAPABILITY. The plausible wrong implementation is a chain check that validates the steps
    and stops there, so `()` renders the identifier with nothing after its third separator and the
    parse reads it back as an empty step — the round-trip silently lost; the mirror-image wrong
    implementation refuses every repeated step, which makes `identity+log+identity` unrepresentable and
    is caught by the third admit arm below. The check is red under either.
    Reachability: the input class is any chain of legal step names, and six members are exercised — an
    empty chain, an empty step, an adjacent repeat, a single step, two different steps, and a
    non-adjacent repeat — so the refusals are shown to discriminate rather than to refuse chains.
    Both-states anchor: the assertions turn on whether the refusal fires for chains whose steps are all
    legal names, so what separates the refused members from the admitted ones is the ARRANGEMENT, not
    the vocabulary this test supplies.
    Killed by: dropping the empty-chain guard; dropping the empty-step guard; dropping the adjacency
    guard; or widening the repeat guard to every position, which the third admit arm reds.
    Second correct implementation: an adjacency check by index, by `zip` over offset slices, or by
    `itertools.pairwise` — all read the same arrangement, and the test asserts only the refusal and the
    repeated step's name in its message.
    """

    for chain in ((), ("identity", "")):
        with pytest.raises(SelectionArtifactError):
            _identifier(transformation_chain=chain)

    with pytest.raises(SelectionArtifactError) as repeated:
        _identifier(transformation_chain=("identity", "identity"))
    assert "'identity'" in str(repeated.value), str(repeated.value)

    assert _identifier(transformation_chain=("identity",)).transformation_chain == ("identity",)
    assert _identifier(transformation_chain=("log", "identity")).transformation_chain == ("log", "identity")
    non_adjacent = _identifier(transformation_chain=("identity", "log", "identity"))
    assert non_adjacent.transformation_chain == ("identity", "log", "identity")


def test_each_leaf_derives_from_the_refusal_root_and_the_root_is_not_a_key_error():
    """Property: each of the three leaf refusals derives from `SelectionArtifactError` and is not the root itself, and the root derives from `Exception` but not from `KeyError`.

    Class: NEW CAPABILITY. The plausible wrong implementation is a leaf deriving from `Exception`
    directly, so a caller catching the module's root misses one refusal and a run proceeds on a
    document nothing validated; the other is a root deriving from `KeyError`, whose `__str__` renders
    the message inside repr quotes and so delivers the reader's whole recovery path as a quoted
    literal. The check is red under either.
    Reachability: the input class is the module's four refusal classes, and all four are exercised, so
    the assertion is shown to be reading a hierarchy that exists rather than an empty collection.
    Both-states anchor: the assertions turn on the subclass relation, a property of the import graph,
    not on wording this test introduces.
    Killed by: deriving a leaf from `Exception` or from `KeyError` directly; deriving the root from
    `KeyError`; or removing a leaf.
    Second correct implementation: an intermediate private base class between the root and the leaves —
    `issubclass` is transitive, so a leaf under an intermediate base still derives from the root.
    """

    leaves = (
        UnsupportedSelectionVersionError,
        UnstampedSelectionError,
        UndescribedDependenceError,
    )
    assert leaves, "the leaf tuple is empty, so the subclass arms compare nothing"

    for leaf in leaves:
        assert issubclass(leaf, SelectionArtifactError), leaf
        assert leaf is not SelectionArtifactError, leaf

    assert issubclass(SelectionArtifactError, Exception)
    assert not issubclass(SelectionArtifactError, KeyError)


def test_the_module_docstring_states_the_two_rulings_verbatim():
    """Property: the module docstring CONTAINS the selection ruling and the no-goodness-of-fit constraint, transcribed character-for-character by this test module, and names the identifier type and the refusal root.

    Class: NEW CAPABILITY. The plausible wrong implementation is a docstring that describes the four
    components and stops there, dropping the two sentences the module is the deliverable for — so the
    next editor reads a module with no stated prohibition on embedding a metric in an identifier, and
    re-derives or re-introduces the defect the ruling forbids. The check is red under it.
    Reachability: the input class is any version of the docstring, and the whole of it is searched, so
    every version is reached.
    Both-states anchor: NONE independent of the wording, and the field says so rather than claiming one.
    The property here IS the text: these two sentences are deliverable text the module must carry, and
    the constants above are transcriptions of sentences this test does not author, so a paraphrase
    applied to BOTH the constant and the docstring would pass. What carries verbatim-ness is a byte
    comparison of each constant against its sentence at the source, and no test inside this repository
    can reach that source; the check here establishes CONTAINMENT and nothing stronger, and the two are
    read together.
    Killed by: deleting or rephrasing either sentence in the docstring, or ceasing to name
    `FitIdentifier` or `SelectionArtifactError` in it.
    Second correct implementation: the sentences at any position, in either order, inside any
    surrounding prose — the test asserts containment, so it constrains neither order nor the wording
    around them.
    Not established: that the constants equal their sources. That comparison reaches text outside this
    repository, so the strongest claim this test supports is that the docstring carries the constants'
    bytes.
    """

    docstring = selection_artifact.__doc__ or ""

    assert SELECTION_IS_ONE_DOCUMENT in docstring
    assert NO_GOODNESS_OF_FIT in docstring
    assert "FitIdentifier" in docstring
    assert "SelectionArtifactError" in docstring


def test_the_document_rule_is_a_fixed_literal_compared_exactly():
    """Property: `SELECTION_RULE` is the literal `"modelled-selection:1"`; `_require_rule` returns it for a document that declares it, and raises `UnsupportedSelectionVersionError` for a document whose `rule` is absent, names another version, or is not text.

    Class: NEW CAPABILITY. The plausible wrong implementation is a tolerant read that accepts any
    `modelled-selection:N` and warns on an unexpected one, or one that treats an ABSENT header as "the
    only version there is". Both admit a document written against a different form, and making that loud
    is the header's whole job: a document whose `selection` is well-formed under one form but whose other
    fields changed shape under another then loads without a word, and every consumer reads fields it was
    never written to read. The check is red under either at the version arm.
    Reachability: the input class is any document mapping, and six members are exercised — the matching
    literal admitted, and five refused: an absent `rule`, three other version numbers ONE OF WHICH HAS
    THE LITERAL AS ITS PREFIX, a bare prefix with no version, and a non-text value.
    Both-states anchor: the assertions turn on whether the exception is raised and on the value returned
    for the admitted case, both consequences of the literal comparison rather than wording this test
    introduces.
    Killed by: the prefix mutation `rule.startswith(SELECTION_RULE)` on a string-typed `rule`, which
    admits the `modelled-selection:10` member — measured, that member parses to `modelled-selection:1`
    under the prefix comparison and raises under the shipped one, and without it the prefix mutation is
    GREEN on every other member; accepting an absent `rule`; or bumping the literal without changing the
    admitted call.
    Second correct implementation: `rule != SELECTION_RULE`, or a type check followed by `str(rule) !=
    SELECTION_RULE` — the test asserts the raised class and the returned literal, never the expression.
    """

    assert SELECTION_RULE == "modelled-selection:1", SELECTION_RULE
    assert selection_artifact._require_rule({"rule": SELECTION_RULE}) == "modelled-selection:1"

    for rule in (
        None,
        "modelled-selection:0",
        "modelled-selection:2",
        "modelled-selection:10",
        "modelled-selection",
        1,
    ):
        document = {} if rule is None else {"rule": rule}
        with pytest.raises(UnsupportedSelectionVersionError):
            selection_artifact._require_rule(document)


def test_a_top_level_key_the_form_does_not_name_is_refused():
    """Property: `_require_named_keys` admits a document carrying exactly `rule`, `selection`, `dependence` and `stamp` — including the two whose own refusals live in other helpers — and raises `SelectionArtifactError` for one carrying any other top-level key.

    Class: NEW CAPABILITY. The plausible wrong implementation is a check drawn over only the keys some
    ONE helper reads (`rule` and `selection`), which refuses a conforming document the emitter wrote for a
    field another helper reads: a check that is right about the fields it knows
    and wrong about the document. The mirror-image wrong implementation checks nothing, so a
    `goodness_of_fit` table beside `selection` is written, maintained and read by nobody. The check is red
    under either.
    Reachability: the input class is any mapping, and five members are exercised — the conforming
    four-key document, a document whose only keys are the two read today, two plausible misspellings, and
    a mixed-case key.
    Both-states anchor: the refused documents differ from the admitted one by exactly ONE ADDED KEY with
    the same value under it, so what separates them is the key and not what it carries.
    Killed by: narrowing the closed set to the keys read today; admitting any key; or comparing keys
    case-insensitively, which the mixed-case member reds.
    Second correct implementation: a set difference (`set(document) - set(_DOCUMENT_KEYS)`) or a
    comprehension over the document — the test asserts the raised class and the admitted call's `None`
    return, never the expression.
    """

    assert selection_artifact._require_named_keys(_document()) is None
    assert selection_artifact._require_named_keys({"rule": SELECTION_RULE, "selection": ["a.b.c.identity"]}) is None

    for key in ("goodness_of_fit", "selection_rule", "Selection", "dependences"):
        with pytest.raises(SelectionArtifactError) as refused:
            selection_artifact._require_named_keys(_document(**{key: {}}))
        assert key in str(refused.value), str(refused.value)


def test_the_selection_array_must_be_present_and_non_empty():
    """Property: `_require_selection` returns the document's entries, and refuses an absent `selection`, one that is not an array, and an EMPTY one, with the absent case and the empty case reporting DIFFERENT defects.

    Class: NEW CAPABILITY. The plausible wrong implementation defaults an absent `selection` to an empty
    array, which makes the two document defects indistinguishable and leaves the emptiness refusal
    unreachable — a default that turns two repairs into one report. The mirror-image wrong
    implementation tests only emptiness, which admits a bare string: `"compound.x.gamma.identity"` is a
    sequence of characters, so a later parse would walk its letters. The check is red under either.
    Reachability: the input class is any value under `selection`, and six members are exercised — a
    one-entry array, a two-entry array, an absent key, a bare string, an empty array, and a tuple.
    Both-states anchor: the assertions turn on whether the refusal fires and on the absent case and the
    empty case carrying different messages, so the two are shown to be two defects rather than one
    reached twice.
    Killed by: defaulting an absent array; dropping the emptiness refusal; testing `Sequence`, which
    admits the bare string and the tuple; or testing only the first entry.
    Second correct implementation: `not isinstance(declared, list)`, or `type(declared) is not list` —
    the test asserts the raised classes, the admitted entries' equality, and the two messages'
    inequality, never the expression.
    """

    assert selection_artifact._require_selection(_document()) == ["compound.rainfall_depth.gamma.identity"]
    two = ["compound.rainfall_depth.gamma.identity", "compound.rainfall_intensity.gev.log+identity"]
    assert selection_artifact._require_selection(_document(selection=two)) == two

    with pytest.raises(SelectionArtifactError) as absent:
        selection_artifact._require_selection({"rule": SELECTION_RULE})
    with pytest.raises(SelectionArtifactError):
        selection_artifact._require_selection(_document(selection="compound.rainfall_depth.gamma.identity"))
    with pytest.raises(SelectionArtifactError) as empty:
        selection_artifact._require_selection(_document(selection=[]))
    with pytest.raises(SelectionArtifactError):
        selection_artifact._require_selection(_document(selection=("compound.rainfall_depth.gamma.identity",)))

    assert str(absent.value) != str(empty.value), (str(absent.value), str(empty.value))


def test_a_selection_entry_that_is_not_text_is_refused():
    """Property: a `selection` entry that is not a string — a TOML table above all — raises `SelectionArtifactError` naming the offending value, and the same spelling given as a string is admitted.

    Class: NEW CAPABILITY. The plausible wrong implementation validates only the array's shape and hands
    the entries on, so an entry written as a table is admitted. That is the one place on disk a
    goodness-of-fit value could live: `{spelling = "...", gof = 0.03}` is a document carrying the metric
    the ruling forbids, formatted as data the form never refuses. The check is red under it.
    Reachability: the input class is any entry value, and four members are exercised — a table, an
    integer, a null, and a list of one string — with the same spelling admitted as a bare string in the
    arm above, so the refusal is attributable to the entry's type and not to its content.
    Both-states anchor: the assertions turn on the entry's TYPE, with the identical spelling on both
    sides of the boundary.
    Killed by: checking only the array's type and length; coercing an entry with `str(entry)`, which
    admits the table by rendering it; or checking text for the first entry only.
    Second correct implementation: an `isinstance(entry, str)` test per entry inside the loop, or a
    single `all(isinstance(entry, str) for entry in declared)` before the return — the test asserts the
    raised class and the offending value in the message, never the loop's shape.
    """

    admitted = ["compound.rainfall_depth.gamma.identity"]
    assert selection_artifact._require_selection(_document(selection=admitted)) == admitted

    for entry in ({"spelling": "compound.rainfall_depth.gamma.identity", "gof": 0.03}, 7, None, ["a.b.c.identity"]):
        with pytest.raises(SelectionArtifactError) as refused:
            selection_artifact._require_selection(_document(selection=[entry]))
        assert repr(entry) in str(refused.value), str(refused.value)


@pytest.mark.parametrize(
    ("entry", "named"),
    [
        ("gamma.identity", "gamma.identity"),
        ("compound.rainfall_depth.identity", "compound.rainfall_depth.identity"),
        ("compound.rainfall_depth.gamma.identity.extra", "compound.rainfall_depth.gamma.identity.extra"),
        ("compound.rainfall_depth.gamma(indian).identity", "gamma(indian)"),
    ],
)
def test_an_entry_that_does_not_parse_is_refused_naming_the_entry_and_the_defect(entry: str, named: str):
    """Property: `_parse_selection` raises `SelectionArtifactError` carrying the entry's own text — or, where the grammar refuses a component, that component — and returns identifiers for the entries it admits.

    Class: NEW CAPABILITY. The plausible wrong implementation catches the parse refusal and re-raises a
    generic message, which drops the entry's spelling: the one fact a reader acts on, because the
    recovery is a one-line rewrite of that entry. The other wrong implementation restates the grammar
    here instead of delegating, so the spelling this helper accepts and the spelling a consumer's
    `FitIdentifier` accepts drift apart. The check is red under both at the naming arm.
    Reachability: the input class is any entry spelling, and four members are exercised — three segment
    counts the grammar refuses and one spelling whose COMPONENT the charset refuses — with a conforming
    spelling admitted in the arm below.
    Both-states anchor: the assertions turn on the entry's own bytes appearing in the refusal, which is a
    property of the message rather than of the wording around it.
    Killed by: re-raising a generic message with the entry dropped; admitting a three-segment spelling;
    or restating the charset loosely enough to admit `(`.
    Second correct implementation: wrapping the refusal as `raise SelectionArtifactError(f"{entry}:
    {error}") from error`, or letting `parse`'s own message through unchanged — the test asserts that the
    entry appears, not that any particular wrapper is used.
    """

    assert selection_artifact._parse_selection(["compound.rainfall_depth.gamma.identity"]) == (_identifier(),)

    with pytest.raises(SelectionArtifactError) as refused:
        selection_artifact._parse_selection([entry])
    assert named in str(refused.value), str(refused.value)


def test_a_repeated_identifier_is_refused_rather_than_merged():
    """Property: `_parse_selection` raises `SelectionArtifactError` naming the identifier when two entries parse to equal identifiers, and returns both when they differ in any one component.

    Class: NEW CAPABILITY. The plausible wrong implementation silently de-duplicates, so the artifact
    holds one fit where the document named two and nothing records that one was dropped. The
    mirror-image wrong implementation refuses nothing, so a curated list holds the same fit twice and a
    consumer counting fits reads a wrong count. The check is red under both.
    Reachability: the input class is any pair of entries, and five members are exercised — an exact
    repeat, a repeat separated by another entry, and three pairs that differ in the family, the chain
    and the event class respectively.
    Both-states anchor: the assertions turn on EQUALITY of the parsed identifiers, with entries differing
    in one component admitted, so what separates refused from admitted is the parsed value.
    Killed by: merging with a set or a dict without a membership test; or dropping the refusal entirely.
    Second correct implementation: comparing the raw entry STRINGS rather than the parsed values — equal
    identifiers have equal canonical spellings, so the two disagree on nothing — or a `seen` list, or a
    dict used as an ordered set.
    """

    repeated = ["compound.rainfall_depth.gamma.identity", "compound.rainfall_depth.gamma.identity"]
    with pytest.raises(SelectionArtifactError) as refused:
        selection_artifact._parse_selection(repeated)
    assert "compound.rainfall_depth.gamma.identity" in str(refused.value), str(refused.value)

    with pytest.raises(SelectionArtifactError):
        selection_artifact._parse_selection(
            [
                "compound.rainfall_depth.gamma.identity",
                "compound.rainfall_intensity.gamma.identity",
                "compound.rainfall_depth.gamma.identity",
            ]
        )

    for differing in (
        ["compound.rainfall_depth.gamma.identity", "compound.rainfall_depth.gev.identity"],
        ["compound.rainfall_depth.gamma.identity", "compound.rainfall_depth.gamma.log+identity"],
        ["compound.rainfall_depth.gamma.identity", "pluvial.rainfall_depth.gamma.identity"],
    ):
        parsed = selection_artifact._parse_selection(differing)
        assert len(parsed) == 2, parsed
        assert [str(identifier) for identifier in parsed] == differing


def test_the_parsed_entries_keep_the_documents_order():
    """Property: `_parse_selection` returns identifiers in the order the entries appear, so reversing the input reverses the output.

    Class: NEW CAPABILITY. The plausible wrong implementation returns a `set` or sorts, which loses the
    emitter's order. A consumer reads a positional structure against that order, so a reordering is a
    silent change to what the document says while every spelling stays valid. The check is red under
    either.
    Reachability: the input class is any ordered sequence of entries, and two members are exercised — one
    order and its reverse — so an implementation with a fixed output order cannot pass both.
    Both-states anchor: the assertions turn on the output order tracking the input order, which the
    reversed member establishes independently of which order this test writes first.
    Killed by: returning a `set`; sorting the entries or the identifiers; or rebuilding the tuple from a
    sorted key list.
    Second correct implementation: a list built by `append`, or a dict keyed by identifier — both iterate
    in insertion order; the test asserts the tuple's order only.
    """

    forward = ["pluvial.rainfall_depth.gamma.identity", "compound.rainfall_depth.gamma.identity"]
    backward = list(reversed(forward))

    assert [str(identifier) for identifier in selection_artifact._parse_selection(forward)] == forward
    assert [str(identifier) for identifier in selection_artifact._parse_selection(backward)] == backward


# The marginal identifiers every dependence fixture is built from: two at one event class, and a third
# so the arity arms can reach d = 3.
_DEPTH_MARGINAL = "compound.rainfall_depth.gamma.identity"
_INTENSITY_MARGINAL = "compound.rainfall_intensity.gev.log+identity"
_LEVEL_MARGINAL = "compound.water_level.gaussian.identity"

# One conforming document as TOML, so the loader is exercised on bytes rather than on a mapping.
_FULL_TOML = """
rule = "modelled-selection:1"

selection = [
  "compound.rainfall_depth.gamma.identity",
  "compound.rainfall_intensity.gev.log+identity",
]

[stamp.inputs]
mrms = "mrms-2020"

[stamp.seeds]
rainfall = 24

[stamp.identities.swg]
system = "swg"
implementation = "vine-copula"
code_version = "0.2.0.dev0"

[[dependence]]
event_class = "compound"
marginals = [
  "compound.rainfall_depth.gamma.identity",
  "compound.rainfall_intensity.gev.log+identity",
]
structure = "c_vine"
copula_families = ["gaussian"]
"""


def _stamp(**overrides: Any) -> dict[str, Any]:
    """One well-formed stamp: the record's three sections, each with content in it."""
    stamp: dict[str, Any] = {
        "inputs": {"mrms": "mrms-2020"},
        "seeds": {"rainfall": 24},
        "identities": {
            "swg": {"system": "swg", "implementation": "vine-copula", "code_version": "0.2.0.dev0"},
        },
    }
    stamp.update(overrides)
    return stamp


def _entry(**overrides: Any) -> dict[str, Any]:
    """One well-formed dependence entry at d = 2, so a test states only the field it is proving something about."""
    entry: dict[str, Any] = {
        "event_class": "compound",
        "marginals": [_DEPTH_MARGINAL, _INTENSITY_MARGINAL],
        "structure": "c_vine",
        "copula_families": ["gaussian"],
    }
    entry.update(overrides)
    return entry


def _full_document(**overrides: Any) -> dict[str, Any]:
    """One well-formed document, matching the mapping shape `tomllib` yields for this form."""
    document: dict[str, Any] = {
        "rule": SELECTION_RULE,
        "selection": [_DEPTH_MARGINAL, _INTENSITY_MARGINAL],
        "dependence": [_entry()],
        "stamp": _stamp(),
    }
    document.update(overrides)
    return document


def _three_marginal_document(**overrides: Any) -> dict[str, Any]:
    """One well-formed document at d = 3, whose dependence entry carries the three edges that arity requires."""
    document = _full_document(
        selection=[_DEPTH_MARGINAL, _INTENSITY_MARGINAL, _LEVEL_MARGINAL],
        dependence=[
            _entry(
                marginals=[_DEPTH_MARGINAL, _INTENSITY_MARGINAL, _LEVEL_MARGINAL],
                copula_families=["gaussian", "gaussian", "gaussian"],
            )
        ],
    )
    document.update(overrides)
    return document


def test_a_conforming_document_loads_and_carries_the_rule_it_declared(tmp_path):
    """Property: `SelectionArtifact(document)` builds from a conforming mapping and carries the rule literal the document declared, and a conforming TOML file loaded through `load_selection` yields the same.

    Class: NEW CAPABILITY. The plausible wrong implementation is a constructor that validates and
    returns nothing, or one that drops `rule` — leaving a consumer unable to name which version it read
    at the one point where a later form change has to be distinguishable. The check is red under both.
    Reachability: the input class is any conforming document, and two members are exercised — a mapping
    given directly and a TOML file read from disk — so the constructor and the loader are shown to reach
    the same value by two routes.
    Both-states anchor: the assertion turns on the value the artifact carries, which the document
    declares independently of both code paths.
    Killed by: not carrying `rule`; carrying the document's mapping instead of the literal; or having
    `load_selection` return something other than a `SelectionArtifact`.
    Second correct implementation: an `init=False` dataclass setting fields through
    `object.__setattr__`, or a plain class with a normal `__init__` — the test asserts the carried value
    and the type, never the construction machinery.
    """

    artifact = SelectionArtifact(_full_document())
    assert isinstance(artifact, SelectionArtifact)
    assert artifact.rule == "modelled-selection:1"

    path = tmp_path / "selection.toml"
    path.write_text(_FULL_TOML, encoding="utf-8")
    loaded = load_selection(path)
    assert isinstance(loaded, SelectionArtifact)
    assert loaded.rule == artifact.rule


def test_the_stamp_is_required_and_is_exactly_its_three_sections():
    """Property: an absent `stamp`, one that is not a table, one carrying a section the form does not name, and one whose three sections are absent or not tables each raise `UnstampedSelectionError`, while the conforming stamp is admitted.

    Class: NEW CAPABILITY. The plausible wrong implementation defaults an absent `stamp` to an empty
    record, which makes an unstamped artifact indistinguishable from a stamped one at every consumer —
    the silent pass of a run built on a selection it cannot audit. The mirror is a check that looks for
    one section only, so a stamp carrying `inputs` and nothing else loads. The check is red under either.
    Reachability: the input class is any value under `stamp`, and eight members are exercised — a
    conforming table, an absent key, a string, an extra section, three absent sections and three
    non-table sections.
    Both-states anchor: each refused member differs from the conforming one by ONE property of the
    stamp's own shape, so what separates them is that property and not the record's content.
    Killed by: defaulting an absent stamp; checking only `identities`; or admitting an extra section.
    Second correct implementation: a set comparison over the stamp's keys, or three explicit membership
    tests — the test asserts the raised class and the admitted construction, never the expression.
    """

    assert isinstance(SelectionArtifact(_full_document()).stamp, ProvenanceRecord)

    without = _full_document()
    del without["stamp"]
    with pytest.raises(UnstampedSelectionError):
        SelectionArtifact(without)

    with pytest.raises(UnstampedSelectionError):
        SelectionArtifact(_full_document(stamp="not a table"))

    with pytest.raises(UnstampedSelectionError):
        SelectionArtifact(_full_document(stamp=_stamp(author="someone")))

    for section in ("inputs", "seeds", "identities"):
        missing = _stamp()
        del missing[section]
        with pytest.raises(UnstampedSelectionError):
            SelectionArtifact(_full_document(stamp=missing))

        not_a_table = _stamp()
        not_a_table[section] = "scalar"
        with pytest.raises(UnstampedSelectionError):
            SelectionArtifact(_full_document(stamp=not_a_table))


def test_a_stamp_sections_content_is_checked_as_well_as_its_shape():
    """Property: a non-integer seed, a boolean seed, an identity that is not a table, an identity missing or carrying an extra field, an identity whose `system` disagrees with its key, and a non-text identity field each raise the module's root refusal, while the conforming stamp is admitted.

    Class: NEW CAPABILITY. The plausible wrong implementation checks only that the three sections are
    tables and then assembles a record from whatever it is handed, so a record can say a seed is the
    string `"24"` and a rerun reads a value no generator takes. The boolean arm is the sharpest member:
    `isinstance(True, int)` is true in Python, so a check written without excluding `bool` admits a TOML
    `true` as a seed. The check is red under both.
    Reachability: the input class is any stamp section value, and seven members are exercised across
    `seeds` and `identities`, with the conforming stamp admitted in the first statement.
    Both-states anchor: each refused member differs from the conforming one in ONE field's value or
    presence, so the refusal is attributable to that field and not to the section it sits in.
    Killed by: testing `isinstance(value, int)` without excluding `bool`; testing only the section's
    type; or dropping the key-versus-`system` comparison.
    Second correct implementation: `type(value) is int`, or `isinstance(value, int) and not isinstance(
    value, bool)` — the test asserts the raised class only.
    """

    SelectionArtifact(_full_document())

    with pytest.raises(SelectionArtifactError):
        SelectionArtifact(_full_document(stamp=_stamp(seeds={"rainfall": "24"})))

    with pytest.raises(SelectionArtifactError):
        SelectionArtifact(_full_document(stamp=_stamp(seeds={"rainfall": True})))

    with pytest.raises(SelectionArtifactError):
        SelectionArtifact(_full_document(stamp=_stamp(identities={"swg": "vine-copula"})))

    for broken in (
        {"system": "swg", "implementation": "vine-copula"},
        {"system": "swg", "implementation": "vine-copula", "code_version": "0.2.0.dev0", "commit": "abc123"},
        {"system": "triton", "implementation": "vine-copula", "code_version": "0.2.0.dev0"},
        {"system": "swg", "implementation": 7, "code_version": "0.2.0.dev0"},
    ):
        with pytest.raises(SelectionArtifactError):
            SelectionArtifact(_full_document(stamp=_stamp(identities={"swg": broken})))


def test_a_stamp_of_three_shape_valid_but_empty_tables_is_refused():
    """Property: an empty `seeds` table or an empty `identities` table raises `UnstampedSelectionError`, while the same sections with one entry each are admitted.

    Class: NEW CAPABILITY. The plausible wrong implementation checks the stamp's three sections for
    shape and stops, which admits a document naming no producer and no entropy — provenance-stamped in
    form and unaccountable in content, the degenerate case the stamp exists to rule out. The check is
    red under it.
    Reachability: the input class is any populated-or-empty pair of sections, and four members are
    exercised — both empty, seeds empty, identities empty, and the conforming stamp.
    Both-states anchor: the refused members and the admitted one carry the SAME three section names and
    differ only in whether a section has content, so the check is shown to read content and not shape.
    Killed by: checking shape only; checking one section and not the other; or refusing every stamp,
    which the first assertion reds.
    Second correct implementation: `if not raw_seeds or not identities`, or a per-section truth test —
    the test asserts the raised class and the admitted construction.
    """

    SelectionArtifact(_full_document())

    for empty in ({"seeds": {}}, {"identities": {}}, {"seeds": {}, "identities": {}}):
        with pytest.raises(UnstampedSelectionError):
            SelectionArtifact(_full_document(stamp=_stamp(**empty)))


def test_the_artifact_carries_the_stamp_as_a_captured_record():
    """Property: `artifact.stamp` is a `ProvenanceRecord` whose `seeds` carries the declared integer and whose `identities` values are `ImplementationIdentity` values rebuilt from the document.

    Class: NEW CAPABILITY. The plausible wrong implementation holds the document's raw tables as the
    stamp, so a consumer reads a `dict` where the record's declared type says `ProvenanceRecord`, the
    identity it names is a table rather than a value, and the completeness check the record exists for
    cannot be called on it at all. The check is red under it.
    Reachability: the input class is any stamp; the conforming member is exercised with its seed value
    and its one identity read back, so the assertion is shown to read a record that was assembled.
    Both-states anchor: the assertion turns on the TYPE of the assembled value and on a seed the
    document declares, both independent of how the record is built.
    Killed by: holding the document's tables instead of a record; dropping a section on the way in; or
    leaving an identity as a mapping.
    Second correct implementation: a record built through `capture` positionally or by keyword, and an
    identity rebuilt from the entry's fields in any order — the test asserts the types and the declared
    values.
    """

    artifact = SelectionArtifact(_full_document())

    assert isinstance(artifact.stamp, ProvenanceRecord)
    assert artifact.stamp.seeds["rainfall"] == 24
    assert isinstance(artifact.stamp.identities["swg"], ImplementationIdentity)
    assert artifact.stamp.identities["swg"].system == "swg"
    assert artifact.stamp.identities["swg"].code_version == "0.2.0.dev0"


def test_the_artifact_carries_its_four_values_and_keeps_no_copy_of_the_document():
    """Property: `SelectionArtifact`'s fields are exactly `rule`, `fits`, `dependence` and `stamp`, and a document mutated AFTER construction does not change what the artifact carries.

    Class: NEW CAPABILITY. The plausible wrong implementation keeps the document as a field, so a
    caller that edits the mapping afterwards reads a value its own validated `rule` disagrees with —
    validation performed on bytes that have since moved. The check is red under it at the field-name arm
    and again at the mutation arm.
    Reachability: the input class is any conforming document, and the conforming member is exercised
    twice — constructed, then mutated, then read back.
    Both-states anchor: the mutation arm turns on whether the artifact's value tracks the document's,
    which is a property of retention rather than of wording.
    Killed by: retaining the document as a field; storing `rule` by reference into the mapping; or
    renaming or adding a field.
    Second correct implementation: any construction that copies the four values out of the document —
    the test asserts the field names and that a post-construction edit does not reach the artifact.
    """

    assert [field.name for field in dataclasses.fields(SelectionArtifact)] == [
        "rule",
        "fits",
        "dependence",
        "stamp",
    ]

    document = _full_document()
    artifact = SelectionArtifact(document)
    before = artifact.rule

    document["rule"] = "modelled-selection:9"
    document["selection"].append("compound.other.thing.gamma.identity")

    assert artifact.rule == before
    assert len(artifact.fits) == 2


def test_the_dependence_block_is_required_and_non_empty():
    """Property: an absent `dependence`, one that is not an array, an EMPTY array, and an entry that is not a table each raise `UndescribedDependenceError`, while the conforming block is admitted.

    Class: NEW CAPABILITY. The plausible wrong implementation defaults an absent `dependence` to an
    empty tuple, which makes the multivariate half of the selection optional — and `ADR-l` routes the
    whole selection through this artifact, so a document without it holds half of what the run chose and
    says nothing about the other half. The mirror is a check that looks only for the key, so an empty
    block passes: present and holding nothing, which is a shape-valid container with no content. The
    check is red under either.
    Reachability: the input class is any value under `dependence`, and five members are exercised — the
    conforming block, an absent key, a table, an empty array, and an array holding a string.
    Both-states anchor: each refused member differs from the conforming one by ONE property of the
    block's own shape, so the refusal is attributable to that property and not to the entries.
    Killed by: defaulting an absent block; admitting an empty array; or checking only element types.
    Second correct implementation: `if not declared` after a list test, or a truth test on a tuple built
    from it — the test asserts the raised class and the admitted construction.
    """

    assert len(SelectionArtifact(_full_document()).dependence) == 1

    without = _full_document()
    del without["dependence"]
    with pytest.raises(UndescribedDependenceError):
        SelectionArtifact(without)

    with pytest.raises(UndescribedDependenceError):
        SelectionArtifact(_full_document(dependence=_entry()))

    with pytest.raises(UndescribedDependenceError):
        SelectionArtifact(_full_document(dependence=[]))

    with pytest.raises(UndescribedDependenceError):
        SelectionArtifact(_full_document(dependence=["compound"]))


def test_a_dependence_entry_must_carry_exactly_its_four_fields():
    """Property: a dependence entry carrying a fifth key, or omitting any one of `event_class`, `marginals`, `structure` and `copula_families`, raises `UndescribedDependenceError`.

    Class: NEW CAPABILITY. The plausible wrong implementation reads the four fields it knows and
    ignores the rest, which is exactly how a fitted copula PARAMETER gets carried: a `theta = 0.31` key
    beside the others is written, maintained, and never read, while the document claims to hold a model
    class and silently holds a fitted model instead. The mirror is a check that reads the keys without
    requiring the four, so an entry missing `structure` loads and a consumer reads `None`. The check is
    red under both.
    Reachability: the input class is any entry mapping, and five members are exercised — a fifth key and
    the four possible omissions — against the conforming entry.
    Both-states anchor: each refused member differs from the conforming one by exactly one key, so the
    check is shown to read the KEY SET rather than the values under it.
    Killed by: reading the four fields without refusing an unnamed key; refusing an unnamed key without
    requiring the four; or allowing `theta` through as a tolerated extra.
    Second correct implementation: `set(entry) != set(fields)` in one test, or a per-key loop in two
    passes — the test asserts the raised class only.
    """

    SelectionArtifact(_full_document())

    with pytest.raises(UndescribedDependenceError):
        SelectionArtifact(_full_document(dependence=[_entry(theta=0.31)]))

    for field_name in ("event_class", "marginals", "structure", "copula_families"):
        incomplete = _entry()
        del incomplete[field_name]
        with pytest.raises(UndescribedDependenceError):
            SelectionArtifact(_full_document(dependence=[incomplete]))


def test_a_dependence_name_outside_the_identifier_charset_is_refused():
    """Property: a `structure` or a `copula_families` member carrying a dot, an `=`, a space or a parenthesis raises the module's root refusal, while conforming names at both positions are admitted.

    Class: NEW CAPABILITY. The plausible wrong implementation validates the entry's SHAPE and passes the
    two name fields through unchecked, so `structure = "c.vine"` or a family written
    `gaussian(theta=0.31)` is admitted and carries a parameter value inside what the form calls a name —
    the same surface-form hole the identifier charset exists to close. The check is red under it.
    Reachability: the input class is any name at either position, and four refused members are exercised
    across both, with conforming names admitted at both in the first statement.
    Both-states anchor: each refused member differs from an admitted one only in the characters it
    carries, so the check is shown to be reading the charset and not the field.
    Killed by: validating only `structure`; admitting `=` inside a family name; or applying the charset
    to the first family only.
    Second correct implementation: one predicate applied to all three positions, or three separate
    checks — the test asserts the raised class and the admitted construction.
    """

    SelectionArtifact(_full_document())

    for broken in ({"structure": "c.vine"}, {"structure": "c vine"}, {"structure": "c_vine=1"}):
        with pytest.raises(SelectionArtifactError):
            SelectionArtifact(_full_document(dependence=[_entry(**broken)]))

    with pytest.raises(SelectionArtifactError):
        SelectionArtifact(
            _full_document(
                dependence=[
                    _entry(
                        marginals=[_DEPTH_MARGINAL, _INTENSITY_MARGINAL, _LEVEL_MARGINAL],
                        copula_families=["gaussian", "gaussian", "gaussian(theta=0.31)"],
                    )
                ],
                selection=[_DEPTH_MARGINAL, _INTENSITY_MARGINAL, _LEVEL_MARGINAL],
            )
        )


def test_the_marginals_must_be_two_distinct_identifier_values():
    """Property: a `marginals` that is not an array, that holds one entry, that repeats one, or that is constructed with a bare spelling instead of a `FitIdentifier` raises the module's root refusal, while two distinct identifiers are admitted.

    Class: NEW CAPABILITY. The plausible wrong implementation accepts the array and its members
    unchecked, so a one-marginal entry loads — a "joint" model over one quantity — and a repeated
    marginal loads, placing one quantity at two positions of a structure whose meaning is the order of
    those positions. The bare-spelling member catches the third: a consumer reads `.event_class` off
    each marginal, so a string there fails deep in a consumer instead of at construction.
    Reachability: the input class is any marginals value, and four refused members are exercised plus a
    conforming one, so the refusal is shown to discriminate rather than to refuse arrays.
    Both-states anchor: the admitted pair and the repeated pair differ only in whether the two
    identifiers are equal, so the check turns on equality rather than on the array's shape.
    Killed by: dropping the two-minimum check; dropping the repeat check; or accepting a string where
    an identifier is declared.
    Second correct implementation: a length test plus `len(set(marginals)) != len(marginals)`, or the
    index comparison the module uses — the test asserts the raised class only.
    """

    artifact = SelectionArtifact(_full_document())
    assert len(artifact.dependence[0].marginals) == 2

    with pytest.raises(SelectionArtifactError):
        SelectionArtifact(_full_document(dependence=[_entry(marginals=_DEPTH_MARGINAL)]))

    with pytest.raises(SelectionArtifactError):
        SelectionArtifact(_full_document(dependence=[_entry(marginals=[_DEPTH_MARGINAL])]))

    with pytest.raises(SelectionArtifactError):
        SelectionArtifact(_full_document(dependence=[_entry(marginals=[_DEPTH_MARGINAL, _DEPTH_MARGINAL])]))

    with pytest.raises(SelectionArtifactError):
        DependenceSelection(
            event_class="compound",
            marginals=(_DEPTH_MARGINAL, _INTENSITY_MARGINAL),
            structure="c_vine",
            copula_families=("gaussian",),
        )


def test_a_marginal_must_resolve_against_this_documents_selection():
    """Property: a marginal the document's `selection` does not hold, or holds under ANOTHER event class, raises the module's root refusal, while the same identifier under the right event class is admitted.

    Class: NEW CAPABILITY. The plausible wrong implementation accepts any well-formed marginal, so a
    reference to a fit the document never selected loads — a dangling name a consumer resolves to
    nothing — and a marginal held under a different event class loads, which makes one joint model span
    two populations. The check is red under either.
    Reachability: the input class is any marginal reference, and three members are exercised — an
    identifier absent from the collection, one present under another event class, and the conforming
    pair — so the refusal is shown to be about membership and not about spelling.
    Both-states anchor: all three members use VALID identifiers and differ only in whether the document
    holds them for the entry's event class, so the check turns on resolution rather than on the grammar.
    Killed by: resolving against any event class; dropping the membership check; or resolving against
    the raw spellings instead of the parsed collection.
    Second correct implementation: a tuple membership test plus an event-class equality test in either
    order, or a precomputed mapping from event class to identifiers — the test asserts the raised class
    only.
    """

    SelectionArtifact(_full_document())

    with pytest.raises(SelectionArtifactError):
        SelectionArtifact(
            _full_document(dependence=[_entry(marginals=[_DEPTH_MARGINAL, "compound.unselected.thing.gamma.identity"])])
        )

    with pytest.raises(SelectionArtifactError):
        SelectionArtifact(
            _full_document(dependence=[_entry(event_class="pluvial", marginals=[_DEPTH_MARGINAL, _INTENSITY_MARGINAL])])
        )


def test_two_entries_for_one_event_class_are_refused():
    """Property: two dependence entries naming the same `event_class` raise the module's root refusal, while two entries naming different event classes are admitted when each resolves against its own selected fits.

    Class: NEW CAPABILITY. The plausible wrong implementation collects the entries into a tuple without
    checking the key, so the artifact holds two joint models for one population and every consumer that
    looks one up gets whichever it finds first — with nothing recording that the other exists. The check
    is red under it.
    Reachability: the input class is any multi-entry block, and two members are exercised — a repeated
    event class and a pair of distinct ones — so the refusal is shown to discriminate rather than to
    refuse multi-entry blocks.
    Both-states anchor: both members carry well-formed entries with resolvable marginals and differ
    only in the event classes they name, so the check turns on the key and not on the entries' content.
    Killed by: dropping the uniqueness check; or keying uniqueness on the structure instead of the event
    class.
    Second correct implementation: a set of seen event classes, or a dict keyed by event class — the
    test asserts the raised class and the admitted two-entry construction.
    """

    two_classes = _full_document(
        selection=[
            _DEPTH_MARGINAL,
            _INTENSITY_MARGINAL,
            "pluvial.rainfall_depth.gamma.identity",
            "pluvial.rainfall_intensity.gev.identity",
        ],
        dependence=[
            _entry(),
            _entry(
                event_class="pluvial",
                marginals=["pluvial.rainfall_depth.gamma.identity", "pluvial.rainfall_intensity.gev.identity"],
            ),
        ],
    )
    assert len(SelectionArtifact(two_classes).dependence) == 2

    with pytest.raises(SelectionArtifactError):
        SelectionArtifact(_full_document(dependence=[_entry(), _entry()]))


def test_the_family_count_is_d_d_minus_one_over_two():
    """Property: `copula_families` must hold exactly d(d-1)/2 names, where d is the entry's marginal count; a uniform selection is admitted when it is written by REPETITION, and a repeated family is never refused.

    Class: NEW CAPABILITY. The plausible wrong implementation carries one family for the whole model,
    which cannot denote a two-edge or three-edge vine and reads as complete. The mirror is one that
    de-duplicates the family tuple, which silently turns a uniform three-edge selection into a
    one-element tuple and then reds its own arity rule — so the encoding the form prescribes becomes
    unwritable. The check is red under both.
    Reachability: the input class is (marginal count, family count), and six members are exercised —
    d = 2 with 1, d = 3 with 3, d = 3 with 1, 2 and 4, d = 2 with 2 — plus a repeated-family member at
    d = 3 and a non-array member.
    Both-states anchor: the admitted d = 3 member is three IDENTICAL names, so an implementation that
    refuses a repeated family cannot pass it, and the admitted and refused members differ only in the
    TUPLE'S LENGTH.
    Killed by: requiring one family; requiring the count to equal the marginal count; de-duplicating
    before the count; or accepting a non-array value.
    Second correct implementation: an integer comparison against `d * (d - 1) // 2`, or a count built
    from a pair enumeration — the test asserts the raised class, the admitted constructions, and the
    required and given counts appearing in the message.
    """

    assert len(SelectionArtifact(_full_document()).dependence[0].copula_families) == 1
    assert len(SelectionArtifact(_three_marginal_document()).dependence[0].copula_families) == 3

    with pytest.raises(SelectionArtifactError) as refused:
        SelectionArtifact(
            _three_marginal_document(
                dependence=[
                    _entry(
                        marginals=[_DEPTH_MARGINAL, _INTENSITY_MARGINAL, _LEVEL_MARGINAL],
                        copula_families=["gaussian"],
                    )
                ]
            )
        )
    message = str(refused.value)
    assert "3" in message and "1" in message, message

    for families in (["gaussian", "clayton"], ["gaussian", "clayton", "frank", "joe"]):
        with pytest.raises(SelectionArtifactError):
            SelectionArtifact(
                _three_marginal_document(
                    dependence=[
                        _entry(
                            marginals=[_DEPTH_MARGINAL, _INTENSITY_MARGINAL, _LEVEL_MARGINAL],
                            copula_families=families,
                        )
                    ]
                )
            )

    with pytest.raises(SelectionArtifactError):
        SelectionArtifact(_full_document(dependence=[_entry(copula_families=["gaussian", "clayton"])]))

    with pytest.raises(SelectionArtifactError):
        SelectionArtifact(_full_document(dependence=[_entry(copula_families="gaussian")]))


def test_the_loader_refuses_invalid_toml_and_keeps_the_refusal_class(tmp_path):
    """Property: `load_selection` raises the module's root refusal for a file that is not valid TOML, and re-raises a document's own refusal under the SAME class with the path attached.

    Class: NEW CAPABILITY. The plausible wrong implementation re-raises every construction refusal as
    the root, which flattens the four classes a consumer catches separately — the distinction the
    hierarchy exists for, undone at the one boundary every consumer crosses. The other wrong
    implementation lets `tomllib`'s decode error escape, so a caller catching this module's root misses
    the commonest defect in a hand-authored document. The check is red under both.
    Reachability: the input class is any file, and three members are exercised — invalid TOML, a
    conforming document, and a valid document whose `rule` is wrong — so both the parse arm and the
    class-preservation arm are reached.
    Both-states anchor: the class-preservation arm turns on the raised CLASS, which the document's own
    defect fixes independently of this test, and the parse arm on whether a decode error is translated.
    Killed by: re-raising the root for every refusal; letting `TOMLDecodeError` escape; or dropping the
    path from the message, which is how a reader holding several run directories tells them apart.
    Second correct implementation: `raise type(error)(...) from error`, or a per-class re-raise — the
    test asserts the class and that the path appears in the message.
    """

    garbage = tmp_path / "garbage.toml"
    garbage.write_text("rule = modelled-selection:1\n", encoding="utf-8")
    with pytest.raises(SelectionArtifactError):
        load_selection(garbage)

    versioned = tmp_path / "versioned.toml"
    versioned.write_text(_FULL_TOML.replace("modelled-selection:1", "modelled-selection:0"), encoding="utf-8")
    with pytest.raises(UnsupportedSelectionVersionError) as refused:
        load_selection(versioned)
    assert str(versioned) in str(refused.value), str(refused.value)

    stamped = tmp_path / "stamped.toml"
    stamped.write_text(_FULL_TOML.replace("[stamp.seeds]", "[stamp.seed]"), encoding="utf-8")
    with pytest.raises(UnstampedSelectionError) as unstamped:
        load_selection(stamped)
    assert str(stamped) in str(unstamped.value), str(unstamped.value)


_NON_TEXT_MARGINAL_TOML = """rule = "modelled-selection:1"
selection = ["compound.rainfall_depth.gamma.identity", "compound.rainfall_intensity.gev.log"]

[stamp.seeds]
rainfall = 24

[stamp.identities.toolkit]
system = "toolkit"
implementation = "multidriver_swg"
code_version = "0.2.0.dev0"

[stamp.inputs]
note = "scalar"

[[dependence]]
event_class = "compound"
marginals = __MARGINALS__
structure = "cvine"
copula_families = ["gaussian"]
"""


@pytest.mark.parametrize(
    "marginals",
    [
        '[{spelling = "compound.rainfall_depth.gamma.identity"}, "compound.rainfall_intensity.gev.log"]',
        '[1, "compound.rainfall_intensity.gev.log"]',
        '[true, "compound.rainfall_intensity.gev.log"]',
    ],
)
def test_a_non_text_dependence_marginal_refuses_inside_the_modules_hierarchy(tmp_path, marginals: str):
    """Property: a `dependence` entry whose `marginals` array carries a NON-TEXT value — a TOML inline table, an integer, a boolean — leaves `load_selection` as `SelectionArtifactError`, not as an exception from outside the module's documented refusal set.

    Class: NEW CAPABILITY. The plausible wrong implementation hands each marginals entry straight to
    `FitIdentifier.parse`, which splits what it is given: a TOML inline table arrives as a `dict` and a
    bare number as an `int`, so `AttributeError: 'dict' object has no attribute 'split'` escapes the
    loader's `except SelectionArtifactError` and reaches a caller of the public read interface as an
    error no refusal row names and no consumer's handler catches. The check is red under it.
    Reachability: the input class is any document whose dependence `marginals` array holds a non-text
    value. Three members are exercised — an inline table, an integer and a boolean — and each is written
    through TOML and read back by `tomllib`, so the value arrives in the document rather than being built
    in Python. Artifact: the reviewer's probe at run `multidriver-swg_10-29_1638_selection-artifact`
    printed `AttributeError ESCAPED the hierarchy — 'dict' object has no attribute 'split'` for all three
    on the coder's tree.
    Both-states anchor: the assertion turns on WHICH EXCEPTION TYPE leaves `load_selection` for the same
    document bytes — a property of the shipped code and of any fix, not on message wording. The control
    arm below asserts that a STRING marginal naming no held identifier still refuses with
    `SelectionArtifactError`, so the test is shown to read the exception's type rather than to assert
    that every document refuses.
    Killed by: removing the text check before the parse, or guarding the parse with a handler that
    catches `SelectionArtifactError` alone, whose failure mode for a non-string is `AttributeError`.
    Invocation not isolatable from this tree — discrimination shown instead: adding an
    `isinstance(spelling, str)` guard ahead of the parse in the constructor greens these nodes, and that
    is a change to what the code DOES on the test's input rather than to how it is invoked.
    Second correct implementation: an `isinstance(spelling, str)` guard in the constructor before the
    parse; a `try/except (SelectionArtifactError, AttributeError)` re-raised as the module's root; or
    `FitIdentifier.parse` refusing a non-string with `SelectionArtifactError` — the test asserts the
    escaping class and never where the guard lives.
    Not established: that the refusal NAMES the offending position. Row 19's recovery is a corrected
    reference, and this test constrains only that the refusal stays inside the hierarchy; the message's
    content is the coder's to choose.
    """

    document = tmp_path / "non_text_marginal.toml"
    document.write_text(_NON_TEXT_MARGINAL_TOML.replace("__MARGINALS__", marginals), encoding="utf-8")
    with pytest.raises(SelectionArtifactError):
        load_selection(document)

    held = tmp_path / "unheld_string_marginal.toml"
    held.write_text(
        _NON_TEXT_MARGINAL_TOML.replace(
            "__MARGINALS__",
            '["compound.duration.weibull.identity", "compound.rainfall_intensity.gev.log"]',
        ),
        encoding="utf-8",
    )
    with pytest.raises(SelectionArtifactError):
        load_selection(held)


# The eight names this module's surface is promised under, and the two constants deliberately withheld
# from it. Both tuples are the ENUMERATION the membership arms range over, so a name added to either
# side of the promise is visibly a change to this list as well.
_SELECTION_EXPORTS = (
    "DependenceSelection",
    "FitIdentifier",
    "SelectionArtifact",
    "SelectionArtifactError",
    "UndescribedDependenceError",
    "UnsupportedSelectionVersionError",
    "UnstampedSelectionError",
    "load_selection",
)
_WITHHELD_FROM_THE_PACKAGE = ("SELECTION_RULE", "IDENTIFIER_COMPONENTS")


def _missing_attributes(package: Any, names: Any) -> list[str]:
    """The names in `names` that `package` does not bind."""
    return [name for name in names if not hasattr(package, name)]


def _defined_public_names(module: Any) -> list[str]:
    """The public names `module` DEFINES, excluding the ones it merely imports.

    A module namespace also holds everything it imported, and those are the package's to export from
    their own homes: `ImplementationIdentity` reaching `selection_artifact` from `provenance` says
    nothing about whether `selection_artifact` exports it. So the domain is names whose `__module__` is
    this module.
    """
    return sorted(
        name
        for name in dir(module)
        if not name.startswith("_") and getattr(getattr(module, name), "__module__", None) == module.__name__
    )


def _unexported_defined_names(module: Any, exported: Any) -> list[str]:
    """The public names `module` defines that `exported` does not carry."""
    return [name for name in _defined_public_names(module) if name not in exported]


def test_the_package_reexports_the_whole_selection_artifact_surface():
    """Property: the eight selection-artifact names are in `run_configuration.__all__` and each is bound on the package; every public class and function `selection_artifact` DEFINES is exported by the package and nothing else this module defines is; the module's two constants are NOT exported; and every name the package advertises anywhere in `__all__` resolves.

    Class: NEW CAPABILITY. The plausible wrong implementation is an `__all__` that lists a name the
    package never imports, so `from multidriver_swg.run_configuration import SelectionArtifact` raises
    `ImportError` from a name the package itself advertised; the mirror-image wrong implementation
    exports the module constants as well, which invites a caller to compare against a package-level
    alias rather than the module's own literal. The check is red under both.
    Reachability: the input class is any package namespace and any `__all__`, and four members are
    exercised — the real package, a list carrying a bogus name, the real list, and a list with one name
    removed — so both directions are shown to fire rather than only the clean one.
    Both-states anchor: the assertions turn on set membership and on attribute resolution, both
    properties of the namespace rather than wording this test introduces.
    Killed by: dropping a name from `__all__`; adding `SELECTION_RULE` or `IDENTIFIER_COMPONENTS` to it;
    listing a name the package does not bind; adding a ninth public definition to the module without
    adding it to `__all__`; or renaming one of the eight in `__all__` alone.
    Second correct implementation: `set(exported) >= set(_SELECTION_EXPORTS)`, or a comprehension over
    `__all__` calling `getattr` — the test asserts membership, the definition equality and the two
    synthetic controls, never the expression.
    Deliberately NOT asserting the package's TOTAL export count. `__all__` is the package's surface and
    it already carries four modules' names; a sibling module's own plan adding its own export must not
    red this test. The equality between `_defined_public_names(selection_artifact)` and
    `_SELECTION_EXPORTS` states the same thing at the scope that belongs here — this module's surface is
    exactly its eight — and it is strictly stronger for that scope, because it reds on a ninth definition
    whether or not anything exports it.
    Not established: that a name listed in `__all__` is the object a caller expects it to be. This
    checks that the name resolves, not that it resolves to the type its own module declares.
    """

    exported = list(run_configuration.__all__)

    for name in _SELECTION_EXPORTS:
        assert name in exported, name
    assert _missing_attributes(run_configuration, _SELECTION_EXPORTS) == []

    assert _defined_public_names(selection_artifact) == sorted(_SELECTION_EXPORTS)
    assert _unexported_defined_names(selection_artifact, exported) == []

    for constant in _WITHHELD_FROM_THE_PACKAGE:
        assert constant not in exported, constant

    assert _missing_attributes(run_configuration, exported) == []

    assert _missing_attributes(run_configuration, ["FitIdentifier", "NoSuchName"]) == ["NoSuchName"]

    trimmed = [name for name in exported if name != "SelectionArtifact"]
    assert _unexported_defined_names(selection_artifact, trimmed) == ["SelectionArtifact"]


# A second conforming document at d = 3, whose dependence entry carries the three edge positions that
# arity requires. Written as its own literal rather than derived, so the fixture is what a hand-authored
# document looks like and not what this test computed.
_THREE_MARGINAL_TOML = """rule = "modelled-selection:1"

selection = [
  "compound.rainfall_depth.gamma.identity",
  "compound.rainfall_intensity.gev.log+identity",
  "compound.water_level.gaussian.identity",
]

[stamp.inputs]
mrms = "mrms-2020"

[stamp.seeds]
rainfall = 24

[stamp.identities.swg]
system = "swg"
implementation = "vine-copula"
code_version = "0.2.0.dev0"

[[dependence]]
event_class = "compound"
marginals = [
  "compound.rainfall_depth.gamma.identity",
  "compound.rainfall_intensity.gev.log+identity",
  "compound.water_level.gaussian.identity",
]
structure = "c_vine"
copula_families = ["gaussian", "gaussian", "gaussian"]
"""

# A conforming document whose first marginal carries a metric ENCODED inside a legal segment. It must
# LOAD, and that is deliberate: it pins the unclosable hole rather than asserting it in prose.
_ENCODED_METRIC_TOML = """rule = "modelled-selection:1"

selection = [
  "compound.rainfall_depth.gamma.identity+ks0_03",
  "compound.rainfall_intensity.gev.log+identity",
]

[stamp.inputs]
mrms = "mrms-2020"

[stamp.seeds]
rainfall = 24

[stamp.identities.swg]
system = "swg"
implementation = "vine-copula"
code_version = "0.2.0.dev0"

[[dependence]]
event_class = "compound"
marginals = [
  "compound.rainfall_depth.gamma.identity+ks0_03",
  "compound.rainfall_intensity.gev.log+identity",
]
structure = "c_vine"
copula_families = ["gaussian"]
"""

# The three spellings a write would take in this module, as the names an AST scan looks for, and the
# mode characters that make a bare `open(...)` a write. Both are stated once and read by the scan.
_WRITE_CALLS = ("write_text", "write_bytes", "writelines")
_WRITE_MODES = ("w", "a", "x", "+")

# The record's class name, and the module-level names bound to it. The alias set is what makes the scan
# resolve `P = ProvenanceRecord` rather than only the literal spelling.
_RECORD_NAME = "ProvenanceRecord"


def _dotted(node: ast.AST) -> str:
    """The dotted name an attribute chain or a bare name reads, or the empty string for anything else."""
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def _record_aliases(tree: ast.Module) -> set[str]:
    """The module-level names bound to the record's class: the class name itself, plus every alias."""
    aliases = {_RECORD_NAME}
    for node in tree.body:
        if isinstance(node, ast.Assign) and _dotted(node.value).rsplit(".", 1)[-1] == _RECORD_NAME:
            aliases.update(target.id for target in node.targets if isinstance(target, ast.Name))
        if isinstance(node, ast.ImportFrom):
            aliases.update(alias.asname or alias.name for alias in node.names if alias.name == _RECORD_NAME)
    return aliases


def _record_reaching_functions(source: str) -> set[str]:
    """Names of the functions in `source` whose body CALLS the record class, under any of its aliases.

    A call-site walk rather than a literal count, because a count is spelling-blind: `P = ProvenanceRecord`
    followed by `P(...)` walks past `grep -c "ProvenanceRecord("` entirely, and that is the spelling a
    second assembler would use if it were written by someone avoiding the obvious token.
    """
    tree = ast.parse(source)
    aliases = _record_aliases(tree)
    reaching: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        for inner in ast.walk(node):
            if isinstance(inner, ast.Call) and _dotted(inner.func).rsplit(".", 1)[-1] in aliases:
                reaching.add(node.name)
                break
    return reaching


def _capture_call_count(source: str) -> int:
    """How many times `source` CALLS `capture`, which is the record's only assembler and must be reached."""
    return sum(
        1
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call) and _dotted(node.func).rsplit(".", 1)[-1] == "capture"
    )


def _write_calls(source: str) -> list[str]:
    """The write call sites in `source`: a write method, or a bare `open` whose mode literal writes."""
    findings: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call):
            continue
        name = _dotted(node.func).rsplit(".", 1)[-1]
        if name in _WRITE_CALLS:
            findings.append(name)
        elif name == "open":
            modes = [arg for arg in node.args[1:2] if isinstance(arg, ast.Constant)]
            modes += [kw.value for kw in node.keywords if kw.arg == "mode" and isinstance(kw.value, ast.Constant)]
            if any(isinstance(mode.value, str) and set(mode.value) & set(_WRITE_MODES) for mode in modes):
                findings.append("open")
    return findings


def _module_source() -> str:
    """The shipped module's own source, read from disk rather than introspected."""
    return Path(selection_artifact.__file__ or "").read_text(encoding="utf-8")


def test_the_conforming_fixture_round_trips_every_selection_it_declares(tmp_path):
    """Property: a conforming TOML document loaded through `load_selection` renders its `fits` back to the document's own `selection` entries, carries the three stamp components the document declares, and carries a dependence entry whose marginals ARE the identifiers in `fits`.

    Class: NEW CAPABILITY. The plausible wrong implementation returns an artifact whose `fits` are the
    parsed values but whose `dependence` marginals are the document's raw STRINGS — a consumer then reads
    `.event_class` off a `str` and fails inside itself, and every membership check the block exists for
    compares a string to an identifier and misses. The other wrong implementation drops or reorders the
    document's entries. The check is red under both.
    Reachability: the input class is any conforming document, and two members are exercised — a d = 2
    document here and a d = 3 document in the next test — so the round trip is shown on two arities.
    Both-states anchor: the assertion turns on EQUALITY between what the artifact carries and what the
    document declares; a `str` is never equal to a `FitIdentifier`, so the marginal arm proves the
    reference RESOLVED rather than that a spelling was echoed back.
    Killed by: carrying raw spellings as marginals; reordering `fits`; dropping a stamp section; or
    resolving a marginal against the collection without checking equality.
    Second correct implementation: a fresh but equal `FitIdentifier` per reference — every consumer reads
    by value — or an artifact built by a different constructor that produces the same four values.
    Not established: that the fixture's `inputs` scalars are the only input shape a run will carry. §
    3.4's depth arm carries the container case, in its own test below.
    """

    path = tmp_path / "round_trip.toml"
    path.write_text(_FULL_TOML, encoding="utf-8")
    artifact = load_selection(path)

    assert [str(identifier) for identifier in artifact.fits] == [
        _DEPTH_MARGINAL,
        _INTENSITY_MARGINAL,
    ]
    assert artifact.rule == "modelled-selection:1"

    assert artifact.stamp.inputs == {"mrms": "mrms-2020"}
    assert artifact.stamp.seeds == {"rainfall": 24}
    assert artifact.stamp.identities == {
        "swg": ImplementationIdentity(system="swg", implementation="vine-copula", code_version="0.2.0.dev0")
    }

    (entry,) = artifact.dependence
    assert entry.event_class == "compound"
    assert entry.marginals == artifact.fits[:2]
    for marginal in entry.marginals:
        assert isinstance(marginal, FitIdentifier), marginal

    assert run_configuration.load_selection is load_selection


def test_a_three_marginal_fixture_carries_three_family_positions(tmp_path):
    """Property: a conforming document at d = 3 loads, its dependence entry carries three marginals and exactly three family positions, and the uniform selection it expresses by repetition is preserved rather than collapsed.

    Class: NEW CAPABILITY. The plausible wrong implementation de-duplicates the family tuple, which
    turns the three identical names into one and then fails its own arity rule — making the encoding the
    form prescribes unwritable. The mirror-image wrong implementation accepts the document but returns
    fewer marginals than it declared. The check is red under both.
    Reachability: the input class is any d = 3 document, and the member exercised carries the maximum
    the fixture set builds; the d = 2 document in the test above is the second member.
    Both-states anchor: the assertion turns on the LENGTH of a tuple the document declares, and on the
    three names being equal to each other, so a de-duplicating implementation cannot pass it.
    Killed by: de-duplicating `copula_families`; truncating `marginals`; or requiring one family per
    entry regardless of arity.
    Second correct implementation: any construction that preserves the tuple as written — the test
    asserts length and equality, never how the tuple is assembled.
    Not established: that three is the largest arity a real run uses; the fixture pins the encoding, not
    the population.
    """

    path = tmp_path / "three_marginals.toml"
    path.write_text(_THREE_MARGINAL_TOML, encoding="utf-8")
    artifact = load_selection(path)

    (entry,) = artifact.dependence
    assert len(entry.marginals) == 3
    assert tuple(str(marginal) for marginal in entry.marginals) == (
        _DEPTH_MARGINAL,
        _INTENSITY_MARGINAL,
        _LEVEL_MARGINAL,
    )
    assert entry.copula_families == ("gaussian", "gaussian", "gaussian")
    assert len(set(entry.copula_families)) == 1


def test_the_artifact_is_read_only_at_every_depth():
    """Property: no field of `SelectionArtifact`, `FitIdentifier` or `DependenceSelection` can be rebound; the artifact's `fits`, `dependence` and each entry's `copula_families` are tuples; the stamp's mappings refuse assignment; and a container INSIDE a stamp mapping refuses mutation — while the same document held by a record built WITHOUT the recursive freeze admits exactly that mutation.

    Class: NEW CAPABILITY. The plausible wrong implementation wraps the stamp in a top-level
    `MappingProxyType` and stops, which refuses `record.seeds["x"] = 1` and admits BOTH
    `record.inputs["files"].append(...)` and `record.inputs["cfg"]["k"] = v` — because the proxy holds the
    parsed document's own objects. A test that only tries the top-level rebind passes against it, which is
    why this one reaches into a nested list and a nested table.
    Reachability: the input class is any conforming document, and two members are exercised — the loaded
    artifact, and a record assembled without the freeze — so the depth arm is shown to discriminate
    rather than to assert an absence.
    Both-states anchor: the must-fire control performs the SAME append on the SAME value held by an
    unfrozen record and it SUCCEEDS, so what separates the two is the freeze and not the container's kind.
    Killed by: a shallow proxy; freezing only the top level; leaving `copula_families` a list; or making
    any of the three types non-frozen.
    Second correct implementation: a recursive freeze by `MappingProxyType` plus `tuple`, or a custom
    read-only mapping class — the test asserts the raised exception types and the container kinds, never
    the mechanism.
    Not established: that the artifact is hashable. It is not, and its own docstring says so, because a
    read-only proxy is unhashable; this test does not assert either way.
    """

    artifact = SelectionArtifact(_full_document(stamp=_stamp(inputs={"files": ["a", "b"], "cfg": {"window": 96}})))

    with pytest.raises(dataclasses.FrozenInstanceError):
        artifact.rule = "modelled-selection:9"
    with pytest.raises(dataclasses.FrozenInstanceError):
        artifact.fits = ()

    assert isinstance(artifact.fits, tuple)
    assert isinstance(artifact.dependence, tuple)
    assert isinstance(artifact.dependence[0].copula_families, tuple)

    with pytest.raises(dataclasses.FrozenInstanceError):
        artifact.dependence[0].structure = "d_vine"
    with pytest.raises(dataclasses.FrozenInstanceError):
        artifact.fits[0].statistic = "other"

    with pytest.raises(TypeError):
        artifact.stamp.seeds["rainfall"] = 25

    assert isinstance(artifact.stamp.inputs["files"], tuple)
    with pytest.raises(AttributeError):
        artifact.stamp.inputs["files"].append("c")

    assert isinstance(artifact.stamp.inputs["cfg"], MappingProxyType)
    with pytest.raises(TypeError):
        artifact.stamp.inputs["cfg"]["window"] = 12

    unfrozen = ProvenanceRecord(inputs={"files": ["a", "b"]}, seeds={"rainfall": 24}, identities={})
    unfrozen.inputs["files"].append("c")
    assert unfrozen.inputs["files"] == ["a", "b", "c"]


def test_the_module_builds_no_record_of_its_own(tmp_path):
    """Property: no function in the shipped module calls `ProvenanceRecord` under any of its module-level aliases, and `capture` IS reached at least once.

    Class: NEW CAPABILITY. The plausible wrong implementation adds a second convenience assembler beside
    `capture` — the reconstruction the provenance module exists to prevent — and it can be spelled with an
    alias so a literal `grep -c "ProvenanceRecord("` sees nothing. The other wrong implementation reaches
    `capture` zero times, which would mean the stamp is never assembled through the record's only
    constructor even while the first arm is vacuously clean.
    Reachability: the input class is any module source, and three members are exercised — the shipped
    module, a module that constructs the record through an ALIAS, and one that constructs it literally —
    so the scan is shown to resolve an alias rather than only a literal.
    Both-states anchor: the two controls differ from the shipped module in exactly the spelling that
    reaches the class, so what the scan is shown to read is the CALL SITE and not the token.
    Killed by: adding any function that calls `ProvenanceRecord`; removing the `capture` call; or writing
    a count that ignores aliases.
    Second correct implementation: an `ast.Call` walk as here, or a symbol-table pass that resolves the
    import — the test asserts the two sets and the count, never the traversal.
    Not established: that no OTHER module assembles a record on this module's behalf. The population is
    this module's source.
    """

    source = _module_source()
    assert _record_reaching_functions(source) == set(), _record_reaching_functions(source)
    assert _capture_call_count(source) >= 1, _capture_call_count(source)

    aliased = tmp_path / "aliased.py"
    aliased.write_text(
        "from multidriver_swg.run_configuration.provenance import ProvenanceRecord\n"
        "\n\n"
        "P = ProvenanceRecord\n"
        "\n\n"
        "def rebuild(inputs):\n"
        "    return P(inputs=inputs, seeds={}, identities={})\n",
        encoding="utf-8",
    )
    assert _record_reaching_functions(aliased.read_text(encoding="utf-8")) == {"rebuild"}
    assert _record_aliases(ast.parse(aliased.read_text(encoding="utf-8"))) == {"ProvenanceRecord", "P"}

    imported_as = (
        "from multidriver_swg.run_configuration.provenance import ProvenanceRecord as R\n"
        "\n\n"
        "def rebuild(inputs):\n"
        "    return R(inputs=inputs, seeds={}, identities={})\n"
    )
    assert _record_reaching_functions(imported_as) == {"rebuild"}

    literal = (
        "from multidriver_swg.run_configuration.provenance import ProvenanceRecord\n"
        "\n\n"
        "def rebuild(inputs):\n"
        "    return ProvenanceRecord(inputs=inputs, seeds={}, identities={})\n"
    )
    assert _record_reaching_functions(literal) == {"rebuild"}


def test_the_module_writes_nothing(tmp_path):
    """Property: the shipped module contains no write call site — no `write_text`, `write_bytes`, `writelines`, and no `open` in a write, append or exclusive mode — while a temporary module that writes is named by the same scan.

    Class: NEW CAPABILITY. The plausible wrong implementation adds a writer, which turns the artifact's
    author into this module and collapses the split the form rests on: this module owns what a selection
    document IS and another party writes one, so a writer here would make one party both. The check is
    red under it.
    Reachability: the input class is any module source, and two members are exercised — the shipped module
    and a temporary module that writes through a path method — so the scan is shown to fire.
    Both-states anchor: the control differs from the shipped module only in carrying a write call, so
    what the scan reads is the call site rather than a token this test introduces.
    Killed by: adding a writer; scanning for a token rather than a call; or missing the mode argument of
    a bare `open`.
    Second correct implementation: an `ast.Call` walk as here, or a textual scan over statement lines
    with a control — the test asserts the findings list, not the traversal.
    Not established: that no helper OUTSIDE this module writes on its behalf. The population is this
    module's source.
    """

    assert _write_calls(_module_source()) == [], _write_calls(_module_source())

    writer = tmp_path / "writer.py"
    writer.write_text(
        "from pathlib import Path\n"
        "\n\n"
        "def emit(document, path):\n"
        "    Path(path).write_text(document)\n"
        "    with open(path, 'w') as handle:\n"
        "        handle.write(document)\n",
        encoding="utf-8",
    )
    assert _write_calls(writer.read_text(encoding="utf-8")) == ["write_text", "open"]


def test_the_encoded_metric_spelling_is_admitted_through_the_whole_document_path(tmp_path):
    """Property: a document whose marginal carries a metric ENCODED inside a legal chain step LOADS, and the artifact carries it as an ordinary identifier.

    Class: NEW CAPABILITY — and this arm is a CHARACTERIZATION of a hole that cannot be closed. The
    plausible wrong implementation is a charset tightened to refuse digit-bearing steps, which would refuse
    `log10` while a metric spelled `kstest` still passed; the honest implementation admits the spelling and
    says so. **Closing this hole REQUIRES editing this arm**, so a later green-to-red change here is a
    deliberate design move rather than a regression, and the assertion message says so.
    Reachability: the input class is any conforming document; the member exercised carries the encoded
    spelling as a marginal, which reaches the parser only through the whole-document path.
    Both-states anchor: the same document without the `+ks0_03` step is the arm above, so the two differ
    in exactly the encoded characters and both load.
    Killed by: tightening the charset against digits; refusing a `+`-joined step in a marginal; or
    treating the encoded spelling as a parse failure.
    Second correct implementation: none that closes the hole — that is the point of the arm. Any
    implementation that admits it passes.
    Not established: that a consumer could TELL an encoded metric from a real step name. It cannot, and
    that is the residual the module's docstring states.
    """

    path = tmp_path / "encoded.toml"
    path.write_text(_ENCODED_METRIC_TOML, encoding="utf-8")

    artifact = load_selection(path)

    encoded = artifact.fits[0]
    assert str(encoded) == "compound.rainfall_depth.gamma.identity+ks0_03", (
        "this arm asserts the encoded metric spelling is ADMITTED. Closing that hole REQUIRES editing "
        "this arm deliberately; a red here is a design change, not a regression."
    )
    assert encoded.transformation_chain == ("identity", "ks0_03")
    assert artifact.dependence[0].marginals[0] == encoded
