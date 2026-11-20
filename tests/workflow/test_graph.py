"""Check the run's rule table: the fifteen rows, their order, their edges and the functions over them.

Each check here is written against a form the defect takes when the table stops being the single
statement of the graph: a row whose system is derived from its name instead of carried as a field, an
edge that names an output no earlier row declares, a downstream query that answers one hop rather than
all of them, and a class listing that forgets the identity file a run writes beside every output. A
check that cannot fail for the reason it claims constrains nothing, so each names the wrong table it is
red under.

The edge checks are stated as PROPERTIES over the table rather than as a second copy where the property
can see the rows — every read resolving to an earlier row's output, every output unique, the downstream
answers hand-computed from the rows. One test does compare the whole table against the approved rows,
because a table whose values drifted is exactly the defect a property check can miss while every
property holds.
"""

from __future__ import annotations

import dataclasses

import pytest

import multidriver_swg.workflow as workflow
import multidriver_swg.workflow.graph as graph
from multidriver_swg.workflow.graph import (
    GRAPH,
    IDENTITY_FILE,
    GraphLookupError,
    RuleSpec,
    downstream_of,
    judgement_rules,
    output_classes,
    rule_named,
)

# The owning system of each of the fifteen rows, in execution order, as the approved rows state them. Kept as a tuple so the
# order is asserted too: a table reordered without its edges moving would invert an edge and still hold
# every name.
EXPECTED_SYSTEMS = (
    "observed-record-assembly",
    "combined-event-identification",
    "observed-record-assembly",
    "observed-record-assembly",
    "combined-event-identification",
    "marginal-distribution-fitting",
    "event-frequency-assessment",
    "event-occurrence-modelling",
    "interactive-decision-support",
    "dependence-modelling",
    "timeseries-synthesis",
    "boundary-condition-output",
    "generator-validation",
    "monte-carlo-design-storms",
    "method-attribution",
)

# The whole table as the approved rows state it: name, system, reads, reads_selection and declared
# outputs, per row. The comparison below is deliberately a second copy of those values: a property over
# the table cannot see a value that changed while every property still holds, and this test can.
EXPECTED_ROWS = (
    (
        "observed_record_pre_selection",
        "observed-record-assembly",
        (),
        False,
        ("pre_selection.json",),
    ),
    (
        "per_source_event_selection",
        "combined-event-identification",
        ("pre_selection.json",),
        False,
        ("per_source_event_sets.json",),
    ),
    (
        "observed_record_licence",
        "observed-record-assembly",
        ("per_source_event_sets.json",),
        False,
        ("licence.json",),
    ),
    (
        "observed_record_assembly",
        "observed-record-assembly",
        ("licence.json",),
        False,
        ("observed_record.json",),
    ),
    (
        "combined_event_identification",
        "combined-event-identification",
        ("observed_record.json",),
        False,
        ("event_set.json",),
    ),
    (
        "marginal_distribution_fitting",
        "marginal-distribution-fitting",
        ("event_set.json",),
        False,
        ("candidate_marginals.json",),
    ),
    (
        "event_frequency_assessment",
        "event-frequency-assessment",
        ("event_set.json",),
        False,
        ("exceedance_fits.json",),
    ),
    (
        "event_occurrence_modelling",
        "event-occurrence-modelling",
        ("observed_record.json", "event_set.json"),
        False,
        ("occurrence_model.json",),
    ),
    (
        "interactive_decision_support",
        "interactive-decision-support",
        ("event_set.json", "candidate_marginals.json"),
        False,
        ("decision_views.html",),
    ),
    (
        "dependence_modelling",
        "dependence-modelling",
        ("candidate_marginals.json",),
        True,
        ("joint_model.json",),
    ),
    (
        "timeseries_synthesis",
        "timeseries-synthesis",
        ("joint_model.json", "occurrence_model.json", "observed_record.json"),
        True,
        ("synthetic_forcing.json",),
    ),
    (
        "boundary_condition_output",
        "boundary-condition-output",
        ("synthetic_forcing.json",),
        False,
        ("boundary_conditions.json",),
    ),
    (
        "generator_validation",
        "generator-validation",
        ("observed_record.json", "event_set.json", "boundary_conditions.json"),
        True,
        ("validation_results.json",),
    ),
    (
        "monte_carlo_design_storms",
        "monte-carlo-design-storms",
        ("exceedance_fits.json", "joint_model.json"),
        False,
        ("design_storm_realizations.json",),
    ),
    (
        "method_attribution",
        "method-attribution",
        (),
        False,
        ("citation_section.html",),
    ),
)

# The downstream answers used below, computed by hand from the rows: the rule that produces the named
# artifact, then every rule whose reads reach it. Stated per case so a wrong traversal cannot satisfy the
# test by agreeing with itself.
EXPECTED_DOWNSTREAM = {
    "observed_record_assembly": (
        "combined_event_identification",
        "marginal_distribution_fitting",
        "event_frequency_assessment",
        "event_occurrence_modelling",
        "interactive_decision_support",
        "dependence_modelling",
        "timeseries_synthesis",
        "boundary_condition_output",
        "generator_validation",
        "monte_carlo_design_storms",
    ),
    "marginal_distribution_fitting": (
        "interactive_decision_support",
        "dependence_modelling",
        "timeseries_synthesis",
        "boundary_condition_output",
        "generator_validation",
        "monte_carlo_design_storms",
    ),
    "dependence_modelling": (
        "timeseries_synthesis",
        "boundary_condition_output",
        "generator_validation",
        "monte_carlo_design_storms",
    ),
    "boundary_condition_output": ("generator_validation",),
    "monte_carlo_design_storms": (),
    "method_attribution": (),
}

# The two class strings the estate register's owner matches, copied here so a class listing that renamed
# one reds this module rather than passing a shape-only check.
OUTPUT_CLASS = "Run reports, figures and outputs small enough to track"
IDENTITY_CLASS = "Run provenance records"


def test_the_table_carries_fifteen_rows_over_the_twelve_systems() -> None:
    """Property: the graph's rows are the fifteen named rows' systems in execution order, and they cover exactly the twelve systems.

    Class: the fifteen-row population the approved table closes over.
    Why it is required: the rule-to-system mapping is a field, so the systems are the graph's coverage
    of the run's compute systems; two systems own several rows, and a missing system would be one the
    run never builds.
    What kills it: dropping a row, adding a sixteenth, or reordering the tuple — all three change the
    sequence this test compares.
    Second correct implementation: a set comparison plus a length check per system; the same three
    defects are refused.
    """
    assert tuple(rule.system for rule in GRAPH) == EXPECTED_SYSTEMS
    assert len(GRAPH) == 15
    assert len(set(EXPECTED_SYSTEMS)) == 12


def test_the_table_matches_the_approved_rows_value_for_value() -> None:
    """Property: every row's name, system, reads, selection flag and declared outputs are the approved values.

    Class: the twelve rows as the approved table states them.
    Why it is required: this is the comparison the table exists for — the graph is a transcription of
    an approved set of rows, and a property over the table cannot notice a value that changed while
    every property still holds.
    What kills it: any edit to a row's name, system, edge list, selection flag or outputs.
    Second correct implementation: comparing the rows one at a time in a loop; the same twelve values
    are compared.
    """
    actual = tuple((rule.name, rule.system, rule.reads, rule.reads_selection, rule.outputs) for rule in GRAPH)

    assert actual == EXPECTED_ROWS


def test_every_read_is_a_declared_output_of_an_earlier_row() -> None:
    """Property: each `reads` entry is an output some earlier row declares, and no row reads itself.

    Class: every edge in the table.
    Why it is required: the table is the single statement of order and dependency, so an edge to a later
    row would mean a step waiting for an artifact nothing has produced yet, while every name still looked
    plausible.
    What kills it: moving a row after one of its readers, or spelling an output one character differently
    in the reader's list.
    Second correct implementation: resolving the whole graph and asserting the produced set grows as the
    rows are walked; the same edges are checked.
    """
    produced: set[str] = set()
    for rule in GRAPH:
        for read in rule.reads:
            assert read in produced, f"{rule.name} reads {read}, which no earlier row declares"
        produced.update(rule.outputs)


def test_every_declared_output_is_unique_across_the_graph() -> None:
    """Property: no two rows declare the same output file name.

    Class: every declared output of every row.
    Why it is required: an edge is stated by NAME, so a repeated output would make one name denote two
    artifacts and a reader would resolve to whichever row the traversal met first.
    What kills it: copying a row and forgetting to rename its artifact.
    Second correct implementation: comparing lengths of the concatenation and of its set; the same
    collision is refused.
    """
    outputs = [output for rule in GRAPH for output in rule.outputs]

    assert len(outputs) == len(set(outputs))


def test_the_target_rule_is_not_a_row() -> None:
    """Property: no row is named `all`.

    Class: the whole table.
    Why it is required: the workflow file's target rule is the graph's one local rule, and it is
    rendered by the file rather than carried here; a row named `all` would be a second statement of
    that decision inside the table.
    What kills it: adding the target rule to `GRAPH`.
    Second correct implementation: any traversal asserting the fact; it is order-blind.
    """
    assert "all" not in {rule.name for rule in GRAPH}


def test_a_row_with_no_output_a_repeated_name_or_a_self_read_is_refused() -> None:
    """Property: constructing a row with no declared output, a repeated read or output, or a read of its own output raises `GraphLookupError`.

    Class: four malformed rows — no output, a repeated output, a repeated read, and a read equal to an
    output — plus the conforming row as the admitted control.
    Why it is required: the row is public, so a caller can build one without the table; a name that is
    empty or repeated is an edge the graph's by-name traversal cannot resolve.
    What kills it: dropping a check from `__post_init__`, or making the dataclass non-frozen.
    Second correct implementation: the same four checks in a classmethod constructor used by both the
    table and callers; the same four rows are refused.
    """
    with pytest.raises(GraphLookupError):
        RuleSpec(name="no_output", system="no-output")

    with pytest.raises(GraphLookupError):
        RuleSpec(name="repeated_output", system="repeated", outputs=("a.json", "a.json"))

    with pytest.raises(GraphLookupError):
        RuleSpec(name="repeated_read", system="repeated", reads=("a.json", "a.json"), outputs=("b.json",))

    with pytest.raises(GraphLookupError):
        RuleSpec(name="self_read", system="self", reads=("a.json",), outputs=("a.json",))

    admitted = RuleSpec(name="conforming", system="conforming", reads=("upstream.json",), outputs=("downstream.json",))
    assert admitted.reads == ("upstream.json",)


def test_a_string_or_a_list_for_a_name_collection_is_stored_as_a_tuple() -> None:
    """Property: a bare string becomes a one-name tuple, a list becomes a tuple, and a set, a mapping or a scalar is refused.

    Class: the two name-collection fields crossed with the four forms an edit produces — a
    parenthesised string without its trailing comma, a list, a set, and a non-collection.
    Why it is required: the string case is the one a compute system's plan reaches when it edits a row
    and drops a comma; if it were stored as text, the duplicate check would compare characters and
    `output_classes` would list one artifact per character, and a string `reads` would yield no edge so
    a consuming rule would look like a leaf. A set has no order the writer intended, and a scalar is not
    a name collection at all.
    What kills it: leaving the fields as given, which stores the string and the set unchanged.
    Second correct implementation: refusing every non-tuple instead of normalising, which the reviewer's
    test accepts; this test pins the normalising branch because that is the branch the table relies on.
    """
    from_text = RuleSpec(name="r", system="s", outputs="x.json", reads="y.json")
    assert from_text.outputs == ("x.json",)
    assert from_text.reads == ("y.json",)

    from_list = RuleSpec(name="r", system="s", outputs=["x.json"], reads=["y.json"])
    assert from_list.outputs == ("x.json",)
    assert from_list.reads == ("y.json",)
    assert isinstance(from_list.outputs, tuple) and isinstance(from_list.reads, tuple)

    with pytest.raises(GraphLookupError):
        RuleSpec(name="r", system="s", outputs={"x.json"})
    with pytest.raises(GraphLookupError):
        RuleSpec(name="r", system="s", outputs=7)


def test_a_rule_and_the_table_are_frozen() -> None:
    """Property: `GRAPH` is a tuple, each row is a `RuleSpec`, and assigning to a row's field raises `FrozenInstanceError`.

    Class: the table object and each row.
    Why it is required: a consumer holds the table across a run's render; a mutable row would let one
    consumer edit what another renders, and the order guarantee would hold only until the first edit.
    What kills it: a list in place of the tuple, or a non-frozen dataclass.
    Second correct implementation: a named tuple of plain tuples; the same assignments are refused.
    """
    assert isinstance(GRAPH, tuple)
    assert all(isinstance(rule, RuleSpec) for rule in GRAPH)
    with pytest.raises(dataclasses.FrozenInstanceError):
        GRAPH[0].name = "renamed"  # type: ignore[misc]


def test_rule_named_returns_the_named_row_for_every_row() -> None:
    """Property: `rule_named` returns the row whose name equals the argument, for every row of the table.

    Class: the fifteen rows the table holds, three of them sharing one system and two another.
    Why it is required: a system may own several rows, so the lookup must read the NAME; a lookup by
    system would return the first row of a multi-row system for every one of its rows.
    What kills it: looking up `rule.system` instead of `rule.name`, or returning the first row.
    Second correct implementation: a dictionary built once from the table; the same rows are returned.
    """
    for rule in GRAPH:
        assert rule_named(rule.name) is rule


def test_rule_named_refuses_a_name_the_table_does_not_hold() -> None:
    """Property: a name outside the table raises `GraphLookupError` whose message names the rules the table holds.

    Class: a misspelling of a held name, and a name no rule resembles.
    Why it is required: the caller is usually holding the misspelling, so a message repeating it back
    withholds the one fact that recovers the call.
    What kills it: returning `None`, or raising `KeyError` with only the argument in the message.
    Second correct implementation: an explicit membership check before a dictionary lookup; the same
    refusal carries the held names.
    """
    with pytest.raises(GraphLookupError) as caught:
        rule_named("observed_record_licensee")

    message = str(caught.value)
    assert "observed_record_licensee" in message
    assert "'observed_record_licence'" in message


@pytest.mark.parametrize(("name", "expected"), sorted(EXPECTED_DOWNSTREAM.items()))
def test_downstream_of_returns_every_transitive_consumer_in_table_order(name: str, expected: tuple[str, ...]) -> None:
    """Property: `downstream_of` returns the hand-computed transitive consumers of a row, in table order, and never the row itself.

    Class: six rows — the first, a rule whose consumers span three hops, a two-hop rule, a leaf, a rule
    nothing reads, and `method_attribution`, whose reads are computed at render time.
    Why it is required: the caller asks which rules a missing artifact strands, so a one-hop answer
    would leave the rules behind the immediate readers, and an answer including the row itself would
    make a rule its own dependency.
    What kills it: a one-hop traversal, a walk that includes the starting row, or an answer returned in
    set order rather than table order.
    Second correct implementation: a depth-first walk with a visited set, then filtering the table by
    membership; the same tuples are returned.
    """
    answer = downstream_of(name)

    assert tuple(rule.name for rule in answer) == expected
    assert name not in {rule.name for rule in answer}


def test_downstream_of_refuses_a_name_the_table_does_not_hold() -> None:
    """Property: `downstream_of` of an unknown rule raises `GraphLookupError` naming the rules the table holds.

    Class: a misspelled rule name.
    Why it is required: an empty tuple means "nothing reads it", so a misspelling answered with one
    would be indistinguishable from a real leaf, and the caller would keep an artifact it meant to cut.
    What kills it: returning `()` for an unknown name.
    Second correct implementation: a membership check before the traversal; the same refusal is raised.
    """
    with pytest.raises(GraphLookupError) as caught:
        downstream_of("observed_records_assembly")

    assert "observed_record_assembly" in str(caught.value)


def test_judgement_rules_are_the_three_rows_that_read_the_selection() -> None:
    """Property: `judgement_rules` returns exactly the three rows whose `reads_selection` is true, in table order.

    Class: the selection-consuming rows the approved table marks.
    Why it is required: the set is computed from the field so a later row that gains the property joins
    it without a second list; a listed set here would drift from the rows, and a run would build a
    judgement-stage rule with no selection to read.
    What kills it: returning every row, returning none, or reading a name convention instead of the
    field.
    Second correct implementation: a comprehension over the field, as here; the same three rows result.
    """
    assert tuple(rule.name for rule in judgement_rules()) == (
        "dependence_modelling",
        "timeseries_synthesis",
        "generator_validation",
    )


def test_output_classes_lists_every_declared_output_and_every_identity_file() -> None:
    """Property: `output_classes` lists one triple per declared output plus one per row's identity file, with the register's two class names verbatim.

    Class: every artifact the graph declares — twelve declared outputs and twelve identity files.
    Why it is required: the listing is what the estate register's owner matches against, so a missing
    identity file or a class string reworded here would publish an artifact no register row classifies.
    What kills it: omitting the identity file, listing it without the `implementation_identity.json`
    name, or changing either class string.
    Second correct implementation: building the listing per row in the same order; the same length,
    membership and class strings result.
    """
    listing = output_classes()

    assert len(listing) == sum(len(rule.outputs) + 1 for rule in GRAPH)
    assert set(listing) == {(rule.name, output, OUTPUT_CLASS) for rule in GRAPH for output in rule.outputs} | {
        (rule.name, IDENTITY_FILE, IDENTITY_CLASS) for rule in GRAPH
    }
    assert [entry[2] for entry in listing].count(IDENTITY_CLASS) == len(GRAPH)


def test_the_module_docstring_states_the_three_commitments() -> None:
    """Property: the module docstring contains the three statements the table implements, and names the four functions and the identity file.

    Class: any version of the docstring, searched whole.
    Why it is required: the statements are the deliverable text; a docstring reduced to a list of names
    would leave the reason the table is one place unstated for the next editor.
    What kills it: deleting or rephrasing a statement, or ceasing to name a function.
    Second correct implementation: the sentences in any surrounding prose; the check asserts containment
    only.
    """
    docstring = graph.__doc__ or ""

    assert "The table is the single statement of step order and of data dependency" in docstring
    assert "The rule-to-system mapping is a FIELD of the row, never a naming convention" in docstring
    assert "A rule that reads the run's selection artifact says so with its own field" in docstring
    for name in ("rule_named", "output_classes", "downstream_of", "judgement_rules"):
        assert name in docstring
    assert "implementation_identity.json" in docstring


def test_the_workflow_package_reexports_the_graph_surface() -> None:
    """Property: the package exposes the graph's public names, each identical to the module's object, and lists them in `__all__`.

    Class: the seven re-exported names.
    Why it is required: a sibling imports from the package, so a name missing there is a second import
    route a reader has to discover; and an `__all__` entry with no attribute is an import failure at the
    first star import.
    What kills it: dropping an import, or listing a name that does not exist.
    Second correct implementation: any import order; the checks are identity and membership assertions.
    """
    for name in (
        "GRAPH",
        "GraphLookupError",
        "RuleSpec",
        "downstream_of",
        "judgement_rules",
        "output_classes",
        "rule_named",
    ):
        assert name in workflow.__all__
        assert getattr(workflow, name) is getattr(graph, name)

    assert "IDENTITY_FILE" not in workflow.__all__
    assert workflow.__all__ == sorted(workflow.__all__)
