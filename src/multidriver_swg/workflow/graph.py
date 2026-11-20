"""The run's rule table: fifteen stub rules, each naming its owning system and the artifacts it reads and writes.

A workflow's step order and its data dependencies are one fact stated twice whenever the order lives in
the file and the edges live in each rule's body. This module states both once: `GRAPH` is a tuple of
frozen records in the order the run executes them, and every dependency is a declared output named in
another row's `reads`. A rule rendered from the table therefore cannot disagree with the table, and a
compute system's own plan replaces a stub by editing its row and its body rather than by editing the
loop that renders it.

The three commitments this module implements, in its own words, so a later reader can check the code
against them rather than against a paraphrase:

    The table is the single statement of step order and of data dependency: a rule's position is its order, and a name in its `reads` is an output another row declares.

    The rule-to-system mapping is a FIELD of the row, never a naming convention, so a row renamed for clarity keeps its owner.

    A rule that reads the run's selection artifact says so with its own field, so the set of such rules is computed from the table and never listed anywhere.

The functions over the table are `rule_named`, which answers by the row's name; `downstream_of`, which
answers a row's transitive consumers; `judgement_rules`, which answers which rows read the selection
artifact; and `output_classes`, which lists every artifact with the register's class. A system's rules
are read by filtering `GRAPH` on the `system` field: a system may own several rows, so no lookup by
system exists.

One row is the run's CHECKPOINT: `RuleSpec.checkpoint` marks the licence row, the point after which the
run may stop because the selection lacks an entry a later stage needs. Exactly one row sets it, and every
row that reads the selection is downstream of it, so the stop always precedes a stage that needs an entry.
Both facts are properties of the constant table, checked by tests rather than by code that re-checks a
constant at every import.

What the run's report shows is a field of the row too: `RuleSpec.report_items` names the declared outputs
the report presents and the heading each appears under, so the workflow file flags an artifact by reading
the row and no second list of what a run publishes exists. The conventions an item must satisfy — a
non-empty heading and a page-native file — live in `report_items`, which this module imports and which
imports nothing back, so the row carries its items and the conventions stay one statement.

The table holds the fifteen rows the run's fixed graph states, and it deliberately does NOT hold the workflow
file's target rule: `all` is the file's first rule and its input is the run's own target list, so its
dependencies are the run's rather than the table's. Nor does the table hold the run-start record, the
engine options or any body: those are the workflow file's and the command's, and a body that reads this
table is what makes the split safe.

`method_attribution` is the row whose `reads` is EMPTY because its inputs are computed, and that emptiness
is a statement rather than an omission: DR-2 gives it every declared output of every rule in the run's
target list, so its inputs are computed from the run-start record at render time and a shortened run still
attributes exactly what ran. The first row, `observed_record_pre_selection`, also has no reads, because
precedes it. Every other row's reads are fixed and precede it in the tuple, which is why a target list
computed from this table is buildable in order.

Artifact classes belong to the outputs, and `output_classes` states them from the two literals below
rather than from any row, because the class is a property of the artifact's KIND: a declared output is
"Run reports, figures and outputs small enough to track", and each rule's `implementation_identity.json` is "Run provenance
records". Those two strings are copied verbatim from the estate register's own names, so the register's
owner matches what a run publishes; no row identifier of the register is written here, because such an
identifier belongs to the planning record and not to this repository.

Not established: that a rule's body does what the row's name says, that an artifact's content matches its
declared class, or that a compute system's own plan will keep the row's reads and outputs when it replaces
the stub. The table states the graph's shape and nothing about what runs behind it.
"""

from __future__ import annotations

from dataclasses import dataclass

from multidriver_swg.workflow.report_items import CITATION, ReportItem

# The two artifact classes, copied verbatim from the estate register's `Name` column. A declared output
# and an identity file are different KINDS of artifact, so the class is a property of the kind and is
# stated once here rather than per row: fifteen rows repeating one string would be fifteen chances to
# mistype it, and the listing is what the register's owner matches.
_OUTPUT_CLASS = "Run reports, figures and outputs small enough to track"

_IDENTITY_CLASS = "Run provenance records"

# The file every rule writes beside its outputs, as `{rule}/implementation_identity.json` in the engine's
# working directory. It is a module constant rather than a row field because it is one form for every
# row, and a rule whose identity file were spelled differently would be a rule no reader could collect.
IDENTITY_FILE = "implementation_identity.json"


class GraphLookupError(Exception):
    """A lookup by a name the table does not hold: a missing rule name, or a name no row carries.

    Deliberately NOT `KeyError`: a `KeyError` renders its message inside repr quotes, and the message
    here is the reader's recovery path — the names the table does hold.
    """


@dataclass(frozen=True)
class RuleSpec:
    """One validated row of the graph: the rule, its owning system, its edges and its declared outputs.

    - `name` — the rule's name, which is also the directory its outputs live under.
    - `system` — the compute system that owns the rule. A FIELD, never a convention derived from the
      name, so a rename cannot silently move ownership; a system may own several rows, so the field is
      read off a row and never resolves to one.
    - `reads` — the declared outputs of OTHER rows this rule reads, by file name. Empty means no fixed
      input: either the rule is first (nothing precedes it) or its inputs are computed at render time,
      which is `method_attribution`'s one case.
    - `outputs` — the files this rule declares besides its identity file, relative to the rule's own
      directory. Every one is also the name another row would use to read it.
    - `report_items` — the declared outputs the run's report shows, by file name, each with the heading it
      appears under. Empty means the rule publishes nothing in the page. A field for the same reason
      `reads` is one: what a run publishes is a fact about its rules, and a list elsewhere would have to
      be edited with them.
    - `reads_selection` — whether the rule consumes the run's recorded model selection. A property of
      the row rather than a list elsewhere, because the set of consumers moves whenever a compute
      system's plan gives it a reason to read the selection.
    - `checkpoint` — whether the rule is the run's checkpoint: the one row after which the run may stop
      because its selection lacks an entry a later stage needs. Exactly one row of the table sets it,
      which a test over the constant states; the workflow file renders that row as an engine checkpoint.

    Frozen, so a table a consumer holds cannot be edited into a graph no reader checked. A row with no
    declared output is refused, because a rule that writes nothing cannot be a step another rule reads;
    a duplicate output or read is refused, because the graph's edges are stated by name and a repeated
    name would make one name mean two edges; and a report item is refused unless it names one of the row's
    own declared outputs exactly once, because an item for a file the rule never declares is an entry the
    engine could only fail on when it builds the report.

    THE TWO NAME COLLECTIONS ARE STORED AS TUPLES WHATEVER THEY ARRIVED AS, and that is a repair rather
    than tidiness. A one-name edit by a compute system's plan is the input this exists for: `outputs =
    ("x.json")` without its trailing comma is the TEXT `x.json`, whose duplicate check compares characters
    and passes, so `output_classes` would list one artifact per character; and `reads = "y.json"` is
    iterable, so `downstream_of` would find no edge and a rule that consumes another's output would look
    like a leaf. A list is ordered, so it is normalised to a tuple and cannot be mutated inside a record
    this module calls frozen; a bare string becomes a ONE-NAME tuple. Any other kind — a set, a mapping, a
    generator, a number — is refused, because its order is either absent or not the writer's intent. The
    report items are normalised the same way and for the same reason: one item written without its trailing
    comma is a bare item, which becomes a one-item tuple rather than a value nothing can iterate.
    """

    name: str
    system: str
    reads: tuple[str, ...] = ()
    outputs: tuple[str, ...] = ()
    report_items: tuple[ReportItem, ...] = ()
    reads_selection: bool = False
    checkpoint: bool = False

    def __post_init__(self) -> None:
        """Store the name and item collections as tuples, then refuse a row whose names are empty, repeated, or whose inputs include its own output."""
        object.__setattr__(self, "reads", _name_tuple(self.reads, self.name, "read"))
        object.__setattr__(self, "outputs", _name_tuple(self.outputs, self.name, "output"))
        object.__setattr__(self, "report_items", _report_item_tuple(self.report_items, self.name))

        for role, value in (("name", self.name), ("system", self.system)):
            if not isinstance(value, str) or not value.strip():
                raise GraphLookupError(f"rule row {role} is {value!r}: a row is named by non-empty text")
        if not self.outputs:
            raise GraphLookupError(
                f"rule {self.name!r} declares no output: a step that writes nothing is not a rule another "
                "step can read, and the identity file is added by the graph rather than declared here"
            )
        for role, values in (("output", self.outputs), ("read", self.reads)):
            if len(set(values)) != len(values):
                raise GraphLookupError(f"rule {self.name!r} repeats a {role} name: a name is one edge")
        if set(self.reads) & set(self.outputs):
            raise GraphLookupError(
                f"rule {self.name!r} reads its own output: a row's `reads` names OTHER rows' outputs, so a "
                "name in both lists is a cycle no execution order can satisfy"
            )
        for item in self.report_items:
            if item.output not in self.outputs:
                raise GraphLookupError(
                    f"rule {self.name!r} reports {item.output!r}, which is not an output it declares: an item "
                    "names a declared output, because the engine flags one of the rule's outputs and a file the "
                    "rule never declares cannot be flagged"
                )
        reported = [item.output for item in self.report_items]
        if len(set(reported)) != len(reported):
            raise GraphLookupError(
                f"rule {self.name!r} reports one output under two headings: an artifact appears in the report "
                "once, so a second item for it is a second statement of where it appears"
            )


def _report_item_tuple(value: object, rule: str) -> tuple[ReportItem, ...]:
    """Return `value` as the tuple of report items a row stores, refusing a kind or a value it cannot mean.

    A bare item is the one-item collection a missing trailing comma produces, so it becomes a one-item
    tuple rather than a value no reader could iterate. A list is ordered and becomes a tuple, which is what
    keeps a frozen record's field from being mutated after the table was checked. Any other kind is refused:
    a set has no order the writer intended, a mapping's values are not items, and a generator would be
    consumed here so a second reader would see a row that publishes nothing.
    """
    if isinstance(value, ReportItem):
        items: tuple[object, ...] = (value,)
    elif isinstance(value, list | tuple):
        items = tuple(value)
    else:
        raise GraphLookupError(
            f"rule {rule!r} carries report items of type {type(value).__name__}: a report-item collection is a "
            "tuple, a list or a bare item, so a value of another kind is not a collection this row can read"
        )
    collected: list[ReportItem] = []
    for item in items:
        if not isinstance(item, ReportItem):
            raise GraphLookupError(
                f"rule {rule!r} carries the report item {item!r}, which is not one: every entry names a declared "
                "output and the heading the report shows it under"
            )
        collected.append(item)
    return tuple(collected)


def _name_tuple(value: object, rule: str, role: str) -> tuple[str, ...]:
    """Return `value` as the tuple of names a row stores, refusing a kind whose order it cannot read.

    A bare string is the one-name collection a missing trailing comma produces, so it becomes a one-name
    tuple rather than a tuple of characters. A list is ordered and becomes a tuple, which is what keeps a
    frozen record's field from being mutated after the table was checked. A set, a mapping, a generator and
    every scalar are refused: a set's order is not the writer's, a mapping's keys are not a name list, and
    a generator would be consumed here so a second reader would see an empty row.
    """
    if isinstance(value, str):
        names: tuple[object, ...] = (value,)
    elif isinstance(value, list | tuple):
        names = tuple(value)
    else:
        raise GraphLookupError(
            f"rule {rule!r} carries {role}s of type {type(value).__name__}: a name collection is a tuple, a "
            "list or a bare string (one name), so a value of another kind has an order this row cannot read"
        )
    for name in names:
        if not isinstance(name, str) or not name.strip():
            raise GraphLookupError(f"rule {rule!r} carries the non-text {role} {name!r}: every name is text")
    return names


# The fifteen rules, in execution order. Every `reads` entry names a declared output of an EARLIER row,
# which is the whole of the ordering guarantee: a consumer can build the tuple's rows in order and never
# wait for an artifact a later row produces.
GRAPH: tuple[RuleSpec, ...] = (
    RuleSpec(
        name="observed_record_pre_selection",
        system="observed-record-assembly",
        outputs=("pre_selection.json",),
    ),
    RuleSpec(
        name="per_source_event_selection",
        system="combined-event-identification",
        reads=("pre_selection.json",),
        outputs=("per_source_event_sets.json",),
    ),
    # The run's one checkpoint: the engine evaluates the rows after it only once this row has run, and the
    # workflow file's target function reads its output to decide whether the run stops here.
    RuleSpec(
        name="observed_record_licence",
        system="observed-record-assembly",
        reads=("per_source_event_sets.json",),
        outputs=("licence.json",),
        checkpoint=True,
    ),
    RuleSpec(
        name="observed_record_assembly",
        system="observed-record-assembly",
        reads=("licence.json",),
        outputs=("observed_record.json",),
    ),
    RuleSpec(
        name="combined_event_identification",
        system="combined-event-identification",
        reads=("observed_record.json",),
        outputs=("event_set.json",),
    ),
    RuleSpec(
        name="marginal_distribution_fitting",
        system="marginal-distribution-fitting",
        reads=("event_set.json",),
        outputs=("candidate_marginals.json",),
    ),
    RuleSpec(
        name="event_frequency_assessment",
        system="event-frequency-assessment",
        reads=("event_set.json",),
        outputs=("exceedance_fits.json",),
    ),
    RuleSpec(
        name="event_occurrence_modelling",
        system="event-occurrence-modelling",
        reads=("observed_record.json", "event_set.json"),
        outputs=("occurrence_model.json",),
    ),
    RuleSpec(
        name="interactive_decision_support",
        system="interactive-decision-support",
        reads=("event_set.json", "candidate_marginals.json"),
        outputs=("decision_views.html",),
    ),
    RuleSpec(
        name="dependence_modelling",
        system="dependence-modelling",
        reads=("candidate_marginals.json",),
        outputs=("joint_model.json",),
        reads_selection=True,
    ),
    RuleSpec(
        name="timeseries_synthesis",
        system="timeseries-synthesis",
        reads=("joint_model.json", "occurrence_model.json", "observed_record.json"),
        outputs=("synthetic_forcing.json",),
        reads_selection=True,
    ),
    RuleSpec(
        name="boundary_condition_output",
        system="boundary-condition-output",
        reads=("synthetic_forcing.json",),
        outputs=("boundary_conditions.json",),
    ),
    RuleSpec(
        name="generator_validation",
        system="generator-validation",
        reads=("observed_record.json", "event_set.json", "boundary_conditions.json"),
        outputs=("validation_results.json",),
        reads_selection=True,
    ),
    RuleSpec(
        name="monte_carlo_design_storms",
        system="monte-carlo-design-storms",
        reads=("exceedance_fits.json", "joint_model.json"),
        outputs=("design_storm_realizations.json",),
    ),
    # Empty `reads` by design: its inputs are every declared output of every rule in the run's target
    # list, so the workflow file renders them at render time and a shortened run attributes exactly what
    # it built. Its citation section is the graph's one report item: the run's account of the methods it
    # used is the artifact a reader opens first, and it is an HTML file so the page shows it rather than
    # offering it as a download.
    RuleSpec(
        name="method_attribution",
        system="method-attribution",
        outputs=("citation_section.html",),
        report_items=(ReportItem("citation_section.html", CITATION),),
    ),
)


def rule_named(name: str) -> RuleSpec:
    """Return the row named `name`, refusing a name the table does not hold.

    The lookup is over the rule's NAME, which is how every consumer addresses a rule: a system may own
    several rows, so the owning system does not identify one. The row's `system` stays a field a reader
    of the row asks for, never a key a lookup resolves.

    The refusal lists the names the table holds, because a caller has usually misspelled one of them and
    a message carrying only the misspelling is the fact the caller already had.
    """
    for rule in GRAPH:
        if rule.name == name:
            return rule
    raise GraphLookupError(
        f"no rule is named {name!r}: the rules the table holds are " + ", ".join(repr(rule.name) for rule in GRAPH)
    )


def downstream_of(name: str) -> tuple[RuleSpec, ...]:
    """Return every row that reads `name`'s output, directly or through another row, in table order.

    Transitive rather than one hop, because the caller this exists for asks which rules a missing
    artifact would strand: one hop would name the immediate readers and leave the rules behind THEM,
    which are stranded just as surely. A row is never included as its own downstream, so the answer is
    the rules that consume it rather than a set that trivially contains it.

    A name the table does not hold is refused rather than answered with an empty tuple: an empty answer
    means "nothing reads it", and a misspelled name would receive that answer while the rule it meant
    has readers.

    `method_attribution` is downstream of nothing under this function, because its reads are computed
    from the run's target list rather than from the table; a caller computing a target list adds its
    output explicitly, which is what the target-list cut does.
    """
    producers: dict[str, str] = {}
    for rule in GRAPH:
        for output in rule.outputs:
            producers[output] = rule.name

    if name not in {rule.name for rule in GRAPH}:
        raise GraphLookupError(
            f"no rule is named {name!r}: the rules the table holds are " + ", ".join(repr(rule.name) for rule in GRAPH)
        )

    reached: set[str] = {name}
    consumers: set[str] = set()
    changed = True
    while changed:
        changed = False
        for rule in GRAPH:
            if rule.name in reached or rule.name in consumers:
                continue
            if any(read in producers and producers[read] in reached for read in rule.reads):
                consumers.add(rule.name)
                reached.add(rule.name)
                changed = True

    return tuple(rule for rule in GRAPH if rule.name in consumers)


def judgement_rules() -> tuple[RuleSpec, ...]:
    """Return the rows that consume the run's recorded model selection, in table order.

    Computed from `reads_selection` rather than listed, because whether a row consumes the selection is
    the row's own property: a compute system's plan that gives its rule a reason to read the selection
    changes one field, and a second list here would have to be remembered in the same edit.
    """
    return tuple(rule for rule in GRAPH if rule.reads_selection)


def output_classes() -> tuple[tuple[str, str, str], ...]:
    """Return `(rule, output, class)` for every artifact the graph declares, in table order.

    Each row contributes its declared outputs in their declared order, then its identity file, so the
    listing is deterministic and one reader can diff two runs' listings line by line. An identity file
    is listed beside the outputs rather than omitted because it is an artifact the run writes and the
    register classifies; it is not a row FIELD because every row's is the same file name, and fifteen
    copies of one constant would be fifteen chances to spell it differently.
    """
    listing: list[tuple[str, str, str]] = []
    for rule in GRAPH:
        for output in rule.outputs:
            listing.append((rule.name, output, _OUTPUT_CLASS))
        listing.append((rule.name, IDENTITY_FILE, _IDENTITY_CLASS))
    return tuple(listing)
