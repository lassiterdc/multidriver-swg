"""Check the pre-run step: the target list's three branches, the checks it runs, and the record it writes.

Each check is written against a form the defect takes when the step stops being one list and one record: a
target list that is recomputed per caller and so can differ between the record and the engine, a selection
that is judged present without its form being read, a delegation accepted because no list has to be
shortened, and a check reordered so the operator repairs the wrong file. The pure function is driven
directly on both branches, and the step itself is driven over a committed copy and a full estate, because
the checks it runs are the other systems' mechanisms and a mocked one would let a mis-wired call pass.

Every check names the wrong implementation it is red under, because a check that cannot fail for the reason
it claims constrains nothing.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from pathlib import Path

import pytest

import multidriver_swg.workflow.pre_run as pre_run
from multidriver_swg.run_configuration.main_configuration import MainConfiguration
from multidriver_swg.workflow.end_of_run import END_OF_RUN_RECORD
from multidriver_swg.workflow.graph import GRAPH
from multidriver_swg.workflow.pre_run import PreRunError, compute_targets, prepare_run
from multidriver_swg.workflow.run_start import RUN_START_FILE, read_run_start
from tests.workflow.conftest import ToolkitCopy

PROJECT = "smoke.run"

# The paths an absent selection with the judgement kept builds: the six pre-choice rules, the attribution
# rule, and the end-of-run record last, in table order. Hand-computed from the table, not from the
# implementation.
KEPT_TARGETS = (
    "observed_record_assembly/observed_record.json",
    "combined_event_identification/event_set.json",
    "marginal_distribution_fitting/candidate_marginals.json",
    "event_frequency_assessment/exceedance_fits.json",
    "event_occurrence_modelling/occurrence_model.json",
    "interactive_decision_support/decision_views.html",
    "method_attribution/citation_section.html",
    END_OF_RUN_RECORD,
)

ALL_TARGETS = tuple(f"{rule.name}/{output}" for rule in GRAPH for output in rule.outputs) + (END_OF_RUN_RECORD,)

# A selection document the artifact's own reader admits: two marginals of one event class, one edge's
# family, and a non-empty stamp. Its content is irrelevant to this step, which reads only its FORM.
VALID_SELECTION = """rule = "modelled-selection:1"
selection = ["compound.rainfall_depth.gamma.identity", "compound.surge_height.lognormal.identity"]

[[dependence]]
event_class = "compound"
marginals = ["compound.rainfall_depth.gamma.identity", "compound.surge_height.lognormal.identity"]
structure = "d-vine"
copula_families = ["gaussian"]

[stamp.inputs]
source = "test"

[stamp.seeds]
swg = 1

[stamp.identities.swg]
system = "swg"
implementation = "vine"
code_version = "0"
"""


@dataclass(frozen=True)
class _Estate:
    """The three keys' directories and files, the declaration, and the configuration."""

    root: Path
    project_directory: Path
    configuration: Path
    bindings: Path
    run_output: Path
    selection: Path


def _estate(tmp_path: Path, toolkit: ToolkitCopy, *, bind_selection: bool = True) -> _Estate:
    """Write a full estate: a binding document for all three keys, the declaration, and the configuration."""
    root = tmp_path / "estate"
    project_directory = root / "projects" / "smoke"
    project_directory.mkdir(parents=True)
    (root / "config").mkdir(parents=True)
    run_output = tmp_path / "run"
    run_output.mkdir()
    selection = project_directory / "selection.toml"

    bindings = root / "config" / "paths_test.toml"
    lines = [
        'rule = "path-bindings:1"',
        "",
        "[paths]",
        f'"{PROJECT}.project_directory" = "{project_directory}"',
        f'"{PROJECT}.run_output" = "{run_output}"',
    ]
    if bind_selection:
        lines.append(f'"{PROJECT}.selection" = "{selection}"')
    bindings.write_text("\n".join(lines) + "\n", encoding="utf-8")

    (project_directory / "toolkit_revision.toml").write_text(
        f'rule = "toolkit-revision:1"\ncommit = "{toolkit.commit}"\n', encoding="utf-8"
    )

    configuration = root / "config" / "run.toml"
    configuration.write_text(
        'rule = "main-configuration:1"\n'
        f'estate_root = "{root}"\n'
        'bindings = "config/paths_test.toml"\n'
        f'project = "{PROJECT}"\n'
        'mode = "local"\n\n[local]\ncores = 1\n',
        encoding="utf-8",
    )

    return _Estate(
        root=root,
        project_directory=project_directory,
        configuration=configuration,
        bindings=bindings,
        run_output=run_output,
        selection=selection,
    )


def _prepare(monkeypatch: pytest.MonkeyPatch, toolkit: ToolkitCopy, estate: _Estate):
    """Run the step as the command would, with the revision gate's package pointed at the committed copy."""
    monkeypatch.setattr(pre_run, "running_package_directory", lambda: toolkit.package)
    return prepare_run(MainConfiguration.load(estate.configuration), estate.configuration)


def test_the_pure_target_function_takes_exactly_the_two_facts() -> None:
    """Property: `compute_targets`'s parameters are exactly `selection_present` and `delegated`, both keyword-only.

    Class: the function's signature.
    Why it is required: the function is the one place the cut is made, and the shape says it takes the
    delegation as an argument; a third parameter — a configuration, a path, a directory — would make the
    branch a function of machine state the record cannot show.
    What kills it: adding a parameter, or making either one positional-only-hidden by a default.
    Second correct implementation: none — the signature is the contract, and any extra operand is red.
    """
    parameters = inspect.signature(compute_targets).parameters

    assert tuple(parameters) == ("selection_present", "delegated")
    assert all(parameter.kind is inspect.Parameter.KEYWORD_ONLY for parameter in parameters.values())


def test_an_absent_selection_keeps_the_pre_choice_rows_and_the_attribution_row() -> None:
    """Property: `compute_targets(selection_present=False, delegated=False)` is exactly the six pre-choice rows' outputs plus the attribution row's.

    Class: the kept-absent branch, over the stub graph's twelve rows.
    Why it is required: this is the cut the run makes when no selection exists — every rule that neither
    reads the selection nor is downstream of a reader, plus the row whose inputs are that list — and a
    wrong cut either strands a rule or builds the judgement branch with nothing to read.
    What kills it: keeping a consumer row (7, 8 or 10), keeping a row downstream of one (9 or 11), or
    forgetting the attribution row.
    Second correct implementation: an excluded-set computation from the graph's own functions; the same
    seven paths result.
    """
    targets = compute_targets(selection_present=False, delegated=False)

    assert targets == KEPT_TARGETS
    for excluded in (
        "dependence_modelling/joint_model.json",
        "timeseries_synthesis/synthetic_forcing.json",
        "boundary_condition_output/boundary_conditions.json",
        "generator_validation/validation_results.json",
        "monte_carlo_design_storms/design_storm_realizations.json",
    ):
        assert excluded not in targets


@pytest.mark.parametrize(
    ("selection_present", "delegated"),
    [(True, False), (False, True), (True, True)],
    ids=["selection", "delegation", "both"],
)
def test_a_present_selection_or_a_delegation_targets_the_whole_graph(selection_present: bool, delegated: bool) -> None:
    """Property: either fact targets every rule's declared outputs, in table order.

    Class: the three fact combinations that do not shorten the list.
    Why it is required: a present selection is read by the judgement rows, so the branch must build; and a
    delegation is not shortened by the shape's own wording, even though the step refuses one today.
    What kills it: shortening whenever the selection file is missing, regardless of the delegation fact.
    Second correct implementation: returning the full list before any branch is read; the same twelve
    paths result.
    """
    assert compute_targets(selection_present=selection_present, delegated=delegated) == ALL_TARGETS


def test_the_target_list_is_deterministic_for_the_same_facts() -> None:
    """Property: two calls with the same facts return equal tuples, and the list is in table order.

    Class: both branches, called twice each.
    Why it is required: the list is written into the record and compared by the engine's `all` rule; an
    unordered or set-derived result would make two runs of the same facts differ.
    What kills it: deriving the order from a set or a mapping.
    Second correct implementation: any table-order traversal; the equality and the order both hold.
    """
    for facts in ({"selection_present": False, "delegated": False}, {"selection_present": True, "delegated": False}):
        first = compute_targets(**facts)
        second = compute_targets(**facts)
        assert first == second
        assert len(first) == len(set(first))
        assert list(first) == sorted(first, key=ALL_TARGETS.index)


def test_a_conforming_run_writes_a_kept_absent_record_with_the_shortened_targets(
    tmp_path: Path, toolkit_copy: ToolkitCopy, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Property: with no selection, the step writes the record into the run output, and the record it wrote reads back equal to the one it returned.

    Class: a full estate over a committed copy, with no selection file.
    Why it is required: the record is the engine's only statement of what to build; a field the write
    loses, or a record written somewhere the engine does not read, makes the run build nothing.
    What kills it: writing before the targets are computed, writing to the process's working directory, or
    recording a relative configuration path.
    Second correct implementation: writing through any writer that produces the same object; the reader is
    the comparison.
    """
    estate = _estate(tmp_path, toolkit_copy)

    record = _prepare(monkeypatch, toolkit_copy, estate)

    written = read_run_start(estate.run_output / RUN_START_FILE)
    assert written == record
    assert record.targets == KEPT_TARGETS
    assert record.judgement == "kept-absent"
    assert record.selection is None
    path = Path(record.main_configuration)
    assert path.is_absolute()
    assert path == estate.configuration.resolve()


def test_a_present_selection_is_validated_and_the_whole_graph_is_targeted(
    tmp_path: Path, toolkit_copy: ToolkitCopy, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Property: with a conforming selection file, the step targets the whole graph and records the selection's absolute path and the `present` state.

    Class: a conforming selection document at the key's path.
    Why it is required: a present artifact means the judgement rows can run, and the record must say which
    artifact the run consumes; a boolean would lose the path a reader needs.
    What kills it: recording `None` whenever no branch reads the file, or shortening the list anyway.
    Second correct implementation: the same facts answered from the path's existence; the same twelve
    targets and the same record fields.
    """
    estate = _estate(tmp_path, toolkit_copy)
    estate.selection.write_text(VALID_SELECTION, encoding="utf-8")

    record = _prepare(monkeypatch, toolkit_copy, estate)

    assert record.targets == ALL_TARGETS
    assert record.judgement == "present"
    assert record.selection == str(estate.selection)


def test_a_malformed_selection_is_refused_naming_it(
    tmp_path: Path, toolkit_copy: ToolkitCopy, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Property: a selection file the artifact's reader refuses stops the step with `PreRunError` naming the file, and no record is written.

    Class: a document declaring another version.
    Why it is required: the shape makes a reader refusal a refusal of the RUN, because a run that consumed
    an unvalidated selection would model something no one chose.
    What kills it: deciding presence from the file's existence and never reading its form.
    Second correct implementation: calling the reader's loader anywhere before the record write; the same
    refusal and the same absent record result.
    """
    estate = _estate(tmp_path, toolkit_copy)
    estate.selection.write_text('rule = "modelled-selection:2"\n', encoding="utf-8")

    with pytest.raises(PreRunError) as caught:
        _prepare(monkeypatch, toolkit_copy, estate)

    assert str(estate.selection) in str(caught.value)
    assert not (estate.run_output / RUN_START_FILE).exists()


def test_a_configuration_naming_a_procedure_is_refused_naming_it(
    tmp_path: Path, toolkit_copy: ToolkitCopy, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Property: an `automatic_selection` procedure stops the step, and the message names the procedure and says none is provided.

    Class: a configuration whose `[judgement]` names a procedure.
    Why it is required: the toolkit provides no procedure, so a run prepared for one would claim a choice
    nothing can make; the operator needs to know the field's value was read and is the blocker.
    What kills it: computing the full list and writing the record for a delegation.
    Second correct implementation: refusing before the target computation or after it; the same class and
    the same named value arrive.
    """
    estate = _estate(tmp_path, toolkit_copy)
    estate.configuration.write_text(
        estate.configuration.read_text(encoding="utf-8").replace(
            "[local]", '[judgement]\nautomatic_selection = "nearest"\n\n[local]'
        ),
        encoding="utf-8",
    )

    with pytest.raises(PreRunError) as caught:
        _prepare(monkeypatch, toolkit_copy, estate)

    message = str(caught.value)
    assert "'nearest'" in message
    assert "provides no such procedure" in message
    assert not (estate.run_output / RUN_START_FILE).exists()


def test_an_unbound_key_is_refused_naming_the_binding_document(
    tmp_path: Path, toolkit_copy: ToolkitCopy, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Property: a binding document missing the selection key stops the step, naming the document and the key.

    Class: the estate's binding document with the selection key removed.
    Why it is required: the three keys are the step's whole route to the files it checks; a resolver that
    defaulted one would read a file no machine bound.
    What kills it: catching the resolver's refusal and continuing, or resolving only the keys the
    judgement branch needs.
    Second correct implementation: resolving the keys in any order; the same key is reported missing.
    """
    estate = _estate(tmp_path, toolkit_copy, bind_selection=False)

    with pytest.raises(PreRunError) as caught:
        _prepare(monkeypatch, toolkit_copy, estate)

    message = str(caught.value)
    assert str(estate.bindings) in message
    assert "selection" in message


def test_a_dirty_checkout_or_a_differing_declaration_stops_the_step(
    tmp_path: Path, toolkit_copy: ToolkitCopy, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Property: the revision gate's checks run in this step, so an untracked file and a declaration naming another commit each raise `PreRunError`.

    Class: the committed copy with one untracked file, and the same copy with its declaration replaced.
    Why it is required: the command must refuse at band 2 before submitting an allocation whose engine
    would refuse at parse; a step that skipped the gate would queue the work and fail later.
    What kills it: writing the record before the gate, or relying on the engine's parse-time gate alone.
    Second correct implementation: the composed gate or its three checks in their order; both cases refuse.
    """
    estate = _estate(tmp_path, toolkit_copy)
    (toolkit_copy.root / "untracked_module.py").write_text("value = 1\n", encoding="utf-8")

    with pytest.raises(PreRunError) as dirty:
        _prepare(monkeypatch, toolkit_copy, estate)
    assert "untracked_module.py" in str(dirty.value)

    (toolkit_copy.root / "untracked_module.py").unlink()
    (estate.project_directory / "toolkit_revision.toml").write_text(
        'rule = "toolkit-revision:1"\ncommit = "' + "0" * 40 + '"\n', encoding="utf-8"
    )

    with pytest.raises(PreRunError) as mismatch:
        _prepare(monkeypatch, toolkit_copy, estate)
    assert "0" * 40 in str(mismatch.value)
    assert toolkit_copy.commit in str(mismatch.value)
    assert not (estate.run_output / RUN_START_FILE).exists()


def test_an_unwritable_run_output_is_refused_naming_it(
    tmp_path: Path, toolkit_copy: ToolkitCopy, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Property: a run output that does not exist stops the step with `PreRunError` naming the directory, rather than an interpreter error.

    Class: a resolved run output removed before the step runs.
    Why it is required: the command reports one class for every refusal it did not start the run for, and
    a raw `FileNotFoundError` would escape as a traceback naming a temp path.
    What kills it: letting `Path.write_text`'s error out of the step.
    Second correct implementation: creating the directory, or checking it first; the shape's run output is
    the resolver's to locate and the step's to write into, and this implementation refuses.
    """
    estate = _estate(tmp_path, toolkit_copy)
    estate.run_output.rmdir()

    with pytest.raises(PreRunError) as caught:
        _prepare(monkeypatch, toolkit_copy, estate)

    assert str(estate.run_output) in str(caught.value)


def test_the_module_docstring_states_the_three_jobs_and_names_its_entry_points() -> None:
    """Property: the module docstring states the checks, the once-computed list and the record, and names `compute_targets`, `prepare_run` and `PreRunError`.

    Class: any version of the docstring, searched whole.
    Why it is required: the statements are the deliverable text; a docstring reduced to a step list would
    leave the order and the one-list rule unstated.
    What kills it: deleting or rephrasing a statement, or ceasing to name an entry point.
    Second correct implementation: the sentences in any surrounding prose; containment only is asserted.
    """
    docstring = pre_run.__doc__ or ""

    assert "The checks other systems own" in docstring
    assert "The target list, computed ONCE" in docstring
    assert "The run-start record" in docstring
    assert "compute_targets" in docstring
    assert "prepare_run" in docstring
    assert "PreRunError" in docstring
