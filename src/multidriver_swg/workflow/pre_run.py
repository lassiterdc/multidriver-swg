"""The pre-run step: check what other systems own, compute the target list once, and write the run-start record.

The command runs this before it starts an engine. Three jobs, in the order the run's definition states:

1. **The checks other systems own.** The binding document and the three path keys it must bind, through the
   path resolver; then the revision gate's three checks — the running package belongs to a checkout, the
   checkout's commit is the one the project declares, and the checkout is clean; then the selection
   artifact's form, when the run has one. Both callers of those mechanisms exist because a parse-time gate
   cannot run before `sbatch`: a dirty tree must refuse here, at band 2, rather than queue an allocation
   whose engine refuses at parse.
2. **The target list, computed ONCE** from the rule table and three facts: whether the selection file
   exists, whether the configuration names an automatic-selection procedure, and which rows read the
   selection. One list is computed and then written into the record, so the step that prepares a run and
   the engine that runs it cannot disagree about what the run builds. The run's end-of-run record is the
   last target in every branch: the final rule depends on every other target of the list and nothing
   depends on it, so a list that omitted it would build every step and never close the run.
3. **The run-start record**, written into the run output the configuration resolves, in the form the
   graph-start gate reads at every parse.

The surface is three names: `prepare_run`, which performs the step and returns the record it wrote;
`compute_targets`, the pure function the step calls once; and `PreRunError`, the one class every refusal
arrives under.

The target list's three branches are the whole of the cut, and each is stated in the shape: a present
selection builds every rule's declared outputs; an absent selection with the judgement kept builds every
rule that neither reads the selection nor is downstream of one, plus the attribution row; and a
configuration naming an automatic-selection procedure is refused, because the toolkit provides no such
procedure and a list computed for one would claim a choice nothing can make.

Nothing here prompts, sleeps or waits: the step either finishes with a record or raises, and the command
turns the raise into its refusal band. Every refusal is a `PreRunError` naming the file or field its
repair acts on, because the command's report is the operator's only channel.

Not established: that the targets it computes can be built, that the estate's directories exist beyond the
run output it writes into, or that the engine will accept the record. The graph-start gate re-reads the
record and re-runs the revision checks, and the engine is the judge of the targets.
"""

from __future__ import annotations

from pathlib import Path

from multidriver_swg.run_configuration.main_configuration import PATH_KEY_KINDS, MainConfiguration
from multidriver_swg.run_configuration.path_resolver import (
    PathResolutionError,
    load_bindings,
    resolve,
)
from multidriver_swg.run_configuration.revision_gate import (
    DeclaredRevision,
    RevisionGateError,
    load_declared_revision,
    require_revision_gate,
    running_package_directory,
)
from multidriver_swg.run_configuration.selection_artifact import SelectionArtifactError, load_selection
from multidriver_swg.workflow.end_of_run import END_OF_RUN_RECORD
from multidriver_swg.workflow.graph import GRAPH, GraphLookupError, downstream_of, judgement_rules, rule_named
from multidriver_swg.workflow.run_start import RUN_START_FILE, RunStart

# The declaration's fixed file name inside the project directory, as the configuration module's contract
# states it. The graph-start gate spells the same name for its own read; the two cannot share a constant
# without one of them importing the other, and each names the file the other reads.
REVISION_FILE = "toolkit_revision.toml"

# The attribution rule, named once because the shortened list names it explicitly: its inputs are computed
# from the list itself, so it is kept even when the branch it would read from is cut.
ATTRIBUTION_RULE = "method_attribution"


class PreRunError(Exception):
    """The pre-run step refused the run: a check failed, or the list or the record could not be produced.

    One class for every refusal, because the command maps all of them to one band — the run did not start —
    and the message names the file or field the operator repairs.
    """


def compute_targets(*, selection_present: bool, delegated: bool) -> tuple[str, ...]:
    """Return the paths the run builds, from the three facts the shape names and nothing else.

    - `selection_present` — the selection artifact's file exists.
    - `delegated` — the configuration names an automatic-selection procedure. The pure function does not
      refuse it: it answers the branch the shape names — the list is NOT shortened — and the refusal of a
      configuration that names one belongs to `prepare_run`, which knows the toolkit provides none.

    A present selection, or a delegated choice, targets every rule's declared outputs. An absent selection
    with the judgement kept targets every rule that neither reads the selection nor is downstream of a
    reader, plus the attribution rule's output, whose inputs are that list. The end-of-run record is the
    LAST target of both lists, because the final rule depends on every other target and nothing depends on
    it: a list without it would build every step and never produce the run's record.

    The list is in table order, so two runs' lists are comparable and a shortened run's `all` rule is built
    from the shortened list.

    The pure function deliberately takes no configuration, no path and no filesystem: the caller answers
    the two booleans, and a test can drive both branches without an estate.
    """
    all_targets = tuple(f"{rule.name}/{output}" for rule in GRAPH for output in rule.outputs)
    if selection_present or delegated:
        return (*all_targets, END_OF_RUN_RECORD)

    excluded = {rule.name for rule in judgement_rules()}
    for reader in tuple(excluded):
        excluded |= {rule.name for rule in downstream_of(reader)}

    kept = [rule for rule in GRAPH if rule.name not in excluded]
    targets = [f"{rule.name}/{output}" for rule in kept for output in rule.outputs]
    attribution = rule_named(ATTRIBUTION_RULE)
    targets.extend(
        f"{attribution.name}/{output}"
        for output in attribution.outputs
        if f"{attribution.name}/{output}" not in targets
    )
    return (*targets, END_OF_RUN_RECORD)


def prepare_run(configuration: MainConfiguration, configuration_path: str | Path) -> RunStart:
    """Run the step: checks, one target list, one record; return the record it wrote.

    `configuration` is the validated configuration the command was given, and `configuration_path` is the
    path it was given at, resolved here to the absolute path the record carries. The run output, the
    project directory and the selection file are all resolved from the binding document the configuration
    names, so no path in this step comes from the process's working directory or environment.

    Raises `PreRunError` naming the file or field every refusal acts on: a binding document that cannot be
    read or does not bind one of the three keys; a projection whose revision declaration cannot be read; a
    revision gate refusal; a selection artifact the artifact's own reader refuses; a configuration that
    names an automatic-selection procedure; a `version_stacking.rerun_rules` name the rule table does not
    hold; and an unwritable run output.

    The record carries the configuration's `version_stacking.allow_mixed_versions`. The rule list is not
    recorded: its one reader is the command, which reads the configuration at every invocation.
    """
    keys = _resolved_keys(configuration)
    run_output = keys["run_output"]
    project_directory = keys["project_directory"]
    selection_path = keys["selection"]

    selection_present = _selection_present(selection_path)
    delegated = configuration.judgement.automatic_selection is not None
    if delegated:
        raise PreRunError(
            f"the configuration's judgement.automatic_selection names the procedure "
            f"{configuration.judgement.automatic_selection!r}, and this toolkit provides no such procedure: "
            "the interactive decision surfaces are the design's to provide, so a run that would delegate the "
            "model choice cannot be prepared; remove the field to keep the judgement stage"
        )

    for name in configuration.version_stacking.rerun_rules:
        try:
            rule_named(name)
        except GraphLookupError as error:
            raise PreRunError(
                f"the configuration's version_stacking.rerun_rules names {name!r}, which is not a rule of the "
                f"workflow: {error}"
            ) from error

    targets = compute_targets(selection_present=selection_present, delegated=False)

    _require_declared_revision(project_directory)
    _require_selection_form(selection_path, present=selection_present)

    record = RunStart(
        main_configuration=str(Path(configuration_path).resolve()),
        targets=targets,
        selection=str(selection_path) if selection_present else None,
        judgement="present" if selection_present else "kept-absent",
        allow_mixed_versions=configuration.version_stacking.allow_mixed_versions,
    )
    try:
        record.write(run_output / RUN_START_FILE)
    except OSError as error:
        raise PreRunError(
            f"the run-start record cannot be written into the run output {run_output}: {error}"
        ) from error
    return record


def _resolved_keys(configuration: MainConfiguration) -> dict[str, Path]:
    """Load the binding document and resolve the three fixed keys, refusing a document or key that fails.

    The document's reader covers the document's FORM; a missing document and a document that is not UTF-8
    reach here as the operating system's and the codec's exceptions, so all three are caught and become
    this step's one refusal naming the document.
    """
    bindings_path = Path(configuration.estate_root) / configuration.bindings
    try:
        bindings = load_bindings(bindings_path)
        keys = {kind: resolve(configuration.path_key(kind), bindings, bindings_path) for kind in PATH_KEY_KINDS}
    except (PathResolutionError, OSError, UnicodeDecodeError) as error:
        raise PreRunError(
            f"the binding document {bindings_path} cannot be read, or does not bind one of this project's "
            f"three path keys: {error}"
        ) from error
    return keys


def _selection_present(selection_path: Path) -> bool:
    """Whether the selection artifact's file exists, refusing a path the filesystem cannot inspect.

    `Path.is_file` answers False for a missing file, but it RAISES for some names: a component longer than
    the filesystem's limit, a permission fault on a directory on the way. Each of those is a fact about the
    PATH rather than a run without a selection, so every `OSError` becomes this step's refusal naming the
    path — reading one as "no selection" would build the shortened graph for a run whose artifact exists and
    could not be looked at. A name under a regular file raises nothing and is read as an absent selection.
    """
    try:
        return selection_path.is_file()
    except OSError as error:
        raise PreRunError(f"the selection artifact's path {selection_path} cannot be inspected: {error}") from error


def _require_declared_revision(project_directory: Path) -> DeclaredRevision:
    """Read the project's revision declaration and run the gate's three checks against it."""
    try:
        declared = load_declared_revision(project_directory / REVISION_FILE)
    except (RevisionGateError, OSError, UnicodeDecodeError) as error:
        raise PreRunError(
            f"the project directory {project_directory} holds no readable {REVISION_FILE}: {error}"
        ) from error
    try:
        require_revision_gate(running_package_directory(), declared)
    except RevisionGateError as error:
        raise PreRunError(str(error)) from error
    return declared


def _require_selection_form(selection_path: Path, *, present: bool) -> None:
    """Validate the selection artifact's form when the run has one; a reader refusal is this step's refusal."""
    if not present:
        return
    try:
        load_selection(selection_path)
    except (SelectionArtifactError, OSError, UnicodeDecodeError) as error:
        raise PreRunError(
            f"the selection artifact {selection_path} is not the form this toolkit reads: {error}"
        ) from error
