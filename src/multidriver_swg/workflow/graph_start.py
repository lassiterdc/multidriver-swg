"""The graph-start gate: what must be true before an engine parses this workflow and schedules anything.

An engine parse is the last moment before work is scheduled, and it is reachable directly — an engine
started by hand has no command to run checks first. So the workflow file calls this gate at its head at
every parse, and the gate refuses, in one fixed order, the facts a run cannot recover from:

1. a run-start record that is absent or does not parse as the form (the record is how the parse learns
   what the run is);
2. the running package not being the package of a git checkout, including the case where an unrelated
   enclosing repository makes `git rev-parse --show-toplevel` succeed while the imported package is a copy
   installed inside it;
3. the checkout's commit differing from the commit the project declares;
4. the checkout holding a working-tree change or an untracked file its ignore rules do not cover.

The order is the contract rather than a convenience. Each refusal names a DIFFERENT repair — write the
record, run from a checkout, check out the declared commit, clean the tree — and reporting a later fact
first sends the operator to repair something that is not yet the blocker. The commit's own source is the
revision gate; this module only places the calls in the order and converts their refusals into the
engine's error, because a parse-time gate's exception is the only channel an engine invocation has.

The project's declared revision is read from `toolkit_revision.toml` in the project directory, which is
resolved from the record: the record names the configuration, the configuration names the estate and the
binding document, the binding document resolves the project directory, and the declaration lives there.
That chain is the record's whole purpose at parse time, and every link is another module's mechanism —
this file composes them and decides only the order.

Not established: that the configuration, estate or declaration is the one the operator intended; the gate
checks that they are mutually consistent and that the executing package matches the declaration. Nor does
it check the run's outputs, its selection artifact's form or the mode bindings: those are the preparing
step's and the command's, and this gate is the backstop for an engine invoked without them.
"""

from __future__ import annotations

from pathlib import Path

from snakemake.exceptions import WorkflowError

from multidriver_swg.run_configuration.main_configuration import MainConfiguration, MainConfigurationError
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
from multidriver_swg.workflow.run_start import RUN_START_FILE, RunStart, RunStartError, read_run_start

# The declaration's fixed file name inside the project directory. It is stated here rather than read from
# the revision gate, whose reader takes the path it is handed; this module is the one that knows WHERE the
# project keeps it, and the configuration's project_directory key is what makes that location resolvable.
_REVISION_FILE = "toolkit_revision.toml"


def require_graph_start(working_directory: str | Path | None = None) -> RunStart:
    """Run the gate's four checks in order and return the record they admit.

    `working_directory` is where the run-start record lives — the engine's working directory, which is the
    run output. It defaults to the process's working directory because that IS the engine's working
    directory under the command's `--directory` option, and a caller that passes one is a test placing the
    gate over a temporary tree.

    Raises the engine's `WorkflowError`, which the engine turns into a non-zero exit with no job run. Every
    refusal this gate meets is re-raised under that class with the module's own text, so an operator sees
    the field, the path and the repair rather than a stack from a helper.

    Deliberately NOT a check that can be skipped by a flag or an environment variable: a gate an engine
    invocation can turn off is a gate that is off in exactly the invocation nobody prepared.
    """
    directory = Path(working_directory) if working_directory is not None else Path.cwd()
    record = _read_record(directory)
    configuration = _read_configuration(record)
    declared = _declared_revision(configuration)
    _require_checkout(declared)
    return record


def _read_record(directory: Path) -> RunStart:
    """Read the run-start record, refusing an absent file as this gate's first check."""
    path = directory / RUN_START_FILE
    try:
        return read_run_start(path)
    except OSError as error:
        raise WorkflowError(
            f"graph start refused: no readable run-start record at {path} ({error}), so this parse cannot "
            "learn what the run builds; prepare the run (which writes the record) or start it through the "
            "command"
        ) from error
    except RunStartError as error:
        raise WorkflowError(f"graph start refused: {error}") from error


def _read_configuration(record: RunStart) -> MainConfiguration:
    """Load the configuration the record names, refusing one the configuration module refuses."""
    try:
        return MainConfiguration.load(record.main_configuration)
    except (MainConfigurationError, OSError) as error:
        raise WorkflowError(
            f"graph start refused: the run-start record names the configuration "
            f"{record.main_configuration!r}, which cannot be read: {error}"
        ) from error


def _declared_revision(configuration: MainConfiguration) -> DeclaredRevision:
    """Resolve the project directory through the estate and read the revision it declares.

    The chain is the record's reason for existing: the configuration names the estate root and the
    binding document, the resolver answers the project directory key from that document, and the
    declaration is a file inside the directory. A failure at any link is this gate's refusal, because the
    parse cannot compare the running commit without the declaration.
    """
    bindings_path = Path(configuration.estate_root) / configuration.bindings
    try:
        bindings = load_bindings(bindings_path)
        project_directory = resolve(configuration.path_key("project_directory"), bindings, bindings_path)
    except (PathResolutionError, OSError, UnicodeDecodeError) as error:
        # Three classes, because the loader's own refusal covers only the document's FORM: a missing
        # document reaches here as the operating system's error and a document that is not UTF-8 reaches
        # here as the codec's, and neither is a `PathResolutionError`. All three are the same fact to the
        # operator — this document cannot be read — so all three become the gate's one refusal.
        raise WorkflowError(
            f"graph start refused: the binding document {bindings_path} named by the configuration cannot "
            f"be read, or does not resolve this project's directory: {error}"
        ) from error

    try:
        return load_declared_revision(project_directory / _REVISION_FILE)
    except (RevisionGateError, OSError) as error:
        raise WorkflowError(
            f"graph start refused: the project directory {project_directory} holds no readable "
            f"{_REVISION_FILE}: {error}"
        ) from error


def _require_checkout(declared: DeclaredRevision) -> None:
    """Run the gate's second, third and fourth checks in their order through the revision gate."""
    try:
        require_revision_gate(running_package_directory(), declared)
    except RevisionGateError as error:
        raise WorkflowError(f"graph start refused: {error}") from error
