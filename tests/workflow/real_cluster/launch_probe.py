"""Launch a probe workflow with the command's own bindings, in one of eight arms.

The command starts the toolkit's workflow file; a measurement has to start a DIFFERENT file (the probe, or the
checkpoint test workflow) in exactly the way the command starts its own, or what it measures is not what the
command does. So this launcher loads the configuration, runs the real pre-run step, and then takes the
command's own launch plan and changes only the workflow file:

- for `local` and `batch_job`, the argument list `cli.engine_command` returns, with only the value after
  `--snakefile` replaced;
- for `1_job_many_srun_tasks`, the submission plan `modes.launch_plan` returns for the probe's file, whose
  script is written into the run output and submitted with the plan's own argument list.

The engine always runs in `cli.engine_environment` of the process's environment. The launcher prints the
argument list it ran and the exit status, and exits with that status.

The arms:

- `run` — launch the probe;
- `noop` — launch it again on an output a `run` already finished, so nothing is to be done;
- `dryrun` — launch it with the engine's dry-run flag, so nothing runs;
- `fail` — make the first rule's directory read-only first, so that rule's job fails in either mode;
- `checkpoint-noruling` — launch the checkpoint test workflow over a selection fixture with no pooling entry
  for the statistic its licence rejects;
- `checkpoint-resume` — add that entry to the fixture and launch the checkpoint workflow again on the same
  output;
- `graph-checkpoint-noruling` — launch the probe over the toolkit's own graph with a licence that requires
  `compound.rainfall_depth`, which the run's selection lacks, so the run ends after the licence. It requires
  the configuration's selection file to be absent, and writes `measurement/licence_required.json`, which the
  probe reads at every parse;
- `graph-checkpoint-resume` — copy the file named by `--selection-from` to the configuration's selection
  path and launch the probe over the graph again on the same output, so the run continues from the merge.

Not established: that a launch succeeds. The exit status and the files the run leaves are the evidence, and
the collector prints them.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

from multidriver_swg.run_configuration.main_configuration import MainConfiguration
from multidriver_swg.run_configuration.path_resolver import load_bindings, resolve
from multidriver_swg.workflow.cli import engine_command, engine_environment
from multidriver_swg.workflow.modes import SubmissionPlan, launch_plan, run_output_directory
from multidriver_swg.workflow.pre_run import prepare_run

ARMS = (
    "run",
    "noop",
    "dryrun",
    "fail",
    "checkpoint-noruling",
    "checkpoint-resume",
    "graph-checkpoint-noruling",
    "graph-checkpoint-resume",
)

# The two workflow files, beside this module. A launcher run from a copy of the toolkit finds that copy's files.
MATERIAL_DIRECTORY = Path(__file__).resolve().parent

PROBE_NAME = "probe.smk"

CHECKPOINT_PROBE_NAME = "checkpoint_probe.smk"

# The selection fixture the checkpoint workflow reads, in the engine's working directory.
FIXTURE_NAME = "selection_fixture.json"

# The statistic the checkpoint workflow's licence rejects pooling for. It is written into the fixture's
# `rejected` key, and the licence copies it, so the fixture is the one statement of it.
REJECTED_STATISTIC = "compound.rainfall_depth"

# The rule whose directory the `fail` arm makes read-only: the table's second row, which the shortened
# target list also builds, so the failure happens in either target list.
FAILING_RULE_DIRECTORY = "combined_event_identification"

_CHECKPOINT_ARMS = ("checkpoint-noruling", "checkpoint-resume")

# The file, in the run output's `measurement` directory, that makes the probe's licence require entries.
LICENCE_REQUIRED_NAME = "licence_required.json"

# The one entry the graph arms' licence requires: a fit the first launch's selection lacks.
REQUIRED_ENTRY = "compound.rainfall_depth"


def snakefile_for(arm: str, directory: Path = MATERIAL_DIRECTORY) -> Path:
    """Return the workflow file an arm launches: the checkpoint workflow for the two checkpoint arms, else the probe."""
    if arm not in ARMS:
        raise ValueError(f"arm {arm!r} is not one of {', '.join(ARMS)}")
    return directory / (CHECKPOINT_PROBE_NAME if arm in _CHECKPOINT_ARMS else PROBE_NAME)


def probe_arguments(
    configuration: MainConfiguration, *, run_output: Path, snakefile: Path, dry_run: bool = False
) -> list[str]:
    """Return the command's own engine argument list with `snakefile` in place of its workflow file.

    Only the value after `--snakefile` changes, and a dry run appends the engine's `-n`. Everything else,
    the mode's options and the report pair included, is what the command would pass.
    """
    command = engine_command(configuration, run_output=run_output)
    command[command.index("--snakefile") + 1] = str(snakefile)
    if dry_run:
        command.append("-n")
    return command


def _dry_run_script(script: str) -> str:
    """Return the allocation's script with the engine's dry-run flag on its last line, which is the engine line."""
    return script.rstrip("\n") + " -n\n"


def _write_fixture(run_output: Path, *, pooling: Sequence[str]) -> None:
    """Write the checkpoint workflow's selection fixture into the run output, replacing any earlier one."""
    document = {"rejected": [REJECTED_STATISTIC], "pooling": list(pooling)}
    (run_output / FIXTURE_NAME).write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")


def _write_licence_requirement(run_output: Path) -> None:
    """Write the requirement the probe reads at every parse into the run output's `measurement` directory."""
    directory = run_output / "measurement"
    directory.mkdir(exist_ok=True)
    (directory / LICENCE_REQUIRED_NAME).write_text(json.dumps([REQUIRED_ENTRY]), encoding="utf-8")


def _selection_path(configuration: MainConfiguration) -> Path:
    """Resolve this project's selection key through the machine's binding document, as the pre-run step does."""
    bindings_path = Path(configuration.estate_root) / configuration.bindings
    return resolve(configuration.path_key("selection"), load_bindings(bindings_path), bindings_path)


def _prepare_selection(arm: str, configuration: MainConfiguration, selection_from: Path | None) -> None:
    """Put the configuration's selection file where the arm needs it, BEFORE the pre-run step reads it.

    The pre-run step decides the target list and the run-start record from whether the file exists, so the
    resume arm's copy has to come first, and the no-ruling arm refuses a file that is already there rather
    than deleting an operator's selection. An arm other than the resume arm takes no selection file.
    """
    if arm == "graph-checkpoint-noruling":
        selection = _selection_path(configuration)
        if selection.exists():
            raise ValueError(f"arm {arm!r} requires the selection file to be absent, and {selection} exists")
    elif arm == "graph-checkpoint-resume":
        if selection_from is None:
            raise ValueError(f"arm {arm!r} requires a selection file to copy")
        selection = _selection_path(configuration)
        selection.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(selection_from, selection)
    if selection_from is not None and arm != "graph-checkpoint-resume":
        raise ValueError(f"arm {arm!r} takes no selection file")


def _prepare_arm(arm: str, run_output: Path) -> None:
    """Do what an arm needs in the run output before the launch: a read-only directory, the fixture, or the licence requirement."""
    if arm == "fail":
        directory = run_output / FAILING_RULE_DIRECTORY
        directory.mkdir(exist_ok=True)
        directory.chmod(0o555)
    elif arm == "checkpoint-noruling":
        _write_fixture(run_output, pooling=())
    elif arm == "checkpoint-resume":
        _write_fixture(run_output, pooling=(REJECTED_STATISTIC,))
    elif arm == "graph-checkpoint-noruling":
        _write_licence_requirement(run_output)


def launch(
    configuration_path: Path,
    arm: str,
    *,
    selection_from: Path | None = None,
    directory: Path = MATERIAL_DIRECTORY,
    environ: Mapping[str, str] | None = None,
) -> int:
    """Run the pre-run step, prepare the arm, launch the probe the command's way, and return the exit status.

    `directory` is where the workflow files are, and `environ` is the environment the engine's is built from
    (the process's own when none is given). `selection_from` is the selection file the resume arm copies to
    the configuration's selection path. The pre-run step is the real one, so the run-start record and its
    checks are the command's. The launch is run in the run output, which is the engine's working directory
    under the command too.
    """
    snakefile = snakefile_for(arm, directory)
    configuration = MainConfiguration.load(configuration_path)
    run_output = run_output_directory(configuration)
    _prepare_selection(arm, configuration, selection_from)
    prepare_run(configuration, configuration_path)
    _prepare_arm(arm, run_output)

    dry_run = arm == "dryrun"
    plan = launch_plan(configuration, snakefile=snakefile, run_output=run_output)
    if isinstance(plan, SubmissionPlan):
        script = _dry_run_script(plan.script) if dry_run else plan.script
        plan.script_path.write_text(script, encoding="utf-8")
        arguments = list(plan.arguments)
    else:
        arguments = probe_arguments(configuration, run_output=run_output, snakefile=snakefile, dry_run=dry_run)

    print(f"argument list: {shlex.join(arguments)}", flush=True)
    completed = subprocess.run(
        arguments,
        cwd=run_output,
        env=engine_environment(os.environ if environ is None else environ),
        check=False,
    )
    print(f"exit status: {completed.returncode}", flush=True)
    return completed.returncode


def main(argv: Sequence[str] | None = None) -> int:
    """Parse the command line, launch, and return the launch's exit status."""
    parser = argparse.ArgumentParser(description="Launch a probe workflow with the command's own bindings.")
    parser.add_argument("configuration", type=Path, help="this machine's main configuration file")
    parser.add_argument("--arm", required=True, choices=ARMS)
    parser.add_argument(
        "--selection-from", type=Path, help="the selection file the graph-checkpoint-resume arm copies in"
    )
    arguments = parser.parse_args(argv)
    if (arguments.arm == "graph-checkpoint-resume") != (arguments.selection_from is not None):
        parser.error("--selection-from is required by the graph-checkpoint-resume arm and taken by no other")
    return launch(arguments.configuration, arguments.arm, selection_from=arguments.selection_from)


if __name__ == "__main__":
    sys.exit(main())
