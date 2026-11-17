"""The launch bindings: what the engine is told about how this machine runs a run.

A mode decides HOW a run is launched and changes no rule: the same graph, the same stubs, the same
identities. This module turns the configuration's mode into the engine's own options, and it reads the
configuration and nothing else — no environment variable, no hostname, no value taken from an allocation —
so two invocations of the same file ask the engine for the same thing.

Three bindings are stated here, and `launch_plan` is the one door to them. `local` and `batch_job` answer
an ENGINE plan: the mode's engine argument list, which the command turns into the engine invocation it
already knows. `1_job_many_srun_tasks` answers a SUBMISSION plan: the allocation's batch script text, the
script's path in the run output, and the `sbatch --wait --parsable` argv, whose standard output and exit
the command classifies into one of the run's three bands through `submission_band`.

`engine_arguments` deliberately keeps refusing the allocation mode: it answers an ENGINE argument list, and
that mode has none — its engine runs inside the allocation the script creates. A caller holding an engine
argv for the allocation mode would launch the graph on the login node as a run nobody asked for, so the
refusal is the guard rather than an omission.

The two HPC bindings rest on the Rivanna spike's measured arms rather than on an assumption. `sbatch
--wait --parsable` prints the job id on standard output only once a job exists, so a refused submission
prints nothing and exits non-zero: that is the command's refusal band. A job id with exit 0 is a completed
run; a job id with any non-zero exit, 2 included, is a run that started and did not complete, which is the
engine-failure band. Both arms are reproduced by the shim under `tests/workflow/slurm_shim/`.

`run_output_directory` resolves the run-output key here as well, because the engine's `--directory` is the
same fact as the pre-run step's record location: resolving it in one module keeps the engine's working
directory and the record's directory one call, and the command passes the value it resolved.

Not established: that the host has the cores a local configuration names, or that the engine accepts the
option list. The scheduler and the engine are the judges of both.
"""

from __future__ import annotations

import shlex
import sys
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from multidriver_swg.run_configuration.main_configuration import MainConfiguration
from multidriver_swg.run_configuration.path_resolver import load_bindings, resolve

# The three bands, as the numbers the command uses. They are stated here because `submission_band` answers in
# band numbers and `cli` defines the same three; a review test compares the two sets so the pair cannot drift
# silently. `modes` does not import `cli` — `cli` imports this module, and an import the other way is a cycle.
BAND_COMPLETED = 0
BAND_REFUSED = 2
BAND_FAILED = 3

# The allocation's engine options, from DR-9's third bullet: the local executor over the allocation's CPUs,
# with the latency wait both HPC modes carry. The wait is the value the spike's batch script used; the
# engine's own default is 5 seconds, and the spike's visibility probe measured a filesystem delay under its
# resolution rather than justifying 5, so the longer wait is kept.
ALLOCATION_ENGINE_OPTIONS = ("--cores", "--latency-wait")
LATENCY_WAIT_SECONDS = "60"

# The file the allocation mode writes into the run output. The name is the spike's, so a reader of a run
# directory from either tool sees the same entry.
BATCH_SCRIPT_NAME = "engine_batch.sh"

# The thread guards the login node needs. The cluster limits a process to 2000 threads, the engine imports
# numpy, and the spike measured that Snakemake itself does not start without them. The command exports them
# in the engine's environment; the allocation's script exports them for the engine inside the job.
THREAD_GUARD_EXPORT = "export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1"


class ModeError(Exception):
    """The configuration's mode has no launch binding in this command yet.

    A leaf rather than a generic refusal because the command maps it to its refusal band with a message
    naming the mode; the operator's repair is to run in a mode this build launches or to wait for the
    binding, and neither is a property of the run's inputs.
    """


def engine_arguments(configuration: MainConfiguration) -> tuple[str, ...]:
    """Return the engine options this mode adds, from the configuration and nothing else.

    The three branches are: `local` binds `--cores` to the `[local] cores` value; `batch_job` binds the
    SLURM executor and its job limit, latency wait and default resources to the `[slurm]` connection; and
    `1_job_many_srun_tasks` raises `ModeError`, because that mode's binding is a submission rather than an
    engine argument list and the command does not yet call the submission path.
    """
    if configuration.mode == "local":
        assert configuration.local is not None
        return ("--cores", str(configuration.local.cores))
    if configuration.mode == "batch_job":
        assert configuration.slurm is not None and configuration.slurm.jobs is not None
        # The engine runs on this host and submits one job per rule that is not in `localrules`; `all` is
        # the workflow file's one local rule, so it runs here and the twelve stubs run as SLURM jobs.
        return (
            "--executor",
            "slurm",
            "--jobs",
            str(configuration.slurm.jobs),
            "--latency-wait",
            LATENCY_WAIT_SECONDS,
            "--default-resources",
            f"slurm_partition={configuration.slurm.partition}",
            f"slurm_account={configuration.slurm.account}",
            f"runtime={configuration.slurm.time_minutes}",
        )
    if configuration.mode == "1_job_many_srun_tasks":
        raise ModeError(
            "the configuration's mode is '1_job_many_srun_tasks', and that binding is a SUBMISSION rather "
            "than an engine argument list: this module builds its batch script and submission argv, and the "
            "command does not yet run them, so an engine argv here would start the graph on this host instead "
            "of in one allocation"
        )
    raise ModeError(
        f"the configuration's mode is {configuration.mode!r}, and no launch binding is built for it: the "
        "bindings this command knows are 'local', 'batch_job' and '1_job_many_srun_tasks'"
    )


def allocation_engine_arguments(configuration: MainConfiguration) -> tuple[str, ...]:
    """Return the engine options the allocation's batch script passes inside the job.

    Only `1_job_many_srun_tasks` has this binding: the local executor over the allocation's own CPUs, with
    the latency wait. The engine's fixed options — the workflow file, the run output, both profile channels,
    `--rerun-incomplete` and the report pair — are the command's, and `engine_batch_script` composes them.
    """
    if configuration.mode != "1_job_many_srun_tasks":
        raise ModeError(
            f"the configuration's mode is {configuration.mode!r}: an allocation engine line belongs to "
            "'1_job_many_srun_tasks' alone"
        )
    assert configuration.slurm is not None and configuration.slurm.cpus_per_task is not None
    return ("--cores", str(configuration.slurm.cpus_per_task), "--latency-wait", LATENCY_WAIT_SECONDS)


def engine_batch_script(
    configuration: MainConfiguration,
    *,
    snakefile: str | Path,
    run_output: str | Path,
    log_path: str | Path | None = None,
) -> str:
    """Return the `#SBATCH` script that runs the whole graph inside one allocation.

    The directives come from the configuration and nothing else: the partition, the account, the time limit
    in minutes, and one node with one task and the allocation's CPUs. The body exports the thread guards —
    the login node's process limit makes them load-bearing rather than defensive — and runs the engine with
    the local executor over the allocation's CPUs.

    The body also CAPTURES its output into `log_path`, defaulting to a fresh `submission_log_path`. A `tee`
    on the script's own stream puts the scheduler's view and the engine's writing into the one file the
    command reads, while still passing the same bytes on to the scheduler's capture: the lock refusal the
    engine raises is then the command's to read, not a file the scheduler happened to name. The engine line
    stays the script's last line, so the line a reader checks is the line the engine sees.

    Every path in the body is shell-quoted, because the script runs under a shell and a run output may
    carry a space. The report pair is included: an allocation run is a run, and its report is the same
    artifact a local run leaves.
    """
    assert configuration.slurm is not None and configuration.slurm.cpus_per_task is not None
    slurm = configuration.slurm
    snakefile_path = shlex.quote(str(snakefile))
    directory = shlex.quote(str(run_output))
    report = shlex.quote(str(Path(run_output) / "report.html"))
    engine = " ".join(
        [
            shlex.quote(sys.executable),
            "-m snakemake",
            "--snakefile",
            snakefile_path,
            "--directory",
            directory,
            "--profile none",
            "--workflow-profile none",
            "--rerun-incomplete",
            *allocation_engine_arguments(configuration),
            "--report",
            report,
            "--report-after-run",
        ]
    )
    return (
        "#!/bin/bash\n"
        f"#SBATCH --partition={slurm.partition}\n"
        f"#SBATCH --account={slurm.account}\n"
        f"#SBATCH --time={slurm.time_minutes}\n"
        "#SBATCH --nodes=1\n"
        "#SBATCH --ntasks=1\n"
        f"#SBATCH --cpus-per-task={slurm.cpus_per_task}\n"
        "set -euo pipefail\n"
        f"{THREAD_GUARD_EXPORT}\n"
        f"exec > >(tee -a {shlex.quote(str(Path(log_path) if log_path is not None else submission_log_path(run_output)))}) 2>&1\n"
        f"{engine}\n"
    )


def write_engine_batch_script(
    configuration: MainConfiguration,
    *,
    snakefile: str | Path,
    run_output: str | Path,
    log_path: str | Path | None = None,
) -> Path:
    """Write the allocation's script into the run output and return its path.

    The script is ESTATE-SIDE: it is written under the run output the configuration resolves, never into the
    toolkit, because it carries this machine's partition, account and time limit. The parent directory is
    not created — the run output is the engine's working directory and must already exist, exactly as the
    record's write requires.
    """
    path = Path(run_output) / BATCH_SCRIPT_NAME
    path.write_text(
        engine_batch_script(configuration, snakefile=snakefile, run_output=run_output, log_path=log_path),
        encoding="utf-8",
    )
    return path


def submission_arguments(script_path: str | Path) -> tuple[str, ...]:
    """Return the submission argv for an allocation script: `sbatch --wait --parsable <script>`.

    `--wait` keeps the command in the allocation until the job returns, which is what lets the command report
    a band at all; `--parsable` prints the job id on standard output once a job exists, which is the signal
    `submission_band` reads.
    """
    return ("sbatch", "--wait", "--parsable", str(script_path))


def submission_band(stdout: str | None, returncode: int) -> int:
    """Return the command's band for a finished `sbatch --wait --parsable` run.

    The measured arms: a refused submission prints NOTHING on standard output and exits non-zero, which is
    the refusal band — no job exists, so nothing ran. A job id with exit 0 is a completed run. A job id with
    any non-zero exit, 2 included, is a run that started and did not complete: the job exists, so it cannot
    read as a refusal. `--parsable` may append `;cluster`, so only the first field is read.
    """
    job_id = ""
    for line in (stdout or "").splitlines():
        candidate = line.strip().split(";", 1)[0].strip()
        if candidate:
            job_id = candidate
    if not job_id:
        return BAND_REFUSED
    return BAND_COMPLETED if returncode == BAND_COMPLETED else BAND_FAILED


@dataclass(frozen=True)
class EnginePlan:
    """A run the command launches directly: the mode's engine argument list, and nothing else.

    The command owns the engine invocation's fixed options; this plan carries only the options the
    CONFIGURATION's mode adds, so the command's local and batch-job behaviour is the behaviour it had
    before the allocation mode existed.
    """

    arguments: tuple[str, ...]


@dataclass(frozen=True)
class SubmissionPlan:
    """A run the command submits: the allocation's script text, where it goes, its own log, and the submission argv.

    `script` is the text `engine_batch_script` produced, `script_path` is where the command writes it (the
    run output, estate-side), `log_path` is where THIS attempt's scheduler-and-engine output is captured —
    a path inside the run output carrying a fresh token, so no attempt ever reads a previous attempt's log —
    and `arguments` is the `sbatch --wait --parsable` argv a scheduler answers. The command writes the one,
    runs the other, and reads the log the script directs; the mode module never touches the filesystem.
    """

    script: str
    script_path: Path
    log_path: Path
    arguments: tuple[str, ...]


def submission_log_path(run_output: str | Path) -> Path:
    """Return this attempt's allocation log path: inside the run output, carrying a fresh token.

    The token is what makes the path per-submission. A fixed name would let a fresh attempt read what an
    earlier one wrote — the lock refusal that attempt met, quoted back as this attempt's recourse — so the
    uniqueness is the contract, not a nicety. The path stays in the run output because that is estate-side
    and travels with the run; nothing is written here.
    """
    return Path(run_output) / f"allocation-{uuid4().hex}.log"


def launch_plan(
    configuration: MainConfiguration, *, snakefile: str | Path, run_output: str | Path
) -> EnginePlan | SubmissionPlan:
    """Return what the command must do for this mode: launch the engine, or submit one allocation.

    This is the mode table's one answer to "how does this run start". `local` and `batch_job` answer an
    engine plan built by `engine_arguments`; `1_job_many_srun_tasks` answers a submission plan built by
    `engine_batch_script` and `submission_arguments`. The command neither knows nor decides which modes
    are which — it runs the plan it is handed, which is why a later mode is a new plan here and no change
    in the command.

    The plan carries paths but writes nothing: the command writes the script and runs the submission, so
    this function stays a value producer.
    """
    if configuration.mode == "1_job_many_srun_tasks":
        script_path = Path(run_output) / BATCH_SCRIPT_NAME
        log_path = submission_log_path(run_output)
        return SubmissionPlan(
            script=engine_batch_script(configuration, snakefile=snakefile, run_output=run_output, log_path=log_path),
            script_path=script_path,
            log_path=log_path,
            arguments=submission_arguments(script_path),
        )
    return EnginePlan(arguments=engine_arguments(configuration))


def run_output_directory(configuration: MainConfiguration) -> Path:
    """Resolve this project's run-output key through the machine's binding document.

    The result is the engine's working directory, the directory the pre-run step writes the run-start
    record into, and the directory the report is written under. Resolving it here rather than in the
    command keeps one spelling of the key and one resolver call.

    A document that cannot be read, or that does not bind the key, raises the resolver's or the operating
    system's error; the command translates it into its refusal band, because the run cannot start without
    a working directory.
    """
    bindings_path = Path(configuration.estate_root) / configuration.bindings
    bindings = load_bindings(bindings_path)
    return resolve(configuration.path_key("run_output"), bindings, bindings_path)
