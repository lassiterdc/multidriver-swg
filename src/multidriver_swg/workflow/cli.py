"""The console command: one configuration path in, one engine run out, three exit bands.

The command is the only supported way to start a run. It takes the path of this machine's main
configuration and nothing else, runs the pre-run step, and then runs the plan the mode table answers:
either the engine, on this host, or one allocation submitted with `sbatch --wait --parsable`. It reports
one of three outcomes either way:

- **0** — the engine ran and the run completed;
- **3** — the engine started and the run did not complete, including a parse-time refusal the graph-start
  gate raises after this command's own checks passed;
- **2** — the command refused to start: a usage error, an invalid configuration, any pre-run refusal, no
  mode binding, or an engine that is not importable.

The numbers are chosen so that "nothing ran" and "the run failed" cannot be confused: a scheduler that
requeues the command, or a wrapper that reads the exit, can tell a refusal from a failure without parsing
a message.

THE ENVIRONMENT IS CONSTRUCTED, NOT INHERITED AS IS. The engine is started with a mapping built from this
process's environment in which every variable whose name begins `SNAKEMAKE_` is removed, and with the three
BLAS thread guards set to `1` where the operator left one unset or empty (an exported value is kept). The
profile channels are closed by the command's own `--profile none` and `--workflow-profile none`, so a
profile named in the environment cannot change the driver's behaviour; and importing the engine imports
`numpy`, which starts one thread per core unless the guards are set. This process's own environment is not
changed, so nothing the command does leaks into a caller that inspects it afterwards.

THE ENGINE IS STARTED WITH NO TARGET. Its default target is the workflow file's first rule, `all`, whose
input is the run-start record's target list; passing a target here would be a second statement of what the
run builds.

A STALE LOCK IS THE OPERATOR'S CALL. After an engine is killed, its lock files hold only file names, so a
liveness check cannot say whether the engine is dead. On the engine's `LockException` this command exits
band 3 and prints the exact recourse — the engine's own `--unlock` invocation, with the workflow file and
the run output filled in, every path quoted for a shell — and never unlocks on its own.

AN INTERRUPT ENDS AT A BAND, NEVER AT A TRACEBACK. `Ctrl-C`, or the same signal to the command's process
group, arrives at the engine too, because it runs in that group. The command catches the interrupt, waits
for the engine's own shutdown, and returns band 3 with one line: band 3 is already "the engine started and
the run did not complete", so no fourth outcome appears. A SECOND interrupt during that wait means the
operator wants to stop waiting; the command then terminates the engine — so it is not left running
unattended — and ends at band 3 rather than ignoring the second signal. An interrupt before the engine is
started returns band 2, because nothing ran and band 3 would claim an engine that never launched.

Not established: that the engine's run succeeds, that the report is written, or that the target list the
record names can be built. The engine's exit and its own logs are the report of those facts.
"""

from __future__ import annotations

import argparse
import contextlib
import importlib.util
import os
import shlex
import signal
import subprocess
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

from multidriver_swg import __version__
from multidriver_swg.run_configuration.main_configuration import MainConfiguration, MainConfigurationError
from multidriver_swg.run_configuration.path_resolver import PathResolutionError
from multidriver_swg.workflow.modes import (
    ModeError,
    SubmissionPlan,
    engine_arguments,
    launch_plan,
    rerun_arguments,
    run_output_directory,
    submission_band,
)
from multidriver_swg.workflow.pre_run import PreRunError, prepare_run

EXIT_COMPLETED = 0

EXIT_ENGINE_FAILED = 3

EXIT_REFUSED = 2

# The command's name, as the distribution and the console script spell it. It is printed by `--version`
# and by the usage line, so the two cannot disagree about what resolved.
PROGRAM = "multidriver-swg"

# The workflow file ships beside this module (package data), so the path is the module's own directory.
# A relocated file would move the answer, which is why the derivation is stated here.
SNAKEFILE = Path(__file__).resolve().parent / "Snakefile"

REPORT_FILE = "report.html"

# The three thread guards. `numpy` is imported by the engine and starts one thread per core unless these
# are set; the command sets only the ones the operator left unset or empty.
BLAS_GUARD_VARIABLES = ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS")

# The engine's lock error's class name, as its message spells it. A message that spells it otherwise is not
# detected, and the command exits band 3 without the recourse line — the run still failed, and the bound is
# stated rather than hidden.
_LOCK_ERROR_NAME = "LockException"

# The engine runs in its OWN session, so this command can signal the whole run — the engine and the shell
# jobs its local executor started — without signalling itself or its own caller. A terminal `Ctrl-C` reaches
# only the command's process group, which is why the first interrupt FORWARDS SIGINT to the engine's group:
# the engine then performs its own shutdown, terminates its jobs and releases its lock.

# How long the command waits, after forwarding the first interrupt, for the engine's clean shutdown. The
# forwarded signal asks the engine to stop, and a local engine that received it stops within seconds; thirty
# seconds is far beyond that and well inside an operator's patience. The bound exists because a signal
# delivered to the command alone (`kill -INT {pid}`, a wrapper's stop, a CI cancellation) originally never
# reached the engine, and a wait for a shutdown nobody asked for would hang the command with a live engine
# behind it.
_INTERRUPT_SHUTDOWN_SECONDS = 30.0

# How long a termination waits for the engine's group to honour SIGTERM before the group is killed. The
# engine has already been signaled, so this only bounds the remaining shutdown before the command stops
# waiting for good.
_INTERRUPT_TERMINATE_SECONDS = 10.0

# How long the final drain waits for the engine's pipes to close after the group is killed. A shell job
# inherits the engine's pipes; a writer that somehow survives the group signal would otherwise hold the
# command in `communicate` after every band it owes has been decided. The drain is closed on expiry and the
# command returns without the surviving writer's output.
_INTERRUPT_DRAIN_SECONDS = 10.0


def engine_environment(environ: Mapping[str, str]) -> dict[str, str]:
    """Return the environment the engine is started with, built from `environ` and never mutating it.

    Every `SNAKEMAKE_`-prefixed variable is dropped, because the profile and other engine settings the
    operator exported for a different invocation would otherwise reach this child; the driver's own
    `--profile none` and `--workflow-profile none` close only the channels it passes. The three BLAS guards
    are set to `1` where the mapping leaves them unset or empty, and an exported value is kept: the operator
    who set a thread count meant it.

    The rule for the engine's variables is a property of the NAMESPACE, not a list of names, so a future
    `SNAKEMAKE_*` variable is dropped without this function being edited.
    """
    environment = {name: value for name, value in environ.items() if not name.startswith("SNAKEMAKE_")}
    for name in BLAS_GUARD_VARIABLES:
        if not environment.get(name):
            environment[name] = "1"
    return environment


def engine_command(configuration: MainConfiguration, *, run_output: Path) -> list[str]:
    """Return the engine invocation this run's configuration asks for.

    The fixed options are this command's, in the order the shape states: the engine as a module of THIS
    interpreter, the workflow file, the run output as its working directory, both profile channels closed,
    `--rerun-incomplete` so a partial output left by a killed engine does not stop the next attempt, the
    rules the configuration's `[version_stacking]` table asks to rebuild (`modes.rerun_arguments`), the
    mode's options, and the report written after a successful run. The mode's options come from
    `modes.engine_arguments`, which reads the configuration and nothing else.

    No target is passed: the workflow file's first rule is the default and its input is the record's list.
    """
    return [
        sys.executable,
        "-m",
        "snakemake",
        "--snakefile",
        str(SNAKEFILE),
        "--directory",
        str(run_output),
        "--profile",
        "none",
        "--workflow-profile",
        "none",
        "--rerun-incomplete",
        *rerun_arguments(configuration),
        *engine_arguments(configuration),
        "--report",
        str(run_output / REPORT_FILE),
        "--report-after-run",
    ]


def unlock_command(run_output: Path) -> str:
    """Return the engine's own unlock invocation, every path quoted the way a POSIX shell carries it.

    The shape fixes this command's words; it names `python`, not this process's interpreter path, because
    it is an instruction a person runs later, possibly from another shell with the environment activated.

    EVERY PATH IS SHELL-QUOTED, because the line exists to be pasted. An estate's run output can sit under a
    synced folder whose name carries a space or a parenthesis, and an unquoted path there splits into several
    words — or fails on the parenthesis — so the one sentence printed for recovering a locked run would not
    run on exactly the machines whose directory names need it. `shlex.quote` leaves a path with no shell
    metacharacter bare, so the common form is unchanged, and wraps the rest in single quotes with an
    embedded apostrophe carried as `'"'"'`, which is what a shell reads back as the same one word.
    """
    snakefile = shlex.quote(str(SNAKEFILE))
    directory = shlex.quote(str(run_output))
    return (
        f"python -m snakemake --snakefile {snakefile} --directory {directory} "
        "--profile none --workflow-profile none --unlock"
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command and return its exit band.

    `argv` defaults to `None`, which makes `argparse` read `sys.argv[1:]`; the generated console-script
    wrapper calls `main()` with no argument, so a required `argv` would fail there before the command ran.
    A usage error is `argparse`'s own `SystemExit(2)`, which IS this command's refusal band.

    An interrupt raised before the engine starts is caught here and reported as the refusal band: nothing
    ran, so band 3 would name an engine that never launched. An interrupt while the engine runs is handled
    inside `_run_engine`, which turns it into `_EngineInterruptedError` after the engine has been waited for or
    terminated, and `_run_command` maps that to band 3.
    """
    arguments = _parser().parse_args(argv)
    try:
        return _run_command(arguments)
    except _EngineInterruptedError as interruption:
        _relay(interruption.stdout, sys.stdout)
        _relay(interruption.stderr, sys.stderr)
        print(
            "interrupted: the engine started and the run did not complete; rerun the command to resume.",
            file=sys.stderr,
        )
        return EXIT_ENGINE_FAILED
    except KeyboardInterrupt:
        return _refuse("interrupted before the run started; nothing was launched")


def _run_command(arguments: argparse.Namespace) -> int:
    """Perform the command's checks, start the engine, and return the band the run ended at.

    Kept apart from `main` so the one `KeyboardInterrupt` catch wraps everything that happens before the
    engine: the engine's own interrupts never reach it, because `_run_engine` converts them.
    """
    if not _engine_available():
        return _refuse(
            "the engine is not importable from this interpreter, so no run can start: install the "
            "package's declared dependency and try again"
        )

    configuration_path = Path(arguments.configuration)
    try:
        configuration = MainConfiguration.load(configuration_path)
    except (MainConfigurationError, OSError, UnicodeDecodeError) as error:
        return _refuse(str(error))

    # The launch binding is checked BEFORE the pre-run step writes the record: a mode this build cannot
    # launch is refused with no record left behind, and the record's absence is then true of every refusal
    # that happens here. The resolver call is the same one the pre-run makes, and resolving twice reads the
    # same document twice rather than two documents.
    try:
        run_output = run_output_directory(configuration)
        plan = launch_plan(configuration, snakefile=SNAKEFILE, run_output=run_output)
    except (ModeError, PathResolutionError, OSError, UnicodeDecodeError) as error:
        return _refuse(str(error))

    try:
        prepare_run(configuration, configuration_path)
    except PreRunError as error:
        return _refuse(str(error))

    if isinstance(plan, SubmissionPlan):
        return _submit(plan, run_output=run_output)

    try:
        completed = _run_engine(
            engine_command(configuration, run_output=run_output),
            environment=engine_environment(os.environ),
            cwd=run_output,
        )
        _relay(completed.stdout, sys.stdout)
        _relay(completed.stderr, sys.stderr)
        if completed.returncode == EXIT_COMPLETED:
            return EXIT_COMPLETED

        if _LOCK_ERROR_NAME in (completed.stderr or "") or _LOCK_ERROR_NAME in (completed.stdout or ""):
            _print_lock_recourse(run_output)
        return EXIT_ENGINE_FAILED
    except KeyboardInterrupt:
        # An interrupt that arrives AFTER the engine started but outside `_run_engine`'s own waits — while
        # the captured output is relayed, or while the band is decided — is still an interrupt of a started
        # run, so it becomes the same interruption error and ends at band 3 rather than at the refusal band.
        raise _EngineInterruptedError(None, None, terminated=False) from None


def _print_lock_recourse(run_output: Path) -> None:
    """Print the one sentence that hands a locked run back to its operator, with the invocation that clears it.

    Both launch paths print THIS sentence. The engine path meets the refusal in the engine's own streams and
    the submission path in the attempt's log, but the recourse is one thing: the command never unlocks, and
    the operator decides whether an engine still writes to the run.
    """
    print(
        "the engine refused to start because a previous run left a lock; if no engine is running, "
        f"release it with: {unlock_command(run_output)}",
        file=sys.stderr,
    )


def _allocation_output(log_path: Path) -> str:
    """Return this attempt's captured allocation output, or nothing when the attempt wrote none.

    Only the plan's path is read, and a missing file is not an error: a submission that failed before its
    script ran wrote no log, and that is exactly the case the command must report without recourse.
    """
    try:
        return log_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def _submit(plan: SubmissionPlan, *, run_output: Path) -> int:
    """Write the allocation's script into the run output, submit it, and return the band its output decides.

    The script is the plan's text, written to the plan's path — which IS the path the submission argv
    carries, so the file submitted is the file written. A write failure is the refusal band, because no
    submission happened.

    The submission runs through the same seam as an engine launch, so an interrupt, an unrunnable process
    and the group shutdown behave exactly as they do for a run started here: band 3, one line, no process
    left. The band itself comes from `submission_band`: a refused submission prints no job id and is the
    refusal band, a job id with exit 0 is completed, and a job id with any non-zero exit — 2 included — is
    the engine band.

    DR-15 is untouched by construction: nothing here unlocks. A lock left by a killed engine is the
    operator's to release through the printed invocation, and a run that still has a writer is never
    touched by this command.
    """
    try:
        plan.script_path.write_text(plan.script, encoding="utf-8")
    except OSError as error:
        return _refuse(f"the allocation script cannot be written into the run output {run_output}: {error}")

    try:
        completed = _run_engine(
            list(plan.arguments),
            environment=engine_environment(os.environ),
            cwd=run_output,
        )
    except _EngineInterruptedError as interruption:
        _relay(interruption.stdout, sys.stdout)
        _relay(interruption.stderr, sys.stderr)
        print(
            "interrupted: the engine started and the run did not complete; rerun the command to resume.",
            file=sys.stderr,
        )
        return EXIT_ENGINE_FAILED

    _relay(completed.stdout, sys.stdout)
    _relay(completed.stderr, sys.stderr)
    band = submission_band(completed.stdout, completed.returncode)
    if band == EXIT_REFUSED:
        print(
            "the submission was refused: sbatch printed no job id on standard output, so no job exists and nothing ran",
            file=sys.stderr,
        )
        return band
    # The engine's refusal lands in THIS attempt's log — the path the plan chose and the script wrote, never
    # a file found by searching. A missing or stale log yields no text, so an attempt that failed before its
    # engine started reports band 3 without quoting an earlier run's lock.
    if band == EXIT_ENGINE_FAILED and _LOCK_ERROR_NAME in _allocation_output(plan.log_path):
        _print_lock_recourse(run_output)
    return band


def _parser() -> argparse.ArgumentParser:
    """Return the command's parser: one positional path and the two options every command owes."""
    parser = argparse.ArgumentParser(
        prog=PROGRAM,
        description="Start a stochastic weather generation run from one main configuration file.",
    )
    parser.add_argument("configuration", help="the path of this run's main configuration file")
    parser.add_argument("--version", action="version", version=f"{PROGRAM} {__version__}")
    return parser


def _engine_available() -> bool:
    """Whether this interpreter can import the engine, without importing it to ask."""
    return importlib.util.find_spec("snakemake") is not None


class _EngineInterruptedError(Exception):
    """The operator interrupted the command while the engine was running.

    Carries the engine's captured output so the caller can relay it before the one-line message, and whether
    the engine had to be terminated (a second interrupt) rather than left to finish its own shutdown.

    An exception rather than a return value, because the ordinary result of `_run_engine` is the engine's
    completed process and an interrupt is not one: nothing about the run's return code is meaningful after
    the operator asked it to stop.
    """

    def __init__(self, stdout: str | None, stderr: str | None, *, terminated: bool) -> None:
        super().__init__("the engine was interrupted")
        self.stdout = stdout
        self.stderr = stderr
        self.terminated = terminated


def _run_engine(command: list[str], *, environment: Mapping[str, str], cwd: Path) -> subprocess.CompletedProcess[str]:
    """Start the engine and return its completed process, capturing its output for the band decision.

    The output is relayed by the caller rather than inherited, because the band decision reads it: the
    engine's lock refusal arrives on standard error. A launch failure is the operating system's error and is
    not caught here — an engine that cannot be launched is the command's own defect, not a run's outcome.

    THE WHOLE RUN IS SIGNALLED, NOT THE ENGINE'S PID. The engine is started in its own session, so its process
    group holds the engine and the shell jobs its local executor started. A terminal `Ctrl-C` reaches only
    this command's group, and a direct `kill -INT {pid}` reaches only this command, so the first interrupt
    FORWARDS SIGINT to the engine's group: the engine, and every job it started, receive the signal the
    operator meant. Signalling only the engine's pid would leave a shell job holding the engine's pipes and
    its lock behind it, and the drain would wait on the survivors.

    AN INTERRUPT IS HANDLED IN TWO STAGES, and both end in `_EngineInterruptedError`:

    - the first `KeyboardInterrupt` forwards SIGINT and waits, bounded by `_INTERRUPT_SHUTDOWN_SECONDS`, for
      the engine's own shutdown. The engine is NOT killed while that wait runs, because killing it would cut
      short the cleanup — termination of its jobs and release of its lock — it was asked to do.
    - a second `KeyboardInterrupt` during that wait, OR the shutdown bound expiring, means the command stops
      waiting: the GROUP is terminated (SIGTERM), and killed (SIGKILL) if the bounded termination wait
      expires, so no process of the run is left running unattended.

    The final drain is bounded too: after a group kill, a writer that somehow survived holds the pipes open,
    and an unbounded `communicate()` would keep the command from returning band 3 for a run whose every
    process it already signalled. On the drain bound the pipes are closed and the command returns without
    the survivor's output.

    A third interrupt while the termination wait runs is answered the same way as the second: the group is
    killed and the command ends.
    """
    process = subprocess.Popen(
        command,
        env=dict(environment),
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate()
    except KeyboardInterrupt:
        _signal_group(process.pid, signal.SIGINT)
        try:
            stdout, stderr = process.communicate(timeout=_INTERRUPT_SHUTDOWN_SECONDS)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            stdout, stderr = _terminate_and_reap(process)
            raise _EngineInterruptedError(stdout, stderr, terminated=True) from None
        raise _EngineInterruptedError(stdout, stderr, terminated=False) from None
    return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)


def _terminate_and_reap(process) -> tuple[str | None, str | None]:
    """Terminate the engine's GROUP, bound the wait, kill the group if it does not take, and bound the drain.

    Both signals go to the process group, not the engine's pid: a shell job is the engine's child and holds
    the engine's pipes, so a pid-only signal leaves it running and the drain waits on it. Neither signal is
    optional: an engine that ignores SIGTERM must not leave the command waiting, and a writer that somehow
    survives the group kill must not hold the command past its drain bound.
    """
    _signal_group(process.pid, signal.SIGTERM)
    try:
        return process.communicate(timeout=_INTERRUPT_TERMINATE_SECONDS)
    except (subprocess.TimeoutExpired, KeyboardInterrupt):
        _signal_group(process.pid, signal.SIGKILL)
        return _drain_bounded(process)


def _drain_bounded(process) -> tuple[str | None, str | None]:
    """Wait at most the drain bound for the engine's pipes to close, then stop waiting and return no output.

    A closed pipe is the proof that no writer of this run is left: the engine and its jobs are those
    writers. A survivor that cannot be signalled — or one that moved itself out of the group — would keep
    the read ends open, so on expiry both pipes are closed, the engine is reaped if it can be, and the
    command returns without the survivor's output rather than waiting past the band it already owes.
    """
    try:
        return process.communicate(timeout=_INTERRUPT_DRAIN_SECONDS)
    except (subprocess.TimeoutExpired, KeyboardInterrupt):
        for stream in (process.stdout, process.stderr):
            if stream is not None:
                with contextlib.suppress(OSError):
                    stream.close()
        try:
            process.wait(timeout=_INTERRUPT_TERMINATE_SECONDS)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            process.kill()
        return None, None


def _signal_group(pgid: int, signum: int) -> None:
    """Send `signum` to the run's process group, ignoring a group that is already gone.

    The engine is the group's leader because it is started in its own session, so its pid IS the group id.
    A `ProcessLookupError` means every process of the run is gone; a `PermissionError` means one is not
    signalable by this user, which the bounded waits still handle. Neither is a refusal: the command's
    ending is band 3 either way.
    """
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(pgid, signum)


def _relay(text: str | None, stream) -> None:
    """Write the engine's captured output to `stream`, ending an unterminated final line.

    The engine's bytes are written unchanged; the only addition is a newline when its output did not end
    with one, because the command prints its own one-line report after the relay and that line must start at
    column 0 rather than glued to a half-written engine line.
    """
    if text:
        stream.write(text)
        if not text.endswith("\n"):
            stream.write("\n")
        stream.flush()


def _refuse(message: str) -> int:
    """Report a refusal on standard error and return the refusal band."""
    print(message, file=sys.stderr)
    return EXIT_REFUSED
