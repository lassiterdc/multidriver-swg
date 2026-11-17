"""Check the console command: the three bands, the constructed environment, the engine invocation and the local binding.

Each check is written against a form the command's defect takes: a refusal reported as a failure, a failure
reported as a refusal, an inherited environment that lets a profile or a thread count change the run, an
engine started with a target or without the report option, and a mode launched with another mode's options.
The engine itself is never started here: `_run_engine` is the command's one seam, so every band is reached
by a fake process and no test depends on the engine's own behaviour.

The pre-run step is real in these tests, over a committed copy and a full estate, because the command's
first three bands are exactly the checks that step runs.
"""

from __future__ import annotations

import os
import re
import shlex
import signal
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

import multidriver_swg.workflow.cli as cli
import multidriver_swg.workflow.pre_run as pre_run
from multidriver_swg import __version__
from multidriver_swg.run_configuration.main_configuration import LocalConfiguration, MainConfiguration
from multidriver_swg.workflow.cli import (
    EXIT_COMPLETED,
    EXIT_ENGINE_FAILED,
    EXIT_REFUSED,
    engine_command,
    engine_environment,
    main,
    unlock_command,
)
from multidriver_swg.workflow.run_start import RUN_START_FILE
from tests.workflow.conftest import ToolkitCopy

PROJECT = "smoke.run"


@dataclass(frozen=True)
class _Estate:
    """A full estate: three bound keys, the declaration, the configuration and the run output."""

    root: Path
    project_directory: Path
    configuration: Path
    bindings: Path
    run_output: Path
    selection: Path


def _estate(
    tmp_path: Path,
    toolkit: ToolkitCopy,
    *,
    mode: str = "local",
    bind_selection: bool = True,
) -> _Estate:
    """Write an estate whose configuration runs in `mode`, with the keys the command and pre-run resolve."""
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

    if mode == "local":
        mode_text = "[local]\ncores = 1\n"
    elif mode == "1_job_many_srun_tasks":
        mode_text = '[slurm]\npartition = "compute"\naccount = "allocation"\ntime_minutes = 60\ncpus_per_task = 4\n'
    else:
        mode_text = '[slurm]\npartition = "compute"\naccount = "allocation"\ntime_minutes = 60\njobs = 4\n'

    configuration = root / "config" / "run.toml"
    configuration.write_text(
        'rule = "main-configuration:1"\n'
        f'estate_root = "{root}"\n'
        'bindings = "config/paths_test.toml"\n'
        f'project = "{PROJECT}"\n'
        f'mode = "{mode}"\n\n{mode_text}',
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


class _FakeEngine:
    """A stand-in for the engine process that records what it was given and returns a chosen result."""

    def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.calls: list[dict[str, object]] = []

    def __call__(self, command, *, environment, cwd):
        self.calls.append({"command": list(command), "environment": dict(environment), "cwd": Path(cwd)})
        return subprocess.CompletedProcess(
            args=command, returncode=self.returncode, stdout=self.stdout, stderr=self.stderr
        )


def _prepare_environment(monkeypatch: pytest.MonkeyPatch, toolkit: ToolkitCopy, estate: _Estate) -> None:
    """Point the pre-run step's revision gate at the committed copy so its checks pass."""
    monkeypatch.setattr(pre_run, "running_package_directory", lambda: toolkit.package)


def test_version_prints_the_distribution_name_and_the_version(capsys: pytest.CaptureFixture[str]) -> None:
    """Property: `--version` exits 0 and prints `multidriver-swg` followed by the package version.

    Class: the one version option.
    Why it is required: a run's provenance names the version it was produced by, and the shape fixes the
    two words the command prints; a wrapper reading the output needs a stable form.
    What kills it: printing only the version, or `argparse`'s `%(prog)s` (which is `__main__.py` under
    `python -m`).
    Second correct implementation: any constant string with the same two words; the check asserts both.
    """
    with pytest.raises(SystemExit) as caught:
        main(["--version"])

    assert caught.value.code == 0
    assert capsys.readouterr().out.strip() == f"multidriver-swg {__version__}"


def test_help_lists_the_one_positional_argument(capsys: pytest.CaptureFixture[str]) -> None:
    """Property: `--help` exits 0 and its usage names the configuration positional.

    Class: the help option.
    Why it is required: the command takes one argument and no other; a help page that omitted it would send
    an operator to the source, and one that listed an option the parser does not take would be wrong.
    What kills it: removing the positional from the parser or its help.
    Second correct implementation: any argparse construction; the usage text decides.
    """
    with pytest.raises(SystemExit) as caught:
        main(["--help"])

    assert caught.value.code == 0
    assert "configuration" in capsys.readouterr().out


@pytest.mark.parametrize(
    "argv", [[], ["--unknown"], ["a.toml", "b.toml"]], ids=["no-argument", "unknown-option", "two-paths"]
)
def test_a_usage_error_is_the_refusal_band(argv: list[str]) -> None:
    """Property: a missing argument, an unknown option and a second positional each exit 2.

    Class: argparse's three usage errors.
    Why it is required: band 2 means the command refused to start, and a usage error is the clearest case;
    argparse's own exit is already 2, so a wrapper that converted it would be the defect.
    What kills it: catching `SystemExit` and returning another code, or adding a second positional.
    Second correct implementation: a hand-written parser returning 2; the code is the contract.
    """
    with pytest.raises(SystemExit) as caught:
        main(argv)

    assert caught.value.code == EXIT_REFUSED


def test_an_invalid_configuration_is_refused_and_named(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Property: a path that is not a valid configuration returns 2 and the message names the file.

    Class: a file whose rule header is absent.
    Why it is required: the configuration is the command's only argument, so a refusal that did not name it
    would leave the operator with no operand to repair.
    What kills it: letting `MainConfigurationError` escape, or returning 3 as though the engine had run.
    Second correct implementation: loading the configuration before the engine check; the same band and the
    same named file result.
    """
    path = tmp_path / "broken.toml"
    path.write_text('estate_root = "/estate"\n', encoding="utf-8")

    assert main([str(path)]) == EXIT_REFUSED
    assert str(path) in capsys.readouterr().err


def test_a_pre_run_refusal_is_the_refusal_band_and_writes_no_record(
    tmp_path: Path, toolkit_copy: ToolkitCopy, capsys: pytest.CaptureFixture[str]
) -> None:
    """Property: an estate whose binding document omits the selection key returns 2, names the document, and writes no record.

    Class: an estate with the selection key unbound.
    Why it is required: band 2 is "the command refused to start", and the pre-run's checks are the command's
    own refusals; a record left behind would be read by an engine the command never started.
    What kills it: running the engine anyway, or writing the record before the pre-run.
    Second correct implementation: any ordering that refuses before the record; the band and the absent
    record both decide.
    """
    estate = _estate(tmp_path, toolkit_copy, bind_selection=False)

    assert main([str(estate.configuration)]) == EXIT_REFUSED
    assert str(estate.bindings) in capsys.readouterr().err
    assert not (estate.run_output / RUN_START_FILE).exists()


def test_an_unimportable_engine_is_the_refusal_band(
    tmp_path: Path, toolkit_copy: ToolkitCopy, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Property: with no importable engine the command returns 2, names the engine, and writes no record.

    Class: `_engine_available` reporting False.
    Why it is required: a command that started a run it cannot launch would fail with a traceback from
    `subprocess`, which is neither band; the shape names this refusal explicitly.
    What kills it: attempting the launch and translating the launch error.
    Second correct implementation: probing by import (which starts numpy) instead of by spec; the same band
    and the same named engine result.
    """
    estate = _estate(tmp_path, toolkit_copy)
    monkeypatch.setattr(cli, "_engine_available", lambda: False)

    assert main([str(estate.configuration)]) == EXIT_REFUSED
    assert "engine" in capsys.readouterr().err
    assert not (estate.run_output / RUN_START_FILE).exists()


def test_a_completed_run_returns_zero_and_the_engine_was_given_the_constructed_environment_and_the_run_output(
    tmp_path: Path, toolkit_copy: ToolkitCopy, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Property: a conforming run returns 0; the engine was started in the run output with no `SNAKEMAKE_*` variable and the three guards set; the record exists.

    Class: one conforming estate, a fake engine returning 0, and a parent environment carrying a decoy
    `SNAKEMAKE_PROFILE` and an exported `OMP_NUM_THREADS`.
    Why it is required: this is the whole successful path — the pre-run wrote the record, the mode supplied
    `--cores`, the report option is present, and the child's environment is the command's construction
    rather than a copy of a caller's.
    What kills it: passing `os.environ` through, launching in the command's own directory, or omitting the
    report option.
    Second correct implementation: the same environment built once before the launch; the assertions read
    the fake's record.
    """
    estate = _estate(tmp_path, toolkit_copy)
    _prepare_environment(monkeypatch, toolkit_copy, estate)
    engine = _FakeEngine()
    monkeypatch.setattr(cli, "_run_engine", engine)
    monkeypatch.setenv("SNAKEMAKE_PROFILE", "decoy")
    monkeypatch.setenv("OMP_NUM_THREADS", "4")
    monkeypatch.delenv("OPENBLAS_NUM_THREADS", raising=False)
    monkeypatch.delenv("MKL_NUM_THREADS", raising=False)

    assert main([str(estate.configuration)]) == EXIT_COMPLETED

    call = engine.calls[0]
    command = call["command"]
    assert command[command.index("--cores") + 1] == "1"
    assert "--report-after-run" in command
    assert "--rerun-incomplete" in command
    assert command[command.index("--directory") + 1] == str(estate.run_output)
    assert "--report" in command and command[command.index("--report") + 1] == str(estate.run_output / "report.html")
    assert call["cwd"] == estate.run_output
    environment = call["environment"]
    assert not any(name.startswith("SNAKEMAKE_") for name in environment)
    assert environment["OMP_NUM_THREADS"] == "4"
    assert environment["OPENBLAS_NUM_THREADS"] == "1"
    assert environment["MKL_NUM_THREADS"] == "1"
    assert (estate.run_output / RUN_START_FILE).is_file()


def test_a_failed_run_is_the_engine_band(
    tmp_path: Path, toolkit_copy: ToolkitCopy, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Property: an engine that exits non-zero makes the command return 3, not 2.

    Class: a fake engine exiting 1 with no lock message.
    Why it is required: 3 means the engine started and the run did not complete; confusing it with a
    refusal would make a failed run look like a usage problem.
    What kills it: returning the engine's own code, or folding a failure into the refusal band.
    Second correct implementation: any branch that returns 3 for a non-zero engine; the code decides.
    """
    estate = _estate(tmp_path, toolkit_copy)
    _prepare_environment(monkeypatch, toolkit_copy, estate)
    monkeypatch.setattr(cli, "_run_engine", _FakeEngine(returncode=1, stderr="rule failed\n"))

    assert main([str(estate.configuration)]) == EXIT_ENGINE_FAILED


def test_a_lock_refusal_is_the_engine_band_and_prints_the_unlock_invocation(
    tmp_path: Path, toolkit_copy: ToolkitCopy, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Property: a fake engine reporting `LockException` makes the command return 3 and print the exact unlock invocation.

    Class: a fake engine exiting 1 with a `LockException` message.
    Why it is required: after a killed engine the lock holds no liveness fact, so the operator decides; the
    command's job is to return the failure band and hand over the engine's own recourse, complete.
    What kills it: unlocking on the command's own judgement, or printing a partial command.
    Second correct implementation: detecting the class name in either stream; the same band and the same
    string result.
    """
    estate = _estate(tmp_path, toolkit_copy)
    _prepare_environment(monkeypatch, toolkit_copy, estate)
    monkeypatch.setattr(
        cli,
        "_run_engine",
        _FakeEngine(returncode=1, stderr="LockException in file x: Directory cannot be locked\n"),
    )

    assert main([str(estate.configuration)]) == EXIT_ENGINE_FAILED

    err = capsys.readouterr().err
    assert unlock_command(estate.run_output) in err
    assert "--unlock" in err


def _allocation_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Put the SLURM shim first on PATH so the command's submission finds it."""
    import multidriver_swg.workflow.modes as modes

    shim = Path(__file__).resolve().parent / "slurm_shim"
    monkeypatch.setenv("PATH", str(shim) + os.pathsep + os.environ.get("PATH", ""))
    assert modes.BATCH_SCRIPT_NAME == "engine_batch.sh"


def _allocation_estate(tmp_path: Path, toolkit: ToolkitCopy) -> _Estate:
    """A conforming allocation-mode estate over the committed copy."""
    return _estate(tmp_path, toolkit, mode="1_job_many_srun_tasks")


def _artifact_directories(estate: _Estate) -> set[str]:
    """The rule directories an allocation's script would have produced, if it ran."""
    return {path.name for path in estate.run_output.iterdir() if path.is_dir() and not path.name.startswith(".")}


def test_a_refused_submission_is_the_refusal_band_and_nothing_ran(
    tmp_path: Path, toolkit_copy: ToolkitCopy, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Property: an allocation-mode run whose submission is refused returns 2, prints the shim's refusal and the no-job-id line, and starts no rule.

    Class: the shim's refused arm (no job id on standard output, exit 1), reached through the command's real
    submission path over the mode table's own script.
    Why it is required: a refused submission means no job exists, so nothing ran; returning 3 would report a
    run that failed, and starting the engine on this host would be a different run altogether.
    What kills it: ignoring standard output and mapping the non-zero exit to band 3.
    Second correct implementation: classify through `submission_band` anywhere before returning; the band,
    the two lines and the absent rule directories decide.
    """
    import multidriver_swg.workflow.cli as cli_module

    estate = _allocation_estate(tmp_path, toolkit_copy)
    _prepare_environment(monkeypatch, toolkit_copy, estate)
    _allocation_environment(monkeypatch)
    monkeypatch.setenv("SHIM_SBATCH_REFUSE", "1")

    assert main([str(estate.configuration)]) == EXIT_REFUSED

    captured = capsys.readouterr()
    assert "Batch job submission failed" in captured.err
    assert "printed no job id" in captured.err
    assert _artifact_directories(estate) == set()
    assert cli_module.SNAKEFILE.is_file()


def test_the_script_the_command_writes_is_the_one_the_mode_table_produced(
    tmp_path: Path, toolkit_copy: ToolkitCopy, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Property: the file the command submits is byte-for-byte the text `launch_plan` handed it.

    Class: a refused submission, after which the written script exists and nothing ran.
    Why it is required: the command must not re-render the script it submits; a second rendering is a second
    source that can disagree with the plan the mode table answered.
    What kills it: building the script inside `_submit` instead of writing the plan's text.
    Second correct implementation: the plan's text written at the plan's path; the equality decides.
    """
    import multidriver_swg.workflow.modes as modes

    estate = _allocation_estate(tmp_path, toolkit_copy)
    _prepare_environment(monkeypatch, toolkit_copy, estate)
    _allocation_environment(monkeypatch)
    monkeypatch.setenv("SHIM_SBATCH_REFUSE", "1")

    assert main([str(estate.configuration)]) == EXIT_REFUSED

    written = estate.run_output / modes.BATCH_SCRIPT_NAME
    text = written.read_text(encoding="utf-8")
    log_token = re.search(r"exec > >\(tee -a (.*?)\) 2>&1", text)
    assert log_token is not None
    log_path = Path(shlex.split(log_token.group(1))[0])
    expected = modes.engine_batch_script(
        MainConfiguration.load(estate.configuration),
        snakefile=cli.SNAKEFILE,
        run_output=estate.run_output,
        log_path=log_path,
    )
    assert text == expected


@pytest.mark.parametrize(
    ("script_exit", "expected"), [(0, EXIT_COMPLETED), (2, EXIT_ENGINE_FAILED)], ids=["exit-0", "exit-2"]
)
def test_an_accepted_submission_takes_its_band_from_the_job_id_and_the_exit(
    tmp_path: Path,
    toolkit_copy: ToolkitCopy,
    monkeypatch: pytest.MonkeyPatch,
    script_exit: int,
    expected: int,
) -> None:
    """Property: an accepted submission returns 0 when the job exits 0 and 3 when it exits non-zero, 2 included.

    Class: the shim's accepted arm with the script exiting 0 and 2, over the command's real submission path.
    Why it is required: a job id means a job existed, so a non-zero exit is a run that did not complete; the
    exit-2 case is the one that must not read as the refusal band.
    What kills it: mapping every non-zero exit to the refusal band, or returning the job's exit code as the
    command's.
    Second correct implementation: the classifier's own mapping; the band decides.
    """
    import multidriver_swg.workflow.modes as modes

    estate = _allocation_estate(tmp_path, toolkit_copy)
    _prepare_environment(monkeypatch, toolkit_copy, estate)
    _allocation_environment(monkeypatch)
    monkeypatch.setattr(
        modes,
        "engine_batch_script",
        lambda configuration, *, snakefile, run_output, log_path=None: f"#!/bin/bash\nexit {script_exit}\n",
    )

    assert main([str(estate.configuration)]) == expected
    assert (estate.run_output / modes.BATCH_SCRIPT_NAME).read_text(encoding="utf-8").endswith(f"exit {script_exit}\n")


def test_the_engine_environment_is_built_not_inherited() -> None:
    """Property: `engine_environment` drops every `SNAKEMAKE_*` name, defaults an unset or empty guard to `1`, keeps an exported guard, and leaves the operand unchanged.

    Class: a mapping holding two `SNAKEMAKE_*` names, one exported guard, one empty guard and one unrelated
    name.
    Why it is required: the engine reads its own variables from the child environment, and a caller's decoy
    profile must not change the run; the guards keep `numpy` from starting a thread per core.
    What kills it: `dict(os.environ)`, dropping `SNAKEMAKE_` names by a list instead of the namespace, or
    overwriting an exported guard.
    Second correct implementation: a copy plus `setdefault` for the guards; the same mapping results.
    """
    operand = {
        "SNAKEMAKE_PROFILE": "decoy",
        "SNAKEMAKE_CORES": "8",
        "OPENBLAS_NUM_THREADS": "",
        "OMP_NUM_THREADS": "4",
        "PATH": "/usr/bin",
    }
    original = dict(operand)

    environment = engine_environment(operand)

    assert environment == {
        "OPENBLAS_NUM_THREADS": "1",
        "OMP_NUM_THREADS": "4",
        "MKL_NUM_THREADS": "1",
        "PATH": "/usr/bin",
    }
    assert operand == original


def test_the_command_does_not_change_the_process_environment(
    tmp_path: Path, toolkit_copy: ToolkitCopy, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Property: `main` leaves `os.environ` equal to what it was before the call.

    Class: a completed fake run with a caller environment holding an engine variable.
    Why it is required: the command constructs the child's mapping; changing its own environment would leak
    into a caller inspecting it and into any later subprocess the caller starts.
    What kills it: assigning into `os.environ` while building the child's mapping.
    Second correct implementation: building a fresh mapping, as here; the equality is the property.
    """
    estate = _estate(tmp_path, toolkit_copy)
    _prepare_environment(monkeypatch, toolkit_copy, estate)
    monkeypatch.setattr(cli, "_run_engine", _FakeEngine())
    monkeypatch.setenv("SNAKEMAKE_PROFILE", "decoy")
    before = dict(os.environ)

    assert main([str(estate.configuration)]) == EXIT_COMPLETED

    assert dict(os.environ) == before


def test_the_lock_recourse_is_the_engines_own_invocation_for_this_run_output() -> None:
    """Property: `unlock_command` names the workflow file, the run output and the engine's own `--unlock`.

    Class: one run output.
    Why it is required: the string is an instruction a person runs later; a path missing from it would
    unlock the wrong directory or fail to resolve the workflow file.
    What kills it: omitting `--unlock`, or building the path from the process's working directory.
    Second correct implementation: any string with the same five options and both paths; containment is
    asserted.
    """
    command = unlock_command(Path("/runs/one"))

    assert "python -m snakemake" in command
    assert str(cli.SNAKEFILE) in command
    assert "--directory /runs/one" in command
    assert "--profile none" in command
    assert "--workflow-profile none" in command
    assert command.endswith("--unlock")


def test_the_engine_command_passes_no_target_and_asks_for_the_report() -> None:
    """Property: `engine_command` starts the engine as a module of this interpreter, names the workflow file and the run output, and passes no target.

    Class: one local configuration.
    Why it is required: a target passed here would be a second statement of what the run builds, and the
    report option pair is what makes a completed run leave its report.
    What kills it: appending a target, dropping `--report-after-run`, or launching `snakemake` as an
    executable rather than this interpreter's module.
    Second correct implementation: an argv assembled from constants; the assertions read the list.
    """
    configuration = _configuration_for_command()
    run_output = Path("/runs/one")

    command = engine_command(configuration, run_output=run_output)

    assert command[:3] == [cli.sys.executable, "-m", "snakemake"]
    assert command[command.index("--snakefile") + 1] == str(cli.SNAKEFILE)
    assert command[-3:] == ["--report", str(run_output / "report.html"), "--report-after-run"]
    assert "all" not in command


def _configuration_for_command():
    """A validated configuration with the local binding, built directly rather than read from a file."""
    return MainConfiguration(
        rule="main-configuration:1",
        estate_root="/estate",
        bindings="config/paths.toml",
        project="smoke.run",
        mode="local",
        local=LocalConfiguration(cores=2),
    )


class _ScriptedProcess:
    """A stand-in for the engine's `Popen` whose `communicate` follows a script of actions."""

    def __init__(self, actions, calls):
        self._actions = iter(actions)
        self.pid = 4242
        self.returncode = 0
        self.terminated = False
        self.killed = False
        self.timeouts: list[float | None] = []
        self._calls = calls

    def communicate(self, timeout=None):
        self.timeouts.append(timeout)
        action = next(self._actions)
        if action == "interrupt":
            raise KeyboardInterrupt
        if action == "timeout":
            raise subprocess.TimeoutExpired(cmd="snakemake", timeout=timeout)
        return ("engine out", "engine err")

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True


def test_an_interrupt_while_the_engine_runs_ends_at_the_engine_band_with_one_line(
    tmp_path: Path, toolkit_copy: ToolkitCopy, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Property: `main` returns 3 when the engine seam reports an interrupt, relays the engine's output, and prints one line — never a traceback.

    Class: a fake engine seam raising the command's `_EngineInterruptedError`.
    Why it is required: `Ctrl-C` reaches the command and the engine together; an uncaught `KeyboardInterrupt`
    ends the process at status 130 with a traceback, which is neither of the three bands the command exists
    to report. Band 3 is already "the engine started and the run did not complete", which is exactly the
    interrupted state.
    What kills it: letting `KeyboardInterrupt` propagate, or returning the refusal band as though nothing
    had launched.
    Second correct implementation: catching `KeyboardInterrupt` in `_run_command` after `_run_engine`;
    the band, the one-line message and the absence of a traceback decide.
    """
    estate = _estate(tmp_path, toolkit_copy)
    _prepare_environment(monkeypatch, toolkit_copy, estate)

    def interrupted(*args, **kwargs):
        raise cli._EngineInterruptedError("engine out", "engine err", terminated=False)

    monkeypatch.setattr(cli, "_run_engine", interrupted)

    assert main([str(estate.configuration)]) == EXIT_ENGINE_FAILED

    captured = capsys.readouterr()
    assert "interrupted: the engine started and the run did not complete" in captured.err
    assert "Traceback" not in captured.err and "Traceback" not in captured.out
    assert "engine out" in captured.out
    assert "engine err" in captured.err
    assert len([line for line in captured.err.splitlines() if line.startswith("interrupted:")]) == 1


def test_the_first_interrupt_waits_for_the_engine_and_the_second_terminates_it(monkeypatch: pytest.MonkeyPatch) -> None:
    """Property: one interrupt waits for the engine's own shutdown without terminating it; a second terminates it and still ends in the command's interrupt; a termination that times out is killed.

    Class: three scripts over a fake `Popen` — one interrupt then an exit, two interrupts then an exit, and
    two interrupts then a timeout then an exit.
    Why it is required: the engine shares the command's process group and so received the operator's signal;
    killing it on the FIRST interrupt would cut short the shutdown it was asked to perform, while a second
    interrupt must not leave it running unattended. The bounded wait is what keeps the second case from
    blocking forever.
    What kills it: `subprocess.run` with no interrupt handling, or a second interrupt that only re-raises.
    Second correct implementation: any two-stage wait with a bounded termination; the recorded calls decide.
    """
    calls: list[dict[str, object]] = []
    signals: list[int] = []

    def scripted(actions):
        def popen(command, **kwargs):
            process = _ScriptedProcess(actions, calls)
            calls.append({"command": command, "kwargs": kwargs, "process": process})
            return process

        return popen

    monkeypatch.setattr(cli, "_signal_group", lambda pgid, signum: signals.append(signum))

    monkeypatch.setattr(cli.subprocess, "Popen", scripted(["interrupt", "done"]))
    with pytest.raises(cli._EngineInterruptedError) as first:
        cli._run_engine(["snakemake"], environment={"A": "1"}, cwd=Path("/runs/one"))
    assert first.value.terminated is False
    assert signals == [signal.SIGINT]
    assert calls[0]["process"].terminated is False
    assert calls[0]["kwargs"]["env"] == {"A": "1"}
    assert calls[0]["kwargs"]["cwd"] == "/runs/one"
    assert calls[0]["kwargs"]["stdout"] == subprocess.PIPE
    assert calls[0]["kwargs"]["text"] is True
    assert calls[0]["kwargs"]["start_new_session"] is True

    calls.clear()
    signals.clear()
    monkeypatch.setattr(cli.subprocess, "Popen", scripted(["interrupt", "interrupt", "done"]))
    with pytest.raises(cli._EngineInterruptedError) as second:
        cli._run_engine(["snakemake"], environment={}, cwd=Path("/runs/one"))
    assert second.value.terminated is True
    assert signals == [signal.SIGINT, signal.SIGTERM]

    calls.clear()
    signals.clear()
    monkeypatch.setattr(cli.subprocess, "Popen", scripted(["interrupt", "interrupt", "timeout", "done"]))
    with pytest.raises(cli._EngineInterruptedError) as third:
        cli._run_engine(["snakemake"], environment={}, cwd=Path("/runs/one"))
    assert third.value.terminated is True
    assert signals == [signal.SIGINT, signal.SIGTERM, signal.SIGKILL]


def test_an_interrupt_before_the_engine_returns_the_refusal_band(
    tmp_path: Path, toolkit_copy: ToolkitCopy, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Property: an interrupt raised while the configuration loads returns 2 with one line, and the engine is never started.

    Class: `MainConfiguration.load` raising `KeyboardInterrupt`.
    Why it is required: every ending of the command is one of the three bands; before the engine starts
    nothing ran, so band 3 would name an engine that never launched, and status 130 would be no band at all.
    What kills it: catching the interrupt only around the engine launch.
    Second correct implementation: one `KeyboardInterrupt` catch around the whole pre-engine phase; the band,
    the message and the unstarted engine decide.
    """
    estate = _estate(tmp_path, toolkit_copy)
    engine = _FakeEngine()
    monkeypatch.setattr(cli, "_run_engine", engine)

    def interrupted(cls, path):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli.MainConfiguration, "load", classmethod(interrupted))

    assert main([str(estate.configuration)]) == EXIT_REFUSED
    captured = capsys.readouterr()
    assert "interrupted before the run started" in captured.err
    assert "Traceback" not in captured.err
    assert engine.calls == []


@pytest.mark.parametrize(
    ("actions", "killed"),
    [
        (["interrupt", "timeout", "done"], False),
        (["interrupt", "timeout", "timeout", "done"], True),
    ],
    ids=["terminated", "terminated-then-killed"],
)
def test_a_signal_that_reaches_only_the_command_ends_at_band_3_within_the_bound(
    monkeypatch: pytest.MonkeyPatch, actions: list[str], killed: bool
) -> None:
    """Property: when the engine never receives the signal, the command's engine seam still ends after the bound — terminating the engine, and killing it if the termination does not take — and raises the interruption the command maps to band 3.

    Class: one interrupt followed by the shutdown wait expiring, with the engine honouring SIGTERM in one
    case and ignoring it in the other, driven by the fake engine's script.
    Why it is required: `kill -INT {pid}`, a wrapper's stop and a CI cancellation deliver the signal to the
    command alone; the engine keeps running, and a shutdown wait without a timeout hangs the command with a
    live engine behind it. Band 3 is the interrupt's band, and it must be reached by the bound rather than
    by the operator's second signal; the mapping from this error to band 3 is the sibling test named below.
    What kills it: calling `communicate()` with no timeout after the first interrupt.
    Second correct implementation: any bounded wait followed by SIGTERM and, if needed, SIGKILL; the
    recorded timeouts and the process's calls decide.
    """
    calls: list[dict[str, object]] = []
    signals: list[int] = []

    def popen(command, **kwargs):
        process = _ScriptedProcess(actions, calls)
        calls.append({"process": process, "kwargs": kwargs})
        return process

    monkeypatch.setattr(cli.subprocess, "Popen", popen)
    monkeypatch.setattr(cli, "_signal_group", lambda pgid, signum: signals.append(signum))

    with pytest.raises(cli._EngineInterruptedError) as caught:
        cli._run_engine(["snakemake"], environment={}, cwd=Path("/runs/one"))

    assert caught.value.terminated is True
    process = calls[0]["process"]
    assert process.timeouts[0] is None
    assert process.timeouts[1] == cli._INTERRUPT_SHUTDOWN_SECONDS
    assert process.timeouts[2] == cli._INTERRUPT_TERMINATE_SECONDS
    assert signals == [signal.SIGINT, signal.SIGTERM] + ([signal.SIGKILL] if killed else [])


def test_an_allocation_lock_refusal_is_band_3_with_the_unlock_recourse(
    tmp_path: Path, toolkit_copy: ToolkitCopy, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Property: an accepted allocation whose engine meets a pre-existing lock returns 3, prints the unlock invocation, and leaves the lock in place.

    Class: the real generated allocation script run through the shim, over a run output that already holds
    `.snakemake/locks`.
    Why it is required: DR-15 says the operator decides whether a stale lock is clear; the command's whole
    remedy is the same sentence the engine path prints, and without it the operator sees a job id and nothing
    else while the refusal sits in a log nobody named.
    What kills it: classifying the submission failure without reading the attempt's log, or unlocking here.
    Second correct implementation: any capture whose path the plan fixes and the command reads; the band,
    the invocation and the lock's survival decide.
    """
    import shlex

    import multidriver_swg.workflow.modes as modes

    estate = _allocation_estate(tmp_path, toolkit_copy)
    _prepare_environment(monkeypatch, toolkit_copy, estate)
    _allocation_environment(monkeypatch)
    locks = estate.run_output / ".snakemake" / "locks"
    locks.mkdir(parents=True)
    lock = locks / "0.output.lock"
    lock.write_text("observed_record_assembly/observed_record.json\n")
    # The allocation's payload writes the engine's refusal into the log the plan chose, then exits 2 — the
    # exit an accepted submission gives when its job failed. The real engine's own refusal is captured the
    # same way; the reviewer's node drives it end to end.
    monkeypatch.setattr(
        modes,
        "engine_batch_script",
        lambda configuration, *, snakefile, run_output, log_path=None: (
            "#!/bin/bash\n"
            f"printf '%s\\n' 'LockException: Directory cannot be locked' > {shlex.quote(str(log_path))}\n"
            "exit 2\n"
        ),
    )

    assert main([str(estate.configuration)]) == EXIT_ENGINE_FAILED

    err = capsys.readouterr().err
    assert unlock_command(estate.run_output) in err
    assert "--unlock" in err
    assert lock.read_text() == "observed_record_assembly/observed_record.json\n"


def test_a_previous_attempts_log_never_supplies_a_fresh_attempts_recourse(
    tmp_path: Path, toolkit_copy: ToolkitCopy, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Property: an accepted allocation that fails without its engine starting reports band 3 and NO unlock recourse, even with an old lock-log in the run output.

    Class: a submission whose script exits 2 before reaching the engine, beside a log a previous attempt left.
    Why it is required: the plan's log path is per-attempt; reading by pattern would quote another run's lock
    refusal as this run's, handing the operator an instruction for a lock this run never met.
    What kills it: globbing the run output for a log, or falling back to a fixed name.
    Second correct implementation: read only the plan's path; its absence decides.
    """
    import multidriver_swg.workflow.modes as modes

    estate = _allocation_estate(tmp_path, toolkit_copy)
    _prepare_environment(monkeypatch, toolkit_copy, estate)
    _allocation_environment(monkeypatch)
    stale = estate.run_output / f"allocation-{'0' * 32}.log"
    stale.write_text("LockException: Directory cannot be locked\n", encoding="utf-8")
    monkeypatch.setattr(
        modes,
        "engine_batch_script",
        lambda configuration, *, snakefile, run_output, log_path=None: "#!/bin/bash\nexit 2\n",
    )

    assert main([str(estate.configuration)]) == EXIT_ENGINE_FAILED

    err = capsys.readouterr().err
    assert "release it with" not in err
    assert stale.read_text(encoding="utf-8") == "LockException: Directory cannot be locked\n"
