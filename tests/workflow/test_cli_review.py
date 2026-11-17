"""Review tests for the command: its entry-point contract, its two explicit options, the bands, and the declared dependencies.

These checks cross boundaries the command's own tests cannot see from inside one run: the console script's
wrapper calls `main()` with no argument, `python -m` calls `__main__.py`, the wheel is what carries the
script and the pins, and the engine's option set is a contract with the shape rather than with any test's
convenience. Each check reads the module as a syntax tree, the launcher as a call, or `pyproject.toml` as
text, so it fails on those changes and not on a band's behaviour.

They deliberately do not repeat the command's band cases: the command's own tests own those, and a review
test that restated them would double the maintenance without adding a check.
"""

from __future__ import annotations

import ast
import contextlib
import os
import shlex
import signal
import threading
import time
from pathlib import Path

import pytest

import multidriver_swg.workflow.cli as cli
import multidriver_swg.workflow.pre_run as pre_run
from tests.workflow.conftest import Estate, ToolkitCopy

REPO_ROOT = Path(__file__).resolve().parents[2]
CLI_SOURCE = Path(cli.__file__).resolve()
MAIN_SOURCE = CLI_SOURCE.parent / "__main__.py"
PYPROJECT = REPO_ROOT / "pyproject.toml"


def _tree(path: Path) -> ast.Module:
    """Return `path` parsed as a syntax tree."""
    return ast.parse(path.read_text(encoding="utf-8"))


def test_main_can_be_called_with_no_argument_and_the_launcher_does_so() -> None:
    """Property: `main` declares `argv` with a `None` default, and `__main__.py` calls `main()` with no argument.

    Class: the entry function's signature and the launcher's call node.
    Why it is required: the generated console-script wrapper calls `main()` with no argument, so a required
    `argv` raises `TypeError` from the installed script before any command code runs; and the module launcher
    must not pass `sys.argv[1:]` itself, because argparse already reads it.
    Killed by: `def main(argv)` without a default, or `main(sys.argv[1:])` in the launcher.
    Second correct implementation: none — the shape fixes the call as `main()` with no argument, so a launcher
    passing anything else is the defect this check exists for.
    """
    parameters = list(cli.main.__code__.co_varnames[: cli.main.__code__.co_argcount])
    defaults = cli.main.__defaults__

    assert parameters == ["argv"]
    assert defaults == (None,)

    calls = [
        node
        for node in ast.walk(_tree(MAIN_SOURCE))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "main"
    ]
    assert len(calls) == 1
    assert calls[0].args == []
    assert calls[0].keywords == []


def test_the_parser_takes_one_positional_and_the_version_option_only() -> None:
    """Property: the parser's explicit arguments are `configuration` and `--version`; `--help` is argparse's own.

    Class: every `add_argument` call in `_parser`.
    Why it is required: the command's contract is one argument and two options; an added option is a second
    way to configure a run that the shape's command does not have.
    Killed by: adding `--cores`, `--target` or a second positional.
    Second correct implementation: any construction whose explicit names are those two; the set decides.
    """
    names: list[str] = []
    for node in ast.walk(_tree(CLI_SOURCE)):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "add_argument"
            and node.args
            and isinstance(node.args[0], ast.Constant)
        ):
            names.append(node.args[0].value)

    assert names == ["configuration", "--version"]


def test_the_three_bands_are_zero_three_and_two() -> None:
    """Property: the module's band constants are `0`, `3` and `2`.

    Class: the three constants.
    Why it is required: the numbers are the command's whole interface with a scheduler or a wrapper; a
    changed number silently changes what "refused" and "failed" mean to a caller.
    Killed by: renumbering a band.
    Second correct implementation: literals in the branches with the constants unused; this module names
    them, and the values are what the check reads.
    """
    assert cli.EXIT_COMPLETED == 0
    assert cli.EXIT_ENGINE_FAILED == 3
    assert cli.EXIT_REFUSED == 2


def test_the_engine_is_invoked_with_the_shape_fixed_options_and_no_target() -> None:
    """Property: the engine argv names both profile channels closed, `--rerun-incomplete`, the report pair, and no rule name.

    Class: the argv `engine_command` returns for a local configuration.
    Why it is required: each option closes a way a run could differ from the one configured — a profile from
    a job's environment, a partial output that refuses the next attempt, a report nobody asked for — and a
    target would be a second statement of what the run builds.
    Killed by: dropping `--profile none` or `--workflow-profile none`, moving the report pair apart, or
    appending `all`.
    Second correct implementation: any argv with the same options in another order the engine accepts; the
    containment checks are order-independent apart from the trailing report pair.
    """
    from multidriver_swg.run_configuration.main_configuration import LocalConfiguration, MainConfiguration

    configuration = MainConfiguration(
        rule="main-configuration:1",
        estate_root="/estate",
        bindings="config/paths.toml",
        project="smoke.run",
        mode="local",
        local=LocalConfiguration(cores=2),
    )
    command = cli.engine_command(configuration, run_output=Path("/runs/one"))
    pairs = list(zip(command, command[1:], strict=False))

    assert ("--profile", "none") in pairs
    assert ("--workflow-profile", "none") in pairs
    assert "--rerun-incomplete" in command
    assert command[-3:] == ["--report", "/runs/one/report.html", "--report-after-run"]
    assert "all" not in command
    assert "observed_record_assembly" not in command


def test_the_engine_variable_rule_is_a_namespace_rule() -> None:
    """Property: a made-up `SNAKEMAKE_`-prefixed variable is dropped, whatever its name.

    Class: one variable no list in the module names.
    Why it is required: the engine's namespace is what a job inherits; a list of known names would admit the
    next variable the engine adds.
    Killed by: removing a fixed list of names instead of the prefix.
    Second correct implementation: the same comprehension; the result is what the check reads.
    """
    environment = cli.engine_environment({"SNAKEMAKE_SOMETHING_NEW": "x", "KEEP": "y"})

    assert "SNAKEMAKE_SOMETHING_NEW" not in environment
    assert environment["KEEP"] == "y"


def test_the_command_never_assigns_into_its_own_environment() -> None:
    """Property: no assignment target in the module is a subscript of `os.environ`.

    Class: every assignment and annotated assignment in the source.
    Why it is required: the command's contract says its own environment is not changed; an assignment would
    be the one way to break it that a reader cannot see from the child's mapping.
    Killed by: `os.environ["X"] = "1"` while defaulting a guard.
    Second correct implementation: building a fresh mapping, as here; the AST check is what decides.
    """
    assigned: list[str] = []
    for node in ast.walk(_tree(CLI_SOURCE)):
        if not isinstance(node, ast.Assign | ast.AnnAssign):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        for target in targets:
            if (
                isinstance(target, ast.Subscript)
                and isinstance(target.value, ast.Attribute)
                and target.value.attr == "environ"
            ):
                assigned.append(ast.dump(target))

    assert assigned == []


def test_pyproject_declares_the_console_script_and_the_two_exact_pins() -> None:
    """Property: `pyproject.toml` names `multidriver-swg = "multidriver_swg.workflow.cli:main"`, pins `snakemake==9.15.0`, and carries the `hpc` extra with the slurm plugin pin.

    Class: the project's scripts table, its dependencies and its optional-dependencies table.
    Why it is required: an installed package without the script has no command; a range in place of the pin
    resolves a release nobody measured; and the HPC binding needs the plugin at the version its behaviour was
    measured against.
    Killed by: editing a pin, dropping the script line, or putting the plugin in the base dependencies.
    Second correct implementation: any TOML layout with those three facts; the checks read the text's
    sections.
    """
    text = PYPROJECT.read_text(encoding="utf-8")

    assert 'multidriver-swg = "multidriver_swg.workflow.cli:main"' in text
    assert '"snakemake==9.15.0"' in text
    assert '"snakemake-executor-plugin-slurm==2.8.0"' in text

    extra = text.split("hpc = [", 1)[1].split("]", 1)[0]
    assert "snakemake-executor-plugin-slurm==2.8.0" in extra


@pytest.mark.parametrize("run_output", ["/runs/My Drive (shared)/run", "/runs/it's here/run"])
def test_the_printed_unlock_invocation_survives_a_shell_split_with_the_run_output_intact(run_output: str):
    """Property: splitting the printed unlock invocation the way a shell does yields the run output as ONE word after `--directory`, and the workflow file as one word after `--snakefile`.

    Shape row: DR-15 (ruled Form A) — on the engine's lock refusal the command "prints the exact recourse,
    `python -m snakemake --snakefile <package Snakefile> --directory <run output> --profile none
    --workflow-profile none --unlock`", which the operator then runs; DR-8 places the run output at a path
    the estate's binding document chooses.
    Class: run-output paths a shell splits: one with spaces and parentheses (a synced-folder name of the
    form people give them), and one with an apostrophe.
    Why it is required: the string is an instruction a person pastes into a shell. Written without quoting,
    `--directory /tmp/My Drive (x)/run` is three words and a syntax error for the parenthesis, so the one
    sentence the command prints for recovering a locked run does not run on exactly the estates whose
    directory names carry spaces, and the operator is left with a half-failed run and a command that
    fails. For a path with none of those characters the quoted and unquoted forms are identical, so the
    command's own existing assertions on `--directory /runs/one` hold under either.
    Killed by: formatting the path into the string with an f-string and no quoting.
    Second correct implementation: `shlex.quote` on each path, or double quotes with escaping; both split
    back to the original word, which is what the test reads.
    """
    words = shlex.split(cli.unlock_command(Path(run_output)))

    assert words[:3] == ["python", "-m", "snakemake"]
    assert words[words.index("--directory") + 1] == run_output
    assert words[words.index("--snakefile") + 1] == str(cli.SNAKEFILE)
    assert words[-1] == "--unlock"


_FAKE_ENGINE = """\
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

signal.signal(signal.SIGINT, signal.SIG_IGN)
signal.signal(signal.SIGTERM, signal.SIG_IGN)
# A job: a child that inherits the engine's output pipes, as a rule's job does under the local executor.
job = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(40)"])
state = Path(os.environ["FAKE_ENGINE_STATE"])
(state / "job.pid").write_text(str(job.pid))
(state / "ready").write_text("1")
time.sleep(40)
"""


def _stuck_engine_run(
    tmp_path: Path, toolkit_copy: ToolkitCopy, estate: Estate, monkeypatch: pytest.MonkeyPatch, capsys
) -> dict[str, object]:
    """Run `cli.main` over an engine that ignores SIGINT and SIGTERM and has a job holding its output pipes, interrupt the command four times, and report how it ended.

    The engine is a stand-in module named `snakemake` placed first on the child's `PYTHONPATH`, so the command
    starts it exactly as it starts the real one (`python -m snakemake`). The interrupts go to this process
    only, which is the delivery a scheduler's direct signal makes. A guard handler turns a late signal into
    nothing once the command has returned, and a safety kill of the job ends the run if the command is
    still blocked ten seconds after the last interrupt, so the scenario cannot hang a session.
    """
    fake_root = tmp_path / "fake_engine"
    (fake_root / "snakemake").mkdir(parents=True)
    (fake_root / "snakemake" / "__init__.py").write_text("", encoding="utf-8")
    (fake_root / "snakemake" / "__main__.py").write_text(_FAKE_ENGINE, encoding="utf-8")
    state_directory = tmp_path / "fake_state"
    state_directory.mkdir()

    estate.bindings.write_text(
        'rule = "path-bindings:1"\n\n[paths]\n'
        f'"smoke.run.project_directory" = "{estate.project_directory}"\n'
        f'"smoke.run.run_output" = "{estate.run_output}"\n'
        f'"smoke.run.selection" = "{estate.project_directory / "selection.toml"}"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(pre_run, "running_package_directory", lambda: toolkit_copy.package)
    monkeypatch.setenv("PYTHONPATH", str(fake_root))
    monkeypatch.setenv("FAKE_ENGINE_STATE", str(state_directory))

    active = threading.Event()
    finished = threading.Event()

    def guard(signum, frame):
        if active.is_set():
            raise KeyboardInterrupt

    def interrupter() -> None:
        deadline = time.time() + 30
        while not (state_directory / "ready").exists():
            if time.time() > deadline or finished.is_set():
                return
            time.sleep(0.05)
        for pause in (0.3, 0.3, 1.0):
            if finished.is_set():
                return
            os.kill(os.getpid(), signal.SIGINT)
            time.sleep(pause)
        if not finished.is_set():
            os.kill(os.getpid(), signal.SIGINT)
        finished.wait(10)
        if not finished.is_set():
            _kill_quietly(int((state_directory / "job.pid").read_text()))

    previous = signal.signal(signal.SIGINT, guard)
    worker = threading.Thread(target=interrupter, daemon=True)
    try:
        worker.start()
        active.set()
        band = cli.main([str(estate.configuration)])
    finally:
        active.clear()
        finished.set()
        signal.signal(signal.SIGINT, previous)
        worker.join(timeout=15)
    job_file = state_directory / "job.pid"
    job_alive = job_file.exists() and _alive(int(job_file.read_text()))
    if job_file.exists():
        _kill_quietly(int(job_file.read_text()))
    return {"band": band, "stderr": capsys.readouterr().err, "job_alive": job_alive}


def _alive(pid: int) -> bool:
    """Whether `pid` names a process that is still running and not a zombie."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    status = Path(f"/proc/{pid}/stat")
    return not (status.exists() and status.read_text().split(") ", 1)[1].startswith("Z"))


def _kill_quietly(pid: int) -> None:
    """Send SIGKILL to `pid`, ignoring a process that is already gone."""
    with contextlib.suppress(ProcessLookupError):
        os.kill(pid, signal.SIGKILL)


def test_repeated_interrupts_end_at_the_engine_band_and_never_claim_nothing_was_launched(
    tmp_path: Path, toolkit_copy: ToolkitCopy, estate: Estate, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    """Property: however many times the operator interrupts a command whose engine does not shut down, the command ends at band 3, with no traceback and without the message that nothing was launched.

    Shape row: the ruling at gate G10 on R-4 — "an interrupt ends at band 3, never at a traceback", band 3
    meaning "the engine started and the run did not complete"; DR-8's refusal band 2 is for a command that
    "refused to start".
    Class: four interrupts in a row delivered to the command alone, against an engine that ignores SIGINT and
    SIGTERM and has a child holding its output pipes. The first waits, the second terminates, the third
    kills the engine, and the fourth arrives while the command is still draining the pipes the job holds.
    Why it is required: the interrupt handling covers the first three stages with its own error, but the
    drain after the kill is a bare `communicate()`, so an interrupt there escapes as `KeyboardInterrupt`;
    `main` catches that as "interrupted before the run started" and returns band 2 with the message that
    nothing was launched, which is false of a run whose engine was started and killed, and is a refusal band
    for a run that did not refuse.
    Killed by: leaving the post-kill `communicate()` calls outside the interrupt handling.
    Second correct implementation: killing the engine's whole process group so the drain ends at once, or
    wrapping the drain in the same handler; either ends at band 3 for all four interrupts.
    """
    outcome = _stuck_engine_run(tmp_path, toolkit_copy, estate, monkeypatch, capsys)

    assert outcome["band"] == 3
    assert "nothing was launched" not in str(outcome["stderr"])
    assert "Traceback" not in str(outcome["stderr"])


def test_no_job_of_an_interrupted_engine_survives_the_command_returning(
    tmp_path: Path, toolkit_copy: ToolkitCopy, estate: Estate, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    """Property: once the command has returned from an interrupted run whose engine had to be killed, the job that engine started is not running.

    Shape row: the ruling at gate G10 on R-4 — band 3 for an interrupt is right only if the engine is gone,
    so no process of the run may outlive the command.
    Class: the same engine and the same four interrupts as above, read after the command has returned; the
    job is the engine's child, as a rule's job is under the local executor.
    Why it is required: the command kills the engine by its own pid, which cannot reach a child; the job is
    orphaned, keeps running against the run output, and holds the pipes open. A rerun then meets a run
    output a stray job is still writing to, and the lock the killed engine left names files that job is
    still producing.
    Killed by: signalling only the engine's pid.
    Second correct implementation: starting the engine in its own process group or session and signalling
    the group, or collecting the engine's children before killing it; the job is then gone at return.
    """
    outcome = _stuck_engine_run(tmp_path, toolkit_copy, estate, monkeypatch, capsys)

    assert outcome["job_alive"] is False


def test_one_command_only_interrupt_leaves_no_real_engine_job_or_lock(tmp_path: Path) -> None:
    """A single command-only interrupt must finish the run's cleanup before returning.

    Class: local engines with an active shell job when only their caller receives SIGINT.
    Otherwise the caller's bounded wait kills an engine never asked to stop, abandoning
    its job and lock; a later invocation cannot safely resume.
    Killed by: terminating only the engine pid when the first shutdown wait expires.
    Second correct implementation: forward SIGINT and wait for graceful cleanup, or
    manage the whole process tree and remove locks only after all writers have stopped.
    """
    import subprocess
    import sys

    snakefile = tmp_path / "Snakefile"
    job_pid = tmp_path / "job.pid"
    snakefile.write_text(
        'rule all:\n    output: "done"\n    shell: "echo $$ > job.pid; sleep 300; touch done"\n',
        encoding="utf-8",
    )
    environment = cli.engine_environment(dict(os.environ))
    environment["XDG_CACHE_HOME"] = str(tmp_path / "cache")
    command = [
        sys.executable,
        "-m",
        "snakemake",
        "--snakefile",
        str(snakefile),
        "--directory",
        str(tmp_path),
        "--cores",
        "1",
        "--profile",
        "none",
        "--workflow-profile",
        "none",
    ]
    finished = threading.Event()
    delivered = threading.Event()
    safety_cleanup = threading.Event()

    def interrupt_once() -> None:
        """Deliver exactly one signal after the real engine has launched its job."""
        deadline = time.monotonic() + 30
        while not finished.is_set() and time.monotonic() < deadline:
            if job_pid.exists():
                delivered.set()
                os.kill(os.getpid(), signal.SIGINT)
                if not finished.wait(80):
                    safety_cleanup.set()
                    pid = int(job_pid.read_text())
                    subprocess.run(["pkill", "-KILL", "-P", str(pid)], check=False)
                    _kill_quietly(pid)
                return
            time.sleep(0.05)

    worker = threading.Thread(target=interrupt_once)
    worker.start()
    try:
        with pytest.raises(cli._EngineInterruptedError):
            cli._run_engine(command, environment=environment, cwd=tmp_path)
        assert delivered.is_set(), "the probe never reached an active engine job"
        assert not safety_cleanup.is_set(), "the command exceeded its shutdown bounds; probe killed the orphan job"
        pid = int(job_pid.read_text())
        locks = list((tmp_path / ".snakemake" / "locks").glob("*"))
        assert not _alive(pid) and not locks, f"job_alive={_alive(pid)}, locks={locks}"
    finally:
        finished.set()
        worker.join(timeout=35)
        if job_pid.exists():
            # The shell and its sleep share the caller's group, so kill descendants by
            # their recorded parent rather than signalling the test runner's group.
            pid = int(job_pid.read_text())
            subprocess.run(["pkill", "-KILL", "-P", str(pid)], check=False)
            _kill_quietly(pid)
