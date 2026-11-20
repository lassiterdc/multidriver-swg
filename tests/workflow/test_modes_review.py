"""Review tests for the HPC bindings: the configuration-only rule, the band constants' agreement with the command, and the script's fixed options.

These checks cross boundaries the mode tests cannot see from inside one value comparison. A binding can
produce the right tuple while reading a hostname, a script can carry the right directives while writing
itself into the toolkit, and the band classifier can agree with every test while numbering its arms
differently from the command's bands. Each check reads the module as a source, compares it with the
command's own constants, or compares two generated scripts, so it fails on those changes and not on a
value.

They deliberately do not restate the batch_job tuple, the script's directives or the band table: the
mode tests own those, and a review test that copied them would double the maintenance without adding a
check.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import multidriver_swg.workflow.cli as cli
import multidriver_swg.workflow.modes as modes

SOURCE = Path(modes.__file__).resolve()

# The ambient facts a binding must not read: the mode is a property of the configuration, and the shape says
# so in one sentence that lists exactly this kind of value. They are read as attribute or call names rather
# than as substrings, because the module's own prose says "environment" about the rule it follows.
AMBIENT_ATTRIBUTES = {"environ", "getcwd", "gethostname", "getuser", "expanduser", "home"}

# The fixed options the command's own engine invocation carries. The allocation's script runs the same engine,
# so the same options must appear in its line, or an allocation run differs from a local one in more than its
# launch.
FIXED_ENGINE_OPTIONS = (
    "--snakefile",
    "--directory",
    "--profile",
    "--workflow-profile",
    "--rerun-incomplete",
    "--report",
    "--report-after-run",
)


def _tree() -> ast.Module:
    """Return the module parsed as a syntax tree."""
    return ast.parse(SOURCE.read_text(encoding="utf-8"))


def _configuration(mode: str, **slurm):
    """A validated configuration in `mode`, built directly so the checks need no file."""
    from multidriver_swg.run_configuration.main_configuration import (
        LocalConfiguration,
        MainConfiguration,
        SlurmConfiguration,
    )

    if mode == "local":
        return MainConfiguration(
            rule="main-configuration:1",
            estate_root="/estate",
            bindings="config/paths.toml",
            project="smoke.run",
            mode="local",
            local=LocalConfiguration(cores=1),
        )
    values = {"partition": "standard", "account": "allocation", "time_minutes": 30}
    values.update(slurm)
    if mode == "1_job_many_srun_tasks":
        values.setdefault("cpus_per_task", 4)
    else:
        values.setdefault("jobs", 4)
    return MainConfiguration(
        rule="main-configuration:1",
        estate_root="/estate",
        bindings="config/paths.toml",
        project="smoke.run",
        mode=mode,
        slurm=SlurmConfiguration(**values),  # type: ignore[arg-type]
    )


def test_the_bindings_read_the_configuration_only() -> None:
    """Property: the module mentions no environment, hostname, working directory, home directory or user.

    Class: the module's whole source text.
    Why it is required: a mode is a property of the machine's configuration; a value read from the process
    would make the same configuration launch differently on two invocations and nothing would record which.
    What kills it: reading an environment variable for a partition, or gating on a hostname.
    Second correct implementation: any source that names none of those; the scan is what decides.
    """
    read: list[str] = []
    for node in ast.walk(_tree()):
        if isinstance(node, ast.Attribute):
            read.append(node.attr)
        elif isinstance(node, ast.Name):
            read.append(node.id)

    assert set(read) & AMBIENT_ATTRIBUTES == set()


def test_the_band_constants_agree_with_the_commands() -> None:
    """Property: the module's three band constants equal the command's three.

    Class: the two constant sets.
    Why it is required: `submission_band` answers in band numbers and the command defines the same numbers;
    a renumbering in one module would silently remap a refused submission or a failed run.
    What kills it: changing one side's number.
    Second correct implementation: importing the numbers from one module in the other; the values agree.
    """
    assert modes.BAND_COMPLETED == cli.EXIT_COMPLETED
    assert modes.BAND_REFUSED == cli.EXIT_REFUSED
    assert modes.BAND_FAILED == cli.EXIT_ENGINE_FAILED


def test_each_generated_script_takes_its_values_from_the_configuration_it_is_given() -> None:
    """Property: two configurations differing only in partition, account, time and CPUs produce scripts differing in exactly those directives.

    Class: two allocation configurations.
    Why it is required: a script that carried a literal from the spike (the partition or account) would run
    on the wrong allocation while every single-configuration test stayed green.
    What kills it: formatting a spike value into the directives instead of the configuration's field.
    Second correct implementation: any template reading the fields; the per-line comparison decides.
    """
    first = modes.engine_batch_script(
        _configuration("1_job_many_srun_tasks", cpus_per_task=2, time_minutes=10),
        snakefile="/checkout/Snakefile",
        run_output="/runs/one",
    )
    second = modes.engine_batch_script(
        _configuration(
            "1_job_many_srun_tasks",
            partition="other",
            account="other-account",
            time_minutes=45,
            cpus_per_task=8,
        ),
        snakefile="/checkout/Snakefile",
        run_output="/runs/one",
    )

    changed = [pair for pair in zip(first.splitlines(), second.splitlines(), strict=True) if pair[0] != pair[1]]
    directive_changes = {old.split("=")[0] for old, _ in changed if old.startswith("#SBATCH")}
    assert directive_changes == {
        "#SBATCH --partition",
        "#SBATCH --account",
        "#SBATCH --time",
        "#SBATCH --cpus-per-task",
    }
    assert any("--cores 2" in line for line in first.splitlines())
    assert any("--cores 8" in line for line in second.splitlines())


def test_the_script_writer_targets_the_run_output_and_the_module_writes_nothing_beside_itself() -> None:
    """Property: the only write call in the module writes the file under the given run output.

    Class: every call whose attribute names a write, and the path the writer constructs.
    Why it is required: the script carries this machine's partition and account, so a copy written beside the
    module would publish machine values into the installed package; the shape places the script estate-side.
    What kills it: writing beside `__file__`, or a second write call for a copy.
    Second correct implementation: the same single write through another path expression; the target decides.
    """
    writes = [
        node.func.attr
        for node in ast.walk(_tree())
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in {"write", "write_text"}
    ]
    assert writes == ["write_text"]

    run_output = Path("/tmp") / "modes-review-run"
    run_output.mkdir(parents=True, exist_ok=True)
    written = modes.write_engine_batch_script(
        _configuration("1_job_many_srun_tasks"),
        snakefile="/checkout/Snakefile",
        run_output=run_output,
    )
    assert written.parent == run_output
    assert written.parent != SOURCE.parent


def test_the_allocation_engine_line_carries_the_commands_fixed_options() -> None:
    """Property: the script's engine line and the command's engine argv name the same fixed options.

    Class: the command's `engine_command` argv for a local configuration and the script's last line.
    Why it is required: the allocation runs the same workflow file through the same engine; a fixed option
    missing from the script (the report pair, `--rerun-incomplete`, a closed profile channel) would make an
    allocation run differ from a local one in behaviour nobody chose.
    What kills it: adding an option to the command's invocation and not to the script's line.
    Second correct implementation: both built from one option tuple; the two sets agree.
    """
    command = cli.engine_command(_configuration("local"), run_output=Path("/runs/one"))
    script_line = modes.engine_batch_script(
        _configuration("1_job_many_srun_tasks"), snakefile="/checkout/Snakefile", run_output="/runs/one"
    ).splitlines()[-1]
    words = re.findall(r"--[a-z-]+", script_line)

    command_options = {word for word in command if word.startswith("--")}
    script_options = set(words)
    for option in FIXED_ENGINE_OPTIONS:
        assert option in command_options, option
        assert option in script_options, option


def test_accepted_wrapped_jobs_execute_with_and_without_wait(tmp_path: Path) -> None:
    """Accepted submissions must run their payload even when the caller does not wait.

    Class: wrapped submissions from a driver executor, and waiting allocation submissions.
    A fake job id without executing its payload makes accounting report success for outputs
    that were never produced, so the shim cannot exercise the driver executor.
    Killed by: returning an accepted id without scheduling the wrapped command.
    Second correct implementation: execute synchronously in the shim, or launch a worker
    and return early for non-waiting submissions; both eventually produce the marker.
    """
    import os
    import shlex
    import subprocess
    import time

    shim = Path(__file__).parent / "slurm_shim" / "sbatch"
    environment = dict(os.environ)
    environment["SHIM_SBATCH_REFUSE"] = "0"
    for waiting in (True, False):
        marker = tmp_path / ("waiting" if waiting else "nonwaiting")
        command = [str(shim), "--parsable"]
        if waiting:
            command.append("--wait")
        command.append(f"--wrap=touch {shlex.quote(str(marker))}")
        completed = subprocess.run(command, env=environment, capture_output=True, text=True, timeout=10)
        assert completed.returncode == 0, completed.stderr
        assert completed.stdout.strip(), "the accepted submission did not identify a job"
        deadline = time.monotonic() + 2
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert marker.exists(), f"accepted wrapped job never executed: waiting={waiting}"


def test_account_query_returns_the_single_column_the_executor_requests() -> None:
    """Account listings must use the column format the executor actually requests.

    Class: account-validation queries requesting one account column, not a parsable pair.
    The executor compares whole stripped lines with the configured account, so an added
    pipe or second column makes every advertised account fail validation.
    Killed by: always emitting a two-column account row regardless of the query format.
    Second correct implementation: list one account or several accounts on separate lines;
    either preserves the requested scalar-column grammar.
    """
    import subprocess

    shim = Path(__file__).parent / "slurm_shim" / "sacctmgr"
    completed = subprocess.run(
        [str(shim), "-n", "-s", "list", "user", "probe-user", "format=account%256"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert completed.returncode == 0, completed.stderr
    accounts = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
    assert accounts
    assert all("|" not in account for account in accounts), accounts


def test_accounting_query_yields_the_configured_state_to_the_real_executor(tmp_path: Path, monkeypatch) -> None:
    """The accounting shim must report state where its consumer reads state.

    Class: successful and failed job-status queries using the executor's own query builder.
    A spike's six-column diagnostic row places the job name in the status position,
    leaving a real driver unable to recognize completion even when the job ran.
    Killed by: emitting the diagnostic row for every requested accounting format.
    Second correct implementation: honor the requested column list generically, or
    support the executor's two-column query and the diagnostic query separately.
    """
    import asyncio
    import logging
    import os

    from snakemake_executor_plugin_slurm.job_status_query import query_job_status, query_job_status_sacct

    shim = Path(__file__).parent / "slurm_shim"
    monkeypatch.setenv("PATH", str(shim) + os.pathsep + os.environ.get("PATH", ""))
    monkeypatch.delenv("SHIM_SACCT_EMPTY", raising=False)
    for expected in ("COMPLETED", "FAILED"):
        monkeypatch.setenv("SHIM_SACCT_STATE", expected)
        statuses, _ = asyncio.run(query_job_status(query_job_status_sacct("probe-run"), logging.getLogger(__name__)))
        assert statuses, "the shim returned no accounting state"
        assert set(statuses.values()) == {expected}, statuses


def _allocation_cli_environment(tmp_path, toolkit_copy, estate):
    """Configure an estate and isolated child environment for the real allocation command."""
    import os

    estate.bindings.write_text(
        'rule = "path-bindings:1"\n\n[paths]\n'
        f'"smoke.run.project_directory" = "{estate.project_directory}"\n'
        f'"smoke.run.run_output" = "{estate.run_output}"\n'
        f'"smoke.run.selection" = "{estate.project_directory / "selection.toml"}"\n',
        encoding="utf-8",
    )
    estate.configuration.write_text(
        'rule = "main-configuration:1"\n'
        f'estate_root = "{estate.root}"\n'
        'bindings = "config/paths_test.toml"\nproject = "smoke.run"\n'
        'mode = "1_job_many_srun_tasks"\n\n[slurm]\n'
        'partition = "standard"\naccount = "allocation"\ntime_minutes = 10\ncpus_per_task = 1\n',
        encoding="utf-8",
    )
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(toolkit_copy.root / "src")
    environment["XDG_CACHE_HOME"] = str(tmp_path / "cache")
    environment["PATH"] = str(Path(__file__).parent / "slurm_shim") + os.pathsep + environment.get("PATH", "")
    environment["SHIM_JOB_OUTPUT"] = str(tmp_path / "allocation.log")
    environment["SHIM_SBATCH_REFUSE"] = "0"
    return environment


def test_allocation_command_refuses_without_jobs_then_runs_the_real_graph(tmp_path, toolkit_copy, estate) -> None:
    """A rejected allocation runs nothing; accepting it runs the actual copied graph.

    Class: refused and accepted allocation requests through the public module entry point.
    Replacing the generated script with an exit stub cannot prove a graph or report runs.
    Killed by: executing on the calling host after rejection, or never executing the script.
    Second correct implementation: any submission driver preserving the refusal, target
    artifacts and report; it need not use the production handler's helper structure.
    """
    import json
    import subprocess
    import sys

    environment = _allocation_cli_environment(tmp_path, toolkit_copy, estate)
    command = [sys.executable, "-m", "multidriver_swg.workflow", str(estate.configuration)]
    environment["SHIM_SBATCH_REFUSE"] = "1"
    refused = subprocess.run(command, env=environment, capture_output=True, text=True, timeout=30)
    assert refused.returncode == 2, refused.stderr
    assert not list(estate.run_output.glob("*/implementation_identity.json"))
    environment["SHIM_SBATCH_REFUSE"] = "0"
    completed = subprocess.run(command, env=environment, capture_output=True, text=True, timeout=60)
    log = Path(environment["SHIM_JOB_OUTPUT"]).read_text()
    assert completed.returncode == 0, completed.stderr + log
    record = json.loads((estate.run_output / "run_start.json").read_text())
    assert record["targets"]
    assert all((estate.run_output / target).exists() for target in record["targets"])
    assert (estate.run_output / "report.html").is_file()


def test_allocation_command_maps_an_independent_scheduler_exit_two_to_three(tmp_path, toolkit_copy, estate) -> None:
    """An accepted job exiting two is a failed run, not a refused submission.

    Class: the recorded scheduler outcome with a job id and job exit two.
    An external scheduler fixture supplies that outcome without replacing a script builder.
    Killed by: returning the scheduler's two or treating every nonzero exit as refusal.
    Second correct implementation: any classifier separating job existence from job result.
    """
    import os
    import subprocess
    import sys

    environment = _allocation_cli_environment(tmp_path, toolkit_copy, estate)
    scheduler = tmp_path / "scheduler"
    scheduler.mkdir()
    sbatch = scheduler / "sbatch"
    sbatch.write_text("#!/bin/sh\nprintf '9002\\n'\nexit 2\n", encoding="utf-8")
    sbatch.chmod(0o755)
    environment["PATH"] = str(scheduler) + os.pathsep + environment["PATH"]
    completed = subprocess.run(
        [sys.executable, "-m", "multidriver_swg.workflow", str(estate.configuration)],
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 3, completed.stderr
    assert "9002" in completed.stdout


def test_interrupt_during_real_allocation_leaves_no_writer_or_lock(tmp_path, toolkit_copy, estate) -> None:
    """A signal to the waiting command must shut down the engine inside the allocation.

    Class: real allocation scripts with an active long-running rule when only the caller is interrupted.
    Reusing an engine seam is insufficient if an intermediate scheduler or shell consumes the signal.
    Killed by: signalling only the waiting scheduler while its engine or rule keeps running.
    Second correct implementation: cooperative group shutdown or explicit allocation cancellation,
    provided all writers are gone and the engine has released its lock before return.
    """
    import json
    import os
    import signal
    import subprocess
    import sys
    import time

    from tests.workflow.conftest import _git

    environment = _allocation_cli_environment(tmp_path, toolkit_copy, estate)
    stub = toolkit_copy.package / "workflow" / "stubs.py"
    stub.write_text(
        stub.read_text().replace(
            "    for output in declared_outputs:",
            "    import os, json, time\n"
            "    Path('allocation_ready.json').write_text(json.dumps([os.getpid(), os.getppid()]))\n"
            "    time.sleep(20)\n"
            "    for output in declared_outputs:",
            1,
        )
    )
    _git(toolkit_copy.root, "add", "src/multidriver_swg/workflow/stubs.py")
    _git(toolkit_copy.root, "commit", "-q", "-m", "slow rule for interruption probe")
    revision = _git(toolkit_copy.root, "rev-parse", "HEAD")
    (estate.project_directory / "toolkit_revision.toml").write_text(
        f'rule = "toolkit-revision:1"\ncommit = "{revision}"\n',
    )
    process = subprocess.Popen(
        [sys.executable, "-m", "multidriver_swg.workflow", str(estate.configuration)],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    ready = estate.run_output / "allocation_ready.json"
    try:
        deadline = time.monotonic() + 30
        while not ready.exists() and process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.02)
        assert ready.exists(), "the allocation never reached a running rule"
        captures = list(estate.run_output.glob("allocation-*.log"))
        assert captures and any(path.stat().st_size for path in captures), "capture never started before interrupt"
        process.send_signal(signal.SIGINT)
        stdout, stderr = process.communicate(timeout=50)
        assert process.returncode == 3, stdout + stderr
        assert "interrupted:" in stderr
        assert not list((estate.run_output / ".snakemake" / "locks").glob("*"))
        for pid in json.loads(ready.read_text()):
            stat = Path(f"/proc/{pid}/stat")
            assert not stat.exists() or stat.read_text().split(") ", 1)[1].startswith("Z"), f"writer {pid} survived"
    finally:
        if ready.exists():
            for pid in json.loads(ready.read_text()):
                try:
                    group = os.getpgid(pid)
                    if group != os.getpgrp():
                        os.killpg(group, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
        process.communicate(timeout=10)


def test_allocation_lock_refusal_preserves_lock_and_prints_operator_recourse(tmp_path, toolkit_copy, estate) -> None:
    """A locked allocation run must keep the lock and tell the operator how to recover.

    Class: accepted allocation submissions whose engine meets a pre-existing output lock.
    Hiding the engine's lock message inside a scheduler log leaves no usable recourse in
    the command's own output; clearing the lock would risk a concurrent writer.
    Killed by: classifying submission failure without reporting the engine's unlock recourse.
    Second correct implementation: relay the job's lock diagnostic or detect the same
    lock through an authoritative scheduler result, without unlocking automatically.
    """
    import subprocess
    import sys

    environment = _allocation_cli_environment(tmp_path, toolkit_copy, estate)
    locks = estate.run_output / ".snakemake" / "locks"
    locks.mkdir(parents=True)
    lock = locks / "0.output.lock"
    content = "observed_record_pre_selection/pre_selection.json\n"
    lock.write_text(content)
    completed = subprocess.run(
        [sys.executable, "-m", "multidriver_swg.workflow", str(estate.configuration)],
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
    )
    log = Path(environment["SHIM_JOB_OUTPUT"]).read_text()
    assert "LockException" in log, log
    assert completed.returncode == 3, completed.stderr + log
    assert lock.read_text() == content
    assert "--unlock" in completed.stdout + completed.stderr
    import shlex

    expected = (
        f"python -m snakemake --snakefile {shlex.quote(str(toolkit_copy.snakefile))} "
        f"--directory {shlex.quote(str(estate.run_output))} "
        "--profile none --workflow-profile none --unlock"
    )
    assert expected in completed.stdout + completed.stderr


def test_second_attempt_failing_before_engine_never_borrows_first_attempt_recourse(
    tmp_path, toolkit_copy, estate
) -> None:
    """An actual earlier lock refusal must not become a later attempt's diagnosis.

    Class: sequential accepted allocation submissions, first locked and then failing
    before the engine starts. A fixed or searched capture path would misreport the second.
    Killed by: reusing a prior attempt's log when a fresh attempt produces none.
    Second correct implementation: unique log identities or explicit fresh capture
    initialization tied to the submission; neither borrows historical refusal text.
    """
    import os
    import subprocess
    import sys

    environment = _allocation_cli_environment(tmp_path, toolkit_copy, estate)
    locks = estate.run_output / ".snakemake" / "locks"
    locks.mkdir(parents=True)
    lock = locks / "0.output.lock"
    lock.write_text("observed_record_pre_selection/pre_selection.json\n")
    command = [sys.executable, "-m", "multidriver_swg.workflow", str(estate.configuration)]
    first = subprocess.run(command, env=environment, capture_output=True, text=True, timeout=30)
    assert first.returncode == 3, first.stderr
    assert "--unlock" in first.stderr
    prior_captures = {path: path.read_bytes() for path in estate.run_output.glob("allocation-*.log")}
    assert prior_captures
    assert any(b"LockException" in content for content in prior_captures.values())

    scheduler = tmp_path / "early_failure_scheduler"
    scheduler.mkdir()
    sbatch = scheduler / "sbatch"
    sbatch.write_text("#!/bin/sh\nprintf '9003\\n'\nexit 2\n", encoding="utf-8")
    sbatch.chmod(0o755)
    environment["PATH"] = str(scheduler) + os.pathsep + environment["PATH"]
    second = subprocess.run(command, env=environment, capture_output=True, text=True, timeout=30)
    assert second.returncode == 3, second.stderr
    assert "9003" in second.stdout
    assert "--unlock" not in second.stdout + second.stderr
    assert all(path.read_bytes() == content for path, content in prior_captures.items())
    assert lock.exists(), "the command cleared a pre-existing lock"
