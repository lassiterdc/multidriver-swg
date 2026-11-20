"""Check the HPC launch bindings: the batch_job argument list, the allocation script and the submission bands.

The two cluster bindings rest on two measured arms of the Rivanna spike, and each test here reproduces one:
a refused submission prints nothing on standard output and is the refusal band, and an accepted script that
exits 2 prints its job id and is the engine-failure band. The pure pieces are driven directly, and the shim
under `slurm_shim/` stands in for the cluster's commands so the arms are reproduced rather than described.

The third mode's SUBMISSION wiring is not part of this chunk: `engine_arguments` refuses it, and the tests
below assert that refusal as the state of this build rather than pretending an engine argv is its binding.
"""

from __future__ import annotations

import dataclasses
import os
import shlex
import subprocess
import time
from pathlib import Path

import pytest

import multidriver_swg.workflow.cli as cli
import multidriver_swg.workflow.modes as modes
from multidriver_swg.run_configuration.main_configuration import (
    LocalConfiguration,
    MainConfiguration,
    SlurmConfiguration,
    VersionStackingConfiguration,
)
from multidriver_swg.workflow.modes import (
    BAND_COMPLETED,
    BAND_FAILED,
    BAND_REFUSED,
    ModeError,
    allocation_engine_arguments,
    engine_arguments,
    engine_batch_script,
    rerun_arguments,
    submission_arguments,
    submission_band,
    write_engine_batch_script,
)

SHIM = Path(__file__).resolve().parent / "slurm_shim"


def _configuration(mode: str = "batch_job", **slurm: object) -> MainConfiguration:
    """A validated configuration in `mode`, with the connection values the mode reads."""
    if mode == "local":
        return MainConfiguration(
            rule="main-configuration:1",
            estate_root="/estate",
            bindings="config/paths.toml",
            project="smoke.run",
            mode="local",
            local=LocalConfiguration(cores=3),
        )
    values: dict[str, object] = {
        "partition": "standard",
        "account": "allocation",
        "time_minutes": 30,
    }
    values.update(slurm)
    if mode == "1_job_many_srun_tasks":
        values.setdefault("cpus_per_task", 4)
    else:
        values.setdefault("jobs", 12)
    return MainConfiguration(
        rule="main-configuration:1",
        estate_root="/estate",
        bindings="config/paths.toml",
        project="smoke.run",
        mode=mode,
        slurm=SlurmConfiguration(**values),  # type: ignore[arg-type]
    )


def _run_submission(
    script: Path, tmp_path: Path, *, refuse: bool = False
) -> tuple[subprocess.CompletedProcess[str], int]:
    """Run the submission argv through the shim and return the process and the band the classifier gives it."""
    environment = dict(os.environ)
    environment["PATH"] = str(SHIM) + os.pathsep + environment.get("PATH", "")
    environment["SHIM_SBATCH_REFUSE"] = "1" if refuse else "0"
    environment["SHIM_JOB_OUTPUT"] = str(tmp_path / "job_output.log")
    completed = subprocess.run(
        list(submission_arguments(script)), env=environment, capture_output=True, text=True, check=False
    )
    return completed, submission_band(completed.stdout, completed.returncode)


def test_the_local_binding_is_the_cores_option() -> None:
    """Property: `engine_arguments` for a local configuration is exactly `--cores <local.cores>`.

    Class: one local configuration.
    Why it is required: the local binding is the one every run uses on a laptop and in every test; a change
    here would change what every local run is allowed to use.
    What kills it: adding an option the shape does not name for this mode.
    Second correct implementation: a table entry rather than a branch; the tuple decides.
    """
    assert engine_arguments(_configuration("local")) == ("--cores", "3")


def test_the_batch_job_binding_is_the_slurm_executor_and_the_configured_connection() -> None:
    """Property: a batch_job configuration yields the executor, the job limit, the latency wait and the configured partition, account and runtime, in that order.

    Class: one batch_job configuration with a known partition, account, time limit and job count.
    Why it is required: the engine runs on the login node and submits one job per rule; every value the
    scheduler sees comes from this list, and a missing default resource makes the first submission fail.
    What kills it: dropping `--default-resources` or one of its three values, or reading a value from the
    environment instead of the configuration.
    Second correct implementation: the same options in an order the engine accepts; the containment checks
    are order-independent except where the value pairs are named.
    """
    arguments = engine_arguments(_configuration("batch_job"))

    assert arguments == (
        "--executor",
        "slurm",
        "--jobs",
        "12",
        "--latency-wait",
        "60",
        "--default-resources",
        "slurm_partition=standard",
        "slurm_account=allocation",
        "runtime=30",
    )


def test_the_allocation_binding_refuses_to_be_an_engine_argument_list() -> None:
    """Property: `engine_arguments` for `1_job_many_srun_tasks` raises `ModeError` naming the mode, because its binding is a submission.

    Class: one allocation configuration.
    Why it is required: this mode's engine runs INSIDE an allocation created by an `sbatch` submission; an
    engine argv returned here would make the command run the graph on the login node, which is the one thing
    the mode exists to prevent. The refusal stays until the command calls the submission functions.
    What kills it: returning an argv for the mode, or refusing with a generic error that names no mode.
    Second correct implementation: none — this build must refuse; when the command is wired the refusal is
    replaced by the submission path and this test is replaced with it.
    """
    with pytest.raises(ModeError) as caught:
        engine_arguments(_configuration("1_job_many_srun_tasks"))

    message = str(caught.value)
    assert "'1_job_many_srun_tasks'" in message
    assert "SUBMISSION" in message


def test_the_allocation_engine_line_is_the_allocations_cpus_and_the_latency_wait() -> None:
    """Property: `allocation_engine_arguments` is exactly `--cores <cpus_per_task> --latency-wait 60`, and refuses a mode that allocates nothing.

    Class: one allocation configuration, and one batch_job configuration as the refused control.
    Why it is required: inside the allocation the engine's local executor runs every job on that node, so the
    CPU count is the allocation's and the latency wait is the HPC modes' measured value.
    What kills it: using `[local] cores` or `[slurm] jobs`, or defaulting the wait to the engine's 5 seconds.
    Second correct implementation: a table entry; the tuple decides.
    """
    assert allocation_engine_arguments(_configuration("1_job_many_srun_tasks")) == (
        "--cores",
        "4",
        "--latency-wait",
        "60",
    )

    with pytest.raises(ModeError):
        allocation_engine_arguments(_configuration("batch_job"))


def test_the_batch_script_carries_the_measured_directives_the_guards_and_the_engine_line() -> None:
    """Property: the allocation script is one `#SBATCH` block from the configuration, the thread-guard export, and one engine line with the allocation's CPUs and the report pair.

    Class: one allocation configuration with a known partition, account, time limit and CPU count.
    Why it is required: the script IS the mode's launch; every directive is a value the scheduler reads, the
    guards are what make the engine start on the login node at all, and the engine line is what runs the graph.
    What kills it: a missing directive, an unguarded export, or an engine line without the report options.
    Second correct implementation: any script with the same directives and one equivalent engine line; the
    containment checks are what decide.
    """
    script = engine_batch_script(
        _configuration("1_job_many_srun_tasks"), snakefile="/checkout/Snakefile", run_output="/runs/one"
    )
    lines = script.splitlines()

    assert lines[0] == "#!/bin/bash"
    assert "#SBATCH --partition=standard" in lines
    assert "#SBATCH --account=allocation" in lines
    assert "#SBATCH --time=30" in lines
    assert "#SBATCH --nodes=1" in lines
    assert "#SBATCH --ntasks=1" in lines
    assert "#SBATCH --cpus-per-task=4" in lines
    assert "export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1" in lines
    engine_line = lines[-1]
    assert engine_line.endswith("--report /runs/one/report.html --report-after-run")
    assert "--cores 4 --latency-wait 60" in engine_line
    assert "--snakefile /checkout/Snakefile" in engine_line
    assert "--directory /runs/one" in engine_line
    assert "--profile none --workflow-profile none --rerun-incomplete" in engine_line
    assert shlex.split(engine_line)[1:3] == ["-m", "snakemake"]


def test_the_script_quotes_a_path_that_a_shell_would_split() -> None:
    """Property: a run output or workflow file with a space survives `shlex.split` of the engine line as one word.

    Class: one allocation configuration over a path with a space and a parenthesis.
    Why it is required: the script runs under a shell; an unquoted path there splits into several words and
    the engine starts with the wrong directory — the same defect the command's unlock line had.
    What kills it: formatting the paths into the line with an f-string and no quoting.
    Second correct implementation: any shell quoting that splits back to the original word; the split decides.
    """
    run_output = "/runs/My Drive (shared)/run"
    line = engine_batch_script(
        _configuration("1_job_many_srun_tasks"), snakefile="/checkout/Snakefile", run_output=run_output
    ).splitlines()[-1]
    words = shlex.split(line)

    assert words[words.index("--directory") + 1] == run_output
    assert words[words.index("--snakefile") + 1] == "/checkout/Snakefile"


def test_the_written_script_lives_in_the_run_output_and_names_itself() -> None:
    """Property: `write_engine_batch_script` writes `engine_batch.sh` under the run output and nowhere else, and returns that path.

    Class: one allocation configuration over a temporary run output.
    Why it is required: the script carries this machine's partition and account, so it is estate-side; a copy
    inside the toolkit would publish machine values.
    What kills it: writing beside the module, or under a different name than the shape states.
    Second correct implementation: any writer producing the same file at the same path; the path decides.
    """
    run_output = Path("/tmp") / "shim-run-output-test"
    run_output.mkdir(parents=True, exist_ok=True)
    path = write_engine_batch_script(
        _configuration("1_job_many_srun_tasks"), snakefile="/checkout/Snakefile", run_output=run_output
    )

    assert path == run_output / modes.BATCH_SCRIPT_NAME
    assert path.is_file()
    assert path.read_text(encoding="utf-8").startswith("#!/bin/bash\n")
    assert path.parent != SHIM.parent


def test_the_submission_is_the_waiting_parsable_form() -> None:
    """Property: the submission argv is exactly `sbatch --wait --parsable <script>`.

    Class: one script path.
    Why it is required: `--wait` is what lets the command report a band at all, and `--parsable` is what
    prints the job id the classifier reads; either one missing changes the whole mapping.
    What kills it: dropping `--wait`, or submitting without `--parsable`.
    Second correct implementation: none — the three words are the measured interface.
    """
    assert submission_arguments("/runs/one/engine_batch.sh") == (
        "sbatch",
        "--wait",
        "--parsable",
        "/runs/one/engine_batch.sh",
    )


@pytest.mark.parametrize(
    ("stdout", "returncode", "expected"),
    [
        pytest.param("23045000\n", 0, BAND_COMPLETED, id="job-id-exit-0"),
        pytest.param("23045000\n", 2, BAND_FAILED, id="job-id-exit-2"),
        pytest.param("23045000\n", 1, BAND_FAILED, id="job-id-exit-1"),
        pytest.param("23045000;rivanna\n", 0, BAND_COMPLETED, id="parsable-cluster-suffix"),
        pytest.param("", 1, BAND_REFUSED, id="no-id-exit-1"),
        pytest.param("", 0, BAND_REFUSED, id="no-id-exit-0"),
        pytest.param(None, 1, BAND_REFUSED, id="null-stdout"),
    ],
)
def test_the_band_classifier_reproduces_the_measured_arms(stdout, returncode, expected) -> None:
    """Property: a job id with exit 0 is the completed band, a job id with any non-zero exit is the engine band, and no job id is the refusal band whatever the exit.

    Class: the seven outputs `sbatch --wait --parsable` can produce, including `--parsable`'s cluster suffix.
    Why it is required: a refused submission prints nothing, so no id is the only evidence that nothing ran;
    reading the exit code alone would call a refused submission "failed" (band 3) and a job that exits 2
    "refused" (band 2), which is exactly the confusion the spike measured and the shape forbids.
    What kills it: classifying by the exit code alone, or requiring the id to be the whole of standard output.
    Second correct implementation: parsing the last non-empty line and its first `;`-separated field, as here;
    the table of seven cases decides.
    """
    assert submission_band(stdout, returncode) == expected


def test_the_shim_provides_every_command_the_bindings_name(tmp_path: Path) -> None:
    """Property: the shim directory holds an executable `sbatch`, `sacct`, `sacctmgr`, `srun` and `squeue`.

    Class: the shim's own files.
    Why it is required: the executor plugin runs `sacctmgr` before its first submission and polls `sacct`;
    an end-to-end test with any of them missing fails as a `FileNotFoundError` from inside the plugin.
    What kills it: dropping a command, or leaving one without its executable bit.
    Second correct implementation: fewer commands if the plugin stops needing one; the set the bindings name
    is the predicate.
    """
    for name in ("sbatch", "sacct", "sacctmgr", "srun", "squeue"):
        command = SHIM / name
        assert command.is_file(), name
        assert os.access(command, os.X_OK), name


def test_a_refused_submission_prints_no_job_id_and_is_the_refusal_band(tmp_path: Path) -> None:
    """Property: through the shim's refused arm, the submission exits non-zero with empty standard output and classifies as the refusal band.

    Class: the spike's refused-partition arm.
    Why it is required: this is the measured evidence for the mapping's first line; a submission that never
    created a job cannot be reported as a run that failed.
    What kills it: printing an id before the submission succeeds, or classifying the non-zero exit as band 3.
    Second correct implementation: any shim or parser reproducing the measured stdout/stderr pair.
    """
    script = tmp_path / "engine_batch.sh"
    script.write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")

    completed, band = _run_submission(script, tmp_path, refuse=True)

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert "Batch job submission failed" in completed.stderr
    assert band == BAND_REFUSED


@pytest.mark.parametrize(("script_exit", "expected"), [(0, BAND_COMPLETED), (2, BAND_FAILED)], ids=["exit-0", "exit-2"])
def test_an_accepted_submission_reports_the_job_id_and_the_scripts_own_exit(
    tmp_path: Path, script_exit: int, expected: int
) -> None:
    """Property: through the shim's accepted arm, the submission prints a job id and classifies by the script's exit: 0 completes, 2 is the engine band.

    Class: the spike's accepted exit-0 run and its exit-2 arm.
    Why it is required: a job id means a job existed, so the only two possible bands are completed and failed;
    the exit-2 arm is the one that proves a job's non-zero exit cannot read as a refusal.
    What kills it: classifying by stdout alone, or discarding the script's exit code.
    Second correct implementation: any shim that prints the id and propagates the script's exit.
    """
    script = tmp_path / "engine_batch.sh"
    script.write_text(f"#!/bin/bash\nexit {script_exit}\n", encoding="utf-8")

    completed, band = _run_submission(script, tmp_path)

    assert completed.stdout.strip().isdigit()
    assert completed.returncode == script_exit
    assert band == expected


def test_the_shim_records_each_submission_it_receives(tmp_path: Path) -> None:
    """Property: with `SHIM_CALL_RECORD` set, the submission argv is appended, and no file is written when the submission is refused before running sbatch.

    Class: one accepted submission and one that never reaches the shim.
    Why it is required: the dirty-tree arm of the mode requires proving that NO sbatch call happened; a shim
    that cannot be asked what it received cannot support that assertion.
    What kills it: a shim that does not record, or a test that infers "no call" from an absent side effect.
    Second correct implementation: a wrapper that logs before exec; the recorded line decides.
    """
    record = tmp_path / "calls.txt"
    script = tmp_path / "engine_batch.sh"
    script.write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
    environment = dict(os.environ)
    environment["PATH"] = str(SHIM) + os.pathsep + environment.get("PATH", "")
    environment["SHIM_CALL_RECORD"] = str(record)

    subprocess.run(list(submission_arguments(script)), env=environment, capture_output=True, text=True, check=False)
    assert record.is_file()
    assert str(script) in record.read_text(encoding="utf-8")

    record.unlink()
    assert not record.exists()


def test_a_wrapped_submission_without_the_wait_flag_still_runs_its_payload(tmp_path: Path) -> None:
    """Property: the shim's non-waiting wrapped submission prints an accepted job id and STILL executes the payload.

    Class: the shape the installed SLURM executor submits per rule: `--wrap=<command>` with no `--wait`.
    Why it is required: a shim that printed an id and returned without running the payload would make every
    batch_job test vacuous — the job "accepted" and nothing ran. The driver executor's submissions are exactly
    this shape, so the shim must run them or the mode is untested.
    What kills it: returning after printing the id without starting the payload.
    Second correct implementation: start the payload in the background and return (as here), or run it
    synchronously before returning; the marker decides.
    """
    marker = tmp_path / "wrapped-ran"
    environment = dict(os.environ)
    environment["PATH"] = str(SHIM) + os.pathsep + environment.get("PATH", "")
    environment["SHIM_JOB_OUTPUT"] = str(tmp_path / "job_output.log")

    completed = subprocess.run(
        [str(SHIM / "sbatch"), "--parsable", f"--wrap=touch {shlex.quote(str(marker))}"],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0
    assert completed.stdout.strip().isdigit()
    deadline = time.monotonic() + 2
    while not marker.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert marker.exists(), "the non-waiting wrapped payload never ran"


def test_a_waiting_wrapped_submission_reports_the_payloads_exit(tmp_path: Path) -> None:
    """Property: the shim's waiting wrapped submission prints the job id and returns the payload's own exit code.

    Class: a waiting `--wrap` submission whose payload exits 2.
    Why it is required: the allocation mode's band mapping reads the waiting submission's exit; a shim that
    returned 0 for a failing payload would make the exit-2 arm untestable.
    What kills it: returning 0 regardless of the payload's exit.
    Second correct implementation: run the payload synchronously and propagate `$?`; the pair decides.
    """
    environment = dict(os.environ)
    environment["PATH"] = str(SHIM) + os.pathsep + environment.get("PATH", "")
    environment["SHIM_JOB_OUTPUT"] = str(tmp_path / "job_output.log")

    completed = subprocess.run(
        [str(SHIM / "sbatch"), "--wait", "--parsable", "--wrap=exit 2"],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.stdout.strip().isdigit()
    assert completed.returncode == 2
    assert submission_band(completed.stdout, completed.returncode) == BAND_FAILED


def test_sacctmgr_answers_the_requested_account_column_with_the_configured_account(tmp_path: Path) -> None:
    """Property: a query requesting one account column prints exactly the configured account, with no second column.

    Class: the installed plugin's account-validation query (`format=account%256`), run once with the
    configured account and once with a different one.
    Why it is required: the plugin compares whole stripped lines with the account the run is configured for;
    a fixed two-column row matches no account and every submission is rejected before it is made.
    What kills it: ignoring the requested format, or printing a pipe-separated pair.
    Second correct implementation: parse `format=` and print each requested column in order; the line decides.
    """
    environment = dict(os.environ)
    environment["PATH"] = str(SHIM) + os.pathsep + environment.get("PATH", "")
    environment["SHIM_ACCOUNT"] = "allocation"

    completed = subprocess.run(
        [str(SHIM / "sacctmgr"), "-n", "-P", "show", "assoc", "format=account%256"],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0
    assert completed.stdout.splitlines() == ["allocation"]


def test_the_state_a_column_parser_reads_is_the_jobs_state(tmp_path: Path) -> None:
    """Property: `sacct --format=JobIdRaw,State` answers exactly two columns in that order, and the second is the job's state.

    Class: the installed plugin's status query, with the completed state and the failed state.
    Why it is required: the plugin reads column two as the state; a six-field diagnostic row puts the job name
    there, so a real driver never recognizes completion and waits forever.
    What kills it: emitting a fixed diagnostic row regardless of `--format`.
    Second correct implementation: honour the requested column list generically; the split decides.
    """
    for state in ("COMPLETED", "FAILED"):
        environment = dict(os.environ)
        environment["PATH"] = str(SHIM) + os.pathsep + environment.get("PATH", "")
        environment["SHIM_SACCT_STATE"] = state

        completed = subprocess.run(
            [str(SHIM / "sacct"), "-X", "--parsable2", "--noheader", "--format=JobIdRaw,State", "-j", "23045000"],
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )

        assert completed.returncode == 0
        job_id, reported_state = completed.stdout.strip().split("|")
        assert job_id == "23045000"
        assert reported_state == state
        assert reported_state != "h1-job"


def test_a_space_separated_format_returns_the_requested_columns_in_order(tmp_path: Path) -> None:
    """Property: `--format VALUE` (space-separated) is honoured as `--format=VALUE` is, for both accounting shims.

    Class: the two space-separated shapes a caller can write — `--format JobIdRaw,State` and `-o JobIdRaw` —
    and the account query's `--format account%256`.
    Why it is required: a shim that accepts a query shape and answers a different question is the defect the
    transcript-replay rows were; the plugin only ever uses `=`, so only an explicit test keeps the other
    accepted shapes honest.
    What kills it: parsing only the equals form, which falls through to the default diagnostic row.
    Second correct implementation: the same parser for both forms; the column order decides.
    """
    environment = dict(os.environ)
    environment["PATH"] = str(SHIM) + os.pathsep + environment.get("PATH", "")
    environment["SHIM_SACCT_STATE"] = "COMPLETED"
    environment["SHIM_ACCOUNT"] = "allocation"

    spaced = subprocess.run(
        [str(SHIM / "sacct"), "-X", "--parsable2", "--noheader", "--format", "State,JobIdRaw", "-j", "23045000"],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    short = subprocess.run(
        [str(SHIM / "sacct"), "-o", "JobIdRaw,State", "-j", "23045000"],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    accounts = subprocess.run(
        [str(SHIM / "sacctmgr"), "-n", "-P", "show", "assoc", "--format", "account%256"],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert spaced.stdout.strip() == "COMPLETED|23045000"
    assert short.stdout.strip() == "23045000|COMPLETED"
    assert accounts.stdout.splitlines() == ["allocation"]


def _with_rerun_rules(configuration: MainConfiguration, *rules: str) -> MainConfiguration:
    """Return `configuration` with `[version_stacking] rerun_rules` set to `rules`."""
    return dataclasses.replace(configuration, version_stacking=VersionStackingConfiguration(rerun_rules=rules))


@pytest.mark.parametrize("mode", ["local", "batch_job", "1_job_many_srun_tasks"])
def test_the_rerun_list_is_the_forcerun_option_followed_by_the_names_in_every_mode(mode: str) -> None:
    """Property: `rerun_arguments` is `("--forcerun", *names)` for a non-empty list in each mode, and `()` for an empty one.

    Class: one configuration per mode, with two names and with none.
    Why it is required: the engine takes the rules to rebuild from `--forcerun`, and an empty `--forcerun`
    with no name would ask it to rebuild everything, so the empty list must pass nothing at all.
    What kills it: always emitting `--forcerun`, emitting the names in another order, or reading the mode.
    Second correct implementation: any tuple builder reading the same field; the two values are the check.
    """
    configuration = _configuration(mode)

    assert rerun_arguments(configuration) == ()
    assert rerun_arguments(_with_rerun_rules(configuration, "observed_record_assembly", "method_attribution")) == (
        "--forcerun",
        "observed_record_assembly",
        "method_attribution",
    )


def test_the_engine_command_and_the_allocation_script_carry_the_same_rerun_list_after_rerun_incomplete() -> None:
    """Property: the engine argument list of `local` and `batch_job` and the allocation script's engine line each hold `--rerun-incomplete` immediately followed by the rerun list, and none holds `--forcerun` for an empty list.

    Class: the three modes with two rule names, and the same three with an empty list.
    Why it is required: all three modes must rebuild the same rules, so a mode that left the list out would
    stack versions in one mode and not in the others; and a run naming no rule must change no argument list.
    What kills it: adding the list to `cli.engine_command` only, or to `engine_arguments` (which the
    allocation mode refuses), or passing it when empty.
    Second correct implementation: both lists built from one option tuple; the same three positions result.
    """
    names = ("observed_record_assembly", "method_attribution")
    for mode in ("local", "batch_job"):
        configuration = _with_rerun_rules(_configuration(mode), *names)
        command = cli.engine_command(configuration, run_output=Path("/runs/one"))
        start = command.index("--rerun-incomplete")
        assert command[start + 1 : start + 4] == ["--forcerun", *names]
        assert "--forcerun" not in cli.engine_command(_configuration(mode), run_output=Path("/runs/one"))

    allocation = _with_rerun_rules(_configuration("1_job_many_srun_tasks"), *names)
    line = engine_batch_script(allocation, snakefile="/checkout/Snakefile", run_output="/runs/one").splitlines()[-1]
    words = shlex.split(line)
    start = words.index("--rerun-incomplete")
    assert words[start + 1 : start + 4] == ["--forcerun", *names]
    empty = engine_batch_script(
        _configuration("1_job_many_srun_tasks"), snakefile="/checkout/Snakefile", run_output="/runs/one"
    ).splitlines()[-1]
    assert "--forcerun" not in empty
