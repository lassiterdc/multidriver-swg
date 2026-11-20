"""Check the real-cluster measurement material under the `local` mode and the shimmed scheduler.

The material is the toolkit's half of a measurement that needs a cluster, so what can be checked here is
what does not: the probe workflow's parse and handler lines, the launcher's arms and its argument list, the
checkpoint test workflow's two launches, and the collector's reading of a run directory. Every engine check
runs the real engine over a committed copy of the toolkit that includes the probe, with the launcher as the
measurement uses it, because the probe's whole purpose is to include the toolkit's workflow file unchanged and
to run through the command's own bindings, and a mocked engine would let a broken include pass.

Every check names the wrong implementation it is red under.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import time
from pathlib import Path

import pytest

import multidriver_swg.workflow.pre_run as pre_run
from multidriver_swg.run_configuration.main_configuration import MainConfiguration
from multidriver_swg.run_configuration.provenance import ImplementationIdentity, capture
from multidriver_swg.side_record import SideRecord
from multidriver_swg.workflow.cli import SNAKEFILE, engine_command
from multidriver_swg.workflow.end_of_run import END_OF_RUN_RECORD
from multidriver_swg.workflow.exercised_set import PROVENANCE_FILE, write_provenance
from multidriver_swg.workflow.run_start import RUN_START_FILE, RunStart
from tests.workflow.conftest import REPO_ROOT, ToolkitCopy, _git
from tests.workflow.real_cluster import collect_measurement, launch_probe

PROJECT = "smoke.run"

SHIM = Path(__file__).resolve().parent / "slurm_shim"

MATERIAL = REPO_ROOT / "tests" / "workflow" / "real_cluster"

# A selection document the artifact's own reader admits, holding the fit `compound.rainfall_depth`.
SELECTION = """rule = "modelled-selection:1"
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


class _Estate:
    """The estate a launch reads: the configuration and the run output."""

    def __init__(self, configuration: Path, run_output: Path) -> None:
        self.configuration = configuration
        self.run_output = run_output


def _copy_with_material(destination: Path) -> ToolkitCopy:
    """Copy the package and the measurement material into `destination`, commit them, and return the copy."""
    package = destination / "src" / "multidriver_swg"
    package.parent.mkdir(parents=True)
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc")
    shutil.copytree(REPO_ROOT / "src" / "multidriver_swg", package, ignore=ignore)
    shutil.copytree(MATERIAL, destination / "tests" / "workflow" / "real_cluster", ignore=ignore)
    shutil.copyfile(REPO_ROOT / ".gitignore", destination / ".gitignore")
    _git(destination, "init", "-q")
    _git(destination, "add", "-A")
    _git(destination, "commit", "-q", "-m", "toolkit copy")
    return ToolkitCopy(
        root=destination,
        package=package,
        snakefile=package / "workflow" / "Snakefile",
        commit=_git(destination, "rev-parse", "HEAD"),
    )


def _estate(tmp_path: Path, toolkit: ToolkitCopy, *, mode: str = "local") -> _Estate:
    """Write an estate binding the three keys, a project declaring the copy's commit, and a configuration in `mode`."""
    root = tmp_path / "estate"
    project_directory = root / "projects" / "smoke"
    project_directory.mkdir(parents=True)
    (root / "config").mkdir(parents=True)
    run_output = tmp_path / "run"
    run_output.mkdir()
    (root / "config" / "paths_test.toml").write_text(
        "\n".join(
            [
                'rule = "path-bindings:1"',
                "",
                "[paths]",
                f'"{PROJECT}.project_directory" = "{project_directory}"',
                f'"{PROJECT}.run_output" = "{run_output}"',
                f'"{PROJECT}.selection" = "{project_directory / "selection.toml"}"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (project_directory / "toolkit_revision.toml").write_text(
        f'rule = "toolkit-revision:1"\ncommit = "{toolkit.commit}"\n', encoding="utf-8"
    )
    section = (
        "[local]\ncores = 1\n"
        if mode == "local"
        else '[slurm]\npartition = "standard"\naccount = "allocation"\ntime_minutes = 30\ncpus_per_task = 1\n'
    )
    configuration = root / "config" / "run.toml"
    configuration.write_text(
        'rule = "main-configuration:1"\n'
        f'estate_root = "{root}"\n'
        'bindings = "config/paths_test.toml"\n'
        f'project = "{PROJECT}"\n'
        f'mode = "{mode}"\n\n{section}',
        encoding="utf-8",
    )
    return _Estate(configuration, run_output)


def _environment(tmp_path: Path, *, shim: bool = False) -> dict[str, str]:
    """Return the environment an engine runs in: no inherited import path, the engine cache under the test's tree."""
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    environment["XDG_CACHE_HOME"] = str(tmp_path / "cache")
    if shim:
        environment["PATH"] = str(SHIM) + os.pathsep + environment.get("PATH", "")
    return environment


def _launch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    toolkit: ToolkitCopy,
    estate: _Estate,
    arm: str,
    *,
    shim: bool = False,
    selection_from: Path | None = None,
) -> int:
    """Launch `arm` over the copy's material with the revision gate pointed at the copy; return the exit status."""
    monkeypatch.setattr(pre_run, "running_package_directory", lambda: toolkit.package)
    return launch_probe.launch(
        estate.configuration,
        arm,
        selection_from=selection_from,
        directory=toolkit.root / "tests" / "workflow" / "real_cluster",
        environ=_environment(tmp_path, shim=shim),
    )


def _lines(run_output: Path, name: str) -> list[dict[str, object]]:
    path = run_output / "measurement" / name
    if not path.is_file():
        return []
    return [json.loads(text) for text in path.read_text(encoding="utf-8").splitlines() if text.strip()]


def _report_categories(report: Path) -> dict[str, dict[str, list[str]]]:
    """Return the category table the report page carries, read from the page's own data."""
    text = report.read_text(encoding="utf-8")
    prefix = "var categories = "
    line = next(candidate for candidate in text.splitlines() if candidate.strip().startswith(prefix))
    return json.loads(line.strip().removeprefix(prefix).removesuffix(";"))


@pytest.fixture
def copy_and_estate(tmp_path: Path) -> tuple[ToolkitCopy, _Estate]:
    toolkit = _copy_with_material(tmp_path / "toolkit")
    return toolkit, _estate(tmp_path, toolkit)


def test_a_run_arm_writes_timed_parse_lines_both_handlers_and_the_end_of_run_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, copy_and_estate: tuple[ToolkitCopy, _Estate]
) -> None:
    """Property: the probe over the copy parses with a timed check, runs `onstart` and `onsuccess`, and the toolkit's own record exists.

    Class: the `run` arm under `local` over a clean committed copy that includes the probe.
    Why it is required: the probe exists to include the toolkit's workflow file unchanged and still log; a
    probe whose include broke would run no rule, and one whose timing was zero would measure nothing.
    What kills it: an include that is not the toolkit's file (no end-of-run record), a parse line written
    before the check returned, a handler that is not declared, or `onerror` firing on success.
    Second correct implementation: any workflow that logs the same facts; the files are the check.
    """
    toolkit, estate = copy_and_estate

    status = _launch(monkeypatch, tmp_path, toolkit, estate, "run")

    assert status == 0
    parses = _lines(estate.run_output, "parses.jsonl")
    assert parses
    assert all(isinstance(line["seconds"], float) and line["seconds"] > 0 for line in parses)  # type: ignore[operator]
    assert all({"host", "pid", "slurm_job_id", "seconds", "utc"} <= set(line) for line in parses)
    handlers = [line["handler"] for line in _lines(estate.run_output, "handlers.jsonl")]
    assert "onstart" in handlers
    assert "onsuccess" in handlers
    assert "onerror" not in handlers
    assert (estate.run_output / END_OF_RUN_RECORD).is_dir()


def test_a_noop_launch_parses_again_and_runs_no_handler(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, copy_and_estate: tuple[ToolkitCopy, _Estate]
) -> None:
    """Property: a second launch on a finished output adds parse lines and no handler line.

    Class: the `run` arm, then the `noop` arm on the same output.
    Why it is required: the handlers' behaviour on a re-run that finds everything up to date is one of the
    facts the measurement records, and the local result is the baseline the cluster is compared with.
    What kills it: a probe that logs handlers from the parse instead of from the handler, or one that wipes
    the measurement directory.
    Second correct implementation: counting lines per file before and after; the counts are the check.
    """
    toolkit, estate = copy_and_estate
    assert _launch(monkeypatch, tmp_path, toolkit, estate, "run") == 0
    parses_before = len(_lines(estate.run_output, "parses.jsonl"))
    handlers_before = len(_lines(estate.run_output, "handlers.jsonl"))

    status = _launch(monkeypatch, tmp_path, toolkit, estate, "noop")

    assert status == 0
    assert len(_lines(estate.run_output, "parses.jsonl")) > parses_before
    assert len(_lines(estate.run_output, "handlers.jsonl")) == handlers_before


def test_a_dry_run_parses_and_runs_no_handler_and_builds_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, copy_and_estate: tuple[ToolkitCopy, _Estate]
) -> None:
    """Property: the `dryrun` arm on a fresh output writes a parse line, no handler line, and no end-of-run record.

    Class: the dry-run arm.
    Why it is required: the measurement states that handlers do not run on a dry run; this is the local
    baseline, and a dry run that built anything would be a run.
    What kills it: a launcher that drops the dry-run flag, or handlers declared so they run at parse.
    Second correct implementation: the engine's own dry-run flag placed elsewhere in the list; same effect.
    """
    toolkit, estate = copy_and_estate

    _launch(monkeypatch, tmp_path, toolkit, estate, "dryrun")

    # The exit status is not asserted: the command's argument list carries the report pair, and the engine
    # refuses to build the report after a dry run because the report's files do not exist. The files are the check.
    assert _lines(estate.run_output, "parses.jsonl")
    assert _lines(estate.run_output, "handlers.jsonl") == []
    assert not (estate.run_output / END_OF_RUN_RECORD).exists()


@pytest.mark.skipif(
    hasattr(os, "geteuid") and os.geteuid() == 0, reason="a superuser writes into a read-only directory"
)
def test_the_fail_arm_exits_non_zero_runs_onerror_and_writes_no_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, copy_and_estate: tuple[ToolkitCopy, _Estate]
) -> None:
    """Property: with the second rule's directory read-only the launch exits non-zero, an `onerror` line is written, and no end-of-run record exists.

    Class: the `fail` arm.
    Why it is required: the measurement states that the final rule does not run when an upstream job fails.
    What kills it: a launcher that does not make the directory unwritable, or a final rule that runs anyway.
    Second correct implementation: any way of failing that rule's job; the three outcomes are the check.
    """
    toolkit, estate = copy_and_estate
    try:
        status = _launch(monkeypatch, tmp_path, toolkit, estate, "fail")
    finally:
        # The engine removes the failed rule's directory, so it may already be gone.
        failing = estate.run_output / launch_probe.FAILING_RULE_DIRECTORY
        if failing.exists():
            failing.chmod(0o755)

    assert status != 0
    handlers = [line["handler"] for line in _lines(estate.run_output, "handlers.jsonl")]
    assert "onerror" in handlers
    assert "onsuccess" not in handlers
    assert not (estate.run_output / END_OF_RUN_RECORD).exists()


def test_the_launchers_argument_list_is_the_commands_apart_from_the_workflow_file(tmp_path: Path) -> None:
    """Property: the probe's argument list equals `cli.engine_command`'s with only the value after `--snakefile` replaced, and a dry run adds only `-n`.

    Class: both engine-launch modes, a local and a batch configuration.
    Why it is required: a measurement of a different argument list measures something the command does not do.
    What kills it: a launcher that builds its own argument list, or drops the report pair or the mode options.
    Second correct implementation: building from the command's function and replacing the value; that is this.
    """
    run_output = tmp_path / "run"
    probe = tmp_path / "probe.smk"
    for mode, section in (
        ("local", "[local]\ncores = 2\n"),
        ("batch_job", '[slurm]\npartition = "standard"\naccount = "allocation"\ntime_minutes = 30\njobs = 4\n'),
    ):
        configuration_path = tmp_path / f"{mode}.toml"
        configuration_path.write_text(
            f'rule = "main-configuration:1"\nestate_root = "{tmp_path}"\nbindings = "paths.toml"\n'
            f'project = "{PROJECT}"\nmode = "{mode}"\n\n{section}',
            encoding="utf-8",
        )
        configuration = MainConfiguration.load(configuration_path)
        command = engine_command(configuration, run_output=run_output)
        index = command.index("--snakefile") + 1
        assert command[index] == str(SNAKEFILE)

        probed = launch_probe.probe_arguments(configuration, run_output=run_output, snakefile=probe)

        assert probed[:index] == command[:index]
        assert probed[index] == str(probe)
        assert probed[index + 1 :] == command[index + 1 :]
        dry = launch_probe.probe_arguments(configuration, run_output=run_output, snakefile=probe, dry_run=True)
        assert dry == [*probed, "-n"]


def test_the_allocation_launch_submits_the_probe_through_the_commands_own_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Property: under the allocation mode the launcher writes the plan's script naming the probe and submits it; a dry run's script ends in `-n`.

    Class: the `run` and `dryrun` arms under `1_job_many_srun_tasks`, with the scheduler's commands shimmed.
    Why it is required: the allocation binding is a submission and not an argument list, so the launcher
    must take the plan and change only the workflow file inside it.
    What kills it: running an engine argument list for this mode, or a script that names the toolkit's file.
    Second correct implementation: any launcher that submits the plan's own script; the script text is the check.
    """
    toolkit = _copy_with_material(tmp_path / "toolkit")
    estate = _estate(tmp_path, toolkit, mode="1_job_many_srun_tasks")

    status = _launch(monkeypatch, tmp_path, toolkit, estate, "run", shim=True)

    assert status == 0
    script = (estate.run_output / "engine_batch.sh").read_text(encoding="utf-8")
    probe = toolkit.root / "tests" / "workflow" / "real_cluster" / "probe.smk"
    assert str(probe) in script
    assert str(SNAKEFILE) not in script
    assert not script.rstrip("\n").endswith(" -n")
    assert _lines(estate.run_output, "parses.jsonl")
    assert (estate.run_output / END_OF_RUN_RECORD).is_dir()

    dry_estate = _estate(tmp_path / "dry", toolkit, mode="1_job_many_srun_tasks")
    _launch(monkeypatch, tmp_path, toolkit, dry_estate, "dryrun", shim=True)
    assert (dry_estate.run_output / "engine_batch.sh").read_text(encoding="utf-8").rstrip("\n").endswith(" -n")
    assert not (dry_estate.run_output / END_OF_RUN_RECORD).exists()


def test_the_launcher_refuses_an_arm_it_does_not_have() -> None:
    """Property: an unknown arm raises `ValueError` naming the arms, and the command line refuses it.

    Class: a misspelled arm.
    Why it is required: an unknown arm falling back to the probe would run a measurement the operator did
    not ask for and record it under another name.
    What kills it: a default branch in the choice of workflow file.
    Second correct implementation: a dictionary lookup; the refusal is the check.
    """
    with pytest.raises(ValueError, match="checkpoint-resume"):
        launch_probe.snakefile_for("runn")
    with pytest.raises(SystemExit):
        launch_probe.main(["configuration.toml", "--arm", "runn"])
    assert launch_probe.snakefile_for("run").name == "probe.smk"
    assert launch_probe.snakefile_for("checkpoint-noruling").name == "checkpoint_probe.smk"
    assert launch_probe.snakefile_for("checkpoint-resume").name == "checkpoint_probe.smk"
    assert launch_probe.snakefile_for("graph-checkpoint-noruling").name == "probe.smk"
    assert launch_probe.snakefile_for("graph-checkpoint-resume").name == "probe.smk"


def test_a_checkpoint_launch_without_a_ruling_stops_after_the_licence_and_the_resume_runs_the_rest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, copy_and_estate: tuple[ToolkitCopy, _Estate]
) -> None:
    """Property: with no pooling entry the run exits 0 after the licence with its report and final output; adding the entry and launching again runs the merge, its consumer and the final rule, not the licence.

    Class: the two checkpoint arms on one output.
    Why it is required: this is the engine behaviour the stop rests on, and a toy workflow is where it is
    first checked; a fixed target list would build the merge in the first launch.
    What kills it: `all` or the final rule taking a static input list, a licence that is not a checkpoint, or
    a report that does not hold the licence item.
    Second correct implementation: a licence whose output is a directory read after the checkpoint; the same
    files exist and the same ones do not.
    """
    toolkit, estate = copy_and_estate
    output = estate.run_output

    first = _launch(monkeypatch, tmp_path, toolkit, estate, "checkpoint-noruling")

    assert first == 0
    assert (output / "licence" / "licence.json").is_file()
    assert not (output / "merge").exists()
    assert not (output / "consumer").exists()
    final = output / "closing" / "record.txt"
    assert final.is_file()
    assert "merge/merge.json" not in final.read_text(encoding="utf-8")
    categories = _report_categories(output / "report.html")
    assert set(categories) == {"Licence"}
    assert [path for paths in categories["Licence"].values() for path in paths] == ["licence/licence.html"]
    licence_time = (output / "licence" / "licence.json").stat().st_mtime_ns
    evaluations = _lines(output, "checkpoint_evaluations.jsonl")
    assert evaluations
    assert {line["covered"] for line in evaluations} == {False}
    assert all(isinstance(line["seconds"], float) for line in evaluations)
    parses_after_first = len(_lines(output, "checkpoint_parses.jsonl"))

    second = _launch(monkeypatch, tmp_path, toolkit, estate, "checkpoint-resume")

    assert second == 0
    assert (output / "merge" / "merge.json").is_file()
    assert (output / "consumer" / "consumer.json").is_file()
    assert "merge/merge.json" in final.read_text(encoding="utf-8")
    assert (output / "licence" / "licence.json").stat().st_mtime_ns == licence_time
    assert set(_report_categories(output / "report.html")) == {"Licence", "Merge"}
    assert True in {line["covered"] for line in _lines(output, "checkpoint_evaluations.jsonl")}
    assert len(_lines(output, "checkpoint_parses.jsonl")) > parses_after_first


def _licence_end_time(run_output: Path) -> float:
    """Return the `endtime` of the engine's metadata record for the licence's output."""
    from base64 import urlsafe_b64encode

    target = "observed_record_licence/licence.json"
    record = run_output / ".snakemake" / "metadata" / urlsafe_b64encode(target.encode()).decode()
    return float(json.loads(record.read_text(encoding="utf-8"))["endtime"])


def test_the_graph_checkpoint_arms_stop_the_real_graph_after_the_licence_and_resume_at_the_merge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, copy_and_estate: tuple[ToolkitCopy, _Estate]
) -> None:
    """Property: the no-ruling arm ends the toolkit's graph after its licence with exit 0, its report and its record; the resume arm over the same output builds the merge and the attribution row and leaves the licence as it was.

    Class: the two graph arms under `local` over a committed copy, with the licence requirement carried by
    the probe's file and the selection copied in by the resume arm.
    Why it is required: this is the toolkit's half of the real-graph checkpoint arm, and it is the only
    place the probe's setting of the stub licence, the pre-run step's choice of target list from the
    selection file and the stop are exercised together before a cluster is used.
    What kills it: a probe that does not set the constant (the licence would require nothing and the merge
    would be built in the first launch), a resume arm that copies the selection after the pre-run step (the
    run-start record would still name no selection and the second launch would stop again), or a launcher
    that rewrote the requirement and so rebuilt the licence.
    Second correct implementation: a requirement passed through an environment variable the licence reads;
    the same directories exist and the same licence time is kept.
    """
    toolkit, estate = copy_and_estate
    output = estate.run_output
    selection_path = tmp_path / "estate" / "projects" / "smoke" / "selection.toml"
    selection_source = tmp_path / "selection_present.toml"
    selection_source.write_text(SELECTION, encoding="utf-8")

    first = _launch(monkeypatch, tmp_path, toolkit, estate, "graph-checkpoint-noruling")

    assert first == 0
    assert json.loads((output / "measurement" / "licence_required.json").read_text(encoding="utf-8")) == [
        "compound.rainfall_depth"
    ]
    licence = output / "observed_record_licence" / "licence.json"
    assert json.loads(licence.read_text(encoding="utf-8")) == {"required_entries": ["compound.rainfall_depth"]}
    assert not (output / "observed_record_assembly").exists()
    assert (output / END_OF_RUN_RECORD).is_dir()
    assert not selection_path.exists()
    licence_end = _licence_end_time(output)
    time.sleep(1.1)

    second = _launch(monkeypatch, tmp_path, toolkit, estate, "graph-checkpoint-resume", selection_from=selection_source)

    assert second == 0
    assert selection_path.read_text(encoding="utf-8") == SELECTION
    assert (output / "observed_record_assembly" / "observed_record.json").is_file()
    assert (output / "method_attribution" / "citation_section.html").is_file()
    assert _licence_end_time(output) == licence_end


def test_the_graph_checkpoint_arms_refuse_a_selection_file_where_it_must_be_absent_or_missing_where_it_must_be_given(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, copy_and_estate: tuple[ToolkitCopy, _Estate]
) -> None:
    """Property: the no-ruling arm refuses an existing selection file and leaves it intact, the resume arm refuses a missing source, any other arm refuses a source, and the command line enforces the same pairing.

    Class: the three argument shapes the two arms and the other arms can be given.
    Why it is required: a no-ruling launch over an operator's selection would not stop, and would record a
    measurement under the name of one that did; a resume without a selection would stop again.
    What kills it: an arm that deletes or overwrites the selection file, or one that ignores `--selection-from`.
    Second correct implementation: a refusal raised by the argument parser alone; the library call is the
    one checked here, and the parser's pairing is checked beside it.
    """
    toolkit, estate = copy_and_estate
    selection_path = tmp_path / "estate" / "projects" / "smoke" / "selection.toml"
    selection_path.write_text(SELECTION, encoding="utf-8")

    with pytest.raises(ValueError, match="to be absent"):
        _launch(monkeypatch, tmp_path, toolkit, estate, "graph-checkpoint-noruling")
    assert selection_path.read_text(encoding="utf-8") == SELECTION
    selection_path.unlink()
    with pytest.raises(ValueError, match="requires a selection file"):
        _launch(monkeypatch, tmp_path, toolkit, estate, "graph-checkpoint-resume")
    with pytest.raises(ValueError, match="takes no selection file"):
        _launch(monkeypatch, tmp_path, toolkit, estate, "run", selection_from=tmp_path / "anything.toml")
    assert not selection_path.exists()
    with pytest.raises(SystemExit):
        launch_probe.main([str(estate.configuration), "--arm", "graph-checkpoint-resume"])
    with pytest.raises(SystemExit):
        launch_probe.main([str(estate.configuration), "--arm", "run", "--selection-from", "x.toml"])


def _synthetic_run_directory(root: Path) -> Path:
    """Build a run directory holding what a measured run leaves, with two scheduler jobs named in its log."""
    run = root / "run"
    (run / ".snakemake" / "log").mkdir(parents=True)
    (run / ".snakemake" / "log" / "2026-01-01T000000.000000.snakemake.log").write_text(
        "\n".join(
            [
                "rule observed_record_assembly:",
                "    output: observed_record_assembly/observed_record.json",
                "    jobid: 2",
                "Job 2 has been submitted with SLURM jobid 4242 (log: x)",
                "localrule end_of_run:",
                "    jobid: 1",
                "checkpoint licence:",
                "    jobid: 5",
                "Job 5 has been submitted with SLURM jobid 4244 (log: z)",
                "rule combined_event_identification:",
                "    jobid: 3",
                "Job 3 has been submitted with SLURM jobid 4243 (log: y)",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    measurement = run / "measurement"
    measurement.mkdir()
    (measurement / "parses.jsonl").write_text(
        "".join(
            json.dumps({"host": host, "pid": pid, "slurm_job_id": job, "seconds": seconds, "utc": "t"}) + "\n"
            for host, pid, job, seconds in (
                ("login", 1, None, 0.25),
                ("node-a", 2, "4242", 0.5),
                ("node-a", 3, "4242", 0.75),
            )
        ),
        encoding="utf-8",
    )
    (measurement / "handlers.jsonl").write_text(
        json.dumps({"handler": "onstart", "host": "login", "pid": 1, "slurm_job_id": None, "utc": "t"}) + "\n",
        encoding="utf-8",
    )
    RunStart(
        main_configuration=str(root / "run.toml"),
        targets=("observed_record_assembly/observed_record.json", "missing/never.json", END_OF_RUN_RECORD),
        selection=None,
        judgement="kept-absent",
    ).write(run / RUN_START_FILE)
    record = run / END_OF_RUN_RECORD
    record.mkdir(parents=True)
    identity = ImplementationIdentity(system="observed-record-assembly", implementation="stub", code_version="abc")
    write_provenance(
        record / PROVENANCE_FILE,
        capture(inputs={}, seeds={}, identities={identity.system: identity}),
        exercised=[identity.system],
        invocations=("observed_record_assembly",),
        side_record=SideRecord({}),
    )
    return run


def _write_metadata(run: Path, target: str, rule: str) -> None:
    """Write the engine's metadata record for `target` by the short form of its identifier."""
    from base64 import urlsafe_b64encode

    directory = run / ".snakemake" / "metadata"
    directory.mkdir(parents=True, exist_ok=True)
    document = {"record_format_version": 6, "rule": rule, "incomplete": False, "starttime": None, "endtime": 1.0}
    (directory / urlsafe_b64encode(target.encode()).decode()).write_text(json.dumps(document), encoding="utf-8")


def test_the_collector_prints_the_job_map_the_accounting_rows_the_hosts_and_the_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Property: over a synthetic run directory with the shimmed `sacct` on the path, the reading holds the rule-to-job map, an accounting row per job, the hosts with the parse statistics, each target's producer and the provenance summary.

    Class: one directory carrying every kind of file the collector reads, and one target with no record.
    Why it is required: the estate's records are made of this output, so a section that is empty, wrong or
    refused for a form it should read would lose evidence from a measurement that cannot be repeated cheaply.
    What kills it: a log reader that misses the `localrule` block or the submitted-job lines, a host table
    built from the wrong file, a target reader that raises on a missing record, a provenance reader other
    than the toolkit's.
    Second correct implementation: another Markdown layout with the same facts; the facts are the check.
    """
    run = _synthetic_run_directory(tmp_path)
    _write_metadata(run, "observed_record_assembly/observed_record.json", "observed_record_assembly")
    (run / "observed_record_assembly").mkdir()
    (run / "observed_record_assembly" / "observed_record.json").write_bytes(b"")
    monkeypatch.setenv("PATH", str(SHIM) + os.pathsep + os.environ.get("PATH", ""))

    reading = collect_measurement.collect(run)

    assert re.search(r"\| observed_record_assembly \| 2 \| 4242 \|", reading)
    assert re.search(r"\| combined_event_identification \| 3 \| 4243 \|", reading)
    assert re.search(r"\| end_of_run \| 1 \|", reading)
    assert re.search(r"\| licence \| 5 \| 4244 \|", reading)
    assert "sacct -X -P -j 4242 " in reading
    assert "sacct -X -P -j 4243 " in reading
    assert "4242|h1-job|COMPLETED|0:0|shim-node" in reading
    assert "4243|h1-job|COMPLETED|0:0|shim-node" in reading
    assert re.search(r"\| node-a \| 2 \|", reading)
    assert "seconds: median 0.500000, maximum 0.750000" in reading
    assert re.search(
        r"\| observed_record_assembly/observed_record.json \| True \| observed_record_assembly \|", reading
    )
    assert re.search(r"\| missing/never.json \| False \| no record: ", reading)
    assert "exercised systems (1): observed-record-assembly" in reading
    assert "implementations: stub" in reading
    assert "rules that ran in the last invocation: observed_record_assembly" in reading


def test_the_collector_says_so_when_a_run_directory_has_no_scheduler_job_or_record(tmp_path: Path) -> None:
    """Property: an empty run directory prints every section with the statement that its source is absent, and the command exits 0.

    Class: a directory with nothing in it.
    Why it is required: a measurement that failed early is a measurement that still has to be read.
    What kills it: a collector that raises on a missing log, measurement file or record.
    Second correct implementation: omitting the absent sections; then the statements here are red.
    """
    run = tmp_path / "empty"
    run.mkdir()

    reading = collect_measurement.collect(run)

    assert "no engine log" in reading
    assert "names no scheduler job" in reading
    assert "no usable run-start record" in reading
    assert f"no `{END_OF_RUN_RECORD}/{PROVENANCE_FILE}`" in reading
    assert collect_measurement.main([str(run)]) == 0
    assert collect_measurement.main([str(tmp_path / "absent")]) == 2


def test_the_material_carries_no_machine_value() -> None:
    """Property: the material's text holds no absolute cluster path and no scheduler value.

    Class: the five text files, read as text.
    Why it is required: the material is committed to the toolkit, and a machine value in it is a leak of
    the estate's contents.
    What kills it: a partition or account literal, or an absolute home, scratch or project path.
    Second correct implementation: the repository's own grep; the same patterns are the check.
    """
    pattern = re.compile(r"(slurm_partition|slurm_account)=[A-Za-z0-9_]|[\"']/(home|scratch|project|gpfs|sfs|nfs)/")
    for name in ("probe.smk", "checkpoint_probe.smk", "launch_probe.py", "collect_measurement.py", "README.md"):
        text = (MATERIAL / name).read_text(encoding="utf-8")
        assert not pattern.search(text), name
