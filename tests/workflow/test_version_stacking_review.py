"""Cross-boundary tests for version stacking: the one reader, the engine option's place, the record's fields, and the refusal's side effects.

These checks cross boundaries the unit tests of each function cannot see. The mixed set is decided in one
place and read in two (the refusal and the provenance file), the rerun list reaches the engine through three
callers, and a refusal must leave no file that describes the run it refused. Each check reads the source as
a call graph, reads the three generated engine invocations side by side, or runs the end-of-run function
over synthetic metadata, so it fails on those changes and not on one function's return value.

They deliberately do not restate the unit checks: the exercised-set, mode and configuration tests
own each function's cases, and a test that copied them would double the maintenance.
"""

from __future__ import annotations

import ast
import dataclasses
import shlex
from pathlib import Path

import pytest

import multidriver_swg.workflow.cli as cli
import multidriver_swg.workflow.end_of_run as end_of_run
import multidriver_swg.workflow.exercised_set as exercised_set
import multidriver_swg.workflow.run_start as run_start_module
from multidriver_swg.run_configuration.main_configuration import (
    LocalConfiguration,
    MainConfiguration,
    SlurmConfiguration,
    VersionStackingConfiguration,
)
from multidriver_swg.run_configuration.provenance import ImplementationIdentity
from multidriver_swg.side_record import SideRecord
from multidriver_swg.workflow.end_of_run import write_end_of_run_record
from multidriver_swg.workflow.exercised_set import (
    PROVENANCE_FILE,
    ExercisedSetError,
    mixed_systems,
    read_provenance,
)
from multidriver_swg.workflow.graph import GRAPH
from multidriver_swg.workflow.modes import engine_batch_script, rerun_arguments
from tests.workflow.test_exercised_set import ORA_TARGETS, _run_start, _synthetic_run, _write_identity

WORKFLOW_SOURCES = sorted(Path(exercised_set.__file__).resolve().parent.glob("*.py"))
RULES = ("marginal_distribution_fitting", "observed_record_assembly")


def _calls_by_function(path: Path, called: str) -> set[str]:
    """Return the names of the functions in `path` whose body calls `called` by name."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    owners: set[str] = set()
    for function in ast.walk(tree):
        if not isinstance(function, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        for node in ast.walk(function):
            if isinstance(node, ast.Call):
                target = node.func
                name = target.id if isinstance(target, ast.Name) else getattr(target, "attr", None)
                if name == called:
                    owners.add(function.name)
    return owners


def _configuration(mode: str, rerun_rules: tuple[str, ...]) -> MainConfiguration:
    """A validated configuration in `mode` naming `rerun_rules`, built directly so the checks need no file."""
    common = {
        "rule": "main-configuration:1",
        "estate_root": "/estate",
        "bindings": "config/paths.toml",
        "project": "smoke.run",
        "mode": mode,
        "version_stacking": VersionStackingConfiguration(rerun_rules=rerun_rules),
    }
    if mode == "local":
        return MainConfiguration(local=LocalConfiguration(cores=1), **common)  # type: ignore[arg-type]
    values = {"partition": "standard", "account": "allocation", "time_minutes": 30}
    values["cpus_per_task" if mode == "1_job_many_srun_tasks" else "jobs"] = 4  # type: ignore[assignment]
    return MainConfiguration(slurm=SlurmConfiguration(**values), **common)  # type: ignore[arg-type]


def _engine_words(mode: str, rerun_rules: tuple[str, ...], tmp_path: Path) -> list[str]:
    """Return the engine invocation of `mode` as the words a shell would read from it."""
    configuration = _configuration(mode, rerun_rules)
    if mode == "1_job_many_srun_tasks":
        script = engine_batch_script(
            configuration, snakefile=tmp_path / "Snakefile", run_output=tmp_path / "out", log_path=tmp_path / "log"
        )
        return shlex.split(script.splitlines()[-1])
    return cli.engine_command(configuration, run_output=tmp_path / "out")


def test_the_persisted_identities_are_read_in_one_function_and_the_mixed_set_decided_in_two_callers_of_one_derivation() -> (
    None
):
    """Property: under `workflow`, `read_identity` is called only inside `rule_identities`, and `mixed_systems` is called only by `build_provenance` and `write_end_of_run_record`.

    Class: the call graph of the workflow package's source.
    Why it is required: the shape makes `rule_identities` the one reader and `mixed_systems` the one
    derivation, so the refusal and the file's list cannot disagree; a third reader or a third deriver would
    be a second statement of the grouping that no test of either function sees.
    What kills it: re-inlining the identity loop in `end_of_run` or the collector, or grouping by system in
    the file writer instead of calling the derivation.
    Second correct implementation: `write_end_of_run_record` taking the mixed set from `build_provenance`'s
    return; the caller set would then shrink to one and this check is amended with that shape.
    """
    readers: dict[str, set[str]] = {}
    derivers: dict[str, set[str]] = {}
    for source in WORKFLOW_SOURCES:
        readers[source.name] = _calls_by_function(source, "read_identity")
        derivers[source.name] = _calls_by_function(source, "mixed_systems")

    assert {name: owners for name, owners in readers.items() if owners} == {"exercised_set.py": {"rule_identities"}}
    assert {name: owners for name, owners in derivers.items() if owners} == {
        "exercised_set.py": {"build_provenance"},
        "end_of_run.py": {"write_end_of_run_record"},
    }


@pytest.mark.parametrize("mode", ["local", "batch_job", "1_job_many_srun_tasks"])
def test_the_rerun_option_names_its_rules_and_is_followed_only_by_an_option_in_every_mode(
    mode: str, tmp_path: Path
) -> None:
    """Property: in each of the three modes the engine words carry `--forcerun` once, then exactly the configured rule names, then an option; and carry no `--forcerun` when the list is empty.

    Class: the invocation of each mode, with two rules and with none.
    Why it is required: `--forcerun` takes every following word as a rule name, so one positional word after
    the list (a target, a path) would be rebuilt as a rule and the engine would refuse or silently force the
    wrong thing; and a mode that dropped the list would rebuild nothing while the pre-run step had accepted it.
    What kills it: placing the option last, a mode that does not call `rerun_arguments`, or emitting
    `--forcerun` with an empty list (the engine would then read the next option as a missing value).
    Second correct implementation: one `--forcerun` per rule; the check is amended with that shape.
    """
    words = _engine_words(mode, RULES, tmp_path)

    assert words.count("--forcerun") == 1
    start = words.index("--forcerun")
    assert words[start + 1 : start + 1 + len(RULES)] == list(RULES)
    assert words[start + 1 + len(RULES)].startswith("--")
    assert words.index("--rerun-incomplete") < start < words.index("--report")
    assert tuple(words[start : start + 1 + len(RULES)]) == rerun_arguments(_configuration(mode, RULES))

    assert "--forcerun" not in _engine_words(mode, (), tmp_path)


def test_the_run_start_record_carries_the_flag_and_never_the_rerun_list() -> None:
    """Property: the run-start record's fields and closed key set name `allow_mixed_versions` and no rerun list.

    Class: the record type's fields and the module's key tuple.
    Why it is required: the list's one reader is the command, which reads the configuration at every
    invocation; a recorded copy would be a second statement of the fact that could disagree with the file the
    command actually read.
    What kills it: adding `rerun_rules` to the record for convenience.
    Second correct implementation: any representation of the flag as long as the list stays out.
    """
    fields = {field.name for field in dataclasses.fields(run_start_module.RunStart)}

    assert "allow_mixed_versions" in fields
    assert not any("rerun" in name for name in fields | set(run_start_module._FIELDS))
    assert set(run_start_module._FIELDS) == {"format"} | fields


def test_a_refused_mixed_run_leaves_no_provenance_file_and_the_override_writes_the_set_that_decided_it(
    tmp_path: Path,
) -> None:
    """Property: the end-of-run step refuses a mixed system before writing anything into its record directory, and under the override the file's mixed list and per-rule identities are the ones the refusal was decided from.

    Class: three rules of one system, the pre-selection rule differing from the others in implementation only,
    run through `write_end_of_run_record` without and with the flag.
    Why it is required: a file written before the refusal would describe a run the record cannot back; and a
    difference in `implementation` alone is a difference of identity, so grouping by code version would let
    this mix through unrefused.
    What kills it: writing the provenance file before building the record, comparing `code_version` only, or
    listing the mixed systems from a second grouping.
    Second correct implementation: any order of reads that refuses first; the directory stays empty.
    """
    working = tmp_path / "work"
    _synthetic_run(working, ORA_TARGETS)
    _write_identity(working, "observed_record_licence")
    pre_selection = working / "observed_record_pre_selection"
    (pre_selection / "implementation_identity.json").write_text(
        (pre_selection / "implementation_identity.json")
        .read_text(encoding="utf-8")
        .replace('"stub"', '"another-implementation"'),
        encoding="utf-8",
    )
    record_directory = tmp_path / "record"

    with pytest.raises(ExercisedSetError, match="allow_mixed_versions"):
        write_end_of_run_record(
            record_directory, side_record=SideRecord({}), working_directory=working, targets=ORA_TARGETS,
            run_start=_run_start(ORA_TARGETS),
        )  # fmt: skip

    assert not (record_directory / PROVENANCE_FILE).exists()
    assert list(record_directory.iterdir()) == []

    write_end_of_run_record(
        record_directory, side_record=SideRecord({}), working_directory=working, targets=ORA_TARGETS,
        run_start=_run_start(ORA_TARGETS, allow_mixed_versions=True),
    )  # fmt: skip
    document = read_provenance(record_directory / PROVENANCE_FILE)
    recorded = document["rule_identities"]
    assert isinstance(recorded, dict)

    assert document["mixed_systems"] == ["observed-record-assembly"]
    assert list(recorded) == ["observed_record_pre_selection", "observed_record_licence", "observed_record_assembly"]
    assert recorded["observed_record_pre_selection"]["implementation"] == "another-implementation"
    assert recorded["observed_record_licence"]["implementation"] == "stub"
    identities = document["identities"]
    assert isinstance(identities, dict)
    assert identities["observed-record-assembly"] == recorded["observed_record_assembly"]
    assert mixed_systems(
        {
            rule: ImplementationIdentity(
                system="observed-record-assembly",
                implementation=value["implementation"],
                code_version=value["code_version"],
            )
            for rule, value in recorded.items()
        }
    ) == tuple(document["mixed_systems"])  # type: ignore[arg-type]


def test_every_graph_rule_can_be_named_by_the_rerun_list_and_the_end_of_run_module_names_the_same_rows() -> None:
    """Property: every row of the rule table is a valid element of a rerun list, and `end_of_run` imports the reader and the derivation from `exercised_set` rather than defining them.

    Class: the table's rows and the end-of-run module's namespace.
    Why it is required: the pre-run step validates the list against `rule_named`; a row that configuration
    construction refused (an empty or padded name) would be a rule no user could ask to rebuild.
    What kills it: a table row whose name carries a space or is empty; a local copy of either function in
    `end_of_run`.
    Second correct implementation: an alias import; the identity comparison still holds.
    """
    for rule in GRAPH:
        configuration = _configuration("local", (rule.name,))
        assert rerun_arguments(configuration) == ("--forcerun", rule.name)
        assert rule.name == rule.name.strip() and rule.name

    assert end_of_run.rule_identities is exercised_set.rule_identities
    assert end_of_run.mixed_systems is exercised_set.mixed_systems
