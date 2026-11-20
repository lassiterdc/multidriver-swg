"""Check the exercised set: the metadata reader, the provenance record built from persisted identities, and the file.

Two kinds of check. The unit checks write a synthetic metadata directory by an implementation of the
engine's record path that lives in this file, so a reader that built the path wrongly is red here and not
only against a real engine. The engine checks run the real workflow over a committed copy of the toolkit,
because what the metadata store holds after a run, and what the final rule can read from it while it runs,
are facts about the engine that a synthetic directory states by assumption.

Every check names the wrong implementation it is red under, because a check that cannot fail for the reason
it claims constrains nothing.
"""

from __future__ import annotations

import json
import os
import time
from base64 import urlsafe_b64encode
from pathlib import Path

import pytest

from multidriver_swg.run_configuration.provenance import ImplementationIdentity, persist_identity
from multidriver_swg.side_record import SideRecord
from multidriver_swg.workflow.end_of_run import END_OF_RUN_RECORD, write_end_of_run_record
from multidriver_swg.workflow.exercised_set import (
    METADATA_FORMAT_VERSION,
    PROVENANCE_FILE,
    PROVENANCE_RULE,
    ExercisedSetError,
    build_provenance,
    exercised_systems,
    invocation_rules,
    producing_rules,
    read_provenance,
    write_provenance,
)
from multidriver_swg.workflow.graph import GRAPH, IDENTITY_FILE, downstream_of
from multidriver_swg.workflow.run_start import RUN_START_FILE, RunStart
from tests.workflow.conftest import Estate, ToolkitCopy, write_record

ALL_TARGETS = tuple(f"{rule.name}/{output}" for rule in GRAPH for output in rule.outputs)

# The rules a run with no selection builds: the six that neither read the selection nor sit below a
# reader, and the attribution row. Written out by hand from the table's `reads_selection` fields.
KEPT_RULES = (
    "observed_record_assembly",
    "combined_event_identification",
    "marginal_distribution_fitting",
    "event_frequency_assessment",
    "event_occurrence_modelling",
    "interactive_decision_support",
    "method_attribution",
)

KEPT_TARGETS = tuple(f"{rule.name}/{output}" for rule in GRAPH if rule.name in KEPT_RULES for output in rule.outputs)

SYSTEM_OF = {rule.name: rule.system for rule in GRAPH}

FIRST_TARGET = f"{GRAPH[0].name}/{GRAPH[0].outputs[0]}"


def _metadata_directory(working_directory: Path) -> Path:
    directory = working_directory / ".snakemake" / "metadata"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _record_file(working_directory: Path, target: str) -> Path:
    """Return the engine's record file for `target`, split into `@`-prefixed directories by this file's own rule."""
    directory = _metadata_directory(working_directory)
    limit = os.pathconf(directory, "PC_NAME_MAX") - 1
    identifier = urlsafe_b64encode(target.encode()).decode()
    parts = [identifier[index : index + limit] for index in range(0, len(identifier), limit)]
    return directory.joinpath(*["@" + part for part in parts[:-1]], parts[-1])


def _write_record(working_directory: Path, target: str, **fields: object) -> Path:
    """Write a metadata record of the engine's current form for `target`, with `fields` replacing defaults."""
    document: dict[str, object] = {
        "record_format_version": METADATA_FORMAT_VERSION,
        "rule": GRAPH[0].name,
        "incomplete": False,
        "starttime": None,
        "endtime": time.time() + 1000,
    }
    document.update(fields)
    path = _record_file(working_directory, target)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def _write_identity(working_directory: Path, rule_name: str, *, system: str | None = None) -> None:
    directory = working_directory / rule_name
    directory.mkdir(parents=True, exist_ok=True)
    persist_identity(
        directory / IDENTITY_FILE,
        ImplementationIdentity(system=system or SYSTEM_OF[rule_name], implementation="stub", code_version="abc123"),
    )


def _run_start(targets: tuple[str, ...] = (FIRST_TARGET,)) -> RunStart:
    return RunStart(
        main_configuration="/estate/run.toml",
        targets=targets,
        selection=None,
        judgement="kept-absent",
    )


def _synthetic_run(working_directory: Path, targets: tuple[str, ...] = ALL_TARGETS) -> None:
    """Write one record per target and one identity per rule, as a run of the graph leaves them."""
    for target in targets:
        _write_record(working_directory, target, rule=target.split("/", 1)[0])
        _write_identity(working_directory, target.split("/", 1)[0])
    (working_directory / RUN_START_FILE).write_text("{}", encoding="utf-8")


def test_each_target_is_mapped_to_the_rule_its_record_names(tmp_path: Path) -> None:
    """Property: `producing_rules` returns each target's own record's rule, so two targets with two rules map to two rules.

    Class: two records naming different rules.
    Why it is required: the whole set is read from this mapping; a reader returning one rule for every
    target, or the rule of the target's directory, would name producers the engine never recorded.
    What kills it: deriving the rule from the target's first path component, or reading one record only.
    Second correct implementation: any reader of the same files; the mapping is the check.
    """
    _write_record(tmp_path, "a/one.json", rule="event_frequency_assessment")
    _write_record(tmp_path, "b/two.json", rule="dependence_modelling")

    assert producing_rules(tmp_path, ["a/one.json", "b/two.json"]) == {
        "a/one.json": "event_frequency_assessment",
        "b/two.json": "dependence_modelling",
    }


def test_a_target_long_enough_to_split_is_found_by_the_engines_segment_rule(tmp_path: Path) -> None:
    """Property: a target whose identifier needs two segments is read from the `@`-prefixed directory and the remainder file.

    Class: a target of about 300 characters, whose URL-safe base64 is longer than one file name allows.
    Why it is required: a reader that did not split would look for one over-long file name and report a
    missing record for an output the engine did record.
    What kills it: omitting the split, omitting the `@` prefix, or splitting at the full name limit rather
    than one short of it.
    Second correct implementation: any reader that applies the engine's rule; the synthetic writer here
    applies it independently.
    """
    target = "rule/" + "x" * 300 + ".json"
    path = _write_record(tmp_path, target, rule="dependence_modelling")
    assert path.parent.name.startswith("@")

    assert producing_rules(tmp_path, [target]) == {target: "dependence_modelling"}


@pytest.mark.parametrize(
    "mutation, fragment",
    [
        ("missing", "wrote no metadata record"),
        ("garbled", "does not parse"),
        ("not_an_object", "not one JSON object"),
        ("version", "record_format_version"),
        ("incomplete", "incomplete"),
        ("unknown_rule", "not a row of the graph"),
    ],
)
def test_every_record_the_engine_did_not_complete_is_refused_naming_the_target_and_file(
    tmp_path: Path, mutation: str, fragment: str
) -> None:
    """Property: a missing, unparsable, non-object, wrong-version, incomplete or unknown-rule record raises `ExercisedSetError` naming the target.

    Class: one record broken in exactly one way each.
    Why it is required: a gap admitted silently is a producer left out of the set, and a record with a
    silent gap passes for a complete one.
    What kills it: returning an empty mapping for a missing record, or ignoring `incomplete`.
    Second correct implementation: raising at the first bad target or collecting all; the refusal is the check.
    """
    target = "rule/out.json"
    _metadata_directory(tmp_path)
    if mutation == "garbled":
        path = _write_record(tmp_path, target)
        path.write_text("{not json", encoding="utf-8")
    elif mutation == "not_an_object":
        path = _write_record(tmp_path, target)
        path.write_text("[1, 2]", encoding="utf-8")
    elif mutation == "version":
        _write_record(tmp_path, target, record_format_version=METADATA_FORMAT_VERSION + 1)
    elif mutation == "incomplete":
        _write_record(tmp_path, target, incomplete=True)
    elif mutation == "unknown_rule":
        _write_record(tmp_path, target, rule="not_in_the_table")

    with pytest.raises(ExercisedSetError, match=fragment) as caught:
        producing_rules(tmp_path, [target])
    assert target in str(caught.value)


def test_a_missing_metadata_directory_is_refused(tmp_path: Path) -> None:
    """Property: a working directory with no metadata directory refuses, and names the directory.

    Class: an engine that never ran in this directory.
    Why it is required: a probe of the filesystem layout on an absent directory otherwise raises an
    operating-system error that names no target.
    What kills it: letting `os.pathconf` raise on the absent directory.
    Second correct implementation: checking the directory first, or the file first; the refusal is the check.
    """
    with pytest.raises(ExercisedSetError, match="metadata directory"):
        producing_rules(tmp_path, [FIRST_TARGET])


def test_exercised_systems_is_the_owning_system_of_each_producing_rule() -> None:
    """Property: the set holds each producing rule's system once, by the row's field rather than the rule's name.

    Class: two targets produced by one rule, and one by another.
    Why it is required: the set is what completeness is checked against; a duplicate or a name taken from
    the rule would make the check compare the wrong strings.
    What kills it: returning rule names, or a list that repeats a system.
    Second correct implementation: building from a dict of rule to system; the set is the check.
    """
    producers = {"a": "dependence_modelling", "b": "dependence_modelling", "c": "event_frequency_assessment"}

    assert exercised_systems(producers) == frozenset({"dependence-modelling", "event-frequency-assessment"})
    with pytest.raises(ExercisedSetError, match="not a row of the graph"):
        exercised_systems({"a": "nope"})


def test_invocation_rules_names_only_rules_started_at_or_after_the_run_start_record(tmp_path: Path) -> None:
    """Property: a record that ended before the run-start record's modification is left out, one that ended at or after it is in, and the answer is in table order.

    Class: three records, one before and two after the boundary, written in the opposite of table order.
    Why it is required: the field tells a reader what this invocation built from what it found; including
    an earlier rule claims work the invocation did not do.
    What kills it: comparing against the current time, ignoring the boundary, or returning the order of
    the mapping.
    Second correct implementation: filtering the table by a set of started rules; the tuple is the check.
    """
    boundary_file = tmp_path / RUN_START_FILE
    boundary_file.write_text("{}", encoding="utf-8")
    boundary = boundary_file.stat().st_mtime
    _write_record(tmp_path, "late/b.json", rule="dependence_modelling", endtime=boundary + 5)
    _write_record(tmp_path, "early/a.json", rule="event_frequency_assessment", endtime=boundary - 5)
    _write_record(tmp_path, "exact/c.json", rule="event_occurrence_modelling", endtime=boundary)
    producers = {
        "late/b.json": "dependence_modelling",
        "early/a.json": "event_frequency_assessment",
        "exact/c.json": "event_occurrence_modelling",
    }

    assert invocation_rules(tmp_path, producers) == ("event_occurrence_modelling", "dependence_modelling")


def test_a_record_with_no_numeric_start_time_is_refused_by_invocation_rules(tmp_path: Path) -> None:
    """Property: a record whose `endtime` is null refuses rather than counting the rule in or out.

    Class: a complete record with no start time.
    Why it is required: whether the rule ran in this invocation cannot be read, and a guess is a claim.
    What kills it: treating a missing start as zero.
    Second correct implementation: treating it as the boundary; that is a guess too, so only the refusal passes.
    """
    (tmp_path / RUN_START_FILE).write_text("{}", encoding="utf-8")
    _write_record(tmp_path, "a/one.json", rule="dependence_modelling", endtime=None)

    with pytest.raises(ExercisedSetError, match="endtime"):
        invocation_rules(tmp_path, {"a/one.json": "dependence_modelling"})


def test_the_provenance_names_every_producer_from_its_persisted_identity(tmp_path: Path) -> None:
    """Property: over a synthetic run of all twelve rules, the record's identities are exactly the twelve systems' persisted ones and its seeds are empty.

    Class: twelve records and twelve identity files.
    Why it is required: the record must be built from what the producers persisted and not from the table.
    What kills it: filling identities from the table, or capturing seeds that nothing persisted.
    Second correct implementation: reading identities in table order or target order; the mapping is the check.
    """
    _synthetic_run(tmp_path)

    record = build_provenance(tmp_path, ALL_TARGETS, _run_start(ALL_TARGETS), SideRecord({}))

    assert set(record.identities) == set(SYSTEM_OF.values())
    assert all(identity.implementation == "stub" for identity in record.identities.values())
    assert record.seeds == {}
    assert record.inputs["targets"] == list(ALL_TARGETS)
    assert record.inputs["judgement"] == "kept-absent"


def test_an_identity_naming_another_system_is_refused(tmp_path: Path) -> None:
    """Property: an identity file whose `system` differs from the rule's row refuses and names the file.

    Class: one rule whose persisted identity claims a neighbouring system.
    Why it is required: the row and the file would otherwise describe two different producers.
    What kills it: keying the identity by the file's system without comparing it with the row's.
    Second correct implementation: comparing before or after capture; the refusal is the check.
    """
    _synthetic_run(tmp_path, KEPT_TARGETS)
    _write_identity(tmp_path, "dependence_modelling", system="timeseries-synthesis")
    _write_record(tmp_path, "dependence_modelling/joint_model.json", rule="dependence_modelling")

    with pytest.raises(ExercisedSetError, match="timeseries-synthesis"):
        build_provenance(
            tmp_path, (*KEPT_TARGETS, "dependence_modelling/joint_model.json"), _run_start(), SideRecord({})
        )


def test_a_missing_identity_file_is_refused_naming_the_file(tmp_path: Path) -> None:
    """Property: a producer whose identity file is absent refuses with `ExercisedSetError` naming the file.

    Class: a record naming a rule whose directory holds no identity.
    Why it is required: a producer that persisted nothing is the gap the completeness check exists for.
    What kills it: skipping a rule without an identity, or leaking the operating system's error.
    Second correct implementation: any refusal that names the path; the match is the check.
    """
    _write_record(tmp_path, "dependence_modelling/joint_model.json", rule="dependence_modelling")

    with pytest.raises(ExercisedSetError, match="implementation_identity.json"):
        build_provenance(tmp_path, ["dependence_modelling/joint_model.json"], _run_start(), SideRecord({}))


def test_a_side_record_of_the_wrong_kind_is_refused_before_any_read(tmp_path: Path) -> None:
    """Property: `build_provenance` refuses a side record that is not a `SideRecord` with `TypeError`.

    Class: a plain mapping passed where the record belongs.
    Why it is required: the file carries the record's own mapping, and a mapping with no `as_mapping` would
    fail late and obscurely.
    What kills it: accepting any object.
    Second correct implementation: checking at write instead; then the build would return and this check is red.
    """
    with pytest.raises(TypeError, match="SideRecord"):
        build_provenance(tmp_path, [FIRST_TARGET], _run_start(), {"a": "current"})  # type: ignore[arg-type]


def test_the_provenance_file_round_trips_and_carries_the_side_record_unaltered(tmp_path: Path) -> None:
    """Property: the file holds the seven keys, sorted exercised systems, and exactly `side_record.as_mapping()`.

    Class: a synthetic run written through `write_provenance` and read through `read_provenance`.
    Why it is required: the side-record contract says a carrying record holds the mapping unaltered.
    What kills it: re-keying the mapping, or writing the exercised systems unsorted.
    Second correct implementation: another JSON layout with the same keys; the document is the check.
    """
    _synthetic_run(tmp_path, KEPT_TARGETS)
    side = SideRecord({"z": "pre_refactor", "a": "current"})
    record = build_provenance(tmp_path, KEPT_TARGETS, _run_start(KEPT_TARGETS), side)
    path = tmp_path / PROVENANCE_FILE

    write_provenance(
        path,
        record,
        exercised=exercised_systems(producing_rules(tmp_path, KEPT_TARGETS)),
        invocations=("observed_record_assembly",),
        side_record=side,
    )
    document = read_provenance(path)

    assert document["format"] == PROVENANCE_RULE
    assert document["side_record"] == side.as_mapping()
    assert document["exercised_systems"] == sorted(SYSTEM_OF[name] for name in KEPT_RULES)
    assert document["invocation_rules"] == ["observed_record_assembly"]
    assert document["seeds"] == {}
    assert set(document["identities"]) == set(document["exercised_systems"])


@pytest.mark.parametrize(
    "breakage", ["extra_key", "missing_key", "format", "not_object", "identity_shape", "list_kind"]
)
def test_the_reader_refuses_a_document_outside_the_form(tmp_path: Path, breakage: str) -> None:
    """Property: an extra key, a missing key, another format, a non-object, a malformed identity or a wrong-kind list each refuse naming the file.

    Class: a valid document broken one way each.
    Why it is required: the reader is the form's one enforcement; a consumer reading past a break would
    trust a record the form does not describe.
    What kills it: reading the keys it wants and ignoring the rest, or not checking the identity's keys.
    Second correct implementation: a schema validator; the refusal is the check.
    """
    path = tmp_path / PROVENANCE_FILE
    document: object = {
        "format": PROVENANCE_RULE,
        "inputs": {},
        "seeds": {},
        "identities": {"s": {"implementation": "stub", "code_version": "c"}},
        "exercised_systems": ["s"],
        "invocation_rules": [],
        "side_record": {},
    }
    assert isinstance(document, dict)
    if breakage == "extra_key":
        document["extra"] = 1
    elif breakage == "missing_key":
        del document["seeds"]
    elif breakage == "format":
        document["format"] = "end-of-run-provenance:2"
    elif breakage == "not_object":
        document = [1]
    elif breakage == "identity_shape":
        document["identities"] = {"s": {"implementation": "stub"}}
    elif breakage == "list_kind":
        document["exercised_systems"] = "s"
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ExercisedSetError) as caught:
        read_provenance(path)
    assert str(path) in str(caught.value)


def test_a_partial_set_of_provenance_arguments_is_refused(tmp_path: Path) -> None:
    """Property: `write_end_of_run_record` refuses one or two of the three provenance arguments and writes no directory.

    Class: working directory given, targets and run start omitted.
    Why it is required: a file read from some of the three would name producers nobody meant.
    What kills it: defaulting each independently.
    Second correct implementation: a single argument holding all three; this check would then change its call.
    """
    destination = tmp_path / "record"

    with pytest.raises(TypeError, match="together"):
        write_end_of_run_record(destination, side_record=SideRecord({}), working_directory=tmp_path)
    assert not destination.exists()


def test_a_full_run_names_the_twelve_systems_with_the_copys_commit(
    toolkit_copy: ToolkitCopy, estate: Estate, engine
) -> None:
    """Property: an engine run over the full target list writes a provenance file naming twelve systems, each implementation `stub` at the copy's commit.

    Class: the full list, ending with the end-of-run record, over a clean committed copy.
    Why it is required: the final rule reads the store while the engine is still running; this shows the
    records of its inputs exist at that moment and the identities beside them are the stubs'.
    What kills it: reading the store before the inputs' records are written, a table-derived set, or a
    code version that is not the running checkout's.
    Second correct implementation: any reader of the same records; the file is the check.
    """
    write_record(estate, (*ALL_TARGETS, END_OF_RUN_RECORD), judgement="present")

    completed = engine(toolkit_copy, estate)

    assert completed.returncode == 0, completed.stderr
    document = read_provenance(estate.run_output / END_OF_RUN_RECORD / PROVENANCE_FILE)
    assert document["exercised_systems"] == sorted(set(SYSTEM_OF.values()))
    identities = document["identities"]
    assert isinstance(identities, dict)
    assert {entry["implementation"] for entry in identities.values()} == {"stub"}
    assert {entry["code_version"] for entry in identities.values()} == {toolkit_copy.commit}
    assert set(document["side_record"].values()) <= {"current"}  # type: ignore[attr-defined]
    assert document["inputs"]["targets"] == list(ALL_TARGETS)  # type: ignore[index]


def test_a_kept_absent_run_names_exactly_the_seven_systems_of_its_list(
    toolkit_copy: ToolkitCopy, estate: Estate, engine
) -> None:
    """Property: an engine run over the kept-absent list writes a provenance file naming exactly the seven systems of that list.

    Class: the shortened list, the judgement stage kept.
    Why it is required: a run that stops early must claim only what it built; a twelve-system claim would
    name producers whose outputs do not exist.
    What kills it: deriving the set from the table, or from a list kept in the module.
    Second correct implementation: reading the same records in any order; the sorted list is the check.
    """
    write_record(estate, (*KEPT_TARGETS, END_OF_RUN_RECORD), judgement="kept-absent")

    completed = engine(toolkit_copy, estate)

    assert completed.returncode == 0, completed.stderr
    document = read_provenance(estate.run_output / END_OF_RUN_RECORD / PROVENANCE_FILE)
    assert document["exercised_systems"] == sorted(SYSTEM_OF[name] for name in KEPT_RULES)
    assert len(document["exercised_systems"]) == 7  # type: ignore[arg-type]
    assert not (estate.run_output / "dependence_modelling").exists()


def test_omitting_one_output_drops_its_producer_unless_another_target_shares_the_system(
    toolkit_copy: ToolkitCopy, estate: Estate, engine
) -> None:
    """Property: a call whose targets omit one leaf output leaves that producer's system out of the record, and omitting nothing keeps it.

    Class: the same finished run read over the full list and over the list minus one output.
    Why it is required: the set is read over the rule's own input list, so the list is what decides the set;
    a reader that ignored its argument would name every producer on disk.
    What kills it: scanning the whole metadata store instead of the given targets.
    Second correct implementation: reading the given targets' records one at a time; the sets are the check.
    """
    write_record(estate, (*ALL_TARGETS, END_OF_RUN_RECORD), judgement="present")
    completed = engine(toolkit_copy, estate)
    assert completed.returncode == 0, completed.stderr
    leaf = "monte_carlo_design_storms/design_storm_realizations.json"
    run_start = _run_start(ALL_TARGETS)

    full = build_provenance(estate.run_output, ALL_TARGETS, run_start, SideRecord({}))
    without = build_provenance(
        estate.run_output, [target for target in ALL_TARGETS if target != leaf], run_start, SideRecord({})
    )

    assert "monte-carlo-design-storms" in full.identities
    assert "monte-carlo-design-storms" not in without.identities
    assert set(without.identities) == set(full.identities) - {"monte-carlo-design-storms"}


def test_a_second_command_style_run_lists_only_rebuilt_rules_while_every_producer_stays_exercised(
    toolkit_copy: ToolkitCopy, estate: Estate, engine
) -> None:
    """Property: after a second run-start record and one deleted output, `invocation_rules` names the rebuilt rule and its descendants only, and `exercised_systems` still names all twelve.

    Class: a finished run, then the command's second invocation: a fresh run-start record and one output removed.
    Why it is required: the store is cumulative, so an output kept from the earlier invocation counts its
    producer; the invocation field is the smaller fact and must not be confused with it.
    What kills it: filtering the exercised set by start time, or counting every rule as started.
    Second correct implementation: reading the engine's log for the rules that ran; the two fields are the check.
    """
    targets = (*ALL_TARGETS, END_OF_RUN_RECORD)
    write_record(estate, targets, judgement="present")
    first = engine(toolkit_copy, estate)
    assert first.returncode == 0, first.stderr
    first_document = read_provenance(estate.run_output / END_OF_RUN_RECORD / PROVENANCE_FILE)
    assert first_document["invocation_rules"] == [rule.name for rule in GRAPH]

    time.sleep(1.1)
    write_record(estate, targets, judgement="present")
    (estate.run_output / "marginal_distribution_fitting" / "candidate_marginals.json").unlink()
    second = engine(toolkit_copy, estate)

    assert second.returncode == 0, second.stderr
    document = read_provenance(estate.run_output / END_OF_RUN_RECORD / PROVENANCE_FILE)
    rebuilt = {"marginal_distribution_fitting", "method_attribution"}
    rebuilt |= {rule.name for rule in downstream_of("marginal_distribution_fitting")}
    assert document["invocation_rules"] == [rule.name for rule in GRAPH if rule.name in rebuilt]
    assert "observed_record_assembly" not in document["invocation_rules"]  # type: ignore[operator]
    assert document["exercised_systems"] == sorted(set(SYSTEM_OF.values()))
