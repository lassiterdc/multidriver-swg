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
import subprocess
import time
from base64 import urlsafe_b64encode
from pathlib import Path

import pytest

import multidriver_swg.workflow.pre_run as pre_run
from multidriver_swg.run_configuration.main_configuration import MainConfiguration
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
    mixed_systems,
    producing_rules,
    read_provenance,
    rule_identities,
    write_provenance,
)
from multidriver_swg.workflow.graph import GRAPH, IDENTITY_FILE, downstream_of
from multidriver_swg.workflow.modes import rerun_arguments
from multidriver_swg.workflow.pre_run import prepare_run
from multidriver_swg.workflow.run_start import RUN_START_FILE, RunStart, read_run_start
from tests.workflow.conftest import (
    Estate,
    ToolkitCopy,
    advance_commit,
    bind_three_keys,
    run_engine,
    write_record,
    write_version_stacking,
)

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


def _write_identity(
    working_directory: Path, rule_name: str, *, system: str | None = None, code_version: str = "abc123"
) -> None:
    directory = working_directory / rule_name
    directory.mkdir(parents=True, exist_ok=True)
    persist_identity(
        directory / IDENTITY_FILE,
        ImplementationIdentity(system=system or SYSTEM_OF[rule_name], implementation="stub", code_version=code_version),
    )


def _run_start(targets: tuple[str, ...] = (FIRST_TARGET,), *, allow_mixed_versions: bool = False) -> RunStart:
    return RunStart(
        main_configuration="/estate/run.toml",
        targets=targets,
        selection=None,
        judgement="kept-absent",
        allow_mixed_versions=allow_mixed_versions,
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
    """Property: the file holds the nine keys, sorted exercised systems, exactly `side_record.as_mapping()`, and a null rule-identity record when the caller passed none.

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
    assert document["rule_identities"] is None
    assert document["mixed_systems"] == []


def test_a_provenance_file_written_with_the_two_keywords_carries_every_rule_identity_and_the_mixed_systems(
    tmp_path: Path,
) -> None:
    """Property: `write_provenance` given `rule_identities` and `mixed_systems` writes each rule's implementation and code version keyed by rule, and the mixed systems sorted; the reader returns both.

    Class: a synthetic run whose pre-selection row carries another code version, written with the override.
    Why it is required: the record's `identities` hold one entry per system, so the file's own
    `rule_identities` is the only place the rule-level identities survive; a writer that dropped the
    keywords would leave the override recording nothing.
    What kills it: ignoring the keywords, writing the mixed systems in the order given, or keying the
    mapping by system.
    Second correct implementation: another JSON layout with the same keys; the document is the check.
    """
    targets = (
        "observed_record_pre_selection/pre_selection.json",
        "observed_record_licence/licence.json",
        "observed_record_assembly/observed_record.json",
    )
    _synthetic_run(tmp_path, targets)
    _write_identity(tmp_path, "observed_record_pre_selection", code_version="older")
    side = SideRecord({})
    record = build_provenance(tmp_path, targets, _run_start(targets, allow_mixed_versions=True), side)
    producers = producing_rules(tmp_path, targets)
    persisted = rule_identities(tmp_path, producers)
    path = tmp_path / PROVENANCE_FILE

    write_provenance(
        path,
        record,
        exercised=exercised_systems(producers),
        invocations=(),
        side_record=side,
        rule_identities=persisted,
        mixed_systems=("z-system", *mixed_systems(persisted)),
    )
    document = read_provenance(path)

    assert document["rule_identities"] == {
        "observed_record_pre_selection": {"implementation": "stub", "code_version": "older"},
        "observed_record_licence": {"implementation": "stub", "code_version": "abc123"},
        "observed_record_assembly": {"implementation": "stub", "code_version": "abc123"},
    }
    assert document["mixed_systems"] == ["observed-record-assembly", "z-system"]


ORA_TARGETS = (
    "observed_record_pre_selection/pre_selection.json",
    "observed_record_licence/licence.json",
    "observed_record_assembly/observed_record.json",
)


def test_two_rules_of_one_system_with_equal_identities_pass_with_one_identity_and_no_mixed_system(
    tmp_path: Path,
) -> None:
    """Property: three rules of `observed-record-assembly` persisting equal identities give one identity for the system, an empty mixed set and the rules' identities keyed by rule in table order.

    Class: three records and three identity files of one system, all at one code version.
    Why it is required: an equal pair is the ordinary case of a system owning several rules, so the
    refusal must fire on a difference and never on the count of rules.
    What kills it: refusing a system because it has several rules, or keeping the identities keyed by system
    so the count of rules is invisible.
    Second correct implementation: grouping by system before or after reading; the same outputs result.
    """
    _synthetic_run(tmp_path, ORA_TARGETS)
    producers = producing_rules(tmp_path, ORA_TARGETS)

    persisted = rule_identities(tmp_path, producers)
    record = build_provenance(tmp_path, ORA_TARGETS, _run_start(ORA_TARGETS), SideRecord({}))

    assert list(persisted) == ["observed_record_pre_selection", "observed_record_licence", "observed_record_assembly"]
    assert mixed_systems(persisted) == ()
    assert set(record.identities) == {"observed-record-assembly"}
    assert record.identities["observed-record-assembly"].code_version == "abc123"


def test_unequal_identities_of_one_system_are_refused_naming_both_rules_both_files_and_the_override(
    tmp_path: Path,
) -> None:
    """Property: a system whose rules persisted unequal code versions is refused, the message naming two of its rules, both identity files and `allow_mixed_versions`; the same files are admitted under the override.

    Class: the pre-selection row at an older code version and the licence and merge rows at a newer one.
    Why it is required: before this refusal the last rule read silently replaced the others, so a record
    named one version for outputs two builds produced; the message is the operator's only route to the
    override, so each fact it must carry is asserted.
    What kills it: keying identities by system without comparing, a message naming one rule or no file, or a
    refusal that ignores the run-start record's flag.
    Second correct implementation: comparing identities pairwise or by set size; the same refusal arises.
    """
    _synthetic_run(tmp_path, ORA_TARGETS)
    _write_identity(tmp_path, "observed_record_pre_selection", code_version="older")

    with pytest.raises(ExercisedSetError) as caught:
        build_provenance(tmp_path, ORA_TARGETS, _run_start(ORA_TARGETS), SideRecord({}))

    message = str(caught.value)
    assert "observed-record-assembly" in message
    assert "observed_record_pre_selection" in message
    assert "observed_record_licence" in message
    assert str(tmp_path / "observed_record_pre_selection" / IDENTITY_FILE) in message
    assert str(tmp_path / "observed_record_licence" / IDENTITY_FILE) in message
    assert "'older'" in message
    assert "allow_mixed_versions" in message

    record = build_provenance(tmp_path, ORA_TARGETS, _run_start(ORA_TARGETS, allow_mixed_versions=True), SideRecord({}))

    assert record.identities["observed-record-assembly"].code_version == "abc123"


def test_the_system_entry_is_the_identity_of_the_rule_latest_in_table_order_whichever_target_comes_first(
    tmp_path: Path,
) -> None:
    """Property: under the override the record's entry for a mixed system is the identity of its rule latest in the table, not the one whose target is listed last.

    Class: the merge row at a distinct version and the pre-selection row at the common one, targets listed in
    reverse table order.
    Why it is required: the entry used to be whichever rule was read last, and the read order followed the
    target list; the table order is the stated rule and must not depend on how a caller lists targets.
    What kills it: letting the target order decide, or choosing the earliest rule.
    Second correct implementation: sorting the rules by table position before assigning; the same entry.
    """
    _synthetic_run(tmp_path, ORA_TARGETS)
    _write_identity(tmp_path, "observed_record_assembly", code_version="merge-version")

    record = build_provenance(
        tmp_path,
        tuple(reversed(ORA_TARGETS)),
        _run_start(ORA_TARGETS, allow_mixed_versions=True),
        SideRecord({}),
    )

    assert record.identities["observed-record-assembly"].code_version == "merge-version"


def test_mixed_systems_groups_by_the_rows_system_and_lists_the_unequal_ones_sorted() -> None:
    """Property: `mixed_systems` lists, sorted, the systems with two unequal rule identities and no other, and refuses a rule the table does not hold.

    Class: identities of two systems, one with an unequal pair and one with an equal pair, plus an unknown rule.
    Why it is required: this is the one derivation of the mixed set, read by the refusal and by the file, so
    the two cannot disagree only if it groups by the row's system and compares identities.
    What kills it: grouping by implementation, comparing code versions across systems, or answering for an
    unknown rule name.
    Second correct implementation: counting distinct identities per system; the same tuple.
    """

    def identity(system: str, version: str) -> ImplementationIdentity:
        return ImplementationIdentity(system=system, implementation="stub", code_version=version)

    persisted = {
        "per_source_event_selection": identity("combined-event-identification", "a"),
        "combined_event_identification": identity("combined-event-identification", "b"),
        "observed_record_pre_selection": identity("observed-record-assembly", "a"),
        "observed_record_licence": identity("observed-record-assembly", "a"),
        "marginal_distribution_fitting": identity("marginal-distribution-fitting", "c"),
    }

    assert mixed_systems(persisted) == ("combined-event-identification",)
    assert mixed_systems({}) == ()
    with pytest.raises(ExercisedSetError, match="no_such_rule"):
        mixed_systems({"no_such_rule": identity("x", "a")})


def test_rule_identities_refuses_an_unreadable_file_and_a_file_naming_another_system(tmp_path: Path) -> None:
    """Property: the one reader of persisted identities refuses a missing identity file and a file naming another system, each naming the rule and the file.

    Class: one rule with no identity file, and one whose file claims a neighbouring system.
    Why it is required: the reader moved out of `build_provenance`, and the loop's two refusals are what the
    record's honesty rests on, so they are asserted at the reader itself.
    What kills it: skipping a rule whose file is absent, or comparing the file's system with nothing.
    Second correct implementation: reading in any order; the two refusals are the check.
    """
    producers = {"a/one.json": "dependence_modelling"}

    with pytest.raises(ExercisedSetError, match=r"dependence_modelling.*implementation_identity\.json"):
        rule_identities(tmp_path, producers)

    _write_identity(tmp_path, "dependence_modelling", system="timeseries-synthesis")
    with pytest.raises(ExercisedSetError, match="timeseries-synthesis"):
        rule_identities(tmp_path, producers)


@pytest.mark.parametrize(
    "breakage",
    [
        "extra_key",
        "missing_key",
        "format",
        "not_object",
        "identity_shape",
        "list_kind",
        "rule_identity_shape",
        "rule_identities_kind",
        "mixed_systems_kind",
    ],
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
        "rule_identities": None,
        "mixed_systems": [],
    }
    assert isinstance(document, dict)
    if breakage == "extra_key":
        document["extra"] = 1
    elif breakage == "missing_key":
        del document["seeds"]
    elif breakage == "format":
        document["format"] = "end-of-run-provenance:1"
    elif breakage == "not_object":
        document = [1]
    elif breakage == "identity_shape":
        document["identities"] = {"s": {"implementation": "stub"}}
    elif breakage == "list_kind":
        document["exercised_systems"] = "s"
    elif breakage == "rule_identity_shape":
        document["rule_identities"] = {"observed_record_assembly": {"implementation": "stub"}}
    elif breakage == "rule_identities_kind":
        document["rule_identities"] = ["observed_record_assembly"]
    elif breakage == "mixed_systems_kind":
        document["mixed_systems"] = "s"
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


def _prepare(estate: Estate, toolkit: ToolkitCopy, monkeypatch: pytest.MonkeyPatch) -> None:
    """Run the pre-run step against `toolkit` as the command would, writing the run-start record."""
    monkeypatch.setattr(pre_run, "running_package_directory", lambda: toolkit.package)
    prepare_run(MainConfiguration.load(estate.configuration), estate.configuration)


def _command_run(toolkit: ToolkitCopy, estate: Estate, cache_root: Path) -> subprocess.CompletedProcess[str]:
    """Run the engine over `toolkit` with the options the command adds after `--rerun-incomplete`: the rerun list.

    The command's own argument list names the checkout's `Snakefile`, so the copy's is substituted here;
    the rerun list itself is the command's one function, `modes.rerun_arguments`, read from the estate's
    configuration as the command reads it.
    """
    configuration = MainConfiguration.load(estate.configuration)
    return run_engine(
        toolkit,
        estate,
        cache_root=cache_root,
        extra_arguments=("--rerun-incomplete", *rerun_arguments(configuration)),
    )


def _stacking_estate(estate: Estate) -> None:
    """Bind the three keys with the selection file absent, so the run builds the kept-absent list."""
    bind_three_keys(estate, estate.project_directory / "selection.toml")


def _versions_by_system(document: dict[str, object]) -> dict[str, str]:
    """Return each system's recorded code version from a provenance document."""
    identities = document["identities"]
    assert isinstance(identities, dict)
    return {system: entry["code_version"] for system, entry in identities.items()}


def test_a_misspelt_rerun_rule_is_refused_by_the_pre_run_step_and_the_override_is_recorded(
    estate: Estate, monkeypatch: pytest.MonkeyPatch, toolkit_copy: ToolkitCopy
) -> None:
    """Property: a `rerun_rules` name the table does not hold refuses the pre-run step naming the table's rules, and `allow_mixed_versions` reaches the run-start record.

    Class: one configuration naming a misspelt rule, one naming a real rule with the override on.
    Why it is required: a name passed to `--forcerun` that is no rule is an engine error after the command
    has written its record, or worse a silent no-rebuild; the pre-run step is where the table is known. The
    record is the only channel by which the engine's final rule learns the override.
    What kills it: skipping the check, checking against a list kept elsewhere, or dropping the flag when
    the record is built.
    Second correct implementation: a check inside the configuration module; it does not import the table, so
    the refusal would then arrive as another class and this check would change.
    """
    _stacking_estate(estate)
    write_version_stacking(estate, rerun_rules=("observed_record_assemblyy",))

    with pytest.raises(pre_run.PreRunError) as caught:
        _prepare(estate, toolkit_copy, monkeypatch)
    assert "observed_record_assemblyy" in str(caught.value)
    assert "observed_record_assembly'" in str(caught.value)
    assert not (estate.run_output / RUN_START_FILE).exists()

    write_version_stacking(estate, rerun_rules=("observed_record_assembly",), allow_mixed_versions=True)
    _prepare(estate, toolkit_copy, monkeypatch)

    assert read_run_start(estate.run_output / RUN_START_FILE).allow_mixed_versions is True


def test_rerunning_one_rule_stacks_two_commits_across_systems_and_needs_no_override(
    tmp_path: Path, toolkit_copy: ToolkitCopy, estate: Estate, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Property: a second commit with `rerun_rules = ["marginal_distribution_fitting"]` rebuilds that rule and its dependants at the new commit, keeps the rest at the first, exits 0, and lists no mixed system.

    Class: a full kept-absent run at the first commit, then a second commit and a run whose rerun list names one rule.
    Why it is required: stacking across systems must stay honest and unrefused, because each system's
    identity comes from its own rule; the refusal is for one system with two versions only.
    What kills it: refusing any two code versions in one record, or a rerun list that rebuilds nothing.
    Second correct implementation: any engine option that forces the same rules; the recorded versions decide.
    """
    _stacking_estate(estate)
    first_commit = toolkit_copy.commit
    _prepare(estate, toolkit_copy, monkeypatch)
    first = _command_run(toolkit_copy, estate, tmp_path / "cache")
    assert first.returncode == 0, first.stderr

    toolkit = advance_commit(toolkit_copy, estate)
    write_version_stacking(estate, rerun_rules=("marginal_distribution_fitting",))
    _prepare(estate, toolkit, monkeypatch)
    second = _command_run(toolkit, estate, tmp_path / "cache")

    assert second.returncode == 0, second.stderr
    document = read_provenance(estate.run_output / END_OF_RUN_RECORD / PROVENANCE_FILE)
    versions = _versions_by_system(document)
    assert versions["marginal-distribution-fitting"] == toolkit.commit
    assert versions["observed-record-assembly"] == first_commit
    assert set(versions.values()) == {first_commit, toolkit.commit}
    assert document["mixed_systems"] == []


def test_rerunning_the_merge_row_is_refused_by_default_recorded_under_the_override_and_repeated_by_the_next_run(
    tmp_path: Path, toolkit_copy: ToolkitCopy, estate: Estate, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Property: rebuilding only the merge row at a second commit leaves two systems with two versions, which the end-of-run rule refuses naming the rules, the files and the override; with the override it exits 0 listing both systems and every rule's identity; a third run at the same commit rebuilds the merge row again.

    Class: a full kept-absent run, a second commit, then three runs on one output: override off, override on, override on again.
    Why it is required: this is the whole of version stacking through the engine — the refusal's message
    reaching the operator, the record the override writes, and the stickiness of a rerun list that is read
    at every run.
    What kills it: an end-of-run rule that does not read the run-start record's flag, a provenance file
    holding one identity per system only, or a rerun list consumed by the first run that reads it.
    Second correct implementation: a different forced set; the mixed systems and identities would then
    differ and this check would change with it.
    """
    _stacking_estate(estate)
    first_commit = toolkit_copy.commit
    _prepare(estate, toolkit_copy, monkeypatch)
    first = _command_run(toolkit_copy, estate, tmp_path / "cache")
    assert first.returncode == 0, first.stderr

    toolkit = advance_commit(toolkit_copy, estate)
    write_version_stacking(estate, rerun_rules=("observed_record_assembly",))
    _prepare(estate, toolkit, monkeypatch)
    refused = _command_run(toolkit, estate, tmp_path / "cache")

    assert refused.returncode != 0
    output = refused.stdout + refused.stderr
    assert "combined-event-identification" in output
    assert "per_source_event_selection" in output
    assert "combined_event_identification" in output
    assert "allow_mixed_versions" in output
    assert str(estate.run_output / "per_source_event_selection" / IDENTITY_FILE) in output
    assert str(estate.run_output / "combined_event_identification" / IDENTITY_FILE) in output

    write_version_stacking(estate, rerun_rules=("observed_record_assembly",), allow_mixed_versions=True)
    _prepare(estate, toolkit, monkeypatch)
    allowed = _command_run(toolkit, estate, tmp_path / "cache")

    assert allowed.returncode == 0, allowed.stderr
    document = read_provenance(estate.run_output / END_OF_RUN_RECORD / PROVENANCE_FILE)
    assert document["mixed_systems"] == ["combined-event-identification", "observed-record-assembly"]
    rule_identity_document = document["rule_identities"]
    assert isinstance(rule_identity_document, dict)
    versions_by_rule = {rule: entry["code_version"] for rule, entry in rule_identity_document.items()}
    for rule in ("observed_record_pre_selection", "per_source_event_selection", "observed_record_licence"):
        assert versions_by_rule[rule] == first_commit
    assert versions_by_rule["observed_record_assembly"] == toolkit.commit
    assert versions_by_rule["combined_event_identification"] == toolkit.commit
    versions = _versions_by_system(document)
    assert versions["observed-record-assembly"] == toolkit.commit

    time.sleep(1.1)
    _prepare(estate, toolkit, monkeypatch)
    again = _command_run(toolkit, estate, tmp_path / "cache")

    assert again.returncode == 0, again.stderr
    repeated = read_provenance(estate.run_output / END_OF_RUN_RECORD / PROVENANCE_FILE)
    assert "observed_record_assembly" in repeated["invocation_rules"]  # type: ignore[operator]
    assert "observed_record_pre_selection" not in repeated["invocation_rules"]  # type: ignore[operator]
