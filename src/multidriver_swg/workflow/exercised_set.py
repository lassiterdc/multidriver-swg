"""The exercised set: which systems a run's outputs came from, read from the engine's own records.

A run's provenance record is only as true as the set of systems it is checked against. A set derived from
the graph's table says which systems COULD have run; a set derived from the run's target list says which
systems the run asked for; neither says which systems produced the files that are on disk now. The engine
already knows that last fact: it writes one metadata record per output file, naming the rule that wrote it.
This module reads those records, so the exercised set is the set of producers of the files the run's
final rule depends on, and a producer is named because a record says it produced something rather than
because a list says it was meant to.

The set is CUMULATIVE by construction. The metadata store outlives an invocation, so an output kept from an
earlier invocation still names its producer, and that producer is exercised by the run the output belongs
to. The rules that ran in THIS invocation are a different, smaller fact; `invocation_rules` states it and
the record carries it as an informational field that no check reads.

The target list is the end-of-run rule's OWN input list, passed in by the caller and never recomputed
here. Whatever the rule waits on is what the set is read over, so a run that ends with fewer targets names
exactly the producers of those targets, and a later change to how that input list is computed changes no
line of this module. The end-of-run record's own output is never among the targets: its metadata record
does not exist while its rule runs.

The record is read from the engine's store layout, which the engine owns and may change. That is why the
reader refuses a record whose format version is not the one this module was written against: a changed
layout is then a refusal naming the file and the version, and never a record misread as another one.

The provenance file written into the end-of-run record is one JSON object with a closed key set, and this
module holds its writer and its only reader, so the form is enforced in one place.

Not established: that the metadata store reads the same way inside a scheduler job as on the driver. The
tests read it on the driver, and what a job sees is a cluster measurement. Not established either: that a
seed was drawn by any producer. No rule draws at this tree and the provenance module defines no persisted
form of a seed, so the record carries no seeds; the field is empty because nothing exists to put in it.
"""

from __future__ import annotations

import json
import os
from base64 import urlsafe_b64encode
from collections.abc import Iterable, Mapping
from pathlib import Path

from multidriver_swg.run_configuration.provenance import (
    ImplementationIdentity,
    ProvenanceRecord,
    capture,
    read_identity,
    require_complete,
)
from multidriver_swg.side_record import SideRecord
from multidriver_swg.workflow.graph import GRAPH, IDENTITY_FILE
from multidriver_swg.workflow.run_start import RUN_START_FILE, RunStart

# The record format the engine writes at the version this module was written against. A record carrying
# another number was written by an engine whose layout this reader has not been checked against.
METADATA_FORMAT_VERSION = 6

# The provenance file's name inside the end-of-run record, and the literal its `format` key carries. The
# literal is compared as an exact string, so a later change to the file's keys is loud rather than
# silently misread by an older reader.
PROVENANCE_FILE = "provenance.json"

PROVENANCE_RULE = "end-of-run-provenance:1"

_PROVENANCE_KEYS = (
    "format",
    "inputs",
    "seeds",
    "identities",
    "exercised_systems",
    "invocation_rules",
    "side_record",
)

_IDENTITY_KEYS = ("implementation", "code_version")

# The engine's metadata directory, relative to its working directory.
_METADATA_DIRECTORY = Path(".snakemake") / "metadata"

# The engine's limit when the filesystem reports none.
_DEFAULT_NAME_MAX = 255


class ExercisedSetError(Exception):
    """A metadata record, identity or provenance file this module refuses, with the file named.

    Deliberately NOT derived from `KeyError`: the message is the reader's recovery path, and a `KeyError`
    renders it inside repr quotes.
    """


def _record_path(working_directory: Path, target: str) -> Path:
    """Return the file the engine keeps `target`'s metadata record in, built the way the engine builds it.

    The identifier is the URL-safe base64 of the target's text. An identifier longer than a file name may be
    is split into segments one character shorter than the longest name the filesystem allows; every segment
    but the last is prefixed with `@` and used as a directory, and the last is the file. The `@` cannot
    occur in base64 text, which is what keeps a directory and a file of one parent from sharing a name.
    """
    metadata_directory = working_directory / _METADATA_DIRECTORY
    name_max = _DEFAULT_NAME_MAX
    if os.name == "posix":
        name_max = os.pathconf(metadata_directory, "PC_NAME_MAX") or _DEFAULT_NAME_MAX
    identifier = urlsafe_b64encode(target.encode()).decode()
    segment = name_max - 1
    parts = [identifier[start : start + segment] for start in range(0, len(identifier), segment)]
    return metadata_directory.joinpath(*(["@" + part for part in parts[:-1]] + [parts[-1]]))


def _read_record(working_directory: Path, target: str) -> tuple[Path, dict[str, object]]:
    """Return the record file for `target` and its parsed content, refusing every record the engine did not complete.

    Each refusal names the target and the file, because the reader is looking at an engine-written file it
    cannot repair and the recovery is to find the run that should have written it.
    """
    metadata_directory = working_directory / _METADATA_DIRECTORY
    if not metadata_directory.is_dir():
        raise ExercisedSetError(
            f"target {target!r}: the engine wrote no metadata record for this output, and its metadata "
            f"directory {metadata_directory} does not exist"
        )
    path = _record_path(working_directory, target)
    if not path.is_file():
        raise ExercisedSetError(
            f"target {target!r}: the engine wrote no metadata record for this output; the record would be {path}"
        )
    try:
        text = path.read_text(encoding="utf-8")
        document = json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ExercisedSetError(
            f"target {target!r}: the metadata record {path} does not parse, so no producer can be read: {error}"
        ) from error
    if not isinstance(document, dict):
        raise ExercisedSetError(
            f"target {target!r}: the metadata record {path} is {document!r}, not one JSON object, so it names no producer"
        )
    version = document.get("record_format_version")
    if version != METADATA_FORMAT_VERSION:
        raise ExercisedSetError(
            f"target {target!r}: the metadata record {path} has record_format_version {version!r}; this reader "
            f"was written against version {METADATA_FORMAT_VERSION}, and an engine that changed the layout can "
            "change what a field means"
        )
    if document.get("incomplete") is not False:
        raise ExercisedSetError(
            f"target {target!r}: the metadata record {path} is marked incomplete ({document.get('incomplete')!r}), "
            "so the job that wrote the output did not finish"
        )
    return path, document


def producing_rules(working_directory: Path, targets: Iterable[str]) -> dict[str, str]:
    """Return, for each target, the name of the rule its metadata record says wrote it.

    The records are the engine's own, read at `{working_directory}/.snakemake/metadata`. A target whose
    record is missing, does not parse, carries another format version, is marked incomplete, or names a rule
    the graph does not hold is refused with `ExercisedSetError` naming the target and the file: a producer
    that cannot be named is a gap in the record, and a record with a silent gap would pass for a complete one.

    A rule the graph does not hold is refused because every identity read next is found under the rule's
    directory and every system is found through its row; a name outside the table has neither.
    """
    known = {rule.name for rule in GRAPH}
    producers: dict[str, str] = {}
    for target in targets:
        path, document = _read_record(working_directory, target)
        rule = document.get("rule")
        if not isinstance(rule, str) or rule not in known:
            raise ExercisedSetError(
                f"target {target!r}: the metadata record {path} names the rule {rule!r}, which is not a row of the "
                "graph: the rules the graph holds are " + ", ".join(repr(name) for name in sorted(known))
            )
        producers[target] = rule
    return producers


def exercised_systems(producers: Mapping[str, str]) -> frozenset[str]:
    """Return the owning system of each producing rule: the exercised set.

    `producers` is what `producing_rules` returns. A rule name the graph does not hold is refused, though
    `producing_rules` already refuses one, because this function is public and a caller may pass any mapping.
    """
    systems = {rule.name: rule.system for rule in GRAPH}
    exercised: set[str] = set()
    for target, rule in producers.items():
        if rule not in systems:
            raise ExercisedSetError(f"target {target!r}: {rule!r} is not a row of the graph, so it names no system")
        exercised.add(systems[rule])
    return frozenset(exercised)


def invocation_rules(working_directory: Path, producers: Mapping[str, str]) -> tuple[str, ...]:
    """Return the producing rules whose record's end time is at or after the run-start record's modification, in table order.

    The result is informational: the record carries it so a reader can tell what this invocation built from
    what it found already built, and no check reads it.

    The end time is read and the start time is not, because the engine's `starttime` field is null in every
    record a local run writes at the engine version this module was written against (measured: all thirteen
    records of one run held null), so it cannot say when a rule began. The `endtime` field is the
    modification time of the output the engine found when the job finished, and an output kept from an
    earlier invocation keeps its earlier modification time, so an end time at or after the run-start record
    means the rule wrote that output in this invocation.

    Its residual: under the command the pre-run step rewrites the run-start record immediately before the
    engine starts, so the comparison is exact; a direct engine invocation that reuses an earlier run-start
    record counts every rule run since that record was written, because nothing marks where an invocation
    began other than that file.

    A record with no numeric end time is refused, because a rule whose output time cannot be read is neither
    in nor out of this invocation, and a guess would be a silent claim.
    """
    boundary = (working_directory / RUN_START_FILE).stat().st_mtime
    started: set[str] = set()
    for target, rule in producers.items():
        path, document = _read_record(working_directory, target)
        endtime = document.get("endtime")
        if isinstance(endtime, bool) or not isinstance(endtime, int | float):
            raise ExercisedSetError(
                f"target {target!r}: the metadata record {path} has endtime {endtime!r}, not a number, so "
                f"whether rule {rule!r} ran in this invocation cannot be read"
            )
        if endtime >= boundary:
            started.add(rule)
    return tuple(rule.name for rule in GRAPH if rule.name in started)


def build_provenance(
    working_directory: Path,
    targets: Iterable[str],
    run_start: RunStart,
    side_record: SideRecord,
) -> ProvenanceRecord:
    """Return the run's provenance record, built from the identities its producers persisted.

    `targets` is the end-of-run rule's own input list. Each target's producing rule is read from the
    engine's metadata, each producing rule's identity from the file it persisted beside its outputs, and the
    record is captured from those identities and then checked complete against the exercised set read from
    the same records. An identity whose `system` is not the row's owning system is refused: the file says
    which system produced the rule's output, and a rule row and an identity that disagree describe two
    different runs.

    `side_record` is checked to be a `SideRecord` and is not captured here. The provenance file carries it
    under its own key, written unaltered by `write_provenance`, so capturing it also as an input would state
    it twice; checking it here refuses a wrong value before any file is written.

    The seeds are empty. No producer draws, and the provenance module persists no seed, so there is no value
    to read; the record is honest about that by carrying an empty mapping.
    """
    if not isinstance(side_record, SideRecord):
        raise TypeError(
            f"build_provenance was given {type(side_record).__name__} rather than a SideRecord: the side "
            "record is the statement of which side each pair ran"
        )
    target_list = list(targets)
    producers = producing_rules(working_directory, target_list)
    systems = {rule.name: rule.system for rule in GRAPH}

    identities: dict[str, ImplementationIdentity] = {}
    for rule_name in dict.fromkeys(producers.values()):
        identity_path = working_directory / rule_name / IDENTITY_FILE
        try:
            identity = read_identity(identity_path)
        except OSError as error:
            raise ExercisedSetError(
                f"rule {rule_name!r}: its identity file {identity_path} cannot be read, so the system that "
                f"produced its outputs is unnamed: {error}"
            ) from error
        if identity.system != systems[rule_name]:
            raise ExercisedSetError(
                f"rule {rule_name!r}: its identity file {identity_path} names the system {identity.system!r}, but "
                f"the graph's row for the rule gives {systems[rule_name]!r}"
            )
        identities[identity.system] = identity

    record = capture(
        inputs={
            "main_configuration": run_start.main_configuration,
            "targets": target_list,
            "selection": run_start.selection,
            "judgement": run_start.judgement,
        },
        seeds={},
        identities=identities,
    )
    require_complete(record, exercised_systems(producers))
    return record


def write_provenance(
    path: Path,
    record: ProvenanceRecord,
    *,
    exercised: Iterable[str],
    invocations: Iterable[str],
    side_record: SideRecord,
) -> None:
    """Write the provenance file at `path`: the record, the exercised systems, this invocation's rules, and the side record.

    One JSON object whose keys are `format`, `inputs`, `seeds`, `identities` (each system's implementation
    and code version), `exercised_systems` (sorted), `invocation_rules` and `side_record`. The side record is
    `side_record.as_mapping()`, unaltered, because the side-record contract requires a provenance record
    that carries its content to carry exactly that mapping. The file is written whole and replaces whatever
    held the path.
    """
    document = {
        "format": PROVENANCE_RULE,
        "inputs": dict(record.inputs),
        "seeds": dict(record.seeds),
        "identities": {
            system: {"implementation": identity.implementation, "code_version": identity.code_version}
            for system, identity in sorted(record.identities.items())
        },
        "exercised_systems": sorted(exercised),
        "invocation_rules": list(invocations),
        "side_record": side_record.as_mapping(),
    }
    path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")


def read_provenance(path: Path) -> dict[str, object]:
    """Return the provenance file's one JSON object, refusing a document outside the form with the key and the file named.

    The refusals: a file that is not UTF-8 JSON or not one object; a key the form does not name, or one of
    the seven it does name that is absent; a `format` other than `PROVENANCE_RULE`; and a key whose value is
    not the kind the form gives it (a mapping for `inputs`, `seeds`, `identities` and `side_record` — each
    identity a mapping of exactly an implementation and a code version — and a list of text for
    `exercised_systems` and `invocation_rules`). This is the form's only reader, so a consumer never parses
    the file itself.
    """
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ExercisedSetError(f"provenance file {path} is not valid UTF-8 JSON: {error}") from error
    if not isinstance(document, dict):
        raise ExercisedSetError(f"provenance file {path} is {document!r}, not one JSON object")
    for key in document:
        if key not in _PROVENANCE_KEYS:
            raise ExercisedSetError(
                f"provenance file {path} carries the key {key!r}: the form is exactly "
                + ", ".join(repr(name) for name in _PROVENANCE_KEYS)
            )
    for key in _PROVENANCE_KEYS:
        if key not in document:
            raise ExercisedSetError(f"provenance file {path} omits the key {key!r}")
    if document["format"] != PROVENANCE_RULE:
        raise ExercisedSetError(
            f"provenance file {path} key 'format' is {document['format']!r}: the form declares {PROVENANCE_RULE!r}"
        )
    for key in ("inputs", "seeds", "identities", "side_record"):
        if not isinstance(document[key], dict):
            raise ExercisedSetError(f"provenance file {path} key {key!r} is {document[key]!r}, not a mapping")
    for key in ("exercised_systems", "invocation_rules"):
        value = document[key]
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise ExercisedSetError(f"provenance file {path} key {key!r} is {value!r}, not a list of text")
    for system, identity in document["identities"].items():
        if not isinstance(identity, dict) or tuple(sorted(identity)) != tuple(sorted(_IDENTITY_KEYS)):
            raise ExercisedSetError(
                f"provenance file {path} key 'identities' entry {system!r} is {identity!r}: each identity is "
                "a mapping of exactly " + " and ".join(repr(name) for name in _IDENTITY_KEYS)
            )
    return document
