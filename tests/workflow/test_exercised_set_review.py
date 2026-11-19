"""Review provenance over shared producers and the engine's usable invocation timestamp.

Targets need not encode their producer's name, and two outputs can share a producer. These cases
separate input-scoped provenance from a metadata-store scan or a per-output identity claim.
The invocation check uses deterministic filesystem times and null starts, as real local records do.
"""

from __future__ import annotations

import json
import os
from base64 import urlsafe_b64encode
from pathlib import Path

from multidriver_swg.run_configuration.provenance import ImplementationIdentity, persist_identity
from multidriver_swg.side_record import SideRecord
from multidriver_swg.workflow.exercised_set import build_provenance, invocation_rules, producing_rules
from multidriver_swg.workflow.graph import GRAPH, IDENTITY_FILE
from multidriver_swg.workflow.run_start import RUN_START_FILE, RunStart


def _metadata(working_directory: Path, target: str, rule: str, *, ended: int) -> None:
    """Write a short target's complete metadata record independently of the production path helper."""
    directory = working_directory / ".snakemake" / "metadata"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / urlsafe_b64encode(target.encode()).decode()
    path.write_text(
        json.dumps(
            {"record_format_version": 6, "incomplete": False, "rule": rule, "starttime": None, "endtime": ended}
        ),
        encoding="utf-8",
    )


def test_shared_producers_are_kept_only_while_an_input_still_names_them(tmp_path: Path) -> None:
    """Property: omitting one output keeps its shared producer, but omitting every such output removes it.

    Class: two opaque target paths sharing one producer, alongside a different unrequested producer.
    Why it is required: provenance is the producers of the supplied inputs, not the run-start target
    list, the path's first segment, or the cumulative metadata store's whole population. Implementation
    and code version must be the persisted values rather than constants inferred from the graph.
    What kills it: scanning all records, deriving producers from paths, dropping a producer on any
    omitted output, or replacing persisted identity fields with stub/version constants.
    Second correct implementation: group the supplied targets by their recorded producer, then read
    one identity per group; identical identity mappings and input lists result.
    """
    owner, unrelated = GRAPH[:2]
    first, second, other = "opaque/first.json", "opaque/second.json", "opaque/other.json"
    for target, rule in ((first, owner), (second, owner), (other, unrelated)):
        _metadata(tmp_path, target, rule.name, ended=1000)
        directory = tmp_path / rule.name
        directory.mkdir(exist_ok=True)
        persist_identity(
            directory / IDENTITY_FILE,
            ImplementationIdentity(rule.system, "current" if rule == owner else "pre_refactor", "measured-version"),
        )
    run_start = RunStart(
        main_configuration=str(tmp_path / "configuration.toml"),
        targets=(first, second, other),
        selection=None,
        judgement="present",
    )

    both = build_provenance(tmp_path, (first, second), run_start, SideRecord({}))
    one = build_provenance(tmp_path, (second,), run_start, SideRecord({}))
    neither = build_provenance(tmp_path, (), run_start, SideRecord({}))

    expected = {owner.system: ImplementationIdentity(owner.system, "current", "measured-version")}
    assert producing_rules(tmp_path, (first, second)) == {first: owner.name, second: owner.name}
    assert both.identities == expected
    assert one.identities == expected
    assert neither.identities == {}
    assert both.inputs["targets"] == [first, second]
    assert one.inputs["targets"] == [second]
    assert neither.inputs["targets"] == []


def test_null_starts_use_endtime_at_the_invocation_boundary_in_graph_order(tmp_path: Path) -> None:
    """Property: with null starts, outputs ending at or after run-start mtime count in graph order.

    Class: complete local-style records ending before, exactly at, and after a fixed boundary, with
    their target mapping deliberately reversed relative to the graph.
    Why it is required: null starttime cannot classify local jobs. The informational invocation list
    must distinguish retained outputs from newly completed outputs without shrinking the exercised set.
    What kills it: reading null starttime, using a strict greater-than comparison, including earlier
    endtimes, or returning target iteration order instead of graph order.
    Second correct implementation: filter the metadata records into a set of producer names before
    traversing the graph, rather than accumulating them while reading; the same tuple results.
    """
    before, exact, after = GRAPH[:3]
    boundary = tmp_path / RUN_START_FILE
    boundary.write_text("{}", encoding="utf-8")
    os.utime(boundary, (1000, 1000))
    targets = {"opaque/after.json": after.name, "opaque/exact.json": exact.name, "opaque/before.json": before.name}
    for target, rule, ended in (
        ("opaque/after.json", after.name, 1001),
        ("opaque/exact.json", exact.name, 1000),
        ("opaque/before.json", before.name, 999),
    ):
        _metadata(tmp_path, target, rule, ended=ended)

    assert invocation_rules(tmp_path, targets) == (exact.name, after.name)
    assert producing_rules(tmp_path, targets) == targets
