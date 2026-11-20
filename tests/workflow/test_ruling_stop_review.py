"""Review tests for the ruling stop: the checkpoint's lookup refusal, the cut's independence of table order, the module's layer, the licence rows and the workflow file's single post-checkpoint list.

The author's tests drive the three predicate functions over the real table and run the engine twice. These
checks state what those leave open: the private checkpoint lookup refuses a table with none or two (the
author tests only the real table, which has one), the cut follows edges rather than position (the real table
has one independent row, and it is last), the module keeps the layer the design gives it, the first four rows
are the rows the design names, and the workflow file states the post-checkpoint list once and renders the
checkpoint with the directives of every other rule. The workflow file is read as text, because the engine
is the only thing that can execute it and every check here must stay far below a second.
"""

from __future__ import annotations

import ast
from dataclasses import replace
from pathlib import Path

import pytest

import multidriver_swg.workflow.graph as graph
import multidriver_swg.workflow.ruling_stop as ruling_stop
from multidriver_swg.workflow.graph import GRAPH, RuleSpec
from multidriver_swg.workflow.ruling_stop import RulingStopError, stop_targets

REPO_ROOT = Path(__file__).resolve().parents[2]
SNAKEFILE = REPO_ROOT / "src" / "multidriver_swg" / "workflow" / "Snakefile"
RULING_STOP = REPO_ROOT / "src" / "multidriver_swg" / "workflow" / "ruling_stop.py"


def _function_span(lines: list[str], name: str) -> range:
    """Return the line indexes of the top-level function `name`: its `def` line up to the next unindented line."""
    start = next(index for index, line in enumerate(lines) if line.startswith(f"def {name}("))
    end = start + 1
    while end < len(lines) and (not lines[end].strip() or lines[end].startswith((" ", "\t"))):
        end += 1
    return range(start, end)


def _block(lines: list[str], header: str) -> list[str]:
    """Return the lines of the block opened by `header` (compared stripped), up to the next line at its indentation or shallower."""
    start = next(index for index, line in enumerate(lines) if line.strip() == header)
    indentation = len(lines[start]) - len(lines[start].lstrip())
    end = start + 1
    while end < len(lines):
        candidate = lines[end]
        if candidate.strip() and len(candidate) - len(candidate.lstrip()) <= indentation:
            break
        end += 1
    return lines[start:end]


def test_the_checkpoint_lookup_refuses_a_table_with_no_checkpoint_row_or_two(monkeypatch: pytest.MonkeyPatch) -> None:
    """Property: `stop_targets` raises `RulingStopError` naming the count when the table holds zero or two checkpoint rows.

    Class: the real table with its checkpoint flag cleared, and with the merge row flagged as a second one.
    Why it is required: the stop is defined against exactly one checkpoint; the design makes the private
    lookup the one runtime read of that invariant, and the author's tests only ever see the real table.
    What kills it: `next(...)` over the flagged rows, which would return the first row of two or raise a
    bare `StopIteration` for none.
    Second correct implementation: a public lookup raising the same error; only the call here would change.
    """
    cleared = tuple(replace(rule, checkpoint=False) for rule in GRAPH)
    monkeypatch.setattr(ruling_stop, "GRAPH", cleared)
    with pytest.raises(RulingStopError) as none_held:
        stop_targets(["observed_record_assembly/observed_record.json"])
    assert "0 checkpoint rows" in str(none_held.value)

    doubled = tuple(
        replace(rule, checkpoint=True) if rule.name == "observed_record_assembly" else rule for rule in GRAPH
    )
    monkeypatch.setattr(ruling_stop, "GRAPH", doubled)
    with pytest.raises(RulingStopError) as two_held:
        stop_targets(["observed_record_assembly/observed_record.json"])
    assert "2 checkpoint rows" in str(two_held.value)


def test_the_cut_follows_the_table_edges_and_never_the_table_position(monkeypatch: pytest.MonkeyPatch) -> None:
    """Property: over a table whose independent row is placed BEFORE a reader of the checkpoint, the cut keeps the independent row and drops the reader.

    Class: a four-row table: an independent row first, the checkpoint, a row reading it, and a row reading
    only the independent row; plus a target naming no row and a target with no slash.
    Why it is required: the design rejects "every row at or before the checkpoint in table order" because it
    breaks the first time a row independent of the licence sits early; the real table's one independent row
    is the last, so only a table of another shape tells the two cuts apart.
    What kills it: keeping the rows that precede the checkpoint in position, or dropping every row after it.
    Second correct implementation: filtering by the set `downstream_of` returns; the same kept list.
    """
    table = (
        RuleSpec(name="early", system="s", outputs=("early.json",)),
        RuleSpec(name="gate", system="s", outputs=("gate.json",), checkpoint=True),
        RuleSpec(name="after_gate", system="s", reads=("gate.json",), outputs=("after.json",)),
        RuleSpec(name="after_early", system="s", reads=("early.json",), outputs=("late.json",)),
    )
    monkeypatch.setattr(graph, "GRAPH", table)
    monkeypatch.setattr(ruling_stop, "GRAPH", table)
    targets = [
        "early/early.json",
        "gate/gate.json",
        "after_gate/after.json",
        "after_early/late.json",
        "ghost/x.json",
        "end_of_run_record",
    ]

    assert stop_targets(targets) == (
        "early/early.json",
        "gate/gate.json",
        "after_early/late.json",
        "ghost/x.json",
        "end_of_run_record",
    )


def test_the_ruling_stop_module_imports_only_the_graph_and_the_selection_artifact_and_touches_no_file() -> None:
    """Property: the module's toolkit imports are exactly the graph and the selection artifact, and its code names no file or stream access.

    Class: the module's syntax tree.
    Why it is required: the design gives the module those two imports and makes every function pure so it can
    be driven without an estate; an import of the stubs, the pre-run step or the end-of-run record would
    couple the stop to the layers that call it, and a read inside it would hide the licence's source.
    What kills it: `from multidriver_swg.workflow.stubs import ...`, `open(...)`, `Path(...).read_text()`.
    Second correct implementation: a module importing the same two names under other aliases; the module
    paths are what is compared.
    """
    tree = ast.parse(RULING_STOP.read_text(encoding="utf-8"))
    toolkit_modules = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("multidriver_swg")
    }
    toolkit_modules |= {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
        if alias.name.startswith("multidriver_swg")
    }
    names = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
    attributes = {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)}

    assert toolkit_modules == {
        "multidriver_swg.run_configuration.selection_artifact",
        "multidriver_swg.workflow.graph",
    }
    assert names.isdisjoint({"open", "Path", "print"})
    assert attributes.isdisjoint({"read_text", "write_text", "read_bytes", "write_bytes", "open"})


def test_the_first_four_rows_are_the_rows_the_design_names_with_the_merge_row_unchanged() -> None:
    """Property: rows one to four carry the design's names, systems, reads and outputs, and the merge row keeps its old name, output and path.

    Class: the table's first four rows, compared whole.
    Why it is required: a consumer addresses a rule by its name and the design fixes the names when
    the chunk lands; the merge row's output path is named by every later row's input and by documents the run
    writes, so a changed name would strand them without a failing import.
    What kills it: renaming a row, swapping the system of the pre-selection and per-source rows, or giving the
    merge row a new output name.
    Second correct implementation: building the rows from a helper; the same fields result.
    """
    rows = [(rule.name, rule.system, rule.reads, rule.outputs, rule.checkpoint) for rule in GRAPH[:4]]

    assert rows == [
        ("observed_record_pre_selection", "observed-record-assembly", (), ("pre_selection.json",), False),
        (
            "per_source_event_selection",
            "combined-event-identification",
            ("pre_selection.json",),
            ("per_source_event_sets.json",),
            False,
        ),
        (
            "observed_record_licence",
            "observed-record-assembly",
            ("per_source_event_sets.json",),
            ("licence.json",),
            True,
        ),
        ("observed_record_assembly", "observed-record-assembly", ("licence.json",), ("observed_record.json",), False),
    ]
    assert len(GRAPH) == 15
    assert len({rule.system for rule in GRAPH}) == 12


def test_the_workflow_file_states_the_post_checkpoint_list_once() -> None:
    """Property: the run-start record's targets are read in exactly one place, `run_targets`, and `all`, `end_of_run` and the attribution inputs each read that function.

    Class: the workflow file's text.
    Why it is required: the design makes `run_targets` the single statement of the post-checkpoint list with three
    readers; a second read of the record's targets would let one reader keep the full list after a stop and
    wait on an output that was never built.
    What kills it: `all` given `RUN_START.targets` again, or an attribution function that filters the record's
    targets instead of the function's list.
    Second correct implementation: the list held in a helper function other than `run_targets`; the
    constant names here would change and the one-reader property would not.
    """
    lines = SNAKEFILE.read_text(encoding="utf-8").splitlines()
    span = _function_span(lines, "run_targets")
    readers = [index for index, line in enumerate(lines) if "RUN_START.targets" in line]
    all_block = "\n".join(_block(lines, "rule all:"))
    end_block = "\n".join(_block(lines, "rule end_of_run:"))
    attribution = "\n".join(lines[index] for index in _function_span(lines, "attribution_inputs"))

    assert readers, "no read of the record's targets"
    assert all(index in span for index in readers)
    assert "run_targets" in all_block
    assert "run_targets(wildcards)" in end_block
    assert "run_targets(wildcards)" in attribution
    assert "END_OF_RUN_RECORD" in end_block
    assert "END_OF_RUN_RECORD" in attribution


def test_the_checkpoint_is_rendered_with_the_directives_of_every_other_rule_and_writes_the_licence_after_the_stub() -> (
    None
):
    """Property: the `checkpoint:` and `rule:` blocks of the rendering loop carry identical directives, and only the checkpoint's body writes the licence, after the stub outputs.

    Class: the rendering loop's two branches, compared line by line up to their `run:` directive.
    Why it is required: the design renders the one row as an engine checkpoint "with identical directives"; a
    directive edited in one branch only would give the licence row inputs, outputs or params no other row
    has, and a licence written before the stub would be overwritten by it with an empty file.
    What kills it: a different `input:` in one branch, `write_licence_stub` in the plain rule's body, or the
    licence write placed ahead of `write_stub_outputs`.
    Second correct implementation: one block with the keyword chosen by a variable; this check would then
    find one branch and change.
    """
    lines = SNAKEFILE.read_text(encoding="utf-8").splitlines()
    checkpoint = _block(lines, "checkpoint:")
    plain = _block(lines, "rule:")

    def directives(block: list[str]) -> list[str]:
        run = next(index for index, line in enumerate(block) if line.strip() == "run:")
        return [line.strip() for line in block[1:run]]

    def body(block: list[str]) -> list[str]:
        run = next(index for index, line in enumerate(block) if line.strip() == "run:")
        return [line.strip() for line in block[run + 1 :] if line.strip()]

    assert directives(checkpoint) == directives(plain)
    assert any(line.startswith("rendered_inputs(spec)") for line in directives(checkpoint))
    checkpoint_body = body(checkpoint)
    stub = next(index for index, line in enumerate(checkpoint_body) if line.startswith("write_stub_outputs("))
    licence = next(index for index, line in enumerate(checkpoint_body) if line.startswith("write_licence_stub("))
    assert stub < licence
    assert not any("write_licence_stub" in line for line in body(plain))
    assert sum(1 for line in lines if line.strip() == "checkpoint:") == 1
