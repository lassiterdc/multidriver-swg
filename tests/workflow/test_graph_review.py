"""Review tests for the rule table: the closed public surface, the stdlib-only layer, and the artifact path form.

These checks cross boundaries the table's own tests cannot see from inside a value comparison. The table
is rendered by a workflow file another change writes, listed against a register another repository owns,
and consumed by a target-list calculation that walks it; a change that keeps every row comparison green
can still add a public name the shape does not govern, import the rest of the toolkit into a module that
must stay a table, or give an artifact a name that is not a single path segment. Each check below reads
the module as a syntax tree or reads the package surface, so it fails on those changes and not on the
wording of a row.

They are deliberately not a second copy of the rows: the row-level comparisons belong to the module's own
tests, and a review test that restated them would double the maintenance without adding a check.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

import multidriver_swg.workflow as workflow
import multidriver_swg.workflow.graph as graph

SOURCE = Path(graph.__file__).resolve()

# The module's public surface: the record, the table, the identity file name, the lookup refusal, and the
# four functions the approved graph states. Any other public name is a surface no shape row governs.
EXPECTED_PUBLIC_NAMES = {
    "GRAPH",
    "GraphLookupError",
    "IDENTITY_FILE",
    "RuleSpec",
    "downstream_of",
    "judgement_rules",
    "output_classes",
    "rule_for_system",
}

# The names a table module must never reach: file and stream access, a path library, and an output call.
# The first three are how a table stops being data and starts being a step; the last is how it starts
# writing into a command's output.
FORBIDDEN_CALLS = {"open", "print", "read_text", "write_text", "read_bytes", "write_bytes"}

# One path segment: the rule name is the directory an artifact lives under, and an output name is a file
# inside it with exactly one extension, so neither may carry a separator.
RULE_SEGMENT = re.compile(r"[A-Za-z0-9_-]+")
OUTPUT_SEGMENT = re.compile(r"[A-Za-z0-9_-]+\.[A-Za-z0-9]+")


def _module_tree() -> ast.Module:
    """Return the graph module parsed as a syntax tree, read from its own file."""
    return ast.parse(SOURCE.read_text(encoding="utf-8"))


def _public_names(tree: ast.Module) -> set[str]:
    """Return every module-level name the module defines without a leading underscore."""
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
    return {name for name in names if not name.startswith("_")}


def test_the_module_presents_exactly_the_public_names_the_table_is_made_of() -> None:
    """Property: the module's public names are exactly the table, its record, its identity-file constant, its refusal and the four functions.

    Class: every module-level public definition in the source.
    Why it is required: the module is the graph's whole surface, and a name added here is a name the
    workflow file and every compute system must be told about. An extra public callable is a second way
    to answer a question the four functions already answer, and the shape governs none of it.
    Killed by: adding a public helper, a public class or a module-level list, or renaming one of the
    governed names.
    Second correct implementation: any parse that reads module-level definitions; the set comparison is
    what decides, not the traversal.
    """
    assert _public_names(_module_tree()) == EXPECTED_PUBLIC_NAMES
    assert len(_public_names(_module_tree())) == len(EXPECTED_PUBLIC_NAMES)


def test_the_module_is_data_only_importing_only_the_standard_library_and_calling_no_io() -> None:
    """Property: the module's only imports are `__future__` and `dataclasses`, and no call in it is a file, stream or output call.

    Class: every import node and every call node in the source.
    Why it is required: the table is read by the workflow file at parse time and by a target-list
    calculation before any step runs; a module that reached the package's I/O or the engine would make
    reading the graph a side effect, and a module that printed would write into a command's output.
    Killed by: importing a package module or a third-party library, or adding a file read or a print.
    Second correct implementation: executing the module in a child process and inspecting `sys.modules`
    and stdout; the same properties are observed rather than parsed.
    """
    tree = _module_tree()

    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])

    assert imported == {"__future__", "dataclasses"}

    calls: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name):
            calls.add(node.func.id)
        elif isinstance(node.func, ast.Attribute):
            calls.add(node.func.attr)
    assert calls & FORBIDDEN_CALLS == set()


def test_the_module_holds_no_mutable_top_level_state() -> None:
    """Property: every module-level value is a tuple, a frozen record type, an exception class or a string constant, and the table is a tuple.

    Class: every module-level assignment and annotated assignment.
    Why it is required: a consumer holds the table across a render, so a module-level list or dictionary
    would be state one consumer can mutate for every other; the order guarantee would then hold until
    the first edit rather than for the run.
    Killed by: changing `GRAPH` to a list, or adding a module-level dictionary of rules by system.
    Second correct implementation: a named tuple of plain tuples; the values are immutable as well.
    """
    tree = _module_tree()
    mutable: list[str] = []

    for node in tree.body:
        if not isinstance(node, ast.Assign | ast.AnnAssign):
            continue
        value = node.value
        if isinstance(value, ast.Call) and isinstance(value.func, ast.Name) and value.func.id == "RuleSpec":
            continue
        if isinstance(value, ast.Tuple | ast.Constant):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        mutable.extend(target.id for target in targets if isinstance(target, ast.Name))

    assert mutable == []
    assert isinstance(graph.GRAPH, tuple)


def test_every_rule_name_and_declared_output_is_a_single_path_segment() -> None:
    """Property: every rule name is one path segment, every declared output is one segment with one extension, the identity file is the exact name the graph publishes, and no row declares or reads it.

    Class: the twelve rule names, the twelve declared outputs and the identity file constant.
    Why it is required: an artifact's path is `{rule}/{output}` under the engine's working directory, so
    a separator in either cell would move an artifact out of its rule's directory while every listing
    still looked correct; and a row that declared the identity file would list it twice, once as an
    output and once as provenance.
    Killed by: a rule name or output carrying a slash, an output with no extension, a reworded
    identity-file constant, or a row adding `implementation_identity.json` to its outputs or reads.
    Second correct implementation: building each artifact's path and asserting it has exactly two parts;
    the same names are checked.
    """
    assert graph.IDENTITY_FILE == "implementation_identity.json"

    for rule in graph.GRAPH:
        assert RULE_SEGMENT.fullmatch(rule.name), rule.name
        for output in rule.outputs:
            assert OUTPUT_SEGMENT.fullmatch(output), output
        assert graph.IDENTITY_FILE not in rule.outputs
        assert graph.IDENTITY_FILE not in rule.reads


def test_the_package_reexports_exactly_the_governed_names() -> None:
    """Property: the package's `__all__` is exactly the seven governed names, they are the module's objects, and the identity-file constant stays in the module.

    Class: the package surface and the module's objects.
    Why it is required: a sibling imports from the package, so the package list is the surface a later
    change must extend deliberately; a constant re-exported here would invite a comparison against a
    package attribute instead of the module that owns it.
    Killed by: adding or removing an `__all__` entry, or aliasing a name to a different object.
    Second correct implementation: an explicit import list in the package checked by identity; the same
    seven names and the same absence hold.
    """
    assert set(workflow.__all__) == EXPECTED_PUBLIC_NAMES - {"IDENTITY_FILE"}
    for name in workflow.__all__:
        assert getattr(workflow, name) is getattr(graph, name)
    assert not hasattr(workflow, "IDENTITY_FILE")


@pytest.mark.parametrize(
    ("field", "value", "expected"),
    [
        ("outputs", "x.json", ("x.json",)),
        ("reads", "y.json", ("y.json",)),
        ("outputs", ["x.json"], ("x.json",)),
        ("reads", ["y.json"], ("y.json",)),
    ],
)
def test_a_row_never_stores_its_reads_or_outputs_as_a_string_or_a_list(field: str, value: object, expected: tuple):
    """Property: a row built with a bare string or a list for `reads` or `outputs` is either refused with `GraphLookupError` or stores the names as the tuple of those names; it never stores the value as given.

    Shape row: DR-1 — `GRAPH` is "a tuple of frozen `RuleSpec(name, system, reads, outputs, reads_selection,
    local)` records", and "a compute system's own plan replaces a stub by editing its row"; DR-7 — the target
    cut is computed from `downstream_of`.
    Class: the two name-collection fields crossed with the two non-tuple forms a one-name edit produces: a
    parenthesised string written without its trailing comma, and a list.
    Why it is required: a string is iterable, so `outputs=("x.json")` stores the text `x.json`; the row's
    duplicate-name check then compares CHARACTERS and passes, `output_classes` lists one artifact per
    character, and `downstream_of` finds no edge for a `reads` string, so a row that consumes the selection
    chain looks like a leaf and the target cut keeps it. A list passes every check and is then mutable
    inside a record the module calls frozen. Neither error is visible in the table's own row tests.
    Killed by: `__post_init__` checks that test the collection's length and repeats without testing its
    type, so a str or a list constructs unchanged.
    Second correct implementation: refusing any non-tuple with `GraphLookupError`, or normalising a str
    to a one-name tuple and a list to a tuple; both pass, because the test accepts either outcome.
    """
    arguments: dict[str, object] = {"outputs": ("o.json",)}
    arguments[field] = value

    try:
        rule = graph.RuleSpec(name="r", system="s", **arguments)  # type: ignore[arg-type]
    except graph.GraphLookupError:
        return

    stored = getattr(rule, field)
    assert isinstance(stored, tuple)
    assert stored == expected
