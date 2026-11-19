"""Review tests for the pre-run step: the check order, the one refusal class, the one write, and the absence of prompts.

These checks cross boundaries the step's own tests cannot see from inside a run. A step can produce the
right record while running its checks in an order that reports a later defect first, raising three
exception classes the command maps three ways, writing a second file a reader does not expect, or reaching
for the process's working directory to find a path the record then cannot show. Each check below reads the
module as a syntax tree or its entry function's signature, so it fails on those changes and not on the
value of a target.

They deliberately do not restate the target-list values: the step's own tests own those, and a review test
that copied them would double the maintenance without adding a check.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pytest

import multidriver_swg.workflow.pre_run as pre_run
from multidriver_swg.run_configuration.main_configuration import MainConfiguration
from multidriver_swg.workflow.end_of_run import END_OF_RUN_RECORD
from multidriver_swg.workflow.graph import IDENTITY_FILE
from multidriver_swg.workflow.pre_run import PreRunError, prepare_run
from tests.workflow.conftest import Estate, ToolkitCopy, run_engine

SOURCE = Path(pre_run.__file__).resolve()

# The order the step's body must follow: the resolver's keys, then the list, then the declaration and gate,
# then the selection's form, then the record. Each later check presupposes the earlier ones' facts.
EXPECTED_CALL_ORDER = (
    "_resolved_keys(",
    "compute_targets(",
    "_require_declared_revision(",
    "_require_selection_form(",
    "record.write(",
)

# Calls that would make the step interactive or a process launcher: the shape says nothing here prompts or
# waits, and a command that blocked would be the failure the pre-run step exists to prevent.
FORBIDDEN_CALLS = {"input", "getpass", "sleep", "subprocess", "system", "popen", "urlopen"}

# The modules the step must import to run the checks it exists for. Each is another system's mechanism; a
# step that reimplemented one would diverge from the module the parse-time gate also runs.
REQUIRED_IMPORTS = {
    "multidriver_swg.run_configuration.main_configuration",
    "multidriver_swg.run_configuration.path_resolver",
    "multidriver_swg.run_configuration.revision_gate",
    "multidriver_swg.run_configuration.selection_artifact",
    "multidriver_swg.workflow.graph",
    "multidriver_swg.workflow.run_start",
}


def _tree() -> ast.Module:
    """Return the module parsed as a syntax tree."""
    return ast.parse(SOURCE.read_text(encoding="utf-8"))


def _prepare_run_body(tree: ast.Module) -> str:
    """Return the source of `prepare_run`'s body, for the order check."""
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "prepare_run":
            return ast.get_source_segment(SOURCE.read_text(encoding="utf-8"), node) or ""
    raise AssertionError("prepare_run is not defined at module level")


def test_the_step_runs_its_calls_in_the_stated_order() -> None:
    """Property: `prepare_run` resolves the keys, computes the list, reads the declaration and runs the gate, reads the selection's form, and writes the record, in that order.

    Class: the textual order of those five calls inside the entry function.
    Why it is required: each call's repair differs, and an operator told about a dirty checkout while the
    real blocker is an unbound key repairs the wrong file. The list must also be computed once BEFORE the
    record, so the record and the engine cannot disagree.
    Killed by: moving the record write before the gate, or validating the selection before the resolver.
    Second correct implementation: the same calls through a list of deferred steps; the order still decides.
    """
    body = _prepare_run_body(_tree())

    positions = [body.index(call) for call in EXPECTED_CALL_ORDER]
    assert positions == sorted(positions)


def test_every_refusal_is_the_steps_one_class() -> None:
    """Property: every `raise` in the module raises `PreRunError`.

    Class: every raise statement in the source.
    Why it is required: the command maps this step's refusals to one exit band; a second class escaping the
    module would be caught by nothing and reach the operator as a traceback.
    Killed by: raising a stdlib error or a subsystem's own class from a new failure path.
    Second correct implementation: any translation that ends at `PreRunError`; the check reads the class
    raised, not how it was chosen.
    """
    raised: list[str] = []
    for node in ast.walk(_tree()):
        if not isinstance(node, ast.Raise) or node.exc is None:
            continue
        exc = node.exc
        if isinstance(exc, ast.Call):
            exc = exc.func
        raised.append(exc.id if isinstance(exc, ast.Name) else ast.dump(exc))

    assert raised, "the module raises nothing, so the class check would pass vacuously"
    assert set(raised) == {"PreRunError"}


def test_the_step_writes_one_file_and_never_opens_another() -> None:
    """Property: the module's only write call is the record's `write`, and no `open`, `write_text` or `write_bytes` appears.

    Class: every call node whose attribute names a write, over the module source.
    Why it is required: the step's contract is to produce the record; a second file it wrote would be an
    output the command's report and the run's provenance do not account for.
    Killed by: writing a marker or a copy beside the record.
    Second correct implementation: the same record through a helper that writes it; the single write call
    requirement is what the check reads.
    """
    writes = [
        node.func.attr
        for node in ast.walk(_tree())
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in {"write", "write_text", "write_bytes"}
    ]
    opens = [
        node.func.id
        for node in ast.walk(_tree())
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "open"
    ]

    assert writes == ["write"]
    assert opens == []


def test_the_step_imports_the_other_systems_mechanisms_it_exists_to_run() -> None:
    """Property: the module imports the configuration, resolver, revision gate, selection reader, graph and record modules.

    Class: every import statement in the source.
    Why it is required: the step's whole purpose is to run those mechanisms before an engine starts; a
    reimplementation would drift from the modules the parse-time gate also runs, and a missing import
    would show only at the first refusal's message.
    Killed by: inlining a key lookup, a commit comparison or a selection parse.
    Second correct implementation: imports at any position; the set of modules is what decides.
    """
    imported = {
        node.module
        for node in ast.walk(_tree())
        if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("multidriver_swg")
    }

    assert imported >= REQUIRED_IMPORTS


def test_the_step_takes_no_machine_state_and_prompts_for_nothing() -> None:
    """Property: `prepare_run`'s parameters are the configuration and its path, and the module reads no environment, working directory or home directory and calls no prompt or sleep.

    Class: the entry function's signature and every call node in the module.
    Why it is required: the record must be a function of the configuration and the estate alone; a path
    taken from the process's working directory or an environment variable would make the record describe a
    machine the run's configuration never named. Prompting or sleeping would block a command.
    Killed by: `Path.cwd()` for the run output, `os.environ` for a path, or an `input()` prompt.
    Second correct implementation: the same two parameters and the resolver for every path; the checks are
    structural and admit any such implementation.
    """
    parameters = inspect.signature(pre_run.prepare_run).parameters
    assert tuple(parameters) == ("configuration", "configuration_path")

    source = SOURCE.read_text(encoding="utf-8")
    for forbidden in ("os.environ", "getcwd", "cwd()", "expanduser", "home()"):
        assert forbidden not in source

    calls: set[str] = set()
    for node in ast.walk(_tree()):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name):
            calls.add(node.func.id)
        elif isinstance(node.func, ast.Attribute):
            calls.add(node.func.attr)
    assert calls & FORBIDDEN_CALLS == set()
    assert not any(isinstance(node, ast.While) for node in ast.walk(_tree()))


def _bind_all_three_keys(estate: Estate, selection: Path) -> None:
    """Rewrite the estate's binding document so it binds the three keys the step resolves, the selection at `selection`."""
    estate.bindings.write_text(
        'rule = "path-bindings:1"\n\n[paths]\n'
        f'"smoke.run.project_directory" = "{estate.project_directory}"\n'
        f'"smoke.run.run_output" = "{estate.run_output}"\n'
        f'"smoke.run.selection" = "{selection}"\n',
        encoding="utf-8",
    )


def test_a_selection_path_the_filesystem_cannot_inspect_never_leaves_the_step_as_an_interpreter_error(
    toolkit_copy: ToolkitCopy, estate: Estate, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Property: when the bound selection path cannot be inspected, the step either refuses with `PreRunError` or proceeds with an absent selection; it never raises another exception class.

    Shape row: DR-7 — the target list branches on "whether the selection file exists"; the step's own
    contract, "every refusal is a `PreRunError` naming the file or field its repair acts on", because the
    command maps that one class to its refusal band.
    Class: a bound selection path whose final component is longer than the filesystem's name limit, a state
    in which no file can exist at that name and in which `stat` fails with an error that is not "not found".
    Why it is required: the step decides presence with `Path.is_file()`, which swallows "not found" and a few
    other errors but raises the rest. The error escapes `prepare_run` before the declaration is read, so the
    command meets a bare `OSError` that is neither the refusal class it maps nor the success it would map,
    and the operator is told a byte count and a path buried in a traceback.
    Killed by: deciding presence with `Path.is_file()` and no translation of the error it can raise.
    Second correct implementation: translating the `OSError` into `PreRunError` naming the path, or treating
    an uninspectable name as absent after a `stat` that catches it; the test accepts both outcomes.
    """
    _bind_all_three_keys(estate, estate.project_directory / ("a" * 300))
    monkeypatch.setattr(pre_run, "running_package_directory", lambda: toolkit_copy.package)

    try:
        prepare_run(MainConfiguration.load(estate.configuration), estate.configuration)
    except PreRunError as refusal:
        assert "a" * 300 in str(refusal)


def test_the_record_the_step_writes_for_an_absent_selection_is_accepted_at_parse_and_builds_exactly_its_targets(
    tmp_path: Path, toolkit_copy: ToolkitCopy, estate: Estate, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Property: the engine started over an absent-selection record builds exactly its compute targets and its closing record directory.

    Shape row: DR-4 — the record's form is `run_start.py`'s and the graph-start gate reads it at every parse;
    DR-7 — absent selection, judgement kept, builds "rows 1 to 6" plus `method_attribution` and ends.
    Class: one estate whose binding document binds all three keys and whose selection file does not exist,
    over a committed copy of the toolkit, with the record produced by the step and not by hand.
    Why it is required: the step and the parse are written by different chunks, and a record the gate refuses
    (a field spelled differently, a target the table does not declare) or a target list the engine cannot
    satisfy would pass each chunk's own tests and stop every real run. This test is GREEN at the reviewed
    commit; it is a held test and must remain green.
    Killed by: a `prepare_run` that writes a record whose `judgement`, `selection` or `targets` the gate or
    the engine refuses, or one whose targets name a rule the cut should have removed.
    Second correct implementation: any step that writes the same record by another route; the engine's own
    exit status and the set of built rules decide.
    """
    _bind_all_three_keys(estate, estate.project_directory / "selection.toml")
    monkeypatch.setattr(pre_run, "running_package_directory", lambda: toolkit_copy.package)
    record = prepare_run(MainConfiguration.load(estate.configuration), estate.configuration)

    completed = run_engine(toolkit_copy, estate, cache_root=tmp_path / "cache")

    assert completed.returncode == 0, completed.stderr
    built = {path.name for path in estate.run_output.iterdir() if path.is_dir() and (path / IDENTITY_FILE).is_file()}
    assert built == {target.split("/", 1)[0] for target in record.targets if target != END_OF_RUN_RECORD}
    assert (estate.run_output / END_OF_RUN_RECORD).is_dir()
    assert len(built) == 7
