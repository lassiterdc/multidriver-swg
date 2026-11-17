"""Review tests for the workflow chunk: the one rendering loop, the source-root insert, the gate's order, and the engine's error class.

These checks cross the boundaries the end-to-end tests cannot see. A run can be green while the workflow
file has grown a second hand-written rule, while the source-root insert sits after the import it exists to
protect, while the body reads the loop's variable for a value it also happens to get right, or while the
gate's checks run in an order that reports a later defect first. Each check reads the file as text or the
gate as a call sequence, so it fails on those changes and not on the engine's own output.

They are deliberately not a second copy of the run's assertions: the end-to-end module owns what the engine
does, and a review test restating it would double the runtime without adding a check.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
from snakemake.exceptions import WorkflowError

import multidriver_swg.workflow.graph_start as graph_start
from multidriver_swg.run_configuration.provenance import read_identity
from multidriver_swg.workflow.graph import IDENTITY_FILE
from multidriver_swg.workflow.run_start import RUN_START_FILE
from tests.workflow.conftest import Estate, ToolkitCopy, committed_copy, run_engine, write_record

REPO_ROOT = Path(__file__).resolve().parents[2]
SNAKEFILE = REPO_ROOT / "src" / "multidriver_swg" / "workflow" / "Snakefile"
GRAPH_START = REPO_ROOT / "src" / "multidriver_swg" / "workflow" / "graph_start.py"
PYPROJECT = REPO_ROOT / "pyproject.toml"


def test_the_workflow_file_writes_one_rule_by_hand_and_reaches_it_first() -> None:
    """Property: the file declares exactly one hand-written rule (`all`), renders every other rule with one loop, and puts `all` before the loop.

    Class: the file's own text, read as lines.
    Why it is required: the table is the single statement of order and dependency, so a second hand-written
    rule would restate part of it; and with `all` after the loop the engine's default target is the first
    rendered rule, so a default invocation builds one step and exits 0.
    Killed by: adding a hand-written rule, splitting the loop in two, or moving `all` below the loop.
    Second correct implementation: a loop over a function that returns the rows; the counts and the order
    still decide.
    """
    lines = SNAKEFILE.read_text(encoding="utf-8").splitlines()

    rule_lines = [line for line in lines if line.strip().startswith("rule ")]
    assert rule_lines == ["rule all:"]
    assert sum(1 for line in lines if line.strip() == "for spec in GRAPH:") == 1
    assert lines.index("rule all:") < next(
        index for index, line in enumerate(lines) if line.strip() == "for spec in GRAPH:"
    )


def test_the_workflow_file_puts_this_checkouts_source_root_on_the_path_before_it_imports() -> None:
    """Property: the file derives its source root from `workflow.basedir`, inserts it at the head of `sys.path`, and does so before the first toolkit import.

    Class: the file's header lines.
    Why it is required: the engine and every job import the package, and an import that resolved an
    installed copy would gate and run a different checkout from the one the file belongs to.
    Killed by: importing the toolkit before the insert, appending instead of inserting first, or deriving
    the path from the process's working directory.
    Second correct implementation: an equivalent computation of the package's parent, with the insert still
    before the import; the order assertion is what decides.
    """
    lines = SNAKEFILE.read_text(encoding="utf-8").splitlines()

    insert = next(index for index, line in enumerate(lines) if "sys.path.insert(0, _SOURCE_ROOT)" in line)
    first_toolkit_import = next(index for index, line in enumerate(lines) if "from multidriver_swg" in line)
    assert "workflow.basedir" in SNAKEFILE.read_text(encoding="utf-8")
    assert insert < first_toolkit_import


def test_the_rendered_body_takes_the_system_from_params_and_never_from_the_loop_variable() -> None:
    """Property: the rendered run block reads `params.system` and `params.rule_name`, and mentions no `spec.` attribute.

    Class: the text after the file's single `run:` line.
    Why it is required: a rendered body runs after the rendering loop has finished, so a body reading the
    loop's row would read the LAST row for every job; the identity would then name one system twelve times
    while every output existed.
    Killed by: `write_stub_outputs(spec.name, spec.system, ...)` in the run block.
    Second correct implementation: a body reading a params mapping built per rule; the text check passes
    for any spelling that resolves through `params`.
    """
    text = SNAKEFILE.read_text(encoding="utf-8")
    body = text.split("run:", 1)[1]

    assert "params.rule_name" in body
    assert "params.system" in body
    assert "spec." not in body


def test_the_gate_reads_the_record_before_it_reaches_the_checkout_checks() -> None:
    """Property: `require_graph_start` calls the record read, the configuration read, the declaration read and the checkout check in that order.

    Class: the call order inside the gate's entry function, read from the source.
    Why it is required: each refusal names a different repair, and reporting a checkout problem to an
    operator whose real blocker is a missing record sends them to clean a tree that was not the problem.
    Killed by: reordering the four calls, or inlining a check before the record is read.
    Second correct implementation: a list of deferred checks run in order; the source order still matches.
    """
    text = GRAPH_START.read_text(encoding="utf-8")
    body = text.split("def require_graph_start(", 1)[1].split("\ndef ", 1)[0]

    positions = [
        body.index("_read_record("),
        body.index("_read_configuration("),
        body.index("_declared_revision("),
        body.index("_require_checkout("),
    ]
    assert positions == sorted(positions)


def test_the_gate_raises_the_engines_own_error_for_a_missing_record(tmp_path: Path) -> None:
    """Property: a direct call over a directory with no record raises the engine's `WorkflowError` naming the record file.

    Class: an empty temporary directory.
    Why it is required: a parse-time gate has one channel to the operator, the exception the engine turns
    into a non-zero exit; raising a local exception would exit with a traceback the engine does not
    translate into its own refusal.
    Killed by: raising `RunStartError` from the gate instead of the engine's class.
    Second correct implementation: any conversion that raises `WorkflowError` with the file named; the same
    class and text arrive.
    """
    with pytest.raises(WorkflowError) as caught:
        graph_start.require_graph_start(tmp_path)

    assert RUN_START_FILE in str(caught.value)


def test_the_package_data_names_the_workflow_file_so_a_wheel_carries_it() -> None:
    """Property: `pyproject.toml`'s package-data lists `workflow/Snakefile`, and the file exists in the package.

    Class: the pyproject's package-data table and the workflow directory.
    Why it is required: setuptools ships only Python modules by default, so a wheel built without the entry
    installs a package whose command points at a file that is not there.
    Killed by: removing the package-data entry or renaming the file.
    Second correct implementation: a `MANIFEST.in`-based inclusion; the entry requirement is what the
    check reads.
    """
    pyproject = PYPROJECT.read_text(encoding="utf-8")
    package_data = pyproject.split("[tool.setuptools.package-data]", 1)[1].split("\n[", 1)[0]

    assert "workflow/Snakefile" in package_data
    assert SNAKEFILE.is_file()


def test_importing_the_workflow_package_does_not_import_the_engine() -> None:
    """Property: importing `multidriver_swg.workflow` in a clean interpreter leaves `snakemake` unimported, while importing `graph_start` does import it.

    Class: two child imports over the package's `src`.
    Why it is required: the package is imported by tests and by the command's other modules, and pulling
    the engine in at package import would make every consumer depend on it; the engine belongs to the two
    modules that run inside a parse.
    Killed by: importing `graph_start` from `multidriver_swg.workflow.__init__`.
    Second correct implementation: a lazy import inside `require_graph_start`; the first child still leaves
    the engine unimported, and the second loads it when the gate runs.
    """
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(REPO_ROOT / "src")
    probe = "import sys, multidriver_swg.workflow; print('snakemake' in sys.modules)"

    package_only = subprocess.run(
        [sys.executable, "-c", probe], env=environment, capture_output=True, text=True, check=True
    )
    with_engine = subprocess.run(
        [sys.executable, "-c", "import sys, multidriver_swg.workflow.graph_start; print('snakemake' in sys.modules)"],
        env=environment,
        capture_output=True,
        text=True,
        check=True,
    )

    assert package_only.stdout.strip() == "False"
    assert with_engine.stdout.strip() == "True"


_FIRST_TARGET = ("observed_record_assembly/observed_record.json",)


def _break_bindings_document_missing(estate: Estate) -> Path:
    """Delete the binding document the configuration names and return the path the refusal must name."""
    estate.bindings.unlink()
    return estate.bindings


def _break_bindings_document_not_utf8(estate: Estate) -> Path:
    """Overwrite the binding document with bytes that are not UTF-8 and return it."""
    estate.bindings.write_bytes(b"rule = \xff\xfe\n")
    return estate.bindings


def _break_configuration_not_utf8(estate: Estate) -> Path:
    """Overwrite the main configuration with bytes that are not UTF-8 and return it."""
    estate.configuration.write_bytes(b"rule = \xff\xfe\n")
    return estate.configuration


def _break_declaration_not_utf8(estate: Estate) -> Path:
    """Overwrite the project's revision declaration with bytes that are not UTF-8 and return it."""
    declaration = estate.project_directory / "toolkit_revision.toml"
    declaration.write_bytes(b"rule = \xff\xfe\n")
    return declaration


@pytest.mark.parametrize(
    "break_link",
    [
        _break_bindings_document_missing,
        _break_bindings_document_not_utf8,
        _break_configuration_not_utf8,
        _break_declaration_not_utf8,
    ],
)
def test_every_unreadable_link_between_the_record_and_the_declaration_is_refused_as_the_engines_error(
    toolkit_copy: ToolkitCopy, estate: Estate, break_link
) -> None:
    """Property: whichever file between the run-start record and the revision declaration cannot be read, the gate raises the engine's `WorkflowError` and the message names that file.

    Shape row: DR-4 — `graph_start.py` "runs at every parse of the workflow file ... and raises the engine's
    `WorkflowError`", with the four refusals in order; the gate's own docstring states that "every refusal
    this gate meets is re-raised under that class".
    Class: the links of the chain the gate walks (configuration, binding document, declaration), each broken
    in the ways an operator's file reaches the gate: absent, and saved in an encoding that is not UTF-8.
    Why it is required: the binding document's loader does not translate a missing file or a decoding
    failure into the resolver's refusal, so the gate's `except PathResolutionError` misses both and the
    parse ends in the interpreter's `FileNotFoundError` or `UnicodeDecodeError` from inside the engine's
    include, which names no step of the gate and is not the refusal the shape says an unprepared parse
    receives.
    Killed by: catching only `PathResolutionError` around the binding document's read.
    Second correct implementation: catching `OSError` and `UnicodeDecodeError` beside it, or reading the
    document through a helper that already translates both; each case raises `WorkflowError` naming the file.
    """
    write_record(estate, _FIRST_TARGET)
    broken = break_link(estate)

    with pytest.raises(WorkflowError) as caught:
        graph_start.require_graph_start(estate.run_output)

    assert broken.name in str(caught.value)


def test_a_toolkit_copy_earlier_on_the_import_path_does_not_replace_the_workflow_files_own(
    tmp_path: Path, toolkit_copy: ToolkitCopy, estate: Estate
) -> None:
    """Property: with a second committed copy of the toolkit ahead of the workflow file's own on the import path, the run still checks and executes the copy the workflow file belongs to.

    Shape row: DR-4 — the workflow file "inserts its own package's source root ... at the head of `sys.path`
    before importing the toolkit, so the engine, and every job that re-parses the file, checks and runs the
    toolkit the workflow file belongs to".
    Class: an import path holding two copies of the package, the other one first, the other one being a clean
    checkout at a commit the project does not declare.
    Why it is required: the insert is conditional on the source root being absent from the path, so a root
    already present but BEHIND another copy is left where it is. The first `multidriver_swg` found is then
    the other copy, the gate compares that copy's commit with the declaration, and the run is refused (or,
    for a copy at the declared commit, runs another copy's code under this workflow file) although the
    workflow file's own package is the declared one.
    Killed by: guarding the insert with `if root not in sys.path`.
    Second correct implementation: inserting the root at the head unconditionally, or removing an existing
    entry before inserting it; the engine exits zero and every identity records the workflow file's commit.
    """
    other = committed_copy(tmp_path / "other")
    (other.package / "other_copy_marker.py").write_text("MARKER = True\n", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=other.root, check=True, capture_output=True, text=True)
    subprocess.run(
        ["git", "-c", "user.name=review", "-c", "user.email=review@example.invalid", "commit", "-q", "-m", "other"],
        cwd=other.root,
        check=True,
        capture_output=True,
        text=True,
    )
    other_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=other.root, check=True, capture_output=True, text=True
    ).stdout.strip()
    assert other_commit != toolkit_copy.commit

    write_record(estate, _FIRST_TARGET)
    completed = run_engine(
        toolkit_copy,
        estate,
        cache_root=tmp_path / "cache",
        extra_environment={
            "PYTHONPATH": os.pathsep.join([str(other.root / "src"), str(toolkit_copy.root / "src")]),
        },
    )

    assert completed.returncode == 0, completed.stderr
    identity = read_identity(estate.run_output / "observed_record_assembly" / IDENTITY_FILE)
    assert identity.code_version == toolkit_copy.commit
