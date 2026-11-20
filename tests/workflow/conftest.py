"""Fixtures for the workflow tests: a committed copy of the toolkit, a temporary estate, and an engine runner.

The graph-start gate reads the checkout that HOLDS the running package, so a test that ran the engine from
this working tree would be gated by this tree's own commit and its uncommitted files. The `toolkit_copy`
fixture builds the tree the gate is given instead: the working `src/multidriver_swg` and `.gitignore`
copied into a fresh repository and committed, so the running package is a clean checkout at a commit the
test can declare. Each refusal is then reached by changing exactly one fact of that copy or of the estate.

`estate` builds the temporary estate the record's configuration resolves through: a binding document, the
project directory with its revision declaration, the main configuration, and the run output directory.

Every engine run sets `XDG_CACHE_HOME` to a directory under the test's own temporary tree, because a plain
run writes a source cache under the user's cache directory and no test may write a shared one.

The helpers below are imported by the tests as `tests.workflow.conftest`; they are not fixtures because a
record is a value a test varies per call rather than a resource with a lifetime.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, replace
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

SOURCE_TREE = REPO_ROOT / "src" / "multidriver_swg"

PROJECT = "smoke.run"

RUN_START_FILE = "run_start.json"


@dataclass(frozen=True)
class ToolkitCopy:
    """A committed copy of the toolkit: its repository root, the package directory and the workflow file."""

    root: Path
    package: Path
    snakefile: Path
    commit: str


@dataclass(frozen=True)
class Estate:
    """A temporary estate: its root, the project directory, the configuration and the run output."""

    root: Path
    project_directory: Path
    configuration: Path
    bindings: Path
    run_output: Path


def _git(repository: Path, *arguments: str) -> str:
    """Run one git command in `repository` with a throwaway identity and return its trimmed standard output."""
    completed = subprocess.run(
        [
            "git",
            "-c",
            "user.name=workflow-test",
            "-c",
            "user.email=workflow-test@example.invalid",
            "-c",
            "commit.gpgsign=false",
            *arguments,
        ],
        cwd=repository,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    return completed.stdout.strip()


def committed_copy(destination: Path) -> ToolkitCopy:
    """Copy the working package and `.gitignore` into `destination`, commit them, and return the copy.

    The copy ignores `__pycache__` and `*.pyc`: they are compiled artifacts of the working tree, and a
    committed bytecode file would be an untracked-file refusal waiting to happen.
    """
    package = destination / "src" / "multidriver_swg"
    package.parent.mkdir(parents=True)
    shutil.copytree(SOURCE_TREE, package, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
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


def write_record(
    estate: Estate,
    targets: list[str] | tuple[str, ...],
    *,
    judgement: str = "present",
    selection: str | None = None,
    configuration: Path | None = None,
    allow_mixed_versions: bool = False,
) -> Path:
    """Write a conforming run-start record into the estate's run output and return its path."""
    document = {
        "format": "run-start:2",
        "main_configuration": str(configuration or estate.configuration),
        "targets": list(targets),
        "selection": selection,
        "judgement": judgement,
        "allow_mixed_versions": allow_mixed_versions,
    }
    path = estate.run_output / RUN_START_FILE
    path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    return path


def commit_all(repository: Path, message: str) -> str:
    """Commit every change in `repository` and return the new commit id."""
    _git(repository, "add", "-A")
    _git(repository, "commit", "-q", "-m", message)
    return _git(repository, "rev-parse", "HEAD")


def advance_commit(toolkit: ToolkitCopy, estate: Estate) -> ToolkitCopy:
    """Commit a change the engine does not see in any rule's text, and make the estate declare the new commit.

    The change is a comment line at the end of `stubs.py`, a callee of every rule body: the engine's code
    trigger reads the rendered `run:` text, so no rule is rebuilt by the new commit alone, while every
    identity a rule persists afterwards carries the new commit.
    """
    stubs_file = toolkit.package / "workflow" / "stubs.py"
    stubs_file.write_text(
        stubs_file.read_text(encoding="utf-8") + f"\n# a later commit, made after {toolkit.commit}\n",
        encoding="utf-8",
    )
    commit = commit_all(toolkit.root, "a later commit")
    (estate.project_directory / "toolkit_revision.toml").write_text(
        f'rule = "toolkit-revision:1"\ncommit = "{commit}"\n', encoding="utf-8"
    )
    return replace(toolkit, commit=commit)


def bind_three_keys(estate: Estate, selection: Path) -> None:
    """Rewrite the estate's binding document so it binds the three keys the pre-run step resolves."""
    estate.bindings.write_text(
        'rule = "path-bindings:1"\n\n[paths]\n'
        f'"{PROJECT}.project_directory" = "{estate.project_directory}"\n'
        f'"{PROJECT}.run_output" = "{estate.run_output}"\n'
        f'"{PROJECT}.selection" = "{selection}"\n',
        encoding="utf-8",
    )


def write_version_stacking(
    estate: Estate, *, rerun_rules: tuple[str, ...] = (), allow_mixed_versions: bool = False
) -> None:
    """Replace the configuration's `[version_stacking]` table with one holding the given values."""
    text = estate.configuration.read_text(encoding="utf-8")
    text = text.split("\n[version_stacking]", 1)[0].rstrip("\n") + "\n"
    text += (
        "\n[version_stacking]\n"
        f"rerun_rules = {json.dumps(list(rerun_rules))}\n"
        f"allow_mixed_versions = {'true' if allow_mixed_versions else 'false'}\n"
    )
    estate.configuration.write_text(text, encoding="utf-8")


def run_engine(
    toolkit: ToolkitCopy,
    estate: Estate,
    *,
    cache_root: Path,
    extra_arguments: tuple[str, ...] = (),
    extra_environment: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run the engine over `toolkit`'s workflow file with the estate's output as its working directory.

    `extra_arguments` are appended after the fixed options, so a caller can ask for an engine behaviour the
    command never passes — the report flags a report test needs — while every test keeps one invocation and
    the engine's option order stays the engine's business rather than each test's.
    """
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    environment["XDG_CACHE_HOME"] = str(cache_root)
    environment.update(extra_environment or {})
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "snakemake",
            "--snakefile",
            str(toolkit.snakefile),
            "--directory",
            str(estate.run_output),
            "--cores",
            "1",
            "--profile",
            "none",
            "--workflow-profile",
            "none",
            *extra_arguments,
        ],
        cwd=estate.run_output,
        env=environment,
        capture_output=True,
        text=True,
    )


@pytest.fixture
def toolkit_copy(tmp_path: Path) -> ToolkitCopy:
    """A committed copy of the working toolkit, so the gate sees a clean checkout."""
    return committed_copy(tmp_path / "toolkit")


@pytest.fixture
def estate(tmp_path: Path, toolkit_copy: ToolkitCopy) -> Estate:
    """A temporary estate whose project directory declares `toolkit_copy`'s commit."""
    root = tmp_path / "estate"
    project_directory = root / "projects" / "smoke"
    project_directory.mkdir(parents=True)
    (root / "config").mkdir(parents=True)

    bindings = root / "config" / "paths_test.toml"
    bindings.write_text(
        f'rule = "path-bindings:1"\n\n[paths]\n"{PROJECT}.project_directory" = "{project_directory}"\n',
        encoding="utf-8",
    )

    (project_directory / "toolkit_revision.toml").write_text(
        f'rule = "toolkit-revision:1"\ncommit = "{toolkit_copy.commit}"\n', encoding="utf-8"
    )

    configuration = root / "config" / "run.toml"
    configuration.write_text(
        'rule = "main-configuration:1"\n'
        f'estate_root = "{root}"\n'
        'bindings = "config/paths_test.toml"\n'
        f'project = "{PROJECT}"\n'
        'mode = "local"\n\n[local]\ncores = 1\n',
        encoding="utf-8",
    )

    run_output = tmp_path / "run"
    run_output.mkdir()

    return Estate(
        root=root,
        project_directory=project_directory,
        configuration=configuration,
        bindings=bindings,
        run_output=run_output,
    )


@pytest.fixture
def engine(tmp_path: Path):
    """Return the runner a test calls, with the engine's cache redirected under the test's tree."""

    def run(
        toolkit: ToolkitCopy,
        estate: Estate,
        *,
        extra_arguments: tuple[str, ...] = (),
        extra_environment: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        return run_engine(
            toolkit,
            estate,
            cache_root=tmp_path / "cache",
            extra_arguments=extra_arguments,
            extra_environment=extra_environment,
        )

    return run
