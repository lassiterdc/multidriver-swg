"""Tests of the release lock renderer, the two renderings it produces, and the committed lock.

The lock is two renderings of one resolution -- `requirements.txt` for an installer and `pylock.toml`
for the PEP 751 reader -- and the property that makes a comparison possible is that a rendering
depends on the declaration, the cutoff and the tested stack and on nothing about the invocation: two
directories, two constraint-file paths, and the same bytes. The first test below is that property,
measured the way `--check` measures it, because a renderer that writes the invocation's paths into
its output can never compare and would pass every other test here.

The remaining tests run the renderer as a subprocess in a synthetic root holding only what it reads
(`pyproject.toml` and the script), so each is a fact about the script rather than about this
checkout: `--write` produces both files, `--check` conforms on a fresh rendering, a changed or
missing committed file is named and exits 1, and each usage or environment error exits 2 under its
own refusal id. The last test reads this checkout: the committed lock must equal a fresh rendering at
the cutoff the declaration states, which is the arm that reds when the declaration moves and the lock
is not regenerated.

The renders reach an index (and uv's cache), so these tests are the environment's: they run in the
suite environment DR-1's `test` group declares, which carries `uv`.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "render_lock.py"
PYPROJECT = REPO_ROOT / "pyproject.toml"
LOCK_FILES = ("pylock.toml", "requirements.txt")
CUTOFF_TABLE = "[tool.uv.pip]"
TEST_GROUP_TABLE = "[dependency-groups]"


def _declaration() -> dict[str, object]:
    """The parsed declaration this checkout renders from."""
    return tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))


def _root(directory: Path) -> Path:
    """A synthetic repository root holding only what the renderer reads: the declaration and itself."""
    (directory / "scripts").mkdir(parents=True, exist_ok=True)
    shutil.copy2(PYPROJECT, directory / "pyproject.toml")
    shutil.copy2(SCRIPT, directory / "scripts" / "render_lock.py")
    return directory


def _copy_root(source: Path, directory: Path) -> Path:
    """A copy of a rendered root, so a test that damages one file leaves the fixture's files intact."""
    shutil.copytree(source, directory, dirs_exist_ok=True)
    return directory


def _without(text: str, header: str) -> str:
    """`text` with one table -- its header and its keys, up to the next table -- removed."""
    kept: list[str] = []
    skipping = False
    for line in text.splitlines():
        if line.strip() == header:
            skipping = True
            continue
        if skipping and line.startswith("["):
            skipping = False
        if not skipping:
            kept.append(line)
    return "\n".join(kept) + "\n"


def _run(root: Path, *arguments: str, environment: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    """One invocation of the renderer, in the form the release step gives it."""
    return subprocess.run(
        [sys.executable, str(root / "scripts" / "render_lock.py"), *arguments],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
        env=os.environ if environment is None else environment,
    )


@pytest.fixture(scope="module")
def rendered(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """One synthetic root rendered by `--write`, shared by every comparison test."""
    root = _root(tmp_path_factory.mktemp("rendered"))
    result = _run(root, "--write")
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    return root


def test_two_directories_render_byte_identical_lock_files(tmp_path):
    """The rendering carries nothing of the invocation, so two roots produce the same bytes.

    Class: NEW CAPABILITY -- a renderer that prints the header or the `-c {path}` annotation renders
    two different files from two directories, and `--check` can then never pass. Red under it,
    because the bytes differ at line 2.
    """
    first = _root(tmp_path / "first")
    second = _root(tmp_path / "second")
    for root in (first, second):
        result = _run(root, "--write")
        assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"

    for name in LOCK_FILES:
        assert (first / name).read_bytes() == (second / name).read_bytes(), name

    assert "numpy==2.4.6 \\" in (first / LOCK_FILES[1]).read_text(encoding="utf-8")
    pylock = (first / LOCK_FILES[0]).read_text(encoding="utf-8")
    assert pylock.startswith('lock-version = "1.0"'), pylock.splitlines()[:1]
    assert 'requires-python = ">=3.11"' in pylock


def test_a_fresh_rendering_conforms(rendered):
    """A root whose committed files are its own rendering passes `--check`."""
    result = _run(rendered, "--check")
    assert result.returncode == 0, result.stdout
    assert "release lock: conforms" in result.stdout


@pytest.mark.parametrize("name", LOCK_FILES)
def test_a_committed_file_that_differs_is_named(rendered, tmp_path, name):
    """One byte added to one committed file is a refusal that names that file and only that file."""
    root = _copy_root(rendered, tmp_path / "damaged")
    with (root / name).open("a", encoding="utf-8") as handle:
        handle.write("# a byte the rendering did not write\n")

    result = _run(root, "--check")
    assert result.returncode == 1, result.stdout
    cutoff = _declaration()["tool"]["uv"]["pip"]["exclude-newer"]  # type: ignore[index]
    expected = f"differs: {name}: a fresh rendering at cutoff {cutoff} differs from the committed file"
    difference = [line for line in result.stdout.splitlines() if line.startswith("differs:")]
    assert difference == [expected], difference
    assert "release lock: 1 file(s) differ" in result.stdout


def test_a_committed_file_that_is_missing_is_named(rendered, tmp_path):
    """A missing committed file is a difference, not a crash: the release step must fail and say which."""
    root = _copy_root(rendered, tmp_path / "missing")
    (root / LOCK_FILES[1]).unlink()

    result = _run(root, "--check")
    assert result.returncode == 1, result.stdout
    assert f"differs: {LOCK_FILES[1]}: {root / LOCK_FILES[1]} is missing" in result.stdout


def test_a_declaration_without_the_cutoff_is_refused(tmp_path):
    """No cutoff means a resolution at the moment of the run, which is the drift the lock stops."""
    root = _root(tmp_path / "no-cutoff")
    declaration = (root / "pyproject.toml").read_text(encoding="utf-8")
    (root / "pyproject.toml").write_text(_without(declaration, CUTOFF_TABLE), encoding="utf-8")

    result = _run(root, "--check")
    assert result.returncode == 2, result.stdout
    assert "refused: no-cutoff:" in result.stdout


def test_a_declaration_without_the_test_group_is_refused(tmp_path):
    """No tested stack means the newest satisfying version, not the one the suite exercised."""
    root = _root(tmp_path / "no-group")
    declaration = (root / "pyproject.toml").read_text(encoding="utf-8")
    (root / "pyproject.toml").write_text(_without(declaration, TEST_GROUP_TABLE), encoding="utf-8")

    result = _run(root, "--check")
    assert result.returncode == 2, result.stdout
    assert "refused: no-test-group:" in result.stdout


@pytest.mark.parametrize("arguments", [(), ("--write", "--check")])
def test_a_missing_or_doubled_mode_is_a_usage_error(rendered, arguments):
    """Exactly one mode is required, and argparse's usage status is the script's usage status."""
    result = _run(rendered, *arguments)
    assert result.returncode == 2, result.stdout


def test_uv_off_the_path_is_refused(rendered):
    """The renderer is uv; without it the script reports the fact and renders nothing."""
    environment = {**os.environ, "PATH": ""}
    result = _run(rendered, "--check", environment=environment)
    assert result.returncode == 2, result.stdout
    assert "refused: uv-missing:" in result.stdout


def test_the_committed_lock_is_a_fresh_rendering_at_the_declared_cutoff():
    """This checkout's committed lock must equal a rendering now, or the declaration moved alone."""
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--check"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout
    assert "release lock: conforms" in result.stdout
