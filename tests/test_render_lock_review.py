"""Reviewer tests of `scripts/render_lock.py`: what `--write` leaves behind and whether `--check` accepts it.

Both tests run the renderer as a subprocess in a synthetic root holding only the declaration and the
script (the idiom of `tests/test_render_lock.py`), and neither reaches an index: the first replaces
`uv` with a stub on `PATH`, the second renders from a local wheel directory (`UV_NO_INDEX`,
`UV_FIND_LINKS`), as `tests/test_check_install.py` does.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "render_lock.py"
LOCK_FILES = ("pylock.toml", "requirements.txt")

DECLARATION = """\
[project]
name = "probe"
version = "0.1"
requires-python = ">=3.11"
dependencies = ["dep"]

[dependency-groups]
test = ["other==1.0"]

[tool.uv.pip]
exclude-newer = "2026-11-16T00:00:00Z"
"""

# A stub `uv` whose Nth `pip compile` invocation fails: the first invocation is the `--format
# pylock.toml` rendering and the second is the `requirements.txt` rendering. Each successful
# invocation writes new bytes to its `-o` target and nothing else.
STUB = """\
#!/bin/sh
count_file="$STUB_COUNT"
n=$(cat "$count_file" 2>/dev/null || echo 0)
n=$((n + 1))
echo "$n" > "$count_file"
out=""; prev=""
for a in "$@"; do
  [ "$prev" = "-o" ] && out="$a"
  prev="$a"
done
if [ "$n" -eq "$STUB_FAIL_ON" ]; then
  echo "index unreachable" >&2
  exit 1
fi
echo "NEW RENDERING $n" > "$out"
exit 0
"""


def _root(directory: Path) -> Path:
    """A synthetic repository root holding only what the renderer reads: a declaration and itself."""
    (directory / "scripts").mkdir(parents=True)
    (directory / "pyproject.toml").write_text(DECLARATION, encoding="utf-8")
    shutil.copy2(SCRIPT, directory / "scripts" / "render_lock.py")
    return directory


def _wheel(directory: Path, version: str) -> None:
    """A minimal pure-Python wheel of the package `dep` at `version`, in a local wheel directory."""
    directory.mkdir(parents=True, exist_ok=True)
    name = f"dep-{version}-py3-none-any.whl"
    with zipfile.ZipFile(directory / name, "w") as archive:
        archive.writestr("dep/__init__.py", "")
        archive.writestr(f"dep-{version}.dist-info/METADATA", f"Metadata-Version: 2.1\nName: dep\nVersion: {version}\n")
        archive.writestr(
            f"dep-{version}.dist-info/WHEEL",
            "Wheel-Version: 1.0\nGenerator: review\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        )
        archive.writestr(f"dep-{version}.dist-info/RECORD", "")


def _run(root: Path, mode: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(root / "scripts" / "render_lock.py"), mode],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


@pytest.mark.skipif(sys.platform == "win32", reason="the stub `uv` is a POSIX shell script")
@pytest.mark.parametrize("fail_on", [1, 2], ids=["first-rendering-fails", "second-rendering-fails"])
def test_a_failed_rendering_leaves_both_committed_lock_files_as_they_were(tmp_path, fail_on):
    """Input class: any `--write` whose Nth `uv pip compile` fails (a flaky index between the two renders).

    The committed pair must stay a pair: whichever rendering fails, both files hold the bytes they held
    before the run and the exit status is 2. A renderer that writes the first rendering into the root
    before the second succeeds leaves a new `pylock.toml` beside an old `requirements.txt`, two files
    that no longer describe one resolution; that is the wrong implementation this reds under. The
    assertion turns on the files' bytes after a failure, a property of the old and the changed script
    alike, not on any message the script prints.

    Enforces the DR-7 sentence that the two files are renderings "of one resolution",
    and the module docstring's own claim that a failed resolution leaves `--write` having replaced
    nothing. Killed by: rendering both files into a temporary directory first and copying them into the
    root only when both succeeded. A second correct implementation, materially different: copying the
    committed pair aside and restoring it when a rendering fails.
    """
    root = _root(tmp_path / "root")
    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    stub = stub_dir / "uv"
    stub.write_text(STUB, encoding="utf-8")
    stub.chmod(stub.stat().st_mode | stat.S_IXUSR)
    for name in LOCK_FILES:
        (root / name).write_text(f"OLD {name}\n", encoding="utf-8")
    env = {
        **os.environ,
        "PATH": f"{stub_dir}{os.pathsep}{os.environ['PATH']}",
        "STUB_COUNT": str(tmp_path / "count"),
        "STUB_FAIL_ON": str(fail_on),
    }

    result = _run(root, "--write", env)

    assert result.returncode == 2, (result.stdout, result.stderr)
    assert "refused: uv-failed" in result.stdout
    after = {name: (root / name).read_text(encoding="utf-8") for name in LOCK_FILES}
    assert after == {name: f"OLD {name}\n" for name in LOCK_FILES}, after


@pytest.mark.skipif(shutil.which("uv") is None, reason="renders with uv, which the test group carries")
def test_write_over_a_stale_lock_renders_what_check_then_accepts(tmp_path):
    """Input class: a `--write` over a committed lock that pins an older version a newer one now supersedes.

    `uv pip compile -o {existing file}` prefers the pins already in that file, so a `--write` into a
    root that holds a lock keeps the old version, while `--check` renders into an empty directory and
    takes the newest version at the cutoff. The pair of commands the release procedure runs in order
    (regenerate, then compare) must agree: after `--write` the same declaration, cutoff and wheel
    directory must `--check` clean. Input: `dep` 1.0 pinned in the committed lock, `dep` 1.1 now in the
    wheel directory, no other change. A renderer whose `--write` target already holds the old pins
    reds here; the wrong implementation is rendering into the root over the existing files.

    Enforces DR-7's shape sentence: a committed `pylock.toml` that "differs from a fresh rendering at
    the same cutoff" is refused, which a lock `--write` produced must not be. Killed by: removing the
    existing outputs before rendering. A second correct implementation, materially different:
    rendering into a temporary directory and copying the pair into the root.
    """
    wheels = tmp_path / "wheels"
    _wheel(wheels, "1.0")
    root = _root(tmp_path / "root")
    env = {**os.environ, "UV_NO_INDEX": "1", "UV_FIND_LINKS": str(wheels)}
    first = _run(root, "--write", env)
    assert first.returncode == 0, (first.stdout, first.stderr)
    assert "dep==1.0 \\" in (root / "requirements.txt").read_text(encoding="utf-8")

    _wheel(wheels, "1.1")
    second = _run(root, "--write", env)
    assert second.returncode == 0, (second.stdout, second.stderr)
    check = _run(root, "--check", env)

    assert check.returncode == 0, (check.stdout, check.stderr)
