#!/usr/bin/env python3
"""Render the release lock, or compare the committed lock against a fresh rendering.

The lock is two renderings of ONE resolution: `requirements.txt`, a hash-pinned requirement list an
installer reads with `--require-hashes`, and `pylock.toml`, the PEP 751 form of the same resolution.
Both sit at the repository root and both are committed, so the bytes a release installs are the bytes
the release reviewed.

Each rendering is `uv pip compile` over the declaration in `pyproject.toml`. The resolution is not
the declaration alone, and the fixed inputs are what make it a fact about this project rather than a
fact about the day it ran:

- `--universal` with `--python-version 3.11`: one file covers every platform and every Python from
  the declaration's `requires-python` floor up. Without the explicit floor, a probe narrowed the
  lock's `requires-python` to the interpreter that rendered it.
- `--exclude-newer {cutoff}`, where the cutoff is `[tool.uv.pip].exclude-newer` read from
  `pyproject.toml` with `tomllib` and passed on the command line: a resolution is reproducible only
  at a fixed cutoff, and reading it from the declaration keeps one statement of it.
- `--constraint {file}` with the `test` group's pins: the runtime resolution is then the stack the
  suite tested. `numpy>=2,<3` resolves to the `numpy` the reference values were computed under, so
  the lock and the suite's red do not describe two different environments.
- `--no-header --no-annotate`: the invocation's own paths must not reach the bytes. With them, two
  renderings made from two directories, with constraint files at two paths, are byte-identical;
  without them they differ in the header and in a `-c {path}` annotation on every run, and `--check`
  could never pass. The constraint file is written into a temporary directory for the same reason:
  no rendering depends on where this invocation happened to put it.

Both modes render BOTH files into a fresh temporary directory, and the committed pair is never a
renderer's `-o` target. That is load-bearing twice over. `uv pip compile` prefers the pins already in
an existing `-o` file, so a `--write` rendering over the committed lock would keep the versions that
lock pins while `--check`, rendering into an empty directory, took the newest versions at the cutoff:
the pair a release had just regenerated would fail the very next comparison. And a rendering that
fails part way leaves the incomplete pair in the temporary directory, never in the root.

`--write` copies the finished pair into the repository root, and only after BOTH renderings
succeeded, so a failed resolution leaves the committed pair exactly as it was rather than a new
`pylock.toml` beside a stale `requirements.txt`. `--check` compares the committed pair against the
fresh rendering byte for byte, so a committed lock that no longer matches a rendering at the same
cutoff is a refusal rather than a silent drift between the declaration and what installs it.

The output is one `wrote: {name}` line per file `--write` replaced, or one `differs: {name}: {detail}`
line per file `--check` found stale, and a closing line that reads `release lock: {n} file(s)
written`, `release lock: conforms`, or `release lock: {n} file(s) differ`. The exit status is 0 when
the lock was written or conforms, 1 when a committed file differs, and 2 on a usage or environment
error, each as one `refused: {id}: {detail}` line, the closed set being:

- `pyproject-unreadable`: `pyproject.toml` cannot be read or is not TOML, so there is no declaration
  to render and no cutoff to read.
- `no-cutoff`: `[tool.uv.pip].exclude-newer` is absent or is not a string. A lock rendered without a
  cutoff resolves at the moment of the run, which is the drift the lock exists to stop.
- `no-test-group`: `[dependency-groups].test` is absent or holds no requirement, so the constraint
  the tested stack supplies is missing and the runtime resolution would be the newest satisfying
  version instead of the one the suite exercised.
- `uv-missing`: `uv` is not on `PATH`; the renderer is uv and this script adds no resolution of its
  own.
- `uv-failed`: `uv pip compile` exited non-zero, and the detail names the invocation and its last
  line of output, so a yanked version or an unreachable index reads as itself.

Not established: that the resolved versions are ones the code runs correctly under. This script
records the resolution the declaration, the cutoff and the tested stack produce; whether that stack
passes the suite is the suite's answer.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

# The repository root this script renders into, resolved from its own location so an invocation from
# any working directory renders the project it belongs to, and a copied checkout renders its own copy.
ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = "pyproject.toml"
# The two renderings, in the order a reader meets them: the PEP 751 lock and the hash-pinned list.
LOCK_FILES = ("pylock.toml", "requirements.txt")
PEP751_FORMAT = "pylock.toml"
CONSTRAINT_FILE = "constraints.txt"
# Both modes render into this directory under the invocation's temporary directory, never into the
# root: an `-o` target that already holds a lock biases uv's resolution toward that lock's pins.
RENDERED_DIRECTORY = "rendered"
CUTOFF_PATH = ("tool", "uv", "pip", "exclude-newer")
TEST_GROUP_PATH = ("dependency-groups", "test")
UV = "uv"
PYTHON_VERSION = "3.11"
EXIT_USAGE = 2


def _last_line(result: subprocess.CompletedProcess) -> str:
    """The last non-blank line uv printed, whichever stream carried it."""
    for stream in (result.stderr, result.stdout):
        lines = [line.strip() for line in (stream or "").splitlines() if line.strip()]
        if lines:
            return lines[-1]
    return f"exit status {result.returncode} with no output"


def _nested(data: object, keys: tuple[str, ...]) -> object:
    """The value at a path of nested tables, or None when the path is absent or not a table."""
    for key in keys:
        if not isinstance(data, dict) or key not in data:
            return None
        data = data[key]
    return data


def _read_pyproject(path: Path) -> tuple[dict[str, object] | None, str]:
    """The parsed declaration, and why it could not be read when it is None."""
    try:
        return tomllib.loads(path.read_text(encoding="utf-8")), ""
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        return None, f"{path} cannot be read as TOML ({type(error).__name__}: {error})"


def _cutoff(data: dict[str, object]) -> tuple[str | None, str]:
    """`[tool.uv.pip].exclude-newer`, and why it cannot be used when it is None."""
    value = _nested(data, CUTOFF_PATH)
    if not isinstance(value, str) or not value.strip():
        return None, (
            f"{'.'.join(CUTOFF_PATH)} is absent from {PYPROJECT} or is not a non-empty string; "
            "state the cutoff the lock renders at"
        )
    return value.strip(), ""


def _pins(data: dict[str, object]) -> list[str]:
    """The `test` group's members, which are the constraints the runtime resolution is rendered under."""
    value = _nested(data, TEST_GROUP_PATH)
    if not isinstance(value, list):
        return []
    return [pin.strip() for pin in value if isinstance(pin, str) and pin.strip()]


def _constraints(pins: list[str], directory: Path) -> Path:
    """Write the pins as a constraint file inside `directory`, and return its path.

    The path is a temporary directory's in every mode, so no rendering depends on it: uv prints a
    constraint's path in the header and in a `-c {path}` annotation, which `--no-header
    --no-annotate` strip, and a rendering made from another directory is then byte-identical.
    """
    path = directory / CONSTRAINT_FILE
    path.write_text("\n".join(pins) + "\n", encoding="utf-8")
    return path


def _compile(cutoff: str, constraints: Path, target: Path) -> tuple[bool, str]:
    """Render both files into `target`; the second member says what failed when the first is False."""
    common = [
        UV,
        "pip",
        "compile",
        PYPROJECT,
        "--universal",
        "--python-version",
        PYTHON_VERSION,
        "--generate-hashes",
        "--no-header",
        "--no-annotate",
        "--exclude-newer",
        cutoff,
        "--constraint",
        str(constraints),
    ]
    for name, extra in ((LOCK_FILES[0], ["--format", PEP751_FORMAT]), (LOCK_FILES[1], [])):
        command = [*common, *extra, "-o", str(target / name)]
        result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            return False, f"`{' '.join(command)}` exited {result.returncode}: {_last_line(result)}"
    return True, ""


def _differences(root: Path, rendered: Path, cutoff: str) -> list[str]:
    """One `differs` line per committed file a fresh rendering at `cutoff` did not reproduce."""
    lines: list[str] = []
    for name in LOCK_FILES:
        committed = root / name
        if not committed.is_file():
            lines.append(f"differs: {name}: {committed} is missing and a fresh rendering produced it")
            continue
        fresh = (rendered / name).read_bytes()
        if committed.read_bytes() != fresh:
            lines.append(f"differs: {name}: a fresh rendering at cutoff {cutoff} differs from the committed file")
    return lines


def _parse_arguments(argv: list[str] | None) -> argparse.Namespace:
    """The one mode: render and replace the root's pair, or compare the committed files against a rendering."""
    parser = argparse.ArgumentParser(
        description="Render the release lock, or compare the committed lock against a fresh rendering."
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--write",
        action="store_true",
        help="render both lock files and copy the pair into the repository root once both render",
    )
    mode.add_argument(
        "--check",
        action="store_true",
        help="render into a temporary directory and exit 1 naming each committed file that differs",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Render, write or compare, and return the exit status."""
    arguments = _parse_arguments(argv)
    if shutil.which(UV) is None:
        print(f"refused: uv-missing: {UV} is not on PATH; install uv {UV} and render the lock again")
        return EXIT_USAGE
    data, problem = _read_pyproject(ROOT / PYPROJECT)
    if data is None:
        print(f"refused: pyproject-unreadable: {problem}")
        return EXIT_USAGE
    cutoff, problem = _cutoff(data)
    if cutoff is None:
        print(f"refused: no-cutoff: {problem}")
        return EXIT_USAGE
    pins = _pins(data)
    if not pins:
        print(
            f"refused: no-test-group: {'.'.join(TEST_GROUP_PATH)} is absent from {PYPROJECT} or holds no "
            "requirement; the tested stack is the constraint the runtime resolution is rendered under"
        )
        return EXIT_USAGE

    with tempfile.TemporaryDirectory(prefix="render-lock-") as work:
        constraints = _constraints(pins, Path(work))
        # Both modes render into a fresh directory under the invocation's temporary directory. The
        # committed pair is never an `-o` target: uv prefers the pins already in an existing `-o`
        # file, so rendering over the committed lock would keep its versions while `--check`
        # (rendering empty) took the newest at the cutoff, and the pair a release had just written
        # would fail the next comparison. It is also what lets `--write` copy only a COMPLETE pair.
        rendered = Path(work) / RENDERED_DIRECTORY
        rendered.mkdir()
        complete, problem = _compile(cutoff, constraints, rendered)
        if not complete:
            print(f"refused: uv-failed: {problem}")
            return EXIT_USAGE

        if arguments.write:
            # Both renderings succeeded, so the pair is copied whole: a failure above left the root
            # holding exactly the bytes it held before this invocation.
            for name in LOCK_FILES:
                shutil.copyfile(rendered / name, ROOT / name)
                print(f"wrote: {name}")
            print(f"release lock: {len(LOCK_FILES)} file(s) written")
            return 0

        differences = _differences(ROOT, rendered, cutoff)
        for line in differences:
            print(line)
        print(f"release lock: {len(differences)} file(s) differ" if differences else "release lock: conforms")
        return 1 if differences else 0


if __name__ == "__main__":
    sys.exit(main())
