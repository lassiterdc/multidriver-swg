"""Tests of the sdist's manifest: the test tree stays out of the archive and both lock files ship.

An sdist is not `git archive`, so `.gitattributes`' `export-ignore` lines do not reach it: the `build`
job builds it from a checkout, and setuptools' own sdist defaults put the top-level `tests/test*.py`
files in the archive. `MANIFEST.in` is the statement that closes that gap, and this test measures the
archive rather than the manifest text, because the archive is what a release ships and what the
release-state comparison classifies.

The archive is built from a copy of this checkout's tracked and untracked-but-unignored files, in a
temporary directory, with `python -m build --sdist --no-isolation`, so nothing here writes into the
checkout and the backend is the one DR-1's `test` group pins (`setuptools`, `build`). The membership
assertions are read from the tar members with the archive's single top-level directory stripped, so
they are facts about the sdist's contents rather than about the version in its name.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
LOCK_FILES = ("pylock.toml", "requirements.txt")


def _tracked_files() -> list[Path]:
    """Every file a build of this checkout would see: tracked, plus untracked and not ignored."""
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=REPO_ROOT,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr.decode(errors="replace")
    names = result.stdout.decode("utf-8", errors="replace").split("\0")
    return [REPO_ROOT / name for name in names if name]


def _copy_checkout(destination: Path) -> None:
    """Copy every file a build would see into `destination`, preserving relative paths."""
    for source in _tracked_files():
        target = destination / source.relative_to(REPO_ROOT)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)


def _members(archive: Path) -> list[str]:
    """The archive's member names with its one top-level directory stripped."""
    with tarfile.open(archive, "r:gz") as handle:
        names = handle.getnames()
    return [name.split("/", 1)[1] if "/" in name else "" for name in names]


@pytest.fixture(scope="module")
def members(tmp_path_factory: pytest.TempPathFactory) -> list[str]:
    """The member names of the sdist this checkout builds."""
    source = tmp_path_factory.mktemp("checkout")
    _copy_checkout(source)
    output = tmp_path_factory.mktemp("dist")
    result = subprocess.run(
        [sys.executable, "-m", "build", "--sdist", "--no-isolation", "--outdir", str(output)],
        cwd=source,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    archives = sorted(output.glob("*.tar.gz"))
    assert len(archives) == 1, archives
    return _members(archives[0])


def test_no_member_of_the_sdist_is_a_test_file(members):
    """`prune tests` holds: no path under `tests/` is a member of the archive.

    Class: NEW CAPABILITY -- without the manifest line setuptools' defaults put the top-level
    `tests/test*.py` files in the sdist (18 of them at the run's base commit), so a release would
    ship a class `private-estate` declares excluded. Red under it, because those members are present.
    """
    under_tests = sorted(name for name in members if name == "tests" or name.startswith("tests/"))
    assert under_tests == [], under_tests[:20]


def test_both_lock_renderings_are_members_of_the_sdist(members):
    """`include pylock.toml requirements.txt` holds: an sdist carries the resolution it was built from."""
    present = set(members)
    missing = [name for name in LOCK_FILES if name not in present]
    assert missing == [], sorted(present)[:40]
