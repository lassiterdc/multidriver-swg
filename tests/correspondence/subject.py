"""Stop a test session whose imported `multidriver_swg` is not the copy in the checkout being tested.

The toolkit is installed in editable mode, so `import multidriver_swg` resolves to whichever
checkout the environment was installed from, not necessarily the one the tests were collected
from. Run from a second checkout, such as a worktree, without `PYTHONPATH` naming its `src`, the
equivalence tests would then compare the old code with the other checkout's new code and report
verdicts about code nobody in this checkout is changing. So at session start this module checks
that the imported package lies inside the collected checkout's root, and ends the session before
any test runs when it does not, or when the package cannot be imported at all.

The comparison is containment of resolved paths, never a string prefix: a checkout `…/proj` and
a sibling `…/proj2` share a prefix, so a prefix test would accept the sibling's package.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

PACKAGE = "multidriver_swg"


def _inside(package: Path, root: Path) -> bool:
    """Whether the resolved package file lies under the resolved root."""
    return package.is_relative_to(root)


def subject_findings(package_file, rootdir) -> list[str]:
    """Name the package file and the checkout root when the first does not lie inside the second, or is absent."""
    root = Path(rootdir).resolve()
    if package_file is None:
        return [f"the package {PACKAGE} cannot be imported, so the tests in {root} have no code to test"]
    package = Path(package_file).resolve()
    if _inside(package, root):
        return []
    return [
        f"the package {PACKAGE} resolves to {package} which is outside the checkout {root} that the tests were collected from"
    ]


def _package_file() -> str | None:
    """The file the imported package was loaded from, or None when it cannot be imported or has no file."""
    try:
        return getattr(importlib.import_module(PACKAGE), "__file__", None)
    except Exception:  # any failure to import, not only ImportError: a broken __init__ raises whatever it raises
        return None


def _stop(message: str) -> None:
    """End the session before any test runs."""
    pytest.exit(message, returncode=4)


def pytest_sessionstart(session):
    """Stop the session when the package under test is not this checkout's."""
    findings = subject_findings(_package_file(), session.config.rootpath)
    if findings:
        _stop("; ".join(findings) + " (a run in a second checkout needs PYTHONPATH naming its src)")
