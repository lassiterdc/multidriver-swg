"""Tests of scripts/check_planning_references.py: every pattern fires, ordinary text does not, and the exits hold."""

from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location(
    "check_planning_references", ROOT / "scripts" / "check_planning_references.py"
)
cpr = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cpr)

MUST_FIRE = [
    "see ADR-14 for this",
    "the parent ADR-n grammar",
    "under R32 the rule holds",
    "as ruled at D132",
    "landed in chunk 6a.4",
    "the 7a.1 change",
    "package 3a reviewer tests",
    "nothing in this work chunk",
    "read library/" + "docs/" + "planning/x.md",
    "the agentic" + "-workspace record",
    "ask the snakemake" + "-specialist",
    "the PIP says",
    "ruling U-21 applies",
    "open item O-2",
    "the TO-5 line",
]
MUST_NOT_FIRE = [
    "chi-square with P-1 degrees of freedom",
    "tolerance 1E-03 and 5e-04",
    "encoded as UTF-8",
    "a SHA-256 digest",
    "licensed AGPL-3.0-or-later",
    "an R2 of 0.9",
    "Python 3.12 and numpy 2.4",
    "the shape of the array",
    "the plan for the season",
]


def _repo(tmp_path: Path, files: dict[str, str]) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    for name, text in files.items():
        (tmp_path / name).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / name).write_text(text, encoding="utf-8")
    return tmp_path


@pytest.mark.parametrize("line", MUST_FIRE)
def test_each_planning_form_is_reported(tmp_path, line):
    """Property: every identifier form the scanner exists for is reported, so a pattern that stops firing reds."""
    root = _repo(tmp_path, {"a.py": f"x = 1  # {line}\n"})
    scanned, hits, _ = cpr.scan(root)
    assert scanned == 1 and len(hits) == 1 and hits[0].startswith("a.py:1: "), (line, hits)


@pytest.mark.parametrize("line", MUST_NOT_FIRE)
def test_ordinary_text_is_not_reported(tmp_path, line):
    """Property: numbers, encodings, licences and plain words that resemble a label are not reported."""
    root = _repo(tmp_path, {"a.py": f"x = 1  # {line}\n"})
    assert cpr.scan(root)[1] == [], line


def test_marker_keeps_a_line_and_is_counted(tmp_path):
    root = _repo(tmp_path, {"a.md": "quoting ADR-14 as an example  planning-ref-ok: counter-example\n"})
    assert cpr.scan(root)[1:] == ([], 1)


def test_a_file_name_is_scanned_like_its_bytes(tmp_path):
    root = _repo(tmp_path, {"tests/test_x_7a_1_review.py": "x = 1\n"})
    assert cpr.scan(root)[1] == ["tests/test_x_7a_1_review.py: work-chunk identifier in a file name (in the path)"]


def test_legacy_code_is_out_of_scope_and_legacy_records_are_not(tmp_path):
    root = _repo(tmp_path, {"_old_code_to_refactor/a.py": "# ADR-1\n", "_old_code_to_refactor/README.md": "ADR-2\n"})
    assert cpr.scan(root)[1] == ["_old_code_to_refactor/README.md:1: decision or record number"]


def test_exit_codes(tmp_path):
    clean = _repo(tmp_path / "c", {"a.py": "x = 1\n"})
    dirty = _repo(tmp_path / "d", {"a.py": "# ADR-1\n"})
    empty = _repo(tmp_path / "e", {})
    assert (cpr.main(clean), cpr.main(dirty), cpr.main(empty)) == (0, 1, 2)


def test_this_repository_carries_no_reference():
    """Property: the real tree is clean. This is the check that reaches any contributor who runs the suite."""
    scanned, hits, _ = cpr.scan(ROOT)
    assert scanned > 0 and hits == [], "\n".join(hits)
