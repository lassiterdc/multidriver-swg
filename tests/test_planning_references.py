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
    "the coder's tests of the cutting function",
    "ask the coordinator about it",
    "reported in the returning seat's note",
    "measured in the build return",
    "Build class: NEW CAPABILITY",
    "done on developer directive",
    "Ruled by the developer on 2026-08-17",
    "a developer ruling gives it",
    "not found by the absorbing agent",
    "participant C as Claude Code",
    "generated from copier-python-" + "template",
    "stored privately in claude-" + "workspace",
    "the sub" + "agent transcripts",
    "added by refactor-" + "plan audit",
    "follows a plan-then-" + "implement workflow",
    "participant P as Planning Doc",
    "D->>C: /proceed-with-" + "implementation",
    "by decision 3.3 and is",
    "as the cell that calls it does (row 108)",
    "Invocation isolation (B4-4, arm 2)",
    "the Definition of Done line",
    "Coordinator reads the returns",
    "Developer directive: keep it",
    "Build return shows it",
    "as the approved shape's unit table states",
    "measured in the review return",
    "per the reviewer return",
    "set out in the builder return",
    "done on the developer's instruction",
    "a developer directive applies",
    "the developer's rule for unseeded calls",
    "the developer ruled that site unregistered",
    "not found by the implementing agent",
    "as the reviewing agent reports",
    "asked of the building agent",
    "run /qaqc-and-" + "commit",
    "run /plan-" + "implementation",
    "run /plan-and-" + "implement",
    "this shape stage ends",
    "each shape iteration adds one",
    "an open item remains",
    "Reviewing agent reports it first",
    "at the developer's request",
    "The developer decided that it moves",
    "the developer's preference for the code",
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
    "/.claude/",
    "/CLAUDE.md",
    "/.codex/",
    "/AGENTS.md",
    "which a reviewer will ask about",
    "reproduced by the reviewer",
    'chunks = dict(year = "auto")',
    "the first chunk of the array",
    "the coordinates of the gauge",
    "an encoder and a decoder",
    "On a developer machine, link the folder",
    "the developer's environment cannot move a test",
    "Developers that use our General Public Licenses",
    "scoped-in rows 0, family tests 0",
    "rows 1, surviving 2",
    "a peak discharge Q100 and an F1 score of 0.9",
    "the ledger records a verdict per session",
    "Reviewer tests for the divergence detector",
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


def test_the_check_files_spell_no_private_name():
    """Property: neither check file spells the name of a private repository or tool, so the check does not carry what it forbids.

    The names are assembled from pieces here for the same reason the patterns are written in pieces.
    """
    names = [
        "copier-python-" + "template",
        "claude-" + "workspace",
        "agentic" + "-workspace",
        "proceed-with-" + "implementation",
        "qaqc-and-" + "commit",
        "refactor-" + "plan",
        "plan-then-" + "implement",
    ]
    for rel in cpr.SELF:
        text = (ROOT / rel).read_text(encoding="utf-8")
        assert [name for name in names if name in text] == [], rel


def test_this_repository_carries_no_reference():
    """Property: the real tree is clean. This is the check that reaches any contributor who runs the suite."""
    scanned, hits, _ = cpr.scan(ROOT)
    assert scanned > 0 and hits == [], "\n".join(hits)
