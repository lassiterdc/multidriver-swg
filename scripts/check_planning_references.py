#!/usr/bin/env python3
"""Fail if a file of this repository carries a reference into the private planning record.

The design and planning record behind this repository is private. A decision number, a
work-chunk or work-package identifier, a constraint label, or a path into that record
resolves for its author and for no reader of this repository, so such a reference is a
defect whoever wrote it. Every run scans the whole tree (tracked files plus untracked files
git does not ignore), not only the lines a change adds, so an inherited reference fails the
next run for anyone until it is fixed.

Only forms with an identifier's shape are matched. Plain words are left to a reader: a
pattern that fires on ordinary prose gets bypassed. A clean run does not establish that no
reference remains: names and copied text have no shape to match.

To keep a match on purpose, put `planning-ref-ok: {reason}` on the same line; the skip is
printed. Legacy code under `_old_code_to_refactor/` is out of scope.

Exit 0 = clean. 1 = at least one reference. 2 = nothing to scan.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
LEGACY_DIR = "_old_code_to_refactor/"
LEGACY_CODE_SUFFIXES = (".py", ".sh", ".ipynb")
MARKER = "planning-ref-ok:"
# These two files state the patterns and their controls, so they are not scanned.
SELF = ("scripts/check_planning_references.py", "tests/test_planning_references.py")

PATTERNS = {
    "decision or record number": re.compile(r"\b(?:ADR|D|U|UC|UG|TO|OD|OI|O)-\d+[a-z]?\b"),
    "letter-named decision": re.compile(r"\bADR-[a-z]\b"),
    "constraint or review label": re.compile(r"\b(?:C-[UDE]\d+|R\d{2,})\b"),
    "two- or three-digit decision": re.compile(r"\bD\d{2,3}\b"),
    "work-chunk identifier": re.compile(r"(?i:\bchunks?\s+`?\d+[a-z](?:\.\d+)?\b)|\b\d+[a-z]\.\d+\b"),
    "work-package identifier": re.compile(r"(?i:\b(?:work[- ])?packages?\s+\d+[a-z]\b)"),
    "work-chunk phrase": re.compile(r"(?i:\bwork[ -]?chunks?\b)"),
    "planning-record path": re.compile(
        r"\blibrary/(?:docs|knowledge|prompts|_scratch)\b|\bdocs/plann?ing\b|\$AGENTIC_\w+|\bagentic.workspace\b"
    ),
    "agent role name": re.compile(r"\b[a-z]+(?:-[a-z]+)*-specialist\b"),
    "plan-prompt acronym": re.compile(r"\bPIP\b"),
}
PATH_ONLY = {"work-chunk identifier in a file name": re.compile(r"(?:^|[/_])\d+[a-z]_\d+(?:[_.]|$)")}


def in_scope(name: str) -> bool:
    if name in SELF:
        return False
    return not (name.startswith(LEGACY_DIR) and name.endswith(LEGACY_CODE_SUFFIXES))


def files(root: Path) -> list[str]:
    out = subprocess.run(
        ["git", "ls-files", "-co", "--exclude-standard", "-z"], cwd=root, capture_output=True, check=True
    ).stdout.decode("utf-8", "replace")
    return sorted(n for n in out.split("\0") if n and in_scope(n))


def scan(root: Path) -> tuple[int, list[str], int]:
    hits: list[str] = []
    skipped = scanned = 0
    for rel in files(root):
        if not (root / rel).is_file():
            continue  # a tracked file deleted in the working tree is not part of what a reader will see
        for name, pattern in {**PATTERNS, **PATH_ONLY}.items():
            if pattern.search(rel):
                hits.append(f"{rel}: {name} (in the path)")
        try:
            data = (root / rel).read_bytes()
        except OSError:
            continue
        if b"\0" in data:
            continue
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            continue
        scanned += 1
        for number, line in enumerate(text.splitlines(), 1):
            names = [name for name, pattern in PATTERNS.items() if pattern.search(line)]
            if not names:
                continue
            if MARKER in line:
                skipped += 1
                print(f"[skip] {rel}:{number}: kept by a {MARKER} marker", file=sys.stderr)
                continue
            hits.append(f"{rel}:{number}: {', '.join(names)}")
    return scanned, hits, skipped


def main(root: Path = REPO_ROOT) -> int:
    scanned, hits, skipped = scan(root)
    if scanned == 0:
        print("check_planning_references: nothing to scan", file=sys.stderr)
        return 2
    print(
        f"check_planning_references: scanned {scanned} files, {len(hits)} reference(s), {skipped} kept", file=sys.stderr
    )
    for hit in hits:
        print(f"  {hit}", file=sys.stderr)
    if hits:
        print(
            "  Each line names something in the private planning record. Fix every line listed,\n"
            "  whoever wrote it: write what was decided in plain words, or remove the reference.\n"
            f"  To keep one on purpose, put on that line: {MARKER} {{reason}}",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else REPO_ROOT))
