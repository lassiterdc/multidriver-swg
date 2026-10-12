"""Find the runs of commented-out code in the retained legacy files, by one published predicate.

Each ledger row must acknowledge every commented-out run inside its unit, so the runs have to
be found the same way every time and the rule that finds them has to be stated where a reader
sees it: different predicates count different runs (87, 94 and 97 were measured on the same
files), so a count means nothing without its predicate. The predicate is `PREDICATE` below,
verbatim. Runs it misses are not counted, and nothing here claims to find them.

The module also asserts that every run lies inside one unit's extent; a run spanning two units
could not be acknowledged by one ledger row. Run as a script it prints one summary line with
the predicate and exits 0, or 1 when a run spans two units.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

from tests.correspondence import extent

REPO_ROOT = extent.REPO_ROOT
PREDICATE = (
    "a run is three or more consecutive lines, each of which, after leading whitespace, starts with `#`, "
    "does not start with `#%%`, and whose text after the leading `#` characters, stripped, is non-empty, "
    "is not a single identifier, and parses as Python; a run ends at any other line"
)
MINIMUM = 3


def _is_code_comment(line: str) -> bool:
    """Whether one line meets the predicate's per-line condition."""
    stripped = line.lstrip()
    if not stripped.startswith("#") or stripped.startswith(extent.MARKER):
        return False
    body = stripped.lstrip("#").strip()
    if not body or body.isidentifier():
        return False
    try:
        ast.parse(body)
    except SyntaxError:
        return False
    return True


def find_runs(text: str) -> list[tuple[int, int]]:
    """Return each run in `text` as its first and last 1-based line."""
    found: list[tuple[int, int]] = []
    current: list[int] = []
    for number, line in enumerate(text.splitlines() + [""], start=1):
        if _is_code_comment(line):
            current.append(number)
            continue
        if len(current) >= MINIMUM:
            found.append((current[0], current[-1]))
        current = []
    return found


def runs(name: str, root: Path = REPO_ROOT) -> list[tuple[int, int]]:
    """Return the runs of one retained file, read through the tracked set."""
    return find_runs(extent._read(root, name))


def runs_in(key: str, root: Path = REPO_ROOT) -> list[tuple[int, int]]:
    """Return the runs lying inside the unit whose canonical key is `key`; empty when the key resolves to no unit."""
    lines = extent.resolve(key, root)
    if lines is None:
        return []
    inside = set(lines)
    name = key.split(":", 1)[0]
    return [(a, b) for a, b in runs(name, root) if set(range(a, b + 1)) <= inside]


def straddles(name: str, text: str) -> list[str]:
    """Name each run whose lines lie in more than one unit's extent, with both units' keys."""
    found, _counts = extent.extents(name, text)
    owner = {line: unit["key"] for unit in found for line in unit["lines"]}
    findings = []
    for a, b in find_runs(text):
        keys = sorted({owner.get(line, "(no unit)") for line in range(a, b + 1)})
        if len(keys) > 1:
            findings.append(f"run {name}:{a}-{b} spans units " + " and ".join(f"`{k}`" for k in keys))
    return findings


def check(root: Path = REPO_ROOT) -> tuple[list[str], dict]:
    """Return straddle findings over every retained file, and the run and line totals."""
    findings: list[str] = []
    totals = {"runs": 0, "lines": 0}
    for name in extent.retained_files(root):
        text = extent._read(root, name)
        found = find_runs(text)
        totals["runs"] += len(found)
        totals["lines"] += sum(b - a + 1 for a, b in found)
        findings.extend(straddles(name, text))
    return findings, totals


def main(root: Path = REPO_ROOT) -> int:
    """Print the counts with the predicate; return 1 when any run spans two units."""
    findings, totals = check(root)
    print(f"commented-out runs: {totals['runs']}, lines {totals['lines']}; predicate: {PREDICATE}")
    for finding in findings:
        print(f"  {finding}", file=sys.stderr)
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
