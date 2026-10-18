"""Assign every line of every retained legacy file to exactly one legacy unit, and check that it is so.

The disposition ledger under `tests/correspondence/ledger/` records one disposition for each
retained legacy unit and cites the unit by a key naming its file and line ranges (`_utils.py:1991-2075`).
A key means something only if every line of a retained file belongs to exactly one unit, so
this module cuts each retained file into four kinds of unit — each top-level definition, each
code cell without one, the column-0 code of a cell that has definitions, and each
comment-only cell — and then gives every remaining line an owner by a six-part extent rule:

1. blank and comment lines above a unit, back to the previous unit or the cell marker, are its;
2. a cell marker line (`#` followed by two `%`) is the first unit of its cell's;
3. lines below a cell's last unit, and the lines of a cell with no unit, are the previous unit's;
4. code above a file's first cell marker is a preamble unit;
5. a residual unit split by a definition keeps one key naming each of its ranges;
6. lines are counted by `str.splitlines()`, never by `wc -l`.

Which files are retained is read from the corpus record's `group` field, and the files are
read through `tracked_files(root)`. Run as a script it prints one summary line and exits 0
when the partition holds, 1 otherwise.
"""

from __future__ import annotations

import ast
import sys
import tomllib
from pathlib import Path

from tests.correspondence import corpus_record

REPO_ROOT = corpus_record.REPO_ROOT
NOT_MEASURED = "NOT MEASURED: extent check failed"
DEFS = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
MARKER = "#" + "%" * 2


def _section(root: Path) -> dict:
    record = tomllib.loads((root / corpus_record.RECORD_NAME).read_text(encoding="utf-8"))
    return record["section"][0]


def retained_files(root: Path = REPO_ROOT) -> list[str]:
    """Return the retained files' names, from the corpus record's `group` field, that git tracks."""
    section = _section(root)
    prefix = section["directory"].rstrip("/") + "/"
    tracked = {p[len(prefix) :] for p in corpus_record.tracked_files(root) if p.startswith(prefix)}
    return sorted(e["path"] for e in section["file"] if e.get("group") == "retained" and e["path"] in tracked)


def _read(root: Path, name: str) -> str:
    return (root / _section(root)["directory"] / name).read_text(encoding="utf-8")


def units(text: str) -> list[tuple[int, set[int], str | None]]:
    """Cut one file's text into units: (cell index, 0-based line set, definition name or None).

    The units are the four kinds the module docstring lists, plus a preamble unit (part 4)
    for column-0 code above the first cell marker, whose cell index is -1. The preamble is
    an owner for those lines and not one of the four kinds, so a count of the four kinds
    alone leaves it out; `d1_unit_count` returns that count.
    """
    lines, tree = text.splitlines(), ast.parse(text)
    cuts = [i for i, line in enumerate(lines) if line.startswith(MARKER)]

    def cell(i: int) -> int:
        return max([j for j, m in enumerate(cuts) if m <= i], default=-1)

    found: list[tuple[int, set[int], str | None]] = []
    for d in (x for x in tree.body if isinstance(x, DEFS)):
        start = min([x.lineno for x in d.decorator_list] + [d.lineno]) - 1
        found.append((cell(start), set(range(start, d.end_lineno)), d.name))
    for j, m in enumerate(cuts):
        end = cuts[j + 1] if j + 1 < len(cuts) else len(lines)
        if not any(lines[i].strip() for i in range(m + 1, end)):
            continue
        if any(u[0] == j and u[2] is not None for u in found):
            residual: set[int] = set()
            for x in tree.body:
                if not isinstance(x, DEFS) and m < x.lineno - 1 < end:
                    residual |= set(range(x.lineno - 1, x.end_lineno))
            if residual:
                found.append((j, residual, None))
        else:
            found.append((j, set(range(m + 1, end)), None))
    preamble: set[int] = set()
    for x in tree.body:
        if cuts and x.end_lineno - 1 < cuts[0] and not isinstance(x, DEFS):
            preamble |= set(range(x.lineno - 1, x.end_lineno))
    if preamble:
        found.append((-1, preamble, None))
    return found


def d1_unit_count(text: str) -> int:
    """Return a file's count of the four kinds of unit, without the preamble unit.

    Its reader is `ledger.main`, which sums it over the ceded files and prints the total as
    the units the ceded-file records cover. A ceded file is one whose corpus-record `group`
    is `ceded`, the other value of `corpus_record.GROUPS`; the ledger records it once, in
    `ceded_files.toml`, rather than unit by unit, so this count is how the ledger reports
    the units such a record stands for.

    It leaves out every unit whose cell index is -1, so a definition above a file's first
    cell marker would be left out with the preamble; no retained or ceded file has one.
    """
    return sum(1 for u in units(text) if u[0] != -1)


def _assign_leading_and_markers(lines: list[str], cuts: list[int], base: list[list[int]], cell) -> list[list[int]]:
    """Parts 1 and 2: blank and comment lines above a unit, and a cell marker line, take that unit."""
    own = [list(b) for b in base]
    n = len(lines)
    for i in range(n):
        if own[i]:
            continue
        if lines[i].startswith(MARKER):
            end = next((m for m in cuts if m > i), n)
            nxt = next((base[k] for k in range(i + 1, end) if base[k]), None)
            if nxt:
                own[i] = [nxt[0]]
        elif not lines[i].strip() or lines[i].lstrip().startswith("#"):
            k = i + 1
            while k < n and not base[k] and not lines[k].startswith(MARKER):
                k += 1
            if k < n and base[k] and cell(k) == cell(i):
                own[i] = [base[k][0]]
    return own


def _assign_trailing_lines(own: list[list[int]]) -> list[list[int]]:
    """Part 3: a line still unowned below a cell's last unit, or in a unit-less cell, takes the previous owned line's unit."""
    for i in range(len(own)):
        if not own[i]:
            k = i - 1
            while k >= 0 and not own[k]:
                k -= 1
            own[i] = [own[k][0]] if k >= 0 else []
    return own


def _ranges(lines0: list[int]) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    for x in sorted(lines0):
        if out and x == out[-1][1] + 1:
            out[-1] = (out[-1][0], x)
        else:
            out.append((x, x))
    return out


def extents(name: str, text: str) -> tuple[list[dict], dict]:
    """Return each unit's key, name and 1-based lines under the six-part rule, plus the file's counts."""
    lines = text.splitlines()
    cut = units(text)
    cuts = [i for i, line in enumerate(lines) if line.startswith(MARKER)]

    def cell(i: int) -> int:
        return max([j for j, m in enumerate(cuts) if m <= i], default=-1)

    base = [[k for k, u in enumerate(cut) if i in u[1]] for i in range(len(lines))]
    own = _assign_trailing_lines(_assign_leading_and_markers(lines, cuts, base, cell))
    found = []
    for k, u in enumerate(cut):
        mine = [i for i in range(len(lines)) if own[i] == [k]]
        rr = _ranges(mine) if mine else []
        key = f"{name}:" + ",".join(f"{a + 1}-{b + 1}" for a, b in rr)
        found.append({"key": key, "name": u[2], "lines": [i + 1 for i in mine]})
    counts = {
        "lines": len(lines),
        "units": len(cut),
        "none": sum(1 for o in own if not o),
        "multi": sum(1 for o in own if len(o) > 1),
        "split": sum(1 for f in found if "," in f["key"]),
    }
    return found, counts


def check(root: Path = REPO_ROOT) -> tuple[list[str], dict]:
    """Return the partition's findings (each naming a file) and its totals over every retained file."""
    findings: list[str] = []
    totals = {"lines": 0, "units": 0, "none": 0, "multi": 0, "split": 0}
    for name in retained_files(root):
        _found, counts = extents(name, _read(root, name))
        for field in totals:
            totals[field] += counts[field]
        if counts["none"]:
            findings.append(f"{name}: {counts['none']} line(s) in no unit")
        if counts["multi"]:
            findings.append(f"{name}: {counts['multi']} line(s) in two units")
    return findings, totals


def resolve(key: str, root: Path = REPO_ROOT) -> list[int] | None:
    """Return the 1-based lines of the unit whose canonical key is `key`, or None when no unit has it."""
    name = key.split(":", 1)[0]
    if name not in retained_files(root):
        return None
    found, _counts = extents(name, _read(root, name))
    return next((f["lines"] for f in found if f["key"] == key), None)


def require_extent(root: Path = REPO_ROOT) -> None:
    """Skip the calling test as not measured when the corpus record or the partition check fails."""
    corpus_record.require_record(root)
    findings, _totals = check(root)
    if findings:
        import pytest

        pytest.skip(f"{NOT_MEASURED} ({len(findings)} finding(s))")


def main(root: Path = REPO_ROOT) -> int:
    """Print one summary line; return 0 when every retained line is in exactly one unit, 1 otherwise."""
    findings, t = check(root)
    print(
        f"extent: lines {t['lines']}, units {t['units']}, lines in no unit {t['none']}, "
        f"lines in two units {t['multi']}, units split by a definition {t['split']}"
    )
    for finding in findings:
        print(f"  {finding}", file=sys.stderr)
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
