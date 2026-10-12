"""Generate the toolkit's copy of the project's system partition, or check that the committed copy is current.

The refactor's checks need each system's slug and the clauses of its routing predicate
(`assignment_criteria`), but those live in the project architecture, which this public
repository cannot read. So this script, run on a machine that holds both, writes them
into `tests/correspondence/partition_copy.json`, and its `--check` mode proves the
committed file is still what a fresh generation produces. Nothing runs it automatically:
until it is re-run after a change to the partition, the toolkit's checks pass against the
old copy.

Two transformations separate the copy from the source, and both are deterministic:

- Every match of the anonymization guard's own carrier patterns is replaced by
  `[withheld]`. Deliberately NOT a verbatim copy: the source names private things, and a
  verbatim copy would fail the guard and publish them. The guard's own patterns are used,
  rather than a second matcher, so what is withheld is exactly what the guard refuses.
- Each clause is identified by its system's slug plus the first 8 hex digits of the
  SHA-256 of its stored text. Deliberately NOT a position number: an ordinal would keep
  pointing at a different clause after an insertion, while a content address stops
  resolving, by name, when the clause it named changes.

Usage: `python3 _refactor_tools/generate_partition_copy.py --architecture {path} [--check]`.
Exit 0 on success, 1 when `--check` finds the committed copy differs, 2 on a refusal.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = REPO_ROOT / "tests" / "correspondence" / "partition_copy.json"
DEFAULT_ANCHOR = REPO_ROOT / "scripts" / "anonymization_blocklist.txt"
HEADER = "| System | system_status | description | assignment_criteria |"
WITHHELD = "[withheld]"
CLAUSE_START = re.compile(
    r"(?=\b(?:Belongs here if|Does NOT belong here if|does NOT belong here if|Does belong here|does belong here)\b)"
)
CLAUSE_TAIL = re.compile(r"(;\s*and|;|,\s*and|\band)\s*$")


class RefusalError(Exception):
    """A precondition the generator will not proceed without; exit 2."""


def _load_guard(anchor: Path):
    """Import the toolkit's anonymization guard from `scripts/` beside the anchor.

    The module is registered in `sys.modules` before it executes, because the guard
    declares a dataclass and the dataclass machinery looks its module up there; without
    the registration the import raises `AttributeError`.
    """
    path = anchor.parent / "check_anonymization.py"
    if not path.is_file():
        path = REPO_ROOT / "scripts" / "check_anonymization.py"
    spec = importlib.util.spec_from_file_location("_anonymization_guard_for_partition_copy", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _source_identity(architecture: Path) -> tuple[str, str]:
    """Return the architecture's path inside its repository and the last commit that changed it.

    Refuses when the file has uncommitted changes, because the recorded commit would then
    not be the content the copy was generated from.
    """
    folder = architecture.parent
    dirty = subprocess.run(
        ["git", "status", "--porcelain", "--", architecture.name], cwd=folder, capture_output=True, text=True
    )
    if dirty.returncode != 0:
        raise RefusalError("the architecture file is not inside a git repository")
    if dirty.stdout.strip():
        raise RefusalError("the architecture file has uncommitted changes")
    top = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], cwd=folder, capture_output=True, text=True, check=True
    )
    commit = subprocess.run(
        ["git", "log", "-1", "--format=%H", "--", architecture.name],
        cwd=folder,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    if not commit:
        raise RefusalError("the architecture file has no commit")
    relative = architecture.resolve().relative_to(Path(top.stdout.strip()).resolve()).as_posix()
    return relative, commit


def _rows(text: str) -> list[list[str]]:
    """Return the cells of each row of the descriptor table under the header line."""
    lines = text.splitlines()
    try:
        start = next(i for i, line in enumerate(lines) if line.strip() == HEADER)
    except StopIteration:
        raise RefusalError("the descriptor table's header line was not found") from None
    rows = []
    for line in lines[start + 2 :]:
        if not line.startswith("|"):
            break
        rows.append([cell.strip() for cell in line.strip().strip("|").split(" | ")])
    if not rows or any(len(row) != 4 for row in rows):
        raise RefusalError("the descriptor table has no rows, or a row without four cells")
    return rows


def _polarity(clause: str) -> str:
    """Classify a clause by its opening phrase; refuse one that opens with neither."""
    lowered = clause.lower()
    if lowered.startswith(("belongs here if", "does belong here")):
        return "belongs"
    if lowered.startswith("does not belong here if"):
        return "does-not-belong"
    raise RefusalError("a clause opens with neither a belongs nor a does-not-belong phrase")


def build(architecture: Path, anchor: Path) -> tuple[dict, dict]:
    """Return the copy as a dictionary, and its counts for the summary line."""
    guard = _load_guard(anchor)
    if not guard.load_local_supplement(anchor):
        raise RefusalError("no private carrier is reachable beside the anchor, so the copy cannot be screened")
    patterns = guard.compile_patterns(guard.load_carrier(anchor))
    document, commit = _source_identity(architecture)
    systems = []
    withheld = belongs = against = 0
    for cells in _rows(architecture.read_text(encoding="utf-8")):
        slug = cells[0].strip("`")
        clauses = []
        for part in CLAUSE_START.split(cells[3]):
            text = CLAUSE_TAIL.sub("", part.strip()).strip()
            if not text:
                continue
            for _token, pattern in patterns:
                text, hits = pattern.subn(WITHHELD, text)
                withheld += hits
            polarity = _polarity(text)
            belongs += polarity == "belongs"
            against += polarity == "does-not-belong"
            clause_id = f"{slug}/{hashlib.sha256(text.encode('utf-8')).hexdigest()[:8]}"
            clauses.append({"id": clause_id, "polarity": polarity, "text": text})
        systems.append({"slug": slug, "clauses": clauses})
    copy = {
        "rule": "partition-copy:1",
        "source_document": document,
        "source_commit": commit,
        "clause_id_rule": "sha256-of-stored-clause-text:1",
        "withheld_marker": WITHHELD,
        "withheld_count": withheld,
        "systems": systems,
    }
    counts = {
        "systems": len(systems),
        "clauses": belongs + against,
        "belongs": belongs,
        "against": against,
        "withheld": withheld,
    }
    return copy, counts


def render(copy: dict) -> str:
    """Serialize the copy deterministically, so a fresh generation compares byte for byte."""
    return json.dumps(copy, indent=1, ensure_ascii=False) + "\n"


def main(argv: list[str] | None = None) -> int:
    """Write the copy, or with `--check` compare a fresh generation with the committed file."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--architecture", type=Path, required=True, help="the project architecture document")
    parser.add_argument("--check", action="store_true", help="compare with the committed copy instead of writing")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="the copy's path")
    parser.add_argument("--carrier-anchor", type=Path, default=DEFAULT_ANCHOR, help="the guard's blocklist anchor")
    args = parser.parse_args(argv)
    try:
        copy, counts = build(args.architecture, args.carrier_anchor)
    except RefusalError as refusal:
        print(f"partition copy: refused — {refusal}", file=sys.stderr)
        return 2
    summary = (
        f"partition copy: {counts['systems']} systems, {counts['clauses']} clauses "
        f"({counts['belongs']} belongs, {counts['against']} does-not-belong), {counts['withheld']} withheld"
    )
    fresh = render(copy)
    if not args.check:
        args.output.write_text(fresh, encoding="utf-8")
        print(f"{summary}; written")
        return 0
    committed = args.output.read_text(encoding="utf-8") if args.output.is_file() else ""
    if committed == fresh:
        print(f"{summary}; equal to the committed file")
        return 0
    try:
        old = {s["slug"]: s for s in json.loads(committed)["systems"]} if committed else {}
    except (ValueError, KeyError, TypeError):
        old = {}
    differing = next((s["slug"] for s in copy["systems"] if old.get(s["slug"]) != s), "(header fields)")
    print(f"{summary}; DIFFERS from the committed file, first at {differing}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
