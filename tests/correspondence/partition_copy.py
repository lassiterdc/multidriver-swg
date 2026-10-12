"""Read the toolkit's copy of the project's system partition, and check that the committed copy is internally consistent.

The copy (`partition_copy.json`) is generated outside CI, by `_refactor_tools/generate_partition_copy.py`
on a machine that holds the project architecture and the private carrier, so CI can never
regenerate it. What CI CAN do is recompute every field of the committed file that follows
from the file itself: each clause identifier from its stored text, the withheld count from
the stored texts, and the two closed vocabularies. `copy_findings()` does that, so a hand
edit or a bad merge of the copy is caught on the runner even though the generator's own
`--check` cannot run there. `guard_case_findings()` compares the `system=` literals of the
guard cases with the copy's slugs, in both directions.
"""

from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
COPY_PATH = Path("tests") / "correspondence" / "partition_copy.json"
POLARITIES = ("belongs", "does-not-belong")


def load(root: Path = REPO_ROOT) -> dict:
    """Return the committed copy under `root` as a dictionary."""
    return json.loads((root / COPY_PATH).read_text(encoding="utf-8"))


def slugs(root: Path = REPO_ROOT) -> list[str]:
    """Return the partition's system slugs, in the copy's order."""
    return [system["slug"] for system in load(root)["systems"]]


def ids(root: Path = REPO_ROOT) -> list[str]:
    """Return every clause identifier in the copy, in order."""
    return [clause["id"] for system in load(root)["systems"] for clause in system["clauses"]]


def clause(clause_id: str, root: Path = REPO_ROOT) -> dict | None:
    """Return the clause with this identifier, with its system's slug added, or None."""
    for system in load(root)["systems"]:
        for entry in system["clauses"]:
            if entry["id"] == clause_id:
                return {**entry, "slug": system["slug"]}
    return None


def _id_findings(copy: dict) -> list[str]:
    """Name each clause whose identifier is not its slug plus the 8-hex prefix of its text's SHA-256."""
    findings = []
    for system in copy["systems"]:
        for entry in system["clauses"]:
            expected = f"{system['slug']}/{hashlib.sha256(entry['text'].encode('utf-8')).hexdigest()[:8]}"
            if entry["id"] != expected:
                findings.append(f"clause `{entry['id']}`: identifier does not match its stored text")
    return findings


def _unique_findings(copy: dict) -> list[str]:
    """Name each identifier that occurs more than once."""
    seen: set[str] = set()
    repeated = []
    for system in copy["systems"]:
        for entry in system["clauses"]:
            if entry["id"] in seen and entry["id"] not in repeated:
                repeated.append(entry["id"])
            seen.add(entry["id"])
    return [f"clause `{cid}`: identifier occurs more than once" for cid in repeated]


def _polarity_findings(copy: dict) -> list[str]:
    """Name each clause whose polarity is outside the closed vocabulary."""
    return [
        f"clause `{entry['id']}`: polarity {entry['polarity']!r} is not one of {POLARITIES}"
        for system in copy["systems"]
        for entry in system["clauses"]
        if entry.get("polarity") not in POLARITIES
    ]


def _withheld_findings(copy: dict) -> list[str]:
    """Report a `withheld_count` that differs from the marker's occurrences in the stored texts."""
    marker = copy["withheld_marker"]
    occurrences = sum(entry["text"].count(marker) for system in copy["systems"] for entry in system["clauses"])
    if copy["withheld_count"] != occurrences:
        return [f"withheld_count: recorded {copy['withheld_count']}, stored texts hold {occurrences}"]
    return []


def _belongs_findings(copy: dict) -> list[str]:
    """Name each system with no belongs clause, which no scoped-in unit could then be routed to."""
    return [
        f"system `{system['slug']}`: no belongs clause"
        for system in copy["systems"]
        if not any(entry.get("polarity") == "belongs" for entry in system["clauses"])
    ]


def copy_findings(root: Path = REPO_ROOT) -> list[str]:
    """Return every inconsistency of the committed copy that CI can recompute without the carrier; empty on pass."""
    copy = load(root)
    return (
        _id_findings(copy)
        + _unique_findings(copy)
        + _polarity_findings(copy)
        + _withheld_findings(copy)
        + _belongs_findings(copy)
    )


def _guard_literals(guard_file: Path) -> set[str]:
    """Read the `system=` literals of `@pytest.mark.guard(...)` decorators without importing the file."""
    found: set[str] = set()
    for node in ast.walk(ast.parse(guard_file.read_text(encoding="utf-8"))):
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        for decorator in node.decorator_list:
            if (
                isinstance(decorator, ast.Call)
                and isinstance(decorator.func, ast.Attribute)
                and decorator.func.attr == "guard"
            ):
                for keyword in decorator.keywords:
                    if keyword.arg == "system" and isinstance(keyword.value, ast.Constant):
                        found.add(keyword.value.value)
    return found


def guard_case_findings(guard_file: Path, root: Path = REPO_ROOT) -> list[str]:
    """Name each slug present on only one side of: the guard cases' `system=` literals, and the copy's slugs."""
    literals = _guard_literals(guard_file)
    partition = set(slugs(root))
    return [
        f"system `{slug}`: in the partition copy and has no guard case" for slug in sorted(partition - literals)
    ] + [f"guard case `{slug}`: names no system of the partition copy" for slug in sorted(literals - partition)]
