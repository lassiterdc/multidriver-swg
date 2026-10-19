"""Read the toolkit's statement of each system's scope, and check the file against itself and the guard cases.

Every test names its owning system, and every scoped-in ledger row cites a scope statement of
that system, so the toolkit keeps its own file of the twenty systems and what each covers and
excludes, written in its own words (`system_scope.toml`). Each statement's identifier is made
from its system's slug and its text, so the file can be checked from itself alone. Seven checks
do that: each identifier is rebuilt from its slug and the start of its text's SHA-256 digest;
no identifier occurs twice; no statement holds a line break; each mark is `covers` or
`excludes`; every system has a `covers` statement; the file uses only its format's keys; and the
guard cases name exactly the file's systems. Run as a script it prints a one-line summary,
exiting 0 when clean and 1 otherwise.
"""

from __future__ import annotations

import ast
import hashlib
import sys
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCOPE_PATH = Path("tests") / "correspondence" / "system_scope.toml"
GUARD_PATH = Path("tests") / "correspondence" / "test_guard_cases.py"
RULE = "system-scope:1"
MARKS = ("covers", "excludes")
TOP_KEYS = ("rule", "system")
SYSTEM_KEYS = ("slug", "statement")
STATEMENT_KEYS = ("mark", "text", "id")


def _read(root: Path) -> dict:
    """The file under `root`, parsed."""
    return tomllib.loads((root / SCOPE_PATH).read_text(encoding="utf-8"))


def _statements(data: dict) -> list[tuple[str, dict]]:
    """Every statement with its system's slug, in file order."""
    return [
        (system.get("slug", ""), entry) for system in data.get("system", []) for entry in system.get("statement", [])
    ]


def _id_findings(data: dict) -> list[str]:
    """Name each statement whose identifier is not its slug, a slash and the 8-hex prefix of its text's SHA-256."""
    findings = []
    for slug, entry in _statements(data):
        expected = f"{slug}/{hashlib.sha256(str(entry.get('text', '')).encode('utf-8')).hexdigest()[:8]}"
        if entry.get("id") != expected:
            findings.append(f"statement `{entry.get('id')}`: identifier is not computed from its slug and text")
    return findings


def _unique_findings(data: dict) -> list[str]:
    """Name each identifier that occurs more than once."""
    seen: set[str] = set()
    repeated: list[str] = []
    for _slug, entry in _statements(data):
        sid = entry.get("id")
        if sid in seen and sid not in repeated:
            repeated.append(sid)
        seen.add(sid)
    return [f"statement `{sid}`: identifier occurs more than once" for sid in repeated]


def _line_break_findings(data: dict) -> list[str]:
    """Name each statement whose text holds a line break, since a statement is one line."""
    return [
        f"statement `{entry.get('id')}`: text holds a line break"
        for _slug, entry in _statements(data)
        if "\n" in str(entry.get("text", "")) or "\r" in str(entry.get("text", ""))
    ]


def _mark_findings(data: dict) -> list[str]:
    """Name each statement whose mark is neither `covers` nor `excludes`."""
    return [
        f"statement `{entry.get('id')}`: mark {entry.get('mark')!r} is not one of {MARKS}"
        for _slug, entry in _statements(data)
        if entry.get("mark") not in MARKS
    ]


def _covers_findings(data: dict) -> list[str]:
    """Name each system with no `covers` statement, which no scoped-in row could then cite."""
    return [
        f"system `{system.get('slug')}`: no covers statement"
        for system in data.get("system", [])
        if not any(entry.get("mark") == "covers" for entry in system.get("statement", []))
    ]


def _key_findings(data: dict) -> list[str]:
    """Name each key outside the format, at the top level, in a system table and in a statement."""
    findings = [f"top level: key {k!r} is not one of {TOP_KEYS}" for k in data if k not in TOP_KEYS]
    for system in data.get("system", []):
        findings += [
            f"system `{system.get('slug')}`: key {k!r} is not one of {SYSTEM_KEYS}"
            for k in system
            if k not in SYSTEM_KEYS
        ]
        for entry in system.get("statement", []):
            findings += [
                f"statement `{entry.get('id')}`: key {k!r} is not one of {STATEMENT_KEYS}"
                for k in entry
                if k not in STATEMENT_KEYS
            ]
    return findings


def load(root: Path = REPO_ROOT) -> tuple[list[dict], list[str]]:
    """Return the systems under `root` and every finding of the six checks that read the file alone."""
    data = _read(root)
    findings = (
        _id_findings(data)
        + _unique_findings(data)
        + _line_break_findings(data)
        + _mark_findings(data)
        + _covers_findings(data)
        + _key_findings(data)
    )
    return data.get("system", []), findings


def slugs(root: Path = REPO_ROOT) -> list[str]:
    """Return the systems' slugs, in the file's order."""
    return [system["slug"] for system in _read(root).get("system", [])]


def statement(scope_id: str, root: Path = REPO_ROOT) -> dict | None:
    """Return the statement with this identifier, with its system's slug added, or None when no statement has it."""
    for slug, entry in _statements(_read(root)):
        if entry.get("id") == scope_id:
            return {**entry, "slug": slug}
    return None


def _guard_literals(guard_file: Path) -> set[str]:
    """Read the `system=` literals of the `guard` marker decorators without importing the file."""
    found: set[str] = set()
    for node in ast.walk(ast.parse(guard_file.read_text(encoding="utf-8"))):
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Call) and getattr(decorator.func, "attr", None) == "guard":
                for keyword in decorator.keywords:
                    if keyword.arg == "system" and isinstance(keyword.value, ast.Constant):
                        found.add(keyword.value.value)
    return found


def guard_case_findings(guard_file: Path, root: Path = REPO_ROOT) -> list[str]:
    """The seventh check: name each slug that is in only one of the guard cases and the file."""
    literals, systems = _guard_literals(guard_file), set(slugs(root))
    return [f"system `{s}`: in the system-scope file and has no guard case" for s in sorted(systems - literals)] + [
        f"guard case `{s}`: names no system of the system-scope file" for s in sorted(literals - systems)
    ]


def summary(root: Path = REPO_ROOT) -> tuple[str, list[str]]:
    """The printed line and every finding of all seven checks, the real guard cases included."""
    systems, findings = load(root)
    findings += guard_case_findings(root / GUARD_PATH, root)
    marks = [entry.get("mark") for system in systems for entry in system.get("statement", [])]
    line = (
        f"system scope: {len(systems)} systems, {len(marks)} statements "
        f"({marks.count('covers')} covers, {marks.count('excludes')} excludes); findings {len(findings)}"
    )
    return line, findings


def main(root: Path = REPO_ROOT) -> int:
    """Print the summary line and each finding; return 0 when there is none, 1 otherwise."""
    line, findings = summary(root)
    print(line)
    for finding in findings:
        print(f"  {finding}")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
