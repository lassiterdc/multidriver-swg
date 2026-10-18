"""List every family test from its decorator, without importing it, and check the list against collection.

Each equivalence and refactor-reminder test names its unit, part and owning system in a literal
marker on the test function. Systems select their tests by those markers, and later checks read
them to decide which unit has a test and who owns it, so the list must be computed from the test
files on every run rather than stored. This module reads every test file with the standard
library, fails (never skips) on a decorator it cannot read, prints the list, and holds the
collection capture that the cross-check test compares with: pytest's item list taken before any
`-m` or `-k` deselection, because a list taken after deselection would report every deselected
test as missing.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

from tests.correspondence import corpus_record

REPO_ROOT = corpus_record.REPO_ROOT
FAMILIES = ("equivalence", "refactor_reminder")
NAMES = (*FAMILIES, "guard")
KEYWORDS = ("unit", "part", "system")
CAPTURE = pytest.StashKey[list]()
COLLECTED = pytest.StashKey[int]()  # how many items the capture saw, so a test can tell it ran before deselection


def _test_files(root: Path) -> list[Path]:
    """The files pytest's default patterns collect under `tests/`: `test_*.py` and `*_test.py`."""
    tests = root / "tests"
    return sorted({*tests.rglob("test_*.py"), *tests.rglob("*_test.py")})


def _marker_name(decorator: ast.expr) -> str | None:
    """The marker name of a `pytest.mark.{name}(...)` decorator, or None for any other decorator."""
    func = decorator.func if isinstance(decorator, ast.Call) else decorator
    if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Attribute) and func.value.attr == "mark":
        return func.attr
    return None


def _family_name(decorator: ast.expr) -> str | None:
    """The family or guard name found anywhere along a decorator's callee, or None when it carries none.

    `equivalence` for `pytest.mark.equivalence(...)`, for `mark.equivalence(...)` and for
    `pytest.mark.equivalence.with_args(...)` alike. The whole chain is walked rather than its last
    name only, because a call such as `with_args` puts another name last while pytest still
    applies the family marker.
    """
    node = decorator
    while True:
        if isinstance(node, ast.Call):
            node = node.func
        elif isinstance(node, ast.Attribute):
            if node.attr in NAMES:
                return node.attr
            node = node.value
        else:
            return node.id if isinstance(node, ast.Name) and node.id in NAMES else None


def listing(root: Path = REPO_ROOT) -> tuple[list[dict], list[str]]:
    """Return every decorated family test (family, unit, part, system, file, line, function) and every unreadable form."""
    entries, findings = [], []
    for path in _test_files(root):
        rel = path.relative_to(root).as_posix()
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            for decorator in node.decorator_list:
                name = _marker_name(decorator)
                if name is None and (term := _family_name(decorator)) in NAMES:
                    # Refused rather than listed: the read cannot tell that `mark`, or an alias, is pytest's,
                    # and skipping the decorator would hide a test that pytest runs and `-m` selects.
                    findings.append(f"{rel}:{decorator.lineno}: unreadable {term} marker on {node.name}")
                    continue
                if name not in NAMES:
                    continue
                values = {}
                readable = isinstance(decorator, ast.Call) and not decorator.args
                for keyword in decorator.keywords if isinstance(decorator, ast.Call) else []:
                    if keyword.arg not in KEYWORDS or not isinstance(keyword.value, ast.Constant):
                        readable = False
                    else:
                        values[keyword.arg] = keyword.value.value
                if not readable or not all(isinstance(v, str) for v in values.values()):
                    findings.append(f"{rel}:{decorator.lineno}: unreadable {name} marker on {node.name}")
                    continue
                if name in FAMILIES:
                    entries.append(
                        {
                            "family": name,
                            "file": rel,
                            "line": decorator.lineno,
                            "function": node.name,
                            **{k: values.get(k) for k in KEYWORDS},
                        }
                    )
    return entries, findings


def reminders_per_system(entries: list[dict]) -> dict[str, int]:
    """Count the refactor-reminder tests of each owning system."""
    counts: dict[str, int] = {}
    for entry in entries:
        if entry["family"] == "refactor_reminder":
            counts[entry["system"]] = counts.get(entry["system"], 0) + 1
    return dict(sorted(counts.items()))


def narrowing(config) -> str | None:
    """The path arguments that narrow collection below `testpaths`, or None when collection is not narrowed."""
    root = config.rootpath
    # Against the directory pytest was started in, as pytest itself resolves them: against the root,
    # `cd tests && pytest .` reads as narrowed although it collects the whole tree.
    given = {(config.invocation_params.dir / arg.split("::")[0]).resolve() for arg in config.args}
    whole = {(root / path).resolve() for path in config.getini("testpaths")}
    return None if given == whole else " ".join(config.args)


def captured_from(items) -> list[dict]:
    """Describe each collected family test by its family, unit, part, system and node id."""
    captured = []
    for item in items:
        for family in FAMILIES:
            mark = item.get_closest_marker(family)
            if mark is not None:
                captured.append({"family": family, **{k: mark.kwargs.get(k) for k in KEYWORDS}, "nodeid": item.nodeid})
    return captured


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(session, config, items):
    """Record every collected family test, and the number of collected items, before `-m` or `-k` deselection."""
    config.stash[CAPTURE] = captured_from(items)
    config.stash[COLLECTED] = len(items)


def cross_check(entries: list[dict], captured: list[dict]) -> list[str]:
    """Name each (family, unit, part, system) present on only one side of the static list and the collection."""

    def key(e):
        """The identity both sides are compared on: family, unit, part and owning system."""
        return (e["family"], e["unit"], e["part"], e["system"])

    listed, collected = {key(e): e for e in entries}, {key(c): c for c in captured}
    return [
        f"listed and not collected: {listed[k]['file']}::{listed[k]['function']} {k}"
        for k in sorted(set(listed) - set(collected), key=str)
    ] + [
        f"collected and not listed: {collected[k]['nodeid']} {k}" for k in sorted(set(collected) - set(listed), key=str)
    ]


def main(root: Path = REPO_ROOT) -> int:
    """Print one line per listed test and the summary line; return 1 when a decorator is unreadable."""
    entries, findings = listing(root)
    for e in entries:
        print(f"{e['family']} unit={e['unit']} part={e['part'] or '-'} system={e['system']} {e['file']}:{e['line']}")
    reminders = reminders_per_system(entries)
    per = ", ".join(f"{s}={n}" for s, n in reminders.items()) or "none"
    print(
        f"test inventory: {len(entries)} tests in {len({e['file'] for e in entries})} files over families "
        f"{', '.join(FAMILIES)}; reminders per system: {per}"
    )
    for finding in findings:
        print(f"  {finding}", file=sys.stderr)
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
