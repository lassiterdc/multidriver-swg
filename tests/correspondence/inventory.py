"""List every family test from its decorator, without importing it, and check the list against collection.

Each equivalence and refactor-reminder test names its unit, part and owning system in a literal
marker on the test function. Systems select their tests by those markers, and later checks read
them to decide which unit has a test and who owns it, so the list must be computed from the test
files on every run rather than stored. This module reads every test file with the standard
library, fails (never skips) on a decorator it cannot read, prints the list, and holds the
collection capture that the cross-check test compares with: pytest's item list taken before any
`-m` or `-k` deselection, because a list taken after deselection would report every deselected
test as missing.

It also holds the result record's two collection-time fields: the population, every collected node
id taken at that same pre-deselection point, and the exclusion, that population minus the ids that
survived deselection. The cross-check still compares the family capture, which is a check on the
population rather than the population itself.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

from tests.correspondence import corpus_record, record_schema

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
    """Record every collected family test, the count, and every collected node id, before deselection.

    pytest performs `-m`/`-k` deselection inside its own plain-tier
    `pytest_collection_modifyitems` hook, so whether a capture here sees the full list depends on
    DISPATCH ORDER, and `tryfirst=True` is what puts this hook ahead of that one whatever order the
    two were registered in. A set taken later would describe the survivors and report every
    deselected test as never collected. The node-id list is the result record's population, and
    `pytest_collection_finish` below derives the exclusion from it.
    """
    config.stash[CAPTURE] = captured_from(items)
    config.stash[COLLECTED] = len(items)
    config.stash[record_schema.POPULATION] = sorted(item.nodeid for item in items)


@pytest.hookimpl(tryfirst=True)
def pytest_collection_finish(session):
    """Stash the collected node ids that deselection removed, before any plain finish hook reads them.

    `tryfirst=True`, not registration order, is what makes the exclusion readable: the result
    record's own `pytest_collection_finish` is plain, so tryfirst dispatches this hook first whatever
    order the two plugins were registered in. Without it the record reads an exclusion nobody has
    written yet, and an empty exclusion reads as "nothing was deselected".

    A MISSING capture is left missing rather than defaulted or indexed. pytest calls this hook from
    the `finally` around collection (`Session.perform_collect`), so it runs even when collection
    raised before the capture hook did -- a mistyped path is the everyday case. Indexing the stash
    there raises `KeyError` inside that `finally`, which replaces the error in flight and turns
    pytest's usage error (exit 4) into an INTERNALERROR (exit 3) with a traceback into this plugin;
    defaulting to an empty list instead would report the same "nothing was deselected" as a clean
    run. So `EXCLUDED` stays unset when the population is absent, and a consumer must read absence as
    "this run never collected" rather than as an empty exclusion.
    """
    population = session.config.stash.get(record_schema.POPULATION, None)
    if population is None:
        return
    surviving = {item.nodeid for item in session.items}
    session.config.stash[record_schema.EXCLUDED] = sorted(set(population) - surviving)


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
