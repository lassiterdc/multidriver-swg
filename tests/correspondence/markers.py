"""Refuse, at collection, a test whose family or guard marker cannot be trusted to scope it.

Each equivalence or refactor-reminder test names its unit and its owning system in a marker
on the test function, and each guard case names its system the same way. A run selecting one
system's tests by `-m` relies on those markers, so a marker that is duplicated, names a
system outside the partition, or does not come from the function's own decorator would make
the selection wrong without any test failing. This plugin stops collection on any of them,
with `pytest.UsageError` (exit 4) and a message naming each offending test.

Deliberately NOT a failing test: a test runs after selection, so it would not see the tests
a `-m` expression deselected, and a duplicate must stop the whole collection rather than fail one test.
"""

from __future__ import annotations

import pytest

from tests.correspondence import system_scope

FAMILIES = ("equivalence", "refactor_reminder")
NAMES = (*FAMILIES, "guard")


def records(item) -> list[dict]:
    """Describe each marker of the three names on one collected item, with where it came from.

    `origin` is `decorator` when the mark is one of the test function's own decorator marks,
    `parametrize` when a parametrize case carries it, and `other` otherwise (module-level,
    class-level or added by a hook).
    """
    function = getattr(item, "function", None)
    own = list(getattr(function, "pytestmark", []))
    callspec_marks = list(getattr(getattr(item, "callspec", None), "marks", []))
    found = []
    for name in NAMES:
        for mark in item.iter_markers(name):
            if mark in callspec_marks:
                origin = "parametrize"
            elif mark in own:
                origin = "decorator"
            else:
                origin = "other"
            found.append(
                {
                    "nodeid": item.nodeid,
                    "name": name,
                    "unit": mark.kwargs.get("unit"),
                    "part": mark.kwargs.get("part"),
                    "system": mark.kwargs.get("system"),
                    "origin": origin,
                }
            )
    return found


def refusals(found: list[dict], slugs: set[str]) -> list[str]:
    """Name each refusal over the collected marker records; empty when collection may proceed.

    A duplicate (family, unit, part) is refused over the two families only: a guard case
    carries no unit or part, so the twenty guard cases would otherwise share one key.
    """
    findings = []
    first: dict[tuple, str] = {}
    for record in found:
        if record["name"] in FAMILIES:
            key = (record["name"], record["unit"], record["part"])
            if key in first and first[key] != record["nodeid"]:
                findings.append(f"duplicate test identifier {key}: {first[key]} and {record['nodeid']}")
            first.setdefault(key, record["nodeid"])
        if record["system"] not in slugs:
            findings.append(
                f"{record['nodeid']}: owning system {record['system']} is not a system of the system-scope file"
            )
        if record["origin"] == "parametrize":
            findings.append(f"{record['nodeid']}: marker {record['name']} is carried by a parametrize case")
        elif record["origin"] == "other":
            findings.append(
                f"{record['nodeid']}: marker {record['name']} does not come from the test function's own decorator"
            )
    return findings


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(session, config, items):
    """Stop collection, before any `-m` deselection, when a marker record is refused."""
    findings = refusals([r for item in items for r in records(item)], set(system_scope.slugs()))
    if findings:
        raise pytest.UsageError("collection refused:\n" + "\n".join(findings))
