"""Check that the ledger's scoped-in units and the family tests agree on who owns what.

A legacy unit the ledger scopes in must be tested, and owned by exactly one system; the owner is
written only on its tests' decorators, while the ledger row cites a scope statement of one system.
Nothing ties the two together except these four checks, which read the ledger, the test inventory
(computed from the test files on every run) and the system-scope file. Check 4 refuses a
scoped-in unit that has no equivalence or refactor-reminder test, and one whose tests name more
than one owning system. Check 5 refuses a test whose unit key does not resolve to exactly one
scoped-in row: a test of a discarded or scoped-out unit, or of a unit with no row. Check 6
refuses a test whose owning system is not a system of the system-scope file. Check 7 refuses a
scoped-in unit whose tests name one owner while its ledger row cites a scope statement of another
system, and says that choosing between the two is a human ruling, because either side may be the
one that is wrong. Each finding names its unit, test or system.

The ledger's own three checks print through `python -m tests.correspondence.ledger`; these four
print through this module, and `WHOLE_LEDGER_CHECKS` here is the ledger's tuple followed by these
four, which is what the guard cases run. Deliberately NOT an extension of the ledger's tuple
inside `ledger.py`: these checks import the ledger, so the ledger importing them back would be an
import cycle whose outcome depends on which module is imported first. Run as a script it prints one
summary line, which states how many scoped-in rows and family tests the checks looked at, and
exits 0, or 1 on any finding.
"""

from __future__ import annotations

import sys
from pathlib import Path

from tests.correspondence import inventory, ledger, system_scope

REPO_ROOT = ledger.REPO_ROOT


def _scoped_in(root: Path) -> list[dict]:
    """The ledger's scope-in rows under `root`."""
    return [row for row in ledger.load(root)[0] if row.get("disposition") == "scope-in"]


def _family_tests(root: Path) -> list[dict]:
    """Every equivalence and refactor-reminder test under `root`, as the static inventory lists it."""
    return inventory.listing(root)[0]


def _test_name(entry: dict) -> str:
    """A test's file and function, the form every finding below names it by."""
    return f"{entry['file']}::{entry['function']}"


def _owners(key: str, tests: list[dict]) -> list[str]:
    """The distinct owning systems named by the tests of one unit, sorted."""
    return sorted({str(t["system"]) for t in tests if t["unit"] == key})


def check_4(root: Path = REPO_ROOT) -> list[str]:
    """Check 4: each scoped-in unit has a test, and its tests name exactly one owning system."""
    tests = _family_tests(root)
    findings = []
    for row in _scoped_in(root):
        key = row.get("key")
        owners = _owners(key, tests)
        if not owners:
            findings.append(f"unit {key}: scoped in and has no equivalence or refactor-reminder test")
        elif len(owners) > 1:
            findings.append(f"unit {key}: its tests name {len(owners)} owning systems: {', '.join(owners)}")
    return findings


def check_5(root: Path = REPO_ROOT) -> list[str]:
    """Check 5: each test's unit key resolves to exactly one scoped-in row, never to a discard or scope-out."""
    rows = ledger.load(root)[0]
    findings = []
    for entry in _family_tests(root):
        key = entry["unit"]
        matching = [row for row in rows if row.get("key") == key]
        other = sorted({str(row.get("disposition")) for row in matching if row.get("disposition") != "scope-in"})
        scoped_in = sum(1 for row in matching if row.get("disposition") == "scope-in")
        if other:
            findings.append(f"test {_test_name(entry)}: unit {key} is {', '.join(other)}, not scoped in")
        elif scoped_in != 1:
            findings.append(f"test {_test_name(entry)}: unit {key} resolves to {scoped_in} scoped-in rows, not one")
    return findings


def check_6(root: Path = REPO_ROOT) -> list[str]:
    """Check 6: each test's owning system is a system of the system-scope file."""
    known = set(system_scope.slugs(root))
    return [
        f"test {_test_name(entry)}: owning system {entry['system']} is not a system of the system-scope file"
        for entry in _family_tests(root)
        if entry["system"] not in known
    ]


def check_7(root: Path = REPO_ROOT) -> list[str]:
    """Check 7: a scoped-in row's cited statement belongs to the one owner its tests name.

    A unit whose tests name no owner or two is check 4's finding, so this compares only a unit whose
    tests name exactly one; a row whose citation names no statement is the ledger's format finding.
    """
    tests = _family_tests(root)
    findings = []
    for row in _scoped_in(root):
        key = row.get("key")
        owners = _owners(key, tests)
        cited = system_scope.statement(row.get("scope", ""), root)
        if len(owners) != 1 or cited is None or cited["slug"] == owners[0]:
            continue
        findings.append(
            f"unit {key}: its row cites a scope statement of {cited['slug']} and its tests name {owners[0]} "
            "as owner; choosing between the two is a human ruling"
        )
    return findings


WHOLE_LEDGER_CHECKS = ledger.WHOLE_LEDGER_CHECKS + (check_4, check_5, check_6, check_7)


def summary(root: Path = REPO_ROOT) -> tuple[str, list[str]]:
    """The printed line, which states what the checks looked at, and every finding of checks 4 to 7."""
    rows, tests = len(_scoped_in(root)), len(_family_tests(root))
    findings = [f for check in (check_4, check_5, check_6, check_7) for f in check(root)]
    looked_at = f"scoped-in rows {rows}, family tests {tests}"
    if rows == 0 and tests == 0:
        # Stated rather than left as a bare zero, so a pass over nothing is not read as a pass over the ledger.
        looked_at += " — nothing to check"
    return f"checks 4 to 7: {looked_at}; findings {len(findings)}", findings


def main(root: Path = REPO_ROOT) -> int:
    """Print the summary line and each finding; return 0 when there is none, 1 otherwise."""
    line, findings = summary(root)
    print(line)
    for finding in findings:
        print(f"  {finding}")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
