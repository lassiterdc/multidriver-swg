"""Read the triage log and refuse a log or row whose form cannot be relied on.

When an equivalence test fails and someone closes the failure, the closing is recorded as one
row of `tests/correspondence/triage_log.toml`: which test, which of the four findings it was,
what artifact changed, and the commit. One row per closing, kept apart from the tests, lets a
test that was closed twice show both closings. A row written by hand can be wrong in ways nobody
notices until someone tries to follow it, so this module checks each row's form: the test is
written as a test identifier whose unit key is canonical, the finding is one of 1 to 4, a third
finding explains in words where the old code went wrong and what the expected value now stands
for, a fourth finding states one of its two states, and the third and fourth findings — and only
they — point at a departure entry by the unit's key and a line number within it. A key the log's form
does not name is refused, at the top level and in a row, so rows filed under a misspelt table
name are never read as an empty log. The header states, in words, when someone working on a
failing test must stop and hand it to the developer.

Run as a script it prints one summary line and exits 0, or 1 on any finding.

Not established: that a departure pointer names an entry that exists. No divergence ledger exists
in this tree to resolve it against, so the summary line says so unconditionally; this module
cannot see a ledger added later, and the line becomes false at that moment unless it is changed
in the same change. Nor is a row's test checked against the tests that exist: only its form is.
"""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path

from tests.correspondence import ledger

REPO_ROOT = ledger.REPO_ROOT
LOG_PATH = Path("tests") / "correspondence" / "triage_log.toml"
RULE = "triage-log:1"
HALT_CONDITION = (
    "work on a failing equivalence test stops and goes to the developer when the failure's finding"
    " is undecided, or when the test cannot pass against correctly written new code"
)
TOP_LEVEL_KEYS = ("rule", "halt_condition", "statement", "row")
ROW_KEYS = ("test", "finding", "artifact", "commit", "departure", "ruled_outcome", "state")
FINDINGS = (1, 2, 3, 4)
POINTING_FINDINGS = (3, 4)
STATES = ("fork ruled, method not built", "pair registered")
DEPARTURE_RESOLUTION = "not measured: no divergence ledger in this tree"
FAMILY_RE = re.compile(r"[A-Za-z_]\w*")
PART_RE = re.compile(r"[^\s:]+")
COMMIT_RE = re.compile(r"[0-9a-fA-F]{40}")


def load(root: Path = REPO_ROOT) -> tuple[list[dict], list[str]]:
    """Return the log's rows under `root` and every finding of its header and its rows.

    Each row finding opens with `row {n}`, the row's 1-based position in the file, because a
    malformed row may carry no test identifier to name it by.
    """
    log = tomllib.loads((root / LOG_PATH).read_text(encoding="utf-8"))
    findings = _top_level_key_findings(log) + _header_findings(log)
    rows = log.get("row", [])
    for index, row in enumerate(rows, 1):
        findings.extend(_row_findings(f"row {index}", row))
    return rows, findings


def _top_level_key_findings(log: dict) -> list[str]:
    """Name each top-level key the log's form does not name.

    Rows written under `[[rows]]` instead of `[[row]]` leave `row` absent, so without this the
    log would read as holding no row, with every check green.
    """
    allowed = ", ".join(TOP_LEVEL_KEYS)
    return [f"header: key {key!r} is not one of {allowed}" for key in log if key not in TOP_LEVEL_KEYS]


def _header_findings(log: dict) -> list[str]:
    """Name a header whose rule or halt condition is not the expected text, or whose statement is empty."""
    findings = []
    for field, expected in (("rule", RULE), ("halt_condition", HALT_CONDITION)):
        if log.get(field) != expected:
            findings.append(f"header: {field} {log.get(field)!r} is not {expected!r}")
    if not isinstance(log.get("statement"), str) or not log["statement"].strip():
        findings.append("header: statement is absent or empty")
    return findings


def _row_findings(label: str, row: dict) -> list[str]:
    """Collect one row's findings; the checks that depend on the finding run only when it is valid."""
    findings = _row_key_findings(label, row) + _test_findings(label, row) + _finding_findings(label, row)
    findings += _text_findings(label, row) + _commit_findings(label, row)
    if _valid_finding(row.get("finding")):
        findings += _departure_presence_findings(label, row) + _departure_form_findings(label, row)
        findings += _ruled_outcome_findings(label, row) + _state_findings(label, row)
    return findings


def _row_key_findings(label: str, row: dict) -> list[str]:
    """Name each row key the row form does not name; a misspelt optional field would otherwise pass silently."""
    allowed = ", ".join(ROW_KEYS)
    return [f"{label}: key {key!r} is not one of {allowed}" for key in row if key not in ROW_KEYS]


def _valid_finding(value: object) -> bool:
    """Whether `value` is one of the four findings; `True` is refused although it equals 1."""
    return isinstance(value, int) and not isinstance(value, bool) and value in FINDINGS


def _test_findings(label: str, row: dict) -> list[str]:
    """Name a `test` that is not `{family}:{unit key}` or `{family}:{unit key}:{part}` with a canonical key.

    The unit key itself contains a colon (`_utils.py:1991-2075`), so the key is tried whole
    first and only then with a trailing part split off at the last colon. Canonical means what
    the disposition ledger means by it — ranges ascending, in order and disjoint — because a key
    the ledger would refuse names no unit.
    """
    value = row.get("test")
    if isinstance(value, str):
        family, _sep, rest = value.partition(":")
        if FAMILY_RE.fullmatch(family):
            if ledger._canonical(rest):
                return []
            key, _sep, part = rest.rpartition(":")
            if ledger._canonical(key) and PART_RE.fullmatch(part):
                return []
    return [f"{label}: test {value!r} is not a test identifier ({{family}}:{{unit key}}[:{{part}}])"]


def _finding_findings(label: str, row: dict) -> list[str]:
    """Name a `finding` that is not one of the four integers."""
    if _valid_finding(row.get("finding")):
        return []
    return [f"{label}: finding {row.get('finding')!r} is not one of 1, 2, 3, 4"]


def _text_findings(label: str, row: dict) -> list[str]:
    """Name an `artifact` that is absent or empty."""
    value = row.get("artifact")
    if isinstance(value, str) and value.strip():
        return []
    return [f"{label}: artifact is absent or empty"]


def _commit_findings(label: str, row: dict) -> list[str]:
    """Name a `commit` that is not forty hexadecimal digits, in either case, as git resolves both."""
    value = row.get("commit")
    if isinstance(value, str) and COMMIT_RE.fullmatch(value):
        return []
    return [f"{label}: commit {value!r} is not 40 hex digits"]


def _departure_presence_findings(label: str, row: dict) -> list[str]:
    """Name a third or fourth finding with no `departure`, and a first or second finding with one.

    A departure entry explains a place where the new code is meant to behave differently from the
    old code, which is what the third and fourth findings say; a pointer on a first or second
    finding claims such a place where none was found.
    """
    pointing, present = row["finding"] in POINTING_FINDINGS, "departure" in row
    if pointing and not present:
        return [f"{label}: finding {row['finding']} has no departure"]
    if present and not pointing:
        return [f"{label}: finding {row['finding']} carries a departure, which only findings 3 and 4 do"]
    return []


def _departure_form_findings(label: str, row: dict) -> list[str]:
    """Name a `departure` that is not `{ unit = "{unit key}", site = {line} }` with a canonical key and a line of 1 or more."""
    if "departure" not in row:
        return []
    value = row["departure"]
    if isinstance(value, dict) and set(value) == {"unit", "site"}:
        unit, site = value["unit"], value["site"]
        unit_ok = isinstance(unit, str) and ledger._canonical(unit)
        if unit_ok and isinstance(site, int) and not isinstance(site, bool) and site >= 1:
            return []
    return [f"{label}: departure {value!r} is not {{ unit = {{unit key}}, site = {{line}} }}"]


def _ruled_outcome_findings(label: str, row: dict) -> list[str]:
    """Name a third finding whose `ruled_outcome` is absent or empty.

    A third finding says the old code was wrong, so the reference value no longer means what the
    old code computed; a reader of the row needs, in words, where the old code went wrong and what
    the expected value now stands for.
    """
    if row["finding"] != 3:
        return []
    value = row.get("ruled_outcome")
    if isinstance(value, str) and value.strip():
        return []
    return [f"{label}: finding 3 has no ruled_outcome"]


def _state_findings(label: str, row: dict) -> list[str]:
    """Name a fourth finding whose `state` is not one of its two states."""
    if row["finding"] != 4 or row.get("state") in STATES:
        return []
    return [f"{label}: state {row.get('state')!r} is not one of {' | '.join(repr(s) for s in STATES)}"]


def main(root: Path = REPO_ROOT) -> int:
    """Print the row count, the halt condition and the pointers' state; return 0 with no finding, 1 otherwise."""
    rows, findings = load(root)
    log = tomllib.loads((root / LOG_PATH).read_text(encoding="utf-8"))
    print(
        f"triage log: {len(rows)} rows; halt condition: {log.get('halt_condition')}; "
        f"departure pointers: {DEPARTURE_RESOLUTION}"
    )
    for finding in findings:
        print(f"  {finding}", file=sys.stderr)
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
