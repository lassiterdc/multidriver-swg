"""Read the disposition ledger and run its three ledger-only checks.

Every retained legacy unit must carry exactly one recorded disposition — scope-in,
scope-out or discard — with its reason, so the ledger (one TOML file per retained script in
`tests/correspondence/ledger/`, plus `ceded_files.toml` for the four ceded files) is read
here, validated for form, and counted. Check 1 counts one row per unit; check 2 refuses a
scope-out with no destination, or a member destination with no acceptance state, and counts
the `OPEN` ones; check 3 requires each row to acknowledge exactly the commented-out runs the
published detector finds inside its unit, in both directions. Each finding names its unit,
row or file. Run as a script it prints one summary line and exits 0, or 1 on any finding.
"""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path

from tests.correspondence import commented_runs, extent, system_scope

REPO_ROOT = extent.REPO_ROOT
LEDGER_DIR = Path("tests") / "correspondence" / "ledger"
CEDED_FILE = "ceded_files.toml"
DISPOSITIONS = ("scope-in", "scope-out", "discard")
# Anchored with \Z rather than $, because $ also matches just before a final newline, so
# "x.py:1-18\n" would read as canonical; and the file part admits no whitespace at all.
KEY_RE = re.compile(r"^(?P<file>[^:\s]+):(?P<ranges>\d+-\d+(?:,\d+-\d+)*)\Z")


def _canonical(key: str) -> bool:
    """Whether a key is `file:a-b[,c-d...]` with each range ascending and the ranges ascending and disjoint."""
    match = KEY_RE.match(key)
    if not match:
        return False
    previous = 0
    for part in match.group("ranges").split(","):
        a, b = (int(x) for x in part.split("-"))
        if a > b or a <= previous:
            return False
        previous = b
    return True


def load(root: Path = REPO_ROOT) -> tuple[list[dict], list[dict], list[str]]:
    """Return the rows, the ceded-file records and every format finding of the ledger under `root`."""
    folder = root / LEDGER_DIR
    findings: list[str] = []
    retained = set(extent.retained_files(root))
    section = extent._section(root)
    ceded_group = {e["path"] for e in section["file"] if e.get("group") == "ceded"}
    rows: list[dict] = []
    script_files = {p.name for p in folder.glob("*.toml") if p.name != CEDED_FILE}
    for name in sorted(retained):
        if f"{name[:-3]}.toml" not in script_files:
            findings.append(f"{name}: group reads retained and no per-script ledger file names it")
    for file_name in sorted(script_files):
        data = tomllib.loads((folder / file_name).read_text(encoding="utf-8"))
        legacy = data.get("legacy_file")
        if legacy not in retained:
            findings.append(f"{file_name}: legacy_file {legacy!r} is not a retained file of the corpus record")
        for row in data.get("row", []):
            rows.append(row)
            findings.extend(_row_format_findings(row, legacy, root))
    ceded_path = folder / CEDED_FILE
    ceded = tomllib.loads(ceded_path.read_text(encoding="utf-8")).get("ceded_file", []) if ceded_path.is_file() else []
    named = {record.get("legacy_file") for record in ceded}
    for name in sorted(ceded_group - named):
        findings.append(f"{name}: group reads ceded and ceded_files.toml has no record for it")
    for name in sorted(named - ceded_group):
        findings.append(f"{name}: in ceded_files.toml and its group does not read ceded")
    return rows, ceded, findings


def _row_format_findings(row: dict, legacy: str | None, root: Path) -> list[str]:
    """Name each form defect of one row: key, disposition, reason, runs, and the disposition's own fields."""
    key = row.get("key", "")
    findings = []
    if not isinstance(key, str) or not _canonical(key) or key.split(":", 1)[0] != legacy:
        findings.append(f"row {key!r}: key is not canonical for {legacy}")
    if row.get("disposition") not in DISPOSITIONS:
        findings.append(f"row {key}: disposition {row.get('disposition')!r} is not one of {DISPOSITIONS}")
    if not isinstance(row.get("reason"), str) or not row["reason"].strip():
        findings.append(f"row {key}: reason is absent or empty")
    if not isinstance(row.get("commented_out_runs"), list):
        findings.append(f"row {key}: commented_out_runs is absent")
    if row.get("disposition") == "scope-in":
        findings += _scope_missing_findings(row, root) + _scope_excludes_findings(row, root)
    return findings


def _scope_missing_findings(row: dict, root: Path) -> list[str]:
    """Name a scope-in row whose `scope` names no statement of the system-scope file."""
    if system_scope.statement(row.get("scope", ""), root) is None:
        return [f"row {row.get('key')}: scope {row.get('scope')!r} names no statement of the system-scope file"]
    return []


def _scope_excludes_findings(row: dict, root: Path) -> list[str]:
    """Name a scope-in row whose `scope` names an excludes statement, which says what a system does not own."""
    cited = system_scope.statement(row.get("scope", ""), root)
    if cited is not None and cited.get("mark") == "excludes":
        return [f"row {row.get('key')}: scope {row.get('scope')!r} names an excludes statement, not a covers one"]
    return []


def check_1(root: Path = REPO_ROOT) -> list[str]:
    """Check 1: exactly one row per unit, counted; names each unit with no row or with two, and each row naming no unit."""
    rows, _ceded, _findings = load(root)
    counts: dict[str, int] = {}
    for row in rows:
        counts[row.get("key")] = counts.get(row.get("key"), 0) + 1
    keys = []
    for name in extent.retained_files(root):
        found, _ = extent.extents(name, extent._read(root, name))
        keys += [unit["key"] for unit in found]
    findings = [f"unit {k}: no row" for k in keys if counts.get(k, 0) == 0]
    findings += [f"unit {k}: {counts[k]} rows" for k in keys if counts.get(k, 0) > 1]
    findings += [f"row {k}: names no unit" for k in sorted(set(counts) - set(keys), key=str)]
    return findings


def check_2(root: Path = REPO_ROOT) -> list[str]:
    """Check 2: a scope-out names its destination, and a member destination carries an acceptance state."""
    rows, ceded, _findings = load(root)
    findings = []
    for item in [*rows, *ceded]:
        if item.get("disposition") != "scope-out":
            continue
        label = item.get("key") or item.get("legacy_file")
        if not item.get("destination"):
            findings.append(f"{label}: scope-out with no destination")
        elif item.get("destination_kind") == "member":
            acceptance = item.get("acceptance")
            ok = acceptance == "OPEN" or (
                isinstance(acceptance, dict) and acceptance.get("record_path") and acceptance.get("record_commit")
            )
            if not ok:
                findings.append(f"{label}: scope-out to a member with no acceptance state")
    return findings


def scope_out_counts(root: Path = REPO_ROOT) -> tuple[int, int]:
    """Return the number of scope-outs and how many of them are `OPEN`."""
    rows, ceded, _findings = load(root)
    outs = [item for item in [*rows, *ceded] if item.get("disposition") == "scope-out"]
    return len(outs), sum(1 for item in outs if item.get("acceptance") == "OPEN")


def check_3(root: Path = REPO_ROOT) -> list[str]:
    """Check 3: each row acknowledges exactly the runs the detector finds in its unit — none missing, none stale."""
    rows, _ceded, _findings = load(root)
    findings = []
    for row in rows:
        key = row.get("key", "")
        found = {f"{a}-{b}" for a, b in commented_runs.runs_in(key, root)}
        acknowledged = {run.get("lines") for run in row.get("commented_out_runs", []) if isinstance(run, dict)}
        for lines in sorted(found - acknowledged):
            findings.append(f"row {key}: does not acknowledge run {lines}")
        for lines in sorted(acknowledged - found, key=str):
            findings.append(f"row {key}: acknowledges {lines}, which is no run of the detector")
    return findings


def check_format(root: Path = REPO_ROOT) -> list[str]:
    """The ledger's format findings, as a whole-ledger check."""
    return load(root)[2]


WHOLE_LEDGER_CHECKS = (check_format, check_1, check_2, check_3)


def main(root: Path = REPO_ROOT) -> int:
    """Print the ledger's summary line; return 0 when every whole-ledger check is clean, 1 otherwise."""
    rows, ceded, _ = load(root)
    unit_count = sum(len(extent.extents(n, extent._read(root, n))[0]) for n in extent.retained_files(root))
    one = check_1(root)
    ceded_units = sum(extent.unit_count_without_preamble(extent._read(root, r["legacy_file"])) for r in ceded)
    outs, open_outs = scope_out_counts(root)
    print(
        f"units enumerated: {unit_count}; rows: {len(rows)}; "
        f"units with no row: {sum(1 for f in one if f.endswith(': no row'))}; "
        f"units with two or more rows: {sum(1 for f in one if f.endswith(' rows'))}; "
        f"rows naming no unit: {sum(1 for f in one if f.endswith('names no unit'))}; "
        f"ceded-file records: {len(ceded)}, covering {ceded_units} units; scope-outs: {outs}, of which OPEN: {open_outs}"
    )
    findings = [f for check in WHOLE_LEDGER_CHECKS for f in check(root)]
    for finding in findings:
        print(f"  {finding}", file=sys.stderr)
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
