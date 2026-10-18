"""Print, in one line, how far the port of the legacy code has got, each count with its unit.

The refactor replaces the legacy scripts unit by unit, and nobody can see from the tree alone how
much of that is done. This module answers it in counts a reader can check, each with what it
counts: the files the corpus record lists and the files among them the parser cannot read; the
legacy units the extent rule enumerates, how many of them carry a ledger row, and how many are
ported; and how many equivalence tests have no recorded proof that they can fail, out of all of
them. The ceded files' units are printed beside the retained units and never added to them,
because the ledger records each ceded file once, as a scope-out, rather than unit by unit.

A unit is ported once one or more equivalence tests carry its key and each of them passed, as
recorded by a run given `--run-record`, against new code the unit's owning system registered (the record marks
such a pass `bound` true). Whether a test passed is a fact about one
run, so without a run record the ported count is `not measured` — except in a tree holding no
equivalence test at all, in which case no unit can be ported and the count is a measured 0. That exception is
what makes the declaration usable before the first equivalence test is written.

A test is unproven when the tracked proof records (`tests/correspondence/proofs.toml`) hold no row
for it. Only equivalence tests are counted: a refactor reminder is written to fail until an
equivalence test takes its place, so there is no proof of it to record.

Run as `python -m tests.correspondence.port_state [--run-record {path}]`; it prints the line and
exits 0. It writes nothing.
"""

from __future__ import annotations

import argparse
import json
import sys
import tomllib
from pathlib import Path

from tests.correspondence import corpus_record, extent, inventory, ledger, proofs

REPO_ROOT = corpus_record.REPO_ROOT
FAMILY = "equivalence"
NOT_MEASURED = "not measured"
RULE = (
    "a unit is ported once it has one or more equivalence tests and each of them passed "
    "in the given run against new code its system registered"
)


def _identity(item: dict) -> tuple:
    """The (family, unit, part) a test, a run-record line and a proof row are all keyed by."""
    return (item.get("family"), item.get("unit"), item.get("part"))


def _corpus_counts(root: Path) -> tuple[int, int]:
    """How many files the corpus record lists, and how many of its unit sources it records as unparseable.

    Read from the record rather than recomputed: the record's own check is what compares these
    with the tracked files, and a second recomputation here would be a second check of the same thing.
    """
    record = tomllib.loads((root / corpus_record.RECORD_NAME).read_text(encoding="utf-8"))
    sections = record.get("section", [])
    files = sum(len(section.get("file", [])) for section in sections)
    unreadable = sum(section.get("unparseable_candidate_files", 0) for section in sections)
    return files, unreadable


def _unit_keys(root: Path) -> list[str]:
    """Every retained unit's key, as the extent rule cuts the retained files."""
    keys: list[str] = []
    for name in extent.retained_files(root):
        found, _counts = extent.extents(name, extent._read(root, name))
        keys += [unit["key"] for unit in found]
    return keys


def _ceded_units(root: Path, ceded: list[dict]) -> int:
    """How many units the ceded-file records cover, counted as the ledger's own summary counts them."""
    return sum(extent.unit_count_without_preamble(extent._read(root, record["legacy_file"])) for record in ceded)


def _run_lines(path: Path | None) -> list[dict] | None:
    """The JSON lines of a run record, or None when no run record was supplied."""
    if path is None:
        return None
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def ported(unit_keys: list[str], tests: list[dict], run_lines: list[dict] | None) -> int | str:
    """The number of units that are ported, or `not measured` when no run record was supplied and a test exists.

    A test passed only if every line the run record holds for it reads outcome `passed` with `bound`
    true: the recorder writes a failed setup as its own line, and a test passing against the
    placeholder rather than a registered callable has not shown that any new code works. A test the
    run record does not mention did not pass in that run.
    """
    if run_lines is None:
        return 0 if not tests else NOT_MEASURED
    passed: dict[tuple, bool] = {}
    for line in run_lines:
        if line.get("family") != FAMILY:
            continue
        ok = line.get("outcome") == "passed" and line.get("bound") is True
        passed[_identity(line)] = passed.get(_identity(line), True) and ok
    by_unit: dict[str, list[bool]] = {}
    for test in tests:
        by_unit.setdefault(test["unit"], []).append(passed.get(_identity(test), False))
    return sum(1 for key in unit_keys if by_unit.get(key) and all(by_unit[key]))


def unproven(tests: list[dict], proof_rows: list[dict]) -> int:
    """The number of equivalence tests that no proof row names."""
    proven = {_identity(row) for row in proof_rows}
    return sum(1 for test in tests if _identity(test) not in proven)


def declaration(root: Path = REPO_ROOT, run_record: Path | None = None) -> dict:
    """Every count the declaration line prints, read from the tree under `root` and the optional run record."""
    files, unreadable = _corpus_counts(root)
    unit_keys = _unit_keys(root)
    rows, ceded, _format_findings = ledger.load(root)
    tests = [entry for entry in inventory.listing(root)[0] if entry["family"] == FAMILY]
    proof_rows, _proof_findings = proofs.load(root)
    return {
        "files": files,
        "unreadable_files": unreadable,
        "units": len(unit_keys),
        "dispositioned": len(set(unit_keys) & {row.get("key") for row in rows}),
        "ported": ported(unit_keys, tests, _run_lines(run_record)),
        "ceded_units": _ceded_units(root, ceded),
        "ceded_records": len(ceded),
        "unproven": unproven(tests, proof_rows),
        "equivalence_tests": len(tests),
    }


def summary(counts: dict) -> str:
    """The declaration line: each count beside what it counts, ending with when a unit is ported."""
    return (
        f"port state: files {counts['files']} in the corpus manifest, "
        f"{counts['unreadable_files']} the parser cannot read; "
        f"units {counts['units']} enumerated, {counts['dispositioned']} dispositioned, {counts['ported']} ported "
        f"({counts['ceded_units']} more units in {counts['ceded_records']} ceded-file scope-out records); "
        f"tests {counts['unproven']} unproven of {counts['equivalence_tests']} equivalence tests; {RULE}"
    )


def main(argv: list[str] | None = None, root: Path = REPO_ROOT) -> int:
    """Print the declaration line for the tree under `root`; return 0."""
    parser = argparse.ArgumentParser(prog="python -m tests.correspondence.port_state")
    parser.add_argument("--run-record", type=Path, default=None, help="a run record written with --run-record")
    args = parser.parse_args(argv)
    print(summary(declaration(root, args.run_record)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
