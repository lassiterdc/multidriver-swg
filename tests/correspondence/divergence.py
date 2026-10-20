"""Report every disagreement between the divergence ledger and the code it describes.

The divergence ledger records each deliberate departure from the legacy code, with the
fingerprint of the code the record was written against. A record goes stale when that code
changes and nobody revisits it. This module recomputes the fingerprints from the source,
compares them with the ledger, and names every place the two disagree, so a stale or missing
record stops the build rather than passing unnoticed.

Order of evaluation:
  1. The printer gate (G). If this interpreter prints the fixed check function differently from
     the stored label, every fingerprint comparison would report a move that is the printer's,
     not the code's. So the detector reports that one finding and measures nothing else.
  2. Structural findings: every finding of the pair scan, and every form defect of the ledger.
     A malformed ledger entry is named here once and is not compared by any case, so it
     cannot raise inside one.
  3. The cases, over well-formed entries only:
     P1/P2 — a pair entry's recorded `current` / `pre_refactor` labels differ from the
             closure computed now; the finding names each moved function.
     P3    — a pair entry names no scanned pair.
     P4    — a scanned pair has no pair entry; the finding names both sites.
     D1    — a departure entry's recorded fingerprint differs from the closure of its callable.
     D2    — a departure entry's unit is registered by some registration module, but the entry
             records no fingerprint.

`main` (run as `python3 -m tests.correspondence.divergence` from the repository root) prints the
denominators, then every finding, and returns 1 on any finding, else 0. Only the standard library
and standard-library modules of this directory are imported, so a pre-commit hook's interpreter,
which has no `pytest`, can run it.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path

from tests.correspondence import divergence_ledger_load, fingerprint, pair_scan, registration_read

REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE = "multidriver_swg"
LEDGER_PATH = Path("src") / "multidriver_swg" / "divergence_ledger.toml"
REGISTRATIONS = Path("tests") / "registrations"
SIDES = ("pre_refactor", "current")
SIDE_CASE = {"current": "P1", "pre_refactor": "P2"}


@dataclass
class Report:
    """What the detector measured, as one line, and every finding, in evaluation order."""

    denominators: str
    findings: list[str] = field(default_factory=list)


def detect(root: Path = REPO_ROOT, package: str = PACKAGE) -> Report:
    """Compare the ledger under `root` with the source under `root/src/{package}` and the registration modules."""
    root = Path(root)
    gate = fingerprint.printer_check()
    if gate is not None:
        return Report("not measured: the printer check failed", [f"G: {gate}; P1 to D2 were not measured"])
    package_dir = root / "src" / package
    scan = pair_scan.scan_pairs(package_dir, package)
    findings = [f"scan: {finding}" for finding in scan.findings]
    closures = pair_scan.pair_closures(scan, package_dir, package)
    reader = divergence_ledger_load.reader()
    ledger_path = root / LEDGER_PATH
    if not ledger_path.is_file():
        findings.append(f"ledger: no divergence ledger at {LEDGER_PATH.as_posix()}")
        return Report(pair_scan.denominators(scan, closures), findings)
    ledger = reader.load(ledger_path)
    findings += [f"ledger: {finding}" for finding in reader.validate(ledger)]
    pairs = [e for e in ledger.pairs if _well_formed(reader, ledger, pair=e)]
    departures = [e for e in ledger.departures if _well_formed(reader, ledger, departure=e)]
    findings += _pair_cases(pairs, scan, closures)
    findings += _departure_cases(departures, root, package_dir, package)
    line = f"{pair_scan.denominators(scan, closures)}; ledger: {len(ledger.pairs)} pair entries, {len(ledger.departures)} departure entries"
    return Report(line, findings)


def _well_formed(reader, ledger, pair: dict | None = None, departure: dict | None = None) -> bool:
    """Whether one entry, validated alone under the ledger's own header, has no finding of its own."""
    alone = reader.Ledger(
        [pair] if pair is not None else [], [departure] if departure is not None else [], ledger.header
    )
    return not [f for f in reader.validate(alone) if not f.startswith("header:")]


def _pair_cases(pairs: list[dict], scan, closures: dict) -> list[str]:
    """P1 to P4: each pair entry against the scanned pairs and their closures, and each scanned pair against the entries."""
    findings = []
    recorded_ids = {e["id"] for e in pairs}
    for entry in pairs:
        pair = entry["id"]
        if pair not in scan.pairs:
            findings.append(f"P3: pair entry {pair} names no scanned pair")
            continue
        for side in SIDES:
            if side not in closures.get(pair, {}):
                continue  # a one-sided pair is already a scan finding
            moved = _moved(entry[side], closures[pair][side])
            if moved:
                findings.append(
                    f"{SIDE_CASE[side]}: pair entry {pair} {side} ({entry[side]['function']}) moved: {', '.join(moved)}"
                )
    for pair, sides in sorted(scan.pairs.items()):
        if pair not in recorded_ids:
            sites = "; ".join(
                f"{side} at {path}:{line} ({qualname})" for side, (path, qualname, line) in sorted(sides.items())
            )
            findings.append(f"P4: scanned pair {pair} has no pair entry; {sites}")
    return findings


def _departure_cases(departures: list[dict], root: Path, package_dir: Path, package: str) -> list[str]:
    """D1 and D2: each departure entry's fingerprint against its callable's closure, and against the registered units."""
    registered, unreadable = _registered_units(root)
    findings = [f"registration: {finding}" for finding in unreadable]
    for entry in departures:
        label = f"departure entry {entry['unit']} site {entry['site']}"
        recorded = entry.get("fingerprint")
        if recorded is None:
            if entry["unit"] in registered:
                findings.append(
                    f"D2: {label} records no fingerprint, but {registered[entry['unit']]} registers {entry['unit']}"
                )
            continue
        module, _, function = recorded["callable"].partition(":")
        path = root.joinpath(*module.split(".")).with_suffix(".py")
        try:
            computed = pair_scan.closure(path, function, package_dir, package)
        except (OSError, ValueError) as error:
            findings.append(f"D1: {label}: its callable {recorded['callable']} cannot be read ({error})")
            continue
        moved = _moved(recorded, computed)
        if moved:
            findings.append(f"D1: {label} ({recorded['callable']}) moved: {', '.join(moved)}")
    return findings


def _moved(recorded: dict, computed) -> list[str]:
    """The functions whose recorded label differs from the computed one, or a note when only the combined label moved."""
    keys = sorted(set(recorded["functions"]) | set(computed.functions))
    moved = [k for k in keys if recorded["functions"].get(k) != computed.functions.get(k)]
    if not moved and recorded["combined"] != computed.combined:
        moved = ["a module-level name a function reads"]
    return moved


def _registered_units(root: Path) -> tuple[dict[str, str], list[str]]:
    """Each unit a registration module under `root` registers, mapped to its module, and every form the reader cannot read.

    The reader's findings are returned, not dropped: a registration it cannot read is one D2
    cannot see, so it must be named rather than leave D2 silently incomplete.
    """
    units, unreadable = {}, []
    for path in sorted((root / REGISTRATIONS).glob("*.py")):
        if path.name == "__init__.py":
            continue
        module = ".".join(path.relative_to(root).with_suffix("").parts)
        read, findings = registration_read.registered_units(path)
        unreadable += findings
        for unit in read:
            units.setdefault(unit, module)
    return units, unreadable


def main(root: Path = REPO_ROOT) -> int:
    """Print the denominators and every finding; return 1 on any finding, else 0."""
    report = detect(root)
    print(report.denominators)
    for finding in report.findings:
        print(f"  {finding}")
    return 1 if report.findings else 0


if __name__ == "__main__":
    sys.exit(main())
