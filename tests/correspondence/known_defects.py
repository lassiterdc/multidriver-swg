"""Read the known-defect register and refuse a register whose form or keys are wrong.

The register (`tests/correspondence/known_defects.toml`) lists the defects found in the legacy
corpus, each keyed by its unit and a line inside it, with a ruling that says whether a test may
reproduce the defect. The committed register has no entry yet, because no part of the corpus
has been examined for defects — and an empty register must not read as a defect-free corpus. The
register therefore carries its own scope: `examined_scope` and `examination_command` both read
`UNEXAMINED` until an examination is stated, and then both are stated. The check keys on those
two fields agreeing, never on whether the entry list is empty, because an examined register with
no entry is a state the register must be able to represent. Run as a script it prints one summary
line and exits 0, or 1 on any finding.

Not established: that an entry's `line` lies inside its unit; only its form (an integer) is checked.
"""

from __future__ import annotations

import sys
import tomllib
from pathlib import Path

from tests.correspondence import extent

REPO_ROOT = extent.REPO_ROOT
REGISTER_PATH = Path("tests") / "correspondence" / "known_defects.toml"
RULE = "known-defect-register:1"
UNEXAMINED = "UNEXAMINED"
RULINGS = ("OBLIGED", "LICENSED", "UNRULED")
ENTRY_TEXT_FIELDS = ("defect", "kind", "reason")


def load(root: Path = REPO_ROOT) -> tuple[dict, list[str]]:
    """Return the register under `root` and every finding of its form and its keys."""
    register = tomllib.loads((root / REGISTER_PATH).read_text(encoding="utf-8"))
    findings: list[str] = []
    if register.get("rule") != RULE:
        findings.append(f"rule {register.get('rule')!r} is not {RULE!r}")
    scope, command = register.get("examined_scope"), register.get("examination_command")
    for field, value in (("examined_scope", scope), ("examination_command", command)):
        if not isinstance(value, str) or not value.strip():
            findings.append(f"{field} is absent or empty")
    if isinstance(scope, str) and isinstance(command, str) and (scope == UNEXAMINED) != (command == UNEXAMINED):
        findings.append(
            f"examined_scope {scope!r} and examination_command {command!r} disagree: "
            f"both read {UNEXAMINED} or both are stated"
        )
    for entry in register.get("entry", []):
        findings.extend(_entry_findings(entry, root))
    return register, findings


def _entry_findings(entry: dict, root: Path) -> list[str]:
    """Name each defect of one entry: its key resolves to no unit, its line is not an integer,
    a text field is missing, or its ruling is outside the three the register admits."""
    key, line = entry.get("key"), entry.get("line")
    label = f"entry {key} line {line}:"
    findings = []
    if not isinstance(key, str) or extent.resolve(key, root) is None:
        findings.append(f"{label} key resolves to no unit")
    if not isinstance(line, int) or isinstance(line, bool):
        findings.append(f"{label} line is not an integer")
    for field in ENTRY_TEXT_FIELDS:
        if not isinstance(entry.get(field), str) or not entry[field].strip():
            findings.append(f"{label} {field} is absent or empty")
    if entry.get("ruling") not in RULINGS:
        findings.append(f"{label} ruling {entry.get('ruling')!r} is not one of {', '.join(RULINGS)}")
    return findings


def main(root: Path = REPO_ROOT) -> int:
    """Print the register's state and entry count; return 0 when it has no finding, 1 otherwise."""
    register, findings = load(root)
    print(f"known-defect register: {register.get('examined_scope')}, {len(register.get('entry', []))} entries")
    for finding in findings:
        print(f"  {finding}", file=sys.stderr)
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
