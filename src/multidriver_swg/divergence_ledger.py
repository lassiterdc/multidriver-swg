"""Read the divergence ledger and refuse a ledger whose entries cannot be relied on.

Some new code is meant to compute something different from the legacy code it replaces. Each
such deliberate departure is recorded once in `divergence_ledger.toml`, beside this module, so
that a later reader — a person, a check, or a report run with the installed package — can learn
what changed, why, and how to judge that the new behaviour is right. There are two kinds of
entry. A PAIR entry describes a legacy function and its replacement registered as a pair, with
the fingerprint of the code each side reaches and either the reasons it diverges or an explicit
statement that it does not. A DEPARTURE entry describes a departure for which no pair exists:
which legacy unit and line it concerns, on what grounds it was allowed, and, once the new code
is registered, the fingerprint of that code.

`load` reads a ledger file and `validate` names every entry or field that does not have the
ledger's form. A ledger written by hand can be wrong in ways nobody notices until someone tries
to follow it, so a key the form does not name is refused rather than ignored.

This module imports only the standard library and nothing from its own package. A pre-commit
hook runs under an interpreter that has none of the package's dependencies, and test-tree code
loads this file by its path so that the package's `__init__` never runs there.

Not established: that a unit key names a real legacy unit, that a `grounds.register` key names a
register entry, or that a fingerprint matches the code it describes. Only the form is checked
here; resolving entries against the code is the detector's work.
"""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

PATH = Path(__file__).with_name("divergence_ledger.toml")
RULE = "divergence-ledger:1"
TOP_LEVEL_KEYS = ("rule", "statement", "pair", "departure")
PAIR_REQUIRED = ("id", "pre_refactor", "current", "diverges")
PAIR_REASONS = ("changed", "why", "judged_correct_when")
PAIR_KEYS = (*PAIR_REQUIRED, *PAIR_REASONS, "departure")
SIDE_KEYS = ("function", "combined", "functions")
DEPARTURE_REQUIRED = ("unit", "site", "kind", "grounds", "new_code", "why", "judged_correct_when")
DEPARTURE_KEYS = (*DEPARTURE_REQUIRED, "fingerprint", "future_pair")
FINGERPRINT_KEYS = ("callable", "combined", "functions")
KINDS = ("licensed", "finding-3", "finding-4")
LABEL_RE = re.compile(r"canon-ast:\d+:[0-9a-f]{64}")


@dataclass
class Ledger:
    """A ledger's entries and its header, as read from one file."""

    pairs: list[dict] = field(default_factory=list)
    departures: list[dict] = field(default_factory=list)
    header: dict = field(default_factory=dict)  # every top-level key that is not an entry list


def load(path: Path = PATH) -> Ledger:
    """Read the ledger at `path` into its pair entries, its departure entries and its header."""
    table = tomllib.loads(Path(path).read_text(encoding="utf-8"))
    header = {key: value for key, value in table.items() if key not in ("pair", "departure")}
    return Ledger(list(table.get("pair", [])), list(table.get("departure", [])), header)


def validate(ledger: Ledger) -> list[str]:
    """Every finding of the ledger's header and its entries; an empty list means the ledger has the form."""
    findings = _header_findings(ledger.header)
    for index, entry in enumerate(ledger.pairs, 1):
        findings += _pair_findings(f"pair {index}", entry)
    for index, entry in enumerate(ledger.departures, 1):
        findings += _departure_findings(f"departure {index}", entry)
    return findings


def _header_findings(header: dict) -> list[str]:
    """Name a top-level key the form does not name, a rule that is not this ledger's, and an empty statement."""
    findings = [
        f"header: key {k!r} is not one of {', '.join(TOP_LEVEL_KEYS)}" for k in header if k not in TOP_LEVEL_KEYS
    ]
    if header.get("rule") != RULE:
        findings.append(f"header: rule {header.get('rule')!r} is not {RULE!r}")
    if not _text(header.get("statement")):
        findings.append("header: statement is absent or empty")
    return findings


def _pair_findings(label: str, entry: object) -> list[str]:
    """Name each defect of one pair entry: its keys, its two sides, its divergence flag and the reasons that flag requires."""
    if not isinstance(entry, dict):
        return [f"{label}: is not a table"]
    findings = _key_findings(label, entry, PAIR_KEYS, PAIR_REQUIRED)
    if "id" in entry and not _text(entry["id"]):
        findings.append(f"{label}: id is not a non-empty string")
    for side in ("pre_refactor", "current"):
        if side in entry:
            findings += _labelled_code_findings(f"{label} {side}", entry[side], SIDE_KEYS, "function")
    diverges = entry.get("diverges")
    if "diverges" in entry and not isinstance(diverges, bool):
        findings.append(f"{label}: diverges {diverges!r} is not true or false")
    # The three reasons explain a divergence, so they are required exactly when it diverges:
    # a reason on a pair that does not diverge would describe a change that was not made.
    for reason in PAIR_REASONS:
        if diverges is True and not _text(entry.get(reason)):
            findings.append(f"{label}: diverges = true but {reason} is absent or empty")
        if diverges is False and reason in entry:
            findings.append(f"{label}: diverges = false but it carries {reason}")
    # A pair carrying `departure` replaced a fourth finding's departure entry, and a fourth
    # finding is a desired deviation whose old behaviour must still be reproduced, so the pair
    # diverges by definition; marked `false`, it would record that nothing changed.
    if "departure" in entry and diverges is False:
        findings.append(f"{label}: carries a departure, so it replaced a fourth finding and must be diverges = true")
    if "departure" in entry and not _pointer(entry["departure"]):
        findings.append(f"{label}: departure {entry['departure']!r} is not {{ unit = {{unit key}}, site = {{line}} }}")
    return findings


def _departure_findings(label: str, entry: object) -> list[str]:
    """Name each defect of one departure entry: its keys, its pointer, its kind, its grounds and its fingerprint."""
    if not isinstance(entry, dict):
        return [f"{label}: is not a table"]
    findings = _key_findings(label, entry, DEPARTURE_KEYS, DEPARTURE_REQUIRED)
    if ("unit" in entry or "site" in entry) and not _pointer({"unit": entry.get("unit"), "site": entry.get("site")}):
        findings.append(
            f"{label}: unit {entry.get('unit')!r} and site {entry.get('site')!r} are not a unit key and a line of 1 or more"
        )
    kind = entry.get("kind")
    if "kind" in entry and kind not in KINDS:
        findings.append(f"{label}: kind {kind!r} is not one of {', '.join(KINDS)}")
    if kind == "finding-4" and not _text(entry.get("future_pair")):
        findings.append(f"{label}: kind finding-4 has no future_pair")
    if kind != "finding-4" and "future_pair" in entry:
        findings.append(f"{label}: future_pair is carried by kind {kind!r}, which only finding-4 does")
    if "grounds" in entry and not _grounds(entry["grounds"]):
        findings.append(
            f"{label}: grounds {entry['grounds']!r} is not {{ register = {{ key, line }} }} or {{ triage = {{test}} }}"
        )
    for field_name in ("new_code", "why", "judged_correct_when"):
        if field_name in entry and not _text(entry[field_name]):
            findings.append(f"{label}: {field_name} is empty")
    if "fingerprint" in entry:
        findings += _labelled_code_findings(f"{label} fingerprint", entry["fingerprint"], FINGERPRINT_KEYS, "callable")
    return findings


def _key_findings(label: str, entry: dict, allowed: tuple[str, ...], required: tuple[str, ...]) -> list[str]:
    """Name each key the entry's form does not name and each required key it lacks."""
    findings = [f"{label}: key {k!r} is not one of {', '.join(allowed)}" for k in entry if k not in allowed]
    return findings + [f"{label}: required key {k!r} is absent" for k in required if k not in entry]


def _labelled_code_findings(label: str, value: object, keys: tuple[str, ...], name_key: str) -> list[str]:
    """Name each defect of a side or a fingerprint: its keys, the code it names, and every label's form."""
    if not isinstance(value, dict):
        return [f"{label}: is not a table"]
    findings = _key_findings(label, value, keys, keys)
    if name_key in value and not _qualified(value[name_key]):
        findings.append(f"{label}: {name_key} {value[name_key]!r} is not {{module}}:{{qualified name}}")
    if "combined" in value and not _label(value["combined"]):
        findings.append(f"{label}: combined {value['combined']!r} is not canon-ast:{{n}}:{{64 hex}}")
    functions = value.get("functions")
    if "functions" in value and not isinstance(functions, dict):
        findings.append(f"{label}: functions is not a table")
    elif isinstance(functions, dict):
        for key, function_label in functions.items():
            if not _qualified(key):
                findings.append(f"{label}: functions key {key!r} is not {{module}}:{{qualified name}}")
            if not _label(function_label):
                findings.append(
                    f"{label}: functions {key!r} label {function_label!r} is not canon-ast:{{n}}:{{64 hex}}"
                )
    return findings


def _text(value: object) -> bool:
    """Whether `value` is a string with something other than whitespace in it."""
    return isinstance(value, str) and bool(value.strip())


def _label(value: object) -> bool:
    """Whether `value` is a fingerprint label, `canon-ast:{printer version}:{64 lower-case hex}`."""
    return isinstance(value, str) and LABEL_RE.fullmatch(value) is not None


def _qualified(value: object) -> bool:
    """Whether `value` reads `{module}:{qualified name}`, both parts non-empty."""
    if not isinstance(value, str):
        return False
    module, sep, name = value.partition(":")
    return bool(sep and module.strip() and name.strip())


def _pointer(value: object) -> bool:
    """Whether `value` is `{ unit = "{unit key}", site = {line} }` with a non-empty unit and a line of 1 or more."""
    if not isinstance(value, dict) or set(value) != {"unit", "site"}:
        return False
    site = value["site"]
    return _text(value["unit"]) and isinstance(site, int) and not isinstance(site, bool) and site >= 1


def _grounds(value: object) -> bool:
    """Whether `value` is exactly one of `{ register = { key, line } }` and `{ triage = "{test}" }`."""
    if not isinstance(value, dict) or len(value) != 1:
        return False
    if "triage" in value:
        return _text(value["triage"])
    register = value.get("register")
    if not isinstance(register, dict) or set(register) != {"key", "line"}:
        return False
    line = register["line"]
    return _text(register["key"]) and isinstance(line, int) and not isinstance(line, bool) and line >= 1
