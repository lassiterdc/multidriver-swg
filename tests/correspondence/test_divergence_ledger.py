"""Tests of the divergence ledger's reader: the shipped ledger is clean, each refusal fires once, and the reader stays standard-library.

Each refusal arm writes a ledger into `tmp_path` holding one well-formed pair entry and one
well-formed departure entry, then breaks exactly one thing, and asserts that exactly one finding
comes back naming that thing. So the well-formed entries are every arm's satisfying input, and a
refusal that fires on them reds the arm. The reader is reached only through
`divergence_ledger_load.reader()`, the one by-path load in the test tree.
"""

import ast
import subprocess
import sys
from pathlib import Path

import pytest

from tests.correspondence import divergence_ledger_load

pytestmark = pytest.mark.state_check

reader = divergence_ledger_load.reader()
LABEL = "canon-ast:1:" + "a" * 64
SIDE = f'{{ function = "m:f", combined = "{LABEL}", functions = {{ "m:f" = "{LABEL}" }} }}'
HEADER = 'rule = "divergence-ledger:1"\nstatement = "a statement"\n'
PAIR = {
    "id": '"p"',
    "pre_refactor": SIDE,
    "current": SIDE,
    "diverges": "true",
    "changed": '"the rounding"',
    "why": '"the old code truncated"',
    "judged_correct_when": '"the totals match to the cent"',
}
DEPARTURE = {
    "unit": '"_utils.py:1-18"',
    "site": "3",
    "kind": '"finding-4"',
    "grounds": '{ triage = "refactor_reminder:_utils.py:1-18:storm" }',
    "new_code": '"not yet written"',
    "why": '"the storm method is forked"',
    "judged_correct_when": '"the fork is ruled"',
    "future_pair": '"storm"',
    "fingerprint": f'{{ callable = "tests.registrations.x:storm", combined = "{LABEL}", functions = {{ "x:y" = "{LABEL}" }} }}',
}


def _entry(table: str, fields: dict[str, str]) -> str:
    """One `[[table]]` entry whose keys and TOML values are `fields`."""
    return f"\n[[{table}]]\n" + "".join(f"{key} = {value}\n" for key, value in fields.items())


def _findings(tmp_path: Path, pair: dict | None = None, departure: dict | None = None, extra: str = "") -> list[str]:
    """Validate a ledger of one pair entry and one departure entry, each the well-formed one unless replaced."""
    path = tmp_path / "divergence_ledger.toml"
    path.write_text(
        HEADER
        + extra
        + _entry("pair", PAIR if pair is None else pair)
        + _entry("departure", DEPARTURE if departure is None else departure),
        encoding="utf-8",
    )
    return reader.validate(reader.load(path))


def _without(fields: dict[str, str], key: str) -> dict[str, str]:
    """`fields` with `key` removed."""
    return {k: v for k, v in fields.items() if k != key}


def test_the_shipped_ledger_has_no_finding():
    """Property: the ledger the package ships validates with no finding.

    Class: the shipped ledger at every stage — no entry at landing, entries added as departures are
    recorded. Why: the State check job reaches the ledger's form only through this test, so an
    entry written in a wrong form would otherwise fail nothing. Killed by: a validator that finds
    something in a well-formed ledger, for example refusing a ledger with no entry. Second correct
    implementation: a validator that checks each entry against a schema table before any
    cross-field rule.
    """
    assert reader.validate(reader.load(reader.PATH)) == []


def test_the_well_formed_entries_have_no_finding(tmp_path):
    """Property: one well-formed pair entry and one well-formed departure entry validate with no finding.

    Class: the two entry kinds with every optional field the form allows. Why: every refusal arm
    below rests on these entries being clean, so a refusal that fires on them must red here first.
    Killed by: any refusal that reads a well-formed field as malformed. Second correct
    implementation: validating each field with a per-field predicate table.
    """
    assert _findings(tmp_path) == []


@pytest.mark.parametrize(
    ("where", "kwargs", "token"),
    [
        ("top level", {"extra": 'note = "x"\n'}, "note"),
        ("pair", {"pair": {**PAIR, "colour": '"red"'}}, "colour"),
        ("departure", {"departure": {**DEPARTURE, "colour": '"red"'}}, "colour"),
    ],
    ids=["top-level", "pair", "departure"],
)
def test_a_key_the_form_does_not_name_is_named(tmp_path, where, kwargs, token):
    """Property: a key the ledger's form does not name, at the top level or in either entry kind, is named.

    Class: any key outside the form, including a misspelt optional field. Why: entries filed under
    a misspelt table name, or a reason written under a misspelt key, would otherwise be ignored
    while the ledger reads as complete. Killed by: accepting any key. Second correct implementation:
    comparing each table's key set with its allowed set and naming the difference.
    """
    findings = _findings(tmp_path, **kwargs)
    assert len(findings) == 1 and token in findings[0], (where, findings)


@pytest.mark.parametrize(
    ("kind", "key"),
    [("pair", k) for k in ("id", "pre_refactor", "current", "diverges")]
    + [("departure", k) for k in ("unit", "site", "kind", "grounds", "new_code", "why", "judged_correct_when")],
)
def test_a_missing_required_key_of_each_entry_kind_is_named(tmp_path, kind, key):
    """Property: an entry lacking one of its kind's required keys is named, for every required key of both kinds.

    Class: each required key of a pair entry and of a departure entry, absent while the rest are
    present. Why: a departure entry without its grounds, or a pair without one of its sides,
    cannot be followed or compared, so it must not read as recorded. Killed by: skipping the
    required-key check. Second correct implementation: building each entry into a dataclass whose
    constructor raises on a missing field.
    """
    kwargs = {"pair": _without(PAIR, key)} if kind == "pair" else {"departure": _without(DEPARTURE, key)}
    findings = _findings(tmp_path, **kwargs)
    assert any(key in f and "absent" in f for f in findings), findings
    assert all(f.startswith(kind) for f in findings), findings


def test_a_kind_outside_the_three_is_named(tmp_path):
    """Property: a departure entry whose kind is not licensed, finding-3 or finding-4 is named with the value.

    Class: any kind string other than the three. Why: the kind says what allowed the departure,
    and an unknown kind says nothing a reader can act on. Killed by: accepting any kind. Second
    correct implementation: an enum of the three kinds looked up by value.
    """
    findings = _findings(tmp_path, departure={**_without(DEPARTURE, "future_pair"), "kind": '"waived"'})
    assert len(findings) == 1 and "waived" in findings[0], findings


def test_a_finding_4_entry_without_future_pair_is_named(tmp_path):
    """Property: a finding-4 departure entry that names no future pair is named.

    Class: a finding-4 entry with `future_pair` absent; and, as its satisfying input, a finding-3
    entry with none, which is not named. Why: a fourth finding records that the old behaviour
    will live on as a separate method, so the entry must say which pair will hold it. Killed by:
    dropping the check, or requiring `future_pair` of every kind. Second correct implementation:
    a per-kind table of required fields, with `future_pair` required for finding-4.
    """
    findings = _findings(tmp_path, departure=_without(DEPARTURE, "future_pair"))
    assert len(findings) == 1 and "future_pair" in findings[0], findings
    assert _findings(tmp_path, departure={**_without(DEPARTURE, "future_pair"), "kind": '"finding-3"'}) == []


@pytest.mark.parametrize("reason", ["changed", "why", "judged_correct_when"])
def test_a_diverging_pair_without_its_three_reasons_is_named(tmp_path, reason):
    """Property: a pair entry with `diverges = true` lacking any of its three reasons is named for that reason.

    Class: each of `changed`, `why` and `judged_correct_when`, absent on a diverging pair; and, as
    the satisfying input, a pair with `diverges = false` and none of them, which is not named.
    Why: a divergence nobody explained is the silent change the ledger exists to prevent. Killed
    by: dropping the reasons check, or requiring the reasons on a pair that does not diverge.
    Second correct implementation: requiring the three as a group with one finding listing the
    missing ones.
    """
    findings = _findings(tmp_path, pair=_without(PAIR, reason))
    assert len(findings) == 1 and reason in findings[0], findings
    not_diverging = {k: v for k, v in PAIR.items() if k not in ("changed", "why", "judged_correct_when")}
    assert _findings(tmp_path, pair={**not_diverging, "diverges": "false"}) == []


@pytest.mark.parametrize(
    "bad",
    [
        "canon-ast:1:" + "a" * 63,
        "canon-ast:1:" + "A" * 64,
        "canon-ast::" + "a" * 64,
        "sha256:" + "a" * 64,
    ],
    ids=["63-hex", "upper-case", "no-version", "other-rule"],
)
def test_a_label_outside_the_fingerprint_form_is_named(tmp_path, bad):
    """Property: a fingerprint label that is not `canon-ast:{n}:{64 lower-case hex}` is named.

    Class: a digest of the wrong length or case, a missing printer version, and another rule's
    prefix, in a pair side's `combined`. Why: a label is compared as a string with one the printer
    computes, so a label of another form can never match and would report a moved function on
    every run. Killed by: accepting any string as a label. Second correct implementation: splitting
    on the colons and checking each part.
    """
    side = SIDE.replace(f'combined = "{LABEL}"', f'combined = "{bad}"')
    findings = _findings(tmp_path, pair={**PAIR, "current": side})
    assert len(findings) == 1 and "combined" in findings[0], findings


def test_the_reader_imports_only_the_standard_library_and_nothing_of_its_package():
    """Property: every import statement in `divergence_ledger.py` names a standard-library module, and none names `multidriver_swg`.

    Class: each `import` and `from ... import` anywhere in the module, including a relative import.
    Why: the reader is loaded by path under an interpreter with none of the package's dependencies,
    and an import of its own package would run the package's `__init__`; either makes it fail
    there while every test in the project environment passes. Killed by: adding a third-party
    import, `from multidriver_swg import ...`, or `from . import ...`. Second correct
    implementation: a reader importing `re` lazily inside the label check.
    """
    tree = ast.parse(divergence_ledger_load.SOURCE.read_text(encoding="utf-8"))
    imported = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    imported |= {
        n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module and n.level == 0
    }
    relative = [n.lineno for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.level > 0]
    outside = sorted(imported - set(sys.stdlib_module_names) - {"__future__"})
    assert imported and not outside and "multidriver_swg" not in imported and not relative, (sorted(imported), relative)


CHILD = """
import sys
from tests.correspondence import divergence_ledger_load
first, second = divergence_ledger_load.reader(), divergence_ledger_load.reader()
print(first is second, "multidriver_swg" in sys.modules, isinstance(first.load(first.PATH), first.Ledger))
"""


def test_the_loader_returns_one_module_and_never_runs_the_package(tmp_path):
    """Property: `reader()` returns one module object across calls, its `Ledger` is the class `load` builds, and the package never imports.

    Class: a fresh interpreter calling `reader()` twice. Why: two module objects would hold two
    `Ledger` classes, so an `isinstance` across two consumers could fail, and an import of the
    package would run its `__init__` under an interpreter that lacks its dependencies. Killed by:
    loading the file afresh on each call, or importing the module by name. Second correct
    implementation: caching the loaded module in a module-level variable rather than reading it
    back from `sys.modules`.
    """
    run = subprocess.run(
        [sys.executable, "-c", CHILD],
        cwd=divergence_ledger_load.SOURCE.parents[2],
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin"},
    )
    assert run.returncode == 0, run.stderr
    assert run.stdout.split() == ["True", "False", "True"], run.stdout


NOT_DIVERGING = {
    **{k: v for k, v in PAIR.items() if k not in ("changed", "why", "judged_correct_when")},
    "diverges": "false",
}


@pytest.mark.parametrize(
    ("reason", "value"),
    [
        ("changed", '"the rounding"'),
        ("why", '"the old code truncated"'),
        ("judged_correct_when", '"the totals match"'),
        ("why", '""'),
    ],
    ids=["changed", "why", "judged_correct_when", "empty-why"],
)
def test_a_pair_that_does_not_diverge_carrying_a_reason_is_named(tmp_path, reason, value):
    """Property: a `diverges = false` pair carrying any of the three reasons, an empty one included, is named for that reason.

    Class: each of `changed`, `why` and `judged_correct_when` on a pair marked as not diverging,
    and an empty `why`; and, as the satisfying input, the same pair carrying none, which is not
    named. Why: the three reasons describe a change, so on a pair whose marker says nothing
    changed they would record a change that was not made, and a reader could not tell from the
    field alone whether it is true. One case per reason, because a validator refusing only one
    of them passes a single-reason case. Killed by: refusing only one reason, or admitting an
    empty one. Second correct implementation: comparing the set of reason keys present with the
    empty set on a `false` pair and naming each member of the difference.
    """
    assert _findings(tmp_path, pair=NOT_DIVERGING) == []
    findings = _findings(tmp_path, pair={**NOT_DIVERGING, reason: value})
    assert len(findings) == 1 and reason in findings[0] and "false" in findings[0], findings


def test_a_pair_carrying_a_departure_must_diverge(tmp_path):
    """Property: a pair entry carrying `departure` with `diverges = false` is named; the same pair diverging, with its reasons, is not.

    Class: every pair entry that replaced a fourth finding's departure entry, which is what a
    `departure` on a pair records. Why: a fourth finding is a deviation that is desired while the
    old behaviour must still be reproduced, so the pair that replaces its departure entry
    diverges by definition, and marking it `false` would record that nothing changed. Killed by:
    admitting a `departure` on a pair of either marker. Second correct implementation: requiring
    `diverges = true` whenever a pair's key set contains `departure`, checked before the reasons.
    """
    departure = '{ unit = "_inputs.py:30-41", site = 41 }'
    findings = _findings(tmp_path, pair={**NOT_DIVERGING, "departure": departure})
    assert len(findings) == 1 and "departure" in findings[0] and "diverges = true" in findings[0], findings
    assert _findings(tmp_path, pair={**PAIR, "departure": departure}) == []
