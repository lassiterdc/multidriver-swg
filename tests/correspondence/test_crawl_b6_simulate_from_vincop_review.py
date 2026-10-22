"""Checks that every refactor reminder sits in the file its own marker names.

A reminder file is named for one owning system and one legacy script,
`tests/reminders/test_{owner with hyphens as underscores}__{script stem}.py`, so that the later
work which replaces reminders with equivalence tests can take one owner's files without touching
another owner's. The ownership checks read each reminder's `system=` and `unit=` marker arguments
and never the file the reminder is written in, so a reminder whose marker is right and whose file
is wrong passes every one of them. This module reads the two together.
"""

import ast
from pathlib import Path

import pytest

pytestmark = pytest.mark.state_check

REMINDERS = Path(__file__).resolve().parents[1] / "reminders"


def _stem(script):
    """Return the file-name stem of a legacy script: no `.py`, no leading underscores, dots as underscores."""
    return script.removesuffix(".py").lstrip("_").replace(".", "_")


def _reminder_markers(path):
    """Yield (function name, unit, system) for each `refactor_reminder` decorator in one file."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if not isinstance(node, ast.FunctionDef):
            continue
        for deco in node.decorator_list:
            if isinstance(deco, ast.Call) and getattr(deco.func, "attr", None) == "refactor_reminder":
                args = {k.arg: k.value.value for k in deco.keywords if isinstance(k.value, ast.Constant)}
                yield node.name, args.get("unit"), args.get("system")


def test_every_reminder_is_in_the_file_its_owner_and_script_name():
    """Property: each reminder's file is named by its marker's system and by its unit's legacy script.

    Class: every `refactor_reminder` decorator in every `test_*.py` file under `tests/reminders/`,
    tracked or not, for every script and every owner. Why: one owner's later replacement work takes
    that owner's reminder files by name, so a reminder filed under another owner's name is either
    missed by its own owner or edited by the wrong one. The ownership checks read only the marker,
    so on a reminder whose marker says `dependence-modelling` and whose file is the
    `event-occurrence-modelling` file they report no finding. Killed by: moving one reminder
    function, with its marker unchanged, into another owner's file of the same script. Second
    correct implementation: one file per owner holding the reminders in any order, with extra
    blank lines or comments, and functions named differently; none of that is asserted.
    """
    files = sorted(REMINDERS.glob("test_*.py"))
    assert files, f"no reminder files under {REMINDERS}"
    misplaced = []
    for path in files:
        for function, unit, system in _reminder_markers(path):
            expected = f"test_{system.replace('-', '_')}__{_stem(unit.split(':')[0])}.py"
            if path.name != expected:
                misplaced.append(f"{path.name}::{function} (unit={unit}, system={system}) belongs in {expected}")
    assert not misplaced, "\n".join(misplaced)
