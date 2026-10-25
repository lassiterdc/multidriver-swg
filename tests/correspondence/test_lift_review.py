"""Reviewer's test of the lift runner: a refusal comes before ANY range of the unit has run.

The runner promises to refuse an unknown name or an unread binding before it executes anything.
That promise is observable only through a side effect that outlives the refused call, so this test
gives the declaration a list that persists across calls (a default argument, evaluated once when the
declaration is defined) and lets the unit's first range append to it. A runner that checks and runs
one range at a time appends before it reaches the refusal; a runner that checks every range first
leaves the list empty.

Each test builds a small root in `tmp_path`: a corpus record naming one section, the unit's stored
texts at the paths the extraction store uses, and one lift module in `tests/lifts/`.
"""

from pathlib import Path

import pytest

from tests.correspondence import corpus_record, extraction, lift

pytestmark = pytest.mark.state_check

RECORD = 'rule = "corpus-record:1"\n\n[[section]]\nname = "stormy"\ndirectory = "legacy"\n'


def _root(tmp_path: Path, second_range: str, extra_binding: str) -> Path:
    """Build a root whose unit `mod.py:1-1,5-5` appends to a persistent list in range 1 and runs `second_range` in range 5."""
    (tmp_path / corpus_record.RECORD_NAME).write_text(RECORD, encoding="utf-8")
    for (first, last), text in {(1, 1): "seen.append(1)\n", (5, 5): second_range}.items():
        path = tmp_path / extraction.stored_path("stormy", "mod.py", first, last)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    source = (
        "from tests.correspondence.lift import declares_lift\n\n\n"
        '@declares_lift(unit="mod.py:1-1,5-5")\n'
        "def declare(fixtures, seen=[]):\n"
        '    """Supply the one list every call returns, so a test can read what an earlier run appended."""\n'
        f'    return {{"seen": seen{extra_binding}}}, lambda ns: seen\n'
    )
    (tmp_path / lift.LIFTS).mkdir(parents=True)
    (tmp_path / lift.LIFTS / "sys_a.py").write_text(source, encoding="utf-8")
    return tmp_path


@pytest.mark.parametrize(
    ("second_range", "extra_binding", "named"),
    [
        ("x = missing + 1\n", "", "missing"),
        ("x = 1\n", ', "spare": 1', "spare"),
    ],
    ids=["unknown-name-in-the-later-range", "binding-no-range-reads"],
)
def test_a_refusal_comes_before_any_range_of_the_unit_has_run(tmp_path, second_range, extra_binding, named):
    """Property: when a unit is refused for an unknown name or an unread binding, not even its first range has executed.

    Class: a two-range unit whose first range has a side effect and whose refusal cause lies in the
    second range, or in no range at all (a binding none reads). Why it is required: a refusal after a
    range has run leaves a half-run unit whose side effects a caller cannot undo, and the refusal's
    whole purpose is to stop before any of the text runs. Killed by: checking and executing one range
    at a time, which appends to the list before it reaches range 5's unknown name, and checking
    unread bindings only after the ranges have run. Second correct implementation: building one
    combined name check over every compiled range, then executing, passes.
    """
    root = _root(tmp_path, second_range, extra_binding)
    (declaration,) = lift.discover(root)
    with pytest.raises(lift.LiftError, match=named):
        lift.run_lift(declaration)
    persistent = declaration.function({})[0]["seen"]
    assert persistent is declaration.function({})[0]["seen"]
    assert persistent == []
