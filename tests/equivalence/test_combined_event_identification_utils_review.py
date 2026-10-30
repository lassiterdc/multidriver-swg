"""Reviewer test for the `sys` recorders of the three `_utils.py` lifts of combined-event-identification that carry one.

The chunk's equivalence tests reach a recorder only on the stop lines their fixtures run, so a recorder that returned
at every line gives the same lift values; this test calls each recorder directly, in both ways its lift's texts reach
it: from a text compiled under a legacy `.py` path (the unit's own text) and from one compiled under a stored `.txt`
path (a callee). It builds nothing in the tracked tree.
"""

import pytest

from tests.equivalence import _support

STORED = "tests/correspondence/extractions/stormy/_utils/"
LEGACY = "somewhere/_utils.py"
# unit -> the register stop-marker lines of `_utils.py` that its lift passes
RECORDERS = {
    "_utils.py:1269-1422": [1274],
    "_utils.py:1540-1577": [1563],
    "_utils.py:1786-1989": [1563],
}


def _recorder(unit):
    """The `sys` binding the declaration hands to the legacy texts."""
    declaration = _support.find_declaration(unit)
    bindings, _reduce = declaration.function(_support.load_inputs(declaration))
    return bindings["sys"]


def _exit_at(recorder, path, line):
    """Call `recorder.exit('boom')` from a statement at legacy `line` of a text compiled under `path`."""
    exec(compile("\n" * (line - 1) + "sys.exit('boom')", path, "exec"), {"sys": recorder})


@pytest.mark.parametrize("unit", sorted(RECORDERS))
def test_the_sys_recorder_passes_only_the_registers_stop_lines_whether_the_text_is_a_unit_or_a_callee(unit):
    """Property: `sys.exit` returns at exactly the listed `_utils.py` stop lines and raises `SystemExit('boom')` at every other line or in another file, for a frame in a legacy `.py` path and for one in a stored `.txt` path.

    Class: any call of `exit` from a statement of a legacy text; the satisfying inputs are the register's stop lines
    of `_utils.py` under the legacy path and under the stored path, the violating ones the neighbouring lines, the
    conditional exit at legacy line 1277 that the register does not list, and the stop line in a file of another
    name. Reachability: the code objects are compiled with leading blank lines under the two path shapes the
    declarations' texts have (the unit's own text under the legacy file path, a callee under its stored path), so the
    recorder reads the file name and line number that a lift run gives it. Why: the register rules a stop marker
    passed and every other exit still a stop; the lifts' fixtures reach no other exit, so only this call tells a
    recorder that passes everything from one that passes the register's lines. The assertion turns on the verdict at
    a line, which the current and a correct recorder both give. Killed by: `exit` returning at every line. Second
    correct implementation: a set of (path stem, line) pairs in place of (file name, line).
    """
    recorder = _recorder(unit)
    for line in RECORDERS[unit]:
        for path in (LEGACY, f"{STORED}1540-1577.txt"):
            _exit_at(recorder, path, line)
            with pytest.raises(SystemExit) as stopped:
                _exit_at(recorder, path, line + 1)
            assert stopped.value.args == ("boom",)
        with pytest.raises(SystemExit):
            _exit_at(recorder, "somewhere/_other.py", line)
    with pytest.raises(SystemExit):
        _exit_at(recorder, LEGACY, 1277)
    with pytest.raises(AttributeError, match="argv"):
        _ = recorder.argv
