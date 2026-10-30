"""Reviewer test for the `sys` recorder that the combined-event-identification lifts bind into legacy callee texts.

The chunk's equivalence tests exercise a recorder only on the stop lines its fixtures reach, so a recorder that
returned at every line would give the same lift values; this test calls each declaration's recorder directly. It
builds nothing in the tracked tree.
"""

import pytest

from tests.equivalence import _support

STORED = "tests/correspondence/extractions/stormy/"
# unit -> (a callee function the declaration binds, the register stop-marker lines of the texts the lift runs)
RECORDERS = {
    "_b2_event_selection.py:273-324": ("compute_event_timeseries_statistics", [1274, 1563]),
    "_b2_event_selection.py:553-568": ("compute_event_timeseries_statistics", [1563]),
    "_b2_event_selection.py:588-617": ("determine_combined_event_statistics", [1563]),
}


def _recorder(unit, callee):
    """The `sys` recorder in the namespace the declaration compiled its callee texts into."""
    declaration = _support.find_declaration(unit)
    bindings, _reduce = declaration.function(_support.load_inputs(declaration))
    return bindings[callee].__globals__["sys"]


def _exit_at(recorder, directory, line):
    """Call `recorder.exit('boom')` from a statement at legacy `line` of a text compiled under `directory`'s stored path."""
    path = f"{STORED}{directory}/{line}-{line}.txt"
    exec(compile("\n" * (line - 1) + "sys.exit('boom')", path, "exec"), {"sys": recorder})


@pytest.mark.parametrize("unit", sorted(RECORDERS))
def test_the_sys_recorder_passes_only_the_registers_stop_lines_of_the_stored_utils_texts(unit):
    """Property: `sys.exit` returns at exactly the listed `_utils.py` stop lines and raises `SystemExit('boom')` at every other line, in another file's text, and for any other `sys` attribute an `AttributeError`.

    Class: any call of `exit` from a statement of a stored text; the satisfying inputs are the register's
    stop-marker lines of `_utils.py` (1274 and 1563 where the declaration lists them), the violating ones are
    the neighbouring lines, the one conditional exit at line 1277 that the register does not list, and a stop
    line number met in a text of another legacy file. Reachability: each call is made from a code object compiled
    under a stored-extraction path with leading blank lines, which is how the declaration compiles its callees,
    so the frame's file name and line number are those the recorder reads. Why: the register rules a stop marker
    passed and every other exit still a stop, so a recorder that returns at any line would let a legacy exit that
    the kept outputs never passed produce a value; the lift's own fixtures do not reach such an exit, and only
    this call does. The assertion turns on the recorder's verdict at a line, which the current recorder and a
    correct one both give. Killed by: `exit` returning at every line. Second correct implementation: keying the
    allowance on a set of (stored path, line) pairs instead of (file name, line).
    """
    callee, listed = RECORDERS[unit]
    recorder = _recorder(unit, callee)
    for line in listed:
        _exit_at(recorder, "_utils", line)
        with pytest.raises(SystemExit) as stopped:
            _exit_at(recorder, "_utils", line + 1)
        assert stopped.value.args == ("boom",)
        with pytest.raises(SystemExit):
            _exit_at(recorder, "_b2_event_selection", line)
    with pytest.raises(SystemExit):
        _exit_at(recorder, "_utils", 1277)
    with pytest.raises(AttributeError, match="argv"):
        _ = recorder.argv
