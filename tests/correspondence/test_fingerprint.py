"""Tests of the canonical printer: what moves a function's fingerprint, what does not, and the printer check.

A fingerprint is useful only if it moves exactly when the function's behaviour could have
changed. So each test pairs sources that must print alike — the same function reformatted,
commented, re-documented, or carrying only the decorators that record it — with sources that
must print differently: a changed default, an added behaviour-changing decorator, and a
different kind of function. Every source here is parsed from a string, never imported.
"""

import ast
import hashlib
import sys
import textwrap

import pytest

from tests.correspondence import fingerprint

pytestmark = pytest.mark.state_check

BASE = '''
def total(values, start=0):
    """Add the values to start."""
    result = start
    for value in values:
        result += value
    return result
'''


def _label(source: str) -> str:
    """The fingerprint label of the first function in `source`."""
    function = ast.parse(textwrap.dedent(source)).body[0]
    return fingerprint.label(fingerprint.canonical(fingerprint.for_fingerprint(function)))


def test_layout_comments_and_the_docstring_leave_the_label_unchanged():
    """Property: reformatting, comments and any change to the leading docstring leave a function's label unchanged.

    Class: sources that differ from one function only in whitespace, line breaks, redundant
    parentheses, comments, and the leading docstring's text, presence or absence. Why: none of
    these changes what the function does, so a fingerprint that moved on them would report a
    departure record as stale when its function had not changed. Killed by: hashing the source
    text, keeping line positions, or keeping the docstring. Second correct implementation: a
    printer that writes every field, empty ones included, which differs in output and still
    prints these sources alike.
    """
    same = {
        "reformatted": "def total( values , start = 0 ):\n    result=(start)\n"
        "    for value in values :\n        result += (value)\n\n    return result\n",
        "commented": BASE.replace("    result = start\n", "    # begin at start\n    result = start  # running sum\n"),
        "docstring changed": BASE.replace("Add the values to start.", "Sum."),
        "docstring removed": BASE.replace('    """Add the values to start."""\n', ""),
    }
    base = _label(BASE)
    assert {name: _label(source) == base for name, source in same.items()} == dict.fromkeys(same, True)


def test_a_changed_default_or_an_added_decorator_moves_the_label():
    """Property: a changed default argument or an added behaviour-changing decorator moves a function's label.

    Class: sources that differ from one function in a default value, or in a decorator other than
    `pair_side` and `registers`, here `functools.cache`. Why: either changes what calling the
    function does, so a fingerprint that stayed put would let a departure record vouch for code
    it never described. Killed by: dropping defaults from the printing, or removing every
    decorator rather than only the recording ones. Second correct implementation: a printer that
    sorts each node's fields by name, which prints differently and still separates these.
    """
    moved = {
        "default changed": BASE.replace("start=0", "start=1"),
        "cache added": "@functools.cache\n" + BASE.lstrip("\n"),
    }
    base = _label(BASE)
    assert {name: _label(source) != base for name, source in moved.items()} == dict.fromkeys(moved, True)


def test_recording_decorators_are_removed_and_the_input_is_left_alone():
    """Property: a function carrying `pair_side` or `registers` labels as the same function without them.

    Class: either recording decorator, written as a bare call, as an attribute of a module, or
    both stacked, above or below a kept decorator. Why: registering a function only records it,
    so the act of registering must not move the fingerprint a record stores for it. The copy is
    asserted too, because a caller may print the same node again. Killed by: keeping every
    decorator, removing only the first, or editing the input node in place. Second correct
    implementation: rebuilding the function node field by field without the recording
    decorators instead of deep-copying it.
    """
    base = _label(BASE)
    recorded = [
        "@pair_side(pair='p', side=CURRENT)\n" + BASE.lstrip("\n"),
        "@binding.registers(unit='a.py:1-2')\n" + BASE.lstrip("\n"),
        "@pairs.pair_side(pair='p', side='current')\n@registers(unit='a.py:1-2')\n" + BASE.lstrip("\n"),
    ]
    assert [_label(source) for source in recorded] == [base] * len(recorded)
    kept = "@functools.cache\n@registers(unit='a.py:1-2')\n" + BASE.lstrip("\n")
    assert _label(kept) == _label("@functools.cache\n" + BASE.lstrip("\n")) != base
    node = ast.parse(recorded[2]).body[0]
    fingerprint.for_fingerprint(node)
    assert len(node.decorator_list) == 2 and ast.get_docstring(node) == "Add the values to start."


def test_ctx_never_appears_in_the_canonical_printing():
    """Property: the canonical printing carries no `ctx` field, for names that are read, assigned or deleted.

    Class: every expression with a load, store or delete context — names, attributes,
    subscripts, starred targets, tuple and list targets. Why: `ctx` restates what the node's
    position already fixes, and it is the field whose representation has differed between
    interpreter versions. Killed by: printing every field. Second correct implementation: a
    printer that writes the fields as keyword-sorted pairs and also skips `ctx`. Control: the
    same tree under `ast.dump` must carry `ctx`, or the source exercised no context at all.
    """
    source = "def f(a):\n    x, *y = a.b[0]\n    [c] = y\n    del x, a.b\n    return c\n"
    tree = ast.parse(source)
    assert "ctx" in ast.dump(tree)
    printed = fingerprint.canonical(tree)
    assert "ctx" not in printed and "Load" not in printed and "Store" not in printed and "Del(" not in printed, printed


def test_an_async_def_prints_and_fingerprints_as_an_async_def():
    """Property: `for_fingerprint` accepts an `async def`, returns an `async def`, and drops its recording decorators and docstring.

    Class: every `async def` a registration module may register. Why: the registration reader
    hands out async functions as well as plain ones, so a fingerprint of a registered function
    must not fail, or silently print it as a plain `def`, on that input. Killed by: rebuilding the
    copy as an `ast.FunctionDef`, or reading only `ast.FunctionDef` nodes. Second correct
    implementation: one that copies the node with `ast.AsyncFunctionDef(**fields)` instead of a
    deep copy.
    """
    async_source = BASE.replace("def total", "async def total")
    node = ast.parse(textwrap.dedent(async_source)).body[0]
    prepared = fingerprint.for_fingerprint(node)
    assert isinstance(prepared, ast.AsyncFunctionDef) and fingerprint.canonical(prepared).startswith(
        "AsyncFunctionDef("
    )
    recorded = "@registers(unit='a.py:1-2')\n" + async_source.lstrip("\n")
    assert _label(recorded) == _label(async_source) != _label(BASE)


def test_a_label_is_the_rule_the_version_and_a_sha256():
    """Property: a label reads `canon-ast:1:` followed by the 64-hex SHA-256 of the printing's UTF-8 bytes.

    Class: any printed text, ASCII or not. Why: a recorded label is compared as a string, so its
    form is the contract between the record and the detector. Killed by: another digest, another
    encoding, or a missing version. Second correct implementation: building the label with
    `"{}:{}:{}".format(...)`, which yields the same bytes.
    """
    for text in ("FunctionDef(name='f')", "Constant(value='é')"):
        assert fingerprint.label(text) == "canon-ast:1:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def test_the_printer_check_passes_here_and_names_the_interpreter_when_the_label_differs(monkeypatch):
    """Property: the printer check returns nothing on this interpreter, and a finding naming `sys.version` when the stored label differs.

    Class: the stored label with any one hex digit changed. Why: the check is what turns an
    interpreter that prints differently into one named finding; one that never fires, or fires
    without naming the interpreter, gives the person reading it nothing to act on. Killed by: a
    check comparing nothing, or a finding that omits the interpreter. Second correct
    implementation: a check that compares only the hex digest after the label's last colon with
    the stored label's digest, and words its finding differently while still naming
    `sys.version`.
    """
    assert fingerprint.printer_check() is None
    stored = fingerprint.PRINTER_CHECK_LABEL
    last = stored[-1]
    monkeypatch.setattr(fingerprint, "PRINTER_CHECK_LABEL", stored[:-1] + ("0" if last != "0" else "1"))
    finding = fingerprint.printer_check()
    assert finding is not None and sys.version in finding, finding
