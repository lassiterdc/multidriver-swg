"""Check that the lift-import check scans every function the lift runner discovers as a declaration, however its decorator was spelled.

`test_lift_imports.import_findings` finds a declaration by reading the decorator's own name in the syntax tree:
`declares_lift`, as a bare name or as the last part of an attribute. The runner finds one differently:
`lift.discover` takes every function of a lift module that carries the `__lift__` attribute the decorator sets,
whatever name the decorator was reached by. The two populations differ wherever a lift module renames or rebinds
the decorator, and a declaration only the runner finds is one whose imports of this checkout the check never
reads, so a shared helper imported inside it can change the lift's value while no lift digest moves.
"""

import pytest

from tests.correspondence import lift
from tests.equivalence.test_lift_imports import import_findings

pytestmark = pytest.mark.state_check

DECLARATION = (
    '@{decorator}(unit="mod.py:1-3")\n'
    "def declare(fixtures):\n"
    '    """Supply nothing."""\n'
    "    import tests.helper\n"
    '    return {{}}, lambda ns: ns["y"]\n'
)

SPELLINGS = {
    "imported under another name": (
        "from tests.correspondence.lift import declares_lift as mark_lift\n\n\n",
        "mark_lift",
    ),
    "bound to a second name": (
        "from tests.correspondence.lift import declares_lift\n\nmark = declares_lift\n\n\n",
        "mark",
    ),
    "bound from the module attribute": (
        "from tests.correspondence import lift\n\nmark = lift.declares_lift\n\n\n",
        "mark",
    ),
}


@pytest.mark.parametrize(("head", "decorator"), SPELLINGS.values(), ids=list(SPELLINGS))
def test_an_import_of_this_checkout_inside_a_declaration_is_found_whatever_the_decorator_is_called(
    tmp_path, head, decorator
):
    """Property: a function the runner discovers as a declaration is scanned, and its import of the checkout is one finding.

    Class: every spelling by which a lift module reaches `declares_lift` under another name: an `as` import,
    a second name bound to it, and a name bound from the module's attribute. In each the runner discovers the
    declaration (asserted first), and the declaration imports `tests.helper` in its body. Why it is required:
    the runner's module-level refusal does not see an import inside the body and the lift digest does not
    cover the helper's text, so the check is the only thing that reports it; a spelling it does not read leaves
    that import unreported. Killed by: an `_is_declaration` that matches the decorator by its written name, as
    this check does today; the change that kills it takes the declarations from `lift.discover(root)`, or
    follows the module's own bindings of `declares_lift`, instead of reading the decorator's spelling. Second
    correct implementation: resolving each decorator through the module's `import ... as` and assignment
    statements passes the same three inputs, as does the discovery-based form.
    """
    (tmp_path / lift.LIFTS).mkdir(parents=True)
    (tmp_path / lift.LIFTS / "sys_a.py").write_text(head + DECLARATION.format(decorator=decorator), encoding="utf-8")
    assert [(d.unit, d.part) for d in lift.discover(tmp_path)] == [("mod.py:1-3", None)]
    findings = import_findings(tmp_path)
    assert len(findings) == 1, findings
    assert findings[0].startswith("tests/lifts/sys_a.py:") and "declaration declare" in findings[0]
    assert "import tests.helper" in findings[0]


@pytest.mark.parametrize(
    "call",
    ['importlib.import_module(name="tests.helper")', '__import__(name="tests.helper")'],
    ids=["import_module", "__import__"],
)
def test_a_string_literal_dynamic_import_is_refused_whether_its_name_is_passed_by_position_or_by_keyword(
    tmp_path, call
):
    """Property: a call of `import_module` or `__import__` whose name is a string literal passed by keyword is judged as its positional form is.

    Class: the two calls with `name=` written out, in a declaration that also imports `importlib`; the positional form
    of the same call is the control and is refused already. Why it is required: the rule is about the literal first
    argument of the call, and Python binds it the same way by position or by keyword, so a check reading only the
    positional list leaves the keyword spelling an import of the checkout that no check reports. Killed by:
    an `_imports_of` that reads `node.args[0]` and never `node.keywords`, as the check does today; the change that
    kills it reads the first argument from either place. Second correct implementation: resolving the call's
    bound arguments with `inspect.signature` of `importlib.import_module` and `__import__` passes the same two inputs.
    """
    root = tmp_path / "keyword"
    (root / lift.LIFTS).mkdir(parents=True)
    body = f'    import importlib\n    {call}\n    return {{}}, lambda ns: ns["y"]\n'
    declaration = '@declares_lift(unit="mod.py:1-3")\ndef declare(fixtures):\n    """Supply nothing."""\n' + body
    head = "from tests.correspondence.lift import declares_lift\n\n\n"
    (root / lift.LIFTS / "sys_a.py").write_text(head + declaration, encoding="utf-8")
    findings = import_findings(root)
    assert len(findings) == 1, findings
    assert "a dynamic import of tests.helper" in findings[0]
