"""Tests of the binding to new code: the `new_side` fixture, the `registers` decorator and its static reader.

Most tests drive an in-process `pytester` session that loads only the binding (`-p
tests.correspondence.binding`), so the inner test can request `new_side` without the root
configuration's other plugins. A synthetic registration module is written under the test's own
temporary directory and made importable as `tests.registrations.{name}` by extending that
package's search path for the test's duration, so nothing outside the temporary directory is
written. The placeholder test uses no registration module at all, so it exercises the binding
end to end against the real `tests.registrations` package, where no system has registered yet.
"""

import importlib
import sys
from pathlib import Path

import pytest

import tests.registrations as registrations
from tests.correspondence import binding

pytestmark = pytest.mark.state_check

UNIT = "a.py:1-2"
INI = "[pytest]\nmarkers =\n    equivalence(unit, system, part): a synthetic equivalence test\n"
LICENCE = (
    "class Plus:\n"
    "    def admits(self, reference, value):\n"
    "        return value == reference\n"
    "\n"
    "    def outside(self, reference):\n"
    "        return reference + 1\n"
)


def _registration_dir(tmp_path, monkeypatch, request, modules):
    """Write `modules` as synthetic registration modules and make them importable for this test only."""
    place = tmp_path / "registrations_fixture"
    place.mkdir()
    for name, source in modules.items():
        (place / f"{name}.py").write_text(source)
        request.addfinalizer(lambda name=name: sys.modules.pop(f"{binding.REGISTRATIONS}.{name}", None))
    monkeypatch.setattr(registrations, "__path__", [*registrations.__path__, str(place)])


def _inner_reports(pytester, monkeypatch, request, body, modules=None):
    """Run `body` as the only test file of an in-process session loading the binding; return its test reports."""
    if modules:
        _registration_dir(pytester.path, monkeypatch, request, modules)
    pytester.makeini(INI)
    pytester.makepyfile(test_inner=body)
    reprec = pytester.inline_run("-p", "no:cacheprovider", "-p", "tests.correspondence.binding")
    return reprec.getreports("pytest_runtest_logreport")


def _test_body(system, call):
    """An inner test of `UNIT` owned by `system` that asserts the value `call` returns is within its licence."""
    return (
        f"import pytest\n{LICENCE}\n\n"
        f"@pytest.mark.equivalence(unit='{UNIT}', system='{system}')\n"
        f"def test_one(new_side):\n    value = {call}\n    assert Plus().admits(4, value)\n"
    )


def test_with_no_registration_module_the_placeholder_fails_at_the_tests_own_assertion(pytester, monkeypatch, request):
    """Property: a test whose system has no registration module fails, and fails at its own assertion.

    Class: any test whose owning system has not registered yet, here against the real
    `tests.registrations` package. Why: the placeholder exists to make an unwritten unit read as a
    failing comparison in the test itself, not as an error elsewhere and not as a pass. Killed by:
    returning the reference as the placeholder, which either passes the comparison or is caught
    inside the binding, so the failure no longer sits in the test file. Second correct
    implementation: a placeholder built by perturbing the reference by more than the licence
    allows, rather than asking the licence for it.
    """
    reports = _inner_reports(
        pytester, monkeypatch, request, _test_body("unregistered-system", "new_side(reference=4, licence=Plus())")
    )
    call = [r for r in reports if r.when == "call"]
    assert [r.outcome for r in call] == ["failed"], [(r.when, r.outcome) for r in reports]
    assert Path(call[0].longrepr.reprcrash.path).name == "test_inner.py", call[0].longrepr.reprcrash


def test_a_registration_module_that_raises_on_import_is_a_setup_error(pytester, monkeypatch, request):
    """Property: a registration module whose import raises makes the test an error at setup, naming the module.

    Class: any exception from importing a registration module other than that exact module being
    absent. Why: only a missing module means "not registered"; a broken one read as missing would
    hide the breakage behind a placeholder failure that looks like a wrong value. Killed by:
    treating any import error as not registered. Second correct implementation: importing every
    registration module once per session and reporting the broken ones at each test's setup.
    """
    modules = {"fixture_raises": "raise RuntimeError('broken on purpose')\n"}
    body = _test_body("fixture-raises", "new_side(reference=4, licence=Plus())")
    reports = _inner_reports(pytester, monkeypatch, request, body, modules)
    setup = [r for r in reports if r.when == "setup"]
    assert [r.outcome for r in setup] == ["failed"], [(r.when, r.outcome) for r in reports]
    assert f"{binding.REGISTRATIONS}.fixture_raises" in str(setup[0].longrepr).split(), setup[0].longrepr


def test_a_present_module_with_no_callable_for_the_unit_is_a_setup_error(pytester, monkeypatch, request):
    """Property: a registration module that imports but registers nothing for the unit is an error at setup.

    Class: any registration module that exists and lacks the test's unit. Why: the module's
    presence says the system has started registering, so a missing unit is a mistake to report,
    not an unwritten unit to placehold. Killed by: returning the placeholder when the module has
    no callable for the unit. Second correct implementation: a registry kept in the binding,
    keyed by module and unit, consulted the same way.
    """
    modules = {
        "fixture_other_unit": "from tests.correspondence.binding import registers\n\n\n"
        "@registers(unit='b.py:1-2')\ndef other(x):\n    return x\n"
    }
    body = _test_body("fixture-other-unit", "new_side(reference=4, licence=Plus())")
    reports = _inner_reports(pytester, monkeypatch, request, body, modules)
    setup = [r for r in reports if r.when == "setup"]
    assert [r.outcome for r in setup] == ["failed"], [(r.when, r.outcome) for r in reports]
    tokens = str(setup[0].longrepr).split()
    assert UNIT in tokens and f"{binding.REGISTRATIONS}.fixture_other_unit" in tokens, setup[0].longrepr


def test_a_unit_registered_twice_in_one_module_raises_at_import(tmp_path, monkeypatch, request):
    """Property: importing a module that registers one unit twice raises an error naming the unit.

    Class: any registration module with two `registers` decorators for one unit key. Why: without
    the refusal the second function silently replaces the first, and the test runs against code
    its author did not mean. Killed by: letting the last registration win. Second correct
    implementation: a static read of the module that refuses the duplicate before it is imported.
    """
    source = "from tests.correspondence.binding import registers\n\n\n"
    source += "".join(f"@registers(unit='{UNIT}')\ndef side_{i}(x):\n    return x\n\n\n" for i in range(2))
    _registration_dir(tmp_path, monkeypatch, request, {"fixture_twice": source})
    with pytest.raises(ValueError) as excinfo:
        importlib.import_module(f"{binding.REGISTRATIONS}.fixture_twice")
    assert UNIT in str(excinfo.value).split(), excinfo.value


def test_the_static_reader_refuses_a_registration_whose_unit_is_not_a_literal(tmp_path):
    """Property: a `registers` decorator whose unit is not a string literal is a finding naming its file and line.

    Class: any registration whose unit value is a name, call or other expression. Why: the
    static reader exists so a check can learn the registered units without importing the module,
    and a computed value cannot be read from the file. Killed by: skipping non-literal
    decorators. Second correct implementation: a read that also lists the literal units of the
    same file while refusing the computed one.
    """
    path = tmp_path / "fixture_static.py"
    path.write_text(
        "from tests.correspondence.binding import registers\n\nKEY = 'a.py:1-2'\n\n\n"
        "@registers(unit=KEY)\ndef side(x):\n    return x\n"
    )
    units, findings = binding.registered_units(path)
    assert units == [] and any(f"{path}:6:" in f.split() for f in findings), (units, findings)


def test_every_registers_call_the_reader_cannot_read_is_named(tmp_path):
    """Property: a `registers(...)` call outside a top-level function's decorator list is named by its file and line.

    Class: calls of `registers` by its bare name or as an attribute (`binding.registers`, and
    `b.registers` when `binding` is imported as `b`), in any position other than a module-level
    function decorator: under an `if`, on a class, applied by hand, or on a nested function. Why:
    each such call still registers at import, so a static list that skipped it would silently
    miss a unit; and a form the module-level read lists as a unit must not pass in silence under
    an `if`, or the list is silently incomplete for a form the reader itself recognises. Killed
    by: reading only module-level function decorators; or naming only the `binding.registers`
    spelling outside the decorator list while the module-level read lists any attribute named
    `registers`. Second correct implementation: comparing the units an import registers with the
    units the static read lists; for the alias pair, a module-level read that does not list the
    `b.registers` form either.
    """
    head = "from tests.correspondence import binding\nfrom tests.correspondence.binding import registers\n\n\n"
    modules = {
        "in_if.py": (head + "if True:\n\n    @registers(unit='a.py:1-2')\n    def side(x):\n        return x\n", 7),
        "on_class.py": (head + "@registers(unit='a.py:1-2')\nclass Side:\n    pass\n", 5),
        "by_hand.py": (head + "def side(x):\n    return x\n\n\nregisters(unit='a.py:1-2')(side)\n", 9),
        "attr_by_hand.py": (head + "def side(x):\n    return x\n\n\nbinding.registers(unit='a.py:1-2')(side)\n", 9),
        "nested.py": (head + "def outer():\n    @registers(unit='a.py:1-2')\n    def side(x):\n        return x\n", 6),
    }
    for name, (text, line) in modules.items():
        path = tmp_path / name
        path.write_text(text)
        units, findings = binding.registered_units(path)
        assert units == [], (name, units)
        assert len(findings) == 1 and f"{path}:{line}:" in findings[0].split(), (name, findings)
    alias = "from tests.correspondence import binding as b\n\n\n"
    top = tmp_path / "alias_top.py"
    top.write_text(alias + "@b.registers(unit='a.py:1-2')\ndef side(x):\n    return x\n")
    nested = tmp_path / "alias_in_if.py"
    nested.write_text(alias + "if True:\n\n    @b.registers(unit='a.py:1-2')\n    def side(x):\n        return x\n")
    read_at_top = bool(binding.registered_units(top)[0])
    named_when_nested = bool(binding.registered_units(nested)[1])
    assert read_at_top == named_when_nested, (read_at_top, named_when_nested)


def test_a_registered_callables_value_is_returned(pytester, monkeypatch, request):
    """Property: when the unit is registered, the test receives the registered callable's value and can pass.

    Class: any registration module that registers the test's unit. Why: the binding is only
    useful if it delivers new code once it exists; a binding that always placeholds keeps every
    test red forever. Killed by: always returning the placeholder. Second correct implementation:
    a fixture that returns the registered function itself for the test to call.
    """
    modules = {
        "fixture_registered": "from tests.correspondence.binding import registers\n\n\n"
        f"@registers(unit='{UNIT}')\ndef double(x):\n    return 2 * x\n"
    }
    body = _test_body("fixture-registered", "new_side(2, reference=4, licence=Plus())")
    reports = _inner_reports(pytester, monkeypatch, request, body, modules)
    assert [(r.when, r.outcome) for r in reports] == [("setup", "passed"), ("call", "passed"), ("teardown", "passed")]


SAME_LICENCE = (
    "import numpy\nimport pytest\n\n\n"
    "class Same:\n"
    "    def admits(self, reference, value):\n"
    "        return numpy.array_equal(value, reference)\n"
    "\n"
    "    def outside(self, reference):\n"
    "        return reference\n"
)


def test_a_placeholder_its_own_licence_admits_is_refused(pytester, monkeypatch, request):
    """Property: when the licence admits the placeholder it produced, the binding raises instead of returning it.

    Class: any licence with no value outside it for the given reference, here one whose
    `outside` returns the reference itself, on an empty array. Why: a placeholder the licence
    admits would let an unregistered test pass with no new code at all. Killed by: returning the
    placeholder unchecked. Second correct implementation: checking the licence once when the
    test starts, before any value is computed, and raising there.
    """
    body = (
        f"{SAME_LICENCE}\n\n@pytest.mark.equivalence(unit='{UNIT}', system='unregistered-system')\n"
        "def test_one(new_side):\n    reference = numpy.empty(0, dtype='float64')\n"
        "    value = new_side(reference=reference, licence=Same())\n"
        "    assert Same().admits(reference, value)\n"
    )
    reports = _inner_reports(pytester, monkeypatch, request, body)
    call = [r for r in reports if r.when == "call"]
    assert [r.outcome for r in call] == ["failed"], [(r.when, r.outcome) for r in reports]
    tokens = str(call[0].longrepr).split()
    assert "Same" in tokens and "(0,)" in tokens, call[0].longrepr
