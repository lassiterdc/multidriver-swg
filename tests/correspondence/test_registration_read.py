"""Tests of the standard-library registration reader: one rule behind both readers, the node each returns, its imports.

The reader is read by two kinds of caller — the harness through `binding`, and checks run by an
interpreter with no `pytest` — so these tests pin what must hold for both: `binding` hands out
the very functions defined in `registration_read`; `registered_functions` and `registered_units`
agree on every registration-module form the harness's own tests build, plus the forms only the
function-returning read can get wrong; each unit maps to the `def` its decorator sits on; and importing the
module executes nothing outside the standard library and the `tests` tree. Every registration module here is a file
under the test's temporary directory, read as text and never imported.
"""

import subprocess
import sys
from pathlib import Path

import pytest

from tests.correspondence import binding, registration_read

pytestmark = pytest.mark.state_check

REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTRATIONS_PACKAGE = REPO_ROOT / "tests" / "registrations" / "__init__.py"
FROM = "from tests.correspondence.binding import registers\n\n\n"
HEAD = "from tests.correspondence import binding\nfrom tests.correspondence.binding import registers\n\n\n"
ALIAS = "from tests.correspondence import binding as b\n\n\n"

# Every registration-module source the harness's binding tests build, then the forms only a
# reader that returns function nodes can get wrong: an async def, a stacked decorator, a
# positional unit and a bare decorator.
SHAPES = {
    "other_unit": FROM + "@registers(unit='b.py:1-2')\ndef other(x):\n    return x\n",
    "twice": FROM + "".join(f"@registers(unit='a.py:1-2')\ndef side_{i}(x):\n    return x\n\n\n" for i in range(2)),
    "static": FROM.replace("\n\n\n", "\n\nKEY = 'a.py:1-2'\n\n\n")
    + "@registers(unit=KEY)\ndef side(x):\n    return x\n",
    "in_if": HEAD + "if True:\n\n    @registers(unit='a.py:1-2')\n    def side(x):\n        return x\n",
    "on_class": HEAD + "@registers(unit='a.py:1-2')\nclass Side:\n    pass\n",
    "by_hand": HEAD + "def side(x):\n    return x\n\n\nregisters(unit='a.py:1-2')(side)\n",
    "attr_by_hand": HEAD + "def side(x):\n    return x\n\n\nbinding.registers(unit='a.py:1-2')(side)\n",
    "nested": HEAD + "def outer():\n    @registers(unit='a.py:1-2')\n    def side(x):\n        return x\n",
    "alias_top": ALIAS + "@b.registers(unit='a.py:1-2')\ndef side(x):\n    return x\n",
    "alias_in_if": ALIAS + "if True:\n\n    @b.registers(unit='a.py:1-2')\n    def side(x):\n        return x\n",
    "registered": FROM + "@registers(unit='a.py:1-2')\ndef double(x):\n    return 2 * x\n",
    "raises": "raise RuntimeError('broken on purpose')\n",
    "async_def": FROM + "@registers(unit='a.py:1-2')\nasync def side(x):\n    return x\n",
    "stacked": FROM
    + "import functools\n\n\n@functools.cache\n@registers(unit='a.py:1-2')\ndef side(x):\n    return x\n",
    "positional": FROM + "@registers('a.py:1-2')\ndef side(x):\n    return x\n",
    "bare": FROM + "@registers\ndef side(x):\n    return x\n",
}


def test_binding_hands_out_the_functions_defined_in_the_reader():
    """Property: the reader's names reached through `binding` are the very objects `registration_read` defines.

    Class: each of the three relocated names — `registered_units`, `_calls_registers`,
    `_unread_registrations`. Why: a pytest-free check imports them from `registration_read`
    while the harness reaches them through `binding`; if `binding` kept its own copy the two
    could drift and disagree on a registration form without any test of either noticing.
    Killed by: leaving a definition of any of the three in `binding` beside the new module.
    Second correct implementation: `binding` importing the module and binding the three names
    by attribute assignment rather than by a `from` import.
    """
    names = ("registered_units", "_calls_registers", "_unread_registrations")
    assert all(getattr(binding, n) is getattr(registration_read, n) for n in names), [
        (n, getattr(binding, n).__module__) for n in names
    ]


@pytest.mark.parametrize("name", [*SHAPES, "registrations_package"])
def test_both_readers_return_one_unit_set_and_one_findings_list(tmp_path, name):
    """Property: for one file, `registered_functions` lists the same units and the same findings as `registered_units`.

    Class: every registration-module form — each source the binding tests build, the real
    `tests/registrations/__init__.py`, and an async def, a stacked decorator, a positional unit
    and a bare decorator. Why: the fingerprinting checks read `registered_functions` and the
    harness reads `registered_units`; a unit one lists and the other does not is a registration
    one check sees and the other silently misses. Killed by: a `registered_functions` that reads
    only `ast.FunctionDef`, keeps only `findings` from the decorator pass, or skips decorators
    after the first. Second correct implementation: `registered_functions` built by calling
    `registered_units` and then locating each unit's node with a second pass over the tree.
    """
    if name == "registrations_package":
        path = REGISTRATIONS_PACKAGE
    else:
        path = tmp_path / f"{name}.py"
        path.write_text(SHAPES[name], encoding="utf-8")
    units, unit_findings = registration_read.registered_units(path)
    functions, function_findings = registration_read.registered_functions(path)
    assert set(functions) == set(units) and function_findings == unit_findings, (
        sorted(functions),
        units,
        function_findings,
        unit_findings,
    )


def test_each_unit_maps_to_the_function_its_decorator_sits_on(tmp_path):
    """Property: each registered unit maps to the module-level `def` that carries its `registers` decorator.

    Class: modules registering several units on several functions, with undecorated functions
    between them, a decorator that is not first in its list, and an async def. Why: a check that
    fingerprints the code registered for a unit must fingerprint that unit's function and no
    neighbour; a node one position off fingerprints the wrong code and reports it as current.
    Killed by: mapping every unit to the first or the last decorated function, or to the node
    after the decorated one. Second correct implementation: returning, per unit, a fresh node
    from a second parse of the file, compared here by name and line rather than by identity.
    """
    path = tmp_path / "several.py"
    path.write_text(
        FROM + "def helper(x):\n    return x\n\n\n"
        "@registers(unit='a.py:1-2')\ndef first(x):\n    return helper(x)\n\n\n"
        "def between(x):\n    return x\n\n\n"
        "@staticmethod\n@registers(unit='b.py:3-4')\ndef second(x):\n    return x\n\n\n"
        "@registers(unit='c.py:5-6')\nasync def third(x):\n    return x\n",
        encoding="utf-8",
    )
    functions, findings = registration_read.registered_functions(path)
    got = {unit: (type(node).__name__, node.name, node.lineno) for unit, node in functions.items()}
    expected = {
        "a.py:1-2": ("FunctionDef", "first", 9),
        "b.py:3-4": ("FunctionDef", "second", 19),
        "c.py:5-6": ("AsyncFunctionDef", "third", 24),
    }
    assert got == expected and findings == [], (got, findings)


# Run in a fresh interpreter: refuse, at the import system, every module whose top-level package is
# neither standard library nor this repository's `tests` tree, then import the named module. A
# `tests` module is admitted only as far as its OWN imports are, so a sibling that pulls in
# `pytest` is refused transitively. `-S` skips `site`, whose `.pth` hooks would otherwise load
# third-party code before the finder is in place.
REFUSING_IMPORT = """
import importlib, importlib.abc, sys
ALLOWED = set(sys.stdlib_module_names) | {"tests"}
class Refuse(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] not in ALLOWED:
            raise ModuleNotFoundError(f"refused import outside the standard library: {name}", name=name)
        return None
sys.meta_path.insert(0, Refuse())
importlib.import_module(sys.argv[1])
"""


def _import_refusing_non_stdlib(module: str) -> subprocess.CompletedProcess:
    """Import `module` from the repository root in a child that refuses every non-standard-library import."""
    return subprocess.run(
        [sys.executable, "-S", "-c", REFUSING_IMPORT, module],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )


def test_the_reader_imports_only_the_standard_library():
    """Property: importing `registration_read` executes nothing outside the standard library and the `tests` tree.

    Class: everything the import executes — the module's own imports, the `tests` and
    `tests.correspondence` package `__init__` files run on the way, and every `tests` sibling
    those reach, transitively. Why: the reader exists so an interpreter without `pytest` or the
    toolkit's dependencies can import it; one such import anywhere on that path makes it fail
    there with `ModuleNotFoundError` while every test in the project environment still passes.
    The same child importing `binding` must be refused, or the refusal itself is not working.
    Killed by: an `import pytest` added to `registration_read`, to either package `__init__`, or
    to a `tests` sibling the reader imports. Second correct implementation, differing in its
    import set: a reader that also imports the standard-library sibling `tests.correspondence.ledger`.
    """
    reader = _import_refusing_non_stdlib("tests.correspondence.registration_read")
    control = _import_refusing_non_stdlib("tests.correspondence.binding")
    assert reader.returncode == 0, reader.stderr
    assert control.returncode != 0 and "refused import outside the standard library: pytest" in control.stderr, (
        control.returncode,
        control.stderr,
    )
