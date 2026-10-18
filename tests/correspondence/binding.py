"""Bind an equivalence test to the new code its owning system registers, without importing that code at collection.

An equivalence test is written before, or alongside, the new code it checks, and it must not
name a function of the toolkit's package: if it imported new code that does not exist yet, the
whole session would stop at collection and no other test would run. So a test reaches the new
side only through the `new_side` fixture. The fixture reads the test's family marker for its unit
and owning system, and looks for the callable that system registered for that unit in its own
registration module, `tests/registrations/{slug with hyphens as underscores}.py`, using the
`registers` decorator below.

When that module does not exist the unit is not registered yet, and the fixture returns a
placeholder: a value derived from the reference value and the test's licence that lies outside
the licence, so the test fails at its own assertion until real code is registered. Any other
problem with a registration module — it fails to import, or it registers no callable for the
unit — is an error at the test's setup, because only the absence of the module itself means
"not registered".

A licence is any object with two methods, `admits(reference, value) -> bool` and
`outside(reference) -> value`, where `outside` returns a value of the reference's comparable form
that `admits` rejects. This module fixes that interface and no concrete licence.

`registered_units` reads a registration module as a syntax tree, without importing it, so a
check can learn which units a system has registered from the file alone.
"""

from __future__ import annotations

import ast
import importlib
import sys
from pathlib import Path
from typing import Any, Protocol

import pytest

FAMILIES = ("equivalence", "refactor_reminder")
REGISTRATIONS = "tests.registrations"
BOUND = pytest.StashKey[bool]()  # whether the test's new side was a registered callable, for the run record


class Licence(Protocol):
    """What a test's licence must provide for the binding to derive a placeholder from it."""

    def admits(self, reference: Any, value: Any) -> bool:
        """Whether `value` is close enough to `reference` for the test to pass."""

    def outside(self, reference: Any) -> Any:
        """A value of the reference's comparable form that `admits` rejects."""


class RegistrationError(Exception):
    """A registration module that exists but cannot supply the unit's callable, or a misused fixture."""


def registers(unit: str):
    """Register the decorated function as the new side of `unit` in the module that defines it.

    The table is kept on the registration module itself rather than here, so that a module
    imported afresh starts with an empty table instead of colliding with its earlier import.
    A second registration of one unit in one module raises at import, because the first one
    would otherwise be replaced silently and the test would run against the wrong function.
    """

    def decorate(function):
        """Record `function` under `unit` in its own module's table, refusing a second entry for the unit."""
        table = vars(sys.modules[function.__module__]).setdefault("_REGISTERED_UNITS", {})
        if unit in table:
            raise ValueError(f"unit {unit} is registered twice in {function.__module__}")
        table[unit] = function
        return function

    return decorate


def registered_units(path: Path) -> tuple[list[str], list[str]]:
    """Return the units a registration module registers, read without importing it, and each form it cannot read.

    Only a module-level function decorated `registers(unit="...")` (or `binding.registers(...)`)
    with a string literal counts; any other value for `unit` is a finding naming the file and
    line, because a value computed at import cannot be read from the file. Every other call of
    `registers`, by its bare name or as an attribute, is a finding too (`_unread_registrations`).
    """
    units, findings = [], []
    tree = ast.parse(Path(path).read_text(encoding="utf-8"))
    read: set[int] = set()
    for node in tree.body:
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        for decorator in node.decorator_list:
            read.add(id(decorator))
            func = decorator.func if isinstance(decorator, ast.Call) else decorator
            name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
            if name != "registers":
                continue
            keywords = {k.arg: k.value for k in decorator.keywords} if isinstance(decorator, ast.Call) else {}
            value = keywords.get("unit")
            if isinstance(value, ast.Constant) and isinstance(value.value, str) and not decorator.args:
                units.append(value.value)
            else:
                findings.append(
                    f"{path}:{decorator.lineno}: registration of {node.name} names its unit by a non-literal"
                )
    return units, findings + _unread_registrations(tree, read, path)


def _calls_registers(node: ast.AST) -> bool:
    """Whether `node` is a call of `registers`, by its bare name or as an attribute of any object.

    This is the same rule the module-level read lists a decorator by, which reads any attribute
    named `registers` (`binding.registers`, or `b.registers` when `binding` is imported as `b`).
    Deliberately NOT `binding.registers` alone: the module-level read would then list a unit
    for `@b.registers(...)` and stay silent on the same decorator under an `if`, so the list
    would be silently incomplete for a form the reader itself reads.
    """
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Name):
        return func.id == "registers"
    return isinstance(func, ast.Attribute) and func.attr == "registers"


def _unread_registrations(tree: ast.Module, read: set[int], path) -> list[str]:
    """Name, by file and line, each `registers(...)` call found anywhere but on a top-level function's decorator list.

    Such a call still registers when the module is imported, inside an `if`, on a class, nested
    in a function or applied by hand as `registers(...)(f)`, so the static list would silently
    miss its unit. Naming every call of `registers`, rather than a list of known shapes, keeps
    the list either complete or visibly incomplete. A call through some other name (`r =
    registers`, then `@r(...)`) is not seen.
    """
    return [
        f"{path}:{node.lineno}: a registers(...) call outside the decorator list of a top-level function"
        for node in ast.walk(tree)
        if _calls_registers(node) and id(node) not in read
    ]


def _module_name(system: str) -> str:
    """The registration module of an owning system: its slug with hyphens as underscores."""
    return f"{REGISTRATIONS}.{system.replace('-', '_')}"


def _import_registration(module: str, unit: str):
    """The system's registration module, or None when that exact module does not exist."""
    try:
        return importlib.import_module(module)
    except ModuleNotFoundError as error:
        if error.name == module:
            return None
        raise RegistrationError(
            f"registration module {module} could not be imported for unit {unit}: {error!r}"
        ) from error
    except Exception as error:
        raise RegistrationError(
            f"registration module {module} could not be imported for unit {unit}: {error!r}"
        ) from error


def _registered_callable(registration, module: str, unit: str):
    """The callable the module registered for `unit`; a present module without one is an error, not an absence."""
    target = getattr(registration, "_REGISTERED_UNITS", {}).get(unit)
    if target is None:
        raise RegistrationError(f"registration module {module} registers no callable for unit {unit}")
    return target


def _placeholder(reference: Any, licence: Licence) -> Any:
    """The value a test receives while its unit is unregistered: the licence's value outside the reference."""
    return licence.outside(reference)


def _refuse_admitted(reference: Any, licence: Licence, placeholder: Any) -> None:
    """Refuse a placeholder the licence admits, since a test would then pass with no new code at all."""
    if licence.admits(reference, placeholder):
        shape = getattr(reference, "shape", None)
        if shape is None and hasattr(reference, "__len__"):
            shape = (len(reference),)
        raise RegistrationError(
            f"licence {type(licence).__name__} admits its own placeholder for a reference of type "
            f"{type(reference).__name__} and shape {shape} — no value lies outside it, so the placeholder "
            "could not fail the comparison"
        )


def _new_value(target, args: tuple, reference: Any, licence: Licence) -> Any:
    """The registered callable's value, or the checked placeholder when no callable is registered."""
    if target is None:
        placeholder = _placeholder(reference, licence)
        _refuse_admitted(reference, licence, placeholder)
        return placeholder
    return target(*args)


@pytest.fixture
def new_side(request):
    """A callable `bound(*args, reference, licence)` that reaches the new code of the test's unit.

    The registration module is resolved here, at setup, so a broken module is an error at the
    test's setup rather than a failure that could be mistaken for a wrong value.
    """
    marker = next((m for m in map(request.node.get_closest_marker, FAMILIES) if m is not None), None)
    if marker is None or "unit" not in marker.kwargs or "system" not in marker.kwargs:
        raise RegistrationError(
            f"{request.node.nodeid} requests new_side without a family marker naming unit and system"
        )
    unit, module = marker.kwargs["unit"], _module_name(marker.kwargs["system"])
    registration = _import_registration(module, unit)
    target = None if registration is None else _registered_callable(registration, module, unit)
    request.node.stash[BOUND] = target is not None

    def bound(*args, reference, licence):
        """Call the registered callable with `args`, or return the placeholder for `reference` under `licence`."""
        return _new_value(target, args, reference, licence)

    return bound
