"""Read which units a registration module registers, and on which function, from its source alone.

An owning system registers new code for a legacy unit by decorating a function in
`tests/registrations/{slug with hyphens as underscores}.py` with `registers(unit="{unit key}")`.
A check that must know what a system has registered — and, for a check that fingerprints the
registered code, which function carries each registration — has to learn it without importing
the module: importing it runs the system's new code, and that code may not exist yet or may
need libraries the check's interpreter does not have.

This module imports only the standard library, and nothing else from the test tree, so that an
interpreter with no `pytest` installed — the one a pre-commit hook runs under — can import it.
Deliberately NOT kept in `tests.correspondence.binding`, which imports `pytest` at its top: the
reader would then be unimportable exactly where a hook needs it, failing with
`ModuleNotFoundError: No module named 'pytest'`. `binding` imports these names back, so a
caller that reaches them through `binding` reads the same functions.

Both readers apply ONE rule, `_read`, and differ only in what they return. Deliberately NOT a
second reader written for the second return: two readers of one format can disagree on a form,
and the disagreement would show only as a unit one check sees and the other does not.
"""

from __future__ import annotations

import ast
from pathlib import Path

RegisteredFunction = ast.FunctionDef | ast.AsyncFunctionDef


def registered_units(path: Path) -> tuple[list[str], list[str]]:
    """Return the units a registration module registers, read without importing it, and each form it cannot read.

    Only a module-level function decorated `registers(unit="...")` (or `binding.registers(...)`)
    with a string literal counts; any other value for `unit` is a finding naming the file and
    line, because a value computed at import cannot be read from the file. Every other call of
    `registers`, by its bare name or as an attribute, is a finding too (`_unread_registrations`).
    """
    registrations, findings = _read(path)
    return [unit for unit, _ in registrations], findings


def registered_functions(path: Path) -> tuple[dict[str, RegisteredFunction], list[str]]:
    """Return each registered unit's function node, read without importing the module, and the same findings.

    The units and the findings are exactly those `registered_units` returns for the same file;
    what this adds is the `def` node each `registers(unit=...)` decorator sits on, so a caller
    can fingerprint the code registered for a unit without importing it. A unit registered twice
    in one file maps to its FIRST registration. Not established: which of the two a caller
    should see — the module refuses the duplicate when it is imported, so no test runs against
    either, and this read neither names the duplicate nor chooses for that reason.
    """
    registrations, findings = _read(path)
    functions: dict[str, RegisteredFunction] = {}
    for unit, node in registrations:
        functions.setdefault(unit, node)
    return functions, findings


def _read(path: Path) -> tuple[list[tuple[str, RegisteredFunction]], list[str]]:
    """Return each literal registration as `(unit, function node)` in source order, and every form it cannot read."""
    registrations, findings = [], []
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
                registrations.append((value.value, node))
            else:
                findings.append(
                    f"{path}:{decorator.lineno}: registration of {node.name} names its unit by a non-literal"
                )
    return registrations, findings + _unread_registrations(tree, read, path)


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
