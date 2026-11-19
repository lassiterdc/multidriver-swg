#!/usr/bin/env python3
"""Refuse a built wheel whose public API surface has drifted from the package's own declarations.

The surface is declared in the sources and in no other place: the package root lists its public
modules and subpackages in `__all__`, and each listed subpackage lists the names it publishes in its
own `__all__`. Nothing reads either list at run time, so a rename that leaves a list stale ships a
wheel whose surface promises a name the wheel does not carry, and a caller learns it from an
`ImportError` at their own call site rather than from the push that made the promise. This gate reads
the lists out of the BUILT WHEEL, so what it checks is what a caller would install: it opens the
archive as a zip and never imports it, so it runs before any project environment exists.

Every ground runs on one invocation, so one invocation reports everything it finds: each refusal is
one stdout line `refused: {id}: {detail}`, and the closing line reads `{wheel}: {n} refusal(s)` or
`{wheel}: conforms`. The exit status is 0 when nothing was refused, 1 when anything was, and 2 on a
usage error.

The four grounds, and the refusal set is CLOSED at these four:

- `no-root-all`: `multidriver_swg/__init__.py` in the wheel binds no `__all__` that is a literal list
  or tuple of strings. A list computed at import time cannot be read without importing the wheel,
  which this gate never does, so such a list is not a surface this gate can check.
- `listed-absent`: a root entry the wheel carries neither as `multidriver_swg/{name}.py` nor as
  `multidriver_swg/{name}/__init__.py`; or a name a listed subpackage's `__all__` promises that the
  subpackage's `__init__.py` does not bind at top level; or such a name bound by a `from ...
  import` whose source module the wheel also carries and which does not bind the name at top level.
- `subpackage-without-all`: a listed subpackage's `__init__.py` binds no literal `__all__`, so its
  surface is whatever happens to be imported.
- `unreadable-wheel`: the path is not a zip holding `multidriver_swg/__init__.py`, or one of the
  wheel's own modules cannot be read or parsed as Python. The second case is the same ground because
  a wheel whose sources do not read as Python is a wheel whose surface cannot be decided, and the
  order this gate works in is to fail closed rather than to pass by default.

A subpackage's own submodules are NOT listed, and are not checked: the subpackage's `__all__` is its
surface, and its re-exports are the reason it imports them, so refusing a listed subpackage for
importing an unlisted module would refuse both of this package's subpackages by construction. A name
a subpackage binds by a `from` import from a module the wheel does not carry is not refused either:
the wheel is the population this gate reads, and a name taken from outside it is not a claim about
this wheel.
"""

from __future__ import annotations

import argparse
import ast
import sys
import zipfile
from pathlib import Path

PACKAGE = "multidriver_swg"
ROOT_MODULE = f"{PACKAGE}/__init__.py"


def _parsed(archive: zipfile.ZipFile, name: str) -> tuple[ast.Module | None, str]:
    """The module at `name` in the archive, parsed, and the reason it could not be read when it is None."""
    try:
        source = archive.read(name).decode("utf-8")
    except (KeyError, UnicodeDecodeError, OSError) as error:
        return None, f"{name} cannot be read from the wheel ({type(error).__name__}: {error})"
    try:
        tree = ast.parse(source, filename=name)
    except SyntaxError as error:
        return None, f"{name} in the wheel does not parse as Python ({error})"
    return tree, ""


def _string_elements(node: ast.expr) -> list[str] | None:
    """The string elements of a literal list or tuple expression, or None when it is not one."""
    if not isinstance(node, ast.List | ast.Tuple):
        return None
    elements: list[str] = []
    for element in node.elts:
        if not isinstance(element, ast.Constant) or not isinstance(element.value, str):
            return None
        elements.append(element.value)
    return elements


def _literal_all(tree: ast.Module) -> list[str] | None:
    """A module's `__all__` as a list of strings when it binds one literally, and None otherwise.

    The last module-level binding wins, as it does at run time. A binding whose value is not a list
    or a tuple, or that holds anything but string constants, is not a literal list of strings: a
    starred element or a computed one leaves a surface this gate would have to import the wheel to
    know, which is the one thing it does not do.
    """
    bound = False
    value: ast.expr | None = None
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets, assigned = node.targets, node.value
        elif isinstance(node, ast.AnnAssign):
            targets, assigned = [node.target], node.value
        else:
            continue
        if not any(isinstance(target, ast.Name) and target.id == "__all__" for target in targets):
            continue
        bound = True
        value = assigned
    if not bound or value is None:
        return None
    return _string_elements(value)


def _bind_target(target: ast.expr, bound: dict[str, tuple[str, str] | None]) -> None:
    """Record each name an assignment target binds, an unpacking pattern contributing its own."""
    if isinstance(target, ast.Name):
        bound[target.id] = None
    elif isinstance(target, ast.List | ast.Tuple):
        for element in target.elts:
            _bind_target(element, bound)


def _import_source(parent: str, node: ast.ImportFrom) -> str:
    """The absolute module a `from` import names, resolved against `parent` when the import is relative."""
    if node.level == 0:
        return node.module or ""
    base = parent
    for _ in range(node.level - 1):
        base = base.rpartition(".")[0]
    if node.module is None:
        return base
    return f"{base}.{node.module}" if base else node.module


def _module_level(statements: list[ast.stmt]) -> list[ast.stmt]:
    """Every statement the module executes at its own level: the body, and the bodies of compound statements in it.

    A `try`, `if`, `with`, `for`, `while` or `match` at module level binds in the module's namespace
    exactly as a bare statement does, so a name bound inside one is a top-level binding. Function and
    class bodies bind in their own scope and are not entered.
    """
    found: list[ast.stmt] = []
    for node in statements:
        found.append(node)
        if isinstance(node, ast.If | ast.For | ast.AsyncFor | ast.While):
            found.extend(_module_level(node.body + node.orelse))
        elif isinstance(node, ast.With | ast.AsyncWith):
            found.extend(_module_level(node.body))
        elif isinstance(node, ast.Try | ast.TryStar):
            found.extend(_module_level(node.body + node.orelse + node.finalbody))
            for handler in node.handlers:
                found.extend(_module_level(handler.body))
        elif isinstance(node, ast.Match):
            for case in node.cases:
                found.extend(_module_level(case.body))
    return found


def _bindings(module: str, is_package: bool, tree: ast.Module) -> dict[str, tuple[str, str] | None]:
    """Each name a module binds at top level, and the module and name a `from` import takes it from.

    The second member is the absolute dotted name of the module the name is imported from and the name
    that module is asked for, when the binding is a `from ... import` — the two differ when the import
    is aliased — and None when it is a `def`, a `class` or an assignment. A star import binds no
    statically known name and contributes nothing.
    """
    parent = module if is_package else module.rpartition(".")[0]
    bound: dict[str, tuple[str, str] | None] = {}
    for node in _module_level(tree.body):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            bound[node.name] = None
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                _bind_target(target, bound)
        elif isinstance(node, ast.AnnAssign | ast.AugAssign):
            _bind_target(node.target, bound)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                bound[alias.asname or alias.name.split(".", 1)[0]] = None
        elif isinstance(node, ast.ImportFrom):
            source = _import_source(parent, node)
            for alias in node.names:
                if alias.name != "*":
                    bound[alias.asname or alias.name] = (source, alias.name)
        elif isinstance(node, ast.For | ast.AsyncFor):
            _bind_target(node.target, bound)
        elif isinstance(node, ast.With | ast.AsyncWith):
            for item in node.items:
                if item.optional_vars is not None:
                    _bind_target(item.optional_vars, bound)
        for walrus in ast.walk(node) if isinstance(node, ast.Expr | ast.Assign) else ():
            if isinstance(walrus, ast.NamedExpr):
                _bind_target(walrus.target, bound)
    return bound


def _archive_names(module: str) -> tuple[str, str]:
    """The two archive names a dotted module may live at: its module file first, its package second."""
    stem = module.replace(".", "/")
    return f"{stem}.py", f"{stem}/__init__.py"


def _subpackage_refusals(archive: zipfile.ZipFile, carried: set[str], package_path: str) -> list[tuple[str, str]]:
    """Every ground one listed subpackage gives: its own `__all__`, and each name that list promises."""
    subpackage = package_path.removesuffix("/__init__.py").replace("/", ".")
    tree, problem = _parsed(archive, package_path)
    if tree is None:
        return [("unreadable-wheel", problem)]
    listed = _literal_all(tree)
    if listed is None:
        return [
            (
                "subpackage-without-all",
                f"{package_path} binds no __all__ that is a literal list or tuple of strings; "
                "declare the names the subpackage publishes there",
            )
        ]

    bound = _bindings(subpackage, True, tree)
    refusals: list[tuple[str, str]] = []
    for name in listed:
        if name not in bound:
            refusals.append(
                (
                    "listed-absent",
                    f"{name} is listed in {package_path}'s __all__ and that module binds no top-level {name}; "
                    "bind it there or drop it from the list",
                )
            )
            continue
        imported = bound[name]
        if imported is None:
            continue
        source, imported_name = imported
        module_path, source_package = _archive_names(source)
        upstream = module_path if module_path in carried else source_package if source_package in carried else None
        if upstream is None:
            continue
        tree, problem = _parsed(archive, upstream)
        if tree is None:
            refusals.append(("unreadable-wheel", problem))
            continue
        if imported_name not in _bindings(source, upstream.endswith("/__init__.py"), tree):
            refusals.append(
                (
                    "listed-absent",
                    f"{name} is listed in {package_path}'s __all__, which takes it from {source}, "
                    f"and the wheel's {upstream} binds no top-level {name}",
                )
            )
    return refusals


def _refusals(wheel: Path) -> list[tuple[str, str]]:
    """Every ground the wheel gives, in the order the wheel's own lists name them."""
    try:
        archive = zipfile.ZipFile(wheel)
    except (OSError, zipfile.BadZipFile) as error:
        return [
            (
                "unreadable-wheel",
                f"{wheel} is not a readable zip ({type(error).__name__}: {error}); "
                "pass the wheel a build of this package produced",
            )
        ]
    with archive:
        carried = set(archive.namelist())
        if ROOT_MODULE not in carried:
            return [
                (
                    "unreadable-wheel",
                    f"{wheel} carries no {ROOT_MODULE}; pass the wheel a build of this package produced",
                )
            ]
        root, problem = _parsed(archive, ROOT_MODULE)
        if root is None:
            return [("unreadable-wheel", problem)]
        listed = _literal_all(root)
        if listed is None:
            return [
                (
                    "no-root-all",
                    f"{ROOT_MODULE} binds no __all__ that is a literal list or tuple of strings; "
                    "declare the package's public modules and subpackages there",
                )
            ]
        refusals: list[tuple[str, str]] = []
        for name in listed:
            module_path, package_path = f"{PACKAGE}/{name}.py", f"{PACKAGE}/{name}/__init__.py"
            if module_path not in carried and package_path not in carried:
                refusals.append(
                    (
                        "listed-absent",
                        f"{name} is listed in {ROOT_MODULE}'s __all__ and the wheel carries neither "
                        f"{module_path} nor {package_path}; list only names the wheel carries",
                    )
                )
                continue
            if package_path in carried:
                refusals.extend(_subpackage_refusals(archive, carried, package_path))
    return refusals


def _parse_arguments(argv: list[str] | None) -> argparse.Namespace:
    """The wheel this gate reads."""
    parser = argparse.ArgumentParser(
        description="Refuse a built wheel whose public API surface has drifted from the package's declarations."
    )
    parser.add_argument(
        "--wheel",
        required=True,
        help="the built wheel, for example dist/multidriver_swg-0.2.0.dev0-py3-none-any.whl",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Print one line per refusal and one summary line, and return the exit status."""
    arguments = _parse_arguments(argv)
    refusals = _refusals(Path(arguments.wheel))
    for name, detail in refusals:
        print(f"refused: {name}: {detail}")
    print(f"{arguments.wheel}: " + (f"{len(refusals)} refusal(s)" if refusals else "conforms"))
    return 1 if refusals else 0


if __name__ == "__main__":
    sys.exit(main())
