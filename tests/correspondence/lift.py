"""Run a stored copy of legacy code unchanged, on declared inputs, and say what built the result.

An equivalence test compares a new implementation with what a legacy unit computes. The legacy
tree must not be imported or run, and its unit is not a function of its own: a cell reads names that
earlier cells bound, and a function may read a name bound only through a star-import. So the unit's
stored text (the extraction) is compiled here, in a namespace built only from bindings its
declaration supplies, and executed. This module is that runner, with the registry of declarations
it runs and the digest that identifies a lift.

A declaration is a function in `tests/lifts/{system}.py`, found by walking that directory the way the
binding walks `tests/registrations/`, decorated with `declares_lift(unit=..., ...)`. It receives
the fixtures it names, loaded and digest-checked, as one dict keyed by fixture name, and returns
`(bindings, result)`: the names the legacy text reads, and a function that reduces the finished
namespace to the value the test compares (the legacy side's reduction).

`run_lift` compiles each stored text of the unit under the legacy file's repository-relative path with
its original line numbers, so a coverage measurement names legacy lines. It refuses, before executing
anything, a text that reads a name that is neither bound by the texts nor a builtin nor declared, and
a declared binding that is never read. `lift_digest` is the label of a canonical record of the lift;
it moves when the lift's inputs or logic move and does not when only the declaration's formatting
does. Every code object `run_lift` compiles is kept in `compiled_code_objects()` for the check that
no test runs a legacy file.

A declaration is itself refused when any code object it compiles reads a global name that its module
binds at top level, other than a name bound by an import of a library outside the checkout. A helper
a lift needs is therefore nested inside the declaration, where its text is covered by the digest.
"""

from __future__ import annotations

import ast
import builtins
import difflib
import dis
import hashlib
import importlib.util
import inspect
import json
import re
import sys
import textwrap
import types
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tests.correspondence import extent, extraction, fixtures, generate_extractions, ledger

REPO_ROOT = extraction.REPO_ROOT
LIFTS = Path("tests/lifts")
ADAPTED = Path("tests/lifts/adapted")
RUNNER_VERSION = 1
FORMS = ("unchanged", "adapted")
DRAWS = ("none", "seeded", "unseeded")
INTERNAL_PACKAGES = ("tests", "multidriver_swg")
NON_PATH_ORIGINS = ("built-in", "frozen")  # a spec origin that names a kind of module and not a file
_COMPILED: list[types.CodeType] = []


class LiftError(Exception):
    """A lift or its declaration that cannot be registered, run or digested, with the reason in the message."""


# ---- declaring a lift ----


def declares_lift(
    unit: str,
    part: str | None = None,
    form: str = "unchanged",
    fixtures: list[str] | tuple[str, ...] = (),
    draws: str = "none",
) -> Callable[[Callable], Callable]:
    """Mark a function as the declaration of the lift of `unit` (and `part`), refusing a malformed mark.

    The mark is a plain attribute on the function and not an entry in a registry held here, so it
    survives this module being loaded twice: discovery reads the attribute and never compares classes.
    """
    if not isinstance(unit, str) or not ledger.KEY_RE.match(unit):
        raise LiftError(f"a lift's unit is a canonical key such as 'file.py:1-9'; {unit!r} is not")
    if part is not None and not isinstance(part, str):
        raise LiftError(f"the part of lift {unit!r} must be a string or None, not {part!r}")
    if form not in FORMS or draws not in DRAWS:
        raise LiftError(
            f"lift {unit!r}: form must be one of {FORMS} and draws one of {DRAWS}, not {form!r} and {draws!r}"
        )
    if not all(isinstance(name, str) for name in fixtures):
        raise LiftError(f"lift {unit!r}: fixtures must be a list of names")
    mark = {"unit": unit, "part": part, "form": form, "fixtures": tuple(fixtures), "draws": draws}

    def decorate(function: Callable) -> Callable:
        """Attach the mark to `function` and return it unchanged."""
        function.__lift__ = mark
        return function

    return decorate


@dataclass(frozen=True)
class LiftDeclaration:
    """One discovered declaration: its mark, the function, the system it belongs to and the file it is in."""

    unit: str
    part: str | None
    form: str
    fixtures: tuple[str, ...]
    draws: str
    function: Callable
    system: str
    path: Path
    root: Path


# ---- reading names out of code ----


def _walk_code(code: types.CodeType):
    yield code
    for const in code.co_consts:
        if isinstance(const, types.CodeType):
            yield from _walk_code(const)


def _names(code: types.CodeType) -> tuple[set[str], set[str], bool]:
    """Return the names read as globals, the names bound at module level, and whether a star-import occurs.

    A name read in a class body that the same body binds is the body's own, so it is not a read of
    a global. Only the module-level code object binds module names; a `global` statement's store is
    a module binding as well.
    """
    reads: set[str] = set()
    binds: set[str] = set()
    star = False
    for each in _walk_code(code):
        local_reads: set[str] = set()
        local_stores: set[str] = set()
        for instruction in dis.get_instructions(each):
            if instruction.opname in ("LOAD_GLOBAL", "LOAD_NAME"):
                local_reads.add(instruction.argval)
            elif instruction.opname in ("STORE_NAME", "DELETE_NAME"):
                local_stores.add(instruction.argval)
            elif instruction.opname == "STORE_GLOBAL":
                binds.add(instruction.argval)
            elif instruction.opname == "IMPORT_STAR":
                star = True
        if each.co_name == "<module>":
            binds |= local_stores
            reads |= local_reads
        else:
            reads |= local_reads - local_stores
    return reads, binds, star


# ---- refusing a declaration at registration ----


def _top_level_bindings(tree: ast.Module) -> dict[str, list[ast.AST]]:
    """Return each name the module binds at top level, with the nodes that bind it.

    The walk stops at a function, class, lambda or comprehension, whose inside is a scope of its own;
    the definition's own name is bound at the level it stands on.
    """
    found: dict[str, list[ast.AST]] = {}

    def add(name: str, node: ast.AST) -> None:
        """Record `node` as binding `name`."""
        found.setdefault(name, []).append(node)

    def visit(node: ast.AST) -> None:
        """Record the bindings of `node` and of its children, stopping at the inside of another scope."""
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            add(node.name, node)
            return
        if isinstance(node, ast.Lambda | ast.ListComp | ast.SetComp | ast.DictComp | ast.GeneratorExp):
            return
        if isinstance(node, ast.Import | ast.ImportFrom):
            for alias in node.names:
                add((alias.asname or alias.name).split(".")[0], node)
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store | ast.Del):
            add(node.id, node)
        elif isinstance(node, ast.ExceptHandler) and node.name:
            add(node.name, node)
        for child in ast.iter_child_nodes(node):
            visit(child)

    for statement in tree.body:
        visit(statement)
    return found


def _admitted_import(node: ast.AST, root: Path) -> bool:
    """Whether an import binds a library that lies outside the checkout `root` and is not one of its packages.

    A module whose spec origin is `built-in` or `frozen` is outside the checkout whatever the working directory;
    a namespace package (origin `None`) is judged by the directories it searches.
    """
    if isinstance(node, ast.ImportFrom):
        modules = [node.module or ""] if node.level == 0 else []
    elif isinstance(node, ast.Import):
        modules = [alias.name for alias in node.names]
    else:
        return False
    if not modules or any(not module for module in modules):
        return False
    for module in modules:
        top = module.split(".")[0]
        if top in INTERNAL_PACKAGES:
            return False
        try:
            spec = importlib.util.find_spec(top)
        except (ImportError, ValueError):
            return False
        if spec is None:
            return False
        # A built-in or frozen module's origin is the word `built-in` or `frozen`, not a path: resolving it
        # as one reads it against the working directory, which lies under the checkout when a run starts at
        # the root, and would refuse `sys` or `os` there and admit them from anywhere else.
        if spec.origin in NON_PATH_ORIGINS:
            continue
        # A namespace package has no origin; its directories say where it lies.
        places = list(spec.submodule_search_locations or []) if spec.origin is None else [spec.origin]
        for place in places:
            try:
                Path(place).resolve().relative_to(Path(root).resolve())
            except ValueError:
                continue
            return False
    return True


def declaration_findings(source: str, name: str, root: Path = REPO_ROOT) -> list[str]:
    """Return one finding per top-level name that the declaration `name` reads and its module binds, naming the name.

    `source` is the whole module's text. The declaration is compiled without its decorators, so that
    the code objects checked are the function's own, nested ones included. A name bound at top level
    only by admitted imports (a library outside the checkout) is allowed; any other top-level binding,
    a relative import, an import of this checkout's packages and the declaration's own name are not.
    """
    tree = ast.parse(source)
    declaration = next(
        (n for n in tree.body if isinstance(n, ast.FunctionDef | ast.AsyncFunctionDef) and n.name == name), None
    )
    if declaration is None:
        return [f"{name}: no top-level function of that name in the module"]
    bare = ast.FunctionDef(**{**{f: getattr(declaration, f) for f in declaration._fields}, "decorator_list": []})
    bare = ast.copy_location(bare, declaration)
    module = ast.Module(body=[bare], type_ignores=[])
    code = compile(ast.fix_missing_locations(module), f"<declaration {name}>", "exec")
    reads, _binds, _star = _names(code)
    bindings = _top_level_bindings(tree)
    findings = []
    for read in sorted(reads):
        nodes = bindings.get(read)
        if not nodes:
            continue
        if all(_admitted_import(node, root) for node in nodes):
            continue
        findings.append(
            f"{name}: reads the module-level name {read!r}, which the module binds at top level; "
            "nest the helper inside the declaration, or import a library from outside the checkout"
        )
    return findings


# ---- discovering declarations ----


def discover(root: Path = REPO_ROOT) -> list[LiftDeclaration]:
    """Every declaration under `{root}/tests/lifts/`, refused at registration if it reads a module-level name.

    Each `{system}.py` is executed once to read its marks, after its source is checked for names a
    declaration may not read. Two declarations of one (unit, part) are refused, naming both.
    """
    found: dict[tuple[str, str | None], LiftDeclaration] = {}
    directory = Path(root) / LIFTS
    if not directory.is_dir():
        return []
    for path in sorted(directory.glob("*.py")):
        if path.name.startswith("_"):
            continue
        module_name = f"_lifts_{path.stem}"
        spec = importlib.util.spec_from_file_location(module_name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        source = path.read_text(encoding="utf-8")
        for function in vars(module).values():
            if not callable(function) or not hasattr(function, "__lift__"):
                continue
            if getattr(function, "__module__", None) != module_name:
                continue
            problems = declaration_findings(source, function.__name__, root)
            if problems:
                raise LiftError("\n".join(problems))
            mark = function.__lift__
            declaration = LiftDeclaration(**mark, function=function, system=path.stem, path=path, root=Path(root))
            key = (mark["unit"], mark["part"])
            if key in found:
                raise LiftError(
                    f"system {path.stem!r} declares the lift {key!r} twice: {found[key].function.__name__} and {function.__name__}"
                )
            found[key] = declaration
    return list(found.values())


# ---- the lift's stored texts ----


def _section(root: Path) -> tuple[str, str]:
    """Return the first corpus-record section's name and its directory, the section the extraction store covers."""
    section = extent._section(root)
    return section["name"], section["directory"].rstrip("/")


def _ranges(declaration: LiftDeclaration) -> list[tuple[str, int, int]]:
    return generate_extractions._ranges(declaration.unit)


def _stored_texts(declaration: LiftDeclaration) -> list[tuple[str, int, int, bytes, Path]]:
    """Return each range of the unit as (file, first, last, bytes, path) from the stored extraction or the adapted copy."""
    name, _ = _section(declaration.root)
    out = []
    for file, first, last in _ranges(declaration):
        stored = declaration.root / extraction.stored_path(name, file, first, last)
        if declaration.form == "adapted":
            path = declaration.root / ADAPTED / declaration.system / Path(file).stem / f"{first}-{last}.txt"
        else:
            path = stored
        if not path.is_file():
            raise LiftError(f"{declaration.unit}: the {declaration.form} text {path} is missing")
        out.append((file, first, last, path.read_bytes(), path))
    return out


def _adapted_diff(old: bytes, new: bytes) -> str:
    """The unified diff of a stored text to its adapted copy, with fixed labels and unix line ends."""
    lines = difflib.unified_diff(
        old.decode("utf-8").splitlines(), new.decode("utf-8").splitlines(), "stored", "adapted", lineterm=""
    )
    return "".join(f"{line}\n" for line in lines)


def _check_adapted(declaration: LiftDeclaration) -> None:
    """Refuse an adapted lift whose stored diff is not the diff of its stored text to its adapted copy."""
    name, _ = _section(declaration.root)
    for file, first, last, adapted, path in _stored_texts(declaration):
        stored = declaration.root / extraction.stored_path(name, file, first, last)
        diff_path = path.with_suffix(".diff")
        if not stored.is_file() or not diff_path.is_file():
            raise LiftError(f"{declaration.unit}: the stored text {stored} or the stored diff {diff_path} is missing")
        if diff_path.read_text(encoding="utf-8") != _adapted_diff(stored.read_bytes(), adapted):
            raise LiftError(
                f"{diff_path}: it is not the diff of {stored} to {path}; the adapted copy or its diff was edited"
            )


def legacy_lines(declaration: LiftDeclaration) -> dict[str, dict[int, int]]:
    """Map each adapted copy's line numbers to legacy line numbers, per range, through the stored diff's unchanged blocks.

    The key is `{first}-{last}` of the range. A line inside a changed hunk has no legacy number and is
    absent from the mapping. Only an adapted declaration has an adapted copy to map.
    """
    if declaration.form != "adapted":
        raise LiftError(f"{declaration.unit}: only an adapted lift has lines to map")
    _check_adapted(declaration)
    return {
        f"{first}-{last}": _map_range(first, last, path.with_suffix(".diff").read_text(encoding="utf-8"))
        for _file, first, last, _adapted, path in _stored_texts(declaration)
    }


def _map_range(first: int, last: int, diff: str) -> dict[int, int]:
    """Map the adapted lines of one range to legacy line numbers by walking the unified diff of stored text to adapted copy."""
    pairs: dict[int, int] = {}
    position = [0, 0]  # lines consumed from the stored text and from the adapted copy

    def carry(count: int) -> None:
        """Advance over `count` unchanged lines, recording each adapted line's legacy number."""
        for _ in range(count):
            position[0] += 1
            position[1] += 1
            pairs[position[1]] = first + position[0] - 1

    for line in diff.splitlines()[2:]:
        header = re.match(r"@@ -(\d+)(?:,(\d+))? \+\d+(?:,\d+)? @@", line)
        if header:
            length = 1 if header[2] is None else int(header[2])
            carry((int(header[1]) if length == 0 else int(header[1]) - 1) - position[0])
        elif line.startswith(" "):
            carry(1)
        elif line.startswith("-"):
            position[0] += 1
        elif line.startswith("+"):
            position[1] += 1
    carry(last - first + 1 - position[0])
    return pairs


# ---- running a lift ----


def _compile(declaration: LiftDeclaration) -> list[types.CodeType]:
    """Compile each stored text of the unit, under the legacy path or the adapted copy's own path."""
    _, directory = _section(declaration.root)
    codes = []
    for file, first, _last, data, path in _stored_texts(declaration):
        try:
            tree = ast.parse(data.decode("utf-8"))
        except (SyntaxError, UnicodeDecodeError) as exc:
            raise LiftError(f"{declaration.unit}: the text {path} does not parse ({exc})") from exc
        if declaration.form == "adapted":
            filename = path.relative_to(declaration.root).as_posix()
        else:
            ast.increment_lineno(tree, first - 1)
            filename = f"{directory}/{file}"
        codes.append(compile(tree, filename, "exec"))
    return codes


def _load(declaration: LiftDeclaration) -> dict[str, Any]:
    return {name: fixtures.load(declaration.system, name, root=declaration.root) for name in declaration.fixtures}


def run_lift(declaration: LiftDeclaration) -> Any:
    """Run the unit's stored texts in order in one namespace of the declared bindings; return the declared reduction.

    Before anything executes, the texts are compiled and their names checked: a name read that no text
    binds, no builtin supplies and no binding declares is refused, and so is a binding never read, each
    naming the names. The namespace holds the bindings, `__builtins__` and `__name__` and nothing else.
    """
    if declaration.form == "adapted":
        _check_adapted(declaration)
    codes = _compile(declaration)
    bindings, result = declaration.function(_load(declaration))
    reads: set[str] = set()
    binds: set[str] = set()
    for code in codes:
        code_reads, code_binds, star = _names(code)
        if star:
            raise LiftError(f"{declaration.unit}: a star-import binds names that cannot be read from the text")
        reads |= code_reads
        binds |= code_binds
    unknown = sorted(reads - binds - set(dir(builtins)) - set(bindings) - {"__name__"})
    unread = sorted(set(bindings) - reads)
    if unknown:
        raise LiftError(f"{declaration.unit}: the text reads names that nothing binds or declares: {unknown}")
    if unread:
        raise LiftError(f"{declaration.unit}: declared bindings that the text never reads: {unread}")
    _COMPILED.extend(codes)
    namespace: dict[str, Any] = {"__builtins__": builtins, "__name__": "lift", **bindings}
    for code in codes:
        exec(code, namespace)
    return result(namespace)


def compiled_code_objects() -> list[types.CodeType]:
    """Return every code object `run_lift` has compiled in this process, in order, for the check that reads them."""
    return list(_COMPILED)


# ---- identifying a lift ----


def _canonical(node: Any) -> Any:
    """Serialise a syntax tree as nested lists: class name and fields, omitting positions, empty fields and docstrings."""
    if isinstance(node, ast.AST):
        fields = {}
        for name in node._fields:
            value = getattr(node, name, None)
            if name == "body" and isinstance(value, list):
                value = _without_docstring(node, value)
            if value is None or value == []:
                continue
            fields[name] = _canonical(value)
        return [type(node).__name__, fields]
    if isinstance(node, list):
        return [_canonical(item) for item in node]
    return [type(node).__name__, repr(node)]


def _without_docstring(parent: ast.AST, body: list) -> list:
    """Drop the leading string-constant statement of a function, class or module body."""
    if (
        isinstance(parent, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef | ast.Module)
        and body
        and isinstance(body[0], ast.Expr)
        and isinstance(body[0].value, ast.Constant)
        and isinstance(body[0].value.value, str)
    ):
        return body[1:]
    return body


def canonical_tree(source: str) -> str:
    """Return the canonical serialisation of a declaration's source, as JSON text."""
    tree = ast.parse(textwrap.dedent(source))
    return json.dumps(_canonical(tree), sort_keys=True, separators=(",", ":"))


def lift_digest(declaration: LiftDeclaration) -> str:
    """Return `lift:{RUNNER_VERSION}:{sha256}` of the canonical record of the lift.

    The record holds the unit, part, form, the digests of the stored texts (or of the adapted copies),
    the fixture names, the runner's version and the canonical tree of the declaration function.
    """
    texts = [extraction.label(extraction.STORED_RULE, data) for _f, _a, _b, data, _p in _stored_texts(declaration)]
    record = {
        "unit": declaration.unit,
        "part": declaration.part,
        "form": declaration.form,
        "texts": texts,
        "fixtures": list(declaration.fixtures),
        "runner": RUNNER_VERSION,
        "declaration": canonical_tree(inspect.getsource(declaration.function)),
    }
    digest = hashlib.sha256(json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    return f"lift:{RUNNER_VERSION}:{digest}"


if __name__ == "__main__":
    sys.exit("tests.correspondence.lift is a library; discover its declarations with lift.discover()")
