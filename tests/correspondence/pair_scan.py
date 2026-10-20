"""Find every declared pair in the package's source, and fingerprint the code each side reaches, without importing it.

A pair is a legacy function and its replacement, each decorated `pair_side(pair=..., side=...)`.
A record of how the two differ stays true only while the code it describes is unchanged, and
that code is more than the decorated function: it is everything the function calls inside the
package, and every module-level value those functions read. So this module does two things
from source text alone.

`scan_pairs` reads every module of a package and lists each declared pair with the site of
each side, plus every declaration it cannot read or that is incomplete. `closure` starts at
one function and follows the calls it can resolve into the package, fingerprinting each
function reached and each module-level binding read. Every call it cannot resolve is LISTED
rather than dropped, because a closure that silently stopped at a call it could not follow
would vouch for code it never looked at.

A call is resolvable when it is a bare-name call of a module-level `def` in the same module;
a call of a name bound by `from {package}[.x] import f` or the relative form; or `alias.f()`
where `alias` is bound by `import {package}.x as alias` or `from {package} import x`. A method
call on an object, a call of a library function, and anything else is unresolved. An
`async def` is followed exactly like a `def`.

Only the standard library and two standard-library modules of this directory are imported, so
an interpreter without `pytest` or the package's dependencies — the one a pre-commit hook runs
— can import this module. The package itself is never imported here; `registry_mismatch`
compares a scan with a registry a caller obtained by importing.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path

from tests.correspondence import fingerprint

# The two sides `multidriver_swg.pairs` accepts; the package is not imported here, so the
# values are restated, and a test pins them equal to `multidriver_swg.pairs.SIDES`.
PAIR_SIDES = ("pre_refactor", "current")

Function = ast.FunctionDef | ast.AsyncFunctionDef
Site = tuple[str, str, int]  # (path relative to the repository root, qualified name, line)


@dataclass
class PairScan:
    """Each declared pair's sides and their sites, and every declaration the scan could not accept."""

    pairs: dict[str, dict[str, Site]] = field(default_factory=dict)
    findings: list[str] = field(default_factory=list)


@dataclass
class Closure:
    """The labels of the code one function reaches, and every call that could not be followed."""

    functions: dict[str, str]
    names: dict[str, str]
    combined: str
    unresolved: list[str]


def repository_root(package_dir: Path) -> Path:
    """The directory module names are counted from: above `src/` for a src layout, else the package's parent."""
    package_dir = Path(package_dir).resolve()
    return package_dir.parent.parent if package_dir.parent.name == "src" else package_dir.parent


def scan_pairs(package_dir: Path, package: str = "multidriver_swg") -> PairScan:
    """Every `pair_side` declaration in the package's modules, read as syntax trees, with each unreadable or incomplete one named."""
    package_dir = Path(package_dir).resolve()
    root = repository_root(package_dir)
    scan = PairScan()
    for path in sorted(package_dir.rglob("*.py")):
        relative = path.relative_to(root).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"))
        aliases = _pair_side_aliases(tree, _dotted(path, root), path.name == "__init__.py")
        read: set[int] = set()
        for qualname, node in _functions_with_qualnames(tree):
            for decorator in node.decorator_list:
                if _is_pair_side(decorator, aliases):
                    read.add(id(decorator))
                    _record_side(scan, decorator, (relative, qualname, decorator.lineno))
        # Every other `pair_side(...)` call still registers when its module is imported (under
        # an `if`, nested, applied by hand, or inside another decorator's expression), so it is
        # named by node identity rather than by position: a call inside a module-level
        # function's decorator expression sits where a read declaration sits and is not one.
        scan.findings += [
            f"{relative}:{n.lineno}: a pair_side(...) call outside the decorator list of a module-level function or method"
            for n in ast.walk(tree)
            if isinstance(n, ast.Call) and _is_pair_side(n, aliases) and id(n) not in read
        ]
    for pair, sides in sorted(scan.pairs.items()):
        if len(sides) == 1:
            ((side, (site_path, qualname, line)),) = sides.items()
            scan.findings.append(f"pair {pair}: only its {side} side is declared, at {site_path}:{line} ({qualname})")
    return scan


def closure(path: Path, function: str, package_dir: Path, package: str = "multidriver_swg") -> Closure:
    """The labels of `function` in the module at `path` and of everything it reaches in the package.

    `path` is absolute or relative to the repository root; `function` is a qualified name in that
    module, such as `f` or `Class.method`. Keys are `{module}:{qualified name}`, the module named
    by its dotted path from the repository root (`multidriver_swg.x`, `tests.registrations.y`).
    """
    index = _Index(Path(package_dir).resolve(), package)
    entry_module = index.module_at(Path(path))
    entry = dict(_functions_with_qualnames(entry_module.tree)).get(function)
    if entry is None:
        raise ValueError(f"{entry_module.name} defines no function {function}")
    functions: dict[str, tuple[_Module, Function]] = {}
    unresolved: list[str] = []
    queue = [(entry_module, function, entry)]
    while queue:
        module, qualname, node = queue.pop(0)
        key = f"{module.name}:{qualname}"
        if key in functions:
            continue
        functions[key] = (module, node)
        local = _local_names(node)
        # Walk the function as it is fingerprinted: a recording decorator only registers the
        # function, so following it would pull the registry's own code into every closure.
        prepared = _prepared(module, node)
        for call in (n for n in ast.walk(prepared) if isinstance(n, ast.Call)):
            target = index.resolve_call(module, call.func, local)
            if target is None:
                unresolved.append(f"{key}:{call.lineno}: {ast.unparse(call.func)}")
            else:
                queue.append(target)
        # A decorator written without parentheses is still a call, `f = deco(f)`, made in the
        # enclosing scope, so it is resolved with no function locals and is followed or listed
        # exactly like a call; the walk above sees only the decorators written as calls.
        for decorator in (d for d in prepared.decorator_list if not isinstance(d, ast.Call)):
            target = index.resolve_call(module, decorator, set())
            if target is None:
                unresolved.append(f"{key}:{decorator.lineno}: {ast.unparse(decorator)}")
            else:
                queue.append(target)
    function_labels = {key: _function_label(module, node) for key, (module, node) in functions.items()}
    names: dict[str, str] = {}
    for module, node in functions.values():
        local = _local_names(node)
        prepared = _prepared(module, node)
        for read in ast.walk(prepared):
            if isinstance(read, ast.Name) and isinstance(read.ctx, ast.Load) and read.id not in local:
                bound = index.binding_of_name(module, read.id)
            elif (
                isinstance(read, ast.Attribute)
                and isinstance(read.ctx, ast.Load)
                and isinstance(read.value, ast.Name)
                and read.value.id not in local
                and read.value.id in module.module_aliases
            ):
                bound = index.binding_in(module.module_aliases[read.value.id], read.attr, seen=set())
            else:
                continue
            if bound is None:
                continue
            owner, name = bound
            key = f"{owner.name}:{name}"
            if key not in function_labels:
                names[key] = _binding_label(owner, owner.bindings[name])
    parts = [f"function {k} {v}" for k, v in sorted(function_labels.items())]
    parts += [f"name {k} {v}" for k, v in sorted(names.items())]
    return Closure(function_labels, names, fingerprint.label("\n".join(parts)), unresolved)


def pair_closures(scan: PairScan, package_dir: Path, package: str = "multidriver_swg") -> dict[str, dict[str, Closure]]:
    """The closure of every scanned side, keyed by pair and side."""
    root = repository_root(package_dir)
    return {
        pair: {
            side: closure(root / path, qualname, package_dir, package) for side, (path, qualname, _) in sides.items()
        }
        for pair, sides in sorted(scan.pairs.items())
    }


def denominators(scan: PairScan, closures: dict[str, dict[str, Closure]]) -> str:
    """One line stating how much the scan covered: `pairs scanned: {n}; functions covered: {per pair}; unresolved calls: {n}`."""
    covered = ", ".join(
        f"{pair}={len({k for c in sides.values() for k in c.functions})}" for pair, sides in sorted(closures.items())
    )
    unresolved = sum(len(c.unresolved) for sides in closures.values() for c in sides.values())
    return f"pairs scanned: {len(scan.pairs)}; functions covered: {covered or 'none'}; unresolved calls: {unresolved}"


def scanned_modules(scan: PairScan, package_dir: Path) -> list[str]:
    """The dotted names of the modules that declare a scanned pair, which a registry check must import."""
    root = repository_root(package_dir)
    return sorted({_dotted(root / path, root) for sides in scan.pairs.values() for path, _, _ in sides.values()})


def registry_mismatch(scan: PairScan, registry: dict[str, dict[str, object]], expected_count: int) -> list[str]:
    """Every disagreement between the scanned pairs, an import-time registry and the expected pair count."""
    scanned = {(pair, side) for pair, sides in scan.pairs.items() for side in sides}
    registered = {(pair, side) for pair, sides in registry.items() for side in sides}
    findings = [
        f"pair {p} side {s} is declared in source but not registered at import" for p, s in sorted(scanned - registered)
    ]
    findings += [
        f"pair {p} side {s} is registered at import but not found by the scan" for p, s in sorted(registered - scanned)
    ]
    if len(scan.pairs) != expected_count:
        findings.append(f"{len(scan.pairs)} pair(s) scanned but the expected pair count is {expected_count}")
    return findings


def _record_side(scan: PairScan, decorator: ast.expr, site: Site) -> None:
    """Add one `pair_side(...)` decorator to the scan, or a finding when it cannot be read or repeats a side."""
    path, qualname, line = site
    keywords = {k.arg: k.value for k in decorator.keywords} if isinstance(decorator, ast.Call) else {}
    pair, side = keywords.get("pair"), keywords.get("side")
    literal = all(isinstance(v, ast.Constant) and isinstance(v.value, str) for v in (pair, side))
    if not literal or decorator.args:
        scan.findings.append(f"{path}:{line}: pair_side on {qualname} names its pair or side by a non-literal")
        return
    if side.value not in PAIR_SIDES:
        scan.findings.append(
            f"{path}:{line}: pair_side on {qualname} names side {side.value!r}, not one of {PAIR_SIDES}"
        )
        return
    sides = scan.pairs.setdefault(pair.value, {})
    if side.value in sides:
        first = sides[side.value]
        scan.findings.append(
            f"pair {pair.value}: side {side.value} declared twice, at {first[0]}:{first[2]} and {path}:{line}"
        )
        return
    sides[side.value] = site


def _call_name(node: ast.expr) -> str | None:
    """The name a decorator or call is written with: `name`, `module.name` and `name(...)` all give `name`."""
    func = node.func if isinstance(node, ast.Call) else node
    if isinstance(func, ast.Attribute):
        return func.attr
    return getattr(func, "id", None)


def _functions_with_qualnames(tree: ast.Module) -> list[tuple[str, Function]]:
    """Every module-level function and every method of a module-level class, with its qualified name."""
    found = []
    for node in tree.body:
        if isinstance(node, Function):
            found.append((node.name, node))
        elif isinstance(node, ast.ClassDef):
            found += [(f"{node.name}.{n.name}", n) for n in node.body if isinstance(n, Function)]
    return found


def _local_names(function: Function) -> set[str]:
    """Names bound inside `function` — parameters, assignments, imports, nested definitions — minus its `global` names."""
    args = function.args
    local = {a.arg for a in [*args.posonlyargs, *args.args, *args.kwonlyargs, args.vararg, args.kwarg] if a}
    declared_global: set[str] = set()
    for node in ast.walk(function):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store | ast.Del):
            local.add(node.id)
        elif isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef) and node is not function:
            local.add(node.name)
        elif isinstance(node, ast.Import | ast.ImportFrom):
            local.update((a.asname or a.name).split(".")[0] for a in node.names)
        elif isinstance(node, ast.Global):
            declared_global.update(node.names)
    return local - declared_global


def _prepared(module: _Module, node: Function) -> Function:
    """`node` as it is fingerprinted: `for_fingerprint`'s copy, also without a `pair_side` decorator written by an alias.

    `for_fingerprint` drops decorators by their written name, so `@ps(...)` after
    `from {package}.pairs import pair_side as ps` would stay, and the closure would follow it
    into the registry's own code. The scan's recognition test decides which decorators record.
    """
    prepared = fingerprint.for_fingerprint(node)
    prepared.decorator_list = [d for d in prepared.decorator_list if not _is_pair_side(d, module.pair_side_aliases)]
    return prepared


def _function_label(module: _Module, node: Function) -> str:
    """A function's label: the canonical printing of it without its docstring and recording decorators."""
    return fingerprint.label(fingerprint.canonical(_prepared(module, node)))


def _binding_label(module: _Module, statements: list[ast.stmt]) -> str:
    """The label of every module-level statement that binds one name, in source order."""
    return fingerprint.label(
        "\n".join(fingerprint.canonical(_prepared(module, s) if isinstance(s, Function) else s) for s in statements)
    )


def _pair_side_aliases(tree: ast.Module, module_name: str, is_package: bool) -> set[str]:
    """Names a module binds to `pair_side` by `from {package}.pairs import pair_side as X` or that import's relative form.

    The relative form is resolved to its absolute module first, at whatever depth it is
    written — `from .pairs` in a root module, `from ..pairs` one subpackage down, and so on —
    so the test is one comparison on the resolved module rather than on the number of dots: a
    test keyed on one depth reads the root-level alias and misses the same import written
    from a subpackage, which still registers at import.
    """
    aliases = set()
    for statement in _top_level(tree.body):
        if not isinstance(statement, ast.ImportFrom):
            continue
        if statement.level == 0:
            names_pairs = (statement.module or "").endswith(".pairs")
        else:
            source = _resolve_relative(module_name, is_package, statement.level, statement.module)
            names_pairs = source is not None and source.split(".")[-1] == "pairs"
        if names_pairs:
            aliases.update(a.asname for a in statement.names if a.name == "pair_side" and a.asname)
    return aliases


def _resolve_relative(module_name: str, is_package: bool, level: int, module: str | None) -> str | None:
    """The absolute dotted module a relative `from {dots}{module} import` names, or `None` when its dots climb past the top."""
    base = module_name.split(".") if is_package else module_name.split(".")[:-1]
    keep = len(base) - (level - 1)
    if keep < 1:
        return None
    return ".".join([*base[:keep], module] if module else base[:keep])


def _is_pair_side(node: ast.expr, aliases: set[str]) -> bool:
    """Whether a decorator or call is `pair_side`: final name `pair_side`, bare or as an attribute, or a bare import alias of it."""
    func = node.func if isinstance(node, ast.Call) else node
    return _call_name(node) == "pair_side" or (isinstance(func, ast.Name) and func.id in aliases)


def _dotted(path: Path, root: Path) -> str:
    """A module's dotted name from the repository root; a package's `__init__` is named by its directory."""
    parts = list(path.resolve().relative_to(root).with_suffix("").parts)
    if parts and parts[0] == "src":
        parts = parts[1:]
    if parts and parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


@dataclass
class _Module:
    """One parsed module and what its top level binds."""

    name: str
    is_package: bool
    tree: ast.Module
    defs: dict[str, Function]
    bindings: dict[str, list[ast.stmt]]
    from_imports: dict[str, tuple[str, str]]  # local name -> (package module, attribute)
    module_aliases: dict[str, str]  # local name -> package module
    pair_side_aliases: set[str] = field(default_factory=set)


class _Index:
    """Parses package modules on demand and resolves calls against them."""

    def __init__(self, package_dir: Path, package: str) -> None:
        """Index the package rooted at `package_dir`, whose import name is `package`."""
        self.package_dir, self.package, self.root = package_dir, package, repository_root(package_dir)
        self._modules: dict[Path, _Module] = {}

    def module_at(self, path: Path) -> _Module:
        """The module at `path`, absolute or relative to the repository root, parsed once."""
        path = (path if path.is_absolute() else self.root / path).resolve()
        if path not in self._modules:
            self._modules[path] = self._parse(path)
        return self._modules[path]

    def package_module(self, dotted: str) -> _Module | None:
        """The package module with that dotted name, or `None` when it is not a module of the package."""
        if dotted != self.package and not dotted.startswith(self.package + "."):
            return None
        base = self.package_dir.joinpath(*dotted.split(".")[1:])
        for candidate in (base.with_suffix(".py"), base / "__init__.py"):
            if candidate.is_file():
                return self.module_at(candidate)
        return None

    def resolve_call(self, module: _Module, func: ast.expr, local: set[str]) -> tuple[_Module, str, Function] | None:
        """The package function a call reaches, as `(module, qualified name, node)`, or `None` when it is unresolvable."""
        if isinstance(func, ast.Name) and func.id not in local:
            if func.id in module.defs:
                return module, func.id, module.defs[func.id]
            if func.id in module.from_imports:
                return self._function(*module.from_imports[func.id], seen=set())
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name) and func.value.id not in local:
            target = module.module_aliases.get(func.value.id)
            if target is not None:
                return self._function(target, func.attr, seen=set())
        return None

    def binding_of_name(self, module: _Module, name: str) -> tuple[_Module, str] | None:
        """Where a module-level name `module` reads is bound: its defining package module and name, or `module` itself.

        A name imported from the package (`from {package}[.x] import NAME`, or the relative form)
        is followed to the binding of `NAME` in the module that defines it, through re-exports;
        it keeps its import statement only when it resolves to no package binding. A name the
        module binds itself, by assignment, definition or a non-package import, is its own.
        `None` when the module binds no such name.
        """
        if name in module.from_imports:
            return self.binding_in(*module.from_imports[name], seen=set()) or (module, name)
        return (module, name) if name in module.bindings else None

    def binding_in(self, dotted: str, attribute: str, seen: set) -> tuple[_Module, str] | None:
        """The module and name that bind `attribute` in package module `dotted`, following re-exports by import."""
        module = self.package_module(dotted)
        if module is None or (dotted, attribute) in seen:
            return None
        seen.add((dotted, attribute))
        if attribute in module.from_imports:
            return self.binding_in(*module.from_imports[attribute], seen=seen)
        return (module, attribute) if attribute in module.bindings else None

    def _function(self, dotted: str, attribute: str, seen: set) -> tuple[_Module, str, Function] | None:
        """A function named `attribute` in package module `dotted`, following re-exports by import."""
        module = self.package_module(dotted)
        if module is None or (dotted, attribute) in seen:
            return None
        seen.add((dotted, attribute))
        if attribute in module.defs:
            return module, attribute, module.defs[attribute]
        if attribute in module.from_imports:
            return self._function(*module.from_imports[attribute], seen=seen)
        return None

    def _parse(self, path: Path) -> _Module:
        """Parse one module and record its definitions, its bindings and its imports of package names."""
        tree = ast.parse(path.read_text(encoding="utf-8"))
        name = _dotted(path, self.root)
        is_package = path.name == "__init__.py"
        module = _Module(name, is_package, tree, {}, {}, {}, {}, _pair_side_aliases(tree, name, is_package))
        for statement in _top_level(tree.body):
            for bound in _bound_names(statement):
                module.bindings.setdefault(bound, []).append(statement)
            if isinstance(statement, Function):
                module.defs[statement.name] = statement
            elif isinstance(statement, ast.Import):
                for alias in statement.names:
                    if alias.asname and self.package_module(alias.name) is not None:
                        module.module_aliases[alias.asname] = alias.name
            elif isinstance(statement, ast.ImportFrom):
                source = self._absolute(module, statement)
                if source is None:
                    continue
                for alias in statement.names:
                    local = alias.asname or alias.name
                    if self.package_module(f"{source}.{alias.name}") is not None:
                        module.module_aliases[local] = f"{source}.{alias.name}"
                    else:
                        module.from_imports[local] = (source, alias.name)
        return module

    def _absolute(self, module: _Module, statement: ast.ImportFrom) -> str | None:
        """The package module a `from ... import` names, resolving the relative form; `None` outside the package."""
        if statement.level == 0:
            source = statement.module or ""
        elif module.name == self.package or module.name.startswith(self.package + "."):
            base = module.name.split(".") if module.is_package else module.name.split(".")[:-1]
            base = base[: len(base) - (statement.level - 1)]
            source = ".".join([*base, statement.module] if statement.module else base)
        else:
            return None
        return source if source == self.package or source.startswith(self.package + ".") else None


def _top_level(body: list[ast.stmt]) -> list[ast.stmt]:
    """Module-level statements, including those inside a top-level `if` or `try`, but not inside a definition."""
    found = []
    for statement in body:
        found.append(statement)
        if isinstance(statement, ast.If | ast.Try):
            for block in (statement.body, statement.orelse, getattr(statement, "finalbody", [])):
                found += _top_level(block)
            for handler in getattr(statement, "handlers", []):
                found += _top_level(handler.body)
    return found


def _bound_names(statement: ast.stmt) -> list[str]:
    """The names one module-level statement binds."""
    if isinstance(statement, Function | ast.ClassDef):
        return [statement.name]
    if isinstance(statement, ast.Import | ast.ImportFrom):
        return [(a.asname or a.name).split(".")[0] for a in statement.names]
    if isinstance(statement, ast.Assign | ast.AnnAssign | ast.AugAssign):
        targets = statement.targets if isinstance(statement, ast.Assign) else [statement.target]
        return [n.id for t in targets for n in ast.walk(t) if isinstance(n, ast.Name)]
    return []
