"""Fingerprint a function by its syntax tree, so a record can tell when the function's code has changed.

A record of a deliberate departure from legacy code describes a function as it stood when the
record was written. To know later whether that function has changed since, the record stores
a FINGERPRINT: a hash of the function's syntax tree printed in a canonical form. The tree, not
the text, because reformatting, comments and a docstring change nothing the function does.
And a canonical printing, not `ast.dump`, because `ast.dump`'s output moves between
interpreter versions on code that did not change — 3.12 adds an empty `type_params` field to
every function — so a dump-based fingerprint would report every function as changed after an
interpreter upgrade.

The canonical printing writes each node as its type name and its fields in declaration order,
omitting a field whose value is `None` or an empty list, and omitting `ctx` (whether a name is
read or assigned is already fixed by where it sits). Line and column positions are node
attributes rather than fields, so they never appear.

`for_fingerprint` prepares a function for fingerprinting: a copy without its leading docstring
and without the decorators that only record it (`pair_side` and `registers`), so registering a
function, or documenting it, does not change its fingerprint, while every other decorator
does. A label is `canon-ast:{printer version}:{sha256 hex}`.

The printer can still move between interpreters on some syntax. `printer_check` prints a fixed
function and compares the label with the one stored here: a check run under an interpreter
that prints it differently reports that once, naming the interpreter, instead of every recorded
fingerprint reading as moved. Only the standard library is imported, so an interpreter without
`pytest` — the one a pre-commit hook runs — can import this module.
"""

from __future__ import annotations

import ast
import copy
import hashlib
import sys

RULE = "canon-ast"
VERSION = 1

Function = ast.FunctionDef | ast.AsyncFunctionDef

RECORDING_DECORATORS = ("pair_side", "registers")

PRINTER_CHECK_SOURCE = '''
@functools.wraps(other)
def sample(a, /, b: int = 1, *args, c=None, d: "str" = "x", **kwargs) -> dict[str, int]:
    """A docstring the fingerprint ignores."""
    global counter
    total = 0
    for i, value in enumerate(args[1:-1:2]):
        if value is not None and not (i % 2 == 0 or value in {1, 2}):
            total += value ** 2
        elif (n := len(kwargs)) > 3:
            continue
        else:
            break
    try:
        with open(d) as handle, lock:
            data = [x for x in handle if x] + [*args, -b]
    except (OSError, ValueError) as error:
        raise RuntimeError(f"failed {d!r:>10} at {total}") from error
    finally:
        del data
    squares = {k: v for k, v in kwargs.items()}
    pick = (lambda x, *, y=2: x + y)(c) if c else {**squares}
    assert pick, "empty"

    async def inner(source):
        async with source as stream:
            return [x async for x in stream] + [await other(*args, key=b)]

    match total:
        case 0 | 1:
            return {"total": total}
        case [first, *rest] if first:
            return {"first": first}
        case {"k": k, **others}:
            return others
        case Point(x=0) as p:
            return {"p": p}
        case _:
            return {}
'''
PRINTER_CHECK_LABEL = "canon-ast:1:31f2cdfbe79f832b8db1302466a07f5ac59e28c821b93433afb62ec6d58b3590"


def canonical(node: object) -> str:
    """Print `node` as its type and its non-empty fields, recursively, with `ctx` and positions left out."""
    if isinstance(node, ast.AST):
        fields = []
        for name in node._fields:
            if name == "ctx":
                continue
            value = getattr(node, name, None)
            if value is None or value == []:
                continue
            fields.append(f"{name}={canonical(value)}")
        return f"{type(node).__name__}({', '.join(fields)})"
    if isinstance(node, list):
        return f"[{', '.join(canonical(item) for item in node)}]"
    return repr(node)


def for_fingerprint(function: Function) -> Function:
    """A copy of `function` without its leading docstring and without its `pair_side` and `registers` decorators.

    Every other decorator is kept, because it changes what calling the function does. The
    input is not modified. An `async def` is accepted and returned as an `async def`.
    """
    prepared = copy.deepcopy(function)
    body = prepared.body
    first = body[0] if body else None
    if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
        prepared.body = body[1:]
    prepared.decorator_list = [d for d in prepared.decorator_list if _decorator_name(d) not in RECORDING_DECORATORS]
    return prepared


def label(text: str) -> str:
    """The fingerprint label of a canonical printing: `canon-ast:{VERSION}:{sha256 hex of its UTF-8 bytes}`."""
    return f"{RULE}:{VERSION}:{hashlib.sha256(text.encode('utf-8')).hexdigest()}"


def printer_check() -> str | None:
    """A finding naming this interpreter when it does not print the fixed function as recorded, else `None`."""
    function = ast.parse(PRINTER_CHECK_SOURCE).body[0]
    measured = label(canonical(for_fingerprint(function)))
    if measured == PRINTER_CHECK_LABEL:
        return None
    return (
        f"printer check failed under Python {sys.version}: the fixed function prints as {measured}, "
        f"recorded as {PRINTER_CHECK_LABEL}, so no fingerprint was compared"
    )


def _decorator_name(decorator: ast.expr) -> str | None:
    """The name a decorator is written with: `name`, `module.name` and `name(...)` all give `name`."""
    func = decorator.func if isinstance(decorator, ast.Call) else decorator
    if isinstance(func, ast.Attribute):
        return func.attr
    return getattr(func, "id", None)
