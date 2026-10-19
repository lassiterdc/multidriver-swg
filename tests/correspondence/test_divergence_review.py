"""Reviewer tests for package 3a: properties of the pair scanner, the closure and the detector that the reviewing seat found unenforced.

Each test builds a synthetic package under the test's temporary directory and reads it as source,
never importing it, so nothing here depends on what the real package registers.
"""

from pathlib import Path

import pytest

from tests.correspondence import pair_scan

pytestmark = pytest.mark.state_check

DECORATORS = "SCALE = 2\n\n\ndef wrap(fn):\n    return fn\n"
CHANGED_DECORATORS = "SCALE = 2\n\n\ndef wrap(fn):\n    return print\n"


def _package(tmp_path: Path, decorated: str) -> Path:
    """A src-layout package `p` with a decorator module `p.deco` and a module `p.a` holding `decorated`."""
    package = tmp_path / "src" / "p"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "deco.py").write_text(DECORATORS, encoding="utf-8")
    (package / "a.py").write_text(
        "import functools\n\nimport p.deco as dm\nfrom p.deco import wrap\n\n\n"
        "def local_wrap(fn):\n    return fn\n\n\n" + decorated,
        encoding="utf-8",
    )
    return package


@pytest.mark.parametrize(
    ("decorated", "decorator_key"),
    [
        ("@wrap\ndef side(x):\n    return x\n", "p.deco:wrap"),
        ("@dm.wrap\ndef side(x):\n    return x\n", "p.deco:wrap"),
        ("@local_wrap\ndef side(x):\n    return x\n", "p.a:local_wrap"),
    ],
    ids=["from-import", "module-alias-attribute", "same-module"],
)
def test_a_decorator_written_without_a_call_is_followed_as_the_call_it_is(tmp_path, decorated, decorator_key):
    """Property: a kept decorator written as a bare name or attribute, naming a package function by a resolvable form, is in the closure.

    Class: every decorator applied to a function in a closure that is written without call
    parentheses (`@wrap`, `@dm.wrap`, `@local_wrap`) and that names a package function by one of
    the resolvable forms: a same-module `def`, a `from {package}.x import f` name, or an
    `import {package}.x as alias` attribute. Why: Python applies a decorator by calling it, so
    `@wrap` above `def side` runs `side = wrap(side)`, and what calling `side` does depends on
    `wrap`'s code. A closure that neither follows nor lists that call lets `wrap` change, here
    to return `print` instead of the function, while the side's combined label and its function
    set stay exactly as recorded, which is a pair entry vouching for code it never looked at.
    Killed by: resolving each kept decorator expression that is not an `ast.Call` exactly as the
    closure resolves a call's `func`, following it when it resolves and listing it as unresolved
    when it does not. Second correct implementation: a separate pass over the kept decorator
    list that enqueues each resolved decorator target after the call walk, rather than
    resolving decorators inside it; the decorator then lands in the same function set.
    """
    package = _package(tmp_path, decorated)
    before = pair_scan.closure(package / "a.py", "side", package, package="p")
    (package / "deco.py").write_text(CHANGED_DECORATORS, encoding="utf-8")
    (package / "a.py").write_text(
        (package / "a.py")
        .read_text(encoding="utf-8")
        .replace("def local_wrap(fn):\n    return fn", "def local_wrap(fn):\n    return print"),
        encoding="utf-8",
    )
    after = pair_scan.closure(package / "a.py", "side", package, package="p")
    assert decorator_key in before.functions and after.combined != before.combined, (
        sorted(before.functions),
        before.unresolved,
        after.combined == before.combined,
    )


def test_a_decorator_written_without_a_call_that_cannot_be_resolved_is_listed(tmp_path):
    """Property: a kept decorator written without call parentheses that names no package function is listed as unresolved.

    Class: every bare-name or attribute decorator in a closure that is not one of the resolvable
    forms, such as a library function (`@functools.cache`). Why: it is a call Python makes when
    the module is imported, and the closure's contract is that a call it cannot follow is
    counted and listed, never dropped; a decorator from a library changes what the decorated
    function does exactly as a library call inside its body does, and the body call is listed.
    Killed by: the same change that kills the test above, which lists a kept decorator
    expression it cannot resolve. Second correct implementation: one that lists the decorator
    as `@functools.cache` at the decorated function's line rather than at the decorator's own
    line, which still names the expression.
    """
    package = _package(tmp_path, "@functools.cache\ndef side(x):\n    return x\n")
    result = pair_scan.closure(package / "a.py", "side", package, package="p")
    assert any("functools.cache" in entry for entry in result.unresolved), result.unresolved


def _subpackage_tree(tmp_path: Path) -> tuple[Path, Path]:
    """A src-layout package `z` with a root `pairs` re-export and the subpackages `z.sub` and `z.sub.deep`; returns (root, package)."""
    package = tmp_path / "src" / "z"
    (package / "sub" / "deep").mkdir(parents=True)
    for directory in (package, package / "sub", package / "sub" / "deep"):
        (directory / "__init__.py").write_text("", encoding="utf-8")
    (package / "pairs.py").write_text("from multidriver_swg.pairs import pair_side\n", encoding="utf-8")
    return tmp_path, package


def test_a_relative_pair_side_alias_from_a_subpackage_is_read_or_named(tmp_path):
    """Property: `pair_side` imported under an alias by the relative form from ANY depth of the package is recognised.

    Class: every module of a subpackage that binds `pair_side` to an alias by the relative import
    reaching the package's root `pairs` module, `from ..pairs import pair_side as decl` from
    `z.sub` and `from ...pairs import pair_side as decl` from `z.sub.deep`, then declares with
    `@decl(...)` either on a module-level function, where it must be a scanned pair, or under a
    module-level `if`, where it must be a finding. Why: each of these registers its pair when the
    module is imported, exactly as `from .pairs import pair_side as decl` in a root-level module
    does, which the scan reads; a module one directory deeper writes the same import with one
    more dot, and today the scan neither reads nor names it, so a registered pair is invisible
    to the scan, to the closure and to the registry check, and only the expected count can
    disagree. Killed by: resolving each relative `ImportFrom` to its absolute module, as the
    closure's own `_absolute` already does for `from_imports`, before testing whether it names a
    `pairs` module. Second correct implementation: counting the leading dots against the
    importing module's package depth to decide whether the import reaches the root `pairs`.
    """
    root, package = _subpackage_tree(tmp_path)
    body = "\ndef a(x):\n    return x\n"
    (package / "sub" / "s2.py").write_text(
        "from ..pairs import pair_side as decl\n@decl(pair='k2', side='current')" + body, encoding="utf-8"
    )
    (package / "sub" / "deep" / "d4.py").write_text(
        "from ...pairs import pair_side as decl\n@decl(pair='k4', side='current')" + body, encoding="utf-8"
    )
    (package / "sub" / "s3.py").write_text(
        "from ..pairs import pair_side as decl\nif True:\n\n    @decl(pair='k3', side='current')\n    def a(x):\n        return x\n",
        encoding="utf-8",
    )
    scan = pair_scan.scan_pairs(package, package="z")
    named_s3 = [f for f in scan.findings if f.startswith("src/z/sub/s3.py:") and "pair_side" in f]
    assert {"k2", "k4"} <= set(scan.pairs) and "k3" not in scan.pairs and named_s3, (sorted(scan.pairs), scan.findings)
