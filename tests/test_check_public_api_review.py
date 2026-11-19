"""Reviewer tests of the public-API refusal script: a subpackage that re-exports through a RELATIVE import.

The script's `listed-absent` row for a subpackage does two things with each name the subpackage's
`__all__` promises: it reads the subpackage's `__init__.py` with `ast` for a top-level binding of the
name, and when that binding is a `from ... import` from a module the wheel carries, it reads THAT
module for a top-level binding of the name as well. Both halves of that row are stated over "a
`from` import ... absolute or relative", and an `ast.ImportFrom` reports a relative import as a
`level` above zero with `module` holding only the part after the dots, so an implementation that
resolves the source module by `node.module` alone reads a relative re-export as an import from the
empty module, or from a module the wheel does not carry, and then decides the name by the wrong rule.

This module holds the two directions of that one class of input, both of which the author's tests
reach only through ABSOLUTE imports: a relative re-export of a name its source module does bind must
conform, and a relative re-export of a name its source module does not bind must be refused. The
script is invoked as a subprocess, as the install-check job invokes it, and only its stdout and exit
status are read.
"""

from __future__ import annotations

import re
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "check_public_api.py"

_REFUSAL_LINE = re.compile(r"^refused: (?P<name>[a-z-]+): (?P<detail>.+)$")

# One relative re-export at the subpackage's own level, and one that names the package from its parent:
# the second is the form a checker that strips the dots and prefixes the subpackage once still lands
# on a module, because `beta.inner` is a real suffix of both.
RELATIVE_REEXPORTS = (".inner", "..beta.inner")


def _wheel(directory: Path, source_import: str, inner: str) -> Path:
    """A wheel whose one subpackage re-exports `gamma` from `source_import`, with `inner.py` as given."""
    path = directory / "relative-1.0-py3-none-any.whl"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("multidriver_swg/__init__.py", '__all__ = ["beta"]\n')
        archive.writestr(
            "multidriver_swg/beta/__init__.py", f'from {source_import} import gamma\n\n__all__ = ["gamma"]\n'
        )
        archive.writestr("multidriver_swg/beta/inner.py", inner)
    return path


def _run(wheel: Path) -> subprocess.CompletedProcess:
    """One invocation of the gate over `wheel`."""
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--wheel", str(wheel)], capture_output=True, text=True, check=False
    )


def _refusals(stdout: str) -> list[tuple[str, str]]:
    """The `refused: {id}: {detail}` lines of a run, in the order the script printed them."""
    found = []
    for line in stdout.splitlines():
        matched = _REFUSAL_LINE.match(line)
        if matched is not None:
            found.append((matched.group("name"), matched.group("detail")))
    return found


@pytest.mark.parametrize("source_import", RELATIVE_REEXPORTS)
def test_a_relative_reexport_of_a_bound_name_conforms(tmp_path, source_import):
    """Property: a subpackage re-exporting, by a relative import, a name its source module binds is admitted.

    Class: subpackage `__init__.py` files whose one `from` import is relative -- `.inner` at the
    subpackage's own level, and `..beta.inner` one level above it -- where the source module the
    import names is in the wheel and binds the re-exported name at top level. This is the shape both
    of this package's subpackages would take if their re-exports were written relatively, so it is a
    wheel the gate must admit, and admitting it is what tells a false refusal from a real one.

    Why required: the refusal set is closed at four, so a conforming wheel refused here is not a
    near-miss. It fails the push that produced it and it names a ground -- `listed-absent` -- that is
    false of the wheel, so the reader is sent to repair a declaration that is already correct.

    What kills it: resolving the imported module by `node.module` alone. For `.inner` that is the
    module `inner`, which the wheel does not carry, so a checker that then refuses an unresolvable
    source module refuses this wheel; for `..beta.inner` a resolver that strips the dots and prefixes
    the subpackage once reads `multidriver_swg.beta.beta.inner`, which the wheel does not carry
    either. Either route reds this test, and both leave the correct rule -- walking `level` stepwise
    up from the subpackage's own name -- green.

    A second correct implementation, differing in what the test asserts on: a gate that decides the
    source module by importing the wheel in a throwaway environment instead of reading it with `ast`
    reaches the same module object, refuses nothing, and passes this test, because the test asserts
    the absence of a refusal and not how the source module was found.
    """
    wheel = _wheel(tmp_path, source_import, "def gamma():\n    return 1\n")

    result = _run(wheel)

    assert result.returncode == 0, (result.stdout, result.stderr)
    assert _refusals(result.stdout) == []
    assert result.stdout.splitlines()[-1] == f"{wheel}: conforms"


@pytest.mark.parametrize("source_import", RELATIVE_REEXPORTS)
def test_a_relative_reexport_of_an_absent_name_is_refused(tmp_path, source_import):
    """Property: the same relative re-export is refused with `listed-absent` when its source module lacks the name.

    Class: the class above with the one member changed that decides the verdict -- the module the
    relative import names is in the wheel and binds no `gamma`. The list promises a name the wheel's
    sources cannot supply, which is exactly what `listed-absent` exists to report, and the refusal
    must survive the import being relative.

    Why required: the row's first half is satisfied here -- `__init__.py` really does bind `gamma`,
    by the `from` import itself -- so a checker that stops at the binding and never follows the import
    passes this wheel. The name then fails for the caller at the import that the list promised, which
    is the failure the whole gate exists to move to the push.

    What kills it: treating any top-level `from` binding as sufficient. Follow the import to the
    module it names, and the name is absent there, which turns this test green; stop at the binding
    and the wheel is admitted, reddening it.

    A second correct implementation, differing in what the test asserts on: a gate that refuses with
    a detail naming the source module rather than the subpackage still passes, because only the
    refusal's id and the presence of the name in its detail are asserted.
    """
    wheel = _wheel(tmp_path, source_import, "def delta():\n    return 1\n")

    result = _run(wheel)

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result.stdout)
    assert [name for name, _ in refusals] == ["listed-absent"]
    assert "gamma" in refusals[0][1], refusals[0][1]
    assert result.stdout.splitlines()[-1] == f"{wheel}: 1 refusal(s)"


def _subpackage_wheel(directory: Path, init_source: str, inner: str = "") -> Path:
    """A wheel whose one subpackage `beta` has the given `__init__.py` and an `inner.py` holding `inner`."""
    path = directory / "subpackage-1.0-py3-none-any.whl"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("multidriver_swg/__init__.py", '__all__ = ["beta"]\n')
        archive.writestr("multidriver_swg/beta/__init__.py", init_source)
        archive.writestr("multidriver_swg/beta/inner.py", inner)
    return path


def test_an_aliased_reexport_is_decided_by_the_name_the_source_module_binds(tmp_path):
    """Property: `from .inner import gamma as delta` with `__all__ = ["delta"]` conforms when `inner` binds `gamma`.

    Class: a subpackage that publishes a re-exported name under an alias (`import ... as`, listed in
    the criterion's own binding forms). The name the subpackage's list promises is `delta`, the name
    the subpackage binds; the name the source module must bind is `gamma`, the one the import asks it
    for. The criterion's third clause refuses a name "bound by `from` import from a module in the wheel
    in which ... the name is not a top-level binding", and the import's name is the one that module is
    asked for.

    Why required: a wheel whose alias re-export resolves at the caller's import is refused here with a
    `listed-absent` naming a `delta` that `inner` was never asked for, so the refusal is false and the
    reader is sent to repair a declaration that is correct.

    What kills it: looking the LISTED name up in the source module's bindings. `inner` binds `gamma`
    and not `delta`, so that lookup refuses this wheel; looking up the imported name conforms it.

    A second correct implementation, differing in what the test asserts on: a gate that imports the
    wheel in a throwaway environment resolves `delta` by running the import, and admits this wheel
    too, because only the absence of a refusal is asserted.
    """
    wheel = _subpackage_wheel(
        tmp_path,
        'from .inner import gamma as delta\n\n__all__ = ["delta"]\n',
        "def gamma():\n    return 1\n",
    )

    result = _run(wheel)

    assert result.returncode == 0, (result.stdout, result.stderr)
    assert _refusals(result.stdout) == []


def test_an_aliased_reexport_of_a_name_the_source_module_lacks_is_refused_though_it_binds_the_alias(tmp_path):
    """Property: `from .inner import gamma as delta` is refused when `inner` binds `delta` and not `gamma`.

    Class: the alias class above with the one member changed that decides the verdict: the source
    module holds a binding of the ALIAS and none of the imported name, so the import fails for the
    caller at `gamma` while a lookup of the alias finds a binding.

    Why required: the failure the gate exists to move to the push is an `ImportError` at the call
    site; this wheel raises one at `import multidriver_swg.beta`, and a gate that admits it has
    checked the wrong name.

    What kills it: looking the listed name up in the source module, which finds `delta` there and
    admits the wheel; looking up the imported name refuses it.

    A second correct implementation, differing in what the test asserts on: a gate that follows the
    import by running it refuses with a different detail and the same id, and passes, because only
    the id is asserted.
    """
    wheel = _subpackage_wheel(
        tmp_path,
        'from .inner import gamma as delta\n\n__all__ = ["delta"]\n',
        "def delta():\n    return 1\n",
    )

    result = _run(wheel)

    assert result.returncode == 1, result.stdout
    assert [name for name, _ in _refusals(result.stdout)] == ["listed-absent"]


NESTED_BINDINGS = (
    pytest.param(
        "try:\n    from .inner import gamma\nexcept ImportError:\n    gamma = None\n",
        id="try-except-import",
    ),
    pytest.param("import sys\n\nif sys.platform:\n    gamma = 1\n", id="if-block-assignment"),
    pytest.param("with open(__file__) as gamma:\n    pass\n", id="with-as"),
    pytest.param("for gamma in (1, 2):\n    pass\n", id="for-target"),
)


@pytest.mark.parametrize("init_source", NESTED_BINDINGS)
def test_a_name_bound_inside_a_top_level_compound_statement_is_a_top_level_binding(tmp_path, init_source):
    """Property: a name a subpackage's `__init__.py` binds inside a top-level `try`, `if`, `with` or `for` is bound.

    Class: subpackage `__init__.py` files that bind the listed name at module level by a statement
    nested in a compound statement the module executes at import: an optional-dependency
    `try`/`except ImportError`, a platform `if`, a `with ... as`, a `for` target. The criterion's
    row is "a property over the grammar of a module's top-level statements, not a list of binding
    forms" and the forms a future `__init__` may use are an OPEN set, so the property is that the
    name is bound in the module's namespace by code at module level, wherever in that grammar the
    binding sits.

    Why required: an `__init__` that guards an optional import with `try`/`except ImportError` is
    the commonest way a package binds a name conditionally; refusing it with `listed-absent` is a
    false refusal of a wheel whose `from pkg import *` works.

    What kills it: reading only the direct children of the module body. The binding is a child of
    the compound statement, so the name is found by no top-level statement of that kind and the
    wheel is refused; walking the compound statements' bodies, but not function or class bodies,
    admits it.

    A second correct implementation, differing in what the test asserts on: a gate that decides
    the namespace by importing the wheel in a throwaway environment admits all four, and passes,
    because the test asserts the absence of a refusal and not how the binding was found.
    """
    wheel = _subpackage_wheel(tmp_path, init_source + '\n__all__ = ["gamma"]\n', "def gamma():\n    return 1\n")

    result = _run(wheel)

    assert result.returncode == 0, (result.stdout, result.stderr)
    assert _refusals(result.stdout) == []
