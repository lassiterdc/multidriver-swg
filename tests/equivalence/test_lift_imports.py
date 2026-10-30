"""Check that no lift declaration imports anything the lift runner's own refusal would turn away, at any depth of its syntax tree.

A lift's value is identified by a digest of its stored legacy text and of its declaration's syntax tree. A helper
shared between declarations is a module of this checkout, and importing one from inside a declaration passes the
runner's own refusal, which looks only at names the declaring module binds at its top level. Only the import
statement then enters the digest, not the helper's text. So a shared helper could be edited and change a lift's
value while the lift's digest, the stored value's key and the reach record's key all stayed put, and nothing would
report that a stored reference value had gone stale.

`import_findings` takes every function the runner discovers as a declaration (the functions carrying the mark
`declares_lift` leaves, whatever name reached the decorator) together with every function decorated with a decorator
written `declares_lift`, and walks each one's whole tree. It reports every `import` or `from ... import` that
`lift._admitted_import` refuses for the checkout root, with one departure: an import of `tests` or `multidriver_swg`, a relative import,
an import that resolves to no module and whose top-level name is an entry at the checkout root, and an import of any top-level module whose spec lies inside the checkout,
which includes the namespace packages `scripts` and `_refactor_tools`; the legacy directory `_old_code_to_refactor` is refused by its name, because the session's import hook raises on any attempt to resolve it. A name that resolves to no module and names no entry at the checkout root is admitted, so the verdict does not depend
on which libraries are installed (the push job installs only numpy, pandas and xarray). A call of
`__import__` or `import_module` whose first argument, by position or as `name=`, is a string literal is judged the same way, as an import of that
literal's top-level name. An import of a library outside the checkout, `import numpy as np` inside a declaration for
instance, is allowed; so is the module-level `from tests.correspondence.lift import declares_lift` a lift module
needs to declare anything, because it lies outside every declaration. The check is a function of a root so the tests
below can plant a violating tree.

Not established: that a call of `__import__` or `import_module` whose argument is computed, or a file of the checkout
loaded and executed by path, is caught; no check reads either. The one checkout file a declaration may execute is a
callee's stored extraction, held by its pinned label.
"""

import ast
import importlib
import importlib.util
import os
import subprocess
from pathlib import Path

import pytest

from tests.correspondence import lift

pytestmark = pytest.mark.state_check

REPO_ROOT = lift.REPO_ROOT
DECORATOR = "declares_lift"
DYNAMIC = ("__import__", "import_module")
LEGACY = "_old_code_to_refactor"  # refused by name: the session's import hook raises on any attempt to resolve it


def _is_declaration(node: ast.AST) -> bool:
    """Whether `node` is a function decorated with a decorator written `declares_lift`, called or not, bare or through a module."""
    if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
        return False
    for decorator in node.decorator_list:
        target = decorator.func if isinstance(decorator, ast.Call) else decorator
        if (isinstance(target, ast.Name) and target.id == DECORATOR) or (
            isinstance(target, ast.Attribute) and target.attr == DECORATOR
        ):
            return True
    return False


def root_entries(root: Path) -> set[str]:
    """The names of the entries at the checkout root, as `git ls-tree --name-only HEAD` lists them, minus a `.py` suffix.

    Git is asked first, with the location variables a hook exports removed so the lookup reads `root`; when `root` is
    no git repository, or has no commit, the directory listing stands in (names beginning with `.` and `__pycache__`
    left out). The set is a function of the checkout's contents and of nothing installed.
    """
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    run = subprocess.run(
        ["git", "-C", str(root), "ls-tree", "--name-only", "HEAD"], capture_output=True, text=True, env=env
    )
    names = (
        run.stdout.splitlines()
        if run.returncode == 0
        else [e.name for e in root.iterdir() if not e.name.startswith(".") and e.name != "__pycache__"]
    )
    return {name.removesuffix(".py") for name in names}


def _refused(node: ast.AST, root: Path) -> bool:
    """Whether an import is refused: the runner's verdict, except that an unresolved name naming no root entry is admitted.

    The runner's `_admitted_import` refuses a name that resolves to no module, which makes its verdict depend on
    which libraries are installed. This check keeps its verdict for every name that resolves, refuses an unresolved
    name only when it names an entry at the checkout root, and admits the rest, since a name that is neither
    resolvable nor an entry of the checkout cannot be checkout source. The legacy directory's name is refused
    without resolving it, because the session's import hook raises on any attempt to.
    """
    if isinstance(node, ast.ImportFrom) and node.level:
        return True
    names = [alias.name for alias in node.names] if isinstance(node, ast.Import) else [node.module or ""]
    for name in names:
        top = name.split(".")[0]
        if top == LEGACY or top in lift.INTERNAL_PACKAGES or not top:
            return True
        if lift._admitted_import(ast.Import(names=[ast.alias(name=name)], lineno=1, col_offset=0), root):
            continue
        try:
            resolves = importlib.util.find_spec(top) is not None
        except (ImportError, ValueError):
            resolves = False
        if resolves or top in root_entries(root):
            return True
    return False


def _call_name(call: ast.Call) -> str | None:
    """The bare name of a call's callee, for `f(...)` and `module.f(...)` alike."""
    target = call.func
    return target.id if isinstance(target, ast.Name) else target.attr if isinstance(target, ast.Attribute) else None


def _first_argument(call: ast.Call) -> ast.expr | None:
    """The call's first argument, given by position or as the keyword `name`, or None when it has neither."""
    if call.args:
        return call.args[0]
    return next((k.value for k in call.keywords if k.arg == "name"), None)


def _imports_of(declaration: ast.AST, root: Path) -> list[tuple[int, str]]:
    """Each (line, description) of an import the runner's refusal turns away, found anywhere under `declaration`."""
    found = []
    for node in ast.walk(declaration):
        if isinstance(node, ast.Import):
            for alias in node.names:
                single = ast.Import(names=[alias], lineno=node.lineno, col_offset=0)
                if _refused(single, root):
                    found.append((node.lineno, f"import {alias.name}"))
        elif isinstance(node, ast.ImportFrom):
            if _refused(node, root):
                what = (
                    f"a relative import (level {node.level}, module {node.module!r})"
                    if node.level
                    else f"from {node.module} import ..."
                )
                found.append((node.lineno, what))
        elif isinstance(node, ast.Call) and _call_name(node) in DYNAMIC and _first_argument(node) is not None:
            first = _first_argument(node)
            if isinstance(first, ast.Constant) and isinstance(first.value, str):
                single = ast.Import(names=[ast.alias(name=first.value)], lineno=node.lineno, col_offset=0)
                if _refused(single, root):
                    found.append((node.lineno, f"a dynamic import of {first.value}"))
    return found


def import_findings(root: Path = REPO_ROOT) -> list[str]:
    """One finding per refused import inside a lift declaration, naming file, line and declaration.

    A module the runner cannot load is itself a finding, and its syntactically marked declarations are still read.
    """
    findings = []
    try:
        discovered = {(d.path.resolve(), d.function.__name__) for d in lift.discover(root)}
    except Exception as error:  # a module the runner cannot load must not hide the others
        discovered = set()
        findings.append(f"{lift.LIFTS.as_posix()}: the lift runner could not discover the declarations ({error!r})")
    for path in sorted((root / lift.LIFTS).rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        rel = path.relative_to(root).as_posix()
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and (
                _is_declaration(node) or (path.resolve(), node.name) in discovered
            ):
                findings += [
                    f"{rel}:{line}: declaration {node.name} contains {what}"
                    for line, what in sorted(_imports_of(node, root))
                ]
    return findings


# ---- tests ----

HEAD = "from tests.correspondence.lift import declares_lift\n\n\n"
DECLARATION = (
    '@declares_lift(unit="mod.py:1-3")\n'
    "def declare(fixtures):\n"
    '    """Supply the scale."""\n'
    "{body}"
    '    return {{"scale": 3}}, lambda ns: ns["y"]\n'
)


def _root(tmp_path: Path, body: str = "", *, head: str = HEAD, tail: str = "") -> Path:
    """A root whose one lift module holds a declaration with `body` inside it, `head` above it and `tail` below."""
    (tmp_path / lift.LIFTS).mkdir(parents=True)
    source = head + DECLARATION.format(body="".join(f"    {line}\n" for line in body.splitlines())) + tail
    (tmp_path / lift.LIFTS / "sys_a.py").write_text(source, encoding="utf-8")
    return tmp_path


def test_a_declaration_importing_only_outside_libraries_has_no_finding(tmp_path):
    """Property: a declaration that imports numpy and a standard-library module, in a module that imports the lift
    decorator from the checkout at its top level, has no finding.

    Class: the satisfying arm, with an import of `tests.correspondence.lift` that lies outside the declaration and
    imports inside it of installed libraries outside the checkout. Why it is required: the other tests would pass
    for a check that refuses every import. Killed by: flagging the module-level import. Second correct
    implementation: collecting the imports of the whole module and subtracting those outside any declaration passes
    the same input.
    """
    root = _root(tmp_path, "import numpy as np\nimport pandas\nfrom os import path")
    assert import_findings(root) == []


@pytest.mark.parametrize(
    ("body", "fragment"),
    [
        ("import tests.correspondence.fixtures", "import tests.correspondence.fixtures"),
        ("from tests.correspondence import fixtures", "from tests.correspondence import"),
        ("import multidriver_swg as m", "import multidriver_swg"),
        ("from multidriver_swg.core import helper", "from multidriver_swg.core import"),
        ("from . import helper", "a relative import"),
        ("from ..shared import helper", "a relative import"),
        ("def inner():\n    import tests.helper", "import tests.helper"),
        (
            "class Holder:\n    def method(self):\n        from multidriver_swg import helper",
            "from multidriver_swg import",
        ),
        ('__import__("tests.helper")', "a dynamic import of tests.helper"),
        (
            'import importlib\nimportlib.import_module("multidriver_swg.helper")',
            "a dynamic import of multidriver_swg.helper",
        ),
    ],
)
def test_an_import_of_this_checkout_inside_a_declaration_is_a_finding(tmp_path, body, fragment):
    """Property: each way a declaration can import this checkout is one finding naming the file, the declaration and the import.

    Class: an `import`, a `from ... import`, a relative import, one nested in an inner function and one in a method
    of a class inside the declaration, and a string-literal `__import__` or `import_module`. Why it is required:
    each of these passes the runner's module-level refusal and none moves the lift's digest. Killed by: reading only
    the declaration's top-level statements, only `ast.Import`, or only the statement forms. Second correct
    implementation: compiling the declaration and reading its imported names from the code objects passes the
    eight inputs that are import statements.
    """
    (finding,) = import_findings(_root(tmp_path, body))
    assert finding.startswith("tests/lifts/sys_a.py:") and "declaration declare" in finding and fragment in finding


NAMESPACE_PACKAGES = ("scripts", "_refactor_tools")


@pytest.mark.parametrize("package", NAMESPACE_PACKAGES)
@pytest.mark.parametrize("form", ["statement", "dynamic"])
def test_an_import_of_a_namespace_package_inside_the_checkout_is_a_finding(tmp_path, monkeypatch, package, form):
    """Property: a declaration importing a top-level directory of the checkout that has no `__init__.py` is refused, by statement or by literal call.

    Class: the two namespace packages of the toolkit's root that resolve normally, planted as directories under the root and put on
    `sys.path` as pytest's rootdir puts the repository, each imported as a statement and as a string-literal
    `import_module`. Why it is required: `importlib.util.find_spec` resolves each inside the checkout, and a check
    that names only `tests` and `multidriver_swg` reports nothing for them, although their text is outside every
    lift digest. Killed by: a fixed pair of package names. Second correct implementation: resolving each imported
    top-level name with `find_spec` and testing whether its locations lie under the root passes.
    """
    (tmp_path / package).mkdir()
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    body = (
        f"import {package}.anything"
        if form == "statement"
        else f'import importlib\nimportlib.import_module("{package}.anything")'
    )
    (finding,) = import_findings(_root(tmp_path, body))
    assert package in finding and "declaration declare" in finding


@pytest.mark.parametrize("form", ["statement", "dynamic"])
def test_an_import_of_the_legacy_directory_is_a_finding_without_resolving_it(tmp_path, form):
    """Property: a declaration importing `_old_code_to_refactor`, by statement or literal call, is one finding.

    Class: the legacy directory's name, which the session's import hook refuses to resolve and records as a
    session failure. Why it is required: this class of import is refused by the check, and resolving the name to test it would
    fail the whole session whatever the check found. Killed by: asking `find_spec` about it. Second correct
    implementation: reading the name from the corpus record instead of a constant passes.
    """
    body = (
        "import _old_code_to_refactor.x"
        if form == "statement"
        else 'import importlib\nimportlib.import_module("_old_code_to_refactor.x")'
    )
    (finding,) = import_findings(_root(tmp_path, body))
    assert "_old_code_to_refactor" in finding


def test_an_import_of_a_library_outside_the_checkout_stays_allowed_beside_a_refused_one(tmp_path):
    """Property: with an allowed and a refused import in one declaration only the refused one is a finding.

    Class: `import numpy` beside `import tests.helper` in one `import a, b` free pair of statements. Why it is
    required: the refusal is per import, and a check that refused a declaration whole would hide which import broke
    the rule. Killed by: reporting every import of a declaration that holds one refused import. Second correct
    implementation: reporting by the refused module names passes.
    """
    (finding,) = import_findings(_root(tmp_path, "import numpy\nimport os, tests.helper"))
    assert "import tests.helper" in finding and "numpy" not in finding and "import os" not in finding


GHOST = "zz_mdswg_no_such_library"  # a name no environment installs, so the verdicts below cannot depend on one


@pytest.mark.parametrize("form", ["statement", "from", "dynamic", "keyword"])
def test_an_unresolved_name_that_is_no_entry_of_the_checkout_is_admitted_in_every_environment(tmp_path, form):
    """Property: a library the environment does not have installed, and the checkout does not hold, is not a finding.

    Class: a name no environment installs, imported by statement, by `from`, by literal call and by keyword literal
    call, in a root whose entries do not include it. Why it is required: the push job installs only numpy, pandas
    and xarray, so a lift importing `scipy` or `pyvinecopulib` must read the same there as everywhere; a name that
    is neither resolvable nor an entry of the checkout cannot be checkout source. Killed by: `lift._admitted_import`
    used as is, which refuses every unresolved name. Second correct implementation: testing the name against the
    checkout's entries only, with no resolution step, passes this input.
    """
    body = {
        "statement": f"import {GHOST}",
        "from": f"from {GHOST}.sub import thing",
        "dynamic": f'import importlib\nimportlib.import_module("{GHOST}.sub")',
        "keyword": f'import importlib\nimportlib.import_module(name="{GHOST}")',
    }[form]
    assert import_findings(_root(tmp_path, body)) == []


@pytest.mark.parametrize("form", ["statement", "dynamic", "keyword"])
@pytest.mark.parametrize("entry", [GHOST + "_dir", GHOST + "_file.py"])
def test_an_unresolved_name_that_is_an_entry_at_the_checkout_root_is_refused(tmp_path, form, entry):
    """Property: an unresolved name equal to a directory or a `.py` file at the root is one finding.

    Class: a directory and a module file planted at the root under a name nothing resolves, imported three ways.
    Why it is required: the refusal of an unresolvable name exists to catch checkout source that is not on the
    path; the names `scripts`, `_refactor_tools` and `tests` are such entries when the repository root is not on
    `sys.path`. Killed by: admitting every unresolved name. Second correct implementation: resolving the name
    against `git ls-tree` output rather than the directory listing passes the planted-tree input.
    """
    name = entry.removesuffix(".py")
    if entry.endswith(".py"):
        (tmp_path / entry).write_text("", encoding="utf-8")
    else:
        (tmp_path / entry).mkdir()
    body = {
        "statement": f"import {name}",
        "dynamic": f'import importlib\nimportlib.import_module("{name}")',
        "keyword": f'import importlib\nimportlib.import_module(name="{name}")',
    }[form]
    (finding,) = import_findings(_root(tmp_path, body))
    assert name in finding


def test_the_root_entries_are_the_committed_ones_when_the_root_is_a_git_repository(tmp_path):
    """Property: with a commit present the entries are those `git ls-tree` lists; an untracked file is not an entry.

    Class: a repository with one tracked module and one untracked module, and the tracked name imported. Why it is required: the entries are the ones git lists, and a
    directory listing would count build leftovers and untracked files. Killed by: always listing the directory.
    Second correct implementation: `git ls-files` over the root, which also lists tracked files only, passes.
    """
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    git = ["git", "-C", str(tmp_path), "-c", "user.name=probe", "-c", "user.email=probe@example.invalid"]
    subprocess.run([*git, "init", "-q"], check=True, env=env)
    (tmp_path / "zz_mdswg_tracked.py").write_text("", encoding="utf-8")
    subprocess.run([*git, "add", "zz_mdswg_tracked.py"], check=True, env=env)
    subprocess.run([*git, "commit", "-q", "-m", "x"], check=True, env=env)
    (tmp_path / "zz_mdswg_untracked.py").write_text("", encoding="utf-8")
    assert root_entries(tmp_path) == {"zz_mdswg_tracked"}
    (finding,) = import_findings(_root(tmp_path, "import zz_mdswg_tracked"))
    assert "zz_mdswg_tracked" in finding


def test_an_import_in_a_function_that_is_not_a_declaration_is_not_this_checks_finding(tmp_path):
    """Property: an import of the checkout in a module-level function that is no declaration is not reported here.

    Class: a helper function below the declaration importing `tests.helper`. Why it is required: the check is about
    what the lift's digest does not cover, which is a declaration's tree; a helper outside every declaration is
    refused by the runner when a declaration reads it, and reporting it here too would give one defect two owners.
    Killed by: scanning every import in a lift module. Second correct implementation: none other.
    """
    root = _root(tmp_path, tail="\n\ndef helper():\n    import tests.helper\n")
    assert import_findings(root) == []


def test_a_decorator_reached_through_a_module_still_marks_a_declaration(tmp_path):
    """Property: `@lift.declares_lift(...)` marks a declaration as `@declares_lift(...)` does.

    Class: the attribute form of the decorator, with a violating import in its body. Why it is required: the runner
    finds a declaration by the mark the decorator leaves, whatever name reached it, so a check that knew one
    spelling would be bypassed by the other. Killed by: matching the decorator by `ast.Name` only. Second correct
    implementation: matching on the decorator's final name, as here, passes; so does matching on the resolved import.
    """
    (tmp_path / lift.LIFTS).mkdir(parents=True)
    (tmp_path / lift.LIFTS / "sys_a.py").write_text(
        "from tests.correspondence import lift\n\n\n"
        '@lift.declares_lift(unit="mod.py:1-3")\n'
        "def declare(fixtures):\n"
        "    import tests.helper\n"
        '    return {}, lambda ns: ns["y"]\n',
        encoding="utf-8",
    )
    (finding,) = import_findings(tmp_path)
    assert "import tests.helper" in finding


def test_no_lift_declaration_of_this_checkout_imports_the_checkout():
    """Property: on this checkout `import_findings` is empty.

    Class: the committed `tests/lifts/`. Why it is required: it is the check the push job runs and the one that goes
    red when a declaration imports a shared helper. Killed by: a check that cannot read the committed lift modules.
    Second correct implementation: none other. Not established: that any declaration exists yet.
    """
    assert import_findings(REPO_ROOT) == []
