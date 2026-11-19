"""Tests of the public-API refusal script: its four grounds, its output shape, and this tree's own lists.

The script is the surface's only reader: nothing imports `__all__` at run time, so a list that has
gone stale is caught here or it is caught by whoever installs the wheel. Every ground is a fact about
the BUILT WHEEL, so each test below assembles a wheel-shaped zip in `tmp_path` -- the package at the
archive root, as a wheel keeps it -- and runs the script as a subprocess, because its deliverable is
its stdout and its exit status and nothing else.

Three tests are not scheduled around a synthetic wheel. One assembles a zip from THIS checkout's
`src/` in the wheel's layout, so the real tree's lists are checked on every push rather than only
when a build runs. The second compares the root `__all__` against the package directory itself,
because the script's grounds refuse a listed name the wheel does not carry and cannot refuse a module
the list forgets -- the refusal set is closed at four and none of them is `unlisted-present` -- so
the other direction of the declaration is held here. The third reads the root module with `ast` for
the `__version__` literal a caller outside this run imports, which no ground of a surface check would
notice going missing.
"""

from __future__ import annotations

import ast
import re
import subprocess
import sys
import zipfile
from pathlib import Path

import multidriver_swg

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "check_public_api.py"
PACKAGE = REPO_ROOT / "src" / "multidriver_swg"

_REFUSAL_LINE = re.compile(r"^refused: (?P<name>[a-z-]+): (?P<detail>.+)$")

# A wheel whose declared surface the wheel carries in full: one plain module, one subpackage that
# re-exports the one name it publishes, and that name bound where the import takes it from.
CONFORMING = {
    "multidriver_swg/__init__.py": '__all__ = ["alpha", "beta"]\n',
    "multidriver_swg/alpha.py": "VALUE = 1\n",
    "multidriver_swg/beta/__init__.py": 'from multidriver_swg.beta.inner import gamma\n\n__all__ = ["gamma"]\n',
    "multidriver_swg/beta/inner.py": "def gamma():\n    return 1\n",
}


def _wheel(directory: Path, members: dict[str, str]) -> Path:
    """A wheel-shaped zip holding `members` at the archive root, which is where a wheel keeps the package."""
    path = directory / "sample-1.0-py3-none-any.whl"
    with zipfile.ZipFile(path, "w") as archive:
        for member, text in members.items():
            archive.writestr(member, text)
    return path


def _members(**replacements: str) -> dict[str, str]:
    """The conforming wheel's members with the named ones replaced."""
    return {**CONFORMING, **replacements}


def _run(*arguments: str) -> subprocess.CompletedProcess:
    """One invocation of the gate, in the form the install-check job gives it."""
    return subprocess.run([sys.executable, str(SCRIPT), *arguments], capture_output=True, text=True, check=False)


def _refusals(stdout: str) -> list[tuple[str, str]]:
    """The `refused: {id}: {detail}` lines of a run, in the order the script printed them."""
    found = []
    for line in stdout.splitlines():
        matched = _REFUSAL_LINE.match(line)
        if matched is not None:
            found.append((matched.group("name"), matched.group("detail")))
    return found


def _checkout_wheel(directory: Path) -> Path:
    """A zip in the wheel's layout assembled from this checkout's package directory, as a build would."""
    path = directory / "checkout-0.2.0.dev0-py3-none-any.whl"
    with zipfile.ZipFile(path, "w") as archive:
        for source in sorted(PACKAGE.rglob("*")):
            if not source.is_file() or "__pycache__" in source.parts:
                continue
            archive.write(source, source.relative_to(PACKAGE.parent).as_posix())
    return path


def _public_names() -> list[str]:
    """Every module and subpackage of this checkout's package whose name has no leading underscore."""
    names = set()
    for entry in PACKAGE.iterdir():
        if entry.name.startswith("_"):
            continue
        if entry.is_dir():
            if (entry / "__init__.py").is_file():
                names.add(entry.name)
        elif entry.suffix == ".py":
            names.add(entry.stem)
    return sorted(names)


def test_a_conforming_wheel_conforms(tmp_path):
    """A wheel that carries every name its root list declares prints one line and exits 0."""
    wheel = _wheel(tmp_path, CONFORMING)

    result = _run("--wheel", str(wheel))

    assert result.returncode == 0, (result.stdout, result.stderr)
    assert _refusals(result.stdout) == []
    assert result.stdout.splitlines() == [f"{wheel}: conforms"]


def test_a_root_without_all_is_refused(tmp_path):
    """A root module that declares no `__all__` is refused, and nothing else is reported."""
    wheel = _wheel(
        tmp_path, _members(**{"multidriver_swg/__init__.py": 'def hello(name):\n    return f"Hello, {name}!"\n'})
    )

    result = _run("--wheel", str(wheel))

    assert result.returncode == 1, result.stdout
    assert [name for name, _ in _refusals(result.stdout)] == ["no-root-all"]
    assert result.stdout.splitlines()[-1] == f"{wheel}: 1 refusal(s)"


def test_a_computed_all_is_refused(tmp_path):
    """An `__all__` computed at import time is not a literal list, so the gate cannot read it and refuses."""
    members = _members(**{"multidriver_swg/__init__.py": '_NAMES = ("alpha", "beta")\n__all__ = list(_NAMES)\n'})
    wheel = _wheel(tmp_path, members)

    result = _run("--wheel", str(wheel))

    assert result.returncode == 1, result.stdout
    assert [name for name, _ in _refusals(result.stdout)] == ["no-root-all"]


def test_a_tuple_all_is_admitted(tmp_path):
    """A literal tuple of strings is a literal list-or-tuple surface, so the wheel conforms."""
    wheel = _wheel(tmp_path, _members(**{"multidriver_swg/__init__.py": '__all__ = ("alpha", "beta")\n'}))

    result = _run("--wheel", str(wheel))

    assert result.returncode == 0, (result.stdout, result.stderr)
    assert _refusals(result.stdout) == []


def test_a_listed_module_the_wheel_does_not_carry_is_refused(tmp_path):
    """A root entry with neither a module file nor a package file in the wheel is refused, by name."""
    wheel = _wheel(tmp_path, _members(**{"multidriver_swg/__init__.py": '__all__ = ["alpha", "ghost"]\n'}))

    result = _run("--wheel", str(wheel))

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result.stdout)
    assert [name for name, _ in refusals] == ["listed-absent"]
    assert "ghost" in refusals[0][1]


def test_a_listed_name_the_subpackage_does_not_bind_is_refused(tmp_path):
    """A name in a subpackage's `__all__` that its `__init__.py` never binds at top level is refused."""
    members = _members(**{"multidriver_swg/beta/__init__.py": '__all__ = ["gamma"]\n'})
    wheel = _wheel(tmp_path, members)

    result = _run("--wheel", str(wheel))

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result.stdout)
    assert [name for name, _ in refusals] == ["listed-absent"]
    assert "gamma" in refusals[0][1]


def test_a_listed_name_whose_source_module_does_not_bind_it_is_refused(tmp_path):
    """A name imported from a module the wheel carries, which does not bind it either, is refused."""
    members = _members(
        **{
            "multidriver_swg/beta/__init__.py": (
                'from multidriver_swg.beta.inner import gamma\n\n__all__ = ["gamma"]\n'
            ),
            "multidriver_swg/beta/inner.py": "def delta():\n    return 1\n",
        }
    )
    wheel = _wheel(tmp_path, members)

    result = _run("--wheel", str(wheel))

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result.stdout)
    assert [name for name, _ in refusals] == ["listed-absent"]
    assert "gamma" in refusals[0][1]


def test_a_subpackage_without_all_is_refused(tmp_path):
    """A listed subpackage that binds no literal `__all__` has no surface the gate can read, so it is refused."""
    members = _members(**{"multidriver_swg/beta/__init__.py": "from multidriver_swg.beta.inner import gamma\n"})
    wheel = _wheel(tmp_path, members)

    result = _run("--wheel", str(wheel))

    assert result.returncode == 1, result.stdout
    assert [name for name, _ in _refusals(result.stdout)] == ["subpackage-without-all"]


def test_a_path_that_is_not_a_zip_is_refused(tmp_path):
    """A file that is not a zip is one unreadable wheel, refused rather than raised."""
    path = tmp_path / "not-a-wheel.whl"
    path.write_text("this is not a zip\n", encoding="utf-8")

    result = _run("--wheel", str(path))

    assert result.returncode == 1, result.stdout
    assert [name for name, _ in _refusals(result.stdout)] == ["unreadable-wheel"]


def test_a_zip_without_the_package_is_refused(tmp_path):
    """A zip holding some other distribution is refused: the gate reads this package's own root module."""
    wheel = _wheel(tmp_path, {"other_package/__init__.py": "__all__ = []\n"})

    result = _run("--wheel", str(wheel))

    assert result.returncode == 1, result.stdout
    assert [name for name, _ in _refusals(result.stdout)] == ["unreadable-wheel"]


def test_a_root_module_that_does_not_parse_is_refused(tmp_path):
    """A wheel whose root module is not Python is refused, because its surface cannot be decided."""
    wheel = _wheel(tmp_path, _members(**{"multidriver_swg/__init__.py": "def broken(:\n"}))

    result = _run("--wheel", str(wheel))

    assert result.returncode == 1, result.stdout
    assert [name for name, _ in _refusals(result.stdout)] == ["unreadable-wheel"]


def test_a_missing_wheel_argument_is_a_usage_error(tmp_path):
    """The one required option is missing, which is the caller's mistake and not a refused wheel."""
    result = _run()

    assert result.returncode == 2
    assert result.stdout == ""
    assert "--wheel" in result.stderr


def test_this_checkout_s_surface_conforms(tmp_path):
    """The real tree's two lists pass the gate, so a stale list reds the suite on the push that makes it."""
    wheel = _checkout_wheel(tmp_path)

    result = _run("--wheel", str(wheel))

    assert result.returncode == 0, (result.stdout, result.stderr)
    assert _refusals(result.stdout) == []


def test_the_root_all_names_exactly_the_packages_public_modules():
    """The root `__all__` names every module and subpackage without a leading underscore, and nothing else."""
    listed = sorted(multidriver_swg.__all__)
    published = _public_names()

    assert listed == published, (
        f"the root __all__ lists {listed} and the package carries {published}; "
        f"unlisted: {sorted(set(published) - set(listed))}, "
        f"listed but absent from the tree: {sorted(set(listed) - set(published))}"
    )


def test_the_root_binds_the_version_the_workflow_command_reads():
    """The root binds `__version__` at module level to a string literal, and keeps it out of `__all__`.

    The name is not this package's surface and it is not a list entry: `__all__` names modules and
    subpackages, and a dunder stays out of it. It is read from the tree rather than asserted through
    an import, because what the caller on the trunk's command path executes is the module-level
    binding of a source file, and a root that drops it makes `from multidriver_swg import __version__`
    raise at import.
    """
    tree = ast.parse((PACKAGE / "__init__.py").read_text(encoding="utf-8"))
    assigned = [
        node.value
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "__version__" for target in node.targets)
    ]

    assert assigned, "src/multidriver_swg/__init__.py binds no __version__, which the workflow command imports"
    for value in assigned:
        assert isinstance(value, ast.Constant) and isinstance(value.value, str), (
            f"src/multidriver_swg/__init__.py binds __version__ to {ast.dump(value)}, not a string literal"
        )
    assert "__version__" not in multidriver_swg.__all__, (
        "the version literal is a dunder and stays out of the surface list"
    )
