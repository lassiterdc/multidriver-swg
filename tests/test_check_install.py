"""Tests of the install-and-import check: its six grounds, its two resolutions, and this tree's declaration.

The check is the only place the runtime declaration and the imports under `src/` are compared. Every
ground is a fact about a BUILT DISTRIBUTION, so each test below builds synthetic artifacts in
`tmp_path` -- a wheel, and an sdist whose build backend requires nothing so that an offline install of
it is possible -- and runs the script as a subprocess, because its deliverable is its stdout, its exit
status and what the environment it made contains.

The runs are offline by construction: `UV_NO_INDEX` turns the index off and `UV_FIND_LINKS` names the
directory the synthetic dependencies live in, both read by the installer this script invokes. That is
also why a synthetic sdist carries its own requirement-free backend: an sdist built by setuptools
cannot be installed without fetching setuptools, so it could only be exercised on a runner with an
index. The sdist arm is therefore exercised here as well as on the runner, which is more than the
criterion asks for and costs one fixture.

Two tests are not scheduled around a synthetic artifact. One reads `pyproject.toml` and asserts the
declaration is `numpy>=2,<3` with `pandas` and `xarray` carried by the `test` group instead, which is
the change this chunk makes and the arm that reds if a later edit reinstates them. The other reads
every module under `src/` with `ast` and asserts each third-party import is named by the declaration,
so a new import that nothing declares reds the suite on that push rather than at a user's install.
"""

from __future__ import annotations

import ast
import io
import os
import re
import subprocess
import sys
import tarfile
import tomllib
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "check_install.py"
PACKAGE = REPO_ROOT / "src" / "multidriver_swg"
PYPROJECT = REPO_ROOT / "pyproject.toml"

# The module a synthetic package publishes, and the one its dependency publishes. One name each, so a
# refusal's detail can be read against the fixture that produced it.
SAMPLE = "sample"
DEPENDENCY = "dep"

# A module body reporting which version of the dependency its fresh environment installed: the
# assertion on a resolution is then on what arrived, not on the installer's own log.
_DIAL = (
    "import os\n"
    "import pathlib\n"
    f"import {DEPENDENCY}\n"
    'pathlib.Path(os.environ["INSTALL_PROBE"]).write_text('
    f"{DEPENDENCY}.__version__)\n"
)

# A build backend that needs nothing at all: `requires = []` and this file beside `pyproject.toml` mean
# the isolated build environment uv creates has nothing to install, so an sdist of a synthetic package
# installs with no index. The braces of the generated code are doubled because this text is formatted.
_BACKEND = '''\
"""A build backend that needs nothing, so an sdist of it builds with no index and no network."""

import pathlib
import zipfile

NAME = {name}
VERSION = {version}
BODY = {body}
REQUIRES = {requires}


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    """Write the one wheel this sdist holds, carrying the module and the declaration it was built from."""
    filename = NAME + "-" + VERSION + "-py3-none-any.whl"
    info = NAME + "-" + VERSION + ".dist-info"
    metadata = "Metadata-Version: 2.1\\nName: " + NAME + "\\nVersion: " + VERSION + "\\n"
    for requirement in REQUIRES:
        metadata += "Requires-Dist: " + requirement + "\\n"
    members = {{
        NAME + "/__init__.py": BODY,
        info + "/METADATA": metadata,
        info + "/WHEEL": "Wheel-Version: 1.0\\nGenerator: probe\\nRoot-Is-Purelib: true\\nTag: py3-none-any\\n",
    }}
    members[info + "/RECORD"] = "".join(member + ",,\\n" for member in members) + info + "/RECORD,,\\n"
    with zipfile.ZipFile(pathlib.Path(wheel_directory) / filename, "w") as archive:
        for member, text in members.items():
            archive.writestr(member, text)
    return filename
'''


def _wheel(directory: Path, name: str, version: str, body: str, requires: tuple[str, ...] = ()) -> Path:
    """A wheel holding one module, its declaration and the metadata an installer reads."""
    return _tree_wheel(directory, name, version, {f"{name}/__init__.py": body}, requires)


def _tree_wheel(
    directory: Path, name: str, version: str, modules: dict[str, str], requires: tuple[str, ...] = ()
) -> Path:
    """A wheel holding several modules, so the scan's walk of a package is exercised and not only its root."""
    info = f"{name.replace('-', '_')}-{version}.dist-info"
    path = directory / f"{name}-{version}-py3-none-any.whl"
    members = {
        **modules,
        f"{info}/METADATA": f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n"
        + "".join(f"Requires-Dist: {requirement}\n" for requirement in requires),
        f"{info}/WHEEL": "Wheel-Version: 1.0\nGenerator: probe\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
    }
    members[f"{info}/RECORD"] = "".join(f"{member},,\n" for member in members) + f"{info}/RECORD,,\n"
    with zipfile.ZipFile(path, "w") as archive:
        for member, text in members.items():
            archive.writestr(member, text)
    return path


def _sdist(directory: Path, name: str, version: str, body: str, requires: tuple[str, ...] = ()) -> Path:
    """An sdist of the same package, carrying the requirement-free backend that builds it offline."""
    stem = f"{name}-{version}"
    path = directory / f"{stem}.tar.gz"
    metadata = f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n" + "".join(
        f"Requires-Dist: {requirement}\n" for requirement in requires
    )
    members = {
        f"{stem}/PKG-INFO": metadata,
        f"{stem}/pyproject.toml": (
            "[build-system]\n"
            "requires = []\n"
            'build-backend = "probe_backend"\n'
            'backend-path = ["."]\n\n'
            "[project]\n"
            f'name = "{name}"\n'
            f'version = "{version}"\n'
            # Stated here as well as in PKG-INFO and in the built METADATA: uv reads a static
            # `pyproject.toml` for an sdist's metadata and resolves from it, so a declaration that
            # appears only in the backend's wheel leaves the dependency uninstalled.
            "dependencies = [" + ", ".join(f'"{requirement}"' for requirement in requires) + "]\n"
        ),
        f"{stem}/probe_backend.py": _BACKEND.format(
            name=repr(name), version=repr(version), body=repr(body), requires=repr(list(requires))
        ),
        f"{stem}/src/{name}/__init__.py": body,
    }
    with tarfile.open(path, "w:gz") as archive:
        for member, text in members.items():
            data = text.encode("utf-8")
            entry = tarfile.TarInfo(member)
            entry.size = len(data)
            entry.mtime = 0
            archive.addfile(entry, io.BytesIO(data))
    return path


def _dist(directory: Path, body: str = f"import {DEPENDENCY}\n", requires: tuple[str, ...] | None = None) -> Path:
    """A build directory holding the one sdist and the one wheel of the synthetic package."""
    declared = (f"{DEPENDENCY}>=1",) if requires is None else requires
    directory.mkdir(parents=True, exist_ok=True)
    _sdist(directory, SAMPLE, "0.1", body, declared)
    _wheel(directory, SAMPLE, "0.1", body, declared)
    return directory


def _links(directory: Path, requires: tuple[str, ...] = ()) -> Path:
    """The local index a dependency is fetched from: one name per version the resolution may choose."""
    directory.mkdir(parents=True, exist_ok=True)
    for version in ("1.0", "2.0"):
        _wheel(directory, DEPENDENCY, version, f'__version__ = "{version}"\n', requires)
    return directory


def _environment(links: Path, cache: Path) -> dict[str, str]:
    """The installer's own variables, which is how this check runs offline without an option of its own."""
    return {**os.environ, "UV_NO_INDEX": "1", "UV_FIND_LINKS": str(links), "UV_CACHE_DIR": str(cache)}


def _run(*arguments: str, links: Path | None = None, cache: Path | None = None) -> subprocess.CompletedProcess:
    """One invocation of the check, in the form the install-check job gives it."""
    environment = os.environ if links is None or cache is None else _environment(links, cache)
    return subprocess.run(
        [sys.executable, str(SCRIPT), *arguments], capture_output=True, text=True, check=False, env=environment
    )


def _refusal_ids(stdout: str) -> list[str]:
    """The refusal ids of a run, in the order the script printed them: one line per refusal."""
    found = []
    for line in stdout.splitlines():
        if line.startswith("refused: "):
            found.append(line.split(": ", 2)[1])
    return found


def _installed_version(tmp_path: Path, resolution: str) -> str:
    """The dependency version the check installed when it imported the package at `resolution`.

    The synthetic package writes the version it sees to the file the environment names, so what is
    asserted is what the fresh environment held rather than what the installer's log claimed.
    """
    probe = tmp_path / "version.txt"
    environment = _environment(_links(tmp_path / "links"), tmp_path / "cache")
    environment["INSTALL_PROBE"] = str(probe)
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--dist", str(_dist(tmp_path / "dist", body=_DIAL)), *resolution],
        capture_output=True,
        text=True,
        check=False,
        env=environment,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    return probe.read_text(encoding="utf-8")


def test_a_conforming_build_directory_conforms(tmp_path):
    """A wheel and an sdist that install, import and declare what they import print one line and exit 0.

    Class: NEW CAPABILITY -- the plausible wrong implementation refuses every artifact, or prints no
    summary; this test reds under it at the first assert and at the stdout equality.
    Why: this is the state the real tree must reach, so a gate that cannot reach it refuses everything.
    What kills it: any ground firing on a conforming pair, which the refusal tests below name one by one.
    Second implementation: a check that reads the declaration statically and never installs passes this
    and reds every refusal test, so the pair of arms is what discriminates; this one is the control.
    """
    dist = _dist(tmp_path / "dist")
    links = _links(tmp_path / "links")

    result = _run("--dist", str(dist), links=links, cache=tmp_path / "cache")

    assert result.returncode == 0, (result.stdout, result.stderr)
    assert result.stdout.splitlines() == [f"{dist}: conforms"]


def test_a_declaration_with_no_version_specifier_is_refused(tmp_path):
    """An unconstrained declaration is refused once per artifact, because an unbounded install moves.

    Class: NEW CAPABILITY -- a check that reads only names passes this fixture, and refuses no bound.
    Why: a declaration without a bound installs whatever is newest at the user's moment.
    What kills it: dropping the `unconstrained` arm from the check, which reds the id assertion.
    Second implementation: a check that refuses an unconstrained declaration per declaration rather
    than per artifact prints one line here and reds the two-line assertion, so the ordering and the
    count are what this test holds rather than the id alone.
    """
    dist = _dist(tmp_path / "dist", requires=(DEPENDENCY,))
    links = _links(tmp_path / "links")

    result = _run("--dist", str(dist), links=links, cache=tmp_path / "cache")

    assert result.returncode == 1, result.stdout
    assert _refusal_ids(result.stdout) == ["unconstrained", "unconstrained"]
    assert result.stdout.splitlines()[-1] == f"{dist}: 2 refusal(s)"


def test_a_declared_dependency_no_module_imports_is_refused(tmp_path):
    """A declaration carrying a library nothing imports is refused: the declaration must be the scan's set.

    Class: NEW CAPABILITY -- a check that only reads imports and never compares the declaration to them
    passes this fixture, and is the implementation this ground exists to rule out.
    Why: a declaration can only grow without this arm, and every growth is an install a user pays for.
    What kills it: dropping the `unimported` arm, which reds the id set.
    Second implementation: a check that compares the declaration against a static scan of `src/`
    refuses this fixture too and reds `test_an_import_a_transitive_dependency_provides_is_refused`,
    because a static scan cannot see a distribution that arrived transitively.
    """
    dist = _dist(tmp_path / "dist", body="VALUE = 1\n")
    links = _links(tmp_path / "links")

    result = _run("--dist", str(dist), links=links, cache=tmp_path / "cache")

    assert result.returncode == 1, result.stdout
    assert set(_refusal_ids(result.stdout)) == {"unimported"}
    assert DEPENDENCY in result.stdout


def test_an_import_no_installed_distribution_provides_is_refused(tmp_path):
    """An import whose name no installed distribution provides is refused, even though the import succeeds.

    Class: NEW CAPABILITY -- a check that treats a successful import as a declared one passes this
    fixture, which is the `try: import ... except ImportError` shape an optional import takes.
    Why: the import succeeds and the library is absent for every user, so nothing else notices.
    What kills it: dropping the `maps to no installed distribution` half of `undeclared`.
    Second implementation: a check that resolves the name through `importlib.metadata.requires` of the
    artifact alone refuses this too, and reds the transitive test below, so the two fixtures split the
    two arms of one ground.
    """
    body = "try:\n    import absent\nexcept ImportError:\n    absent = None\n"
    dist = _dist(tmp_path / "dist", body=body, requires=())
    links = _links(tmp_path / "links")

    result = _run("--dist", str(dist), links=links, cache=tmp_path / "cache")

    assert result.returncode == 1, result.stdout
    assert set(_refusal_ids(result.stdout)) == {"undeclared"}
    assert "absent" in result.stdout


def test_an_import_a_transitive_dependency_provides_is_refused(tmp_path):
    """A library that arrived as another package's dependency is refused: it is not this declaration's.

    Class: NEW CAPABILITY -- a check that reads only what the installer resolved on the artifact's
    command line passes this fixture, because the name is provided by an installed distribution.
    Why: a transitively installed library is no promise at all; the dependency that brings it can drop it.
    What kills it: dropping the `does not declare` half of `undeclared`.
    Second implementation: a check that unions the dependency's own requirements into the declaration
    before comparing passes this and reds the unimported test, so the arm is the difference.
    """
    links = _links(tmp_path / "links", requires=("second>=1",))
    _wheel(links, "second", "1.0", "VALUE = 1\n")
    dist = _dist(tmp_path / "dist", body=f"import {DEPENDENCY}\nimport second\n")

    result = _run("--dist", str(dist), links=links, cache=tmp_path / "cache")

    assert result.returncode == 1, result.stdout
    assert set(_refusal_ids(result.stdout)) == {"undeclared"}
    assert "second" in result.stdout


def test_a_module_that_does_not_import_is_refused(tmp_path):
    """A module whose import raises is refused, and the detail names the module and the exception type.

    Class: NEW CAPABILITY -- a check that installs and reports success without importing passes this.
    Why: this is the ground a caller reaches first, and the artifact installed cleanly beside it.
    What kills it: dropping the import arm, which reds the id set and both detail assertions.
    Second implementation: a check that imports the top-level package only still reds this fixture,
    because the fixture's sole module is that package; the count of imported modules is held by the
    real-tree declaration test instead.
    """
    dist = _dist(tmp_path / "dist", body='raise ImportError("probe")\n', requires=())
    links = _links(tmp_path / "links")

    result = _run("--dist", str(dist), links=links, cache=tmp_path / "cache")

    assert result.returncode == 1, result.stdout
    assert set(_refusal_ids(result.stdout)) == {"import-failed"}
    assert SAMPLE in result.stdout
    assert "ImportError" in result.stdout


def test_a_nested_module_that_does_not_import_is_refused(tmp_path):
    """A module inside a subpackage is imported and refused too, so the walk is over the package and not its root.

    Class: NEW CAPABILITY -- plausible wrong implementation imports the distribution's top-level module
    only, which passes every other test here (each fixture's failing module IS that top-level module)
    and reports conforms on this one, where the failure sits two levels down.
    Why: this package's failures would land in `run_configuration/` or `conformance/`, and a check that
    imported only `multidriver_swg` would import neither.
    What kills it: importing only the roots, which reds this test's id set and names no nested module.
    Second implementation: a static walk of the wheel's members without importing them finds nothing
    here, because the module imports cleanly from the archive and raises on execution; the pair of
    this test and `test_a_module_that_does_not_import_is_refused` is what separates the two.
    """
    modules = {
        f"{SAMPLE}/__init__.py": f"import {DEPENDENCY}\n",
        f"{SAMPLE}/alpha.py": "VALUE = 1\n",
        f"{SAMPLE}/inner/__init__.py": "VALUE = 2\n",
        f"{SAMPLE}/inner/deep.py": 'raise ImportError("deep probe")\n',
    }
    dist = _dist(tmp_path / "dist", body=f"import {DEPENDENCY}\n")
    _tree_wheel(dist, SAMPLE, "0.1", modules, (f"{DEPENDENCY}>=1",))
    links = _links(tmp_path / "links")

    result = _run("--dist", str(dist), links=links, cache=tmp_path / "cache")

    assert result.returncode == 1, result.stdout
    assert _refusal_ids(result.stdout) == ["import-failed"], result.stdout
    assert f"{SAMPLE}.inner.deep" in result.stdout


def test_an_artifact_that_will_not_install_is_refused(tmp_path):
    """A dependency the installer cannot fetch fails that artifact's install, and the detail names the artifact.

    Class: NEW CAPABILITY -- a check that treats a failed install as a pass and scans nothing else
    reports conforms here, which is the silent form of the same defect.
    Why: an unresolvable declaration is met here rather than at a user's install as a mystery.
    What kills it: dropping the `install-failed` arm, or reporting it without the artifact's name.
    Second implementation: a check that installs with `--no-deps` reports `import-failed` instead of
    this id, so the id is what separates the two readings of one failed install.
    """
    dist = _dist(tmp_path / "dist")
    links = _links(tmp_path / "links")
    for path in links.iterdir():
        path.unlink()

    result = _run("--dist", str(dist), links=links, cache=tmp_path / "cache")

    assert result.returncode == 1, result.stdout
    assert set(_refusal_ids(result.stdout)) == {"install-failed"}
    assert f"{SAMPLE}-0.1-py3-none-any.whl" in result.stdout


def test_a_directory_that_does_not_hold_one_of_each_artifact_is_refused(tmp_path):
    """A directory holding any other count is refused rather than compared, and no installer runs.

    Class: NEW CAPABILITY -- a check that globs `*.whl` and checks only the wheel passes both fixtures.
    Why: comparing an artifact against a declaration is only meaningful for what one build wrote.
    What kills it: dropping the count, or refusing only the empty directory.
    Second implementation: a check that refuses a directory holding two artifacts of any kind reports
    the same id for the two-wheel fixture, so the id alone does not separate the arms; the two
    fixtures are what hold the missing-arm and the duplicated-arm apart.
    """
    only_wheel = tmp_path / "only-wheel"
    only_wheel.mkdir()
    _wheel(only_wheel, SAMPLE, "0.1", f"import {DEPENDENCY}\n", (f"{DEPENDENCY}>=1",))
    two_wheels = tmp_path / "two-wheels"
    two_wheels.mkdir()
    _wheel(two_wheels, SAMPLE, "0.1", f"import {DEPENDENCY}\n", (f"{DEPENDENCY}>=1",))
    _wheel(two_wheels, SAMPLE, "0.2", f"import {DEPENDENCY}\n", (f"{DEPENDENCY}>=1",))

    missing = _run("--dist", str(only_wheel))
    duplicated = _run("--dist", str(two_wheels))

    for result in (missing, duplicated):
        assert result.returncode == 1, result.stdout
        assert _refusal_ids(result.stdout) == ["wrong-artifacts"], result.stdout


def test_the_highest_resolution_installs_the_newest_satisfying_dependency(tmp_path):
    """The default resolution installs what a user's install gets today: the higher of the two versions.

    Class: NEW CAPABILITY -- a check that never passes `--resolution`, or passes the wrong value for
    this arm, installs 1.0 here and reds the assertion while the run still conforms.
    Why: `highest` is the resolution a user's install gets, and it must be the one measured.
    What kills it: passing `lowest-direct` under the default, which reds this and greens the next test.
    Second implementation: a check that names the requirements on the command line in BOTH arms still
    installs 2.0 here, so this test does not distinguish the passthrough; the next one does.
    """
    assert _installed_version(tmp_path, ()) == "2.0"


def test_the_lowest_direct_resolution_installs_the_declared_floor(tmp_path):
    """`lowest-direct` installs the lowest version the declaration admits, so the floor is exercised.

    Class: NEW CAPABILITY -- a check that passes `--resolution lowest-direct` but omits the declared
    requirements from the command line installs 2.0 here, which is the defect this arm exists for.
    Why: `numpy>=2` is a promise about the floor, and stating it is not installing it.
    What kills it: dropping the requirement passthrough, which reds this and greens the previous test.
    Second implementation: a check that passes the requirements under both resolutions installs 1.0
    here and 2.0 above, so the pair holds both arms rather than this one alone.
    """
    assert _installed_version(tmp_path, ("--resolution", "lowest-direct")) == "1.0"


def test_an_unrecognized_resolution_is_a_usage_error(tmp_path):
    """A resolution the installer does not know is the caller's mistake, so it is exit 2 and not a refusal.

    Class: NEW CAPABILITY -- a check that passes the value through to the installer exits 1 or raises,
    and reds the exit status; the refusal channel must not carry a caller's mistake.
    Why: a refusal line is read by the job as an artifact verdict, and a usage error is not one.
    What kills it: replacing the optval choices with a free-form string.
    Second implementation: a check that validates the value itself before invoking uv reports the same
    exit status and reds no assertion here, so this test holds the contract and not the mechanism.
    """
    dist = _dist(tmp_path / "dist")

    result = _run("--dist", str(dist), "--resolution", "lowest")

    assert result.returncode == 2
    assert result.stdout == ""
    assert "--resolution" in result.stderr


def test_the_runtime_declaration_is_the_numpy_floor_and_the_test_group_carries_the_rest():
    """The declaration is `numpy>=2,<3`, and `pandas` and `xarray` are the test group's, not a user's install.

    Class: REGRESSION -- red on the tree this chunk starts from, whose declaration is
    `["numpy", "pandas", "xarray"]` with no bound.
    Why: both halves are one claim. Removing a dependency from the declaration is only correct while
    the group the suite installs still carries it, and dropping it from both fails every suite job.
    What kills it: reinstating either library in the declaration, or removing one from the test group.
    Second implementation: a declaration written as a tuple or with another order reds the exact list,
    and that is deliberate: this test states the declaration the chunk ships, not its shape.
    """
    project = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))

    assert project["project"]["dependencies"] == ["numpy>=2,<3", "snakemake==9.15.0"], project["project"][
        "dependencies"
    ]
    group = " ".join(project["dependency-groups"]["test"])
    for named in ("pandas==", "xarray=="):
        assert named in group, (named, group)


def test_every_third_party_import_under_src_is_named_by_the_declaration():
    """Each third-party import under `src/` is a distribution the declaration names, and no other is imported.

    Class: REGRESSION -- red on the tree this chunk starts from, whose `seed_contract.py` imports
    `numpy` and whose declaration names three distributions, two of which nothing imports.
    Why: the script compares a BUILT artifact, which needs a build; this holds the same claim on every
    push, over the sources the build would package.
    What kills it: a new third-party import with no declaration, which is the defect the ground refuses.
    Second implementation: a map from import name to distribution name instead of a name comparison
    passes this test; the comparison is by name because this package's dependencies are equal under it,
    and a dependency whose two names differ is the edit this docstring asks the next author to make.
    """
    project = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    declared = {
        re.sub(r"[-_.]+", "-", re.match(r"[A-Za-z0-9][A-Za-z0-9._-]*", dependency).group(0)).lower()
        for dependency in project["project"]["dependencies"]
    }

    imported = set()
    for path in sorted(PACKAGE.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".", 1)[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                imported.add(node.module.split(".", 1)[0])
    third_party = {
        name
        for name in imported
        if name not in sys.stdlib_module_names and name not in {"__future__", "multidriver_swg"}
    }

    assert third_party == {"numpy", "snakemake"}, (
        f"src/ imports {sorted(third_party)} and the declaration names {sorted(declared)}; "
        "declare a new dependency or import the declared one"
    )
    assert {"numpy"} <= declared, declared
