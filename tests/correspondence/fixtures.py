"""Make the synthetic inputs that unit lifts run on, once, and refuse a changed or unrecorded copy.

An equivalence test runs a copy of a legacy unit on an input. If the input were drawn when the test
runs, a change of the random-number library would move the input and every verdict computed from it;
if it were read from real data, the public toolkit would publish that data. So an input is a fixture:
a file generated once by a function that declares its name and seed, committed together with a record
of its digest, and read back by `load` with the digest checked. Nothing in a test run calls `generate`.

The three parts, and where each lives:

- A generator is a function decorated with `generates_fixture(name=..., seed=...)` in
  `tests/fixture_generators/{system}.py`, one module per owning system, found by walking that
  directory the way the binding walks `tests/registrations/`. It receives a `numpy.random.Generator`
  seeded with the declared seed and returns a value the codec stores (`value_codec`).
- A fixture is `tests/correspondence/fixtures/{system}/{name}.npz` with its row in
  `tests/correspondence/fixtures/{system}/record.toml`: the digest label of the file, the generator's
  name and seed, and the numpy version, which is a diagnostic and decides nothing. `{system}` is the
  owning system's slug with hyphens as underscores, the spelling of the registration modules.
- `python -m tests.correspondence.fixtures generate --system {slug} --name {name}` runs one generator
  once and writes both. It refuses to overwrite a recorded fixture unless `--replace` is given.

While the generator function runs, an audit hook refuses to open any path outside the interpreter's
installation (`sys.prefix` and `sys.base_prefix`) and refuses to start any process (`subprocess`,
`os.system`, `os.exec*`, `os.spawn*`, `os.posix_spawn`, `os.fork`, and the interpreter launch that
`multiprocessing`'s `spawn` and `forkserver` contexts make). That is how a fixture stays
synthetic: the generator function cannot read a repository file, a home-directory file or a file of a
private data repository, directly or through a child process, and write its content into a binary
that the anonymization guard, which skips what it cannot read as text, would never scan. Library
files that a lazy import, a pytz lookup, or a zoneinfo lookup whose search path lies inside the
installation opens stay readable; a dateutil zone lookup, which reads the system zone database, is
refused.

The hook covers the generator function and nothing before it. The generator's module is executed
once at discovery, outside the hook, so a file the module reads at its top level is NOT refused: a
generator module must not read files at module level. A fixture's content is therefore synthetic up
to data placed inside the installation and data the module reads when it is imported.

Every function takes the root it works under as a parameter, and the tests pass a temporary one, so no
test writes into the checkout.
"""

from __future__ import annotations

import argparse
import contextlib
import importlib.util
import os
import re
import sys
import tomllib
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import numpy as np

from tests.correspondence import value_codec

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURES = Path("tests/correspondence/fixtures")
GENERATORS = Path("tests/fixture_generators")
RECORD = "record.toml"

_NAME = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_-]*")  # a system slug or a fixture name, safe as a file name
_ROW_FIELDS = ("name", "digest", "generator", "seed", "numpy")


class FixtureError(Exception):
    """A fixture that cannot be generated, or a stored fixture that cannot be trusted, with the reason in the message."""


# ---- declaring a generator ----


def generates_fixture(name: str, seed: int) -> Callable[[Callable], Callable]:
    """Mark a function as the generator of the fixture `name`, drawing from a generator seeded with `seed`.

    The mark is two plain attributes on the function, not an entry in a registry held by this
    module, so that it survives this module being loaded twice (once as a package module and once as
    `__main__` when run with `-m`): discovery reads the attributes, and never compares classes.
    """
    if not isinstance(name, str) or not _NAME.fullmatch(name):
        raise FixtureError(f"a fixture name is letters, digits, underscores and hyphens; {name!r} is not")
    if type(seed) is not int or seed < 0:
        raise FixtureError(f"the seed of fixture {name!r} must be a non-negative int, not {seed!r}")

    def mark(function: Callable) -> Callable:
        """Attach the declared name and seed to `function` and return it unchanged."""
        function.__fixture_name__ = name
        function.__fixture_seed__ = seed
        return function

    return mark


def system_directory_name(system: str) -> str:
    """The spelling of a system slug in file and directory names: hyphens become underscores."""
    if not isinstance(system, str) or not _NAME.fullmatch(system):
        raise FixtureError(f"a system slug is letters, digits, underscores and hyphens; {system!r} is not")
    return system.replace("-", "_")


def discover(root: Path = REPO_ROOT) -> dict[tuple[str, str], Callable]:
    """Every generator declared under `{root}/tests/fixture_generators/`, keyed by (system directory name, fixture name).

    Each `{system}.py` is executed once to read its marks. Two functions declaring one name in one
    system are refused, naming both, because `generate --name` could not say which to run.
    """
    found: dict[tuple[str, str], Callable] = {}
    directory = Path(root) / GENERATORS
    if not directory.is_dir():
        return found
    for path in sorted(directory.glob("*.py")):
        if path.name.startswith("_"):
            continue
        module_name = f"_fixture_generators_{path.stem}"
        spec = importlib.util.spec_from_file_location(module_name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        for function in vars(module).values():
            if not callable(function) or not hasattr(function, "__fixture_name__"):
                continue
            if getattr(function, "__module__", None) != module_name:
                continue  # an imported generator belongs to the module that defines it
            key = (path.stem, function.__fixture_name__)
            if key in found:
                raise FixtureError(
                    f"system {path.stem!r} declares the fixture {key[1]!r} twice: "
                    f"{found[key].__name__} and {function.__name__}"
                )
            found[key] = function
    return found


# ---- running a generator inside the installation only ----

PROCESS_EVENTS = frozenset(
    {
        "subprocess.Popen",
        "os.system",
        "os.exec",
        "os.posix_spawn",
        "os.spawn",
        "os.fork",
        "os.forkpty",
        "os.startfile",
    }
)  # the audit events through which Python starts a process
_guard: dict[str, Any] = {"installed": False, "active": 0, "allowed": (), "refused": []}


def _audit(event: str, args: tuple) -> None:
    """Refuse an `open` of a path outside the installation, and the creation of any process, while a generator runs.

    A file descriptor is not a path and passes. The refusal is also recorded, because a generator
    can catch the exception; `_installation_only` fails the run at its end if any was recorded.
    """
    if not _guard["active"]:
        return
    if event in PROCESS_EVENTS:
        _guard["refused"].append(f"a process started by {event}")
        raise PermissionError(f"a fixture generator may not start a process ({event})")
    if event != "open":
        return
    target = args[0]
    if isinstance(target, int):
        return
    try:
        path = os.path.realpath(os.fsdecode(target))
    except TypeError:
        return
    for prefix in _guard["allowed"]:
        if path == prefix or path.startswith(prefix + os.sep):
            return
    _guard["refused"].append(path)
    raise PermissionError(f"a fixture generator may not open {path}: it is outside the interpreter's installation")


@contextlib.contextmanager
def _installation_only() -> Iterator[None]:
    """Run the body with `open` limited to the interpreter's installation and no process started, and fail afterwards if either was refused once."""
    if not _guard["installed"]:
        sys.addaudithook(_audit)  # an audit hook cannot be removed, so it is installed once and switched by `active`
        _guard["installed"] = True
    _guard["allowed"] = tuple({os.path.realpath(sys.prefix), os.path.realpath(sys.base_prefix)})
    _guard["refused"] = []
    _guard["active"] += 1
    restore = _refuse_interpreter_launch()
    try:
        yield
    except Exception as exc:  # the refusal itself, or whatever the generator raised after being refused
        if _guard["refused"]:
            raise FixtureError(_refusal_message()) from exc
        raise
    finally:
        restore()
        _guard["active"] -= 1
    if _guard["refused"]:
        raise FixtureError(_refusal_message())


def _refuse_interpreter_launch() -> Callable[[], None]:
    """Replace `_posixsubprocess.fork_exec` with a recording refusal and return the function that puts it back.

    The `spawn` and `forkserver` contexts of `multiprocessing` start a new interpreter through this
    function and raise none of the audit events in `PROCESS_EVENTS`, so the hook alone would let such
    a worker read any file. `subprocess` reaches the same function, so this also covers it. Deliberately
    NOT refusing only non-`fork` multiprocessing contexts: that covers one library, where this covers every
    caller of the primitive. Not established: a process started by code that calls the C library directly.
    """
    try:
        import _posixsubprocess
    except ImportError:  # a platform without it starts processes by other routes, whose audit events the hook watches
        return lambda: None
    original = _posixsubprocess.fork_exec

    def refuse(*args: Any, **kwargs: Any) -> None:
        """Record the attempt and raise, standing in for the real interpreter launch while a generator runs."""
        _guard["refused"].append("a process started by _posixsubprocess.fork_exec")
        raise PermissionError("a fixture generator may not start a process (_posixsubprocess.fork_exec)")

    _posixsubprocess.fork_exec = refuse

    def restore() -> None:
        """Put the real launch function back."""
        _posixsubprocess.fork_exec = original

    return restore


def _refusal_message() -> str:
    """The sentence naming every path or process the generator was refused."""
    return (
        "the generator tried to open a path outside the interpreter's installation or to start a process, so "
        "its content is not known to be synthetic: " + ", ".join(sorted(set(_guard["refused"])))
    )


# ---- the record beside the fixtures of one system ----


def _quote(text: str) -> str:
    """A TOML basic string for text that is plain ASCII without a quote or a backslash; the fields written here are."""
    if not re.fullmatch(r"[ !#-\[\]-~]*", text):
        raise FixtureError(f"internal error: the record field {text!r} would need TOML escaping")
    return f'"{text}"'


def _read_record(directory: Path) -> dict[str, dict]:
    """The rows of one system's `record.toml` by fixture name, or an empty mapping when there is no record."""
    path = directory / RECORD
    if not path.exists():
        return {}
    try:
        document = tomllib.loads(path.read_text(encoding="utf-8"))
    except (tomllib.TOMLDecodeError, UnicodeDecodeError) as exc:
        raise FixtureError(f"{path} is not readable TOML: {exc}") from exc
    rows: dict[str, dict] = {}
    for row in document.get("fixture", []):
        missing = [field for field in _ROW_FIELDS if field not in row]
        if missing:
            raise FixtureError(f"a row of {path} lacks the fields {missing}")
        if row["name"] in rows:
            raise FixtureError(f"{path} records the fixture {row['name']!r} twice")
        rows[row["name"]] = row
    return rows


def _write_record(directory: Path, rows: dict[str, dict]) -> None:
    """Write the whole record, rows sorted by name, so that the same rows always give the same bytes."""
    lines = ["# Generated by `python -m tests.correspondence.fixtures generate`; edit nothing here by hand.", ""]
    for name in sorted(rows):
        row = rows[name]
        lines += [
            "[[fixture]]",
            f"name = {_quote(row['name'])}",
            f"digest = {_quote(row['digest'])}",
            f"generator = {_quote(row['generator'])}",
            f"seed = {int(row['seed'])}",
            f"numpy = {_quote(row['numpy'])}",
            "",
        ]
    (directory / RECORD).write_text("\n".join(lines), encoding="utf-8")


# ---- generating, loading and checking ----


def generate(system: str, name: str, *, root: Path = REPO_ROOT, replace: bool = False) -> Path:
    """Run the generator of fixture `name` once, write its file and its record row, and return the file's path.

    Refuses, writing nothing, when no generator declares the name, when the fixture is already
    recorded (or an unrecorded file stands where it would go) and `replace` is false, when the
    generator opens a path outside the installation, and when the codec does not store its value.
    """
    root = Path(root)
    system_dir = system_directory_name(system)
    if not _NAME.fullmatch(name):
        raise FixtureError(f"a fixture name is letters, digits, underscores and hyphens; {name!r} is not")
    generators = discover(root)
    function = generators.get((system_dir, name))
    if function is None:
        known = sorted(n for s, n in generators if s == system_dir)
        raise FixtureError(
            f"no generator declares the fixture {name!r} for system {system!r} under {root / GENERATORS}; it declares {known}"
        )
    directory = root / FIXTURES / system_dir
    rows = _read_record(directory)
    path = directory / f"{name}.npz"
    if (name in rows or path.exists()) and not replace:
        raise FixtureError(
            f"the fixture {name!r} of system {system!r} already exists at {path}; pass --replace to write it again"
        )
    rng = np.random.default_rng(function.__fixture_seed__)
    with _installation_only():
        value = function(rng)
    data = value_codec.encode(value)
    directory.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    rows[name] = {
        "name": name,
        "digest": value_codec.label(data),
        "generator": function.__name__,
        "seed": function.__fixture_seed__,
        "numpy": np.__version__,
    }
    _write_record(directory, rows)
    return path


def load(system: str, name: str, *, root: Path = REPO_ROOT) -> Any:
    """Read fixture `name`, refusing it unless it is recorded and its file's digest is the recorded one.

    Nothing here runs a generator: a fixture that is missing or does not match is refused, never
    rebuilt, so a test run cannot move its own inputs.
    """
    directory = Path(root) / FIXTURES / system_directory_name(system)
    row = _read_record(directory).get(name)
    if row is None:
        raise FixtureError(f"the fixture {name!r} of system {system!r} is not recorded in {directory / RECORD}")
    path = directory / f"{name}.npz"
    if not path.is_file():
        raise FixtureError(f"the fixture {name!r} is recorded but its file {path} is missing")
    data = path.read_bytes()
    actual = value_codec.label(data)
    if actual != row["digest"]:
        raise FixtureError(
            f"the fixture file {path} has digest {actual} and its record says {row['digest']}; it was edited or replaced"
        )
    return value_codec.decode(data)


def _directory_findings(directory: Path) -> list[str]:
    """What is wrong with one system's fixture directory, one sentence each."""
    try:
        rows = _read_record(directory)
    except FixtureError as exc:
        return [str(exc)]
    findings = []
    expected_label = f"{value_codec.RULE}:{value_codec.CODEC_VERSION}:"
    for name, row in sorted(rows.items()):
        path = directory / f"{name}.npz"
        if not path.is_file():
            findings.append(f"the fixture {name!r} is recorded but its file {path} is missing")
            continue
        if not row["digest"].startswith(expected_label):
            findings.append(
                f"the fixture {name!r} is recorded under {row['digest'].rsplit(':', 1)[0]!r} and the codec's rule is "
                f"{expected_label[:-1]!r}; generate it again with --replace"
            )
            continue
        actual = value_codec.label(path.read_bytes())
        if actual != row["digest"]:
            findings.append(f"the fixture file {path} has digest {actual} and its record says {row['digest']}")
    for path in sorted(directory.iterdir()):
        if path.name == RECORD or (path.suffix == ".npz" and path.stem in rows):
            continue
        findings.append(f"{path} is in a fixture directory and no row of {RECORD} records it")
    return findings


def fixture_findings(root: Path = REPO_ROOT) -> list[str]:
    """Every defect of the committed fixtures: a recorded one that is missing or changed, an unrecorded file, a bad record.

    A tree with no fixture directory has no findings. That is the state this row lands in, and it is
    a pass over zero fixtures, not a measurement of one; `fixture_count` says how many were read.
    """
    base = Path(root) / FIXTURES
    if not base.is_dir():
        return []
    findings = []
    for entry in sorted(base.iterdir()):
        if entry.is_dir():
            findings += _directory_findings(entry)
        else:
            findings.append(f"{entry} is in the fixtures directory and belongs to no system's directory")
    return findings


def fixture_count(root: Path = REPO_ROOT) -> int:
    """How many fixtures the records under `root` name."""
    base = Path(root) / FIXTURES
    if not base.is_dir():
        return 0
    return sum(len(_read_record(entry)) for entry in sorted(base.iterdir()) if entry.is_dir())


# ---- the command ----


def main(argv: list[str] | None = None) -> int:
    """Run `generate` from the command line; print the file and its digest, or the refusal, and return the exit status."""
    parser = argparse.ArgumentParser(
        prog="python -m tests.correspondence.fixtures", description=__doc__.splitlines()[0]
    )
    commands = parser.add_subparsers(dest="command", required=True)
    generate_parser = commands.add_parser(
        "generate", help="run one generator once and write its fixture and record row"
    )
    generate_parser.add_argument("--system", required=True, help="the owning system's slug")
    generate_parser.add_argument("--name", required=True, help="the fixture's declared name")
    generate_parser.add_argument("--replace", action="store_true", help="write over a recorded fixture")
    generate_parser.add_argument("--root", type=Path, default=REPO_ROOT, help="the checkout to write under")
    arguments = parser.parse_args(argv)
    try:
        path = generate(arguments.system, arguments.name, root=arguments.root, replace=arguments.replace)
    except (FixtureError, value_codec.CodecError) as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 1
    print(f"wrote {path} {value_codec.label(path.read_bytes())}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
