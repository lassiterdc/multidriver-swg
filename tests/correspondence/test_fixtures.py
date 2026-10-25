"""Tests of the fixture generator, the loader and the state check over committed fixtures.

Each test builds its own checkout-shaped root under pytest's temporary directory and passes it as the
`root` parameter, so no test writes into the checkout and no test generates a fixture of the real tree.
Each docstring names the property, the change that makes the test fail, and a second correct
implementation that it still passes.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from tests.correspondence import compare, fixtures, value_codec

REPO_ROOT = Path(fixtures.__file__).resolve().parents[2]

PREAMBLE = """\
import numpy as np
import pandas as pd

from tests.correspondence.fixtures import generates_fixture
"""

SERIES_GENERATOR = (
    PREAMBLE
    + """
@generates_fixture(name="draws", seed=7)
def draws(rng):
    return pd.Series(rng.normal(size=4), index=pd.RangeIndex(4), name="v")
"""
)


def make_root(tmp_path: Path, **modules: str) -> Path:
    """A checkout-shaped root whose `tests/fixture_generators/` holds the given modules, keyed by file stem."""
    directory = tmp_path / "tests" / "fixture_generators"
    directory.mkdir(parents=True)
    for stem, source in modules.items():
        (directory / f"{stem}.py").write_text(source, encoding="utf-8")
    return tmp_path


def record_path(root: Path, system: str = "a_system") -> Path:
    """Where a system's record lives under `root`."""
    return root / fixtures.FIXTURES / system / fixtures.RECORD


def test_generate_writes_the_file_and_one_record_row(tmp_path):
    """Property: `generate` writes `{name}.npz` and a record row holding the file's digest, the generator's name and seed, and numpy's version.

    Class: NEW CAPABILITY; the wrong generator writes the file and records nothing, or records a digest of the value instead of the file.
    Why required: the record is what `load` and the state check hold the file to.
    Killed by: skipping `_write_record`, or digesting something other than the written bytes.
    A second correct implementation: writing the record first and the file second, with the same final state.
    """
    root = make_root(tmp_path, a_system=SERIES_GENERATOR)
    path = fixtures.generate("a-system", "draws", root=root)
    assert path == root / fixtures.FIXTURES / "a_system" / "draws.npz"
    rows = fixtures._read_record(path.parent)
    assert rows["draws"] == {
        "name": "draws",
        "digest": value_codec.label(path.read_bytes()),
        "generator": "draws",
        "seed": 7,
        "numpy": np.__version__,
    }


def test_the_fixture_is_the_seeded_draw_and_a_different_seed_gives_a_different_one(tmp_path):
    """Property: the generator receives `numpy.random.default_rng(seed)` for its declared seed.

    Class: NEW CAPABILITY; the wrong generator draws from an unseeded or wrongly seeded generator.
    Why required: a fixture is reproducible only if its seed is what the record says.
    Killed by: `np.random.default_rng()` with no seed, or the seed offset by one.
    A second correct implementation: seeding `np.random.Generator(np.random.PCG64(seed))`, the same stream.
    """
    other = SERIES_GENERATOR.replace("seed=7", "seed=8")
    first = fixtures.generate("a", "draws", root=make_root(tmp_path / "one", a=SERIES_GENERATOR))
    second = fixtures.generate("a", "draws", root=make_root(tmp_path / "two", a=other))
    expected = pd.Series(np.random.default_rng(7).normal(size=4), index=pd.RangeIndex(4), name="v")
    assert compare.Exact().admits(expected, value_codec.decode(first.read_bytes()))
    assert not compare.Exact().admits(expected, value_codec.decode(second.read_bytes()))


def test_two_generations_in_two_roots_give_identical_bytes(tmp_path):
    """Property: one generator run in two roots writes byte-identical files and records.

    Class: NEW CAPABILITY; the wrong generator embeds a path, a time or a process id in the file or the record.
    Why required: a fixture regenerated under `--replace` must not move its digest without a change of content.
    Killed by: writing the root or a timestamp into the record row.
    A second correct implementation: leaving the record's `numpy` field out, trading a diagnostic for stability.
    """
    one = make_root(tmp_path / "one", a=SERIES_GENERATOR)
    two = make_root(tmp_path / "two", a=SERIES_GENERATOR)
    fixtures.generate("a", "draws", root=one)
    fixtures.generate("a", "draws", root=two)
    for name in ("draws.npz", fixtures.RECORD):
        assert (one / fixtures.FIXTURES / "a" / name).read_bytes() == (
            two / fixtures.FIXTURES / "a" / name
        ).read_bytes()


def test_a_recorded_fixture_is_not_overwritten_without_replace(tmp_path):
    """Property: `generate` on a recorded fixture refuses naming `--replace` and changes nothing; with `replace` it writes again.

    Class: NEW CAPABILITY; the wrong generator overwrites silently, moving a committed digest.
    Why required: a committed fixture changes only by a deliberate act.
    Killed by: dropping the `replace` check.
    A second correct implementation: refusing even with `replace` unless the digest is unchanged.
    """
    root = make_root(tmp_path, a=SERIES_GENERATOR)
    path = fixtures.generate("a", "draws", root=root)
    before = (path.read_bytes(), record_path(root, "a").read_bytes())
    with pytest.raises(fixtures.FixtureError, match="--replace"):
        fixtures.generate("a", "draws", root=root)
    assert (path.read_bytes(), record_path(root, "a").read_bytes()) == before
    assert fixtures.generate("a", "draws", root=root, replace=True) == path


def test_an_unrecorded_file_where_a_fixture_would_go_is_not_overwritten(tmp_path):
    """Property: a stray file at the fixture's path is refused like a recorded one.

    Class: NEW CAPABILITY; the wrong generator checks only the record and writes over an unrecorded file.
    Why required: an unrecorded file is already a state-check finding; overwriting it hides who wrote it.
    Killed by: testing `name in rows` alone.
    A second correct implementation: moving the stray file aside before writing.
    """
    root = make_root(tmp_path, a=SERIES_GENERATOR)
    stray = root / fixtures.FIXTURES / "a" / "draws.npz"
    stray.parent.mkdir(parents=True)
    stray.write_bytes(b"stray")
    with pytest.raises(fixtures.FixtureError, match="--replace"):
        fixtures.generate("a", "draws", root=root)
    assert stray.read_bytes() == b"stray"


def test_an_unknown_fixture_name_is_refused_listing_the_declared_ones(tmp_path):
    """Property: asking for a name no generator declares raises `FixtureError` that lists the names the system declares.

    Class: NEW CAPABILITY; the wrong generator runs the only generator it finds, or raises a bare KeyError.
    Why required: a typo must say what was available.
    Killed by: ignoring `name` when looking up the generator.
    A second correct implementation: suggesting the nearest declared name.
    """
    root = make_root(tmp_path, a=SERIES_GENERATOR)
    with pytest.raises(fixtures.FixtureError, match=r"\['draws'\]"):
        fixtures.generate("a", "missing", root=root)
    assert not (root / fixtures.FIXTURES).exists()


def test_discovery_ignores_underscore_modules_and_refuses_a_name_declared_twice(tmp_path):
    """Property: a module whose stem starts with an underscore is not a system; two functions declaring one name in one system are refused naming both.

    Class: NEW CAPABILITY; the wrong discovery imports helpers as systems, or lets the later declaration win.
    Why required: `generate --name` must have exactly one function to run.
    Killed by: dropping the underscore skip or the duplicate check.
    A second correct implementation: failing on any module whose stem is not a system slug.
    """
    twice = SERIES_GENERATOR + SERIES_GENERATOR.split("fixtures import generates_fixture\n")[1].replace(
        "def draws", "def other"
    )
    root = make_root(tmp_path, _helpers=SERIES_GENERATOR, a=twice)
    with pytest.raises(fixtures.FixtureError, match="draws.*other|other.*draws"):
        fixtures.discover(root)
    clean = make_root(tmp_path / "clean", _helpers=SERIES_GENERATOR, b=SERIES_GENERATOR)
    assert set(fixtures.discover(clean)) == {("b", "draws")}


def test_load_returns_the_fixture_and_never_runs_a_generator(tmp_path, monkeypatch):
    """Property: `load` of a recorded, unchanged fixture returns its value without calling `discover` or `generate`.

    Class: NEW CAPABILITY; the wrong loader regenerates a missing or stale fixture on demand.
    Why required: fixtures are never regenerated when the tests run, so a library change cannot move an input.
    Killed by: a loader that falls back to `generate`.
    A second correct implementation: a loader that has no access to the generators at all.
    """
    root = make_root(tmp_path, a=SERIES_GENERATOR)
    path = fixtures.generate("a", "draws", root=root)
    expected = value_codec.decode(path.read_bytes())

    def forbidden(*args, **kwargs):
        """Stand-in for `discover` and `generate` that fails the test if a load ever calls it."""
        raise AssertionError("a generator was consulted while loading")

    monkeypatch.setattr(fixtures, "discover", forbidden)
    monkeypatch.setattr(fixtures, "generate", forbidden)
    assert compare.Exact().admits(expected, fixtures.load("a", "draws", root=root))
    path.write_bytes(path.read_bytes() + b"\0")
    with pytest.raises(fixtures.FixtureError, match="digest"):
        fixtures.load("a", "draws", root=root)
    assert path.read_bytes().endswith(b"\0")  # refused, and not repaired by regenerating


def test_load_refuses_an_unrecorded_name_and_a_missing_file(tmp_path):
    """Property: `load` refuses a name the record does not hold and a recorded name whose file is gone, each by name.

    Class: NEW CAPABILITY; the wrong loader reads any file in the directory, or returns None.
    Why required: a shared cached artifact whose key does not match what was recorded is refused, not adopted.
    Killed by: reading `{name}.npz` before consulting the record.
    A second correct implementation: raising FileNotFoundError for the missing file.
    """
    root = make_root(tmp_path, a=SERIES_GENERATOR)
    path = fixtures.generate("a", "draws", root=root)
    shadow = path.with_name("shadow.npz")
    shadow.write_bytes(path.read_bytes())
    with pytest.raises(fixtures.FixtureError, match="not recorded"):
        fixtures.load("a", "shadow", root=root)
    path.unlink()
    with pytest.raises(fixtures.FixtureError, match="missing"):
        fixtures.load("a", "draws", root=root)


@pytest.mark.parametrize("bad", ["../fixtures/a", "a/b", "", ".hidden", "x y"])
def test_a_name_that_is_not_a_plain_file_name_is_refused(tmp_path, bad):
    """Property: a system slug or fixture name that is not letters, digits, underscores and hyphens is refused by that rule, in `generate` and in `load`.

    Class: NEW CAPABILITY; the wrong module joins the slug into a path, so `load("../fixtures/a", ...)` reads another directory's fixture.
    Why required: the slug and the name become path components under a committed directory.
    Killed by: removing the `_NAME` check; the message is matched because an unknown generator also raises `FixtureError`.
    A second correct implementation: resolving the path and checking it stays inside the fixtures directory.
    """
    root = make_root(tmp_path, a=SERIES_GENERATOR)
    fixtures.generate("a", "draws", root=root)
    with pytest.raises(fixtures.FixtureError, match="letters, digits"):
        fixtures.generate("a", bad, root=root)
    with pytest.raises(fixtures.FixtureError, match="letters, digits"):
        fixtures.generate(bad, "draws", root=root)
    with pytest.raises(fixtures.FixtureError, match="letters, digits"):
        fixtures.load(bad, "draws", root=root)
    assert fixtures.load("a", "draws", root=root) is not None  # control: the same load is admitted by its plain slug


# ---- the generator may open nothing outside the interpreter's installation ----

READS_A_FILE = (
    PREAMBLE
    + """
@generates_fixture(name="leak", seed=1)
def leak(rng):
    text = open({path!r}).read()
    return np.array([len(text)])
"""
)

CATCHES_THE_REFUSAL = (
    PREAMBLE
    + """
@generates_fixture(name="leak", seed=1)
def leak(rng):
    try:
        open({path!r}).read()
    except OSError:
        pass
    return np.array([1])
"""
)


def test_a_generator_that_opens_a_repository_file_is_refused_and_writes_nothing(tmp_path):
    """Property: a generator opening a file outside the installation fails the run and leaves no fixture or record.

    Class: NEW CAPABILITY; the wrong generator lets a fixture be built from repository, home or private data.
    Why required: the anonymization guard never scans a binary fixture, so this hook is its only check.
    Killed by: not installing the audit hook, or scoping it to a path list that omits the file.
    A second correct implementation: running the generator in a subprocess with no filesystem access.
    """
    target = str(REPO_ROOT / "pyproject.toml")
    root = make_root(tmp_path, a=READS_A_FILE.format(path=target))
    with pytest.raises(fixtures.FixtureError, match="pyproject.toml"):
        fixtures.generate("a", "leak", root=root)
    assert not (root / fixtures.FIXTURES).exists()


def test_the_hook_is_off_outside_a_generator_run(tmp_path):
    """Property (the control of the test above): the file the generator was refused is readable by the test itself, before and after.

    Class: NEW CAPABILITY; without this, a refusal could mean the file is unreadable to everyone.
    Why required: it shows the refusal comes from the hook and that the hook does not outlive the generator.
    Killed by: a hook that stays active after a generator run.
    A second correct implementation: any readable file outside the installation.
    """
    target = REPO_ROOT / "pyproject.toml"
    assert target.read_text(encoding="utf-8")
    root = make_root(tmp_path, a=READS_A_FILE.format(path=str(target)))
    with pytest.raises(fixtures.FixtureError):
        fixtures.generate("a", "leak", root=root)
    assert target.read_text(encoding="utf-8")


def test_a_generator_that_catches_the_refusal_still_fails(tmp_path):
    """Property: a generator that swallows the `PermissionError` of a refused open still fails the run.

    Class: NEW CAPABILITY; the wrong hook raises only, so a `try/except` around the open defeats it.
    Why required: the guard must not depend on the generator's cooperation.
    Killed by: removing the recorded-refusal check in `_installation_only`.
    A second correct implementation: the hook aborting the process.
    """
    target = str(REPO_ROOT / "pyproject.toml")
    root = make_root(tmp_path, a=CATCHES_THE_REFUSAL.format(path=target))
    with pytest.raises(fixtures.FixtureError, match="pyproject.toml"):
        fixtures.generate("a", "leak", root=root)
    assert not (root / fixtures.FIXTURES).exists()


def test_a_generator_may_open_a_file_inside_the_installation(tmp_path):
    """Property: opening a file under the interpreter's installation, here the standard library's `os.py`, is admitted.

    Class: NEW CAPABILITY; the wrong hook refuses every open, which stops lazy imports and library data.
    Why required: pandas and numpy open library files while a generator runs.
    Killed by: refusing all `open` events.
    A second correct implementation: allowing only a list of library directories.
    """
    reads_library = READS_A_FILE.format(path=os.__file__)
    root = make_root(tmp_path, a=reads_library)
    assert fixtures.generate("a", "leak", root=root).is_file()


ZONE_AWARE = (
    PREAMBLE
    + """
@generates_fixture(name="zone", seed=3)
def zone(rng):
    index = pd.date_range("2020-03-07", periods=48, freq="h", tz="America/New_York")
    return pd.Series(rng.normal(size=48), index=index, name="v")
"""
)


def test_a_time_zone_aware_index_is_admitted_and_round_trips(tmp_path):
    """Property: a generator building a time-zone-aware pandas index runs under the hook and its fixture loads back exactly.

    Class: NEW CAPABILITY; the wrong hook refuses the zone database a time-zone lookup reads.
    Why required: the project's inputs are time series with zones, across a daylight-saving change.
    Killed by: restricting opens to the checkout, or to non-library paths.
    A second correct implementation: allowing the zone database's directory by name as well.
    """
    root = make_root(tmp_path, a=ZONE_AWARE)
    path = fixtures.generate("a", "zone", root=root)
    loaded = fixtures.load("a", "zone", root=root)
    assert isinstance(loaded.index.dtype, pd.DatetimeTZDtype)
    assert str(loaded.index.dtype.tz) == "America/New_York"
    assert compare.Exact().admits(loaded, value_codec.decode(path.read_bytes()))


def test_a_value_the_codec_does_not_store_is_refused_and_writes_nothing(tmp_path):
    """Property: a generator returning a dict fails with the codec's refusal and leaves no fixture or record.

    Class: NEW CAPABILITY; the wrong generator creates the directory and a partial record before encoding.
    Why required: a failed generation must not leave a half-recorded fixture for the state check to find later.
    Killed by: writing the record before `encode`.
    A second correct implementation: writing to a temporary name and renaming.
    """
    bad = PREAMBLE + '\n@generates_fixture(name="bad", seed=1)\ndef bad(rng):\n    return {"a": 1}\n'
    root = make_root(tmp_path, a=bad)
    with pytest.raises(value_codec.CodecError, match="dict"):
        fixtures.generate("a", "bad", root=root)
    assert not (root / fixtures.FIXTURES).exists()


# ---- the state check over committed fixtures ----


def _tamper_a_byte(root: Path) -> None:
    path = root / fixtures.FIXTURES / "a" / "draws.npz"
    data = bytearray(path.read_bytes())
    data[-1] ^= 1
    path.write_bytes(bytes(data))


def _add_a_stray_file(root: Path) -> None:
    (root / fixtures.FIXTURES / "a" / "stray.npz").write_bytes(b"x")


def _delete_the_file(root: Path) -> None:
    (root / fixtures.FIXTURES / "a" / "draws.npz").unlink()


def _add_a_loose_file_in_the_fixtures_directory(root: Path) -> None:
    (root / fixtures.FIXTURES / "loose.npz").write_bytes(b"x")


def _record_under_an_older_rule(root: Path) -> None:
    path = record_path(root, "a")
    path.write_text(
        path.read_text().replace(f"{value_codec.RULE}:{value_codec.CODEC_VERSION}:", f"{value_codec.RULE}:0:")
    )


def _break_the_record(root: Path) -> None:
    record_path(root, "a").write_text("this is [not toml")


def _drop_a_record_field(root: Path) -> None:
    path = record_path(root, "a")
    path.write_text(path.read_text().replace('generator = "draws"\n', ""))


@pytest.mark.state_check
@pytest.mark.parametrize(
    ("damage", "fragment"),
    [
        (_tamper_a_byte, "has digest"),
        (_add_a_stray_file, "stray.npz"),
        (_delete_the_file, "missing"),
        (_add_a_loose_file_in_the_fixtures_directory, "loose.npz"),
        (_record_under_an_older_rule, "--replace"),
        (_break_the_record, "not readable TOML"),
        (_drop_a_record_field, "lacks the fields"),
    ],
    ids=["edited byte", "unrecorded file", "missing file", "loose file", "older rule", "broken record", "short row"],
)
def test_the_check_names_each_kind_of_damage(tmp_path, damage, fragment):
    """Property: `fixture_findings` is empty on a freshly generated tree and names each of seven kinds of damage after it.

    Class: NEW CAPABILITY; the wrong check passes a tree whose recorded digest no longer matches, or that holds an unrecorded file.
    Why required: the state-check job relies on this to refuse an edited or unrecorded fixture.
    Killed by: skipping the digest comparison, or the directory listing.
    A second correct implementation: reading the digests from git's own object ids.
    """
    root = make_root(tmp_path, a=SERIES_GENERATOR)
    fixtures.generate("a", "draws", root=root)
    assert fixtures.fixture_findings(root) == []
    damage(root)
    findings = fixtures.fixture_findings(root)
    assert findings
    assert any(fragment in finding for finding in findings), findings


@pytest.mark.state_check
def test_a_tree_with_no_fixtures_has_no_findings_and_says_it_read_none(tmp_path):
    """Property: a root with no fixture directory has no findings, and `fixture_count` reports zero fixtures read.

    Class: NEW CAPABILITY; the wrong check raises on the missing directory, or reports a pass that hides it read nothing.
    Why required: this row lands with no committed fixture, and the count is what separates that pass from a measured one.
    Killed by: raising when the directory is absent, or returning a non-zero count.
    A second correct implementation: returning a finding that the tree is empty.
    """
    assert fixtures.fixture_findings(tmp_path) == []
    assert fixtures.fixture_count(tmp_path) == 0


@pytest.mark.state_check
def test_the_fixtures_committed_in_this_checkout_match_their_records(capsys):
    """Property: every fixture file in this checkout is recorded with its digest, and no file is unrecorded.

    Class: NEW CAPABILITY; the wrong check passes a tree that holds an edited fixture or a fixture file with no record row.
    Why required: a fixture edited by hand would move every verdict computed from it unnoticed.
    Killed by: editing any committed fixture byte, or committing a fixture file with no record row.
    A second correct implementation: a pre-commit hook computing the same findings.
    """
    findings = fixtures.fixture_findings(REPO_ROOT)
    with capsys.disabled():
        print(f"\nfixtures committed: {fixtures.fixture_count(REPO_ROOT)}")
    assert findings == []


# ---- the command ----


def _run_command(root: Path, *arguments: str) -> subprocess.CompletedProcess:
    """Run `python -m tests.correspondence.fixtures generate` from the checkout, writing under `root`."""
    environment = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    return subprocess.run(
        [sys.executable, "-m", "tests.correspondence.fixtures", "generate", "--root", str(root), *arguments],
        cwd=REPO_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


def test_the_command_generates_once_and_refuses_the_second_run(tmp_path):
    """Property: the command run with `-m` writes the fixture and prints its digest; the same command again exits 1 naming `--replace`.

    Class: NEW CAPABILITY; the wrong command exits 0 on a refusal, or fails to see generators that import the decorator from the package copy of the module.
    Why required: the command is how fixtures are made; run as `__main__` it is a second copy of this module, and discovery must still find the decorated functions.
    Killed by: matching generators by comparing classes held in a registry of the `__main__` copy.
    A second correct implementation: a console script entry point calling `main`.
    """
    root = make_root(tmp_path, a_system=SERIES_GENERATOR)
    first = _run_command(root, "--system", "a-system", "--name", "draws")
    assert first.returncode == 0, first.stderr
    assert first.stdout.startswith("wrote ") and value_codec.RULE in first.stdout
    second = _run_command(root, "--system", "a-system", "--name", "draws")
    assert second.returncode == 1
    assert "--replace" in second.stderr
    third = _run_command(root, "--system", "a-system", "--name", "draws", "--replace")
    assert third.returncode == 0, third.stderr


def test_the_command_refuses_a_generator_that_reads_a_repository_file(tmp_path):
    """Property: the hook is active in the command's process too, so a leaking generator makes it exit 1 and write nothing.

    Class: NEW CAPABILITY; the wrong command installs the hook only for in-process callers.
    Why required: the command, not the in-process call, is how a real fixture is made.
    Killed by: guarding in `generate`'s callers instead of in `generate`.
    A second correct implementation: the command re-running itself under a restricted subprocess.
    """
    root = make_root(tmp_path, a=READS_A_FILE.format(path=str(REPO_ROOT / "pyproject.toml")))
    result = _run_command(root, "--system", "a", "--name", "leak")
    assert result.returncode == 1
    assert "outside the interpreter's installation" in result.stderr
    assert not (root / fixtures.FIXTURES).exists()


def test_the_generator_decorator_refuses_a_bad_name_or_seed():
    """Property: `generates_fixture` raises for a name that is not a plain file name and for a seed that is not a non-negative int.

    Class: NEW CAPABILITY; the wrong decorator accepts a bool or a negative number as a seed, which numpy reads in other ways.
    Why required: the seed in the record must be the seed that produced the draw.
    Killed by: removing the `type(seed) is not int` test.
    A second correct implementation: converting the seed with `operator.index`.
    """
    for bad in (
        {"name": "a b", "seed": 1},
        {"name": "ok", "seed": -1},
        {"name": "ok", "seed": True},
        {"name": "ok", "seed": 1.5},
    ):
        with pytest.raises(fixtures.FixtureError):
            fixtures.generates_fixture(**bad)
    mark = fixtures.generates_fixture(name="ok", seed=0)(lambda rng: None)
    assert (mark.__fixture_name__, mark.__fixture_seed__) == ("ok", 0)


def test_the_conventions_module_for_generators_exists_and_is_a_package():
    """Property: `tests/fixture_generators/` is a package whose docstring states the one-module-per-system convention.

    Class: NEW CAPABILITY; the wrong layout leaves the directory undeclared, so the first author has no convention to follow.
    Why required: the authors of fixtures write modules here by following this convention.
    Killed by: deleting `__init__.py`.
    A second correct implementation: a README stating the same convention.
    """
    package = REPO_ROOT / fixtures.GENERATORS / "__init__.py"
    assert package.is_file()
    assert "one module here" in " ".join(package.read_text(encoding="utf-8").split())


STARTS_A_PROCESS = (
    PREAMBLE
    + """
import os
import subprocess
import sys

@generates_fixture(name="child", seed=1)
def child(rng):
    try:
        {call}
    except OSError:
        pass
    return np.array([1])
"""
)

PROCESS_CALLS = {
    "subprocess": "subprocess.run([sys.executable, '-c', 'pass'], check=False)",
    "os.system": "os.system('true')",
    "os.spawn": "os.spawnv(os.P_WAIT, sys.executable, [sys.executable, '-c', 'pass'])",
    "os.posix_spawn": "os.waitpid(os.posix_spawn(sys.executable, [sys.executable, '-c', 'pass'], os.environ), 0)",
}


@pytest.mark.parametrize("call", list(PROCESS_CALLS))
def test_a_generator_that_starts_a_child_process_is_refused_and_writes_nothing(tmp_path, call):
    """Property: a generator starting a process by any route, even one that swallows the refusal, fails the run and leaves no fixture or record.

    Class: NEW CAPABILITY; the wrong guard watches only `open`, so a child process (`cat` of a repository file) carries data past it.
    Why required: the anonymization guard never scans a binary fixture, and a child process reads what the hook cannot see.
    Killed by: removing an event from `PROCESS_EVENTS`, or not recording the refusal.
    A second correct implementation: running the generator in a subprocess with no process-creation permission.
    """
    root = make_root(tmp_path, a=STARTS_A_PROCESS.format(call=PROCESS_CALLS[call]))
    with pytest.raises(fixtures.FixtureError, match="start a process"):
        fixtures.generate("a", "child", root=root)
    assert not (root / fixtures.FIXTURES).exists()


def test_the_same_child_process_runs_outside_a_generator(tmp_path):
    """Property (the control of the test above): starting that process from the test itself works, before and after a refused run.

    Class: NEW CAPABILITY; without this, a refusal could mean processes cannot start in this environment.
    Why required: it shows the refusal comes from the hook and that the hook does not outlive the run.
    Killed by: a process guard that stays active after the generator.
    A second correct implementation: any process the environment can start.
    """
    assert subprocess.run([sys.executable, "-c", "pass"], check=False).returncode == 0
    root = make_root(tmp_path, a=STARTS_A_PROCESS.format(call=PROCESS_CALLS["subprocess"]))
    with pytest.raises(fixtures.FixtureError):
        fixtures.generate("a", "child", root=root)
    assert subprocess.run([sys.executable, "-c", "pass"], check=False).returncode == 0


def test_the_interpreter_launch_is_put_back_after_a_generator_run(tmp_path):
    """Property: `_posixsubprocess.fork_exec` is the same object before and after a run, refused or not.

    Class: NEW CAPABILITY; the wrong guard leaves the stand-in installed, so every later process in the session is refused.
    Why required: the stand-in is a process-wide replacement, and the test session starts processes itself.
    Killed by: not calling `restore` in `_installation_only`'s `finally`.
    A second correct implementation: patching with `unittest.mock.patch.object`, which restores the same way.
    """
    import _posixsubprocess

    before = _posixsubprocess.fork_exec
    root = make_root(tmp_path, a=STARTS_A_PROCESS.format(call=PROCESS_CALLS["subprocess"]))
    with pytest.raises(fixtures.FixtureError):
        fixtures.generate("a", "child", root=root)
    assert _posixsubprocess.fork_exec is before
