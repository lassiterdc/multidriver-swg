"""Tests of the result-record emitter: its staged schedule, its fields, and what a killed run leaves.

The plugin is driven in a `pytester` tree, because the record's subject is a RUN: which hook wrote
which field, and what survives a run that dies part-way. One test asserts the whole object after a
normal run; two drive a kill at a chosen hook and assert the marker says how far the run got; one
asserts that no option means no file.

The inner tree carries its own conftest, because the root conftest is not an ancestor of a
`pytester` tree: the plugins that must run on the inner session are loaded there, and the record's
option is added there, exactly as the root conftest does it for a real run.
"""

import json
import re
from pathlib import Path

import pytest

from tests.correspondence import record_schema, subject

pytestmark = pytest.mark.state_check

DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
INNER_PLUGINS = ("-p", "no:cacheprovider")

INNER_CONFTEST = """\
pytest_plugins = ["tests.correspondence.inventory", "tests.correspondence.record"]


def pytest_addoption(parser):
    from tests.correspondence import record

    record.add_options(parser)
"""

# The inner tree: one passing test, one failing at call, one failing in a fixture's teardown, and one
# the run deselects. The teardown failure is the case that separates "one row per CALL" from "one row
# per test outcome": pytest counts it as an error while the call phase passed.
TREE = """\
import pytest


@pytest.fixture
def boom():
    yield
    assert False


def test_alpha():
    pass


def test_beta():
    assert False


def test_teardown_fails(boom):
    pass


def test_gamma():
    pass
"""

# Ends the session from a hook that runs after the population write. `trylast` makes that order
# independent of registration: every plain `pytest_collection_modifyitems` has run by then.
KILLER_MID = """\
import pytest


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(session, config, items):
    pytest.exit("killed during collection", returncode=1)
"""

KILLER_START = """\
import pytest


@pytest.hookimpl(trylast=True)
def pytest_sessionstart(session):
    pytest.exit("killed before collection", returncode=1)
"""

# Ends the session from inside a CALL phase of the second test, so the run finishes collection and
# then stops with survivors still unexecuted. The record must fall back to `collected` and `VOID`,
# because it then holds fewer call rows than survivors.
KILLER_CALL = """\
import pytest

CALLS = {"n": 0}


@pytest.hookimpl(trylast=True)
def pytest_runtest_call(item):
    CALLS["n"] += 1
    if CALLS["n"] == 2:
        pytest.exit("stopped during the run", returncode=1)
"""


# A tree with no failing teardown, for the truncation test: an interruption while a fixture with a
# failing finalizer is pending makes pytest's OWN sessionfinish raise during fixture teardown, which
# would measure pytest's behaviour rather than this plugin's.
TREE_PLAIN = """\
def test_one():
    pass


def test_two():
    pass


def test_three():
    pass
"""


# A completed run whose second test never reaches its call phase. A marker skip produces a setup
# report and no call row, which is the shape that made a row-count comparison mislabel a green run
# as truncated.
TREE_SKIP = """\
import pytest


def test_ok():
    pass


@pytest.mark.skip(reason="deliberately skipped")
def test_skipped():
    pass
"""

# The same shape with a setup ERROR instead of a skip: the fixture raises before the test's call
# phase, so again there is no call row for a test the run did reach.
TREE_SETUP_ERROR = """\
import pytest


@pytest.fixture
def broken():
    raise RuntimeError("fixture failed at setup")


def test_ok():
    pass


def test_uses_broken(broken):
    pass
"""


def _tree(pytester, source: str = TREE) -> None:
    """Write the inner tree: a plain `tests` directory, the given tests, and the inner conftest."""
    pytester.mkdir("tests")  # a plain directory: the outer session already holds a `tests` package
    pytester.path.joinpath("tests", "test_pair.py").write_text(source, encoding="utf-8")
    pytester.makeconftest(INNER_CONFTEST)


def _record_at(path):
    """The parsed record at `path`, failing with what is there instead when it is not JSON."""
    assert path.is_file(), f"no record written at {path}"
    return json.loads(path.read_text(encoding="utf-8"))


def test_a_normal_run_writes_the_whole_record_and_one_row_per_call(pytester):
    """Property: a completed run's record carries the eight fields, the four invocation facts, a row per CALL phase, and the two derived sets partitioning the population.

    Class (input): every completed run -- here a tree of four tests where one is deselected, one
    fails at call and one fails in a fixture's teardown; artifact: `record.pytest_collection_finish`
    and `record.pytest_runtest_makereport`. Build class: NEW CAPABILITY -- the plausible wrong
    implementation records a row per PHASE, which puts a second row under the teardown test's node
    id; a second wrong implementation derives the exclusion from the session's items instead of from
    the inventory's pre-deselection capture, which reports an empty exclusion for a run that
    deselected a test. This test is red under both. Why: a consumer reads the exclusion to learn what
    a run did not measure, and reads the rows to attribute an outcome to a test, so a doubled row or
    an empty exclusion makes the record describe a different run than the one that happened.
    Both-states anchor: the assertions turn on set relations among values the run itself produced and
    on the call-phase outcome pytest reports, not on the field names this change introduces.
    Invocation isolation: not isolatable from this tree -- discrimination shown instead: writing rows
    for every phase reds the row set, and taking the exclusion from `session.items` reds the
    partition. Killed by: a row per phase; an exclusion of the wrong members; a verdict taken from
    the rows rather than from the session's failures; or an `environment` naming anything but the
    imported package's file. Second correct implementation: the same object built in one place and
    serialized once at session end by a run that never dies, provided the marker still reads
    `complete` and the rows are the call phases only.
    """
    _tree(pytester)
    path = pytester.path / "record.json"
    result = pytester.runpytest(*INNER_PLUGINS, "--result-record", str(path), "-k", "not test_gamma", "tests")
    # Two passes: the teardown test's CALL phase passes, so pytest counts the test as passed and the
    # teardown assertion as an error -- which is exactly the pair the rows must not confuse.
    result.assert_outcomes(passed=2, failed=1, errors=1)

    written = _record_at(path)

    # The object's shape: the eight declared fields, and exactly the two keys outside them.
    assert set(record_schema.FIELDS) <= set(written), sorted(set(record_schema.FIELDS) - set(written))
    assert set(written) - set(record_schema.FIELDS) == {"completeness", "tests"}, sorted(written)
    assert written["completeness"] == "complete"
    assert written["subject_resolution"] == {"state": "unresolved"}
    assert written["verdict"] == "fail"  # a failing test and an error, so a pass here would be a lie

    # The four derived run-facts. The two digests are over the two node-id lists, so they differ when
    # the lists do.
    invocation = written["invocation"]
    assert set(invocation) == {"rootdir", "collection_scope", "population_digest", "surviving_digest"}, invocation
    assert invocation["rootdir"] == written["declared_context"]
    assert Path(invocation["rootdir"]).resolve() == pytester.path.resolve(), invocation["rootdir"]
    assert DIGEST.match(invocation["population_digest"]), invocation["population_digest"]
    assert DIGEST.match(invocation["surviving_digest"]), invocation["surviving_digest"]
    assert invocation["population_digest"] != invocation["surviving_digest"]

    # `environment` is the resolved package path, not a version or an interpreter.
    assert written["environment"] == subject._package_file()
    assert written["environment"] and written["environment"].endswith("multidriver_swg/__init__.py")

    # The derived sets partition the population, and the deselected test is the exclusion.
    population, surviving, excluded = written["population"], written["surviving"], written["exclusion"]
    assert set(excluded).isdisjoint(surviving)
    assert set(surviving) | set(excluded) == set(population)
    assert len(population) == 4, population
    assert all(nodeid.endswith("::test_gamma") for nodeid in excluded), excluded
    assert not any(nodeid.endswith("::test_gamma") for nodeid in surviving), surviving

    # One row per surviving test, call phase only: the teardown test's row says `passed` while the
    # session counted its teardown failure, which is the whole point of the call-phase restriction.
    rows = written["tests"]
    assert sorted(row["nodeid"] for row in rows) == sorted(surviving), rows
    assert len(rows) == len(surviving), rows
    teardown = [row for row in rows if row["nodeid"].endswith("::test_teardown_fails")]
    assert [row["outcome"] for row in teardown] == ["passed"], teardown
    assert {row["outcome"] for row in rows} == {"passed", "failed"}, rows
    assert all(isinstance(row["duration"], float) for row in rows), rows

    # No selection expression is recorded anywhere beside the derived exclusion set.
    assert "test_gamma" not in json.dumps(invocation), invocation
    assert "-k" not in json.dumps(invocation), invocation


def test_a_run_killed_during_collection_reads_collected_and_never_complete(pytester, monkeypatch):
    """Property: a session ended from a later collection hook leaves the marker at `collected` and a NON-empty population.

    Class (input): any collection that ends after the population write and before the run -- the
    `pytest.exit` in a `trylast` modifyitems hook driven here, and by the same call path an interrupt
    or a `--maxfail` stop in a later hook; artifact: `record.pytest_collection_finish`, which pytest
    calls from the `finally` around collection. Build class: NEW CAPABILITY -- the plausible wrong
    implementation advances the marker to `complete` unconditionally in that hook, and then this run
    reads `complete` over a healthy-looking survivor list, because `session.items` already holds the
    survivors when the hook runs (measured). A second wrong implementation writes the record only at
    session end, and then this run leaves no record at all. This test is red under both. Why: the
    marker is the only thing separating a run that died from a run that finished, and a consumer that
    cannot tell them apart reads a killed run's partial results as results. Both-states anchor: the
    assertion turns on the marker's value after a real kill and on the population's length, both
    properties of the invocation rather than of the field names. Invocation isolation: not isolatable
    from this tree -- discrimination shown instead: deleting the in-flight-exception guard from
    `pytest_collection_finish` reds this test, because the marker then reads `complete`. Killed by:
    advancing the marker unconditionally in the finish hook; writing the record only at session end;
    or reporting an empty population because the capture hook was skipped. Second correct
    implementation: the marker advanced by a wrapper around collection that catches the exception
    itself, provided the record still carries the population and stops at `collected`.
    """
    _tree(pytester)
    pytester.path.joinpath("killer_mid.py").write_text(KILLER_MID, encoding="utf-8")
    monkeypatch.syspath_prepend(str(pytester.path))
    path = pytester.path / "record.json"
    result = pytester.runpytest(*INNER_PLUGINS, "-p", "killer_mid", "--result-record", str(path), "tests")
    assert result.ret == 1, result.outlines

    written = _record_at(path)
    assert written["completeness"] == "collected", written["completeness"]
    assert written["verdict"] == "VOID", written["verdict"]
    assert written["population"], "a killed run over a non-empty tree must still report its population"
    assert "surviving" not in written, "a run that never finished collecting cannot report survivors"
    assert "exclusion" not in written, sorted(written)


def test_a_run_truncated_after_collection_reads_collected_and_void(pytester, monkeypatch):
    """Property: a session that finishes collection and then stops, with survivors unexecuted, is written back to `collected` and `VOID`.

    Class (input): every run that completes collection and then stops early -- the `pytest.exit` from
    a call-phase hook driven here, an interrupt during the run, a `--maxfail` stop; artifact:
    `record.pytest_sessionfinish`, which compares the count of tests that produced ANY report against
    `surviving`. Build class: NEW CAPABILITY -- the plausible wrong implementation writes the verdict
    whenever the marker reads `complete`, and then this run reads `complete` over a survivor list it
    never reached; a second wrong implementation consults pytest's exit status instead of the reached
    count, which for this run is the same code an ordinary failing run produces. This test is red
    under both. Why: the marker is what tells a consumer whether the record's own account of the run
    is finished, and a truncated run recorded as complete is a record that overstates what it
    measured. Both-states anchor: the assertion turns on the relation between the record's survivor
    list and the tests the run reached, not on the values this change introduces. Invocation
    isolation: not isolatable from this tree -- discrimination shown instead: deleting the reached-count
    comparison reds this test, because the marker stays `complete` and a verdict is written. Killed
    by: writing the verdict whenever the marker is complete; consulting the exit status instead of the
    reached count; or leaving the marker at `complete` while withholding the verdict. Second correct
    implementation: the shortfall detected as reports arrive and compared at the end, provided the
    same two values are restored.
    """
    _tree(pytester, TREE_PLAIN)
    pytester.path.joinpath("killer_call.py").write_text(KILLER_CALL, encoding="utf-8")
    monkeypatch.syspath_prepend(str(pytester.path))
    path = pytester.path / "record.json"
    result = pytester.runpytest(*INNER_PLUGINS, "-p", "killer_call", "--result-record", str(path), "tests")
    assert result.ret == 1, result.outlines

    written = _record_at(path)
    assert written["completeness"] == "collected", written["completeness"]
    assert written["verdict"] == "VOID", written["verdict"]
    assert written["surviving"], "the run stopped after collection, so it has survivors"
    assert len(written["tests"]) < len(written["surviving"]), (written["tests"], written["surviving"])


def test_a_completed_run_with_a_skipped_test_keeps_its_verdict(pytester):
    """Property: a green run containing a marker-skipped test reads `complete` with a real verdict, and its call rows are fewer than its survivors.

    Class (input): every completed run containing a test that never reaches its call phase -- a
    marker skip, driven here, and by the same path an `xfail(run=False)`; artifact:
    `record.pytest_sessionfinish`, whose comparison must count tests the run REACHED rather than
    call-phase rows. Build class: NEW CAPABILITY -- the plausible wrong implementation compares the
    call-phase rows against `surviving`, and then this run reads `collected` + `VOID` (measured on
    that mechanism: rows 1, surviving 2, on a run pytest reports as `1 passed, 1 skipped`, exit 0).
    This test is red under it. Why: a mislabelled run is worse than an unlabelled one, because a
    consumer reading `VOID` discards a green run's result. Both-states anchor: the assertion turns on
    the marker and verdict of a run pytest itself reports as one pass and one skip, both properties
    of the invocation rather than of the counting mechanism. Invocation isolation: not isolatable
    from this tree -- discrimination shown instead: comparing rows instead of reached tests reds this
    test. Killed by: comparing call rows; counting only tests with a call report; or downgrading
    whenever any test skipped. Second correct implementation: counting reached tests through
    `pytest_runtest_logreport` into a config attribute, provided setup-phase reports are counted.
    """
    _tree(pytester, TREE_SKIP)
    path = pytester.path / "record.json"
    result = pytester.runpytest(*INNER_PLUGINS, "--result-record", str(path), "tests")
    result.assert_outcomes(passed=1, skipped=1)

    written = _record_at(path)
    assert written["completeness"] == "complete", written["completeness"]
    assert written["verdict"] == "pass", written["verdict"]
    # The rows stay call-phase only, so they are short of the survivors even though nothing truncated.
    assert len(written["tests"]) < len(written["surviving"]), (written["tests"], written["surviving"])


def test_a_completed_run_with_a_setup_error_keeps_its_verdict(pytester):
    """Property: a completed run whose fixture raises at setup is not downgraded; it reads `complete` with the failing verdict.

    Class (input): every completed run containing a test that errors before its call phase -- a
    fixture raising at setup, driven here; artifact: `record.pytest_sessionfinish`. Build class:
    NEW CAPABILITY -- the plausible wrong implementation counts call rows, finds one for two
    survivors, and writes the completed run back to `collected` + `VOID`, erasing an error the
    consumer needs to see. This test is red under it. Why: the record's job is to say what a run
    measured and what it did not; a setup error is a measurement of failure, not an absent
    measurement. Both-states anchor: the assertion turns on the marker and on the verdict pytest's
    own totals produce, not on the counting mechanism. Invocation isolation: not isolatable from
    this tree -- discrimination shown instead: comparing rows instead of reached tests reds this
    test. Killed by: comparing call rows; or treating an error as an unreached test. Second correct
    implementation: counting reached tests through `pytest_runtest_logreport`, provided a failed
    setup report counts as reached.
    """
    _tree(pytester, TREE_SETUP_ERROR)
    path = pytester.path / "record.json"
    result = pytester.runpytest(*INNER_PLUGINS, "--result-record", str(path), "tests")
    result.assert_outcomes(passed=1, errors=1)

    written = _record_at(path)
    assert written["completeness"] == "complete", written["completeness"]
    assert written["verdict"] == "fail", written["verdict"]
    assert len(written["tests"]) < len(written["surviving"]), (written["tests"], written["surviving"])


def test_a_run_killed_before_collection_reads_pre_collection_and_still_names_its_checkout(pytester, monkeypatch):
    """Property: a session ended at session start still leaves a record whose header and provisional verdict are written.

    Class (input): every invocation stopped before collection -- the `pytest.exit` at session start
    driven here, and by the same path `subject`'s own stop when the imported package is not this
    checkout's; artifact: `record.pytest_sessionstart`, which runs before any test exists. Build
    class: NEW CAPABILITY -- the plausible wrong implementation writes the record only at session
    end, and then a stopped run leaves nothing at all; a second wrong implementation writes the
    header at session start but the verdict only at the end, and then a stopped run carries no
    verdict. This test is red under both. Why: this is the run a consumer most needs to recognize --
    the checkout that produced no results -- and a record written only at the end is absent for
    exactly it. Both-states anchor: the assertion turns on which fields exist after a real kill, a
    property of the hook schedule rather than of the field names. Invocation isolation: not
    isolatable from this tree -- discrimination shown instead: deleting the session-start
    serialization reds this test, because no file is written. Killed by: writing the record only at
    session end; deferring the verdict to the end; or truncating without serializing at session
    start. Second correct implementation: the header written from a wrapper around session start,
    provided the four config-derived fields and the provisional verdict are all present after the
    stop.
    """
    _tree(pytester)
    pytester.path.joinpath("killer_start.py").write_text(KILLER_START, encoding="utf-8")
    monkeypatch.syspath_prepend(str(pytester.path))
    path = pytester.path / "record.json"
    result = pytester.runpytest(*INNER_PLUGINS, "-p", "killer_start", "--result-record", str(path), "tests")
    assert result.ret == 1, result.outlines

    written = _record_at(path)
    assert written["completeness"] == "pre-collection", written["completeness"]
    assert written["verdict"] == "VOID", written["verdict"]
    assert written["subject_resolution"] == {"state": "unresolved"}, written.get("subject_resolution")
    # The header is what makes a stopped run legible: the checkout it was reading is written before
    # anything can stop the session.
    assert written["declared_context"], sorted(written)
    assert written["invocation"]["rootdir"] == written["declared_context"], written["invocation"]
    assert "environment" in written, sorted(written)


def test_an_unwritable_record_path_is_a_usage_error_and_never_an_internal_one(pytester):
    """Property: a `--result-record` target under a directory that does not exist ends the run as a usage error naming the option, with no INTERNALERROR and no file.

    Class (input): every record path the invoker cannot write -- here a parent directory that does
    not exist, the case a mistyped path produces; artifact: `record._write`, whose first call is from
    a session hook. Build class: NEW CAPABILITY -- the plausible wrong implementation lets the
    `FileNotFoundError` escape, and pytest then reports an INTERNALERROR whose traceback points at
    this plugin and aborts the run before a single test is collected (measured: exit 3, 30
    `INTERNALERROR` lines, traceback through `record.py`). This test is red under it. Why: an
    invoker's mistyped path is the invoker's error, and a plugin that turns it into an internal error
    both hides the cause and refuses to run the suite. Both-states anchor: the assertion turns on the
    process's exit code and on whether the output names the option, both properties of any pytest
    invocation rather than of the values this change introduces. Invocation isolation: not
    isolatable from this tree -- discrimination shown instead: removing the `OSError` handling reds
    this test on the internal-error assertion. Killed by: letting the `OSError` escape; catching it
    and continuing without a record, which would silently ignore the option; or creating the parent
    directory instead, which turns a typo into a record the consumer never looks for. Second correct
    implementation: checking writability once at `pytest_configure` and raising the same usage error
    there, provided the exit code and the message match.
    """
    _tree(pytester, TREE_PLAIN)
    path = pytester.path / "no_such_dir" / "record.json"
    result = pytester.runpytest(*INNER_PLUGINS, "--result-record", str(path), "tests")
    out = "\n".join(result.outlines) + "\n" + "\n".join(result.stderr.lines)
    assert result.ret == 4, out
    assert "INTERNALERROR" not in out, out
    assert "--result-record: cannot write" in out, out
    assert not path.exists(), path


def test_no_option_writes_no_record(pytester):
    """Property: with no `--result-record` the run writes no file anywhere in its tree.

    Class (input): every run that does not ask for a record, which is every ordinary run of this
    suite; artifact: `record._path`, which the option's absence turns into None. Build class: NEW
    CAPABILITY -- the plausible wrong implementation defaults the option to a path in the checkout,
    which makes every run write into the tree it is testing. This test is red under it. Why: the
    record is opt-in, and a default path would put a per-run artifact into a tracked tree that no
    gate declared. Both-states anchor: the assertion turns on the absence of any file the plugin
    could have written, not on a name. Invocation isolation: not isolatable from this tree --
    discrimination shown instead: giving the option a default reds the directory listing below.
    Killed by: defaulting the option; or writing the record whenever the plugin is registered.
    Second correct implementation: the same opt-in check performed in each hook rather than at the
    start of each, provided nothing is written when the option is absent.
    """
    _tree(pytester)
    result = pytester.runpytest(*INNER_PLUGINS, "tests")
    result.assert_outcomes(passed=3, failed=1, errors=1)
    assert not list(pytester.path.rglob("*.json")), list(pytester.path.rglob("*.json"))
