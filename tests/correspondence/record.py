"""Emit one JSON object per run: the result record a consumer reads instead of inferring from output.

A run of this tree leaves no artifact saying what it ran over, so a consumer reading only pytest's
output cannot tell a complete run from one that deselected most of the tree or stopped before it
finished. This plugin writes that artifact when the invoker asks for it: one JSON object at the path
`--result-record` names, and no file at all when the option is absent.

The object is written in STAGES, one hook per field, because a run can die at any of them. A run
killed during collection must leave a record whose completeness marker reads `collected` and never
`complete`; a record written only at session end would be missing for exactly the runs a consumer
most needs to recognize. So `pytest_sessionfinish` writes the final verdict only for a run that
reached every surviving test -- and even that has an earlier write point (the provisional `VOID` at
session start) -- while a run TRUNCATED after collection is written back to `collected` and `VOID`,
because the set of tests that produced any report, compared against the survivors, is evidence that
the run did not finish.

Deliberately NOT a second declaration of the field names: this module writes the fields
`record_schema` declares and reads its keys, so a rename lands in one place. Deliberately NOT a new
option group: `--result-record` joins `correspondence proofs`, where the other two per-run records
already live, so an invoker finds all three together.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest

from tests.correspondence import inventory, record_schema, subject

# The record under construction, carried across the run's hooks. Not a record_schema key: that module
# declares what a RECORD carries, and this is where one particular run keeps its working copy.
STATE = pytest.StashKey[dict]()

# The distinct node ids the run REACHED, in any phase. Kept out of the record because it is the
# plugin's own evidence about the run rather than a field a consumer reads; it is what session finish
# compares against `surviving` to tell a truncated run from a completed one.
EXECUTED = pytest.StashKey[set]()


def add_options(parser) -> None:
    """Add the record's opt-in path; with no path given no record is written."""
    group = parser.getgroup("correspondence proofs")
    group.addoption("--result-record", default=None, help="write this run's result record as one JSON object")


def _path(config) -> Path | None:
    """The path this run passed for the record, or None, in which case no record is written."""
    value = config.getoption("result_record", None)
    return Path(value) if value else None


def _write(config) -> None:
    """Serialize the record to the option's path, truncating and rewriting the whole object.

    ONE object rather than a line stream because the fields arrive across five hook points and the
    completeness marker has to advance with them: a consumer calls `json.load` once and reads the
    marker beside the fields it describes.

    A path the invoker cannot write is THE INVOKER'S mistake, and it is reported as one. An uncaught
    `OSError` raised from a hook is an INTERNALERROR whose traceback points at this plugin and which
    aborts the run before a single test is collected; `pytest.UsageError` reaches the terminal
    instead as `ERROR: --result-record: cannot write {path}`, with exit 4 and the option named.
    """
    path = _path(config)
    if path is None:
        return
    try:
        path.write_text(json.dumps(config.stash[STATE], indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except OSError as exc:
        raise pytest.UsageError(f"--result-record: cannot write {path}") from exc


def _digest(nodeids) -> str:
    """The sha256 of the node ids, sorted and one per line, so two runs over one population agree."""
    text = "".join(f"{nodeid}\n" for nodeid in sorted(nodeids))
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def pytest_configure(config):
    """Write the four config-derived fields: declared context, rootdir, collection scope and environment.

    All four are settled before any test is collected, which is why they are written here rather than
    where the rest of the invocation is completed: a run that dies during collection still names the
    checkout it was reading.
    """
    if _path(config) is None:
        return
    config.stash[STATE] = {
        "invocation": {
            "rootdir": str(config.rootpath),
            "collection_scope": inventory.narrowing(config),
        },
        # The same resolution `subject` uses to decide whether the imported package is this
        # checkout's: reusing it rather than re-deriving it keeps one definition of "which code the
        # import loaded" in the tree.
        "environment": subject._package_file(),
        "declared_context": str(config.rootpath),
    }
    config.stash[EXECUTED] = set()


def pytest_sessionstart(session):
    """Write the subject placeholder, the provisional verdict and the starting marker, then truncate the file."""
    config = session.config
    if _path(config) is None:
        return
    record = config.stash[STATE]
    # A COPY, not the schema's constant: the record is serialized and could be mutated by a reader,
    # and the constant is shared by every run in the process.
    record["subject_resolution"] = dict(record_schema.SUBJECT_UNRESOLVED)
    record["verdict"] = "VOID"
    record["completeness"] = "pre-collection"
    _write(config)


def pytest_collection_modifyitems(session, config, items):
    """Write the population the inventory captured, and advance the marker to `collected`.

    A PLAIN hook, never `tryfirst`: `inventory`'s capture is tryfirst, so it is dispatched before this
    one whatever order the two plugins were registered in, and the population is already stashed when
    this reads it. The read is guarded because a tree can run this plugin without the inventory's,
    and an unguarded index would then end the session with an internal error.
    """
    if _path(config) is None:
        return
    population = config.stash.get(record_schema.POPULATION, None)
    if population is None:
        return
    record = config.stash[STATE]
    record["population"] = list(population)
    record["completeness"] = "collected"
    _write(config)


def pytest_collection_finish(session):
    """Write the survivors, the exclusion and the completed invocation, and advance the marker to `complete`.

    A PLAIN hook, so `inventory`'s tryfirst finish hook has already stashed the exclusion.

    Two guards, each for a case that would otherwise make the record lie. The stash reads are guarded
    because pytest calls this hook from the `finally` around collection, so it runs even when
    collection raised before anything was captured. And the whole hook returns while an exception is
    IN FLIGHT: measured on this tree, `sys.exc_info()` here names the `Exit` a killed collection is
    unwinding with, while `session.items` is already the survivors -- so without this guard a run that
    died during collection would be recorded as `complete` over a healthy-looking survivor list. A
    record left at `collected` is the honest report of such a run.
    """
    if sys.exc_info()[0] is not None:
        return
    config = session.config
    if _path(config) is None:
        return
    population = config.stash.get(record_schema.POPULATION, None)
    excluded = config.stash.get(record_schema.EXCLUDED, None)
    if population is None or excluded is None:
        return
    surviving = sorted(item.nodeid for item in session.items)
    record = config.stash[STATE]
    record["surviving"] = surviving
    record["exclusion"] = list(excluded)
    record["invocation"]["population_digest"] = _digest(population)
    record["invocation"]["surviving_digest"] = _digest(surviving)
    record["completeness"] = "complete"
    _write(config)


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item, call):
    """Tally each test the run REACHED, and append one row per finished CALL phase.

    The tally counts ANY phase -- setup, call or teardown -- because a test skipped by marker, or one
    whose fixture raises at setup, produces a setup report and no call report at all. A tally taken
    only on call reports would therefore read a COMPLETED run as truncated, which is the defect
    session finish's comparison must not inherit.

    The rows stay call-phase only. A teardown failure is a real failure and pytest counts it in the
    session's totals, but it is not a row about the test's own result, and recording both phases
    would put two rows under one node id. Every test is recorded, not only the marker-selected
    families, because the record describes the run rather than the family inventory.
    """
    report = yield
    config = item.config
    if _path(config) is None:
        return report
    executed = config.stash.get(EXECUTED, None)
    if executed is not None:
        executed.add(report.nodeid)
    if report.when != "call":
        return report
    record = config.stash.get(STATE, None)
    if record is None:
        return report
    record.setdefault("tests", []).append(
        {"nodeid": report.nodeid, "outcome": report.outcome, "duration": report.duration}
    )
    _write(config)
    return report


def pytest_sessionfinish(session, exitstatus):
    """Write the final verdict when the run reached every surviving test, and downgrade a truncated run.

    The completeness marker is the record's own account of how far the run got, and a run can stop
    AFTER collection -- a `pytest.exit` from a run-phase hook, an interrupt, a `--maxfail` stop --
    leaving the marker at `complete` over a survivor list only partly reached. The evidence is the
    set of distinct node ids that produced ANY report, compared against `surviving`. A shortfall
    means truncation, so the record is written back to `collected` and the provisional `VOID`
    instead of being given a verdict for a run that did not happen.

    Any-phase counting is load-bearing and is not interchangeable with the `tests` rows: a test
    skipped by marker, or one whose fixture raises at setup, has no call row but WAS reached, so a
    row-count comparison would relabel a completed run as truncated.

    pytest's exit status is deliberately NOT consulted: its meaning varies with the interruption's
    cause, while the reached-test count is a fact about this run.

    No field is written ONLY here: `collected` was written by the collection hook and `VOID` by
    session start, so this hook re-writes two values that already have an earlier source rather than
    introducing one whose only source is the run's end.
    """
    config = session.config
    if _path(config) is None:
        return
    record = config.stash.get(STATE, None)
    if record is None or record.get("completeness") != "complete":
        return
    executed = config.stash.get(EXECUTED, set())
    if len(executed) < len(record.get("surviving", [])):
        record["completeness"] = "collected"
        record["verdict"] = "VOID"
        _write(config)
        return
    record["verdict"] = "fail" if session.testsfailed else "pass"
    _write(config)
