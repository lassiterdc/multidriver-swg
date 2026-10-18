"""Record, for each failing equivalence test, whether it failed at an assertion, and check the tracked proof records.

A test is worth keeping only if it can fail for the reason it claims to check. For an
equivalence test that reason is its own comparison: a test written before its new code proves
itself by failing at its assertion against the placeholder, and a test written afterwards proves
itself by failing at its assertion under a deliberate break of that code. Both are recorded by
hand in `tests/correspondence/proofs.toml`, from facts this module's report hook supplies.

The hook classifies each failure of a test carrying `equivalence` by its LAST traceback frame:
an assertion when the exception is an `AssertionError` raised in the test's own file or in the
shared comparison helper `tests/correspondence/compare.py` (under the session's root), and not
an assertion otherwise. The exception type alone does not decide it, because an `assert` inside
the new code raises the same `AssertionError` while proving nothing about the test.

The hook writes nothing unless a run asks: `--proof-record={path}` appends one JSON line per
classified failure, and `--run-record={path}` writes one JSON line per family test with its
outcome and whether `new_side` reached a registered callable. `load` and the command check the
tracked proof records' form.
"""

from __future__ import annotations

import contextlib
import json
import sys
import tomllib
from pathlib import Path

import pytest

from tests.correspondence import binding, corpus_record, inventory

REPO_ROOT = corpus_record.REPO_ROOT
FAMILIES = ("equivalence", "refactor_reminder")
HELPER = Path("tests/correspondence/compare.py")
ROUTES = ("authoring-red", "mutation")
RECORDS = Path("tests/correspondence/proofs.toml")


def add_options(parser) -> None:
    """Add the two opt-in record paths; with neither given the hook writes nothing."""
    group = parser.getgroup("correspondence proofs")
    group.addoption("--proof-record", default=None, help="append one JSON line per classified equivalence failure")
    group.addoption("--run-record", default=None, help="write one JSON line per family test: outcome and binding")


def _family(item):
    """The test's family marker and its family name, or (None, None) for a test of no family."""
    for family in FAMILIES:
        mark = item.get_closest_marker(family)
        if mark is not None:
            return family, mark
    return None, None


def _key(family: str, mark) -> dict:
    """The (family, unit, part) a record is keyed by."""
    return {"family": family, "unit": mark.kwargs.get("unit"), "part": mark.kwargs.get("part")}


def _in_test_file(path: Path, item) -> bool:
    """Whether a traceback frame lies in the test's own file."""
    return path.resolve() == Path(item.path).resolve()


def _in_helper(path: Path, config) -> bool:
    """Whether a traceback frame lies in the shared comparison helper under the session's root."""
    return path.resolve() == (config.rootpath / HELPER).resolve()


def _is_assertion(excinfo, item) -> bool:
    """An `AssertionError` whose last frame is in the test's own file or in the comparison helper."""
    path = Path(excinfo.traceback[-1].path)
    return excinfo.errisinstance(AssertionError) and (_in_test_file(path, item) or _in_helper(path, item.config))


def _site(excinfo, config) -> str:
    """`{path}:{line}` of the last traceback frame, the path relative to the session's root where it lies under it."""
    entry = excinfo.traceback[-1]
    path = Path(entry.path)
    with contextlib.suppress(ValueError):  # a frame outside the root keeps its absolute path
        path = path.resolve().relative_to(config.rootpath.resolve())
    return f"{path.as_posix()}:{entry.lineno + 1}"


def _record_path(config, option: str) -> Path | None:
    """The path a run passed for one record, or None, in which case that record is not written."""
    value = config.getoption(option, None)
    return Path(value) if value else None


def _bound(item) -> bool:
    """Whether the test's `new_side` reached a registered callable; False when it did not run."""
    return item.stash.get(binding.BOUND, False)


def _append(path: Path, line: dict) -> None:
    """Append one JSON line to a record file."""
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(line, sort_keys=True) + "\n")


def pytest_sessionstart(session):
    """Start the run record empty, because it describes one run; the proof record is appended to across runs."""
    path = _record_path(session.config, "run_record")
    if path is not None:
        path.write_text("", encoding="utf-8")


def _classification(excinfo, item) -> dict:
    """Whether a failed call is an assertion, its site and its exception type.

    A call can fail with no exception at all — a test marked `xfail(strict=True)` that passes — and
    then there is no frame to read: it is recorded as not an assertion, with no site and no type,
    rather than read, which would end the whole session with an internal error.
    """
    if excinfo is None:
        return {"assertion": False, "site": None, "exception": None}
    return {
        "assertion": _is_assertion(excinfo, item),
        "site": _site(excinfo, item.config),
        "exception": excinfo.typename,
    }


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item, call):
    """Classify each failing equivalence test's call, and record each family test's outcome, when asked to."""
    report = yield
    family, mark = _family(item)
    if family is None:
        return report
    proof_path = _record_path(item.config, "proof_record")
    if proof_path is not None and family == "equivalence" and report.when == "call" and report.failed:
        _append(proof_path, {**_key(family, mark), **_classification(call.excinfo, item)})
    run_path = _record_path(item.config, "run_record")
    if run_path is not None and (report.when == "call" or (report.when == "setup" and not report.passed)):
        _append(run_path, {**_key(family, mark), "outcome": report.outcome, "bound": _bound(item)})
    return report


def _route_findings(n: int, row: dict) -> list[str]:
    """A row whose route is not one of the two ways a test is proven."""
    route = row.get("route")
    if route in ROUTES:
        return []
    return [f"proof row {n}: route {route!r} is not one of {', '.join(ROUTES)}"]


def _test_findings(n: int, row: dict, tests: set) -> list[str]:
    """A row that names no test the inventory lists."""
    key = (row.get("family"), row.get("unit"), row.get("part"))
    if key in tests:
        return []
    name = ":".join(str(v) for v in key if v is not None)
    return [f"proof row {n}: {name} names no existing test"]


def load(root: Path = REPO_ROOT) -> tuple[list[dict], list[str]]:
    """Return the tracked proof rows and every row that breaks their form."""
    data = tomllib.loads((root / RECORDS).read_text(encoding="utf-8"))
    rows = data.get("proof", [])
    tests = {(e["family"], e["unit"], e["part"]) for e in inventory.listing(root)[0]}
    findings = []
    for n, row in enumerate(rows, 1):
        findings += _route_findings(n, row) + _test_findings(n, row, tests)
    return rows, findings


def main(root: Path = REPO_ROOT) -> int:
    """Print the number of proof rows; return 1, naming each, when a row breaks the form."""
    rows, findings = load(root)
    print(f"proof records: {len(rows)} rows")
    for finding in findings:
        print(f"  {finding}", file=sys.stderr)
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
