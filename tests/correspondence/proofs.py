"""Record, for each failing equivalence test, whether it failed at an assertion, and check the tracked proof records.

A test is worth keeping only if it can fail for the reason it claims to check. For an
equivalence test that reason is its own comparison: a test written before its new code proves
itself by failing at its assertion against the placeholder, and a test written afterwards proves
itself by failing at its assertion under a deliberate break of that code. Both are recorded by
hand, from facts this module's report hook supplies, in the owning system's own proof file,
`tests/correspondence/proofs/{system slug with hyphens as underscores}.toml`: one file per owning
system, because two systems' proof rows written into one shared file conflict when their changes
are merged.

The hook classifies each failure of a test carrying `equivalence` by its LAST traceback frame:
an assertion when the exception is an `AssertionError` raised in the test's own file or in the
shared comparison helper `tests/correspondence/compare.py` (under the session's root), and not
an assertion otherwise. The exception type alone does not decide it, because an `assert` inside
the new code raises the same `AssertionError` while proving nothing about the test.

The hook writes nothing unless a run asks: `--proof-record={path}` appends one JSON line per
classified failure, and `--run-record={path}` writes one JSON line per family test with its
outcome and whether `new_side` reached a registered callable. `load` and the command check the
tracked proof records' form.

A proof file starts `rule = "proof-record:1"` and a `statement`, then holds one `[[proof]]` table
per proven equivalence test, with these fields:

    family = "equivalence", unit = "{unit key}", part = "{part}" (optional),
    route = "authoring-red" | "mutation", site = "{path}:{line}", exception = "AssertionError",
    commit = "{40 hex}", and for route "mutation": mutation = "{the change made}", restored = true

`load` reads every file of the directory in name order and returns the union of their rows. It
refuses a file whose name is no owning system's, and a row whose test the inventory lists under a
different owning system than the file names, so a row filed under the wrong system is loud rather
than counted. A missing directory reads as zero rows.
"""

from __future__ import annotations

import contextlib
import sys
import tomllib
from pathlib import Path

import pytest

from tests.correspondence import binding, corpus_record, inventory, system_scope, write_root

REPO_ROOT = corpus_record.REPO_ROOT
FAMILIES = ("equivalence", "refactor_reminder")
HELPER = Path("tests/correspondence/compare.py")
ROUTES = ("authoring-red", "mutation")
RECORDS = Path("tests/correspondence/proofs")


def add_options(parser) -> None:
    """Add the two opt-in record paths; with neither given the hook writes nothing."""
    group = parser.getgroup("correspondence proofs")
    group.addoption("--proof-record", default=None, help="append one JSON line per classified equivalence failure")
    group.addoption("--run-record", default=None, help="write one JSON line per family test: outcome and binding")


# The three record channels, in the order the refusal below reports them, so a configuration with
# more than one collision names the same pair on every run. The result record is declared by
# `record.py`; `getoption` answers its default here when that plugin is not loaded, which is how one
# module can refuse a pair involving an option it does not declare.
RECORD_OPTIONS = ("proof_record", "run_record", "result_record")
RECORD_OPTION_PAIRS = (
    ("proof_record", "run_record"),
    ("proof_record", "result_record"),
    ("run_record", "result_record"),
)


def _flag(option: str) -> str:
    """The command-line spelling of a record option's name, so a message names the flag the invoker typed."""
    return "--" + option.replace("_", "-")


def pytest_configure(config):
    """Refuse a record configuration that cannot be honoured, before anything has been written.

    The three record options write differently -- the proof record is appended across runs, the run
    record is truncated at session start, and the result record rewrites its whole object at every
    hook point -- so two of them naming one path means one channel's write destroys another's
    record, and what survives depends on which hook ran first. A pair that resolves to one path is
    therefore refused here, which is the earliest hook that receives `config` and runs before any
    record write.

    `pytest.UsageError` is the channel rather than a bare exception: a raise from a hook without it
    reaches the terminal as an INTERNALERROR with a traceback, while this prints a named `ERROR:`
    line and ends the run before collection.

    Deliberately NOT `pytest_sessionstart`: that is the hook that performs the truncation, so
    check-before-truncate inside one function is one edit away from being wrong, and an
    earlier-registered `sessionstart` could already have written.
    """
    values = {option: config.getoption(option, None) for option in RECORD_OPTIONS}
    set_options = {option: value for option, value in values.items() if value}
    if not set_options:
        return
    resolved = {option: Path(value).resolve() for option, value in set_options.items()}
    for first, second in RECORD_OPTION_PAIRS:
        if first in resolved and second in resolved and resolved[first] == resolved[second]:
            raise pytest.UsageError(f"{_flag(first)} and {_flag(second)} name one resolved path: {resolved[first]}")
    if "proof_record" in resolved:
        admitted, reason = write_root.admitted(resolved["proof_record"])
        if not admitted:
            raise pytest.UsageError(f"--proof-record: {reason}")


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


def _append(path: Path, line: dict, option: str) -> None:
    """Append one JSON line to a record file, reporting an over-long line as the invoker's mistake.

    The write itself belongs to `write_root`, which owns the shared root; this function supplies the
    one thing that module cannot know -- which option's path was handed to it -- so a refusal can
    name the flag the invoker typed.

    Only `RecordLineTooLong` is translated. A `ShortRecordWrite` is deliberately left alone: it is an
    environment failure, and the usage channel would blame the invoker for a filesystem that
    accepted fewer bytes than it was given.
    """
    try:
        write_root.append_line(path, line)
    except write_root.RecordLineTooLong as exc:
        raise pytest.UsageError(
            f"{option}: {path}: record line {exc.length} bytes exceeds the {exc.bound}-byte bound"
        ) from exc


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
        _append(proof_path, {**_key(family, mark), **_classification(call.excinfo, item)}, "--proof-record")
    run_path = _record_path(item.config, "run_record")
    if run_path is not None and (report.when == "call" or (report.when == "setup" and not report.passed)):
        _append(run_path, {**_key(family, mark), "outcome": report.outcome, "bound": _bound(item)}, "--run-record")
    return report


def file_name(system: str) -> str:
    """The proof file of an owning system: its slug with hyphens as underscores, and `.toml`."""
    return f"{system.replace('-', '_')}.toml"


def _route_findings(where: str, n: int, row: dict) -> list[str]:
    """A row whose route is not one of the two ways a test is proven."""
    route = row.get("route")
    if route in ROUTES:
        return []
    return [f"{where}: proof row {n}: route {route!r} is not one of {', '.join(ROUTES)}"]


def _test_findings(where: str, n: int, row: dict, owners: dict) -> list[str]:
    """A row that names no test the inventory lists."""
    key = (row.get("family"), row.get("unit"), row.get("part"))
    if key in owners:
        return []
    name = ":".join(str(v) for v in key if v is not None)
    return [f"{where}: proof row {n}: {name} names no existing test"]


def _owner_findings(where: str, n: int, row: dict, owners: dict, system: str) -> list[str]:
    """A row whose test the inventory lists under other owning systems than the file's own.

    A row naming no listed test is left to `_test_findings`, so one row draws one finding.
    """
    key = (row.get("family"), row.get("unit"), row.get("part"))
    if key not in owners or system in owners[key]:
        return []
    name = ":".join(str(v) for v in key if v is not None)
    listed = ", ".join(sorted(owners[key]))
    return [f"{where}: proof row {n}: {name} is a test of {listed}, not of {system}"]


def load(root: Path = REPO_ROOT) -> tuple[list[dict], list[str]]:
    """Return the tracked proof rows of every owning system's file, in file-name order, and every finding.

    A file whose name is no owning system's proof file (`file_name` of a slug of the system-scope
    file) is a finding and its rows are still read, so a misnamed file cannot hide a bad row. A
    missing directory reads as zero rows.
    """
    directory = root / RECORDS
    if not directory.is_dir():
        return [], []
    entries = inventory.listing(root)[0]
    owners: dict[tuple, set[str]] = {}
    for entry in entries:
        owners.setdefault((entry["family"], entry["unit"], entry["part"]), set()).add(entry["system"])
    system_of = {file_name(slug): slug for slug in system_scope.slugs(root)}
    rows: list[dict] = []
    findings: list[str] = []
    for path in sorted(p for p in directory.iterdir() if p.is_file()):
        where = f"{RECORDS.as_posix()}/{path.name}"
        system = system_of.get(path.name)
        if system is None:
            findings.append(
                f"{where}: no owning system has this proof file name (expected `{{slug with underscores}}.toml`)"
            )
        file_rows = tomllib.loads(path.read_text(encoding="utf-8")).get("proof", [])
        rows += file_rows
        for n, row in enumerate(file_rows, 1):
            findings += _route_findings(where, n, row) + _test_findings(where, n, row, owners)
            if system is not None:
                findings += _owner_findings(where, n, row, owners, system)
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
