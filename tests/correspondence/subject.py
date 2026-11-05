"""Establish which `multidriver_swg` a run exercises, and refuse the run when that cannot be established.

The toolkit is installed in editable mode, so `import multidriver_swg` resolves to whichever
checkout the environment was installed from, not necessarily the one the tests were collected
from. Run from a second checkout, such as a worktree, without `PYTHONPATH` naming its `src`, the
equivalence tests would compare the old code with the other checkout's new code and report
verdicts about code nobody in this checkout is changing. So at session start this module checks
that the imported package lies inside the collected checkout's root, and ends the session before
any test runs when it does not, or when the package cannot be imported at all.

The comparison behind that stop is containment of resolved paths, never a string prefix: a
checkout `…/proj` and a sibling `…/proj2` share a prefix, so a prefix test would accept the
sibling's package.

Alongside the stop, this module carries the resolution instrument whose findings the result
record writes into its `subject_resolution` field: the imported package's file is read in this
process and, again, in a clean child process that never loads the pytest harness, and each
reading is compared for EQUALITY -- never containment -- with the one file the declared context
yields, `{rootdir}/src/multidriver_swg/__init__.py`. A reading that crashed is `cannot-import`
and is never a disagreement; two readings disagree only when both returned files and the
resolved files differ. The session-start stop still issues the containment verdict until the
record integration lands; the instrument's findings are the closed vocabulary the completed
record carries.
"""

from __future__ import annotations

import datetime
import importlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

from tests.correspondence import record_schema

PACKAGE = "multidriver_swg"
EXPECTED_RELATIVE = Path("src") / PACKAGE / "__init__.py"

# The closed vocabularies this instrument writes come from the schema's single declaration.
# Unpacking, not indexing: a vocabulary change fails here loudly instead of mis-spelling a
# finding id or state somewhere downstream.
(_CANNOT_IMPORT, _RESOLVED_OUTSIDE, _READINGS_DISAGREE) = record_schema.SUBJECT_FINDING_IDS
(_STATE_UNRESOLVED, _STATE_RESOLVED, _STATE_VOID) = record_schema.SUBJECT_RESOLUTION_STATES

# The staged test, landed with the record integration, that makes the disagreement observation
# real: it runs this tree in a subprocess whose parent prepends the declared src at
# plugin-load time and freezes the wall clock, so the observation this mapping describes is
# produced under that freeze, not asserted. The node id is a contract -- the record names it,
# and the staged test must carry it.
STAGED_DISAGREEMENT_TEST = "tests/correspondence/test_subject.py::test_a_parent_only_path_stages_disagreement"

# The clean child reads the package file with importlib, json and sys only -- never pytest, so
# the reading cannot inherit the harness, and never isolated mode: a `-I` child discards
# PYTHONPATH, which would hide the stale-path failure this reading exists to catch rather than
# observe it. The child inherits the parent environment and runs with cwd=rootdir.
_CHILD_PROBE = (
    "import importlib, json, sys\n"
    "result = {'file': None, 'error': None, 'pytest_loaded': 'pytest' in sys.modules}\n"
    "try:\n"
    "    module = importlib.import_module('multidriver_swg')\n"
    "    result['file'] = getattr(module, '__file__', None)\n"
    "    if result['file'] is None:\n"
    "        result['error'] = {'type': 'NoFile', 'message': 'imported module has no __file__'}\n"
    "except Exception as exc:\n"
    "    result['error'] = {'type': type(exc).__name__, 'message': str(exc)}\n"
    "print(json.dumps(result))\n"
)
_CHILD_TIMEOUT_SECONDS = 60

# The named disagreeing condition of the file-identity design, stated in repository terms:
# which comparison would disagree, and the three mechanisms that realize it.
_DISAGREEMENT_CONDITION = (
    "a run whose declared context is checkout C while multidriver_swg resolves to a file other "
    "than C/src/multidriver_swg/__init__.py -- realized by a stale editable install pointing at "
    "another checkout, a PYTHONPATH naming a sibling worktree, or an environment installed "
    "from a second clone"
)


def _inside(package: Path, root: Path) -> bool:
    """Whether the resolved package file lies under the resolved root."""
    return package.is_relative_to(root)


def subject_findings(package_file, rootdir) -> list[str]:
    """Name the package file and the checkout root when the first does not lie inside the second, or is absent."""
    root = Path(rootdir).resolve()
    if package_file is None:
        return [f"the package {PACKAGE} cannot be imported, so the tests in {root} have no code to test"]
    package = Path(package_file).resolve()
    if _inside(package, root):
        return []
    return [
        f"the package {PACKAGE} resolves to {package} which is outside the checkout {root} that the tests were collected from"
    ]


def _read_in_process() -> dict:
    """Import the package in this process and report the file it loaded from, or the error that stopped the import.

    A module without `__file__` is a failed reading, not an agreement: its location is
    unestablished, and recording it as resolved would claim a comparison that never ran.
    """
    try:
        module = importlib.import_module(PACKAGE)
    except Exception as exc:  # any failure to import, not only ImportError: a broken __init__ raises whatever it raises
        return {"file": None, "error": {"type": type(exc).__name__, "message": str(exc)}}
    file = getattr(module, "__file__", None)
    if file is None:
        return {"file": None, "error": {"type": "NoFile", "message": "imported module has no __file__"}}
    return {"file": file, "error": None}


def _package_file() -> str | None:
    """The file the imported package was loaded from, or None when it cannot be imported or has no file."""
    return _read_in_process()["file"]


def _read_subprocess(rootdir) -> dict:
    """Take the second reading in a clean child that inherits this environment, never the pytest harness.

    A launch failure, a non-zero exit, malformed JSON, or a child that loaded pytest is a
    failed reading: an error value, never a fabricated file and never a disagreement -- a
    crashed channel and a disagreeing channel are distinct evidence, and the finding
    vocabulary keeps them distinct.
    """
    try:
        out = subprocess.run(
            [sys.executable, "-B", "-c", _CHILD_PROBE],
            cwd=rootdir,
            capture_output=True,
            text=True,
            timeout=_CHILD_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return {
            "file": None,
            "error": {"type": "TimeoutExpired", "message": f"child did not finish within {_CHILD_TIMEOUT_SECONDS}s"},
            "pytest_loaded": False,
        }
    except OSError as exc:
        return {"file": None, "error": {"type": "LaunchError", "message": str(exc)}, "pytest_loaded": False}
    if out.returncode != 0:
        return {
            "file": None,
            "error": {"type": "ChildExit", "message": f"child exited {out.returncode}: {out.stderr.strip()[:200]}"},
            "pytest_loaded": False,
        }
    try:
        payload = json.loads(out.stdout.strip().splitlines()[-1])
    except (json.JSONDecodeError, IndexError):
        return {
            "file": None,
            "error": {"type": "BadJson", "message": f"child output was not the promised JSON: {out.stdout[:200]!r}"},
            "pytest_loaded": False,
        }
    if not isinstance(payload, dict) or "file" not in payload or "error" not in payload:
        return {
            "file": None,
            "error": {"type": "BadJson", "message": f"child JSON lacked file/error keys: {out.stdout[:200]!r}"},
            "pytest_loaded": False,
        }
    if payload["pytest_loaded"]:
        return {
            "file": None,
            "error": {
                "type": "HarnessLoaded",
                "message": "the child process loaded pytest, so the reading is not independent",
            },
            "pytest_loaded": True,
        }
    file = payload["file"]
    error = payload["error"]
    if file is None and error is None:
        error = {"type": "NoFile", "message": "child module had no __file__ and reported no error"}
    return {"file": file, "error": error, "pytest_loaded": False}


def expected_package_file(rootdir) -> Path:
    """The only file that satisfies the subject: the declared context's own package file, resolved."""
    return Path(rootdir).resolve() / EXPECTED_RELATIVE


def evaluate_readings(in_process: dict, subprocess_reading: dict, rootdir) -> dict:
    """Compare two readings against the expected file and each other, emitting findings in stable order.

    Order is fixed: a `cannot-import` finding per failed reading, then a `resolved-outside`
    finding per successful reading whose resolved file is unequal to the expected file, then
    one `readings-disagree` finding when both readings succeeded and their resolved files
    differ. A failed reading never produces a disagreement, and two successful readings of
    the same outside file disagree with nothing.
    """
    expected = expected_package_file(rootdir)
    readings = {"in_process": in_process, "subprocess": subprocess_reading}
    findings = []
    resolved_files = {}
    for name in ("in_process", "subprocess"):
        reading = readings[name]
        file = reading.get("file")
        error = reading.get("error")
        if file is None or error is not None:
            detail = error["message"] if error else "no file and no error reported"
            findings.append(
                {
                    "id": _CANNOT_IMPORT,
                    "comparison": "import",
                    "reading": name,
                    "message": f"the {name} reading failed: {detail}",
                }
            )
            continue
        resolved = Path(file).resolve()
        resolved_files[name] = resolved
        if resolved != expected:
            findings.append(
                {
                    "id": _RESOLVED_OUTSIDE,
                    "comparison": "expected-file-equality",
                    "reading": name,
                    "message": f"the {name} reading resolved to {resolved}, not the declared source file {expected}",
                }
            )
    if len(resolved_files) == 2 and resolved_files["in_process"] != resolved_files["subprocess"]:
        findings.append(
            {
                "id": _READINGS_DISAGREE,
                "comparison": "two-readings-equality",
                "reading": "both",
                "message": (
                    f"the in-process reading {resolved_files['in_process']} and the subprocess reading "
                    f"{resolved_files['subprocess']} resolved different files"
                ),
            }
        )
    return {
        "state": _STATE_RESOLVED if not findings else _STATE_VOID,
        "expected_file": str(expected),
        "readings": readings,
        "findings": findings,
    }


def _today() -> str:
    """The observation date, read from the ambient clock.

    Under the staged freeze this reads 2026-11-03; restoring the real clock moves it to the
    run's own date. Reading the clock is what refuses a bare constant that merely prints the
    frozen date.
    """
    return datetime.date.today().isoformat()


def resolve_subject(rootdir) -> dict:
    """Take both readings and evaluate them; the result is the completed subject_resolution value.

    Besides the readings and findings, the value carries the disagreement condition a reader
    would falsify it against, and the observation describing the staged test that makes a real
    disagreement observable: its parent-only sys.path state, its frozen clock, the date that
    clock produced, and whether this resolution observed the disagreement itself.
    """
    resolution = evaluate_readings(_read_in_process(), _read_subprocess(rootdir), rootdir)
    resolution["disagreement_condition"] = _DISAGREEMENT_CONDITION
    resolution["observation"] = {
        "staged_test": STAGED_DISAGREEMENT_TEST,
        "sys_path": "parent-only",
        "clock": "frozen",
        "date": _today(),
        "observed_in_this_run": any(finding["id"] == _READINGS_DISAGREE for finding in resolution["findings"]),
    }
    return resolution


def _stop(message: str) -> None:
    """End the session before any test runs."""
    pytest.exit(message, returncode=4)


def pytest_sessionstart(session):
    """Resolve the subject in two processes, record the completed resolution, then stop on any finding.

    The completed reading replaces the record's placeholder BEFORE any stop, so a denied run
    leaves the evidence rather than only the placeholder. The emitter helper is imported
    here, not at module level, because `record` already imports this module and a module-level
    import would be a cycle. A clean resolution produces no verdict: the ordinary
    session-finish path still decides pass or fail.
    """
    resolution = resolve_subject(session.config.rootpath)
    from tests.correspondence import record  # function-local: `record` imports this module at its own module level

    record.write_subject_resolution(session.config, resolution)
    if resolution["findings"]:
        messages = "; ".join(finding["message"] for finding in resolution["findings"])
        _stop(messages + " (a run in a second checkout needs PYTHONPATH naming its src)")
