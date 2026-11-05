"""Tests of the subject-location check: the session runs only against this checkout's `multidriver_swg`.

The containment test is a pure call of `subject_findings`. The stop test runs the real tree in a
subprocess from the repository root with `PYTHONPATH` naming a synthetic `multidriver_swg` under
the test's temporary directory instead of the checkout's `src`, so the child's import resolves
outside the checkout; the child is restricted with `-k` to the containment test, which needs no
package at all, so it can never select the test that started it. The named check asserts the
same verdict for the session it runs in, so a run reports it by name.
"""

import datetime
import json
import os
import subprocess
import sys
import types

import pytest

from tests.correspondence import corpus_record, record_schema, subject

pytestmark = pytest.mark.state_check

CONTAINMENT = "test_the_package_must_lie_inside_the_checkout_not_beside_it"


def test_the_package_under_test_resolves_inside_this_checkout(request):
    """Property: the `multidriver_swg` this session imports lies inside the checkout the tests were collected from.

    Class: every run of the suite. Why: the session-start stop ends a wrong-subject run before any
    test, and this test states the same verdict by name in a run's report. Killed by: a session
    whose package resolves outside its root, which the stop ends first unless it is disabled.
    Second correct implementation: comparing the package's directory with the root's `src`
    directory instead of the package's file with the root.
    """
    findings = subject.subject_findings(subject._package_file(), request.config.rootpath)
    assert not findings, "\n".join(findings)


def test_the_package_must_lie_inside_the_checkout_not_beside_it(tmp_path):
    """Property: a package file in a sibling checkout whose name extends the root's is outside it, named with both paths.

    Class: any package path that shares the root's string prefix without lying under it, here
    `{tmp}/proj2/...` against the root `{tmp}/proj`. Why: a checkout and a sibling whose name
    extends it share a prefix, so a prefix comparison would accept the sibling's code as the code
    under test. Killed by: comparing by string prefix. Second correct implementation: comparing
    the parts of the two resolved paths instead of calling `is_relative_to`.
    """
    root = tmp_path / "proj"
    package = tmp_path / "proj2" / "src" / "multidriver_swg" / "__init__.py"
    root.mkdir()
    package.parent.mkdir(parents=True)
    package.write_text("")
    findings = subject.subject_findings(package, root)
    tokens = " ".join(findings).split()
    assert str(package.resolve()) in tokens and str(root.resolve()) in tokens, findings
    assert subject.subject_findings(root / "src" / "multidriver_swg" / "__init__.py", root) == []


def _child_with_package(tmp_path, name, init_text):
    """Run the real tree from the root with a synthetic package `{tmp}/{name}/src/multidriver_swg` first on the path."""
    root = corpus_record.REPO_ROOT
    package = tmp_path / name / "src" / "multidriver_swg" / "__init__.py"
    package.parent.mkdir(parents=True)
    package.write_text(init_text)
    path = [str(tmp_path / name / "src"), str(root), os.environ.get("PYTHONPATH", "")]
    out = subprocess.run(
        [sys.executable, "-B", "-m", "pytest", "-p", "no:cacheprovider", "-q", "-k", CONTAINMENT],
        cwd=root,
        env=dict(os.environ, PYTHONPATH=os.pathsep.join(path)),
        capture_output=True,
        text=True,
    )
    return package, root, out


def test_a_package_from_outside_the_checkout_stops_the_session(tmp_path):
    """Property: a session whose `multidriver_swg` is not this checkout's importable copy ends before any test.

    Class: any run whose import path puts another copy of the package first — here a synthetic
    copy under the test's temporary directory, which must stop the session naming both paths; and
    a copy whose `__init__` raises while it is imported (a `RuntimeError` here; a syntax error left
    mid-edit is the everyday case), which must stop it with the check's own exit code 4, naming
    the failure, rather than end it as pytest's internal error. Why: a run that only warned would
    go on to report equivalence verdicts about code nobody in this checkout is changing, and an
    internal error reads as a failure of the test runner rather than of the code under test.
    Killed by: warning instead of stopping; and by treating only `ImportError` as a failed import.
    Second correct implementation: raising a usage error from the configuration hook instead of
    exiting at session start, with a broad exception handler around the import there.
    """
    package, root, out = _child_with_package(
        tmp_path, "outside", '"""A synthetic copy of the package, outside the checkout."""\n'
    )
    combined = out.stdout + out.stderr
    assert out.returncode != 0, combined
    assert str(package.resolve()) in combined and str(root) in combined, combined
    _package, root, out = _child_with_package(
        tmp_path, "broken", 'raise RuntimeError("a package that breaks while importing")\n'
    )
    assert out.returncode == 4, (out.returncode, out.stdout + out.stderr)
    assert "breaks while importing" in (out.stdout + out.stderr), out.stdout + out.stderr


def _reading(file=None, error=None):
    return {"file": file, "error": error}


def _checkout(tmp_path):
    return tmp_path / "checkout"


def _package_file_under(root):
    return root / "src" / "multidriver_swg" / "__init__.py"


def test_the_expected_file_is_the_declared_source_file(tmp_path):
    """Property: the only satisfying subject file is `{rootdir}/src/multidriver_swg/__init__.py`, resolved.

    Class: any declared context root, here a bare temporary directory that need not exist --
    the expectation is a pure path derivation. Build
    class: NEW CAPABILITY -- the plausible wrong implementation returns the root's `src`
    directory (a container, never loadable as the package file) or the unresolved join (so a
    symlinked root never matches the resolved reading). Red under both. Why: every comparison
    the instrument makes is equality against this file, so a wrong expected file makes every
    verdict wrong in the same direction. Killed by: returning `src`, returning an unresolved
    path, or accepting any file below the root. Second correct implementation: building the
    path from `Path(rootdir, "src", "multidriver_swg", "__init__.py")` in one call instead of
    a shared EXPECTED_RELATIVE constant.
    """
    root = _checkout(tmp_path)
    assert subject.expected_package_file(root) == _package_file_under(root).resolve()


def test_equal_readings_of_the_expected_file_resolve_with_no_findings(tmp_path):
    """Property: two successful readings of the expected file resolve the subject with an empty finding set.

    Class: the agreeing case any healthy run of this checkout produces. Build
    class: NEW CAPABILITY -- the plausible wrong implementation emits `state: "resolved"` only
    when the two files are equal as strings without resolving them, so a symlinked reading
    reads void in a healthy run. Red under that. Why: `resolved` is the state a reader
    branches on to trust every other field, so it must require exactly the no-finding
    condition. Killed by: keying `resolved` on one reading, or emitting a finding for a
    difference of spelling rather than of resolved path. Second correct implementation:
    computing state from `not findings` after building the list, as this implementation does,
    versus a flag threaded through the branches.
    """
    root = _checkout(tmp_path)
    expected = subject.expected_package_file(root)
    resolution = subject.evaluate_readings(_reading(file=str(expected)), _reading(file=str(expected)), root)
    assert resolution["state"] == "resolved"
    assert resolution["expected_file"] == str(expected)
    assert resolution["findings"] == []


def test_a_nested_checkout_below_the_root_is_resolved_outside(tmp_path):
    """Property: a package file below the declared root at any other location is `resolved-outside`, named with both operands.

    Class: a nested checkout under the declared root -- here `{root}/nested/src/multidriver_swg/__init__.py`
    against root `{root}` -- which containment would accept. Build
    class: NEW CAPABILITY -- the plausible wrong implementation narrows acceptance to the
    root's `src` directory, which still accepts the nested checkout. Red under that. Why: a
    nested checkout is the declared context of a DIFFERENT run, and its package file fails
    equality with the expected file even though containment passes. Killed by: comparing
    against the root or its `src` instead of the exact file. Second correct implementation:
    comparing the reading's resolved path part-by-part against the expected path's parts.
    """
    root = _checkout(tmp_path)
    nested = root / "nested"
    resolution = subject.evaluate_readings(
        _reading(file=str(_package_file_under(nested))),
        _reading(file=str(_package_file_under(nested))),
        root,
    )
    findings = resolution["findings"]
    assert [f["id"] for f in findings] == ["resolved-outside", "resolved-outside"]
    assert {f["reading"] for f in findings} == {"in_process", "subprocess"}
    nested_finding = next(f for f in findings if f["reading"] == "in_process")
    assert nested_finding["comparison"] == "expected-file-equality"
    assert str(_package_file_under(nested).resolve()) in nested_finding["message"]
    assert str(subject.expected_package_file(root)) in nested_finding["message"]


def test_a_sibling_prefix_path_is_resolved_outside_not_accepted(tmp_path):
    """Property: a package file in a sibling whose name extends the root's is `resolved-outside`, never accepted by prefix.

    Class: any package path that shares the root's string prefix without being the expected
    file, here `{tmp}/checkout2/src/...` against root `{tmp}/checkout`. Build
    class: NEW CAPABILITY -- the plausible wrong implementation compares common string
    prefixes, which accepts the sibling. Red under that. Why: the sibling prefix is exactly
    the case a prefix comparison cannot reject and equality rejects trivially, so it is the
    discriminating input for the acceptance relation. Killed by: accepting on prefix or on
    containment in the root. Second correct implementation: comparing the two resolved paths
    with os.path.samefile where both exist -- path-equality is preferred because it needs no
    filesystem and cannot raise on a missing operand.
    """
    root = _checkout(tmp_path)
    sibling = tmp_path / "checkout2"
    resolution = subject.evaluate_readings(
        _reading(file=str(_package_file_under(sibling))),
        _reading(file=str(_package_file_under(sibling))),
        root,
    )
    findings = resolution["findings"]
    assert [f["id"] for f in findings] == ["resolved-outside", "resolved-outside"]
    assert {f["reading"] for f in findings} == {"in_process", "subprocess"}
    assert all(f["id"] != "readings-disagree" for f in findings)


def test_two_equal_outside_files_yield_resolved_outside_without_disagreement(tmp_path):
    """Property: two successful readings of the SAME outside file produce one `resolved-outside` per reading and no `readings-disagree`.

    Class: both processes importing the same wrong copy -- the shared-environment case the
    disagreement condition names. Build
    class: NEW CAPABILITY -- the plausible wrong implementation emits `readings-disagree`
    whenever the files differ from the expected file rather than from each other. Red under
    that. Why: agreement between the readings is real evidence about the environment even
    when both agree on the wrong file; calling it disagreement would send the reader to the
    wrong remedy. Killed by: keying `readings-disagree` on inequality with the expected file.
    Second correct implementation: comparing the two resolved paths for equality before
    deciding, which is the same test stated once.
    """
    root = _checkout(tmp_path)
    outside = tmp_path / "shared-env" / "src" / "multidriver_swg" / "__init__.py"
    resolution = subject.evaluate_readings(_reading(file=str(outside)), _reading(file=str(outside)), root)
    findings = resolution["findings"]
    assert [f["id"] for f in findings] == ["resolved-outside", "resolved-outside"]
    assert {f["reading"] for f in findings} == {"in_process", "subprocess"}
    assert all(f["id"] != "readings-disagree" for f in findings)


def test_two_unequal_files_yield_readings_disagree_after_the_outside_findings(tmp_path):
    """Property: two successful unequal readings emit one `readings-disagree` after the per-reading `resolved-outside` findings, naming both files.

    Class: the two-process split the instrument exists to catch -- here the parent reading
    the expected file and the child reading an outside file. Build
    class: NEW CAPABILITY -- the plausible wrong implementation emits `readings-disagree`
    before the per-reading findings, or only when neither file equals the expected file. Red
    under both. Why: the finding order is part of the contract a reader scans, and
    disagreement must survive even when exactly one reading is the expected file. Killed by:
    reordering the findings, or withholding `readings-disagree` when one side is expected.
    Second correct implementation: emitting the disagreement finding from a separate pass
    over the two resolved files after the per-reading pass.
    """
    root = _checkout(tmp_path)
    expected = subject.expected_package_file(root)
    outside = tmp_path / "elsewhere" / "src" / "multidriver_swg" / "__init__.py"
    resolution = subject.evaluate_readings(_reading(file=str(expected)), _reading(file=str(outside)), root)
    findings = resolution["findings"]
    assert [f["id"] for f in findings] == ["resolved-outside", "readings-disagree"]
    assert findings[1]["reading"] == "both"
    assert findings[1]["comparison"] == "two-readings-equality"
    assert str(expected) in findings[1]["message"]
    assert str(outside.resolve()) in findings[1]["message"]


def test_an_import_exception_in_either_channel_is_cannot_import_and_never_disagreement(tmp_path):
    """Property: an import exception in the in-process channel or the subprocess channel yields `cannot-import` for that channel only, and no `readings-disagree`.

    Class: any reading whose import raised -- a syntax error left mid-edit, a broken
    `__init__`, here `SyntaxError` and `RuntimeError` standing for the class. Build
    class: NEW CAPABILITY -- the plausible wrong implementation treats a failed reading as
    `readings-disagree` (the channels "differ") or reports only `ImportError` as a failure.
    Red under both. Why: a crash is not a disagreement -- the channels produced one file
    between them -- and a broken package raises whatever it raises, so only the broad handler
    classifies it. Killed by: mapping a failed reading to disagreement, or catching only
    ImportError. Second correct implementation: a reading type with a discriminated error
    field instead of the file/error pair.
    """
    root = _checkout(tmp_path)
    expected = subject.expected_package_file(root)
    for failing in ("in_process", "subprocess"):
        readings = {
            "in_process": _reading(file=str(expected)),
            "subprocess": _reading(file=str(expected)),
        }
        readings[failing] = _reading(file=None, error={"type": "SyntaxError", "message": "invalid syntax"})
        resolution = subject.evaluate_readings(readings["in_process"], readings["subprocess"], root)
        assert [f["id"] for f in resolution["findings"]] == ["cannot-import"], failing
        assert resolution["findings"][0]["reading"] == failing
        assert resolution["findings"][0]["comparison"] == "import"
        assert "invalid syntax" in resolution["findings"][0]["message"]


def test_a_module_without_file_is_cannot_import_not_agreement(tmp_path):
    """Property: a reading whose import succeeded but produced no `__file__` is `cannot-import`, never `resolved` and never `readings-disagree`.

    Class: loaders with no module file -- a zip-loaded or frozen package -- the loaders the
    file-identity design's flip condition names. Build
    class: NEW CAPABILITY -- the plausible wrong implementation reports the file as None and
    proceeds, which either crashes the equality comparison or reads as agreement. Red under
    both. Why: a location that cannot be established must read as `cannot-import`, so the
    record carries the subject as unestablished rather than as agreement. Killed by: passing
    a None file into the comparison, or emitting no finding for it. Second correct
    implementation: rejecting the reading before evaluation, in the reader, with the same
    NoFile error this reader produces.
    """
    root = _checkout(tmp_path)
    expected = subject.expected_package_file(root)
    resolution = subject.evaluate_readings(
        _reading(file=None, error={"type": "NoFile", "message": "imported module has no __file__"}),
        _reading(file=str(expected)),
        root,
    )
    assert [f["id"] for f in resolution["findings"]] == ["cannot-import"]
    assert resolution["findings"][0]["reading"] == "in_process"


def test_two_failed_readings_yield_cannot_import_only(tmp_path):
    """Property: when both readings failed, the findings are exactly one `cannot-import` per channel, in channel order, with no `readings-disagree`.

    Class: the doubly-broken environment -- a package whose import raises in every process.
    Build
    class: NEW CAPABILITY -- the plausible wrong implementation emits a disagreement finding
    on top of the two import findings because the two error dicts differ. Red under that.
    Why: with zero successful files there is nothing to disagree about, and the both-failed
    case is the one a reader most needs kept distinct from disagreement, since its remedy is
    "fix the package", not "fix the environment". Killed by: any `readings-disagree` emission
    when fewer than two files resolved. Second correct implementation: gating the
    disagreement branch on a two-entry resolved-files map, as this implementation does.
    """
    root = _checkout(tmp_path)
    broken = {"type": "RuntimeError", "message": "boom"}
    resolution = subject.evaluate_readings(_reading(file=None, error=broken), _reading(file=None, error=broken), root)
    findings = resolution["findings"]
    assert [f["id"] for f in findings] == ["cannot-import", "cannot-import"]
    assert [f["reading"] for f in findings] == ["in_process", "subprocess"]
    assert resolution["state"] == "void"


def test_every_emitted_id_and_state_comes_from_the_closed_vocabularies(tmp_path):
    """Property: every finding id the evaluator can emit is a member of record_schema.SUBJECT_FINDING_IDS, and every state is a member of record_schema.SUBJECT_RESOLUTION_STATES.

    Class: the whole emission surface of `evaluate_readings` across the finding-producing
    scenarios -- failed readings, outside files, disagreement -- enumerated in this test's
    scenario list. Build
    class: NEW CAPABILITY -- the plausible wrong implementation spells a finding id in
    prose-local form (an underscore, a different case) that no consumer's branch reaches. Red
    under that. Why: the schema declares these vocabularies closed exactly so a record
    written by this instrument cannot carry a value no reader handles, and this test is the
    seam that holds the instrument to the schema's single declaration. Killed by: any
    emitted id outside SUBJECT_FINDING_IDS, or any state outside SUBJECT_RESOLUTION_STATES.
    Second correct implementation: importing the ids from the schema at call time inside
    evaluate_readings rather than unpacking them at module load. Not established: this test
    pins membership, not that each vocabulary's own test in test_record_schema.py pins the
    exact members -- that is the schema tests' subject, by design.
    """
    root = _checkout(tmp_path)
    expected = subject.expected_package_file(root)
    outside_a = tmp_path / "a" / "src" / "multidriver_swg" / "__init__.py"
    outside_b = tmp_path / "b" / "src" / "multidriver_swg" / "__init__.py"
    error = {"type": "OSError", "message": "no such file"}
    scenarios = [
        subject.evaluate_readings(_reading(file=str(expected)), _reading(file=str(expected)), root),
        subject.evaluate_readings(_reading(file=str(outside_a)), _reading(file=str(outside_b)), root),
        subject.evaluate_readings(_reading(file=None, error=error), _reading(file=None, error=error), root),
        subject.evaluate_readings(_reading(file=str(expected)), _reading(file=None, error=error), root),
    ]
    emitted_ids = {finding["id"] for resolution in scenarios for finding in resolution["findings"]}
    emitted_states = {resolution["state"] for resolution in scenarios}
    assert emitted_ids == set(record_schema.SUBJECT_FINDING_IDS)
    assert emitted_states == {
        record_schema.SUBJECT_RESOLUTION_STATES[1],
        record_schema.SUBJECT_RESOLUTION_STATES[2],
    }


def test_the_in_process_reader_reports_the_imported_file(monkeypatch):
    """Property: `_read_in_process` returns the imported module's `__file__` with no error when the import succeeds.

    Class: any importable package, here a synthetic module object standing in for it. Build
    class: NEW CAPABILITY -- the plausible wrong implementation resolves the file eagerly
    inside the reader, so a reading taken through a symlink names the link, not the target,
    and disagrees with the evaluator's resolved comparison by spelling. Red under that. Why:
    resolution belongs to the comparison, not the reading, so both channels resolve the same
    way and one spelling convention governs the record. Killed by: resolving in the reader.
    Second correct implementation: returning a named tuple with file and error fields
    instead of the dict the record serializes.
    """
    module = types.SimpleNamespace(__file__="/somewhere/src/multidriver_swg/__init__.py")
    monkeypatch.setattr(subject, "importlib", types.SimpleNamespace(import_module=lambda name: module))
    assert subject._read_in_process() == {
        "file": "/somewhere/src/multidriver_swg/__init__.py",
        "error": None,
    }


def test_the_in_process_reader_reports_an_import_exception_as_an_error(monkeypatch):
    """Property: an import exception becomes `{"file": None, "error": {"type", "message"}}`, whatever exception type the broken package raises.

    Class: any exception out of the import -- a SyntaxError left mid-edit is the everyday
    case, a RuntimeError from a broken `__init__` the general one; here RuntimeError stands
    for the class. Build
    class: NEW CAPABILITY -- the plausible wrong implementation catches only ImportError and
    lets a RuntimeError escape as the reader's own crash, which downstream reads as the
    instrument failing rather than the package failing. Red under that. Why: the reading
    must classify any import failure the same way, because the consumer cannot act on the
    difference. Killed by: an ImportError-only handler. Second correct implementation: a
    try/except around each of the two import paths that catches BaseException and re-raises
    KeyboardInterrupt -- broader in one direction, narrower in the other.
    """

    def _raise(name):
        raise RuntimeError("broken while importing")

    monkeypatch.setattr(subject, "importlib", types.SimpleNamespace(import_module=_raise))
    reading = subject._read_in_process()
    assert reading["file"] is None
    assert reading["error"]["type"] == "RuntimeError"
    assert "broken while importing" in reading["error"]["message"]


def test_the_in_process_reader_reports_a_module_without_file_as_an_error(monkeypatch):
    """Property: a module whose import succeeded but which has no `__file__` becomes a NoFile error reading, not a None-file agreement.

    Class: loaders with no module file -- zip-loaded or frozen packages. Build
    class: NEW CAPABILITY -- the plausible wrong implementation returns {"file": None,
    "error": None}, which the evaluator then cannot classify and either crashes on or treats
    as agreement. Red under both. Why: the reader is where the module is in hand, so it is
    where the no-file condition is knowable; pushing it downstream makes the evaluator guess.
    Killed by: returning no error for a None file. Second correct implementation: raising a
    NoFile exception from the reader and translating it to the error dict at the call site.
    """
    monkeypatch.setattr(
        subject, "importlib", types.SimpleNamespace(import_module=lambda name: types.SimpleNamespace(__file__=None))
    )
    reading = subject._read_in_process()
    assert reading["file"] is None
    assert reading["error"]["type"] == "NoFile"


def test_the_subprocess_reader_runs_a_clean_child_and_parses_its_json(monkeypatch, tmp_path):
    """Property: the second reading launches `sys.executable -B -c {stdlib probe}` with cwd=rootdir and inherits the environment, and parses the child's one JSON object into file/error/pytest_loaded.

    Class: a healthy child reporting a real file. Build
    class: NEW CAPABILITY -- the plausible wrong implementation runs the child with
    `python -I` (discarding PYTHONPATH, so a stale path reads as agreement) or parses the
    child's whole stdout as JSON (so any interpreter warning before the JSON line fails the
    reading). Red under both. Why: the child must observe the same import environment as the
    parent, independently of the harness, or the second reading tests a different world than
    the first. Killed by: an isolated-mode child, or whole-stdout parsing. Second correct
    implementation: reading the child's output from a temp file the probe writes, with the
    parent reading the last line of stdout -- the JSON-line discipline is what this test
    pins.
    """
    root = _checkout(tmp_path)
    calls = {}

    def _fake_run(args, **kwargs):
        calls["args"] = args
        calls["kwargs"] = kwargs
        payload = {"file": str(_package_file_under(root)), "error": None, "pytest_loaded": False}
        return subprocess.CompletedProcess(args, 0, stdout="\n" + json.dumps(payload) + "\n", stderr="")

    monkeypatch.setattr(
        subject, "subprocess", types.SimpleNamespace(run=_fake_run, TimeoutExpired=subprocess.TimeoutExpired)
    )
    reading = subject._read_subprocess(root)
    assert calls["args"][0] == sys.executable
    assert "-B" in calls["args"] and "-c" in calls["args"]
    assert calls["kwargs"]["cwd"] == root
    assert "env" not in calls["kwargs"], "the child must inherit the parent environment, never a scrubbed one"
    assert calls["kwargs"]["timeout"] == subject._CHILD_TIMEOUT_SECONDS
    assert reading == {
        "file": str(_package_file_under(root)),
        "error": None,
        "pytest_loaded": False,
    }


@pytest.mark.parametrize(
    "label,completed,expected_type",
    [
        ("non-zero exit", subprocess.CompletedProcess([], 1, stdout="", stderr="child blew up"), "ChildExit"),
        ("malformed json", subprocess.CompletedProcess([], 0, stdout="not json at all", stderr=""), "BadJson"),
        ("empty stdout", subprocess.CompletedProcess([], 0, stdout="  \n", stderr=""), "BadJson"),
        (
            "missing keys",
            subprocess.CompletedProcess([], 0, stdout=json.dumps({"file": None}), stderr=""),
            "BadJson",
        ),
        (
            "pytest loaded",
            subprocess.CompletedProcess(
                [],
                0,
                stdout=json.dumps({"file": "/x/__init__.py", "error": None, "pytest_loaded": True}),
                stderr="",
            ),
            "HarnessLoaded",
        ),
    ],
    ids=["nonzero-exit", "malformed-json", "empty-stdout", "missing-keys", "pytest-loaded"],
)
def test_a_child_protocol_failure_is_an_error_reading_never_a_file(
    monkeypatch, tmp_path, label, completed, expected_type
):
    """Property: a child that exits non-zero, prints no parseable JSON object, or reports pytest loaded yields an error reading with no file -- never a fabricated file and never a disagreement.

    Class: the child-protocol failure modes of the clean-child design -- here one parametrized member
    per mode. Build
    class: NEW CAPABILITY -- the plausible wrong implementation takes the child's file at
    face value when the exit code is non-zero, or treats harness contamination as a usable
    reading. Red under both. Why: a reading that did not follow the protocol produced no
    evidence, and evidence the harness touched is not independent; either way the channel is
    `cannot-import` downstream, keeping a crashed channel distinct from a disagreeing one.
    Killed by: returning the payload's file when the exit was non-zero, or accepting
    pytest_loaded True. Second correct implementation: a probe that exits non-zero itself on
    any internal failure, so the parent classifies on exit code alone -- the parent-side
    guards this test pins stay cheap either way.
    """
    monkeypatch.setattr(
        subject,
        "subprocess",
        types.SimpleNamespace(run=lambda *a, **k: completed, TimeoutExpired=subprocess.TimeoutExpired),
    )
    reading = subject._read_subprocess(_checkout(tmp_path))
    assert reading["file"] is None, label
    assert reading["error"]["type"] == expected_type, (label, reading)


def test_a_child_launch_failure_is_an_error_reading(monkeypatch, tmp_path):
    """Property: when the child cannot be launched, or times out, the reading is an error with no file.

    Class: the launch path itself failing -- here OSError (interpreter vanished) and
    TimeoutExpired (child hung) standing for the class. Build
    class: NEW CAPABILITY -- the plausible wrong implementation lets OSError escape, which
    ends the session as the instrument's own crash rather than a cannot-import finding. Red
    under that. Why: the run's verdict machinery must see a failed reading it can classify,
    not an exception channel nobody catches. Killed by: an unhandled OSError or TimeoutExpired
    out of the reader. Second correct implementation: catching subprocess.SubprocessError
    generally and mapping its concrete type into the error dict's type field.
    """

    def _raise_oserror(*args, **kwargs):
        raise OSError("no such interpreter")

    def _raise_timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd="child", timeout=subject._CHILD_TIMEOUT_SECONDS)

    for raiser, expected_type in (
        (_raise_oserror, "LaunchError"),
        (_raise_timeout, "TimeoutExpired"),
    ):
        monkeypatch.setattr(
            subject, "subprocess", types.SimpleNamespace(run=raiser, TimeoutExpired=subprocess.TimeoutExpired)
        )
        reading = subject._read_subprocess(_checkout(tmp_path))
        assert reading["file"] is None
        assert reading["error"]["type"] == expected_type


def test_resolve_subject_reports_a_clean_resolution_with_the_observation_contract(monkeypatch, tmp_path):
    """Property: an agreeing resolution reports state resolved, the disagreement condition, and an observation whose date comes from the ambient clock and whose observed_in_this_run is False.

    Class: the healthy real-tree resolution any run of this checkout should produce. Build
    class: NEW CAPABILITY -- the plausible wrong implementation hard-codes the observation
    date, which passes on 2026-11-03 and fails every later day, or sets
    observed_in_this_run from whether any finding exists rather than exactly
    readings-disagree. Red under both. Why: the observation is the record's answer to "show
    me a run where the reading disagreed", and a date that does not move with the clock is a
    bare constant wearing a timestamp. Killed by: a constant date, or observed_in_this_run
    keyed on any-finding. Second correct implementation: passing the clock in as a parameter
    instead of reading datetime.date.today() at the seam; the seam is what the frozen-clock
    staged test patches.
    """
    root = _checkout(tmp_path)
    expected = subject.expected_package_file(root)

    class _FrozenDate:
        @staticmethod
        def today():
            return datetime.date(2031, 4, 5)

    monkeypatch.setattr(subject, "datetime", types.SimpleNamespace(date=_FrozenDate))
    monkeypatch.setattr(subject, "_read_in_process", lambda: _reading(file=str(expected)))
    monkeypatch.setattr(
        subject, "_read_subprocess", lambda rootdir: {**_reading(file=str(expected)), "pytest_loaded": False}
    )
    resolution = subject.resolve_subject(root)
    assert resolution["state"] == "resolved"
    assert resolution["expected_file"] == str(expected)
    assert resolution["readings"]["subprocess"]["pytest_loaded"] is False
    assert "checkout C" in resolution["disagreement_condition"]
    observation = resolution["observation"]
    assert observation["staged_test"] == subject.STAGED_DISAGREEMENT_TEST
    assert observation["sys_path"] == "parent-only"
    assert observation["clock"] == "frozen"
    assert observation["date"] == "2031-04-05"
    assert observation["observed_in_this_run"] is False


def test_resolve_subject_flags_disagreement_observed_in_this_run(monkeypatch, tmp_path):
    """Property: when the two readings disagree, state is void, the observation is flagged observed_in_this_run, and the readings are preserved for audit.

    Class: the parent-only path staging the record integration runs -- parent reading the
    expected file, child reading an outside file. Build
    class: NEW CAPABILITY -- the plausible wrong implementation reports state resolved
    because one reading matched, or drops the disagreement finding from the audit copy of
    the readings. Red under both. Why: a disagreement is the strongest signal this
    instrument produces; if it does not flip the state and the flag together, a reader
    comparing them sees a record that disagrees with itself. Killed by: any path where
    readings-disagree exists but state is resolved or the flag is False. Second correct
    implementation: deriving the flag and the state from one shared `any(...)` expression
    over the findings rather than two traversals.
    """
    root = _checkout(tmp_path)
    expected = subject.expected_package_file(root)
    outside = tmp_path / "other" / "src" / "multidriver_swg" / "__init__.py"
    monkeypatch.setattr(subject, "_read_in_process", lambda: _reading(file=str(expected)))
    monkeypatch.setattr(
        subject,
        "_read_subprocess",
        lambda rootdir: {**_reading(file=str(outside)), "pytest_loaded": False},
    )
    resolution = subject.resolve_subject(root)
    assert [f["id"] for f in resolution["findings"]] == [
        "resolved-outside",
        "readings-disagree",
    ]
    assert resolution["state"] == "void"
    assert resolution["observation"]["observed_in_this_run"] is True
    assert resolution["readings"]["in_process"]["file"] == str(expected)
    assert resolution["readings"]["subprocess"]["file"] == str(outside)


# The parent-only plugin the staged disagreement run loads: it prepends the declared src to the
# PARENT's sys.path at plugin-load time -- before the record plugin's configure hook can import
# and cache the package -- and freezes the observation clock at the staged date. The clean child
# inherits only the environment, so it never sees either effect: that asymmetry is the staged
# condition itself. Deliberately a `-p` module written into the test's temporary directory,
# never a tracked file: the run's staging must not survive into the next run.
STAGED_PLUGIN = """
import datetime
import os
import sys
import types

sys.path.insert(0, os.environ["STAGED_SRC"])

from tests.correspondence import subject  # noqa: E402


class _FrozenDate:
    @staticmethod
    def today():
        return datetime.date(2026, 11, 3)


subject.datetime = types.SimpleNamespace(date=_FrozenDate)
"""


def _recorded_child(tmp_path, name, init_text=None):
    """Run the real tree with `--result-record` at `{tmp}/{name}.json`, `-k` the pure containment node.

    `init_text` writes a synthetic package at `{tmp}/{name}/src` and names that directory first
    on the child's PYTHONPATH; otherwise the declared src is named first. Every inner run selects
    only the pure containment node, so no staging test is selected recursively.
    """
    root = corpus_record.REPO_ROOT
    record_path = tmp_path / f"{name}.json"
    if init_text is not None:
        package = tmp_path / name / "src" / "multidriver_swg" / "__init__.py"
        package.parent.mkdir(parents=True)
        package.write_text(init_text)
        first = [str(tmp_path / name / "src")]
    else:
        first = [str(root / "src")]
    path = first + [str(root), os.environ.get("PYTHONPATH", "")]
    out = subprocess.run(
        [
            sys.executable,
            "-B",
            "-m",
            "pytest",
            "-p",
            "no:cacheprovider",
            "-q",
            "-k",
            CONTAINMENT,
            "--result-record",
            str(record_path),
        ],
        cwd=root,
        env=dict(os.environ, PYTHONPATH=os.pathsep.join(path)),
        capture_output=True,
        text=True,
    )
    return record_path, out


def _read_record(record_path):
    return json.loads(record_path.read_text(encoding="utf-8"))


def _expected_source(root):
    return str((root / "src" / "multidriver_swg" / "__init__.py").resolve())


def test_a_real_tree_run_records_a_resolved_subject_and_its_verdict(tmp_path):
    """Property: a healthy run of this checkout records a resolved subject_resolution, both readings equal to the expected file, and the run's real verdict.

    Class: any run of the real tree with `--result-record`, here one selecting the pure
    containment node so the child finishes fast. Build
    class: NEW CAPABILITY -- the plausible wrong implementation resolves the subject once, in
    the parent only, so a record can never show the two readings agreeing; a second wrong
    implementation writes the resolved state but leaves the provisional VOID as the verdict
    even when the run completed. Red under both. Why: the record is the artifact a consumer
    reads instead of the run's console output, so an agreeing run must name its own verdict
    rather than the placeholder. Killed by: resolving in one process, or never advancing the
    verdict past VOID on a completed run. Second correct implementation: writing the verdict
    from `pytest_sessionfinish`'s own outcome in the same hook that writes the subject
    resolution -- rejected in the design because a truncated run must not read as passed.
    """
    record_path, out = _recorded_child(tmp_path, "resolved")
    assert out.returncode == 0, out.stdout + out.stderr
    record = _read_record(record_path)
    resolution = record["subject_resolution"]
    assert resolution["state"] == "resolved"
    assert resolution["expected_file"] == _expected_source(corpus_record.REPO_ROOT)
    assert resolution["readings"]["in_process"]["file"] == resolution["expected_file"]
    assert resolution["readings"]["subprocess"]["file"] == resolution["expected_file"]
    assert resolution["readings"]["subprocess"]["pytest_loaded"] is False
    assert resolution["findings"] == []
    assert record["verdict"] == "pass"
    assert record["completeness"] == "complete"


def test_an_outside_package_run_records_void_before_the_exit(tmp_path):
    """Property: a run whose import resolves outside the checkout ends with exit code 4, and the record on disk already carries the two resolved-outside findings and no readings-disagree.

    Class: any run whose environment imports another copy of the package in BOTH processes --
    here a synthetic copy the child PYTHONPATH names first. Build
    class: NEW CAPABILITY -- the plausible wrong implementation stops before serializing, so
    the record still shows only the unresolved placeholder; a second wrong implementation
    classifies two equal outside files as disagreement. Red under both. Why: a denied run's
    only artifact is this record, and two agreeing channels are evidence about WHERE the
    package resolved, not a disagreement of readings. Killed by: stopping before the write, or
    emitting readings-disagree for equal files. Second correct implementation: serializing
    from the stop handler itself rather than through the emitter helper -- the helper exists
    so the record owns serialization and the subject owns resolution.
    """
    record_path, out = _recorded_child(
        tmp_path, "outside", '"""A synthetic copy of the package, outside the checkout."""\n'
    )
    assert out.returncode == 4, (out.returncode, out.stdout + out.stderr)
    record = _read_record(record_path)
    resolution = record["subject_resolution"]
    outside = (tmp_path / "outside" / "src" / "multidriver_swg" / "__init__.py").resolve()
    assert record["verdict"] == "VOID"
    assert record["completeness"] == "pre-collection"
    assert resolution["state"] == "void"
    assert [f["id"] for f in resolution["findings"]] == ["resolved-outside", "resolved-outside"]
    assert [f["reading"] for f in resolution["findings"]] == ["in_process", "subprocess"]
    assert all(f["id"] != "readings-disagree" for f in resolution["findings"])
    assert str(outside) in resolution["findings"][0]["message"]
    assert _expected_source(corpus_record.REPO_ROOT) in resolution["findings"][0]["message"]


def test_a_broken_package_run_records_cannot_import_per_failed_reading(tmp_path):
    """Property: a package whose import raises in every process produces exactly one cannot-import finding per channel, no readings-disagree, and exit code 4.

    Class: any package that breaks while importing -- here a RuntimeError, so a failure that
    is not ImportError stays classified. Build
    class: NEW CAPABILITY -- the plausible wrong implementation reports only the parent's
    failure, or converts one failed channel into a disagreement. Red under both. Why: both
    channels observed the same broken package, so the record must show two import failures and
    zero disagreements -- the both-failed case is the one a reader must not confuse with
    disagreement. Killed by: a single finding, or any readings-disagree. Second correct
    implementation: the child probe exiting non-zero on an import error and the parent
    classifying on exit code alone -- the probe instead reports the error in JSON so the
    record carries the exception type.
    """
    record_path, out = _recorded_child(
        tmp_path, "broken", 'raise RuntimeError("breaks while importing, and not with ImportError")\n'
    )
    assert out.returncode == 4, (out.returncode, out.stdout + out.stderr)
    record = _read_record(record_path)
    resolution = record["subject_resolution"]
    assert record["verdict"] == "VOID"
    assert record["completeness"] == "pre-collection"
    assert resolution["state"] == "void"
    assert [f["id"] for f in resolution["findings"]] == ["cannot-import", "cannot-import"]
    assert [f["reading"] for f in resolution["findings"]] == ["in_process", "subprocess"]
    assert all(f["id"] != "readings-disagree" for f in resolution["findings"])


def _staged_child(tmp_path, name, staged_src):
    """Run the real tree under the staged condition: outside package first on the child path, parent-only plugin prepending the declared src and freezing the clock."""
    root = corpus_record.REPO_ROOT
    outside_src = tmp_path / name / "src"
    package = outside_src / "multidriver_swg" / "__init__.py"
    package.parent.mkdir(parents=True)
    package.write_text('"""A synthetic copy the clean child resolves."""\n')
    plugin = tmp_path / "staged_plugin.py"
    plugin.write_text(STAGED_PLUGIN)
    record_path = tmp_path / f"{name}.json"
    path = [str(tmp_path), str(outside_src), str(root), os.environ.get("PYTHONPATH", "")]
    out = subprocess.run(
        [
            sys.executable,
            "-B",
            "-m",
            "pytest",
            "-p",
            "no:cacheprovider",
            "-q",
            "-k",
            CONTAINMENT,
            "-p",
            "staged_plugin",
            "--result-record",
            str(record_path),
        ],
        cwd=root,
        env=dict(os.environ, PYTHONPATH=os.pathsep.join(path), STAGED_SRC=str(staged_src)),
        capture_output=True,
        text=True,
    )
    return package, record_path, out


def test_a_parent_only_path_stages_disagreement(tmp_path):
    """Property: when the parent alone sees the declared src, the parent reads the expected file, the clean child reads the outside file, and the record carries readings-disagree with the staged observation under the frozen clock.

    Class: the stale-path condition the instrument exists to catch -- a parent whose path
    names the declared checkout while the environment the child inherits names another. Build
    class: NEW CAPABILITY -- the plausible wrong implementation stages the prepend at
    configure time, after the record plugin has already cached the import, so both readings
    agree and the observation can never fire; a second wrong implementation stamps the
    observation date from a constant. Red under both. Why: the observation a reader audits is
    only real if the run actually produced disagreeing readings under the freeze, with the
    date the frozen clock produced. Killed by: moving the prepend after configure, deleting
    the prepend, or hard-coding the date. Second correct implementation: staging the
    disagreement through a PYTHONPATH the parent mutates before importing -- the plugin-load
    prepend is preferred because it runs before any hook can import the package first.
    """
    root = corpus_record.REPO_ROOT
    package, record_path, out = _staged_child(tmp_path, "staged-disagreement", root / "src")
    assert out.returncode == 4, (out.returncode, out.stdout + out.stderr)
    record = _read_record(record_path)
    resolution = record["subject_resolution"]
    assert [f["id"] for f in resolution["findings"]] == ["resolved-outside", "readings-disagree"]
    outside_finding = resolution["findings"][0]
    assert outside_finding["reading"] == "subprocess"
    assert outside_finding["comparison"] == "expected-file-equality"
    assert str(package.resolve()) in outside_finding["message"]
    assert _expected_source(root) in outside_finding["message"]
    disagree = resolution["findings"][1]
    assert str(package.resolve()) in disagree["message"]
    assert _expected_source(root) in disagree["message"]
    assert resolution["readings"]["subprocess"]["pytest_loaded"] is False
    assert resolution["state"] == "void"
    assert record["verdict"] == "VOID"
    assert record["completeness"] == "pre-collection"
    observation = resolution["observation"]
    assert observation["staged_test"] == subject.STAGED_DISAGREEMENT_TEST
    assert observation["sys_path"] == "parent-only"
    assert observation["clock"] == "frozen"
    assert observation["date"] == "2026-11-03"
    assert observation["observed_in_this_run"] is True


def test_the_staged_disagreement_control_agrees_in_one_environment(tmp_path):
    """Property: the same staged run with the declared src named for BOTH channels produces equal readings, no disagreement finding, and observed_in_this_run False.

    Class: the same-environment arm of the staged condition -- identical plugin, identical
    freeze, only the environment names the declared src to the clean child too. Build
    class: NEW CAPABILITY -- the plausible wrong implementation emits the disagreement from
    the plugin's presence alone rather than from the readings, so the control disagrees too.
    Red under that. Why: a control that cannot go green proves nothing about the staged run's
    redness -- the disagreement must come from the readings, so removing the path asymmetry
    must remove it. Killed by: any disagreement finding in the control. Second correct
    implementation: asserting on the parent-only subprocess reading's file directly instead of
    on the absence of the finding -- the absence is the property the shape asks the control
    to prove.
    """
    root = corpus_record.REPO_ROOT
    record_path = tmp_path / "control.json"
    plugin = tmp_path / "staged_plugin.py"
    plugin.write_text(STAGED_PLUGIN)
    path = [str(tmp_path), str(root / "src"), str(root), os.environ.get("PYTHONPATH", "")]
    out = subprocess.run(
        [
            sys.executable,
            "-B",
            "-m",
            "pytest",
            "-p",
            "no:cacheprovider",
            "-q",
            "-k",
            CONTAINMENT,
            "-p",
            "staged_plugin",
            "--result-record",
            str(record_path),
        ],
        cwd=root,
        env=dict(os.environ, PYTHONPATH=os.pathsep.join(path), STAGED_SRC=str(root / "src")),
        capture_output=True,
        text=True,
    )
    assert out.returncode == 0, out.stdout + out.stderr
    record = _read_record(record_path)
    resolution = record["subject_resolution"]
    assert resolution["state"] == "resolved"
    assert resolution["findings"] == []
    assert resolution["readings"]["in_process"]["file"] == _expected_source(root)
    assert resolution["readings"]["subprocess"]["file"] == _expected_source(root)
    assert resolution["readings"]["subprocess"]["pytest_loaded"] is False
    assert resolution["observation"]["observed_in_this_run"] is False
    assert resolution["observation"]["date"] == "2026-11-03"
    assert record["verdict"] == "pass"
