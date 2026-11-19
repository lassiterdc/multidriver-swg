"""Reviewer tests of the pre-release scan: what a release is refused on, and what no step may switch off.

Two properties the build's own test file does not hold, each over a class and not over one artifact. The
first is the corpus the secret class reads: the scan's stated invariant is that no secret reaches a
PUBLISHED BYTE, and a member the text reader cannot decode is a published byte. The second is the
step's refusal itself: the release stops because the step exits non-zero and every upload job
inherits `needs` on its job, and one `continue-on-error` or `if` on any step of that job turns the
refusal into a log line while every later job still runs.

The samples are assembled from adjacent string literals for the reason the build's own test file gives.
"""

from __future__ import annotations

import json
import re
import zipfile

import pytest

from tests.test_content_scan_release import PEM_HEADER, _release, _run, _scan_of
from tests.test_publish_workflow import (
    DISTRIBUTION_CHECK,
    SCAN_STEP,
    WORKFLOW,
    _content,
    _entries,
    _job,
    _step,
)

# A member the text reader declines: a NUL anywhere marks a file binary, and a byte UTF-8 cannot
# decode makes the whole file unreadable as text. Both reach a wheel or an archive unchanged.
NUL_BEARING = b"\x00binary preamble\n" + PEM_HEADER.encode() + b"\nnot-a-real-key\n"
LATIN_ONE = "# caf\xe9 credentials\n".encode("latin-1") + PEM_HEADER.encode() + b"\nnot-a-real-key\n"


@pytest.mark.parametrize("data", [NUL_BEARING, LATIN_ONE], ids=["nul-bearing", "not-utf8"])
def test_a_secret_shape_in_a_member_the_text_reader_cannot_decode_still_refuses_the_release(tmp_path, data):
    """Property: a released member of ANY encoding that holds a whole secret-shape line makes the scan
    exit 1, so a credential does not reach an index because the file carrying it is not UTF-8 text.

    Class: NEW CAPABILITY -- plausible wrong implementation is the one the build shipped: the secret
    matcher is loaded with `reads_bytes` false, `core.read_text_or_none` returns None for a NUL-bearing
    or non-UTF-8 file, and the class finds nothing in it while `machine_path`, which reads bytes, opens
    the same file, so the record's `scanned` figure counts it as read. Input class: members of the
    three sets whose bytes are not UTF-8 text (a data file, a notebook with a stray NUL, a Latin-1
    source). Red under that implementation; the run is pasted in the reviewer's file.

    Reachability: a release member `src/sample/blob.dat` reaches `core.scan` through `materialize`
    unchanged; artifact: `scripts/content_scan/release_corpus.py` `CLASSES`/`scan_release` and
    `scripts/content_scan/secret.py` `load()` (no `reads_bytes`).

    Why: `release_corpus.py`'s own docstring states the invariant as "no secret reaches a published
    byte" and the record's `corpus.scanned` is the figure a later reader takes as "the files the
    predicate was checked against", so a skipped file reported as scanned is a false record and not
    only a missed finding.

    Both-states anchor: the assertion turns on the exit status and the secret count over a planted
    shape, which a correct implementation changes the same way whatever reader it uses.

    Killed by: loading the secret matcher with `reads_bytes=True` inside `release_corpus.py` (for
    example `dataclasses.replace(secret.load(), reads_bytes=True)`). Second correct implementation:
    decoding each member with `errors="surrogateescape"` before the scan, or a scan of the raw bytes
    by `re.compile(...).search` over a `latin-1` decode, which refuses by a different mechanism.
    """
    record, dist, repo = _release(
        tmp_path,
        {
            "README.md": b"# sample\n",
            "src/sample/blob.dat": data,
        },
    )

    completed = _run(record, dist, repo)
    assert completed.returncode == 1, completed.stdout + completed.stderr
    assert _scan_of(record)["counts"]["secret"] == 3, _scan_of(record)


def _unguarded_steps(text: str) -> list[str]:
    """The names of the steps of `post-build-check` that can end green without the command passing."""
    workflow = _entries(_content(text))
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict), jobs
    steps = jobs[DISTRIBUTION_CHECK]["steps"]
    return [
        str(step.get("name", "?"))
        for step in steps
        if isinstance(step, dict) and ("continue-on-error" in step or "if" in step)
    ]


def test_no_step_of_the_distribution_check_can_pass_over_a_failing_command():
    """Property: no step of `post-build-check` carries `continue-on-error` or `if`, so the scan's exit
    1 fails the job and the three upload jobs, which need it with no condition, never start.

    Class: NEW CAPABILITY -- plausible wrong implementation is the scan step written, in a later
    edit of this job, with `continue-on-error: true` (the release-gate job's own download step
    already uses the key) or `if: always()`; the step still exits 1 and its own test, which reads only
    the command and the flags, stays green, while the job succeeds and the release uploads a secret.
    Input class: every step of the job, so the next step another chunk adds is held to the same
    bar. A mutant text carrying the key on the scan step is red; the real file is green, and the
    mutant arm is what shows the predicate is not vacuously true.

    Reachability: `.github/workflows/publish.yml`, `post-build-check` steps; the build's disposition
    table asserts "a plain `run:` with no `if:`/`continue-on-error`" and no test holds it.

    Both-states anchor: the assertion turns on the keys present in each step's mapping, a property
    of the file before and after any edit.

    Killed by: adding `continue-on-error: true` or an `if:` to any step of the job. Second correct
    implementation: the scan step written with `run: |` and a multi-line command, or with its
    arguments quoted, neither of which carries either key.
    """
    text = WORKFLOW.read_text(encoding="utf-8")
    assert _unguarded_steps(text) == [], _unguarded_steps(text)

    scan = _step(_job(DISTRIBUTION_CHECK), SCAN_STEP)
    assert "run" in scan, sorted(scan)

    mutant = text.replace(
        f"      - name: {SCAN_STEP}\n", f"      - name: {SCAN_STEP}\n        continue-on-error: true\n", 1
    )
    assert mutant != text, "the scan step is not written as `- name:` first, so the mutant did not apply"
    assert _unguarded_steps(mutant) == [SCAN_STEP], _unguarded_steps(mutant)


def test_a_record_naming_a_path_a_wheel_holds_as_file_and_directory_exits_unusable_not_refused(tmp_path):
    """Property: a wheel in which one name is both a file and the parent directory of another member
    makes the scan exit 2 with one sentence, and never a traceback whose status (1) reads as a
    refusal on a secret.

    Class: REGRESSION is not claimed; NEW CAPABILITY -- plausible wrong implementation is
    `materialize` letting the `FileExistsError`/`NotADirectoryError` of `target.parent.mkdir` escape,
    which Python reports as exit 1, the code the scanner reserves for "the released bytes hold a
    secret". Input class: archives whose member names collide once written to a directory.

    Reachability: a zip may hold `sample/x` and `sample/x/y`; the enumerator records both; artifact:
    `release_corpus.materialize`.

    Both-states anchor: the assertion turns on the exit status and the absence of a traceback.

    Killed by: removing the collision from `materialize`'s error mapping. Second correct
    implementation: refusing a colliding record before any write, or catching `OSError` once in
    `main`.
    """
    record, dist, repo = _release(tmp_path, {"README.md": b"# sample\n"})
    wheel = next(dist.glob("*.whl"))
    with zipfile.ZipFile(wheel, "a") as archive:
        archive.writestr("sample/x", b"a file\n")
        archive.writestr("sample/x/y", b"a file below it\n")
    document = json.loads(record.read_text(encoding="utf-8"))
    for name in ("sample/x", "sample/x/y"):
        document["entries"].append({"set": "wheel", "path": name, "sha256": "0" * 64, "size": 1})
    record.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")

    completed = _run(record, dist, repo)
    assert "Traceback" not in completed.stderr, completed.stderr
    assert completed.returncode == 2, completed.stdout + completed.stderr
    assert re.search(r"content-scan:", completed.stderr), completed.stderr
    assert _scan_of(record)["status"] == "not-run"
