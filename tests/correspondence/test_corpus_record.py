"""Tests of the corpus record check: it reports every disagreement between the record and the tracked legacy files by file and field, ignores files git does not track, and makes the checks below it report "not measured" when it fails.

The failing-case tests' fixtures are git repositories built inside `tmp_path` from a copy of the
real record and the real tracked corpus files; they are staged, never committed, so no
git identity is needed.
"""

import shutil
import subprocess
from pathlib import Path

import pytest

from tests.correspondence import corpus_record

pytestmark = pytest.mark.state_check

CORPUS = "_old_code_to_refactor"


def _corpus_fixture(tmp_path: Path) -> Path:
    """Copy the record and every tracked corpus file into a fresh git repository and stage them."""
    listed = subprocess.run(
        ["git", "ls-files", "-z", "--", CORPUS], cwd=corpus_record.REPO_ROOT, capture_output=True, check=True
    ).stdout.decode("utf-8")
    for rel in (p for p in listed.split("\0") if p):
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(corpus_record.REPO_ROOT / rel, tmp_path / rel)
    shutil.copyfile(corpus_record.REPO_ROOT / corpus_record.RECORD_NAME, tmp_path / corpus_record.RECORD_NAME)
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "-A"], cwd=tmp_path, check=True)
    return tmp_path


def test_record_matches_the_tracked_files():
    """Property: the committed record equals its recomputation from this checkout's tracked files.

    Class: the whole record, every file and field. Why: every cited line range assumes these
    bytes. Killed by: any recomputation that disagrees with the record (a wrong digest rule,
    a wrong line count). Second correct implementation: one that hashes files in a different
    order but sorts before building the manifest still passes.
    """
    findings = corpus_record.record_findings()
    assert not findings, "\n".join(findings)


def test_record_matches_a_copied_corpus(tmp_path):
    """Property: the check passes on a correct corpus at a root other than the repository.

    Class: any root whose tracked corpus and record agree. Why: the failing-case tests below
    run at such roots, so this test shows that a correct corpus there yields no finding.
    Killed by: counting a file's lines by its newline characters instead of `str.splitlines()`,
    because 8 of the 18 tracked corpus files end without a final newline. Not killed by: a check
    that ignores `root` and reads the repository instead, since the copy equals the repository;
    the changed-byte and missing-field tests below kill that one. Second correct implementation:
    one reading files through `git show :path` instead of the working tree still passes, since
    the copy is staged unchanged.
    """
    root = _corpus_fixture(tmp_path)
    findings = corpus_record.record_findings(root)
    assert not findings, "\n".join(findings)


def test_a_changed_byte_is_named_by_file_and_field(tmp_path):
    """Property: a candidate file whose bytes differ from the record is named with the field `sha256`.

    Class: any tracked file whose content changed while its length in lines did not. Why: the
    stored extractions and ledger keys cite these bytes; a silent change moves what they cite.
    Killed by: skipping the per-file digest comparison (the set digest still fails, but it does
    not name the file). Second correct implementation: comparing a different strong digest of
    each file, named in the same field, still passes.
    """
    root = _corpus_fixture(tmp_path)
    target = root / CORPUS / "_inputs.py"
    data = bytearray(target.read_bytes())
    data[0:1] = b"X" if data[0:1] != b"X" else b"Y"
    target.write_bytes(bytes(data))
    findings = corpus_record.record_findings(root)
    assert any(f.startswith("_inputs.py:") and "sha256" in f for f in findings), "\n".join(findings)


def test_an_untracked_python_file_is_not_counted(tmp_path):
    """Property: a `.py` file in the corpus directory that git does not track changes no figure.

    Class: untracked or ignored files in the corpus directory, such as a stray script or a
    compiled module an import left behind. Why: the population must be the same on every
    clone. Killed by: listing the directory instead of calling `tracked_files(root)`. Second
    correct implementation: `git ls-tree -r HEAD`-style listing over a committed fixture would
    also ignore it.
    """
    root = _corpus_fixture(tmp_path)
    (root / CORPUS / "stray_untracked.py").write_text("x = 1\n", encoding="utf-8")
    findings = corpus_record.record_findings(root)
    assert not findings, "\n".join(findings)


def test_a_missing_hand_written_field_is_named(tmp_path):
    """Property: a section without its `revision` field is reported naming `revision`.

    Class: any hand-written section field deleted or emptied. Why: the pin is what the upstream
    verification and every citation rest on. Killed by: dropping the presence check. Second
    correct implementation: a schema validator that reports missing keys by name still passes.
    """
    root = _corpus_fixture(tmp_path)
    path = root / corpus_record.RECORD_NAME
    text = path.read_text(encoding="utf-8")
    lines = [line for line in text.splitlines() if not line.startswith("revision = ")]
    assert len(lines) == len(text.splitlines()) - 1
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    findings = corpus_record.record_findings(root)
    assert any("revision" in f for f in findings), "\n".join(findings)


def test_a_dependent_check_reports_not_measured(tmp_path):
    """Property: a check below a failed record skips with the prefix `NOT MEASURED: corpus record check failed`.

    Class: any failing record seen by any dependent check. Why: one broken record must show as
    one failure with one cause, not as one failure per dependent check. Killed by: making
    `require_record` return without skipping. Second correct implementation: raising pytest's
    skip from a fixture instead of the function body still passes.
    """
    root = _corpus_fixture(tmp_path)
    corpus_record.require_record(root)  # intact copy: returns, does not skip
    target = root / CORPUS / "_inputs.py"
    target.write_bytes(target.read_bytes() + b"\n")
    with pytest.raises(pytest.skip.Exception) as raised:
        corpus_record.require_record(root)
    assert str(raised.value).startswith(corpus_record.NOT_MEASURED)


def test_a_tracked_file_missing_from_the_tree_is_reported_not_raised(tmp_path):
    """A tracked corpus file that cannot be read from the working tree.

    Property: the record check REPORTS, rather than raises, when a path git tracks is missing
    from the working tree; a dependent check therefore skips as not measured instead of erroring.
    Class: every tracked corpus path whose working-tree read fails — deleted but not
    unstaged, replaced by a directory, or excluded by a sparse checkout (git ls-files lists the
    index, so all three stay in the population while their bytes are unreadable).
    Why: the record check is the head of the chain; if it raises, every dependent check
    that calls require_record() errors with the same traceback, which is the "one broken record
    reported as hundreds of failures" outcome the chain exists to prevent.
    Killed by: letting the working-tree read's OSError escape record_findings().
    Second correct implementation: reading each tracked file's bytes from the index
    (`git show :path`) reports no finding at all for this input and still passes, because the
    assertions require only "no exception escapes" and "any finding names the file".
    """
    root = _corpus_fixture(tmp_path)
    (root / CORPUS / "_inputs.py").unlink()  # still in the index, so still in git ls-files
    findings = corpus_record.record_findings(root)
    assert all(isinstance(f, str) for f in findings)
    if findings:
        assert any("_inputs.py" in f for f in findings), "\n".join(findings)
    try:
        corpus_record.require_record(root)
    except pytest.skip.Exception as skipped:
        assert str(skipped).startswith(corpus_record.NOT_MEASURED)
