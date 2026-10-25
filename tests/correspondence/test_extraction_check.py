"""Tests of the three-way fidelity check of the stored extractions and of its place in the chain.

The failing-case tests run on a temporary copy of the corpus, its record, the ledger, the stored
texts and the extraction record, held in a git repository inside `tmp_path` that is staged and
never committed. Each test breaks exactly one thing in the copy and asserts the one finding that
the check owes. The git location variables a hook exports are removed first so that a lookup
reads the temporary repository. The real-tree test is the only one that reads the checkout.

Every failing-case test is a NEW CAPABILITY: no check existed before this module, so a test's
failure is shown by the wrong implementation its docstring names.
"""

import re
import shutil
import subprocess
import tomllib
from pathlib import Path

import pytest

from tests.correspondence import corpus_record, extent, extraction, extraction_check, generate_extractions

pytestmark = pytest.mark.state_check

GIT_LOCATION_VARS = ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR", "GIT_PREFIX")
REAL = extraction_check.REPO_ROOT
COPIED = ("_old_code_to_refactor", "tests/correspondence/ledger", extraction.STORED_DIR)


def _copy(tmp_path: Path, monkeypatch) -> Path:
    """Copy the corpus, its record, the ledger, the stored texts and the extraction record into a staged repository."""
    for var in GIT_LOCATION_VARS:
        monkeypatch.delenv(var, raising=False)
    for rel in COPIED:
        shutil.copytree(REAL / rel, tmp_path / rel)
    for rel in (corpus_record.RECORD_NAME, generate_extractions.RECORD_PATH):
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REAL / rel, tmp_path / rel)
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "-A"], cwd=tmp_path, check=True)
    return tmp_path


def _rows(root: Path) -> list[dict]:
    return tomllib.loads((root / generate_extractions.RECORD_PATH).read_text(encoding="utf-8"))["extraction"]


def _record_text(root: Path) -> str:
    return (root / generate_extractions.RECORD_PATH).read_text(encoding="utf-8")


def _rewrite_record(root: Path, old: str, new: str) -> None:
    text = _record_text(root)
    assert text.count(old) == 1, old
    (root / generate_extractions.RECORD_PATH).write_text(text.replace(old, new), encoding="utf-8")


def _flip_first_byte(path: Path) -> None:
    data = bytearray(path.read_bytes())
    data[0] = data[0] ^ 1
    path.write_bytes(bytes(data))


def _one(findings: list[str], needle: str) -> str:
    """Return the single finding containing `needle`, asserting the check reported exactly one finding."""
    assert len(findings) == 1, "\n".join(findings)
    assert needle in findings[0], findings[0]
    return findings[0]


def test_the_check_passes_on_the_real_tree_with_130_rows():
    """Property: on this checkout the chain passes and the check has no finding over 130 record rows.

    Class: the committed record and the committed stored texts. Why it is required: the
    check ranges over a real population from the day it lands. Killed by: a check that reads no
    row (the count assertion), or one that reports a finding on a faithful generation. Second
    correct implementation: comparing digests only, with no cut, passes here. Not established:
    that the corpus is unmoved; the chain gate above this call establishes that.
    """
    extraction_check.require_chain(REAL)
    assert extraction_check.extraction_findings(REAL) == []
    assert len(_rows(REAL)) == 130
    assert extraction_check.main(REAL) == 0


def test_three_agreeing_values_pass_and_do_not_skip(tmp_path, monkeypatch):
    """Property: on an untouched copy there is no finding and `require_extractions` returns without skipping.

    Class: a faithful copy of the whole population. Why it is required: it is the control for
    every failing case below, which differ from it by one break. Killed by: any outcome that
    reports a finding on agreement. Second correct implementation: a check that compares only
    digests passes.
    """
    root = _copy(tmp_path, monkeypatch)
    assert extraction_check.extraction_findings(root) == []
    assert extraction_check.require_extractions(root) is None


def test_an_edited_stored_text_fails_naming_that_text(tmp_path, monkeypatch):
    """Property: one edited byte in a stored text is reported as the copy being edited, naming the text, and nothing else.

    Class: S alone differs (the cut and the record still agree). Why it is required: the cause
    decides the remedy, and here re-running the generator restores agreement. Killed by: a
    check that reports every disagreement as one message, or that names the record. Second
    correct implementation: comparing S with C first and then R passes.
    """
    root = _copy(tmp_path, monkeypatch)
    row = _rows(root)[5]
    _flip_first_byte(root / row["path"])
    finding = _one(extraction_check.extraction_findings(root), row["path"])
    assert "copy was edited" in finding
    assert "re-running" in finding


def test_a_changed_cut_fails_naming_the_range(tmp_path, monkeypatch):
    """Property: a cut that now returns other bytes is reported as the cutting function having changed, naming the range.

    Class: C alone differs (the stored text and the record agree). Why it is required: the
    remedy is a rule-version change and not a regeneration. Killed by: reporting this as a
    stored-text edit. Second correct implementation: any check that tests the three in
    another order passes. The rebinding stands for a changed `cut` in `extraction`, whose
    own tests pin its behaviour.
    """
    root = _copy(tmp_path, monkeypatch)
    row = _rows(root)[7]
    real_cut = extraction.cut

    def other_bytes(r, section, file, first, last):
        """Return the real cut, with one byte appended for the one range under test."""
        data = real_cut(r, section, file, first, last)
        return data + b"#" if (file, first, last) == (row["file"], row["first"], row["last"]) else data

    monkeypatch.setattr(extraction, "cut", other_bytes)
    finding = _one(extraction_check.extraction_findings(root), f"{row['file']}:{row['first']}-{row['last']}")
    assert "without a rule-version change" in finding


@pytest.mark.parametrize("field", ["cut", "stored"])
def test_an_edited_record_digest_fails_naming_the_row(tmp_path, monkeypatch, field):
    """Property: one edited digest in a record row is reported as the record having been edited, naming the row.

    Class: R alone differs, for either of the row's two digests (the cut and the stored text
    agree). Why it is required: the generator restores it. Killed by: checking only one of the
    two digests. Second correct implementation: comparing the two digests with each other
    first passes.
    """
    root = _copy(tmp_path, monkeypatch)
    row = _rows(root)[3]
    label = row[field]
    _rewrite_record(root, label, label[:-1] + ("0" if label[-1] != "0" else "1"))
    finding = _one(extraction_check.extraction_findings(root), f"{row['file']}:{row['first']}-{row['last']}")
    assert "record was edited" in finding


def test_a_stored_text_and_its_digest_edited_to_different_values_fail_naming_both(tmp_path, monkeypatch):
    """Property: when the stored text and its recorded digest are both edited to unequal values, the finding names the stored text and the record row.

    Class: all three differ. Why it is required: neither the copy nor the record can be
    trusted, and the message must say so. Killed by: calling this a stored-text edit alone,
    which is what comparing S with the cut and R with the cut separately gives. Second
    correct implementation: a coherence test of the record's two digests first passes.
    """
    root = _copy(tmp_path, monkeypatch)
    row = _rows(root)[11]
    _flip_first_byte(root / row["path"])
    label = row["stored"]
    _rewrite_record(root, label, label[:-1] + ("0" if label[-1] != "0" else "1"))
    finding = _one(extraction_check.extraction_findings(root), row["path"])
    assert f"record row {row['file']}:{row['first']}-{row['last']}" in finding
    assert "all differ" in finding


def test_a_label_under_an_older_rule_version_fails_naming_the_label(tmp_path, monkeypatch):
    """Property: a label recorded under another rule version is a finding naming the label, with no comparison of the three made.

    Class: a record row whose `cut` label reads version 0. Why it is required: a digest under
    another rule cannot be checked against a cut made under this one. Killed by: comparing the
    digest part only. Second correct implementation: comparing the whole label string with the
    one the current rule would give passes.
    """
    root = _copy(tmp_path, monkeypatch)
    row = _rows(root)[2]
    old = row["cut"].replace("cut-lines:1:", "cut-lines:0:")
    _rewrite_record(root, row["cut"], old)
    finding = _one(extraction_check.extraction_findings(root), old)
    assert "re-run under the new rule" in finding


def test_a_scope_in_range_with_no_record_row_fails_by_name(tmp_path, monkeypatch):
    """Property: deleting a record row leaves a finding naming the range that has no row.

    Class: a scope-in key's range absent from the record. Why it is required: the check must
    range over the ledger's population and not over the record's own rows. Killed by: looping
    over the record only. Second correct implementation: comparing the two sets of ranges
    passes.
    """
    root = _copy(tmp_path, monkeypatch)
    row = _rows(root)[0]
    text = _record_text(root)
    block = re.search(
        r"\n\[\[extraction\]\]\nsection = [^\n]*\nfile = \""
        + re.escape(row["file"])
        + r"\"\nfirst = "
        + str(row["first"])
        + r"\nlast = "
        + str(row["last"])
        + r"\n[^\[]*",
        text,
    )
    (root / generate_extractions.RECORD_PATH).write_text(text.replace(block.group(0), "\n"), encoding="utf-8")
    finding = [f for f in extraction_check.extraction_findings(root) if "no record row" in f]
    assert len(finding) == 1 and f"{row['file']}:{row['first']}-{row['last']}" in finding[0]


def test_a_record_row_that_is_no_scope_in_keys_range_fails_by_name(tmp_path, monkeypatch):
    """Property: a record row for a range no scope-in key cites is a finding naming the row.

    Class: an extra row with an uncited range. Why it is required: a row left behind after a
    unit was re-ruled would otherwise be trusted. Killed by: looping over ledger ranges only.
    Second correct implementation: the same set comparison, run from the record side, passes.
    """
    root = _copy(tmp_path, monkeypatch)
    row = _rows(root)[0]
    text = _record_text(root)
    extra = text.rstrip("\n") + (
        f'\n\n[[extraction]]\nsection = "{row["section"]}"\nfile = "{row["file"]}"\nfirst = 9000\nlast = 9001\n'
        f'path = "{row["path"]}"\ncut = "{row["cut"]}"\nstored = "{row["stored"]}"\n'
    )
    (root / generate_extractions.RECORD_PATH).write_text(extra, encoding="utf-8")
    finding = _one(extraction_check.extraction_findings(root), f"{row['file']}:9000-9001")
    assert "not the range of any scope-in key" in finding


def test_a_stray_file_under_extractions_fails_by_name(tmp_path, monkeypatch):
    """Property: a file under `extractions/` that is neither recorded nor the licence is a finding naming it.

    Class: a text file at an unrecorded path and a file of another suffix. Why it is required:
    an orphan copy would be trusted by whoever reads the directory. Killed by: listing `.txt`
    files only. Second correct implementation: comparing the directory listing with the
    record's paths passes.
    """
    root = _copy(tmp_path, monkeypatch)
    (root / extraction.STORED_DIR / "stormy" / "stray.txt").write_bytes(b"x\n")
    (root / extraction.STORED_DIR / "NOTE.md").write_bytes(b"x\n")
    findings = extraction_check.extraction_findings(root)
    assert len(findings) == 2, "\n".join(findings)
    assert any("stormy/stray.txt" in f for f in findings) and any("NOTE.md" in f for f in findings)


def test_a_missing_licence_fails(tmp_path, monkeypatch):
    """Property: removing `extractions/LICENSE` is a finding naming it.

    Class: the licence absent while every stored text is intact. Why it is required: the MIT
    notice must sit beside the copies. Killed by: not checking for it, or treating it as an
    optional file. Second correct implementation: a presence test on the path passes.
    """
    root = _copy(tmp_path, monkeypatch)
    (root / extraction.STORED_DIR / "LICENSE").unlink()
    _one(extraction_check.extraction_findings(root), f"{extraction.STORED_DIR}/LICENSE")


def test_a_missing_stored_text_fails_by_name(tmp_path, monkeypatch):
    """Property: a recorded stored text that is not on disk is a finding naming it.

    Class: one deleted stored text. Why it is required: reading it would raise, and a raised
    error names no cause. Killed by: letting the read raise. Second correct implementation:
    checking existence before the read passes.
    """
    root = _copy(tmp_path, monkeypatch)
    row = _rows(root)[9]
    (root / row["path"]).unlink()
    finding = _one(extraction_check.extraction_findings(root), row["path"])
    assert "missing" in finding


def test_an_edited_legacy_byte_makes_every_check_skip_while_the_corpus_record_fails(tmp_path, monkeypatch):
    """Property: one edited byte in a cited legacy file makes `require_extractions` skip with the corpus record's not-measured sentence, and the corpus record's own check fails.

    Class: the corpus moved. Why it is required: the corpus record certifies every corpus
    file's bytes, so the cause must be reported once, by that record, and not once per stored
    text. Killed by: reporting a mismatch per stored text, or skipping with another sentence.
    Second correct implementation: calling the record's check before reading any stored text
    passes.
    """
    root = _copy(tmp_path, monkeypatch)
    _flip_first_byte(root / "_old_code_to_refactor" / "_inputs.py")
    assert corpus_record.record_findings(root)
    with pytest.raises(pytest.skip.Exception, match=re.escape(corpus_record.NOT_MEASURED)):
        extraction_check.require_extractions(root)


def test_a_broken_key_resolution_makes_every_check_skip_with_the_extent_sentence(tmp_path, monkeypatch):
    """Property: when the extent check has a finding, `require_extractions` skips with the extent check's not-measured sentence.

    Class: the partition check failing while the corpus record passes. Why it is required: a
    stored text can only be matched to a unit through a resolving key. Killed by: calling the
    corpus record only. Second correct implementation: calling `extent.require_extent` and
    nothing else passes, since it calls the record itself. The finding is supplied by
    replacing `extent.check`, which stands for a partition failure that no edit of the copy
    can cause while the corpus record still passes.
    """
    root = _copy(tmp_path, monkeypatch)
    monkeypatch.setattr(extent, "check", lambda r=None: (["_utils.py: 1 line(s) in no unit"], {}))
    with pytest.raises(pytest.skip.Exception, match=re.escape(extent.NOT_MEASURED)):
        extraction_check.require_extractions(root)


def test_an_extraction_finding_makes_the_callers_skip_but_not_the_finder(tmp_path, monkeypatch):
    """Property: with a stored text edited, `require_extractions` skips with this check's sentence while `extraction_findings` returns the finding.

    Class: a finding of the check's own. Why it is required: callers below the check must not
    run on copies that cannot be trusted, and the finder must stay readable. Killed by:
    skipping inside `extraction_findings`, or not skipping callers. Second correct
    implementation: a skip raised by a wrapper around the finder passes.
    """
    root = _copy(tmp_path, monkeypatch)
    _flip_first_byte(root / _rows(root)[1]["path"])
    assert len(extraction_check.extraction_findings(root)) == 1
    with pytest.raises(pytest.skip.Exception, match=re.escape(extraction_check.NOT_MEASURED)):
        extraction_check.require_extractions(root)


def test_a_unit_restricts_the_findings_to_its_own_stored_texts(tmp_path, monkeypatch):
    """Property: with `unit` given, only that key's stored texts are checked, and an unknown unit is a finding.

    Class: one stored text edited, queried by its own key, by another key, and by a name that
    is no scope-in key. Why it is required: a test of one unit must not fail for another's
    copy. Killed by: ignoring `unit`. Second correct implementation: filtering the whole-tree
    findings by path passes for the first two queries.
    """
    root = _copy(tmp_path, monkeypatch)
    keys = generate_extractions.population(root)[0]
    row = _rows(root)[0]
    own = next(k for k in keys if (row["file"], row["first"], row["last"]) in generate_extractions._ranges(k))
    other = next(k for k in keys if k != own)
    _flip_first_byte(root / row["path"])
    assert len(extraction_check.extraction_findings(root, unit=own)) == 1
    assert extraction_check.extraction_findings(root, unit=other) == []
    assert "not a scope-in key" in extraction_check.extraction_findings(root, unit="nope.py:1-2")[0]


def test_a_duplicated_faithful_record_row_is_a_finding_naming_the_range(tmp_path, monkeypatch):
    """Property: a second, byte-identical record row for one range is a finding naming the range and the row count, and nothing else.

    Class: a duplicate that is faithful in every digest, which a row-by-row comparison would
    pass. Why it is required: the generator writes one row per range, so a second row is an
    edit even when it agrees; and an edited duplicate is covered by the reviewer's held test.
    Killed by: comparing every row as read without counting rows. Second correct
    implementation: comparing every row as read and also counting rows passes. Not
    established: that a record may never hold several rows per range for another purpose.
    """
    root = _copy(tmp_path, monkeypatch)
    row = _rows(root)[6]
    block = (
        f'\n[[extraction]]\nsection = "{row["section"]}"\nfile = "{row["file"]}"\nfirst = {row["first"]}\n'
        f'last = {row["last"]}\npath = "{row["path"]}"\ncut = "{row["cut"]}"\nstored = "{row["stored"]}"\n'
    )
    path = root / generate_extractions.RECORD_PATH
    path.write_text(path.read_text(encoding="utf-8").rstrip("\n") + "\n" + block, encoding="utf-8")
    finding = _one(extraction_check.extraction_findings(root), f"{row['file']}:{row['first']}-{row['last']}")
    assert "2 record rows for one range" in finding
