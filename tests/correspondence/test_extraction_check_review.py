"""Reviewer's test of the extraction check: a record row for a range already recorded is still checked.

The check matches record rows to ranges through a mapping keyed by (section, file, first, last).
A record holding two rows for one range keeps only one of them in that mapping, so the other
is never compared with the cut or the stored text. If the dropped row carries an edited digest,
an edited record passes the check. This test places the edited duplicate both before and after
the faithful row, because only one of the two positions is the one the mapping drops.

The test runs on a temporary copy of the corpus, its record, the ledger, the stored texts and
the extraction record, held in a git repository inside `tmp_path`, staged and never committed.
The git location variables a hook exports are removed first, because a lookup that inherits
`GIT_DIR` reads the repository that exported it and not the temporary one.
"""

import shutil
import subprocess
import tomllib
from pathlib import Path

import pytest

from tests.correspondence import corpus_record, extraction, extraction_check, generate_extractions

pytestmark = pytest.mark.state_check

GIT_LOCATION_VARS = ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR", "GIT_PREFIX")
REAL = extraction_check.REPO_ROOT


def _copy(tmp_path: Path, monkeypatch) -> Path:
    """Copy the corpus, its record, the ledger, the stored texts and the extraction record into a staged repository."""
    for var in GIT_LOCATION_VARS:
        monkeypatch.delenv(var, raising=False)
    for rel in ("_old_code_to_refactor", "tests/correspondence/ledger", extraction.STORED_DIR):
        shutil.copytree(REAL / rel, tmp_path / rel)
    for rel in (corpus_record.RECORD_NAME, generate_extractions.RECORD_PATH):
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REAL / rel, tmp_path / rel)
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "--", "."], cwd=tmp_path, check=True)
    return tmp_path


@pytest.mark.parametrize("edited_first", [True, False], ids=["edited-row-before-the-faithful-one", "edited-row-after"])
def test_a_second_record_row_for_one_range_with_an_edited_digest_is_a_finding(tmp_path, monkeypatch, edited_first):
    """Property: a record holding an extra row for an already-recorded range, with an edited digest, has a finding naming that range.

    Class: a duplicated record row whose `stored` digest differs from the faithful row's by one
    character, placed before and after the faithful row. Why it is required: the check exists to
    report an edited record, and a duplicate row with an edited digest is an edited record;
    a mapping that keeps the last row per range compares only that one, so an edit in the
    earlier row is never read. Killed by: collapsing the rows into a mapping keyed by range,
    which reports nothing when the edited row comes first. Second correct implementation:
    comparing every row as read, or reporting any range that has more than one row as its own
    finding, passes both positions.
    """
    root = _copy(tmp_path, monkeypatch)
    record = root / generate_extractions.RECORD_PATH
    text = record.read_text(encoding="utf-8")
    row = tomllib.loads(text)["extraction"][4]
    edited = row["stored"][:-1] + ("0" if row["stored"][-1] != "0" else "1")
    block = (
        f'[[extraction]]\nsection = "{row["section"]}"\nfile = "{row["file"]}"\nfirst = {row["first"]}\n'
        f'last = {row["last"]}\npath = "{row["path"]}"\ncut = "{row["cut"]}"\nstored = "{edited}"\n'
    )
    if edited_first:
        head, marker, rest = text.partition("[[extraction]]")
        text = f"{head}{block}\n{marker}{rest}"
    else:
        text = f"{text.rstrip(chr(10))}\n\n{block}"
    record.write_text(text, encoding="utf-8")
    assert len(tomllib.loads(text)["extraction"]) == 131
    findings = extraction_check.extraction_findings(root)
    assert any(f"{row['file']}:{row['first']}-{row['last']}" in f for f in findings), findings
