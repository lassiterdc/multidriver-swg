"""Reviewer's test of the extraction generator: a scope-in row whose key is not a string is refused by name.

The generator reads the ledger's files directly rather than through the ledger's validating
loader, so a malformed row reaches it, and it promises to refuse such a key by name before it
writes anything. This test gives it the malformed row in the position that separates that
refusal from a crash: beside well-formed string keys.

The test builds a git repository inside `tmp_path` holding a one-section corpus record, one
retained file, a one-file ledger and the licence beside the stored copies, staged and never
committed. The git location variables a hook exports are removed first, because a lookup that
inherits `GIT_DIR` reads the repository that exported it and not the temporary one.
"""

import subprocess
from pathlib import Path

import pytest

from tests.correspondence import corpus_record, extent, extraction, generate_extractions

pytestmark = pytest.mark.state_check

GIT_LOCATION_VARS = ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR", "GIT_PREFIX")
SOURCE = b"def f():\n    return 1\n\n\ndef g():\n    return 2\n"
RECORD = """rule = "corpus-record:1"

[[section]]
name = "alpha"
directory = "legacy_a"

[[section.file]]
path = "mod_a.py"
group = "retained"
"""


def _repository(tmp_path: Path, monkeypatch, extra_row: str) -> Path:
    """Build a repository whose ledger holds the two string keys of `mod_a.py` and then `extra_row`."""
    for var in GIT_LOCATION_VARS:
        monkeypatch.delenv(var, raising=False)
    (tmp_path / corpus_record.RECORD_NAME).write_text(RECORD, encoding="utf-8")
    (tmp_path / "legacy_a").mkdir()
    (tmp_path / "legacy_a" / "mod_a.py").write_bytes(SOURCE)
    found, _ = extent.extents("mod_a.py", SOURCE.decode())
    rows = "".join(f'[[row]]\nkey = "{f["key"]}"\ndisposition = "scope-in"\nreason = "r"\n\n' for f in found)
    ledger_dir = tmp_path / "tests" / "correspondence" / "ledger"
    ledger_dir.mkdir(parents=True)
    (ledger_dir / "mod_a.toml").write_text(f'legacy_file = "mod_a.py"\n\n{rows}{extra_row}', encoding="utf-8")
    (tmp_path / extraction.STORED_DIR).mkdir(parents=True)
    (tmp_path / extraction.STORED_DIR / "LICENSE").write_bytes(b"MIT notice\n")
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "--", "."], cwd=tmp_path, check=True)
    return tmp_path


def _files(root: Path) -> dict[str, bytes]:
    """Return every file under the stored-copy directory plus the record, by repository-relative path."""
    paths = [p for p in (root / extraction.STORED_DIR).rglob("*") if p.is_file()]
    record = root / generate_extractions.RECORD_PATH
    paths += [record] if record.is_file() else []
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in paths}


@pytest.mark.parametrize(
    ("extra_row", "named"),
    [
        ('[[row]]\nkey = 7\ndisposition = "scope-in"\nreason = "r"\n\n', "7"),
        ('[[row]]\ndisposition = "scope-in"\nreason = "r"\n\n', "None"),
    ],
    ids=["integer-key", "missing-key"],
)
def test_a_scope_in_row_whose_key_is_not_a_string_is_refused_by_name_and_nothing_is_written(
    tmp_path, monkeypatch, extra_row, named
):
    """Property: a scope-in row whose key is an integer or is absent is refused naming the key, the run returns 1, and nothing is written.

    Class: a non-string key in a ledger that also holds well-formed string keys, which is the
    case where ordering the keys meets two incomparable types. Why it is required: the
    generator reads the ledger without its validating loader precisely so that a malformed row
    is refused by name; a crash instead raises `TypeError` out of `main`, which catches only
    the refusal, so the run ends in a traceback naming no key. Killed by: sorting the keys
    before checking that each is a string, which raises `TypeError` on this input. Second
    correct implementation: sorting with `key=str`, or filtering and refusing non-string keys
    before any sort, passes.
    """
    root = _repository(tmp_path, monkeypatch, extra_row)
    before = _files(root)
    with pytest.raises(extraction.ExtractionError, match=named):
        generate_extractions.generate(root)
    assert _files(root) == before
    assert generate_extractions.main(root) == 1
