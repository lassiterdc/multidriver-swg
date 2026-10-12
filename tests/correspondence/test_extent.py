"""Tests of the extent rule: every retained legacy line lies in exactly one unit, and a key resolves only to a unit's exact ranges."""

import hashlib
import subprocess
from pathlib import Path

import pytest

from tests.correspondence import corpus_record, extent

pytestmark = pytest.mark.state_check

D_FILE = "_d_build_water_level_generator.py"


def test_every_retained_line_is_in_exactly_one_unit():
    """Property: the six-part rule puts each of the 8,020 retained lines in exactly one of the 188 units (arm row 4).

    Class: every line of every retained file, on the real tree. Why: a unit key is a citation
    only if lines and units partition each file. Killed by: removing part 3 (59 lines below a
    cell's last unit, and the empty cell's marker, then fall in no unit). Second correct
    implementation: one assigning part 3's lines in a separate pass after building all keys
    still yields the same partition.
    """
    findings, totals = extent.check()
    assert not findings, "\n".join(findings)
    assert (totals["lines"], totals["units"], totals["none"], totals["multi"]) == (8020, 188, 0, 0)


def test_a_key_resolves_only_to_a_units_exact_ranges():
    """Property: a canonical key resolves to its unit's lines, and a key one line short resolves to nothing (extent's half of arm row 5).

    Class: any key naming a retained file with ranges no unit has. Why: a ledger row or a test
    citing wrong ranges must not resolve. Killed by: resolving keys by file only. Second
    correct implementation: a dictionary from key to lines built once per file.
    """
    lines = extent.resolve(f"{D_FILE}:154-216")
    assert lines == list(range(154, 217))
    assert extent.resolve(f"{D_FILE}:154-215") is None


CORPUS = "_old_code_to_refactor"
FILE = "_z_synthetic.py"
TEXT = "# a comment above the first cell marker\n#%%\nx = 1\n"


def _synthetic_record(digest: str, lines: int, set_digest: str) -> str:
    section = "\n".join(
        f'{field} = "{value}"'
        for field, value in [
            ("name", "synthetic"),
            ("directory", CORPUS),
            ("upstream", "https://example.invalid/synthetic"),
            ("upstream_branch", "main"),
            ("revision", "0" * 40),
            ("upstream_path", "synthetic"),
            ("candidate_rule", "top-level .py files"),
            ("extent_rule", "ADR-1, six parts"),
            ("set_digest", set_digest),
            ("verification_date", "2026-10-12"),
            ("verification_command", "none"),
            ("verification_result", "synthetic"),
        ]
    )
    return (
        f'rule = "{corpus_record.RECORD_RULE}"\n\n[[section]]\n{section}\nunparseable_candidate_files = 0\n\n'
        f'[[section.file]]\npath = "{FILE}"\nrole = "candidate"\ngroup = "retained"\nupstream_sourced = true\n'
        f'sha256 = "{digest}"\nlines = {lines}\nruns = "UNESTABLISHED"\nruns_ground = "synthetic"\n'
    )


def _synthetic_corpus(tmp_path: Path) -> Path:
    (tmp_path / CORPUS).mkdir()
    (tmp_path / CORPUS / FILE).write_text(TEXT, encoding="utf-8")
    digest = hashlib.sha256(TEXT.encode("utf-8")).hexdigest()
    manifest = f"{digest}  {FILE}\n"
    set_digest = corpus_record.SET_DIGEST_PREFIX + hashlib.sha256(manifest.encode("utf-8")).hexdigest()
    (tmp_path / corpus_record.RECORD_NAME).write_text(
        _synthetic_record(digest, len(TEXT.splitlines()), set_digest), encoding="utf-8"
    )
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "-A"], cwd=tmp_path, check=True)
    return tmp_path


def test_a_failed_extent_check_skips_dependents_in_its_own_name(tmp_path):
    """Reviewer test for chunk 1a.4, ported: the extent link of the check chain reports "not measured" in its own name.

    Property: when the corpus record passes and the extent check fails, `require_extent()` skips the
    calling test with the prefix `NOT MEASURED: extent check failed` rather than letting it run.
    Class (i): every corpus whose record matches its tracked files while some retained line lies in
    no unit or in two units. A comment line above a retained file's first cell marker is one such
    input, because none of the six parts of the extent rule assigns it.
    Why (iii): a check below the extent check (the ledger's check 1, the known-defect register's key
    resolution) that ran on a broken partition would report hundreds of unit-level failures whose
    one cause is the partition, which is the condition under which a check gets switched off.
    Killed by (iv): `require_extent` returning without skipping when `check()` has findings, or
    skipping with the corpus record's prefix instead of its own.
    Second correct implementation: one that raises pytest's skip from a fixture wrapping the chain,
    or that caches `check()`'s result per root, still passes, because the assertions read only the
    skip and its message prefix.
    """
    root = _synthetic_corpus(tmp_path)
    assert corpus_record.record_findings(root) == []  # the record link passes: the skip below is the extent's
    findings, _totals = extent.check(root)
    assert any(FILE in f for f in findings), findings  # the extent link fails, naming the file
    with pytest.raises(pytest.skip.Exception) as skipped:
        extent.require_extent(root)
    assert str(skipped.value).startswith(extent.NOT_MEASURED)
