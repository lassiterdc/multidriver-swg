"""Tests of the extent rule: every retained legacy line lies in exactly one unit, and a key resolves only to a unit's exact ranges."""

import hashlib
import subprocess
from pathlib import Path

import pytest

from tests.correspondence import corpus_record, extent

pytestmark = pytest.mark.state_check

D_FILE = "_d_build_water_level_generator.py"


def test_every_retained_line_is_in_exactly_one_unit():
    """Property: the six-part rule puts each of the 8,020 retained lines in exactly one of the 188 units.

    Class: every line of every retained file, on the real tree. Why: a unit key is a citation
    only if lines and units partition each file. Killed by: removing part 3 (59 lines, among
    them the empty cell's marker, then fall in no unit). Second correct
    implementation: one assigning part 3's lines in a separate pass after building all keys
    still yields the same partition.
    """
    findings, totals = extent.check()
    assert not findings, "\n".join(findings)
    assert (totals["lines"], totals["units"], totals["none"], totals["multi"]) == (8020, 188, 0, 0)


def test_a_key_resolves_only_to_a_units_exact_ranges():
    """Property: a canonical key resolves to its unit's lines, and a key one line short resolves to nothing.

    This is the extent side of a key naming no unit; the ledger side is `test_ledger.py`'s
    `test_a_row_naming_no_unit_is_named`.

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
    """The extent link of the check chain reports "not measured" in its own name.

    Property: when the corpus record passes and the extent check fails, `require_extent()` skips the
    calling test with the prefix `NOT MEASURED: extent check failed` rather than letting it run.
    Class: every corpus whose record matches its tracked files while some retained line lies in
    no unit or in two units. A comment line above a retained file's first cell marker is one such
    input, because none of the six parts of the extent rule assigns it.
    Why: a check below the extent check that ran on a broken partition would report hundreds of
    unit-level failures whose one cause is the partition, which is the condition under which a
    check gets switched off. The dependent this skip guards today is the known-defect register's
    key resolution, whose test in `test_known_defects.py` calls `require_extent()` first; the
    ledger's check 1 also resolves keys against the partition but does not call the skip.
    Killed by: `require_extent` returning without skipping when `check()` has findings, or
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


M11_KEYS = {
    f"{D_FILE}:{ranges}"
    for ranges in (
        "1-18",
        "19-27",
        "28-79",
        "80-85",
        "86-100",
        "101-104",
        "105-116",
        "117-127,131-136",
        "128-130",
        "137-144",
        "145-153",
        "154-216",
        "217-241",
        "242-251",
        "252-265",
        "266-271",
        "272-292",
        "293-350",
        "351-363",
        "364-375",
        "376-377",
        "378-381",
        "382-385",
        "386-392",
        "393-396",
        "397-426",
        "427-443",
        "444-449",
        "450-457",
        "458-580",
        "581-601",
    )
}


def test_the_d_file_keys_equal_the_independently_computed_keys():
    """Property: the 31 unit keys the extent rule gives `_d_build_water_level_generator.py` equal the 31 keys of an independent oracle.

    The oracle is `M11_KEYS`, a literal set of keys produced outside this module by a separate
    construction and copied in, so no change to `extent.py` can change it. Not established here:
    how those keys were constructed, since that construction is not in this repository.
    Class: every unit key of that file, i.e. which line belongs to which unit, not only whether
    every line belongs to exactly one.
    Why: the partition's totals stay 8,020 lines, 188 units, 0 unowned and 0 doubly owned under
    a change to which unit a blank or comment line joins (part 1 of the rule), yet such a change
    moves 93 of the 188 keys. The ledger's rows and every later test cite these keys and are computed
    from this same module, so without an independent oracle the change agrees with itself everywhere.
    Killed by: removing part 1 of the rule (blank and comment lines above a unit joining that
    unit), which moves keys of this file while every total is unchanged.
    Second correct implementation: one that builds the partition in a single pass, assigning each
    line by scanning forward to the next unit in its own cell, still produces these keys and passes.
    """
    found, _counts = extent.extents(D_FILE, extent._read(extent.REPO_ROOT, D_FILE))
    keys = {f["key"] for f in found}
    assert keys == M11_KEYS, "\n".join(sorted(keys ^ M11_KEYS))
