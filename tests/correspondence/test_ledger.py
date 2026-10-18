"""Tests of the disposition ledger: check 1 on the real tree, red while any unit has no row, and one violating case per ledger refusal.

Fixtures copy the record, the tracked corpus, the ledger and the system-scope file into a
git repository inside `tmp_path` (staged, never committed), then edit one ledger file.
"""

import shutil
import subprocess
from pathlib import Path

import pytest

from tests.correspondence import corpus_record, ledger

pytestmark = pytest.mark.state_check

D_TOML = "_d_build_water_level_generator.toml"


def _ledger_fixture(tmp_path: Path) -> Path:
    """Copy every input the ledger checks read into a staged git repository at `tmp_path`; return it."""
    root = corpus_record.REPO_ROOT
    listed = subprocess.run(
        ["git", "ls-files", "-z", "--", "_old_code_to_refactor"], cwd=root, capture_output=True, check=True
    ).stdout.decode("utf-8")
    paths = [p for p in listed.split("\0") if p]
    paths += [corpus_record.RECORD_NAME, "tests/correspondence/system_scope.toml"]
    paths += [str(p.relative_to(root)) for p in (root / ledger.LEDGER_DIR).glob("*.toml")]
    for rel in paths:
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(root / rel, tmp_path / rel)
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "-A"], cwd=tmp_path, check=True)
    return tmp_path


def _edit(root: Path, name: str, old: str, new: str) -> None:
    """Replace exactly one occurrence of `old` in one ledger file."""
    path = root / ledger.LEDGER_DIR / name
    text = path.read_text(encoding="utf-8")
    assert text.count(old) == 1, old
    path.write_text(text.replace(old, new), encoding="utf-8")


def _names(finding: str, *tokens: str) -> bool:
    """Whether the finding names every token as a whole word, never as a substring.

    Keys and line ranges can contain one another (`:1-18` inside `:1-180`), so a substring test
    would pass on a finding that names a different unit or run.
    """
    words = {word.strip("'\":,") for word in finding.split()}
    return set(tokens) <= words


def test_check_1_one_row_per_unit():
    """Property: the real ledger holds exactly one row per unit — RED while any unit has no row.

    Class: every unit of the 188. Why: a unit with no recorded disposition is the defect the
    ledger exists to expose, and the message counts check 1's units with no row and its units with
    two or more rows, so a reader of the red sees which kind failed; a row naming no unit appears
    only in the listed findings. Killed by: none while it is red — it is expected to fail until
    every unit has a row, and the duplicate-row and no-unit tests below grade check 1's code.
    Second correct implementation: comparing a Counter of row keys with the unit list.
    """
    findings = ledger.check_1()
    no_row = sum(1 for f in findings if f.endswith(": no row"))
    two_or_more = sum(1 for f in findings if f.endswith(" rows"))
    assert not findings, f"units with no row: {no_row}; units with two or more rows: {two_or_more}\n" + "\n".join(
        findings
    )


def test_a_unit_with_two_rows_is_named(tmp_path):
    """Property: a duplicated row is reported naming its unit.

    Class: any unit with two or more rows. Killed by: counting distinct keys instead of rows.
    Second correct implementation: a Counter over row keys.
    """
    root = _ledger_fixture(tmp_path)
    text = (root / ledger.LEDGER_DIR / D_TOML).read_text(encoding="utf-8")
    first = text[text.index("[[row]]") : text.index("[[row]]", text.index("[[row]]") + 1)]
    (root / ledger.LEDGER_DIR / D_TOML).write_text(text + "\n" + first, encoding="utf-8")
    findings = ledger.check_1(root)
    assert any(f.endswith(" rows") and _names(f, "_d_build_water_level_generator.py:1-18") for f in findings), findings


def test_a_non_canonical_key_is_named(tmp_path):
    """Property: a key with a descending range is a format finding naming the row.

    Class: any key not in `file:a-b[,c-d]` ascending form. Killed by: accepting any range order.
    Second correct implementation: parse and re-format the key, and compare.
    """
    root = _ledger_fixture(tmp_path)
    _edit(
        root,
        D_TOML,
        'key = "_d_build_water_level_generator.py:154-216"',
        'key = "_d_build_water_level_generator.py:216-154"',
    )
    assert any(
        _names(f, "_d_build_water_level_generator.py:216-154") and "canonical" in f for f in ledger.load(root)[2]
    )


def _cite(root, scope_id: str) -> list[str]:
    """Make the fixture's first row scope-in, citing `scope_id`, and return the ledger's format findings."""
    _edit(
        root,
        D_TOML,
        'key = "_d_build_water_level_generator.py:1-18"\ndisposition = "discard"',
        f'key = "_d_build_water_level_generator.py:1-18"\ndisposition = "scope-in"\nscope = "{scope_id}"',
    )
    return ledger.load(root)[2]


def test_a_scope_in_row_citing_no_statement_is_named(tmp_path):
    """Property: a scope-in row whose `scope` names no statement of the system-scope file is named by its key.

    Class: any identifier no statement carries. Why: such a row claims an owning scope that does
    not exist, so nothing could check the tests it is routed to. Killed by: skipping the citation
    check. Second correct implementation: a set of the file's identifiers built once per load.
    """
    root = _ledger_fixture(tmp_path)
    findings = _cite(root, "dependence-modelling/00000000")
    assert any(_names(f, "_d_build_water_level_generator.py:1-18") and "names no statement" in f for f in findings)


def test_a_scope_in_row_citing_an_excludes_statement_is_named(tmp_path):
    """Property: a scope-in row whose `scope` names an excludes statement is named by its key.

    Class: any excludes statement of any system. Why: an excludes statement says what a system
    does not own, so it cannot be the scope a unit is routed into. Killed by: accepting an
    excludes citation. Second correct implementation: a set of covers identifiers built once.
    """
    root = _ledger_fixture(tmp_path)
    from tests.correspondence import system_scope

    against = next(e["id"] for s in system_scope.load(root)[0] for e in s["statement"] if e["mark"] == "excludes")
    findings = _cite(root, against)
    assert any(_names(f, "_d_build_water_level_generator.py:1-18") and "excludes statement" in f for f in findings)


def test_a_ceded_group_without_its_record_is_named(tmp_path):
    """Property: a ceded file with no record in ceded_files.toml is named.

    Class: any ceded file the ceded records omit. Killed by: taking the ceded set from
    `ceded_files.toml` alone, without comparing it to the corpus record's `ceded` group. Second
    correct implementation: compare record sets both ways.
    """
    root = _ledger_fixture(tmp_path)
    path = root / ledger.LEDGER_DIR / ledger.CEDED_FILE
    text = path.read_text(encoding="utf-8")
    block = text[: text.index("[[ceded_file]]", 1)]
    assert "_a2_dwnld_aorc_pre_mrms.py" in block
    path.write_text(text.replace(block, ""), encoding="utf-8")
    assert any(f.startswith("_a2_dwnld_aorc_pre_mrms.py:") for f in ledger.load(root)[2])


def test_a_retained_group_without_its_ledger_file_is_named(tmp_path):
    """Property: a retained file with no per-script ledger file is named.

    Class: any retained file whose ledger file is missing. Killed by: dropping the ledger-files
    comparison. Second correct implementation: compare file stems with the retained set.
    """
    root = _ledger_fixture(tmp_path)
    (root / ledger.LEDGER_DIR / "_b2_event_selection.toml").unlink()
    assert any(f.startswith("_b2_event_selection.py:") for f in ledger.load(root)[2])


def test_a_scope_out_without_a_destination_is_named(tmp_path):
    """Property: a scope-out with no destination is a check-2 finding naming it.

    Class: any scope-out lacking `destination`. Killed by: dropping the destination test. Second
    correct implementation: a required-field table per disposition.
    """
    root = _ledger_fixture(tmp_path)
    _edit(
        root,
        ledger.CEDED_FILE,
        'legacy_file = "_c_gen_ncei_hrly_and_daily_data.py"\ndisposition = "scope-out"\ndestination = "hydro-fetch"\n',
        'legacy_file = "_c_gen_ncei_hrly_and_daily_data.py"\ndisposition = "scope-out"\n',
    )
    assert any(f.startswith("_c_gen_ncei_hrly_and_daily_data.py:") and "destination" in f for f in ledger.check_2(root))


def test_a_member_scope_out_without_acceptance_is_named(tmp_path):
    """Property: a scope-out to a member with no acceptance state is named.

    Class: any member destination lacking `acceptance`. Killed by: dropping the acceptance test.
    Second correct implementation: validate acceptance as `OPEN` or a path-and-commit table.
    """
    root = _ledger_fixture(tmp_path)
    text = (root / ledger.LEDGER_DIR / ledger.CEDED_FILE).read_text(encoding="utf-8")
    start = text.index('legacy_file = "_c_gen_ncei_hrly_and_daily_data.py"')
    _edit(
        root,
        ledger.CEDED_FILE,
        text[start : text.index('acceptance = "OPEN"\n', start) + 20],
        text[start : text.index('acceptance = "OPEN"\n', start)],
    )
    assert any(f.startswith("_c_gen_ncei_hrly_and_daily_data.py:") and "acceptance" in f for f in ledger.check_2(root))


def test_an_unacknowledged_run_is_named(tmp_path):
    """Property: a row missing one detected run is named with the run.

    Class: any run inside a unit its row omits. Killed by: comparing in one direction only.
    Second correct implementation: symmetric set difference reported per side.
    """
    root = _ledger_fixture(tmp_path)
    _edit(root, D_TOML, '  { lines = "82-84", treatment = "discarded with the unit" },\n', "")
    assert any(
        _names(f, "_d_build_water_level_generator.py:80-85", "82-84") and "does not acknowledge" in f
        for f in ledger.check_3(root)
    )


def test_a_stale_acknowledgement_is_named(tmp_path):
    """Property: an acknowledged range that is no run of the detector is named.

    Class: any acknowledged range the predicate does not find. Killed by: comparing in the other
    direction only. Second correct implementation: symmetric set difference.
    """
    root = _ledger_fixture(tmp_path)
    _edit(
        root,
        D_TOML,
        '  { lines = "155-158", treatment = "discarded with the unit" },\n',
        '  { lines = "155-157", treatment = "discarded with the unit" },\n',
    )
    assert any(
        _names(f, "_d_build_water_level_generator.py:154-216", "155-157") and "no run" in f
        for f in ledger.check_3(root)
    )


def test_a_row_naming_no_unit_is_named(tmp_path):
    """Property: a row whose key names no unit's ranges is named by check 1.

    Class: any key one range off a unit. Killed by: resolving keys by file only. Second correct
    implementation: compare row keys with the extent keys as sets.
    """
    root = _ledger_fixture(tmp_path)
    _edit(
        root,
        D_TOML,
        'key = "_d_build_water_level_generator.py:154-216"',
        'key = "_d_build_water_level_generator.py:154-215"',
    )
    assert any(f == "row _d_build_water_level_generator.py:154-215: names no unit" for f in ledger.check_1(root))


def test_checks_other_than_check_1_are_clean_on_the_real_ledger():
    """Property: on the real ledger, every whole-ledger check other than check 1 reports nothing.

    The committed ledger produces no format finding, no check-2 finding and no check-3 finding;
    only check 1 is red, and it stays red while any unit has no row.
    Class: every correct ledger, however many units have rows so far. Rows added later must still be
    well formed, still name a destination and an acceptance state for each scope-out, and still
    acknowledge exactly the detected runs, so the property never becomes false on a correct ledger.
    Why: check 1 is red while any unit has no row, so the State check job's verdict is "failure" whatever the
    other checks do. Its one discriminating signal is WHICH tests fail. A check that wrongly reports
    findings on the real rows changes nothing in that signal unless some test runs it on the real
    rows; without this test the job reads "1 failed, check 1" whether checks 2, 3 and the format
    check are correct or are reporting every row.
    Killed by: any change that makes one of those checks report a finding on a correct row —
    for example treating every member scope-out as lacking an acceptance state, refusing every key as
    non-canonical, comparing a row's acknowledged runs with every run of its file rather than of its
    unit, or adding a finding per row in check 3.
    Second correct implementation: a ledger reader that validates each file against a schema and
    computes runs once per file, bucketing them by unit, still passes, because the assertion reads
    only whether each check returns findings, not how it computes them.
    """
    results = {
        "format": ledger.load()[
            2
        ],  # load()'s format findings, read directly so this test does not depend on WHOLE_LEDGER_CHECKS
        "check 2": ledger.check_2(),
        "check 3": ledger.check_3(),
    }
    failing = {name: found for name, found in results.items() if found}
    assert not failing, "\n".join(f"{name}: {len(found)} finding(s): {found[:5]}" for name, found in failing.items())
