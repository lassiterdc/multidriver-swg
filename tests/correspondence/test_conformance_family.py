"""The conformance family's harness split, probed end to end: registration, listing-versus-grading, and the independent control.

The change under test registers the `conformance(unit, system, part)` marker in the root configuration,
widens the static inventory to three families, and splits legacy grading from listing through
`ownership.LEDGER_FAMILIES`, while `test_ownership.py`'s decorator matcher stays restricted to
the two legacy families as an independent control on the filter. These tests are the
reviewer's probes of that split; a follow-up change extends this module with the family-selection
probes over the 44 applied markers. This module carries `state_check` only, never a family
marker, so it joins the State-check job without entering any family's inventory.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys

import pytest

from tests.correspondence import corpus_record, inventory, ownership, test_ownership

pytestmark = pytest.mark.state_check

REPO_ROOT = corpus_record.REPO_ROOT
SRC = REPO_ROOT / "src"


def _run_pytest(*args: str) -> subprocess.CompletedProcess:
    """Run pytest with the real root configuration against arguments, returning the completed process."""
    env = {**os.environ, "PYTHONPATH": str(SRC)}
    return subprocess.run(
        [sys.executable, "-m", "pytest", "-c", str(REPO_ROOT / "pyproject.toml"), *args],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )


def test_a_conformance_marked_test_collects_passes_and_selects_under_the_real_root_config(tmp_path):
    """Property: a function-local conformance decorator is legal under `strict_markers`,
    collected and passed by the real root configuration, and selected by `-m conformance`.

    Class: any test file holding a conformance-decorated function, run against this tree's
    own `pyproject.toml`; the inner tree lives only under `tmp_path`, so whole-suite
    collection never sees it. Why: the follow-up change decorates 44 existing functions with
    this marker, and a registration the root configuration does not carry turns every one of them
    into the collection error `'conformance' not found in markers` — the family split is only
    real if the marker is selectable on the tree the decorators land on. Killed by: removing
    the marker declaration from `pyproject.toml`'s `markers` list, which reproduces exactly
    that collection error (run against the HEAD pre-image config in the round file, exit 2).
    Second correct implementation: the same declaration with a different help string or the
    parenthesized arguments in another order — the assertion turns on collection succeeding
    under `-m conformance`, never on the declaration's prose.
    """
    inner = tmp_path / "inner"
    (inner / "tests").mkdir(parents=True)
    (inner / "tests" / "test_probe.py").write_text(
        "import pytest\n\n"
        '@pytest.mark.conformance(unit="equation-register", '
        'system="publication-math-conformance", part="probe")\n'
        "def test_probe():\n"
        "    assert True\n",
        encoding="utf-8",
    )
    plain = _run_pytest(
        "--rootdir", str(REPO_ROOT), str(inner / "tests" / "test_probe.py"), "-q", "-p", "no:cacheprovider"
    )
    assert plain.returncode == 0, plain.stderr + plain.stdout
    assert "1 passed" in plain.stdout
    selected = _run_pytest(
        "--rootdir",
        str(REPO_ROOT),
        "-m",
        "conformance",
        str(inner / "tests" / "test_probe.py"),
        "-q",
        "-p",
        "no:cacheprovider",
    )
    assert selected.returncode == 0, selected.stderr + selected.stdout
    assert "1 passed" in selected.stdout
    legacy = _run_pytest(
        "--rootdir",
        str(REPO_ROOT),
        "-m",
        "equivalence",
        str(inner / "tests" / "test_probe.py"),
        "-q",
        "-p",
        "no:cacheprovider",
    )
    assert legacy.returncode == 5, legacy.stderr + legacy.stdout
    assert "1 deselected" in legacy.stdout


def test_the_inventory_lists_three_families_while_ownership_grades_only_the_legacy_two(tmp_path):
    """Property: on a synthetic tree holding one conformance and one equivalence test,
    `inventory.listing` admits both entries while `ownership._family_tests` admits only the
    equivalence one.

    Class: any test tree mixing a conformance-decorated test with legacy-family tests. Why:
    decision 2's split is that listing covers three families while checks 4 to 7 grade two; a
    filter grading the whole listing would grade the conformance test against a ledger no row
    of which carries its unit key and refuse it for that key, so the listed-not-graded
    differential is the split's functional content — and the widening must not move the
    graded population. Killed by: deleting the `LEDGER_FAMILIES` filter so `_family_tests`
    consumes the whole listing (the graded count becomes 2 on this tree). Second correct
    implementation: a filter keyed on a `frozenset` of the two legacy names, or a
    `listing()` argument naming the graded families — the assertion counts the graded
    families, never the constant's spelling or the filter's syntax.
    """
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_probe.py").write_text(
        "import pytest\n\n"
        '@pytest.mark.conformance(unit="equation-register", '
        'system="publication-math-conformance", part="p0")\n'
        "def test_conformance():\n"
        "    pass\n\n"
        '@pytest.mark.equivalence(unit="_d_build_water_level_generator.py:1-18", '
        'system="dependence-modelling", part="p1")\n'
        "def test_equivalence():\n"
        "    pass\n",
        encoding="utf-8",
    )
    assert inventory.FAMILIES == ("equivalence", "refactor_reminder", "conformance")
    assert ownership.LEDGER_FAMILIES == ("equivalence", "refactor_reminder")
    listed = inventory.listing(tmp_path)[0]
    graded = ownership._family_tests(tmp_path)
    listed_by_family = {(entry["family"], entry["unit"]) for entry in listed}
    graded_by_family = {(entry["family"], entry["unit"]) for entry in graded}
    assert listed_by_family == {
        ("conformance", "equation-register"),
        ("equivalence", "_d_build_water_level_generator.py:1-18"),
    }
    assert graded_by_family == {("equivalence", "_d_build_water_level_generator.py:1-18")}


def test_ownerships_independent_matcher_stays_restricted_to_the_two_legacy_families():
    """Property: `test_ownership.py`'s independent decorator matcher counts only the two
    legacy families — it does not match a conformance decorator — and its real-tree count
    equals the family-tests denominator `ownership.summary()` reports.

    Class: the real tree at any state of conformance-marker application, before or after
    the follow-up change's 44 decorators land. Why: the matcher's restriction is what keeps the
    ownership summary's denominator independently checkable — an instrument that does not
    read the module under test; widening the matcher to `conformance` would count the 44
    markers against a filter that must exclude them, reddening
    `test_ownership.py`'s real-tree summary test on a correct landing of the follow-up
    change, so the
    restriction must hold with zero conformance markers in the tree (today) exactly as it
    must hold with 44 (after the follow-up change lands). Killed by: adding `|conformance` to the matcher's
    alternation — the real-tree count diverges from the filtered denominator the first time
    a conformance marker lands, and the negative-match assertion fires immediately. Second
    correct implementation: an AST-based counter over decorator names restricted to the same
    two names — the assertion compares the count and the restriction, never the regex's
    mechanics.
    """
    matcher = test_ownership.FAMILY_DECORATOR
    legacy_decorator = '@pytest.mark.equivalence(unit="u", system="s", part="p")'
    conformance_decorator = '@pytest.mark.conformance(unit="u", system="s", part="p")'
    assert matcher.search(legacy_decorator), "the must-fire control: a legacy decorator matches"
    assert not matcher.search(conformance_decorator)
    independent = sum(
        len(matcher.findall((REPO_ROOT / path).read_text(encoding="utf-8")))
        for path in test_ownership._listed_family_files(REPO_ROOT)
    )
    line, findings = ownership.summary()
    assert findings == []
    match = re.search(r"family tests (\d+)", line)
    assert match is not None, line
    assert independent == int(match.group(1)), (independent, line)


def test_a_module_level_marker_is_captured_but_not_listed_while_a_function_local_one_is_both(pytester):
    """Property: on one synthetic tree, a module-level conformance `pytestmark` is captured by
    collection but missing from the static inventory, so the cross-check names it `collected and
    not listed`; a function-local conformance marker on the same tree is listed AND captured, so
    it produces no finding.

    Class: any test file holding the two placements of one family marker; both arms are required
    because either alone cannot tell a listing reader that misses nothing from one that misses
    the module arm. Why: the function-local rule pins every conformance marker to the test
    function's own decorator precisely because the module-level spelling feeds collection but
    not the static inventory — a file that regressed to `pytestmark` would pass collection and
    the family selection while vanishing from every listing count, so this differential is the
    property the 44 applied markers rest on. Killed by: widening the static read to module-level
    assignments, which empties the module arm's finding; and by a capture that never sees the
    module-level mark, which empties the module arm's capture. Second correct implementation: a
    static read over tokenized lines and a capture taken from `pytest_collection_modifyitems`
    agree with the AST read and the closest-marker walk used here.
    """
    pytester.mkdir("tests")  # a plain directory: the outer session already holds a `tests` package
    pytester.path.joinpath("tests", "test_module.py").write_text(
        "import pytest\n\n"
        'pytestmark = pytest.mark.conformance(unit="equation-register", system="publication-math-conformance")\n\n'
        "def test_one():\n    pass\n",
        encoding="utf-8",
    )
    pytester.path.joinpath("tests", "test_function.py").write_text(
        "import pytest\n\n"
        '@pytest.mark.conformance(unit="equation-register", part="p0", system="publication-math-conformance")\n'
        "def test_one():\n    pass\n",
        encoding="utf-8",
    )
    entries, findings = inventory.listing(pytester.path)
    assert findings == [], findings
    assert [(entry["file"], entry["function"]) for entry in entries] == [("tests/test_function.py", "test_one")]
    items, _ = pytester.inline_genitems("tests")
    captured = inventory.captured_from(items)
    assert sorted(c["nodeid"] for c in captured) == [
        "tests/test_function.py::test_one",
        "tests/test_module.py::test_one",
    ]
    mismatch = inventory.cross_check(entries, captured)
    assert len(mismatch) == 1, mismatch
    assert "collected and not listed: tests/test_module.py::test_one" in mismatch[0], mismatch


def test_the_family_selection_reports_one_red_and_one_green_on_an_inner_tree_confined_to_tmp_path(tmp_path):
    """Property: an inner pytest tree under `tmp_path` carrying one passing and one failing
    function-local conformance test, run by the real root configuration with `-m conformance`,
    reports exactly one pass and one failure — while this outer harness test stays green.

    Class: any synthetic red/green pair selected by the family marker; the inner tree lives only
    under `tmp_path`, so whole-suite collection never sees the synthetic red. Why: the
    red-on-build inversion makes a tracked red mean paper/code divergence, so the family's
    selection semantics are proven with a synthetic failure that exists only inside the probe — a
    family whose `-m` expression cannot report one pass and one failure is not the selection the
    44 applied markers rely on, and a red committed to the tracked tree would invert the
    inversion. Killed by: a `-m conformance` expression that also admits a foreign family, which
    moves the inner run's counts; and by writing the inner tree inside the tracked tree, which
    whole-suite collection would reach. Second correct implementation: the same pair split
    across two module files, or a red raised by `pytest.fail` rather than an explicit raise,
    reports the same one-pass-one-failure line.
    """
    inner = tmp_path / "inner"
    (inner / "tests").mkdir(parents=True)
    (inner / "tests" / "test_probe.py").write_text(
        "import pytest\n\n"
        '@pytest.mark.conformance(unit="equation-register", part="green", system="publication-math-conformance")\n'
        "def test_green():\n    pass\n\n"
        '@pytest.mark.conformance(unit="equation-register", part="red", system="publication-math-conformance")\n'
        "def test_red():\n"
        '    raise AssertionError("the synthetic divergence this probe deliberately selects by")\n',
        encoding="utf-8",
    )
    whole = _run_pytest(
        "--rootdir", str(REPO_ROOT), str(inner / "tests" / "test_probe.py"), "-q", "-p", "no:cacheprovider"
    )
    assert whole.returncode == 1, whole.stderr + whole.stdout
    assert "1 passed" in whole.stdout
    assert "1 failed" in whole.stdout
    selected = _run_pytest(
        "--rootdir",
        str(REPO_ROOT),
        "-m",
        "conformance",
        str(inner / "tests" / "test_probe.py"),
        "-q",
        "-p",
        "no:cacheprovider",
    )
    assert selected.returncode == 1, selected.stderr + selected.stdout
    assert "1 passed" in selected.stdout
    assert "1 failed" in selected.stdout


def test_the_family_selection_excludes_a_foreign_family_whose_legacy_control_still_fires(tmp_path):
    """Property: on an inner tree carrying one conformance and one equivalence test, `-m
    conformance` runs the conformance test and deselects the equivalence one, and the equivalence
    marker's own selection runs the equivalence test — the legacy must-fire control proving the
    excluded test is live and its exclusion is the family filter.

    Class: any inner tree mixing the new family with a legacy family; the control arm is
    required because a deselected count alone cannot tell a filter that excludes the foreign
    family from a legacy marker that never fires. Why: the widened inventory lists three
    families while legacy grading and the State-check job consume the two legacy ones, so `-m
    conformance` must select only the conformance family — and the exclusion is only real if the
    excluded test demonstrably passes under its own family. The legacy markers are registered by
    the real tree's `tests/conftest.py`, which an out-of-tree run never loads, so the inner tree
    carries its own conftest registering them — without it the inner run reports
    `'equivalence' not found in markers` under `strict_markers` before any selection happens,
    which the control run measured when this test was written. Killed by: a `-m conformance`
    expression that admits `equivalence`, which reports two passes; and by dropping the inner
    conftest, which the control run reports as the strict-markers collection error rather than
    one pass. Second correct implementation: the same pair in one module file, or a
    `refactor_reminder` control in place of the equivalence one, reports the same
    one-pass-one-deselected pair of runs.
    """
    inner = tmp_path / "inner"
    (inner / "tests").mkdir(parents=True)
    (inner / "tests" / "conftest.py").write_text(
        "def pytest_configure(config):\n"
        '    config.addinivalue_line("markers", "equivalence(unit, system, part): a legacy equivalence test")\n'
        '    config.addinivalue_line("markers", "refactor_reminder(unit, system, part): a legacy refactor reminder")\n',
        encoding="utf-8",
    )
    (inner / "tests" / "test_probe.py").write_text(
        "import pytest\n\n"
        '@pytest.mark.conformance(unit="equation-register", part="c0", system="publication-math-conformance")\n'
        "def test_conformance():\n    pass\n\n"
        '@pytest.mark.equivalence(unit="_d_build_water_level_generator.py:1-18", '
        'system="dependence-modelling", part="e0")\n'
        "def test_equivalence():\n    pass\n",
        encoding="utf-8",
    )
    selected = _run_pytest(
        "--rootdir",
        str(REPO_ROOT),
        "-m",
        "conformance",
        "-v",
        str(inner / "tests" / "test_probe.py"),
        "-p",
        "no:cacheprovider",
    )
    assert selected.returncode == 0, selected.stderr + selected.stdout
    assert "1 passed, 1 deselected" in selected.stdout
    assert "test_conformance PASSED" in selected.stdout
    assert "test_equivalence PASSED" not in selected.stdout
    control = _run_pytest(
        "--rootdir",
        str(REPO_ROOT),
        "-m",
        "equivalence",
        "-v",
        str(inner / "tests" / "test_probe.py"),
        "-p",
        "no:cacheprovider",
    )
    assert control.returncode == 0, control.stderr + control.stdout
    assert "1 passed, 1 deselected" in control.stdout
    assert "test_equivalence PASSED" in control.stdout
    assert "test_conformance PASSED" not in control.stdout
