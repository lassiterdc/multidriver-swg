"""Tests of the test inventory: its static read, its collection capture, its cross-check and its printed line.

The real-tree cross-check runs here as an ordinary test. The narrowing test and the capture's
timing test each run the real tree in a subprocess restricted with `-k` to the cross-check, so
the child never selects the test that started it, and each also grades a small tree in an
in-process `pytester` session; the tests of the two comparison directions use such sessions
alone.
"""

import os
import re
import subprocess
import sys

import pytest

from tests.correspondence import inventory, system_scope

pytestmark = pytest.mark.state_check

SLUG = system_scope.slugs()[0]
CROSS_CHECK = "test_the_inventory_equals_collection"
JOB_EXPRESSION = "state_check and not equivalence and not refactor_reminder"  # the State check job's `-m`
LANDING = "test inventory: 0 tests in 0 files over families equivalence, refactor_reminder; reminders per system: none"


def test_the_inventory_equals_collection(request):
    """Property: on an unnarrowed run the static list equals collection taken before deselection; a narrowed run is skipped.

    Class: every run of the suite. Why: systems select and own tests by these markers, so a
    test the static read misses, or a marker it cannot see, is a test no check accounts for.
    Killed by: the capture hook storing nothing, which leaves this test no collected list to
    compare with. Second correct implementation: a second, unselected collection in a child process.
    """
    narrowed = inventory.narrowing(request.config)
    if narrowed:
        pytest.skip(f"NOT MEASURED: collection narrowed by a path ({narrowed})")
    entries, findings = inventory.listing()
    findings += inventory.cross_check(entries, request.config.stash[inventory.CAPTURE])
    assert not findings, "\n".join(findings)


def test_an_unreadable_decorator_fails_and_never_skips(tmp_path):
    """Property: no family decorator is skipped; one the static read cannot evaluate is a finding naming its file and line.

    Class: any family or guard decorator the static read cannot evaluate — keyword values that
    are not string literals; the marker reached through a name other than `pytest.mark`
    (`from pytest import mark`, then `@mark.equivalence(...)`); and the family name placed
    before another call (`@pytest.mark.equivalence.with_args(...)`). Pytest applies the last two
    exactly as the plain spelling, so for them listing the test is as correct as naming it.
    Why: a skipped form is a test that pytest runs and no check sees. Killed by: skipping
    unreadable forms; by dropping the refusal of other spellings; and by recognising a family
    decorator from its last name alone. Second correct implementation: a regular-expression
    pre-scan that rejects any non-literal keyword value, beside a read that lists the other two
    spellings instead of refusing them.
    """
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_x.py").write_text(
        f"import pytest\nKEY = 'a.py:1-2'\n@pytest.mark.equivalence(unit=KEY, system='{SLUG}')\ndef test_one():\n    pass\n"
    )
    (tmp_path / "tests" / "test_mark.py").write_text(
        f"from pytest import mark\n\n\n@mark.equivalence(unit='a.py:1-2', system='{SLUG}')\ndef test_two():\n    pass\n"
    )
    (tmp_path / "tests" / "test_with_args.py").write_text(
        "import pytest\n\n\n"
        f"@pytest.mark.equivalence.with_args(unit='a.py:3-4', system='{SLUG}')\ndef test_three():\n    pass\n"
    )
    entries, findings = inventory.listing(tmp_path)
    assert any("tests/test_x.py:3:" in f.split() for f in findings), findings
    listed = [e["function"] for e in entries]
    for function, site in (("test_two", "tests/test_mark.py:4:"), ("test_three", "tests/test_with_args.py:4:")):
        named = [f for f in findings if site in f.split()]
        assert function in listed or named, f"{function} neither listed nor named: {entries} {findings}"
    assert inventory.main(tmp_path) == 1


def _pair(pytester, *args):
    """The static list of `pytester`'s tree and its collected family tests, for the cross-check."""
    items, _ = pytester.inline_genitems(*args)
    return inventory.listing(pytester.path)[0], inventory.captured_from(items)


def test_a_listed_test_that_pytest_does_not_collect_is_named(pytester):
    """Property: a decorated function that pytest does not collect is named.

    Class: any listed family test with no collected item. Why: such a test is counted as owned
    and adopted but never runs. Killed by: comparing in the collected-to-listed direction only.
    Second correct implementation: a symmetric set difference reported per side.
    """
    pytester.mkdir("tests")  # a plain directory: the outer session already holds a `tests` package
    pytester.path.joinpath("tests", "test_a.py").write_text(
        f"import pytest\n@pytest.mark.equivalence(unit='a.py:1-2', system='{SLUG}')\ndef check_x():\n    pass\n"
    )
    findings = inventory.cross_check(*_pair(pytester, "tests"))
    assert any("tests/test_a.py::check_x" in f.split() for f in findings), findings


def test_a_collected_test_the_static_read_does_not_list_is_named(pytester):
    """Property: a collected family test that the static read does not list is named.

    Class: any collected family test outside the read's file patterns. Why: a changed
    `python_files` would otherwise hide tests from every check. Killed by: comparing in the
    listed-to-collected direction only. Second correct implementation: reading `python_files`
    from the configuration in the static read.
    """
    pytester.makeini("[pytest]\npython_files = check_*.py\n")
    pytester.mkdir("tests")  # a plain directory: the outer session already holds a `tests` package
    pytester.path.joinpath("tests", "check_a.py").write_text(
        f"import pytest\n@pytest.mark.equivalence(unit='a.py:1-2', system='{SLUG}')\ndef test_one():\n    pass\n"
    )
    findings = inventory.cross_check(*_pair(pytester, "tests"))
    assert any("tests/check_a.py::test_one" in f.split() for f in findings), findings


NARROWING_PRINTER = (
    "from tests.correspondence import inventory\n"
    "def test_n(request):\n"
    "    print('NARROWING=' + repr(inventory.narrowing(request.config)))\n"
)


def _narrowing_of(pytester, *args):
    """What `narrowing` returns inside an in-process run of `pytester`'s tree given `args`."""
    result = pytester.runpytest_inprocess("-p", "no:cacheprovider", "-s", *args)
    values = [m.group(1) for line in result.outlines for m in re.finditer(r"NARROWING=(\S+)", line)]
    assert len(values) == 1, result.outlines
    return values[0]


def test_a_path_narrowed_run_is_not_measured(pytester, monkeypatch):
    """Property: a run narrowed below `testpaths` is not measured; a run naming the whole of `testpaths` is.

    Class: any run given a path below `testpaths`, which skips the cross-check `NOT MEASURED`
    naming the path; and any run started away from the root whose relative path names the whole
    of `testpaths`, such as `cd tests && pytest .`, which is not a narrowing because pytest
    resolves the path against the directory it was started in. Why: a narrowed collection
    cannot be compared with the whole static list, and a whole run misread as narrowed skips a
    comparison it should make. Killed by: dropping the narrowing test; and by resolving path
    arguments against the root instead of the invocation directory. Second correct
    implementation: comparing `config.args_source` with `TESTPATHS` plus an explicit-path check,
    or comparing the directories actually collected with those `testpaths` names.
    """
    pytester.makeini("[pytest]\ntestpaths = tests\n")
    tests = pytester.mkdir("tests")  # a plain directory: the outer session already holds a `tests` package
    (tests / "inner").mkdir()
    (tests / "inner" / "test_n.py").write_text(NARROWING_PRINTER)
    monkeypatch.chdir(tests)
    whole, inner = _narrowing_of(pytester, "."), _narrowing_of(pytester, "inner")
    assert (whole, inner != "None") == ("None", True), f"whole={whole} inner={inner}"
    root = inventory.REPO_ROOT
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(root), str(root / "src"), os.environ.get("PYTHONPATH", "")]))
    out = subprocess.run(
        [
            sys.executable,
            "-B",
            "-m",
            "pytest",
            "-p",
            "no:cacheprovider",
            "-q",
            "-rs",
            "tests/correspondence",
            "-k",
            CROSS_CHECK,
        ],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
    ).stdout
    skipped = [line for line in out.splitlines() if line.startswith("SKIPPED")]
    assert any("NOT MEASURED" in line and "tests/correspondence)" in line for line in skipped), out


# Loaded into the child run below with `-p`: it reports what the inventory's capture counted and
# how many items survived deselection, after the session has collected and deselected.
COUNT_PROBE = """\
def pytest_collection_finish(session):
    from tests.correspondence import inventory

    session.config._probe_line = (
        f"CAPTURE_PROBE captured={session.config.stash.get(inventory.COLLECTED, None)} selected={len(session.items)}"
    )


def pytest_terminal_summary(terminalreporter, config):
    terminalreporter.write_line(getattr(config, "_probe_line", "CAPTURE_PROBE missing"))
"""


def test_the_capture_counts_items_before_the_job_expression_deselects_them(tmp_path, pytester):
    """Property: the capture, both its count and its list of family tests, is taken before any deselection.

    Class: any run whose `-m` or `-k` deselects items, which the State check job's run always
    does — here the real tree under the job's expression, and a small tree in which `-m` deselects
    a family test that a second test then looks for in the capture. Why: a capture taken after
    deselection sees only the selected items, and the cross-check would then report every
    deselected family test as missing. The real tree runs in a child process with the job's
    expression and `-k` naming the cross-check, so the child never selects this test; the child
    must pass the cross-check and report a captured count above its selected count. Killed by:
    capturing at collection finish, after deselection; and by moving only the list of family
    tests after deselection while the count stays before it. Second correct implementation: a
    capture that records each item's node id and is counted when read.
    """
    pytester.mkdir("tests")  # a plain directory: the outer session already holds a `tests` package
    pytester.path.joinpath("tests", "test_f.py").write_text(
        f"import pytest\n@pytest.mark.equivalence(unit='a.py:1-2', system='{SLUG}')\ndef test_one():\n    pass\n"
    )
    pytester.path.joinpath("tests", "test_cc.py").write_text(
        "from pathlib import Path\nfrom tests.correspondence import inventory\n"
        "def test_cc(request):\n"
        "    entries = inventory.listing(Path.cwd())[0]\n"
        "    assert not inventory.cross_check(entries, request.config.stash[inventory.CAPTURE])\n"
    )
    result = pytester.runpytest(
        "-p", "no:cacheprovider", "-p", "tests.correspondence.inventory", "-m", "not equivalence", "tests"
    )
    result.assert_outcomes(passed=1, deselected=1)
    root = inventory.REPO_ROOT
    (tmp_path / "count_probe.py").write_text(COUNT_PROBE)
    path = [str(tmp_path), str(root), str(root / "src"), os.environ.get("PYTHONPATH", "")]
    out = subprocess.run(
        [
            sys.executable,
            "-B",
            "-m",
            "pytest",
            "-p",
            "no:cacheprovider",
            "-p",
            "count_probe",
            "-q",
            "-rA",
            "-m",
            JOB_EXPRESSION,
            "-k",
            CROSS_CHECK,
        ],
        cwd=root,
        env=dict(os.environ, PYTHONPATH=os.pathsep.join(path)),
        capture_output=True,
        text=True,
    ).stdout
    assert f"PASSED tests/correspondence/test_inventory.py::{CROSS_CHECK}" in out.splitlines(), out
    probe = re.search(r"^CAPTURE_PROBE captured=(\d+) selected=(\d+)$", out, re.M)
    assert probe and int(probe.group(1)) > int(probe.group(2)), out


def test_reminders_are_counted_per_system(tmp_path):
    """Property: reminders are counted per owning system, and only reminders are counted.

    Class: any mix of families. Why: the count per system is meant to reach zero as porting ends.
    Killed by: counting every family. Second correct implementation: a Counter over the
    reminders' systems.
    """
    (tmp_path / "tests").mkdir()
    body = "import pytest\n"
    for i, family in enumerate(("refactor_reminder", "refactor_reminder", "equivalence")):
        body += f"@pytest.mark.{family}(unit='a.py:{i + 1}-{i + 1}', system='{SLUG}')\ndef test_{i}():\n    pass\n"
    (tmp_path / "tests" / "test_r.py").write_text(body)
    assert inventory.reminders_per_system(inventory.listing(tmp_path)[0]) == {SLUG: 2}


def test_the_inventory_prints_its_landing_line_exactly(capsys):
    """Property: over the real tree, the inventory command prints exactly the line stating its zero counts, and exits 0.

    Class: the real tree as it stands, which holds no family test. Why: a check that finds
    nothing must say how much it looked at, so this test pins the whole line rather than a part
    of it; the first family test a later change adds alters the line and reds this test, and that
    change updates the expected line on purpose. Killed by: a summary that prints an empty
    reminder clause instead of `none`, or any other change to the line's wording or counts.
    Second correct implementation: a `main` that builds the line from a `Counter` of the
    reminders' systems in one format string, printing `none` for an empty counter.
    """
    assert inventory.main() == 0
    assert capsys.readouterr().out == LANDING + "\n"
