r"""Independent checks of the real-tree binding-coverage assertion, kept apart from the test it checks.

This file is the checking arm of `tests/conformance/test_binding_coverage.py`. It is a separate file so
the checks can be read, run and reverted on their own, and so the assertion under review is never
strengthened or weakened by the file that grades it.

The assertion under review is the one place the obligation becomes a claim about this repository rather
than about a synthetic tree: `bindings.coverage_findings` over the checked-in ledger, the checked-in
register and the checked-in conformance markers, followed by the ledger's 21/6/6/3 partition. A claim of
that shape fails in three ways nobody sees from its green, so the checks here make each one visible.

- It can pass over the WRONG SUBJECT. The checks show that the root and the ledger resolve from the
  module's own location, and that the assertion changes its verdict when the tree or the ledger it reads
  changes, so it is reading what it says it reads.
- It can pass VACUOUSLY. The assertion is run, unchanged, over a copy of the real conformance tree with
  one defect planted at a time; an assertion that cannot fail on a violating tree is the failure this
  file exists to catch. Each planted defect is paired with an unmodified control copy that must stay
  green, so a red is the defect and not the copy.
- Its two halves can hide each other. A row moved off `TEST` together with its function draws no finding
  and moves only the partition; a renamed function draws a finding and leaves the partition alone. Each
  half is reddened with the other satisfied, and the finding set is shown to fail first.

A fourth check recounts the criterion on the real tree with a second, independent reader of the markers,
so a defect shared by `bindings.coverage_findings` and `inventory.listing` cannot make both agree on a
tree that violates the criterion.

Nothing here carries a family marker, so these checks enter no inventory count and the real-tree listing
the assertion under review reads is unchanged by this file's existence.
"""

from __future__ import annotations

import ast
import re
import shutil
from collections import Counter
from pathlib import Path

import pytest

from tests.conformance import bindings
from tests.conformance import test_binding_coverage as subject
from tests.conformance.register import EXPECTED_IDS
from tests.correspondence import inventory

REAL_ROOT = inventory.REPO_ROOT
REAL_LOAD = bindings.load
SUBJECT_FILE = "tests/conformance/test_binding_coverage.py"
SUBJECT_FUNCTION = "test_the_real_tree_binds_every_equation_and_covers_every_bound_test"
SYSTEM = "publication-math-conformance"
EQUATION_LIKE = re.compile(r"[Ee]-?\d+")
PARTITION = {"TEST": 21, "CROSS-REPO": 6, "UNREACHABLE": 6, "NO-MANUSCRIPT-TEXT": 3}


@pytest.fixture(scope="module")
def real_listing():
    """`inventory.listing` over the real tree, read once: its entries and its unreadable-marker findings."""
    return inventory.listing(REAL_ROOT)


@pytest.fixture(scope="module")
def real_ledger():
    """The real ledger as `bindings.load()` returns it."""
    return REAL_LOAD()


@pytest.fixture
def tree(tmp_path):
    """A faithful copy of the real conformance directory, with the real ledger and register beside it."""
    shutil.copytree(
        REAL_ROOT / "tests" / "conformance",
        tmp_path / "tests" / "conformance",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    return tmp_path


def _file_defining(root: Path, function: str) -> Path:
    """The one file under the copy's conformance directory that defines `function`."""
    hits = [
        path
        for path in sorted((root / "tests" / "conformance").rglob("*.py"))
        if re.search(rf"^def {re.escape(function)}\(", path.read_text(encoding="utf-8"), re.MULTILINE)
    ]
    assert len(hits) == 1, f"expected exactly one file defining {function}; found {hits}"
    return hits[0]


def _replace_once(path: Path, old: str, new: str) -> None:
    """Rewrite `path` with `old` replaced by `new`, refusing a mutation that does not land exactly once."""
    text = path.read_text(encoding="utf-8")
    assert text.count(old) == 1, f"{path.name}: expected exactly one {old!r}, found {text.count(old)}"
    path.write_text(text.replace(old, new), encoding="utf-8")


def _append(path: Path, source: str) -> None:
    """Append `source` to `path`, after the file's own text."""
    path.write_text(path.read_text(encoding="utf-8") + source, encoding="utf-8")


def _ledger_text_with(root: Path, equation_id: str, new_row: str) -> Path:
    """Rewrite the copy's ledger so `equation_id`'s table is `new_row`, returning the ledger's path."""
    path = root / "tests" / "conformance" / "bindings.toml"
    text = path.read_text(encoding="utf-8")
    pattern = re.compile(rf"^\[{re.escape(equation_id)}\]\n(?:(?!\[E-).*\n?)*", re.MULTILINE)
    assert len(pattern.findall(text)) == 1, f"{equation_id} must be one table in the ledger"
    path.write_text(pattern.sub(lambda _: f"[{equation_id}]\n{new_row}\n\n", text), encoding="utf-8")
    return path


def _run_subject(monkeypatch, root: Path, ledger: Path | None = None) -> None:
    """Run the assertion under review, unchanged, over the tree at `root` and the ledger at `ledger`.

    Only the two inputs the assertion reads are redirected: the root it lists and the loader it calls.
    The loader is the real one, pointed at a different file, so the ledger is validated exactly as the
    real one is.
    """
    monkeypatch.setattr(subject, "REPO_ROOT", root)
    path = ledger if ledger is not None else root / "tests" / "conformance" / "bindings.toml"
    monkeypatch.setattr(subject.bindings, "load", lambda *_args, **_kwargs: REAL_LOAD(path))
    getattr(subject, SUBJECT_FUNCTION)()


def _subject_assert_lines() -> list[int]:
    """The line of each `assert` the assertion under review makes, in source order."""
    module = ast.parse(Path(subject.__file__).read_text(encoding="utf-8"))
    function = next(node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == SUBJECT_FUNCTION)
    return [node.lineno for node in ast.walk(function) if isinstance(node, ast.Assert)]


def _failed_assert_index(excinfo) -> int:
    """Which of the assertion's `assert` statements raised, counted from zero in source order."""
    traceback = excinfo.tb
    while traceback.tb_next is not None:
        traceback = traceback.tb_next
    return sorted(_subject_assert_lines()).index(traceback.tb_lineno)


def _own_conformance_markers(root: Path) -> list[tuple[str, str, str]]:
    """Every `(unit, file, function)` of a conformance marker under `root/tests`, read independently.

    A second reader on purpose: it does not call `inventory.listing`, takes the decorator's own `unit`
    keyword wherever the decorator's callee ends in `conformance`, and has no notion of the ledger.
    """
    found = []
    for path in sorted((root / "tests").rglob("*.py")):
        if not (path.name.startswith("test_") or path.name.endswith("_test.py")):
            continue
        relative = path.relative_to(root).as_posix()
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            for decorator in node.decorator_list:
                if not (isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute)):
                    continue
                if decorator.func.attr != "conformance":
                    continue
                for keyword in decorator.keywords:
                    if keyword.arg == "unit" and isinstance(keyword.value, ast.Constant):
                        found.append((keyword.value.value, relative, node.name))
    return found


def test_the_assertion_carries_one_function_level_marker_that_the_listing_reads(real_listing):
    """Property: the assertion is listed once, as
    `conformance(unit="binding-coverage", part="{its own name}", system="publication-math-conformance")`,
    on the function and not at module level, with no unreadable marker in its file.

    Class: the one function of one file, read from the file's own syntax tree and from the listing.
    NEW CAPABILITY: the plausible wrong implementation is a marker whose keywords are module constants
    or a module-level `pytestmark`; `inventory.listing` then reports it unreadable or lists nothing, and
    the real-tree coverage check is red against its own file. Red under both.
    Reachability: `listing` is the reader the coverage check itself calls, so what it cannot read in this
    file reaches that check as a finding.
    Both-states anchor: the expected tuple is built from the function's name in the syntax tree, not
    from the marker text.
    Why: a unit that is equation-like would be a finding against the file that asserts there are none.
    Kills: a stale `part`, a different `unit` or `system`, an equation-spelled unit, a module-level
    marker.
    Second correct implementation: a decorator written in any literal form the listing reads passes.
    """
    entries, findings = real_listing
    source = (REAL_ROOT / SUBJECT_FILE).read_text(encoding="utf-8")
    module = ast.parse(source)
    declared = [node.name for node in module.body if isinstance(node, ast.FunctionDef)]
    assert SUBJECT_FUNCTION in declared, f"the file must define the assertion; found {declared}"

    own = [entry for entry in entries if entry["file"] == SUBJECT_FILE]
    assert [(entry["family"], entry["function"], entry["unit"], entry["part"], entry["system"]) for entry in own] == [
        ("conformance", SUBJECT_FUNCTION, "binding-coverage", SUBJECT_FUNCTION, SYSTEM)
    ]
    assert EQUATION_LIKE.fullmatch(own[0]["unit"]) is None
    assert [finding for finding in findings if finding.startswith(SUBJECT_FILE)] == []

    module_level = [
        target.id
        for node in module.body
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name) and target.id == "pytestmark"
    ]
    assert module_level == [], "a module-level pytestmark is a marker the listing does not read"


def test_nothing_in_the_assertions_own_text_can_skip_or_guard_it():
    """Property: the assertion's module names no skip, xfail, parametrize or import-skip, and every
    `assert` of the assertion's function is a direct statement of its body, with at least two of them.

    Class: the syntax of one function and its module. NEW CAPABILITY: the plausible wrong
    implementation guards its assertions (`if rows:`, `try/except`) or skips on a missing file, so that
    a tree which violates the criterion reports a pass or a skip; every shape of that is visible in the
    syntax tree. Red under each.
    Reachability: the function body is the only code the assertion runs.
    Both-states anchor: the oracle is the syntax of the function, not its behaviour on one tree.
    Why: a skipped or conditionally empty assertion is a green that no tree can turn red.
    Kills: `@pytest.mark.skip*`, `xfail`, `importorskip`, `parametrize` over an empty list, an assert
    nested under `if`, `for` or `try`.
    Second correct implementation: any straight-line body with its two assertions passes.
    """
    module = ast.parse(Path(subject.__file__).read_text(encoding="utf-8"))
    function = next(node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == SUBJECT_FUNCTION)

    names = {node.id for node in ast.walk(module) if isinstance(node, ast.Name)}
    names |= {node.attr for node in ast.walk(module) if isinstance(node, ast.Attribute)}
    assert names.isdisjoint({"skip", "skipif", "xfail", "parametrize", "importorskip"})

    guards = [node for node in ast.walk(function) if isinstance(node, ast.If | ast.For | ast.While | ast.Try)]
    assert guards == [], "no control flow may stand between the function's start and its assertions"
    direct = [node for node in function.body if isinstance(node, ast.Assert)]
    assert len(direct) == len(_subject_assert_lines()) >= 2


def test_the_assertion_reads_the_checkout_it_lives_in_whatever_the_working_directory(monkeypatch, tmp_path):
    """Property: the assertion's root and the ledger its loader reads are the checkout the test module
    lives in; the assertion is green from a different working directory; and its verdict follows the
    root and the loader it reads rather than a copy of either.

    Class: the two inputs of the assertion, `REPO_ROOT` and `bindings.load()`'s default path, and the
    working directory. REGRESSION: a root taken from the working directory reads the tree above wherever
    pytest was started, and a loader default bound to a fixture's ledger reads a copy.
    Reachability: the other tests of this file show the verdict changing with each input.
    Both-states anchor: the expected root is computed from the module file's own path, not from the value
    the module computed.
    Why: asserting on the real tree is the whole point; a green over any other tree proves nothing.
    Kills: `REPO_ROOT = Path.cwd()`, a relative root, a ledger path taken from the environment.
    Second correct implementation: a root computed from `__file__` by any other path arithmetic passes.
    """
    expected_root = Path(subject.__file__).resolve().parents[2]
    assert subject.REPO_ROOT == expected_root == REAL_ROOT
    assert subject.REPO_ROOT.is_absolute()
    assert (expected_root / SUBJECT_FILE).samefile(subject.__file__)

    ledger_path = Path(subject.__file__).resolve().with_name("bindings.toml")
    assert bindings.LEDGER_PATH.resolve() == ledger_path
    assert bindings.load.__defaults__ == (bindings.LEDGER_PATH,)

    monkeypatch.chdir(tmp_path)
    getattr(subject, SUBJECT_FUNCTION)()


def test_a_faithful_copy_of_the_real_tree_is_green(tree, monkeypatch):
    """Property: the assertion, run unchanged over an unmodified copy of the conformance directory,
    passes, so every red elsewhere in this file is the planted defect and not the copy.

    Class: the control of every mutation test below, the same inputs with nothing planted.
    Reachability: the copy holds the directory's own files, including the ledger and register beside
    them, and the listing reads the copy because `REPO_ROOT` is redirected to it.
    Both-states anchor: the control and each mutation differ by exactly one edit.
    Why: without it a mutation test that reds proves only that the copy is unreadable.
    Kills: a copy that drops a file, so that every mutation reds for the same wrong reason.
    Second correct implementation: any faithful copy passes.
    """
    _run_subject(monkeypatch, tree)
    copied = REAL_LOAD(tree / "tests" / "conformance" / "bindings.toml")
    assert dict(Counter(row["binding"] for row in copied.values())) == PARTITION


def test_renamed_bound_functions_red_the_finding_arm_and_every_finding_is_in_the_message(tree, monkeypatch):
    """Property: with the functions of two `TEST` rows renamed and nothing else changed, the assertion
    fails on its FIRST `assert`, and its message carries every finding `coverage_findings` reports.

    Class: a `TEST` row whose named function no longer exists. NEW CAPABILITY: the plausible wrong
    implementation compares the partition only, or reports a count rather than the findings; neither
    reds on a rename, because a rename moves no row.
    Reachability: a rename nobody re-listed in the ledger is the usual edit that arrives here.
    Both-states anchor: the oracle is the finding list computed by the function itself over the same
    copy, compared with the exception's text; the control arm shows the same copy green first.
    Why: the finding is how an author finds which of 21 names went stale.
    Kills: dropping the findings from the message; asserting the partition before the finding set.
    Second correct implementation: a message that formats the findings differently passes, since each
    is looked for as a substring.
    """
    _run_subject(monkeypatch, tree)
    ledger = REAL_LOAD(tree / "tests" / "conformance" / "bindings.toml")
    for equation_id in ("E-04", "E-05"):
        name = ledger[equation_id]["test"]
        _replace_once(_file_defining(tree, name), f"def {name}(", f"def {name}_renamed(")

    findings = bindings.coverage_findings(ledger, EXPECTED_IDS, inventory.listing(tree))
    assert len(findings) >= 2

    with pytest.raises(AssertionError) as excinfo:
        _run_subject(monkeypatch, tree)
    assert _failed_assert_index(excinfo) == 0
    text = str(excinfo.value)
    for finding in findings:
        assert finding in text, f"the failure does not name {finding!r}"
    assert "E-04" in text and "E-05" in text


@pytest.mark.parametrize(
    ("planted", "needle"),
    [
        pytest.param(
            '\n\n@pytest.mark.conformance(unit="E-06", part="test_eq06_planted", system="publication-math-conformance")\n'
            "def test_eq06_planted() -> None:\n    pass\n",
            "E-06",
            id="a-marker-on-a-row-that-owes-no-test",
        ),
        pytest.param(
            '\n\n@pytest.mark.conformance(unit="E-99", part="test_eq99_planted", system="publication-math-conformance")\n'
            "def test_eq99_planted() -> None:\n    pass\n",
            "E-99",
            id="a-marker-on-an-id-no-row-carries",
        ),
        pytest.param(
            '\n\n@pytest.mark.conformance(unit="e-04", part="test_e04_planted", system="publication-math-conformance")\n'
            "def test_e04_planted() -> None:\n    pass\n",
            "e-04",
            id="a-lower-case-spelling-of-an-equation",
        ),
        pytest.param(
            '\n\n@pytest.mark.conformance(unit="E-04", part="test_eq04_duplicate", system="publication-math-conformance")\n'
            "def test_eq04_duplicate() -> None:\n    pass\n",
            "found 2",
            id="a-second-function-carrying-a-bound-id",
        ),
        pytest.param(
            '\n\n@pytest.mark.conformance(unit=PLANTED_UNIT, part="test_planted", system="publication-math-conformance")\n'
            "def test_planted() -> None:\n    pass\n",
            "unreadable conformance marker",
            id="a-marker-the-listing-cannot-read",
        ),
    ],
)
def test_a_planted_marker_reds_the_finding_arm(tree, monkeypatch, planted, needle):
    """Property: one extra conformance-marked function planted in the copy, on a row that owes no test,
    on an id no row carries, under a mistyped equation id, as a second function carrying a bound id, or
    with a keyword the listing cannot read,
    fails the assertion on its FIRST `assert` with the evidence in the message.

    Class: the conformance markers that the ledger does not account for, one arm per way a marker can
    be unaccounted for. NEW CAPABILITY: the plausible wrong implementation checks only that each ledger
    row finds its test, so an extra marker is invisible to it; the criterion's last clause is exactly
    that no conformance-marked function names an equation-like unit that is not a `TEST` row.
    Reachability: any test file under the tree can carry the marker; the planted function is appended to
    a file the listing reads.
    Both-states anchor: the control arm is the unmodified copy, green; the planted arm changes only the
    appended text, and `coverage_findings` over the same copy is the oracle for the evidence named.
    Why: an equation tested in two places, or a typo that tests nothing, is the defect the ledger exists
    to stop.
    Kills: dropping check 4, dropping check 5, a case-sensitive or dash-required grammar.
    Second correct implementation: a grammar that admits the same spellings by other means passes.
    """
    _run_subject(monkeypatch, tree)
    _append(tree / "tests" / "conformance" / "test_status.py", planted)

    ledger = REAL_LOAD(tree / "tests" / "conformance" / "bindings.toml")
    findings = bindings.coverage_findings(ledger, EXPECTED_IDS, inventory.listing(tree))
    assert any(needle in finding for finding in findings), findings

    with pytest.raises(AssertionError) as excinfo:
        _run_subject(monkeypatch, tree)
    assert _failed_assert_index(excinfo) == 0
    assert needle in str(excinfo.value)


def test_a_retyped_bound_unit_reds_with_both_the_missing_and_the_stray_id(tree, monkeypatch):
    """Property: a `TEST` row's marker whose unit is retyped from `E-04` to `E-4` leaves the row with no
    test and the marker with no row, and the assertion fails on its first `assert` naming both.

    Class: a bound test whose marker no longer carries its row's id. NEW CAPABILITY: the plausible wrong
    implementation looks the row up by function name only, so the retyped unit still finds a function
    called what the row says; red under it, because the marker's own unit is what the criterion binds.
    Reachability: a one-character edit to a decorator is the commonest way a marker drifts.
    Both-states anchor: the control arm is the unmodified copy; the planted arm changes one keyword.
    Why: the criterion binds the id to the marker, not to the function name.
    Kills: matching a row by `function` alone; matching a unit by prefix.
    Second correct implementation: matching on `unit` equality by any route passes.
    """
    _run_subject(monkeypatch, tree)
    ledger = REAL_LOAD(tree / "tests" / "conformance" / "bindings.toml")
    _replace_once(_file_defining(tree, ledger["E-04"]["test"]), 'unit="E-04"', 'unit="E-4"')

    with pytest.raises(AssertionError) as excinfo:
        _run_subject(monkeypatch, tree)
    assert _failed_assert_index(excinfo) == 0
    text = str(excinfo.value)
    assert "E-04" in text and "'E-4'" in text


def test_a_bound_file_moved_outside_the_conformance_directory_reds_the_finding_arm(tree, monkeypatch):
    """Property: the file that holds one group of bound tests, moved to another directory under `tests/`,
    fails the assertion on its first `assert` and the message says the tests live outside the directory.

    Class: a `TEST` row whose one marked function exists, matches its row, and sits where the
    conformance family's own selection never looks. NEW CAPABILITY: the plausible wrong implementation
    matches on unit and function name, so a test the family never runs satisfies the ledger.
    Reachability: `listing` returns conformance entries from any file under `tests/`.
    Both-states anchor: the control arm is the same file in place, green; the planted arm moves the
    whole file and changes no text in it.
    Why: the criterion says each bound function lives under `tests/conformance/`.
    Kills: deleting the directory comparison from check 2.
    Second correct implementation: a comparison by path parts passes.
    """
    _run_subject(monkeypatch, tree)
    ledger = REAL_LOAD(tree / "tests" / "conformance" / "bindings.toml")
    moved = _file_defining(tree, ledger["E-04"]["test"])
    destination = tree / "tests" / "equivalence"
    destination.mkdir()
    shutil.move(moved, destination / moved.name)

    with pytest.raises(AssertionError) as excinfo:
        _run_subject(monkeypatch, tree)
    assert _failed_assert_index(excinfo) == 0
    assert "outside tests/conformance/" in str(excinfo.value)
    assert "E-04" in str(excinfo.value)


def test_a_row_retired_together_with_its_test_reds_the_partition_arm_alone(tree, monkeypatch):
    """Property: a `TEST` row reclassified `UNREACHABLE` while its function's marker is retyped to a
    unit that is not equation-like draws NO finding, and the assertion fails on its SECOND `assert`.

    Class: the one change the finding set cannot see, because the row now owes nothing and nothing is
    listed against it. NEW CAPABILITY: the plausible wrong implementation asserts the finding set only,
    or the partition only through a count of `TEST` rows read from the markers; red under the first.
    Reachability: retiring a test and its row in one edit is a normal-looking change.
    Both-states anchor: the precondition is asserted, on the same inputs, that `coverage_findings` is
    empty, so the red is attributable to the partition and to nothing else.
    Why: this is the edit that turns a published obligation into silence.
    Kills: dropping the partition assertion; comparing only the count of `TEST` rows.
    Second correct implementation: comparing the partition as a mapping or as four counts passes.
    """
    _run_subject(monkeypatch, tree)
    ledger_before = REAL_LOAD(tree / "tests" / "conformance" / "bindings.toml")
    test_name = ledger_before["E-04"]["test"]
    ledger_path = _ledger_text_with(
        tree, "E-04", "binding = \"UNREACHABLE\"\nground = '''retired together with its test'''"
    )
    _replace_once(_file_defining(tree, test_name), 'unit="E-04"', 'unit="retired-watershed-mean"')

    mutated = REAL_LOAD(ledger_path)
    assert bindings.coverage_findings(mutated, EXPECTED_IDS, inventory.listing(tree)) == []
    assert Counter(row["binding"] for row in mutated.values()) != PARTITION

    with pytest.raises(AssertionError) as excinfo:
        _run_subject(monkeypatch, tree, ledger_path)
    assert _failed_assert_index(excinfo) == 1


def test_a_ledger_and_tree_that_disagree_fail_as_findings_before_as_a_partition(tree, monkeypatch):
    """Property: a copy that breaks the finding set and the partition together fails on the FIRST
    `assert`, with the findings in the message.

    Class: a defect pair whose two halves each have an assertion. NEW CAPABILITY: the plausible wrong
    implementation asserts the partition first, so a broken ledger reads as a count mismatch and the
    author learns a number moved without learning which row.
    Reachability: moving `E-06` onto `TEST` with no function written moves the partition to 22/6/5/3 and
    leaves a missing-test finding at the same time.
    Both-states anchor: the precondition is that the partition really is wrong and the finding set
    really is non-empty on the same inputs.
    Why: the finding names the row; the partition names only a number.
    Kills: reordering the two assertions.
    Second correct implementation: any order that fails on the findings first passes.
    """
    _run_subject(monkeypatch, tree)
    ledger_path = _ledger_text_with(
        tree, "E-06", "binding = \"TEST\"\ntest = \"test_eq06_never_written\"\nground = '''a row ahead of its test'''"
    )
    mutated = REAL_LOAD(ledger_path)
    assert Counter(row["binding"] for row in mutated.values()) != PARTITION
    assert bindings.coverage_findings(mutated, EXPECTED_IDS, inventory.listing(tree)) != []

    with pytest.raises(AssertionError) as excinfo:
        _run_subject(monkeypatch, tree, ledger_path)
    assert _failed_assert_index(excinfo) == 0
    assert "E-06" in str(excinfo.value)


def test_a_second_reader_of_the_markers_finds_the_criterion_true_on_the_real_tree(real_ledger):
    """Property: read independently of `inventory.listing` and of `bindings.coverage_findings`, the real
    tree holds exactly one function under `tests/conformance/` for each `TEST` row's id and named as the
    row names it, none for any other id, and no equation-like unit that is not a `TEST` row's id; the
    ledger's ids are `E-01`..`E-36` and its partition is 21/6/6/3.

    Class: the criterion's own sentences, recounted by a reader that shares no code with the checker.
    NEW CAPABILITY: a defect in the shared listing, or in the coverage function, that makes both
    agree on a tree which violates the criterion, since the assertion under review trusts both.
    Reachability: the second reader walks every `test_*.py` under the real `tests/` directory.
    Both-states anchor: the oracle is built from the ledger rows and the files, not from any finding.
    Why: the real-tree assertion is the run's acceptance criterion, so one reader's green is not enough.
    Kills: a listing that skips a directory or a decorator form; a coverage function that counts rows
    and not markers.
    Second correct implementation: any independent walk of the markers passes.
    """
    assert tuple(real_ledger) == tuple(f"E-{number:02d}" for number in range(1, 37))
    assert dict(Counter(row["binding"] for row in real_ledger.values())) == PARTITION

    found = _own_conformance_markers(REAL_ROOT)
    assert found, "the second reader found no conformance marker at all"
    for equation_id, row in real_ledger.items():
        hits = [(file, function) for unit, file, function in found if unit == equation_id]
        if row["binding"] == "TEST":
            assert len(hits) == 1, f"{equation_id}: expected exactly one marked function; found {hits}"
            file, function = hits[0]
            assert function == row["test"], f"{equation_id}: {function} is marked, the row names {row['test']}"
            assert file.startswith("tests/conformance/"), f"{equation_id}: {file} is outside the directory"
        else:
            assert hits == [], f"{equation_id}: binding {row['binding']} owes no test and {hits} is marked"

    equation_like = {unit for unit, _, _ in found if isinstance(unit, str) and EQUATION_LIKE.fullmatch(unit)}
    bound = {equation_id for equation_id, row in real_ledger.items() if row["binding"] == "TEST"}
    assert equation_like == bound


def test_this_file_adds_no_family_marker_and_so_changes_no_listing(real_listing):
    """Property: no function of this file is listed as a family test and no unreadable marker is
    reported in it, so the listing the assertion under review reads is the one it would read without
    this file.

    Class: the markers of this file. REGRESSION: a family marker added to a check here would enter the
    conformance listing; if its unit were equation-like it would draw a finding against the assertion
    this file grades.
    Reachability: `listing` reads every `test_*.py` under `tests/`, this file included.
    Both-states anchor: the oracle is the listing's own entry list filtered by this file's path.
    Why: the grading arm must not be able to move the verdict of what it grades.
    Kills: a marker added to any check in this file.
    Second correct implementation: none; the property is the absence of the marker.
    """
    entries, findings = real_listing
    own = "tests/conformance/test_binding_coverage_review.py"
    assert [entry for entry in entries if entry["file"] == own] == []
    assert [finding for finding in findings if finding.startswith(own)] == []
