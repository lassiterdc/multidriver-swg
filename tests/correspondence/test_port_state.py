"""Tests of the port-state declaration: the ported rule, the unproven and dispositioned counts, the not-measured state, and the real tree's line.

The synthetic cases copy the corpus record, the tracked legacy corpus and the ledger into a git
repository inside `tmp_path` (staged, never committed), then add a synthetic test file whose
equivalence tests bear the key of the first retained unit, a proof-record file, and, where the
case needs one, a synthetic run record in the form the recorder writes. Each synthetic case reads
the declaration's counts rather than its printed line, so a change to how the line is rendered
reds only the real-tree test.
"""

import json
import shutil
import subprocess
import tomllib
from pathlib import Path

import pytest

from tests.correspondence import corpus_record, extent, ledger, port_state, proofs

pytestmark = pytest.mark.state_check

SYSTEM = "pre-refactor-correspondence"
PROOFS_HEADER = 'rule = "proof-record:1"\nstatement = "synthetic"\n'
LINE = (
    "port state: files 18 in the corpus manifest, 0 the parser cannot read; units 188 enumerated, "
    "31 dispositioned, 0 ported (50 more units in 4 ceded-file scope-out records); tests 0 unproven of "
    "0 equivalence tests; a unit is ported once it has one or more equivalence tests and each of them "
    "passed in the given run against new code its system registered"
)


def _tree(tmp_path: Path, parts: list[str], proof_parts: list[str] = ()) -> tuple[Path, str]:
    """Build a synthetic tree with one equivalence test per part of the first retained unit; return it and that key.

    `proof_parts` names, in order, the part of each proof row to write; a part may repeat.
    """
    root = corpus_record.REPO_ROOT
    listed = subprocess.run(
        ["git", "ls-files", "-z", "--", "_old_code_to_refactor"], cwd=root, capture_output=True, check=True
    ).stdout.decode("utf-8")
    paths = [p for p in listed.split("\0") if p] + [corpus_record.RECORD_NAME]
    paths += [str(p.relative_to(root)) for p in (root / ledger.LEDGER_DIR).glob("*.toml")]
    for rel in paths:
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(root / rel, tmp_path / rel)
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "-A"], cwd=tmp_path, check=True)
    name = extent.retained_files(tmp_path)[0]
    key = extent.extents(name, extent._read(tmp_path, name))[0][0]["key"]
    tests = ["import pytest\n"]
    for part in parts:
        tests.append(
            f'\n\n@pytest.mark.equivalence(unit="{key}", system="{SYSTEM}", part="{part}")\n'
            f"def test_{part}():\n    pass\n"
        )
    (tmp_path / "tests").mkdir(exist_ok=True)
    (tmp_path / "tests" / "test_synthetic.py").write_text("".join(tests), encoding="utf-8")
    rows = "".join(
        f'\n[[proof]]\nfamily = "equivalence"\nunit = "{key}"\npart = "{part}"\nroute = "authoring-red"\n'
        for part in proof_parts
    )
    (tmp_path / proofs.RECORDS).write_text(PROOFS_HEADER + rows, encoding="utf-8")
    return tmp_path, key


def _run_record(path: Path, key: str, outcomes: dict[str, tuple[str, bool]]) -> Path:
    """Write a run record holding one line per part: its outcome and whether it was bound."""
    lines = [
        json.dumps({"family": "equivalence", "unit": key, "part": part, "outcome": outcome, "bound": bound})
        for part, (outcome, bound) in outcomes.items()
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def test_a_unit_is_ported_only_when_every_one_of_its_tests_passed_bound(tmp_path):
    """Property: one test that did not pass keeps its unit out of the ported count, whatever the unit's other tests did.

    Class: any unit with two or more equivalence tests in a run where at least one passed bound and
    at least one did not; the satisfying input is the same unit with every test passing bound.
    Why: a unit is replaced by its new code only when all of its tests hold, so counting a unit
    whose other test failed would print a unit as ported while its new code still breaks a
    comparison. Killed by: letting a single passing test carry its whole unit into the count.
    Second correct implementation: grouping the run record's lines by unit and asking whether the
    group's outcomes are all `passed` with `bound` true, rather than marking each test first.
    """
    root, key = _tree(tmp_path / "tree", ["a", "b"])
    mixed = _run_record(tmp_path / "mixed.jsonl", key, {"a": ("passed", True), "b": ("failed", True)})
    assert port_state.declaration(root, mixed)["ported"] == 0
    both = _run_record(tmp_path / "both.jsonl", key, {"a": ("passed", True), "b": ("passed", True)})
    assert port_state.declaration(root, both)["ported"] == 1


def test_unproven_counts_the_tests_no_proof_row_names(tmp_path):
    """Property: the unproven count is how many equivalence tests no proof row names, printed out of all of them.

    Class: any set of equivalence tests of which some have proof rows and some do not, including a
    test with more than one proof row, which a second proof route legitimately gives it. Why: a
    test with no recorded proof may be one that cannot fail, so the count must say how many such
    tests exist; a count of proof rows says how many proofs were written, which is a different
    number as soon as one test has two. Killed by: counting proof rows instead of the tests they
    name — under it the second tree reads two unproven or none, where it holds one.
    Second correct implementation: the size of the set difference between the tests' identities
    and the proof rows' identities.
    """
    root, _key = _tree(tmp_path / "one_row", ["a", "b"], ["a"])
    counts = port_state.declaration(root)
    assert (counts["unproven"], counts["equivalence_tests"]) == (1, 2)
    root, _key = _tree(tmp_path / "two_rows_one_test", ["a", "b"], ["a", "a"])
    counts = port_state.declaration(root)
    assert (counts["unproven"], counts["equivalence_tests"]) == (1, 2)


def test_without_a_run_record_ported_is_not_measured_once_an_equivalence_test_exists(tmp_path):
    """Property: read with no run record, the ported count is `not measured` once any equivalence test exists, and 0 while none does.

    Class: any tree holding an equivalence test, read with no run record; the satisfying
    input is a tree holding none. Why: whether a test passed is a fact about one run, so with no run
    record a 0 would claim a measurement nobody made, while with no test at all no unit can be
    ported and the 0 is measured. Killed by: printing 0 whenever no run record is supplied.
    Second correct implementation: deciding on the number of equivalence tests before reading the
    run record at all, rather than inside the ported count.
    """
    root, _key = _tree(tmp_path / "one_test", ["a"])
    assert port_state.declaration(root)["ported"] == "not measured"
    root, _key = _tree(tmp_path / "no_test", [])
    assert port_state.declaration(root)["ported"] == 0


def test_a_test_that_passed_against_the_placeholder_does_not_port_its_unit(tmp_path):
    """Property: a unit whose every test passed is still not ported when a test passed without a registered callable.

    Class: any unit all of whose run-record lines read `passed` while at least one reads `bound`
    false. Why: a test that passed with `bound` false either requested no new code or compared
    against the placeholder, so it says nothing about the owning system's new code; counting it
    would print a unit as ported that has no new code behind it. Killed by: dropping the `bound`
    conjunct from the per-test pass, so that outcome `passed` alone counts. Second correct
    implementation: filtering the run-record lines to those with `bound` true before grouping them,
    rather than testing `bound` inside the per-test check.
    """
    root, key = _tree(tmp_path / "tree", ["a"])
    unbound = _run_record(tmp_path / "unbound.jsonl", key, {"a": ("passed", False)})
    assert port_state.declaration(root, unbound)["ported"] == 0


def test_a_test_the_run_record_does_not_mention_did_not_pass(tmp_path):
    """Property: with a run record supplied, a test the record holds no line for counts as not passed.

    Class: any supplied run record that omits one or more equivalence tests of a unit, including the
    record of a run whose selection excluded every equivalence test, which is an empty file. Why: a
    unit enters the ported count only through tests that passed in that run, and a test that did not run
    in it has passed nothing there; reading an omission as a pass would print every unit as ported
    after a run that selected none of their tests. Killed by: treating a test absent from the run
    record as passed. Second correct implementation: building the set of identities that passed
    bound and asking whether each of the unit's tests is a member of it.
    """
    root, _key = _tree(tmp_path / "tree", ["a"])
    empty = tmp_path / "empty.jsonl"
    empty.write_text("", encoding="utf-8")
    assert port_state.declaration(root, empty)["ported"] == 0


def test_the_dispositioned_count_counts_units_not_ledger_rows(tmp_path):
    """Property: the dispositioned count is the number of enumerated units some ledger row names, not the number of rows.

    Class: any ledger in which the rows and the units they name differ in number — two rows naming
    one unit, which the ledger's own check refuses but which the declaration must still count
    truthfully. Why: the declaration prints units against units (enumerated, dispositioned,
    ported), so a row count beside a unit count would overstate the work done by every duplicate
    row. Killed by: counting the ledger rows instead of the distinct enumerated units they name.
    Second correct implementation: counting the enumerated units for which any row's key equals
    the unit's key, unit by unit, rather than intersecting two sets.
    """
    root, _key = _tree(tmp_path / "tree", [])
    folder = root / ledger.LEDGER_DIR
    target = next(p for p in sorted(folder.glob("*.toml")) if tomllib.loads(p.read_text(encoding="utf-8")).get("row"))
    first = tomllib.loads(target.read_text(encoding="utf-8"))["row"][0]
    duplicate = "\n[[row]]\n" + "".join(f"{field} = {json.dumps(value)}\n" for field, value in first.items())
    with target.open("a", encoding="utf-8") as handle:
        handle.write(duplicate)
    rows, _ceded, _findings = ledger.load(root)
    assert port_state.declaration(root)["dispositioned"] == len(rows) - 1


def test_the_declaration_line_on_the_real_tree(capsys):
    """Property: the command prints the real tree's declaration line exactly and exits 0.

    Class: the real tree at landing, where no equivalence test, proof row or run record exists.
    Why: every count must be printed with its unit and beside the ported rule, so a reader of a zero
    knows what was counted; this line changes when the first equivalence test or ledger row is
    added, and the change that adds it updates this test. Killed by: leaving the unit counts out of
    the line. Second correct implementation: building the line from a list of labelled fields joined by
    separators rather than from one format string.
    """
    assert port_state.main([]) == 0
    assert capsys.readouterr().out == LINE + "\n"
