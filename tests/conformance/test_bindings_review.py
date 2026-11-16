"""Review tests for the binding ledger chunk: marker readability and form, the path arm, wrong-typed values.

These tests accompany `bindings.py`, `bindings.toml` and `test_bindings.py`. The ledger's coverage check
(`bindings.coverage_findings`) reads the conformance tests it binds through `inventory.listing`, a
STATIC reader that evaluates a marker's keyword values only when each is a string literal. So a
marker written with a module constant is a test the ledger, the real-tree inventory check and the
real-tree coverage assertion all report as unreadable. The tests below grade four classes the chunk's
own suite does not reach.

These carry no family marker, so they enter no inventory count; they are chunk tests and this file
guards `tests/conformance/bindings.py` and `tests/conformance/test_bindings.py` (a module test of
`bindings.py` on promotion; its tests pin no absolute count and no stored blob).

Not established: that the 36 binding assignments are the right ones, which no test here can decide.
"""

from __future__ import annotations

import ast

import pytest

from tests.conformance import bindings
from tests.conformance.register import EXPECTED_IDS, entries
from tests.correspondence import inventory

CONFORMANCE_DIR = "tests/conformance/"
BINDINGS_TESTS = "tests/conformance/test_bindings.py"
SYSTEM = "publication-math-conformance"


@pytest.fixture(scope="module")
def real_listing():
    """`inventory.listing` over the real tree, read once: its entries and its unreadable-marker findings."""
    return inventory.listing(inventory.REPO_ROOT)


def test_every_conformance_marker_under_the_conformance_directory_is_statically_readable(real_listing):
    """Property: `inventory.listing` reports no unreadable marker in any file under `tests/conformance/`.

    Class: every family marker written in a file under `tests/conformance/` — here the 16 markers of
    `test_bindings.py`, whose keyword values are the module names `MARKER_UNIT` and `SYSTEM` where
    the 44 landed markers write string literals. REGRESSION: red on a tree whose markers are module
    names (Unexecuted hypothesis — the read is `inventory.py:87-95`, which clears `readable` for any
    keyword value that is not an `ast.Constant`; no run of it is recorded here).
    Reachability: `listing` is the reader the real-tree inventory check, the ledger's fifth check and
    the real-tree coverage assertion all call, so each unreadable marker reaches all three as a finding.
    Both-states anchor: the assertion turns on the marker's keyword values being literals, a property
    of the file both before and after any repair, not on wording the repair adds.
    Why: an unreadable marker leaves a test pytest runs and no check lists, and the real-tree
    `test_the_inventory_equals_collection` and the real-tree coverage assertion go red on it for good.
    Kills: replacing a literal with a module constant in any marker under the directory.
    Second correct implementation: literals inlined in each decorator, or a generator that writes the
    decorators as literal source; either passes because only the static read of the marker is judged.
    """
    _, findings = real_listing
    unreadable = [finding for finding in findings if finding.startswith(CONFORMANCE_DIR)]
    assert unreadable == [], "\n".join(unreadable)


def test_the_binding_tests_carry_the_marker_the_chunk_specifies_each(real_listing):
    """Property: every `test_` function of `test_bindings.py` is listed once, as
    `conformance(unit="binding-ledger", part="{its own name}", system="publication-math-conformance")`.

    Class: the functions of one file, enumerated from that file's own syntax tree. NEW CAPABILITY:
    the plausible wrong implementation is a marker whose `part` is a stale or copied function name,
    or a file whose markers are all unreadable, which lists nothing; red under both, and under the
    present file because its markers are unreadable (Unexecuted hypothesis, as above).
    Reachability: a copied decorator keeps a `part` that names another function, the usual edit when
    a test is added by duplicating one; `listing` returns it with that `part`.
    Both-states anchor: the expected tuples come from the function names in the file, not from the
    marker text, so a repair that changes the marker leaves the oracle where it was.
    Why: the chunk specifies this exact marker for every function in the file, and the family's
    duplicate-identifier refusal reads `(family, unit, part)`.
    Kills: a `part` that differs from its function, a different `unit`, or a different `system`.
    Second correct implementation: a helper that stamps the marker on each function by name passes,
    because only the listed tuple is compared.
    """
    entries_listed, _ = real_listing
    source = (inventory.REPO_ROOT / BINDINGS_TESTS).read_text(encoding="utf-8")
    declared = sorted(
        node.name
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.FunctionDef) and node.name.startswith("test_")
    )
    listed = sorted(
        (entry["function"], entry["unit"], entry["part"], entry["system"])
        for entry in entries_listed
        if entry["file"] == BINDINGS_TESTS
    )
    assert declared, "the control read found no test function in the file"
    assert listed == [(name, "binding-ledger", name, SYSTEM) for name in declared]


def test_a_row_test_that_lives_outside_the_conformance_directory_is_reported(tmp_path):
    """Property: a `TEST` row whose one marked function sits in a file outside `tests/conformance/`
    is reported, naming the id and the file; the same function inside the directory reports nothing.

    Class: every location outside the directory that `inventory.listing` reads (any `test_*.py` under
    `tests/`), here `tests/equivalence/`. NEW CAPABILITY: the plausible wrong implementation matches
    the row on unit and function name only, so a test the conformance family's selection never runs
    satisfies the ledger; red under it, since the defect arm then reports nothing (Unexecuted
    hypothesis: the path arm is `bindings.py:183-187` and no test in `test_bindings.py` reaches it).
    Reachability: `listing` returns conformance-family entries from any file under `tests/`.
    Both-states anchor: the oracle is the file's location, present in both the current and a repaired
    check; the control arm shows the same function and row pass from the right directory.
    Why: AD-4 check 2 requires the function to be in a file under `tests/conformance/`; a marked
    function elsewhere is counted as bound and is not selected by the family's own run.
    Kills: deleting the prefix comparison from check 2.
    Second correct implementation: a path test by `Path.parts` or `PurePosixPath.is_relative_to`.
    """
    row = {"binding": "TEST", "test": "test_eq04_probe", "ground": "synthetic"}
    marked = (
        "import pytest\n\n\n"
        f'@pytest.mark.conformance(unit="E-04", part="test_eq04_probe", system="{SYSTEM}")\n'
        "def test_eq04_probe():\n    pass\n"
    )
    inside = tmp_path / "inside" / "tests" / "conformance"
    inside.mkdir(parents=True)
    (inside / "test_probe.py").write_text(marked, encoding="utf-8")
    outside = tmp_path / "outside" / "tests" / "equivalence"
    outside.mkdir(parents=True)
    (outside / "test_probe.py").write_text(marked, encoding="utf-8")

    control = bindings.coverage_findings({"E-04": row}, ("E-04",), inventory.listing(tmp_path / "inside"))
    assert control == []

    findings = bindings.coverage_findings({"E-04": row}, ("E-04",), inventory.listing(tmp_path / "outside"))
    assert any("E-04" in finding and "tests/equivalence/test_probe.py" in finding for finding in findings), findings


def _ledger_text(*, override_id: str, top_level: str | None = None, table: str | None = None) -> str:
    """A synthetic ledger text over the register's ids with one id's row replaced by raw TOML.

    Every other row follows the register's own span, as a fixture built from the register and not a
    copy of `bindings.toml`. `top_level` writes `{id} = {value}` before any table; `table` is the
    body of that id's own table.
    """
    register = entries()
    top: list[str] = []
    tables: list[str] = []
    for equation_id in EXPECTED_IDS:
        if equation_id == override_id:
            if top_level is not None:
                top.append(f"{equation_id} = {top_level}\n")
            else:
                tables.append(f"[{equation_id}]\n{table}\n")
        elif register[equation_id]["verbatim_span"] == "":
            tables.append(f'[{equation_id}]\nbinding = "NO-MANUSCRIPT-TEXT"\nground = "synthetic"\n')
        else:
            number = equation_id.removeprefix("E-")
            tables.append(
                f'[{equation_id}]\nbinding = "TEST"\ntest = "test_eq{number}_synthetic"\nground = "synthetic"\n'
            )
    return "".join(top) + "\n" + "\n".join(tables)


def test_a_wrong_typed_value_in_a_ledger_row_is_a_refusal_naming_the_id(tmp_path):
    """Property: a row that is not a table, or a `binding`, `ground` or `test` of the wrong type, raises
    `LedgerValidationError` naming the id, and never a raw `TypeError` or `AttributeError`.

    Class: every TOML value type outside the schema in each ledger position, here an integer and an
    array as the whole row, an integer and an array as `ground`, an integer as `test`, an integer as
    `binding`. NEW CAPABILITY: the plausible wrong implementation reads `value["ground"].strip()` or
    `"binding" in value` without a type test, so an integer row raises `TypeError` and an integer
    `ground` raises `AttributeError` out of `load` (Unexecuted hypothesis: red under it by
    construction, and the blank-ground arm of `test_bindings.py` writes a string so it does not reach this).
    Reachability: a hand edit of `bindings.toml` producing `E-05 = 5` or `ground = 5` is a valid TOML
    document, so `tomllib` hands the value to the validator.
    Both-states anchor: the oracle is the exception type and the id in its message, true of any
    refusal wording; the control arm loads the same synthetic ledger with a valid E-05 row.
    Why: AD-2 says the loader refuses any other shape by naming the id and the defect; a raw exception
    names neither and the caller cannot catch the family.
    Kills: dropping the `isinstance` tests in `_validate_row`.
    Second correct implementation: a schema library raising a wrapped validation error, or `try/except
    (TypeError, AttributeError)` re-raised as `LedgerValidationError`.
    """
    valid = 'binding = "TEST"\ntest = "test_eq05_synthetic"\nground = "synthetic"\n'
    path = tmp_path / "control.toml"
    path.write_text(_ledger_text(override_id="E-05", table=valid), encoding="utf-8")
    assert len(bindings.load(path)) == len(EXPECTED_IDS)

    cases = {
        "row-integer": {"top_level": "5"},
        "row-array": {"top_level": '["TEST"]'},
        "ground-integer": {"table": 'binding = "TEST"\ntest = "test_eq05_synthetic"\nground = 5\n'},
        "ground-array": {"table": 'binding = "TEST"\ntest = "test_eq05_synthetic"\nground = ["x"]\n'},
        "test-integer": {"table": 'binding = "TEST"\ntest = 5\nground = "synthetic"\n'},
        "binding-integer": {"table": 'binding = 5\nground = "synthetic"\n'},
    }
    for name, case in cases.items():
        path = tmp_path / f"{name}.toml"
        path.write_text(_ledger_text(override_id="E-05", **case), encoding="utf-8")
        with pytest.raises(bindings.LedgerValidationError) as refusal:
            bindings.load(path)
        assert "E-05" in str(refusal.value), (name, str(refusal.value))
