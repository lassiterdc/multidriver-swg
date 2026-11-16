"""Prove the binding ledger's loader refuses every malformed row and its coverage check names every defect.

The ledger is the one place a reader learns what this repository owes each of the register's 36
equations, and every later check reads it through `bindings.load`. So a row the loader accepts is a
row the coverage check, the real-tree assertion and the classifier all trust: a `TEST` row that
silently lost its `test` would report an equation as tested while naming nothing, and a `binding`
word outside the closed set would arrive at a consumer as a derived `Binding` that never existed.
Nothing else reads the ledger, so these refusals are the only place those shapes are refused.

The positive arms read the real ledger, so a checked-in file that drifts from the schema fails. The
refusal arms drive synthetic ledgers built FROM THE REAL REGISTER'S ID SET AND ITS SPAN POPULATION,
never a copy of `bindings.toml`: a fixture that copies the real ledger re-reds on that file's first
real row, and it would hide the manuscript-agreement refusal behind a ledger that already agrees.

The five coverage checks are driven on `pytester` trees, each with a clean arm that must report no
finding and a defect arm that must name it, because a check with only one arm cannot tell a detector
from a function that always returns a finding.

What this does NOT establish: that the 36 binding assignments are the RIGHT ones. That is a
judgement about the paper and the code, carried by each row's `ground` and reviewed against § B2;
these tests pin the schema, the two derivations and the five checks, and no reading of this file can
answer whether `E-06` is genuinely unreachable.

Retirement disposition: this file guards `tests/conformance/bindings.py`'s refusals, its two
derivations and `coverage_findings`. Its partition assertion pins the published absolute 21/6/6/3
figure, which `test_binding_coverage.py` asserts again on the real tree when the equation tests
land; on promotion that assertion moves there and the rest is promoted as a module test of
`bindings.py`.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import pytest

from multidriver_swg.conformance.taxonomy import Binding
from tests.conformance import bindings
from tests.conformance.register import EXPECTED_IDS, entries
from tests.correspondence import inventory

# `SYSTEM` is used INSIDE the synthetic marker strings this file renders for its `pytester` trees.
# Every marker this file itself carries writes its `unit` and `system` as STRING LITERALS and never
# as a name: `inventory.listing` clears a marker's `readable` flag for any keyword whose value is not
# an `ast.Constant`, so a marker built from a module constant is reported as an unreadable marker by
# the real-tree inventory check and by this ledger's own fifth coverage check — the test would run and
# nothing would list it.
SYSTEM = "publication-math-conformance"


def _test_name(equation_id: str) -> str:
    """A deterministic synthetic function name for one equation id, distinct per id."""
    return f"test_{equation_id.replace('-', '').lower()}_synthetic"


def _rows() -> dict[str, dict]:
    """A synthetic ledger derived from the real register's id set and its span population.

    Each row's binding follows what the register's own `verbatim_span` decides — an empty span is
    `NO-MANUSCRIPT-TEXT`, a stated span is `TEST` — so the fixture agrees with the loader's
    manuscript rule by construction rather than by copying the checked-in file.
    """
    register = entries()
    rows: dict[str, dict] = {}
    for equation_id in EXPECTED_IDS:
        if register[equation_id]["verbatim_span"] == "":
            rows[equation_id] = {
                "binding": "NO-MANUSCRIPT-TEXT",
                "ground": f"synthetic: the paper states no equation for {equation_id}",
            }
        else:
            rows[equation_id] = {
                "binding": "TEST",
                "test": _test_name(equation_id),
                "ground": f"synthetic: {equation_id} is implemented and testable",
            }
    return rows


def _toml_string(value: str) -> str:
    """Render `value` as a TOML basic string, escaping the two characters that would end it early."""
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _ledger_file(root: Path, name: str, rows: dict[str, dict]) -> Path:
    """Write `rows` as a ledger file TOML can parse, one table per id, and return its path."""
    text = "".join(
        f"[{equation_id}]\n" + "".join(f"{field} = {_toml_string(value)}\n" for field, value in row.items()) + "\n"
        for equation_id, row in rows.items()
    )
    path = root / f"{name}.toml"
    path.write_text(text, encoding="utf-8")
    return path


def _loads(root: Path, name: str, rows: dict[str, dict]) -> dict:
    """Load `rows` through the real loader, so every refusal arm starts from a ledger that is accepted."""
    loaded = bindings.load(_ledger_file(root, name, rows))
    assert len(loaded) == len(EXPECTED_IDS), loaded
    return loaded


def _refused(root: Path, name: str, rows: dict[str, dict]) -> str:
    """Load `rows` expecting a refusal, and return its message."""
    path = _ledger_file(root, name, rows)
    with pytest.raises(bindings.LedgerValidationError) as refusal:
        bindings.load(path)
    return str(refusal.value)


def _marked_function(equation_id: str, function: str) -> str:
    """One conformance-marked function whose own marker names `equation_id` and `function`."""
    return (
        f'\n@pytest.mark.conformance(unit="{equation_id}", part="{function}", system="{SYSTEM}")\n'
        f"def {function}():\n    pass\n"
    )


def _tree_source(
    rows: dict[str, dict],
    *,
    dropped: frozenset[str] = frozenset(),
    renamed: dict[str, str] | None = None,
    duplicated: frozenset[str] = frozenset(),
    extra: str = "",
) -> str:
    """Render a synthetic test tree's source: one marked function per `TEST` row, with named defects."""
    renamed = renamed or {}
    parts = ["import pytest\n"]
    for equation_id, row in rows.items():
        if row["binding"] != "TEST" or equation_id in dropped:
            continue
        parts.append(_marked_function(equation_id, renamed.get(equation_id, row["test"])))
        if equation_id in duplicated:
            parts.append(_marked_function(equation_id, f"{row['test']}_second"))
    parts.append(extra)
    return "".join(parts)


def _tree(pytester, source: str):
    """Write `source` under `pytester`'s tree at `tests/conformance/`, returning the root.

    The directory is `tests/conformance/` rather than `tests/` because check 2 requires a `TEST`
    row's function to live inside the conformance family's directory; a fixture written one level
    up would red every clean arm with a defect the arm does not name.
    """
    directory = pytester.path / "tests" / "conformance"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "test_conformance.py").write_text(source, encoding="utf-8")
    return pytester.path


@pytest.mark.conformance(
    unit="binding-ledger",
    part="test_the_real_ledger_loads_with_the_published_partition",
    system="publication-math-conformance",
)
def test_the_real_ledger_loads_with_the_published_partition() -> None:
    """Property: the checked-in ledger loads, carries the register's 36 ids in order, and partitions 21
    `TEST`, 6 `CROSS-REPO`, 6 `UNREACHABLE`, 3 `NO-MANUSCRIPT-TEXT`.

    Class: NEW CAPABILITY — the plausible wrong implementation is a ledger transcribed from § B2's
    Status column without its test column, which yields `TEST` rows carrying no function name; this
    test is red under it at the `test` assertion and at the partition. Why: the partition is the
    published figure and the ledger is where it is written down, so a row moving between groups is
    exactly the drift an agreement assertion over the ids alone cannot see. Reachability: the real
    file is the input, so a missing, extra or reclassified row reaches these assertions.
    Invocation isolated: the ledger is found from `__file__`, so neither the working directory nor
    the checkout changes the result. Kills: dropping a row, writing a derived field, or binding a
    text-less equation as `TEST`. Second correct implementation: a loader that sorts ids on load
    passes, because `E-01`..`E-36` sorts identically to its declared order.
    """
    rows = bindings.load()
    assert tuple(rows) == EXPECTED_IDS
    partition = Counter(row["binding"] for row in rows.values())
    assert dict(partition) == {"TEST": 21, "CROSS-REPO": 6, "UNREACHABLE": 6, "NO-MANUSCRIPT-TEXT": 3}
    assert [equation_id for equation_id, row in rows.items() if not row["ground"].strip()] == []
    assert [
        equation_id for equation_id, row in rows.items() if row["binding"] == "TEST" and not row["test"].strip()
    ] == []


@pytest.mark.conformance(
    unit="binding-ledger",
    part="test_binding_of_and_reachable_of_derive_from_the_one_word",
    system="publication-math-conformance",
)
def test_binding_of_and_reachable_of_derive_from_the_one_word() -> None:
    """Property: each of the four wire words derives its landed `Binding` and its reachability, with
    reachability false exactly for `UNREACHABLE`; an unknown word is refused by both functions.

    Class: NEW CAPABILITY — the plausible wrong implementation is `reachable_of` reading
    `binding_of(row)` and comparing it against `Binding.UNREACHABLE`, which cannot exist because
    `UNREACHABLE` lands as `BOUND`; every row would then read reachable and this test is red on the
    `UNREACHABLE` case. Why: the two facts live in one word, so a derivation that reads the other
    derived value answers a question about a value that never encoded it. Reachability: the four
    words are the ledger's whole closed set, so every row's answer is covered, and the `TEST` case
    is the must-fire control that the lookup is not simply returning `False`. Invocation isolated:
    the function reads only the dict it is handed. Kills: collapsing `UNREACHABLE` into `BOUND`
    reachability, and admitting a word outside the closed set. Second correct implementation: a
    match statement over the four words, or a mapping from word to a (binding, reachable) pair.
    """
    cases = (
        ("TEST", Binding.BOUND, True),
        ("CROSS-REPO", Binding.CROSS_REPO, True),
        ("UNREACHABLE", Binding.BOUND, False),
        ("NO-MANUSCRIPT-TEXT", Binding.NO_MANUSCRIPT_TEXT, True),
    )
    for word, landed, reachable in cases:
        row = {"binding": word}
        assert bindings.binding_of(row) is landed, word
        assert bindings.reachable_of(row) is reachable, word
    with pytest.raises(bindings.LedgerValidationError):
        bindings.binding_of({"binding": "BOUND"})
    with pytest.raises(bindings.LedgerValidationError):
        bindings.reachable_of({"binding": "REACHABLE"})


@pytest.mark.conformance(
    unit="binding-ledger", part="test_load_refuses_a_missing_equation_row", system="publication-math-conformance"
)
def test_load_refuses_a_missing_equation_row(tmp_path) -> None:
    """Property: a ledger missing one register id is refused, naming the id.

    Class: NEW CAPABILITY — the plausible wrong implementation validates whatever tables the file
    holds, so a ledger transcribed from a filtered § B2 scrape agrees with itself while ranging over
    35 equations. Why: every coverage check reads the ledger's own ids, so a missing row narrows
    what the checks range over and reports nothing — the narrowing reads as a smaller corpus.
    Reachability: the control load of the same synthetic ledger is accepted (`_loads`), so the
    refusal is the missing row and not the fixture. Invocation isolated: the file is written under
    `tmp_path`. Kills: dropping `E-05` from the ledger. Second correct implementation: refusing on a
    count of 35, or on the id set, both report.
    """
    rows = _rows()
    _loads(tmp_path, "control", rows)
    rows.pop("E-05")
    message = _refused(tmp_path, "missing", rows)
    assert "E-05" in message and "missing" in message, message


@pytest.mark.conformance(
    unit="binding-ledger", part="test_load_refuses_an_id_the_register_lacks", system="publication-math-conformance"
)
def test_load_refuses_an_id_the_register_lacks(tmp_path) -> None:
    """Property: a ledger carrying an id outside the register's 36 is refused, naming the id.

    Class: NEW CAPABILITY — the plausible wrong implementation iterates the file's own keys only, so
    a table added for a renamed or invented equation loads clean and every check skips it. Why: an
    extra id is an equation this repository claims to bind and the register cannot resolve, which is
    a false obligation rather than a missing one. Reachability: the control load is accepted, then
    one extra table is added. Invocation isolated: the file is written under `tmp_path`. Kills:
    adding `E-99`. Second correct implementation: refusing on any key not in `EXPECTED_IDS`.
    """
    rows = _rows()
    _loads(tmp_path, "control", rows)
    rows["E-99"] = {"binding": "TEST", "test": "test_e99_synthetic", "ground": "synthetic"}
    message = _refused(tmp_path, "extra", rows)
    assert "E-99" in message and "unexpected" in message, message


@pytest.mark.conformance(
    unit="binding-ledger",
    part="test_load_refuses_a_binding_outside_the_closed_set",
    system="publication-math-conformance",
)
def test_load_refuses_a_binding_outside_the_closed_set(tmp_path) -> None:
    """Property: a row whose `binding` is a landed value or any other word is refused, naming both the
    value and the closed set.

    Class: NEW CAPABILITY — the plausible wrong implementation accepts a `Binding` member's value
    (`BOUND`), because the vocabulary is the one the rest of the system speaks; the row is then
    unreachable by every check keyed on `TEST` and reads as a bound equation with no test. Why: the
    ledger's word is the single home of the fact, so a synonym is a second value for it. Reachability:
    the control load is accepted, then one word is replaced. Invocation isolated: the file is written
    under `tmp_path`. Kills: writing `BOUND` in place of `TEST`. Second correct implementation:
    refusing through a `Binding(value)` construction that raises on `TEST`.
    """
    rows = _rows()
    _loads(tmp_path, "control", rows)
    rows["E-05"]["binding"] = "BOUND"
    message = _refused(tmp_path, "unknown-word", rows)
    assert "'BOUND'" in message and "closed set" in message, message


@pytest.mark.conformance(
    unit="binding-ledger", part="test_load_refuses_a_test_key_on_a_non_test_row", system="publication-math-conformance"
)
def test_load_refuses_a_test_key_on_a_non_test_row(tmp_path) -> None:
    """Property: a `CROSS-REPO` row carrying a `test` key is refused, naming the unexpected field.

    Class: NEW CAPABILITY — the plausible wrong implementation copies rows from a working ledger and
    edits the binding word, leaving the function name behind. Why: the stray name would be a second
    answer to "does this equation owe a test", and its presence reads as a row somebody meant to
    test, which no reader can distinguish from a row nobody finished editing. Reachability: the
    control load is accepted, then the key is added. Invocation isolated: the file is under
    `tmp_path`. Kills: adding `test` to `E-01`. Second correct implementation: refusing on a field
    set computed from the binding word, as here, or on the reverse direction `binding != TEST and
    test in row`.
    """
    rows = _rows()
    _loads(tmp_path, "control", rows)
    rows["E-01"] = {"binding": "CROSS-REPO", "ground": "synthetic: implemented in a sibling repository"}
    rows["E-01"]["test"] = "test_eq01_synthetic"
    message = _refused(tmp_path, "stray-test", rows)
    assert "E-01" in message and "unexpected test" in message, message


@pytest.mark.conformance(
    unit="binding-ledger", part="test_load_refuses_a_test_row_without_a_function", system="publication-math-conformance"
)
def test_load_refuses_a_test_row_without_a_function(tmp_path) -> None:
    """Property: a `TEST` row carrying no `test` key is refused, naming the missing field.

    Class: NEW CAPABILITY — the plausible wrong implementation treats `test` as optional, so the row
    advertises an equation as tested and names nothing; every coverage check then looks for zero
    entries and reports a finding against a row whose author believed it complete. Why: the row's
    whole job is to name the function, so its absence is a missing obligation rather than untidy
    metadata. Reachability: the control load is accepted, then the key is removed. Invocation
    isolated: the file is under `tmp_path`. Kills: removing `test` from `E-05`. Second correct
    implementation: a row dataclass whose required fields are enforced at construction.
    """
    rows = _rows()
    _loads(tmp_path, "control", rows)
    rows["E-05"].pop("test")
    message = _refused(tmp_path, "missing-test", rows)
    assert "E-05" in message and "missing test" in message, message


@pytest.mark.conformance(
    unit="binding-ledger", part="test_load_refuses_a_blank_ground", system="publication-math-conformance"
)
def test_load_refuses_a_blank_ground(tmp_path) -> None:
    """Property: a row whose `ground` is empty or whitespace is refused, naming the id.

    Class: NEW CAPABILITY — the plausible wrong implementation requires the key to exist and not its
    content, so `ground = ""` loads and the one field that records WHY the row says what it says is
    gone. Why: the ground is the only artifact a reviewer can check the classification against § B2
    with, so a blank one turns a decision into an assertion. Reachability: the control load is
    accepted, then four spaces are written. Invocation isolated: the file is under `tmp_path`.
    Kills: blanking `E-05`'s ground. Second correct implementation: stripping before the emptiness
    test, or requiring a minimum character count.
    """
    rows = _rows()
    _loads(tmp_path, "control", rows)
    rows["E-05"]["ground"] = "    "
    message = _refused(tmp_path, "blank-ground", rows)
    assert "E-05" in message and "ground" in message, message


@pytest.mark.conformance(
    unit="binding-ledger",
    part="test_load_refuses_negating_an_empty_manuscript_span",
    system="publication-math-conformance",
)
def test_load_refuses_negating_an_empty_manuscript_span(tmp_path) -> None:
    """Property: an equation whose register span is empty cannot be bound `TEST`, and the refusal says
    the empty span decides `NO-MANUSCRIPT-TEXT`.

    Class: NEW CAPABILITY — the plausible wrong implementation checks the ledger against itself, so a
    text-less equation bound `TEST` loads and the coverage check then demands a test for an equation
    the manuscript never states. Why: the register's span is the manuscript's own evidence about its
    coverage, and a ledger that contradicts it converts a paper omission into a code defect. `E-34`
    is the register's own empty-span carrier, so the fixture's span population supplies the input.
    Reachability: the control load of the same ledger is accepted, then `E-34` is bound `TEST`.
    Invocation isolated: the file is under `tmp_path` and the span is read from the register package,
    which resolves from `__file__`. Kills: binding `E-34` as `TEST`. Second correct implementation:
    consulting `manuscript_binding`, as here, or refusing on the register's empty-span set directly.
    """
    rows = _rows()
    _loads(tmp_path, "control", rows)
    rows["E-34"] = {"binding": "TEST", "test": "test_e34_synthetic", "ground": "synthetic"}
    message = _refused(tmp_path, "empty-span", rows)
    assert "E-34" in message and "verbatim_span is empty" in message, message


@pytest.mark.conformance(
    unit="binding-ledger",
    part="test_load_refuses_a_text_less_binding_on_a_stated_span",
    system="publication-math-conformance",
)
def test_load_refuses_a_text_less_binding_on_a_stated_span(tmp_path) -> None:
    """Property: an equation whose register carries a span cannot be bound `NO-MANUSCRIPT-TEXT`.

    Class: NEW CAPABILITY — the plausible wrong implementation applies the manuscript rule in one
    direction only, so a stated equation classified text-less loads; its row then owes no test while
    § B2 prints the equation, and the equation silently leaves the tested population. Why: the two
    directions are separate refusals because a single-direction check passes every fixture that only
    ever tests the empty-span arm. Reachability: the control load is accepted, then `E-04` is
    reclassified. Invocation isolated: the file is under `tmp_path`. Kills: binding `E-04` as
    `NO-MANUSCRIPT-TEXT`. Second correct implementation: comparing the register's span emptiness
    against the ledger's word as a bijection.
    """
    rows = _rows()
    _loads(tmp_path, "control", rows)
    rows["E-04"] = {"binding": "NO-MANUSCRIPT-TEXT", "ground": "synthetic"}
    message = _refused(tmp_path, "stated-span", rows)
    assert "E-04" in message and "the manuscript does state this equation" in message, message


@pytest.mark.conformance(
    unit="binding-ledger", part="test_load_refuses_a_written_derived_field", system="publication-math-conformance"
)
def test_load_refuses_a_written_derived_field(tmp_path) -> None:
    """Property: a row that writes `reachable` — a derived field — is refused as an unexpected field.

    Class: NEW CAPABILITY — the plausible wrong implementation tolerates extra keys, so a row can
    carry both the ledger's word and the derived fact. Why: the two homes are then two statements of
    one fact and can disagree, which is exactly the defect `status.py`'s own ground records; the
    refusal is keyed on the field set rather than on `reachable` alone so no derived field can be
    added later without landing here. Reachability: the control load is accepted, then `reachable`
    is added with the value the derivation would produce, so even an agreeing copy is refused.
    Invocation isolated: the file is under `tmp_path`. Kills: writing `reachable = true` beside
    `binding = "UNREACHABLE"`. Second correct implementation: a schema library with
    `extra="forbid"`.
    """
    rows = _rows()
    _loads(tmp_path, "control", rows)
    rows["E-05"]["reachable"] = "true"
    message = _refused(tmp_path, "derived-field", rows)
    assert "E-05" in message and "unexpected reachable" in message, message


@pytest.mark.conformance(
    unit="binding-ledger",
    part="test_coverage_reports_a_missing_and_an_unexpected_id",
    system="publication-math-conformance",
)
def test_coverage_reports_a_missing_and_an_unexpected_id(pytester) -> None:
    """Property: check 1 reports a register id the ledger lacks and a ledger id the register lacks,
    while the clean world reports nothing.

    Class: NEW CAPABILITY — the plausible wrong implementation compares the two id sets one way and
    calls the comparison a total check. Why: a missing row narrows the population every later check
    ranges over, and an unexpected row claims an obligation no equation carries; each is invisible in
    the other's direction. Reachability: the clean arm is the same synthetic ledger and tree with no
    defect, and each defect arm changes exactly one side. Invocation isolated: the tree is under
    `pytester.path` and the register ids are imported. Kills: dropping `E-05`, and adding `E-99`.
    Second correct implementation: two set differences, or a sorted pairwise walk with sentinels.
    """
    rows = _rows()
    clean = inventory.listing(_tree(pytester, _tree_source(rows)))
    assert bindings.coverage_findings(rows, EXPECTED_IDS, clean) == []

    missing = dict(rows)
    missing.pop("E-05")
    findings = bindings.coverage_findings(missing, EXPECTED_IDS, clean)
    assert any("E-05" in finding and "register carries this equation" in finding for finding in findings), findings

    unexpected = dict(rows)
    unexpected["E-99"] = {"binding": "TEST", "test": "test_e99_synthetic", "ground": "synthetic"}
    findings = bindings.coverage_findings(unexpected, EXPECTED_IDS, clean)
    assert any("E-99" in finding and "ledger carries this id" in finding for finding in findings), findings


@pytest.mark.conformance(
    unit="binding-ledger",
    part="test_coverage_reports_a_test_row_its_test_disagrees",
    system="publication-math-conformance",
)
def test_coverage_reports_a_test_row_its_test_disagrees(pytester) -> None:
    """Property: check 2 reports a `TEST` row with no entry, with two entries, or with an entry whose
    function name differs from the row's — and the clean tree reports nothing.

    Class: NEW CAPABILITY — the plausible wrong implementation looks for a function NAMED by the row
    anywhere in the family (or looks for the row's unit anywhere), and accepts a rename because
    either side alone still matches; a renamed test then sits green while the row's name resolves to
    nothing. Why: the row and the function are two statements of one identity, and the check's job is
    their agreement. Reachability: the clean arm pairs the same ledger with the same tree; each
    defect arm changes one side. Invocation isolated: the tree is under `pytester.path`. Kills:
    renaming `E-05`'s function, duplicating it under the same unit, and dropping it. Second correct
    implementation: indexing the listing by unit and comparing the row's name against the single
    member.
    """
    rows = _rows()
    clean = inventory.listing(_tree(pytester, _tree_source(rows)))
    assert bindings.coverage_findings(rows, EXPECTED_IDS, clean) == []

    renamed = inventory.listing(_tree(pytester, _tree_source(rows, renamed={"E-05": "test_eq05_renamed"})))
    findings = bindings.coverage_findings(rows, EXPECTED_IDS, renamed)
    assert any("E-05" in finding and _test_name("E-05") in finding for finding in findings), findings

    duplicated = inventory.listing(_tree(pytester, _tree_source(rows, duplicated=frozenset({"E-05"}))))
    findings = bindings.coverage_findings(rows, EXPECTED_IDS, duplicated)
    assert any("E-05" in finding and "exactly one conformance test" in finding for finding in findings), findings

    dropped = inventory.listing(_tree(pytester, _tree_source(rows, dropped=frozenset({"E-05"}))))
    findings = bindings.coverage_findings(rows, EXPECTED_IDS, dropped)
    assert any("E-05" in finding and "found 0" in finding for finding in findings), findings


@pytest.mark.conformance(
    unit="binding-ledger", part="test_coverage_reports_a_test_on_a_non_test_row", system="publication-math-conformance"
)
def test_coverage_reports_a_test_on_a_non_test_row(pytester) -> None:
    """Property: check 3 reports a non-`TEST` row that a conformance entry names, and its clean world
    reports nothing.

    Class: NEW CAPABILITY — the plausible wrong implementation only walks `TEST` rows outward, so an
    entry naming a `CROSS-REPO` or `UNREACHABLE` equation is never examined; that entry is a test
    the family will run while the ledger says no test is owed. Why: the direction that matters here
    is the tree's, not the ledger's — a stray test is written by a later author who believed the row
    said something else. Reachability: the clean arm holds the same `UNREACHABLE` row and no entry;
    the defect arm appends one marked function. Invocation isolated: the tree is under
    `pytester.path`. Kills: planting a conformance test for `E-06`. Second correct implementation:
    building the unit index first and consulting it per row, as here, or per entry as check 4 does.
    """
    rows = _rows()
    rows["E-06"] = {"binding": "UNREACHABLE", "ground": "synthetic: unreachable past a stop"}
    clean = inventory.listing(_tree(pytester, _tree_source(rows)))
    assert bindings.coverage_findings(rows, EXPECTED_IDS, clean) == []

    planted = inventory.listing(
        _tree(pytester, _tree_source(rows, extra=_marked_function("E-06", "test_eq06_synthetic")))
    )
    findings = bindings.coverage_findings(rows, EXPECTED_IDS, planted)
    assert any("E-06" in finding and "carries no test" in finding for finding in findings), findings


@pytest.mark.conformance(
    unit="binding-ledger",
    part="test_coverage_reports_an_equation_like_entry_no_row_carries",
    system="publication-math-conformance",
)
def test_coverage_reports_an_equation_like_entry_no_row_carries(pytester) -> None:
    """Property: check 4 reports an equation-like `unit` that no ledger row carries, and one whose row
    is not `TEST`; the clean tree reports nothing.

    Class: NEW CAPABILITY — the plausible wrong implementation enumerates the register's ids as the
    population of names a marker may carry, so a typo (`E4`, `e-04`) or an invented id is not
    examined at all; the property is over the grammar instead. Why: the marker is written by hand in
    the test file and the ledger by hand in the TOML, so a typo on either side is the ordinary
    defect, and an id list would require an edit per new component. Reachability: the clean arm is
    the same ledger and tree; each defect arm adds one entry. Invocation isolated: the tree
    is under `pytester.path`. Kills: a marker carrying `E-99`; a lower-case `e-06`, which the grammar
    admits and no row carries; and a marker carrying `E-06` whose row is `UNREACHABLE`. Second correct
    implementation: a regex elsewhere and a membership test, which is what this arm pins the
    behaviour of rather than the regex's spelling.
    """
    rows = _rows()
    rows["E-06"] = {"binding": "UNREACHABLE", "ground": "synthetic: unreachable past a stop"}
    clean = inventory.listing(_tree(pytester, _tree_source(rows)))
    assert bindings.coverage_findings(rows, EXPECTED_IDS, clean) == []

    stray = inventory.listing(_tree(pytester, _tree_source(rows, extra=_marked_function("E-99", "test_e99_synthetic"))))
    findings = bindings.coverage_findings(rows, EXPECTED_IDS, stray)
    assert any("E-99" in finding and "no ledger row carries" in finding for finding in findings), findings

    # A case variant is a typo and not a row lookup: the grammar admits it so the check names it as
    # carrying no row, which is the discrimination a list of the register's ids could not make.
    typo = inventory.listing(_tree(pytester, _tree_source(rows, extra=_marked_function("e-06", "test_e06_lower"))))
    findings = bindings.coverage_findings(rows, EXPECTED_IDS, typo)
    assert any("'e-06'" in finding and "no ledger row carries" in finding for finding in findings), findings

    non_test = inventory.listing(_tree(pytester, _tree_source(rows, extra=_marked_function("E-06", "test_e06_lower"))))
    findings = bindings.coverage_findings(rows, EXPECTED_IDS, non_test)
    assert any("'E-06'" in finding and "whose binding is UNREACHABLE" in finding for finding in findings), findings


@pytest.mark.conformance(
    unit="binding-ledger",
    part="test_coverage_reports_an_unreadable_marker_and_refuses_a_bare_list",
    system="publication-math-conformance",
)
def test_coverage_reports_an_unreadable_marker_and_refuses_a_bare_list(pytester) -> None:
    """Property: check 5 carries `listing`'s unreadable-marker findings through, and a bare entry list
    — which carries none — is refused instead of read as a clean population.

    Class: NEW CAPABILITY — the plausible wrong implementation iterates the entry list and ignores
    the findings half, so a decorator the static read cannot parse is dropped from both the listing
    and the report and the equation it names looks untested while no finding says why. Why: the
    unreadable marker is the one case where the inventory knows a test exists and cannot name it, and
    the check's population is exactly those findings. Reachability: the clean arm is the same ledger
    and a literal marker; the defect arm adds a marker whose `unit` is a name rather than a literal,
    which `listing` refuses. Invocation isolated: the tree is under `pytester.path`. Kills: passing
    `listing(root)[0]` alone, and dropping the findings half. Second correct implementation: a
    dedicated `unreadable` parameter, which would move the refusal to the call site.
    """
    rows = _rows()
    clean = inventory.listing(_tree(pytester, _tree_source(rows)))
    assert bindings.coverage_findings(rows, EXPECTED_IDS, clean) == []

    unreadable_marker = (
        '\n@pytest.mark.conformance(unit=MARKER_UNIT, part="p", system="publication-math-conformance")\n'
        "def test_unreadable_marker():\n    pass\n"
    )
    listing = inventory.listing(_tree(pytester, _tree_source(rows, extra=unreadable_marker)))
    assert listing[1], listing
    findings = bindings.coverage_findings(rows, EXPECTED_IDS, listing)
    assert findings, "the unreadable marker produced no finding"
    assert any(finding in findings for finding in listing[1]), findings

    with pytest.raises(bindings.LedgerValidationError):
        bindings.coverage_findings(rows, EXPECTED_IDS, listing[0])
