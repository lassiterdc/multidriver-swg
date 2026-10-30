"""Tests of the reference-value store: its key, every report of the comparison, the state check and the summary.

Each test builds a small git repository in `tmp_path`: a one-section corpus record, a retained file whose
unit is one cell, a ledger, the generated stored extractions with their licence, one lift declaration and
one generated fixture. Files are staged and never committed. The git location variables a hook exports are
removed first so a lookup reads the temporary repository. Every test is a NEW CAPABILITY: no store existed,
so a failure is shown by the wrong implementation a docstring names.
"""

import hashlib
import subprocess
import sys
import warnings
from pathlib import Path

import pytest

from tests.correspondence import (
    corpus_record,
    extraction,
    extraction_check,
    fixtures,
    generate_extractions,
    lift,
    reference_values,
)

pytestmark = pytest.mark.state_check

GIT_LOCATION_VARS = ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR", "GIT_PREFIX")


def _record(source: str) -> str:
    """Write a complete corpus record for the one-file corpus, with every derived field computed from `source`."""
    digest = hashlib.sha256(source.encode()).hexdigest()
    manifest = hashlib.sha256(f"{digest}  mod.py\n".encode()).hexdigest()
    fields = "".join(
        f'{name} = "x"\n'
        for name in (
            "upstream",
            "upstream_branch",
            "revision",
            "upstream_path",
            "candidate_rule",
            "extent_rule",
            "verification_command",
            "verification_result",
        )
    )
    return (
        'rule = "corpus-record:1"\n\n[[section]]\nname = "stormy"\ndirectory = "legacy"\n'
        f'{fields}verification_date = "2026-01-01"\nset_digest = "sha256-manifest:1:{manifest}"\n'
        "unparseable_candidate_files = 0\n\n[[section.file]]\n"
        f'path = "mod.py"\nrole = "candidate"\nupstream_sourced = false\nsha256 = "{digest}"\n'
        f'lines = {len(source.splitlines())}\ngroup = "retained"\nruns = "UNESTABLISHED"\nruns_ground = "x"\n'
    )


SOURCE = "#%% cell\nx = scale * 2\ny = x\n"
LEDGER = 'legacy_file = "mod.py"\n\n[[row]]\nkey = "mod.py:1-3"\ndisposition = "scope-in"\nreason = "r"\n'
DECLARATION = (
    "from tests.correspondence.lift import declares_lift\n\n\n"
    '@declares_lift(unit="mod.py:1-3", fixtures=["numbers"]{extra})\n'
    "def declare(fixtures):\n"
    '    """Supply the scale from the fixture."""\n'
    '    return {{"scale": int(fixtures["numbers"].sum())}}, lambda ns: ns["y"]\n'
)
GENERATOR = (
    "import numpy as np\n"
    "from tests.correspondence.fixtures import generates_fixture\n\n\n"
    '@generates_fixture(name="numbers", seed=1)\n'
    "def numbers(rng):\n"
    "    return np.array([1, 2, 3])\n"
)


def _build(tmp_path: Path, monkeypatch, extra: str = "") -> Path:
    """Build the temporary repository, generate its extractions and its fixture, and return its root."""
    for var in GIT_LOCATION_VARS:
        monkeypatch.delenv(var, raising=False)
    root = tmp_path
    (root / corpus_record.RECORD_NAME).write_text(_record(SOURCE), encoding="utf-8")
    (root / "legacy").mkdir()
    (root / "legacy" / "mod.py").write_text(SOURCE, encoding="utf-8")
    ledger = root / "tests" / "correspondence" / "ledger"
    ledger.mkdir(parents=True)
    (ledger / "mod.toml").write_text(LEDGER, encoding="utf-8")
    (root / extraction.STORED_DIR).mkdir(parents=True)
    (root / extraction.STORED_DIR / "LICENSE").write_text("MIT\n", encoding="utf-8")
    (root / lift.LIFTS).mkdir(parents=True)
    (root / lift.LIFTS / "sys_a.py").write_text(DECLARATION.format(extra=extra), encoding="utf-8")
    (root / fixtures.GENERATORS).mkdir(parents=True)
    (root / fixtures.GENERATORS / "sys_a.py").write_text(GENERATOR, encoding="utf-8")
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    subprocess.run(["git", "add", "-A"], cwd=root, check=True)
    generate_extractions.generate(root)
    fixtures.generate("sys_a", "numbers", root=root)
    return root


def _declaration(root: Path) -> lift.LiftDeclaration:
    (declaration,) = lift.discover(root)
    return declaration


def _report(declaration, recomputed):
    """Run `compare_stored` and return (report, the warnings it raised)."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        report = reference_values.compare_stored(declaration, recomputed)
    return report, [w for w in caught if issubclass(w.category, reference_values.ReferenceValueReport)]


def test_an_unstored_lift_reports_absent_and_the_report_is_a_warning(tmp_path, monkeypatch):
    """Property: with no stored value the report is `ABSENT`, raised as a `ReferenceValueReport` warning, never an error.

    Class: a declared lift whose store holds nothing. Why it is required: the store starts empty and the
    comparison must say so without failing. Killed by: returning None, or raising on a missing file. Second
    correct implementation: returning the report and warning through `warnings.warn` at the call site passes.
    """
    declaration = _declaration(_build(tmp_path, monkeypatch))
    report, warned = _report(declaration, 4)
    assert report == "ABSENT"
    assert len(warned) == 1 and "ABSENT" in str(warned[0].message)


def test_a_matching_stored_value_agrees_and_a_different_one_reports_moved_environment(tmp_path, monkeypatch):
    """Property: with every key part matching, the report is `CHARACTERIZED DIVERGENCE: agrees` for an equal value and `: moved environment` for another.

    Class: a stored value of 12 (the lift's value for the fixture's sum 6), compared with 12 and with 13.
    Why it is required: these are the only two comparison words, and neither is `pass` or `fail`. Killed by:
    reporting `pass`/`fail`, or comparing under a looser licence. Second correct implementation: any
    comparison through `compare.Exact().admits` passes.
    """
    declaration = _declaration(_build(tmp_path, monkeypatch))
    reference_values.store(declaration, 12)
    assert _report(declaration, 12)[0] == "CHARACTERIZED DIVERGENCE: agrees"
    assert _report(declaration, 13)[0] == "CHARACTERIZED DIVERGENCE: moved environment"


def test_the_comparison_never_writes_the_store(tmp_path, monkeypatch):
    """Property: `compare_stored` leaves every file of the store as it was, whatever it reports.

    Class: a comparison against a stale value and against an absent one. Why it is required: regenerating
    is a separate act. Killed by: a comparison that stores when absent or stale. Second correct
    implementation: none other; the property is the file listing and bytes.
    """
    root = _build(tmp_path, monkeypatch)
    declaration = _declaration(root)
    before = (
        {p: p.read_bytes() for p in (root / reference_values.STORE).rglob("*")}
        if (root / reference_values.STORE).exists()
        else {}
    )
    assert _report(declaration, 1)[0] == "ABSENT"
    assert not (root / reference_values.STORE).exists()
    reference_values.store(declaration, 12)
    (root / lift.LIFTS / "sys_a.py").write_text(
        (root / lift.LIFTS / "sys_a.py").read_text().replace("sum()", "sum() + 0"), encoding="utf-8"
    )
    snapshot = {p: p.read_bytes() for p in (root / reference_values.STORE).rglob("*") if p.is_file()}
    assert _report(_declaration(root), 1)[0].startswith("STALE")
    assert {p: p.read_bytes() for p in (root / reference_values.STORE).rglob("*") if p.is_file()} == snapshot
    assert before == {}


@pytest.mark.parametrize("field", ["lift", "fixtures", "cuts", "environment"])
def test_each_moved_key_part_reports_stale_naming_the_field_and_is_not_compared(tmp_path, monkeypatch, field):
    """Property: editing exactly one key part of the stored value reports `STALE {field}`, and the stored value is not compared.

    Class: the descriptor's lift digest, fixture list, cut digests or environment edited in turn, with a
    payload that would disagree with the recomputed value (so a comparison would say `moved environment`).
    Why it is required: a stale value must not decide or inform anything. Killed by: omitting a part from
    the key, or comparing before checking the key. Second correct implementation: comparing the whole key
    dict at once and then naming the first differing field passes.
    """
    root = _build(tmp_path, monkeypatch)
    declaration = _declaration(root)
    descriptor = reference_values.store(declaration, 12)
    text = descriptor.read_text(encoding="utf-8")
    if field == "environment":
        edited = text.replace('environment = "UNCHARACTERIZED"', 'environment = "container=a;lockfile=b"')
    elif field == "fixtures":
        edited = text.replace("numbers=", "numbers=0")
    elif field == "lift":
        edited = text.replace("lift:1:", "lift:1:0")
    else:
        edited = text.replace("cut-lines:1:", "cut-lines:1:0", 1)
    assert edited != text
    descriptor.write_text(edited, encoding="utf-8")
    assert _report(declaration, 99)[0] == f"STALE {field}"


def test_a_stored_payload_that_is_not_its_records_is_stale_payload(tmp_path, monkeypatch):
    """Property: an edited payload is reported `STALE payload`, never compared.

    Class: one byte of the `.npz` changed. Why it is required: a shared artifact whose digest is not its record's must be refused; the key records what it was built from that does
    not match its record. Killed by: not checking the payload digest. Second correct implementation:
    checking the digest after decoding the payload fails earlier and passes the same input.
    """
    root = _build(tmp_path, monkeypatch)
    declaration = _declaration(root)
    descriptor = reference_values.store(declaration, 12)
    payload = descriptor.with_suffix(".npz")
    data = bytearray(payload.read_bytes())
    data[-1] ^= 1
    payload.write_bytes(bytes(data))
    assert _report(declaration, 12)[0] == "STALE payload"


def test_an_unseeded_draw_is_excluded_even_when_a_stored_value_exists(tmp_path, monkeypatch):
    """Property: a declaration saying `draws="unseeded"` reports `EXCLUDED unseeded draw` and is not compared.

    Class: an unseeded lift with a stored value that matches. Why it is required: an unseeded draw has no
    repeatable value to compare. Killed by: comparing anyway. Second correct implementation: checking the
    draws field before any read passes.
    """
    root = _build(tmp_path, monkeypatch, extra=', draws="unseeded"')
    declaration = _declaration(root)
    reference_values.store(declaration, 12)
    assert _report(declaration, 12)[0] == "EXCLUDED unseeded draw"


def test_a_broken_corpus_record_gives_not_measured_and_does_not_raise_a_skip(tmp_path, monkeypatch):
    """Property: when the corpus record has a finding the report is `NOT MEASURED` with its sentence, and no `Skipped` is raised.

    Class: one edited corpus byte. Why it is required: a raised skip would suppress the recomputed value's
    verdict. Killed by: calling `require_record`. Second correct implementation: reading
    `corpus_record.record_findings` directly, as here, passes.
    """
    root = _build(tmp_path, monkeypatch)
    declaration = _declaration(root)
    reference_values.store(declaration, 12)
    (root / "legacy" / "mod.py").write_text(SOURCE + "# edited\n", encoding="utf-8")
    report, _ = _report(declaration, 12)
    assert report == f"NOT MEASURED {corpus_record.NOT_MEASURED}"


def test_an_edited_stored_text_of_this_unit_gives_not_measured_for_the_extraction_check(tmp_path, monkeypatch):
    """Property: an edited stored text of the unit's own range gives `NOT MEASURED` with the extraction check's sentence.

    Class: one byte of the unit's stored copy changed while the corpus is untouched. Why it is required:
    the cut digests in the key would otherwise be vouched for by a copy that no longer matches. Killed by:
    not consulting `extraction_findings` for the unit. Second correct implementation: calling it with no unit
    would also report here, but would report for any unit's finding, which the next test rules out.
    """
    root = _build(tmp_path, monkeypatch)
    declaration = _declaration(root)
    reference_values.store(declaration, 12)
    stored = root / extraction.stored_path("stormy", "mod.py", 1, 3)
    stored.write_bytes(stored.read_bytes() + b"# edited\n")
    assert _report(declaration, 12)[0] == f"NOT MEASURED {extraction_check.NOT_MEASURED}"


def test_another_units_extraction_finding_does_not_suppress_this_units_comparison(tmp_path, monkeypatch):
    """Property: a stray file under `extractions/` (a directory-wide finding, not this unit's) leaves this unit's report as the comparison's.

    Class: a stray `.txt` under the store directory. Why it is required: one other unit's finding must not
    suppress every unit's verdict, which is why `compare_stored` asks about its own unit. Killed by:
    `extraction_findings(root)` with no unit. Second correct implementation: a unit-restricted call, as here.
    """
    root = _build(tmp_path, monkeypatch)
    declaration = _declaration(root)
    reference_values.store(declaration, 12)
    (root / extraction.STORED_DIR / "stormy" / "stray.txt").write_text("x\n", encoding="utf-8")
    assert extraction_check.extraction_findings(root)
    assert _report(declaration, 12)[0] == "CHARACTERIZED DIVERGENCE: agrees"


def test_the_environment_identity_is_uncharacterized_unless_both_digests_are_given():
    """Property: `UNCHARACTERIZED` for none or one digest, and a joined identity for both.

    Class: the four combinations. Why it is required: a half-known environment is not a characterised one.
    Killed by: `or` in place of `and`. Second correct implementation: requiring both through `all()` passes.
    """
    identity = reference_values.environment_identity
    assert identity() == identity("c") == identity(None, "l") == reference_values.UNCHARACTERIZED
    assert (
        identity("c", "l") != reference_values.UNCHARACTERIZED
        and "c" in identity("c", "l")
        and "l" in identity("c", "l")
    )


def test_the_diagnostics_are_recorded_and_no_rule_reads_them(tmp_path, monkeypatch):
    """Property: the descriptor records the interpreter and library versions, and changing them changes no report.

    Class: a descriptor with its numpy version edited to another value. Why it is required: the versions are
    a diagnostic and never a gate. Killed by: putting a version in the key. Second correct implementation:
    recording the versions in a separate file passes the second half.
    """
    root = _build(tmp_path, monkeypatch)
    declaration = _declaration(root)
    descriptor = reference_values.store(declaration, 12)
    text = descriptor.read_text(encoding="utf-8")
    assert "[diagnostics]" in text and "python = " in text and "numpy = " in text
    lines = [('numpy = "0.0"' if line.startswith("numpy = ") else line) for line in text.splitlines()]
    descriptor.write_text("\n".join(lines) + "\n", encoding="utf-8")
    assert _report(declaration, 12)[0] == "CHARACTERIZED DIVERGENCE: agrees"


def test_the_state_check_names_a_stale_value_and_its_moved_field_and_a_good_one_passes(tmp_path, monkeypatch):
    """Property: `stored_value_findings` is empty for a fresh value and names the file and the moved field for a stale one.

    Class: the same value fresh and after its lift's logic is edited. Why it is required: a stale value
    fails the state check. Killed by: printing without returning a finding. Second correct
    implementation: raising from the check instead passes the second half.
    """
    root = _build(tmp_path, monkeypatch)
    declaration = _declaration(root)
    reference_values.store(declaration, 12)
    assert reference_values.stored_value_findings(root) == []
    source = root / lift.LIFTS / "sys_a.py"
    source.write_text(source.read_text().replace("sum()", "sum() + 1"), encoding="utf-8")
    (finding,) = reference_values.stored_value_findings(root)
    assert "STALE lift" in finding and finding.startswith("tests/correspondence/reference_values/sys_a/")


def test_the_state_check_names_a_payload_that_is_not_its_records(tmp_path, monkeypatch):
    """Property: an edited payload is a finding naming the payload file.

    Class: one flipped byte. Why it is required: a payload whose digest is not its record's must fail the
    check. Killed by: checking the descriptor only. Second correct implementation: re-encoding the decoded
    payload and comparing it passes for this input.
    """
    root = _build(tmp_path, monkeypatch)
    descriptor = reference_values.store(_declaration(root), 12)
    payload = descriptor.with_suffix(".npz")
    data = bytearray(payload.read_bytes())
    data[-1] ^= 1
    payload.write_bytes(bytes(data))
    (finding,) = reference_values.stored_value_findings(root)
    assert payload.name in finding


def test_the_summary_counts_two_uncharacterized_one_stale_and_one_unstored_lift(tmp_path, monkeypatch):
    """Property: on a store of two values, both with no environment identity and one of them stale, plus one declared lift with none, the summary counts 2 stored, 2 uncharacterized, 1 stale and 1 without a stored value.

    Class: three lifts in one temporary tree: a fresh stored value, a stored value edited to be stale, and
    a lift never stored; the stale value is also uncharacterized, which is why that count is 2. Why it is
    required: it shows a stale value is counted as stale and as uncharacterized, and an unstored lift only
    as unstored. Killed by: counting an unstored lift as stale, or leaving a stale value out of the
    stored count. Second correct implementation: computing each count from `stored_value_findings` and the
    declarations passes. Not established: that the count of uncharacterized values excludes a value stored
    with a characterised identity; the companion test of the one-of-each store carries that arm.
    """
    root = _build(tmp_path, monkeypatch)
    (root / lift.LIFTS / "sys_b.py").write_text(
        (root / lift.LIFTS / "sys_a.py").read_text().replace('fixtures=["numbers"]', 'part="p", fixtures=["numbers"]'),
        encoding="utf-8",
    )
    (root / lift.LIFTS / "sys_c.py").write_text(
        (root / lift.LIFTS / "sys_a.py").read_text().replace('fixtures=["numbers"]', 'part="q", fixtures=["numbers"]'),
        encoding="utf-8",
    )
    for system in ("sys_b", "sys_c"):
        (root / fixtures.GENERATORS / f"{system}.py").write_text(GENERATOR, encoding="utf-8")
        fixtures.generate(system, "numbers", root=root)
    by_system = {d.system: d for d in lift.discover(root)}
    reference_values.store(by_system["sys_a"], 12)
    stale_descriptor = reference_values.store(by_system["sys_b"], 12)
    stale_descriptor.write_text(stale_descriptor.read_text().replace("lift:1:", "lift:1:0"), encoding="utf-8")
    assert reference_values.summary(root) == {"stored": 2, "uncharacterized": 2, "stale": 1, "no_stored_value": 1}


def test_main_prints_one_line_naming_the_uncharacterized_count(tmp_path, monkeypatch, capsys):
    """Property: `main` prints exactly one line holding the four counts and the word `UNCHARACTERIZED`, and returns 0.

    Class: a store with one value. Why it is required: it is the line a person reads. Killed by: printing
    per-value lines. Second correct implementation: formatting from `summary` passes.
    """
    root = _build(tmp_path, monkeypatch)
    reference_values.store(_declaration(root), 12)
    assert reference_values.main(root) == 0
    out = capsys.readouterr().out
    assert out.count("\n") == 1 and "1 UNCHARACTERIZED" in out and out.startswith("reference values: 1 stored")


def _expected_counts(root: Path) -> dict[str, int]:
    """The summary a tree must show: every declared lift not declared unseeded is stored and uncharacterized, none is stale.

    A lift declared `unseeded` is never stored, so it is the one declared lift with no stored value. Derived from
    the declarations alone, never from the store, so that it can disagree with `summary`.
    """
    declared = lift.discover(root)
    stored = sum(1 for d in declared if d.draws != "unseeded")
    return {"stored": stored, "uncharacterized": stored, "stale": 0, "no_stored_value": len(declared) - stored}


def test_the_real_tree_stores_a_value_for_each_declared_lift_not_declared_unseeded_and_none_is_stale():
    """Property: on this checkout the state check has no finding and the summary equals `_expected_counts`.

    Class: the committed tree, whatever lifts it holds. Why it is required: the store grows with the lifts, so the
    earlier "zero uncharacterized" assertion went red on the first stored value; the relation holds at every state
    of the tree, zero included. Killed by: a store missing the value of one declared lift, a value stored for a lift
    declared unseeded, or any stale value. Second correct implementation: comparing the four counts one by one
    passes. Not established: that the relation distinguishes two errors that cancel, a missing value of one lift and
    a value stored for an unseeded one, in one tree; the synthetic tests below take each error alone.
    """
    extraction_check.require_extractions(reference_values.REPO_ROOT)
    assert reference_values.stored_value_findings(reference_values.REPO_ROOT) == []
    assert reference_values.summary(reference_values.REPO_ROOT) == _expected_counts(reference_values.REPO_ROOT)


def test_a_declared_lift_with_no_stored_value_breaks_the_relation_and_a_stored_one_restores_it(tmp_path, monkeypatch):
    """Property: `summary` differs from `_expected_counts` while a declared lift is unstored and equals it once stored.

    Class: the one-lift synthetic tree, before and after `store`. Why it is required: it shows the repaired real-tree
    assertion can fail, which an empty committed tree cannot show. Killed by: an expectation computed from the
    store's own descriptors, which is equal to `summary` in both states. Second correct implementation: counting
    `lift.discover(root)` and the descriptors on disk separately passes.
    """
    root = _build(tmp_path, monkeypatch)
    assert reference_values.summary(root) != _expected_counts(root)
    reference_values.store(_declaration(root), 12)
    assert reference_values.summary(root) == _expected_counts(root)


def test_a_lift_declared_unseeded_is_expected_unstored_and_a_value_stored_for_it_breaks_the_relation(
    tmp_path, monkeypatch
):
    """Property: with the one lift declared `unseeded`, an empty store matches `_expected_counts` and a stored value does not.

    Class: the same tree with `draws="unseeded"` declared, a position the lift above does not take. Why it is
    required: the expectation excludes unseeded lifts, and a rule that expected every declared lift stored would
    pass the test above and fail here. Killed by: counting every declared lift as expected stored. Second correct
    implementation: filtering on `draws != "unseeded"` as here, or on the descriptors' units, passes.
    """
    root = _build(tmp_path, monkeypatch, extra=', draws="unseeded"')
    assert (
        reference_values.summary(root)
        == _expected_counts(root)
        == {
            "stored": 0,
            "uncharacterized": 0,
            "stale": 0,
            "no_stored_value": 1,
        }
    )
    reference_values.store(_declaration(root), 12)
    assert reference_values.summary(root) != _expected_counts(root)


def test_the_command_line_prints_its_one_summary_line():
    """Property: `python -m tests.correspondence.reference_values` exits 0 and prints exactly the one line `_expected_counts` gives.

    Class: a child interpreter on this checkout. Why it is required: a person runs this command to read the count.
    The earlier assertion looked for the substring `0 UNCHARACTERIZED`, which is also true of a count ending in 0, so
    it passed at 10 stored values; the whole line is compared instead. Killed by: a module with no `__main__` block,
    or a line whose counts differ from the relation. Second correct implementation: formatting the line from
    `summary` passes.
    """
    run = subprocess.run(
        [sys.executable, "-m", "tests.correspondence.reference_values"],
        cwd=reference_values.REPO_ROOT,
        capture_output=True,
        text=True,
    )
    counts = _expected_counts(reference_values.REPO_ROOT)
    assert run.returncode == 0, run.stderr
    assert run.stdout == (
        f"reference values: {counts['stored']} stored, {counts['uncharacterized']} UNCHARACTERIZED, "
        f"{counts['stale']} stale, {counts['no_stored_value']} declared lift(s) with no stored value\n"
    )


def test_an_input_that_cannot_be_read_is_a_not_measured_report_and_not_an_exception(tmp_path, monkeypatch):
    """Property: when the key cannot be built (the lift's fixture is no longer recorded), the report is `NOT MEASURED` naming the cause, and nothing is raised.

    Class: a stored value whose fixture record was removed. Why it is required: `compare_stored` runs inside
    an equivalence test, and an error there would replace the recomputed value's verdict. Killed by: letting
    the fixture error escape. Second correct implementation: validating every input before reading passes.
    """
    root = _build(tmp_path, monkeypatch)
    declaration = _declaration(root)
    reference_values.store(declaration, 12)
    (root / fixtures.FIXTURES / "sys_a" / fixtures.RECORD).unlink()
    report, _ = _report(declaration, 12)
    assert report.startswith("NOT MEASURED ") and "numbers" in report
