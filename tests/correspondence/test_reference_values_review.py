"""Reviewer's tests of the reference-value store: the summary on a store whose three counts differ, and the key-resolution arm of NOT MEASURED.

The summary counts stored values whose environment is `UNCHARACTERIZED`. On a store where every stored
value is uncharacterized, that count equals the stored-value count, so a summary that counted every
stored value as uncharacterized prints the same line. The first test therefore stores one value with a
characterised environment beside one without, which is the only arrangement in which the two counts
separate; a characterised value is also stale here, because the run's own environment is uncharacterised.

`compare_stored` reports `NOT MEASURED` when the corpus record, the key resolution, or this unit's stored
texts have a finding. The second test gives it a key-resolution finding alone, with the corpus record and
the stored texts intact, so that only that arm can produce the report.

Each test builds a small git repository in `tmp_path`: a one-section corpus record, a retained file whose
unit is one cell, a ledger, the generated stored extractions with their licence, lift declarations and
their generated fixtures. Files are staged and never committed.
"""

import hashlib
import subprocess
from pathlib import Path

import pytest

from tests.correspondence import (
    corpus_record,
    extent,
    extraction,
    fixtures,
    generate_extractions,
    lift,
    reference_values,
)

pytestmark = pytest.mark.state_check

GIT_LOCATION_VARS = ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR", "GIT_PREFIX")
SOURCE = "#%% cell\nx = scale * 2\ny = x\n"
LEDGER = 'legacy_file = "mod.py"\n\n[[row]]\nkey = "mod.py:1-3"\ndisposition = "scope-in"\nreason = "r"\n'
DECLARATION = (
    "from tests.correspondence.lift import declares_lift\n\n\n"
    '@declares_lift(unit="mod.py:1-3"{part}, fixtures=["numbers"])\n'
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


def _record(source: str) -> str:
    """Return a complete corpus record for the one-file corpus, with every derived field computed from `source`."""
    digest = hashlib.sha256(source.encode()).hexdigest()
    manifest = hashlib.sha256(f"{digest}  mod.py\n".encode()).hexdigest()
    names = (
        "upstream",
        "upstream_branch",
        "revision",
        "upstream_path",
        "candidate_rule",
        "extent_rule",
        "verification_command",
        "verification_result",
    )
    fields = "".join(f'{name} = "x"\n' for name in names)
    return (
        'rule = "corpus-record:1"\n\n[[section]]\nname = "stormy"\ndirectory = "legacy"\n'
        f'{fields}verification_date = "2026-01-01"\nset_digest = "sha256-manifest:1:{manifest}"\n'
        "unparseable_candidate_files = 0\n\n[[section.file]]\n"
        f'path = "mod.py"\nrole = "candidate"\nupstream_sourced = false\nsha256 = "{digest}"\n'
        f'lines = {len(source.splitlines())}\ngroup = "retained"\nruns = "UNESTABLISHED"\nruns_ground = "x"\n'
    )


def _build(tmp_path: Path, monkeypatch, parts: dict[str, str]) -> Path:
    """Build the repository with one lift module per entry of `parts` (system to part), generate its inputs, return its root."""
    for var in GIT_LOCATION_VARS:
        monkeypatch.delenv(var, raising=False)
    root = tmp_path
    (root / corpus_record.RECORD_NAME).write_text(_record(SOURCE), encoding="utf-8")
    (root / "legacy").mkdir()
    (root / "legacy" / "mod.py").write_text(SOURCE, encoding="utf-8")
    (root / "tests" / "correspondence" / "ledger").mkdir(parents=True)
    (root / "tests" / "correspondence" / "ledger" / "mod.toml").write_text(LEDGER, encoding="utf-8")
    (root / extraction.STORED_DIR).mkdir(parents=True)
    (root / extraction.STORED_DIR / "LICENSE").write_text("MIT\n", encoding="utf-8")
    (root / lift.LIFTS).mkdir(parents=True)
    (root / fixtures.GENERATORS).mkdir(parents=True)
    for system, part in parts.items():
        text = DECLARATION.format(part=f', part="{part}"' if part else "")
        (root / lift.LIFTS / f"{system}.py").write_text(text, encoding="utf-8")
        (root / fixtures.GENERATORS / f"{system}.py").write_text(GENERATOR, encoding="utf-8")
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    subprocess.run(["git", "add", "--", "."], cwd=root, check=True)
    generate_extractions.generate(root)
    for system in parts:
        fixtures.generate(system, "numbers", root=root)
    return root


def test_the_summary_prints_1_for_each_count_on_a_store_holding_one_of_each_case(tmp_path, monkeypatch, capsys):
    """Property: one uncharacterised stored value, one stale stored value and one unstored lift give a summary of 1 for each of those three counts.

    Class: a store whose stale value carries a characterised environment (and is stale for that reason), so
    that the uncharacterised count and the stored count differ. Why it is required: on a store where every
    value is uncharacterised the two counts are equal, so the uncharacterised count is never shown to count
    only uncharacterised values. Killed by: counting every stored value as uncharacterised, which prints 2
    here. Second correct implementation: counting the descriptors whose key's environment differs from any
    characterised identity passes.
    """
    root = _build(tmp_path, monkeypatch, {"sys_a": "", "sys_b": "p", "sys_c": "q"})
    by_system = {d.system: d for d in lift.discover(root)}
    reference_values.store(by_system["sys_a"], 12)
    reference_values.store(by_system["sys_b"], 12, reference_values.environment_identity("c", "l"))
    figures = reference_values.summary(root)
    assert (figures["uncharacterized"], figures["stale"], figures["no_stored_value"]) == (1, 1, 1), figures
    assert reference_values.main(root) == 0
    line = capsys.readouterr().out
    assert "1 UNCHARACTERIZED, 1 stale, 1 declared lift(s) with no stored value" in line, line


def test_a_key_resolution_finding_alone_gives_not_measured_with_the_extent_sentence(tmp_path, monkeypatch):
    """Property: when only the key resolution reports a finding, the report is `NOT MEASURED` with the extent check's sentence.

    Class: the extent check returning a finding while the corpus record and this unit's stored texts are
    intact, beside a stored value that would otherwise agree. Why it is required: a stored value matched to
    its unit through a key that no longer resolves cannot be trusted, and a raised skip there would suppress
    the recomputed value's verdict. Killed by: omitting the key-resolution arm, which reports
    `CHARACTERIZED DIVERGENCE: agrees` here. Second correct implementation: calling `extent.check` after the
    unit's extraction findings instead of before passes, since those are empty here. The finding is supplied
    by replacing `extent.check`, which stands for a partition failure that no edit of the copy causes while
    the corpus record still passes.
    """
    root = _build(tmp_path, monkeypatch, {"sys_a": ""})
    (declaration,) = lift.discover(root)
    reference_values.store(declaration, 12)
    with pytest.warns(reference_values.ReferenceValueReport):
        control = reference_values.compare_stored(declaration, 12)
    assert control == "CHARACTERIZED DIVERGENCE: agrees"
    monkeypatch.setattr(extent, "check", lambda root=None: (["mod.py: 1 line(s) in no unit"], {}))
    with pytest.warns(reference_values.ReferenceValueReport):
        report = reference_values.compare_stored(declaration, 12)
    assert report == f"NOT MEASURED {extent.NOT_MEASURED}"
