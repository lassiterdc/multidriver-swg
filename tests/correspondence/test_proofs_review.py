"""Reviewer test for the per-system proof record: the file name of each owning system.

It closes a gap the chunk's own tests leave: they spell a proof file's name by the module's own
`file_name` or from a one-hyphen slug, so a name check that mishandles a slug of two or more hyphens
passes them. It builds a synthetic root in `tmp_path`, so nothing is written to the tracked tree.
"""

import shutil

import pytest

from tests.correspondence import proofs, system_scope

pytestmark = pytest.mark.state_check

# The nine owning systems and their proof files, spelled out here and never by `proofs.file_name`.
OWNING_SYSTEM_FILES = [
    ("boundary-condition-output", "boundary_condition_output.toml"),
    ("combined-event-identification", "combined_event_identification.toml"),
    ("dependence-modelling", "dependence_modelling.toml"),
    ("event-occurrence-modelling", "event_occurrence_modelling.toml"),
    ("generator-validation", "generator_validation.toml"),
    ("interactive-decision-support", "interactive_decision_support.toml"),
    ("marginal-distribution-fitting", "marginal_distribution_fitting.toml"),
    ("observed-record-assembly", "observed_record_assembly.toml"),
    ("timeseries-synthesis", "timeseries_synthesis.toml"),
]
HEADER = 'rule = "proof-record:1"\n'


def _root(tmp_path, tests, files):
    """A synthetic root: one equivalence test per (unit, system) of `tests`, the real system-scope file, and the proof files `files` maps name -> rows."""
    (tmp_path / "tests" / "correspondence").mkdir(parents=True)
    shutil.copyfile(system_scope.REPO_ROOT / system_scope.SCOPE_PATH, tmp_path / system_scope.SCOPE_PATH)
    body = "import pytest\n"
    for n, (unit, system) in enumerate(tests):
        body += f"\n\n@pytest.mark.equivalence(unit='{unit}', system='{system}')\ndef test_{n}():\n    pass\n"
    (tmp_path / "tests" / "test_x.py").write_text(body)
    directory = tmp_path / proofs.RECORDS
    directory.mkdir(parents=True)
    for name, rows in files.items():
        (directory / name).write_text(HEADER + rows)
    return tmp_path


def _row(unit):
    return f'\n[[proof]]\nfamily = "equivalence"\nunit = "{unit}"\nroute = "authoring-red"\n'


@pytest.mark.parametrize(("system", "name"), OWNING_SYSTEM_FILES)
def test_the_proof_file_of_each_owning_system_is_read_under_its_underscored_slug_name(tmp_path, system, name):
    """Property: the proof file named `{slug with every hyphen as an underscore}.toml` of each of the nine owning systems is read clean, with its row returned.

    Class: every owning system of the partition, including the ones whose slug holds two or three
    hyphens; the satisfying input is the file spelled in this test, never by `proofs.file_name`.
    Reachability: each slug is a slug of the real system-scope file and holds the one test the row
    names, so the file name is the only thing under test. artifact: the nine system packages each
    write `tests/correspondence/proofs/{slug with underscores}.toml`, so a reader that refuses one of
    those names refuses the file its own system package must write. Why: a name check that turns
    only the first hyphen into an underscore passes a one-hyphen slug and refuses a slug of two or more,
    and the chunk's own tests spell the file by the module's function or use a one-hyphen slug.
    The assertion turns on the file name against the slug, which the current code and the changed
    code both hold. Killed by: replacing only the first hyphen of the slug in `file_name`. Second
    correct implementation: comparing each file's stem with the slug after `slug.replace('-', '_')`
    in place of building the expected file name.
    """
    assert system in system_scope.slugs(), system
    unit = "a.py:1-2"
    root = _root(tmp_path, [(unit, system)], {name: _row(unit)})
    rows, findings = proofs.load(root)
    assert findings == [], findings
    assert [row["unit"] for row in rows] == [unit], rows
