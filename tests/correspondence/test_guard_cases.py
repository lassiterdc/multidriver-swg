"""The guard cases: one test per system of the partition, each running the whole correspondence check.

Each system of the project's partition gets one test function, decorated
`@pytest.mark.guard(system="{slug}")` with the slug written as a literal, so that
`pytest -m 'guard(system="...")'` runs one system's case and a static scan of the decorators can
read every slug without importing this file. Deliberately NOT one parametrized function over the
system-scope file's slugs: a `system=` value carried through a parametrize case is unreadable to that scan.
`test_system_scope.py` checks that these literals equal the system-scope file's slugs, in both
directions, so a change of the partition that is not made here fails by name.

Every case runs the same check today, so the per-system split adds no coverage. It exists so that
work on one system has a selection point of its own: that work runs its system's case by slug,
and does not have to select every other system's case to get the whole check.

Every case calls one helper, `run_guard_case`, which runs the extent check and every callable of
`ledger.WHOLE_LEDGER_CHECKS`, collects all of their findings, and only then fails, once, with a
message naming each failing check. Deliberately NOT one assertion per check: check 1 is red until
the ledger holds a row for every retained unit, so a per-check assertion would stop there and leave
every later check unevaluated in every guard case for as long as any unit has no row.

The `guard` marker is not in `pyproject.toml`'s `markers` list and strict markers are off, so
pytest emits one unknown-marker warning per guard case rather than an error. This module does not
carry the `state_check` marker as a whole: the State check job does not select the guard cases.
"""

import shutil
import subprocess
from pathlib import Path

import pytest

from tests.correspondence import corpus_record, extent, ledger, system_scope


def guard_findings(root: Path = extent.REPO_ROOT) -> dict[str, list[str]]:
    """Run the extent check and every whole-ledger check under `root`; return each failing check's findings by name."""
    results = {"extent": extent.check(root)[0]}
    for check in ledger.WHOLE_LEDGER_CHECKS:
        results[check.__name__] = check(root)
    return {name: found for name, found in results.items() if found}


def run_guard_case(root: Path = extent.REPO_ROOT) -> None:
    """Fail once, naming every failing check and its findings, after every check has run."""
    failing = guard_findings(root)
    summary = "; ".join(f"{name}: {len(found)} finding(s)" for name, found in failing.items())
    details = "\n".join(f"{name}: {finding}" for name, found in failing.items() for finding in found)
    assert not failing, f"failing checks: {summary}\n{details}"


@pytest.mark.state_check
def test_the_helper_names_every_failing_check(tmp_path):
    """Property: when two checks fail at once, the helper's one failure names both.

    Class: any ledger failing two or more whole-ledger checks. Why: a guard case that stopped at
    the first failing check would hide every later check's regression behind check 1, which is red
    while any retained unit has no ledger row. Killed by: asserting each check in turn. Second
    correct implementation: raising one `AssertionError` whose message is built from a list of
    (check, findings) pairs and still names each failing check, as a bare word, on its first line.
    This test carries `state_check` by its own decorator, so the State check job grades the helper
    although it selects no guard case.
    """
    root = corpus_record.REPO_ROOT
    listed = subprocess.run(
        ["git", "ls-files", "-z", "--", "_old_code_to_refactor"], cwd=root, capture_output=True, check=True
    ).stdout.decode("utf-8")
    paths = [p for p in listed.split("\0") if p]
    paths += [corpus_record.RECORD_NAME, str(system_scope.SCOPE_PATH)]
    paths += [str(p.relative_to(root)) for p in (root / ledger.LEDGER_DIR).glob("*.toml")]
    for rel in paths:
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(root / rel, tmp_path / rel)
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "-A"], cwd=tmp_path, check=True)
    d_file = tmp_path / ledger.LEDGER_DIR / "_d_build_water_level_generator.toml"
    run = '  { lines = "82-84", treatment = "discarded with the unit" },\n'
    text = d_file.read_text(encoding="utf-8")
    assert text.count(run) == 1
    d_file.write_text(text.replace(run, ""), encoding="utf-8")  # check 3 now fails; check 1 already does
    with pytest.raises(AssertionError) as caught:
        run_guard_case(tmp_path)
    first_line = str(caught.value).splitlines()[0]
    named = {word.strip(":;") for word in first_line.split()}
    assert {"check_1", "check_3"} <= named, first_line


@pytest.mark.guard(system="pre-refactor-correspondence")
def test_guard_pre_refactor_correspondence():
    """Guard case of `pre-refactor-correspondence`: the whole correspondence check, run for this system."""
    run_guard_case()


@pytest.mark.guard(system="publication-math-conformance")
def test_guard_publication_math_conformance():
    """Guard case of `publication-math-conformance`: the whole correspondence check, run for this system."""
    run_guard_case()


@pytest.mark.guard(system="observed-record-assembly")
def test_guard_observed_record_assembly():
    """Guard case of `observed-record-assembly`: the whole correspondence check, run for this system."""
    run_guard_case()


@pytest.mark.guard(system="combined-event-identification")
def test_guard_combined_event_identification():
    """Guard case of `combined-event-identification`: the whole correspondence check, run for this system."""
    run_guard_case()


@pytest.mark.guard(system="marginal-distribution-fitting")
def test_guard_marginal_distribution_fitting():
    """Guard case of `marginal-distribution-fitting`: the whole correspondence check, run for this system."""
    run_guard_case()


@pytest.mark.guard(system="dependence-modelling")
def test_guard_dependence_modelling():
    """Guard case of `dependence-modelling`: the whole correspondence check, run for this system."""
    run_guard_case()


@pytest.mark.guard(system="event-occurrence-modelling")
def test_guard_event_occurrence_modelling():
    """Guard case of `event-occurrence-modelling`: the whole correspondence check, run for this system."""
    run_guard_case()


@pytest.mark.guard(system="timeseries-synthesis")
def test_guard_timeseries_synthesis():
    """Guard case of `timeseries-synthesis`: the whole correspondence check, run for this system."""
    run_guard_case()


@pytest.mark.guard(system="boundary-condition-output")
def test_guard_boundary_condition_output():
    """Guard case of `boundary-condition-output`: the whole correspondence check, run for this system."""
    run_guard_case()


@pytest.mark.guard(system="generator-validation")
def test_guard_generator_validation():
    """Guard case of `generator-validation`: the whole correspondence check, run for this system."""
    run_guard_case()


@pytest.mark.guard(system="interactive-decision-support")
def test_guard_interactive_decision_support():
    """Guard case of `interactive-decision-support`: the whole correspondence check, run for this system."""
    run_guard_case()


@pytest.mark.guard(system="workflow-orchestration")
def test_guard_workflow_orchestration():
    """Guard case of `workflow-orchestration`: the whole correspondence check, run for this system."""
    run_guard_case()


@pytest.mark.guard(system="run-configuration-and-provenance")
def test_guard_run_configuration_and_provenance():
    """Guard case of `run-configuration-and-provenance`: the whole correspondence check, run for this system."""
    run_guard_case()


@pytest.mark.guard(system="packaging-and-release")
def test_guard_packaging_and_release():
    """Guard case of `packaging-and-release`: the whole correspondence check, run for this system."""
    run_guard_case()


@pytest.mark.guard(system="event-frequency-assessment")
def test_guard_event_frequency_assessment():
    """Guard case of `event-frequency-assessment`: the whole correspondence check, run for this system."""
    run_guard_case()


@pytest.mark.guard(system="monte-carlo-design-storms")
def test_guard_monte_carlo_design_storms():
    """Guard case of `monte-carlo-design-storms`: the whole correspondence check, run for this system."""
    run_guard_case()


@pytest.mark.guard(system="documentation-and-examples")
def test_guard_documentation_and_examples():
    """Guard case of `documentation-and-examples`: the whole correspondence check, run for this system."""
    run_guard_case()


@pytest.mark.guard(system="private-estate")
def test_guard_private_estate():
    """Guard case of `private-estate`: the whole correspondence check, run for this system."""
    run_guard_case()


@pytest.mark.guard(system="test-result-legibility")
def test_guard_test_result_legibility():
    """Guard case of `test-result-legibility`: the whole correspondence check, run for this system."""
    run_guard_case()


@pytest.mark.guard(system="method-attribution")
def test_guard_method_attribution():
    """Guard case of `method-attribution`: the whole correspondence check, run for this system."""
    run_guard_case()
