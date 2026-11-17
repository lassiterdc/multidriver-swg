"""Check the stub graph end to end: the full run, a shortened target list, and the four graph-start refusals.

Every check here runs the engine from the committed copy `conftest.committed_copy` builds, with the cache
redirected under the test's tree. Running the real engine is the point: the workflow file's rendering
loop, the record's read, the gate's order and the stub body's identity are four mechanisms whose
interaction is what a run depends on, and a mocked engine would let a broken one pass.

Each refusal is reached by changing exactly one fact of the same clean copy or its estate, so the check
names the fact it is red without. Every run asserts that a refusal scheduled no job, because a gate that
refused after scheduling has already produced output the run cannot account for.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

from multidriver_swg.run_configuration.provenance import read_identity
from multidriver_swg.workflow.graph import GRAPH, IDENTITY_FILE
from multidriver_swg.workflow.run_start import RUN_START_FILE
from tests.workflow.conftest import Estate, ToolkitCopy, write_record

# Every artifact the graph declares, in table order: the full target list a conforming run builds.
ALL_TARGETS = tuple(f"{rule.name}/{output}" for rule in GRAPH for output in rule.outputs)

# The rules a run targeting only `generator_validation` must build, computed by hand from the table:
# its own reads and everything they transitively read. The four rows NOT listed are downstream of it or
# on a branch it does not need, and the check asserts they stayed unbuilt.
GENERATOR_VALIDATION_ANCESTORS = {
    "observed_record_assembly",
    "combined_event_identification",
    "marginal_distribution_fitting",
    "event_occurrence_modelling",
    "dependence_modelling",
    "timeseries_synthesis",
    "boundary_condition_output",
    "generator_validation",
}

UNBUILT_BY_THAT_RUN = {
    "event_frequency_assessment",
    "interactive_decision_support",
    "monte_carlo_design_storms",
    "method_attribution",
}


def _artifact_directories(estate: Estate) -> set[str]:
    """The rules whose directory holds an output in the run output, which is the set of jobs that ran."""
    return {
        path.name
        for path in estate.run_output.iterdir()
        if path.is_dir() and not path.name.startswith(".") and (path / IDENTITY_FILE).is_file()
    }


def test_the_full_graph_runs_and_every_identity_names_its_own_system(
    toolkit_copy: ToolkitCopy, estate: Estate, engine
) -> None:
    """Property: every declared target is built, and each rule's identity file names that rule's system, the `stub` implementation and the copy's commit.

    Class: the full target list — twelve declared outputs — over a clean committed copy.
    Why it is required: the identity's system is read from the rule's `params`, so a body that read the
    rendering loop's variable would write the LAST row's system into every file while every output still
    existed; and the code version is what makes the stub's output attributable to a checkout.
    What kills it: a body that reads the loop variable, binds no `system` param, or records an empty
    version.
    Second correct implementation: a body that reads the row from a rendered table keyed by rule name;
    the same twelve identities are written.
    """
    write_record(estate, ALL_TARGETS)

    completed = engine(toolkit_copy, estate)

    assert completed.returncode == 0, completed.stderr
    assert _artifact_directories(estate) == {rule.name for rule in GRAPH}
    for rule in GRAPH:
        assert (estate.run_output / rule.name / rule.outputs[0]).is_file(), rule.name
        identity = read_identity(estate.run_output / rule.name / IDENTITY_FILE)
        assert identity.system == rule.system, rule.name
        assert identity.implementation == "stub"
        assert identity.code_version == toolkit_copy.commit


def test_a_shortened_target_list_runs_those_rules_and_their_ancestors_only(
    toolkit_copy: ToolkitCopy, estate: Estate, engine
) -> None:
    """Property: a run targeting one downstream artifact builds exactly its ancestors and the target, and no other rule.

    Class: the target list holding only `generator_validation/validation_results.json`.
    Why it is required: the engine orders rules by the table's declared edges, so this measures that the
    rendered inputs are the producing rules' paths and that no rule was given an input the run does not
    build. A target list that is wrong in either direction either strands a rule or builds a branch the
    run did not ask for.
    What kills it: a rendered input naming an artifact by its bare name rather than its producing rule's
    path (the engine then finds no rule that produces it), or an `all` rule written by hand with a fixed
    list.
    Second correct implementation: any rendering that resolves each read through the table; the same eight
    directories result.
    """
    write_record(estate, ("generator_validation/validation_results.json",))

    completed = engine(toolkit_copy, estate)

    assert completed.returncode == 0, completed.stderr
    assert _artifact_directories(estate) == GENERATOR_VALIDATION_ANCESTORS
    assert _artifact_directories(estate) & UNBUILT_BY_THAT_RUN == set()


def test_a_missing_record_is_refused_before_any_job_runs(toolkit_copy: ToolkitCopy, estate: Estate, engine) -> None:
    """Property: with no record, the engine exits non-zero, names the record file, and builds nothing.

    Class: a run output directory holding no `run_start.json`.
    Why it is required: the record is how a parse learns what to build; the pre-run step writes it, and an
    engine invoked by hand without one must not build a graph from a default.
    What kills it: rendering the rules before reading the record, or defaulting an absent record.
    Second correct implementation: a gate raising the engine's error at any point before the first rule is
    scheduled; the same refusal arrives.
    """
    completed = engine(toolkit_copy, estate)

    assert completed.returncode != 0
    assert RUN_START_FILE in completed.stderr
    assert _artifact_directories(estate) == set()


@pytest.mark.parametrize(
    ("document", "expected"),
    [
        pytest.param({"format": "run-start:2"}, "format", id="wrong-version"),
        pytest.param({"unexpected": 1}, "unexpected", id="unnamed-key"),
        pytest.param({"targets": []}, "targets", id="empty-targets"),
    ],
)
def test_a_record_outside_the_form_is_refused_and_no_job_runs(
    toolkit_copy: ToolkitCopy, estate: Estate, engine, document: dict[str, object], expected: str
) -> None:
    """Property: a wrong version, an unnamed key and an empty target list each stop the parse, name the failing field, and build nothing.

    Class: three records that parse as JSON but not as the form.
    Why it is required: a record that is JSON but not the form is the likeliest hand-written defect, and
    its repair differs by field; a message naming the field is what makes the refusal actionable.
    What kills it: reading the record with `.get` defaults, or catching the refusal after rules are made.
    Second correct implementation: the same refusal from any validation point before scheduling.
    """
    record = json.loads((write_record(estate, ALL_TARGETS)).read_text(encoding="utf-8"))
    record.update(document)
    (estate.run_output / RUN_START_FILE).write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")

    completed = engine(toolkit_copy, estate)

    assert completed.returncode != 0
    assert f"'{expected}'" in completed.stderr
    assert _artifact_directories(estate) == set()


def test_a_commit_that_differs_from_the_declaration_is_refused(
    toolkit_copy: ToolkitCopy, estate: Estate, engine
) -> None:
    """Property: a declaration naming another commit stops the parse, names both commits, and builds nothing.

    Class: the estate's declaration replaced by forty zeroes.
    Why it is required: a run whose executed code is not the code its project named produces output no
    commit accounts for, and the comparison is the one fact that ties an artifact to a checkout.
    What kills it: comparing the declaration with itself, or skipping the comparison on a clean tree.
    Second correct implementation: any equality check of the two full ids; both values are in the message.
    """
    write_record(estate, ALL_TARGETS)
    (estate.project_directory / "toolkit_revision.toml").write_text(
        'rule = "toolkit-revision:1"\ncommit = "' + "0" * 40 + '"\n', encoding="utf-8"
    )

    completed = engine(toolkit_copy, estate)

    assert completed.returncode != 0
    assert "0" * 40 in completed.stderr
    assert toolkit_copy.commit in completed.stderr
    assert _artifact_directories(estate) == set()


def test_an_untracked_file_in_the_checkout_is_refused(toolkit_copy: ToolkitCopy, estate: Estate, engine) -> None:
    """Property: an untracked file the ignore rules do not cover stops the parse, names the file, and builds nothing.

    Class: one untracked module added to an otherwise clean copy.
    Why it is required: an untracked module can change what runs, so a run from a tree holding one is not
    the code any commit names.
    What kills it: reading `git diff` rather than the status, which misses an untracked file.
    Second correct implementation: the status read under an explicit untracked-files mode; the same file
    is refused.
    """
    write_record(estate, ALL_TARGETS)
    (toolkit_copy.root / "untracked_module.py").write_text("value = 1\n", encoding="utf-8")

    completed = engine(toolkit_copy, estate)

    assert completed.returncode != 0
    assert "untracked_module.py" in completed.stderr
    assert _artifact_directories(estate) == set()


def test_a_package_outside_any_checkout_is_refused(
    tmp_path: Path, toolkit_copy: ToolkitCopy, estate: Estate, engine
) -> None:
    """Property: a package directory with no enclosing repository stops the parse and says so.

    Class: the working package copied to a directory that is not inside any repository.
    Why it is required: an installed wheel has no commit to compare, and the refusal must say that rather
    than report a mismatch against an invented commit.
    What kills it: treating a failed `git rev-parse` as an empty checkout, or skipping the gate when git
    fails.
    Second correct implementation: an explicit repository-presence check; the same refusal arrives.
    """
    write_record(estate, ALL_TARGETS)
    installed = tmp_path / "installed" / "src" / "multidriver_swg"
    installed.parent.mkdir(parents=True)
    shutil.copytree(toolkit_copy.package, installed)

    completed = engine(
        ToolkitCopy(root=installed, package=installed, snakefile=installed / "workflow" / "Snakefile", commit=""),
        estate,
    )

    assert completed.returncode != 0
    assert "not the package of a git checkout" in completed.stderr
    assert _artifact_directories(estate) == set()


def test_a_package_inside_an_unrelated_repository_is_refused(
    tmp_path: Path, toolkit_copy: ToolkitCopy, estate: Estate, engine
) -> None:
    """Property: a package copy inside an unrelated repository stops the parse, naming both the copy and the checkout's source tree.

    Class: a directory named `multidriver_swg` under `vendor/` inside a second committed repository.
    Why it is required: `git rev-parse --show-toplevel` succeeds there, so without the source-root clause
    the gate would compare the enclosing repository's commit against a package it does not contain.
    What kills it: the toplevel lookup alone.
    Second correct implementation: comparing the imported package's file against the checkout's own
    source file; the same two paths are named.
    """
    write_record(estate, ALL_TARGETS)
    other = tmp_path / "other"
    vendor = other / "vendor" / "multidriver_swg"
    vendor.parent.mkdir(parents=True)
    shutil.copytree(toolkit_copy.package, vendor)
    (other / "placeholder.txt").write_text("", encoding="utf-8")
    subprocess.run(["git", "init", "-q"], cwd=other, check=True, capture_output=True, text=True)
    subprocess.run(["git", "add", "-A"], cwd=other, check=True, capture_output=True, text=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=workflow-test",
            "-c",
            "user.email=workflow-test@example.invalid",
            "commit",
            "-q",
            "-m",
            "other",
        ],
        cwd=other,
        check=True,
        capture_output=True,
        text=True,
    )

    completed = engine(
        ToolkitCopy(root=other, package=vendor, snakefile=vendor / "workflow" / "Snakefile", commit=""),
        estate,
    )

    assert completed.returncode != 0
    assert str(vendor.resolve()) in completed.stderr
    assert str((other / "src" / "multidriver_swg").resolve()) in completed.stderr
    assert _artifact_directories(estate) == set()


def test_the_package_ships_no_workflow_profile_beside_the_snakefile(toolkit_copy: ToolkitCopy) -> None:
    """Property: no `profiles` directory sits beside the shipped workflow file.

    Class: the workflow package directory of a committed copy.
    Why it is required: an engine started inside a job carries no profile flag and reads a workflow
    profile beside the Snakefile when one exists, so the profile channel is closed inside a job only by
    the folder's absence.
    What kills it: adding a `workflow/profiles/` directory, which the engine would then read.
    Second correct implementation: no folder; the assertion is its absence.
    """
    assert not (toolkit_copy.package / "workflow" / "profiles").exists()
