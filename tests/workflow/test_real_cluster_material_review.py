"""Quick checks for the real-graph checkpoint arms' probe material, with no engine run.

The engine tests run the real engine; these check, quickly, the properties it would only show indirectly:
the order in which the launcher places the selection and the requirement relative to the pre-run step,
what the resume arm leaves alone, and where the probe sets the stub licence relative to its include.

Every check names the wrong implementation it is red under.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.workflow.conftest import REPO_ROOT
from tests.workflow.real_cluster import launch_probe
from tests.workflow.test_real_cluster_material import _estate

MATERIAL = REPO_ROOT / "tests" / "workflow" / "real_cluster"


@pytest.fixture
def recorded_launch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Replace the pre-run step and the engine with recorders; return the list of what the pre-run step saw."""
    seen: list[dict[str, object]] = []

    def prepare(configuration, configuration_path):
        selection = launch_probe._selection_path(configuration)
        seen.append(
            {
                "selection_exists": selection.exists(),
                "selection_text": selection.read_text(encoding="utf-8") if selection.exists() else None,
                "requirement_exists": (
                    launch_probe.run_output_directory(configuration) / "measurement" / "licence_required.json"
                ).exists(),
            }
        )

    monkeypatch.setattr(launch_probe, "prepare_run", prepare)
    monkeypatch.setattr(launch_probe.subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, 0))
    return seen


def test_the_resume_arm_places_the_selection_before_the_pre_run_step_and_leaves_the_requirement_alone(
    tmp_path: Path, recorded_launch: list[dict[str, object]]
) -> None:
    """Property: when the pre-run step runs under the resume arm the selection is already in place, and the requirement file the first launch wrote is byte-identical afterwards.

    Class: the resume arm over an output that carries an earlier requirement file.
    Why it is required: the pre-run step picks the target list and the run-start record from the file's
    existence; a late copy would record no selection, and a rewritten requirement would change the licence's
    inputs and rebuild it.
    What kills it: a copy placed after `prepare_run`, or a resume arm that calls the requirement writer.
    Second correct implementation: any launcher ordering that gives the same observations.
    """
    estate = _estate(tmp_path, SimpleNamespace(commit="0" * 40))
    source = tmp_path / "selection_present.toml"
    source.write_text("chosen\n", encoding="utf-8")
    measurement = estate.run_output / "measurement"
    measurement.mkdir()
    requirement = measurement / "licence_required.json"
    requirement.write_text('["kept.entry"]', encoding="utf-8")

    status = launch_probe.launch(
        estate.configuration, "graph-checkpoint-resume", selection_from=source, directory=MATERIAL
    )

    assert status == 0
    assert recorded_launch == [{"selection_exists": True, "selection_text": "chosen\n", "requirement_exists": True}]
    assert requirement.read_text(encoding="utf-8") == '["kept.entry"]'


def test_the_no_ruling_arm_finds_no_selection_at_the_pre_run_step_and_writes_the_requirement(
    tmp_path: Path, recorded_launch: list[dict[str, object]]
) -> None:
    """Property: the no-ruling arm launches with no selection file, and the requirement it writes is exactly the one entry the shape names.

    Class: the no-ruling arm over a fresh output.
    Why it is required: the run must stop after the licence, which needs an absent selection and a licence
    that requires a fit the selection lacks.
    What kills it: a requirement list with another entry or none, or a selection created by the arm.
    Second correct implementation: the requirement written before or after the pre-run step; only its content
    and the absence of the selection are checked.
    """
    estate = _estate(tmp_path, SimpleNamespace(commit="0" * 40))

    launch_probe.launch(estate.configuration, "graph-checkpoint-noruling", directory=MATERIAL)

    assert recorded_launch[0]["selection_exists"] is False
    written = json.loads((estate.run_output / "measurement" / "licence_required.json").read_text(encoding="utf-8"))
    assert written == ["compound.rainfall_depth"]


def test_the_probe_sets_the_licence_constant_from_a_file_that_may_be_absent_before_it_includes_the_graph() -> None:
    """Property: the probe assigns the stub licence's constant after the graph-start check, only when the requirement file exists, and before the toolkit's workflow file is included.

    Class: the probe's source text.
    Why it is required: the included file's rules read the constant when they run, and the setting must be
    made at every parse and be absent-safe so the run, dryrun and fail arms keep their stub licence.
    What kills it: an unconditional read (a missing file would raise in every older arm), an assignment after
    the include, or an assignment to a copy of the name instead of the module attribute.
    Second correct implementation: the same assignment inside a helper called before the include.
    """
    text = (MATERIAL / "probe.smk").read_text(encoding="utf-8")

    guard = text.index("if _LICENCE_REQUIRED.is_file():")
    assignment = text.index("stubs.LICENCE_REQUIRED_ENTRIES = ")
    include = text.index("workflow.include(")

    assert guard < assignment < include
    assert text.index("require_graph_start()\n_seconds") < guard


def test_the_readme_names_both_graph_arms_and_the_launcher_offers_them() -> None:
    """Property: the launcher's arm list and its README agree on the two graph arms and the resume arm's source option.

    Class: the launcher's arm tuple and the README text.
    Why it is required: the estate's measurement script invokes these names; a README that omits one sends
    an operator to an arm that is refused.
    What kills it: an arm missing from `ARMS`, or a README that names neither the arm nor `--selection-from`.
    Second correct implementation: a README that names them in a different layout.
    """
    readme = (MATERIAL / "README.md").read_text(encoding="utf-8")

    for arm in ("graph-checkpoint-noruling", "graph-checkpoint-resume"):
        assert arm in launch_probe.ARMS
        assert arm in readme
    assert "--selection-from" in readme
