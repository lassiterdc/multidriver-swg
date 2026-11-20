"""Check the ruling stop: the licence's three predicate functions, the table's checkpoint, the stub licence, and the engine run that stops after the licence and resumes from the merge.

The three pure functions are checked over hand-written values, because they read nothing: a licence
document, a selection and a target list are all they take. The table's two checkpoint invariants are
properties of the constant table rather than runtime code, so they are stated here and nowhere else.

The engine checks run the real engine over a committed copy of the toolkit whose stub licence requires an
entry. The constant lives in `stubs.py`, so the copy is edited and re-committed BEFORE the estate declares
its commit: the gate compares the declaration with the copy's commit, and a copy edited after the
declaration would be refused at parse. Production code reads no environment variable or file to change the
requirement, so a copy edit is the only way a test reaches the stopped branch.
"""

from __future__ import annotations

import json
import subprocess
import time
from dataclasses import replace
from pathlib import Path

import pytest

import multidriver_swg.workflow.pre_run as pre_run
import multidriver_swg.workflow.stubs as stubs
from multidriver_swg.run_configuration.main_configuration import MainConfiguration
from multidriver_swg.run_configuration.selection_artifact import FitIdentifier, SelectionArtifact
from multidriver_swg.workflow.end_of_run import END_OF_RUN_RECORD
from multidriver_swg.workflow.exercised_set import PROVENANCE_FILE, read_provenance
from multidriver_swg.workflow.graph import GRAPH, IDENTITY_FILE, downstream_of
from multidriver_swg.workflow.pre_run import prepare_run
from multidriver_swg.workflow.ruling_stop import (
    LICENCE_FILE,
    REQUIRED_ENTRIES_KEY,
    RulingStopError,
    missing_entries,
    required_entries,
    stop_targets,
)
from tests.workflow.conftest import Estate, ToolkitCopy, run_engine

CHECKPOINT = next(rule for rule in GRAPH if rule.checkpoint)

# The three rows before the merge and the attribution row: the directories a stopped run builds.
STOPPED_DIRECTORIES = {
    "observed_record_pre_selection",
    "per_source_event_selection",
    "observed_record_licence",
    "method_attribution",
}

# The constant's declaration line in `stubs.py`, replaced in the copy to make the stub licence require an entry.
CONSTANT_LINE = "LICENCE_REQUIRED_ENTRIES: tuple[str, ...] = ()"

# A selection document the artifact's own reader admits, holding the fit `compound.rainfall_depth`.
SELECTION = """rule = "modelled-selection:1"
selection = ["compound.rainfall_depth.gamma.identity", "compound.surge_height.lognormal.identity"]

[[dependence]]
event_class = "compound"
marginals = ["compound.rainfall_depth.gamma.identity", "compound.surge_height.lognormal.identity"]
structure = "d-vine"
copula_families = ["gaussian"]

[stamp.inputs]
source = "test"

[stamp.seeds]
swg = 1

[stamp.identities.swg]
system = "swg"
implementation = "vine"
code_version = "0"
"""


def _fit(event_class: str, statistic: str) -> FitIdentifier:
    """Return a fit identifier of `event_class` and `statistic`, with the family and chain a selection holds."""
    return FitIdentifier(event_class, statistic, "gamma", ("identity",))


class _SelectionStub:
    """The one attribute `missing_entries` reads from a selection: its fits."""

    def __init__(self, *fits: FitIdentifier) -> None:
        self.fits = fits


def _selection(*fits: FitIdentifier) -> SelectionArtifact:
    """Return an object carrying `fits` where `missing_entries` reads them."""
    return _SelectionStub(*fits)  # type: ignore[return-value]


def test_required_entries_reads_the_one_key_in_the_order_it_lists_them() -> None:
    """Property: a document whose only key is `required_entries` returns its entries as a tuple in the listed order.

    Class: an empty list, one entry, and two entries in a non-sorted order.
    Why it is required: the stop's predicate is this list, and an order or a membership the reader changed
    would change which entry the stop reports first.
    What kills it: sorting the entries, deduplicating them, or returning a list.
    Second correct implementation: any reader returning the same tuple.
    """
    assert required_entries({REQUIRED_ENTRIES_KEY: []}) == ()
    assert required_entries({REQUIRED_ENTRIES_KEY: ["compound.rainfall_depth"]}) == ("compound.rainfall_depth",)
    assert required_entries({REQUIRED_ENTRIES_KEY: ["b.x", "a.y"]}) == ("b.x", "a.y")


@pytest.mark.parametrize(
    ("document", "named"),
    [
        pytest.param({}, "required_entries", id="no-key"),
        pytest.param({"required_entries": [], "other": 1}, "other", id="extra-key"),
        pytest.param({"entries": []}, "entries", id="other-key"),
        pytest.param({"required_entries": "compound.rainfall_depth"}, "required_entries", id="not-a-list"),
        pytest.param({"required_entries": [1]}, "1", id="entry-not-text"),
        pytest.param({"required_entries": ["compound"]}, "compound", id="one-part"),
        pytest.param({"required_entries": ["a.b.c"]}, "a.b.c", id="three-parts"),
        pytest.param({"required_entries": [".statistic"]}, ".statistic", id="empty-class"),
        pytest.param({"required_entries": ["class."]}, "class.", id="empty-statistic"),
        pytest.param({"required_entries": ["fine.entry", " .x"]}, " .x", id="blank-class"),
    ],
)
def test_required_entries_refuses_a_document_outside_the_form_naming_the_key_or_entry(
    document: dict[str, object], named: str
) -> None:
    """Property: any other key set, a non-list, or an entry that is not two non-empty dot-separated parts raises `RulingStopError` naming the key or entry.

    Class: ten malformed documents — the key set, the value kind and the entry form each refused.
    Why it is required: a requirement the reader cannot parse would otherwise read as "nothing is required"
    or as an entry no selection can satisfy, and the stop would decide about the wrong thing; the message
    names the part so the licence's body can be repaired.
    What kills it: `.get` with a default, splitting an entry at the first dot only, or truncating the check.
    Second correct implementation: a validator run over the whole document first; the same ten refused.
    """
    with pytest.raises(RulingStopError) as caught:
        required_entries(document)

    assert named in str(caught.value)


def test_required_entries_refuses_a_document_that_is_not_a_mapping() -> None:
    """Property: a JSON list in place of the document raises `RulingStopError`, not an interpreter error.

    Class: the one non-mapping a licence file can hold that still parses as JSON.
    Why it is required: the workflow file reads the document with `json.loads`, which returns whatever
    the file holds, so the form's refusal must cover the kind of the whole value.
    What kills it: calling `set(document)` before the kind check.
    Second correct implementation: an isinstance check before any key read; the same refusal.
    """
    with pytest.raises(RulingStopError):
        required_entries([])  # type: ignore[arg-type]


def test_missing_entries_reports_the_required_entries_no_fit_satisfies() -> None:
    """Property: an entry is missing when no fit of the selection has its event class and statistic, whatever the family.

    Class: a selection with the fit, a selection with the statistic under another class, a selection with
    the class under another statistic, and the required entries in a fixed order.
    Why it is required: the entry is the pair the selection names a fit by; matching on the statistic
    alone, or on the family, would call an entry met that no stage could use.
    What kills it: matching the statistic only, the event class only, or the whole identifier.
    Second correct implementation: building the set of pairs once; the same entries are reported.
    """
    required = ("compound.rainfall_depth", "compound.surge_height")

    assert missing_entries(required, _selection(_fit("compound", "rainfall_depth"))) == ("compound.surge_height",)
    assert missing_entries(required, _selection(_fit("rain_only", "rainfall_depth"))) == required
    assert missing_entries(required, _selection(_fit("compound", "rainfall_intensity"))) == required
    assert (
        missing_entries(required, _selection(_fit("compound", "surge_height"), _fit("compound", "rainfall_depth")))
        == ()
    )
    assert missing_entries((), _selection()) == ()


def test_missing_entries_without_a_selection_reports_every_entry() -> None:
    """Property: with no selection every required entry is missing, and with nothing required nothing is.

    Class: `None` against two entries and against none.
    Why it is required: a run with no recorded selection has met no requirement, and one that read `None`
    as "nothing to check" would let a stage that needs an entry run without it.
    What kills it: returning an empty tuple for `None`.
    Second correct implementation: treating `None` as an empty selection; the same answers.
    """
    assert missing_entries(("a.b", "c.d"), None) == ("a.b", "c.d")
    assert missing_entries((), None) == ()


def test_stop_targets_on_the_full_list_drops_every_row_downstream_of_the_licence_and_keeps_the_attribution_and_record() -> (
    None
):
    """Property: over the full list, the stopped run keeps the rows not downstream of the checkpoint, the attribution row's output and the end-of-run record.

    Class: the full target list the pre-run step writes for a present selection.
    Why it is required: the stopped run's report must still carry the citation section and the run its
    closing record, and the merge row's output and every later stage's must be gone.
    What kills it: cutting at "the checkpoint's position in table order", which drops the last row; or
    keeping the merge row's output.
    Second correct implementation: filtering by the set from `downstream_of`; the same list.
    """
    full = [f"{rule.name}/{output}" for rule in GRAPH for output in rule.outputs] + [END_OF_RUN_RECORD]
    cut = {rule.name for rule in downstream_of(CHECKPOINT.name)}

    stopped = stop_targets(full)

    assert "method_attribution/citation_section.html" in stopped
    assert stopped[-1] == END_OF_RUN_RECORD
    assert "observed_record_assembly/observed_record.json" not in stopped
    assert {target.split("/", 1)[0] for target in stopped} == ({rule.name for rule in GRAPH} - cut | {"end_of_run"})
    assert list(stopped) == [target for target in full if target.split("/", 1)[0] not in cut]


def test_stop_targets_on_the_kept_absent_list_drops_the_merge_row_and_keeps_the_attribution_and_record() -> None:
    """Property: over the kept-absent list the cut leaves the three rows before the merge, the attribution row and the record.

    Class: the list the pre-run step writes when the selection is absent.
    Why it is required: both stopping branches must compute their list one way, and the kept-absent list
    is the one a first run with no selection starts from.
    What kills it: a cut that reads the list's own order rather than the table's edges.
    Second correct implementation: the same set difference; the same four rows and the record.
    """
    kept_absent = pre_run.compute_targets(selection_present=False, delegated=False)

    stopped = stop_targets(kept_absent)

    assert {target.split("/", 1)[0] for target in stopped} == STOPPED_DIRECTORIES | {"end_of_run"}
    assert "method_attribution/citation_section.html" in stopped
    assert END_OF_RUN_RECORD in stopped
    assert "observed_record_assembly/observed_record.json" not in stopped


def test_exactly_one_row_is_the_checkpoint_and_it_is_the_licence_row() -> None:
    """Property: exactly one row of the table sets `checkpoint`, and it is the licence row whose output is the licence file.

    Class: the whole constant table.
    Why it is required: the stop is defined against one checkpoint; two would stop at whichever the engine
    met first, and none would leave a run with no point at which it can end. The table is a module
    constant fixed at authoring time, so a test over it is total and no runtime code re-checks it.
    What kills it: setting `checkpoint` on a second row, or on none.
    Second correct implementation: counting with a comprehension; the same count and row.
    """
    checkpoints = [rule for rule in GRAPH if rule.checkpoint]

    assert [rule.name for rule in checkpoints] == ["observed_record_licence"]
    assert checkpoints[0].outputs == (LICENCE_FILE,)


def test_every_row_that_reads_the_selection_is_downstream_of_the_checkpoint() -> None:
    """Property: each `reads_selection` row is in `downstream_of(<the checkpoint row>)`, so the stop always precedes a stage that needs an entry.

    Class: the rows that read the selection, evaluated over the constant table.
    Why it is required: a stage that needs a selection entry but does not follow the licence could run
    before the stop decides, using an entry the stop was meant to withhold.
    What kills it: moving the checkpoint after a selection reader, or giving a new reader no edge to it.
    Second correct implementation: computing the readers' ancestors instead; the same containment.
    """
    downstream = {rule.name for rule in downstream_of(CHECKPOINT.name)}

    for rule in GRAPH:
        if rule.reads_selection:
            assert rule.name in downstream, f"{rule.name} reads the selection but is not downstream of the licence"


def test_the_stub_licence_writes_the_constant_as_the_required_entries_list(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Property: `write_licence_stub` writes `{"required_entries": [...]}` from the constant read at call time, and the shipped constant is empty.

    Class: the shipped constant, and the constant set to one entry.
    Why it is required: the shipped graph must never stop, and a test or probe that sets the constant must
    change what the checkpoint writes; a constant read at import would ignore the later setting.
    What kills it: reading the constant at import, writing a non-empty shipped default, or a second key.
    Second correct implementation: passing the entries as an argument; this check would change its call.
    """
    assert stubs.LICENCE_REQUIRED_ENTRIES == ()
    path = tmp_path / LICENCE_FILE

    stubs.write_licence_stub(path)
    assert json.loads(path.read_text(encoding="utf-8")) == {REQUIRED_ENTRIES_KEY: []}

    monkeypatch.setattr(stubs, "LICENCE_REQUIRED_ENTRIES", ("compound.rainfall_depth",))
    stubs.write_licence_stub(path)
    document = json.loads(path.read_text(encoding="utf-8"))
    assert document == {REQUIRED_ENTRIES_KEY: ["compound.rainfall_depth"]}
    assert required_entries(document) == ("compound.rainfall_depth",)


def _commit(repository: Path, message: str) -> str:
    """Commit every change in `repository` under a throwaway identity and return the new commit id."""
    arguments = ["git", "-c", "user.name=workflow-test", "-c", "user.email=workflow-test@example.invalid"]
    subprocess.run([*arguments, "add", "-A"], cwd=repository, check=True, capture_output=True, text=True)
    subprocess.run(
        [*arguments, "-c", "commit.gpgsign=false", "commit", "-q", "-m", message],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )
    completed = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repository, check=True, capture_output=True, text=True)
    return completed.stdout.strip()


def _requiring(toolkit: ToolkitCopy, estate: Estate, entries: tuple[str, ...]) -> ToolkitCopy:
    """Set the copy's stub licence to require `entries`, re-commit, and make the estate declare the new commit."""
    stubs_file = toolkit.package / "workflow" / "stubs.py"
    text = stubs_file.read_text(encoding="utf-8")
    assert CONSTANT_LINE in text
    stubs_file.write_text(
        text.replace(CONSTANT_LINE, f"LICENCE_REQUIRED_ENTRIES: tuple[str, ...] = {entries!r}"), encoding="utf-8"
    )
    commit = _commit(toolkit.root, "require an entry")
    (estate.project_directory / "toolkit_revision.toml").write_text(
        f'rule = "toolkit-revision:1"\ncommit = "{commit}"\n', encoding="utf-8"
    )
    return replace(toolkit, commit=commit)


def _bind_three_keys(estate: Estate, selection: Path) -> None:
    """Rewrite the estate's binding document so it binds the three keys the pre-run step resolves."""
    estate.bindings.write_text(
        'rule = "path-bindings:1"\n\n[paths]\n'
        f'"smoke.run.project_directory" = "{estate.project_directory}"\n'
        f'"smoke.run.run_output" = "{estate.run_output}"\n'
        f'"smoke.run.selection" = "{selection}"\n',
        encoding="utf-8",
    )


def _prepare(estate: Estate, toolkit: ToolkitCopy, monkeypatch: pytest.MonkeyPatch) -> None:
    """Run the pre-run step against `toolkit` as the command would, writing the run-start record."""
    monkeypatch.setattr(pre_run, "running_package_directory", lambda: toolkit.package)
    prepare_run(MainConfiguration.load(estate.configuration), estate.configuration)


def _built(estate: Estate) -> set[str]:
    """The rule directories holding an identity file: the rules whose job ran."""
    return {
        path.name
        for path in estate.run_output.iterdir()
        if path.is_dir() and not path.name.startswith(".") and (path / IDENTITY_FILE).is_file()
    }


def test_a_licence_requirement_the_selection_lacks_ends_the_run_after_the_licence_and_a_second_run_continues_from_the_merge(
    tmp_path: Path, toolkit_copy: ToolkitCopy, estate: Estate, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Property: a required entry with no selection ends the run after the licence with exit 0, its report and its record; adding the fit and running again builds the merge and every later rule without re-running rows 1 to 3.

    Class: a copy whose stub licence requires `compound.rainfall_depth`, an absent selection, then a
    selection holding the fit and a second command-style run on the same output.
    Why it is required: this is the run's whole behaviour — the stop, what survives it, and the resume
    that does not repeat the rules before the licence; no pure function shows that the engine plans the
    rows after a checkpoint only once it has run.
    What kills it: an `all` whose input is fixed at parse, a cut that drops the attribution row or the
    record, or a resume that re-runs the checkpoint.
    Second correct implementation: any rendering whose post-checkpoint list is computed from the licence
    and the selection; the same directories and the same invocation list.
    """
    toolkit = _requiring(toolkit_copy, estate, ("compound.rainfall_depth",))
    selection_path = estate.project_directory / "selection.toml"
    _bind_three_keys(estate, selection_path)
    _prepare(estate, toolkit, monkeypatch)
    report = estate.run_output / "report.html"

    first = run_engine(
        toolkit,
        estate,
        cache_root=tmp_path / "cache",
        extra_arguments=("--report", str(report), "--report-after-run"),
    )

    assert first.returncode == 0, first.stderr
    assert _built(estate) == STOPPED_DIRECTORIES
    assert not (estate.run_output / "observed_record_assembly").exists()
    assert (estate.run_output / END_OF_RUN_RECORD).is_dir()
    stopped_document = read_provenance(estate.run_output / END_OF_RUN_RECORD / PROVENANCE_FILE)
    assert stopped_document["exercised_systems"] == [
        "combined-event-identification",
        "method-attribution",
        "observed-record-assembly",
    ]
    assert "citation_section.html" in report.read_text(encoding="utf-8")

    selection_path.write_text(SELECTION, encoding="utf-8")
    time.sleep(1.1)
    _prepare(estate, toolkit, monkeypatch)
    second = run_engine(toolkit, estate, cache_root=tmp_path / "cache")

    assert second.returncode == 0, second.stderr
    assert _built(estate) == {rule.name for rule in GRAPH}
    document = read_provenance(estate.run_output / END_OF_RUN_RECORD / PROVENANCE_FILE)
    invoked = document["invocation_rules"]
    assert isinstance(invoked, list)
    assert {"observed_record_pre_selection", "per_source_event_selection", "observed_record_licence"}.isdisjoint(
        invoked
    )
    assert "observed_record_assembly" in invoked
    assert {rule.name for rule in downstream_of("observed_record_assembly")} <= set(invoked)


def test_the_shipped_empty_requirement_runs_the_full_graph(
    tmp_path: Path, toolkit_copy: ToolkitCopy, estate: Estate, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Property: with the shipped constant empty and a selection present, the run builds every row, the licence among them, and exits 0.

    Class: the unedited copy, a present selection, the full target list.
    Why it is required: the shipped graph never stops, so adding the checkpoint must leave today's full
    run intact; a post-checkpoint function that stopped on an empty requirement would shorten every run.
    What kills it: treating an empty required list as a missing entry, or a licence stub that writes
    a requirement by default.
    Second correct implementation: a copy whose licence never carries the key; this check would then change.
    """
    selection_path = estate.project_directory / "selection.toml"
    selection_path.write_text(SELECTION, encoding="utf-8")
    _bind_three_keys(estate, selection_path)
    _prepare(estate, toolkit_copy, monkeypatch)

    completed = run_engine(toolkit_copy, estate, cache_root=tmp_path / "cache")

    assert completed.returncode == 0, completed.stderr
    assert _built(estate) == {rule.name for rule in GRAPH}
    licence = json.loads((estate.run_output / "observed_record_licence" / LICENCE_FILE).read_text(encoding="utf-8"))
    assert licence == {REQUIRED_ENTRIES_KEY: []}
