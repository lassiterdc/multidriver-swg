"""Tests that the identifiers workflow records the release it was dispatched for, commits it once,
and fails the run when the deposit states a licence other than the release's.

`.github/workflows/record-identifiers.yml` exists because a release's identifiers are not available
while the release runs: the deposit is created by a webhook after the Release appears and the
archive's revision exists only after it has seen the tag. So the workflow is dispatched by hand,
takes one input — the tag — and does three things: it resolves the commit that tag names, runs
`scripts/record_identifiers.py` against it, and commits the two files the script wrote to `develop`.

Five rules follow, and each is a rule over the parsed file rather than over one line of it:

1. the only trigger is a dispatch that names the tag, and the tag is a required input;
2. the workflow may write, because the last step pushes a commit;
3. the recorder runs on the tag the dispatch names and the COMMIT the tag resolves to, not on the
   run's `github.sha`, which on a dispatched run is the branch head and not the released commit;
4. the commit step stages the two files the script writes and pushes `develop`;
5. every `steps.NAME.outputs.FIELD` the file reads is declared by a step with that id, and that
   step writes FIELD.

The fourth rule is the one the first release discovers the hard way, because a dispatched run
starts from a branch: `github.sha` names the branch's tip, the archive is asked about a revision
nobody released, and the step refuses `archive-locator` on a release that is perfectly public.

The file is read as text and not parsed as YAML, for the reason `tests/test_publish_workflow.py`
gives, and this module reuses that file's reader rather than establishing a second one: the State
check job installs the toolkit and `pytest` and no YAML library, so a module-level `import yaml`
would stop that job at collection, and two readers over two workflow files would drift. The
assertions are over the steps present by name, so the licence-comparison step a later chunk adds to
this workflow sits beside them without moving them.
"""

import re
from pathlib import Path

from tests.test_publish_workflow import _content, _entries, _step, _steps, _unquoted

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "record-identifiers.yml"
RECORDER = REPO_ROOT / "scripts" / "record_identifiers.py"

JOB = "record-identifiers"
INPUT = "tag"

# The five steps that carry the workflow's whole chain, in the order they must run: the record
# cannot be made before the tag's commit is known, the commit cannot be made before the record
# exists, and the deposit's licence is read last, from the record the DOI the commit wrote names.
CHECKOUT_STEP = "Check out develop with the tag"
RESOLVE_STEP = "Resolve the tagged commit"
RECORD_STEP = "Record the identifiers"
COMMIT_STEP = "Commit the identifiers to develop"
LICENCE_STEP = "Check the deposit states the release's licence"
STEP_ORDER = (CHECKOUT_STEP, RESOLVE_STEP, RECORD_STEP, COMMIT_STEP, LICENCE_STEP)

RECORD_SCRIPT = "scripts/record_identifiers.py"
RECORD_PATH = "releases/$TAG/release-enumeration.json"
CITATION_PATH = "CITATION.cff"
TARGET_BRANCH = "develop"
TABLE_PATH = "release_state.toml"
ZENODO_API = "https://zenodo.org/api"

# The step's own output reference, and the field it must write, so a reference to an output nobody
# writes is caught rather than read as an empty string at run time.
OUTPUT_REFERENCE = re.compile(r"steps\.([A-Za-z0-9_-]+)\.outputs\.([A-Za-z0-9_-]+)")


def _workflow() -> dict[str, object]:
    """`record-identifiers.yml`, read as text and returned as nested dicts and lists."""
    return _entries(_content(WORKFLOW.read_text(encoding="utf-8")))


def _job() -> dict[str, object]:
    """The one job this workflow runs, asserted to exist so a renamed job reds instead of keying on None."""
    jobs = _workflow()["jobs"]
    assert isinstance(jobs, dict), jobs
    assert JOB in jobs, sorted(jobs)
    job = jobs[JOB]
    assert isinstance(job, dict), job
    return job


def _names() -> list[str]:
    """Every named step of the job, in file order."""
    return [str(step.get("name", "")) for step in _steps(_job())]


def _run(name: str) -> str:
    """The `run:` value of the step named `name`, with its quotes removed."""
    return _unquoted(str(_step(_job(), name).get("run")))


def test_the_only_trigger_is_a_dispatch_that_names_the_tag():
    """Property: the workflow is dispatched by hand with exactly one input, the tag, and that input
    is required.

    Class: NEW CAPABILITY — plausible wrong implementations are a workflow that also runs on a tag
    push, which would race the release run that creates the Release; one that takes no input and
    records whatever release it guesses; and one whose input is optional, so a dispatch with no
    value reaches `gh` with an empty tag.

    Why: the identifiers belong to one release, and only a person knows which release the deposit
    and the archive have finished with. A trigger that fires by itself records identifiers for a
    release that may not exist yet, and an optional tag makes that failure silent.

    Killed by: adding any other trigger, dropping `workflow_dispatch`, adding another input, or
    making the tag optional. Second correct implementation: the input written with `type: string`
    or with no `type` at all, which this test does not read.
    """
    triggers = _workflow()["on"]
    assert isinstance(triggers, dict), triggers
    assert set(triggers) == {"workflow_dispatch"}, sorted(triggers)

    dispatch = triggers["workflow_dispatch"]
    assert isinstance(dispatch, dict), dispatch
    inputs = dispatch.get("inputs")
    assert isinstance(inputs, dict), inputs
    assert set(inputs) == {INPUT}, sorted(inputs)

    declaration = inputs[INPUT]
    assert isinstance(declaration, dict), declaration
    assert declaration.get("required") == "true", declaration


def test_the_workflow_may_write_and_the_commit_lands_on_develop():
    """Property: the workflow declares `contents: write`, and the commit it makes targets `develop`.

    Class: NEW CAPABILITY — plausible wrong implementations are a `contents: read` grant, which
    fails the push after the recorder has already written the files; and a push to the tag or to
    `main`, which either rewrites a published tree or bypasses the branch the record belongs on.

    Why: the files are written by the recorder into the checkout and reach the repository only
    through this workflow's own push, so the grant and the branch are the whole delivery of the
    record. A tag is immutable by intent and `develop` is where the run's other records land.

    Killed by: narrowing the grant, dropping it, or pushing another ref. Second correct
    implementation: the push written as `git push origin develop` after a local branch of that
    name, which names the same ref.
    """
    permissions = _workflow()["permissions"]
    assert permissions == {"contents": "write"}, permissions

    command = _run(COMMIT_STEP)
    assert f"git push origin HEAD:{TARGET_BRANCH}" in command, command


def test_the_recorder_runs_on_the_tag_and_the_commit_the_tag_names():
    """Property: the recorder runs with the dispatched tag, the commit that tag resolves to, and the
    repository's public origin — and that commit is resolved by a git command over the tag, never
    taken from the run's own `github.sha`.

    Class: NEW CAPABILITY — plausible wrong implementations are `--commit "$GITHUB_SHA"`, which on
    a dispatched run names the branch head, so the archive is asked about a revision nobody
    released and refuses `archive-locator`; a literal tag, which records the wrong release; and an
    origin the script cannot reduce to `owner/name`, which refuses before any request is made. The
    real submission's own history includes the first of these, which is why it is asserted here.

    Why: every identifier the step writes is looked up BY the commit and the tag, so a value about
    another commit writes a citation that names a revision the archive never archived.

    Killed by: rebinding any of the three arguments, dropping one, or passing the run's sha.
    Second correct implementation: the arguments in another order, or the tag read from an `env:`
    entry this test resolves through, which the assertions below name by their source.
    """
    assert RECORDER.is_file(), RECORDER
    recorder = _step(_job(), RECORD_STEP)
    command = _run(RECORD_STEP)
    assert f"python {RECORD_SCRIPT}" in command, command
    assert "--tag $TAG" in command, command
    assert "--commit $COMMIT" in command, command
    assert "--origin $ORIGIN" in command, command
    assert "$GITHUB_SHA" not in command, command

    environment = recorder.get("env")
    assert isinstance(environment, dict), environment
    assert environment.get("TAG") == "${{ inputs.tag }}", environment
    assert environment.get("COMMIT") == "${{ steps.tag.outputs.commit }}", environment
    assert environment.get("ORIGIN") == "${{ github.server_url }}/${{ github.repository }}", environment


def test_every_step_output_the_workflow_reads_is_declared_and_written_by_its_step():
    """Property: each `steps.NAME.outputs.FIELD` in the file names a step id the job declares, and
    that step's command writes FIELD.

    Class: NEW CAPABILITY — plausible wrong implementations are a reference to an id no step
    carries, and one to a field the step never writes. Neither is an error in GitHub Actions: both
    resolve to an empty string, so the recorder receives `--commit ""` and refuses after the release
    is public. `tests/test_publish_workflow_enumeration_review.py` records the same failure on the
    release side, which is why the rule is asserted here rather than left to the first release.

    Why: the only visible form of this mistake is a comparison of the references with the ids and
    the writes, because the runner substitutes silence for a wrong name.

    Killed by: any reference whose id or field is absent. Second correct implementation: the
    workflow reading no step output at all, in which case the arithmetic below asserts over an empty
    set and passes — a correct conclusion, since there is then nothing to resolve wrongly.
    """
    text = WORKFLOW.read_text(encoding="utf-8")
    referenced = set(OUTPUT_REFERENCE.findall(text))
    steps = _steps(_job())

    declared = {str(step.get("id")) for step in steps if "id" in step}
    for step_name, field in sorted(referenced):
        assert step_name in declared, (step_name, sorted(declared))

        writer = next(step for step in steps if str(step.get("id", "")) == step_name)
        assert f"{field}=" in str(writer.get("run", "")), (field, writer.get("name"))
