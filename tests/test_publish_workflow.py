"""Tests that the release workflow builds and publishes only behind its gate.

A `v*` tag runs seven jobs. One runs the whole test tree at the tagged commit, the next reads that
run's result record and refuses the tag unless the run was whole, complete and passing, and the
build, the distribution listing and both uploads hang off those two. So the gate's position in the
graph, the condition on it, and the record it reads are what keep a tag the suite does not support
from reaching an index. A gate moved after the build, a gate skipped when the suite job fails, or a
gate pointed at another artifact all keep this file's shape and publish anyway.

The last job, `create-release`, hangs off the last upload and is the release's own record: it
carries the enumeration the check job kept as an asset, states that asset's `sha256` and the three
licence statements in the notes, and is the one job with a write token. A Release created before the
uploads, an asset attached from another job's artifact, or a digest the notes do not state all keep
the workflow green and leave the release without the evidence the identifiers step reads.

The file is read as text and not parsed as YAML, the form `tests/test_whole_suite_workflow.py` and
`tests/correspondence/test_divergence_mounts.py` already take. The State check job installs only the
toolkit and `pytest`, so a YAML library would be a new dependency of the job these tests run in.

What is asserted here, and what is deliberately not: the trigger, the called workflow and that the
file it names exists, each job's `needs` and `if`, the run identity the gate publishes, the artifact
the gate downloads, the command that reads it, the Release job's grant, its asset and its notes, and
the steps the distribution check owes the release. Not the set of jobs: a later change may add a job
after the uploads, and that does not make this graph wrong. What it does hold of the distribution
check is the relative order of the steps the plan fixes by name: a checkout placed after the
download deletes the `dist/` the enumeration reads, an enumeration placed after the comparison
leaves the comparison with no record, a licence check placed after the keeper keeps the record of a
release whose statements the check refused, a scan placed after the comparison leaves the record of
a release that carries a secret compared and kept, and a step that keeps the enumeration before the
comparison keeps the record of a release that was refused. The order is asserted over the steps
PRESENT by name, so the steps a later chunk adds sit between these without moving them.
"""

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "publish.yml"

# The one workflow this file may call, and the file it names. The call must be same-repository and
# relative, because that is what runs the called workflow at the caller's commit, which is the
# tagged commit, and not at a branch head.
WHOLE_SUITE_CALL = "./.github/workflows/whole-suite.yml"
WHOLE_SUITE_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "whole-suite.yml"

# The graph: what each job waits for, and the condition it carries. `needs` is a SET because the
# order of the jobs a job waits for changes nothing about when it runs. A condition of None is a
# job with no `if`, which GitHub reads as "run only when every job it needs succeeded": that is
# what keeps the build, the check and both uploads off a tag the gate refused. The gate itself is
# the one conditioned job and runs on `!cancelled()`, so a failed suite reaches it and is refused
# with its reason printed instead of the gate being skipped.
GRAPH = {
    "whole-suite": (frozenset(), None),
    "release-gate": (frozenset({"whole-suite"}), "!cancelled()"),
    "build": (frozenset({"release-gate"}), None),
    "post-build-check": (frozenset({"release-gate", "build"}), None),
    "publish-to-testpypi": (frozenset({"post-build-check"}), None),
    "publish-to-pypi": (frozenset({"post-build-check", "publish-to-testpypi"}), None),
    "create-release": (frozenset({"publish-to-pypi"}), None),
}

GATE = "release-gate"
DISTRIBUTION_CHECK = "post-build-check"
DISTRIBUTION_STEP = "List the distributions"
RECORD_ARTIFACT = "whole-suite-result-record"
GATE_SCRIPT = "scripts/check_release_tag.py"
GATE_FLAGS = ("--tag", "--commit", "--main-ref", "--record")

# The distribution check's own steps, by the names the plan fixes. The order is the order the release
# reads them in: the checkout and the interpreter, then the download that writes `dist/`, then the
# listing, then the pinned `uv` the next two checks run, the artifact install and the lock
# comparison, then the sdist check that reads the archive the build wrote, then the enumeration of
# the released bytes and the acceptance read that holds that record to the run the gate read, then
# the scan of those bytes, the comparison against the release-state table, and the step that keeps
# the enumeration. A step an earlier or later chunk places is named here too where this file must
# hold its position, because the enumeration reads `dist/` and the archives the checkout supplies,
# the scan fills the record the comparison and the keeper read, and the acceptance read judges the
# record the enumeration has just written.
CHECKOUT_STEP = "Check out the tagged commit"
PYTHON_STEP = "Set up Python"
DOWNLOAD_STEP = "Download distribution artifacts"
DISTRIBUTION_STEP = "List the distributions"
UV_STEP = "Install uv"
ARTIFACT_INSTALL_STEP = "Install and import the built distributions"
LOCK_STEP = "Regenerate the lock and compare"
SDIST_LOCK_STEP = "Check the sdist lists the lock files"
ENUMERATION_STEP = "Enumerate the released bytes"
ACCEPTANCE_STEP = "Check the enumeration names the run this release read"
SCAN_STEP = "Scan the released bytes"
COMPARISON_STEP = "Compare against the release-state table"
LICENCE_STEP = "Check the licence statements"
KEEP_STEP = "Keep the enumeration"
CHECK_STEP_ORDER = (
    CHECKOUT_STEP,
    PYTHON_STEP,
    DOWNLOAD_STEP,
    DISTRIBUTION_STEP,
    UV_STEP,
    ARTIFACT_INSTALL_STEP,
    LOCK_STEP,
    SDIST_LOCK_STEP,
    ENUMERATION_STEP,
    ACCEPTANCE_STEP,
    SCAN_STEP,
    COMPARISON_STEP,
    LICENCE_STEP,
    KEEP_STEP,
)

# The pinned renderer the two lock steps run, the two scripts they call, and the pair of files the
# release ships and compares. `uv` decides the rendered bytes, so its version is an input to the
# comparison and not a detail of it: a runner whose uv resolved differently would refuse a committed
# lock that is correct. The scripts are named by path because a workflow naming a script the tree
# does not hold fails on the release run and nowhere before it.
UV_PIN = "uv==0.11.31"
UV_INSTALL_COMMAND = f'python -m pip install "{UV_PIN}"'
ARTIFACT_INSTALL_SCRIPT = "scripts/check_install.py"
ARTIFACT_INSTALL_COMMAND = f"python {ARTIFACT_INSTALL_SCRIPT} --dist dist"
LOCK_SCRIPT = "scripts/render_lock.py"
LOCK_COMMAND = f"python {LOCK_SCRIPT} --check"
LOCK_FILES = ("pylock.toml", "requirements.txt")
# Every spelling of the record the acceptance read opens, so a step written through another spelling
# of the runner's temporary directory still resolves to the file the enumeration wrote.
RECORD_PATH = "release-enumeration/release-enumeration.json"

# What the enumeration and the comparison run, and the artifact the enumeration is kept as. The two
# scripts are the ones the plan's earlier chunk writes; a workflow that names a script the tree does
# not hold fails on the release run and nowhere before it.
ENUMERATION_SCRIPT = "scripts/enumerate_release.py"
COMPARISON_SCRIPT = "scripts/compare_release_state.py"
SCAN_SCRIPT = "scripts/content_scan/release_corpus.py"
ENUMERATION_ARTIFACT = "release-enumeration"
RELEASE_STATE_TABLE = "release_state.toml"
ENUMERATION_PATH = "$RUNNER_TEMP/release-enumeration/release-enumeration.json"
ENUMERATION_FLAGS = (
    "--dist dist",
    "--repo .",
    "--commit $GITHUB_SHA",
    "--tag $GITHUB_REF_NAME",
    "--run-id ${{ needs.release-gate.outputs.whole_suite_run_id }}",
    "--run-attempt ${{ needs.release-gate.outputs.whole_suite_run_attempt }}",
    "--run-commit ${{ needs.release-gate.outputs.whole_suite_commit }}",
    f"--out {ENUMERATION_PATH}",
)

# The pre-release scan's own script and the three inputs it reads: the record it fills, the
# directory the download wrote, and the checkout `git archive` takes the tagged commit's source
# archive from. It is the same record the comparison and the licence check read, so the bytes
# judged for a secret are the bytes every later check judges.
SCAN_FLAGS = (
    f"--enumeration {ENUMERATION_PATH}",
    "--dist dist",
    "--repo .",
)

# The licence check's own script, the two documents it reads beside the enumeration and the table,
# and the flags it takes. It is the same record the comparison read, so the two checks judge one
# release; the documents are the tree's, because the README becomes the long description the index
# publishes and the citation's `license` is the field the deposit inherits.
LICENCE_SCRIPT = "scripts/check_licences.py"
README_PATH = "README.md"
CITATION_PATH = "CITATION.cff"
LICENCE_FLAGS = (
    f"--enumeration {ENUMERATION_PATH}",
    f"--table {RELEASE_STATE_TABLE}",
    "--dist dist",
    f"--readme {README_PATH}",
    f"--citation {CITATION_PATH}",
)

# The run identity the gate publishes for the steps that later read `needs.release-gate.outputs.*`.
# It is the whole-suite run's own identity, which is how a reader reaches that run's record: a gate
# that published a value it computed itself would point a later step at the wrong run.
GATE_OUTPUTS = {
    "whole_suite_run_id": "${{ github.run_id }}",
    "whole_suite_run_attempt": "${{ github.run_attempt }}",
    "whole_suite_commit": "${{ github.sha }}",
}

# The Release job and the three things it must get right. It is the one job with a `contents: write`
# grant, because creating a Release and attaching an asset are writes, and it waits for the last
# upload so a Release never announces bytes that did not reach the index. Its notes carry the
# digest of the asset it attaches, which is the value the identifiers step checks its download
# against, and the three licence statements, because the Release page is what a reader of the
# published archive lands on.
RELEASE = "create-release"
RELEASE_DOWNLOAD_STEP = "Download the release enumeration"
RELEASE_NOTES_STEP = "Write the release notes"
RELEASE_CREATE_STEP = "Create the Release for the tagged version"
RELEASE_PERMISSIONS = {"contents": "write"}
RELEASE_NOTES_PATH = "$RUNNER_TEMP/release-notes.md"

_KEY_LINE = re.compile(r"^([A-Za-z_][A-Za-z0-9_-]*):(?:\s+(.*))?$")
_BLOCK_MARKER = re.compile(r"^[|>][+-]?$")


def _strip_comment(text: str) -> str:
    """`text` up to a `#` that starts a comment: at the start, or after whitespace, outside quotes.

    Without this, a compliant line such as `tags: [ "v*" ]  # a release tag only` reads as a list
    whose last item carries the comment, and the equality assertions red on a correct file. A `#`
    inside a quoted scalar is not reached, which is the case this reader must get right for the
    quoted environment variables in the gate's command.
    """
    quote = ""
    for index, char in enumerate(text):
        if quote:
            if char == quote:
                quote = ""
        elif char in "\"'":
            quote = char
        elif char == "#" and (index == 0 or text[index - 1].isspace()):
            return text[:index].rstrip()
    return text.rstrip()


def _content(text: str) -> list[tuple[int, str]]:
    """Every non-blank, non-comment line of the file as (indent, text), a trailing comment removed.

    Comments and blank lines never belong to a block, so a comment written between two keys does
    not end the mapping it sits inside.
    """
    lines: list[tuple[int, str]] = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            lines.append((len(line) - len(line.lstrip()), _strip_comment(stripped)))
    return lines


def _scalar(text: str) -> str:
    """A scalar as written, with a trailing comment removed and one layer of matching quotes."""
    value = _strip_comment(text.strip())
    if len(value) > 1 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    return value


def _value(text: str) -> object:
    """An inline value: `[a, b]` becomes a list of scalars, and anything else a scalar."""
    value = _strip_comment(text.strip())
    if value.startswith("[") and value.endswith("]"):
        return [_scalar(item) for item in value[1:-1].split(",") if item.strip()]
    return _scalar(value)


def _nested(block: list[tuple[int, str]]) -> object:
    """The block below a key that carries no inline value: a mapping, or a list of items.

    A list item written as `key: value` lines is read as a mapping, so a step's `name`, `uses` and
    `run` are all reachable; an item written as one scalar stays a scalar. This is what makes a
    `needs:` written as a block sequence and a `steps:` written as a list of mappings read alike.
    """
    if not block:
        return None
    if not block[0][1].startswith("-"):
        return _entries(block)
    indent = block[0][0]
    items: list[object] = []
    index = 0
    while index < len(block):
        item_indent, text = block[index]
        if item_indent != indent or not text.startswith("-"):
            index += 1
            continue
        content = text[1:].strip()
        end = index + 1
        while end < len(block) and block[end][0] > indent:
            end += 1
        if _KEY_LINE.match(content):
            items.append(_entries([(indent + len(text) - len(content), content), *block[index + 1 : end]]))
        else:
            items.append(_scalar(content))
        index = end
    return items


def _entries(block: list[tuple[int, str]]) -> dict[str, object]:
    """The `key: value` entries at the block's own indent, each value inline, a block, or a body.

    A `run: |` body sits on the following lines and not in the value, so a reader that stopped at
    the marker would see no command at all and the gate's command assertions would pass over a
    step that runs nothing.
    """
    indent = block[0][0]
    mapping: dict[str, object] = {}
    index = 0
    while index < len(block):
        line_indent, text = block[index]
        if line_indent != indent:
            index += 1
            continue
        matched = _KEY_LINE.match(text)
        assert matched is not None, f"not a `key: value` line: {text!r}"
        key = matched.group(1)
        assert key not in mapping, f"`{key}` occurs twice in the same block of {WORKFLOW.name}"
        inline = matched.group(2)
        end = index + 1
        while end < len(block) and block[end][0] > indent:
            end += 1
        if inline is None:
            mapping[key] = _nested(block[index + 1 : end])
            index = end
            continue
        marker = _strip_comment(inline)
        if _BLOCK_MARKER.match(marker):
            joiner = "\n" if marker.startswith("|") else " "
            mapping[key] = joiner.join(body for _, body in block[index + 1 : end])
        else:
            mapping[key] = _value(inline)
        index = end
    return mapping


def _workflow() -> dict[str, object]:
    """`publish.yml`, read as text and returned as nested dicts and lists."""
    return _entries(_content(WORKFLOW.read_text(encoding="utf-8")))


def _job(name: str) -> dict[str, object]:
    """The job with id `name`, asserted to exist so a renamed job reds instead of keying on None."""
    jobs = _workflow()["jobs"]
    assert isinstance(jobs, dict), jobs
    assert name in jobs, sorted(jobs)
    job = jobs[name]
    assert isinstance(job, dict), job
    return job


def _steps(job: dict[str, object]) -> list[dict[str, object]]:
    """Every step of a job that is written as a mapping, in file order."""
    steps = job.get("steps")
    assert isinstance(steps, list), steps
    return [step for step in steps if isinstance(step, dict)]


def _needs(job: dict[str, object]) -> frozenset[str]:
    """A job's `needs` as a set: a flow list, a block list, or one job id, all read alike.

    GitHub accepts `needs: build` for a single job, so a reader that assumed a list would read the
    characters of a correct file and report a graph nobody wrote.
    """
    value = job.get("needs")
    if value is None:
        return frozenset()
    if isinstance(value, list):
        return frozenset(str(item) for item in value)
    return frozenset({str(value)})


def _condition(value: object) -> str | None:
    """A job's `if` with its `${{ }}` wrapper removed, so `!cancelled()` and `${{ !cancelled() }}` compare equal."""
    if value is None:
        return None
    return re.sub(r"^\$\{\{\s*(.*?)\s*\}\}$", r"\1", str(value)).strip()


def _command(job: dict[str, object]) -> str:
    """Every `run:` value of a job's steps, joined by newlines."""
    return "\n".join(str(step.get("run", "")) for step in _steps(job))


def _step(job: dict[str, object], name: str) -> dict[str, object]:
    """The one step of `job` named `name`, asserted unique so a renamed step reds instead of keying on nothing."""
    steps = _steps(job)
    named = [step for step in steps if str(step.get("name", "")) == name]
    assert len(named) == 1, (name, [step.get("name") for step in steps])
    return named[0]


def _uses(step: dict[str, object], prefix: str) -> bool:
    """Whether a step's `uses` names the action in `prefix`, the prefix carrying the version pin."""
    return str(step.get("uses", "")).startswith(prefix)


def _unquoted(text: str) -> str:
    """`text` with both quote characters removed, so a quoted and a bare argument compare equal."""
    return text.replace('"', "").replace("'", "")


def test_the_workflow_runs_on_a_release_tag_push_and_nothing_else():
    """Property: the only trigger is a push of a tag matching `v*`.

    Class: NEW CAPABILITY — plausible wrong implementation is the same file with a `branches`
    filter added under `push`, which starts a release run on a branch push, or with a
    `workflow_dispatch` trigger, which starts one with no tag for the gate to measure. The test is
    red under each.

    Why: every job below the gate measures the tag and the commit the release is published from,
    so a run that no release tag started has no release to publish and must not start one. One
    trigger, one tag filter, no other entry point.

    Killed by: adding any trigger, adding `branches` under `push`, or loosening the tag filter to
    `[*]` or `v[0-9]*`. Second correct implementation: the tag filter written as a block sequence
    (`tags:` and then `- "v*"`), and the keys in another order, which this reader normalises.
    """
    triggers = _workflow()["on"]
    assert isinstance(triggers, dict), triggers
    assert set(triggers) == {"push"}, sorted(triggers)
    push = triggers["push"]
    assert isinstance(push, dict), push
    assert push["tags"] == ["v*"], push["tags"]
    assert "branches" not in push, sorted(push)


def test_the_suite_job_calls_the_whole_suite_workflow_and_that_file_exists():
    """Property: the suite job calls the same-repository workflow file, and that file is present.

    Class: NEW CAPABILITY — plausible wrong implementation is a `uses:` naming a published
    action, or a path that resolves nowhere, either of which leaves the release run with no suite
    job at all while the gate still reads an artifact name. The test is red under each.

    Why: the called workflow has to run at the caller's commit for the record the gate reads to be
    the record of the tagged commit, and a relative same-repository call is the form that does
    that. A path that does not resolve cannot run at all, so the file the path names is asserted
    to exist in this tree.

    Killed by: changing the call to an absolute or published reference, renaming the called file,
    or adding `runs-on` or `steps` to a calling job, which a reusable-workflow call cannot carry.
    Second correct implementation: a comment line inside the job, or the `uses` key written after
    another key of the job.
    """
    job = _job("whole-suite")
    assert job.get("uses") == WHOLE_SUITE_CALL, job.get("uses")
    assert "steps" not in job and "runs-on" not in job, sorted(job)
    called = (REPO_ROOT / WHOLE_SUITE_CALL).resolve()
    assert called == WHOLE_SUITE_WORKFLOW.resolve(), called
    assert called.is_file(), called


@pytest.mark.parametrize("job_id", sorted(GRAPH))
def test_each_job_waits_for_the_jobs_that_must_finish_first(job_id):
    """Property: `job_id` waits for exactly the jobs the graph names, and nothing else.

    Class: NEW CAPABILITY — plausible wrong implementation is `build` waiting on nothing, or on
    the suite job alone, which builds and uploads a tag the gate refused; or
    `publish-to-pypi` waiting on `build`, which reaches the index without passing the check job.
    The test is red under each.

    Why: `needs` is the only thing that orders these jobs, so it is the whole of "nothing is
    built or uploaded before the gate passes, and nothing is uploaded before the distributions
    have been listed". Asserting the set per job, and not the order within it, holds each edge
    without holding the spelling.

    Killed by: dropping `release-gate` from `build`'s `needs`, dropping `build` from the check
    job's, or pointing either upload at `build` instead of the check job. Second correct
    implementation: `needs` written as a block sequence or as a single job id where the set has
    one member, and the keys in another order.
    """
    expected, _ = GRAPH[job_id]
    assert _needs(_job(job_id)) == expected, (job_id, _job(job_id).get("needs"))


@pytest.mark.parametrize("job_id", sorted(GRAPH))
def test_only_the_gate_is_conditioned_so_a_red_suite_is_refused_rather_than_skipped(job_id):
    """Property: `job_id` carries the condition the graph names, and no other condition.

    Class: NEW CAPABILITY — plausible wrong implementation is the gate carrying no `if`, so a
    failed suite job skips it and the run reads as a release that quietly did not happen, or the
    gate carrying `if: success()` with the same effect, or `build` carrying `always()`, which
    publishes past a refusal. The test is red under each.

    Why: the gate is the only job whose failure is meant to stop the release, and the only way it
    sees a failing suite is to run on `!cancelled()`. Every job after the gate relies on GitHub's
    default condition, which is that each job it needs succeeded, so it must carry no `if` at all
    — a condition there is either a way past the gate or a way to skip a job for no reason.

    Killed by: removing the gate's `if`, changing it to `success()` or `always()`, or adding any
    `if` to one of the other five jobs. Second correct implementation: the same condition written
    with different whitespace inside `${{ }}`, which `_condition` normalises.
    """
    _, expected = GRAPH[job_id]
    assert _condition(_job(job_id).get("if")) == expected, (job_id, _job(job_id).get("if"))


def test_the_gate_publishes_the_run_identity_the_steps_after_it_read():
    """Property: the gate publishes the whole-suite run's id, attempt and commit under the names a
    later step reads.

    Class: NEW CAPABILITY — plausible wrong implementation is the gate publishing a commit it
    resolved from `origin/main`, or omitting the attempt, so a step that later names the run under
    acceptance reads a run that is not the one this release was gated on. The test is red under
    each.

    Why: the record is reachable only through the run that wrote it, and the run is reachable only
    through this identity. A value the gate computed itself would be the same shape and the wrong
    run.

    Killed by: dropping any one of the three outputs, or rebinding one to a literal or to another
    context value. Second correct implementation: other outputs published beside these three,
    which this test does not read.
    """
    outputs = _job(GATE).get("outputs")
    assert isinstance(outputs, dict), outputs
    for name, value in GATE_OUTPUTS.items():
        assert outputs.get(name) == value, (name, outputs.get(name))


def test_the_gate_downloads_the_record_artifact_and_runs_the_refusal_script_on_it():
    """Property: the gate downloads the artifact the suite job keeps and runs the refusal script
    over the record inside it, with the tag, the commit and the main reference it names.

    Class: NEW CAPABILITY — plausible wrong implementation is the gate reading a record from the
    checkout, downloading the distribution artifact instead of the record, or naming another
    branch as the main reference, each of which leaves the gate passing over evidence it did not
    read. The test is red under each.

    Why: a gate that does not read the record cannot refuse a tag on it, and one that reads the
    wrong file refuses every tag or none. The four arguments are the gate's whole input, so a
    missing one is a check the gate cannot run.

    Killed by: dropping the download step, renaming the artifact it asks for, pointing `--record`
    at a path outside the download, or dropping any of the four flags. Second correct
    implementation: the command written as a `|` or `>` block scalar over several lines, and the
    record path written through either spelling of the runner's temporary directory.
    """
    steps = _steps(_job(GATE))
    downloads = [step for step in steps if str(step.get("uses", "")).startswith("actions/download-artifact@")]
    assert len(downloads) == 1, [step.get("uses") for step in steps]
    arguments = downloads[0].get("with")
    assert isinstance(arguments, dict), arguments
    assert arguments.get("name") == RECORD_ARTIFACT, arguments.get("name")

    command = _command(_job(GATE))
    assert GATE_SCRIPT in command, command
    for flag in GATE_FLAGS:
        assert flag in command, (flag, command)
    assert re.search(r"--record\s+[\"']?\S*result-record\.json", command), command


def test_the_distribution_check_holds_the_named_listing_step():
    """Property: the job between the build and both uploads holds a step named `List the distributions`.

    Class: NEW CAPABILITY — plausible wrong implementation is the check job deleted, which lets
    both uploads hang off the build and ship a distribution nothing listed, or the step renamed,
    which leaves a later row with no anchor to put its checks beside. The test is red under each.

    Why: the step is the place a later change lists and checks the built distributions, and a step
    nothing names cannot be found by a reader or extended by an edit that must not move.

    Killed by: removing the step, renaming it, or moving it out of this job. Second correct
    implementation: any other step beside it, which this test does not read.
    """
    names = [str(step["name"]) for step in _steps(_job(DISTRIBUTION_CHECK)) if "name" in step]
    assert DISTRIBUTION_STEP in names, names


def test_the_distribution_check_places_the_enumeration_steps_in_the_plan_order():
    """Property: the check job holds every step this change owes it, in the order the plan fixes,
    and no checkout follows the download that writes `dist/`.

    Class: NEW CAPABILITY — plausible wrong implementations are the checkout placed after the
    download, which empties the workspace and deletes `dist/` (the enumeration then refuses
    `archive-failed` on every release), the comparison placed before the enumeration, which reads
    a record that does not exist yet, and the enumeration kept before it is compared, which stores
    the record of a release that was refused. The test is red under each.

    Why: `post-build-check` is the only job that sees both the built artifacts and the tagged
    commit, and its steps communicate through two files in the workspace: `dist/`, written by the
    download, and the enumeration, written by the enumeration step and read by the comparison. The
    order is the whole of that handoff, so it is asserted by name over the steps present and not by
    counting steps, which would hold a later chunk's insertion against a correct file.

    Killed by: moving the checkout, the setup, the enumeration, the comparison or the keeper out of
    this order, or adding a second checkout after the download. Second correct implementation: any
    other step beside these, which this test does not name, and a comment line between them.
    """
    steps = _steps(_job(DISTRIBUTION_CHECK))
    names = [str(step.get("name", "")) for step in steps]
    present = [name for name in CHECK_STEP_ORDER if name in names]
    assert present == list(CHECK_STEP_ORDER), (present, names)
    positions = [names.index(name) for name in CHECK_STEP_ORDER]
    assert positions == sorted(positions), positions

    checkouts = [index for index, step in enumerate(steps) if _uses(step, "actions/checkout")]
    downloads = [index for index, step in enumerate(steps) if _uses(step, "actions/download-artifact")]
    assert len(downloads) == 1, [step.get("uses") for step in steps]
    assert checkouts and max(checkouts) < downloads[0], (checkouts, downloads)


def test_the_checkout_and_the_interpreter_before_the_download_carry_the_pinned_settings():
    """Property: the checkout of the tagged commit takes one commit and keeps no credential, and the
    interpreter is 3.11, the version the whole suite runs under.

    Class: NEW CAPABILITY — plausible wrong implementations are a checkout with the default depth,
    which is correct by accident and stops being correct at the first archive the enumeration reads
    from a shallow clone, a checkout with `persist-credentials` left on, which writes the run's token
    into the checkout the release ships, and a `3.x` interpreter, which is whatever GitHub serves
    that day. The test is red under each.

    Why: the enumeration archives `$GITHUB_SHA` from this checkout, so the checkout decides which
    commit the source-archive set describes; the credential decides whether the token can reach a
    file the build reads; and the interpreter is the one the suite's result record was produced
    under, so a record and an enumeration made under two interpreters are not one release.

    Killed by: dropping `fetch-depth`, dropping `persist-credentials: false`, or changing the
    version. Second correct implementation: the version written as a quoted or unquoted scalar, and
    the keys in another order, which this reader normalises.
    """
    job = _job(DISTRIBUTION_CHECK)
    checkout = _step(job, CHECKOUT_STEP)
    assert _uses(checkout, "actions/checkout@v6"), checkout.get("uses")
    checkout_arguments = checkout.get("with")
    assert isinstance(checkout_arguments, dict), checkout_arguments
    assert checkout_arguments.get("fetch-depth") == "1", checkout_arguments
    assert checkout_arguments.get("persist-credentials") == "false", checkout_arguments

    interpreter = _step(job, PYTHON_STEP)
    assert _uses(interpreter, "actions/setup-python@v6"), interpreter.get("uses")
    interpreter_arguments = interpreter.get("with")
    assert isinstance(interpreter_arguments, dict), interpreter_arguments
    assert interpreter_arguments.get("python-version") == "3.11", interpreter_arguments


def test_the_uv_install_is_pinned_to_the_renderer_the_lock_was_compared_under():
    """Property: the step that installs `uv` before the two checks calling it carries the one pinned
    command, and the pin is the version the declaration's own `test` group records.

    Class: NEW CAPABILITY — plausible wrong implementations are an unpinned `pip install uv`, which
    renders under whatever renderer the runner fetched that day, and a step placed after the lock
    comparison, which renders with a `uv` the job has not installed. The test is red under each.

    Why: `uv pip compile` decides the bytes of both lock files, so the renderer is an input to the
    comparison rather than a detail of it. A floating version makes `--check` a comparison against
    a resolver that may differ from the one that wrote the committed pair, and the refusal it
    produces then names the lock instead of the renderer.

    Killed by: dropping the step, dropping the pin, or changing the version. Second correct
    implementation: the same command with single quotes around the pin, which `_unquoted`
    normalises.

    Not established: that `uv==0.11.31` is the version the committed pair was rendered with is
    recorded by the lock's own rendering run; nothing here re-renders.
    """
    command = _unquoted(str(_step(_job(DISTRIBUTION_CHECK), UV_STEP).get("run")))
    assert command == f"python -m pip install {UV_PIN}", command

    declaration = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert f'"{UV_PIN}"' in declaration, (UV_PIN, "pyproject.toml")


def test_the_artifacts_are_installed_and_imported_before_the_lock_is_compared():
    """Property: the artifact-install step runs the install check over `dist/`, and the script it
    names is in the tree.

    Class: NEW CAPABILITY — plausible wrong implementations are the step pointed at the repository
    root instead of `dist/`, which refuses `wrong-artifacts` on every release, and the step placed
    after the lock comparison, which leaves a release whose declaration will not resolve refused
    for the lock that mirrors it. The test is red under each.

    Why: a wheel that builds and does not install is a release nobody can use, and no other step in
    this job installs one. The directory is the one the download wrote, so the artifact judged is
    the artifact the upload below ships.

    Killed by: dropping the step, changing `--dist`, naming another script, or deleting the script.
    Second correct implementation: the flags in another order, and the arguments quoted or bare,
    which `_unquoted` normalises.
    """
    command = _unquoted(str(_step(_job(DISTRIBUTION_CHECK), ARTIFACT_INSTALL_STEP).get("run")))
    assert command == ARTIFACT_INSTALL_COMMAND, command
    assert (REPO_ROOT / ARTIFACT_INSTALL_SCRIPT).is_file(), ARTIFACT_INSTALL_SCRIPT


def test_the_lock_step_compares_the_committed_pair_and_never_rewrites_it():
    """Property: the lock step runs the renderer in its comparing mode only, and the script it names
    is in the tree.

    Class: NEW CAPABILITY — plausible wrong implementations are the step running `--write`, which
    would regenerate the lock on the runner and publish a release nobody reviewed, and a step that
    names no mode, which exits 2 on a usage error and so reports as a refusal on every release.
    The test is red under each.

    Why: the value of the comparison is that the bytes a reader fetches at the tag are the bytes the
    run measured, and a `--write` here would replace them with what the runner resolved after the
    commit the tag names. The mode is therefore asserted rather than the script's presence alone.

    Killed by: changing the mode, dropping the flag, naming another script, or deleting the script.
    Second correct implementation: the flag written before the script's other arguments, where a
    later chunk adds any.
    """
    command = _unquoted(str(_step(_job(DISTRIBUTION_CHECK), LOCK_STEP).get("run")))
    assert command == LOCK_COMMAND, command
    assert "--write" not in command, command
    assert (REPO_ROOT / LOCK_SCRIPT).is_file(), LOCK_SCRIPT


def test_the_sdist_check_reads_the_top_level_lock_files_out_of_the_archive():
    """Property: the sdist step lists the archive the build wrote and requires one top-level member
    per lock file, with the check anchored so a nested copy cannot satisfy it.

    Class: NEW CAPABILITY — plausible wrong implementations are a check of the working tree rather
    than the archive, which passes while the manifest omits the files; a `grep -qx pylock.toml`,
    which no sdist member matches and so refuses every release; and a bare `grep -q`, which a
    differently named member shadows. The test is red under each.

    Why: the sdist is what a caller who builds from source downloads, and `MANIFEST.in` is the only
    thing that puts the lock pair in it. A manifest line and a build omission are one silent gap
    between them, and the archive is the only place the gap shows.

    Killed by: dropping either lock file, unquoting the archive name, or loosening `-x` or the
    anchored pattern. Second correct implementation: the two greps in either order, over one `tar`
    invocation or two.
    """
    run = str(_step(_job(DISTRIBUTION_CHECK), SDIST_LOCK_STEP).get("run"))
    assert 'SDIST="$(ls dist/*.tar.gz)"' in run, run
    assert run.count("tar -tzf") == len(LOCK_FILES), run
    for name in LOCK_FILES:
        assert f'grep -qx "[^/]*/{name}"' in run, (name, run)


def test_the_acceptance_read_holds_the_record_to_the_run_identity_the_gate_published():
    """Property: the acceptance-read step opens the enumeration and compares its `whole_suite_run`
    and commit against the gate's three outputs and `$GITHUB_SHA`, failing when they differ.

    Class: NEW CAPABILITY — plausible wrong implementations are a step that reads the record and
    prints it without comparing, one that compares only the run id, and one whose program exits 0
    on a mismatch, each of which keeps the job green over a record naming another run. The test is
    red under each.

    Why: the enumeration is the release's byte record and every later step reads it as evidence of
    this run. A record whose run identity came from a literal or from a stale context looks
    complete, so the comparison has to be over all three values and the step has to fail on a
    difference.

    Killed by: dropping any of the three outputs, dropping `$GITHUB_SHA`, comparing no field, or
    printing without exiting non-zero. Second correct implementation: the same comparison written
    over a dict the program builds from `sys.argv` in another order, which the assertions here do
    not read.
    """
    run = str(_step(_job(DISTRIBUTION_CHECK), ACCEPTANCE_STEP).get("run"))
    command = _unquoted(run)
    assert f"python - $RUNNER_TEMP/{RECORD_PATH}" in command, command
    for output in GATE_OUTPUTS:
        assert f"needs.release-gate.outputs.{output}" in command, (output, command)
    assert "$GITHUB_SHA" in command, command
    assert "whole_suite_run" in command, command
    for field in ("run_id", "run_attempt", "commit"):
        assert field in command, (field, command)
    assert "sys.exit(1 if refusals else 0)" in command, command


def test_the_enumeration_step_records_the_bytes_and_the_run_the_gate_read():
    """Property: the enumeration step runs the enumeration script over `dist/`, the checkout and the
    tag, with the whole-suite run's id, attempt and commit taken from the gate's outputs.

    Class: NEW CAPABILITY — plausible wrong implementations are a command that names a literal run
    id, which records a run nobody read, one that takes the commit from a value the step resolved
    itself rather than from the gate, and one that omits `--tag` or `--out`, which writes a record
    no later step can find. Each keeps the step's name and the job's shape. The test is red under
    each.

    Why: the record's whole value is that it names the run the gate measured, and the gate publishes
    that identity under three output names. A step that reads any other value writes a record that
    looks complete and points at the wrong run, which no later check can detect, so the binding of
    each flag is asserted rather than the presence of the script alone.

    Killed by: dropping any flag, pointing one at a literal or at another context value, or naming
    another script. Second correct implementation: the flags in another order, and the arguments
    quoted or bare, which `_unquoted` normalises.
    """
    command = _unquoted(str(_step(_job(DISTRIBUTION_CHECK), ENUMERATION_STEP).get("run")))
    assert ENUMERATION_SCRIPT in command, command
    for flag in ENUMERATION_FLAGS:
        assert flag in command, (flag, command)


def test_the_release_scan_reads_the_enumeration_the_artifacts_and_the_checkout():
    """Property: the scan step runs the pre-release scan on the record the enumeration wrote, the
    directory the download filled, and the checkout the source archive is taken from, and the
    script it names is present in the tree.

    Class: NEW CAPABILITY — plausible wrong implementations are a scan pointed at `dist/` alone,
    which judges the built artifacts and never the commit's source archive; one pointed at the
    gate's record instead of the enumeration, which reads a file of the wrong shape and exits 2 on
    every release; one that omits `--repo`, so `git archive` runs in whatever directory the step
    was left in; and a step named something else, which leaves the record's `scan` object at
    `not-run` while the run reads as scanned. Each keeps the step's name where the name is kept.
    The test is red under each.

    Why: those three inputs are the whole of what the scan ranges over, and the step sits before
    the comparison so a release carrying a secret is refused on the bytes and not on the table. A
    workflow naming a script the tree does not hold also fails on the release run and nowhere
    before it, which is the one run nobody can iterate on.

    Killed by: dropping or renaming a flag, pointing one at another path, naming another script, or
    deleting the script. Second correct implementation: the flags in another order, and the
    arguments quoted or bare, which `_unquoted` normalises.
    """
    command = _unquoted(str(_step(_job(DISTRIBUTION_CHECK), SCAN_STEP).get("run")))
    assert SCAN_SCRIPT in command, command
    for flag in SCAN_FLAGS:
        assert flag in command, (flag, command)

    assert (REPO_ROOT / SCAN_SCRIPT).is_file(), SCAN_SCRIPT


def test_the_comparison_step_reads_the_enumeration_and_the_release_state_table():
    """Property: the comparison step runs the comparison script on the record the enumeration wrote
    and the table the repository keeps.

    Class: NEW CAPABILITY — plausible wrong implementations are a comparison pointed at `dist/`, at
    the gate's record, or at a table path outside the checkout, each of which compares the release
    against evidence it did not write or a table nobody committed. The test is red under each.

    Why: the comparison's refusals are the release's only defence against shipping a file of a class
    the table declares excluded, so the two paths are its whole input. Both files are also asserted
    to exist in this tree: a workflow naming a script or a table the tree does not hold fails on the
    release run, which is the one run nobody can iterate on.

    Killed by: dropping either flag, pointing `--enumeration` at another path, renaming the table,
    or naming a script the tree does not hold. Second correct implementation: the flags in another
    order, and the arguments quoted or bare.
    """
    command = _unquoted(str(_step(_job(DISTRIBUTION_CHECK), COMPARISON_STEP).get("run")))
    assert COMPARISON_SCRIPT in command, command
    assert f"--enumeration {ENUMERATION_PATH}" in command, command
    assert f"--table {RELEASE_STATE_TABLE}" in command, command

    for script in (ENUMERATION_SCRIPT, COMPARISON_SCRIPT):
        assert (REPO_ROOT / script).is_file(), script
    assert (REPO_ROOT / RELEASE_STATE_TABLE).is_file(), RELEASE_STATE_TABLE


def test_the_licence_check_reads_the_enumeration_the_table_the_artifacts_and_the_two_documents():
    """Property: the licence step runs the licence script on the record the enumeration wrote, the
    table, the directory the download filled, the README and the citation, and all three of those
    files the step reads from the tree are present.

    Class: NEW CAPABILITY — plausible wrong implementations are a check pointed at the gate's record
    or at `dist/` instead of the enumeration, which judges a release against a byte set nobody
    else in this job read; `--dist` pointed at the workspace root, which holds no sdist and refuses
    `wrong-artifacts` on every release; and a check that reads one document and not the other, so
    the README or the citation can state a licence the metadata does not carry. The step's name and
    the job's shape survive each. The test is red under each.

    Why: the four inputs are the whole of what the check compares, and the step runs before the
    enumeration is kept, so this is the last point at which a disagreement can stop the release. A
    check naming a document the tree does not hold also fails on the release run and nowhere
    before it, which is the one run nobody can iterate on.

    Killed by: dropping or renaming a flag, pointing one at another path, naming another script, or
    deleting the README or the citation. Second correct implementation: the flags in another order,
    and the arguments quoted or bare, which `_unquoted` normalises.
    """
    command = _unquoted(str(_step(_job(DISTRIBUTION_CHECK), LICENCE_STEP).get("run")))
    assert LICENCE_SCRIPT in command, command
    for flag in LICENCE_FLAGS:
        assert flag in command, (flag, command)

    for path in (LICENCE_SCRIPT, README_PATH, CITATION_PATH):
        assert (REPO_ROOT / path).is_file(), path


def test_the_enumeration_is_kept_as_the_named_artifact_with_no_condition():
    """Property: the check job keeps exactly one artifact — the enumeration, under the name the
    Release job downloads — and the step that keeps it carries no `if:`.

    Class: NEW CAPABILITY — plausible wrong implementations are the keeper renamed, so the job that
    attaches the record downloads nothing; the artifact named something else, so a later reader's
    name resolves nowhere until the first release; and the keeper carrying `if: always()`, which
    stores the enumeration of a release that the comparison refused, which is the record of a
    release that did not happen read by a procedure that assumes it did. The test is red under each.

    Why: this artifact is the release's own evidence and the only durable one short of the Release
    asset a later chunk attaches. A conditional keeper makes it ambiguous whether a stored
    enumeration is one of a passing release, and an ambiguity in evidence is worse than a gap.

    Killed by: renaming the step or the artifact, adding any `if` to the step, adding a second
    upload to this job, or pointing `path` at a file the enumeration does not write. Second correct
    implementation: `if-no-files-found`, `retention-days` or a comment on the step, none of which is
    an `if`.
    """
    steps = _steps(_job(DISTRIBUTION_CHECK))
    uploads = [step for step in steps if _uses(step, "actions/upload-artifact@v5")]
    assert len(uploads) == 1, [step.get("uses") for step in steps]
    keeper = uploads[0]
    assert keeper.get("name") == KEEP_STEP, keeper.get("name")
    assert "if" not in keeper, sorted(keeper)
    arguments = keeper.get("with")
    assert isinstance(arguments, dict), arguments
    assert arguments.get("name") == ENUMERATION_ARTIFACT, arguments.get("name")
    assert str(arguments.get("path", "")).endswith("release-enumeration.json"), arguments.get("path")


def test_the_release_job_runs_after_the_uploads_and_is_the_only_job_that_may_write():
    """Property: `create-release` waits for the last upload, carries no condition, and is the one
    job granted `contents: write`.

    Class: NEW CAPABILITY — plausible wrong implementations are the Release created from `build`,
    which announces a release whose uploads have not run; the job carrying `if: always()`, which
    creates it after a refused check; and the job carrying no `permissions:` at all, which leaves
    `gh` unable to create anything. The test is red under each.

    Why: a Release is the durable, public record of a release, so it must not exist before the
    bytes are on the index, and the write grant is the one permission this workflow needs beyond
    the gate's read-only access. Granting it to a job that only publishes bytes widens the token
    for nothing.

    Killed by: changing the `needs`, adding an `if`, dropping or narrowing the job's grant, or
    granting `contents: write` to another job. Second correct implementation: `needs` written as a
    block sequence or as the single job id, and the grant as a flow mapping, both of which this
    reader normalises.
    """
    job = _job(RELEASE)
    assert _needs(job) == frozenset({"publish-to-pypi"}), job.get("needs")
    assert _condition(job.get("if")) is None, job.get("if")
    permissions = job.get("permissions")
    assert permissions == RELEASE_PERMISSIONS, permissions

    writers = []
    for name in GRAPH:
        granted = _job(name).get("permissions")
        if isinstance(granted, dict) and granted.get("contents") == "write":
            writers.append(name)
    assert writers == [RELEASE], writers


def test_the_release_job_attaches_the_kept_enumeration_to_the_release_it_creates():
    """Property: the Release job downloads the `release-enumeration` artifact the check job keeps,
    and creates the Release for the tag with that record as its asset.

    Class: NEW CAPABILITY — plausible wrong implementations are the Release created with no asset,
    which leaves the byte record a CI artifact again; the asset taken from
    `python-package-distributions`, which attaches the wrong file under the right name; and the
    Release created for a computed version string rather than the tag the run was started by, which
    names a Release the identifiers step cannot find.

    Why: the asset is the whole point of the job — the byte record has to outlive the artifact's
    retention as well as the Release, and `scripts/record_identifiers.py` finds the release by tag
    and reads the digest of the asset it downloads. `--verify-tag` keeps the tag the run's own
    rather than letting `gh` create one.

    Killed by: dropping the download, pointing `path` at another artifact, naming another tag, or
    dropping `--verify-tag`, `--notes-file` or the asset argument. Second correct implementation:
    the asset named through a variable the job sets, and the flags in another order.
    """
    steps = _steps(_job(RELEASE))
    downloads = [
        step
        for step in steps
        if _uses(step, "actions/download-artifact@")
        and isinstance(step.get("with"), dict)
        and step["with"].get("name") == ENUMERATION_ARTIFACT
    ]
    assert len(downloads) == 1, [step.get("name") for step in steps]
    arguments = downloads[0]["with"]
    assert str(arguments.get("path", "")).endswith("release-enumeration"), arguments

    create = _step(_job(RELEASE), RELEASE_CREATE_STEP)
    command = _unquoted(str(create.get("run")))
    assert command.startswith("gh release create"), command
    assert "$GITHUB_REF_NAME" in command, command
    assert ENUMERATION_PATH in command, command
    assert "--verify-tag" in command, command
    assert f"--notes-file {RELEASE_NOTES_PATH}" in command, command

    environment = create.get("env")
    assert isinstance(environment, dict), environment
    assert environment.get("GH_TOKEN") == "${{ github.token }}", environment
    assert environment.get("GH_REPO") == "${{ github.repository }}", environment


def test_the_release_notes_state_the_attached_digest_and_the_three_licence_statements():
    """Property: the notes step hashes the file it attaches, writes the digest into the notes, and
    states the source, legacy-corpus and data licence statements the release owes.

    Class: NEW CAPABILITY — plausible wrong implementations are notes with no digest, which leaves
    the identifiers step comparing its download against nothing and refusing every release; a
    digest computed from `dist/` rather than from the attached record, which states a value the
    asset does not hash to; and notes carrying one licence statement where the release ships three
    artifact sets under two licences and deposits no data.

    Why: `scripts/record_identifiers.py` refuses `asset-digest` unless the digest of the bytes it
    downloads is one the release's own notes state, so a digest that is absent or about another
    file fails the identifiers run after the release is public. The three statements are what the
    release ships and under which terms, and the Release is where a reader of the archive reads
    them.

    Killed by: dropping the `sha256sum` of the attached path, writing the digest outside the notes,
    or dropping any of the three statements. Second correct implementation: the digest produced by
    another hashing command, and the statements in another order.
    """
    command = _unquoted(str(_step(_job(RELEASE), RELEASE_NOTES_STEP).get("run")))
    assert f"sha256sum {ENUMERATION_PATH}" in command, command
    assert "$DIGEST" in command, command
    assert "has sha256 %s" in command, command
    assert RELEASE_NOTES_PATH in command, command

    assert "AGPL-3.0-or-later" in command, command
    assert "MIT" in command, command
    assert "no dataset" in command, command


def test_the_reader_reads_a_trailing_comment_a_block_sequence_and_a_block_scalar():
    """Property: the reader this module applies to the workflow handles the forms a correct edit may take.

    Class: NEW CAPABILITY — the reader itself. Plausible wrong implementations are a reader that
    keeps a trailing comment in a value, one that reads only a flow `needs:` list, and one that
    stops at a `run: |` marker; each makes an assertion above pass over a file it did not read,
    which is worse than a red test because it reads as evidence.

    Why: the assertions above are only as good as this reader, and a reader's own failures are
    invisible in a passing suite. The three forms checked here are the ones the workflow files in
    this tree use or may use.

    Killed by: any of the three regressions named above. Second correct implementation: none
    beyond another spelling of the same three forms, which is what this test feeds it.
    """
    text = (
        "on:\n"
        '  push:\n    tags: [ "v*" ]  # a release tag only\n'
        "jobs:\n"
        "  gate:\n"
        "    needs:\n"
        "      - build\n"
        "    steps:\n"
        "      - name: Run\n"
        "        run: |\n"
        "          python scripts/check_release_tag.py --tag v1\n"
        "          echo done\n"
        "  build:\n"
        "    needs: gate\n"
    )
    parsed = _entries(_content(text))
    assert parsed["on"]["push"]["tags"] == ["v*"], parsed["on"]["push"]["tags"]
    jobs = parsed["jobs"]
    assert jobs["gate"]["needs"] == ["build"], jobs["gate"]["needs"]
    assert jobs["build"]["needs"] == "gate", jobs["build"]["needs"]
    step = jobs["gate"]["steps"][0]
    assert step["name"] == "Run", step
    assert step["run"] == "python scripts/check_release_tag.py --tag v1\necho done", repr(step["run"])
