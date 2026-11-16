"""Tests that the release workflow builds and publishes only behind its gate.

A `v*` tag runs six jobs. One runs the whole test tree at the tagged commit, the next reads that
run's result record and refuses the tag unless the run was whole, complete and passing, and the
build, the distribution listing and both uploads hang off those two. So the gate's position in the
graph, the condition on it, and the record it reads are what keep a tag the suite does not support
from reaching an index. A gate moved after the build, a gate skipped when the suite job fails, or a
gate pointed at another artifact all keep this file's shape and publish anyway.

The file is read as text and not parsed as YAML, the form `tests/test_whole_suite_workflow.py` and
`tests/correspondence/test_divergence_mounts.py` already take. The State check job installs only the
toolkit and `pytest`, so a YAML library would be a new dependency of the job these tests run in.

What is asserted here, and what is deliberately not: the trigger, the called workflow and that the
file it names exists, each of the six jobs' `needs` and `if`, the run identity the gate publishes,
the artifact the gate downloads, the command that reads it, and the one named step the distribution
check must hold. Not the set of jobs, and not the other steps of the distribution check: a later
change adds steps there and may add a job after the uploads, and neither makes this graph wrong.
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
}

GATE = "release-gate"
DISTRIBUTION_CHECK = "post-build-check"
DISTRIBUTION_STEP = "List the distributions"
RECORD_ARTIFACT = "whole-suite-result-record"
GATE_SCRIPT = "scripts/check_release_tag.py"
GATE_FLAGS = ("--tag", "--commit", "--main-ref", "--record")

# The run identity the gate publishes for the steps that later read `needs.release-gate.outputs.*`.
# It is the whole-suite run's own identity, which is how a reader reaches that run's record: a gate
# that published a value it computed itself would point a later step at the wrong run.
GATE_OUTPUTS = {
    "whole_suite_run_id": "${{ github.run_id }}",
    "whole_suite_run_attempt": "${{ github.run_attempt }}",
    "whole_suite_commit": "${{ github.sha }}",
}

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
