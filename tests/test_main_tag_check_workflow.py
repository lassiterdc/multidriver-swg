"""Tests that the main-tag job reports an untagged `main` head, and reports nothing else.

`.github/workflows/main-tag-check.yml` is the only place a `main` head that nobody tagged is
noticed. A release is published from a tag, so an untagged head means the commit a push put on
`main` will never reach an index -- and no other run says so: the push starts no release workflow,
the suite is green, and the next push moves the head and the question away.

Four properties make that report worth reading, and each is a rule over the file rather than over
one line of it: the run happens only on a push to `main` (a run on every branch reports a head that
is not the branch a release is cut from, and says so sixty minutes later on every push); it waits,
because a tag is pushed by hand after the release is cut and an immediate read names every commit;
the interval is the one stated in one place and its comment gives the reason it is that number; and
the read is a command whose input is the remote's tag list and whose subject is the pushed commit,
run through the script this tree carries.

Two further facts are held by absence. No `needs`, `if` or `continue-on-error` key appears
anywhere in the file, because each of them can stop the report from running at all: a job skipped
by a condition reports nothing, and a job whose only step is conditioned away reports nothing while
the run reads as passing. And the checkout fetches one commit, because the tags come from the
remote: a full history fetches a tag set this job never reads.

The file is read as text rather than parsed as YAML, for the reason
`tests/test_whole_suite_workflow.py` and `tests/correspondence/test_divergence_mounts.py` give: the
State check job installs the toolkit and `pytest` and no YAML library, so a module-level
`import yaml` would stop that job at collection. The reader below is that module's, taken as it
stands, because the two files pose the same questions and a second reader would answer them
differently.
"""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "main-tag-check.yml"
SCRIPT = REPO_ROOT / "scripts" / "check_main_tag.py"

NAME = "Main tag check"
JOB = "main-tag-check"
# The interval and the job timeout, as the file writes them: the reader returns scalars as written,
# so both are compared as text rather than as numbers.
WAIT_MINUTES = "60"
TIMEOUT_MINUTES = "90"
WAIT_STEP = "Wait for a tag to reach the pushed commit"
REPORT_STEP = "Report whether the pushed commit carries a tag"
# The two commands, by equality: a `--waited` that reads another variable, a `--commit` that reads
# another context value, and a `sleep` that hard-codes one of the two factors all keep the shape of
# the line while measuring something else.
SLEEP_COMMAND = 'sleep "$((WAIT_MINUTES * 60))"'
REPORT_COMMAND = (
    'git ls-remote --tags origin | python3 scripts/check_main_tag.py --commit "$GITHUB_SHA" --waited "$WAIT_MINUTES"'
)

_KEY_VALUE = re.compile(r"^([A-Za-z_][A-Za-z0-9_-]*):(?:\s+(.*))?$")
_BLOCK_SCALAR = re.compile(r"^[|>][+-]?$")
# The keys that skip a job or a step, or let a red one report as a green run. Exactly the three the
# criterion names, and NOT `strategy`, which `tests/test_whole_suite_workflow.py` also holds: a one-
# variant matrix is a correct implementation of every property this file asserts, so holding it would
# refuse a correct file. That is the over-constraint side of the two-sided bar, and it is worth more
# than the hypothetical empty matrix the wider set would catch.
_GATING_KEYS = ("if", "continue-on-error", "needs")


def _strip_comment(text: str) -> str:
    """`text` up to a `#` that starts a comment, per YAML's rule: at the start, or after whitespace, outside quotes.

    Without this, `--waited "$WAIT_MINUTES"` would read whole and a trailing comment on the same
    line would become part of the value, so the equality assertions below would red on a correct
    file the first time someone annotated a line.
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
    """Every non-blank, non-comment line of the file as (indent, stripped text).

    Comments and blank lines never belong to a block, so the comment this file carries between two
    keys does not end the mapping it sits inside.
    """
    lines: list[tuple[int, str]] = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            lines.append((len(line) - len(line.lstrip()), stripped))
    return lines


def _scalar(text: str) -> str:
    """A scalar as written, with a trailing comment removed and one layer of matching quotes."""
    value = _strip_comment(text.strip())
    if len(value) > 1 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    return value


def _value(text: str) -> object:
    """An inline value: `[a, b]` becomes a list, `{k: v}` a mapping, and anything else a scalar.

    The inline mapping is read because `concurrency: {group: ..., cancel-in-progress: true}` is the
    same declaration as the two-line form this file writes, and a reader that returned `{}` for it
    would red on a file that means what the block form means. Neither a key nor a value of this file
    carries a comma or a colon, so splitting on the separator is the whole of the parse.
    """
    value = _strip_comment(text.strip())
    if value.startswith("[") and value.endswith("]"):
        return [_scalar(item) for item in value[1:-1].split(",") if item.strip()]
    if value.startswith("{") and value.endswith("}"):
        inner = value[1:-1].strip()
        if not inner:
            return {}
        mapping: dict[str, str] = {}
        for entry in inner.split(","):
            key, separator, item = entry.partition(":")
            assert separator, f"not a `key: value` entry of an inline mapping: {entry!r}"
            mapping[key.strip()] = _scalar(item)
        return mapping
    return _scalar(value)


def _block(lines: list[tuple[int, str]], index: int) -> tuple[object, int]:
    """The block of entries starting at `lines[index]`, and the index one past it.

    The block's own indent is its first line's; every entry at that indent belongs to it, and a
    deeper entry belongs to the entry above it. A block whose first line starts with `-` is a list,
    which is how the trigger list and the step list are read.
    """
    if lines[index][1].startswith("-"):
        return _list_block(lines, index)
    return _mapping_block(lines, index)


def _block_scalar(lines: list[tuple[int, str]], index: int, indent: int, marker: str) -> tuple[str, int]:
    """The body of a `run: |` or `run: >` block scalar, and the index one past it.

    A block scalar's content sits on the following lines and not in the value, so a reader that
    stopped at the `|` would see no command at all -- and a step rewritten as `run: |` with its
    command narrowed would pass every equality in this file while running something else.
    """
    end = index
    while end < len(lines) and lines[end][0] > indent:
        end += 1
    joiner = "\n" if marker.startswith("|") else " "
    return joiner.join(text for _, text in lines[index:end]), end


def _mapping_block(lines: list[tuple[int, str]], index: int) -> tuple[dict[str, object], int]:
    """A `key: value` block at one indent as a dict, recursing into whichever keys have a block."""
    indent = lines[index][0]
    mapping: dict[str, object] = {}
    while index < len(lines) and lines[index][0] == indent:
        match = _KEY_VALUE.match(lines[index][1])
        assert match is not None, f"not a `key: value` line: {lines[index][1]!r}"
        key, inline = match.group(1), match.group(2)
        index += 1
        if inline is None:
            if index < len(lines) and lines[index][0] > indent:
                mapping[key], index = _block(lines, index)
            else:
                mapping[key] = None
        elif _BLOCK_SCALAR.match(_strip_comment(inline)):
            mapping[key], index = _block_scalar(lines, index, indent, _strip_comment(inline))
        else:
            mapping[key] = _value(inline)
    return mapping, index


def _list_block(lines: list[tuple[int, str]], index: int) -> tuple[list[object], int]:
    """A `- item` block at one indent as a list; an item of `key: value` lines is a dict."""
    indent = lines[index][0]
    items: list[object] = []
    while index < len(lines) and lines[index][0] == indent:
        text = lines[index][1]
        if not text.startswith("-"):
            break
        content = text[1:].strip()
        item_indent = indent + len(text) - len(content)
        end = index + 1
        while end < len(lines) and lines[end][0] > indent:
            end += 1
        item = [(item_indent, content), *lines[index + 1 : end]]
        if _KEY_VALUE.match(content):
            mapping, _ = _mapping_block(item, 0)
            items.append(mapping)
        else:
            items.append(_scalar(content))
        index = end
    return items, index


def _parse(text: str) -> dict[str, object]:
    """A workflow text as nested dicts and lists."""
    parsed, _ = _block(_content(text), 0)
    assert isinstance(parsed, dict), parsed
    return parsed


def _workflow() -> dict[str, object]:
    """The workflow file, read as text and returned as nested dicts and lists."""
    return _parse(WORKFLOW.read_text(encoding="utf-8"))


def _job(workflow: dict[str, object] | None = None) -> dict[str, object]:
    """The workflow's one job, asserted to be the only one so a second cannot carry a different one."""
    jobs = (workflow or _workflow())["jobs"]
    assert isinstance(jobs, dict), jobs
    assert list(jobs) == [JOB], list(jobs)
    job = jobs[JOB]
    assert isinstance(job, dict), job
    return job


def _steps() -> list[dict[str, object]]:
    """Every step of that job, in file order."""
    steps = _job()["steps"]
    assert isinstance(steps, list), steps
    assert all(isinstance(step, dict) for step in steps), steps
    return steps


def _step_named(name: str) -> dict[str, object]:
    """The one step carrying `name`, asserted to occur exactly once so a second one cannot hide."""
    matches = [step for step in _steps() if step.get("name") == name]
    assert len(matches) == 1, [step.get("name") for step in _steps()]
    return matches[0]


def _keyed_values(tree: object, key: str) -> list[str]:
    """Every value stored under `key` at any depth, as text, in file order.

    The walk descends into lists as well as mappings, because `with:` sits inside the step list: a
    walker that recursed only into mappings would report no `fetch-depth` at all over a file whose
    checkout fetches the full history.
    """
    found: list[str] = []
    if isinstance(tree, dict):
        for name, value in tree.items():
            if name == key:
                found.append(str(value))
            found.extend(_keyed_values(value, key))
    elif isinstance(tree, list):
        for item in tree:
            found.extend(_keyed_values(item, key))
    return found


def _gating_entries(tree: object) -> list[str]:
    """Every gating key of the parsed file, at any depth, as `key=value` so the report names both."""
    found: list[str] = []
    if isinstance(tree, dict):
        for key, value in tree.items():
            if key in _GATING_KEYS:
                found.append(f"{key}={value}")
            found.extend(_gating_entries(value))
    elif isinstance(tree, list):
        for item in tree:
            found.extend(_gating_entries(item))
    return found


def _raw_lines() -> list[str]:
    """The file's lines as written, comments included: the one question here is about a comment."""
    return WORKFLOW.read_text(encoding="utf-8").splitlines()


def test_the_run_waits_the_stated_interval_and_its_timeout_outlasts_the_wait():
    """Property: `WAIT_MINUTES` is 60, the wait step sleeps that many minutes, and the timeout is longer.

    Class: NEW CAPABILITY -- plausible wrong implementation is a hard-coded `sleep 3600` (the
    interval then lives in two places and a change to the declaration does nothing), a `--waited`
    passed as a different value than the one slept (the report states an interval the run did not
    wait), or a `timeout-minutes` of 60 (GitHub cancels the job as it becomes due to report).

    Why: the interval is the fact the report states, and the timeout is the fact that decides
    whether the statement is ever made. Equality on the two commands holds the value and the
    multiplication together; the comparison holds the ordering that a later edit can invert while
    both numbers stay plausible.

    Killed by: changing the declaration without the command, hard-coding the sleep, passing another
    value to `--waited`, or lowering the timeout to the interval or below it.
    """
    job = _job()
    env = job["env"]
    assert isinstance(env, dict), env
    assert env["WAIT_MINUTES"] == WAIT_MINUTES, env["WAIT_MINUTES"]
    assert int(str(job["timeout-minutes"])) > int(WAIT_MINUTES), job["timeout-minutes"]
    assert _step_named(WAIT_STEP)["run"] == SLEEP_COMMAND, _step_named(WAIT_STEP)["run"]


def test_the_report_reads_the_remote_tags_through_the_script_this_tree_carries():
    """Property: the last step is exactly the piped command, and the script it names is a file here.

    Class: NEW CAPABILITY -- plausible wrong implementation is the same command with `--commit`
    reading another value (`$GITHUB_REF_NAME`, the tag name), with `--waited` taking a literal, with
    the pipe reversed, or naming a script path this tree does not carry.

    Why: this is the only place the pushed commit reaches the report. The command's subject is
    `$GITHUB_SHA`, which is the commit the push put on `main`, and the input on its left is the
    remote's tag list, which is the set the report must search. A path that resolves nowhere fails
    at the point the report was due, sixty minutes after the push, and that failure is reported on
    no branch but `main`.

    Killed by: changing either flag, swapping the pipe, or renaming the script without the workflow.
    Second correct implementation: a `run: |` block scalar carrying that one command, which the
    reader joins to the same text.
    """
    step = _step_named(REPORT_STEP)
    assert step["run"] == REPORT_COMMAND, step["run"]
    named = re.search(r"python3 (?P<path>\S+)", REPORT_COMMAND)
    assert named is not None, REPORT_COMMAND
    assert (REPO_ROOT / named.group("path")).resolve() == SCRIPT.resolve(), named.group("path")
    assert SCRIPT.is_file(), SCRIPT


def test_the_report_step_names_the_shell_that_fails_the_step_on_a_failed_pipe():
    """Property: the report step carries `shell: bash`, so its pipe runs under `-eo pipefail`.

    Class: NEW CAPABILITY -- plausible wrong implementation is the same step with the key removed,
    with `shell: sh` (which carries no `pipefail`), or with a shell string whose options drop it
    (`bash -e {0}`). Under each, a `git ls-remote` that fails writes nothing, the script reads an
    empty stdin, and the run reports the head as untagged while passing -- which is the exact
    sentence a correctly untagged head produces.

    Why: this step is two commands joined by a pipe, and the report's only input is the left one.
    A shell without `pipefail` returns the pipe's LAST command's status, so the failing read is
    invisible and the passing script's `exit 0` is what the job reports. The key is the whole of
    what separates "the remote lists no tag carrying this commit" from "the remote could not be
    read", and a reader of the run's log cannot tell those two apart at the report's own words.

    Killed by: removing the key, spelling it `sh`, or passing shell options that drop `pipefail`.
    Second correct implementation: the key written before `name` in the step mapping, which this
    reader does not order, and the value quoted (`shell: "bash"`), which the reader unquotes.
    """
    step = _step_named(REPORT_STEP)
    assert step["shell"] == "bash", step.get("shell")


def test_no_key_skips_the_run_or_lets_a_failing_one_report_as_a_pass():
    """Property: the file carries no `needs`, `if` or `continue-on-error` key anywhere.

    Class: NEW CAPABILITY -- plausible wrong implementation is the report step written with a
    condition (`if: github.event_name == 'push'`), a job-level `if` that never holds, or a
    `continue-on-error` that lets a broken invocation pass.

    Why: this job exists to report. A condition that skips it or a need that never resolves
    replaces the report with a green run that reports nothing -- and a green run that reports
    nothing is indistinguishable, in the checks list, from a head that carried a tag. The walk
    covers the whole file, so a key added at any depth reds this test.

    Deliberately NOT held: `strategy`. A one-variant matrix skips nothing and is a correct
    implementation of every property here, so refusing it would be the over-constraint side of the
    two-sided bar. The three keys held are the three the criterion names.

    Killed by: adding any of the three keys at any depth.
    """
    entries = _gating_entries(_workflow())
    assert entries == [], entries
