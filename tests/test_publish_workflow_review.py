"""Reviewer tests of the release workflow: a gate that is in the graph can still be made to refuse nothing.

The workflow test beside this one pins where the gate sits (each job's `needs`), the one condition it
runs on, the artifact it downloads, and that the four flags of the refusal command appear. All of that
holds for a gate that cannot refuse: the refusal step ending in `|| true`, carrying `continue-on-error`
or `if: false`, the main reference pointed at `HEAD`, the record path pointed at a directory the
download never wrote, or a checkout too shallow to resolve `origin/main`. Each of those keeps every
assertion in that file green and lets a refused tag reach both indexes. Seven rules, each a function of
the workflow's TEXT so one function judges the real file, variants a correct edit may take, and
variants a wrong one takes; a rule that accepts a wrong variant measures nothing, which is what the
refused cases check:

1. no step or job softens a failure, and the only condition anywhere is the gate's own;
2. the refusal step is one bare invocation of the script with the four flags and nothing else;
3. the gate's checkout has the whole history and no `ref`, so the script it runs and the history it
   reads are the tagged commit's;
4. the artifact, the directory it lands in and the file inside it are the ones the whole-suite
   workflow keeps and the refusal command reads, in that order of steps;
5. every job that builds or uploads waits, transitively, for the gate;
6. the call to the whole-suite workflow passes it nothing and grants it no more than a read;
7. the refusal command, run as the runner runs it over a throwaway checkout, refuses what the script
   refuses and passes what it passes.

The text is read and not parsed with a YAML library, for the reason `tests/test_publish_workflow.py`
gives: the State check job installs none. The reader below is this module's own and is shared with no
other test, so a change to the other module's reader cannot change what this one holds the file to.
"""

from __future__ import annotations

import os
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
PUBLISH = REPO_ROOT / ".github" / "workflows" / "publish.yml"
SUITE = REPO_ROOT / ".github" / "workflows" / "whole-suite.yml"
GATE_SCRIPT = REPO_ROOT / "scripts" / "check_release_tag.py"

CALL = "whole-suite"
GATE = "release-gate"
BUILD = "build"
GATE_CONDITION = "!cancelled()"
SCRIPT_PATH = "scripts/check_release_tag.py"
FLAGS = ("--tag", "--commit", "--main-ref", "--record")
TAG_SPELLINGS = ("$GITHUB_REF_NAME", "${GITHUB_REF_NAME}")
COMMIT_SPELLINGS = ("$GITHUB_SHA", "${GITHUB_SHA}")
MAIN_REF = "origin/main"
CALLED_FILE = "./.github/workflows/whole-suite.yml"

# Passed to every git call: a runner has no identity configured, and `git commit` refuses without one.
IDENTITY = ("-c", "user.name=Release Gate", "-c", "user.email=release-gate@example.invalid")

_KEY = re.compile(r"^(?P<key>[A-Za-z0-9_./-]+):(?:[ \t]+(?P<value>.*))?$")
_BLOCK_MARKER = re.compile(r"^[|>][+-]?$")
_TEMP = re.compile(r"\$\{\{\s*(?:runner\.temp|env\.RUNNER_TEMP)\s*\}\}|\$\{?RUNNER_TEMP\}?")
_REFUSAL_LINE = re.compile(r"^refused: (?P<name>[a-z-]+): ", re.MULTILINE)


# --- the reader ------------------------------------------------------------------------------------


def _uncomment(text: str) -> str:
    """`text` up to a `#` that starts a comment: at the start, or after whitespace, outside quotes."""
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


def _rows(text: str) -> list[tuple[int, str]]:
    """Every non-blank, non-comment line as (indent, text), a trailing comment removed."""
    rows = []
    for line in text.splitlines():
        body = _uncomment(line.strip())
        if body:
            rows.append((len(line) - len(line.lstrip()), body))
    return rows


def _unquote(text: str) -> str:
    """One layer of matching quotes removed, so `"v*"` and `v*` read alike."""
    text = text.strip()
    if len(text) > 1 and text[0] == text[-1] and text[0] in "\"'":
        return text[1:-1]
    return text


def _inline(text: str):
    """An inline value: a flow list of scalars, or one scalar. A flow mapping is refused, not misread."""
    text = text.strip()
    assert not text.startswith("{"), f"a flow mapping is not read by this module: {text!r}"
    if text.startswith("["):
        assert text.endswith("]"), f"a flow list that spans lines is not read by this module: {text!r}"
        return [_unquote(item) for item in text[1:-1].split(",") if item.strip()]
    return _unquote(text)


def _is_item(text: str) -> bool:
    """Whether a row opens a block-sequence item: a dash alone, or a dash and a space."""
    return text == "-" or text.startswith("- ")


def _node(rows: list[tuple[int, str]]):
    """The value of the block `rows` open: a sequence, a mapping, or one scalar written over its rows."""
    indent, text = rows[0]
    if _is_item(text):
        return _sequence(rows, indent)
    if _KEY.match(text):
        return _mapping(rows, indent)
    return _inline(" ".join(body for _, body in rows))


def _sequence(rows: list[tuple[int, str]], indent: int) -> list:
    """The items of a block sequence; an item written as `- key: value` rows reads as a mapping."""
    items: list = []
    index = 0
    while index < len(rows):
        row_indent, text = rows[index]
        assert row_indent == indent and _is_item(text), f"unreadable sequence row: {text!r}"
        end = index + 1
        while end < len(rows) and rows[end][0] > indent:
            end += 1
        rest = text[1:].lstrip()
        item = rows[index + 1 : end]
        if rest:
            item = [(indent + len(text) - len(rest), rest), *item]
        items.append(_node(item) if item else None)
        index = end
    return items


def _mapping(rows: list[tuple[int, str]], indent: int) -> dict:
    """The `key: value` rows at one indent; a block scalar is joined, a nested block is read in turn."""
    mapping: dict = {}
    index = 0
    while index < len(rows):
        row_indent, text = rows[index]
        assert row_indent == indent, f"unreadable mapping row: {text!r}"
        matched = _KEY.match(text)
        assert matched is not None, f"not a `key: value` row: {text!r}"
        key, value = matched.group("key"), matched.group("value")
        assert key not in mapping, f"`{key}` occurs twice in one block"
        end = index + 1
        while end < len(rows) and (
            rows[end][0] > indent or (value is None and rows[end][0] == indent and _is_item(rows[end][1]))
        ):
            end += 1
        children = rows[index + 1 : end]
        if value is None:
            mapping[key] = _node(children) if children else None
        elif _BLOCK_MARKER.match(value):
            mapping[key] = ("\n" if value[0] == "|" else " ").join(body for _, body in children)
        else:
            assert not children, f"a plain scalar that spans lines is not read by this module: {text!r}"
            mapping[key] = _inline(value)
        index = end
    return mapping


def _tree(text: str) -> dict:
    """A workflow's text as nested dicts and lists, read by this module's own reader."""
    rows = _rows(text)
    return _mapping(rows, rows[0][0])


def _jobs(tree: dict) -> dict:
    """The `jobs` mapping, asserted to exist so a file with no jobs reds and does not read as clean."""
    jobs = tree["jobs"]
    assert isinstance(jobs, dict), jobs
    return jobs


def _steps(job: dict) -> list[dict]:
    """The steps of a job that are written as mappings, in file order; a job with none, such as a call, has none."""
    steps = job.get("steps") or []
    assert isinstance(steps, list), steps
    return [step for step in steps if isinstance(step, dict)]


def _gate_steps(tree: dict) -> list[dict]:
    """The gate job's steps, and none when the file has no gate job, so a renamed gate reports as a missing step and not as a lookup error."""
    gate = _jobs(tree).get(GATE)
    return _steps(gate) if gate is not None else []


def _as_list(value) -> list:
    """`value` as a list: nothing is empty, a scalar is one item, so `needs: build` and `needs: [build]` read alike."""
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _needs(job: dict) -> frozenset[str]:
    """A job's `needs` as a set, in whichever of the three spellings it was written."""
    return frozenset(str(item) for item in _as_list(job.get("needs")))


def _condition(value) -> str:
    """A condition with its `${{ }}` wrapper removed, so `!cancelled()` and `${{ !cancelled() }}` compare equal."""
    return re.sub(r"^\$\{\{\s*(.*?)\s*\}\}$", r"\1", str(value).strip()).strip()


def _temp(text: str) -> str:
    """Every spelling of the runner's temporary directory replaced by one token."""
    return _TEMP.sub("{temp}", text)


def _uses(step: dict, action: str) -> bool:
    """Whether `step` runs the action `action` at any version."""
    return str(step.get("uses", "")).startswith(f"{action}@")


def _tokens(command: str) -> list[str]:
    """The words of one shell command, with `;`, `|`, `||`, `&&` and redirections as words of their own."""
    lexer = shlex.shlex(command, posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    return list(lexer)


# --- the rules -------------------------------------------------------------------------------------


def _softening_problems(tree: dict) -> list[str]:
    """Every `continue-on-error` and every `if` that is not the gate's own condition or the download's leniency."""
    problems = []
    for job_id, job in _jobs(tree).items():
        if "continue-on-error" in job:
            problems.append(f"job {job_id} carries continue-on-error")
        if "if" in job and not (job_id == GATE and _condition(job["if"]) == GATE_CONDITION):
            problems.append(f"job {job_id} carries the condition {job['if']!r}")
        for step in _steps(job):
            label = step.get("name") or step.get("uses") or step.get("run")
            if "if" in step:
                problems.append(f"step {label!r} of {job_id} carries the condition {step['if']!r}")
            if "continue-on-error" in step and not (
                job_id == GATE and _uses(step, "actions/download-artifact") and str(step["continue-on-error"]) == "true"
            ):
                problems.append(f"step {label!r} of {job_id} carries continue-on-error")
    return problems


def _gate_command(tree: dict) -> tuple[list[str], dict[str, str], dict | None]:
    """The refusal step's problems, its four flags with their values, and the step itself.

    The step is the one step of the gate whose command names the script. It is a bare invocation:
    one line, the script, each of the four flags once, and no other word, so a `|| true`, a second
    command, a redirect or an extra flag is a problem and not a spelling.
    """
    steps = [step for step in _gate_steps(tree) if SCRIPT_PATH in str(step.get("run", ""))]
    if len(steps) != 1:
        return [f"the gate has {len(steps)} steps that run {SCRIPT_PATH}, not one"], {}, None
    step = steps[0]
    problems = []
    if set(step) - {"name", "run"}:
        problems.append(f"the refusal step carries {sorted(set(step) - {'name', 'run'})}, beyond a name and a command")
    command = str(step["run"]).strip()
    if "\n" in command:
        return [*problems, "the refusal command spans more than one line"], {}, step
    words = _tokens(command)
    if words[:2] not in (["python", SCRIPT_PATH], ["python3", SCRIPT_PATH]):
        return [*problems, f"the command does not start `python {SCRIPT_PATH}`: {words[:2]}"], {}, step
    rest = words[2:]
    if len(rest) != 2 * len(FLAGS) or sorted(rest[0::2]) != sorted(FLAGS):
        return [*problems, f"the command is not the four flags and their values, and nothing else: {rest}"], {}, step
    flags = dict(zip(rest[0::2], rest[1::2], strict=True))
    if flags["--tag"] not in TAG_SPELLINGS:
        problems.append(f"--tag is {flags['--tag']!r}, not the runner's own tag variable")
    if flags["--commit"] not in COMMIT_SPELLINGS:
        problems.append(f"--commit is {flags['--commit']!r}, not the runner's own commit variable")
    if flags["--main-ref"] != MAIN_REF:
        problems.append(f"--main-ref is {flags['--main-ref']!r}, not {MAIN_REF!r}")
    if not _temp(flags["--record"]).startswith("{temp}/"):
        problems.append(f"--record is {flags['--record']!r}, not a path under the runner's temporary directory")
    return problems, flags, step


def _command_problems(tree: dict) -> list[str]:
    """The problems with the refusal step and its command, as `_gate_command` reports them."""
    return _gate_command(tree)[0]


def _checkout_problems(tree: dict) -> list[str]:
    """The gate's checkout: whole history, no `ref`, no other repository, and ahead of the record and the script."""
    steps = _gate_steps(tree)
    checkouts = [index for index, step in enumerate(steps) if _uses(step, "actions/checkout")]
    if len(checkouts) != 1:
        return [f"the gate has {len(checkouts)} checkout steps, not one"]
    index = checkouts[0]
    arguments = steps[index].get("with") or {}
    problems = []
    if str(arguments.get("fetch-depth")) != "0":
        problems.append(f"fetch-depth is {arguments.get('fetch-depth')!r}, so `{MAIN_REF}` may not resolve")
    extra = sorted(set(arguments) - {"fetch-depth", "persist-credentials"})
    if extra:
        problems.append(f"the checkout carries {extra}, so the script it runs may not be the tagged commit's")
    later = [
        i
        for i, step in enumerate(steps)
        if _uses(step, "actions/download-artifact") or SCRIPT_PATH in str(step.get("run", ""))
    ]
    if any(i < index for i in later):
        problems.append("a step that reads the record or runs the script comes before the checkout")
    return problems


def _seam_problems(tree: dict, suite: dict) -> list[str]:
    """Where the artifact the suite keeps and the record the gate reads part ways."""
    steps = _gate_steps(tree)
    downloads = [(i, step) for i, step in enumerate(steps) if _uses(step, "actions/download-artifact")]
    uploads = [step for job in _jobs(suite).values() for step in _steps(job) if _uses(step, "actions/upload-artifact")]
    if len(downloads) != 1 or len(uploads) != 1:
        return [f"expected one download in the gate and one upload in the suite: {len(downloads)} {len(uploads)}"]
    _, flags, refusal = _gate_command(tree)
    if refusal is None or not flags:
        return ["the refusal command is unreadable, so the record it reads cannot be placed"]
    position, download = downloads[0]
    received = download.get("with") or {}
    kept = uploads[0].get("with") or {}
    problems = []
    if received.get("name") != kept.get("name"):
        problems.append(f"the gate downloads {received.get('name')!r} and the suite keeps {kept.get('name')!r}")
    directory, _, filename = _temp(flags["--record"]).rpartition("/")
    if directory != _temp(str(received.get("path", ""))).rstrip("/"):
        problems.append(f"the record is read from {directory!r} and the download lands in {received.get('path')!r}")
    if filename != _temp(str(kept.get("path", ""))).rpartition("/")[2]:
        problems.append(f"the record is read as {filename!r} and the suite keeps {kept.get('path')!r}")
    if str(download.get("continue-on-error")) != "true":
        problems.append("the download does not continue on error, so a missing artifact is an error and not a refusal")
    if steps.index(refusal) < position:
        problems.append("the refusal step runs before the record is downloaded")
    return problems


def _reaches(jobs: dict, start: str, target: str, seen: frozenset[str] = frozenset()) -> bool:
    """Whether `target` is `start` or a job `start` waits for, directly or through a chain of `needs`."""
    if start == target:
        return True
    if start in seen or start not in jobs:
        return False
    return any(_reaches(jobs, parent, target, seen | {start}) for parent in _needs(jobs[start]))


def _publishes(job: dict) -> bool:
    """A job that holds an environment, or a token that can mint an upload credential."""
    permissions = job.get("permissions")
    grants = (
        isinstance(permissions, dict) and str(permissions.get("id-token")) == "write"
    ) or permissions == "write-all"
    return "environment" in job or grants


def _graph_problems(tree: dict) -> list[str]:
    """Every job that builds or uploads waits, through some chain of `needs`, for the gate."""
    jobs = _jobs(tree)
    if GATE not in jobs or CALL not in jobs:
        return [f"the workflow has no {GATE!r} or no {CALL!r} job"]
    problems = []
    if _needs(jobs[GATE]) != {CALL}:
        problems.append(f"the gate waits for {sorted(_needs(jobs[GATE]))}, not for the suite job alone")
    guarded = sorted({BUILD, *(job_id for job_id, job in jobs.items() if _publishes(job))} - {GATE})
    for job_id in guarded:
        if job_id not in jobs:
            problems.append(f"there is no {job_id!r} job")
        elif not _reaches(jobs, job_id, GATE):
            problems.append(f"{job_id} does not wait for {GATE}, directly or through another job")
    return problems


def _call_problems(tree: dict) -> list[str]:
    """The call hands the suite workflow no secret, no input and no wider token than a read."""
    job = _jobs(tree).get(CALL)
    if job is None:
        return [f"no {CALL!r} job"]
    problems = []
    extra = sorted(set(job) - {"name", "uses", "permissions"})
    if extra:
        problems.append(f"the call carries {extra}, beyond a name, the called file and a permission")
    if "permissions" in job and job["permissions"] != {"contents": "read"}:
        problems.append(f"the call grants {job['permissions']!r}, not `contents: read`")
    return problems


# --- the cases -------------------------------------------------------------------------------------

_REFERENCE = """\
name: Publish to PyPI

on:
  push:
    tags:
      - "v*"

jobs:
  whole-suite:
    name: Whole suite
    uses: ./.github/workflows/whole-suite.yml

  release-gate:
    name: Refuse a tag that is not a whole passing release
    needs: [whole-suite]
    if: ${{ !cancelled() }}
    runs-on: ubuntu-latest
    outputs:
      whole_suite_run_id: ${{ github.run_id }}
    steps:
      - name: Check out the tagged commit
        uses: actions/checkout@v4
        with:
          fetch-depth: 0
          persist-credentials: false
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Download the whole-suite result record
        uses: actions/download-artifact@v6
        continue-on-error: true
        with:
          name: whole-suite-result-record
          path: ${{ runner.temp }}/whole-suite
      - name: Refuse the tag
        run: python scripts/check_release_tag.py --tag "$GITHUB_REF_NAME" --commit "$GITHUB_SHA" --main-ref origin/main --record "$RUNNER_TEMP/whole-suite/result-record.json"

  build:
    name: Build distribution
    needs: [release-gate]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - name: Build wheel and sdist
        run: python -m build

  post-build-check:
    name: Check the built distributions
    needs: [release-gate, build]
    runs-on: ubuntu-latest
    steps:
      - name: List the distributions
        run: sha256sum dist/*

  publish-to-testpypi:
    name: Publish to TestPyPI
    needs: [post-build-check]
    runs-on: ubuntu-latest
    environment:
      name: testpypi
    permissions:
      id-token: write
    steps:
      - name: Publish to TestPyPI
        uses: pypa/gh-action-pypi-publish@release/v1

  publish-to-pypi:
    name: Publish to PyPI
    needs: [post-build-check, publish-to-testpypi]
    runs-on: ubuntu-latest
    environment:
      name: pypi
    permissions:
      id-token: write
    steps:
      - name: Publish to PyPI
        uses: pypa/gh-action-pypi-publish@release/v1
"""

_SUITE_REFERENCE = """\
name: Whole suite

on:
  workflow_call:

jobs:
  whole-suite:
    runs-on: ubuntu-latest
    steps:
      - name: Keep the result record
        if: always()
        uses: actions/upload-artifact@v5
        with:
          name: whole-suite-result-record
          path: ${{ runner.temp }}/result-record.json
"""

_REFUSE_HEAD = "      - name: Refuse the tag\n"
_GATE_IF = "    if: ${{ !cancelled() }}\n"
_BUILD_NEEDS = "    name: Build distribution\n    needs: [release-gate]\n"
_COMMAND_TAIL = '--record "$RUNNER_TEMP/whole-suite/result-record.json"\n'
_CHECKOUT_STEP = (
    "      - name: Check out the tagged commit\n"
    "        uses: actions/checkout@v4\n"
    "        with:\n"
    "          fetch-depth: 0\n"
    "          persist-credentials: false\n"
)
_DOWNLOAD_STEP = (
    "      - name: Download the whole-suite result record\n"
    "        uses: actions/download-artifact@v6\n"
    "        continue-on-error: true\n"
    "        with:\n"
    "          name: whole-suite-result-record\n"
    "          path: ${{ runner.temp }}/whole-suite\n"
)
_BEFORE_BUILD = "\n  build:\n"
_PUBLISH_PYPI_TAIL = "      - name: Publish to PyPI\n        uses: pypa/gh-action-pypi-publish@release/v1\n"


def _swap(text: str, old: str, new: str, *, text_name: str = "reference") -> str:
    """`text` with the one occurrence of `old` replaced; an anchor absent or repeated is a bug in the case."""
    assert text.count(old) == 1, f"the anchor {old!r} must occur once in the {text_name}"
    return text.replace(old, new)


def _moved(text: str, block: str, before: str) -> str:
    """`text` with the one occurrence of `block` taken out and put back immediately before `before`."""
    return _swap(_swap(text, block, ""), before, block + before)


def _publish_text(case: str | None) -> str:
    """The real workflow for the sentinel `None`, and the case's own text otherwise."""
    return PUBLISH.read_text(encoding="utf-8") if case is None else case


def _suite_text(case: str | None) -> str:
    """The real whole-suite workflow for the sentinel `None`, and the case's own text otherwise."""
    return SUITE.read_text(encoding="utf-8") if case is None else case


@pytest.mark.parametrize(
    ("case", "refused"),
    [
        pytest.param(None, False, id="the real workflow"),
        pytest.param(_REFERENCE, False, id="correct: the reference file"),
        pytest.param(
            _swap(_REFERENCE, _GATE_IF, "    if: !cancelled()  # a bare condition\n"),
            False,
            id="correct: the gate condition without its wrapper, with a comment",
        ),
        pytest.param(
            "# a comment may say continue-on-error and if: always()\n" + _REFERENCE,
            False,
            id="correct: a comment names the keys",
        ),
        pytest.param(
            _swap(_REFERENCE, _REFUSE_HEAD, _REFUSE_HEAD + "        continue-on-error: true\n"),
            True,
            id="wrong: continue-on-error on the refusal step",
        ),
        pytest.param(
            _swap(_REFERENCE, _GATE_IF, _GATE_IF + "    continue-on-error: true\n"),
            True,
            id="wrong: continue-on-error on the gate job",
        ),
        pytest.param(
            _swap(_REFERENCE, _REFUSE_HEAD, "      - if: false\n        name: Refuse the tag\n"),
            True,
            id="wrong: a false condition on the refusal step",
        ),
        pytest.param(
            _swap(
                _REFERENCE,
                "      - uses: actions/checkout@v6\n",
                "      - if: always()\n        uses: actions/checkout@v6\n",
            ),
            True,
            id="wrong: a condition on a build step",
        ),
        pytest.param(
            _swap(_REFERENCE, _BUILD_NEEDS, _BUILD_NEEDS + "    if: ${{ always() }}\n"),
            True,
            id="wrong: always() on the build job, which runs past a refusal",
        ),
        pytest.param(
            _swap(
                _REFERENCE,
                "      - name: Build wheel and sdist\n",
                "      - continue-on-error: true\n        name: Build wheel and sdist\n",
            ),
            True,
            id="wrong: continue-on-error on the build step",
        ),
    ],
)
def test_nothing_in_the_workflow_softens_a_failure_and_the_only_condition_is_the_gates(case, refused):
    """Property: no job or step carries `continue-on-error`, and no job or step carries an `if`, except the gate's `!cancelled()` and the record download's leniency.

    Class: NEW CAPABILITY — the keys of the jobs and steps. The plausible wrong implementations are
    `continue-on-error: true` on the refusal step or on the gate job, which turns a refusal into a
    green job that every later job then waits on; `if: false` on the refusal step; `always()` on the
    build job, which runs it past a failed gate; and a condition or leniency on a build step. The
    workflow test beside this one reads each job's own `needs` and `if` and nothing at step level or
    in `continue-on-error`, so each of these stays green there; this test is red under each.

    Why: the gate is the one job whose failure is meant to stop a release, and every softening key is
    a way for it to fail without stopping anything. A gate that reads a failing record and still lets
    the build run is indistinguishable, in the run's graph, from one that passed.

    Killed by: adding any of the two keys anywhere but the one place each is allowed. Second correct
    implementation: the gate condition written with or without `${{ }}`, and a comment that names the
    keys.

    Not established: `timeout-minutes`, `concurrency` and `strategy` can also end or alter a run and
    are not read; a later job that legitimately needs a condition (cleanup after the uploads) has to
    change this rule and say why.
    """
    assert bool(_softening_problems(_tree(_publish_text(case)))) is refused, _softening_problems(
        _tree(_publish_text(case))
    )


@pytest.mark.parametrize(
    ("case", "refused"),
    [
        pytest.param(None, False, id="the real workflow"),
        pytest.param(_REFERENCE, False, id="correct: the reference file"),
        pytest.param(
            _swap(_REFERENCE, '"$GITHUB_SHA"', '"${GITHUB_SHA}"'),
            False,
            id="correct: the braced spelling of the commit variable",
        ),
        pytest.param(
            _swap(
                _REFERENCE,
                "run: python scripts/check_release_tag.py --tag",
                "run: python3 scripts/check_release_tag.py --tag",
            ),
            False,
            id="correct: python3",
        ),
        pytest.param(
            _swap(
                _REFERENCE,
                '--tag "$GITHUB_REF_NAME" --commit "$GITHUB_SHA" --main-ref origin/main ',
                '--main-ref origin/main --commit "$GITHUB_SHA" --tag "$GITHUB_REF_NAME" ',
            ),
            False,
            id="correct: the flags in another order",
        ),
        pytest.param(
            _swap(
                _REFERENCE,
                "        run: python scripts/check_release_tag.py",
                "        run: >-\n          python scripts/check_release_tag.py",
            ),
            False,
            id="correct: the command as a folded block scalar",
        ),
        pytest.param(
            _swap(_REFERENCE, _COMMAND_TAIL, _COMMAND_TAIL.rstrip("\n") + " || true\n"), True, id="wrong: || true"
        ),
        pytest.param(
            _swap(_REFERENCE, _COMMAND_TAIL, _COMMAND_TAIL.rstrip("\n") + "; true\n"), True, id="wrong: ; true"
        ),
        pytest.param(
            _swap(_REFERENCE, "--main-ref origin/main", "--main-ref HEAD"), True, id="wrong: main reference HEAD"
        ),
        pytest.param(
            _swap(_REFERENCE, "--main-ref origin/main", "--main-ref main"), True, id="wrong: main reference main"
        ),
        pytest.param(_swap(_REFERENCE, '--tag "$GITHUB_REF_NAME"', "--tag v0.0.0"), True, id="wrong: a literal tag"),
        pytest.param(
            _swap(_REFERENCE, '--tag "$GITHUB_REF_NAME"', '--tag "${{ github.ref_name }}"'),
            True,
            id="wrong: the tag spliced into the script text as an expression",
        ),
        pytest.param(
            _swap(_REFERENCE, _COMMAND_TAIL, _COMMAND_TAIL.rstrip("\n") + " --repo ..\n"),
            True,
            id="wrong: an extra flag",
        ),
        pytest.param(
            _swap(_REFERENCE, _COMMAND_TAIL, _COMMAND_TAIL + "        working-directory: /tmp\n"),
            True,
            id="wrong: another working directory",
        ),
        pytest.param(
            _swap(_REFERENCE, _COMMAND_TAIL, _COMMAND_TAIL + "        shell: bash {0}\n"),
            True,
            id="wrong: another shell",
        ),
        pytest.param(
            _swap(_REFERENCE, _COMMAND_TAIL, _COMMAND_TAIL + "        env:\n          GITHUB_SHA: 0000\n"),
            True,
            id="wrong: an environment block on the step",
        ),
        pytest.param(
            _swap(
                _REFERENCE, "run: python scripts/check_release_tag.py", "run: echo skipped scripts/check_release_tag.py"
            ),
            True,
            id="wrong: a step that only names the script",
        ),
    ],
)
def test_the_refusal_step_is_one_bare_invocation_of_the_script_with_the_four_flags(case, refused):
    """Property: the one step of the gate that names the script runs it, once, with each of the four flags and the runner's own tag and commit variables, `origin/main`, and a record under the runner's temporary directory.

    Class: NEW CAPABILITY — the refusal command. The plausible wrong implementations are `|| true` or
    `; true` after it, which exits 0 whatever the script said; a main reference of `HEAD` or `main`,
    which is satisfied by any commit on the checked-out branch or does not resolve at all in a tag
    checkout; a literal tag; and the tag spliced in as `${{ github.ref_name }}`, which a tag name can
    use to run shell. The workflow test beside this one reads that the four flag names are present
    and that `--record` points at some `result-record.json`, so each stays green there; this test is
    red under each.

    Why: the script's exit status is the gate. Anything that changes what reaches the script, or what
    becomes of its status, changes what a release is held to without changing the graph.

    Killed by: any word on the line beyond the script and its four flag-value pairs, any value but
    the one named, and any key on the step but its name and command. Second correct implementation:
    the flags in another order, a braced spelling of a variable, `python3`, and the command as a
    folded block scalar.

    Not established: that the values the runner puts in the variables are the ones intended; the
    integration test below runs the line as the runner does, over a checkout it builds.
    """
    assert bool(_command_problems(_tree(_publish_text(case)))) is refused, _command_problems(_tree(_publish_text(case)))


@pytest.mark.parametrize(
    ("case", "refused"),
    [
        pytest.param(None, False, id="the real workflow"),
        pytest.param(_REFERENCE, False, id="correct: the reference file"),
        pytest.param(
            _swap(_REFERENCE, "          fetch-depth: 0\n", '          fetch-depth: "0"\n'),
            False,
            id="correct: the depth quoted",
        ),
        pytest.param(
            _swap(_REFERENCE, "          fetch-depth: 0\n", ""),
            True,
            id="wrong: no fetch-depth, so a one-commit checkout and no origin/main",
        ),
        pytest.param(
            _swap(_REFERENCE, "fetch-depth: 0", "fetch-depth: 1"),
            True,
            id="wrong: a depth of one",
        ),
        pytest.param(
            _swap(_REFERENCE, "          fetch-depth: 0\n", "          fetch-depth: 0\n          ref: main\n"),
            True,
            id="wrong: a ref, so the script run is the branch's and not the tagged commit's",
        ),
        pytest.param(
            _swap(
                _REFERENCE,
                "          fetch-depth: 0\n",
                "          fetch-depth: 0\n          repository: other/repository\n",
            ),
            True,
            id="wrong: another repository",
        ),
        pytest.param(
            _moved(_REFERENCE, _CHECKOUT_STEP, _BEFORE_BUILD),
            True,
            id="wrong: the checkout after the refusal step",
        ),
        pytest.param(
            _swap(_REFERENCE, _CHECKOUT_STEP, ""),
            True,
            id="wrong: no checkout in the gate",
        ),
    ],
)
def test_the_gates_checkout_has_the_whole_history_and_no_ref_and_comes_first(case, refused):
    """Property: the gate's one checkout fetches the whole history, names no `ref` or other repository, and precedes the download and the refusal step.

    Class: NEW CAPABILITY — the checkout the script and its ancestry check depend on. The plausible
    wrong implementations are a default checkout (depth one), where `origin/main` does not exist and
    the ancestry check refuses every tag or, worse, is reworked to pass; a `ref:` that makes the
    runner run the branch's copy of the script and not the tagged commit's; and a checkout after the
    refusal step. The workflow test beside this one reads no checkout at all, so each stays green
    there; this test is red under each.

    Why: the script reads git history for one of its five refusals and is itself a file of the
    commit under test. A shallow or redirected checkout changes what the gate can see or runs a
    different gate.

    Killed by: removing or lowering `fetch-depth`, adding `ref` or `repository`, deleting the
    checkout, or moving it behind a step that needs it. Second correct implementation: the depth
    written as a quoted number, and `persist-credentials` set either way.

    Not established: that GitHub's checkout of a tag push creates `origin/main` at depth zero; the
    integration test below builds that state and does not measure the runner's.
    """
    assert bool(_checkout_problems(_tree(_publish_text(case)))) is refused, _checkout_problems(
        _tree(_publish_text(case))
    )


@pytest.mark.parametrize(
    ("publish", "suite", "refused"),
    [
        pytest.param(None, None, False, id="the real files"),
        pytest.param(_REFERENCE, _SUITE_REFERENCE, False, id="correct: the reference files"),
        pytest.param(
            _swap(
                _swap(_REFERENCE, '"$RUNNER_TEMP/whole-suite/', '"${RUNNER_TEMP}/whole-suite/'),
                "path: ${{ runner.temp }}/whole-suite",
                "path: ${{ env.RUNNER_TEMP }}/whole-suite",
            ),
            _SUITE_REFERENCE,
            False,
            id="correct: the temporary directory spelled three ways",
        ),
        pytest.param(
            _swap(_REFERENCE, "name: whole-suite-result-record", "name: python-package-distributions"),
            _SUITE_REFERENCE,
            True,
            id="wrong: the gate downloads another artifact than the suite keeps",
        ),
        pytest.param(
            _REFERENCE,
            _swap(
                _SUITE_REFERENCE, "name: whole-suite-result-record", "name: suite-record", text_name="suite reference"
            ),
            True,
            id="wrong: the suite renames its artifact",
        ),
        pytest.param(
            _swap(_REFERENCE, "path: ${{ runner.temp }}/whole-suite", "path: ${{ runner.temp }}/downloaded"),
            _SUITE_REFERENCE,
            True,
            id="wrong: the download lands in a directory the command does not read",
        ),
        pytest.param(
            _swap(_REFERENCE, '"$RUNNER_TEMP/whole-suite/result-record.json"', '"$RUNNER_TEMP/result-record.json"'),
            _SUITE_REFERENCE,
            True,
            id="wrong: the command reads the temporary directory itself and not the download",
        ),
        pytest.param(
            _swap(_REFERENCE, "whole-suite/result-record.json", "whole-suite/record.json"),
            _SUITE_REFERENCE,
            True,
            id="wrong: the command reads another file name than the suite keeps",
        ),
        pytest.param(
            _REFERENCE,
            _swap(_SUITE_REFERENCE, "result-record.json", "record.json", text_name="suite reference"),
            True,
            id="wrong: the suite keeps another file name than the command reads",
        ),
        pytest.param(
            _swap(
                _REFERENCE,
                "        continue-on-error: true\n        with:\n          name: whole-suite",
                "        with:\n          name: whole-suite",
            ),
            _SUITE_REFERENCE,
            True,
            id="wrong: the download does not continue on error",
        ),
        pytest.param(
            _moved(_REFERENCE, _DOWNLOAD_STEP, _BEFORE_BUILD),
            _SUITE_REFERENCE,
            True,
            id="wrong: the download after the refusal step",
        ),
    ],
)
def test_the_artifact_the_suite_keeps_is_the_record_the_gate_downloads_and_reads(publish, suite, refused):
    """Property: the artifact name, the download directory and the file name agree across the two workflows and the refusal command, and the download comes first and continues on error.

    Class: NEW CAPABILITY — the hand-off between the whole-suite workflow and the gate. The plausible
    wrong implementations are an artifact renamed in one file and not the other, a download
    directory the command does not read, a command that reads the runner's temporary directory and
    not the download, and a download ordered after the command. Each leaves the gate reading a path
    that never holds a record, which the script refuses as a missing record on every tag, so the
    release is blocked and no test says why. The workflow test beside this one pins the artifact
    name against a constant and reads the record path only as `\\S*result-record.json`, and the
    whole-suite tests pin the upload against that file alone, so each stays green; this test is red
    under each.

    Why: the gate and the suite are two files, and nothing but this rule holds the one name, one
    directory and one file name they share. A drift in either is invisible until a release is cut.

    Killed by: a rename of the artifact on either side, a download path or record path edited alone,
    either file name edited alone, a download that fails the job, or a download after the command.
    Second correct implementation: the runner's temporary directory spelled as `$RUNNER_TEMP`,
    `${RUNNER_TEMP}`, `${{ runner.temp }}` or `${{ env.RUNNER_TEMP }}`.

    Not established: that the runner places an artifact of one file at the root of the download
    directory, which is the behaviour of `actions/download-artifact` and is exercised only by the
    first release run.
    """
    problems = _seam_problems(_tree(_publish_text(publish)), _tree(_suite_text(suite)))
    assert bool(problems) is refused, problems


@pytest.mark.parametrize(
    ("case", "refused"),
    [
        pytest.param(None, False, id="the real workflow"),
        pytest.param(_REFERENCE, False, id="correct: the reference file"),
        pytest.param(
            _swap(
                _REFERENCE, "    needs: [release-gate, build]\n", "    needs:\n      - release-gate\n      - build\n"
            ),
            False,
            id="correct: needs as a block sequence",
        ),
        pytest.param(
            _swap(_REFERENCE, "    needs: [post-build-check]\n", "    needs: post-build-check\n"),
            False,
            id="correct: needs as one scalar",
        ),
        pytest.param(
            _REFERENCE
            + "\n  announce:\n    name: Announce\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo done\n",
            False,
            id="correct: a job that neither builds nor uploads and waits for nothing",
        ),
        pytest.param(
            _REFERENCE + "\n  publish-to-mirror:\n    needs: [post-build-check]\n    runs-on: ubuntu-latest\n"
            "    environment:\n      name: mirror\n    steps:\n      - run: echo up\n",
            False,
            id="correct: a further upload job behind the check job",
        ),
        pytest.param(
            _swap(_REFERENCE, _BUILD_NEEDS, "    name: Build distribution\n    needs: [whole-suite]\n"),
            True,
            id="wrong: the build waits for the suite and not the gate",
        ),
        pytest.param(
            _swap(_REFERENCE, "    needs: [post-build-check]\n", "    needs: [whole-suite]\n"),
            True,
            id="wrong: the first upload waits for the suite and not the gate",
        ),
        pytest.param(
            _REFERENCE + "\n  publish-to-mirror:\n    needs: [whole-suite]\n    runs-on: ubuntu-latest\n"
            "    environment:\n      name: mirror\n    steps:\n      - run: echo up\n",
            True,
            id="wrong: a further upload job that does not wait for the gate",
        ),
        pytest.param(
            _REFERENCE + "\n  mint:\n    runs-on: ubuntu-latest\n    permissions:\n      id-token: write\n"
            "    steps:\n      - run: echo mint\n",
            True,
            id="wrong: a job that can mint an upload credential and waits for nothing",
        ),
        pytest.param(
            _swap(
                _REFERENCE, "    needs: [whole-suite]\n    if: ${{ !cancelled() }}\n", "    if: ${{ !cancelled() }}\n"
            ),
            True,
            id="wrong: the gate waits for nothing, so it runs before the suite has written a record",
        ),
    ],
)
def test_every_job_that_builds_or_uploads_waits_for_the_gate_through_some_chain(case, refused):
    """Property: the gate waits for the suite job alone, and the build job and every job holding an environment or a token that can mint an upload credential reaches the gate through `needs`.

    Class: NEW CAPABILITY — the reachability of the gate, as a property and not a list of edges. The
    plausible wrong implementations are a job added later that uploads or mints a credential and
    waits for the build, the suite or nothing, which publishes past a refused tag; and a gate that
    waits for nothing and so reads a record the suite has not yet written. The workflow test beside
    this one pins six named jobs' edges and says it asserts nothing about other jobs, so a seventh
    job is invisible to it; this test is red on it.

    Why: nothing but the graph stops a job from running on a refused tag, and the graph is one
    file. The jobs that hold the means to publish are a property of the job, not of its name.

    Killed by: a building or uploading job outside the gate's descendants. Second correct
    implementation: a further upload behind the check job, `needs` written as a block sequence or a
    scalar, and a job that holds no environment and no token grant.

    Not established: a job that publishes through a token or an action other than the two named
    holds (an environment, `id-token: write`, `write-all`) is not found; a job that waits for the
    gate and has a condition is the softening rule's.
    """
    problems = _graph_problems(_tree(_publish_text(case)))
    assert bool(problems) is refused, problems


@pytest.mark.parametrize(
    ("case", "refused"),
    [
        pytest.param(None, False, id="the real workflow"),
        pytest.param(_REFERENCE, False, id="correct: the reference file"),
        pytest.param(
            _swap(
                _REFERENCE,
                "    uses: ./.github/workflows/whole-suite.yml\n",
                "    uses: ./.github/workflows/whole-suite.yml\n    permissions:\n      contents: read\n",
            ),
            False,
            id="correct: a read-only permission on the call",
        ),
        pytest.param(
            _swap(
                _REFERENCE,
                "    uses: ./.github/workflows/whole-suite.yml\n",
                "    uses: ./.github/workflows/whole-suite.yml\n    secrets: inherit\n",
            ),
            True,
            id="wrong: every secret handed to the suite run",
        ),
        pytest.param(
            _swap(
                _REFERENCE,
                "    uses: ./.github/workflows/whole-suite.yml\n",
                "    uses: ./.github/workflows/whole-suite.yml\n    with:\n      narrow: true\n",
            ),
            True,
            id="wrong: an input on the call",
        ),
        pytest.param(
            _swap(
                _REFERENCE,
                "    uses: ./.github/workflows/whole-suite.yml\n",
                "    uses: ./.github/workflows/whole-suite.yml\n    permissions:\n      id-token: write\n",
            ),
            True,
            id="wrong: a token that can mint an upload credential on the call",
        ),
        pytest.param(
            _swap(
                _REFERENCE,
                "    uses: ./.github/workflows/whole-suite.yml\n",
                "    uses: ./.github/workflows/whole-suite.yml\n    permissions: write-all\n",
            ),
            True,
            id="wrong: write-all on the call",
        ),
    ],
)
def test_the_call_to_the_suite_workflow_hands_it_no_secret_and_no_more_than_a_read(case, refused):
    """Property: the job that calls the whole-suite workflow carries a name, the called file, and at most `permissions: contents: read`.

    Class: NEW CAPABILITY — what the release run gives the suite run. The suite installs the tagged
    commit and runs its tests, so whatever the call passes is handed to that commit's code. The
    plausible wrong implementations are `secrets: inherit`, an input that narrows the run, and a
    permission wider than a read. The workflow test beside this one asserts the job has no `steps` and
    no `runs-on`, which is all a reusable-workflow call cannot carry, so each of these stays green;
    this test is red under each.

    Why: the called workflow can only keep or reduce the caller's token, and takes secrets only when
    the caller passes them; the one place that narrows what test code runs with is the call.

    Killed by: any key but a name, the called file and a read-only permission. Second correct
    implementation: the call with no permission, which takes the repository default, and the call with
    `contents: read` written out.

    Not established: the repository's default token permission, which the call inherits when it
    names none; the called file's own `permissions` is a limit and is held by its own test.
    """
    problems = _call_problems(_tree(_publish_text(case)))
    assert bool(problems) is refused, problems


# --- the command, run as the runner runs it ----------------------------------------------------


def _git(repo: Path, *arguments: str) -> str:
    """One git command in `repo`, its stdout returned and a failure raising with git's own message."""
    completed = subprocess.run(["git", *IDENTITY, *arguments], cwd=repo, capture_output=True, text=True, check=False)
    assert completed.returncode == 0, f"git {' '.join(arguments)}: {completed.stderr}"
    return completed.stdout.strip()


def _tag_checkout(tmp_path: Path) -> tuple[Path, str, str]:
    """A checkout the way a tag push leaves it: `origin/main` present, no local `main`, HEAD ahead of it.

    Returns the repository, the commit `origin/main` holds (a release), and the commit HEAD holds (a
    later one that `origin/main` does not). Both carry the gate script, the two version files and no
    legacy tree, so no ground but the one a scenario sets can refuse.
    """
    repo = tmp_path / "checkout"
    (repo / "scripts").mkdir(parents=True)
    _git(repo, "init", "--quiet", "-b", "trunk")
    shutil.copy(GATE_SCRIPT, repo / "scripts" / GATE_SCRIPT.name)
    (repo / "pyproject.toml").write_text('[project]\nname = "sample"\nversion = "1.2.3"\n', encoding="utf-8")
    (repo / "CITATION.cff").write_text('cff-version: 1.2.0\nversion: "1.2.3"\n', encoding="utf-8")
    _git(repo, "add", "--all")
    _git(repo, "commit", "--quiet", "-m", "the release")
    released = _git(repo, "rev-parse", "HEAD")
    _git(repo, "update-ref", "refs/remotes/origin/main", released)
    (repo / "later.txt").write_text("a commit main does not hold\n", encoding="utf-8")
    _git(repo, "add", "--all")
    _git(repo, "commit", "--quiet", "-m", "a later commit")
    return repo, released, _git(repo, "rev-parse", "HEAD")


_WHOLE_RECORD = (
    '{"verdict": "pass", "completeness": "complete", "exclusion": [], "invocation": {"collection_scope": null}}'
)
_FAILING_RECORD = (
    '{"verdict": "fail", "completeness": "complete", "exclusion": [], "invocation": {"collection_scope": null}}'
)


def _gate_run_problems(publish: str, suite: str, tmp_path: Path) -> list[str]:
    """Where the workflow's refusal command, run over a built checkout, disagrees with the script it runs.

    Five scenarios: a released commit with the matching tag and a whole record must conform; a missing
    record, a failing record, a commit `origin/main` does not hold and a tag that is not the version
    must each be refused on exactly the ground they set.
    """
    tree = _tree(publish)
    _, _, refusal = _gate_command(tree)
    assert refusal is not None, "the gate has no single step that runs the script"
    download = next(step for step in _gate_steps(tree) if _uses(step, "actions/download-artifact"))
    kept = next(
        step for job in _jobs(_tree(suite)).values() for step in _steps(job) if _uses(step, "actions/upload-artifact")
    )
    repo, released, later = _tag_checkout(tmp_path)
    runner_temp = tmp_path / "runner-temp"
    runner_temp.mkdir()
    shim = tmp_path / "bin"
    shim.mkdir()
    (shim / "python").symlink_to(sys.executable)
    (shim / "python3").symlink_to(sys.executable)
    landing = Path(_temp(str(download["with"]["path"])).replace("{temp}", str(runner_temp)))
    filename = _temp(str(kept["with"]["path"])).rpartition("/")[2]

    scenarios = (
        ("a released commit, its tag and a whole record", released, "v1.2.3", _WHOLE_RECORD, 0, set()),
        ("no record downloaded", released, "v1.2.3", None, 1, {"whole-suite-record"}),
        ("a failing record", released, "v1.2.3", _FAILING_RECORD, 1, {"whole-suite-record"}),
        ("a commit origin/main does not hold", later, "v1.2.3", _WHOLE_RECORD, 1, {"not-on-main"}),
        ("a tag that is not the version", released, "v9.9.9", _WHOLE_RECORD, 1, {"tag-version"}),
    )
    problems = []
    for label, sha, tag, record, status, grounds in scenarios:
        if landing.exists():
            shutil.rmtree(landing)
        if record is not None:
            landing.mkdir(parents=True)
            (landing / filename).write_text(record, encoding="utf-8")
        environment = {
            **os.environ,
            "PATH": f"{shim}{os.pathsep}{os.environ['PATH']}",
            "GITHUB_REF_NAME": tag,
            "GITHUB_SHA": sha,
            "RUNNER_TEMP": str(runner_temp),
            "PYTHONDONTWRITEBYTECODE": "1",
        }
        script = re.sub(r"\$\{\{\s*runner\.temp\s*\}\}", str(runner_temp), str(refusal["run"]))
        completed = subprocess.run(
            ["bash", "-e", "-c", script], cwd=repo, env=environment, capture_output=True, text=True, check=False
        )
        refused = {match.group("name") for match in _REFUSAL_LINE.finditer(completed.stdout)}
        if completed.returncode != status or refused != grounds:
            problems.append(
                f"{label}: exit {completed.returncode} with {sorted(refused)}, expected exit {status} with "
                f"{sorted(grounds)}; {completed.stdout.strip()!r} {completed.stderr.strip()!r}"
            )
    return problems


@pytest.mark.parametrize(
    ("publish", "suite", "refused"),
    [
        pytest.param(None, None, False, id="the real files"),
        pytest.param(_REFERENCE, _SUITE_REFERENCE, False, id="correct: the reference files"),
        pytest.param(
            _swap(
                _swap(_REFERENCE, '"$GITHUB_SHA"', '"${GITHUB_SHA}"'),
                '"$RUNNER_TEMP/whole-suite/result-record.json"',
                '"${{ runner.temp }}/whole-suite/result-record.json"',
            ),
            _SUITE_REFERENCE,
            False,
            id="correct: the braced variable and the context spelling of the temporary directory",
        ),
        pytest.param(
            _swap(_REFERENCE, _COMMAND_TAIL, _COMMAND_TAIL.rstrip("\n") + " || true\n"),
            _SUITE_REFERENCE,
            True,
            id="wrong: || true, which exits 0 on every refusal",
        ),
        pytest.param(
            _swap(_REFERENCE, "--main-ref origin/main", "--main-ref HEAD"),
            _SUITE_REFERENCE,
            True,
            id="wrong: the main reference HEAD, which holds every commit the checkout holds",
        ),
        pytest.param(
            _swap(_REFERENCE, "--main-ref origin/main", "--main-ref main"),
            _SUITE_REFERENCE,
            True,
            id="wrong: the main reference main, which a tag checkout does not have",
        ),
        pytest.param(
            _swap(_REFERENCE, '"$RUNNER_TEMP/whole-suite/result-record.json"', '"$RUNNER_TEMP/result-record.json"'),
            _SUITE_REFERENCE,
            True,
            id="wrong: the record read from outside the download directory",
        ),
        pytest.param(
            _swap(_REFERENCE, '--tag "$GITHUB_REF_NAME"', "--tag v1.2.3"),
            _SUITE_REFERENCE,
            True,
            id="wrong: a literal tag, which never refuses a tag that is not the version",
        ),
        pytest.param(
            _swap(_REFERENCE, '--commit "$GITHUB_SHA"', "--commit HEAD"),
            _SUITE_REFERENCE,
            True,
            id="wrong: the checked-out commit and not the tagged one",
        ),
    ],
)
def test_the_refusal_command_run_over_a_tag_checkout_refuses_what_the_script_refuses(publish, suite, refused, tmp_path):
    """Property: the workflow's refusal command, run by bash with the runner's variables over a checkout shaped as a tag push leaves it, exits 0 on a conforming release and exits 1 naming exactly the ground a scenario sets.

    Class: NEW CAPABILITY — the wiring of the script into the workflow, as behaviour. The plausible
    wrong implementations are a command that always exits 0 (`|| true`), a main reference that holds
    every commit the checkout holds (`HEAD`) or that does not resolve in a tag checkout (`main`), a
    record read from a directory the download does not write, a literal tag that never disagrees
    with the version, and the checked-out commit in place of the tagged one. Each of these has the
    four flags and a record path, so the text rules above would accept some of them as spellings;
    this test is red under each because it runs the line.

    Why: the script's own tests drive the script with arguments they choose. Nothing else runs the
    arguments the workflow chooses, so a wiring error between the two files is otherwise first seen
    on a real release tag.

    Killed by: any change to the command that alters the exit status or the set of grounds in any of
    the five scenarios. Second correct implementation: the braced spelling of a variable, and the
    temporary directory written as an expression.

    Not established: the runner's own checkout, `GITHUB_SHA` for an annotated tag, and the artifact
    download's layout; the fixture builds the state the workflow assumes and does not measure it.
    """
    problems = _gate_run_problems(_publish_text(publish), _suite_text(suite), tmp_path)
    assert bool(problems) is refused, problems
