"""Tests that `.github/workflows/whole-suite.yml` cannot quietly stop being a whole, recorded run.

The workflow is the toolkit's only run of the whole test tree, and the release workflow reads the
result record it keeps as the evidence that a tagged commit passes every test. The workflow test
beside this one pins the three triggers and the exact pytest line. A workflow can keep every one of
those lines and still run a narrower suite, keep no record, skip its own suite step, or grant itself
more than a read; this module covers that. Four rules, each over every line or step of the file:

1. no line but the one suite command and a package pin mentions pytest, so no other step, flag or
   environment variable can narrow the run;
2. the file the suite command writes is the file the upload step keeps, and the steps run in the
   order that makes the file exist before it is uploaded;
3. only the upload step carries a condition, and nothing continues on error;
4. the file's grants and call interface are read-only permissions, a call that takes no input, and a
   push that carries no path or branch filter.

Each rule is a function of the file's TEXT, so one function judges the real file, variants a correct
workflow may take, and variants a wrong one takes; a rule that accepts a wrong variant measures
nothing, which is what the refused cases check. The text is read and not parsed as YAML, for the
reason `tests/test_whole_suite_workflow.py` gives: the State check job installs no YAML library.
"""

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "whole-suite.yml"

# The one command the workflow runs. Copied, not imported from the workflow test, so that a rewrite
# of that test's reader cannot change what this module holds the workflow to.
SUITE_COMMAND = 'python -m pytest -p no:cacheprovider -ra --result-record "$RUNNER_TEMP/result-record.json"'
KEEPER_STEP = "Keep the result record"

# A comment that trails a value, as YAML reads it: whitespace, then `#`. Not established: a ` #`
# inside a quoted scalar is cut too, which the workflow does not contain.
_COMMENT_TAIL = re.compile(r"\s+#.*$")
# A requirement pin such as `"pytest==9.1.1"`: the one way a package line may name pytest.
_PYTEST_PIN = re.compile(r"""["']?pytest(?:==|>=|<=|~=|!=)[^\s"']*["']?""", re.IGNORECASE)
_PYTEST_NAME = re.compile(r"py\.?test", re.IGNORECASE)
# The keys that skip a job or step, or let it fail without failing the run.
_GATING_KEYS = ("if", "continue-on-error", "needs", "strategy")


def _lines(text: str) -> list[tuple[int, str]]:
    """Every non-blank, non-comment line as (indent, text), with a trailing ` # comment` removed."""
    lines = []
    for raw in text.splitlines():
        stripped = raw.strip()
        if stripped and not stripped.startswith("#"):
            lines.append((len(raw) - len(raw.lstrip()), _COMMENT_TAIL.sub("", stripped)))
    return lines


def _body(line: str) -> str:
    """The line without its list marker, so `- name: x` and `name: x` read alike."""
    return line.removeprefix("- ")


def _children(lines: list[tuple[int, str]], index: int) -> list[tuple[int, str]]:
    """The lines nested under `lines[index]`: every following line indented deeper than it."""
    nested = []
    for indent, line in lines[index + 1 :]:
        if indent <= lines[index][0]:
            break
        nested.append((indent, line))
    return nested


def _condition(value: str) -> str:
    """A condition with its `${{ }}` wrapper removed, so `always()` and `${{ always() }}` compare equal."""
    return re.sub(r"^\$\{\{\s*(.*?)\s*\}\}$", r"\1", value.strip())


def _stray_pytest_lines(text: str) -> list[str]:
    """Every line that mentions pytest in any spelling and is neither the suite command nor a package pin.

    A second step, a flag on another line, and an environment variable such as `PYTEST_ADDOPTS` all
    reach pytest through a line of this kind, which is why the rule is over lines and not over steps.
    """
    stray = []
    for _, line in _lines(text):
        body = _body(line)
        if body.removeprefix("run: ") == SUITE_COMMAND:
            continue
        if _PYTEST_NAME.search(_PYTEST_PIN.sub("", body)):
            stray.append(line)
    return stray


def _record_problems(text: str) -> list[str]:
    """Where the record the suite command writes and the file the upload step keeps part ways.

    Checks that the command's `--result-record` target and the upload step's `path` name one file
    once each spelling of the runner's temporary directory is normalised, and that the install,
    suite and upload steps appear in that order.
    """
    bodies = [_body(line) for _, line in _lines(text)]
    suite = [i for i, body in enumerate(bodies) if body.removeprefix("run: ") == SUITE_COMMAND]
    install = [i for i, body in enumerate(bodies) if body.startswith("run: ") and "pip install" in body]
    upload = [i for i, body in enumerate(bodies) if body.startswith("uses: actions/upload-artifact@")]
    paths = [body.removeprefix("path: ") for body in bodies if body.startswith("path: ")]
    if not (len(suite) == len(upload) == len(paths) == 1 and install):
        return [
            f"expected one suite command, one upload step, one path and an install line: {suite} {upload} {paths} {install}"
        ]
    problems = []
    target = re.search(r"--result-record\s+(\S+)", bodies[suite[0]])
    assert target is not None, bodies[suite[0]]
    written = re.sub(r"\$\{?RUNNER_TEMP\}?", "{temp}", target.group(1).strip("\"'"))
    kept = re.sub(r"\$\{\{\s*(?:runner\.temp|env\.RUNNER_TEMP)\s*\}\}", "{temp}", paths[0].strip("\"'"))
    if written != kept:
        problems.append(f"the command writes {written!r} and the upload step keeps {kept!r}")
    if not install[0] < suite[0] < upload[0]:
        problems.append(f"steps are out of order: install {install[0]}, suite {suite[0]}, upload {upload[0]}")
    return problems


def _gating_problems(text: str) -> list[str]:
    """Every condition or continue-on-error that is not the upload step's `if: always()`."""
    problems = []
    step = None
    for _, line in _lines(text):
        body = _body(line)
        if line.startswith("- "):
            step = body
        key, _, value = body.partition(":")
        if key not in _GATING_KEYS:
            continue
        if key == "if" and _condition(value) == "always()" and step == f"name: {KEEPER_STEP}":
            continue
        problems.append(f"{body} on {step!r}")
    return problems


def _surface_problems(text: str) -> list[str]:
    """Where the file grants more than a read, takes an input on its call, or filters its push."""
    lines = _lines(text)
    problems = []
    blocks = [i for i, (_, line) in enumerate(lines) if line.startswith("permissions:")]
    for i in blocks:
        inline = lines[i][1].partition(":")[2].strip()
        nested = [line for _, line in _children(lines, i)]
        if inline or nested != ["contents: read"]:
            problems.append(f"permissions grant {inline or nested}, not `contents: read`")
    if not blocks:
        problems.append("no permissions block, so the token keeps the repository default")
    for i, (_, line) in enumerate(lines):
        if line.startswith("workflow_call:") and (line.partition(":")[2].strip() or _children(lines, i)):
            problems.append("the call takes input or secrets, which the release workflow does not pass")
        if line == "push:":
            nested = _children(lines, i)
            keys = {
                text.partition(":")[0] for indent, text in nested if indent == nested[0][0] and not text.startswith("-")
            }
            if keys != {"branches", "tags"}:
                problems.append(f"push carries {sorted(keys)}, not exactly branches and tags")
    return problems


_HEAD = """\
name: Whole suite

on:
  push:
    branches: [ "**" ]
    tags: [ "**", "!v*" ]
  workflow_call:

permissions:
  contents: read

jobs:
  whole-suite:
    runs-on: ubuntu-latest
    steps:
"""
_STEPS = {
    "checkout": "      - uses: actions/checkout@v4\n",
    "python": "      - name: Set up Python\n        uses: actions/setup-python@v5\n        with:\n          python-version: '3.11'\n",
    "install": (
        "      - name: Install the toolkit and the test runner\n"
        '        run: python -m pip install --disable-pip-version-check -e . "pytest==9.1.1" matplotlib\n'
    ),
    "suite": f"      - name: Run the whole suite\n        run: {SUITE_COMMAND}\n",
    "keep": (
        f"      - name: {KEEPER_STEP}\n"
        "        if: always()\n"
        "        uses: actions/upload-artifact@v5\n"
        "        with:\n"
        "          name: whole-suite-result-record\n"
        "          path: ${{ runner.temp }}/result-record.json\n"
        "          if-no-files-found: warn\n"
    ),
}
_ORDER = ("checkout", "python", "install", "suite", "keep")


def _build(head: str = _HEAD, order: tuple[str, ...] = _ORDER, **steps: str) -> str:
    """A workflow assembled from the head and the named steps; a keyword replaces or adds a step by name."""
    pieces = {**_STEPS, **steps}
    return head + "".join(pieces[name] for name in order)


def _swap(text: str, old: str, new: str) -> str:
    """`text` with the one occurrence of `old` replaced; an anchor that is absent or repeated is a bug in the case."""
    assert text.count(old) == 1, f"the anchor {old!r} must occur once"
    return text.replace(old, new)


def _text(case: str | None) -> str:
    """The real workflow for the sentinel `None`, and the case's own text otherwise."""
    return WORKFLOW.read_text(encoding="utf-8") if case is None else case


_NARROW_STEP = "      - name: Narrow\n        run: |\n          python -m pytest -k nothing\n"


@pytest.mark.parametrize(
    ("case", "refused"),
    [
        pytest.param(None, False, id="the real workflow"),
        pytest.param(_build(), False, id="correct: the shape's own file"),
        pytest.param(
            _swap(_build(), "matplotlib\n", "matplotlib joblib  # one more distribution\n"),
            False,
            id="correct: the install line names another distribution",
        ),
        pytest.param(
            "# the suite runs under pytest, in the whole\n" + _build(),
            False,
            id="correct: a comment names pytest",
        ),
        pytest.param(
            _build(order=(*_ORDER[:4], "narrow", "keep"), narrow=_NARROW_STEP),
            True,
            id="wrong: a second step as a block scalar",
        ),
        pytest.param(
            _build(
                order=(*_ORDER[:4], "narrow", "keep"), narrow="      - name: Narrow\n        run: pytest -k nothing\n"
            ),
            True,
            id="wrong: a second step that is `pytest -k`",
        ),
        pytest.param(
            _build(
                head=_swap(
                    _HEAD,
                    "    runs-on: ubuntu-latest\n",
                    '    runs-on: ubuntu-latest\n    env:\n      PYTEST_ADDOPTS: "--ignore=tests/equivalence"\n',
                )
            ),
            True,
            id="wrong: PYTEST_ADDOPTS in the job's environment",
        ),
        pytest.param(
            _build(
                install=_swap(
                    _STEPS["install"], "matplotlib\n", "matplotlib && python -m pytest --ignore=tests/equivalence\n"
                )
            ),
            True,
            id="wrong: a second pytest run on the install line",
        ),
    ],
)
def test_no_line_but_the_suite_command_and_a_package_pin_mentions_pytest(case, refused):
    """Property: every line of the workflow that mentions pytest is the suite command or a pin of pytest itself.

    Class: NEW CAPABILITY — the lines of the workflow, which any narrowing of the run must reach
    pytest through. The plausible wrong implementations are a second step written as a block
    scalar (`run: |` over `python -m pytest -k …`), a step that runs `pytest -k …` with no `-m`, and
    `PYTEST_ADDOPTS` set in an `env:` block. The workflow test beside this one finds a second step
    only by the text `-m pytest` in a one-line `run:` value, reads a block scalar's value as `|`, and
    reads no `env:` block, so each of the three leaves it green over a run narrower than the whole
    tree; this test is red under each.

    Why: the release workflow reads the record as the proof of a whole run, and the record cannot see
    a narrowing made before collection (`--ignore`), which is exactly what an option in
    `PYTEST_ADDOPTS` or a second run produces. The suite command's equality is the only cover, and it
    covers the one `run:` line and not the other places pytest can be steered from.

    Killed by: a second step or env entry naming pytest, or a second pytest run on any line, whatever
    spelling it takes. Second correct implementation: the same workflow with the install line naming
    another distribution, with a comment that mentions pytest, or with a trailing comment on a line,
    none of which mentions pytest in a line the rule reads.

    Not established: a narrowing made outside the workflow file, in `pyproject.toml` or a conftest,
    is not reached by this rule; a plugin distribution named `pytest-…` on the install line is
    refused as a stray mention, which a later correct line may need to relax.
    """
    assert bool(_stray_pytest_lines(_text(case))) is refused, _stray_pytest_lines(_text(case))


@pytest.mark.parametrize(
    ("case", "refused"),
    [
        pytest.param(None, False, id="the real workflow"),
        pytest.param(_build(), False, id="correct: the shape's own file"),
        pytest.param(
            _build(keep=_swap(_STEPS["keep"], "${{ runner.temp }}", "${{ env.RUNNER_TEMP }}")),
            False,
            id="correct: the upload path spelled through env",
        ),
        pytest.param(
            _build(keep=_swap(_STEPS["keep"], "${{ runner.temp }}/result-record.json", "result-record.json")),
            True,
            id="wrong: the upload path is relative",
        ),
        pytest.param(
            _build(keep=_swap(_STEPS["keep"], "${{ runner.temp }}", "$RUNNER_TEMP")),
            True,
            id="wrong: the upload path uses the shell spelling",
        ),
        pytest.param(
            _build(order=("checkout", "python", "install", "keep", "suite")),
            True,
            id="wrong: the upload step runs before the suite step",
        ),
        pytest.param(
            _build(order=("checkout", "python", "suite", "install", "keep")),
            True,
            id="wrong: the install step runs after the suite step",
        ),
    ],
)
def test_the_file_the_suite_writes_is_the_file_the_upload_keeps_and_it_exists_first(case, refused):
    """Property: the `--result-record` target and the upload `path` are one file, and install, suite and upload run in that order.

    Class: NEW CAPABILITY — the workflow's steps. The plausible wrong implementations are an upload
    `path` that is relative (`result-record.json`), an upload `path` written with the shell spelling
    `$RUNNER_TEMP/…` (a `with:` value is not passed through a shell), and an upload step listed before
    the suite step. Each leaves the upload step finding no file; because the step sets
    `if-no-files-found: warn`, the job stays green, no record reaches the release workflow, and the
    workflow test beside this one, which reads the upload step's `name` and `if` and not its `path`
    or its position, stays green too. This test is red under each.

    Why: a green run with no kept record is the failure the record exists to remove; the gate then
    refuses with "could not read" and nothing says the cause is the workflow.

    Killed by: changing the upload path to any other file, or reordering the install, suite and
    upload steps. Second correct implementation: the same workflow with the upload path written
    `${{ env.RUNNER_TEMP }}/result-record.json`, and with a checkout and a Python step in either
    order before the install step.

    Not established: that the runner expands `${{ runner.temp }}` and `$RUNNER_TEMP` to one
    directory, and that a `with:` value is not shell-expanded, are GitHub's documented behaviour and
    were not run here.
    """
    assert bool(_record_problems(_text(case))) is refused, _record_problems(_text(case))


_SKIP_SUITE = _swap(_STEPS["suite"], "        run:", "        if: false\n        run:")
_SOFT_SUITE = _swap(_STEPS["suite"], "        run:", "        continue-on-error: true\n        run:")


@pytest.mark.parametrize(
    ("case", "refused"),
    [
        pytest.param(None, False, id="the real workflow"),
        pytest.param(_build(), False, id="correct: the shape's own file"),
        pytest.param(
            _build(keep=_swap(_STEPS["keep"], "if: always()", "if: ${{ always() }}")),
            False,
            id="correct: the upload condition written with the expression wrapper",
        ),
        pytest.param(
            _swap(_build(), "    runs-on: ubuntu-latest\n", "    runs-on: ubuntu-latest\n    timeout-minutes: 120\n"),
            False,
            id="correct: a job timeout",
        ),
        pytest.param(_build(suite=_SKIP_SUITE), True, id="wrong: the suite step is `if: false`"),
        pytest.param(_build(suite=_SOFT_SUITE), True, id="wrong: the suite step continues on error"),
        pytest.param(
            _swap(
                _build(),
                "    runs-on: ubuntu-latest\n",
                "    runs-on: ubuntu-latest\n    if: github.ref == 'refs/heads/main'\n",
            ),
            True,
            id="wrong: the job runs only on main",
        ),
        pytest.param(
            _swap(_build(), "    runs-on: ubuntu-latest\n", "    runs-on: ubuntu-latest\n    needs: [lint]\n"),
            True,
            id="wrong: the job waits on another job",
        ),
    ],
)
def test_only_the_upload_step_carries_a_condition_and_nothing_continues_on_error(case, refused):
    """Property: every `if`, `continue-on-error`, `needs` and `strategy` key in the workflow is the upload step's `if: always()`.

    Class: NEW CAPABILITY — the keys of the job and of its steps. The plausible wrong implementations
    are `if: false` or `continue-on-error: true` on the suite step, and a job-level `if` or `needs`.
    The first leaves the suite unrun while the job reports success, the second reports a red suite as
    a green job, the third and fourth make the run depend on a branch or on another job. The
    workflow test beside this one reads only the suite step's `run` and the upload step's `name`,
    `uses`, `if` and `with`, so each stays green; this test is red under each.

    Why: the run exists to be whole and to report truthfully, and a green job over an unrun or
    forgiven suite is a report the branch's reader will believe.

    Killed by: adding any of the four keys anywhere except the upload step's `always()` condition.
    Second correct implementation: the same workflow with the upload condition written
    `if: ${{ always() }}`, and with a job `timeout-minutes`, which fails a run loudly and skips
    nothing.

    Not established: `concurrency` and `timeout-minutes` can also end a run early and are not in the
    four; GitHub's keys that skip or soften a step are these four.
    """
    assert bool(_gating_problems(_text(case))) is refused, _gating_problems(_text(case))


@pytest.mark.parametrize(
    ("case", "refused"),
    [
        pytest.param(None, False, id="the real workflow"),
        pytest.param(_build(), False, id="correct: the shape's own file"),
        pytest.param(
            _swap(
                _swap(_build(), "permissions:\n  contents: read\n\n", ""),
                "    runs-on: ubuntu-latest\n",
                "    runs-on: ubuntu-latest\n    permissions:\n      contents: read\n",
            ),
            False,
            id="correct: the read grant on the job instead of the workflow",
        ),
        pytest.param(
            _swap(
                _build(),
                '    branches: [ "**" ]\n    tags: [ "**", "!v*" ]\n',
                '    branches:\n      - "**"\n    tags:\n      - "**"\n      - "!v*"\n',
            ),
            False,
            id="correct: the triggers as block sequences",
        ),
        pytest.param(
            _swap(_build(), "permissions:\n  contents: read\n", "permissions: write-all\n"), True, id="wrong: write-all"
        ),
        pytest.param(_swap(_build(), "  contents: read\n", "  contents: write\n"), True, id="wrong: contents write"),
        pytest.param(_swap(_build(), "permissions:\n  contents: read\n\n", ""), True, id="wrong: no permissions block"),
        pytest.param(
            _swap(
                _build(),
                "  workflow_call:\n",
                "  workflow_call:\n    inputs:\n      ref:\n        required: true\n        type: string\n",
            ),
            True,
            id="wrong: the call requires an input",
        ),
        pytest.param(
            _swap(
                _build(), '    tags: [ "**", "!v*" ]\n', '    tags: [ "**", "!v*" ]\n    paths-ignore: [ "docs/**" ]\n'
            ),
            True,
            id="wrong: the push ignores paths",
        ),
        pytest.param(
            _swap(_build(), '    branches: [ "**" ]\n', '    branches: [ "**" ]\n    branches-ignore: [ "develop" ]\n'),
            True,
            id="wrong: the push ignores a branch",
        ),
    ],
)
def test_the_workflow_grants_a_read_takes_no_input_on_its_call_and_filters_no_push(case, refused):
    """Property: the only permission is `contents: read`, the call carries no input, and the push carries only `branches` and `tags`.

    Class: NEW CAPABILITY — the keys of the file's `permissions`, `workflow_call` and `push` blocks.
    The plausible wrong implementations are `permissions: write-all` or `contents: write`, a
    `workflow_call` that declares a required input (the release workflow calls it with none, so its
    own run stops at parse time), and a `push` with `paths-ignore` or `branches-ignore`, which makes
    the run skip pushes the workflow promises to run on. The workflow test beside this one reads the
    values of `branches` and `tags` and the set of top-level trigger names, and reads neither
    `permissions` nor the body of `workflow_call` nor any other key of `push`, so each stays green;
    this test is red under each.

    Why: the run executes the repository's own test code under a token, which should hold a read and
    nothing more, and the call interface and the push filter decide whether the run happens at all.

    Killed by: any `permissions` block other than `contents: read`, any child under `workflow_call`,
    or any `push` key other than `branches` and `tags`. Second correct implementation: the same
    workflow with the read grant on the job in place of the workflow, and with the two trigger lists
    written as block sequences.

    Not established: that a required input on `workflow_call` stops a caller that passes none is
    GitHub's documented behaviour and was not run here.
    """
    assert bool(_surface_problems(_text(case))) is refused, _surface_problems(_text(case))
