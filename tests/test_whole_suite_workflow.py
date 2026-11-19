"""Tests that the whole-suite job keeps its promises: run everything, run it once, and keep the record.

`.github/workflows/whole-suite.yml` is the toolkit's only run of the whole test tree. It
runs on every branch push, on every tag push that is not a release tag, and when the
release workflow calls it at a tagged commit. Its value is that the run is WHOLE: a run
narrowed by a marker, a path, an `--ignore` or a `-k` expression is a pass over a subset
that reads exactly like a pass over everything, and the release gate reads this job's
result record as the evidence that the tagged commit passes the whole suite.

Seven rules follow from that, and five of them are rules over the file rather than over one
line of it, because the file has more places to reach pytest from than the one `run:` value
the command equality pins: a second step, a block scalar's body, an `env:` entry, an upload
that keeps a different file, a condition that skips the suite, and a grant wider than a
read. The two line-level rules pin the two commands: the suite command itself, and the
install command, which installs the environment declared once in `pyproject.toml`'s `test`
dependency group rather than listing distributions of its own. Each rule is a function of the
parsed file, so one function judges the real file and a variant a later edit may introduce.

The file is read as text rather than parsed as YAML, for the reason
`tests/correspondence/test_divergence_mounts.py` gives: the State check job installs the
toolkit and `pytest` and no YAML library, so a module-level `import yaml` in a new test
module would stop that job at collection. The reader below is the smallest thing that
answers those questions, and it normalises the flow-sequence and block-sequence spellings
of a trigger list, a trailing `# comment` after a value, a `{}` inline mapping and a `|`
block scalar, so that each of those spellings reads the same as the plain one.
"""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "whole-suite.yml"

# The one command the release run executes. It is asserted by equality rather than by the
# absence of `-m`, `-k` and a path, because `--ignore`, `--deselect`, `--lf` and `-x` each
# narrow a run that a negative test would pass.
SUITE_COMMAND = 'python -m pytest -p no:cacheprovider -ra --result-record "$RUNNER_TEMP/result-record.json"'
# The one command the job installs with. The environment the stored reference values were computed in
# is declared once, in `pyproject.toml`'s `test` dependency group, and this line installs that group
# rather than listing distributions of its own: a second list is a second declaration, and it is what
# put `pandas 3.1.1` under 103 stores that record `pandas 2.3.3`.
#
# The runtime stack comes from the committed lock ahead of both, under `--require-hashes` and with
# `--no-deps` on the project: the versions this job tests are then the bytes the release compares
# and ships, and a source distribution that no longer builds resolves here rather than at a user's
# install. It is one line because a block scalar is a spelling the whole-suite test module cannot
# read the command out of, and a second line would be a second declaration.
INSTALL_COMMAND = (
    'python -m pip install --disable-pip-version-check "pip>=25.1" '
    "&& python -m pip install --disable-pip-version-check --require-hashes -r requirements.txt "
    "&& python -m pip install --disable-pip-version-check --no-deps -e . "
    "&& python -m pip install --disable-pip-version-check --group test"
)
INSTALL_STEP = "Install the toolkit and the test runner"
ARTIFACT_NAME = "whole-suite-result-record"
SUITE_STEP = "Run the whole suite"
KEEPER_STEP = "Keep the result record"

_KEY_VALUE = re.compile(r"^([A-Za-z_][A-Za-z0-9_-]*):(?:\s+(.*))?$")
_BLOCK_SCALAR = re.compile(r"^[|>][+-]?$")
# A requirement pin such as `"pytest==9.1.1"`: the one spelling by which a line may name pytest,
# because a pin installs the runner and steers nothing.
_PYTEST_PIN = re.compile(r"""["']?pytest(?:==|>=|<=|~=|!=)[^\s"']*["']?""", re.IGNORECASE)
# Every spelling of the runner's temporary directory the file may use, mapped onto one token so
# that the command's target and the upload's `path` compare as one path.
_TEMP_DIR = re.compile(r"\$\{\{\s*(?:runner\.temp|env\.RUNNER_TEMP)\s*\}\}|\$\{RUNNER_TEMP\}|\$RUNNER_TEMP\b")
_RESULT_TARGET = re.compile(r"--result-record\s+(\S+)")
# The keys that skip a job or a step, or let a red one report as a green run.
_GATING_KEYS = ("if", "continue-on-error", "needs", "strategy")


def _strip_comment(text: str) -> str:
    """`text` up to a `#` that starts a comment, per YAML's rule: at the start, or after whitespace, outside quotes.

    Without this, a compliant line such as `tags: [ "**" ]  # every branch` reads as a list whose
    last item is `**" ]  # every branch`, and the equality assertions below red on a correct file.
    A `#` inside a quoted scalar is not reached, which is the case this reader must get right for
    `"pytest==9.1.1"` and for no other value the file carries.
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

    Comments and blank lines never belong to a block, so a comment written between two keys does
    not end the mapping it sits inside.
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
    """An inline value: `[a, b]` becomes a list, `{}` an empty mapping, and anything else a scalar."""
    value = _strip_comment(text.strip())
    if value.startswith("[") and value.endswith("]"):
        return [_scalar(item) for item in value[1:-1].split(",") if item.strip()]
    if value == "{}":
        return {}
    return _scalar(value)


def _block(lines: list[tuple[int, str]], index: int) -> tuple[object, int]:
    """The block of entries starting at `lines[index]`, and the index one past it.

    The block's own indent is its first line's; every entry at that indent belongs to it, and a
    deeper entry belongs to the entry above it. A block whose first line starts with `-` is a list,
    which is how the two trigger lists and the step list are read.
    """
    if lines[index][1].startswith("-"):
        return _list_block(lines, index)
    return _mapping_block(lines, index)


def _block_scalar(lines: list[tuple[int, str]], index: int, indent: int, marker: str) -> tuple[str, int]:
    """The body of a `run: |` or `run: >` block scalar, and the index one past it.

    A block scalar's content sits on the following lines and not in the value, so a reader that
    stops at the `|` cannot see the command a step runs at all — which is how a second pytest step
    written as `run: |` stayed invisible to this file's first version while it narrowed the run.
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
    """The workflow's one job, asserted to carry the id the release workflow calls."""
    jobs = (workflow or _workflow())["jobs"]
    assert isinstance(jobs, dict), jobs
    assert list(jobs) == ["whole-suite"], list(jobs)
    job = jobs["whole-suite"]
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


def _mentions_text(node: object, needle: str, where: str = "$") -> list[tuple[str, str]]:
    """Every key and every scalar value of the parsed file that contains `needle`, as (location, text)."""
    found: list[tuple[str, str]] = []
    if isinstance(node, dict):
        for key, value in node.items():
            location = f"{where}.{key}"
            if needle in key.lower():
                found.append((location, key))
            found.extend(_mentions_text(value, needle, location))
    elif isinstance(node, list):
        for index, item in enumerate(node):
            found.extend(_mentions_text(item, needle, f"{where}[{index}]"))
    elif isinstance(node, str) and needle in node.lower():
        found.append((where, node))
    return found


def _gating_entries(tree: object, where: str = "$") -> list[tuple[str, str]]:
    """Every gating key of the parsed file and its value, as (location, value), at any depth.

    The walk descends into lists as well as mappings, because the step list is where three of the
    four keys bite: a walker that recurses only into mappings reports an empty set over a file whose
    suite step is `if: false`, and an empty set is exactly what a clean file reports.
    """
    found: list[tuple[str, str]] = []
    if isinstance(tree, dict):
        for key, value in tree.items():
            location = f"{where}.{key}"
            if key in _GATING_KEYS:
                found.append((location, str(value)))
            found.extend(_gating_entries(value, location))
    elif isinstance(tree, list):
        for index, item in enumerate(tree):
            found.extend(_gating_entries(item, f"{where}[{index}]"))
    return found


def _condition(value: str) -> str:
    """A condition with its `${{ }}` wrapper removed, so `always()` and `${{ always() }}` compare equal."""
    return re.sub(r"^\$\{\{\s*(.*?)\s*\}\}$", r"\1", value.strip())


def test_the_workflow_runs_on_every_branch_push_every_other_tag_push_and_a_call():
    """Property: the workflow's triggers are exactly a branch push, a non-release tag push, and a call.

    Class: NEW CAPABILITY — plausible wrong implementation is the same file with
    `branches: [ "main" ]`, which leaves every feature branch and `develop` unruns, or with
    the `!v*` entry dropped from `tags`, which makes a release tag start a standalone run
    that races the release workflow's own call at the same commit. The test is red under
    each; the output of both runs is in the round record.

    Why: a suite that runs on one branch is not the evidence the release workflow reads, and
    a release tag that starts two runs of the same commit makes the record the gate reads a
    coin toss. One trigger set, three members, one run per push.

    Killed by: dropping any of the three triggers, adding a fourth, or narrowing `branches`
    to a named branch. Second correct implementation: the same three triggers with
    `branches` and `tags` written as block sequences (`branches:` and then `- "**"`) instead
    of flow sequences, and the keys in another order, which this reader normalises.
    """
    on = _workflow()["on"]
    assert isinstance(on, dict), on
    assert set(on) == {"push", "workflow_call"}, sorted(on)
    push = on["push"]
    assert isinstance(push, dict), push
    assert push["branches"] == ["**"], push["branches"]
    assert push["tags"] == ["**", "!v*"], push["tags"]


def test_the_suite_is_one_step_running_the_named_command_with_no_narrowing():
    """Property: exactly one step runs pytest, it is the named one, and its command is the fixed one.

    Class: NEW CAPABILITY — plausible wrong implementation is the same step running
    `python -m pytest -m "state_check" -p no:cacheprovider -ra --result-record …`, which
    passes over the subset the State check job already covers, or the same command with no
    `--result-record`, which leaves the artifact step nothing to keep. The test is red under
    each; the output of both runs is in the round record.

    Why: a second pytest step can add a narrowing the first step's command does not carry,
    and the record cannot see a run narrowed before collection. Equality on the command plus
    "no other step runs pytest" is the pair that closes both.

    Killed by: any edit to the command, a second pytest step under another name, renaming
    the step, or dropping the artifact step. Second correct implementation: the same five
    steps with their keys in another order, a comment line inside a step, and the install
    line replaced by another set of distributions, none of which this test reads.
    """
    steps = _steps()
    pytest_steps = [step for step in steps if "-m pytest" in str(step.get("run", ""))]
    assert [step.get("name") for step in pytest_steps] == [SUITE_STEP], [step.get("name") for step in steps]
    assert pytest_steps[0]["run"] == SUITE_COMMAND, pytest_steps[0]["run"]

    keeper = _step_named(KEEPER_STEP)
    assert keeper["uses"] == "actions/upload-artifact@v5", keeper
    assert keeper["if"] == "always()", keeper
    with_block = keeper["with"]
    assert isinstance(with_block, dict), with_block
    assert with_block["name"] == ARTIFACT_NAME, with_block


def test_the_install_step_installs_the_one_declared_environment():
    """Property: the install step's `run:` value is the one command that installs the `test` group, by equality.

    Class: NEW CAPABILITY — the install step's `run:` value. Plausible wrong implementations are
    the line as it stood at `20825f1`, which listed the distributions itself
    (`"pytest==9.1.1" matplotlib seaborn scipy …`), and that line with `--group test` dropped,
    which installs the project and no environment for its tests. The first is why the suite ran
    `pandas 3.1.1` under 103 stored values recording `pandas 2.3.3` and reported eleven
    environment failures as failures of the code under test; the second is the same defect one
    edit later. Both are red here.

    Why: the environment those values were computed in is declared once, in `pyproject.toml`'s
    `test` dependency group, and a job that lists distributions of its own is a second declaration
    that drifts from the first while every other test stays green. Equality on the whole command is
    what holds the job to the one declaration; `tests/test_test_environment.py` holds the group's
    pins to what the stores record, and holds the State check job's install line equal to this one.

    Killed by: any edit to the install command — another distribution on it, a dropped upgrade of
    pip, a dropped `--group test`, or the two pip calls split over two `run:` keys. Second correct
    implementation: none. The equality is the rule, because a second spelling of the command is a
    second environment, and the point of the pin is that there is one.

    Not established: that `pip==25.1` is the first pip that reads `--group` was measured by the
    testing work (`pip==25.0.1` refuses it), and nothing here runs pip; the live proof is the run
    of this job (RC-3).
    """
    assert _step_named(INSTALL_STEP)["run"] == INSTALL_COMMAND


def test_no_key_or_value_but_the_command_and_a_package_pin_mentions_pytest():
    """Property: every mention of pytest in the file is the suite command itself or a pin of pytest.

    Class: NEW CAPABILITY — the parsed file's keys and scalar values. Plausible wrong
    implementations are a step running `pytest -k nothing` with no `-m`,
    `PYTEST_ADDOPTS: "--ignore=tests/equivalence"` in a job `env:`, and a second step whose
    `run:` is a `|` block scalar over `python -m pytest -k nothing`. The first two leave the
    command equality above green because they name pytest in a line that equality never
    reads; the third is caught here as well as by that equality, and only once this file's
    reader follows a block scalar to its body — a reader that stops at the `|` reads the
    step's command as `|`, which was this file's state at `f7690c2` (pasted in the round
    record). Run against a file carrying all three, this test reports all three locations.

    Why: the release workflow reads the record as proof of a whole run, and a record cannot
    see a narrowing made before collection — which is exactly what an `--ignore` in
    `PYTEST_ADDOPTS`, or a second run, produces.

    Killed by: any key or scalar value other than the command and a pin that names pytest,
    whatever spelling it takes. Second correct implementation: the same file with the install
    line naming another distribution, a `#` comment that mentions pytest, or a trailing
    comment after a value, none of which this rule reads as a mention.

    Not established: a narrowing made outside this file, in `pyproject.toml` or a conftest,
    is not reached here; and a plugin distribution whose name is `pytest-…` on the install
    line would be refused as a stray mention.
    """
    stray = [(where, text) for where, text in _mentions_text(_workflow(), "pytest") if text != SUITE_COMMAND]
    remaining = [(where, text) for where, text in stray if _PYTEST_PIN.search(text) is None]
    assert remaining == [], remaining


def test_the_record_the_suite_writes_is_the_file_the_upload_keeps_and_it_exists_first():
    """Property: the command's `--result-record` target is the upload step's `path`, and install, suite and upload run in that order.

    Class: NEW CAPABILITY — the file's steps. Plausible wrong implementations are an upload
    `path` that is relative (`result-record.json`), a `path` written with the shell spelling
    `$RUNNER_TEMP/…` (a `with:` value is not shell-expanded), and an upload step listed before
    the suite step. Each leaves the upload finding no file while the job stays green, because
    the step sets `if-no-files-found: warn`; this test is red under each.

    Why: a green run with no kept record is the failure the record exists to remove, and the
    gate then refuses with "could not read" while nothing says the workflow caused it.

    Killed by: changing the upload path to any other file, or reordering the install, suite
    and upload steps. Second correct implementation: the upload path written
    `${{ env.RUNNER_TEMP }}/result-record.json`, and a checkout and a Python step in either
    order before the install step.

    Not established: that the runner expands `${{ runner.temp }}` and `$RUNNER_TEMP` to one
    directory, and that a `with:` value is not shell-expanded, are GitHub's documented
    behaviour and were not run here.
    """
    steps = _steps()
    names = [step.get("name") for step in steps]
    suite = _step_named(SUITE_STEP)
    keeper = _step_named(KEEPER_STEP)
    with_block = keeper["with"]
    assert isinstance(with_block, dict), with_block
    target = _RESULT_TARGET.search(str(suite["run"]))
    assert target is not None, suite["run"]
    written = _TEMP_DIR.sub("{temp}", target.group(1).strip("\"'"))
    kept = _TEMP_DIR.sub("{temp}", str(with_block["path"]).strip("\"'"))
    assert written == kept, (written, kept)
    install = [index for index, step in enumerate(steps) if "pip install" in str(step.get("run", ""))]
    assert install and install[0] < names.index(SUITE_STEP) < names.index(KEEPER_STEP), names


def test_only_the_upload_step_carries_a_gating_key_and_nothing_continues_on_error():
    """Property: every `if`, `continue-on-error`, `needs` and `strategy` key in the file is the upload step's `if: always()`.

    Class: NEW CAPABILITY — the gating keys at the workflow, job and step level. Plausible
    wrong implementations are `if: false` or `continue-on-error: true` on the suite step, and
    a job-level `if` or `needs`. The first leaves the suite unrun while the job reports
    success, the second reports a red suite as a green job, and the last two make the run
    depend on a branch or on another job; this test is red under each.

    Why: the run exists to be whole and to report truthfully, and a green job over an unrun
    or forgiven suite is a report the branch's reader will believe.

    Killed by: any of the four keys anywhere except the upload step's `always()` condition.
    Second correct implementation: the upload condition written `if: ${{ always() }}`, and a
    job `timeout-minutes`, which fails a run loudly and skips nothing.

    Not established: `concurrency` and `timeout-minutes` can also end a run early and are not
    among the four keys; GitHub's keys that skip or soften are these four.
    """
    entries = _gating_entries(_workflow())
    assert [where for where, _ in entries] == ["$.jobs.whole-suite.steps[4].if"], entries
    assert _condition(entries[0][1]) == "always()", entries


def test_the_file_grants_only_a_read_takes_no_call_input_and_filters_no_push():
    """Property: the only permission is `contents: read`, the call carries no input, and `push` carries only `branches` and `tags`.

    Class: NEW CAPABILITY — the `permissions`, `workflow_call` and `push` keys. Plausible
    wrong implementations are `permissions: write-all` or `contents: write`, a
    `workflow_call` declaring a required input (the release workflow calls it with none, so
    its own run stops at parse time), and a `push` with `paths-ignore` or `branches-ignore`,
    which skips pushes the workflow promises to run on; this test is red under each.

    Why: the run executes this repository's own test code under a token, which should hold a
    read and nothing more, and the call interface and the push filter decide whether the run
    happens at all.

    Killed by: any permission other than `contents: read`, any child under `workflow_call`,
    or any `push` key other than `branches` and `tags`. Second correct implementation: the
    read grant on the job in place of the workflow, and the trigger lists written as block
    sequences.

    Not established: that a required input on `workflow_call` stops a caller that passes none
    is GitHub's documented behaviour and was not run here.
    """
    workflow = _workflow()
    grants = [block for block in (workflow.get("permissions"), _job(workflow).get("permissions")) if block is not None]
    assert grants, "the token falls back to the repository default with no permissions block"
    assert grants == [{"contents": "read"}], grants
    on = workflow["on"]
    assert isinstance(on, dict), on
    assert on.get("workflow_call") in (None, {}), on.get("workflow_call")
    push = on["push"]
    assert isinstance(push, dict), push
    assert set(push) == {"branches", "tags"}, sorted(push)


def test_the_reader_strips_a_trailing_comment_and_reads_a_block_scalar():
    """Property: a `# comment` trailing a value is not part of the value, and a `run: |` body is read.

    Class: NEW CAPABILITY — the reader's two blind spots. Plausible wrong implementation is
    the reader as it stood at `f7690c2`, where `_value` returned the comment as part of the
    value and `_mapping_block` stopped at the `|`, so a compliant file with a trailing comment
    failed the trigger assertions and a block-scalar step was invisible to every rule. Red
    under that reader; green under this one.

    Why: a rule that cannot see a step's body cannot refuse a second pytest run, and a reader
    that folds a comment into a value reds on a correct file — both directions of the same
    defect, which is why they are tested together.

    Killed by: dropping the comment strip, or treating `|` as an ordinary scalar. Second
    correct implementation: stripping the comment inside `_scalar` only, provided every value
    path goes through it, and joining a folded `>` body with spaces rather than newlines.
    """
    text = (
        "on:\n"
        "  push:\n"
        '    branches: [ "**" ]  # every branch\n'
        "steps:\n"
        "  - name: Narrow\n"
        "    run: |\n"
        "      python -m pytest -k nothing\n"
    )
    parsed = _parse(text)
    on = parsed["on"]
    assert isinstance(on, dict), on
    assert on["push"]["branches"] == ["**"], on["push"]
    steps = parsed["steps"]
    assert isinstance(steps, list), steps
    assert steps[0]["run"] == "python -m pytest -k nothing", steps[0]
