"""Tests that the install check runs both artifacts at both ends of the declaration, and blocks nothing.

The workflow's deliverable is its text: a job that installs at `highest` and a second that installs at
`lowest-direct`, each building the sdist and the wheel and running the script this tree carries. So
this file reads the workflow as text with the same reader `tests/test_whole_suite_workflow.py` and
`tests/test_main_tag_check_workflow.py` carry -- the earlier run's form -- rather than importing a YAML
library, because the two files pose the same questions and a second reader would answer them
differently. Every command is compared by EQUALITY: a `--dist` that reads another path, a build that
runs `-m build` on another tree, and a resolution that is stated in prose but never passed all keep the
line's shape while measuring something else.

What the file does NOT assert, and why: the action versions. `actions/checkout@v4` and
`actions/setup-python@v5` are what the sibling push jobs use at this tree, and a later chunk that moves
them forward would red a test that held the numbers without changing what this workflow establishes.
"""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "install-check.yml"
SCRIPT = REPO_ROOT / "scripts" / "check_install.py"
SURFACE_SCRIPT = REPO_ROOT / "scripts" / "check_public_api.py"

NAME = "Install check"
JOBS = ("install-check", "install-check-lowest")
# The commands, by equality. The two install commands differ in one option, which is the whole point of
# the second job: the same artifact installed at the resolution that installs the declaration's floor.
INSTALL_COMMAND = "python scripts/check_install.py --dist dist"
FLOOR_COMMAND = "python scripts/check_install.py --dist dist --resolution lowest-direct"
BUILD_COMMAND = "python -m build"
BUILD_FRONTEND_COMMAND = "python -m pip install --disable-pip-version-check build"
UV_COMMAND = 'python -m pip install --disable-pip-version-check "uv==0.11.31"'
SURFACE_COMMAND = "python scripts/check_public_api.py --wheel dist/*.whl"
# The floor of `requires-python`, as the file writes it. The reader returns scalars as written, so this
# is compared as text rather than as a number.
PYTHON_VERSION = "3.11"

_KEY_VALUE = re.compile(r"^([A-Za-z_][A-Za-z0-9_-]*):(?:\s+(.*))?$")
_BLOCK_SCALAR = re.compile(r"^[|>][+-]?$")
# The keys that skip a job or a step, or let a red one report as a green run. This job blocks nothing,
# so a gating key on it is either a report that hides a failure or a `needs` this workflow does not have.
_GATING_KEYS = ("if", "continue-on-error", "needs")


def _strip_comment(text: str) -> str:
    """`text` up to a `#` that starts a comment, per YAML's rule: at the start, or after whitespace, outside quotes."""
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
    """Every non-blank, non-comment line of the file as (indent, stripped text)."""
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
    """An inline value: `[a, b]` becomes a list, `{k: v}` a mapping, and anything else a scalar."""
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
    """The block of entries starting at `lines[index]`, and the index one past it."""
    if lines[index][1].startswith("-"):
        return _list_block(lines, index)
    return _mapping_block(lines, index)


def _block_scalar(lines: list[tuple[int, str]], index: int, indent: int, marker: str) -> tuple[str, int]:
    """The body of a `run: |` or `run: >` block scalar, and the index one past it."""
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


def _jobs(workflow: dict[str, object] | None = None) -> dict[str, object]:
    """The workflow's job mapping, asserted to hold exactly the two jobs this file names."""
    jobs = (workflow or _workflow())["jobs"]
    assert isinstance(jobs, dict), jobs
    assert list(jobs) == list(JOBS), list(jobs)
    return jobs


def _steps(job: str) -> list[dict[str, object]]:
    """Every step of one job, in file order."""
    steps = _jobs()[job]["steps"]
    assert isinstance(steps, list), steps
    assert all(isinstance(step, dict) for step in steps), steps
    return steps


def _step_named(job: str, name: str) -> dict[str, object]:
    """The one step of `job` carrying `name`, asserted to occur exactly once so a second cannot hide."""
    matches = [step for step in _steps(job) if step.get("name") == name]
    assert len(matches) == 1, [step.get("name") for step in _steps(job)]
    return matches[0]


def _commands(job: str) -> list[str]:
    """Every `run:` of one job, in file order."""
    return [str(step["run"]) for step in _steps(job) if "run" in step]


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


def test_the_workflow_runs_on_every_branch_push_and_every_tag_push_and_nothing_else():
    """Property: the name is `Install check`, the only trigger is a push, and it filters every branch and tag.

    Class: NEW CAPABILITY -- plausible wrong implementation is the same file with `branches` narrowed
    to `main` (an install check that runs on one branch reports nothing about the others), with the
    tags filter dropped (no check on the commits a release publishes), or with a second trigger.

    Why: the artifact is built from the pushed commit, and every commit is a candidate for the release
    path that reads the same script. A trigger narrowed anywhere leaves a push unchecked.
    Killed by: adding or removing a trigger, or widening or narrowing either filter.
    Second correct implementation: the same filters written as block sequences, which this reader
    normalises to the same lists.
    """
    workflow = _workflow()
    assert workflow["name"] == NAME, workflow["name"]
    triggers = workflow["on"]
    assert isinstance(triggers, dict), triggers
    assert set(triggers) == {"push"}, sorted(triggers)
    push = triggers["push"]
    assert isinstance(push, dict), push
    assert push["branches"] == ["**"], push["branches"]
    assert push["tags"] == ["**"], push["tags"]
    assert set(push) == {"branches", "tags"}, sorted(push)


def test_the_workflow_has_the_two_jobs_and_grants_only_a_read():
    """Property: exactly the two jobs, `install-check` and `install-check-lowest`, and `contents: read`.

    Class: NEW CAPABILITY -- plausible wrong implementation is the second job dropped (the floor is
    then never installed) or a write permission added, which no step of this workflow needs.
    Why: the two jobs are the two ends of the declaration's range, and the permission is the least a
    job that only builds can have.
    Killed by: renaming, merging or adding a job, or granting a write scope.
    Second correct implementation: the two jobs written in either order, since this test compares the
    set the file declares and each job is asserted by name below.
    """
    workflow = _workflow()
    assert sorted(_jobs(workflow)) == sorted(JOBS), sorted(_jobs(workflow))
    assert workflow["permissions"] == {"contents": "read"}, workflow["permissions"]


def test_each_job_installs_the_built_distributions_and_the_floor_job_passes_the_resolution():
    """Property: both jobs run `python -m build` and the check, and only the second passes `lowest-direct`.

    Class: NEW CAPABILITY -- plausible wrong implementation is a job that runs the check before the
    build (nothing to install, refused as `wrong-artifacts`), a first job that passes `--resolution
    lowest-direct` as well (so nothing ever installs what a user gets), and a second job whose step
    name says "lowest" while its command omits the option.
    Why: the resolution is the only difference between the two jobs, and the option is the only thing
    that makes it a measurement rather than a name.
    Killed by: swapping the two commands, dropping either, or stating the resolution in the step name
    alone.
    Second correct implementation: the steps ordered differently around the build, as long as the
    build precedes both the check and the surface check, which the order test below holds.
    """
    assert _commands("install-check") == [
        BUILD_FRONTEND_COMMAND,
        UV_COMMAND,
        BUILD_COMMAND,
        INSTALL_COMMAND,
        SURFACE_COMMAND,
    ], _commands("install-check")
    assert _commands("install-check-lowest") == [
        BUILD_FRONTEND_COMMAND,
        UV_COMMAND,
        BUILD_COMMAND,
        FLOOR_COMMAND,
    ], _commands("install-check-lowest")


def test_the_checkout_is_first_and_the_build_precedes_every_check():
    """Property: each job checks out the repository first, and builds before it checks or scans.

    Class: NEW CAPABILITY -- plausible wrong implementation is a build that runs before the checkout
    (nothing to build), or a check placed before the build (refused as `wrong-artifacts`, and the run
    would report a check that never happened).
    Why: a step order is the only thing that makes each later step's input exist.
    Killed by: reordering the checkout, or inserting a step between the build and its readers.
    Second correct implementation: extra steps between the build and the readers, such as a listing of
    `dist/`, which changes no input of either reader.
    """
    for job in JOBS:
        steps = _steps(job)
        names = [str(step.get("name", "")) for step in steps]
        assert str(steps[0].get("uses", "")).startswith("actions/checkout@"), steps[0]
        built = names.index("Build the sdist and the wheel")
        readers = [
            index
            for index, name in enumerate(names)
            if "install and import the built distributions" in name.lower() or "check the wheel" in name.lower()
        ]
        assert readers, names
        assert all(built < index for index in readers), names


def test_no_key_skips_a_job_or_a_step_lets_a_failing_one_report_as_a_pass():
    """Property: the file carries no `needs`, `if` or `continue-on-error` key anywhere.

    Class: NEW CAPABILITY -- plausible wrong implementation is `continue-on-error: true` on the check
    step, which turns the one refusal this workflow exists to report into a green run.
    Why: nothing requires this job, so it is a report; a gating key makes the report unable to say no.
    Killed by: adding any of the three keys at any depth.
    Second correct implementation: a `needs` added to make the second job wait for the first, which
    this test refuses on purpose -- the two jobs are independent readings of one build, and a wait
    would make the floor's reading depend on the highest's outcome.
    """
    entries = _gating_entries(_workflow())

    assert entries == [], entries
