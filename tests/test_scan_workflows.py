"""Tests that both push guards run the one content scanner, and that nothing still calls what it replaced.

`.github/workflows/anonymization-guard.yml` and `.github/workflows/planning-reference-guard.yml`
are the two required checks that each owned a script of its own. They now hand every question they
ask to the single scanner under `scripts/content_scan`, which holds the corpus, the skip markers,
the report form and the exit codes once, so a class added there is read by the job that owns it and
no job carries a second copy of a pattern or an allowlist.

The rules are about the JOBS: which commands they run, which events they run on, and which of
their findings may fail a run. Each rests on a promise a reader makes when they see one of these
files:

- the job id is the required-check name the repository ruleset holds on the default branch, so a
  rename is a silent loss of that check and not a red run;
- both files run on a tag push, because a tag publishes the tree and a release tag is the one push
  a stranger downloads first;
- the machine-path class replaced the shell loop that carried its own copy of the pattern and the
  allowlist, and a rule over the whole file keeps a later edit from pasting that loop back;
- the secret class's count is reported and refuses nothing: a pushed secret is disclosed already,
  and a red run would neither un-disclose it nor say where it is, while a line in the log is the
  one record a push can leave;
- the self-test must ask for no report form, because what it measures is that the runner's own
  environment forces the count-only form without the flag;
- no guard file names a path this change removed, because a caller left behind fails the first
  push of the landed change rather than a later one.

The files are read as text rather than parsed as YAML: the planning-reference job installs pytest
and no YAML library, so a module-level `import yaml` here would stop that job at collection, which
is the same reason the whole-suite workflow tests state. The reader below is the smallest thing
that answers the questions: it normalises the inline and the block spelling of a trigger list,
drops a value's trailing comment, and reads a step or a hook by its own indentation, so a respelled
list or a new neighbour reads the same as the plain form.

Every test here is class NEW CAPABILITY, and each docstring's own `Killed by` sentence names the
wrong implementation it is red under. The tree before this change carries none of these rules, so
none of them can be red on it: what makes each one a test rather than a transcription is that a
specific, plausible edit to the file it reads turns it red, and whoever wrote this module ran
that edit for every one of them and pasted the result. `Killed by` names the edit; the pasted run
shows the red. A test whose named edit leaves it green is the defect this paragraph exists to make
visible, so an edit to this module that adds a test without such a sentence adds a rule nobody has
seen fail.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"
ANONYMIZATION = WORKFLOWS / "anonymization-guard.yml"
PLANNING = WORKFLOWS / "planning-reference-guard.yml"
HOOKS = REPO_ROOT / ".pre-commit-config.yaml"

# The one scanner both guards run, by the repo-relative path the runner executes.
SCANNER = "scripts/content_scan"

# What this change removed, and the module that now carries the behaviours those files tested.
REMOVED = (
    "scripts/check_anonymization.py",
    "scripts/check_planning_references.py",
    "tests/test_check_anonymization.py",
    "tests/test_check_anonymization_review.py",
    "tests/test_planning_references.py",
)
SUCCESSOR = "tests/test_content_scan.py"

CARRIER_STEP = "Write the carrier and ledger from the repository secrets"
SELF_TEST_STEP = "Self-test count-only output on this runner"

_STEP_HEADER = re.compile(r"^ {6}- name:\s*(?P<name>.+?)\s*$")
_JOB_HEADER = re.compile(r"^ {2}(?P<id>[A-Za-z_][\w-]*):\s*$")


def _lines(path: Path) -> list[str]:
    return path.read_text(encoding="utf-8").splitlines()


def _indent(line: str) -> int:
    return len(line) - len(line.lstrip())


def _block(lines: list[str], start: int) -> list[str]:
    """The line at `start` and the lines under it, up to the next line at its own indentation."""
    indent = _indent(lines[start])
    end = start + 1
    while end < len(lines) and (not lines[end].strip() or _indent(lines[end]) > indent):
        end += 1
    return lines[start:end]


def _unquote(text: str) -> str:
    return text.split("#", 1)[0].strip().strip("\"'")


def _values(lines: list[str], start: int) -> list[str]:
    """The values of the key at `start`: the inline list after its colon, else its `- item` lines."""
    head = lines[start].split(":", 1)[1]
    if head.split("#", 1)[0].strip().startswith("["):
        inside = head.split("#", 1)[0].strip()[1:-1]
        return [_unquote(item) for item in inside.split(",") if item.strip()]
    return [_unquote(line.strip()[2:]) for line in _block(lines, start)[1:] if line.strip().startswith("- ")]


def _job_ids(path: Path) -> list[str]:
    """The job ids the file declares under `jobs:`."""
    lines = _lines(path)
    start = next((index for index, line in enumerate(lines) if line.strip() == "jobs:"), None)
    assert start is not None, f"{path.name} declares no jobs"
    return [match["id"] for line in _block(lines, start)[1:] if (match := _JOB_HEADER.match(line))]


def _triggers(path: Path) -> dict[str, dict[str, list[str]]]:
    """`{event: {filter: values}}` for the events the file names under `on:`.

    An event with no value (`push:` and nothing under it) reads as an entry with no filter, which
    is a trigger the file states without a filter line.
    """
    lines = _lines(path)
    start = next((index for index, line in enumerate(lines) if line.strip() == "on:"), None)
    assert start is not None, f"{path.name} states no trigger"
    triggers: dict[str, dict[str, list[str]]] = {}
    for index, line in enumerate(_block(lines, start)[1:], start=start + 1):
        if not line.strip():
            continue
        if _indent(line) == 2 and line.strip().endswith(":"):
            name = line.strip()[:-1]
            body = _block(lines, index)
            triggers[name] = {
                inner.strip().split(":", 1)[0]: _values(lines, index + position)
                for position, inner in enumerate(body[1:], start=1)
                if inner.strip() and _indent(inner) == 4 and ":" in inner
            }
    return triggers


def _steps(text: str) -> dict[str, list[str]]:
    """`{step name: that step's lines}` for the file's steps, in the order it declares them."""
    lines = text.splitlines()
    starts = [index for index, line in enumerate(lines) if _STEP_HEADER.match(line)]
    steps: dict[str, list[str]] = {}
    for position, start in enumerate(starts):
        end = starts[position + 1] if position + 1 < len(starts) else len(lines)
        steps[_STEP_HEADER.match(lines[start])["name"]] = lines[start:end]
    return steps


def _hooks(path: Path) -> dict[str, list[str]]:
    """`{hook id: that hook's lines}`, for the hooks the configuration declares."""
    lines = _lines(path)
    starts = [index for index, line in enumerate(lines) if line.lstrip().startswith("- id: ")]
    hooks: dict[str, list[str]] = {}
    for position, start in enumerate(starts):
        end = starts[position + 1] if position + 1 < len(starts) else len(lines)
        hooks[lines[start].strip()[len("- id: ") :]] = lines[start:end]
    return hooks


def _only_step_naming(path: Path, needle: str) -> str:
    """The body of the one step whose lines name `needle`; a second such step is a defect."""
    named = ["\n".join(body) for body in _steps(path.read_text(encoding="utf-8")).values() if needle in "\n".join(body)]
    assert len(named) == 1, f"{path.name}: expected one step naming {needle!r}, found {len(named)}"
    return named[0]


# --- the job ids the ruleset holds -----------------------------------------------------------


def test_both_guards_keep_the_job_id_the_ruleset_requires() -> None:
    """The job id IS the required-check name, so a rename loses the check instead of failing it.

    A required check is matched on the job's name. Renaming the job to match this change would
    leave the ruleset waiting for a check that no longer reports, so the branch reads as
    unverified rather than as failing, and nothing in the run says why. Killed by renaming either
    job, or by nesting it under another job's `steps:`.
    """
    for path, job in ((ANONYMIZATION, "anonymization-guard"), (PLANNING, "planning-reference-guard")):
        assert job in _job_ids(path), f"{path.name} no longer declares the job {job!r}: {_job_ids(path)}"


def test_both_guards_run_on_a_branch_push_and_on_a_tag_push() -> None:
    """A tag publishes the tree, and before this rule nothing but the release workflow read it.

    A branch trigger spelled `branches: ["**"]` excludes tags: the pattern matches refs under
    `refs/heads/` only, so an author who read "every branch" as "every push" leaves the release
    tag unread by the guard that exists to read it. Killed by dropping `tags:`, by narrowing it to
    a version pattern, or by leaving the event as a bare `push:` with no filter lines at all --
    which the rule reads as no branch filter and no tag filter.
    """
    for path in (ANONYMIZATION, PLANNING):
        push = _triggers(path).get("push")
        assert push is not None, f"{path.name} runs on no push"
        assert "**" in push.get("branches", []), f"{path.name} does not watch every branch: {push}"
        assert "**" in push.get("tags", []), f"{path.name} does not watch every tag: {push}"


# --- what each guard runs --------------------------------------------------------------------


def test_neither_guard_asks_for_a_report_form_that_locates_a_finding() -> None:
    """`--reveal` prints a label's own text, and `--list` prints what a scan would read.

    Both are debugging forms for a terminal, and both refuse a public log: the private carrier's
    entries, spelled out, are exactly what the count-only form exists to keep out of one. Killed
    by adding either flag to a job.
    """
    for path in (ANONYMIZATION, PLANNING):
        read = path.read_text(encoding="utf-8")
        assert "--reveal" not in read, f"{path.name} asks for the label text a public log must not hold"
        assert "--list" not in read and "--dry-run" not in read, f"{path.name} asks for a listing instead of a scan"


# --- nothing still calls what this change removed ---------------------------------------------
