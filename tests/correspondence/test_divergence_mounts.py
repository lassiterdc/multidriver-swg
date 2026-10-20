"""Tests that the divergence detector is mounted where it must run: as the last pre-commit hook, and inside the state-check job.

The detector (`tests/correspondence/divergence.py`) only protects the ledger if something runs it.
It has three mounts. The first is its own test, `test_divergence.py`. The second is the state-check
CI job, which runs every test carrying the `state_check` marker and is never edited to add one.
The third is a pre-commit hook. A mount can be lost without any test of the detector going red:
the hook block can be deleted or reordered, and a test module can lose its marker and silently
drop out of the job. These tests read the two configuration files as text and check both.

Both files are read as text rather than parsed as YAML. The job installs only the toolkit and
`pytest`, so a YAML library would be a new dependency of the state checks, and these tests run in
that job.
"""

import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

from tests.correspondence import divergence

pytestmark = pytest.mark.state_check

REPO_ROOT = Path(divergence.__file__).resolve().parents[2]
PRE_COMMIT_CONFIG = REPO_ROOT / ".pre-commit-config.yaml"
STATE_CHECK_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "state-check.yml"
DETECTOR_NODE = "tests/correspondence/test_divergence.py::test_this_tree_has_no_divergence_finding"
DETECTOR_HOOK = {
    "name": "divergence record detector",
    "entry": "python3 -m tests.correspondence.divergence",
    "language": "system",
    "pass_filenames": "false",
    "always_run": "true",
}

_REPO_LINE = re.compile(r"^\s*- repo:\s*(\S+)")
_ID_LINE = re.compile(r"^(\s*)- id:\s*(\S+)")
_KEY_LINE = re.compile(r"^(\s*)([A-Za-z_]+):\s*(.*?)\s*$")


def _hooks(text: str) -> list[tuple[str, str, dict[str, str]]]:
    """Every hook of a pre-commit config, in file order, as (repo, hook id, its scalar keys).

    A hook starts at a `- id:` line, and its keys are the `key: value` lines indented deeper
    than that line, up to the next line at the same or a shallower indent. Comment and blank
    lines never end a hook, so a comment between two keys does not split one hook into two.
    """
    hooks: list[tuple[str, str, dict[str, str]]] = []
    repo = ""
    current: dict[str, str] | None = None
    hook_indent = -1
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(line) - len(line.lstrip())
        if current is not None and indent <= hook_indent:
            current = None
        repo_match = _REPO_LINE.match(line)
        if repo_match:
            repo = repo_match.group(1)
            continue
        id_match = _ID_LINE.match(line)
        if id_match:
            hook_indent = len(id_match.group(1))
            current = {}
            hooks.append((repo, id_match.group(2), current))
            continue
        key_match = _KEY_LINE.match(line)
        if current is not None and key_match:
            current[key_match.group(2)] = key_match.group(3)
    return hooks


def _job_command() -> list[str]:
    """The state-check job's pytest step, read from the workflow file, as an argument list.

    The step is the one `run:` line that invokes pytest. Reading it from the file, rather than
    restating it here, means a change to the job's marker expression changes what these tests
    collect, instead of leaving them checking a selection the job no longer runs.
    """
    runs = [
        line.split("run:", 1)[1].strip()
        for line in STATE_CHECK_WORKFLOW.read_text(encoding="utf-8").splitlines()
        if line.strip().startswith("run:") and "-m pytest" in line
    ]
    assert len(runs) == 1, runs
    return shlex.split(runs[0])


def _collected(root: Path) -> list[str]:
    """Run the job's pytest step with `--collect-only -q` in a child under `root`; return every collected node id.

    The child is this interpreter with the job's own arguments, so its selection is the job's
    marker expression applied to whatever `root` holds. It runs in a separate process because a
    collection inside this session would share its imported modules and its plugin state.
    """
    command = _job_command()
    assert command[:3] == ["python", "-m", "pytest"], command
    run = subprocess.run(
        [sys.executable, *command[1:], "--collect-only", "-q"],
        cwd=root,
        env=dict(os.environ),
        capture_output=True,
        text=True,
    )
    # Exit 5 is pytest's "no tests collected", which a tree with no selected module legitimately returns.
    assert run.returncode in (0, 5), (run.returncode, run.stdout[-2000:], run.stderr[-2000:])
    return [line.strip() for line in run.stdout.splitlines() if "::" in line and not line.startswith(" ")]


def _uncollected(root: Path, nodes: list[str]) -> list[str]:
    """Every `tests/correspondence/test_*.py` file under `root` from which no node in `nodes` was collected."""
    collected = {node.split("::", 1)[0] for node in nodes}
    modules = sorted(p.relative_to(root).as_posix() for p in (root / "tests" / "correspondence").glob("test_*.py"))
    return [m for m in modules if m not in collected]


def test_the_divergence_detector_is_the_last_pre_commit_hook():
    """Property: the last hook in the pre-commit config is the local divergence detector, run as configured.

    Class: any edit to `.pre-commit-config.yaml` — a hook added after the detector, the detector's
    block removed, or one of its keys changed. Why: the hook is the detector's commit-time mount;
    it runs `python3 -m tests.correspondence.divergence` with the standard library only, so a
    commit made with no environment active is still checked, and being last means every other
    hook's rewrite of the tree has happened before the detector reads it. Killed by: deleting the
    block, appending any hook after it, or changing `entry` to bare `python`. Second correct
    implementation: the same block with its keys in another order, or a comment line inside it,
    both of which this test reads identically.
    """
    hooks = _hooks(PRE_COMMIT_CONFIG.read_text(encoding="utf-8"))
    assert hooks, "no hook found in .pre-commit-config.yaml"
    repo, hook_id, keys = hooks[-1]
    assert (repo, hook_id) == ("local", "divergence-detector"), [(r, i) for r, i, _ in hooks]
    assert keys == DETECTOR_HOOK, keys
    module = keys["entry"].split()[2]
    assert (REPO_ROOT / Path(*module.split("."))).with_suffix(".py") == Path(divergence.__file__).resolve()


def test_the_job_selection_collects_every_correspondence_test_module_and_the_detector_test():
    """Property: the state-check job's selection collects at least one node from every `tests/correspondence/test_*.py`, and the detector's own test.

    Class: every test module that exists in that directory now or later, read from the directory
    rather than from a list. Why: the job is the only workflow that runs pytest, and it selects by
    marker, so a module that forgets `pytestmark = pytest.mark.state_check` drops out of CI with
    nothing going red; the detector's own test is its second mount only if the job collects it.
    Killed by: removing the marker from any module in the directory, or changing the job's marker
    expression to exclude `state_check`. Second correct implementation: marking each test function
    instead of the module, which this test reads identically because it counts collected nodes.
    """
    nodes = _collected(REPO_ROOT)
    assert _uncollected(REPO_ROOT, nodes) == [], sorted({node.split("::", 1)[0] for node in nodes})
    assert DETECTOR_NODE in nodes, "the job's selection does not collect the detector test"


def test_a_module_without_the_marker_is_reported_and_a_marked_one_is_not(tmp_path):
    """Property: the directory check names an unmarked test module and does not name a marked one.

    Class: a `tests/correspondence/` tree holding modules with and without the job's marker. Why:
    the real-tree test above passes on a tree where every module is marked, which is also what a
    check that names nothing would report; this arm shows the check can fire. Killed by: an
    `_uncollected` that returns an empty list, or that lists every module. Second correct
    implementation: comparing the set of module paths with the set of collected node prefixes.
    """
    (tmp_path / "pyproject.toml").write_text(
        '[tool.pytest.ini_options]\nmarkers = ["state_check: s", "equivalence: e", "refactor_reminder: r"]\n',
        encoding="utf-8",
    )
    directory = tmp_path / "tests" / "correspondence"
    directory.mkdir(parents=True)
    (directory / "test_marked.py").write_text(
        "import pytest\n\npytestmark = pytest.mark.state_check\n\n\ndef test_a():\n    pass\n", encoding="utf-8"
    )
    (directory / "test_unmarked.py").write_text("def test_b():\n    pass\n", encoding="utf-8")
    assert _uncollected(tmp_path, _collected(tmp_path)) == ["tests/correspondence/test_unmarked.py"]
