"""Behavioural checks of the two push guards, run by executing their steps in a throwaway clone.

`tests/test_scan_workflows.py` reads the workflow files as text and asserts on their wording. Every
rule here instead RUNS the shell a step carries, the way the runner runs it (`bash -eo pipefail`,
`GITHUB_ACTIONS=true`, a `RUNNER_TEMP`), inside a fresh `git clone` of the committed tree, so the
carrier a developer's machine links in is absent and a planted line is the only thing that varies.

Each test states the invariant it enforces and runs two inputs: one that must produce the finding
and one, in a different correct state, that must not.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

import scripts.content_scan.machine_path as machine_path

REPO_ROOT = Path(__file__).resolve().parents[1]
ANONYMIZATION = REPO_ROOT / ".github" / "workflows" / "anonymization-guard.yml"
PLANNING = REPO_ROOT / ".github" / "workflows" / "planning-reference-guard.yml"
HOOKS = REPO_ROOT / ".pre-commit-config.yaml"

PUBLIC_SURFACE = "Assert the public surface carries no developer cruft"
SELF_TEST = "Self-test count-only output on this runner"
SECRET_STEP = "Report the secret class's count"
PLANNING_SCAN = "Scan the tree"

# Built from pieces so that no line of this module holds a whole machine path, a whole private-key
# header or a whole planning identifier, each of which the guards under test refuse in a file.
HOME_PATH = "/ho" + "me/someone/project"
USERS_PATH = "/Us" + "ers/someone/project"
PEM_HEADER = "-----BEGIN " + "RSA PRIVATE " + "KEY-----"
PLANNING_ID = "AD" + "R-" + "7"

_STEP_HEADER = re.compile(r"^ {6}- name:\s*(?P<name>.+?)\s*$")


def _step_scripts(path: Path) -> dict[str, str]:
    """`{step name: the shell its `run:` key carries}`, block or one-line form."""
    lines = path.read_text(encoding="utf-8").splitlines()
    starts = [i for i, line in enumerate(lines) if _STEP_HEADER.match(line)]
    scripts: dict[str, str] = {}
    for position, start in enumerate(starts):
        end = starts[position + 1] if position + 1 < len(starts) else len(lines)
        body = lines[start:end]
        name = _STEP_HEADER.match(lines[start])["name"]
        for offset, line in enumerate(body):
            stripped = line.strip()
            if stripped.startswith("run: |"):
                indent = len(line) - len(line.lstrip())
                block = []
                for inner in body[offset + 1 :]:
                    if inner.strip() and len(inner) - len(inner.lstrip()) <= indent:
                        break
                    block.append(inner[indent + 2 :] if inner.strip() else "")
                scripts[name] = "\n".join(block)
                break
            if stripped.startswith("run: "):
                scripts[name] = stripped[len("run: ") :]
                break
    return scripts


def _git(clone: Path, *args: str) -> None:
    """Run one git command in a clone, with no inherited repository-location variable."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    subprocess.run(["git", "-C", str(clone), *args], check=True, capture_output=True, env=env)


@pytest.fixture
def make_clone(tmp_path: Path):
    """A factory of fresh clones of the committed tree: no carrier, no untracked files."""
    counter = iter(range(1000))

    def factory() -> Path:
        """One new clone of the committed tree."""
        dest = tmp_path / f"clone{next(counter)}"
        env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
        subprocess.run(
            ["git", "clone", "-q", "--no-local", str(REPO_ROOT), str(dest)], check=True, capture_output=True, env=env
        )
        return dest

    return factory


def _plant(clone: Path, rel: str, text: str, *, append: bool = False) -> None:
    """Write or append a line in a clone and stage it, so the scanner's file listing includes it."""
    target = clone / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    if append:
        with target.open("a", encoding="utf-8") as handle:
            handle.write(text)
    else:
        target.write_text(text, encoding="utf-8")
    _git(clone, "add", "-A")


def _runner_env(tmp_path: Path, *, actions: bool = True) -> dict[str, str]:
    """The environment a runner gives a step: the Actions variable and a temp directory, no PYTHONPATH."""
    env = {
        k: v for k, v in os.environ.items() if k not in {"PYTHONPATH", "GITHUB_ACTIONS"} and not k.startswith("GIT_")
    }
    env["RUNNER_TEMP"] = str(tmp_path / "runner-temp")
    Path(env["RUNNER_TEMP"]).mkdir(exist_ok=True)
    if actions:
        env["GITHUB_ACTIONS"] = "true"
    return env


def _run_step(path: Path, step: str, cwd: Path, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """Run one step's shell as the runner does (`bash -eo pipefail`) and return the finished process."""
    script = _step_scripts(path)[step]
    return subprocess.run(
        ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", script],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


# --- the public-surface step ----------------------------------------------------------------


def test_the_public_surface_step_refuses_a_planted_path_and_a_non_comment_line(make_clone, tmp_path) -> None:
    """Invariant: the step exits non-zero exactly when a tracked file names a machine path or a control file holds a token.

    Clean tree: exit 0. A tracked file naming a home directory: non-zero, and the count-only line
    names no file. A non-comment line in a public token list: non-zero even though the scanner
    itself is clean, so the shell half of the step is not shadowed by the scanner half.

    Class: NEW CAPABILITY. Wrong implementation: the step swallows the scanner's status (`|| true`)
    or drops the prose-only loop; red under either. Second correct implementation: the step runs the
    prose-only check after the scan, or reads the two lists with another tool; still green. Fails when
    a machine path or a token line passes a push, which is the file's own stated job. Reachable by
    any tracked file a contributor adds.
    """
    env = _runner_env(tmp_path)
    clean = _run_step(ANONYMIZATION, PUBLIC_SURFACE, make_clone(), env)
    assert clean.returncode == 0, clean.stdout + clean.stderr

    planted = make_clone()
    _plant(planted, "notes_planted.txt", f"see {HOME_PATH}\n")
    found = _run_step(ANONYMIZATION, PUBLIC_SURFACE, planted, env)
    assert found.returncode != 0, found.stdout
    assert "machine_path 1" in found.stdout
    assert "notes_planted" not in found.stdout + found.stderr, "a public log must not carry the path of a finding"

    listed = make_clone()
    _plant(listed, "scripts/anonymization_baseline.txt", "a-token-line\n", append=True)
    refused = _run_step(ANONYMIZATION, PUBLIC_SURFACE, listed, env)
    assert refused.returncode != 0, refused.stdout
    assert "prose-only" in refused.stderr


# --- the secret step ------------------------------------------------------------------------


def test_the_secret_step_reports_a_finding_without_failing_and_fails_on_an_unreadable_run(make_clone, tmp_path) -> None:
    """Invariant: status 1 (a finding) passes the step and prints the count; status 2 fails it.

    A planted private-key header: the step exits 0 and the count line reads `secret 1`, with no
    file name in it. A scanner run from a directory that is no git repository exits 2, and the step
    must exit non-zero then; the direct scanner call pins that the second input really is status 2.

    Class: NEW CAPABILITY. Wrong implementation: `[ "$rc" -le 1 ] || true`, which keeps the text the
    text-reading rule looks for and hides status 2; red under it (run and pasted in the review file).
    Second correct implementation: `continue-on-error` on a step that prints first; the unreadable
    arm still needs a failing step, so it reads the step's outcome the same way.
    """
    env = _runner_env(tmp_path)
    planted = make_clone()
    _plant(planted, "key_planted.txt", PEM_HEADER + "\n")
    found = _run_step(ANONYMIZATION, SECRET_STEP, planted, env)
    assert found.returncode == 0, found.stdout + found.stderr
    assert "secret 1" in found.stdout
    assert "key_planted" not in found.stdout + found.stderr

    bare = tmp_path / "not-a-repo"
    (bare / "scripts").mkdir(parents=True)
    shutil.copytree(
        REPO_ROOT / "scripts" / "content_scan",
        bare / "scripts" / "content_scan",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    direct = subprocess.run(
        ["python3", "-B", "scripts/content_scan", "--class", "secret", "--count-only"],
        cwd=bare,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert direct.returncode == 2, direct.stdout + direct.stderr
    unreadable = _run_step(ANONYMIZATION, SECRET_STEP, bare, env)
    assert unreadable.returncode != 0, "an unreadable run was reported as a pass"


# --- the self-test step ---------------------------------------------------------------------


def test_the_self_test_passes_on_the_runner_form_and_fails_without_it(make_clone, tmp_path) -> None:
    """Invariant: the self-test passes only when the environment alone forces the count-only form.

    With `GITHUB_ACTIONS=true` the step passes. With the variable removed the same step prints
    located lines and must fail, otherwise it would measure nothing.

    Class: NEW CAPABILITY. Wrong implementation: the scanner stops forcing count-only under
    `GITHUB_ACTIONS`, or the step passes `--count-only`; the step then passes in both arms, so the
    second assertion reds. Second correct implementation: a count-only forcing read from another
    variable the runner sets; still green on the first arm.
    """
    clone = make_clone()
    on = _run_step(ANONYMIZATION, SELF_TEST, clone, _runner_env(tmp_path))
    assert on.returncode == 0, on.stdout + on.stderr
    assert "count-only self-test: PASS" in on.stdout
    off = _run_step(ANONYMIZATION, SELF_TEST, make_clone(), _runner_env(tmp_path, actions=False))
    assert off.returncode != 0, off.stdout


# --- the planning-reference guard -----------------------------------------------------------


def test_the_planning_scan_step_refuses_a_planted_identifier_and_passes_a_clean_tree(make_clone, tmp_path) -> None:
    """Invariant: the planning guard's scan exits 0 on the committed tree and non-zero on a planted identifier.

    Class: NEW CAPABILITY. Wrong implementation: the step runs another class, or ends in `|| true`.
    Second correct implementation: any scanner entry that reports the class; green. The count-only
    form must also keep the planted file's name out of a public log.
    """
    env = _runner_env(tmp_path)
    assert _run_step(PLANNING, PLANNING_SCAN, make_clone(), env).returncode == 0
    planted = make_clone()
    _plant(planted, "notes_planted.md", f"see {PLANNING_ID} for why\n")
    result = _run_step(PLANNING, PLANNING_SCAN, planted, env)
    assert result.returncode != 0, result.stdout
    assert "notes_planted" not in result.stdout + result.stderr


# --- the hook configuration -----------------------------------------------------------------


def _hook_entries() -> dict[str, str]:
    """`{hook id: entry command}` read from the hook configuration."""
    entries: dict[str, str] = {}
    current = None
    for line in HOOKS.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("- id: "):
            current = stripped[len("- id: ") :]
        elif stripped.startswith("entry: ") and current is not None:
            entries[current] = stripped[len("entry: ") :]
    return entries


def test_the_identity_hook_fails_closed_in_a_clone_with_no_carrier(make_clone, tmp_path) -> None:
    """Invariant: the hook comment says a missing carrier already fails the scan closed; run the hook's own entry.

    The entry has no `--require-supplement`, so this is the claim the comment relies on. A clone
    with no carrier exits 2. The planning hook's entry, run in the same clean clone, exits 0 and
    exits 1 once an identifier is planted, so the second entry is not shadowed by the first.

    Class: NEW CAPABILITY. Wrong implementation: an identity entry that swallows exit 2, or a
    planning entry that names a removed script or another class; red under either. Second correct
    implementation: the same entries through `python3 -m`-free wrappers that keep the status; green.
    """
    env = _runner_env(tmp_path, actions=False)
    entries = _hook_entries()
    identity = subprocess.run(
        entries["anonymization-guard"].split(), cwd=make_clone(), env=env, capture_output=True, text=True, check=False
    )
    assert identity.returncode == 2, identity.stdout + identity.stderr
    clean = subprocess.run(
        entries["planning-reference-guard"].split(),
        cwd=make_clone(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert clean.returncode == 0, clean.stdout + clean.stderr
    planted = make_clone()
    _plant(planted, "notes_planted.md", f"see {PLANNING_ID}\n")
    found = subprocess.run(
        entries["planning-reference-guard"].split(), cwd=planted, env=env, capture_output=True, text=True, check=False
    )
    assert found.returncode == 1, found.stdout + found.stderr


# --- what a deleted file leaves behind -----------------------------------------------------------


def test_every_machine_path_allowlist_row_names_a_file_the_tree_holds() -> None:
    """Invariant: a literal path row of the machine-path allowlist names a file the tree holds.

    A row for a deleted file exempts whatever file is later created at that path from the
    machine-path scan, so the row protects nothing and hides the next file that takes its name. The
    allowlist still names the deleted planning-reference script after the commit that removed it.

    Class: REGRESSION (red on the reviewed tip). Killed by deleting the stale row from
    `machine_path.ALLOWLIST`. Second correct implementation: a row for a path that exists, or a
    different test shape that derives its rows from the live tree; green.
    """
    body = machine_path.ALLOWLIST.pattern
    rows = body[len("^(") : -len(")$")].split("|")
    stale = [row.replace("\\.", ".") for row in rows if not (REPO_ROOT / row.replace("\\.", ".")).exists()]
    assert not stale, f"allowlist rows naming no file in the tree: {stale}"
