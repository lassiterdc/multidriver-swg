"""Reviewer tests of the lock wiring: the steps that hold a release to its lock and the install that reads it.

`tests/test_publish_workflow.py` and `tests/test_whole_suite_workflow.py` pin the new steps as TEXT: a
name, a command string, a substring of an inline program. Text equality cannot see a step whose
shell snippet or inline program does the wrong thing while spelling the right words. These tests run
what the steps run, over inputs the step's own tests do not choose, and read the install line against
the files it names.

Module guarded: `.github/workflows/publish.yml` (`post-build-check`'s sdist check and acceptance
read) and `.github/workflows/whole-suite.yml` (the install line), judged against the tarballs and
records they read and against `requirements.txt` and `pyproject.toml`. One-time test: it proves the
lock wiring once and is deleted at the landing unless it is promoted.

Four rules:

1. the sdist step, run over archives of known members, accepts an archive holding both top-level
   lock files and refuses every archive missing one, holding it only nested, or holding a longer name;
2. the acceptance read, run over the record the enumeration script writes, exits 0 when the record
   names the gate's run and the tagged commit and exits non-zero when any one of the four values
   differs;
3. the Whole suite install line installs the lock before the project and the group, and the
   committed requirement list is installable under `--require-hashes` (every requirement pinned `==`
   and carrying a hash);
4. every distribution both the lock and the `test` group name is pinned to one version, so the group
   install after the lock replaces nothing the lock installed.
"""

from __future__ import annotations

import io
import json
import os
import re
import subprocess
import sys
import tarfile
import tomllib
import zipfile
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
PUBLISH = REPO_ROOT / ".github" / "workflows" / "publish.yml"
WHOLE_SUITE = REPO_ROOT / ".github" / "workflows" / "whole-suite.yml"
ENUMERATION_SCRIPT = REPO_ROOT / "scripts" / "enumerate_release.py"
REQUIREMENTS = REPO_ROOT / "requirements.txt"
PYPROJECT = REPO_ROOT / "pyproject.toml"

CHECK_JOB = "post-build-check"
SDIST_STEP = "Check the sdist lists the lock files"
ENUMERATION_STEP = "Enumerate the released bytes"
ACCEPTANCE_STEP = "Check the enumeration names the run this release read"
INSTALL_STEP = "Install the toolkit and the test runner"
EXPRESSION = re.compile(r"\$\{\{\s*needs\.release-gate\.outputs\.([A-Za-z0-9_-]+)\s*\}\}")

# What the gate's outputs resolve to on a run, so a step's expressions can be resolved the way the
# runner resolves them. The values are fixed here and the step's own text decides which it reads.
RUN_ID = "9001"
RUN_ATTEMPT = "2"


def _step_run(path: Path, job: str, name: str) -> str:
    """The `run:` value of the step called `name` in `job`, parsed from the YAML."""
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    steps = document["jobs"][job]["steps"]
    named = [step for step in steps if step.get("name") == name]
    assert len(named) == 1, (name, len(named))
    return str(named[0]["run"])


def _resolve(command: str, sha: str, overrides: dict[str, str] | None = None) -> str:
    """The command with each gate-output expression replaced by the value the gate publishes."""
    values = {"whole_suite_run_id": RUN_ID, "whole_suite_run_attempt": RUN_ATTEMPT, "whole_suite_commit": sha}
    values.update(overrides or {})
    return EXPRESSION.sub(lambda match: values[match.group(1)], command)


def _sdist(directory: Path, members: list[str]) -> None:
    """One `.tar.gz` in `directory/dist` holding exactly `members` as empty files."""
    dist = directory / "dist"
    dist.mkdir(exist_ok=True)
    with tarfile.open(dist / "pkg-0.0.0.tar.gz", "w:gz") as archive:
        for member in members:
            info = tarfile.TarInfo(member)
            info.size = 0
            archive.addfile(info, io.BytesIO(b""))


def _run_bash(command: str, cwd: Path, environment: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """The command under `bash -e`, the shell the runner gives a `run:` step by default."""
    env = {"PATH": os.environ["PATH"], **environment}
    return subprocess.run(["bash", "-e", "-c", command], cwd=cwd, env=env, capture_output=True, text=True, check=False)


@pytest.mark.parametrize(
    ("members", "accepted"),
    [
        (["pkg-0.0.0/pylock.toml", "pkg-0.0.0/requirements.txt", "pkg-0.0.0/pyproject.toml"], True),
        (["pkg-0.0.0/requirements.txt", "pkg-0.0.0/pyproject.toml"], False),
        (["pkg-0.0.0/pylock.toml", "pkg-0.0.0/pyproject.toml"], False),
        (["pkg-0.0.0/pylock.toml", "pkg-0.0.0/docs/requirements.txt"], False),
        (["pkg-0.0.0/docs/pylock.toml", "pkg-0.0.0/requirements.txt"], False),
        (["pkg-0.0.0/pylock.toml.orig", "pkg-0.0.0/requirements.txt"], False),
        (["pkg-0.0.0/pylock.toml", "pkg-0.0.0/requirements.txt.orig"], False),
    ],
    ids=[
        "both",
        "no-pylock",
        "no-requirements",
        "requirements-nested",
        "pylock-nested",
        "pylock-longer",
        "requirements-longer",
    ],
)
def test_the_sdist_step_accepts_only_an_archive_holding_both_lock_files_at_the_top(tmp_path, members, accepted):
    """Property: the sdist step, run as the runner runs it over an archive, exits 0 exactly when the
    archive lists `pylock.toml` and `requirements.txt` directly under its one top directory.

    Class: NEW CAPABILITY over seven member sets — plausible wrong implementations are a pattern
    unanchored at the end (`grep -q`, accepting a `.orig` sibling), one unanchored by depth (accepting
    `docs/requirements.txt`, the Read the Docs list this plan moved), and a check that passes when only
    one of the two files is present. The `both` row is the must-pass arm; the rest are must-refuse.

    Reachability: each member set is an archive a build can produce (a manifest line dropped, a
    documentation list left nested, a backup file shipped); artifact: the step's own `run:` text over
    a real tarball.

    Why: the sdist is what a source install downloads and `MANIFEST.in` is the only thing that puts
    the pair in it, so the refusal has to fire on each way the pair can be absent or shadowed.

    Both-states anchor: the verdict turns on the archive's members, a property of the input that is
    the same under the current step and any correct one.

    Invocation isolated: the step's command is run unchanged; the named change to behaviour that
    greens a refused row is making the step accept that member set.

    Killed by: loosening `-x`, loosening the depth anchor, or requiring only one file. Second correct
    implementation: one `tar -tzf` listing captured once and tested twice, or a `python -m tarfile -l`
    read, both of which give the same verdicts on these archives.
    """
    command = _step_run(PUBLISH, CHECK_JOB, SDIST_STEP)
    _sdist(tmp_path, members)
    completed = _run_bash(command, tmp_path, {})
    assert (completed.returncode == 0) is accepted, (members, completed.returncode, completed.stdout, completed.stderr)


def _checkout(root: Path) -> str:
    """One commit in a throwaway repository at `root`, and its sha."""
    identity = ("-c", "user.name=Reviewer", "-c", "user.email=reviewer@example.invalid")
    subprocess.run(["git", "init", "-q"], cwd=root, check=True, capture_output=True)
    (root / "README.md").write_text("one file\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=root, check=True, capture_output=True)
    subprocess.run(["git", *identity, "commit", "-q", "-m", "one"], cwd=root, check=True, capture_output=True)
    shown = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True)
    return shown.stdout.strip()


def _distributions(dist: Path) -> None:
    """A minimal sdist and wheel, the two files a release's dist directory holds."""
    dist.mkdir()
    payload = b"print('x')\n"
    with tarfile.open(dist / "pkg-0.0.0.tar.gz", "w:gz") as sdist:
        info = tarfile.TarInfo("pkg-0.0.0/pkg.py")
        info.size = len(payload)
        sdist.addfile(info, io.BytesIO(payload))
    with zipfile.ZipFile(dist / "pkg-0.0.0-py3-none-any.whl", "w") as wheel:
        wheel.writestr("pkg.py", payload)


def _write_record(tmp_path: Path) -> tuple[str, Path]:
    """Run the publish workflow's own enumeration step over a throwaway commit; return sha and the runner temp dir."""
    sha = _checkout(tmp_path)
    _distributions(tmp_path / "dist")
    runner_temp = tmp_path / "runner-temp"
    runner_temp.mkdir()
    command = _resolve(_step_run(PUBLISH, CHECK_JOB, ENUMERATION_STEP), sha)
    command = command.replace("python scripts/enumerate_release.py", f'"{sys.executable}" "{ENUMERATION_SCRIPT}"', 1)
    environment = {"GITHUB_SHA": sha, "GITHUB_REF_NAME": "v0.0.0", "RUNNER_TEMP": str(runner_temp)}
    completed = _run_bash(command, tmp_path, environment)
    assert completed.returncode == 0, (command, completed.stdout, completed.stderr)
    return sha, runner_temp


def _acceptance(tmp_path: Path, runner_temp: Path, sha: str, overrides: dict[str, str], github_sha: str):
    """The acceptance step's command run with the gate's outputs resolved, `overrides` replacing some."""
    command = _resolve(_step_run(PUBLISH, CHECK_JOB, ACCEPTANCE_STEP), sha, overrides)
    command = command.replace("python -", f'"{sys.executable}" -', 1)
    environment = {"GITHUB_SHA": github_sha, "RUNNER_TEMP": str(runner_temp)}
    return _run_bash(command, tmp_path, environment)


def test_the_acceptance_read_over_the_record_the_enumeration_writes_refuses_each_differing_value(tmp_path):
    """Property: run over the record the publish workflow's own enumeration step writes, the acceptance
    read exits 0 when the gate's run id, attempt and commit and `$GITHUB_SHA` all match the record,
    and exits non-zero when any single one of the four is changed.

    Class: NEW CAPABILITY — plausible wrong implementations are a comparison that ignores one field
    (the attempt, which the enumeration stores as an integer and the gate publishes as a string, is
    the one a type-strict comparison would refuse on a match or a loose one would skip), a program
    that prints a refusal and exits 0, and one that compares the commit against the gate's value
    alone and never against `$GITHUB_SHA`. The control arm is the must-pass match; each changed value
    is a must-refuse arm positioned on a different field of the record.

    Reachability: the record is written by the real enumeration script through the real step, so a
    value type or key the script writes and the program does not read the same way shows up here;
    artifact: the step's `run:` text and `scripts/enumerate_release.py`.

    Why: the enumeration is the byte record every later step and the Release job read, and this is
    the only step holding it to the run the gate read; a field it does not compare is one the record
    can lie about.

    Both-states anchor: the verdict is the exit status over a record and four values, none of which
    is wording the step introduces.

    Invocation isolated: the step's command is run unchanged; the named change that greens a refused
    row is making the step skip that field.

    Killed by: dropping a field from the comparison, always exiting 0, or dropping `$GITHUB_SHA`.
    Second correct implementation: the same comparison over a dict built in another order, or in a
    script file the step calls, which gives the same exit statuses.
    """
    sha, runner_temp = _write_record(tmp_path)
    other = "0" * 40
    control = _acceptance(tmp_path, runner_temp, sha, {}, sha)
    assert control.returncode == 0, (control.stdout, control.stderr)
    cases = {
        "run_id": ({"whole_suite_run_id": "9002"}, sha),
        "run_attempt": ({"whole_suite_run_attempt": "3"}, sha),
        "gate_commit": ({"whole_suite_commit": other}, sha),
        "github_sha": ({}, other),
    }
    for field, (overrides, github_sha) in cases.items():
        completed = _acceptance(tmp_path, runner_temp, sha, overrides, github_sha)
        assert completed.returncode != 0, (field, completed.stdout, completed.stderr)


def test_the_acceptance_read_refuses_a_record_that_names_no_run(tmp_path):
    """Property: a record with no `whole_suite_run`, or with the key null, makes the acceptance read exit non-zero.

    Class: NEW CAPABILITY — the plausible wrong implementation is a program that indexes the key and
    crashes only on a missing one, or one that treats an absent run as "nothing to compare" and
    passes. Positioned differently from the value-mismatch arms: the violation here is the record's
    shape. The unmodified record is the must-pass control.

    Reachability: a record written by an enumerator that lost the key, or by hand; artifact:
    `scripts/enumerate_release.py` writes the key today, so this is the absence a later change could
    introduce.

    Why: an absent run identity is the clearest case of a record that is not evidence of this release.

    Both-states anchor: turns on the record's keys, not on wording.

    Invocation isolated: the step's command runs unchanged; the named change that greens it is
    skipping the comparison when the key is absent.

    Killed by: treating an absent key as a match. Second correct implementation: a program that
    refuses with a message and exits 1, or one that raises, which both exit non-zero.
    """
    sha, runner_temp = _write_record(tmp_path)
    record_path = runner_temp / "release-enumeration" / "release-enumeration.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert _acceptance(tmp_path, runner_temp, sha, {}, sha).returncode == 0
    for mutation in ("absent", "null"):
        mutated = dict(record)
        if mutation == "absent":
            del mutated["whole_suite_run"]
        else:
            mutated["whole_suite_run"] = None
        record_path.write_text(json.dumps(mutated), encoding="utf-8")
        completed = _acceptance(tmp_path, runner_temp, sha, {}, sha)
        assert completed.returncode != 0, (mutation, completed.stdout, completed.stderr)


def _install_commands(text: str) -> list[str]:
    """The Whole suite install step's pip commands, split on the ` && ` that joins them."""
    document = yaml.safe_load(text)
    steps = document["jobs"]["whole-suite"]["steps"]
    named = [step for step in steps if step.get("name") == INSTALL_STEP]
    assert len(named) == 1, len(named)
    return [part.strip() for part in str(named[0]["run"]).split("&&")]


def test_the_whole_suite_installs_the_lock_before_the_project_and_the_group_under_hashes():
    """Property: in the Whole suite install line, the command naming `requirements.txt` carries
    `--require-hashes` and comes before the command that installs the project and the command that
    installs the group.

    Class: NEW CAPABILITY — plausible wrong implementations are the lock installed after the group,
    where the group's pins have already chosen the runtime versions and the lock is then a no-op or a
    silent replacement, and the lock installed without `--require-hashes`, where the bytes are not the
    ones the lock names. Reorder and drop-the-flag are the two must-refuse positions.

    Reachability: each is a one-line edit of the workflow; artifact: `whole-suite.yml`.

    Why: the claim of the chunk is that the versions this job tests are the bytes the release ships;
    that holds only if the lock is applied first and under hashes.

    Both-states anchor: turns on the commands' relative position and the flag, which any correct line
    keeps.

    Invocation isolated: the line is read from the file; the named change that greens a reordered
    line is moving the command back.

    Killed by: reordering the commands or dropping the flag. Second correct implementation: four
    separate `run:` lines in a block scalar, if the whole-suite module's reader were changed to read
    them, which this test reads through the same split.
    """
    commands = _install_commands(WHOLE_SUITE.read_text(encoding="utf-8"))
    lock = [i for i, command in enumerate(commands) if "requirements.txt" in command]
    project = [i for i, command in enumerate(commands) if re.search(r"\s-e\s+\.(\s|$)", command)]
    group = [i for i, command in enumerate(commands) if "--group test" in command]
    assert len(lock) == len(project) == len(group) == 1, (lock, project, group, commands)
    assert "--require-hashes" in commands[lock[0]], commands[lock[0]]
    assert lock[0] < project[0] and lock[0] < group[0], commands


def _requirement_blocks(text: str) -> list[tuple[str, list[str]]]:
    """Each requirement in a `uv pip compile` requirements file as (specifier line, its hash lines)."""
    blocks: list[tuple[str, list[str]]] = []
    for line in text.replace("\\\n", " ").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        head, *hashes = stripped.split(" --hash=")
        blocks.append((head.strip(), hashes))
    return blocks


def test_every_requirement_in_the_committed_lock_is_pinned_and_carries_a_hash():
    """Property: each requirement in the committed `requirements.txt` is pinned with `==` and carries at
    least one `--hash`, so `pip install --require-hashes -r requirements.txt` does not refuse the file.

    Class: NEW CAPABILITY — the plausible wrong implementation is a lock rendered without
    `--generate-hashes` or with a requirement left as a range, on which `--require-hashes` refuses at
    install time on the runner and nowhere earlier. The real file at this tip is the must-pass arm; a
    synthetic unhashed line, run through the same reader, is the must-refuse control.

    Reachability: `render_lock.py` is the only writer and renders with `--generate-hashes`; a renderer
    change or hand edit that dropped it reaches this; artifact: `scripts/render_lock.py`.

    Why: the whole-suite install line applies the file under `--require-hashes`, so an unhashed or
    unpinned entry fails the job that gates every release.

    Both-states anchor: turns on each line's `==` and hash presence, a property of the file under any
    correct renderer.

    Invocation isolated: the file is read directly; the named change that greens a refused file is
    adding the hashes.

    Killed by: a requirement with no hash or no `==`. Second correct implementation: a lock listing
    the same pins in another order, or a hash per line rather than per requirement.
    """
    blocks = _requirement_blocks(REQUIREMENTS.read_text(encoding="utf-8"))
    assert blocks, "the committed lock lists no requirement, so the check measured nothing"
    for specifier, hashes in blocks:
        assert re.match(r"^[A-Za-z0-9_.-]+(\[[^\]]*\])?==\S+", specifier), specifier
        assert hashes, specifier
    control = _requirement_blocks("numpy==2.4.6\nscipy>=1\n")
    assert [bool(hashes) for _, hashes in control] == [False, False]


def _pins(requirements: list[str]) -> dict[str, str]:
    """Distribution name (lower-cased, `_` as `-`) to the version of each `name==version` entry."""
    pinned: dict[str, str] = {}
    for requirement in requirements:
        match = re.match(r"^([A-Za-z0-9_.-]+)(?:\[[^\]]*\])?==([^\s;]+)", requirement.strip())
        if match:
            pinned[re.sub(r"[-_.]+", "-", match.group(1)).lower()] = match.group(2)
    return pinned


def test_a_distribution_in_both_the_lock_and_the_test_group_has_one_version():
    """Property: every distribution named by both `requirements.txt` and the `test` group is pinned to the
    same version in each, so the group install that follows the lock install replaces nothing.

    Class: NEW CAPABILITY — the plausible wrong implementation is a lock rendered from a test group
    that was later edited (a bumped `numpy` pin), after which `--group test` silently reinstalls the
    distribution over the locked one without hashes and the job tests bytes the release does not
    ship. The tip's own pair is the must-pass arm; the overlap is measured nonzero as the control, so
    a vacuous intersection cannot read as agreement.

    Reachability: a one-line edit of either file; artifact: `pyproject.toml` and `requirements.txt`.

    Why: the chunk's claim is that the Whole suite job tests the versions the release ships; the
    second install step is where a disagreement would undo it, unseen, because pip honours the later
    pin.

    Both-states anchor: turns on version equality over the shared names, which holds under any lock
    rendered from the group.

    Invocation isolated: the files are read directly; the named change that greens a refused pair is
    re-rendering the lock or reverting the pin.

    Killed by: editing one side's pin. Second correct implementation: a lock that lists more
    distributions than the group, or a group that lists more than the lock, which the intersection
    does not read.
    """
    declaration = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    group = _pins([str(member) for member in declaration["dependency-groups"]["test"] if isinstance(member, str)])
    lock = _pins([specifier for specifier, _ in _requirement_blocks(REQUIREMENTS.read_text(encoding="utf-8"))])
    shared = sorted(set(group) & set(lock))
    assert shared, "no distribution is in both, so the check measured nothing"
    disagree = {name: (lock[name], group[name]) for name in shared if lock[name] != group[name]}
    assert not disagree, disagree
