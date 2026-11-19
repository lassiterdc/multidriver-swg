"""Tests that run the shell the Release job and the identifiers workflow carry, instead of reading it.

The tests in `tests/test_record_identifiers_workflow.py` read both workflows as text and assert what the steps SAY. These tests run what
the steps DO: the `run:` body of the notes step, of the tag-resolution step, of the recorder
command and of the commit step are lifted out of the workflow files and executed in a scratch
directory, so a heredoc that does not terminate once the YAML block is dedented, a `git add` that
fails on an unchanged record, an annotated tag resolving to the tag object rather than the commit,
or a flag the recorder does not accept each reds here and stays green under a text assertion.
"""

import hashlib
import importlib.util
import os
import re
import shlex
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
PUBLISH = REPO_ROOT / ".github" / "workflows" / "publish.yml"
RECORD = REPO_ROOT / ".github" / "workflows" / "record-identifiers.yml"
RECORDER = REPO_ROOT / "scripts" / "record_identifiers.py"

_STEP_NAME = re.compile(r"^(\s*)- name:\s*(.*?)\s*$")


def _run_block(path: Path, step_name: str) -> str:
    """The dedented `run:` body of the step named `step_name`, exactly as the runner hands it to bash."""
    lines = path.read_text(encoding="utf-8").splitlines()
    for index, line in enumerate(lines):
        matched = _STEP_NAME.match(line)
        if matched is None or matched.group(2).strip("'\"") != step_name:
            continue
        step_indent = len(matched.group(1))
        key_indent = step_indent + 2
        cursor = index + 1
        while cursor < len(lines):
            current = lines[cursor]
            stripped = current.strip()
            if stripped and len(current) - len(current.lstrip()) < key_indent:
                break
            if current.startswith(" " * key_indent + "run:"):
                tail = current[key_indent + len("run:") :].strip()
                if tail and not tail.startswith("|"):
                    return tail
                body: list[str] = []
                cursor += 1
                block_indent = None
                while cursor < len(lines):
                    row = lines[cursor]
                    if row.strip():
                        row_indent = len(row) - len(row.lstrip())
                        if row_indent <= key_indent:
                            break
                        block_indent = row_indent if block_indent is None else min(block_indent, row_indent)
                    body.append(row)
                    cursor += 1
                assert block_indent is not None, step_name
                return "\n".join(row[block_indent:] for row in body) + "\n"
            cursor += 1
    raise AssertionError(f"no run block for step {step_name!r} in {path.name}")


def _bash(script: str, cwd: Path, env: dict[str, str]) -> subprocess.CompletedProcess:
    merged = {**os.environ, **env}
    return subprocess.run(
        ["bash", "-e", "-o", "pipefail", "-c", script],
        cwd=cwd,
        env=merged,
        capture_output=True,
        text=True,
        check=False,
    )


def _git(cwd: Path, *arguments: str) -> str:
    done = subprocess.run(
        ["git", *arguments],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=True,
        env={**os.environ, "GIT_CONFIG_GLOBAL": "/dev/null"},
    )
    return done.stdout.strip()


def _recorder_module():
    spec = importlib.util.spec_from_file_location("record_identifiers_under_review", RECORDER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_notes_step_writes_notes_the_recorder_accepts_for_the_asset_it_attaches(tmp_path):
    """Property: running the notes step over an enumeration file yields notes whose only 64-hex run
    is that file's sha256, whose three licence statements are whole lines, and that the recorder's
    own digest reader accepts.

    Class: NEW CAPABILITY — plausible wrong implementations are a heredoc whose terminator keeps the
    block's indent (bash then reads to end of file and the step fails or swallows the file), a
    digest of another path, and notes carrying a second hash that would let a wrong asset pass.

    Why: the identifiers run refuses `asset-digest` unless the notes state the downloaded asset's
    digest, and that refusal arrives after the release is public.

    Killed by: an indented `NOTES` terminator, a digest not of the attached bytes, a stray hash.
    Second correct implementation: the statements in another order or another hashing command.
    """
    release_dir = tmp_path / "release-enumeration"
    release_dir.mkdir()
    payload = b'{"enumeration": "bytes the release ships"}\n'
    (release_dir / "release-enumeration.json").write_bytes(payload)
    script = _run_block(PUBLISH, "Write the release notes")
    done = _bash(script, tmp_path, {"RUNNER_TEMP": str(tmp_path)})
    assert done.returncode == 0, (done.stdout, done.stderr)

    notes = (tmp_path / "release-notes.md").read_text(encoding="utf-8")
    recorder = _recorder_module()
    expected = hashlib.sha256(payload).hexdigest()
    stated = {run.lower() for run in recorder.DIGEST_IN_TEXT.findall(notes)}
    assert stated == {expected}, (stated, notes)

    lines = notes.splitlines()
    assert any(line.startswith("- Source: AGPL-3.0-or-later") for line in lines), notes
    assert any(line.startswith("- Legacy corpus: MIT") for line in lines), notes
    assert any(line.startswith("- Data: none") and "no dataset" in line for line in lines), notes
    assert not any(line.startswith(" ") for line in lines), notes


def test_the_resolved_commit_of_an_annotated_tag_is_the_commit_and_not_the_tag_object(tmp_path):
    """Property: the resolve step writes `commit=` the commit an ANNOTATED tag names, which differs
    from the tag object's own id.

    Class: NEW CAPABILITY — the wrong implementation is `git rev-parse "$TAG"`, which for an
    annotated tag returns the tag object, a value the archive holds no revision under.

    Why: the recorder asks the archive about this commit, and a tag object's id draws `archive-locator`
    on a release that is public.

    Killed by: dropping the `^{commit}` peel. Second correct implementation: `git rev-list -n 1 "$TAG"`.
    """
    _git(tmp_path, "init", "-q", "-b", "develop")
    _git(tmp_path, "config", "user.email", "a@b.c")
    _git(tmp_path, "config", "user.name", "n")
    (tmp_path / "f").write_text("x")
    _git(tmp_path, "add", "f")
    _git(tmp_path, "commit", "-q", "-m", "c")
    _git(tmp_path, "tag", "-a", "v9.9.9", "-m", "release")
    commit = _git(tmp_path, "rev-parse", "HEAD")
    tag_object = _git(tmp_path, "rev-parse", "v9.9.9")
    assert commit != tag_object

    output = tmp_path / "github_output"
    script = _run_block(RECORD, "Resolve the tagged commit")
    done = _bash(script, tmp_path, {"TAG": "v9.9.9", "GITHUB_OUTPUT": str(output)})
    assert done.returncode == 0, (done.stdout, done.stderr)
    assert output.read_text(encoding="utf-8").strip() == f"commit={commit}"


def test_the_recorder_command_passes_flags_the_recorder_parses_and_binds_the_three_values(tmp_path):
    """Property: the workflow's recorder command, with the environment the step sets, parses under the
    recorder's own argument parser and binds tag, commit and origin to the step's three values.

    Class: NEW CAPABILITY — plausible wrong implementations are a renamed flag, an origin built from
    `github.repository` alone (no host, so the slug reader refuses it), and the commit taken from
    `github.sha`.

    Why: a flag the parser refuses exits 2 only on the first real dispatch.

    Killed by: any flag the recorder does not define or a missing required flag. Second correct
    implementation: the three values passed positionally through a wrapper, which this reads as a
    different command and would need this test widened.
    """
    command = _run_block(RECORD, "Record the identifiers")
    environment = {
        "TAG": "v1.2.3",
        "COMMIT": "a" * 40,
        "ORIGIN": "https://github.com/example/multidriver-swg",
    }
    expanded = command
    for name, value in environment.items():
        expanded = expanded.replace(f'"${name}"', value)
    tokens = shlex.split(expanded)
    assert tokens[:2] == ["python", "scripts/record_identifiers.py"], tokens
    recorder = _recorder_module()
    parsed = recorder._parse_arguments(tokens[2:])
    assert (parsed.tag, parsed.commit, parsed.origin) == ("v1.2.3", "a" * 40, environment["ORIGIN"])
    slug, problem = recorder._origin_slug(parsed.origin)
    assert slug == "example/multidriver-swg", (slug, problem)


@pytest.fixture()
def develop_checkout(tmp_path):
    """A clone of a bare remote whose `develop` holds a `CITATION.cff`, the state the commit step starts in."""
    remote = tmp_path / "remote.git"
    seed = tmp_path / "seed"
    work = tmp_path / "work"
    seed.mkdir()
    _git(tmp_path, "init", "-q", "--bare", "-b", "develop", str(remote))
    _git(seed, "init", "-q", "-b", "develop")
    _git(seed, "config", "user.email", "a@b.c")
    _git(seed, "config", "user.name", "n")
    (seed / "CITATION.cff").write_text("cff-version: 1.2.0\n")
    _git(seed, "add", "CITATION.cff")
    _git(seed, "commit", "-q", "-m", "seed")
    _git(seed, "remote", "add", "origin", str(remote))
    _git(seed, "push", "-q", "origin", "develop")
    _git(tmp_path, "clone", "-q", "-b", "develop", str(remote), str(work))
    return remote, work


def _write_record(work: Path, tag: str, citation: str, enumeration: str) -> None:
    (work / "CITATION.cff").write_text(citation)
    target = work / "releases" / tag
    target.mkdir(parents=True, exist_ok=True)
    (target / "release-enumeration.json").write_text(enumeration)


def test_the_commit_step_pushes_one_commit_then_pushes_nothing_when_dispatched_again(develop_checkout):
    """Property: with the recorder's two files written, the commit step pushes exactly one commit to
    `develop` carrying both; run again over unchanged files it exits 0 and pushes nothing.

    Class: NEW CAPABILITY — plausible wrong implementations are an unconditional `git commit`, which
    fails the second dispatch with "nothing to commit"; a `git add` over a path the first run never
    created; and a push of the checkout's detached head under another name.

    Why: the same tag is dispatched again after a refused run or a Zenodo delay, and a re-run that
    fails reads as a failed record when the record is already correct.

    Killed by: removing the staged-diff guard, staging a different path, pushing another ref.
    Second correct implementation: `git commit` guarded by `git status --porcelain` instead.
    """
    remote, work = develop_checkout
    script = _run_block(RECORD, "Commit the identifiers to develop")
    _write_record(work, "v1.2.3", "cff-version: 1.2.0\ndoi: 10.5281/zenodo.1\n", '{"a": 1}\n')

    first = _bash(script, work, {"TAG": "v1.2.3", "GIT_CONFIG_GLOBAL": "/dev/null"})
    assert first.returncode == 0, (first.stdout, first.stderr)
    after_first = _git(remote, "rev-list", "--count", "develop")
    assert after_first == "2", after_first
    pushed = _git(remote, "show", "--name-only", "--format=%s", "develop").splitlines()
    assert pushed[0] == "chore(release): record the identifiers for v1.2.3", pushed
    assert sorted(line for line in pushed[1:] if line) == [
        "CITATION.cff",
        "releases/v1.2.3/release-enumeration.json",
    ], pushed

    second = _bash(script, work, {"TAG": "v1.2.3", "GIT_CONFIG_GLOBAL": "/dev/null"})
    assert second.returncode == 0, (second.stdout, second.stderr)
    assert "already recorded" in second.stdout, second.stdout
    assert _git(remote, "rev-list", "--count", "develop") == after_first


def test_the_commit_step_stages_nothing_but_the_two_files(develop_checkout):
    """Property: a stray file in the checkout is left out of the commit the step pushes.

    Class: NEW CAPABILITY — the wrong implementation is `git add -A` or `git commit -a`, which
    would push the recorder's `CITATION.cff.tmp` or any build output into `develop`.

    Why: the commit is the release's identifier record and the workflow's token can write to the
    branch; the commit must hold the two files the recorder owns and nothing else.

    Killed by: widening the `git add`. Second correct implementation: `git add` of the two paths
    through a variable.
    """
    remote, work = develop_checkout
    script = _run_block(RECORD, "Commit the identifiers to develop")
    _write_record(work, "v1.2.3", "cff-version: 1.2.0\ndoi: 10.5281/zenodo.1\n", '{"a": 1}\n')
    (work / "stray.txt").write_text("not part of the record")
    (work / "CITATION.cff.tmp").write_text("leftover")

    done = _bash(script, work, {"TAG": "v1.2.3", "GIT_CONFIG_GLOBAL": "/dev/null"})
    assert done.returncode == 0, (done.stdout, done.stderr)
    pushed = _git(remote, "show", "--name-only", "--format=", "develop").splitlines()
    assert sorted(pushed) == ["CITATION.cff", "releases/v1.2.3/release-enumeration.json"], pushed
