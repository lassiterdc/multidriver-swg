"""Reviewer tests of the enumeration step: a step that names a gate output the gate never publishes.

`tests/test_publish_workflow.py` pins the enumeration step's flags as TEXT, and the text it pins for
`--run-commit` is `needs.release-gate.outputs.whole_suite_run_commit`. The release gate publishes
three outputs and that name is not one of them, so GitHub resolves the reference to an empty string,
the enumeration script reads `--run-commit ""` against `--commit $GITHUB_SHA`, refuses
`run-not-at-commit`, and the job fails on every release that passes the gate. Nothing in the file the
step's author wrote can see it, because the test copies the step's own spelling and compares the step with
itself.

Module guarded: `.github/workflows/publish.yml` (the `post-build-check` enumeration step), judged
against the gate's declared `outputs:` and against `scripts/enumerate_release.py`. One-time test: it
proves the enumeration step's change once and is deleted at the landing unless it is promoted.

Two rules, each a function of the workflow text and of the script it runs:

1. every `needs.release-gate.outputs.NAME` the workflow reads is a NAME the gate's `outputs:` declares;
2. the enumeration command, with each output reference resolved the way the runner resolves it (a
   declared output to its expression's value, an undeclared one to the empty string) and run over a
   throwaway checkout of one commit, writes the record and exits 0.
"""

from __future__ import annotations

import io
import json
import re
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PUBLISH = REPO_ROOT / ".github" / "workflows" / "publish.yml"
SCRIPT = REPO_ROOT / "scripts" / "enumerate_release.py"

GATE = "release-gate"
ENUMERATION_STEP = "Enumerate the released bytes"
OUTPUT_REFERENCE = re.compile(r"needs\." + re.escape(GATE) + r"\.outputs\.([A-Za-z0-9_-]+)")
EXPRESSION_REFERENCE = re.compile(r"\$\{\{\s*needs\." + re.escape(GATE) + r"\.outputs\.([A-Za-z0-9_-]+)\s*\}\}")


def _gate_outputs(text: str) -> dict[str, str]:
    """The gate job's declared `outputs:` as name -> expression, read from the indentation."""
    lines = text.splitlines()
    start = next(i for i, line in enumerate(lines) if line == f"  {GATE}:")
    outputs_at = next(i for i in range(start + 1, len(lines)) if lines[i] == "    outputs:")
    declared: dict[str, str] = {}
    for line in lines[outputs_at + 1 :]:
        if not line.startswith("      "):
            break
        name, _, expression = line.strip().partition(":")
        declared[name] = expression.strip()
    return declared


def _step_command(text: str, name: str) -> str:
    """The one-line `run:` of the step named `name`."""
    lines = text.splitlines()
    named = [i for i, line in enumerate(lines) if line.strip() == f"- name: {name}"]
    assert len(named) == 1, (name, named)
    for line in lines[named[0] + 1 :]:
        if line.strip().startswith("run:"):
            return line.strip()[len("run:") :].strip()
        if line.strip().startswith("- name:"):
            break
    raise AssertionError(f"step {name!r} has no run line")


def test_every_gate_output_the_workflow_reads_is_one_the_gate_publishes():
    """Property: each `needs.release-gate.outputs.NAME` anywhere in the workflow names an output the gate declares.

    Class: NEW CAPABILITY — the plausible wrong implementation is a reference spelled from the
    script's flag (`--run-commit` read as `whole_suite_run_commit`) and not from the gate's
    `outputs:` block, which resolves to the empty string with no error. Reachable on the real file:
    the enumeration step reads `whole_suite_run_commit` and the gate declares `whole_suite_commit`.

    Why: an undeclared output is not an error in GitHub Actions, it is an empty value, so the only
    place the mistake is visible before a release is a comparison of the references with the block.

    Killed by: any reference whose name the gate does not declare. Second correct implementation:
    the enumeration reading `whole_suite_commit`, or the gate renaming its output and every reader
    following, which this test does not distinguish.
    """
    text = PUBLISH.read_text(encoding="utf-8")
    declared = _gate_outputs(text)
    assert declared, "the gate declares no outputs, so the reader found no block"
    referenced = sorted(set(OUTPUT_REFERENCE.findall(text)))
    assert referenced, "no step reads a gate output, so the check measured nothing"
    undeclared = [name for name in referenced if name not in declared]
    assert not undeclared, (undeclared, sorted(declared))


def test_the_enumeration_command_resolved_as_the_runner_resolves_it_names_the_commit_it_archives(tmp_path):
    """Property: with each gate-output reference resolved as the runner resolves it, the enumeration
    step's own command exits 0 over one commit and writes the record naming that commit as the run's.

    Class: NEW CAPABILITY — the wrong implementation is the step that reads an undeclared output,
    which resolves to `""`, so `--run-commit` differs from `--commit` and the script refuses
    `run-not-at-commit` on a release that passed the gate. Must-fire: the real file at this tip.

    Why: the record's `whole_suite_run.commit` is what the gate's measured run is checked against, so
    the step has to hand the script a non-empty value equal to the tagged commit, and a unit test of
    the script alone cannot see a step that hands it nothing.

    Killed by: an empty or different `--run-commit`, a dropped flag, or a path the step cannot write.
    Second correct implementation: the step reading `needs.release-gate.outputs.whole_suite_commit`,
    or `$GITHUB_SHA` for the run commit, since the gate's own value is `github.sha`.
    """
    text = PUBLISH.read_text(encoding="utf-8")
    declared = _gate_outputs(text)
    sha = _make_checkout(tmp_path)
    dist = tmp_path / "dist"
    _make_dist(dist)
    runner_temp = tmp_path / "runner-temp"
    runner_temp.mkdir()

    def resolve(match: re.Match[str]) -> str:
        expression = declared.get(match.group(1), "")
        return {"${{ github.run_id }}": "123", "${{ github.run_attempt }}": "1", "${{ github.sha }}": sha}.get(
            expression, ""
        )

    command = EXPRESSION_REFERENCE.sub(resolve, _step_command(text, ENUMERATION_STEP))
    command = command.replace("python scripts/enumerate_release.py", f'"{sys.executable}" "{SCRIPT}"', 1)
    environment = {
        "GITHUB_SHA": sha,
        "GITHUB_REF_NAME": "v0.0.0",
        "RUNNER_TEMP": str(runner_temp),
        "PATH": __import__("os").environ["PATH"],
    }
    completed = subprocess.run(
        ["bash", "-c", command], cwd=tmp_path, env=environment, capture_output=True, text=True, check=False
    )
    assert completed.returncode == 0, (command, completed.stdout, completed.stderr)
    record = json.loads((runner_temp / "release-enumeration" / "release-enumeration.json").read_text())
    assert record["whole_suite_run"]["commit"] == sha, record["whole_suite_run"]


def _make_checkout(root: Path) -> str:
    """One commit in a throwaway repository at `root`, and its sha."""
    identity = ("-c", "user.name=Reviewer", "-c", "user.email=reviewer@example.invalid")
    for arguments in (("init", "-q"),):
        subprocess.run(["git", *arguments], cwd=root, check=True, capture_output=True)
    (root / "README.md").write_text("one file\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=root, check=True, capture_output=True)
    subprocess.run(["git", *identity, "commit", "-q", "-m", "one"], cwd=root, check=True, capture_output=True)
    shown = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True)
    return shown.stdout.strip()


def _make_dist(dist: Path) -> None:
    """A minimal sdist and wheel, the two files a release's dist directory holds."""
    dist.mkdir()
    payload = b"print('x')\n"
    with tarfile.open(dist / "pkg-0.0.0.tar.gz", "w:gz") as sdist:
        info = tarfile.TarInfo("pkg-0.0.0/pkg.py")
        info.size = len(payload)
        sdist.addfile(info, io.BytesIO(payload))
    with zipfile.ZipFile(dist / "pkg-0.0.0-py3-none-any.whl", "w") as wheel:
        wheel.writestr("pkg.py", payload)
