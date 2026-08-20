"""Unit tests for the anonymization guard."""

from __future__ import annotations

import subprocess
from pathlib import Path

import scripts.check_anonymization as guard  # repo root is on sys.path under pytest


def _init_repo(tmp_path: Path, files: dict[str, str]) -> Path:
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    (tmp_path / "scripts").mkdir(parents=True, exist_ok=True)
    (tmp_path / "scripts" / "anonymization_blocklist.txt").write_text(
        "# test blocklist\nzzsynthacct\nzz-synthetic-estate-repo\n",
        encoding="utf-8",
    )
    for rel, content in files.items():
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=tmp_path, check=True)
    return tmp_path


def test_planted_token_fails(tmp_path: Path, capsys) -> None:
    root = _init_repo(tmp_path, {"src/leak.py": "account = 'zzsynthacct'\n"})
    rc = guard.main(["--root", str(root)])
    err = capsys.readouterr().err
    assert rc == 1
    assert "src/leak.py" in err
    # The finding is reported by blocklist INDEX, never by the token text: a
    # failing run of this guard is public (the workflow's logs are), so printing
    # the identifier would disclose the very string the guard exists to suppress.
    assert "blocklist entry #" in err
    assert "zzsynthacct" not in err


def test_clean_tree_passes(tmp_path: Path) -> None:
    root = _init_repo(tmp_path, {"src/ok.py": "import multidriver_swg\nx = 1\n"})
    assert guard.main(["--root", str(root)]) == 0


def test_public_prefix_not_false_positive(tmp_path: Path) -> None:
    # The public repo/package name appears constantly here; the PRIVATE estate
    # name is a strict extension of it. Whole-word matching on the longer
    # literal must never fire on the shorter public one.
    content = (
        "import multidriver_swg\n# the multidriver-swg repo\n# see the multidriver-swg README\nmultidriver_swg.run()\n"
    ) * 50
    root = _init_repo(tmp_path, {"src/public.py": content})
    assert guard.main(["--root", str(root)]) == 0


def test_estate_name_is_caught(tmp_path: Path, capsys) -> None:
    # The converse of the test above: the private estate name, when it DOES
    # appear, must fail. This is what enforces the one-way reference invariant.
    root = _init_repo(
        tmp_path,
        {"src/leak.py": "DATA_ROOT = '~/dev/zz-synthetic-estate-repo/configs'\n"},
    )
    rc = guard.main(["--root", str(root)])
    err = capsys.readouterr().err
    assert rc == 1
    assert "src/leak.py" in err
    assert "blocklist entry #" in err
    assert "zz-synthetic-estate-repo" not in err


def test_guard_imports_nothing_from_src() -> None:
    # Independence invariant: the guard reads the blocklist, not constants.
    src = Path(guard.__file__).read_text(encoding="utf-8")
    assert "import multidriver_swg" not in src
    assert "from multidriver_swg" not in src


def test_blocklist_is_not_empty() -> None:
    # Vacuous-control guard: a blocklist reduced to comments makes every scan
    # match nothing and exit 0. Measured 2026-08-17 before load_blocklist began
    # failing closed. This pins the real blocklist, not a fixture.
    repo_root = Path(guard.__file__).resolve().parent.parent
    tokens = guard.load_blocklist(repo_root / "scripts" / "anonymization_blocklist.txt")
    assert tokens, "anonymization blocklist defines zero tokens — the guard would pass everything"


def test_hook_entry_is_invocable_as_configured() -> None:
    """The pre-commit config and the filesystem must agree about how the guard runs.

    This is the test that would have caught the 2026-08-17 defect: ruff-format
    dropped the guard's exec bit during the commit, and a `language: script` hook
    then exited 1 with "is not executable" — red for the wrong reason, never
    scanning. The scanner tests all passed, because they call main() in-process.

    Line-scan rather than a YAML parse: the project env does not exist yet and
    this suite should not be what forces a pyyaml dependency. Upgrade to a real
    parse once the env is authored.
    """
    import shutil as _shutil

    repo_root = Path(guard.__file__).resolve().parent.parent
    lines = (repo_root / ".pre-commit-config.yaml").read_text(encoding="utf-8").splitlines()
    start = next(i for i, ln in enumerate(lines) if ln.strip() == "- id: anonymization-guard")
    block = lines[start : start + 6]
    entry = next(ln.split("entry:", 1)[1].strip() for ln in block if ln.strip().startswith("entry:"))
    language = next(ln.split("language:", 1)[1].strip() for ln in block if ln.strip().startswith("language:"))

    if language == "system":
        interpreter = entry.split()[0]
        assert _shutil.which(interpreter), f"hook entry interpreter {interpreter!r} not on PATH"
        script = repo_root / entry.split()[1]
        assert script.is_file(), f"hook entry script {script} does not exist"
    elif language == "script":
        script = repo_root / entry.split()[0]
        assert script.is_file(), f"hook entry script {script} does not exist"
        assert script.stat().st_mode & 0o111, (
            f"{script} is not executable, but the hook declares language: script — "
            "the hook will exit non-zero without ever scanning"
        )
    else:
        raise AssertionError(f"unhandled hook language {language!r} — extend this test")


def _baseline_repo(tmp_path: Path) -> Path:
    """A repo with one accepted finding already in the baseline."""
    root = _init_repo(tmp_path, {"src/leak.py": "acct = 'zzsynthacct'\n"})
    line = "acct = 'zzsynthacct'"
    fp = guard.line_fingerprint(line)
    (root / "scripts" / "anonymization_baseline.txt").write_text(f"{fp}  src/leak.py  zzsynthacct\n", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=root, check=True)
    return root


def test_baseline_accepts_the_recorded_finding(tmp_path: Path) -> None:
    root = _baseline_repo(tmp_path)
    baseline = root / "scripts" / "anonymization_baseline.txt"
    assert guard.main(["--root", str(root), "--baseline", str(baseline)]) == 0


def test_baseline_does_not_leak_to_another_path(tmp_path: Path) -> None:
    # The acceptance is scoped to one path. The same token elsewhere still fires.
    root = _baseline_repo(tmp_path)
    (root / "src" / "other.py").write_text("acct = 'zzsynthacct'\n", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=root, check=True)
    baseline = root / "scripts" / "anonymization_baseline.txt"
    assert guard.main(["--root", str(root), "--baseline", str(baseline)]) == 1


def test_acceptance_lapses_when_the_line_changes(tmp_path: Path) -> None:
    # The property that distinguishes a content-pinned baseline from a path
    # exclusion: edit the accepted line and the acceptance dies with it.
    root = _baseline_repo(tmp_path)
    (root / "src" / "leak.py").write_text("acct = 'zzsynthacct'  # moved\n", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=root, check=True)
    baseline = root / "scripts" / "anonymization_baseline.txt"
    assert guard.main(["--root", str(root), "--baseline", str(baseline)]) == 1


def test_stale_baseline_entry_fails_closed(tmp_path: Path) -> None:
    # A row protecting nothing is reported, not silently kept.
    root = _baseline_repo(tmp_path)
    (root / "src" / "leak.py").write_text("acct = 'redacted'\n", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=root, check=True)
    baseline = root / "scripts" / "anonymization_baseline.txt"
    assert guard.main(["--root", str(root), "--baseline", str(baseline)]) == 1
