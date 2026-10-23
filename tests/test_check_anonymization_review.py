"""Review tests for the anonymization guard's main-checkout fallback.

The guard runs as a pre-commit hook, and git runs a hook in a linked worktree
with `GIT_DIR` exported, naming that worktree's own git directory. A fallback
that inherits that variable finds the right repository and the wrong place in
it, so these tests run the guard in the environment the hook gives it rather
than in a test runner's clean one.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import scripts.check_anonymization as guard  # repo root is on sys.path under pytest


def _git(cwd: Path, *args: str) -> str:
    """Run git with a throwaway identity and return its standard output."""
    proc = subprocess.run(
        ["git", "-c", "user.name=guard-review", "-c", "user.email=guard-review@example.invalid", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
    )
    return proc.stdout.decode("utf-8").strip()


def _carrying_main_and_bare_worktree(tmp_path: Path) -> tuple[Path, Path]:
    """A committed main checkout holding a carrier and a planted token, and a linked worktree holding no carrier."""
    main = tmp_path / "main"
    (main / "scripts").mkdir(parents=True)
    (main / "src").mkdir()
    (main / ".gitignore").write_text(
        "/scripts/anonymization_blocklist.local.txt\n/scripts/anonymization_ledger.local.txt\n", encoding="utf-8"
    )
    # A tracked file keeps scripts/ in the commit, so the worktree has the directory the lookup starts from.
    (main / "scripts" / "anonymization_blocklist.txt").write_text("# policy only\n", encoding="utf-8")
    (main / "src" / "leak.py").write_text("acct = 'zzreviewacct'\n", encoding="utf-8")
    _git(main, "init", "-q")
    _git(main, "add", "-A")
    _git(main, "commit", "-q", "--no-verify", "-m", "init")
    (main / "scripts" / guard.LOCAL_SUPPLEMENT).write_text("zzreviewacct\n", encoding="utf-8")
    worktree = tmp_path / "worktree"
    _git(main, "worktree", "add", "-q", str(worktree))
    assert not (worktree / "scripts" / guard.LOCAL_SUPPLEMENT).exists()
    return main, worktree


def test_a_hook_in_a_worktree_still_reads_the_main_checkouts_carrier(tmp_path: Path, monkeypatch, capsys) -> None:
    """Run as git runs a pre-commit hook in a linked worktree, the guard still scans with the main checkout's carrier.

    Class: any run of the guard in a linked worktree with `GIT_DIR` (and
    `GIT_INDEX_FILE`) exported naming that worktree's own git directory, which
    is what git 2.43 exports to every hook it runs in a linked worktree, and so
    the position of every commit made from a Build worktree or a landing's
    throwaway checkout.

    Why it is required: with `GIT_DIR` set and no work tree named, git takes the
    directory it was pointed at as the top of the work tree, so the common
    directory comes back right and the path prefix comes back empty. A lookup
    that inherits the variable therefore looks for the carrier at the top of the
    main checkout rather than in its `scripts/` directory, finds none, and the
    hook exits 2 on a machine that holds the carrier. That is the failure the
    fallback exists to remove, reappearing in the one position it was built for.

    The tree carries a planted token, so the verdict shows the tokens were loaded:
    exit 1 naming the file, where a guard that found no carrier exits 2.

    Killed by: running the main-checkout lookup with the inherited environment
    (dropping the filtered `env` from its `git` call), which exits 2 here.

    Second correct implementation: one that removes every `GIT_`-prefixed
    variable from the lookup's environment, rather than the three location
    variables, still passes.
    """
    _main, worktree = _carrying_main_and_bare_worktree(tmp_path)
    worktree_git_dir = _git(worktree, "rev-parse", "--absolute-git-dir")
    monkeypatch.setenv("GIT_DIR", worktree_git_dir)
    monkeypatch.setenv("GIT_INDEX_FILE", str(Path(worktree_git_dir) / "index"))
    monkeypatch.delenv("GIT_WORK_TREE", raising=False)
    monkeypatch.delenv("GIT_COMMON_DIR", raising=False)
    # git runs a hook from the top of the worktree; the hook entry passes no --root.
    monkeypatch.chdir(worktree)
    rc = guard.main(["--root", str(worktree)])
    err = capsys.readouterr().err
    assert rc == 1
    assert "src/leak.py" in err
    assert "zzreviewacct" not in err
