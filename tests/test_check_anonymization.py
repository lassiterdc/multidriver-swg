"""Unit tests for the anonymization guard."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

import scripts.check_anonymization as guard  # repo root is on sys.path under pytest


def _init_repo(tmp_path: Path, files: dict[str, str]) -> Path:
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    (tmp_path / "scripts").mkdir(parents=True, exist_ok=True)
    (tmp_path / ".gitignore").write_text("/scripts/anonymization_blocklist.local.txt\n", encoding="utf-8")
    (tmp_path / "scripts" / "anonymization_blocklist.local.txt").write_text(
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
    assert "carrier entry #" in err
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
    assert "carrier entry #" in err
    assert "zz-synthetic-estate-repo" not in err


def test_guard_imports_nothing_from_src() -> None:
    # Independence invariant: the guard reads the blocklist, not constants.
    src = Path(guard.__file__).read_text(encoding="utf-8")
    assert "import multidriver_swg" not in src
    assert "from multidriver_swg" not in src


def test_tracked_control_files_carry_no_tokens() -> None:
    # INVERTED from the pre-consolidation vacuous-control test. That test pinned
    # "the carrier is non-empty"; after consolidation the carrier is private, that
    # invariant is enforced at runtime by load_carrier's exit-2 path (which fires
    # per scan rather than per suite run), and a test following the carrier could
    # only run where the carrier exists -- so in CI it would SKIP, which is a green
    # that means nothing. This pins the invariant that has no other pin in pytest:
    # the two tracked control files carry prose and nothing else.
    repo_root = Path(guard.__file__).resolve().parent.parent
    for rel in ("scripts/anonymization_blocklist.txt", "scripts/anonymization_baseline.txt"):
        path = repo_root / rel
        if not path.is_file():
            continue
        payload = [
            line
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
        assert not payload, f"{rel} carries {len(payload)} non-comment line(s); it must be prose-only"


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
    (root / "scripts" / "anonymization_baseline.txt").write_text(f"{fp}  src/leak.py  1\n", encoding="utf-8")
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


# --- count-only report mode ---------------------------------------------------


@pytest.fixture(autouse=True)
def _no_runner_env(monkeypatch) -> None:
    # The guard forces count-only output whenever GITHUB_ACTIONS=true. Clear it so
    # every test states its mode itself and the suite reads the same on a runner.
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)


def _lines(captured) -> list[str]:
    return [ln for ln in (captured.out + captured.err).splitlines() if ln]


def test_count_only_flag_prints_one_count_line(tmp_path: Path, capsys) -> None:
    """A finding under --count-only prints one line of counts and exits 1.

    Class: a tree with findings, asked for count-only output by the flag. Required
    because a CI log of a public repository is public, and a finding's path and line
    point at the public line holding the private token. Killed by printing any
    located line in count-only mode. A second correct implementation, one that
    writes the count line to stderr instead of stdout, still passes.
    """
    root = _init_repo(tmp_path, {"src/leak.py": "account = 'zzsynthacct'\n"})
    rc = guard.main(["--root", str(root), "--count-only"])
    assert rc == 1
    assert _lines(capsys.readouterr()) == [
        "anonymization-guard: 1 finding(s), 0 stale ledger row(s); carrier 2 token(s), ledger 0 row(s)"
    ]


def test_runner_env_alone_forces_count_only(tmp_path: Path, capsys, monkeypatch) -> None:
    """GITHUB_ACTIONS=true alone forces count-only output, with no flag given.

    Class: an invocation on a GitHub runner that omits --count-only. Required because
    a workflow that forgets the flag would otherwise print locations into a public
    log. Killed by reading only the flag. A second correct implementation, one that
    also honours another runner variable, still passes.
    """
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    root = _init_repo(tmp_path, {"src/leak.py": "account = 'zzsynthacct'\n"})
    rc = guard.main(["--root", str(root)])
    text = "\n".join(_lines(capsys.readouterr()))
    assert rc == 1
    assert "src/leak.py" not in text
    assert "carrier entry #" not in text
    assert "zzsynthacct" not in text


def test_count_only_clean_tree_exits_zero(tmp_path: Path, capsys) -> None:
    """A clean tree under --count-only prints its zero counts and exits 0.

    Class: no finding and no stale ledger row. Required so a green CI run still shows
    how many carrier tokens and ledger rows it loaded. Killed by printing nothing on
    a clean scan, or by exiting non-zero. A second correct implementation that
    builds the same line by a template string still passes.
    """
    root = _init_repo(tmp_path, {"src/ok.py": "x = 1\n"})
    assert guard.main(["--root", str(root), "--count-only"]) == 0
    assert _lines(capsys.readouterr()) == [
        "anonymization-guard: 0 finding(s), 0 stale ledger row(s); carrier 2 token(s), ledger 0 row(s)"
    ]


def test_count_only_stale_row_names_no_path(tmp_path: Path, capsys) -> None:
    """A stale ledger row under --count-only is counted, never named, and exits 1.

    Class: a ledger row whose accepted line no longer matches. Required because the
    detail-mode stale report prints the row's path and fingerprint. Killed by
    reporting stale rows as a located line, or by treating stale rows as clean. A
    second correct implementation that counts stale rows before scanning findings
    still passes.
    """
    root = _baseline_repo(tmp_path)
    (root / "src" / "leak.py").write_text("acct = 'redacted'\n", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=root, check=True)
    baseline = root / "scripts" / "anonymization_baseline.txt"
    rc = guard.main(["--root", str(root), "--baseline", str(baseline), "--count-only"])
    lines = _lines(capsys.readouterr())
    assert rc == 1
    assert lines == ["anonymization-guard: 0 finding(s), 1 stale ledger row(s); carrier 2 token(s), ledger 1 row(s)"]


def test_count_only_missing_carrier_exits_two_naming_no_path(tmp_path: Path, capsys) -> None:
    """A missing carrier under --count-only exits 2 with a fixed sentence and no path.

    Class: a run with no reachable carrier, which is what a fork's CI run presents.
    Required because exit 2 separates "cannot report" from a finding, and the
    detail-mode message names the carrier's path. Killed by exiting 0 or 1, or by
    printing the path. A second correct implementation that checks the carrier
    inside the scan rather than before it still passes.
    """
    root = _init_repo(tmp_path, {"src/ok.py": "x = 1\n"})
    (root / "scripts" / "anonymization_blocklist.local.txt").unlink()
    rc = guard.main(["--root", str(root), "--count-only"])
    text = "\n".join(_lines(capsys.readouterr()))
    assert rc == 2
    assert str(tmp_path) not in text
    assert text.startswith("anonymization-guard: ")


@pytest.mark.parametrize(
    ("source", "content"),
    [
        ("ledger, a malformed ordinal", "abc123  src/ok.py  zzcanaryordinal\n"),
        ("ledger, a row short of three fields", "zzcanaryfingerprint  src/zzcanarypath.py\n"),
        ("carrier, undecodable", None),
        ("tracked-file list, root not a repository", None),
    ],
)
def test_count_only_unreadable_input_prints_one_fixed_line(tmp_path: Path, capsys, source, content) -> None:
    """Every unreadable input under --count-only prints one fixed sentence and exits 2.

    Class: each exception source behind that sentence (a malformed ordinal, a ledger
    row short of three fields, an undecodable carrier, a root that is not a
    repository), each carrying a canary cell. Required because an exception's own
    text can quote a carrier or ledger cell into a public log. A row short of three
    fields raises SystemExit quoting the raw row, not an Exception, so this pins the
    SystemExit member of the guard's except clause; narrowing it to Exception kills
    two arms. A second correct implementation that validates each input before
    reading it, raising nothing, still passes.
    """
    if source.startswith("tracked-file list"):
        root = tmp_path
        (root / "scripts").mkdir()
        (root / "scripts" / "anonymization_blocklist.local.txt").write_text("zzsynthacct\n", encoding="utf-8")
        argv = ["--root", str(root), "--count-only"]
    else:
        root = _init_repo(tmp_path, {"src/ok.py": "x = 1\n"})
        argv = ["--root", str(root), "--count-only"]
        if source.startswith("carrier"):
            (root / "scripts" / "anonymization_blocklist.local.txt").write_bytes(b"zzsynthacct\n\xffzzcanary\n")
        else:
            bad = root / "scripts" / "bad_ledger.txt"
            bad.write_text(content, encoding="utf-8")
            argv += ["--baseline", str(bad)]
    rc = guard.main(argv)
    assert rc == 2
    assert _lines(capsys.readouterr()) == [
        "anonymization-guard: the carrier, the ledger or the tracked-file list could not be read"
    ]


@pytest.mark.parametrize("extra", [["--list"], ["--list", "--reveal"]])
def test_count_only_refuses_list_and_reveal(tmp_path: Path, capsys, extra) -> None:
    """--list and --list --reveal are refused in count-only mode, exit 2, no token.

    Class: the listing options combined with count-only. Required because --reveal
    prints token text, and the CI job must run the guard in scan mode only. Killed
    by honouring --list in count-only mode. A second correct implementation that
    refuses only --reveal and makes --list print counts would fail this test, so
    the refusal of --list is part of the contract, not an implementation choice.
    """
    root = _init_repo(tmp_path, {"src/ok.py": "x = 1\n"})
    rc = guard.main(["--root", str(root), "--count-only", *extra])
    text = "\n".join(_lines(capsys.readouterr()))
    assert rc == 2
    assert "zzsynthacct" not in text


# --- the main-checkout fallback for a linked worktree -------------------------
#
# The carrier and ledger are gitignored links that exist in ONE checkout of a
# repository. A linked worktree of the same repository has neither, so these tests
# build that layout in tmp_path: a main checkout holding the carrier (and, where a
# test needs it, a ledger) and a worktree added from it with `git worktree add`.


def _git(cwd: Path, *args: str) -> None:
    """Run git with a throwaway identity, so a commit works on a machine with none configured."""
    subprocess.run(
        ["git", "-c", "user.name=guard-test", "-c", "user.email=guard-test@example.invalid", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
    )


def _main_and_worktree(tmp_path: Path, files: dict[str, str]) -> tuple[Path, Path]:
    """A committed main checkout holding the carrier, and a linked worktree holding no carrier and no ledger."""
    main = tmp_path / "main"
    main.mkdir(parents=True)
    # The tracked policy file keeps scripts/ in the commit, as in the real repository;
    # a directory holding only ignored files would not exist in the worktree at all.
    _init_repo(main, {"scripts/anonymization_blocklist.txt": "# policy only\n", **files})
    with (main / ".gitignore").open("a", encoding="utf-8") as handle:
        handle.write("/scripts/anonymization_ledger.local.txt\n")
    _git(main, "add", "-A")
    _git(main, "commit", "-q", "--no-verify", "-m", "init")
    worktree = tmp_path / "worktree"
    _git(main, "worktree", "add", "-q", str(worktree))
    assert not (worktree / "scripts" / guard.LOCAL_SUPPLEMENT).exists()
    return main, worktree


def _accept_leak(checkout: Path) -> None:
    """Write a ledger in this checkout accepting the one line `src/leak.py` holds, for the carrier's first token."""
    fp = guard.line_fingerprint("acct = 'zzsynthacct'")
    (checkout / "scripts" / guard.LOCAL_LEDGER).write_text(f"{fp}  src/leak.py  1\n", encoding="utf-8")


def _exit_status(argv: list[str]) -> int:
    """The guard's exit status, whether main() returns it or raises it (a detail-mode carrier refusal raises)."""
    try:
        return guard.main(argv)
    except SystemExit as exc:
        assert isinstance(exc.code, int)
        return exc.code


_LEAK = {"src/leak.py": "acct = 'zzsynthacct'\n"}


def test_a_worktree_without_a_carrier_reads_the_main_checkouts(tmp_path: Path, capsys) -> None:
    """A worktree with no carrier of its own scans with the main checkout's carrier.

    Class: any linked worktree of a repository whose main checkout holds the
    carrier and whose own scripts directory holds none -- the position of a Build
    worktree and of a landing's throwaway checkout. Required because without it
    the guard exits 2 there on a machine that has the carrier, so a commit from
    any worktree has to skip the hook. The tree carries a planted token so that
    the verdict proves the tokens were LOADED: exit 1 naming the file, where a
    guard that found no carrier exits 2 and one that loaded no tokens finds
    nothing. Killed by removing the fallback (exit 2). A second correct
    implementation that finds the main checkout from `git worktree list` rather
    than from the common directory still passes.
    """
    _main, worktree = _main_and_worktree(tmp_path, _LEAK)
    rc = guard.main(["--root", str(worktree)])
    err = capsys.readouterr().err
    assert rc == 1
    assert "src/leak.py" in err
    assert "zzsynthacct" not in err


def test_a_clean_worktree_without_a_carrier_passes_on_the_main_checkouts(tmp_path: Path) -> None:
    """A clean worktree with no carrier of its own exits 0 through the main checkout's carrier.

    Class: the same layout as the test above with no planted token, which is the
    everyday commit from a worktree. Required because that commit must pass the
    hook unskipped. Killed by removing the fallback (exit 2). A second correct
    implementation that resolves the main checkout through `git worktree list`
    still passes.
    """
    _main, worktree = _main_and_worktree(tmp_path, {"src/ok.py": "x = 1\n"})
    assert guard.main(["--root", str(worktree)]) == 0


def test_no_carrier_in_either_checkout_still_fails_closed(tmp_path: Path, capsys) -> None:
    """With no carrier in the worktree or the main checkout, the guard exits 2.

    Class: a machine that never linked the carrier, scanned from a worktree.
    Required because the fallback must not turn "no ground truth anywhere" into a
    green scan. Killed by a fallback that treats an unreachable carrier as an empty
    token list (exit 0). A second correct implementation that checks the main
    checkout before this one when this one lacks the file still passes.
    """
    main, worktree = _main_and_worktree(tmp_path, _LEAK)
    (main / "scripts" / guard.LOCAL_SUPPLEMENT).unlink()
    rc = _exit_status(["--root", str(worktree)])
    assert rc == 2
    assert "no carrier is reachable" in capsys.readouterr().err


def test_a_worktree_with_its_own_carrier_reads_no_ledger_from_the_main_checkout(tmp_path: Path) -> None:
    """A worktree holding its own carrier and no ledger does not borrow the main checkout's ledger.

    Class: a worktree whose carrier exists and whose ledger does not, while the main
    checkout's ledger accepts the very line the worktree's tree carries. Required
    because a ledger row is keyed on a carrier ORDINAL, so a ledger read beside
    another checkout's carrier can accept a different token than the one it was
    recorded for; both must come from one directory. The control scan of the main
    checkout shows the ledger does accept that line there, so the red in the
    worktree is the ledger's absence and not a ledger that accepts nothing. Killed
    by falling back for the ledger independently of the carrier (exit 0). A second
    correct implementation that resolves the directory once and passes it down
    explicitly, rather than rewriting the anchor path, still passes.
    """
    main, worktree = _main_and_worktree(tmp_path, _LEAK)
    _accept_leak(main)
    (worktree / "scripts" / guard.LOCAL_SUPPLEMENT).write_text(
        "zzsynthacct\nzz-synthetic-estate-repo\n", encoding="utf-8"
    )
    assert guard.main(["--root", str(main)]) == 0
    assert guard.main(["--root", str(worktree)]) == 1


def test_a_present_empty_carrier_does_not_fall_back(tmp_path: Path, capsys) -> None:
    """A carrier that exists in the worktree but defines no tokens exits 2 and is not replaced.

    Class: a worktree carrier reduced to comments or blank lines while the main
    checkout's carrier is intact. Required because only ABSENCE triggers the
    fallback: a broken carrier silently replaced by another checkout's copy would
    hide the breakage. Killed by a fallback keyed on the token count rather than
    on the file's existence (exit 1, the main carrier's finding). A second correct
    implementation that tests `is_file()` rather than `exists()` still passes.
    """
    _main, worktree = _main_and_worktree(tmp_path, _LEAK)
    (worktree / "scripts" / guard.LOCAL_SUPPLEMENT).write_text("# nothing listed\n\n", encoding="utf-8")
    rc = _exit_status(["--root", str(worktree)])
    assert rc == 2
    assert "no carrier is reachable" in capsys.readouterr().err


def test_an_explicit_blocklist_anchor_does_not_fall_back(tmp_path: Path) -> None:
    """An explicit `--blocklist` whose directory holds no carrier exits 2, whatever the main checkout holds.

    Class: a caller naming the anchor directory itself, in a worktree whose main
    checkout has the carrier. Required because an option that names a directory
    must read that directory; reading another one instead would make the option
    untrustworthy. Killed by applying the fallback to an explicit anchor too
    (exit 1, the main carrier's finding). A second correct implementation that
    decides "explicit" from whether the option was given rather than from its
    value still passes.
    """
    _main, worktree = _main_and_worktree(tmp_path, _LEAK)
    anchor = worktree / "scripts" / "anonymization_blocklist.txt"
    assert _exit_status(["--root", str(worktree), "--blocklist", str(anchor)]) == 2


def test_the_fallback_is_anchored_on_the_scanned_repository_not_the_working_directory(
    tmp_path: Path, monkeypatch
) -> None:
    """Standing in a worktree whose main checkout has a carrier, a scan of ANOTHER repository with none exits 2.

    Class: any run whose working directory is a checkout of one repository while
    `--root` names a different repository -- which is what this test module does
    whenever it is run from a worktree, since every test scans a tmp_path
    repository. Required because a lookup from the working directory answers for
    the repository the caller stands in, so the other repository would be scanned
    with this machine's carrier and would stop failing closed. Killed by finding
    the main checkout from the working directory (`git rev-parse --git-common-dir`
    with no `-C`, joined with `scripts/`), which exits 0 here. A second correct
    implementation that anchors on `--root` instead of on the blocklist's
    directory still passes.
    """
    _main, worktree = _main_and_worktree(tmp_path / "carrying", {"src/ok.py": "x = 1\n"})
    other = tmp_path / "other"
    other.mkdir()
    _init_repo(other, {"src/ok.py": "x = 1\n"})
    (other / "scripts" / guard.LOCAL_SUPPLEMENT).unlink()
    monkeypatch.chdir(worktree)
    assert _exit_status(["--root", str(other)]) == 2
