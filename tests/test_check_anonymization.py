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
