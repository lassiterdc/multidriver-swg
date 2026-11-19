"""Tests of the content scanner: the reading contract, the four classes and the two report forms.

The scanner is one core (`scripts/content_scan/core.py`) over four matcher modules. These tests
carry over every behaviour the three files this package's second chunk deletes test today -- the
anonymization guard's 26 test functions, the planning-reference guard's 8, and the hook test that
ran the anonymization guard as git runs it -- and add the public label-shape list the clone-carried
test needs, one sample per shape each public class declares.

Two things about how the samples below are written. The secret class's samples, and the machine
path sample, are assembled from adjacent string literals: a tracked file of this repository must
itself pass the scan it tests, and a whole PEM header or a home-directory path written out on one
line would red the guard that runs on every push. The planning-reference samples are split for the
same reason: the guard that this package replaces still scans this file, and a whole decision number would
make it report this test module on every commit. The VALUE of every sample is what a pattern sees;
the split is only in the source text.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

import scripts.content_scan.__main__ as scanner
import scripts.content_scan.core as core
import scripts.content_scan.identity as identity
import scripts.content_scan.machine_path as machine_path
import scripts.content_scan.planning_reference as planning_reference
import scripts.content_scan.secret as secret

SCANNER = Path(scanner.__file__).resolve().parent
ROOT = core.REPO_ROOT

# The files this package's second chunk deletes. They state the planning patterns and their own
# controls, which is why the guard they belong to never scanned them either -- so a scan of the
# real tree reads them as a reference it cannot fix. The filter removes nothing once that chunk
# lands, and nothing outside that pair is filtered.
SUPERSEDED = ("scripts/check_planning_references.py", "tests/test_planning_references.py")


# --- a throwaway repository --------------------------------------------------
#
# Every test that needs a tree builds one: a git repository, the synthetic private carrier, the
# ignore rules the real repository carries, and the files the test plants.


def _git(cwd: Path, *args: str) -> str:
    """Run git with a throwaway identity, so a commit works on a machine with none configured."""
    proc = subprocess.run(
        ["git", "-c", "user.name=scan-test", "-c", "user.email=scan-test@example.invalid", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
    )
    return proc.stdout.decode("utf-8").strip()


def _init_repo(tmp_path: Path, files: dict[str, str]) -> Path:
    """A git repository with the synthetic carrier staged, and `files` staged beyond it."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    _git(tmp_path, "init", "-q")
    (tmp_path / "scripts").mkdir(parents=True, exist_ok=True)
    (tmp_path / ".gitignore").write_text(
        "/scripts/anonymization_blocklist.local.txt\n/scripts/anonymization_ledger.local.txt\n", encoding="utf-8"
    )
    (tmp_path / "scripts" / identity.LOCAL_SUPPLEMENT).write_text(
        "# test blocklist\nzzsynthacct\nzz-synthetic-estate-repo\n", encoding="utf-8"
    )
    for rel, content in files.items():
        target = tmp_path / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    _git(tmp_path, "add", "-A")
    return tmp_path


@pytest.fixture(autouse=True)
def _no_runner_env(monkeypatch) -> None:
    # The scan forces count-only output whenever GITHUB_ACTIONS=true. Clear it so every test
    # states its mode itself and the suite reads the same on a runner.
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)


def _lines(captured) -> list[str]:
    return [line for line in (captured.out + captured.err).splitlines() if line]


def _classes(report: core.Report) -> dict[str, core.ClassReport]:
    return {found.name: found for found in report.classes}


def _only(name: str) -> core.Matcher:
    """The one matcher for a public class, by name."""
    return {
        "machine_path": machine_path,
        "planning_reference": planning_reference,
        "secret": secret,
    }[name].load()


# --- the identity class ------------------------------------------------------


def test_planted_token_fails(tmp_path: Path, capsys) -> None:
    root = _init_repo(tmp_path, {"src/leak.py": "account = 'zzsynthacct'\n"})
    rc = scanner.main(["--root", str(root)])
    err = capsys.readouterr().err
    assert rc == 1
    assert "src/leak.py" in err
    # The finding is reported by carrier ORDINAL, never by the token text: a failing run of this
    # scanner on a public repository is public, so printing the identifier would disclose the very
    # string the class exists to suppress.
    assert "carrier entry #" in err
    assert "zzsynthacct" not in err


def test_public_prefix_not_false_positive(tmp_path: Path) -> None:
    # The public repo/package name appears constantly here; the PRIVATE estate name is a strict
    # extension of it. Whole-word matching on the longer literal must never fire on the shorter
    # public one.
    content = (
        "import multidriver_swg\n# the multidriver-swg repo\n# see the multidriver-swg README\nmultidriver_swg.run()\n"
    ) * 50
    root = _init_repo(tmp_path, {"src/public.py": content})
    assert scanner.main(["--root", str(root)]) == 0


def test_estate_name_is_caught(tmp_path: Path, capsys) -> None:
    # The converse of the test above: the private estate name, when it DOES appear, must fail.
    # This is what enforces the one-way reference invariant.
    root = _init_repo(tmp_path, {"src/leak.py": "DATA_ROOT = '~/dev/zz-synthetic-estate-repo/configs'\n"})
    rc = scanner.main(["--root", str(root)])
    err = capsys.readouterr().err
    assert rc == 1
    assert "src/leak.py" in err
    assert "carrier entry #" in err
    assert "zz-synthetic-estate-repo" not in err


def test_the_scanner_imports_nothing_from_src() -> None:
    # Independence invariant: the scanner reads pattern sources, not the constants the scrub
    # edits. It also has to run before the project environment exists.
    for module in (scanner, core, identity, machine_path, planning_reference, secret):
        text = Path(module.__file__).read_text(encoding="utf-8")
        assert "import multidriver_swg" not in text, module.__name__
        assert "from multidriver_swg" not in text, module.__name__


def test_tracked_control_files_carry_no_tokens() -> None:
    # INVERTED from the pre-consolidation vacuous-control test. That test pinned "the carrier is
    # non-empty"; the carrier is private now, that invariant is enforced at run time by
    # load_carrier (which fires per scan rather than per suite run), and a test following the
    # carrier could only run where the carrier exists -- so in CI it would SKIP, which is a green
    # that means nothing. This pins the invariant that has no other pin in pytest: the two tracked
    # control files carry prose and nothing else.
    for rel in ("scripts/anonymization_blocklist.txt", "scripts/anonymization_baseline.txt"):
        path = ROOT / rel
        if not path.is_file():
            continue
        payload = [
            line
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
        assert not payload, f"{rel} carries {len(payload)} non-comment line(s); it must be prose-only"


def _hook_block(repo_root: Path, hook_id: str) -> list[str]:
    """The `- id:` block of one hook in the pre-commit config, read by line like the file is."""
    lines = (repo_root / ".pre-commit-config.yaml").read_text(encoding="utf-8").splitlines()
    start = next(i for i, line in enumerate(lines) if line.strip() == f"- id: {hook_id}")
    return lines[start : start + 6]


def test_hook_entry_is_invocable_as_configured() -> None:
    """The pre-commit config and the filesystem must agree about how the scan runs.

    This is the test that would have caught the 2026-08-17 defect: ruff-format dropped the
    guard's exec bit during the commit, and a `language: script` hook then exited 1 with "is not
    executable" -- red for the wrong reason, never scanning. The scanner tests all passed,
    because they call main() in-process.

    The target may be a directory (the scanner runs as `python3 scripts/content_scan`, which
    Python runs as a directory holding `__main__.py`), so existence is the check and the exec bit
    is checked for both forms: a directory without it is as unrunnable as a file without it.

    Line-scan rather than a YAML parse: the project env does not exist yet and this suite should
    not be what forces a pyyaml dependency.
    """
    for hook_id in ("anonymization-guard", "planning-reference-guard"):
        block = _hook_block(ROOT, hook_id)
        entry = next(line.split("entry:", 1)[1].strip() for line in block if line.strip().startswith("entry:"))
        language = next(line.split("language:", 1)[1].strip() for line in block if line.strip().startswith("language:"))
        words = entry.split()
        if language == "system":
            assert shutil.which(words[0]), f"{hook_id}: hook entry interpreter {words[0]!r} not on PATH"
            target = ROOT / words[1]
            assert target.exists(), f"{hook_id}: hook entry target {target} does not exist"
        elif language == "script":
            target = ROOT / words[0]
            assert target.exists(), f"{hook_id}: hook entry target {target} does not exist"
            assert target.stat().st_mode & 0o111, (
                f"{hook_id}: {target} is not executable, but the hook declares language: script -- "
                "the hook will exit non-zero without ever scanning"
            )
        else:
            raise AssertionError(f"unhandled hook language {language!r} for {hook_id} -- extend this test")


def _baseline_repo(tmp_path: Path) -> Path:
    """A repo with one accepted finding already in the ledger."""
    root = _init_repo(tmp_path, {"src/leak.py": "acct = 'zzsynthacct'\n"})
    fingerprint = identity.line_fingerprint("acct = 'zzsynthacct'")
    (root / "scripts" / identity.LOCAL_LEDGER).write_text(f"{fingerprint}  src/leak.py  1\n", encoding="utf-8")
    return root


def test_baseline_accepts_the_recorded_finding(tmp_path: Path) -> None:
    root = _baseline_repo(tmp_path)
    ledger = root / "scripts" / identity.LOCAL_LEDGER
    assert scanner.main(["--root", str(root), "--baseline", str(ledger)]) == 0


def test_baseline_does_not_leak_to_another_path(tmp_path: Path) -> None:
    # The acceptance is scoped to one path. The same token elsewhere still fires.
    root = _baseline_repo(tmp_path)
    (root / "src" / "other.py").write_text("acct = 'zzsynthacct'\n", encoding="utf-8")
    _git(root, "add", "-A")
    ledger = root / "scripts" / identity.LOCAL_LEDGER
    assert scanner.main(["--root", str(root), "--baseline", str(ledger)]) == 1


def test_acceptance_lapses_when_the_line_changes(tmp_path: Path) -> None:
    # The property that distinguishes a content-pinned ledger from a path exclusion: edit the
    # accepted line and the acceptance dies with it.
    root = _baseline_repo(tmp_path)
    (root / "src" / "leak.py").write_text("acct = 'zzsynthacct'  # moved\n", encoding="utf-8")
    _git(root, "add", "-A")
    ledger = root / "scripts" / identity.LOCAL_LEDGER
    assert scanner.main(["--root", str(root), "--baseline", str(ledger)]) == 1


def test_stale_baseline_entry_fails_closed(tmp_path: Path) -> None:
    # A row protecting nothing is reported, not silently kept.
    root = _baseline_repo(tmp_path)
    (root / "src" / "leak.py").write_text("acct = 'redacted'\n", encoding="utf-8")
    _git(root, "add", "-A")
    ledger = root / "scripts" / identity.LOCAL_LEDGER
    assert scanner.main(["--root", str(root), "--baseline", str(ledger)]) == 1


def test_an_explicit_corpus_replaces_every_classs_corpus(tmp_path: Path) -> None:
    """A corpus passed to the scan is the corpus every class reads, whatever its module declares.

    Class: one tree read twice, once with each class's own corpus and once with a list naming one
    file. Required because the pre-release scan ranges over the members an enumeration lists and
    not over the repository, and it must do that without a class's own corpus leaking into it -- a
    leak would scan the checkout instead of the released bytes and report the checkout's tree as
    the release's. Killed by ignoring the argument, or by unioning it with the class's list. A
    second correct implementation that takes the members as paths rather than repo-relative names
    still passes.
    """
    root = _init_repo(tmp_path, {"src/tracked.py": "acct = 'zzsynthacct'\n"})
    (root / "src" / "spare.py").write_text("acct = 'zzsynthacct'\n", encoding="utf-8")

    assert {hit.path for hit in core.scan(root, [identity.load(root)]).hits} == {"src/tracked.py"}
    narrow = core.scan(root, [identity.load(root)], corpus=lambda _root: ["src/spare.py"])
    assert {hit.path for hit in narrow.hits} == {"src/spare.py"}


# --- the two report forms ----------------------------------------------------


def _counts(identity_hits: int = 0, stale: int = 0, carrier: int = 2, ledger: int = 0) -> str:
    return (
        f"content-scan: {identity_hits} finding(s), {stale} stale ledger row(s); "
        f"carrier {carrier} token(s), ledger {ledger} row(s); "
        f"identity {identity_hits}, machine_path 0, planning_reference 0, secret 0"
    )


def test_runner_env_alone_forces_count_only(tmp_path: Path, capsys, monkeypatch) -> None:
    """GITHUB_ACTIONS=true alone forces count-only output, with no flag given.

    Class: an invocation on a GitHub runner that omits --count-only. Required because a workflow
    that forgets the flag would otherwise print locations into a public log. Killed by reading
    only the flag. A second correct implementation, one that also honours another runner variable,
    still passes.
    """
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    root = _init_repo(tmp_path, {"src/leak.py": "account = 'zzsynthacct'\n"})
    rc = scanner.main(["--root", str(root)])
    text = "\n".join(_lines(capsys.readouterr()))
    assert rc == 1
    assert "src/leak.py" not in text
    assert "carrier entry #" not in text
    assert "zzsynthacct" not in text


def test_a_runner_env_reading_false_does_not_force_count_only(tmp_path: Path, capsys, monkeypatch) -> None:
    """GITHUB_ACTIONS=false leaves the located form in force.

    Class: a machine that exports the variable with any other value, which is what a local shell
    doing `GITHUB_ACTIONS=0` or a harness that sets it to `false` produces. Required because the
    located form is the only one a developer can act on, and a membership test on the variable's
    NAME would replace it everywhere. Killed by testing the variable for presence rather than for
    the value `true`. A second correct implementation that accepts `True` as well still passes.
    """
    monkeypatch.setenv("GITHUB_ACTIONS", "false")
    root = _init_repo(tmp_path, {"src/leak.py": "account = 'zzsynthacct'\n"})
    rc = scanner.main(["--root", str(root)])
    assert rc == 1
    assert "src/leak.py" in capsys.readouterr().err


def test_count_only_clean_tree_exits_zero(tmp_path: Path, capsys) -> None:
    """A clean tree under --count-only prints its zero counts and exits 0.

    Class: no finding and no stale ledger row. Required so a green CI run still shows how many
    carrier tokens and ledger rows it loaded. Killed by printing nothing on a clean scan, or by
    exiting non-zero. A second correct implementation that builds the same line by a template
    string still passes.
    """
    root = _init_repo(tmp_path, {"src/ok.py": "x = 1\n"})
    assert scanner.main(["--root", str(root), "--count-only"]) == 0
    assert _lines(capsys.readouterr()) == [_counts()]


def test_count_only_stale_row_names_no_path(tmp_path: Path, capsys) -> None:
    """A stale ledger row under --count-only is counted, never named, and exits 1.

    Class: a ledger row whose accepted line no longer matches. Required because the located form's
    stale report prints the row's path and fingerprint. Killed by reporting stale rows as a
    located line, or by treating stale rows as clean. A second correct implementation that counts
    stale rows before scanning findings still passes.
    """
    root = _baseline_repo(tmp_path)
    (root / "src" / "leak.py").write_text("acct = 'redacted'\n", encoding="utf-8")
    _git(root, "add", "-A")
    ledger = root / "scripts" / identity.LOCAL_LEDGER
    rc = scanner.main(["--root", str(root), "--baseline", str(ledger), "--count-only"])
    assert rc == 1
    assert _lines(capsys.readouterr()) == [_counts(stale=1, ledger=1)]


def test_count_only_missing_carrier_exits_two_naming_no_path(tmp_path: Path, capsys) -> None:
    """A missing carrier under --count-only exits 2 with a fixed sentence and no path.

    Class: a run with no reachable carrier, which is what a fork's CI run presents. Required
    because exit 2 separates "cannot report" from a finding, and the located form's message names
    the carrier's path. Killed by exiting 0 or 1, or by printing the path. A second correct
    implementation that checks the carrier inside the scan rather than before it still passes.
    """
    root = _init_repo(tmp_path, {"src/ok.py": "x = 1\n"})
    (root / "scripts" / identity.LOCAL_SUPPLEMENT).unlink()
    rc = scanner.main(["--root", str(root), "--count-only"])
    text = "\n".join(_lines(capsys.readouterr()))
    assert rc == 2
    assert str(tmp_path) not in text
    assert text.startswith("content-scan: ")


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

    Class: each exception source behind that sentence (a malformed ordinal, a ledger row short of
    three fields, an undecodable carrier, a root that is not a repository), each carrying a canary
    cell. Required because an exception's own text can quote a carrier or ledger cell into a
    public log. A row short of three fields is refused by a `core.Unreadable`, whose message quotes
    the raw row, so this pins that the count-only form never prints that message. A second correct
    implementation that validates each input before reading it, raising nothing, still passes.
    """
    if source.startswith("tracked-file list"):
        root = tmp_path
        (root / "scripts").mkdir()
        (root / "scripts" / identity.LOCAL_SUPPLEMENT).write_text("zzsynthacct\n", encoding="utf-8")
        argv = ["--root", str(root), "--count-only"]
    else:
        root = _init_repo(tmp_path, {"src/ok.py": "x = 1\n"})
        argv = ["--root", str(root), "--count-only"]
        if source.startswith("carrier"):
            (root / "scripts" / identity.LOCAL_SUPPLEMENT).write_bytes(b"zzsynthacct\n\xffzzcanary\n")
        else:
            bad = root / "scripts" / "bad_ledger.txt"
            bad.write_text(content, encoding="utf-8")
            argv += ["--baseline", str(bad)]
    assert scanner.main(argv) == 2
    assert _lines(capsys.readouterr()) == [core.COUNT_ONLY_UNREADABLE_ERROR]


@pytest.mark.parametrize("extra", [["--list"], ["--list", "--reveal"]])
def test_count_only_refuses_list_and_reveal(tmp_path: Path, capsys, extra) -> None:
    """--list and --list --reveal are refused in count-only mode, exit 2, no token.

    Class: the listing options combined with count-only. Required because --reveal prints a
    label's own text, and the CI job must run the scan in scan mode only. Killed by honouring
    --list in count-only mode. A second correct implementation that refuses only --reveal and
    makes --list print counts would fail this test, so the refusal of --list is part of the
    contract, not an implementation choice.
    """
    root = _init_repo(tmp_path, {"src/ok.py": "x = 1\n"})
    rc = scanner.main(["--root", str(root), "--count-only", *extra])
    text = "\n".join(_lines(capsys.readouterr()))
    assert rc == 2
    assert "zzsynthacct" not in text


def test_reveal_prints_a_public_label_name_and_never_a_carrier_token(tmp_path: Path, capsys) -> None:
    """--reveal names each public class's labels and hides the identity class's tokens.

    Class: the listing form with --reveal, over a carrier the run could disclose. Required
    because --reveal is one paste away from a workflow, and the identity class's labels are the
    one thing in this tree that must never appear in a log. Killed by revealing an identity label
    with the rest. A second correct implementation that prints the public labels in another order
    still passes.
    """
    root = _init_repo(tmp_path, {"src/ok.py": "x = 1\n"})
    assert scanner.main(["--root", str(root), "--list", "--reveal"]) == 0
    text = "\n".join(_lines(capsys.readouterr()))
    assert "machine path" in text
    assert "PEM private-key header" in text
    assert "zzsynthacct" not in text


# --- the main-checkout fallback for a linked worktree -------------------------
#
# The carrier and ledger are gitignored files that exist in ONE checkout of a repository. A linked
# worktree of the same repository has neither, so these tests build that layout in tmp_path: a main
# checkout holding the carrier (and, where a test needs it, a ledger) and a worktree added from it
# with `git worktree add`.


def _main_and_worktree(tmp_path: Path, files: dict[str, str]) -> tuple[Path, Path]:
    """A committed main checkout holding the carrier, and a linked worktree holding no carrier."""
    main = tmp_path / "main"
    main.mkdir(parents=True)
    # The tracked policy file keeps scripts/ in the commit, as in the real repository; a directory
    # holding only ignored files would not exist in the worktree at all.
    _init_repo(main, {"scripts/anonymization_blocklist.txt": "# policy only\n", **files})
    _git(main, "commit", "-q", "--no-verify", "-m", "init")
    worktree = tmp_path / "worktree"
    _git(main, "worktree", "add", "-q", str(worktree))
    assert not (worktree / "scripts" / identity.LOCAL_SUPPLEMENT).exists()
    return main, worktree


def _accept_leak(checkout: Path) -> None:
    """Write a ledger accepting the one line `src/leak.py` holds, for the carrier's first token."""
    fingerprint = identity.line_fingerprint("acct = 'zzsynthacct'")
    (checkout / "scripts" / identity.LOCAL_LEDGER).write_text(f"{fingerprint}  src/leak.py  1\n", encoding="utf-8")


_LEAK = {"src/leak.py": "acct = 'zzsynthacct'\n"}


def test_a_worktree_without_a_carrier_reads_the_main_checkouts(tmp_path: Path, capsys) -> None:
    """A worktree with no carrier of its own scans with the main checkout's carrier.

    Class: any linked worktree of a repository whose main checkout holds the carrier and whose own
    scripts directory holds none -- the position of a build worktree and of a landing's throwaway
    checkout. Required because without it the scanner exits 2 there on a machine that has the
    carrier, so a commit from any worktree has to skip the hook. The tree carries a planted token
    so that the verdict proves the tokens were LOADED: exit 1 naming the file, where a scanner
    that found no carrier exits 2 and one that loaded no tokens finds nothing. Killed by removing
    the fallback (exit 2). A second correct implementation that finds the main checkout from `git
    worktree list` rather than from the common directory still passes.
    """
    _main, worktree = _main_and_worktree(tmp_path, _LEAK)
    rc = scanner.main(["--root", str(worktree)])
    err = capsys.readouterr().err
    assert rc == 1
    assert "src/leak.py" in err
    assert "zzsynthacct" not in err


def test_a_clean_worktree_without_a_carrier_passes_on_the_main_checkouts(tmp_path: Path) -> None:
    """A clean worktree with no carrier of its own exits 0 through the main checkout's carrier.

    Class: the same layout as the test above with no planted token, which is the everyday commit
    from a worktree. Required because that commit must pass the hook unskipped. Killed by removing
    the fallback (exit 2). A second correct implementation that resolves the main checkout through
    `git worktree list` still passes.
    """
    _main, worktree = _main_and_worktree(tmp_path, {"src/ok.py": "x = 1\n"})
    assert scanner.main(["--root", str(worktree)]) == 0


def test_no_carrier_in_either_checkout_still_fails_closed(tmp_path: Path, capsys) -> None:
    """With no carrier in the worktree or the main checkout, the scan exits 2.

    Class: a machine that never linked the carrier, scanned from a worktree. Required because the
    fallback must not turn "no ground truth anywhere" into a green scan. Killed by a fallback that
    treats an unreachable carrier as an empty token list (exit 0). A second correct implementation
    that checks the main checkout before this one when this one lacks the file still passes.
    """
    main, worktree = _main_and_worktree(tmp_path, _LEAK)
    (main / "scripts" / identity.LOCAL_SUPPLEMENT).unlink()
    assert scanner.main(["--root", str(worktree)]) == 2
    assert "no carrier is reachable" in capsys.readouterr().err


def test_a_worktree_with_its_own_carrier_reads_no_ledger_from_the_main_checkout(tmp_path: Path) -> None:
    """A worktree holding its own carrier and no ledger does not borrow the main checkout's ledger.

    Class: a worktree whose carrier exists and whose ledger does not, while the main checkout's
    ledger accepts the very line the worktree's tree carries. Required because a ledger row is
    keyed on a carrier ORDINAL, so a ledger read beside another checkout's carrier can accept a
    different token than the one it was recorded for; both must come from one directory. The
    control scan of the main checkout shows the ledger does accept that line there, so the red in
    the worktree is the ledger's absence and not a ledger that accepts nothing. Killed by falling
    back for the ledger independently of the carrier (exit 0). A second correct implementation
    that resolves the directory once and passes it down explicitly still passes.
    """
    main, worktree = _main_and_worktree(tmp_path, _LEAK)
    _accept_leak(main)
    (worktree / "scripts" / identity.LOCAL_SUPPLEMENT).write_text(
        "zzsynthacct\nzz-synthetic-estate-repo\n", encoding="utf-8"
    )
    assert scanner.main(["--root", str(main)]) == 0
    assert scanner.main(["--root", str(worktree)]) == 1


def test_a_present_empty_carrier_does_not_fall_back(tmp_path: Path, capsys) -> None:
    """A carrier that exists in the worktree but defines no tokens exits 2 and is not replaced.

    Class: a worktree carrier reduced to comments or blank lines while the main checkout's carrier
    is intact. Required because only ABSENCE triggers the fallback: a broken carrier silently
    replaced by another checkout's copy would hide the breakage. Killed by a fallback keyed on the
    token count rather than on the file's existence (exit 1, the main carrier's finding). A second
    correct implementation that tests `is_file()` rather than `exists()` still passes.
    """
    _main, worktree = _main_and_worktree(tmp_path, _LEAK)
    (worktree / "scripts" / identity.LOCAL_SUPPLEMENT).write_text("# nothing listed\n\n", encoding="utf-8")
    assert scanner.main(["--root", str(worktree)]) == 2
    assert "no carrier is reachable" in capsys.readouterr().err


def test_an_explicit_blocklist_anchor_does_not_fall_back(tmp_path: Path) -> None:
    """An explicit --blocklist whose directory holds no carrier exits 2, whatever the main checkout holds.

    Class: a caller naming the anchor file itself, in a worktree whose main checkout has the
    carrier. Required because an option that names a directory must read that directory; reading
    another one instead would make the option untrustworthy. Killed by applying the fallback to an
    explicit anchor too (exit 1, the main carrier's finding). A second correct implementation that
    decides "explicit" from whether the option was given rather than from its value still passes.
    """
    _main, worktree = _main_and_worktree(tmp_path, _LEAK)
    anchor = worktree / "scripts" / "anonymization_blocklist.txt"
    assert scanner.main(["--root", str(worktree), "--blocklist", str(anchor)]) == 2


def test_the_fallback_is_anchored_on_the_scanned_repository_not_the_working_directory(
    tmp_path: Path, monkeypatch
) -> None:
    """Standing in a worktree whose main checkout has a carrier, a scan of ANOTHER repository with none exits 2.

    Class: any run whose working directory is a checkout of one repository while --root names a
    different repository -- which is what this test module does whenever it is run from a
    worktree, since every test scans a tmp_path repository. Required because a lookup from the
    working directory answers for the repository the caller stands in, so the other repository
    would be scanned with this machine's carrier and would stop failing closed. Killed by finding
    the main checkout from the working directory (`git rev-parse --git-common-dir` with no `-C`,
    joined with `scripts/`), which exits 0 here. A second correct implementation that anchors on
    --root instead of on the blocklist's directory still passes.
    """
    _main, worktree = _main_and_worktree(tmp_path / "carrying", {"src/ok.py": "x = 1\n"})
    other = tmp_path / "other"
    other.mkdir()
    _init_repo(other, {"src/ok.py": "x = 1\n"})
    (other / "scripts" / identity.LOCAL_SUPPLEMENT).unlink()
    monkeypatch.chdir(worktree)
    assert scanner.main(["--root", str(other)]) == 2


_BLOCKLIST_HELP = (
    "path of the blocklist file; the carrier and ledger are read from its directory "
    "(default: <root>/scripts/anonymization_blocklist.txt, or the same path in the main checkout "
    "when this checkout has no carrier)"
)


# --- the planning-reference class --------------------------------------------


def test_a_file_name_is_scanned_like_its_bytes(tmp_path) -> None:
    root = _init_repo(tmp_path, {"tests/test_x_" + "7a_1_review.py": "x = 1\n"})
    hits = _classes(core.scan(root, [planning_reference.load()]))["planning_reference"].findings
    assert [(hit.path, hit.line, hit.label) for hit in hits] == [
        ("tests/test_x_" + "7a_1_review.py", None, "chunk identifier in a file name")
    ]
    assert hits[0].render().endswith("(in the path)")


def test_legacy_code_is_out_of_scope_and_legacy_records_are_not(tmp_path) -> None:
    root = _init_repo(
        tmp_path,
        {"_old_code_to_refactor/a.py": "# ADR-" + "1\n", "_old_code_to_refactor/README.md": "ADR-" + "2\n"},
    )
    hits = _classes(core.scan(root, [planning_reference.load()]))["planning_reference"].findings
    assert [(hit.path, hit.line) for hit in hits] == [("_old_code_to_refactor/README.md", 1)]


def test_exit_codes(tmp_path) -> None:
    """0 clean, 1 findings, 2 a corpus that holds nothing to scan."""
    clean = _init_repo(tmp_path / "c", {"a.py": "x = 1\n"})
    dirty = _init_repo(tmp_path / "d", {"a.py": "# ADR-" + "1\n"})
    empty = tmp_path / "e"
    empty.mkdir()
    _git(empty, "init", "-q")
    argv = ["--class", "planning_reference"]
    assert (
        scanner.main(["--root", str(clean), *argv]),
        scanner.main(["--root", str(dirty), *argv]),
        scanner.main(["--root", str(empty), *argv]),
    ) == (0, 1, 2)


def test_the_scanner_modules_spell_no_private_name() -> None:
    """Property: no file the scan skips spells the name of a private repository or tool.

    The names are assembled from pieces here for the same reason the patterns are written in
    pieces. The set of skipped files is the scanner's own modules and their two test modules --
    the files that state the patterns and their controls -- and a wider entry would blind a whole
    path to every class at once.
    """
    names = [
        "copier-python-" + "template",
        "claude-" + "workspace",
        "agentic" + "-workspace",
        "proceed-with-" + "implementation",
        "qaqc-and-" + "commit",
        "refactor-" + "plan",
        "plan-then-" + "implement",
    ]
    for rel in sorted(core.SELF_EXCLUDED):
        text = (ROOT / rel).read_text(encoding="utf-8")
        assert [name for name in names if name in text] == [], rel


def test_this_repository_carries_no_reference() -> None:
    """Property: the real tree is clean. This is the check that reaches any contributor who runs the suite."""
    report = core.scan(ROOT, [planning_reference.load()])
    hits = [hit for hit in report.hits if hit.path not in SUPERSEDED]
    assert report.scanned > 0 and hits == [], "\n".join(hit.render() for hit in hits)


# --- the hook's environment --------------------------------------------------


def test_a_hook_in_a_worktree_still_reads_the_main_checkouts_carrier(tmp_path: Path) -> None:
    """Run as git runs a pre-commit hook in a linked worktree, the scan still reads the main checkout's carrier.

    Class: any run of the scanner in a linked worktree with `GIT_DIR` (and `GIT_INDEX_FILE`)
    exported naming that worktree's own git directory, which is what git 2.43 exports to every
    hook it runs in a linked worktree, and so the position of every commit made from a build
    worktree or a landing's throwaway checkout.

    Why it is required: with `GIT_DIR` set and no work tree named, git takes the directory it was
    pointed at as the top of the work tree, so the common directory comes back right and the path
    prefix comes back empty. A lookup that inherits the variable therefore looks for the carrier
    at the top of the main checkout rather than in its scripts/ directory, finds none, and the
    hook exits 2 on a machine that holds the carrier. That is the failure the fallback exists to
    remove, reappearing in the one position it was built for.

    It runs the scanner as a SUBPROCESS with `GITHUB_ACTIONS` removed, rather than calling main()
    in this interpreter: the count-only form is forced on a runner, and this test has to be green
    there too. The tree carries a planted token, so the verdict shows the tokens were loaded: exit
    1 naming the file, where a scan that found no carrier exits 2.

    Killed by: running the main-checkout lookup with the inherited environment (dropping the
    filtered `env` from its `git` call), which exits 2 here.

    Second correct implementation: one that removes every `GIT_`-prefixed variable from the
    lookup's environment, rather than the three location variables, still passes.
    """
    main, worktree = _main_and_worktree(tmp_path, {"src/leak.py": "acct = 'zzreviewacct'\n"})
    (main / "scripts" / identity.LOCAL_SUPPLEMENT).write_text("zzreviewacct\n", encoding="utf-8")
    worktree_git_dir = _git(worktree, "rev-parse", "--absolute-git-dir")
    env = {k: v for k, v in os.environ.items() if k not in ("GITHUB_ACTIONS", "GIT_WORK_TREE", "GIT_COMMON_DIR")}
    env["GIT_DIR"] = worktree_git_dir
    env["GIT_INDEX_FILE"] = str(Path(worktree_git_dir) / "index")
    proc = subprocess.run(
        [sys.executable, "-B", str(SCANNER), "--root", str(worktree)],
        capture_output=True,
        text=True,
        env=env,
        cwd=worktree,
        check=False,
    )
    assert proc.returncode == 1, proc.stderr
    assert "src/leak.py" in proc.stderr
    assert "zzreviewacct" not in proc.stderr


# --- the public label shapes, one sample per shape ----------------------------
#
# O-e.3: the scan runs in the toolkit's own suite against a PUBLIC list of label shapes. Every
# public class declares the shapes it looks for, and this table carries one sample for each; a
# class that adds a shape without a sample reds the coverage test, and a sample the patterns no
# longer match reds the firing test.

SAMPLES: dict[str, dict[str, str]] = {
    "machine_path": {
        "machine path": "root = '" + "/ho" + "me/someone/project'",
    },
    "planning_reference": {
        "decision or record number": "see ADR-" + "14 for this",
        "letter-named decision": "the parent ADR-" + "n grammar",
        "constraint or review label": "under R" + "32 the rule holds",
        "two- or three-digit decision": "as ruled at D" + "132",
        "chunk identifier": "landed in chunk 6" + "a.4",
        "work-package identifier": "package " + "3a reviewer tests",
        "chunk phrase": "nothing in this work " + "chunk",
        "planning-record path": "read library/" + "docs/" + "planning/x.md",
        "agent role name": "ask the snakemake" + "-specialist",
        "plan-prompt acronym": "the PI" + "P says",
        "role word": "the cod" + "er's tests of the cutting function",
        "build-record word": "measured in the build " + "return",
        "authority phrase": "done on developer " + "directive",
        "agent-process word": "not found by the absorbing " + "agent",
        "tool name": "participant C as Clau" + "de Code",
        "record number in prose": "by decision " + "3.3 and is",
        "section-style label": "Invocation isolation (B" + "4-4, arm 2)",
        "process phrase": "the Definition of " + "Done line",
        "chunk identifier in a file name": "tests/test_x_" + "7a_1_review.py",
    },
    "secret": {
        "PEM private-key header": "-----BEGIN " + "RSA PRIVATE KEY" + "-----",
        "GitHub token": "ghp_" + "A" * 36,
        "PyPI token": "pypi-" + "A" * 40,
        "cloud access-key id": "AKIA" + "B" * 16,
        "assigned credential": 'password="' + "hunter2hunter2" + '"',
    },
}

MUST_NOT_FIRE = [
    "chi-square with P-1 degrees of freedom",
    "tolerance 1E-03 and 5e-04",
    "encoded as UTF-8",
    "a SHA-256 digest",
    "licensed AGPL-3.0-or-later",
    "an R2 of 0.9",
    "Python 3.12 and numpy 2.4",
    "the shape of the array",
    "the plan for the season",
    "/.claude/",
    "/CLAUDE.md",
    "/.codex/",
    "/AGENTS.md",
    "which a reviewer will ask about",
    "reproduced by the reviewer",
    'chunks = dict(year = "auto")',
    "the first chunk of the array",
    "the coordinates of the gauge",
    "an encoder and a decoder",
    "On a developer machine, link the folder",
    "the developer's environment cannot move a test",
    "Developers that use our General Public Licenses",
    "scoped-in rows 0, family tests 0",
    "rows 1, surviving 2",
    "a peak discharge Q100 and an F1 score of 0.9",
    "the ledger records a verdict per session",
    "Reviewer tests for the divergence detector",
]

MUST_FIRE: list[tuple[str, str]] = [
    ("decision or record number", "see ADR-" + "14 for this"),
    ("letter-named decision", "the parent ADR-" + "n grammar"),
    ("constraint or review label", "under R" + "32 the rule holds"),
    ("two- or three-digit decision", "as ruled at D" + "132"),
    ("chunk identifier", "landed in chunk 6" + "a.4"),
    ("chunk identifier", "the 7" + "a.1 change"),
    ("work-package identifier", "package " + "3a reviewer tests"),
    ("chunk phrase", "nothing in this work " + "chunk"),
    ("planning-record path", "read library/" + "docs/" + "planning/x.md"),
    ("planning-record path", "the agentic" + "-workspace record"),
    ("agent role name", "ask the snakemake" + "-specialist"),
    ("plan-prompt acronym", "the PI" + "P says"),
    ("decision or record number", "ruling U-" + "21 applies"),
    ("process phrase", "an open " + "item remains"),
    ("decision or record number", "the TO-" + "5 line"),
    ("role word", "the cod" + "er's tests of the cutting function"),
    ("role word", "ask the coordinato" + "r about it"),
    ("role word", "reported in the returning sea" + "t's note"),
    ("build-record word", "measured in the build " + "return"),
    ("build-record word", "Build " + "class: NEW CAPABILITY"),
    ("authority phrase", "done on developer " + "directive"),
    ("authority phrase", "Ruled by the develo" + "per on 2026-08-17"),
    ("authority phrase", "a developer " + "ruling gives it"),
    ("agent-process word", "not found by the absorbing " + "agent"),
    ("tool name", "participant C as Clau" + "de Code"),
    ("tool name", "generated from copier-python-" + "template"),
    ("tool name", "stored privately in claude-" + "workspace"),
    ("tool name", "the sub" + "agent transcripts"),
    ("tool name", "added by refactor-" + "plan audit"),
    ("tool name", "follows a plan-then-" + "implement workflow"),
    ("tool name", "participant P as Planning " + "Doc"),
    ("tool name", "D->>C: /proceed-with-" + "implementation"),
    ("record number in prose", "by decision " + "3.3 and is"),
    ("record number in prose", "as the cell that calls it does (row " + "108)"),
    ("section-style label", "Invocation isolation (B" + "4-4, arm 2)"),
    ("process phrase", "the Definition of " + "Done line"),
    ("role word", "Coordinato" + "r reads the returns"),
    ("authority phrase", "Developer " + "directive: keep it"),
    ("build-record word", "Build " + "return shows it"),
    ("process phrase", "as the approved " + "shape's unit table states"),
    ("build-record word", "measured in the review " + "return"),
    ("build-record word", "per the reviewer " + "return"),
    ("build-record word", "set out in the builder " + "return"),
    ("authority phrase", "done on the developer" + "'s instruction"),
    ("authority phrase", "a developer " + "directive applies"),
    ("authority phrase", "the developer" + "'s rule for unseeded calls"),
    ("authority phrase", "the developer " + "ruled that site unregistered"),
    ("agent-process word", "not found by the implementing " + "agent"),
    ("agent-process word", "as the reviewing " + "agent reports"),
    ("agent-process word", "asked of the building " + "agent"),
    ("tool name", "run /qaqc-and-" + "commit"),
    ("tool name", "run /plan-" + "implementation"),
    ("tool name", "run /plan-and-" + "implement"),
    ("process phrase", "this shape " + "stage ends"),
    ("process phrase", "each shape " + "iteration adds one"),
    ("process phrase", "an open " + "item remains"),
    ("agent-process word", "Reviewing " + "agent reports it first"),
    ("authority phrase", "at the developer" + "'s request"),
    ("authority phrase", "The developer " + "decided that it moves"),
    ("authority phrase", "the developer" + "'s preference for the code"),
]


def test_every_public_class_declares_a_sample_for_each_shape() -> None:
    """Every public label shape has exactly one sample, so a new shape cannot arrive untested.

    The identity class declares none on purpose: its labels are the private carrier's entries and
    the carrier is the one list this repository does not publish, so the tests above supply a
    synthetic carrier instead.
    """
    for name, table in SAMPLES.items():
        module = {"machine_path": machine_path, "planning_reference": planning_reference, "secret": secret}[name]
        assert sorted(module.LABEL_SHAPES) == sorted(table), name
    assert identity.LABEL_SHAPES == ()


@pytest.mark.parametrize(("shape", "line"), MUST_FIRE)
def test_each_planning_form_is_reported(tmp_path, line, shape) -> None:
    """Property: every identifier form the scanner exists for is reported, so a pattern that stops firing reds."""
    root = _init_repo(tmp_path, {"a.py": f"x = 1  # {line}\n"})
    report = core.scan(root, [planning_reference.load()])
    hits = _classes(report)["planning_reference"].findings
    assert report.scanned > 0
    assert [(hit.path, hit.line) for hit in hits] == [("a.py", 1)], (line, hits)
    assert shape in [label.name for label in planning_reference.load().predicate(line)], line


@pytest.mark.parametrize("line", MUST_NOT_FIRE)
def test_ordinary_text_is_not_reported(tmp_path, line) -> None:
    """Property: numbers, encodings, licences and plain words that resemble a label are not reported."""
    root = _init_repo(tmp_path, {"a.py": f"x = 1  # {line}\n"})
    assert core.scan(root, [planning_reference.load()]).hits == [], line


@pytest.mark.parametrize(("name", "label"), [(name, label) for name, table in SAMPLES.items() for label in table])
def test_every_public_label_shape_fires(tmp_path, name, label) -> None:
    """Every declared shape is found by its class, under its own name.

    Class: one sample per label shape, planted in a throwaway repository -- as the content of a
    file, or, for a class that also judges a path, as the path itself. Required because a pattern
    that stops matching is a class that has silently stopped looking, and the suite would
    otherwise stay green. Killed by any pattern whose sample no longer fires.
    """
    sample = SAMPLES[name][label]
    matcher = _only(name)
    if label in getattr(planning_reference, "PATH_PATTERNS", {}):
        assert [found.name for found in planning_reference.path_labels(sample)] == [label]
        root = _init_repo(tmp_path, {sample: "x = 1\n"})
        hits = [hit for hit in _classes(core.scan(root, [matcher]))[name].findings if hit.line is None]
        assert [(hit.path, hit.label) for hit in hits] == [(sample, label)]
        return
    labels = [found.name for found in matcher.predicate(sample)]
    assert label in labels, (name, label, labels)
    root = _init_repo(tmp_path, {"sample.txt": sample + "\n"})
    hits = _classes(core.scan(root, [matcher]))[name].findings
    assert [hit.label for hit in hits if hit.line is not None] == [", ".join(labels)]


def test_no_public_shape_is_a_sample_of_another_class(tmp_path) -> None:
    """A sample of one class is not a finding of another class.

    Class: every public sample, run through every other public class. Required because two classes
    that both fire on one line make a report ambiguous and make any count-only total impossible to
    attribute. Killed by a shape that overlaps another class's.
    """
    path_shapes = set(planning_reference.PATH_PATTERNS)
    for name, table in SAMPLES.items():
        for other_name in SAMPLES:
            if other_name == name:
                continue
            matcher = _only(other_name)
            for label, sample in table.items():
                if label in path_shapes:
                    continue  # a file NAME is judged by the path predicate, not by a line
                assert matcher.predicate(sample) == (), (other_name, name, label)


# --- the reading contract after the reviewer's round -------------------------
#
# Five behaviours the unit tests above did not pin, each of which a regression would make silently
# wrong rather than red: a path skipped for every class is a file-level skip marker; a path whose
# file is gone is not part of what a reader sees; an empty corpus in the count form is a green that
# scanned nothing; a binary file is read by the class whose predecessor read bytes; and an
# undecodable private file must still end the run with the code that means "could not report".

# Planted by the tests below; assembled from pieces so this file spells no whole form.
_MACHINE = "root = '" + "/ho" + "me/someone/project'"
_SECRET = "-----BEGIN " + "OPENSSH PRIVATE KEY" + "-----"
_REFERENCE = "quoting ADR-" + "14 as an example"


def test_the_count_only_form_refuses_a_corpus_holding_nothing_to_scan(tmp_path: Path, capsys) -> None:
    """A tree whose only tracked file is binary exits 2 in the count form, as it does in the located form.

    Class: the count form over a corpus that yields no readable text. Required because the count
    line would otherwise read `0 finding(s)` over a scan that opened no file, and a runner always
    takes this form -- the push job would go green over nothing. Killed by the count form never
    reading the scanned count. A second correct implementation that makes the scan itself refuse
    an empty corpus still passes.
    """
    root = tmp_path / "binary"
    root.mkdir()
    _git(root, "init", "-q")
    (root / "blob.bin").write_bytes(b"\0\1\2")
    _git(root, "add", "-A")

    rc = scanner.main(["--root", str(root), "--class", "secret", "--count-only"])
    captured = capsys.readouterr()
    assert rc == 2
    assert "nothing to scan" in captured.err
    assert str(tmp_path) not in captured.err


def test_the_machine_path_class_reads_bytes_so_a_binary_file_is_reachable(tmp_path: Path) -> None:
    """A binary file holding a home-directory path is reported by machine_path and skipped by identity.

    Class: one tracked `.npz`-shaped file with a NUL and a home path, read by both a byte-reading
    class and a text-reading one. Required because the inline `grep -aqE` step this class replaces
    treated a binary file as text, and its allowlist carries two `.npz` rows that only a byte read
    could ever have matched -- a text-only corpus would leave those rows protecting nothing while a
    released `.npz` lost its coverage. Killed by giving every class the text reader. A second
    correct implementation that greps the bytes directly still passes.
    """
    root = _init_repo(tmp_path, {"src/ok.py": "x = 1\n"})
    (root / "data").mkdir()
    (root / "data" / "grid.npz").write_bytes(b"\0\1\2" + _MACHINE.encode() + b"\0")
    _git(root, "add", "-A")

    found = _classes(core.scan(root, [machine_path.load(), identity.load(root)]))
    assert [hit.path for hit in found["machine_path"].findings] == ["data/grid.npz"]
    assert found["identity"].findings == []


def test_the_assigned_credential_shape_covers_a_suffixed_identifier_and_an_unquoted_value() -> None:
    """The four identifier spellings a repository actually holds, and an unquoted value, all fire.

    Class: `access_token`, `GITHUB_TOKEN`, `db_password`, `client_secret`, and `password=` with an
    unquoted literal, beside a quoted control and a dotted-expression negative. Required because a
    shape that wanted the bare word `token` exactly, and a quoted value exactly, misses every one
    of these -- the spellings a repository actually carries -- and the class whose single miss is
    not recoverable is this one. Killed by the exact-keyword quoted-literal pattern. A second
    correct implementation that prefixes the keyword by another regex still passes.
    """
    matcher = secret.load()
    for sample in (
        "access_token = " + '"' + "abcdefgh12345678" + '"',
        "GITHUB_" + "TOKEN=" + "abcdefgh12345678",
        "db_" + "password: " + "hunter2hunter2",
        "client_" + "secret = " + '"' + "abcdefgh12345678" + '"',
        "pass" + 'word="' + "hunter2hunter2" + '"',
    ):
        assert [label.name for label in matcher.predicate(sample)] == ["assigned credential"], sample
    assert matcher.predicate('"PyPI ' + 'token": re.compile(r"x")') == ()
    assert matcher.predicate("the token is refreshed") == ()


def test_the_private_key_shape_covers_the_pgp_algorithm_word() -> None:
    """A PGP private-key header is a private-key header, and the PEM forms stay covered.

    Class: the header's algorithm alternation. Required because the `PGP` form is the one a key
    block carries when it is not a PEM key, and a header pattern that admits `RSA`, `EC`, `DSA`,
    `OPENSSH` and `ENCRYPTED` but not `PGP` misses it while looking complete. Killed by the
    five-word alternation. A second correct implementation that admits any algorithm word passes.
    """
    matcher = secret.load()
    for sample in (
        "-----BEGIN " + "PGP PRIVATE KEY BLOCK" + "-----",
        "-----BEGIN " + "RSA PRIVATE KEY" + "-----",
        "-----BEGIN " + "PRIVATE KEY" + "-----",
    ):
        assert [label.name for label in matcher.predicate(sample)] == ["PEM private-key header"], sample


def test_an_undecodable_private_file_exits_two_in_the_located_form(tmp_path: Path, capsys) -> None:
    """A carrier or a ledger that is not UTF-8 ends the located run with exit 2 and no traceback.

    Class: the two private files, each undecodable, in the form that prints locations. Required
    because 1 means "findings" and 2 means "could not report", and an uncaught decode error leaves
    the interpreter's exit status 1 -- so a broken carrier would read as a dirty tree. Killed by
    reading either file with `read_text` outside a handler. A second correct implementation that
    converts the error in the scan still passes.
    """
    for name in (identity.LOCAL_SUPPLEMENT, identity.LOCAL_LEDGER):
        root = tmp_path / name.replace(".", "_")
        _init_repo(root, {"src/ok.py": "x = 1\n"})
        payload = b"zzsynthacct\xff\xfe\n" if name == identity.LOCAL_SUPPLEMENT else b"\xff\xfe\n"
        (root / "scripts" / name).write_bytes(payload)
        rc = scanner.main(["--root", str(root), "--class", "identity"])
        captured = capsys.readouterr()
        assert rc == 2, name
        assert "Traceback" not in captured.err, name
        assert "not UTF-8" in captured.err, name
