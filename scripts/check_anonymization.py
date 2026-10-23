#!/usr/bin/env python3
"""CI check enforcing the anonymization blocklist.

Enumerates the git-tracked set (`git ls-files`) and fails if any tracked text
file contains a crufty developer or machine string listed in the INDEPENDENT
ground-truth CARRIER, a gitignored file this repository does not track and
reaches as `scripts/anonymization_blocklist.local.txt`. Working-tree scrub
enforcement only; git HISTORY exposure is a separate concern. Pure-stdlib by
design: this guard must remain runnable when the project environment does not
exist, because repo scaffolding — the window in which private paths are most
likely to be committed — happens before that environment is created.

INDEPENDENCE INVARIANT: this module imports NOTHING from src/multidriver_swg/.
Its ground truth is the hand-authored blocklist file, never the constants the
scrub edits (verification guards need an independent ground-truth signal).
Enforced mechanically by
tests/test_check_anonymization.py::test_guard_imports_nothing_from_src.

Matching: case-insensitive, whole-word (\\b...\\b), tokens matched literally
(re.escape). Exit 0 = clean, 1 = >=1 hit.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Files that legitimately CONTAIN blocklisted tokens and must never self-match.
_SELF_EXCLUDE: frozenset[str] = frozenset()


@dataclass(frozen=True)
class Hit:
    path: str  # repo-relative
    line: int
    token: str

    def render(self, index: int | None = None) -> str:
        """Path, line, and either the literal token or its blocklist INDEX.

        The index carries ZERO token-derived bytes, so there is nothing to
        invert. A truncated digest would not do: the candidate set is small and
        low-entropy, and while the tracked list is public a reader could hash
        every entry and read off the match -- a confirmation oracle wearing a
        redaction's clothes. An ordinal is exactly as informative as the token
        to someone holding the list, and exactly as informative as nothing to
        someone who is not.
        """
        who = f"carrier entry #{index}" if index is not None else f"blocklisted token {self.token!r}"
        return f"{self.path}:{self.line}: {who}"


def load_blocklist(blocklist_path: Path) -> list[str]:
    """One token per non-blank, non-comment line.

    Raises on an EMPTY token set. A blocklist reduced to comments makes every
    scan match nothing and exit 0 -- a guard that reports green while checking
    for no identifiers at all. That is the same vacuous-control failure this
    guard exists to prevent, so it fails closed rather than passing quietly.
    """
    tokens: list[str] = []
    for raw in blocklist_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        tokens.append(line)
    if not tokens:
        raise SystemExit(
            f"check_anonymization: blocklist {blocklist_path} defines ZERO tokens. "
            "A guard with an empty blocklist passes everything; refusing to report "
            "green. Restore the tokens, or delete the guard deliberately."
        )
    return tokens


LOCAL_SUPPLEMENT = "anonymization_blocklist.local.txt"
LOCAL_LEDGER = "anonymization_ledger.local.txt"


def load_local_supplement(blocklist_path: Path) -> list[str]:
    """Optional PRIVATE half of a split blocklist; absent is legal.

    The tracked list carries only tokens that already occur in this public
    tree. Prophylactic tokens -- ones that do not yet occur here -- live
    outside the repo, because listing them here would be their first
    publication. This reads an optional plain file with no import, no
    dependency and no secret, so the guard's pure-stdlib,
    runs-without-the-project-environment property is preserved.

    The file is a gitignored symlink created by a per-machine setup step. This
    repo deliberately does NOT name where it points: the public tree carries no
    reference to any private location, so resolving it here by path or by
    environment variable would move a disclosure rather than remove one.
    """
    supp = blocklist_path.parent / LOCAL_SUPPLEMENT
    if not supp.exists():
        return []
    return [
        line
        for raw in supp.read_text(encoding="utf-8").splitlines()
        if (line := raw.strip()) and not line.startswith("#")
    ]


def load_carrier(blocklist_path: Path) -> list[str]:
    """The carrier is the PRIVATE file only; this repository tracks no tokens.

    Fails closed when the carrier is unreachable or defines no tokens, because a
    guard with no ground truth that reports green is the vacuous control this
    module already refuses. Exits 2, not 1: 1 is this module's scan verdict, and
    a caller must tell a dirty tree from a machine that never linked the carrier.
    """
    tokens = load_local_supplement(blocklist_path)
    if not tokens:
        print(
            f"check_anonymization: no carrier is reachable at "
            f"{blocklist_path.parent / LOCAL_SUPPLEMENT}, or it defines ZERO tokens. "
            "This repository tracks no blocklist; the carrier lives in the private "
            "companion repo and reaches this tree as a gitignored symlink. Run the "
            "companion repo's setup.sh to link it. In CI the workflow writes it from a "
            "repository secret before the scan.",
            file=sys.stderr,
        )
        raise SystemExit(2)
    return tokens


# Environment variables that point git at a repository directly. They are dropped
# for the main-checkout lookup because git exports GIT_DIR to every hook it runs in
# a linked worktree, naming that worktree's own git directory; with it inherited,
# `--show-prefix` prints an empty line, the carrier is looked for at the top of the
# main checkout instead of in its scripts/, and the guard exits 2 on every hook
# commit from a worktree. Dropping them also stops a variable naming another repository from
# answering in place of the one the blocklist's directory belongs to.
_GIT_LOCATION_VARS = ("GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR")


def main_checkout_anchor(blocklist_path: Path) -> Path | None:
    """The blocklist's path at the same repository-relative place in the main checkout.

    The carrier and ledger are gitignored files that exist in ONE checkout (on a
    developer machine, links a per-machine setup step creates there), so every
    other worktree of the same repository has neither, and the guard would fail
    closed there even on a machine that holds them. Every worktree shares one git common directory, whose parent is the
    main checkout, so the main checkout's copy of the blocklist's directory is
    the one place the same links can be found.

    The lookup is anchored on the blocklist's own directory, which lies inside
    the repository being scanned. Deliberately NOT the current working
    directory: a lookup from there answers for whichever repository the caller
    stands in, so a scan of an unrelated repository would read this machine's
    carrier and stop failing closed.

    Returns None when the directory does not exist or is not inside a git
    repository, or git is unavailable or reports anything unexpected; the
    caller then keeps the original anchor and fails closed as before.
    """
    env = {k: v for k, v in os.environ.items() if k not in _GIT_LOCATION_VARS}
    try:
        proc = subprocess.run(
            [
                "git",
                "-C",
                str(blocklist_path.parent),
                "rev-parse",
                "--path-format=absolute",
                "--git-common-dir",
                "--show-prefix",
            ],
            capture_output=True,
            check=False,
            env=env,
        )
    except OSError:  # git is not on PATH
        return None
    if proc.returncode != 0:
        return None
    lines = proc.stdout.decode("utf-8", "replace").splitlines()
    if len(lines) != 2:
        return None
    common_dir, prefix = lines
    return Path(common_dir).parent / prefix / blocklist_path.name


def resolve_default_anchor(blocklist_path: Path) -> Path:
    """The anchor whose directory holds the carrier: this checkout's, else the main checkout's.

    The carrier and the ledger are always read from ONE directory, the first of
    the two in which the carrier FILE exists. Deliberately NOT a per-file
    fallback: a ledger read from the main checkout beside a carrier read from
    this one would key every acceptance on another file's token order, so an
    acceptance recorded for one token could silence a different token on the
    same line.

    Absence is the only trigger. A carrier that exists but defines no tokens is
    a broken carrier, not a missing one, and falling back past it would let the
    main checkout's copy hide the breakage, so it keeps this anchor and
    `load_carrier` refuses it with exit 2. When neither directory holds a carrier
    the original anchor is returned, and the scan fails closed exactly as it did
    before this lookup existed.

    Applied only to the default anchor. An explicit `--blocklist` names the
    blocklist file whose directory the caller wants read, and silently reading another one instead
    would make that option untrustworthy.
    """
    if (blocklist_path.parent / LOCAL_SUPPLEMENT).exists():
        return blocklist_path
    fallback = main_checkout_anchor(blocklist_path)
    if fallback is not None and (fallback.parent / LOCAL_SUPPLEMENT).exists():
        return fallback
    return blocklist_path


def compile_patterns(tokens: list[str]) -> list[tuple[str, re.Pattern[str]]]:
    """(token, whole-word case-insensitive literal pattern) per token."""
    return [(t, re.compile(r"\b" + re.escape(t) + r"\b", re.IGNORECASE)) for t in tokens]


def tracked_files(root: Path) -> list[str]:
    """Repo-relative paths of the git-tracked set (NUL-delimited, space-safe)."""
    try:
        proc = subprocess.run(
            ["git", "ls-files", "-z"],
            cwd=root,
            check=True,
            capture_output=True,
        )
    except FileNotFoundError as exc:  # git not on PATH
        raise SystemExit(f"check_anonymization: 'git' not found on PATH: {exc}") from exc
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.decode("utf-8", "replace").strip() if exc.stderr else ""
        raise SystemExit(
            f"check_anonymization: 'git ls-files' failed in {root!r} (not a git repository?): {stderr}"
        ) from exc
    return [p for p in proc.stdout.decode("utf-8").split("\0") if p]


def _read_text_or_none(path: Path) -> str | None:
    """Return decoded text, or None for a binary / absent / unreadable file (skip)."""
    try:
        data = path.read_bytes()
    except (FileNotFoundError, OSError):
        # git ls-files reports tracked-but-deleted paths (rm'd, not yet committed);
        # an absent or unreadable file cannot carry a textual identifier — skip it.
        return None
    if b"\x00" in data[:8192]:
        return None
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return None


def line_fingerprint(line: str) -> str:
    """sha256 of the stripped line -- the content the acceptance is pinned to.

    The LINE is hashed rather than the token because the tokens are fixed
    literals from a hand-authored blocklist. Hashing the token would hash a
    constant and carry no information, degrading the entry to "accept this
    token anywhere in this file, forever" -- a path exclusion with extra steps.
    """
    return hashlib.sha256(line.strip().encode("utf-8")).hexdigest()


def load_baseline(baseline_path: Path) -> set[tuple[str, int, str]]:
    """Accepted findings as {(path, carrier-ordinal, fingerprint)}. Absent file = empty set."""
    accepted: set[tuple[str, int, str]] = set()
    if not baseline_path.exists():
        return accepted
    for raw in baseline_path.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) != 3:
            raise SystemExit(
                f"check_anonymization: malformed baseline row in {baseline_path}: {raw!r}. "
                "Expected: {sha256}  {path}  {carrier-ordinal}"
            )
        fp, rel, ordinal = parts
        accepted.add((rel, int(ordinal), fp))
    return accepted


def scan(
    root: Path, blocklist_path: Path, baseline_path: Path | None = None
) -> tuple[list[Hit], set[tuple[str, int, str]], list[str]]:
    """Return (unaccepted hits, stale baseline rows, the loaded token order).

    The token order is returned so the caller can report a finding by its
    1-based INDEX rather than by its literal text. Tracked entries occupy
    1..len(tracked); supplement entries follow.

    A stale row is one that matched nothing this scan -- its line changed, moved
    file, or was deleted. Stale rows fail the guard rather than being silently
    pruned, so the baseline cannot accumulate entries that protect nothing.
    """
    tokens = load_carrier(blocklist_path)
    patterns = compile_patterns(tokens)
    accepted = load_baseline(baseline_path) if baseline_path is not None else set()
    ordinal_of = {t: i for i, t in enumerate(tokens, start=1)}
    matched: set[tuple[str, int, str]] = set()
    hits: list[Hit] = []
    for rel in tracked_files(root):
        if rel in _SELF_EXCLUDE:
            continue
        text = _read_text_or_none(root / rel)
        if text is None:
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            for token, pat in patterns:
                if pat.search(line):
                    key = (rel, ordinal_of[token], line_fingerprint(line))
                    if key in accepted:
                        matched.add(key)
                        continue
                    hits.append(Hit(rel, lineno, token))
    return hits, accepted - matched, tokens


COUNT_ONLY_PREFIX = "anonymization-guard:"


def count_only_in_force(flag: bool, environ: Mapping[str, str] | None = None) -> bool:
    """Count-only reporting is in force when asked for, and always on a GitHub runner.

    A CI log of this public repository is public, and a finding's path and line
    point at a public line that holds the private token -- so in CI the guard
    reports numbers only. GitHub sets GITHUB_ACTIONS=true on every runner; reading
    it means a workflow that forgets the flag still cannot print a location.
    """
    env = os.environ if environ is None else environ
    return flag or env.get("GITHUB_ACTIONS") == "true"


def _count_only_main(args: argparse.Namespace, blocklist: Path) -> int:
    """Scan and print ONE line of counts; never a path, a line number or a token."""
    if args.list_only or args.reveal:
        print(f"{COUNT_ONLY_PREFIX} --list and --reveal are refused in count-only mode", file=sys.stderr)
        return 2
    baseline = args.baseline or (blocklist.parent / LOCAL_LEDGER)
    # Every failure below is reported by a FIXED sentence. An exception's own text can
    # quote a carrier or ledger cell (a malformed ordinal raises ValueError naming it),
    # and in CI that text would land in a public log.
    try:
        if not load_local_supplement(blocklist):
            print(f"{COUNT_ONLY_PREFIX} no carrier is reachable, or it defines zero tokens", file=sys.stderr)
            return 2
        ledger_rows = len(load_baseline(baseline))
        hits, stale, tokens = scan(args.root, blocklist, baseline)
    except (SystemExit, Exception):
        print(
            f"{COUNT_ONLY_PREFIX} the carrier, the ledger or the tracked-file list could not be read", file=sys.stderr
        )
        return 2
    print(
        f"{COUNT_ONLY_PREFIX} {len(hits)} finding(s), {len(stale)} stale ledger row(s); "
        f"carrier {len(tokens)} token(s), ledger {ledger_rows} row(s)"
    )
    return 1 if hits or stale else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=REPO_ROOT, help="repo root to scan")
    parser.add_argument(
        "--blocklist",
        type=Path,
        default=None,
        help=(
            "path of the blocklist file; the carrier and ledger are read from its directory "
            "(default: <root>/scripts/anonymization_blocklist.txt, or the same path in the main checkout "
            "when this checkout has no carrier)"
        ),
    )
    parser.add_argument(
        "--list",
        "--dry-run",
        dest="list_only",
        action="store_true",
        help=(
            "print blocklist + scan scope and exit 0 without failing. --list does not "
            "fail on FINDINGS; --require-supplement still applies, because a missing "
            "supplement is a coverage fact, not a finding"
        ),
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        default=None,
        help=f"acceptance ledger (default: the gitignored {LOCAL_LEDGER} beside the carrier)",
    )
    parser.add_argument(
        "--reveal",
        action="store_true",
        help="with --list, print token TEXT as well as indices (never use in CI: the logs are public)",
    )
    parser.add_argument(
        "--require-supplement",
        action="store_true",
        help=(
            "exit 2 if the private supplement is absent. For a caller that has just "
            "established it (the estate's setup.sh); NOT for the tracked pre-commit "
            "config, which every clone shares and no third party can satisfy"
        ),
    )
    parser.add_argument(
        "--count-only",
        action="store_true",
        help=(
            "print one line of counts and nothing that locates a finding; always in force "
            "when GITHUB_ACTIONS=true. Refuses --list and --reveal"
        ),
    )
    parser.add_argument("--format", choices=("text", "json"), default="text")
    args = parser.parse_args(argv)

    if args.format == "json":
        raise SystemExit("check_anonymization: --format json not yet implemented")

    # Every read below (carrier, ledger, --require-supplement, --list) goes through
    # this one anchor, so the fallback cannot reach some of them and miss others.
    blocklist = args.blocklist or resolve_default_anchor(args.root / "scripts" / "anonymization_blocklist.txt")

    if count_only_in_force(args.count_only):
        return _count_only_main(args, blocklist)

    if args.require_supplement and not load_local_supplement(blocklist):
        # Checked BEFORE the scan on purpose. A missing supplement is a statement
        # about COVERAGE, not a finding, and a clean scan printed above a fatal
        # error invites the reader to believe the clean result meant something.
        # Exit 2, not 1: 1 is this module's scan verdict ("something was found"),
        # and a caller needs to tell a dirty tree from a misconfigured machine.
        print(
            f"check_anonymization: FAILED -- --require-supplement was passed but no "
            f"{LOCAL_SUPPLEMENT} was found. The prophylactic tokens are NOT being "
            "checked. On a developer machine, run the estate's setup.sh to link it. "
            "CI does not pass this flag: its workflow writes the carrier from a "
            "repository secret and checks it is non-empty first.",
            file=sys.stderr,
        )
        return 2

    if args.list_only:
        tokens = load_carrier(blocklist)
        files = tracked_files(args.root)
        print(f"carrier: {len(tokens)} token(s) from {LOCAL_SUPPLEMENT}")
        if args.reveal:
            for i, t in enumerate(tokens, start=1):
                print(f"  #{i} {t}")
        else:
            # Indices only by default. --list is one paste away from a CI
            # workflow, and rendering token text there would publish the whole
            # list into a public log. The debugging affordance people actually
            # want ("does the guard see my list?") is answered by the counts.
            for i in range(1, len(tokens) + 1):
                print(f"  #{i} (text hidden; pass --reveal to print it)")
        print(f"would scan {len(files)} tracked file(s) (minus {len(_SELF_EXCLUDE)} self-excluded)")
        return 0

    baseline = args.baseline or (blocklist.parent / LOCAL_LEDGER)
    hits, stale, tokens = scan(args.root, blocklist, baseline)
    index_of = {t: i for i, t in enumerate(tokens, start=1)}
    if stale:
        print(
            "Anonymization guard FAILED (stale baseline entries — the accepted line changed or moved):",
            file=sys.stderr,
        )
        for rel, ordinal, fp in sorted(stale):
            print(
                f"  {rel}: accepted carrier entry #{ordinal} (fingerprint {fp[:12]}...) no longer matches",
                file=sys.stderr,
            )
    if hits:
        print("Anonymization guard FAILED (blocklisted identifiers in tracked files):", file=sys.stderr)
        for h in hits:
            print(f"  {h.render(index_of.get(h.token))}", file=sys.stderr)
        return 1
    return 1 if stale else 0


if __name__ == "__main__":
    sys.exit(main())
