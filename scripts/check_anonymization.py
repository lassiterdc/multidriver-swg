#!/usr/bin/env python3
"""CI check enforcing the anonymization blocklist.

Enumerates the git-tracked set (`git ls-files`) and fails if any tracked text
file contains a genuinely-private identifier listed in the INDEPENDENT
ground-truth blocklist `scripts/anonymization_blocklist.txt`. Working-tree scrub
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
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Files that legitimately CONTAIN blocklisted tokens and must never self-match.
_SELF_EXCLUDE = frozenset(
    {
        "scripts/anonymization_blocklist.txt",
        "scripts/anonymization_baseline.txt",
        "scripts/check_anonymization.py",
    }
)


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
        who = f"blocklist entry #{index}" if index is not None else f"blocklisted token {self.token!r}"
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


def load_baseline(baseline_path: Path) -> set[tuple[str, str, str]]:
    """Accepted findings as {(path, token, fingerprint)}. Absent file = empty set."""
    accepted: set[tuple[str, str, str]] = set()
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
                "Expected: {sha256}  {path}  {token}"
            )
        fp, rel, token = parts
        accepted.add((rel, token, fp))
    return accepted


def scan(
    root: Path, blocklist_path: Path, baseline_path: Path | None = None
) -> tuple[list[Hit], set[tuple[str, str, str]], list[str]]:
    """Return (unaccepted hits, stale baseline rows, the loaded token order).

    The token order is returned so the caller can report a finding by its
    1-based INDEX rather than by its literal text. Tracked entries occupy
    1..len(tracked); supplement entries follow.

    A stale row is one that matched nothing this scan -- its line changed, moved
    file, or was deleted. Stale rows fail the guard rather than being silently
    pruned, so the baseline cannot accumulate entries that protect nothing.
    """
    tracked = load_blocklist(blocklist_path)
    supplement = load_local_supplement(blocklist_path)
    tokens = tracked + supplement
    patterns = compile_patterns(tokens)
    accepted = load_baseline(baseline_path) if baseline_path is not None else set()
    matched: set[tuple[str, str, str]] = set()
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
                    key = (rel, token, line_fingerprint(line))
                    if key in accepted:
                        matched.add(key)
                        continue
                    hits.append(Hit(rel, lineno, token))
    return hits, accepted - matched, tokens


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=REPO_ROOT, help="repo root to scan")
    parser.add_argument(
        "--blocklist",
        type=Path,
        default=None,
        help="blocklist file (default: <root>/scripts/anonymization_blocklist.txt)",
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
        help="baseline file of accepted findings (default: <root>/scripts/anonymization_baseline.txt)",
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
    parser.add_argument("--format", choices=("text", "json"), default="text")
    args = parser.parse_args(argv)

    if args.format == "json":
        raise SystemExit("check_anonymization: --format json not yet implemented")

    blocklist = args.blocklist or (args.root / "scripts" / "anonymization_blocklist.txt")

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
            "If you are seeing this in CI, the flag is mis-wired: CI legitimately "
            "has no supplement and must not pass this flag.",
            file=sys.stderr,
        )
        return 2

    if args.list_only:
        tokens = load_blocklist(blocklist)
        files = tracked_files(args.root)
        supplement = load_local_supplement(blocklist)
        print(
            f"blocklist: {len(tokens)} tracked token(s) from {blocklist}; "
            f"{len(supplement)} supplement token(s) from {LOCAL_SUPPLEMENT}"
        )
        if args.reveal:
            for i, t in enumerate(tokens + supplement, start=1):
                print(f"  #{i} {t}")
        else:
            # Indices only by default. --list is one paste away from a CI
            # workflow, and rendering token text there would publish the whole
            # list into a public log. The debugging affordance people actually
            # want ("does the guard see my list?") is answered by the counts.
            for i in range(1, len(tokens) + len(supplement) + 1):
                print(f"  #{i} (text hidden; pass --reveal to print it)")
        print(f"would scan {len(files)} tracked file(s) (minus {len(_SELF_EXCLUDE)} self-excluded)")
        return 0

    baseline = args.baseline or (args.root / "scripts" / "anonymization_baseline.txt")
    hits, stale, tokens = scan(args.root, blocklist, baseline)
    index_of = {t: i for i, t in enumerate(tokens, start=1)}
    if len(tokens) == len(load_blocklist(blocklist)):
        # Absence is legal (CI never has it) but must not be SILENT: on a
        # developer machine it means the per-machine setup step has not run and
        # the prophylactic tokens are unguarded. Exit code deliberately
        # unchanged -- this is visibility, not a gate.
        print(
            f"check_anonymization: NOTE -- no {LOCAL_SUPPLEMENT} found; scanning "
            f"{len(tokens)} tracked token(s) only. On a developer machine, run the "
            "estate's setup.sh to link the private supplement.",
            file=sys.stderr,
        )
    if stale:
        print(
            "Anonymization guard FAILED (stale baseline entries — the accepted line changed or moved):",
            file=sys.stderr,
        )
        for rel, token, fp in sorted(stale):
            print(
                f"  {rel}: accepted blocklist entry #{index_of.get(token, '?')} "
                f"(fingerprint {fp[:12]}...) no longer matches",
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
