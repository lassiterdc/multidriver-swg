"""The identity class: tokens from the PRIVATE carrier, matched against the accepted-findings ledger.

This repository tracks no token list. The carrier -- the independent ground truth of developer
and machine identifiers -- is a gitignored file that reaches a checkout from the private
companion repository's setup step, and in CI the workflow writes it from a repository secret
before the scan. Listing a token in this repository would be its first publication.

The ledger is the accepted-findings list: one row per finding someone has decided to keep, pinned
to the CONTENT of the line (a sha256 of its stripped text) and to the carrier ORDINAL that matched
it, and scoped to one path. Editing the line kills the acceptance; a row that matches nothing is
stale and fails the scan, so the ledger cannot accumulate entries that protect nothing.

Failures here fail CLOSED. A carrier reduced to comments makes every scan match nothing and exit
0 -- a guard that reports green while checking for no identifiers at all -- so it is refused
rather than trusted, with exit 2, which a caller must read as "this machine never linked the
private half", never as the scan's verdict.

The label text is an ORDINAL, never a token. A failing run of this scanner on a public repository
is public.
"""

from __future__ import annotations

import hashlib
import os
import re
import subprocess
from pathlib import Path

try:  # imported as part of the scanner's package (the test suite, and `python3 -m`)
    from . import core
except ImportError:  # run as a directory: `python3 scripts/content_scan` puts this directory on sys.path
    import core  # type: ignore[no-redef]

NAME = "identity"

# There is no PUBLIC label shape for this class: its labels are the carrier's entries, and the
# carrier is the one list this repository does not publish. The clone-carried test therefore
# supplies a synthetic carrier instead, which exercises the whole path without a private token.
LABEL_SHAPES: tuple[str, ...] = ()

SKIP_MARKER = None

LOCAL_SUPPLEMENT = "anonymization_blocklist.local.txt"
LOCAL_LEDGER = "anonymization_ledger.local.txt"

# Environment variables that point git at a repository directly. They are dropped for the
# main-checkout lookup because git exports GIT_DIR to every hook it runs in a linked worktree,
# naming that worktree's own git directory; with it inherited, `--show-prefix` prints an empty
# line, the carrier is looked for at the top of the main checkout instead of in its scripts/, and
# the scan exits 2 on every hook commit from a worktree. Dropping them also stops a variable
# naming another repository from answering in place of the one the blocklist's directory belongs to.
_GIT_LOCATION_VARS = ("GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR")


def _read_private(path: Path) -> str:
    """A private file's text, or `core.UnreadableError` when its bytes are not UTF-8.

    Both forms read the carrier and the ledger, and the located form would otherwise leave the
    interpreter's exit status 1 on an uncaught decode error -- the number this module reserves for
    a finding. The sentence never quotes the bytes, so it is safe to print where a ledger row
    would not be.
    """
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise core.UnreadableError(
            f"{path} holds bytes that are not UTF-8, so neither its tokens nor its acceptance rows "
            "can be read. Restore the file, or relink it from the private companion repo."
        ) from exc
    except OSError as exc:
        raise core.UnreadableError(f"{path} could not be read: {exc.strerror}") from exc


def load_local_supplement(blocklist_path: Path) -> list[str]:
    """The carrier's tokens: one per non-blank, non-comment line. Absent is an empty list.

    The file is a gitignored symlink created by a per-machine setup step. This repository
    deliberately does NOT name where it points: the public tree carries no reference to any
    private location, so resolving it here by path or by environment variable would move a
    disclosure rather than remove one.
    """
    carrier = blocklist_path.parent / LOCAL_SUPPLEMENT
    if not carrier.exists():
        return []
    return [line for raw in _read_private(carrier).splitlines() if (line := raw.strip()) and not line.startswith("#")]


def load_carrier(blocklist_path: Path) -> list[str]:
    """The carrier's tokens, or `core.UnreadableError` when it is unreachable or defines none.

    Fails closed for the reason the module states: a guard with no ground truth that reports
    green is the vacuous control this module refuses.
    """
    tokens = load_local_supplement(blocklist_path)
    if not tokens:
        raise core.UnreadableError(
            f"no carrier is reachable at {blocklist_path.parent / LOCAL_SUPPLEMENT}, or it defines ZERO "
            "tokens. This repository tracks no blocklist; the carrier lives in the private companion "
            "repo and reaches this tree as a gitignored symlink. Run the companion repo's setup.sh to "
            "link it. In CI the workflow writes it from a repository secret before the scan."
        )
    return tokens


def main_checkout_anchor(blocklist_path: Path) -> Path | None:
    """The blocklist's path at the same repository-relative place in the main checkout.

    The carrier and ledger are gitignored files that exist in ONE checkout (on a developer
    machine, links a per-machine setup step creates there), so every other worktree of the same
    repository has neither, and the scan would fail closed there even on a machine that holds
    them. Every worktree shares one git common directory, whose parent is the main checkout, so
    the main checkout's copy of the blocklist's directory is the one place the same links can be
    found.

    The lookup is anchored on the blocklist's own directory, which lies inside the repository
    being scanned. Deliberately NOT the current working directory: a lookup from there answers
    for whichever repository the caller stands in, so a scan of an unrelated repository would read
    this machine's carrier and stop failing closed.

    Returns None when the directory does not exist or is not inside a git repository, or git is
    unavailable or reports anything unexpected; the caller then keeps the original anchor and
    fails closed as before.
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

    The carrier and the ledger are always read from ONE directory, the first of the two in which
    the carrier FILE exists. Deliberately NOT a per-file fallback: a ledger read from the main
    checkout beside a carrier read from this one would key every acceptance on another file's
    token order, so an acceptance recorded for one token could silence a different token on the
    same line.

    Absence is the only trigger. A carrier that exists but defines no tokens is a broken carrier,
    not a missing one, and falling back past it would let the main checkout's copy hide the
    breakage, so it keeps this anchor and `load_carrier` refuses it. When neither directory holds
    a carrier the original anchor is returned, and the scan fails closed exactly as it did before
    this lookup existed.

    Applied only to the default anchor. An explicit `--blocklist` names the blocklist file whose
    directory the caller wants read, and silently reading another one instead would make that
    option untrustworthy.
    """
    if (blocklist_path.parent / LOCAL_SUPPLEMENT).exists():
        return blocklist_path
    fallback = main_checkout_anchor(blocklist_path)
    if fallback is not None and (fallback.parent / LOCAL_SUPPLEMENT).exists():
        return fallback
    return blocklist_path


def line_fingerprint(line: str) -> str:
    """sha256 of the stripped line -- the content the acceptance is pinned to.

    The LINE is hashed rather than the token because the tokens are fixed literals from a
    hand-authored blocklist. Hashing the token would hash a constant and carry no information,
    degrading the entry to "accept this token anywhere in this file, forever" -- a path exclusion
    with extra steps.
    """
    return hashlib.sha256(line.strip().encode("utf-8")).hexdigest()


def load_baseline(baseline_path: Path) -> set[tuple[str, int, str]]:
    """Accepted findings as {(path, carrier-ordinal, fingerprint)}. Absent file = empty set."""
    accepted: set[tuple[str, int, str]] = set()
    if not baseline_path.exists():
        return accepted
    for raw in _read_private(baseline_path).splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) != 3:
            raise core.UnreadableError(
                f"a malformed ledger row in {baseline_path}: {raw!r}. "
                "Expected: {sha256}  {path}  {carrier-ordinal}"
            )
        fingerprint, rel, ordinal = parts
        try:
            accepted.add((rel, int(ordinal), fingerprint))
        except ValueError as exc:
            raise core.UnreadableError(f"a malformed ledger row in {baseline_path}: {raw!r}") from exc
    return accepted


def compile_patterns(tokens: list[str]) -> tuple[core.Label, ...]:
    """One whole-word case-insensitive literal label per carrier token, named by its ordinal."""
    return tuple(
        core.Label(f"carrier entry #{index}", re.compile(r"\b" + re.escape(token) + r"\b", re.IGNORECASE))
        for index, token in enumerate(tokens, start=1)
    )


def load(
    root: Path,
    *,
    blocklist: Path | None = None,
    baseline: Path | None = None,
    require_supplement: bool = False,
) -> core.Matcher:
    """Read the carrier and the ledger and return the identity class's matcher.

    `require_supplement` serves a caller that has just established the carrier (the estate's
    setup step): it refuses a run where the carrier is missing, where a missing carrier would
    otherwise read as a clean tree. The tracked pre-commit config does not pass it, because every
    clone shares that config and no third party can satisfy it.
    """
    anchor = blocklist
    if anchor is None:
        anchor = resolve_default_anchor(root / "scripts" / "anonymization_blocklist.txt")
    if require_supplement and not load_local_supplement(anchor):
        raise core.UnreadableError(
            f"--require-supplement was passed but no {LOCAL_SUPPLEMENT} was found. The prophylactic "
            "tokens are NOT being checked. On a developer machine, run the estate's setup.sh to link "
            "it. CI does not pass this flag: its workflow writes the carrier from a repository "
            "secret and checks it is non-empty first."
        )
    labels = compile_patterns(load_carrier(anchor))
    accepted = load_baseline(baseline or anchor.parent / LOCAL_LEDGER)
    ordinal_of = {label.name: index for index, label in enumerate(labels, start=1)}
    matched: set[tuple[str, int, str]] = set()

    def accepts(rel: str, lineno: int, line: str, label: core.Label) -> bool:
        key = (rel, ordinal_of[label.name], line_fingerprint(line))
        if key not in accepted:
            return False
        matched.add(key)
        return True

    def stale() -> tuple[str, ...]:
        return tuple(
            f"{rel}: accepted carrier entry #{ordinal} (fingerprint {fingerprint[:12]}...) no longer matches"
            for rel, ordinal, fingerprint in sorted(accepted - matched)
        )

    return core.Matcher(
        name=NAME,
        labels=labels,
        predicate=core.line_predicate(labels),
        accepts=accepts,
        stale=stale,
        ledger_note=lambda: f"carrier {len(labels)} token(s), ledger {len(accepted)} row(s)",
        loaded="the private carrier",
    )
