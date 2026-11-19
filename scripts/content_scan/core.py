"""The content scanner's reading contract: the corpus, the skip markers, the report form and the exit codes.

This module owns WHAT is read and HOW a run is reported. A matcher module owns ONE class of
content and nothing else: `identity`, `machine_path`, `planning_reference` and `secret` each
expose their class name, the public list of their label shapes and a `load()` that returns a
`Matcher`. One core and one matcher per class is the whole layout; nothing here knows which
class it is reporting.

Standard library only, and no import from `src/`: the scan must run before any project
environment exists, because repo scaffolding -- the window in which a private path is most likely
to be committed -- happens first. `tests/test_content_scan.py::test_the_scanner_imports_nothing_from_src`
enforces it, and `python3 scripts/content_scan` runs this directory as a script with no install
at all.

Two report forms, and the choice is not the caller's on a runner. A located report names the
path, the line and the class label; a count-only report prints one line of counts and nothing
that locates a finding. A CI log of a public repository is public, so a finding's path and line
would point at a public line holding the private token -- `count_only_in_force` therefore forces
the count-only form whenever `GITHUB_ACTIONS` is set, whether or not a workflow passes the flag.

Exit codes: 0 clean, 1 findings, 2 the run could not report (a usage error, an unreadable
pattern source or ledger, or a corpus holding nothing to scan). 1 and 2 are kept apart on
purpose: a caller must be able to tell a dirty tree from a machine that never linked the private
half of the scanner.
"""

from __future__ import annotations

import os
import re
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path

EXIT_CLEAN = 0
EXIT_FINDINGS = 1
EXIT_UNUSABLE = 2

# `scripts/content_scan/core.py` -> the repository root this scanner guards.
REPO_ROOT = Path(__file__).resolve().parents[2]

# Files that would report once a class reads them: this scanner's own modules state the patterns,
# and its two test modules carry one sample per label shape. Only a class whose PATTERN actually
# matches this text skips them -- `machine_path`, whose `[A-Za-z]:[\\/]` and sync-folder
# alternations match the pattern tables themselves. The other three classes read them: the
# identity guard this core replaces excluded NOTHING, and the secret class's own module says it
# has no allowlist, so a global skip here would be a file-level skip marker that silences a real
# token pasted into one of these files.
SELF_EXCLUDED: frozenset[str] = frozenset(
    {
        "scripts/content_scan/__main__.py",
        "scripts/content_scan/core.py",
        "scripts/content_scan/identity.py",
        "scripts/content_scan/machine_path.py",
        "scripts/content_scan/planning_reference.py",
        "scripts/content_scan/secret.py",
        "tests/test_content_scan.py",
        "tests/test_content_scan_review.py",
    }
)

COUNT_ONLY_PREFIX = "content-scan:"
COUNT_ONLY_UNREADABLE_ERROR = f"{COUNT_ONLY_PREFIX} the pattern source, the ledger or the corpus could not be read"


class UnreadableError(Exception):
    """A pattern source, a ledger or the corpus could not be read.

    The message is the sentence a reader needs, and it may name a private path or quote a
    ledger cell. A caller that must not print such a sentence -- the count-only form, whose log
    is public -- reports `COUNT_ONLY_UNREADABLE_ERROR` instead and never touches this text.
    """


@dataclass(frozen=True)
class Label:
    """One thing a class looks for: the name a report calls it, and the pattern that finds it.

    The name is always printable. Where the pattern source is private (the identity class reads
    a list this repository does not track) the name is an ORDINAL into that list and carries no
    token-derived bytes: a digest would not do, because the candidate set is small and
    low-entropy, so a reader holding the list could hash every entry and read the match off -- a
    confirmation oracle wearing a redaction's clothes.
    """

    name: str
    pattern: re.Pattern[str]


@dataclass(frozen=True)
class Hit:
    """One reported line, or one reported path, of one class."""

    matcher: str
    path: str  # repo-relative
    line: int | None  # None for a hit a class's path predicate made
    label: str  # the comma-joined names of every label that fired here

    def render(self) -> str:
        if self.line is None:
            return f"{self.path}: {self.label} (in the path)"
        return f"{self.path}:{self.line}: {self.label}"


@dataclass
class ClassReport:
    """What one class's pass over the corpus found."""

    name: str
    labels: int
    loaded: str
    findings: list[Hit] = field(default_factory=list)
    kept: int = 0
    stale: tuple[str, ...] = ()
    ledger: str | None = None


@dataclass
class Report:
    """What one scan found, class by class, plus the skip lines the located form prints."""

    classes: list[ClassReport] = field(default_factory=list)
    scanned: int = 0
    skipped: list[str] = field(default_factory=list)

    @property
    def hits(self) -> list[Hit]:
        return [hit for report in self.classes for hit in report.findings]

    @property
    def stale(self) -> list[tuple[str, str]]:
        return [(report.name, row) for report in self.classes for row in report.stale]

    @property
    def refused(self) -> bool:
        """True when this scan is not clean: a finding, or an accepted row that protects nothing."""
        return bool(self.hits or self.stale)


def _everything(rel: str) -> bool:
    return True


def _no_path_labels(rel: str) -> tuple[Label, ...]:
    return ()


def _accept_nothing(rel: str, lineno: int, line: str, label: Label) -> bool:
    return False


def _no_stale() -> tuple[str, ...]:
    return ()


def tracked_files(root: Path) -> list[str]:
    """Repo-relative paths of the git-tracked set (NUL-delimited, space-safe)."""
    return _git_paths(root, ["ls-files", "-z"])


def working_tree_files(root: Path) -> list[str]:
    """Repo-relative paths of the tracked set plus the untracked files git does not ignore.

    This is the wider corpus a class reads when its whole point is that an inherited line must
    fail for the next author, and not only for whoever added it: a file that is staged but not
    yet committed is part of what the next push publishes.
    """
    return sorted(_git_paths(root, ["ls-files", "-co", "--exclude-standard", "-z"]))


def _git_paths(root: Path, args: list[str]) -> list[str]:
    try:
        proc = subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)
    except FileNotFoundError as exc:  # git not on PATH
        raise UnreadableError(f"the content scan: 'git' not found on PATH: {exc}") from exc
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.decode("utf-8", "replace").strip() if exc.stderr else ""
        raise UnreadableError(
            f"the content scan: 'git {' '.join(args)}' failed in {root!r} (not a git repository?): {stderr}"
        ) from exc
    return [p for p in proc.stdout.decode("utf-8").split("\0") if p]


def read_text_or_none(path: Path) -> str | None:
    """Return decoded text, or None for a binary / absent / unreadable file (skip).

    A NUL anywhere marks a binary file, not only one in its first block: a text file whose NUL
    sits past an arbitrary window would be scanned as text, and the window is a speed hack that
    makes the scan's coverage depend on where the byte lands.
    """
    try:
        data = path.read_bytes()
    except OSError:
        # git reports tracked-but-deleted paths (rm'd, not yet committed); an absent or
        # unreadable file cannot carry textual content -- skip it. A directory named by the
        # corpus raises here too, and is skipped for the same reason.
        return None
    if b"\0" in data:
        return None
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return None


def read_bytes_or_none(path: Path) -> str | None:
    """Return a file's bytes as text with every byte preserved, or None when it cannot be read.

    This is the reading the inline `grep -aqE` step this class replaces performed: `-a` treats a
    binary file as text and the pattern matches bytes, so a `.npz` bearing a home-directory string
    is found rather than skipped. `surrogateescape` maps each byte UTF-8 cannot decode to a lone
    surrogate, so the mapping is reversible and no byte is dropped or replaced; a NUL is kept as
    itself. Line numbers are therefore the file's own newlines, the same as `grep -a` counts.
    """
    try:
        return path.read_bytes().decode("utf-8", "surrogateescape")
    except OSError:
        return None


def read_for(path: Path, reads_bytes: bool) -> str | None:
    """The reader a class asks for: byte-preserving, or text-only with binary files skipped."""
    return read_bytes_or_none(path) if reads_bytes else read_text_or_none(path)


def line_predicate(labels: Sequence[Label]) -> Callable[[str], tuple[Label, ...]]:
    """A class's default line predicate: every label whose pattern searches the line, in order."""

    def predicate(line: str) -> tuple[Label, ...]:
        return tuple(label for label in labels if label.pattern.search(line))

    return predicate


@dataclass
class Matcher:
    """One class of content the scanner refuses, as the core reads it.

    A module builds one of these in its `load()`. Everything a class may vary is a field here
    rather than a branch in `scan`, so a new class is a new module and no edit to the core.
    """

    name: str
    labels: tuple[Label, ...]
    predicate: Callable[[str], tuple[Label, ...]]
    corpus: Callable[[Path], list[str]] = tracked_files
    skip_marker: str | None = None
    # A class that judges only part of the tree says so here: the planning class leaves legacy
    # code out, and the machine-path class leaves the files that carry a path on purpose out.
    scope: Callable[[str], bool] = _everything
    # A class whose pattern matches this scanner's own source skips it here. The default is False
    # so a class that would report its own files must SAY so, rather than inheriting a silence.
    skips_own_files: bool = False
    # The byte-preserving reader, for a class whose predecessor matched binary files as text.
    reads_bytes: bool = False
    path_labels: Callable[[str], tuple[Label, ...]] = _no_path_labels
    # An acceptance is a class-side decision: the identity class pins a finding to one line's
    # content hash, and a class with no ledger accepts nothing.
    accepts: Callable[[str, int, str, Label], bool] = _accept_nothing
    stale: Callable[[], tuple[str, ...]] = _no_stale
    ledger_note: Callable[[], str] | None = None
    loaded: str = ""


def scan(root: Path, matchers: Sequence[Matcher], *, corpus: Callable[[Path], list[str]] | None = None) -> Report:
    """Read the corpus for every matcher and return what each one found.

    A line carrying a matcher's `skip_marker` is counted as kept for THAT matcher alone and is
    not reported. No class may be given a marker another class honours: a marker the scanner
    honoured for secrets would let a real secret through, which is why the secret class
    declares none.

    A path in `SELF_EXCLUDED` is skipped only for a class that sets `skips_own_files`, because a
    skip that applied to every class would be a file-level skip marker and would silence a real
    token in the scanner's own test files, which the identity guard this core replaces read.
    A tracked path whose file is absent is skipped for every class: it is not part of what a
    reader will see, and the old guard skipped it explicitly.

    `corpus` replaces every matcher's own list for one call. The pre-release scan reads the bytes
    an enumeration lists rather than the repository, so it must be able to say what the corpus is
    without changing any class's reading.
    """
    report = Report()
    cache: dict[tuple[str, bool], str | None] = {}
    read_paths: set[str] = set()
    for matcher in matchers:
        found = ClassReport(name=matcher.name, labels=len(matcher.labels), loaded=matcher.loaded)
        if matcher.ledger_note is not None:
            found.ledger = matcher.ledger_note()
        for rel in (corpus or matcher.corpus)(root):
            if matcher.skips_own_files and rel in SELF_EXCLUDED:
                continue
            if not matcher.scope(rel):
                continue
            # A tracked path whose file is gone is not part of what a reader will see, so neither
            # its name nor its content is judged: `git ls-files -co` keeps listing a path deleted
            # on disk until the deletion is staged, and a name-only finding would name a file the
            # tree no longer holds.
            if not (root / rel).is_file():
                continue
            key = (rel, matcher.reads_bytes)
            if key not in cache:
                cache[key] = read_for(root / rel, matcher.reads_bytes)
            text = cache[key]
            if text is None:
                continue
            read_paths.add(rel)
            in_path = matcher.path_labels(rel)
            if in_path:
                found.findings.append(Hit(matcher.name, rel, None, ", ".join(label.name for label in in_path)))
            for lineno, line in enumerate(text.splitlines(), start=1):
                labels = matcher.predicate(line)
                if not labels:
                    continue
                if matcher.skip_marker is not None and matcher.skip_marker in line:
                    found.kept += 1
                    report.skipped.append(f"[skip] {rel}:{lineno}: kept by a {matcher.skip_marker} marker")
                    continue
                reported = [label for label in labels if not matcher.accepts(rel, lineno, line, label)]
                if reported:
                    found.findings.append(Hit(matcher.name, rel, lineno, ", ".join(label.name for label in reported)))
        found.stale = matcher.stale()
        report.classes.append(found)
    report.scanned = len(read_paths)
    return report


def count_only_in_force(flag: bool, environ: Mapping[str, str] | None = None) -> bool:
    """Count-only reporting is in force when asked for, and always on a GitHub runner.

    GitHub sets `GITHUB_ACTIONS=true` on every runner; reading it means a workflow that forgets
    the flag still cannot print a location. The comparison is against the literal `true`, so a
    machine that exports `GITHUB_ACTIONS=false` keeps the located form.
    """
    env = os.environ if environ is None else environ
    return flag or env.get("GITHUB_ACTIONS") == "true"


def count_line(report: Report) -> str:
    """The one line the count-only form prints: counts per class, and nothing that locates a hit."""
    counts = ", ".join(f"{found.name} {len(found.findings)}" for found in report.classes)
    notes = "; ".join(found.ledger for found in report.classes if found.ledger)
    tail = f"; {notes}" if notes else ""
    return f"{COUNT_ONLY_PREFIX} {len(report.hits)} finding(s), {len(report.stale)} stale ledger row(s){tail}; {counts}"


def failure_line(report: Report) -> str:
    """The one line the located form prints above a refusal."""
    return f"{COUNT_ONLY_PREFIX} FAILED -- {len(report.hits)} finding(s), {len(report.stale)} stale ledger row(s)"


def located_lines(report: Report) -> list[str]:
    """The located form's failure lines, in the order the scan met them.

    Every line here names a path or a label, so this form is for a developer's terminal and for
    a log that is not public. The count-only form exists because a CI log is. The skip lines are
    NOT here: a deliberate keep is printed whether or not the run fails, so they travel in
    `Report.skipped`.
    """
    lines = [f"[stale] {name}: {row}" for name, row in report.stale]
    for found in report.classes:
        lines.extend(hit.render() for hit in found.findings)
    return lines


def listing_lines(root: Path, matchers: Sequence[Matcher], reveal: bool) -> list[str]:
    """The `--list` form: what each class would read, and how many files the corpus holds."""
    lines: list[str] = []
    for matcher in matchers:
        lines.append(f"{matcher.name}: {len(matcher.labels)} label(s) from {matcher.loaded}")
        for index, label in enumerate(matcher.labels, start=1):
            lines.append(
                f"  #{index} {label.name}" if reveal else f"  #{index} (text hidden; pass --reveal to print it)"
            )
    corpus = sorted({rel for matcher in matchers for rel in matcher.corpus(root)})
    lines.append(f"would scan {len(corpus)} file(s) (minus {len(SELF_EXCLUDED)} self-excluded)")
    return lines
