"""The planning-reference class: a line that points into the private planning record or the process behind it.

The design and planning record behind this repository is private, and so is the process that
produced it. A decision number, a chunk or package identifier, a constraint label, a path
into that record, a word that only that process uses, or the name of a private tool resolves for
its author and for no reader of this repository, so such a reference is a defect whoever wrote it.

Two kinds of pattern are matched. Identifier shapes (a number, a letter and a number, a path) are
matched wherever they occur. Process words and tool names are matched from a short curated list of
words that have no ordinary use in this repository's text; a word that does (shape, chunk, ruling,
ledger, session, reviewer) is not on it, because a pattern that fires on ordinary prose gets
bypassed. A clean run does not establish that no reference remains: a phrase made of other words,
a sentence copied out of a planning document, and a machine path have no pattern here.

This class reads the WIDER corpus -- tracked files plus the untracked files git does not ignore --
because an inherited reference must fail for the next author and not only for whoever added it.
Legacy code under `_old_code_to_refactor/` is out of scope; the record files beside it are not,
and that directory's README is a record like any other.

To keep a match on purpose, put `planning-ref-ok: {reason}` on the same line. This is the one
class with a skip marker, and the marker is honoured only for this class: a marker the scanner
honoured for secrets would let a real secret through.

The pattern text is written in pieces (`"copier-python-" "template"`), so this module does not
match its own patterns -- a pattern written whole would report this file on every run.
"""

from __future__ import annotations

import re

try:  # imported as part of the scanner's package (the test suite, and `python3 -m`)
    from . import core
except ImportError:  # run as a directory: `python3 scripts/content_scan` puts this directory on sys.path
    import core  # type: ignore[no-redef]

NAME = "planning_reference"

SKIP_MARKER = "planning-ref-ok:"

LEGACY_DIR = "_old_code_to_refactor/"
LEGACY_CODE_SUFFIXES = (".py", ".sh", ".ipynb")

PATTERNS = {
    "decision or record number": re.compile(r"\b(?:ADR|D|U|UC|UG|TO|OD|OI|O)-\d+[a-z]?\b"),
    "letter-named decision": re.compile(r"\bADR-[a-z]\b"),
    "constraint or review label": re.compile(r"\b(?:C-[UDE]\d+|R\d{2,})\b"),
    "two- or three-digit decision": re.compile(r"\bD\d{2,3}\b"),
    "chunk identifier": re.compile(r"(?i:\bchunks?\s+`?\d+[a-z](?:\.\d+)?\b)|\b\d+[a-z]\.\d+\b"),
    "work-package identifier": re.compile(r"(?i:\b(?:work[- ])?packages?\s+\d+[a-z]\b)"),
    "chunk phrase": re.compile(r"(?i:\bwork[ -]?chunks?\b)"),
    "planning-record path": re.compile(
        r"\blibrary/(?:docs|knowledge|prompts|_scratch)\b|\bdocs/plann?ing\b|\$AGENTIC_\w+|\bagentic.workspace\b"
    ),
    "agent role name": re.compile(r"\b[a-z]+(?:-[a-z]+)*-specialist\b"),
    "plan-prompt acronym": re.compile(r"\bPIP\b"),
    "role word": re.compile(r"(?i:\b(?:sea[t]s?|code[r]s?|coordinato[r]s?)\b)"),
    "build-record word": re.compile(r"(?i:\b(?:build|review|reviewer|builder) return\b|\bBuild class\b)"),
    "authority phrase": re.compile(
        r"(?i:\bdevelopers?(?:'s)? (?:directive|instruction|rulings?|rule for|requests?|preferences?)\b"
        r"|\bruled by the developer\b|\bthe developer (?:ruled|decided)\b)"
    ),
    "agent-process word": re.compile(r"(?i:\b(?:absorbing|implementing|reviewing|building) agent\b)"),
    "tool name": re.compile(
        r"\bClaude(?: Code)?\b|\bclaude-"
        r"workspace\b|\bcopier-python-"
        r"template\b|\bsub"
        r"agents?\b"
        r"|\brefactor-"
        r"plan\b|\bplan-then-"
        r"implement\b|\bPlanning Doc(?:ument)?s?\b"
        r"|(?<![\w.])/(?:proceed-with-"
        r"implementation|qaqc-and-"
        r"commit|plan-"
        r"implementation|plan-and-"
        r"implement)\b"
    ),
    "record number in prose": re.compile(r"(?i:\bdecision\s+[0-9]+\.[0-9]+\b|\brows?\s+[0-9]{2,3}\b)"),
    "section-style label": re.compile(r"\b[A-Z][0-9]-[0-9]+\b"),
    "process phrase": re.compile(
        r"(?i:\bdefinition of done\b|\bopen item\b|\bapproved shape\b|\bshape (?:stage|iteration)\b)"
    ),
}

PATH_PATTERNS = {"chunk identifier in a file name": re.compile(r"(?:^|[/_])\d+[a-z]_\d+(?:[_.]|$)")}

LABEL_SHAPES: tuple[str, ...] = (*PATTERNS, *PATH_PATTERNS)


def scope(rel: str) -> bool:
    """Legacy CODE is out of scope: it is being replaced, and its records are read as records."""
    return not (rel.startswith(LEGACY_DIR) and rel.endswith(LEGACY_CODE_SUFFIXES))


def labels() -> tuple[core.Label, ...]:
    """The class's line labels, in the table's declaration order."""
    return tuple(core.Label(name, pattern) for name, pattern in PATTERNS.items())


def path_labels(rel: str) -> tuple[core.Label, ...]:
    """The class's path labels: a file NAME that spells a chunk identifier is a reference too."""
    return tuple(core.Label(name, pattern) for name, pattern in PATH_PATTERNS.items() if pattern.search(rel))


def load() -> core.Matcher:
    """Return the planning-reference class's matcher."""
    line_labels = labels()
    return core.Matcher(
        name=NAME,
        labels=line_labels,
        predicate=core.line_predicate(line_labels),
        corpus=core.working_tree_files,
        skip_marker=SKIP_MARKER,
        scope=scope,
        path_labels=path_labels,
        loaded="the pattern table of this module",
    )
