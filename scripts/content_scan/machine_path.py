"""The machine-path class: a path, a drive or a sync-folder name that only one machine resolves.

The pattern and the allowlist are the two lines that lived inline in the anonymization workflow's
bash step before this scanner existed, kept unchanged here. A home directory, a Windows drive and
a named sync folder say where a file lives on the author's machine and resolve for nobody else,
so they are defects in a published tree; the allowlist names the files that carry one on purpose
(the path resolver's own tests, a test that builds a throwaway worktree, and the answer key of the
pre-refactor corpus).

Nothing here reads a private file: the label names are public, so a finding may be printed with
its name even in a log a stranger can read. What may NOT be printed is a path, which the report
form already keeps out of the count-only line.

A spelled-out brand name in this module is written with one bracketed letter (`Drop[b]ox`), so the
module does not match its own pattern. A pattern written the other way would report this file on
every run of the real tree.
"""

from __future__ import annotations

import re

try:  # imported as part of the scanner's package (the test suite, and `python3 -m`)
    from . import core
except ImportError:  # run as a directory: `python3 scripts/content_scan` puts this directory on sys.path
    import core  # type: ignore[no-redef]

NAME = "machine_path"

SKIP_MARKER = None

# One label, because the class is one question -- "does this text name a location on a machine?" --
# and the alternation is its shape, not five separate shapes. The old inline step reported the
# file and never which branch matched, so nothing downstream read the distinction.
LABEL_SHAPES = ("machine path",)

PATTERN = re.compile(
    r"(/home/[A-Za-z0-9._-]+|/Users/[A-Za-z0-9._-]+|\b[A-Za-z]:[\\/]"
    r"|Drop[b]ox|One[D]rive|Google [D]rive|i[C]loud)"
)

ALLOWLIST = re.compile(
    r"^(\.copier-answers\.yml"
    r"|_old_code_to_refactor/_inputs\.py"
    r"|tests/correspondence/test_pair_scan\.py"
    r"|tests/correspondence/test_write_root\.py"
    r"|tests/correspondence/write_root\.py"
    r"|tests/test_path_resolver\.py"
    r"|tests/correspondence/reference_values/marginal_distribution_fitting/_b4_selecting_marginals\.py_268-316\.npz"
    r"|tests/correspondence/reference_values/marginal_distribution_fitting/_utils\.py_1198-1238\.npz)$"
)


def scope(rel: str) -> bool:
    """Every tracked file outside the allowlist is in scope; the allowlist is one full match."""
    return ALLOWLIST.match(rel) is None


def load() -> core.Matcher:
    """Return the machine-path class's matcher.

    Two reading facts are this class's alone. It reads BYTES, because the `grep -aqE` step it
    replaces treated a binary file as text: the two `.npz` rows of the allowlist protect files
    only a byte read would ever have matched, so a text-only corpus would leave them protecting
    nothing while a released `.npz` lost its coverage. And it skips the scanner's own modules,
    because this pattern matches the pattern tables themselves (`[A-Za-z]:[\\/]` is a drive
    letter and the sync-folder names are spelled there with a bracketed letter).
    """
    labels = (core.Label(LABEL_SHAPES[0], PATTERN),)
    return core.Matcher(
        name=NAME,
        labels=labels,
        predicate=core.line_predicate(labels),
        scope=scope,
        skips_own_files=True,
        reads_bytes=True,
        loaded="the inline pattern and allowlist",
    )
