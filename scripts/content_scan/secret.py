"""The secret class: a credential, a key, a token or an assigned password, by its public shape.

Every pattern here is PUBLIC and every one is a SHAPE, never a value: a PEM private-key header
(including the PGP form, whose algorithm word the header carries), a GitHub or PyPI token prefix,
a cloud access-key identifier, and an assignment whose identifier ENDS in `token`, `secret`,
`password`, `passwd` or `api_key` -- `access_token`, `GITHUB_TOKEN`, `db_password` and
`client_secret` are the shapes a repository actually holds -- carrying either a quoted literal or
an unquoted one. That is what lets the clone-carried test load this module and run it in the
toolkit's own suite: it needs no private list, because the shapes are the contract.

The unquoted arm deliberately stops at a character that ends a value: a dotted expression such as
a call to `re.compile` after a dictionary key is two characters, not a credential, and a rule that
called it one would report this module's own pattern table on every run.

This class has NO skip marker, deliberately. A marker the core honoured here would let a real
secret through, which is the one class where a single miss is not recoverable: a token pushed to a
public branch is disclosed whether or not the commit is reverted afterwards. The class therefore
also has no allowlist and no self-exclusion: a file that legitimately holds a secret is a file
that should not be tracked.

Nothing here refuses anything. The push run reports this class's count and refuses nothing, and
the pre-release scan is the only caller that refuses on it -- a secret in the bytes of a release
is what blocks the upload.
"""

from __future__ import annotations

import re

try:  # imported as part of the scanner's package (the test suite, and `python3 -m`)
    from . import core
except ImportError:  # run as a directory: `python3 scripts/content_scan` puts this directory on sys.path
    import core  # type: ignore[no-redef]

NAME = "secret"

SKIP_MARKER = None

PATTERNS = {
    "PEM private-key header": re.compile(
        r"-----BEGIN (?:RSA |EC |DSA |OPENSSH |ENCRYPTED |PGP )?PRIVATE KEY(?: BLOCK)?-----"
    ),
    "GitHub token": re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b"),
    "PyPI token": re.compile(r"\bpypi-[A-Za-z0-9_-]{40,}\b"),
    "cloud access-key id": re.compile(r"\b(?:AKIA|ASIA|AGPA|AIDA|AROA)[0-9A-Z]{16}\b"),
    "assigned credential": re.compile(
        r"(?i:\b\w*(?:api[_-]?key|passwd|password|secret|token)\s*[:=]\s*"
        r"(?:[\"'][^\"'\s]{8,}[\"']|[A-Za-z0-9_+/=-]{8,}))"
    ),
}

LABEL_SHAPES: tuple[str, ...] = tuple(PATTERNS)


def labels() -> tuple[core.Label, ...]:
    """The class's labels, in the table's declaration order."""
    return tuple(core.Label(name, pattern) for name, pattern in PATTERNS.items())


def load() -> core.Matcher:
    """Return the secret class's matcher."""
    line_labels = labels()
    return core.Matcher(
        name=NAME,
        labels=line_labels,
        predicate=core.line_predicate(line_labels),
        loaded="the public label shapes of this module",
    )
