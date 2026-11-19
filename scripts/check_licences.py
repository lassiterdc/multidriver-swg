#!/usr/bin/env python3
"""Check the licence statements a release carries, and refuse a release whose statements disagree.

A release states the licence of each class of bytes it carries separately, and states this release's
own licence on four surfaces: the built distributions' core metadata, the licence file each artifact
carries, the README that becomes their long description, and `CITATION.cff`, whose `license` a
dispatcher copies onto the archive record. Those four surfaces drift, and a published release that
misstates its licence is repaired only by withdrawing it and publishing a successor, so the
disagreement is refused here, before the upload.

The table, `release_state.toml` at the repository root, is the authority. Each `[[class]]` table
declares a class of artifact bytes, the state it ships in, the globs over an enumeration's `path`
values that name it, and — for a `released` class — the SPDX expression the release states for it. The
enumeration is the record `scripts/enumerate_release.py` writes, `release-enumeration.json`.

Four facts refuse the release, each printing one `refused: {id}: {detail}` line, and the closing line
reads `licence check: {n} refusal(s)` or `licence check: conforms`:

- `source-licence`: the sdist's or the wheel's `License-Expression` differs from the licence the table
  records for the `release-metadata` class, or the artifact carries no such field.
- `class-licence`: a set holds entries of a `released` class and that set's covering file is not an
  entry of the same set, or it is and its text states none of the licence that class names.
- `readme-licence`: the README states a licence for this distribution that the table's source licence
  does not carry.
- `citation-licence`: `CITATION.cff`'s top-level `license` differs from the table's source licence, or
  it carries none.

THE COVERING RULE, over the CLOSED domain of the three byte sets. An entry of a `released` class in a
set is covered when the set's covering file is itself an entry of the same set and states the class's
table licence. One covering file per set, whatever class the entry is in:

    set             covering file for an entry
    sdist           LICENSE at the root of the archive
    wheel           {name}-{version}.dist-info/licenses/LICENSE
    source-archive  LICENSE at the root

The sdist and the wheel are inputs, so their covering file is read and its text compared against the
licence each released class present in that set names, and the `{name}-{version}` of the wheel is read
from the wheel's own file name. The source archive is not an input: it is `git archive` of the release
commit, which this check never runs, so its covering file is covered when the enumeration records it
carrying the same sha256 as the built copy of the same file at the same commit. That equality is
checked rather than assumed, so a source archive whose `LICENSE` differs from the distributions' is
refused.

THE LICENCE VOCABULARY IS CLOSED AND DECLARED. Whether a text states a licence is decided by the
family markers below, matched left to right from each position with the match consumed, so `AGPL` is
not also read as `GPL` and `GNU Affero General Public License` as not also `GNU General Public
License`. One marker per family, and the vocabulary is the licence families a release of this toolkit
can carry: a licence file is a licence's own text, so a family is the finest thing its text can be
compared on — `AGPL-3.0` and `AGPL-3.0-or-later` are one document and one family. An expression no
marker names is refused under `class-licence` rather than passing unexamined, because a check that
cannot read a licence must not report that it agrees with one.

A LICENCE FILE STATES THE LICENCE IT IS, NOT EVERY FAMILY ITS BODY MENTIONS. A licence's own text names
its siblings: the GPL's section 13 permits combining a covered work with an AGPL one and the AGPL's
preamble says which licence it is based on, so reading every family a file mentions made the GPL text
pass as an AGPL licence and the AGPL text as a GPL one. The family a covering file states is therefore
read from its title — its first non-blank line — and, where the title names no family, from the family
its body names most often. The residual this leaves, stated rather than hidden: a licence text whose
title and body both name no declared family is refused, because a family no marker names is refused
rather than passed.

THE README'S SCOPE. The README is the distributions' long description, and it also states licences
this release does not carry: the legacy corpus's, which is a separate statement about a path that
ships in no set. So the licence expressions compared are the ones the README states for THIS
distribution, and a statement is read as stating one when it names no repository path — a backticked
token containing `/` scopes what the statement says to that path and not to the distribution. The
statements read are those of the README's licence section, the section whose heading names a licence,
because that is where the README states what governs this software; a README carrying no such section
is read whole, so the absence of a heading cannot switch the comparison off. The residual this leaves,
stated rather than hidden: the test is per statement, not per phrase, so a distribution statement that
carries an incidental path is not compared either. A sentence ending `MIT licence (see {path}).`, where
`{path}` is any backticked repository path, exempts itself and its MIT mention goes unread.

A STATEMENT THAT NAMES A LISTED SPDX IDENTIFIER IS COMPARED AT THE IDENTIFIER, AND EVERY OTHER MENTION
BY FAMILY. `AGPL-3.0-only` and `AGPL-3.0-or-later` are different licences to a user — the second lets
them take a later version — so a README that states the first beside metadata that states the second
does not carry the metadata's licence, and comparing the two by family discarded exactly that
difference. "Listed" is the declared vocabulary below, read on the WHOLE TOKEN the sentence wrote and
not on the piece a marker matched: a marker stops where its own grammar stops, so `AGPL-3.0+` matches as
`AGPL-3.0` and a test on the match alone would compare a spelling the README never wrote.
`The sample is licensed under the AGPL.` names a family and no identifier, so it is compared by family
and conforms beside `AGPL-3.0-or-later` metadata; `The sample is AGPLv3 licensed.` does the same. The
cost this accepts, stated rather than hidden: a bare family name does not say which version, so
`the AGPL` conforms beside metadata stating any AGPL version. A mention of another family still
refuses, because the family is what a statement naming no listed identifier states.

STATEMENTS ARE SPLIT ON THREE BOUNDARIES, NOT ONE. A licence section is commonly a bullet list and
routinely ends a paragraph without a full stop, so a blank line and a line starting a list item are
statement boundaries exactly as a sentence-ending punctuation mark followed by whitespace is. Splitting
on punctuation alone joined a distribution's statement to a path-scoped one, and one backticked path in
the joined text then exempted the whole thing. The punctuation split is still naive in one respect and
the respect is stated: an abbreviation carrying a full stop divides a statement, and a divided
statement that keeps its path scope on the other side of the division would be read as a statement
about the distribution.

Exit 0 = every statement agrees, 1 = refused, 2 = an input could not be read. A malformed table, a
malformed enumeration, a distribution directory that does not hold one sdist and one wheel, a
`--readme` or `--citation` that cannot be read, and a table declaring no released `release-metadata`
class are the inputs this check cannot do without: it reads nothing it can compare, so it reports no
verdict about the release.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import re
import sys
import tarfile
import tomllib
import zipfile
from dataclasses import dataclass
from pathlib import Path

SCHEMA = 1
SDIST = "sdist"
WHEEL = "wheel"
SOURCE_ARCHIVE = "source-archive"
SETS = (SDIST, WHEEL, SOURCE_ARCHIVE)
RELEASED = "released"
EXCLUDED = "excluded"
ESTATE_RESIDENT = "estate-resident"
STATES = (RELEASED, EXCLUDED, ESTATE_RESIDENT)
CLASS_KEYS = frozenset({"name", "system", "state", "paths", "licence"})
ENTRY_KEYS = frozenset({"set", "path", "sha256", "size"})
SDIST_SUFFIX = ".tar.gz"
WHEEL_SUFFIX = ".whl"
METADATA_CLASS = "release-metadata"
ROOT_LICENCE = "LICENSE"
LICENCE_EXPRESSION = "License-Expression"
_HEX_DIGITS = frozenset("0123456789abcdef")

FAMILY_MARKERS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "agpl",
        re.compile(
            r"GNU Affero General Public Licen[cs]e|(?<![A-Za-z])AGPL(?:[- ]?v?\d+(?:\.\d+)?)?(?:-or-later|-only)?",
            re.IGNORECASE,
        ),
    ),
    (
        "lgpl",
        re.compile(
            r"GNU Lesser General Public Licen[cs]e|(?<![A-Za-z])LGPL(?:[- ]?v?\d+(?:\.\d+)?)?(?:-or-later|-only)?",
            re.IGNORECASE,
        ),
    ),
    (
        "gpl",
        re.compile(
            r"GNU General Public Licen[cs]e|(?<![A-Za-z])GPL(?:[- ]?v?\d+(?:\.\d+)?)?(?:-or-later|-only)?",
            re.IGNORECASE,
        ),
    ),
    ("apache", re.compile(r"Apache Licen[cs]e|(?<![A-Za-z])Apache(?:-?2(?:\.0)?)?", re.IGNORECASE)),
    ("mpl", re.compile(r"Mozilla Public Licen[cs]e|(?<![A-Za-z])MPL(?:[- ]?2(?:\.0)?)?", re.IGNORECASE)),
    ("cc-by", re.compile(r"Creative Commons Attribution|(?<![A-Za-z])CC[- ]BY(?:[- ]?\d(?:\.\d)?)?", re.IGNORECASE)),
    (
        "unlicense",
        re.compile(
            r"(?<![A-Za-z])Unlicense|free and unencumbered software released into the public domain", re.IGNORECASE
        ),
    ),
    ("isc", re.compile(r"(?<![A-Za-z])ISC(?![A-Za-z])", re.IGNORECASE)),
    ("bsd", re.compile(r"(?<![A-Za-z])BSD(?![A-Za-z])", re.IGNORECASE)),
    ("mit", re.compile(r"(?<![A-Za-z])MIT(?![A-Za-z])|Expat", re.IGNORECASE)),
)

# The spellings the SPDX Licence List carries for the ten families the markers name, lower-cased. A README
# mention whose WHOLE TOKEN is one of them is compared at the identifier, because `AGPL-3.0-only` and
# `AGPL-3.0-or-later` are different licences to a user; every other mention — a bare family name such as
# `AGPL`, a spelling the list carries nowhere such as `AGPLv3`, or a listed spelling the sentence extended
# such as `AGPL-3.0+` — is compared by family. The deprecated spellings are here because the list carries
# them. Two spellings a reader might expect are absent because the list does not carry them: `BSD` alone,
# whose clause forms (`BSD-3-Clause`) are not tokens any marker produces, and `Expat`, which is the MIT
# licence's other name and not one of its identifiers.
SPDX_IDENTIFIERS: frozenset[str] = frozenset(
    {
        # GNU Affero General Public License.
        "agpl-1.0",
        "agpl-1.0-only",
        "agpl-1.0-or-later",
        "agpl-3.0",
        "agpl-3.0-only",
        "agpl-3.0-or-later",
        # GNU Lesser General Public License.
        "lgpl-2.0",
        "lgpl-2.0-only",
        "lgpl-2.0-or-later",
        "lgpl-2.1",
        "lgpl-2.1-only",
        "lgpl-2.1-or-later",
        "lgpl-3.0",
        "lgpl-3.0-only",
        "lgpl-3.0-or-later",
        # GNU General Public License.
        "gpl-1.0",
        "gpl-1.0-only",
        "gpl-1.0-or-later",
        "gpl-2.0",
        "gpl-2.0-only",
        "gpl-2.0-or-later",
        "gpl-3.0",
        "gpl-3.0-only",
        "gpl-3.0-or-later",
        # Apache License.
        "apache-1.0",
        "apache-1.1",
        "apache-2.0",
        # Mozilla Public License.
        "mpl-1.0",
        "mpl-1.1",
        "mpl-2.0",
        # Creative Commons Attribution.
        "cc-by-1.0",
        "cc-by-2.0",
        "cc-by-2.5",
        "cc-by-3.0",
        "cc-by-4.0",
        # The Unlicense, the ISC License and the MIT License.
        "unlicense",
        "isc",
        "mit",
    }
)

_HEADING = re.compile(r"^(?P<hashes>#{1,6})\s+(?P<title>.*\S)\s*$")
_BACKTICKED = re.compile(r"`([^`]*)`")
_SENTENCE_END = re.compile(r"(?<=[.!?])\s+")
_PARAGRAPH_BREAK = re.compile(r"\n[ \t]*\n")
_LIST_ITEM = re.compile(r"^[ \t]{0,3}(?:[-*+]|\d+[.)])[ \t]+")
_CITATION_LICENCE = re.compile(r"^license\s*:\s*(?P<value>\S.*)$")
_CONTINUES_TOKEN = re.compile(r"[+A-Za-z0-9]")


@dataclass(frozen=True)
class ClassRow:
    """One declared class: who declares it, what state it ships in, and which paths it names."""

    name: str
    system: str
    state: str
    paths: tuple[str, ...]
    licence: str | None


@dataclass(frozen=True)
class Entry:
    """One member of one byte set, as the enumeration records it."""

    set: str
    path: str
    sha256: str | None
    size: int


@dataclass(frozen=True)
class Statement:
    """One licence mention the README makes for this distribution, and the statement it makes it in."""

    family: str
    expression: str | None
    source_text: str


def _mentions(text: str) -> tuple[tuple[str, str, int], ...]:
    """Every licence mention in `text` as (family, token, end), each match consumed so `AGPL` is not also `GPL`."""
    found: list[tuple[str, str, int]] = []
    position = 0
    while position < len(text):
        for family, pattern in FAMILY_MARKERS:
            matched = pattern.match(text, position)
            if matched is None or matched.end() == position:
                continue
            found.append((family, matched.group(), matched.end()))
            position = matched.end()
            break
        else:
            position += 1
    return tuple(found)


def _token_written(text: str, token: str, end: int) -> str:
    """The whole token `text` wrote at the match: `token` plus the characters that continue it past `end`.

    A marker stops where its own grammar stops, so `AGPL-3.0+` matches as `AGPL-3.0`; a listed-spelling
    test on the match alone would then compare a spelling the README never wrote.
    """
    extended = end
    while extended < len(text) and _CONTINUES_TOKEN.match(text, extended) is not None:
        extended += 1
    return token + text[end:extended]


def _families(text: str) -> tuple[str, ...]:
    """The licence families `text` names, in first-mention order and each once."""
    return tuple(dict.fromkeys(family for family, _, _ in _mentions(text)))


def _identifiers(text: str) -> tuple[str, ...]:
    """The SPDX identifiers `text` states: its mentions carrying no whitespace, each once and lower-cased."""
    spelled = (token for _, token, _ in _mentions(text) if " " not in token and "\t" not in token)
    return tuple(dict.fromkeys(token.lower() for token in spelled))


def _dominant_family(text: str) -> str | None:
    """The licence family `text` names most often, the earliest mention breaking a tie, or None when it names none."""
    counts: dict[str, int] = {}
    order: list[str] = []
    for family, _, _ in _mentions(text):
        if family not in counts:
            order.append(family)
            counts[family] = 0
        counts[family] += 1
    if not order:
        return None
    best = order[0]
    for family in order[1:]:
        if counts[family] > counts[best]:
            best = family
    return best


def _stated_family(text: str) -> str | None:
    """The licence a licence file IS: the family its title line names, else the family its body names most often.

    A licence file's title is the licence's own name and its body is that licence's text, which routinely
    names a sibling: the GPL's section 13 permits combining a covered work with an AGPL one and the AGPL's
    preamble says which licence it is based on. A body's mention of a family is therefore not a statement
    that the file is that licence, and reading every mention made the two files interchangeable.
    """
    title = next((line for line in text.splitlines() if line.strip()), "")
    families = _families(title)
    if families:
        return families[0]
    return _dominant_family(text)


def _matches(path: str, pattern: str) -> bool:
    """Whether `pattern` names `path`; `*` crosses a directory separator, so `tests/**` covers `tests/`."""
    return fnmatch.fnmatchcase(path, pattern)


def _read_class(row: object) -> tuple[ClassRow | None, str]:
    """One `[[class]]` table, checked against the five keys a class row may hold, and why not when invalid."""
    if not isinstance(row, dict):
        return None, "is not a table"
    unknown = sorted(set(row) - CLASS_KEYS)
    if unknown:
        return None, f"carries {unknown}, and a class row holds only {sorted(CLASS_KEYS)}"
    for key in ("name", "system", "state"):
        value = row.get(key)
        if not isinstance(value, str) or not value.strip():
            return None, f"holds {key} as {value!r}, which is not a non-empty string"
    state = row["state"]
    if state not in STATES:
        return None, f"declares state {state!r}, and a state is one of {list(STATES)}"
    paths = row.get("paths")
    if not isinstance(paths, list) or not paths or not all(isinstance(item, str) and item.strip() for item in paths):
        return None, f"holds paths as {paths!r}, and paths is a non-empty list of non-empty strings"
    licence = row.get("licence")
    if state == RELEASED:
        if not isinstance(licence, str) or not licence.strip():
            return (
                None,
                "is released and states no licence; a released class states the SPDX expression the release carries",
            )
    elif licence is not None:
        return None, f"states licence {licence!r} and state {state!r}; only a released class states a licence"
    return ClassRow(row["name"], row["system"], state, tuple(paths), licence), ""


def _load_table(path: Path) -> tuple[list[ClassRow] | None, str]:
    """The table's classes, and why they could not be read when they are None."""
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        return None, f"{path} could not be read ({error})"
    try:
        document = tomllib.loads(text)
    except tomllib.TOMLDecodeError as error:
        return None, f"{path} is not one readable TOML document ({error})"
    rows = document.get("class")
    if not isinstance(rows, list) or not rows:
        return None, f"{path} declares no [[class]] table; one row per artifact class is what this check reads"
    classes: list[ClassRow] = []
    seen: set[str] = set()
    for index, row in enumerate(rows, 1):
        declared, problem = _read_class(row)
        if declared is None:
            return None, f"{path} class {index} {problem}"
        if declared.name in seen:
            return None, f"{path} declares class {declared.name!r} twice; one class is one table"
        seen.add(declared.name)
        classes.append(declared)
    return classes, ""


def _read_entry(entry: object) -> tuple[Entry | None, str]:
    """One enumeration entry, checked against the four keys an entry holds, and why not when invalid."""
    if not isinstance(entry, dict):
        return None, "is not an object"
    if set(entry) != ENTRY_KEYS:
        return None, f"holds the keys {sorted(entry)}, and an entry holds {sorted(ENTRY_KEYS)}"
    set_name = entry["set"]
    if set_name not in SETS:
        return None, f"names set {set_name!r}, and a set is one of {list(SETS)}"
    path = entry["path"]
    if not isinstance(path, str) or not path:
        return None, f"names path {path!r}, which is not a non-empty string"
    digest = entry["sha256"]
    if digest is not None and not (isinstance(digest, str) and len(digest) == 64 and _HEX_DIGITS.issuperset(digest)):
        return None, f"holds sha256 {digest!r}, and a digest is 64 lowercase hex digits or null"
    size = entry["size"]
    if not isinstance(size, int) or isinstance(size, bool) or size < 0:
        return None, f"holds size {size!r}, and a size is a count of bytes"
    return Entry(set_name, path, digest, size), ""


def _load_enumeration(path: Path) -> tuple[dict[str, object] | None, list[Entry] | None, str]:
    """The enumeration's document and entries, and why they could not be read when the entries are None."""
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        return None, None, f"{path} could not be read ({error})"
    try:
        document = json.loads(text)
    except json.JSONDecodeError as error:
        return None, None, f"{path} is not one readable JSON document ({error})"
    if not isinstance(document, dict):
        return None, None, f"{path} holds a {type(document).__name__} and not one JSON object"
    if document.get("schema") != SCHEMA:
        return None, None, f"{path} declares schema {document.get('schema')!r}, and this check reads schema {SCHEMA}"
    rows = document.get("entries")
    if not isinstance(rows, list):
        return None, None, f"{path} holds entries as {rows!r}, and entries is a list"
    entries: list[Entry] = []
    for index, row in enumerate(rows, 1):
        read, problem = _read_entry(row)
        if read is None:
            return None, None, f"{path} entry {index} {problem}"
        entries.append(read)
    return document, entries, ""


def _artifact_files(dist: Path) -> tuple[list[tuple[str, Path]], str]:
    """The one sdist and one wheel in `dist`, as (set name, path) pairs, and why not when they are not."""
    if not dist.is_dir():
        return [], f"{dist} is not a directory; point --dist at the directory the build wrote"
    sdists = sorted(path for path in dist.iterdir() if path.is_file() and path.name.endswith(SDIST_SUFFIX))
    wheels = sorted(path for path in dist.iterdir() if path.is_file() and path.name.endswith(WHEEL_SUFFIX))
    if len(sdists) == 1 and len(wheels) == 1:
        return [(SDIST, sdists[0]), (WHEEL, wheels[0])], ""
    return [], (
        f"{dist} holds {len(sdists)} sdist(s) ({_names(sdists)}) and {len(wheels)} wheel(s) ({_names(wheels)}); "
        "a release ships exactly one of each, so point --dist at the directory of a single build"
    )


def _names(paths: list[Path]) -> str:
    """The file names of `paths`, or `none`, so a refusal counts and names what it found."""
    return ", ".join(path.name for path in paths) if paths else "none"


def _dist_info(wheel: Path) -> tuple[str | None, str]:
    """The wheel's `{name}-{version}.dist-info` directory, read from its file name, and why not when it is None."""
    stem = wheel.name[: -len(WHEEL_SUFFIX)]
    parts = stem.split("-")
    if len(parts) < 5:
        return None, f"{wheel.name} is not named {{name}}-{{version}}-{{python}}-{{abi}}-{{platform}}.whl"
    return f"{parts[0]}-{parts[1]}.dist-info", ""


def _sdist_member(sdist: Path, member_path: str) -> tuple[bytes | None, str]:
    """One sdist member's bytes, named by its path with the archive's one top directory stripped."""
    try:
        with tarfile.open(sdist, mode="r:gz") as archive:
            roots = {member.name.split("/", 1)[0] for member in archive.getmembers() if member.name}
            if len(roots) != 1:
                return None, f"{sdist.name} holds members under {len(roots)} top-level names, and an sdist has one"
            root = next(iter(roots))
            try:
                member = archive.getmember(f"{root}/{member_path}")
            except KeyError:
                return None, f"{sdist.name} holds no member {member_path!r}"
            extracted = archive.extractfile(member)
            if extracted is None:
                return None, f"{sdist.name} member {member_path!r} holds no bytes"
            return extracted.read(), ""
    except (OSError, tarfile.TarError) as error:
        return None, f"{sdist.name} could not be read as a gzip tar ({error})"


def _wheel_member(wheel: Path, member_path: str) -> tuple[bytes | None, str]:
    """One wheel member's bytes, named by its path relative to the archive root."""
    try:
        with zipfile.ZipFile(wheel) as archive:
            try:
                return archive.read(member_path), ""
            except KeyError:
                return None, f"{wheel.name} holds no member {member_path!r}"
    except (OSError, zipfile.BadZipFile, RuntimeError) as error:
        return None, f"{wheel.name} could not be read as a zip ({error})"


def _member_text(set_name: str, locations: dict[str, Path], member_path: str) -> tuple[str | None, str]:
    """One member's text, read from the artifact of `set_name`, and why it could not be read when it is None."""
    if set_name == SDIST:
        data, problem = _sdist_member(locations[SDIST], member_path)
    else:
        data, problem = _wheel_member(locations[WHEEL], member_path)
    return (None, problem) if data is None else (data.decode("utf-8", "replace"), "")


def _metadata_expression(text: str) -> str | None:
    """The core metadata's `License-Expression`, its folded continuations joined, or None when it carries none."""
    fields: dict[str, str] = {}
    name: str | None = None
    for line in text.splitlines():
        if not line.strip():
            break
        if line[:1] in (" ", "\t") and name is not None:
            fields[name] = f"{fields[name]} {line.strip()}".strip()
            continue
        key, separator, value = line.partition(":")
        if not separator:
            name = None
            continue
        name = key.strip().lower()
        fields[name] = value.strip()
    expression = fields.get(LICENCE_EXPRESSION.lower())
    return expression or None


def _citation_licence(text: str) -> str | None:
    """`CITATION.cff`'s top-level `license` value, unquoted and without a trailing comment, or None when absent."""
    for line in text.splitlines():
        matched = _CITATION_LICENCE.match(line)
        if matched is None:
            continue
        value = matched.group("value").strip()
        if value[:1] in ("'", '"'):
            closing = value.find(value[0], 1)
            return value[1:closing] if closing > 0 else value[1:]
        comment = value.find(" #")
        return (value[:comment] if comment > 0 else value).strip()
    return None


def _licence_scope(text: str) -> str:
    """The README's licence section, or the whole document when it carries no heading naming a licence."""
    lines = text.splitlines()
    headings = []
    for index, line in enumerate(lines):
        matched = _HEADING.match(line)
        if matched is not None:
            headings.append((index, len(matched.group("hashes")), matched.group("title")))
    for position, (index, level, title) in enumerate(headings):
        lowered = title.lower()
        if "licence" not in lowered and "license" not in lowered:
            continue
        end = len(lines)
        for later_index, later_level, _ in headings[position + 1 :]:
            if later_level <= level:
                end = later_index
                break
        return "\n".join(lines[index:end])
    return text


def _names_a_path(sentence: str) -> bool:
    """Whether the sentence scopes what it says to a repository path, which is a statement about that path.

    The test is per statement, so a distribution statement that carries an incidental path is exempted
    too; that residual is stated in the module docstring rather than closed here.
    """
    return any("/" in token.strip() for token in _BACKTICKED.findall(sentence))


def _statements(scope: str) -> tuple[str, ...]:
    """The scope's statements: one per paragraph, one per list item, and one per sentence, in document order.

    A licence section is commonly a list of bullets and routinely ends a paragraph without a full stop, so
    a boundary carrying no punctuation is still a boundary between two statements. Splitting on sentence
    punctuation alone joined a distribution's statement to a path-scoped one, and one backticked path in
    the joined text then exempted the whole thing.
    """
    statements: list[str] = []
    for paragraph in _PARAGRAPH_BREAK.split(scope):
        block: list[str] = []
        for line in paragraph.splitlines():
            if _LIST_ITEM.match(line) is not None and block:
                statements.extend(_SENTENCE_END.split("\n".join(block)))
                block = []
            block.append(line)
        if block:
            statements.extend(_SENTENCE_END.split("\n".join(block)))
    return tuple(statement for statement in statements if statement.strip())


def _readme_statements(text: str) -> tuple[Statement, ...]:
    """Every licence the README states for this distribution, with the statement it is stated in.

    A mention whose whole token is a spelling on the SPDX list is the statement's identifier and is
    compared as one; a bare family name, a spelling the list carries nowhere, and a listed spelling the
    sentence extended past the marker's match are compared by family, which is what prose states when it
    names no identifier.
    """
    statements: list[Statement] = []
    for sentence in _statements(_licence_scope(text)):
        if _names_a_path(sentence):
            continue
        for family, token, end in _mentions(sentence):
            spelled = _token_written(sentence, token, end).lower()
            expression = spelled if spelled in SPDX_IDENTIFIERS else None
            statements.append(Statement(family=family, expression=expression, source_text=sentence.strip()))
    return tuple(statements)


def _classes_of(path: str, classes: list[ClassRow]) -> tuple[ClassRow, ...]:
    """The classes whose paths name `path`, in the table's order."""
    return tuple(row for row in classes if any(_matches(path, pattern) for pattern in row.paths))


def _covering_entry(set_name: str, covering: str, entries: list[Entry]) -> Entry | None:
    """The entry of `set_name` that a set's covering file names, or None when that set holds no such entry."""
    return next((entry for entry in entries if entry.set == set_name and entry.path == covering), None)


def _released_in_set(members: list[Entry], released: list[ClassRow]) -> list[ClassRow]:
    """The released classes that name at least one of `members`, in the table's order."""
    named = {row.name for entry in members for row in _classes_of(entry.path, released)}
    return [row for row in released if row.name in named]


def _unreadable(problem: str, remedy: str) -> int:
    """One `unreadable-input` refusal and the status a broken invocation takes."""
    print(f"refused: unreadable-input: {problem}; {remedy}")
    return 2


@dataclass(frozen=True)
class Inputs:
    """What this check reads: the table's classes, the enumeration, the artifacts, and the two documents."""

    classes: list[ClassRow]
    entries: list[Entry]
    document: dict[str, object]
    sdists_and_wheels: list[tuple[str, Path]]
    covering: dict[str, str]
    expressions: dict[str, str | None]
    texts: dict[str, str]

    @property
    def source_licence(self) -> str:
        """The licence the table records for the class both `source-licence` and `citation-licence` read."""
        row = next(row for row in self.classes if row.name == METADATA_CLASS)
        assert row.licence is not None, "the metadata class is released, so it states a licence"
        return row.licence


def _read_inputs(arguments: argparse.Namespace) -> tuple[Inputs | None, int]:
    """Every input this check compares, or the `unreadable-input` refusal and status that stopped it."""
    document, entries, problem = _load_enumeration(Path(arguments.enumeration))
    if entries is None or document is None:
        return None, _unreadable(problem, "enumerate the release again and point --enumeration at that record")
    classes, problem = _load_table(Path(arguments.table))
    if classes is None:
        return None, _unreadable(problem, "correct the table and check again")
    metadata = next((row for row in classes if row.name == METADATA_CLASS), None)
    if metadata is None or metadata.state != RELEASED:
        return None, _unreadable(
            f"{arguments.table} declares no released class {METADATA_CLASS!r}",
            "declare it, because the licence the artifacts state is read from that class",
        )

    pairs, problem = _artifact_files(Path(arguments.dist))
    if not pairs:
        return None, _unreadable(problem, "point --dist at the directory the release enumerated")

    locations = dict(pairs)
    dist_info, problem = _dist_info(locations[WHEEL])
    if dist_info is None:
        return None, _unreadable(problem, "build the wheel again and check that build")
    covering = {
        SDIST: ROOT_LICENCE,
        WHEEL: f"{dist_info}/licenses/{ROOT_LICENCE}",
        SOURCE_ARCHIVE: ROOT_LICENCE,
    }
    header_members = {SDIST: "PKG-INFO", WHEEL: f"{dist_info}/METADATA"}
    expressions: dict[str, str | None] = {}
    for set_name in (SDIST, WHEEL):
        text, problem = _member_text(set_name, locations, header_members[set_name])
        if text is None:
            return None, _unreadable(problem, "build the artifacts again and check that build")
        expressions[set_name] = _metadata_expression(text)

    texts: dict[str, str] = {}
    for path, set_name in ((Path(arguments.readme), "readme"), (Path(arguments.citation), "citation")):
        try:
            texts[set_name] = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as error:
            return None, _unreadable(f"{path} could not be read ({error})", f"point --{set_name} at that document")

    return (
        Inputs(
            classes=classes,
            entries=entries,
            document=document,
            sdists_and_wheels=pairs,
            covering=covering,
            expressions=expressions,
            texts=texts,
        ),
        0,
    )


def _print_inputs(inputs: Inputs) -> None:
    """The source licence, each artifact's `License-Expression`, and each set's released entry count."""
    print(f"metadata class {METADATA_CLASS} states {inputs.source_licence}")
    for set_name in (SDIST, WHEEL):
        print(f"set {set_name} {LICENCE_EXPRESSION}: {inputs.expressions[set_name]}")
    released = [row for row in inputs.classes if row.state == RELEASED]
    for set_name in SETS:
        members = [entry for entry in inputs.entries if entry.set == set_name]
        if not members:
            continue
        wanted = _released_in_set(members, released)
        covering = inputs.covering[set_name]
        present = _covering_entry(set_name, covering, inputs.entries) is not None
        print(
            f"set {set_name}: {len(members)} entries, {len(wanted)} released class(es), "
            f"covering file {covering} {'present' if present else 'absent'}"
        )


def _source_licence_refusals(inputs: Inputs) -> list[str]:
    """One line per artifact whose `License-Expression` is not the licence the table records for its class."""
    refusals = []
    for set_name, path in inputs.sdists_and_wheels:
        expression = inputs.expressions[set_name]
        if expression == inputs.source_licence:
            continue
        stated = f"states {LICENCE_EXPRESSION} {expression!r}" if expression else f"carries no {LICENCE_EXPRESSION}"
        refusals.append(
            f"refused: source-licence: {set_name} {path.name} {stated} and class {METADATA_CLASS} states "
            f"{inputs.source_licence!r}; the artifact's metadata and the table state one licence, so correct "
            f"whichever is wrong before the upload"
        )
    return refusals


def _covering_texts(inputs: Inputs, set_names: list[str]) -> tuple[dict[str, str], list[str]]:
    """The covering file's text for each set that needs one, and the refusals of the ones that could not be read."""
    locations = dict(inputs.sdists_and_wheels)
    texts: dict[str, str] = {}
    refusals: list[str] = []
    for set_name in set_names:
        text, problem = _member_text(set_name, locations, inputs.covering[set_name])
        if text is None:
            refusals.append(
                f"refused: class-licence: {problem}, and the enumeration records that entry; the enumeration and "
                f"the artifact disagree, so build the artifacts and enumerate that build again"
            )
            continue
        texts[set_name] = text
    return texts, refusals


def _class_licence_refusals(inputs: Inputs) -> list[str]:
    """One line per set whose covering file is absent, states a licence it does not name, or differs from the archive's."""
    released = [row for row in inputs.classes if row.state == RELEASED]
    needed: dict[str, list[ClassRow]] = {}
    for set_name in SETS:
        members = [entry for entry in inputs.entries if entry.set == set_name]
        if not members:
            continue
        wanted = _released_in_set(members, released)
        if wanted:
            needed[set_name] = wanted
    texts, refusals = _covering_texts(inputs, [name for name in (SDIST, WHEEL) if name in needed])
    for set_name, wanted in needed.items():
        covering = inputs.covering[set_name]
        entry = _covering_entry(set_name, covering, inputs.entries)
        if entry is None:
            names = ", ".join(row.name for row in wanted)
            count = sum(1 for member in inputs.entries if member.set == set_name)
            refusals.append(
                f"refused: class-licence: set {set_name} holds {count} entries in {names} and no entry "
                f"{covering!r}; a set that ships a released class carries that class's licence file, so ship it"
            )
            continue
        if set_name == SOURCE_ARCHIVE:
            refusals.extend(_archive_licence_refusals(inputs, entry, wanted))
            continue
        text = texts.get(set_name)
        if text is None:
            continue
        stated = _stated_family(text)
        for row in wanted:
            assert row.licence is not None, "a released class states a licence"
            family = _families(row.licence)
            if not family:
                refusals.append(
                    f"refused: class-licence: class {row.name} states {row.licence!r}, and no licence marker is "
                    f"declared for that expression, so {set_name} {covering!r} cannot be checked against it; "
                    f"declare a marker for the family or state a licence this check reads"
                )
                continue
            if stated is not None and stated in family:
                continue
            refusals.append(
                f"refused: class-licence: {set_name} {covering!r} states {stated!r} and class "
                f"{row.name} states {row.licence!r}; the file a set ships as its licence must be that licence, "
                f"read from the licence's own title and not from a licence it mentions"
            )
    return refusals


def _archive_licence_refusals(inputs: Inputs, entry: Entry, wanted: list[ClassRow]) -> list[str]:
    """The source archive's covering file against the built copy of the same file at the same commit."""
    built = _covering_entry(SDIST, inputs.covering[SDIST], inputs.entries) or _covering_entry(
        WHEEL, inputs.covering[WHEEL], inputs.entries
    )
    if built is None or built.sha256 is None or entry.sha256 is None:
        names = ", ".join(row.name for row in wanted)
        return [
            f"refused: class-licence: set {SOURCE_ARCHIVE} holds {entry.path!r} with no digest and no built "
            f"distribution carries {inputs.covering[SDIST]!r}, so the licence {names} states cannot be read from "
            f"the archive; ship the licence file in the built distributions"
        ]
    if entry.sha256 == built.sha256:
        return []
    return [
        f"refused: class-licence: set {SOURCE_ARCHIVE} {entry.path!r} carries sha256 {entry.sha256} and the built "
        f"distribution carries {built.sha256}; the archive and the distributions ship the same licence file at the "
        f"same commit, so rebuild the distributions from the tagged commit"
    ]


def _readme_refusals(inputs: Inputs) -> list[str]:
    """One line per licence the README states for this distribution that the table's source licence does not carry."""
    carried = _families(inputs.source_licence)
    carried_expressions = set(_identifiers(inputs.source_licence))
    if not carried:
        print(f"readme: no licence marker is declared for {inputs.source_licence!r}, so it is not compared")
        return []
    refusals = []
    for statement in _readme_statements(inputs.texts["readme"]):
        if statement.expression is not None:
            if statement.expression in carried_expressions:
                continue
            stated = statement.expression
        elif statement.family in carried:
            continue
        else:
            stated = statement.family
        sentence = " ".join(statement.source_text.split())
        refusals.append(
            f"refused: readme-licence: the README states {stated!r} in a statement naming no repository path and "
            f"the release carries {inputs.source_licence!r}; the README is the distributions' long description and "
            f"a licence it names is compared at its SPDX identifier where the list carries the spelling and by "
            f"family otherwise, so state the licence the release carries, or scope that statement to its path. "
            f"Statement: {sentence}"
        )
    return refusals


def _citation_refusals(inputs: Inputs) -> list[str]:
    """One line when `CITATION.cff` states a licence other than the table's source licence, or states none."""
    value = _citation_licence(inputs.texts["citation"])
    if value == inputs.source_licence:
        return []
    stated = f"states license {value!r}" if value else "carries no top-level `license` field"
    return [
        f"refused: citation-licence: the citation {stated} and class {METADATA_CLASS} states "
        f"{inputs.source_licence!r}; the archive record takes this field's value as the deposit's licence, so "
        f"correct whichever is wrong before the upload"
    ]


def _print_probes(inputs: Inputs) -> None:
    """The README's statements and the citation's field, so a reader sees what was compared and stayed silent."""
    families = tuple(dict.fromkeys(statement.family for statement in _readme_statements(inputs.texts["readme"])))
    print(f"readme: {list(families) or 'no licence named'} for this distribution")
    print(f"citation: license {_citation_licence(inputs.texts['citation'])}")


def _parse_arguments(argv: list[str] | None) -> argparse.Namespace:
    """The enumeration, the table, the artifacts and the two documents this check compares."""
    parser = argparse.ArgumentParser(
        description="Check the licence statements a release carries, against the release-state table."
    )
    parser.add_argument("--enumeration", required=True, help="the release enumeration the script wrote")
    parser.add_argument("--table", required=True, help="the release-state table the licences are read from")
    parser.add_argument("--dist", required=True, help="the directory holding the built sdist and wheel")
    parser.add_argument("--readme", required=True, help="the README that becomes the long description")
    parser.add_argument("--citation", required=True, help="the CITATION.cff the record's licence is read from")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Check every statement, print one line per refusal, and return the exit status."""
    arguments = _parse_arguments(argv)
    inputs, status = _read_inputs(arguments)
    if inputs is None:
        return status

    tag = inputs.document.get("tag")
    commit = inputs.document.get("commit")
    if isinstance(tag, str) and tag and isinstance(commit, str) and commit:
        print(f"licence check for {tag} at {commit}")
    else:
        print("licence check for an enumeration naming no release")
    _print_inputs(inputs)
    _print_probes(inputs)

    refusals = (
        _source_licence_refusals(inputs)
        + _class_licence_refusals(inputs)
        + _readme_refusals(inputs)
        + _citation_refusals(inputs)
    )
    for line in refusals:
        print(line)
    print(f"licence check: {len(refusals)} refusal(s)" if refusals else "licence check: conforms")
    return 1 if refusals else 0


if __name__ == "__main__":
    sys.exit(main())
