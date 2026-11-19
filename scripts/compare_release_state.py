#!/usr/bin/env python3
"""Compare an enumeration of a release's bytes against the release-state table.

The table, `release_state.toml` at the repository root, declares one class of artifact bytes per
`[[class]]` table, each with the system that declares it, one of the states `released`, `excluded`
and `estate-resident`, and the globs over an enumeration's `path` values that name the class's
paths. A `released` class also states the licence the release carries for it.

This comparison assigns every entry of every set of an enumeration to the classes whose globs match
its path, and then prints:

- the table's class count;
- one line per set the enumeration holds: its entry count, the count in a `released` class, the
  count in an `excluded` or `estate-resident` class, the count in two classes, and the count in no
  class;
- the first 20 paths of one set that fall in no class, when there are any;
- the sentence `recall direction (a class that should be declared and was not) is unmeasured`,
  which is the limit of what this comparison can say: it can refuse bytes a class declares, and it
  cannot know that a shipped byte belongs to a class nobody declared.

Two facts refuse the release, each printing one `refused: {id}: {detail}` line per path, and the
closing line reads `release state comparison: {n} refusal(s)` or `release state comparison:
conforms`:

- `excluded-shipped` / `estate-resident-shipped`: an entry is in a class the table declares
  excluded from the release or resident in the private estate, so the release would ship bytes the
  system that owns them says it must not.
- `two-classes`: an entry is in two classes, so two systems claim one path and the release states
  no single state for it.

An entry in no class passes: the count is printed and the exit status stays 0, because a class a
system has not declared yet is a gap its owner closes, and refusing every release over it would
make this comparison a census it does not claim to be. A malformed table or enumeration is one
`refused: unreadable-input: {detail}` line and exit status 2, the status a broken invocation takes:
the comparison read nothing it can classify, so it reports no verdict about the release.

A pattern's `*` matches across directory separators, so `tests/**` names every entry under `tests`
and `tests/` itself. That is load-bearing for a directory entry, which is a member of an archive
like any other.

Exit 0 = conforming, 1 = refused, 2 = the table or the enumeration could not be read.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

SCHEMA = 1
SETS = ("sdist", "wheel", "source-archive")
RELEASED = "released"
EXCLUDED = "excluded"
ESTATE_RESIDENT = "estate-resident"
STATES = (RELEASED, EXCLUDED, ESTATE_RESIDENT)
CLASS_KEYS = frozenset({"name", "system", "state", "paths", "licence"})
ENTRY_KEYS = frozenset({"set", "path", "sha256", "size"})
UNCLASSIFIED_SHOWN = 20
RECALL_SENTENCE = "recall direction (a class that should be declared and was not) is unmeasured"
_HEX_DIGITS = frozenset("0123456789abcdef")


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
class SetReport:
    """What one set of an enumeration holds, counted by the classes its entries fall in."""

    name: str
    entries: int
    released: int
    blocked: tuple[tuple[Entry, ClassRow], ...]
    twice: tuple[tuple[Entry, tuple[ClassRow, ...]], ...]
    unclassified: tuple[str, ...]


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
        return None, f"{path} declares no [[class]] table; one row per artifact class is what this comparison reads"
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


def _load_enumeration(path: Path) -> tuple[list[Entry] | None, str]:
    """The enumeration's entries, and why they could not be read when they are None."""
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        return None, f"{path} could not be read ({error})"
    try:
        document = json.loads(text)
    except json.JSONDecodeError as error:
        return None, f"{path} is not one readable JSON document ({error})"
    if not isinstance(document, dict):
        return None, f"{path} holds a {type(document).__name__} and not one JSON object"
    if document.get("schema") != SCHEMA:
        return None, f"{path} declares schema {document.get('schema')!r}, and this comparison reads schema {SCHEMA}"
    rows = document.get("entries")
    if not isinstance(rows, list):
        return None, f"{path} holds entries as {rows!r}, and entries is a list"
    entries: list[Entry] = []
    for index, row in enumerate(rows, 1):
        read, problem = _read_entry(row)
        if read is None:
            return None, f"{path} entry {index} {problem}"
        entries.append(read)
    return entries, ""


def _classes_of(path: str, classes: list[ClassRow]) -> tuple[ClassRow, ...]:
    """The classes whose paths name `path`, in the table's order."""
    return tuple(row for row in classes if any(_matches(path, pattern) for pattern in row.paths))


def _report(entries: list[Entry], classes: list[ClassRow]) -> list[SetReport]:
    """One report per set the enumeration holds, in the schema's set order."""
    reports = []
    for set_name in SETS:
        members = [entry for entry in entries if entry.set == set_name]
        if not members:
            continue
        released = 0
        blocked: list[tuple[Entry, ClassRow]] = []
        twice: list[tuple[Entry, tuple[ClassRow, ...]]] = []
        unclassified: list[str] = []
        for entry in members:
            matched = _classes_of(entry.path, classes)
            if not matched:
                unclassified.append(entry.path)
            elif len(matched) > 1:
                twice.append((entry, matched))
            elif matched[0].state == RELEASED:
                released += 1
            else:
                blocked.append((entry, matched[0]))
        reports.append(SetReport(set_name, len(members), released, tuple(blocked), tuple(twice), tuple(unclassified)))
    return reports


def _remedy(row: ClassRow) -> str:
    """What the declaring system or the release does with a path the class says must not ship."""
    if row.state == ESTATE_RESIDENT:
        return f"take it out of the released bytes, or have {row.system} move the class to released"
    return (
        f"take it out of the released bytes (an export-ignore line for the source archive, a prune for the sdist), "
        f"or have {row.system} restate the class"
    )


def _print_reports(reports: list[SetReport], class_count: int) -> None:
    """The class count, one count line per set, the unclassified paths and the recall sentence."""
    print(f"class count: {class_count}")
    for report in reports:
        print(
            f"set {report.name}: {report.entries} entries; released {report.released}; "
            f"excluded-or-estate-resident {len(report.blocked)}; two-classes {len(report.twice)}; "
            f"unclassified {len(report.unclassified)}"
        )
        if report.unclassified:
            shown = ", ".join(report.unclassified[:UNCLASSIFIED_SHOWN])
            print(f"  not in any class (first {min(len(report.unclassified), UNCLASSIFIED_SHOWN)}): {shown}")
    print(RECALL_SENTENCE)


def _parse_arguments(argv: list[str] | None) -> argparse.Namespace:
    """The enumeration to classify and the table it is classified against."""
    parser = argparse.ArgumentParser(
        description="Compare an enumeration of a release's bytes against the release-state table."
    )
    parser.add_argument("--enumeration", required=True, help="the release enumeration to classify")
    parser.add_argument("--table", required=True, help="the release-state table to classify it against")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Classify every entry, print the counts and every refusal, and return the exit status."""
    arguments = _parse_arguments(argv)
    rows, problem = _load_enumeration(Path(arguments.enumeration))
    if rows is None:
        print(f"refused: unreadable-input: {problem}; enumerate the release again and point --enumeration at it")
        return 2
    classes, problem = _load_table(Path(arguments.table))
    if classes is None:
        print(f"refused: unreadable-input: {problem}; correct the table and compare again")
        return 2

    reports = _report(rows, classes)
    _print_reports(reports, len(classes))

    refusals = 0
    for report in reports:
        for entry, row in report.blocked:
            print(
                f"refused: {row.state}-shipped: {entry.path} is in set {report.name} and in class {row.name} "
                f"({row.system}), which the table declares {row.state}; {_remedy(row)}"
            )
            refusals += 1
        for entry, matched in report.twice:
            names = ", ".join(f"{row.name} ({row.system})" for row in matched)
            print(
                f"refused: two-classes: {entry.path} is in set {report.name} and in classes {names}, and one entry "
                "in two classes refuses the release; have one declaring system narrow its row so one class names it"
            )
            refusals += 1

    print(f"release state comparison: {refusals} refusal(s)" if refusals else "release state comparison: conforms")
    return 1 if refusals else 0


if __name__ == "__main__":
    sys.exit(main())
