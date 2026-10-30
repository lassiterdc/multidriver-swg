"""Record which legacy lines real inputs reach, and check that the committed synthetic inputs reach them too.

A unit's lift runs on synthetic inputs generated once and committed, so that the public toolkit
holds no real data. Synthetic data can miss a branch that real data takes, and then the equivalence
test passes over a branch the real run never leaves while skipping one it does. The remedy is to
measure, once, which lines of the legacy unit the real inputs execute, commit only those line numbers
(no value of any input), and fail a test when the synthetic inputs execute fewer of them.

This module holds the three parts of that remedy.

- The record: `tests/correspondence/reach/{system}.toml`, one file per owning system (the spelling of
  its lift module, `tests/lifts/{system}.py`), holding `[[entry]]` tables. An entry names a unit
  (and part), the cut digests of the unit's stored texts and the digest of its lift as they were when
  the lines were measured, the date and toolkit commit of the measurement, and the sorted legacy line
  numbers reached. It holds no input value and no path of where the inputs were.
- The instrument: `reached_lines` runs a lift under a line tracer and reports the legacy line numbers
  of the unit's own code that executed. It uses `sys.settrace` and `threading.settrace` only, so the
  State check job can run it with the standard library, and the same instrument measures an entry and
  checks it, so the two sets are comparable. It puts back the tracer that was installed before it.
  A line run in another process is not seen, which under-reports reach and never over-reports it.
  For an adapted lift the tracer sees the adapted copy's lines and maps each to a legacy line number
  through the stored diff; a line inside a changed hunk has no legacy number, is listed apart as
  unmapped and is never counted as reach.
- The check: `reach_findings` runs every entry's lift on its committed fixtures and names the lines
  of the entry that the run did not reach, and names an entry whose digests no longer match its unit's
  stored texts or its lift as stale instead of trusting it.
- The named absence: a `[[named]]` table in the same file records a declaration that has no entry and
  that no real input has been shown to measure, with the reason stating the measurement that shows the
  absence. It is a record, not an exemption: a named declaration that also has an entry, or that matches
  no declared lift, is a finding, and the count `summary_line` prints is the declarations that are
  neither entered nor named, so a named absence is visible beside it rather than folded into it.

`python -m tests.correspondence.reach measure --system {slug} --unit {key} [--part {part}] --inputs
{directory}` runs the declared lift once on real inputs and prints the entry for its author to commit.
`{directory}` holds one `{fixture name}.npz` per fixture the declaration names, in the value codec's
form; the declaration receives them in place of the committed fixtures, so its own bindings read them.
The command names no location and reads no variable of where real inputs live: the caller supplies
the directory, and nothing of it is printed or recorded.

Not established: a measurement made under one interpreter and checked under another. A line event
is the interpreter's, and a minor version can add or drop one for a compound statement, so an entry is
measured under the interpreter that checks it or under one that reports the same lines for that unit.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime
import subprocess
import sys
import threading
import tomllib
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tests.correspondence import extraction, lift, value_codec

REPO_ROOT = extraction.REPO_ROOT
REACH = Path("tests/correspondence/reach")
_FIELDS = ("unit", "cuts", "lift", "measured", "commit", "lines")
_NAMED_FIELDS = ("unit", "reason")


class ReachError(Exception):
    """A reach entry that cannot be read, or a measurement that cannot be made, with the reason in the message."""


# ---- the instrument ----


@dataclass
class Reached:
    """What one traced run executed: legacy line numbers reached, and adapted lines that have no legacy number."""

    lines: set[int] = field(default_factory=set)
    unmapped: set[tuple[str, int]] = field(default_factory=set)


def _targets(declaration: lift.LiftDeclaration) -> dict[str, dict[int, int] | None]:
    """The file names the lift compiles under, each with its map from traced line to legacy line.

    An unchanged lift is compiled under the legacy file's path at the legacy line numbers, so its map is
    None (the identity). An adapted lift is compiled under its adapted copy's own path, one per range,
    and each map is that range's stored-diff mapping.
    """
    _, directory = lift._section(declaration.root)
    if declaration.form != "adapted":
        return {f"{directory}/{file}": None for file, _first, _last in lift._ranges(declaration)}
    mapping = lift.legacy_lines(declaration)
    targets: dict[str, dict[int, int] | None] = {}
    for _file, first, last, _data, path in lift._stored_texts(declaration):
        targets[path.relative_to(declaration.root).as_posix()] = mapping[f"{first}-{last}"]
    return targets


def _run_traced(targets: dict[str, dict[int, int] | None], run: Callable[[], Any]) -> Reached:
    """Run `run()` under a tracer that records the lines of frames whose file is one of `targets`."""
    seen: set[tuple[str, int]] = set()

    def local(frame, event, _arg):
        """Record a line event of a target frame, and keep tracing the frame."""
        if event == "line":
            seen.add((frame.f_code.co_filename, frame.f_lineno))
        return local

    def trace(frame, event, _arg):
        """Trace a frame when its code was compiled under one of the target file names."""
        if event == "call" and frame.f_code.co_filename in targets:
            return local
        return None

    previous, previous_thread = sys.gettrace(), threading.gettrace()
    sys.settrace(trace)
    threading.settrace(trace)
    try:
        run()
    finally:
        sys.settrace(previous)
        threading.settrace(previous_thread)
    reached = Reached()
    for filename, number in seen:
        mapping = targets[filename]
        if mapping is None:
            reached.lines.add(number)
        elif number in mapping:
            reached.lines.add(mapping[number])
        else:
            reached.unmapped.add((filename, number))
    return reached


def reached_lines(declaration: lift.LiftDeclaration, inputs: Path | None = None) -> Reached:
    """Run the declared lift under the tracer and return the legacy lines its own code executed.

    With `inputs` (a directory of `{fixture name}.npz` files) the declaration receives those values in
    place of the committed fixtures, which is how a measurement reads real data; without it the
    declaration receives its committed fixtures, which is how the check reads synthetic data. The lift
    is run through `lift.run_lift` in both cases and nowhere else.
    """
    run_declaration = declaration
    if inputs is not None:
        values = _read_inputs(declaration, Path(inputs))
        run_declaration = dataclasses.replace(
            declaration, fixtures=(), function=lambda _loaded: declaration.function(values)
        )
    return _run_traced(_targets(declaration), lambda: lift.run_lift(run_declaration))


def _read_inputs(declaration: lift.LiftDeclaration, directory: Path) -> dict[str, Any]:
    """Decode `{name}.npz` of `directory` for each fixture name the declaration lists, naming a missing one."""
    values = {}
    for name in declaration.fixtures:
        path = directory / f"{name}.npz"
        if not path.is_file():
            raise ReachError(f"{declaration.unit}: no input file {name}.npz in the given inputs directory")
        values[name] = value_codec.decode(path.read_bytes())
    return values


# ---- the entry ----


def _cut_labels(declaration: lift.LiftDeclaration) -> list[str]:
    """The cut-rule digest of each stored text of the unit, read from the stored extraction in range order."""
    name, _ = lift._section(declaration.root)
    return [
        extraction.label(
            extraction.CUT_RULE, (declaration.root / extraction.stored_path(name, file, first, last)).read_bytes()
        )
        for file, first, last in lift._ranges(declaration)
    ]


def build_entry(declaration: lift.LiftDeclaration, lines: set[int], *, commit: str, measured: str) -> dict:
    """The entry holding `lines`, keyed by the digests of the unit's stored texts and of its lift as they are now."""
    entry: dict[str, Any] = {"unit": declaration.unit}
    if declaration.part is not None:
        entry["part"] = declaration.part
    entry.update(
        {
            "cuts": _cut_labels(declaration),
            "lift": lift.lift_digest(declaration),
            "measured": measured,
            "commit": commit,
            "lines": sorted(lines),
        }
    )
    return entry


def render_entry(entry: dict) -> str:
    """The entry as an `[[entry]]` TOML table; every value is a string, an int or a list of them, none needs escaping."""

    def quote(text: str) -> str:
        """A TOML basic string for text without a quote, backslash or control character."""
        if any(c in text for c in '"\\\n\r\t'):
            raise ReachError(f"a reach entry field {text!r} would need escaping")
        return f'"{text}"'

    out = ["[[entry]]", f"unit = {quote(entry['unit'])}"]
    if "part" in entry:
        out.append(f"part = {quote(entry['part'])}")
    out.append("cuts = [" + ", ".join(quote(c) for c in entry["cuts"]) + "]")
    out.append(f"lift = {quote(entry['lift'])}")
    out.append(f"measured = {quote(entry['measured'])}")
    out.append(f"commit = {quote(entry['commit'])}")
    out.append("lines = [" + ", ".join(str(n) for n in entry["lines"]) + "]")
    return "\n".join(out) + "\n"


def read_entries(root: Path = REPO_ROOT) -> list[tuple[str, dict]]:
    """Every entry under `{root}/tests/correspondence/reach/`, as (system, entry), refusing a malformed or duplicated one.

    A file is `{system}.toml`. An entry lacking a field, holding a line that is not a positive int or
    holding its lines unsorted or repeated is refused naming the file and the unit; so are two entries
    of one (unit, part) in one file.
    """
    directory = Path(root) / REACH
    if not directory.is_dir():
        return []
    entries = []
    for path in sorted(directory.glob("*.toml")):
        try:
            document = tomllib.loads(path.read_text(encoding="utf-8"))
        except (tomllib.TOMLDecodeError, UnicodeDecodeError) as exc:
            raise ReachError(f"{path}: not readable TOML ({exc})") from exc
        seen: set[tuple[str, str | None]] = set()
        for entry in document.get("entry", []):
            where = f"{path}: entry {entry.get('unit')!r}"
            missing = [name for name in _FIELDS if name not in entry]
            if missing:
                raise ReachError(f"{where} lacks the fields {missing}")
            lines = entry["lines"]
            if (
                not isinstance(lines, list)
                or any(type(n) is not int or n < 1 for n in lines)
                or lines != sorted(set(lines))
            ):
                raise ReachError(f"{where}: lines must be distinct positive integers in order")
            key = (entry["unit"], entry.get("part"))
            if key in seen:
                raise ReachError(f"{where}: recorded twice")
            seen.add(key)
            entries.append((path.stem, entry))
    return entries


# ---- the check ----


@dataclass
class ReachReport:
    """What `reach_findings` found, and the counts a reader needs beside a pass."""

    findings: list[str]
    declared: int
    entries: int
    named: int
    without_entry: int


def read_named(root: Path = REPO_ROOT) -> list[tuple[str, dict]]:
    """Every `[[named]]` table under `{root}/tests/correspondence/reach/`, as (system, table), refusing a malformed or duplicated one.

    A named table records a declaration with no entry that no real input has been shown to measure: its unit, its
    part where it has one, and the reason, which states the measurement that shows the absence. A table lacking a
    field or holding an empty reason is refused naming the file and the unit; so are two tables of one (unit, part)
    in one file.
    """
    directory = Path(root) / REACH
    if not directory.is_dir():
        return []
    named = []
    for path in sorted(directory.glob("*.toml")):
        try:
            document = tomllib.loads(path.read_text(encoding="utf-8"))
        except (tomllib.TOMLDecodeError, UnicodeDecodeError) as exc:
            raise ReachError(f"{path}: not readable TOML ({exc})") from exc
        seen: set[tuple[str, str | None]] = set()
        for table in document.get("named", []):
            where = f"{path}: named {table.get('unit')!r}"
            missing = [name for name in _NAMED_FIELDS if name not in table]
            if missing:
                raise ReachError(f"{where} lacks the fields {missing}")
            if not isinstance(table["reason"], str) or not table["reason"].strip():
                raise ReachError(f"{where}: the reason must be a non-empty string")
            key = (table["unit"], table.get("part"))
            if key in seen:
                raise ReachError(f"{where}: recorded twice")
            seen.add(key)
            named.append((path.stem, table))
    return named


def reach_findings(root: Path = REPO_ROOT) -> ReachReport:
    """Run every entry's lift on its committed fixtures and report the entry lines it did not reach.

    An entry is stale, and is reported as such without running anything, when its cut digests or its
    lift digest differ from the unit's now. An entry that matches no declared lift is reported. A named
    absence that matches no declared lift, or that also has an entry, is reported. The report also counts
    the declared lifts that are neither entered nor named, so that a pass over no entries reads as what
    it is.
    """
    root = Path(root)
    declarations = lift.discover(root)
    by_key = {(d.system, d.unit, d.part): d for d in declarations}
    entries = read_entries(root)
    findings: list[str] = []
    covered: set[tuple[str, str, str | None]] = set()
    for system, entry in entries:
        key = (system, entry["unit"], entry.get("part"))
        label = f"reach entry {entry['unit']!r}" + (f" part {entry['part']!r}" if "part" in entry else "")
        declaration = by_key.get(key)
        if declaration is None:
            findings.append(f"{label} of system {system!r}: no declared lift matches it")
            continue
        covered.add(key)
        if entry["cuts"] != _cut_labels(declaration):
            findings.append(f"{label} is STALE: its cut digests are not those of the unit's stored texts")
            continue
        if entry["lift"] != lift.lift_digest(declaration):
            findings.append(f"{label} is STALE: its lift digest is not the lift's now")
            continue
        try:
            reached = reached_lines(declaration)
        except Exception as exc:  # a lift that cannot run on its fixtures reaches nothing, whatever the reason
            findings.append(f"{label}: the lift did not run on its committed fixtures ({type(exc).__name__}: {exc})")
            continue
        missed = sorted(set(entry["lines"]) - reached.lines)
        if missed:
            findings.append(f"{label}: the committed fixtures do not reach legacy lines {missed}")
    absent: set[tuple[str, str, str | None]] = set()
    for system, table in read_named(root):
        key = (system, table["unit"], table.get("part"))
        label = f"named absence {table['unit']!r}" + (f" part {table['part']!r}" if "part" in table else "")
        if key not in by_key:
            findings.append(f"{label} of system {system!r}: no declared lift matches it")
            continue
        if key in covered:
            findings.append(
                f"{label} of system {system!r}: it has a reach entry, so naming it as absent contradicts the entry"
            )
            continue
        absent.add(key)
    return ReachReport(findings, len(declarations), len(entries), len(absent), len(set(by_key) - covered - absent))


def summary_line(report: ReachReport) -> str:
    """One line carrying the counts, each with its unit, for a reader of a pass."""
    return (
        f"reach: {report.entries} entr{'y' if report.entries == 1 else 'ies'}, "
        f"{report.declared} declared lift(s), {report.without_entry} declared lift(s) with no entry, "
        f"{report.named} named absence(s), {len(report.findings)} finding(s)"
    )


# ---- the command ----


def _head_commit(root: Path) -> str:
    """The checkout's current commit id, or `unknown` when git cannot say."""
    done = subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"], capture_output=True, text=True, check=False)
    return done.stdout.strip() if done.returncode == 0 and done.stdout.strip() else "unknown"


def measure(
    system: str,
    unit: str,
    part: str | None,
    inputs: Path,
    *,
    root: Path = REPO_ROOT,
    commit: str | None = None,
    measured: str | None = None,
) -> str:
    """Run the declared lift on real inputs and return the entry text to commit, naming a missing or doubled declaration."""
    matches = [
        d
        for d in lift.discover(Path(root))
        if d.system == system.replace("-", "_") and d.unit == unit and d.part == part
    ]
    if len(matches) != 1:
        raise ReachError(f"{len(matches)} declared lifts match system {system!r}, unit {unit!r}, part {part!r}")
    declaration = matches[0]
    reached = reached_lines(declaration, Path(inputs))
    if reached.unmapped:
        print(f"{len(reached.unmapped)} adapted line(s) have no legacy number and are not recorded", file=sys.stderr)
    entry = build_entry(
        declaration,
        reached.lines,
        commit=commit or _head_commit(Path(root)),
        measured=measured or datetime.date.today().isoformat(),
    )
    return render_entry(entry)


def main(argv: list[str] | None = None) -> int:
    """Run `measure` or `summary` from the command line and return the exit status."""
    parser = argparse.ArgumentParser(prog="python -m tests.correspondence.reach", description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    measuring = commands.add_parser("measure", help="run one declared lift on real inputs and print its entry")
    measuring.add_argument("--system", required=True, help="the owning system's slug")
    measuring.add_argument("--unit", required=True, help="the unit's canonical key")
    measuring.add_argument("--part", default=None, help="the unit's part, when the lift declares one")
    measuring.add_argument("--inputs", required=True, type=Path, help="a directory of {fixture name}.npz files")
    measuring.add_argument("--root", type=Path, default=REPO_ROOT, help="the checkout to read")
    summarising = commands.add_parser("summary", help="print the counts and findings of the reach check")
    summarising.add_argument("--root", type=Path, default=REPO_ROOT, help="the checkout to read")
    arguments = parser.parse_args(argv)
    try:
        if arguments.command == "measure":
            print(
                measure(arguments.system, arguments.unit, arguments.part, arguments.inputs, root=arguments.root),
                end="",
            )
            return 0
        report = reach_findings(arguments.root)
    except (ReachError, lift.LiftError, value_codec.CodecError) as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 1
    print(summary_line(report))
    for finding in report.findings:
        print(f"  {finding}", file=sys.stderr)
    return 1 if report.findings else 0


if __name__ == "__main__":
    sys.exit(main())
