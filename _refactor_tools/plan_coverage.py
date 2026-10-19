"""Check that one system's planning documents adopt each of its tests exactly once.

Every equivalence and refactor-reminder test names its owning system in its decorator, and a
system's planning documents say which of those tests they will make pass by carrying one
adoption line per test: `ADOPTS unit={unit key}`, with an optional ` part={part}`. Those lines
are written by hand, so this script shows when they fall out of date. Given one system and its
documents by path, each declared as an implementation plan or as a plan-implementation prompt,
it reads only the adoption lines (a test named anywhere else in a document does not count) and
reports six findings:

- `UNPLANNED`: a test of the system that no implementation plan adopts;
- `MULTIPLY-ADOPTED`: a test of the system adopted by more than one document;
- `NAMES-NO-TEST`: an adoption line naming a unit and part that no test carries;
- `FOREIGN`: an adoption line naming another system's test;
- `MALFORMED`: a line beginning `ADOPTS` that is not an adoption line;
- `UNTESTED`: a scoped-in unit with no test at all, whose ledger row cites a covers statement
  of this system.

A test is identified here by its (unit, part) and never by its family, so a plan's line stays
valid when a reminder is replaced by an equivalence test of the same unit and part.

Run from the repository root as `python -m _refactor_tools.plan_coverage --system {slug}
--plan {path} ... --prompt {path} ... [--require-complete]`. It prints each finding and one
summary line, and exits 1 on any finding except `UNPLANNED`, and on `UNPLANNED` too only with
`--require-complete`. A `--system` value that names no system of the system-scope file is a
usage error (exit 2) naming it, because a mistyped name would otherwise read as a system with no
tests and pass `--require-complete`. The summary line also counts the tests whose family
decorators the test inventory could not read, since the inventory leaves those tests out of its
list and this check therefore cannot see them. It reads only the files it is given and the toolkit's own test tree,
ledger and system-scope file, and writes nothing, so two runs on the same files print the same
output.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from tests.correspondence import inventory, ledger, system_scope

REPO_ROOT = inventory.REPO_ROOT
# fullmatch over lines from str.splitlines(), never a `$`-anchored match over raw text: `$` also
# matches just before a final newline, so a line carrying a trailing space or a second token
# must fail here and be reported as MALFORMED rather than read as an adoption.
ADOPTION_RE = re.compile(r"ADOPTS unit=(\S+)( part=(\S+))?")
PREFIX = "ADOPTS"
FINDINGS = ("UNPLANNED", "MULTIPLY-ADOPTED", "NAMES-NO-TEST", "FOREIGN", "MALFORMED", "UNTESTED")


def adoptions(text: str) -> tuple[list[tuple[int, str, str | None]], list[tuple[int, str]]]:
    """Return the document's adoption lines as (line number, unit, part) and its malformed lines as (line number, text).

    A line counts only when the whole line is an adoption line; any other line beginning
    `ADOPTS` is malformed, and every other line is ignored, however many unit keys it mentions.
    Line numbers start at 1.
    """
    lines, malformed = [], []
    for number, line in enumerate(text.splitlines(), start=1):
        if not line.startswith(PREFIX):
            continue
        match = ADOPTION_RE.fullmatch(line)
        if match:
            lines.append((number, match.group(1), match.group(3)))
        else:
            malformed.append((number, line))
    return lines, malformed


def _part(part: str | None) -> str:
    """A part as printed: its value, or `-` for a test with no part."""
    return part if part is not None else "-"


def _owners(entries: list[dict]) -> dict[tuple[str, str | None], set[str]]:
    """Map each (unit, part) the test inventory lists to the owning systems of its tests."""
    owners: dict[tuple[str, str | None], set[str]] = {}
    for entry in entries:
        owners.setdefault((entry["unit"], entry["part"]), set()).add(entry["system"])
    return owners


def _read_adoptions(documents: list[tuple[str, str]]) -> tuple[list[dict], list[str]]:
    """Read each (kind, path) document; return its adoption lines with their document, and its MALFORMED findings."""
    found, malformed = [], []
    for kind, path in documents:
        lines, bad = adoptions(Path(path).read_text(encoding="utf-8"))
        found += [{"kind": kind, "path": path, "line": n, "unit": u, "part": p} for n, u, p in lines]
        malformed += [
            f"MALFORMED {path}:{n}: {text!r} begins {PREFIX} and is not `{PREFIX} unit={{key}}` "
            "with an optional ` part={part}`"
            for n, text in bad
        ]
    return found, malformed


def _line_findings(system: str, found: list[dict], owners: dict) -> list[str]:
    """The NAMES-NO-TEST and FOREIGN findings, one per adoption line that names no test of this system."""
    findings = []
    for a in found:
        systems = owners.get((a["unit"], a["part"]))
        site = f"{a['path']}:{a['line']}"
        if not systems:
            findings.append(f"NAMES-NO-TEST {site}: adopts {a['unit']} part={_part(a['part'])}, which no test carries")
        elif system not in systems:
            others = ", ".join(sorted(systems))
            findings.append(
                f"FOREIGN {site}: adopts {a['unit']} part={_part(a['part'])}, a test of {others} and not of {system}"
            )
    return findings


def _unplanned_findings(own: list[tuple[str, str | None]], found: list[dict]) -> list[str]:
    """The UNPLANNED findings: each of the system's tests that no implementation plan adopts.

    A test adopted only by plan-implementation prompts is still UNPLANNED, and its line names the
    prompts: a prompt is an early signal of planning, never the commitment an implementation plan
    is, so `--require-complete` must fail on it.
    """
    findings = []
    for unit, part in own:
        kinds = {a["kind"] for a in found if (a["unit"], a["part"]) == (unit, part)}
        if "plan" in kinds:
            continue
        line = f"UNPLANNED {unit} part={_part(part)}: no implementation plan adopts this test"
        if "prompt" in kinds:
            prompts = sorted({a["path"] for a in found if (a["unit"], a["part"]) == (unit, part)})
            line += f"; adopted only by the prompt(s) {', '.join(prompts)}"
        findings.append(line)
    return findings


def _multiple_findings(own: list[tuple[str, str | None]], found: list[dict]) -> list[str]:
    """The MULTIPLY-ADOPTED findings: each of the system's tests adopted by two or more documents, of either kind."""
    findings = []
    for unit, part in own:
        sites = [f"{a['path']}:{a['line']}" for a in found if (a["unit"], a["part"]) == (unit, part)]
        if len({a["path"] for a in found if (a["unit"], a["part"]) == (unit, part)}) > 1:
            findings.append(f"MULTIPLY-ADOPTED {unit} part={_part(part)}: adopted by {', '.join(sites)}")
    return findings


def _untested_findings(system: str, entries: list[dict], root: Path) -> list[str]:
    """The UNTESTED findings: each scoped-in unit with no test whose ledger row cites a covers statement of this system."""
    tested = {entry["unit"] for entry in entries}
    findings = []
    for row in ledger.load(root)[0]:
        if row.get("disposition") != "scope-in" or row.get("key") in tested:
            continue
        cited = system_scope.statement(row.get("scope", ""), root)
        if cited is not None and cited["slug"] == system and cited.get("mark") == "covers":
            findings.append(
                f"UNTESTED {row['key']}: scoped in under {row['scope']}, a covers statement of {system}, with no test"
            )
    return findings


def check(system: str, plans: list[str], prompts: list[str], root: Path = REPO_ROOT) -> list[str]:
    """Return every finding for `system` over its implementation plans and its plan-implementation prompts, by path.

    The system's tests come from the test inventory of the tree at `root`, and its scoped-in
    units from that tree's ledger and system-scope file. Each finding begins with its name.
    """
    entries, _unreadable = inventory.listing(root)
    owners = _owners(entries)
    own = sorted({(e["unit"], e["part"]) for e in entries if e["system"] == system}, key=lambda k: (k[0], _part(k[1])))
    found, malformed = _read_adoptions([("plan", p) for p in plans] + [("prompt", p) for p in prompts])
    return (
        _unplanned_findings(own, found)
        + _multiple_findings(own, found)
        + _line_findings(system, found, owners)
        + malformed
        + _untested_findings(system, entries, root)
    )


def exit_code(findings: list[str], require_complete: bool) -> int:
    """1 when any finding fails the run: any but UNPLANNED always, and UNPLANNED only under `--require-complete`."""
    failing = [f for f in findings if require_complete or not f.startswith("UNPLANNED ")]
    return 1 if failing else 0


def unknown_system(system: str, root: Path = REPO_ROOT) -> str | None:
    """The usage-error message for a system name the system-scope file does not carry, or None for a known one.

    The names are read from the same file the collection-time refusal reads its slugs from, so
    this check and that refusal can never disagree about which systems exist.
    """
    if system in system_scope.slugs(root):
        return None
    return f"argument --system: {system!r} is not a system of the system-scope file"


def unreadable_tests(unreadable: list[str]) -> int:
    """Count the distinct tests, by file and function, behind the test inventory's unreadable-decorator lines.

    The inventory reports each decorator it cannot read as `{file}:{line}: unreadable {name}
    marker on {function}` and skips that decorator only. A test whose every family decorator is
    unreadable is therefore left out of its list, while a test that also carries a readable family
    decorator stays listed under that one and is still counted here. One line is written per
    unreadable decorator, so a test with two of them is one test here, not two. The count is over
    the whole tree, not over one system: the unreadable value may be the `system=` keyword itself,
    so such a test cannot be given an owner. Residual: two functions of one name in one file (in
    two classes) count once.
    """
    return len({(line.split(":", 1)[0], line.rsplit(" ", 1)[-1]) for line in unreadable})


def main(argv: list[str] | None = None, root: Path = REPO_ROOT) -> int:
    """Parse the arguments, print each finding and the summary line, and return the exit code.

    An unknown `--system` name ends the run with argparse's usage error, exit 2, before any
    document is read.
    """
    parser = argparse.ArgumentParser(
        prog="python -m _refactor_tools.plan_coverage", description=__doc__.splitlines()[0]
    )
    parser.add_argument("--system", required=True, help="the owning system's slug")
    parser.add_argument("--plan", action="extend", nargs="+", default=[], help="an implementation plan of the system")
    parser.add_argument("--prompt", action="extend", nargs="+", default=[], help="a plan-implementation prompt")
    parser.add_argument("--require-complete", action="store_true", help="fail on UNPLANNED as well")
    args = parser.parse_args(argv)
    message = unknown_system(args.system, root)
    if message is not None:
        parser.error(message)
    findings = check(args.system, args.plan, args.prompt, root)
    for finding in findings:
        print(finding)
    # The denominators are printed beside the findings so that a clean run over nothing (no test of
    # the system, no adoption line) reads as a stated zero rather than as a pass; the unreadable
    # count says how many tests the inventory, and so this check, could not see at all.
    entries, unreadable = inventory.listing(root)
    tests = {(e["unit"], e["part"]) for e in entries if e["system"] == args.system}
    lines = _read_adoptions([("plan", p) for p in args.plan] + [("prompt", p) for p in args.prompt])[0]
    counts = ", ".join(f"{name} {sum(1 for f in findings if f.startswith(name + ' '))}" for name in FINDINGS)
    print(
        f"plan coverage of {args.system}: {len(tests)} tests by (unit, part), {len(lines)} adoption lines in "
        f"{len(args.plan)} implementation plans and {len(args.prompt)} prompts; "
        f"{unreadable_tests(unreadable)} tests in the tree whose family or guard decorator the inventory "
        f"could not read; findings: {counts}"
    )
    return exit_code(findings, args.require_complete)


if __name__ == "__main__":
    sys.exit(main())
