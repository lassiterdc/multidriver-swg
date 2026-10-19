"""Report how much of the refactor is planned, per system and in total, from the planning documents passed.

Each system's planning documents adopt that system's tests with adoption lines (the grammar the
per-system plan-coverage check reads). This report is given every system's documents at once,
as groups (one `--group {slug}` followed by that system's `--plan` and `--prompt` paths), and
prints how far the planning has got: for each system and in total, the scoped-in units that are
planned out of all its scoped-in units, split by whether implementation plans or prompts
adopted them; its untested units; and the count of each plan-coverage finding. It then prints
the unit totals from the ledger and the extent rule, including the units not yet ruled, and the
commit of the toolkit and of each repository holding the documents, each with whether its tree
had uncommitted changes.

A scoped-in unit is planned when it has at least one test, every one of its tests is owned by
the system its ledger row is routed to, and each of its (unit, part) keys is adopted by exactly
one document of that system's group. A unit is planned by an implementation plan when every
one of those documents is an implementation plan, and by a prompt otherwise, because a prompt
is an early signal of planning rather than the commitment a plan is. A wrong adoption line
affects only the unit it names.

Run from the repository root as `python -m _refactor_tools.progress_report --group {slug}
--plan {path} ... --prompt {path} ... [--group ...]`. A `--group` name that is not a system of
the system-scope file is a usage error, exit 2, naming it, before any document is read: a
mistyped name would otherwise read as a system with nothing scoped in. A system given as two
groups is refused, exit 1, because its counts would enter the total twice; so is a path given
in two groups, because it would count one document's adoptions for two systems. Otherwise it
prints the report and exits 0. The units line also counts the tests whose family or guard
decorator the test inventory could not read: the planned counts cannot see those tests, so a
unit with one can read as planned while that test is adopted by nothing. The exit code does not
depend on that count. It reads only the files it is given and the toolkit's own test tree,
ledger, corpus record and system-scope file, and writes nothing, so two runs on the same files
print the same output.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from _refactor_tools import plan_coverage
from tests.correspondence import extent, inventory, ledger, system_scope

REPO_ROOT = inventory.REPO_ROOT
KINDS = ("plan", "prompt")


def _parse(argv: list[str] | None) -> list[tuple[str, list[str], list[str]]]:
    """Split the command line into (slug, plans, prompts) groups, each `--group` opening a new one.

    argparse cannot attach repeated options to the `--group` before them, so the arguments are
    walked in order instead; the option names are the ones the interface fixes.
    """
    args = list(sys.argv[1:] if argv is None else argv)
    groups: list[tuple[str, list[str], list[str]]] = []
    kind = None
    for arg in args:
        if arg == "--group":
            kind = "group"
        elif arg in ("--plan", "--prompt"):
            if not groups:
                raise SystemExit(f"progress report: {arg} before any --group")
            kind = arg[2:]
        elif kind == "group":
            groups.append((arg, [], []))
            kind = None
        elif kind in KINDS:
            groups[-1][1 if kind == "plan" else 2].append(arg)
        else:
            raise SystemExit(f"progress report: unexpected argument {arg!r}")
    if not groups:
        raise SystemExit("progress report: at least one --group is required")
    return groups


def duplicate_paths(groups: list[tuple[str, list[str], list[str]]]) -> list[str]:
    """Name each document path given in two or more groups, compared after resolving it."""
    seen: dict[Path, set[str]] = {}
    for slug, plans, prompts in groups:
        for path in [*plans, *prompts]:
            seen.setdefault(Path(path).resolve(), set()).add(slug)
    return [f"{path}: given in the groups of {', '.join(sorted(s))}" for path, s in sorted(seen.items()) if len(s) > 1]


def _adopting(plans: list[str], prompts: list[str]) -> dict[tuple[str, str | None], set[tuple[str, str]]]:
    """Map each adopted (unit, part) to the set of (kind, path) documents whose adoption lines name it."""
    found: dict[tuple[str, str | None], set[tuple[str, str]]] = {}
    for kind, path in [("plan", p) for p in plans] + [("prompt", p) for p in prompts]:
        lines, _malformed = plan_coverage.adoptions(Path(path).read_text(encoding="utf-8"))
        for _number, unit, part in lines:
            found.setdefault((unit, part), set()).add((kind, path))
    return found


def _planned(keys: set, owners: set[str], system: str, adopting: dict) -> bool:
    """Whether a unit with these (unit, part) keys and test owners is planned for `system`."""
    if not keys or owners != {system}:
        return False
    return all(len({path for _kind, path in adopting.get(key, set())}) == 1 for key in keys)


def _kind(keys: set, adopting: dict) -> str:
    """`plan` when every document adopting a planned unit's keys is an implementation plan, else `prompt`."""
    kinds = {kind for key in keys for kind, _path in adopting.get(key, set())}
    return "plan" if kinds == {"plan"} else "prompt"


def _scoped_in(system: str, rows: list[dict], root: Path) -> list[str]:
    """The keys of the scope-in rows whose `scope` cites a covers statement of `system`."""
    keys = []
    for row in rows:
        if row.get("disposition") != "scope-in":
            continue
        cited = system_scope.statement(row.get("scope", ""), root)
        if cited is not None and cited["slug"] == system and cited.get("mark") == "covers":
            keys.append(row["key"])
    return sorted(keys)


def _system_counts(system: str, plans: list[str], prompts: list[str], entries, rows, root: Path) -> dict:
    """The planned, by-kind, scoped-in and untested unit counts and the finding counts of one group."""
    adopting = _adopting(plans, prompts)
    counts = {"scoped": 0, "planned": 0, "plan": 0, "prompt": 0, "untested": 0}
    for unit in _scoped_in(system, rows, root):
        tests = [e for e in entries if e["unit"] == unit]
        counts["scoped"] += 1
        if not tests:
            counts["untested"] += 1
            continue
        keys = {(e["unit"], e["part"]) for e in tests}
        if _planned(keys, {e["system"] for e in tests}, system, adopting):
            counts["planned"] += 1
            counts[_kind(keys, adopting)] += 1
    findings = plan_coverage.check(system, plans, prompts, root)
    for name in plan_coverage.FINDINGS:
        counts[name] = sum(1 for f in findings if f.startswith(name + " "))
    return counts


def _line(label: str, c: dict) -> str:
    """One per-system or total line of the report."""
    findings = ", ".join(f"{name} {c[name]}" for name in plan_coverage.FINDINGS)
    return (
        f"{label}: planned {c['planned']} of {c['scoped']} scoped-in units: by plan {c['plan']}, "
        f"by prompt {c['prompt']}; untested {c['untested']}; findings: {findings}"
    )


def _git(cwd: Path, *args: str) -> str | None:
    """The stripped output of one git command run in `cwd`, or None when it fails."""
    done = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True)
    return done.stdout.strip() if done.returncode == 0 else None


def _tree_state(path: Path) -> str:
    """`{commit} ({clean|uncommitted changes})` for the repository holding `path`, read with git."""
    commit = _git(path, "rev-parse", "HEAD")
    status = _git(path, "status", "--porcelain")
    flag = "uncommitted changes" if status else "clean"
    return f"{commit or 'no commit'} ({flag})"


def _toolkit_state(root: Path) -> str:
    """The toolkit's commit and whether its tree had uncommitted changes."""
    return f"toolkit commit {_tree_state(root)}"


def _documents_state(groups: list[tuple[str, list[str], list[str]]]) -> str:
    """The commit of each repository holding a passed document, with its dirty flag; one entry per repository.

    Documents in several repositories give one entry each, sorted, rather than the first one's
    commit, so no document's version goes unstated.
    """
    tops = set()
    outside = 0
    for _slug, plans, prompts in groups:
        for path in [*plans, *prompts]:
            top = _git(Path(path).resolve().parent, "rev-parse", "--show-toplevel")
            if top is None:
                outside += 1
            else:
                tops.add(top)
    states = [_tree_state(Path(top)) for top in sorted(tops)]
    if outside:
        states.append(f"{outside} documents outside any git repository")
    return f"documents' commit {'; '.join(states) or 'none: no document passed'}"


def report(groups: list[tuple[str, list[str], list[str]]], root: Path = REPO_ROOT) -> str:
    """The whole report for these (slug, plans, prompts) groups, as printed text.

    Units not yet ruled are printed in total only: a unit with no ledger row is routed to no
    system, so it cannot be counted under one.
    """
    entries, unreadable = inventory.listing(root)
    rows = ledger.load(root)[0]
    per = [(slug, _system_counts(slug, plans, prompts, entries, rows, root)) for slug, plans, prompts in groups]
    total = {key: sum(c[key] for _slug, c in per) for key in per[0][1]}  # groups is never empty: _parse refuses that
    lines = [_line(f"progress of {slug}", c) for slug, c in per] + [_line("progress in total", total)]
    enumerated = sum(len(extent.extents(n, extent._read(root, n))[0]) for n in extent.retained_files(root))
    not_ruled = sum(1 for f in ledger.check_1(root) if f.endswith(": no row"))
    lines.append(
        f"units: {enumerated} enumerated by the extent rule, {len(rows)} with a ledger row, {not_ruled} not yet ruled; "
        f"{_unreadable_count(unreadable)} tests whose family or guard decorator the inventory could not read"
    )
    lines.append(f"{_toolkit_state(root)}; {_documents_state(groups)}")
    return "\n".join(lines)


def _unreadable_count(unreadable: list[str]) -> int:
    """The tests behind the inventory's unreadable-decorator lines, counted by the plan-coverage check's own rule.

    The planned counts above cannot see such a test, so a unit with one can read as planned
    while one of its tests is adopted by nothing; the count says how many tests that is.
    """
    return plan_coverage.unreadable_tests(unreadable)


def unknown_groups(groups: list[tuple[str, list[str], list[str]]], root: Path = REPO_ROOT) -> list[str]:
    """Each `--group` name the system-scope file does not carry, in the order given.

    The names are read from the same file the per-system check and the collection-time refusal
    read theirs from, so the three never disagree about which systems exist.
    """
    known = set(system_scope.slugs(root))
    return [slug for slug, _plans, _prompts in groups if slug not in known]


def duplicate_systems(groups: list[tuple[str, list[str], list[str]]]) -> list[str]:
    """Name each system given as two or more `--group`s, whose counts would enter the total twice."""
    slugs = [slug for slug, _plans, _prompts in groups]
    return [f"{slug}: given as {slugs.count(slug)} groups" for slug in sorted(set(slugs)) if slugs.count(slug) > 1]


def main(argv: list[str] | None = None, root: Path = REPO_ROOT) -> int:
    """Print the report; return 2 on an unknown system name, 1 on a system or path given twice, 0 otherwise.

    The unknown-name check is a usage error and comes first, before any document is read; the two
    refusals come next, and only then is the report computed.
    """
    args = list(sys.argv[1:] if argv is None else argv)
    if args in (["-h"], ["--help"]):
        print(__doc__)
        return 0
    groups = _parse(args)
    unknown = unknown_groups(groups, root)
    if unknown:
        for slug in unknown:
            print(f"progress report: error: --group {slug!r} is not a system of the system-scope file", file=sys.stderr)
        return 2
    duplicates = duplicate_systems(groups) + duplicate_paths(groups)
    if duplicates:
        for line in duplicates:
            print(f"refused: {line}")
        return 1
    print(report(groups, root))
    return 0


if __name__ == "__main__":
    sys.exit(main())
