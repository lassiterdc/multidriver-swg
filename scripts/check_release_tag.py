#!/usr/bin/env python3
"""Refuse a release tag that is not a whole, complete, passing release of this repository.

The release procedure this gate enforces is four steps, in this order:

1. set `version` in `pyproject.toml` to the version being released;
2. set `version` in `CITATION.cff` to that same value, and set `date-released` to the release's date;
3. land that commit on `main`;
4. tag that commit on `main` with `v` followed by the version, and nothing else.

`version` is the one key of `CITATION.cff` this gate reads. `date-released` is written with it in the
same commit, because the tagged commit is what carries the release's date, and no refusal below reads
that key: the citation's other keys are set by the identifiers step, which runs after the tag exists.

Three facts only the first real release measures are owed after step 4, and none is a condition of any
landing: whether a Release created with `GITHUB_TOKEN` fires Zenodo's webhook, whether Software Heritage
archives a Zenodo record by itself, and whether the copy Zenodo archives omits the paths this repository
marks `export-ignore`, as the archive GitHub serves for the tag does.

The gate reads the TAGGED COMMIT and never the checkout, so every refusal is a fact about what a
release would ship. All five checks run on every invocation, so one invocation reports every ground
it finds: each refusal is one stdout line `refused: {id}: {detail}`, and the closing line reads
`release tag {tag} at {commit}: {n} refusal(s)` or `release tag {tag} at {commit}: conforms`. The
exit status is 0 when nothing was refused, 1 when anything was, and 2 on a usage error.

The five grounds:

- `tag-version`: the tag is not `v` plus `pyproject.toml`'s version. Both are compared as written,
  with no normalisation, so `v1.0` and `v1.0.0` are two different tags and only the one the
  procedure names passes.
- `citation-version`: `CITATION.cff` does not carry exactly one top-level `version:` line, or the
  value of that line is not the same version. A nested `version:` -- under `authors:`, say -- is not
  one of these and does not count either way.
- `not-on-main`: the commit is not an ancestor of the named main reference, so a release would ship
  a commit that main has never held.
- `legacy-tree`: the commit's tree lists a path under `_old_code_to_refactor/`, the unported legacy
  code that a release carries none of.
- `whole-suite-record`: the whole-suite result record at the commit does not read `verdict` as
  `pass`, `completeness` as `complete`, `exclusion` as `[]` and `invocation.collection_scope` as
  null. Each of those four is a way a run can be a pass over a subset: an incomplete run, a run
  narrowed after collection by a marker expression or a deselection, and a run narrowed by a path
  argument. A record that is absent, is not one JSON object, or lacks one of the four keys is
  refused with the missing key named.

That last ground reads four fields and no more, and its limit is stated here rather than implied: a
run narrowed BEFORE collection -- an `--ignore`, a `collect_ignore` -- shrinks the population and
leaves all four fields clean, so this gate cannot see it. The workflow that writes the record pins
its own command by equality in the test beside it, and that is where such a narrowing is refused.

Every refusal's detail names its remedy, because the reader of a refusal is whoever cut the tag, at
the moment it was refused, and a line that only says what is wrong leaves that reader to guess the
order of the four steps above.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path

VERSION_FILE = "pyproject.toml"
VERSION_TABLE = "project"
VERSION_KEY = "version"
CITATION_FILE = "CITATION.cff"
LEGACY_DIRECTORY = "_old_code_to_refactor/"

# The four facts a release's result record must report, held here as constants because this gate runs
# before any project environment: the record's own schema module imports `pytest`, so a gate that
# imported it would need the test environment on the release runner. A rename of any of them
# therefore reds the test that drives the real emitter, rather than silently loosening this gate.
RECORD_VERDICT = "verdict"
REQUIRED_VERDICT = "pass"
RECORD_COMPLETENESS = "completeness"
REQUIRED_COMPLETENESS = "complete"
RECORD_EXCLUSION = "exclusion"
REQUIRED_EXCLUSION: tuple[str, ...] = ()
RECORD_INVOCATION = "invocation"
RECORD_COLLECTION_SCOPE = "collection_scope"
REQUIRED_COLLECTION_SCOPE = None

# A top-level `version:` line of a citation file, with the value optionally double-quoted. Column 0
# only: a YAML mapping nested under another key is indented, and its `version` is a different field.
# At least one space or tab after the colon, because a YAML reader takes `version:"1.0.0"` as the
# start of a plain scalar rather than as a `version` key, so a pattern admitting it would count a
# line no consumer reads as the release's declared version.
_VERSION_LINE = re.compile(r"^version:[ \t]+(?P<value>.+?)[ \t]*$")


def _git(repo: Path, *arguments: str) -> subprocess.CompletedProcess:
    """One git invocation in `repo`, its failure left to the caller to interpret."""
    return subprocess.run(["git", *arguments], cwd=repo, capture_output=True, text=True, check=False)


def _reason(completed: subprocess.CompletedProcess) -> str:
    """The first line git wrote to stderr, or its exit status when it wrote none."""
    for line in completed.stderr.splitlines():
        if line.strip():
            return line.strip()
    return f"git exited {completed.returncode}"


def _project_version(repo: Path, commit: str) -> tuple[str | None, str]:
    """`pyproject.toml`'s version at `commit`, and the reason it could not be read when it is None."""
    shown = _git(repo, "show", f"{commit}:{VERSION_FILE}")
    if shown.returncode != 0:
        return None, f"{VERSION_FILE} could not be read at {commit} ({_reason(shown)})"
    try:
        version = tomllib.loads(shown.stdout)[VERSION_TABLE][VERSION_KEY]
    except (tomllib.TOMLDecodeError, KeyError, TypeError) as error:
        return None, f"{VERSION_FILE} at {commit} records no [{VERSION_TABLE}] {VERSION_KEY} ({error})"
    if not isinstance(version, str):
        return None, f"{VERSION_FILE} at {commit} records {VERSION_KEY} as {version!r}, which is not a string"
    return version, ""


def _citation_versions(text: str) -> list[str]:
    """Every top-level `version:` value in a citation file, one per matching line, unquoted."""
    values = []
    for line in text.splitlines():
        matched = _VERSION_LINE.match(line)
        if matched is None:
            continue
        value = matched.group("value")
        if len(value) > 1 and value[0] == value[-1] == '"':
            value = value[1:-1]
        values.append(value)
    return values


def _check_tag_version(tag: str, version: str | None, problem: str) -> str | None:
    """Refuse a tag that is not `v` plus the version, and name the tag that would pass."""
    if version is None:
        return f"{problem}; set [{VERSION_TABLE}] {VERSION_KEY} and tag the commit v{{version}}"
    expected = f"v{version}"
    if tag == expected:
        return None
    return (
        f"the tag is {tag!r} and this release's tag is {expected!r} "
        f"({VERSION_FILE} declares {version!r}, prefixed with v); retag as {expected!r} or correct the version"
    )


def _check_citation_version(repo: Path, commit: str, version: str | None, problem: str) -> str | None:
    """Refuse a citation file without exactly one top-level version line equal to the version."""
    shown = _git(repo, "show", f"{commit}:{CITATION_FILE}")
    if shown.returncode != 0:
        return (
            f"{CITATION_FILE} could not be read at {commit} ({_reason(shown)}); "
            f'add it with one top-level line version: "{{version}}"'
        )
    values = _citation_versions(shown.stdout)
    if len(values) != 1:
        return (
            f"{CITATION_FILE} at {commit} carries {len(values)} top-level version lines and needs exactly one; "
            f'add one line version: "{{version}}" at column 0 and delete any other'
        )
    if version is None:
        return (
            f"{CITATION_FILE} declares {values[0]!r} and {VERSION_FILE}'s version could not be read ({problem}); "
            f'once {VERSION_FILE} records one, set version: "{{version}}" in {CITATION_FILE}'
        )
    if values[0] != version:
        return (
            f"{CITATION_FILE} declares {values[0]!r} and {VERSION_FILE} declares {version!r}; "
            f'set version: "{version}" in {CITATION_FILE}'
        )
    return None


def _check_on_main(repo: Path, commit: str, main_ref: str) -> str | None:
    """Refuse a commit that `main_ref` does not hold, which a release must never ship."""
    ancestry = _git(repo, "merge-base", "--is-ancestor", commit, main_ref)
    if ancestry.returncode == 0:
        return None
    if ancestry.returncode == 1:
        return (
            f"{commit} is not an ancestor of {main_ref}; land it on {main_ref} and tag the commit {main_ref} then holds"
        )
    return (
        f"{main_ref} could not be resolved ({_reason(ancestry)}); fetch it and tag a commit that the main branch holds"
    )


def _check_legacy_tree(repo: Path, commit: str) -> str | None:
    """Refuse a commit whose tree still lists the unported legacy code."""
    listed = _git(repo, "ls-tree", "-r", "--name-only", commit, "--", LEGACY_DIRECTORY)
    if listed.returncode != 0:
        return f"the tree of {commit} could not be read ({_reason(listed)}); tag a commit this repository holds"
    paths = [line for line in listed.stdout.splitlines() if line.strip()]
    if not paths:
        return None
    return (
        f"{len(paths)} tracked files under {LEGACY_DIRECTORY}; remove that directory from the tagged commit, "
        f"which a release carries none of"
    )


def _shown(value) -> str:
    """A record value as JSON, so a string reads quoted and a null reads as null in a refusal."""
    return json.dumps(value)


def _read_record(path: Path) -> tuple[dict | None, str]:
    """The record at `path` as one mapping, and the reason it could not be read when it is None."""
    if not path.is_file():
        return None, f"no result record at {path}"
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        return None, f"the file at {path} is not one readable JSON object ({error})"
    if not isinstance(record, dict):
        return None, f"the file at {path} holds {_shown(record)} and not one JSON object"
    return record, ""


def _record_problems(record: dict) -> list[str]:
    """Each fact of the record that a release must not carry, one clause per field, naming the field."""
    problems: list[str] = []
    for key, required in (
        (RECORD_VERDICT, REQUIRED_VERDICT),
        (RECORD_COMPLETENESS, REQUIRED_COMPLETENESS),
    ):
        if key not in record:
            problems.append(f"{key} is missing, and the run must record {_shown(required)}")
        elif record[key] != required:
            problems.append(f"{key} reads {_shown(record[key])}, and the run must record {_shown(required)}")

    if RECORD_EXCLUSION not in record:
        problems.append(f"{RECORD_EXCLUSION} is missing, and the run must record no deselection")
    elif record[RECORD_EXCLUSION] != list(REQUIRED_EXCLUSION):
        problems.append(
            f"{RECORD_EXCLUSION} reads {_shown(record[RECORD_EXCLUSION])}, and the run must record no deselection"
        )

    scope = f"{RECORD_INVOCATION}.{RECORD_COLLECTION_SCOPE}"
    invocation = record.get(RECORD_INVOCATION)
    if not isinstance(invocation, dict) or RECORD_COLLECTION_SCOPE not in invocation:
        problems.append(f"{scope} is missing, and the run must record no path narrowing")
    elif invocation[RECORD_COLLECTION_SCOPE] != REQUIRED_COLLECTION_SCOPE:
        problems.append(
            f"{scope} reads {_shown(invocation[RECORD_COLLECTION_SCOPE])}, and the run must record no path narrowing"
        )
    return problems


def _check_record(record_path: Path, commit: str) -> str | None:
    """Refuse a whole-suite record that is not a whole, complete, passing run at the commit."""
    record, problem = _read_record(record_path)
    if record is None:
        return f"{problem}; keep the whole-suite run's artifact and point --record at it"
    problems = _record_problems(record)
    if not problems:
        return None
    return (
        "; ".join(problems)
        + f"; run the whole suite at {commit} with no marker expression, no deselection and no path argument, "
        "keep that run's record, and tag the commit it measured"
    )


def _parse_arguments(argv: list[str] | None) -> argparse.Namespace:
    """The gate's four required facts, and the repository to read them from."""
    parser = argparse.ArgumentParser(
        description="Refuse a release tag that is not a whole, complete, passing release of this repository."
    )
    parser.add_argument("--tag", required=True, help="the tag being pushed, for example v0.2.0.dev0")
    parser.add_argument("--commit", required=True, help="the commit that tag names")
    parser.add_argument("--main-ref", required=True, help="the reference the commit must be an ancestor of")
    parser.add_argument("--record", required=True, help="the whole-suite result record kept for that commit")
    parser.add_argument("--repo", default=".", help="the repository to read; the working directory by default")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run all five checks, print one line per refusal and one summary line, and return the exit status."""
    arguments = _parse_arguments(argv)
    repo = Path(arguments.repo)
    version, problem = _project_version(repo, arguments.commit)

    checks = (
        ("tag-version", _check_tag_version(arguments.tag, version, problem)),
        ("citation-version", _check_citation_version(repo, arguments.commit, version, problem)),
        ("not-on-main", _check_on_main(repo, arguments.commit, arguments.main_ref)),
        ("legacy-tree", _check_legacy_tree(repo, arguments.commit)),
        ("whole-suite-record", _check_record(Path(arguments.record), arguments.commit)),
    )
    refusals = [(name, detail) for name, detail in checks if detail is not None]

    for name, detail in refusals:
        print(f"refused: {name}: {detail}")
    summary = f"release tag {arguments.tag} at {arguments.commit}: "
    print(summary + (f"{len(refusals)} refusal(s)" if refusals else "conforms"))
    return 1 if refusals else 0


if __name__ == "__main__":
    sys.exit(main())
