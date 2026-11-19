"""Tests of the release-state comparison, and of the two facts this repository's tree must keep true.

The comparison classifies every entry of every byte set against the declared classes and refuses a
release that ships a path of an excluded or private class, or a path in two classes. The synthetic
cases below drive it as a subprocess, as the release workflow does, and read only its stdout and exit
status. The two real-tree cases at the end read this checkout: the source archive the commit produces
carries no entry under an export-ignore line, and no tracked path of a private class is an entry of
that archive. Both read `git archive` and not `git check-attr`, because `check-attr` reports a file
under a directory line as unspecified while `git archive` omits it, and the archive is the artifact a
reader of the tag receives.
"""

from __future__ import annotations

import fnmatch
import hashlib
import io
import json
import re
import subprocess
import sys
import tarfile
import tomllib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "compare_release_state.py"
TABLE = REPO_ROOT / "release_state.toml"
RECALL_SENTENCE = "recall direction (a class that should be declared and was not) is unmeasured"
REFUSAL_LINE = re.compile(r"^refused: (?P<name>[a-z-]+): (?P<detail>.+)$")

# The export-ignore paths whose absence from the source archive this repository's tree must keep
# true. The three guard-script lines are not here: the guard moves out of this repository, and its
# lines may go with it. These six name the classes whose size this checkout's tree fixes.
EXPECTED_IGNORED = (
    "/tests",
    "/.github",
    "/.pre-commit-config.yaml",
    "/_old_code_to_refactor",
    "/legacy_corpus_record.toml",
    "/_refactor_tools",
)


def _class_row(
    name: str,
    state: str,
    paths: list[str],
    *,
    system: str = "packaging-and-release",
    licence: str | None = "AGPL-3.0-or-later",
) -> str:
    """One `[[class]]` table as TOML text, with the licence a released class states."""
    lines = [f'name = "{name}"', f'system = "{system}"', f'state = "{state}"']
    if licence is not None:
        lines.append(f'licence = "{licence}"')
    lines.append("paths = [" + ", ".join(f'"{pattern}"' for pattern in paths) + "]")
    return "[[class]]\n" + "\n".join(lines) + "\n"


def _table(path: Path, *rows: str) -> Path:
    path.write_text("\n".join(rows), encoding="utf-8")
    return path


def _entry(set_name: str, path: str, data: bytes = b"x") -> dict:
    """One enumeration entry, with a digest and size of `data`, so a valid record is the default."""
    return {"set": set_name, "path": path, "sha256": hashlib.sha256(data).hexdigest(), "size": len(data)}


def _directory_entry(set_name: str, path: str) -> dict:
    """A directory member: a trailing slash, no bytes, no digest."""
    return {"set": set_name, "path": path.rstrip("/") + "/", "sha256": None, "size": 0}


def _enumeration(path: Path, entries: list[dict]) -> Path:
    path.write_text(json.dumps({"schema": 1, "entries": entries}), encoding="utf-8")
    return path


def _run(enumeration: Path, table: Path) -> subprocess.CompletedProcess:
    """One invocation of the comparison, in the form the release workflow gives it."""
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--enumeration", str(enumeration), "--table", str(table)],
        capture_output=True,
        text=True,
        check=False,
    )


def _refusals(result: subprocess.CompletedProcess) -> dict[str, str]:
    """The `refused: {id}: {detail}` lines of a run, as id -> detail."""
    found = {}
    for line in result.stdout.splitlines():
        matched = REFUSAL_LINE.match(line)
        if matched is not None:
            found[matched.group("name")] = matched.group("detail")
    return found


def _set_line(result: subprocess.CompletedProcess, set_name: str) -> str:
    """The one count line the run printed for `set_name`."""
    prefix = f"set {set_name}: "
    lines = [line for line in result.stdout.splitlines() if line.startswith(prefix)]
    assert len(lines) == 1, result.stdout
    return lines[0]


def test_a_fully_declared_release_conforms_and_every_set_is_counted(tmp_path):
    """Property: a release whose entries all fall in one released class conforms, and each set's counts are printed.

    This is the control every refusal test is read against: without it, a comparison that refused
    everything would satisfy each of them. The class count, one count line per set and the recall
    sentence are asserted whole, because a reader takes the release's state from the closing line and
    its coverage from the counts above it.

    Class: NEW CAPABILITY — the plausible wrong implementation refuses every entry in no class, so a
    release it has not fully accounted for exits 1; red under it, because this run must exit 0. The
    differently positioned input is the same enumeration with a private class added, which is the
    refusal arm of the next test.
    """
    entries = [
        _entry("sdist", "PKG-INFO"),
        _directory_entry("sdist", "sample"),
        _entry("wheel", "sample/__init__.py"),
        _entry("source-archive", ".gitattributes"),
    ]
    enumeration = _enumeration(tmp_path / "enumeration.json", entries)
    table = _table(
        tmp_path / "release_state.toml",
        _class_row("release-metadata", "released", ["PKG-INFO", ".gitattributes"]),
        _class_row("release-package", "released", ["sample/**", "sample/"]),
    )

    result = _run(enumeration, table)

    assert result.returncode == 0, result.stdout
    assert _refusals(result) == {}
    assert result.stdout.splitlines()[0] == "class count: 2"
    assert (
        _set_line(result, "sdist")
        == "set sdist: 2 entries; released 2; excluded-or-estate-resident 0; two-classes 0; unclassified 0"
    )
    assert (
        _set_line(result, "wheel")
        == "set wheel: 1 entries; released 1; excluded-or-estate-resident 0; two-classes 0; unclassified 0"
    )
    assert (
        _set_line(result, "source-archive")
        == "set source-archive: 1 entries; released 1; excluded-or-estate-resident 0; two-classes 0; unclassified 0"
    )
    assert RECALL_SENTENCE in result.stdout
    assert result.stdout.splitlines()[-1] == "release state comparison: conforms"


def test_a_path_in_an_excluded_class_is_refused_and_listed(tmp_path):
    """Property: an entry in a class the table declares excluded refuses the release, one line per path, naming set and class.

    The path is listed because whoever reads the refusal has to find it in an artifact, and the set
    is named because an export-ignore line keeps a path out of one archive and only a manifest prune
    keeps it out of the sdist.

    Class: NEW CAPABILITY — the plausible wrong implementation counts the entries of a private class
    and exits 0, printing no line per path; red under it, because no refusal is printed. The
    differently positioned satisfying input is the conforming enumeration above, whose one released
    class is named by the same matcher.
    """
    enumeration = _enumeration(
        tmp_path / "enumeration.json",
        [_entry("source-archive", "tests/conftest.py"), _entry("sdist", "PKG-INFO")],
    )
    table = _table(
        tmp_path / "release_state.toml",
        _class_row("release-metadata", "released", ["PKG-INFO"]),
        _class_row("tests", "excluded", ["tests/**"], system="private-estate", licence=None),
    )

    result = _run(enumeration, table)

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"excluded-shipped"}, sorted(refusals)
    assert "tests/conftest.py" in refusals["excluded-shipped"]
    assert "source-archive" in refusals["excluded-shipped"]
    assert "private-estate" in refusals["excluded-shipped"]
    assert "excluded-or-estate-resident 1" in _set_line(result, "source-archive")
    assert result.stdout.splitlines()[-1] == "release state comparison: 1 refusal(s)"


def test_a_path_in_an_estate_resident_class_is_refused_with_its_own_id(tmp_path):
    """Property: a class the table declares resident in the private estate refuses with `estate-resident-shipped`.

    The two private states need two ids because they need two fixes: an excluded class is a decision
    about the release, and an estate-resident one is a decision about where the bytes live. A single
    id would leave the reader with neither.

    Class: NEW CAPABILITY — the plausible wrong implementation maps both private states to one
    refusal id; red under it, because the id here is `estate-resident-shipped` and not
    `excluded-shipped`. The differently positioned satisfying input is the excluded arm above, which
    produces the other id from the same reader.
    """
    enumeration = _enumeration(
        tmp_path / "enumeration.json",
        [_entry("wheel", "multidriver_swg/divergence_ledger.toml")],
    )
    table = _table(
        tmp_path / "release_state.toml",
        _class_row("private-ledger", "estate-resident", ["multidriver_swg/*"], system="private-estate", licence=None),
    )

    result = _run(enumeration, table)

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"estate-resident-shipped"}, sorted(refusals)
    assert "multidriver_swg/divergence_ledger.toml" in refusals["estate-resident-shipped"]
    assert "estate-resident" in refusals["estate-resident-shipped"]


def test_a_path_in_two_classes_is_refused_and_both_are_named(tmp_path):
    """Property: an entry two classes match refuses the release, and the line names both declaring systems.

    Two classes can both be `released` and still be one class too many: the release would state two
    states for one path, and the licence a downloader is told would depend on a reader's order.

    Class: NEW CAPABILITY — the plausible wrong implementation takes the first matching class and
    stops, so a path in two released classes counts as released and the release passes; red under it,
    because this run exits 0. The differently positioned satisfying input is the conforming
    enumeration, where each path matches exactly one class.
    """
    enumeration = _enumeration(tmp_path / "enumeration.json", [_entry("sdist", "PKG-INFO")])
    table = _table(
        tmp_path / "release_state.toml",
        _class_row("release-metadata", "released", ["PKG-INFO"]),
        _class_row("release-record", "released", ["*"], system="documentation-and-examples"),
    )

    result = _run(enumeration, table)

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"two-classes"}, sorted(refusals)
    assert "release-metadata" in refusals["two-classes"]
    assert "release-record" in refusals["two-classes"]
    assert "documentation-and-examples" in refusals["two-classes"]
    assert "two-classes 1" in _set_line(result, "sdist")


def test_a_path_in_no_class_passes_and_the_first_twenty_are_printed(tmp_path):
    """Property: entries in no class pass with their count printed, and only the first 20 of them are listed.

    A class a system has not declared yet is a gap its owner closes, so the comparison passes and
    says how much of the release it could not account for. The listing is capped because it is a
    pointer to the artifacts and not a second copy of them.

    Class: NEW CAPABILITY — the plausible wrong implementation either refuses an unclassified entry
    (exit 1) or prints every unclassified path; red under it, because the run exits 1, or the listing
    holds 25 paths rather than 20. The differently positioned satisfying input is the conforming
    enumeration, whose unclassified count is zero.
    """
    entries = [_entry("source-archive", f"docs/page-{index:02d}.md") for index in range(25)]
    entries.append(_entry("source-archive", "scripts/enumerate_release.py"))
    enumeration = _enumeration(tmp_path / "enumeration.json", entries)
    table = _table(tmp_path / "release_state.toml", _class_row("release-scripts", "released", ["scripts/*"]))

    result = _run(enumeration, table)

    assert result.returncode == 0, result.stdout
    assert _refusals(result) == {}
    assert "unclassified 25" in _set_line(result, "source-archive")
    listed = [line for line in result.stdout.splitlines() if line.strip().startswith("not in any class")]
    assert len(listed) == 1, result.stdout
    assert listed[0].startswith("  not in any class (first 20): ")
    assert len(listed[0].split("): ", 1)[1].split(", ")) == 20
    assert result.stdout.splitlines()[-1] == "release state comparison: conforms"


@pytest.mark.parametrize(
    ("enumeration_text", "table_text", "named"),
    [
        pytest.param(None, _class_row("metadata", "released", ["PKG-INFO"]), "enumeration", id="no-enumeration"),
        pytest.param(
            json.dumps({"schema": 2, "entries": []}),
            _class_row("metadata", "released", ["PKG-INFO"]),
            "schema",
            id="another-schema",
        ),
        pytest.param(
            json.dumps({"schema": 1, "entries": [{"set": "sdist", "path": "PKG-INFO"}]}),
            _class_row("metadata", "released", ["PKG-INFO"]),
            "sha256",
            id="entry-without-its-digest",
        ),
        pytest.param(
            json.dumps({"schema": 1, "entries": [{"set": "sdist", "path": "PKG-INFO", "sha256": None, "size": 0}]}),
            "[[class]\n",
            "TOML",
            id="table-not-toml",
        ),
        pytest.param(
            json.dumps({"schema": 1, "entries": []}),
            _class_row("metadata", "released", ["PKG-INFO"], licence=None),
            "licence",
            id="released-class-without-a-licence",
        ),
        pytest.param(
            json.dumps({"schema": 1, "entries": []}),
            _class_row("metadata", "shipped", ["PKG-INFO"]),
            "state",
            id="unknown-state",
        ),
        pytest.param(json.dumps({"schema": 1, "entries": []}), "", "class", id="table-with-no-class"),
    ],
)
def test_unreadable_input_exits_two_and_names_the_fact(tmp_path, enumeration_text, table_text, named):
    """Property: a table or enumeration the comparison cannot read is one `unreadable-input` line and exit 2.

    Exit 2 is the status a broken invocation takes, and it must not read as a verdict about the
    release: nothing was classified, so the comparison has nothing to say about what would ship. A
    released class with no licence is here because the licence check reads that field, and a class in
    an unknown state is here because no count could place its entries.

    Class: NEW CAPABILITY — the plausible wrong implementation reads a table with an unknown state as
    unclassified and exits 0, or raises on a table that is not TOML; red under it, because the run
    exits 0 or prints a traceback in place of the refusal line. The differently positioned satisfying
    input is the conforming table above, which is read and classified.
    """
    table = tmp_path / "release_state.toml"
    table.write_text(table_text, encoding="utf-8")
    enumeration = tmp_path / "enumeration.json"
    if enumeration_text is not None:
        enumeration.write_text(enumeration_text, encoding="utf-8")

    result = _run(enumeration, table)

    assert result.returncode == 2, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"unreadable-input"}, sorted(refusals)
    assert named in refusals["unreadable-input"]
    assert "conforms" not in result.stdout


def _committed_attributes() -> str:
    """`.gitattributes` as the archived commit holds it, which is the copy `git archive` reads."""
    completed = subprocess.run(
        ["git", "show", "HEAD:.gitattributes"], cwd=REPO_ROOT, capture_output=True, text=True, check=False
    )
    assert completed.returncode == 0, completed.stderr
    return completed.stdout


def _archive_entries() -> list[str]:
    """Every member of `git archive --format=tar HEAD`, directory entries included, with a trailing slash."""
    completed = subprocess.run(
        ["git", "archive", "--format=tar", "HEAD"], cwd=REPO_ROOT, capture_output=True, check=False
    )
    assert completed.returncode == 0, completed.stderr
    with tarfile.open(fileobj=io.BytesIO(completed.stdout), mode="r:") as archive:
        return [member.name.rstrip("/") + "/" if member.isdir() else member.name for member in archive.getmembers()]


def _export_ignored(attributes: str) -> list[str]:
    """Every pattern the attribute file marks `export-ignore`, in file order."""
    patterns = []
    for line in attributes.splitlines():
        fields = line.split()
        if not fields or fields[0].startswith("#"):
            continue
        if "export-ignore" in fields[1:]:
            patterns.append(fields[0])
    return patterns


def _covers(pattern: str, entry: str) -> bool:
    """Whether an export-ignore pattern keeps `entry` out: the named path itself, or anything under it."""
    root = pattern.lstrip("/")
    return fnmatch.fnmatchcase(entry.rstrip("/"), root) or fnmatch.fnmatchcase(entry, f"{root}/**")


def test_the_source_archive_carries_no_entry_under_an_export_ignored_path():
    """Property: no member of the committed tree's source archive is the path an export-ignore line names, or under it.

    Directory entries are included, because the glob form of these lines leaves the empty directory
    behind and a test that read files only would pass over it. The archive is the artifact a
    downloader of the tag receives, so it is read directly and not through `git check-attr`, which
    reports a file under a directory line as unspecified.

    Class: NEW CAPABILITY — the plausible wrong state is an attribute file that writes the glob form
    or omits the line; red under it, because the archive then carries `tests/` and every file under
    it. The differently positioned input is the same archive read through the attribute file this
    tree commits, which carries none of them.
    """
    patterns = _export_ignored(_committed_attributes())
    assert set(EXPECTED_IGNORED).issubset(set(patterns)), sorted(set(EXPECTED_IGNORED) - set(patterns))

    entries = _archive_entries()
    assert entries, "the archive holds no member, so the absence below measures nothing"
    for pattern in patterns:
        offenders = [entry for entry in entries if _covers(pattern, entry)]
        assert offenders == [], (pattern, offenders)


def test_no_tracked_path_of_a_private_class_is_an_entry_of_the_source_archive():
    """Property: a tracked path a table row declares excluded or estate-resident is not a member of the source archive.

    The control below is load-bearing: this repository's table declares no private class yet, so the
    loop over private rows can pass while the reader is broken. The same matcher over a released row
    must match a member of the same archive, which proves the table is read and the matcher matches.

    Class: NEW CAPABILITY — the plausible wrong state is a private row whose glob does not cover a
    path its system declares (a `tests/*` written where `tests/**` was meant); red under it, because
    the tracked path under `tests/` is then an archive entry. The differently positioned satisfying
    input is the released-row control below, which matches the archive member `.gitattributes`.
    """
    document = tomllib.loads(TABLE.read_text(encoding="utf-8"))
    rows = document["class"]
    tracked = subprocess.run(["git", "ls-files"], cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    assert tracked.returncode == 0, tracked.stderr
    paths = [line for line in tracked.stdout.splitlines() if line]
    entries = {entry.rstrip("/") for entry in _archive_entries()}

    def matched(row: dict) -> set[str]:
        return {path for path in paths if any(fnmatch.fnmatchcase(path, glob) for glob in row["paths"])}

    released = {path for row in rows if row["state"] == "released" for path in matched(row)}
    assert released & entries, "no released row matches a member of the archive, so the matcher measures nothing"

    for row in rows:
        if row["state"] in ("excluded", "estate-resident"):
            assert matched(row) & entries == set(), row["name"]
