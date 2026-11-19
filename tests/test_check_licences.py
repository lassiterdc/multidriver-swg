"""Tests of the licence check: four statements a release makes, and the tree's own three declarations.

The check compares the licence a release states on four surfaces — the built distributions' core
metadata, the licence file each byte set carries, the README that becomes their long description, and
`CITATION.cff` — against the release-state table, and refuses a release whose statements disagree. Each
test below drives it as a subprocess, as the release workflow will, and reads only its stdout and exit
status. The artifacts are built here rather than by a build backend: what the check must get right is
the comparison, and a real build would make every test measure the backend.

The real-tree tests at the end read this checkout. One holds the three declarations of this repository
equal — `pyproject.toml`'s `license`, `CITATION.cff`'s `license` and the table's source class — because
those three are written by hand in three files and nothing else compares them. The other runs the
check over this checkout's real README, real `CITATION.cff` and real table, with artifacts built here,
which is the arm that proves the README's legacy-corpus sentences are read as statements about that
corpus and not about this distribution.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import subprocess
import sys
import tarfile
import tomllib
import zipfile
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "check_licences.py"
COMPARISON = REPO_ROOT / "scripts" / "compare_release_state.py"
TABLE = REPO_ROOT / "release_state.toml"
README = REPO_ROOT / "README.md"
CITATION = REPO_ROOT / "CITATION.cff"
LICENCE = REPO_ROOT / "LICENSE"
REFUSAL_LINE = re.compile(r"^refused: (?P<name>[a-z-]+): (?P<detail>.+)$")

SOURCE_LICENCE = "AGPL-3.0-or-later"
SDIST_TOP = "sample-1.0.0"
WHEEL_FILE = "sample-1.0.0-py3-none-any.whl"
DIST_INFO = "sample-1.0.0.dist-info"
WHEEL_LICENCE = f"{DIST_INFO}/licenses/LICENSE"
AGPL_TEXT = (
    "                    GNU AFFERO GENERAL PUBLIC LICENSE\n                       Version 3, 19 November 2007\n"
)
MIT_TEXT = "MIT License\n\nPermission is hereby granted, free of charge, to any person obtaining a copy\n"

SAMPLE_README = """# Sample

A sample distribution.

## Installation

```bash
pip install sample
```

## Licence

The sample is licensed under the GNU Affero General Public License, version 3 or
any later version (AGPL-3.0-or-later; `LICENSE`).
That licence does not cover `_old_code_to_refactor/`, which is MIT-licensed
(`_old_code_to_refactor/LICENSE`).
"""

SAMPLE_CITATION = "cff-version: 1.2.0\nlicense: AGPL-3.0-or-later\n"


def _class_row(
    name: str,
    paths: list[str],
    *,
    state: str = "released",
    system: str = "packaging-and-release",
    licence: str | None = SOURCE_LICENCE,
) -> str:
    """One `[[class]]` table as TOML text, with the licence a released class states."""
    lines = [f'name = "{name}"', f'system = "{system}"', f'state = "{state}"']
    if licence is not None:
        lines.append(f'licence = "{licence}"')
    lines.append("paths = [" + ", ".join(f'"{pattern}"' for pattern in paths) + "]")
    return "[[class]]\n" + "\n".join(lines) + "\n"


def _table_text() -> str:
    """The baseline table: the metadata class the check reads, and a released class of package files."""
    return (
        _class_row("release-metadata", ["PKG-INFO", "pyproject.toml", f"{DIST_INFO}/METADATA"])
        + "\n"
        + _class_row("release-package", ["sample/**"])
    )


def _header(expression: str | None) -> bytes:
    """One core metadata header, with `License-Expression` present or omitted, and a description after it."""
    lines = ["Metadata-Version: 2.4", "Name: sample", "Version: 1.0.0"]
    if expression is not None:
        lines.append(f"License-Expression: {expression}")
    return ("\n".join(lines) + "\n\nA sample distribution.\n").encode()


def _sdist(path: Path, members: dict[str, bytes]) -> Path:
    """A gzip tar holding `members` under one top directory, named on no attribute the check reads."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(path, "w:gz") as archive:
        info = tarfile.TarInfo(SDIST_TOP)
        info.type = tarfile.DIRTYPE
        info.mode = 0o755
        archive.addfile(info)
        for name, data in sorted(members.items()):
            member = tarfile.TarInfo(f"{SDIST_TOP}/{name}")
            member.size = len(data)
            member.mode = 0o644
            archive.addfile(member, io.BytesIO(data))
    return path


def _wheel(path: Path, members: dict[str, bytes]) -> Path:
    """A zip holding `members` under one distribution directory."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        for name, data in sorted(members.items()):
            archive.writestr(name, data)
    return path


def _entry(set_name: str, path: str, data: bytes = b"x") -> dict:
    """One enumeration entry, with a digest and size of `data`, so a valid record is the default."""
    return {"set": set_name, "path": path, "sha256": hashlib.sha256(data).hexdigest(), "size": len(data)}


def _entries(
    *,
    sdist_licence: bytes = AGPL_TEXT.encode(),
    wheel_licence: bytes = AGPL_TEXT.encode(),
    archive_licence: bytes | None = None,
    dropped: tuple[tuple[str, str], ...] = (),
) -> list[dict]:
    """The baseline enumeration's entries, with the licence bytes each set's covering entry records."""
    archived = sdist_licence if archive_licence is None else archive_licence
    rows = [
        _entry("sdist", "PKG-INFO"),
        _entry("sdist", "LICENSE", sdist_licence),
        _entry("sdist", "pyproject.toml"),
        _entry("wheel", f"{DIST_INFO}/METADATA"),
        _entry("wheel", WHEEL_LICENCE, wheel_licence),
        _entry("wheel", "sample/__init__.py"),
        _entry("source-archive", "LICENSE", archived),
        _entry("source-archive", "pyproject.toml"),
    ]
    return [row for row in rows if (row["set"], row["path"]) not in dropped]


def _enumeration(path: Path, entries: list[dict], *, released: bool = False) -> Path:
    """The enumeration's document, naming the release it records when `released` is set."""
    document: dict[str, object] = {"schema": 1, "entries": entries}
    if released:
        document = {"schema": 1, "tag": "v1.0.0", "commit": "0123456789abcdef", "entries": entries}
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


@dataclass(frozen=True)
class Release:
    """One synthetic release's four inputs, and the invocation the workflow gives the check."""

    dist: Path
    enumeration: Path
    table: Path
    readme: Path
    citation: Path

    def run(self) -> subprocess.CompletedProcess:
        """One invocation of the check, in the form the release workflow will give it."""
        return subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--enumeration",
                str(self.enumeration),
                "--table",
                str(self.table),
                "--dist",
                str(self.dist),
                "--readme",
                str(self.readme),
                "--citation",
                str(self.citation),
            ],
            capture_output=True,
            text=True,
            check=False,
        )


def _release(
    tmp_path: Path,
    *,
    sdist_expression: str | None = SOURCE_LICENCE,
    wheel_expression: str | None = SOURCE_LICENCE,
    sdist_licence: bytes = AGPL_TEXT.encode(),
    wheel_licence: bytes = AGPL_TEXT.encode(),
    archive_licence: bytes | None = None,
    readme: str = SAMPLE_README,
    citation: str = SAMPLE_CITATION,
    table_text: str | None = None,
    dropped: tuple[tuple[str, str], ...] = (),
    released: bool = False,
) -> Release:
    """A release whose four statements agree, with only the arm a test names moved."""
    dist = tmp_path / "dist"
    _sdist(
        dist / f"{SDIST_TOP}.tar.gz",
        {"PKG-INFO": _header(sdist_expression), "LICENSE": sdist_licence, "pyproject.toml": b"[project]\n"},
    )
    _wheel(
        dist / WHEEL_FILE,
        {
            f"{DIST_INFO}/METADATA": _header(wheel_expression),
            WHEEL_LICENCE: wheel_licence,
            "sample/__init__.py": b"",
        },
    )
    entries = _entries(
        sdist_licence=sdist_licence, wheel_licence=wheel_licence, archive_licence=archive_licence, dropped=dropped
    )
    table = tmp_path / "release_state.toml"
    table.write_text(_table_text() if table_text is None else table_text, encoding="utf-8")
    readme_path = tmp_path / "README.md"
    readme_path.write_text(readme, encoding="utf-8")
    citation_path = tmp_path / "CITATION.cff"
    citation_path.write_text(citation, encoding="utf-8")
    return Release(
        dist, _enumeration(tmp_path / "enumeration.json", entries, released=released), table, readme_path, citation_path
    )


def _refusals(result: subprocess.CompletedProcess) -> dict[str, str]:
    """The `refused: {id}: {detail}` lines of a run, as id -> detail, one per id."""
    found: dict[str, str] = {}
    for line in result.stdout.splitlines():
        matched = REFUSAL_LINE.match(line)
        if matched is not None:
            found[matched.group("name")] = matched.group("detail")
    return found


def _refusal_count(result: subprocess.CompletedProcess) -> int:
    """Every `refused:` line of a run, counted, because one id can refuse twice."""
    return sum(1 for line in result.stdout.splitlines() if REFUSAL_LINE.match(line) is not None)


def test_the_four_statements_agree_and_the_release_conforms(tmp_path):
    """Property: a release whose metadata, covering file, README and citation all state the table's licence exits 0.

    This is the control every refusal below is read against: without it, a check that refused every
    release would satisfy each of them. The release the enumeration names is printed first, because a
    reader of a refused run has to know which release the run was about.

    Class: NEW CAPABILITY — the plausible wrong implementation refuses a release whose covering file is
    absent from a set that ships no released entry, or reads the whole README and refuses the legacy
    corpus's MIT sentence; red under it, because this run must exit 0. The differently positioned
    satisfying input is the same release with one arm moved, which each test below supplies.
    """
    release = _release(tmp_path, released=True)

    result = release.run()

    assert result.returncode == 0, result.stdout
    assert _refusals(result) == {}
    assert result.stdout.splitlines()[0] == "licence check for v1.0.0 at 0123456789abcdef"
    assert "metadata class release-metadata states AGPL-3.0-or-later" in result.stdout
    assert "set sdist License-Expression: AGPL-3.0-or-later" in result.stdout
    assert f"set wheel: 3 entries, 2 released class(es), covering file {WHEEL_LICENCE} present" in result.stdout
    assert "set sdist: 3 entries, 1 released class(es), covering file LICENSE present" in result.stdout
    assert "readme: ['agpl'] for this distribution" in result.stdout
    assert "citation: license AGPL-3.0-or-later" in result.stdout
    assert result.stdout.splitlines()[-1] == "licence check: conforms"


def test_a_distribution_whose_metadata_states_another_licence_is_refused(tmp_path):
    """Property: a wheel whose `License-Expression` differs from the table's source licence is refused, naming both.

    The metadata is what a downloader's tooling reports as the licence, so an artifact that states one
    licence while the table states another is refused rather than reconciled: one of the two is wrong
    and only the release's author knows which.

    Class: NEW CAPABILITY — the plausible wrong implementation compares the sdist only, because it is
    the artifact a source reader takes; red under it, because this run moves the wheel's field and must
    exit 1. The differently positioned satisfying input is the control, whose wheel states the table's
    licence.
    """
    release = _release(tmp_path, wheel_expression="MIT")

    result = release.run()

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"source-licence"}, sorted(refusals)
    assert WHEEL_FILE in refusals["source-licence"]
    assert "'MIT'" in refusals["source-licence"]
    assert "release-metadata" in refusals["source-licence"]
    assert "set wheel License-Expression: MIT" in result.stdout
    assert result.stdout.splitlines()[-1] == "licence check: 1 refusal(s)"


def test_a_distribution_carrying_no_licence_expression_is_refused(tmp_path):
    """Property: an artifact whose core metadata carries no `License-Expression` is refused as a difference.

    A missing field is not a licence that agrees: PEP 639 makes `License-Expression` the machine-readable
    statement, and a release whose metadata omits it states nothing a consumer can read.

    Class: NEW CAPABILITY — the plausible wrong implementation treats an absent field as equal to an
    absent table licence and passes; red under it, because this run must exit 1. The differently
    positioned satisfying input is the control, whose sdist carries the field.
    """
    release = _release(tmp_path, sdist_expression=None)

    result = release.run()

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"source-licence"}, sorted(refusals)
    assert "carries no License-Expression" in refusals["source-licence"]
    assert "set sdist License-Expression: None" in result.stdout


def test_a_set_without_its_covering_file_is_refused(tmp_path):
    """Property: a set holding entries of a released class and no covering licence file is refused, naming the file.

    The covering file is what a consumer of that byte set is given as the licence, so a set that ships
    released bytes without it ships them under a statement it does not carry.

    Class: NEW CAPABILITY — the plausible wrong implementation checks that the file exists on disk
    instead of in the set, so a wheel built without the licence passes; red under it, because this
    run's wheel holds a released entry and no licence member. The differently positioned satisfying
    input is the control, whose every set holds its covering file.
    """
    release = _release(tmp_path, dropped=(("wheel", WHEEL_LICENCE),))

    result = release.run()

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"class-licence"}, sorted(refusals)
    assert WHEEL_LICENCE in refusals["class-licence"]
    assert "set wheel holds" in refusals["class-licence"]


def test_a_covering_file_stating_another_licence_is_refused(tmp_path):
    """Property: a set's covering licence file whose text states another licence is refused, with both families printed.

    The file's name is not its content: a `LICENSE` copied from another project states that project's
    licence, and the release would hand a consumer the wrong terms under the right file name.

    Class: NEW CAPABILITY — the plausible wrong implementation treats the file's presence as the
    statement and passes; red under it, because this run's wheel ships an MIT text against an
    AGPL-3.0-or-later class. The differently positioned satisfying input is the control, whose covering
    files carry the AGPL text.
    """
    release = _release(tmp_path, wheel_licence=MIT_TEXT.encode())

    result = release.run()

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"class-licence"}, sorted(refusals)
    assert WHEEL_LICENCE in refusals["class-licence"]
    assert "states 'mit'" in refusals["class-licence"]
    assert "'AGPL-3.0-or-later'" in refusals["class-licence"]


def test_a_source_archive_whose_licence_differs_from_the_distributions_is_refused(tmp_path):
    """Property: the source archive's root licence must be the bytes the built distributions carry.

    The source archive is not an input — it is `git archive` of the release commit — so the check reads
    the enumeration's digest for it and requires it equal to the built copy's. A difference means the
    distributions were built from another commit, which is a release whose own artifacts disagree.

    Class: NEW CAPABILITY — the plausible wrong implementation checks the source archive's covering
    entry for presence only, because it cannot read the bytes; red under it, because this run's archive
    entry carries a different digest and no refusal would print. The differently positioned satisfying
    input is the control, whose archive entry is derived from the sdist's own bytes.
    """
    release = _release(tmp_path, archive_licence=b"a source archive's own licence\n")

    result = release.run()

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"class-licence"}, sorted(refusals)
    assert "set source-archive 'LICENSE'" in refusals["class-licence"]
    assert "same licence file" in refusals["class-licence"]


def test_a_table_licence_no_marker_names_is_refused_rather_than_passed(tmp_path):
    """Property: a released class whose licence expression no declared marker names refuses, instead of comparing nothing.

    A licence vocabulary is closed, and an expression outside it is a fact the check cannot read. Passing
    it would report that the covering file agrees with a licence nobody compared, which is the failure
    mode a silence produces and no error message does.

    Class: NEW CAPABILITY — the plausible wrong implementation compares the raw strings and passes a
    covering file it never matched, or raises on an unknown value; red under it, because this run must
    exit 1 with a named refusal. The differently positioned satisfying input is the control, whose
    expression `AGPL-3.0-or-later` has a marker.
    """
    table_text = _class_row("release-metadata", ["PKG-INFO", "pyproject.toml"], licence="LicenseRef-Proprietary")
    release = _release(
        tmp_path,
        sdist_expression="LicenseRef-Proprietary",
        wheel_expression="LicenseRef-Proprietary",
        citation="cff-version: 1.2.0\nlicense: LicenseRef-Proprietary\n",
        readme="# Sample\n\nA sample distribution, whose licence this README does not state.\n",
        table_text=table_text,
    )

    result = release.run()

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"class-licence"}, sorted(refusals)
    assert "no licence marker is declared" in refusals["class-licence"]
    assert "'LicenseRef-Proprietary'" in refusals["class-licence"]


def test_a_readme_stating_a_licence_the_release_does_not_carry_is_refused(tmp_path):
    """Property: a licence section that states this distribution's licence as another one is refused.

    The README is the distributions' long description, so the licence it states for the software is
    what a reader of the package page is told. A statement that disagrees with the metadata is the
    published misstatement the release is repaired for only by withdrawing it.

    Class: NEW CAPABILITY — the plausible wrong implementation reads the metadata's own expression out
    of the README and passes anything else, or reads no README at all; red under it, because this run's
    licence section states MIT and no refusal would print. The differently positioned satisfying input
    is the control, whose licence section states the table's licence.
    """
    readme = "# Sample\n\n## Licence\n\nThe sample is licensed under the MIT licence.\n"
    release = _release(tmp_path, readme=readme)

    result = release.run()

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"readme-licence"}, sorted(refusals)
    assert "states 'mit'" in refusals["readme-licence"]
    assert "AGPL-3.0-or-later" in refusals["readme-licence"]
    assert "The sample is licensed under the MIT licence." in refusals["readme-licence"]


@pytest.mark.parametrize(
    "statement",
    [
        pytest.param("The sample is licensed under the AGPL.", id="bare-family-name"),
        pytest.param("The sample is AGPLv3 licensed.", id="unlisted-spelling"),
    ],
)
def test_a_readme_naming_the_licence_by_its_family_name_conforms(tmp_path, statement):
    """Property: a README naming the licence by its family, or by a spelling the SPDX list carries nowhere, conforms.

    G12 Q3's ruling, so ordinary README wording is accepted and the refusal stays for a different family.
    `AGPL` is the family's name and `AGPLv3` is a spelling the SPDX list does not carry; neither is an
    identifier the list has, so neither says anything the family does not, and both conform beside
    `AGPL-3.0-or-later` metadata. The cost the ruling accepts, stated in the module docstring rather
    than hidden: a bare family name does not say which version, so the first sentence would also conform
    beside metadata stating a different AGPL version.

    Class: NEW CAPABILITY — the plausible wrong implementation reads every whitespace-free mention as an
    identifier, which is what `_identifiers` does for the declared licence; red under it, because this
    run must exit 0 and it refuses `'agpl'` and `'agplv3'`. The differently positioned satisfying input
    is the refusal arm below, whose sentence names a listed identifier of the same family.

    Shape row: DR-10's README comparison (gate G12 Q3) — "A bare family name, or a spelling no SPDX list
    carries, is compared by family."
    """
    readme = f"# Sample\n\n## Licence\n\n{statement}\n"
    release = _release(tmp_path, readme=readme)

    result = release.run()

    assert result.returncode == 0, result.stdout
    assert _refusals(result) == {}
    assert "readme: ['agpl'] for this distribution" in result.stdout


def test_a_readme_naming_another_family_by_its_family_name_is_refused(tmp_path):
    """Property: comparing by family still refuses a statement naming a family the release does not carry.

    The family comparison is the whole comparison for a statement naming no listed identifier, so it has
    to refuse a different family or the concession above would accept any licence word at all. `MIT` is a
    listed identifier, refused by `test_a_readme_stating_a_licence_the_release_does_not_carry_is_refused`;
    `GPL` is a bare family name, so this arm reaches the family comparison through prose rather than
    through an identifier.

    Class: NEW CAPABILITY — the plausible wrong implementation accepts any mention whose family it can
    read, or skips every statement it cannot parse; red under it, because this run must exit 1. The
    differently positioned satisfying input is the conforming arm above, a family name the release does
    carry.
    """
    readme = "# Sample\n\n## Licence\n\nThe sample is licensed under the GPL.\n"
    release = _release(tmp_path, readme=readme)

    result = release.run()

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"readme-licence"}, sorted(refusals)
    assert "states 'gpl'" in refusals["readme-licence"]


def test_a_readme_naming_a_listed_identifier_of_another_version_is_refused(tmp_path):
    """Property: a listed SPDX identifier of the same family, differing in its version, is still refused.

    `AGPL-3.0-only` and `AGPL-3.0-or-later` are different licences to a user — the second lets them take
    a later version — so a README stating the first beside metadata stating the second misstates the
    release on its own package page. The family comparison added above must not swallow this arm: it
    applies only to a spelling the SPDX list carries nowhere, and this one is on the list.

    Class: NEW CAPABILITY — the plausible wrong implementation compares every mention by family, which
    passes both arms of `test_a_readme_naming_the_licence_by_its_family_name_conforms` and this one; red
    under it, because this run must exit 1 and the detail names the identifier rather than the family.
    The differently positioned satisfying input is the conforming arm above, a family name that is not
    an identifier.

    Shape row: DR-10's `readme-licence` — "when the README names a licence expression the metadata does
    not carry" — read with the ruling that a listed identifier is compared as that identifier.
    """
    readme = "# Sample\n\n## Licence\n\nThe sample is licensed under the AGPL-3.0-only licence.\n"
    release = _release(tmp_path, readme=readme)

    result = release.run()

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"readme-licence"}, sorted(refusals)
    assert "states 'agpl-3.0-only'" in refusals["readme-licence"]


def test_a_readme_sentence_scoped_to_a_path_is_not_the_distributions_statement(tmp_path):
    """Property: a licence sentence naming a repository path states that path's licence, and is not compared.

    This is the arm that lets this repository's own README pass: its legacy corpus is MIT-licensed, that
    corpus ships in no byte set, and the sentence saying so is a separate statement the release makes
    deliberately. Reading it as the distribution's licence would refuse every release forever.

    Class: NEW CAPABILITY — the plausible wrong implementation scans the whole licence section for
    licence names; red under it, because this run's only licence sentence is scoped to a path and the
    run must exit 0. The differently positioned satisfying input is the refusal arm above, whose
    sentence names no path.
    """
    readme = (
        "# Sample\n\n## Licence\n\n"
        "The sample is licensed under the GNU Affero General Public License (AGPL-3.0-or-later; `LICENSE`).\n"
        "That licence does not cover `_old_code_to_refactor/`, which is MIT-licensed.\n"
    )
    release = _release(tmp_path, readme=readme)

    result = release.run()

    assert result.returncode == 0, result.stdout
    assert _refusals(result) == {}
    assert "readme: ['agpl'] for this distribution" in result.stdout


def test_a_distribution_statement_carrying_an_incidental_path_is_not_compared(tmp_path):
    """Property: a statement about the distribution that carries a backticked path is exempted with it, as a stated residual.

    The path test is per statement, not per phrase, so a distribution statement that cites a path for
    some other reason exempts itself: here `docs/licence.md` is cited in the same sentence, and the MIT
    mention beside it goes unread. G12 Q4 ruled this exemption kept and its cost stated in the module
    docstring rather than closed, because closing it would mean deciding which path a mention is about,
    and no reading in this check can. This test holds the ruling's `keep` side, so a later stricter
    reading is a deliberate change rather than an accident.

    Class: NEW CAPABILITY — the plausible wrong implementation exempts only the path-scoped sentence and
    refuses any statement carrying a path; red under it, because this run must exit 0 and print that no
    licence was compared. The differently positioned satisfying input is the refusal arm above, whose
    sentence carries no path.
    """
    readme = "# Sample\n\n## Licence\n\nThe sample is licensed under the MIT licence (see `docs/licence.md`).\n"
    release = _release(tmp_path, readme=readme)

    result = release.run()

    assert result.returncode == 0, result.stdout
    assert _refusals(result) == {}
    assert "readme: no licence named for this distribution" in result.stdout


def test_a_readme_with_no_licence_section_is_read_whole(tmp_path):
    """Property: a README carrying no heading that names a licence is read whole, so a bare statement still refuses.

    The licence section is where a README states what governs this software, so it is the section the
    check reads. A README with no such section would otherwise escape the comparison entirely, which
    would make the check's scope a way to switch it off.

    Class: NEW CAPABILITY — the plausible wrong implementation returns early when no licence heading is
    found; red under it, because this run's body states MIT and must be refused. The differently
    positioned satisfying input is the control, whose statement sits under a licence heading.
    """
    readme = "# Sample\n\nA sample distribution, licensed under the MIT licence.\n"
    release = _release(tmp_path, readme=readme)

    result = release.run()

    assert result.returncode == 1, result.stdout
    assert set(_refusals(result)) == {"readme-licence"}, sorted(_refusals(result))


def test_a_citation_stating_another_licence_is_refused(tmp_path):
    """Property: `CITATION.cff`'s `license` differing from the table's source licence refuses the release.

    The archive record takes this field's value as the deposit's licence, so a citation that disagrees
    with the table publishes a licence the release does not carry, and the record inherits the error.

    Class: NEW CAPABILITY — the plausible wrong implementation checks only that the field is present, or
    reads the README's value instead; red under it, because this run's citation states MIT and must be
    refused. The differently positioned satisfying input is the control, whose citation states the
    table's licence.
    """
    release = _release(tmp_path, citation="cff-version: 1.2.0\nlicense: MIT\n")

    result = release.run()

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"citation-licence"}, sorted(refusals)
    assert "'MIT'" in refusals["citation-licence"]
    assert "release-metadata" in refusals["citation-licence"]


def test_a_citation_carrying_no_licence_field_is_refused(tmp_path):
    """Property: a `CITATION.cff` with no top-level `license` field refuses, because the record would carry none.

    An absent field is not an agreeing one: the record the dispatcher creates copies this value, so its
    absence leaves the deposit stating no licence at all.

    Class: NEW CAPABILITY — the plausible wrong implementation skips a document it finds no field in;
    red under it, because this run's citation names no licence and must be refused. The differently
    positioned satisfying input is the control, whose citation carries the field.
    """
    release = _release(tmp_path, citation="cff-version: 1.2.0\ntype: software\n")

    result = release.run()

    assert result.returncode == 1, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"citation-licence"}, sorted(refusals)
    assert "carries no top-level `license` field" in refusals["citation-licence"]


def test_every_fact_a_run_can_find_is_reported_in_one_run(tmp_path):
    """Property: one invocation reports every ground it can evaluate, so a reader is not sent round twice.

    The four checks read four surfaces that drift independently. Stopping at the first refusal would
    turn one release's diagnosis into one-per-run, and the surfaces are cheap to read.

    Class: NEW CAPABILITY — the plausible wrong implementation returns at its first refusal; red under
    it, because this run moves all four and must print four lines. The differently positioned
    satisfying input is the control, which prints none.
    """
    release = _release(
        tmp_path,
        wheel_expression="MIT",
        wheel_licence=MIT_TEXT.encode(),
        readme="# Sample\n\n## Licence\n\nMIT.\n",
        citation="cff-version: 1.2.0\nlicense: MIT\n",
    )

    result = release.run()

    assert result.returncode == 1, result.stdout
    assert set(_refusals(result)) == {"source-licence", "class-licence", "readme-licence", "citation-licence"}
    assert _refusal_count(result) >= 4, result.stdout
    assert result.stdout.splitlines()[-1] == f"licence check: {_refusal_count(result)} refusal(s)"


@pytest.mark.parametrize(
    ("arm", "named"),
    [
        pytest.param("no-enumeration", "enumeration", id="no-enumeration"),
        pytest.param("another-schema", "schema", id="another-schema"),
        pytest.param("entry-without-its-digest", "sha256", id="entry-without-its-digest"),
        pytest.param("table-not-toml", "TOML", id="table-not-toml"),
        pytest.param("released-without-licence", "states no licence", id="released-without-licence"),
        pytest.param("unknown-state", "state", id="unknown-state"),
        pytest.param("table-with-no-class", "class", id="table-with-no-class"),
        pytest.param("no-metadata-class", "release-metadata", id="no-metadata-class"),
        pytest.param("dist-with-no-wheel", "wheel", id="dist-with-no-wheel"),
        pytest.param("no-readme", "readme", id="no-readme"),
    ],
)
def test_unreadable_input_exits_two_and_names_the_fact(tmp_path, arm, named):
    """Property: an input the check cannot read is one `unreadable-input` line and exit 2, and never a verdict.

    Exit 2 is the status a broken invocation takes: nothing was compared, so the check has nothing to say
    about what the release states. A run whose inputs are broken must not read as a release that conforms,
    because the step it sits in refuses the upload on exit 1 only.

    Class: NEW CAPABILITY — the plausible wrong implementation raises on a table that is not TOML or
    reports exit 1 for an unreadable artifact, which the workflow reads as a licence defect; red under
    it, because the status is 2 here and the refusal prints one line. The differently positioned
    satisfying input is the control, whose every input is readable.
    """
    release = _release(tmp_path)
    if arm == "no-enumeration":
        release.enumeration.unlink()
    elif arm == "another-schema":
        release.enumeration.write_text(json.dumps({"schema": 2, "entries": []}), encoding="utf-8")
    elif arm == "entry-without-its-digest":
        release.enumeration.write_text(
            json.dumps({"schema": 1, "entries": [{"set": "sdist", "path": "PKG-INFO"}]}), encoding="utf-8"
        )
    elif arm == "table-not-toml":
        release.table.write_text("[[class]\n", encoding="utf-8")
    elif arm == "released-without-licence":
        release.table.write_text(_class_row("release-metadata", ["PKG-INFO"], licence=None), encoding="utf-8")
    elif arm == "unknown-state":
        release.table.write_text(
            _class_row("release-metadata", ["PKG-INFO"], state="shipped", licence=None), encoding="utf-8"
        )
    elif arm == "table-with-no-class":
        release.table.write_text("", encoding="utf-8")
    elif arm == "no-metadata-class":
        release.table.write_text(_class_row("release-package", ["sample/**"]), encoding="utf-8")
    elif arm == "dist-with-no-wheel":
        (release.dist / WHEEL_FILE).unlink()
    elif arm == "no-readme":
        release.readme.unlink()

    result = release.run()

    assert result.returncode == 2, result.stdout
    refusals = _refusals(result)
    assert set(refusals) == {"unreadable-input"}, sorted(refusals)
    assert named in refusals["unreadable-input"]
    assert "conforms" not in result.stdout


def test_the_table_is_read_by_the_rule_the_release_state_comparison_reads_it_by(tmp_path):
    """Property: one malformed table draws the same `unreadable-input` problem from both release scripts.

    The reader is repeated in each script rather than imported, so that each step of the release gate can
    be run and audited alone. This test is what keeps the repetition from drifting: two steps that
    disagreed about which table is readable would read one release two ways.

    The compared part is the problem, which ends at the first `; ` — the prefix each script builds from
    the path, the class index and the fault. What follows the `; ` is each script's own remedy, and the
    remedies legitimately differ: this step says what to do before a licence check can run and the
    comparison says what to do before a class comparison can, so asserting the whole detail constrained
    a fact neither script shares.

    Class: NEW CAPABILITY — the plausible wrong state is a copy edited on one side; red under it,
    because the two problems differ. The differently positioned satisfying input is the control's table,
    which both scripts read.
    """
    release = _release(tmp_path)
    release.table.write_text(_class_row("release-metadata", ["PKG-INFO"], licence=None), encoding="utf-8")

    mine = _refusals(release.run())
    theirs = _refusals(
        subprocess.run(
            [
                sys.executable,
                str(COMPARISON),
                "--enumeration",
                str(release.enumeration),
                "--table",
                str(release.table),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
    )

    assert set(mine) == {"unreadable-input"}, sorted(mine)
    assert set(theirs) == {"unreadable-input"}, sorted(theirs)
    assert mine["unreadable-input"].split("; ")[0] == theirs["unreadable-input"].split("; ")[0]


def test_this_checkouts_hand_written_licence_declarations_agree():
    """Property: `pyproject.toml`'s `license`, `CITATION.cff`'s `license` and the table's source class all name one licence.

    Those three are written by hand in three files and nothing else compares them, so they can drift
    silently while every test that reads one of them stays green. The table's source class is the
    authority the check reads, and the other two are what a consumer and the archive record are told.

    Class: NEW CAPABILITY — the plausible wrong state is a licence changed in one file, which is what
    this repository's 0.1.0 release did; red under it, because that file's value is compared here. The
    differently positioned satisfying input is the same three files agreeing, which is this checkout.
    """
    declared = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]["license"]
    citation = re.search(r"^license\s*:\s*(\S+)\s*$", CITATION.read_text(encoding="utf-8"), re.MULTILINE)
    rows = tomllib.loads(TABLE.read_text(encoding="utf-8"))["class"]
    by_name = {row["name"]: row for row in rows}

    assert by_name["release-metadata"]["licence"] == declared
    assert by_name["release-metadata"]["state"] == "released"
    assert citation is not None, CITATION
    assert citation.group(1) == declared
    assert by_name["release-scripts"]["licence"] == declared


def test_this_checkouts_readme_and_citation_pass_against_this_checkouts_table(tmp_path):
    """Property: the check run over this checkout's real README, citation and table exits 0.

    The README states the legacy corpus's MIT licence in two sentences, and that corpus ships in no byte
    set. This is the arm that establishes the check reads those sentences as statements about that
    corpus: an implementation that read the licence section as one statement about the distribution
    would refuse this release at every tag, and no synthetic arm would show it.

    Class: NEW CAPABILITY — the plausible wrong implementation reads the whole licence section and
    refuses; red under it, because this run exits 0. The differently positioned satisfying input is the
    synthetic refusal arm above, whose sentence names no path.
    """
    release = _release(
        tmp_path, readme=README.read_text(encoding="utf-8"), citation=CITATION.read_text(encoding="utf-8")
    )
    release.table.write_text(TABLE.read_text(encoding="utf-8"), encoding="utf-8")
    licence_bytes = LICENCE.read_bytes()
    entries = [
        _entry("sdist", "PKG-INFO"),
        _entry("sdist", "LICENSE", licence_bytes),
        _entry("sdist", ".gitattributes"),
        _entry("sdist", "CITATION.cff"),
        _entry("wheel", f"{DIST_INFO}/METADATA"),
        _entry("wheel", WHEEL_LICENCE, licence_bytes),
        _entry("source-archive", "LICENSE", licence_bytes),
        _entry("source-archive", ".gitattributes"),
        _entry("source-archive", "scripts/check_licences.py"),
    ]
    release.enumeration.write_text(json.dumps({"schema": 1, "entries": entries}), encoding="utf-8")
    _sdist(
        release.dist / f"{SDIST_TOP}.tar.gz",
        {"PKG-INFO": _header(SOURCE_LICENCE), "LICENSE": licence_bytes, "pyproject.toml": b"[project]\n"},
    )
    _wheel(
        release.dist / WHEEL_FILE,
        {f"{DIST_INFO}/METADATA": _header(SOURCE_LICENCE), WHEEL_LICENCE: licence_bytes},
    )

    result = release.run()

    assert result.returncode == 0, result.stdout
    assert _refusals(result) == {}
    assert "readme-licence" not in result.stdout
    assert "citation-licence" not in result.stdout
    assert result.stdout.splitlines()[-1] == "licence check: conforms"
