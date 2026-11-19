"""Reviewer tests of the licence check: the three places it reads a licence out of free text.

The check decides what licence a text states by matching family markers in it, and it decides what a
README states about the distribution by splitting the README into sentences. Both are readings of
prose, and the author's tests drive each with the one text shape the author had in mind: a licence file
that is a bare title line, and a README of punctuated sentences. This module holds the classes of text
those two readings are wrong on and the author's tests do not reach: a licence file whose body names a
second licence, a README that states the licence as an SPDX identifier differing in its version
qualifier, and a README licence section whose statements are not separated by sentence punctuation.

Each test builds a release whose four statements agree, moves only the text a test names, runs the
script as a subprocess as the release workflow does, and reads only its stdout and exit status. The
artifacts are written here and not built, so each test measures the comparison and not a build backend.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "check_licences.py"
REFUSAL_LINE = re.compile(r"^refused: (?P<name>[a-z-]+): (?P<detail>.+)$")

SDIST_TOP = "sample-1.0.0"
DIST_INFO = "sample-1.0.0.dist-info"
AGPL = "AGPL-3.0-or-later"
GPL = "GPL-3.0-or-later"

# A licence file is the licence's own text, so its title line names it and its body may name others: the
# GPL text lets a covered work be combined with an AGPL work (section 13), and the AGPL text says it is
# the GNU General Public License's sibling.
GPL_FILE = (
    "                    GNU GENERAL PUBLIC LICENSE\n"
    "                       Version 3, 29 June 2007\n\n"
    "  13. Use with the GNU Affero General Public License.\n\n"
    "  Notwithstanding any other provision of this License, you have permission to link or combine any\n"
    "covered work with a work licensed under version 3 of the GNU Affero General Public License into a\n"
    "single combined work.\n"
)
AGPL_FILE = (
    "                    GNU AFFERO GENERAL PUBLIC LICENSE\n"
    "                       Version 3, 19 November 2007\n\n"
    "  The GNU Affero General Public License is a free, copyleft license for software. It is based on the\n"
    "GNU General Public License, with a provision for network use.\n"
)


def _digest(data: bytes) -> str:
    """The sha256 of `data`, as the enumeration records one."""
    return hashlib.sha256(data).hexdigest()


def _release(
    tmp_path: Path, *, licence: str, licence_file: str, readme: str, sdist_wheel_licence: str | None = None
) -> list[str]:
    """One synthetic release whose four statements agree on `licence`, with the README and licence file given."""
    stated = sdist_wheel_licence or licence
    header = f"Metadata-Version: 2.4\nName: sample\nVersion: 1.0.0\nLicense-Expression: {stated}\n\nA sample.\n"
    licence_bytes = licence_file.encode()
    dist = tmp_path / "dist"
    dist.mkdir()
    with tarfile.open(dist / f"{SDIST_TOP}.tar.gz", "w:gz") as archive:
        for name, data in (("PKG-INFO", header.encode()), ("LICENSE", licence_bytes)):
            member = tarfile.TarInfo(f"{SDIST_TOP}/{name}")
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))
    with zipfile.ZipFile(dist / "sample-1.0.0-py3-none-any.whl", "w") as wheel:
        wheel.writestr(f"{DIST_INFO}/METADATA", header)
        wheel.writestr(f"{DIST_INFO}/licenses/LICENSE", licence_bytes)
    entries = [
        {"set": "sdist", "path": "PKG-INFO", "sha256": _digest(header.encode()), "size": len(header)},
        {"set": "sdist", "path": "LICENSE", "sha256": _digest(licence_bytes), "size": len(licence_bytes)},
        {"set": "wheel", "path": f"{DIST_INFO}/METADATA", "sha256": _digest(header.encode()), "size": len(header)},
        {
            "set": "wheel",
            "path": f"{DIST_INFO}/licenses/LICENSE",
            "sha256": _digest(licence_bytes),
            "size": len(licence_bytes),
        },
        {"set": "source-archive", "path": "LICENSE", "sha256": _digest(licence_bytes), "size": len(licence_bytes)},
    ]
    enumeration = tmp_path / "enumeration.json"
    enumeration.write_text(json.dumps({"schema": 1, "entries": entries}), encoding="utf-8")
    table = tmp_path / "release_state.toml"
    table.write_text(
        '[[class]]\nname = "release-metadata"\nsystem = "packaging-and-release"\nstate = "released"\n'
        f'licence = "{licence}"\npaths = ["PKG-INFO", "{DIST_INFO}/METADATA"]\n\n'
        '[[class]]\nname = "release-licence-file"\nsystem = "packaging-and-release"\nstate = "released"\n'
        f'licence = "{licence}"\npaths = ["LICENSE", "{DIST_INFO}/licenses/LICENSE"]\n',
        encoding="utf-8",
    )
    (tmp_path / "README.md").write_text(readme, encoding="utf-8")
    (tmp_path / "CITATION.cff").write_text(f"cff-version: 1.2.0\nlicense: {licence}\n", encoding="utf-8")
    return [
        sys.executable,
        str(SCRIPT),
        "--enumeration",
        str(enumeration),
        "--table",
        str(table),
        "--dist",
        str(dist),
        "--readme",
        str(tmp_path / "README.md"),
        "--citation",
        str(tmp_path / "CITATION.cff"),
    ]


def _run(command: list[str]) -> tuple[int, dict[str, str], str]:
    """The exit status, the `refused:` lines as id -> detail, and the whole stdout of one run."""
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    found = {}
    for line in result.stdout.splitlines():
        matched = REFUSAL_LINE.match(line)
        if matched is not None:
            found[matched.group("name")] = matched.group("detail")
    return result.returncode, found, result.stdout


def _readme(statement: str) -> str:
    """A README whose licence section holds `statement` verbatim and nothing else."""
    return f"# Sample\n\n## Licence\n\n{statement}\n"


@pytest.mark.parametrize(
    ("arm", "licence", "licence_file", "readme_statement"),
    [
        pytest.param(
            "gpl-file-names-the-affero-licence-in-its-body",
            AGPL,
            GPL_FILE,
            "The sample is licensed under the GNU Affero General Public License (AGPL-3.0-or-later).",
            id="gpl-file-for-an-agpl-class",
        ),
        pytest.param(
            "agpl-file-names-the-general-licence-in-its-body",
            GPL,
            AGPL_FILE,
            "The sample is licensed under the GNU General Public License (GPL-3.0-or-later).",
            id="agpl-file-for-a-gpl-class",
        ),
    ],
)
def test_a_licence_file_of_another_licence_that_names_the_class_licence_in_its_body_is_refused(
    tmp_path, arm, licence, licence_file, readme_statement
):
    """Property: a covering file whose own licence is not the class's licence is refused, whatever else its body names.

    Class: every licence file, in the sdist and the wheel alike, whose title identifies one licence and
    whose body mentions a second family's name. The check reads a file's families as every family its
    text names, and passes the file when the class's family is among them; a GPL file mentions the
    Affero licence in its section 13 and an AGPL file mentions the General licence in its preamble, so
    each passes as the other. The release then ships a licence file that is not the licence it states,
    which is the published misstatement this check exists to stop.

    Shape row: DR-10's `class-licence` ("an entry of a `released` class is not covered") and its
    covering rule ("the covering file ... states the class's table licence"): a file states the
    licence it is, and a file that is another licence does not state this one by mentioning it.

    Why required: `_class_licence_refusals` accepts on `any(name in stated for name in family)` over
    `_families(text)`, which is every family found anywhere in the text; the check is then silent on a
    wrong licence file whenever that file names the right licence once.

    Kills it: reading the covering file's licence from its title line (its first non-blank line), or
    from the family that dominates its text, instead of from any mention.

    Class (B4): NEW CAPABILITY — the plausible wrong implementation is the any-mention reading; red
    under it, output pasted in the reviewer's file. Reachability: a real `LICENSE` carrying the GPL's
    section 13 sentence reaches the failure, and so does the real AGPL text's preamble. Both-states
    anchor: the assertion turns on the refusal id and the exit status, which a correct implementation
    produces on this input whatever its wording. Invocation not isolatable from this tree — the
    discrimination shown instead: reading the title line greens this test.

    A second correct implementation, differing in the thing asserted on: one that counts mentions and
    takes the family named most often, or one that reads only the title, still refuses both arms and
    still conforms on the control `test_the_baseline_release_conforms`.
    """
    command = _release(tmp_path, licence=licence, licence_file=licence_file, readme=_readme(readme_statement))

    status, refusals, stdout = _run(command)

    assert status == 1, f"{arm}: {stdout}"
    assert "class-licence" in refusals, f"{arm}: {sorted(refusals)}"


def test_the_baseline_release_conforms(tmp_path):
    """Property: the synthetic release every reviewer arm moves one text of conforms with exit 0.

    Class: control for the arms of this module. A refusal in a moved arm is evidence only if the
    unmoved release exits 0, because a fixture that refuses everything satisfies every refusal test.
    It names the matching AGPL file for the AGPL class and the matching GPL file for the GPL class, so
    it also holds the satisfying side of the licence-file class: a correct file is not refused for
    naming its sibling licence in its body.

    Shape row: DR-10's `class-licence`, `readme-licence`, `citation-licence` and `source-licence`, all
    silent when the four statements agree. Kills it: an implementation that refuses a licence file for
    naming a second licence at all. A second correct implementation: a title-only reader.
    """
    for licence, licence_file, statement in (
        (AGPL, AGPL_FILE, "The sample is licensed under the GNU Affero General Public License (AGPL-3.0-or-later)."),
        (GPL, GPL_FILE, "The sample is licensed under the GNU General Public License (GPL-3.0-or-later)."),
    ):
        directory = tmp_path / licence
        directory.mkdir()
        command = _release(directory, licence=licence, licence_file=licence_file, readme=_readme(statement))

        status, refusals, stdout = _run(command)

        assert status == 0, f"{licence}: {stdout}"
        assert refusals == {}, f"{licence}: {refusals}"


@pytest.mark.parametrize(
    ("arm", "section"),
    [
        pytest.param(
            "bullets-without-full-stops",
            "- Source code: MIT\n- Legacy corpus (`_old_code_to_refactor/`): MIT",
            id="distribution-bullet-first",
        ),
        pytest.param(
            "bullets-path-bullet-first",
            "- Legacy corpus (`_old_code_to_refactor/`): MIT\n- Source code: MIT",
            id="path-bullet-first",
        ),
        pytest.param(
            "paragraphs-without-final-full-stop",
            "The sample is licensed under the MIT licence\n\nThe legacy tree `_old_code_to_refactor/` is MIT-licensed.",
            id="paragraph-break",
        ),
    ],
)
def test_a_readme_statement_not_closed_by_sentence_punctuation_is_not_scoped_by_its_neighbour(tmp_path, arm, section):
    """Property: a distribution licence statement that shares a line-ending-only boundary with a path-scoped one is still read.

    Class: every README licence section whose statements are separated by a list-item boundary or a
    paragraph boundary and not by `.`, `!` or `?` followed by whitespace. The check splits sentences
    only on that punctuation, and exempts a whole sentence when any backticked token in it holds a
    `/`; two statements with no punctuation between them become one sentence, so the path-scoped
    bullet's exemption swallows the statement about the distribution. Each arm below states MIT for the
    distribution where the metadata carries AGPL-3.0-or-later and must be refused.

    Shape row: DR-10's `readme-licence` — "when the README names a licence expression the metadata does
    not carry". A list of licence statements is the usual shape of a licence section and the shape the
    docstring's own "naive split" disclosure does not name (it names an abbreviation's full stop only).

    Why required: `_readme_statements` is `_SENTENCE_END.split(...)` followed by `_names_a_path` per
    piece, so the unit of scoping is whatever the split yields, and the split cannot see a line or a
    paragraph end.

    Kills it: splitting also on a line break that starts a list item and on a blank line, so each
    statement is scoped by its own text.

    Class (B4): REGRESSION on the shipped split — red on the committed tree for the two bullet arms and
    the paragraph arm, output pasted in the reviewer's file. Reachability: each arm is a README an
    author can write. Both-states anchor: the assertion turns on `readme-licence` being refused, which
    a correct implementation does for every arm whatever its split. Invocation not isolatable from this
    tree — the discrimination shown instead: splitting on blank lines and list-item starts greens it.

    A second correct implementation, differing in the thing asserted on: scoping by physical line
    rather than by sentence refuses all three arms and still conforms on
    `test_a_path_scoped_legacy_statement_beside_the_distributions_statement_conforms`.
    """
    command = _release(tmp_path, licence=AGPL, licence_file=AGPL_FILE, readme=_readme(section))

    status, refusals, stdout = _run(command)

    assert status == 1, f"{arm}: {stdout}"
    assert "readme-licence" in refusals, f"{arm}: {sorted(refusals)}"


def test_a_path_scoped_legacy_statement_beside_the_distributions_statement_conforms(tmp_path):
    """Property: the same list shapes with the distribution stated correctly conform, so the arms above refuse on the MIT line only.

    Class: control for the punctuation-free README arms. A correct implementation that refused every
    unpunctuated licence section would pass those arms and read the real README's neighbours wrongly;
    this arm states AGPL-3.0-or-later for the distribution and MIT for the path-scoped legacy corpus in
    both orders and in paragraphs, and must exit 0.

    Shape row: DR-10's `readme-licence` and the legacy-corpus statement ("its MIT licence stays stated
    by ... in the repository at the tag"): the legacy statement is allowed and is about its path.

    Kills it: an implementation that treats any MIT in the licence section as the distribution's.
    A second correct implementation: line-scoped reading, which conforms on all three.
    """
    sections = (
        "- Source code: AGPL-3.0-or-later\n- Legacy corpus (`_old_code_to_refactor/`): MIT",
        "- Legacy corpus (`_old_code_to_refactor/`): MIT\n- Source code: AGPL-3.0-or-later",
        "The sample is licensed under AGPL-3.0-or-later\n\nThe legacy tree `_old_code_to_refactor/` is MIT-licensed.",
    )
    for index, section in enumerate(sections):
        directory = tmp_path / str(index)
        directory.mkdir()
        command = _release(directory, licence=AGPL, licence_file=AGPL_FILE, readme=_readme(section))

        status, refusals, stdout = _run(command)

        assert status == 0, f"arm {index}: {stdout}"
        assert refusals == {}, f"arm {index}: {refusals}"
