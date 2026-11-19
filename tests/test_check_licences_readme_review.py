"""Reviewer probes of the README licence comparison by family and the stated path-scope residual.

Each case drives `scripts/check_licences.py` as a subprocess over one synthetic release whose four
statements agree on `AGPL-3.0-or-later`, with only the README's licence section moved, and reads the exit
status. The criterion is DR-10's README-comparison bullet with gate G12 Q3 and Q4: a bare family name or
a spelling no SPDX list carries is compared by family, a listed identifier is compared as that
identifier, a token of another family still refuses, and a statement naming a backticked repository path
stays exempt.
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
SOURCE_LICENCE = "AGPL-3.0-or-later"
LICENCE_TEXT = (
    b"                    GNU AFFERO GENERAL PUBLIC LICENSE\n                       Version 3, 19 November 2007\n"
)


def _digest(data: bytes) -> str:
    """The sha256 of `data`, as the enumeration records one."""
    return hashlib.sha256(data).hexdigest()


def _run_check(
    tmp_path: Path, readme: str, *, licence: str = SOURCE_LICENCE, text: bytes = LICENCE_TEXT
) -> tuple[int, dict[str, str], str]:
    """One run of the check over a release whose metadata, files and citation state `licence`."""
    header = f"Metadata-Version: 2.4\nName: sample\nVersion: 1.0.0\nLicense-Expression: {licence}\n\nA sample.\n"
    dist = tmp_path / "dist"
    dist.mkdir()
    with tarfile.open(dist / f"{SDIST_TOP}.tar.gz", "w:gz") as archive:
        for name, data in (("PKG-INFO", header.encode()), ("LICENSE", text)):
            member = tarfile.TarInfo(f"{SDIST_TOP}/{name}")
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))
    with zipfile.ZipFile(dist / "sample-1.0.0-py3-none-any.whl", "w") as wheel:
        wheel.writestr(f"{DIST_INFO}/METADATA", header)
        wheel.writestr(f"{DIST_INFO}/licenses/LICENSE", text)
    rows = [
        ("sdist", "PKG-INFO", header.encode()),
        ("sdist", "LICENSE", text),
        ("wheel", f"{DIST_INFO}/METADATA", header.encode()),
        ("wheel", f"{DIST_INFO}/licenses/LICENSE", text),
        ("source-archive", "LICENSE", text),
    ]
    entries = [{"set": s, "path": p, "sha256": _digest(d), "size": len(d)} for s, p, d in rows]
    (tmp_path / "enumeration.json").write_text(json.dumps({"schema": 1, "entries": entries}), encoding="utf-8")
    (tmp_path / "release_state.toml").write_text(
        '[[class]]\nname = "release-metadata"\nsystem = "packaging-and-release"\nstate = "released"\n'
        f'licence = "{licence}"\npaths = ["PKG-INFO", "{DIST_INFO}/METADATA"]\n\n'
        '[[class]]\nname = "release-licence-file"\nsystem = "packaging-and-release"\nstate = "released"\n'
        f'licence = "{licence}"\npaths = ["LICENSE", "{DIST_INFO}/licenses/LICENSE"]\n',
        encoding="utf-8",
    )
    (tmp_path / "README.md").write_text(readme, encoding="utf-8")
    (tmp_path / "CITATION.cff").write_text(f"cff-version: 1.2.0\nlicense: {licence}\n", encoding="utf-8")
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--enumeration",
            str(tmp_path / "enumeration.json"),
            "--table",
            str(tmp_path / "release_state.toml"),
            "--dist",
            str(dist),
            "--readme",
            str(tmp_path / "README.md"),
            "--citation",
            str(tmp_path / "CITATION.cff"),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    refusals = {}
    for line in result.stdout.splitlines():
        matched = REFUSAL_LINE.match(line)
        if matched is not None:
            refusals[matched.group("name")] = matched.group("detail")
    return result.returncode, refusals, result.stdout


def _readme(statement: str) -> str:
    """A README whose licence section holds `statement` verbatim and nothing else."""
    return f"# Sample\n\n## Licence\n\n{statement}\n"


@pytest.mark.parametrize(
    "statement",
    [
        pytest.param("The sample is licensed under the AGPL.", id="bare-family-name"),
        pytest.param("The sample is AGPLv3 licensed.", id="unlisted-spelling"),
        pytest.param("The sample is licensed under the AGPL v3.", id="spaced-unlisted-spelling"),
        pytest.param("The sample is licensed under the AGPL-3.0-or-later.", id="listed-identifier-at-sentence-end"),
        pytest.param("The sample is licensed under the agpl-3.0-or-later licence.", id="listed-identifier-lower-case"),
        pytest.param("The sample is licensed under the GNU AGPL.", id="gnu-prefixed-family-name"),
    ],
)
def test_a_readme_naming_the_carried_licence_by_family_or_listed_identifier_conforms(tmp_path, statement):
    """Property: every wording of the carried licence the ruling names conforms, and each says so on its readme line."""
    code, refusals, stdout = _run_check(tmp_path, _readme(statement))

    assert code == 0, stdout
    assert refusals == {}
    assert "readme: ['agpl'] for this distribution" in stdout


@pytest.mark.parametrize(
    ("statement", "stated"),
    [
        pytest.param("The sample is licensed under the GPL.", "gpl", id="other-family-bare"),
        pytest.param("The sample is GPLv3 licensed.", "gpl", id="other-family-unlisted-spelling"),
        pytest.param("The sample is licensed under the BSD licence.", "bsd", id="bsd-has-no-listed-spelling"),
        pytest.param(
            "The sample is licensed under the AGPL or the MIT licence.", "mit", id="carried-family-beside-other"
        ),
        pytest.param(
            "The sample is licensed under AGPL-3.0-only.", "agpl-3.0-only", id="listed-identifier-other-version"
        ),
        pytest.param("The sample is licensed under AGPL-3.0.", "agpl-3.0", id="deprecated-listed-identifier"),
    ],
)
def test_a_readme_naming_a_licence_the_release_does_not_carry_is_refused(tmp_path, statement, stated):
    """Property: the family comparison refuses a family the metadata lacks, and the identifier comparison a listed other version."""
    code, refusals, stdout = _run_check(tmp_path, _readme(statement))

    assert code == 1, stdout
    assert set(refusals) == {"readme-licence"}, sorted(refusals)
    assert f"states {stated!r}" in refusals["readme-licence"]


def test_a_path_in_a_neighbouring_sentence_does_not_exempt_a_distribution_statement(tmp_path):
    """Property: the residual is per statement, so a path in the NEXT sentence leaves the MIT statement compared."""
    code, refusals, stdout = _run_check(
        tmp_path, _readme("The sample is licensed under the MIT licence. See `docs/licence.md` for the text.")
    )

    assert code == 1, stdout
    assert set(refusals) == {"readme-licence"}, sorted(refusals)


MIT_TEXT = b"MIT License\n\nPermission is hereby granted, free of charge, to any person obtaining a copy\n"


def test_a_spelling_the_spdx_list_does_not_carry_is_compared_by_family_beside_the_metadata_licence(tmp_path):
    """Property: `Expat` is not an SPDX identifier, so a README stating it beside `MIT` metadata is compared by family.

    Input class: a README spelling of the carried licence that the SPDX list does not carry. The shipped
    vocabulary lists `expat` as an identifier, so the statement is compared at the identifier, `expat` is
    not in the metadata's identifiers `{mit}`, and the release refuses on a spelling G12 Q3 says is
    compared by family. Artifact: `SPDX_IDENTIFIERS` in `scripts/check_licences.py`, whose `expat` row is
    absent from the SPDX list (`licenses.json`, list version 8791eda, 740 identifiers).

    Class: REGRESSION on the criterion — red on this tree, exit 1 where the ruling's text gives exit 0.
    The assertion turns on the exit status for a README that names the carried family, a property of the
    old and the new code alike. Second correct implementation: a vocabulary without `expat`, or one that
    maps `expat` to `mit`; both exit 0 here and still refuse `GPL`.
    """
    code, refusals, stdout = _run_check(
        tmp_path, _readme("The sample is licensed under the Expat licence."), licence="MIT", text=MIT_TEXT
    )

    assert code == 0, stdout
    assert refusals == {}


def test_a_trailing_plus_does_not_turn_an_unlisted_spelling_into_a_listed_identifier(tmp_path):
    """Property: `AGPL-3.0+` is a spelling the SPDX list does not carry, so it is compared by family and conforms.

    Input class: an unlisted spelling that EXTENDS a listed identifier with a character the family marker
    does not consume. The marker reads `AGPL-3.0` and stops before the `+`, so the listed test runs on the
    truncated token `agpl-3.0`, which is the deprecated identifier on the list, and the release refuses
    with a detail naming `agpl-3.0` although the README never wrote it. Artifact: `_mentions` and
    `_readme_statements` in `scripts/check_licences.py`; the SPDX list carries `AGPL-3.0` and
    `GPL-3.0+` but no `AGPL-3.0+`.

    Class: REGRESSION on the criterion — red on this tree, exit 1 where G12 Q3 gives exit 0. The
    assertion turns on the exit status for a README whose statement names the carried family. Second
    correct implementation: one that rejects a listed match followed by `[+A-Za-z0-9]` and falls back to
    family; it exits 0 here and still refuses `AGPL-3.0-only`.
    """
    code, refusals, stdout = _run_check(tmp_path, _readme("The sample is licensed under AGPL-3.0+."))

    assert code == 0, stdout
    assert refusals == {}
