"""Reviewer tests of the release-tag refusal script: the citation ground over lines that only look like a version line.

The script decides which `CITATION.cff` line is the declared version with a text pattern, because it
runs before any project environment exists and so cannot parse YAML. A pattern that is looser than
the file format it stands in for accepts a line that no YAML reader takes as the `version` key, and
the release then cites whatever those readers do find. This module holds the one class of input the
author's tests do not reach: a column-0 `version:` line whose colon is not followed by whitespace.
The script is invoked as a subprocess, as the release workflow invokes it, and only its stdout and
exit status are read.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "check_release_tag.py"

# Passed to every git call: a runner has no identity configured, and `git commit` refuses without one.
IDENTITY = ("-c", "user.name=Release Gate", "-c", "user.email=release-gate@example.invalid")

_REFUSAL_LINE = re.compile(r"^refused: (?P<name>[a-z-]+): (?P<detail>.+)$")

# A result record that is whole, complete and passing, so that no ground but the citation one can refuse.
CONFORMING_RECORD = (
    '{"verdict": "pass", "completeness": "complete", "exclusion": [], "invocation": {"collection_scope": null}}'
)


def _git(repo: Path, *arguments: str) -> str:
    """One git command in `repo`, its stdout returned and a failure raising with git's own message."""
    completed = subprocess.run(["git", *IDENTITY, *arguments], cwd=repo, capture_output=True, text=True, check=False)
    assert completed.returncode == 0, f"git {' '.join(arguments)}: {completed.stderr}"
    return completed.stdout


def _release_commit(repo: Path, version_line: str) -> None:
    """A repository on `main` whose one commit is conforming on every ground but the citation line given."""
    repo.mkdir(parents=True)
    _git(repo, "init", "--quiet", "-b", "main")
    (repo / "pyproject.toml").write_text('[project]\nname = "sample"\nversion = "1.0.0"\n', encoding="utf-8")
    (repo / "CITATION.cff").write_text(
        f'cff-version: 1.2.0\ntitle: "sample"\n{version_line}\nlicense: AGPL-3.0-or-later\n', encoding="utf-8"
    )
    _git(repo, "add", "--all")
    _git(repo, "commit", "--quiet", "-m", "the release")


def _refusals(stdout: str) -> dict[str, str]:
    """The `refused: {id}: {detail}` lines of a run as id -> detail."""
    found = {}
    for line in stdout.splitlines():
        matched = _REFUSAL_LINE.match(line)
        if matched is not None:
            found[matched.group("name")] = matched.group("detail")
    return found


@pytest.mark.parametrize(
    "version_line",
    [
        pytest.param('version:"1.0.0"', id="quoted-value-glued-to-the-colon"),
        pytest.param("version:1.0.0", id="bare-value-glued-to-the-colon"),
    ],
)
def test_a_version_line_with_no_separator_after_the_colon_is_not_a_version_line(tmp_path, version_line):
    """Property: a column-0 `version:` line whose colon is not followed by whitespace does not count as the version.

    Class: column-0 lines of `CITATION.cff` that begin `version:` and carry the release's version
    with nothing between the colon and the value. A YAML reader takes such a line as the start of a
    plain scalar, not as the `version` key, and fails to parse the file at all; the file then cannot
    be read for citation. The citation ground must therefore see no version line and refuse, exactly
    as it does for a file with no `version:` line.

    Why required: the ground exists so the version a release cites is the version it ships. Counting
    a line the format does not read as a key makes the check pass on a file that yields no version
    to any consumer, which is the one outcome the ground is there to refuse.

    What kills it: a pattern that treats the whitespace after the colon as optional, as the script's
    `_VERSION_LINE` does with `[ \\t]*`. Requiring at least one space or tab (`[ \\t]+`) turns this
    test green.

    A second correct implementation, differing in what the test asserts on: a gate that loads the
    file with a full YAML parser instead of a pattern. It would refuse these two files because they
    do not parse, and the test would still pass, since it asserts the refusal and not how it is reached.
    """
    repo = tmp_path / "glued"
    _release_commit(repo, version_line)
    record = tmp_path / "record.json"
    record.write_text(CONFORMING_RECORD, encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--repo",
            str(repo),
            "--tag",
            "v1.0.0",
            "--commit",
            "HEAD",
            "--main-ref",
            "main",
            "--record",
            str(record),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    refusals = _refusals(result.stdout)
    assert set(refusals) == {"citation-version"}, (result.stdout, result.stderr)
    assert result.returncode == 1, result.stdout
