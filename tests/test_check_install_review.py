"""Reviewer tests of the install-and-import check: a declared dependency that names an extras bracket.

The check judges every `Requires-Dist` entry of a built artifact that "carries no extra marker". An
entry such as `dep[fast]>=1` carries an extras BRACKET and no extra MARKER: it is a mandatory
dependency on `dep`, installed with `dep`'s `fast` extra, and it is spelled that way by any package
whose own code needs that extra. A marker (`; extra == "docs"`) is what makes an entry optional; a
bracket never does. An implementation that treats the bracket as the marker's sibling drops the entry
from the set it judges, and every ground over that set then misjudges the artifact: the declared name
is missing from the names an import may be provided by, so an import the declaration plainly covers is
refused as undeclared, and under `lowest-direct` the entry's floor is never passed to the installer.

This module holds the two behaviours that follow from judging such an entry: a package whose one
declaration is bracketed and whose module imports it conforms, and the floor of that declaration is
the version the fresh environment holds under `lowest-direct`. Both read the script as a subprocess,
as the install-check job does, and only its stdout, its exit status and what the fresh environment
installed. It stores no count and no blob, so it survives a promotion into the module test of
`scripts/check_install.py`.
"""

from __future__ import annotations

import subprocess
import sys

from tests.test_check_install import (
    _DIAL,
    DEPENDENCY,
    SCRIPT,
    _dist,
    _environment,
    _links,
    _refusal_ids,
    _run,
)

# The declaration as a package spells it when its own code needs the dependency's `fast` extra: no
# extra marker, a bracket, a floor. Two spellings of the bracket, so the test holds the class and not
# one literal: the second carries two extras and inner whitespace, which the grammar allows.
BRACKETED = (f"{DEPENDENCY}[fast]>=1", f"{DEPENDENCY}[fast, extra-two] >=1")


def test_a_bracketed_declaration_the_module_imports_conforms(tmp_path):
    """A declared, bounded, imported dependency spelled with an extras bracket is not refused.

    Class: REGRESSION -- red on the tree at f9e77d7, where the entry is dropped from the judged set;
    the run prints `refused: undeclared: dep is provided by dep, which ... does not declare` per artifact.
    Reachability: any package whose `Requires-Dist` carries a bracketed entry with no extra marker and
    whose module imports the distribution; artifact: scripts/check_install.py, `_requirements`.
    Why: the entry names the distribution the import resolves to, so declaration and import agree,
    and a refusal tells the author to declare what is already declared.
    What kills it: leaving the `extras` skip in `_requirements`, which empties `named` for this entry.
    Second implementation: one that strips the bracket and judges the bare name, and one that keeps
    the bracket and compares on the name before it, both conform here; the test asserts on the verdict
    and not on how the entry was parsed.
    """
    for spelling in BRACKETED:
        dist = _dist(tmp_path / "dist", requires=(spelling,))
        links = _links(tmp_path / "links")

        result = _run("--dist", str(dist), links=links, cache=tmp_path / "cache")

        assert result.returncode == 0, (spelling, result.stdout, result.stderr)
        assert _refusal_ids(result.stdout) == [], (spelling, result.stdout)
        assert result.stdout.splitlines() == [f"{dist}: conforms"]


def test_a_bracketed_declarations_floor_is_the_version_installed_at_lowest_direct(tmp_path):
    """Under `lowest-direct` the bracketed entry's floor is passed to the installer and so installed.

    Class: NEW CAPABILITY -- the plausible wrong implementation stops refusing the entry (adds it to
    the names an import may be provided by) and still leaves it out of the requirements it passes beside
    the artifact; it exits 0 on the verdict above and installs the highest version the index allows,
    `2.0`, here; this test reds under it on the probe, which reads the version the environment held.
    Reachability: a bracketed entry with a floor below the newest version in the index; artifact:
    scripts/check_install.py, `_installed`, whose requirement list is built from `_requirements`.
    Why: the shape states that every entry without an extra marker is passed beside the artifact, and
    that is the only way `--resolution` reaches a dependency the artifact does not name directly.
    What kills it: an `_installed` that passes no declared requirement beside the artifact, which
    installs `2.0` (measured: the test reds with `AssertionError: dep[fast]>=1` on such a copy).
    Second implementation: one that passes the entry's full text (`dep[fast]>=1`) and one that passes
    the bare name with the specifier (`dep>=1`) both install `1.0`; the assertion is on the installed
    version and not on the argument the installer received.
    """
    for spelling in BRACKETED:
        links = _links(tmp_path / "links")
        dist = _dist(tmp_path / "dist", body=_DIAL, requires=(spelling,))
        probe = tmp_path / "version.txt"
        probe.unlink(missing_ok=True)
        environment = _environment(links, tmp_path / "cache")
        environment["INSTALL_PROBE"] = str(probe)

        result = subprocess.run(
            [sys.executable, str(SCRIPT), "--dist", str(dist), "--resolution", "lowest-direct"],
            capture_output=True,
            text=True,
            check=False,
            env=environment,
        )

        assert result.returncode == 0, (spelling, result.stdout, result.stderr)
        assert probe.read_text(encoding="utf-8") == "1.0", spelling
