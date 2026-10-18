"""Tests of the subject-location check: the session runs only against this checkout's `multidriver_swg`.

The containment test is a pure call of `subject_findings`. The stop test runs the real tree in a
subprocess from the repository root with `PYTHONPATH` naming a synthetic `multidriver_swg` under
the test's temporary directory instead of the checkout's `src`, so the child's import resolves
outside the checkout; the child is restricted with `-k` to the containment test, which needs no
package at all, so it can never select the test that started it. The named check asserts the
same verdict for the session it runs in, so a run reports it by name.
"""

import os
import subprocess
import sys

import pytest

from tests.correspondence import corpus_record, subject

pytestmark = pytest.mark.state_check

CONTAINMENT = "test_the_package_must_lie_inside_the_checkout_not_beside_it"


def test_the_package_under_test_resolves_inside_this_checkout(request):
    """Property: the `multidriver_swg` this session imports lies inside the checkout the tests were collected from.

    Class: every run of the suite. Why: the session-start stop ends a wrong-subject run before any
    test, and this test states the same verdict by name in a run's report. Killed by: a session
    whose package resolves outside its root, which the stop ends first unless it is disabled.
    Second correct implementation: comparing the package's directory with the root's `src`
    directory instead of the package's file with the root.
    """
    findings = subject.subject_findings(subject._package_file(), request.config.rootpath)
    assert not findings, "\n".join(findings)


def test_the_package_must_lie_inside_the_checkout_not_beside_it(tmp_path):
    """Property: a package file in a sibling checkout whose name extends the root's is outside it, named with both paths.

    Class: any package path that shares the root's string prefix without lying under it, here
    `{tmp}/proj2/...` against the root `{tmp}/proj`. Why: a checkout and a sibling whose name
    extends it share a prefix, so a prefix comparison would accept the sibling's code as the code
    under test. Killed by: comparing by string prefix. Second correct implementation: comparing
    the parts of the two resolved paths instead of calling `is_relative_to`.
    """
    root = tmp_path / "proj"
    package = tmp_path / "proj2" / "src" / "multidriver_swg" / "__init__.py"
    root.mkdir()
    package.parent.mkdir(parents=True)
    package.write_text("")
    findings = subject.subject_findings(package, root)
    tokens = " ".join(findings).split()
    assert str(package.resolve()) in tokens and str(root.resolve()) in tokens, findings
    assert subject.subject_findings(root / "src" / "multidriver_swg" / "__init__.py", root) == []


def _child_with_package(tmp_path, name, init_text):
    """Run the real tree from the root with a synthetic package `{tmp}/{name}/src/multidriver_swg` first on the path."""
    root = corpus_record.REPO_ROOT
    package = tmp_path / name / "src" / "multidriver_swg" / "__init__.py"
    package.parent.mkdir(parents=True)
    package.write_text(init_text)
    path = [str(tmp_path / name / "src"), str(root), os.environ.get("PYTHONPATH", "")]
    out = subprocess.run(
        [sys.executable, "-B", "-m", "pytest", "-p", "no:cacheprovider", "-q", "-k", CONTAINMENT],
        cwd=root,
        env=dict(os.environ, PYTHONPATH=os.pathsep.join(path)),
        capture_output=True,
        text=True,
    )
    return package, root, out


def test_a_package_from_outside_the_checkout_stops_the_session(tmp_path):
    """Property: a session whose `multidriver_swg` is not this checkout's importable copy ends before any test.

    Class: any run whose import path puts another copy of the package first — here a synthetic
    copy under the test's temporary directory, which must stop the session naming both paths; and
    a copy whose `__init__` raises while it is imported (a `RuntimeError` here; a syntax error left
    mid-edit is the everyday case), which must stop it with the check's own exit code 4, naming
    the checkout, rather than end it as pytest's internal error. Why: a run that only warned would
    go on to report equivalence verdicts about code nobody in this checkout is changing, and an
    internal error reads as a failure of the test runner rather than of the code under test.
    Killed by: warning instead of stopping; and by treating only `ImportError` as a failed import.
    Second correct implementation: raising a usage error from the configuration hook instead of
    exiting at session start, with a broad exception handler around the import there.
    """
    package, root, out = _child_with_package(
        tmp_path, "outside", '"""A synthetic copy of the package, outside the checkout."""\n'
    )
    tokens = (out.stdout + out.stderr).split()
    assert out.returncode != 0, out.stdout + out.stderr
    assert str(package.resolve()) in tokens and str(root) in tokens, out.stdout + out.stderr
    _package, root, out = _child_with_package(
        tmp_path, "broken", 'raise RuntimeError("a package that breaks while importing")\n'
    )
    assert out.returncode == 4, (out.returncode, out.stdout + out.stderr)
    assert str(root) in (out.stdout + out.stderr).split(), out.stdout + out.stderr
