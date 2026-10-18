"""Tests of the collection-time refusals and of strict marker registration.

The four refusal tests drive an inner pytest run in process with `pytester`, loading the refusal
plugin with `-p`, because an inner run inherits none of the root configuration's plugins. The
strictness test runs an inner session against a copy of the toolkit's own pytest table, so the
strictness it grades is the toolkit's and no working-tree file is touched.
"""

import tomllib

import pytest

from tests.correspondence import corpus_record, system_scope

pytestmark = pytest.mark.state_check

SLUG = system_scope.slugs()[0]


def _words(text: str) -> set[str]:
    """The whitespace-separated words of `text`, with surrounding punctuation stripped, compared whole."""
    return {word.strip("'\":,()") for word in text.split()}


def _refused(pytester, source: str):
    """Run `source` as one test file in an inner session with the refusal plugin; return the result."""
    pytester.makepyfile(test_probe=source)
    return pytester.runpytest("-p", "no:cacheprovider", "-p", "tests.correspondence.markers")


def test_a_duplicate_test_identifier_stops_collection(pytester):
    """Property: two tests sharing (family, unit, part) stop collection, naming both.

    Class: any two family tests with one identifier. Why: a duplicate identifier makes a test's
    adoption and its proof ambiguous. Killed by: skipping the duplicate test. Second correct
    implementation: a Counter over identifiers reported after the scan.
    """
    result = _refused(
        pytester,
        "import pytest\n"
        f"@pytest.mark.equivalence(unit='a.py:1-2', system='{SLUG}')\ndef test_one():\n    pass\n"
        f"@pytest.mark.equivalence(unit='a.py:1-2', system='{SLUG}')\ndef test_two():\n    pass\n",
    )
    assert result.ret == pytest.ExitCode.USAGE_ERROR
    words = _words(result.stderr.str())
    assert {"test_probe.py::test_one", "test_probe.py::test_two"} <= words, result.stderr.str()


def test_an_owner_outside_the_projects_systems_stops_collection(pytester):
    """Property: a `system=` naming none of the project's systems stops collection, naming the test and the slug.

    Class: any marker of the three names whose system is not the slug of one of the project's
    systems. Why: such a test is selected by no system's expression and owned by nobody. Killed
    by: dropping the slug check. Second correct implementation: a set difference of the marker
    systems against the slugs.
    """
    result = _refused(
        pytester,
        "import pytest\n@pytest.mark.equivalence(unit='a.py:1-2', system='not-a-system')\ndef test_one():\n    pass\n",
    )
    assert result.ret == pytest.ExitCode.USAGE_ERROR
    assert {"test_probe.py::test_one", "not-a-system"} <= _words(result.stderr.str()), result.stderr.str()


def test_a_module_level_family_marker_stops_collection(pytester):
    """Property: a family marker applied by module-level `pytestmark` stops collection, naming the test.

    Class: any family marker not among the test function's own decorator marks. Why: a static
    scan of decorators cannot see it, so the inventory and the selection would disagree. Killed
    by: accepting marks of any origin. Second correct implementation: comparing the item's marks
    with the function's `pytestmark` by identity.
    """
    result = _refused(
        pytester,
        f"import pytest\npytestmark = pytest.mark.equivalence(unit='a.py:1-2', system='{SLUG}')\n"
        "def test_one():\n    pass\n",
    )
    assert result.ret == pytest.ExitCode.USAGE_ERROR
    assert "test_probe.py::test_one" in _words(result.stderr.str()), result.stderr.str()


def test_a_family_marker_from_a_parametrize_case_stops_collection(pytester):
    """Property: a family marker carried by a parametrize case stops collection, naming the test and the case.

    Class: any family marker reaching a test through `pytest.param(..., marks=...)`. Why: the
    static scan reads decorator literals and cannot see a case's marks. The assertion requires
    the refusal for this test to name the parametrize case, because such a mark is also not one
    of the function's own decorator marks, so the more general refusal would still stop the run
    if callspec marks were ignored. Killed by: ignoring callspec marks. Second correct
    implementation: rejecting any parametrized item that carries a family marker at all, with
    that reason.
    """
    result = _refused(
        pytester,
        "import pytest\n@pytest.mark.parametrize('x', [pytest.param(1, id='a', "
        f"marks=pytest.mark.equivalence(unit='a.py:1-2', system='{SLUG}'))])\n"
        "def test_one(x):\n    pass\n",
    )
    assert result.ret == pytest.ExitCode.USAGE_ERROR
    lines = [line for line in result.stderr.lines if "test_probe.py::test_one[a]" in _words(line)]
    assert any("parametrize" in _words(line) for line in lines), result.stderr.str()


def _pytest_table() -> str:
    """The toolkit's `[tool.pytest.ini_options]` table, rendered as the text of a standalone pyproject file."""
    table = tomllib.loads((corpus_record.REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))["tool"]["pytest"][
        "ini_options"
    ]
    lines = ["[tool.pytest.ini_options]"]
    for key, value in table.items():
        rendered = "[" + ", ".join(repr(v) for v in value) + "]" if isinstance(value, list) else repr(value).lower()
        lines.append(f"{key} = {rendered}".replace("'", '"'))
    return "\n".join(lines) + "\n"


def test_the_toolkit_configuration_refuses_an_unregistered_marker(pytester):
    """Property: under the toolkit's own pytest table, a misspelt marker name is a collection error.

    Class: any marker name the configuration does not register. Why: without strictness a typo
    such as `equivalance` silently selects nothing. Killed by: a pytest table without
    `strict_markers`. Second correct implementation: `--strict-markers` in the table's `addopts`.
    """
    pytester.makefile(".toml", pyproject=_pytest_table())
    pytester.makepyfile(
        test_typo="import pytest\n@pytest.mark.equivalance(unit='a.py:1-2')\ndef test_one():\n    pass\n"
    )
    result = pytester.runpytest("-p", "no:cacheprovider", "-c", str(pytester.path / "pyproject.toml"), "test_typo.py")
    assert result.ret != pytest.ExitCode.OK
    assert "equivalance" in _words(result.stdout.str()), result.stdout.str()
