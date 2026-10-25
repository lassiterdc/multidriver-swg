"""Tests of the check that no test imports or runs a legacy file, driven through inner pytest sessions.

An audit hook cannot be removed from a process, so each case runs a separate inner session with
`pytester.runpytest_subprocess`. The inner tree holds a corpus record naming a directory, and in it a
module that appends to a marker file whenever its code runs. For every way of running that module there
is one inner test, run twice: with the plugin registered the session fails, its output carries the
refusal's own exception name, and the marker file does not exist; with the plugin not registered, the
positive control, the same inner test passes and the marker exists. The control is what makes an absent
marker evidence that the refusal came before the module's code, and not that the inner run stopped early
for another reason.

Every test here is a NEW CAPABILITY: no check existed, so a failure is shown by the wrong implementation
a docstring names.
"""

import os
import textwrap
from pathlib import Path

import pytest

from tests.correspondence import corpus_record

pytestmark = pytest.mark.state_check

REAL = Path(__file__).resolve().parents[2]
PLUGIN = "tests.correspondence.legacy_execution"
NAME = "LegacyExecutionError"
RECORD = 'rule = "corpus-record:1"\n' + "".join(
    f'\n[[section]]\nname = "{name}"\ndirectory = "{name}"\n' for name in ("legacy", "legacy2")
)
WAYS = {
    "import": "import sys\nsys.path.insert(0, {dir!r})\n\n\ndef test_it():\n    import mod_legacy\n",
    "runpy": "import runpy\n\n\ndef test_it():\n    runpy.run_path({file!r})\n",
    "spec-exec-module": (
        "import importlib.util\n\n\ndef test_it():\n"
        "    spec = importlib.util.spec_from_file_location('m', {file!r})\n"
        "    spec.loader.exec_module(importlib.util.module_from_spec(spec))\n"
    ),
    "exec-compile": "def test_it():\n    exec(compile(open({file!r}).read(), {file!r}, 'exec'))\n",
    "subprocess-path": "import subprocess\nimport sys\n\n\ndef test_it():\n    subprocess.run([sys.executable, {file!r}], check=True)\n",
    "subprocess-m-cwd": (
        "import subprocess\nimport sys\n\n\ndef test_it():\n"
        "    subprocess.run([sys.executable, '-m', 'mod_legacy'], cwd={dir!r}, check=True)\n"
    ),
    "pythonpath": (
        "import os\nimport subprocess\nimport sys\n\n\ndef test_it():\n"
        "    env = dict(os.environ, PYTHONPATH={dir!r})\n"
        "    subprocess.run([sys.executable, '-c', 'import mod_legacy'], env=env, check=True)\n"
    ),
    "subprocess-c-program": (
        "import subprocess\nimport sys\n\n\ndef test_it():\n"
        "    program = 'import runpy; runpy.run_path(' + repr({file!r}) + ')'\n"
        "    subprocess.run([sys.executable, '-c', program], check=True)\n"
    ),
    "os-system-bytes": (
        "import os\nimport sys\n\n\ndef test_it():\n"
        "    os.system('cd ' + {dir!r} + ' && ' + sys.executable + ' ' + {file!r})\n"
    ),
    "shell-cd": (
        "import subprocess\nimport sys\n\n\ndef test_it():\n"
        "    command = 'cd ' + {dir!r} + ' && ' + sys.executable + ' ' + {file!r}\n"
        "    subprocess.run(command, shell=True, check=True)\n"
    ),
}


def _inner(pytester, monkeypatch, body: str, registered: bool, section: str = "legacy"):
    """Build the inner tree with `body` as its one test file, run it, and return (result, marker path, directory)."""
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(filter(None, [str(REAL), os.environ.get("PYTHONPATH", "")])))
    monkeypatch.setenv("PYTHONDONTWRITEBYTECODE", "1")
    root = pytester.path
    (root / corpus_record.RECORD_NAME).write_text(RECORD, encoding="utf-8")
    (root / "pytest.ini").write_text("[pytest]\n", encoding="utf-8")
    directory = root / section
    directory.mkdir(exist_ok=True)
    marker = root / "marker.txt"
    (directory / "mod_legacy.py").write_text(f"open({str(marker)!r}, 'a').write('ran')\n", encoding="utf-8")
    pytester.makepyfile(
        test_inner=body.format(dir=str(directory), file=str(directory / "mod_legacy.py"), root=str(root))
    )
    args = ["-p", PLUGIN] if registered else []
    return pytester.runpytest_subprocess("-p", "no:cacheprovider", *args), marker, directory


@pytest.mark.parametrize("way", sorted(WAYS))
def test_each_way_of_running_a_legacy_file_is_refused_before_its_code_runs(pytester, monkeypatch, way):
    """Property: with the plugin registered the inner session fails, names the refusal's exception, and the marker is absent.

    Class: eight ways of running one corpus module (an import, `runpy`, `exec_module`, `exec(compile(...))`
    and four interpreter child processes). Why it is required: each is a way a test could run legacy code.
    Killed by: a plugin that only fails the session at the end after the module has run (marker present), or
    one that covers fewer ways. Second correct implementation: a hook that raises on the same events but with
    a different exception name passes if the name asserted is the plugin's own.
    """
    result, marker, _ = _inner(pytester, monkeypatch, WAYS[way], registered=True)
    assert result.ret != 0, result.stdout.str()
    assert NAME in result.stdout.str() + result.stderr.str()
    assert not marker.exists()


@pytest.mark.parametrize("way", sorted(WAYS))
def test_the_same_inner_test_passes_without_the_plugin_and_the_module_runs(pytester, monkeypatch, way):
    """Property: with the plugin not registered, the same inner test passes and the marker file exists.

    Class: the positive control of the test above, one per way. Why it is required: it makes an absent
    marker under the plugin evidence of a refusal before the code ran. Killed by: an inner test that cannot
    run its way at all (it would fail here). Second correct implementation: none other than the way itself.
    """
    result, marker, _ = _inner(pytester, monkeypatch, WAYS[way], registered=False)
    assert result.ret == 0, result.stdout.str() + result.stderr.str()
    assert marker.exists()


def test_a_test_that_catches_the_refusal_still_fails_the_session(pytester, monkeypatch):
    """Property: an inner test that swallows the exception passes, the session fails anyway, and the refusal is listed.

    Class: an import inside `try/except Exception`. Why it is required: a test must not be able to hide an
    attempt. Killed by: a plugin that only raises. Second correct implementation: failing the session from
    a `pytest_runtest_makereport` hook passes.
    """
    body = (
        "import sys\nsys.path.insert(0, {dir!r})\n\n\ndef test_it():\n"
        "    try:\n        import mod_legacy\n    except Exception:\n        pass\n"
    )
    result, marker, _ = _inner(pytester, monkeypatch, body, registered=True)
    result.assert_outcomes(passed=1)
    assert result.ret != 0
    assert NAME in result.stdout.str()
    assert not marker.exists()


def test_a_lift_run_through_run_lift_passes_with_the_plugin_registered(pytester, monkeypatch):
    """Property: code the lift runner compiles under a corpus file name runs without a refusal.

    Class: a stored text run by `run_lift` under the legacy path. Why it is required: it is the one
    admitted way to run legacy text. Killed by: refusing every code object named under the corpus. Second
    correct implementation: exempting by a registry of code-object identities, as here, or by a token
    in the namespace, passes.
    """
    body = textwrap.dedent(
        """
        from pathlib import Path

        from tests.correspondence import extraction, lift


        def test_it():
            root = Path({root!r})
            stored = root / extraction.stored_path("legacy", "mod.py", 1, 2)
            stored.parent.mkdir(parents=True)
            stored.write_text("total = scale * 2\\nmore = total\\n")
            declaration = lift.LiftDeclaration(
                unit="mod.py:1-2", part=None, form="unchanged", fixtures=(), draws="none",
                function=lambda f: ({{"scale": 2}}, lambda ns: ns["more"]), system="x", path=root / "x.py", root=root,
            )
            assert lift.run_lift(declaration) == 4
        """
    )
    result, _, _ = _inner(pytester, monkeypatch, body, registered=True)
    assert result.ret == 0, result.stdout.str() + result.stderr.str()
    result.assert_outcomes(passed=1)


def test_a_git_process_naming_the_corpus_directory_is_admitted(pytester, monkeypatch):
    """Property: a `git` child whose argument names the corpus directory is not refused.

    Class: `git -C {dir} status`, whose executable is not an interpreter. Why it is required: the toolkit's
    own checks run git over the corpus. Killed by: refusing any process that names the corpus. Second
    correct implementation: deciding on the executable's name alone passes.
    """
    body = "import subprocess\n\n\ndef test_it():\n    subprocess.run(['git', '-C', {dir!r}, 'status'], check=False)\n"
    result, _, _ = _inner(pytester, monkeypatch, body, registered=True)
    assert result.ret == 0, result.stdout.str() + result.stderr.str()


def test_exec_of_the_files_text_is_not_refused_which_pins_the_residual(pytester, monkeypatch):
    """Property: `exec(open(path).read())` runs the corpus text without a refusal, and the marker exists.

    Class: corpus text compiled under another name (`<string>`). Why it is required: it pins the accepted
    residual, so that a later widening of the check is visible as this test failing. Killed by: a check
    that refused by content. Second correct implementation: none; the property is that nothing is refused.
    """
    body = "def test_it():\n    exec(open({file!r}).read())\n"
    result, marker, _ = _inner(pytester, monkeypatch, body, registered=True)
    assert result.ret == 0, result.stdout.str() + result.stderr.str()
    assert marker.exists()


def test_a_relative_file_name_is_resolved_against_the_rootdir_never_the_working_directory(pytester, monkeypatch):
    """Property: a relative code-object name under the corpus is refused from any working directory, and one that only matches the working directory is not.

    Class: `compile('x = 1', 'legacy/other.py', 'exec')` executed with the working directory elsewhere (refused),
    and `compile('x = 1', 'other.py', 'exec')` executed with the working directory in the corpus (admitted).
    Why it is required: the lift runner compiles under repository-relative names. Killed by: resolving against
    the current directory. Second correct implementation: resolving against the rootdir's real path passes.
    """
    refused = (
        "import os\n\n\ndef test_it():\n    os.chdir(os.path.dirname({root!r}))\n"
        "    exec(compile('x = 1', 'legacy/other.py', 'exec'))\n"
    )
    result, _, _ = _inner(pytester, monkeypatch, refused, registered=True)
    assert result.ret != 0 and NAME in result.stdout.str()
    pytester.makepyfile(
        test_inner="import os\n\n\ndef test_it():\n    os.chdir({dir!r})\n    exec(compile('x = 1', 'other.py', 'exec'))\n".format(
            dir=str(pytester.path / "legacy")
        )
    )
    admitted = pytester.runpytest_subprocess("-p", "no:cacheprovider", "-p", PLUGIN)
    assert admitted.ret == 0, admitted.stdout.str() + admitted.stderr.str()


def test_a_second_corpus_section_directory_is_covered_with_no_edit(pytester, monkeypatch):
    """Property: a module in the corpus record's second section directory is refused like the first's.

    Class: a two-section record with the module in the second directory. Why it is required: a corpus
    section that enters later must be covered without editing the plugin. Killed by: reading only the first
    section. Second correct implementation: reading all sections from the record, as here, passes.
    """
    result, marker, directory = _inner(pytester, monkeypatch, WAYS["import"], registered=True, section="legacy2")
    assert directory.name == "legacy2"
    assert result.ret != 0 and NAME in result.stdout.str()
    assert not marker.exists()


def test_a_module_in_sys_modules_with_a_corpus_file_fails_the_session_at_its_end(pytester, monkeypatch):
    """Property: at the end of the session, a module whose file lies in a corpus directory fails the session even though no test failed.

    Class: a module object placed in `sys.modules` with `__file__` in the corpus directory, as a module
    loaded by a route the finder never saw would be. Why it is required: it is the backstop for a load the
    finder and the audit hook both missed. Killed by: not scanning `sys.modules`. Second correct
    implementation: scanning at each test's teardown passes.
    """
    body = (
        "import sys\nimport types\n\n\ndef test_it():\n"
        "    module = types.ModuleType('sneaked')\n    module.__file__ = {file!r}\n    sys.modules['sneaked'] = module\n"
    )
    result, _, _ = _inner(pytester, monkeypatch, body, registered=True)
    result.assert_outcomes(passed=1)
    assert result.ret != 0
    assert "module loaded from the legacy corpus: sneaked" in result.stdout.str()


def test_the_finder_refuses_a_corpus_package_that_has_no_code_to_execute(pytester, monkeypatch):
    """Property: importing the corpus directory itself as a namespace package is refused, and passes without the plugin.

    Class: `import legacy` with the root on `sys.path`, where `legacy` has no `__init__` and so executes no
    code the audit hook could see. Why it is required: it is the case only the finder reaches, and it shows
    the finder works independently of the audit hook. Killed by: relying on the `exec` event alone. Second
    correct implementation: a finder that asks every finder on `sys.meta_path` for the spec passes.
    """
    body = "import sys\nsys.path.insert(0, {root!r})\n\n\ndef test_it():\n    import legacy\n"
    refused, marker, _ = _inner(pytester, monkeypatch, body, registered=True)
    assert refused.ret != 0 and NAME in refused.stdout.str()
    admitted = pytester.runpytest_subprocess("-p", "no:cacheprovider")
    assert admitted.ret == 0, admitted.stdout.str() + admitted.stderr.str()
    assert not marker.exists()


def test_a_program_text_naming_the_corpus_by_its_rootdir_relative_path_is_refused_and_a_bare_word_is_not(
    pytester, monkeypatch
):
    """Property: a `-c` program using the corpus directory's rootdir-relative path is refused, while an argument that merely contains the directory's name is admitted.

    Class: `runpy.run_path('legacy/mod_legacy.py')` run from the rootdir (refused, marker absent), and the
    interpreter given a program that prints the word `legacy` (admitted). Why it is required: the relative
    form is how a test written inside the checkout names the corpus, and matching the bare name would refuse
    unrelated arguments. Killed by: matching only the absolute form, or matching the name without a path
    separator. Second correct implementation: matching the relative path as a whole path component passes.
    """
    body = (
        "import os\nimport subprocess\nimport sys\n\n\ndef test_it():\n"
        "    root = {root!r}\n"
        "    program = 'import runpy; runpy.run_path(\"legacy/mod_legacy.py\")'\n"
        "    subprocess.run([sys.executable, '-c', program], cwd=root, check=True)\n"
    )
    result, marker, _ = _inner(pytester, monkeypatch, body, registered=True)
    assert result.ret != 0 and NAME in result.stdout.str()
    assert not marker.exists()
    admitted = "import subprocess\nimport sys\n\n\ndef test_it():\n    subprocess.run([sys.executable, '-c', \"print('legacy')\"], check=True)\n"
    result, marker, _ = _inner(pytester, monkeypatch, admitted, registered=True)
    assert result.ret == 0, result.stdout.str() + result.stderr.str()
