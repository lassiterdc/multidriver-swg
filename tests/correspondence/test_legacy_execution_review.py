"""Reviewer's test of the no-legacy-execution check: a corpus file reached through a symbolic link is still a corpus file.

The check's finder, its process arm, its `PYTHONPATH` rule and its end-of-session scan all resolve a
path to its real location before asking whether it lies in a corpus directory, and the corpus
directories themselves are stored resolved. A code object's file name, though, is compared after
normalisation only. So code compiled from a corpus file under a path that runs through a symbolic link,
which is what a checkout reached through a linked directory gives, does not compare as lying in the
corpus, and it runs. The first test runs the corpus module through a linked directory in the two ways
that reach the `exec` arm without an import.

The second test pins the process arm's working-directory rule, which no other test isolates: an
interpreter that reads its program from standard input names nothing in its words, so the argument rule
cannot refuse it and only the working-directory rule does.

Each case runs inner pytest sessions with `pytester.runpytest_subprocess`, because an audit hook cannot
be removed from a process. The inner tree holds a corpus record naming one directory, a module in it that
appends to a marker file when its code runs, and a symbolic link to that directory. The same inner test
is run first without the plugin, the positive control, which shows the way really runs the module.
"""

import os
from pathlib import Path

import pytest

from tests.correspondence import corpus_record

pytestmark = pytest.mark.state_check

REAL = Path(__file__).resolve().parents[2]
PLUGIN = "tests.correspondence.legacy_execution"
NAME = "LegacyExecutionError"
RECORD = 'rule = "corpus-record:1"\n\n[[section]]\nname = "legacy"\ndirectory = "legacy"\n'
WAYS = {
    "runpy-through-a-link": "import runpy\n\n\ndef test_it():\n    runpy.run_path({linked!r})\n",
    "exec-compile-through-a-link": ("def test_it():\n    exec(compile(open({linked!r}).read(), {linked!r}, 'exec'))\n"),
}


def _tree(pytester, monkeypatch, body: str) -> Path:
    """Build the inner tree with a linked corpus directory and `body` as its one test; return the marker path."""
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(filter(None, [str(REAL), os.environ.get("PYTHONPATH", "")])))
    monkeypatch.setenv("PYTHONDONTWRITEBYTECODE", "1")
    root = pytester.path
    (root / corpus_record.RECORD_NAME).write_text(RECORD, encoding="utf-8")
    (root / "pytest.ini").write_text("[pytest]\n", encoding="utf-8")
    directory = root / "legacy"
    directory.mkdir()
    marker = root / "marker.txt"
    (directory / "mod_legacy.py").write_text(f"open({str(marker)!r}, 'a').write('ran')\n", encoding="utf-8")
    link = root / "linked_legacy"
    link.symlink_to(directory, target_is_directory=True)
    pytester.makepyfile(test_inner=body.format(linked=str(link / "mod_legacy.py")))
    return marker


@pytest.mark.parametrize("way", sorted(WAYS))
def test_a_corpus_file_run_through_a_symbolic_link_is_refused_before_its_code_runs(pytester, monkeypatch, way):
    """Property: running the corpus module by a path through a linked directory is refused like running it by its own path.

    Class: `runpy.run_path` and `exec(compile(text, path, "exec"))` on the corpus module reached through a
    symbolic link to its directory, run without the plugin (the positive control: the module runs) and then
    with it. Why it is required: a checkout reached through a linked directory names every corpus file by
    such a path, and the check's other arms already resolve links, so this arm alone lets legacy code run
    there. Killed by: comparing a code object's file name after normalisation only, without resolving
    links. Second correct implementation: resolving the file name with `os.path.realpath`, or comparing
    device and inode with `os.path.samefile` against each file under a corpus directory, passes.
    """
    marker = _tree(pytester, monkeypatch, WAYS[way])
    control = pytester.runpytest_subprocess("-p", "no:cacheprovider")
    assert control.ret == 0 and marker.exists(), control.stdout.str() + control.stderr.str()
    marker.unlink()
    result = pytester.runpytest_subprocess("-p", "no:cacheprovider", "-p", PLUGIN)
    assert result.ret != 0, result.stdout.str()
    assert NAME in result.stdout.str() + result.stderr.str()
    assert not marker.exists()


STDIN_IN_CORPUS = (
    "import subprocess\nimport sys\n\n\ndef test_it():\n"
    "    subprocess.run([sys.executable], input='import mod_legacy\\n', text=True, cwd={directory!r}, check=True)\n"
)


def test_an_interpreter_whose_only_route_to_the_corpus_is_its_working_directory_is_refused(pytester, monkeypatch):
    """Property: an interpreter child started with the corpus directory as its working directory is refused even when no argument names the corpus.

    Class: `[sys.executable]` reading its program from standard input, with `cwd` set to the corpus
    directory, so its only route to the corpus is the working directory (an interpreter reading standard
    input puts that directory first on its import path); run without the plugin (the positive control: the
    module runs) and then with it. Why it is required: a child whose words are all absolute or all options
    passes the argument rule, so the working-directory rule is the only one that refuses it. Killed by:
    dropping the working-directory rule, under which every other test of the check still passes. Second
    correct implementation: refusing any interpreter start whose resolved working directory lies under a
    corpus directory, checked before its words, passes.
    """
    marker = _tree(pytester, monkeypatch, STDIN_IN_CORPUS.replace("{directory!r}", repr(str(pytester.path / "legacy"))))
    control = pytester.runpytest_subprocess("-p", "no:cacheprovider")
    assert control.ret == 0 and marker.exists(), control.stdout.str() + control.stderr.str()
    marker.unlink()
    result = pytester.runpytest_subprocess("-p", "no:cacheprovider", "-p", PLUGIN)
    assert result.ret != 0, result.stdout.str()
    assert NAME in result.stdout.str() + result.stderr.str()
    assert not marker.exists()
