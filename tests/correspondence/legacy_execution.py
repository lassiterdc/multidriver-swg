"""Stop a test session in which a test imports or runs a file of the legacy code.

The legacy directory is a corpus to read, never code to run: the equivalence tests compare the new code
with what a stored copy of a legacy unit computes, and that copy is run only through the lift runner. An
import leaves no trace in a tracked file, so a static scan cannot see one, and a test that runs a legacy
file through `runpy`, `importlib`, `exec` or a child interpreter would pass or fail on code nobody is
checking. So this plugin refuses the run at the moment it happens.

At configuration it reads every `[[section]]` directory of `legacy_corpus_record.toml` under the pytest
rootdir, so a corpus section that enters later is covered with no edit here, and installs two things.

A meta-path finder refuses, before the module's code runs, any module whose origin lies in one of those
directories. An audit hook refuses two kinds of event. First, executing a code object whose file name lies
there (a relative file name is resolved against the rootdir and never against the working directory),
unless the lift runner compiled it; this covers `import`, `runpy`, `spec_from_file_location` with
`exec_module` and `exec(compile(text, path, "exec"))`. Second, starting a process whose executable is a
Python interpreter, or a shell command whose words include one, when any argument or word resolves,
against the process's working directory, into one of those directories, when that working directory lies
there, or when its `PYTHONPATH` names one; the audit event of `os.system` carries the command as bytes,
which is decoded first. A `git` process naming a corpus path is admitted, because its executable is not an
interpreter.

A refusal raises `LegacyExecutionError` in the test and is also recorded, and the session fails at its
end if any refusal was recorded, so a test that catches the exception still fails the run. At the end the
session also fails on any module in `sys.modules` whose file lies in a corpus directory.

Accepted residual, stated as a property and not as a list: this check sees a corpus file only by its path,
in a code object's file name or in an interpreter child's launch arguments, working directory or
`PYTHONPATH`. An argument's text is refused when it contains a corpus directory's real path, or its
rootdir-relative path followed by a separator; any other spelling of a corpus path inside an argument's
text, such as one through a symbolic link, is not seen. It therefore does not see corpus text compiled
under another name (`exec(open(path).read())`, whose code object is named `<string>`), and it does not see a
child process that reaches a corpus file through a path assembled after launch. Refusing the first by content at the `compile` audit event is not
available, because the corpus record and the extent check parse whole legacy files with `ast.parse`, which
raises the same event with the same bytes. An audit hook cannot be removed from a process, so the plugin
stays installed for the rest of the process and is made inert when the session ends.
"""

from __future__ import annotations

import importlib.abc
import importlib.machinery
import os
import re
import shlex
import sys
import tomllib
import types
from pathlib import Path

import pytest

from tests.correspondence import corpus_record

SHELLS = ("sh", "bash", "dash", "zsh", "ksh")
INTERPRETER = re.compile(r"^(python|pypy)[\d.]*$")
_STATE: dict = {"active": False, "dirs": [], "rootdir": "", "refusals": [], "installed": False}


class LegacyExecutionError(Exception):
    """A test tried to import or run a file of the legacy corpus; the message names the way and the path."""


def corpus_dirs(rootdir: Path) -> list[str]:
    """Return the resolved directory of every `[[section]]` of the corpus record under `rootdir`; empty when it has none."""
    record = Path(rootdir) / corpus_record.RECORD_NAME
    if not record.is_file():
        return []
    sections = tomllib.loads(record.read_text(encoding="utf-8")).get("section", [])
    return [
        os.path.realpath(Path(rootdir) / section["directory"])
        for section in sections
        if isinstance(section.get("directory"), str) and section["directory"]
    ]


def _inside(path: str, dirs: list[str]) -> bool:
    """Whether the normalised `path` is a corpus directory or lies under one."""
    return any(path == d or path.startswith(d + os.sep) for d in dirs)


def _absolute(name: str, base: str) -> str:
    """Return `name` as a normalised absolute path, a relative name being resolved against `base`."""
    return os.path.normpath(name if os.path.isabs(name) else os.path.join(base, name))


def _refuse(message: str) -> None:
    """Record the refusal for the end of the session, then raise it in the test."""
    _STATE["refusals"].append(message)
    raise LegacyExecutionError(message)


def _compiled_by_lift(code: types.CodeType) -> bool:
    """Whether the lift runner compiled this very code object, which is the one way a corpus file name may execute."""
    module = sys.modules.get("tests.correspondence.lift")
    return module is not None and any(code is compiled for compiled in module.compiled_code_objects())


class _Finder(importlib.abc.MetaPathFinder):
    """Refuse a module whose origin lies in a corpus directory, before its code can run."""

    def find_spec(self, fullname, path=None, target=None):
        """Look the module up the way the path finder does and refuse it if it lies in a corpus directory; never supply one."""
        if not _STATE["active"]:
            return None
        try:
            spec = importlib.machinery.PathFinder.find_spec(fullname, path)
        except Exception:  # a lookup failure is the real finders' to report, not this check's
            return None
        if spec is None:
            return None
        places = [spec.origin] if spec.origin not in (None, "built-in", "frozen") else []
        places += list(spec.submodule_search_locations or [])
        for place in places:
            if _inside(os.path.realpath(place), _STATE["dirs"]):
                _refuse(f"import of {fullname!r} from {place}: a file of the legacy corpus may not be imported")
        return None


def _is_interpreter(word: str) -> bool:
    """Whether a command word names a Python interpreter, by its file name or as this process's own."""
    if INTERPRETER.match(os.path.basename(word)):
        return True
    return os.path.isabs(word) and os.path.exists(word) and os.path.realpath(word) == os.path.realpath(sys.executable)


def _words(executable: str, argv: list[str]) -> list[str]:
    """Return the command's words, including those inside a shell's `-c` command string."""
    words = [executable, *argv]
    if os.path.basename(executable) in SHELLS and "-c" in argv and argv.index("-c") + 1 < len(argv):
        words += _split(argv[argv.index("-c") + 1])
    return words


def _split(command: str) -> list[str]:
    """Split a shell command string into words, with operators such as `&&` as words of their own."""
    lexer = shlex.shlex(command, posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    try:
        return list(lexer)
    except ValueError:  # an unbalanced quote: fall back to whitespace
        return command.split()


def _names_corpus_directory(word: str) -> bool:
    """Whether an argument's text contains a corpus directory's path, in its real form or its rootdir-relative form.

    This is the route `python -c "import runpy; runpy.run_path('<corpus path>')"` takes: the corpus path is
    inside a program text, not an argument that is itself a path. The real form is matched as a plain
    substring, and the relative form (for example `legacy`) as a substring followed by a path separator, so
    the bare word does not refuse an unrelated argument. Not established: that the match is exact. Being a
    substring, it also refuses an argument naming a sibling that shares the text (`<root>/legacy_notes/x`
    for the real form, `notlegacy/x` for the relative form), and any other spelling of a corpus path, such
    as one through a symbolic link, is not seen.
    """
    for directory in _STATE["dirs"]:
        relative = os.path.relpath(directory, _STATE["rootdir"])
        if directory in word or (not relative.startswith("..") and relative + os.sep in word):
            return True
    return False


def _launch_findings(executable: str, words: list[str], cwd: str | None, env) -> str | None:
    """Return why a process start is refused, or None when it is not an interpreter reaching the corpus."""
    if not any(_is_interpreter(word) for word in [executable, *words]):
        return None
    dirs = _STATE["dirs"]
    base = os.path.realpath(cwd or os.getcwd())
    if _inside(base, dirs):
        return f"an interpreter started with the legacy corpus as its working directory {base}"
    for word in words:
        if word and not word.startswith("-") and _inside(os.path.realpath(_absolute(word, base)), dirs):
            return f"an interpreter started with the argument {word!r}, which lies in the legacy corpus"
    for word in words:
        if _names_corpus_directory(word):
            return f"an interpreter started with an argument {word!r} whose text names the legacy corpus"
    environment = dict(os.environ) if env is None else {os.fsdecode(k): os.fsdecode(v) for k, v in dict(env).items()}
    for entry in environment.get("PYTHONPATH", "").split(os.pathsep):
        if entry and _inside(os.path.realpath(_absolute(entry, base)), dirs):
            return f"an interpreter started with PYTHONPATH naming the legacy corpus ({entry})"
    return None


def _process(event: str, args: tuple) -> tuple[str, list[str], str | None, object] | None:
    """Normalise the audit arguments of a process start to (executable, words, working directory, environment)."""
    if event == "subprocess.Popen":
        executable, argv, cwd, env = args
        words = [os.fsdecode(a) for a in (argv if isinstance(argv, list | tuple) else [argv])]
        exe = os.fsdecode(executable or (words[0] if words else ""))
        return exe, _words(exe, words), (os.fsdecode(cwd) if cwd else None), env
    if event == "os.system":
        words = _split(os.fsdecode(args[0]))
        return (words[0] if words else ""), words, None, None
    if event in ("os.exec", "os.posix_spawn"):
        path, argv, env = args[0], args[1], args[2]
    elif event == "os.spawn":
        path, argv, env = args[1], args[2], args[3]
    else:
        return None
    words = [os.fsdecode(a) for a in argv]
    return os.fsdecode(path), _words(os.fsdecode(path), words), None, env


def _audit(event: str, args: tuple) -> None:
    """Refuse the execution of a corpus code object and the start of an interpreter that reaches the corpus."""
    if not _STATE["active"]:
        return
    if event == "exec":
        code = args[0]
        name = getattr(code, "co_filename", "")
        corpus_file = (
            bool(name)
            and not name.startswith("<")
            and _inside(os.path.realpath(_absolute(name, _STATE["rootdir"])), _STATE["dirs"])
        )
        if corpus_file and not _compiled_by_lift(code):
            _refuse(f"execution of code compiled from {name}: a file of the legacy corpus may not be run")
        return
    launch = _process(event, args)
    if launch is not None:
        why = _launch_findings(*launch)
        if why:
            _refuse(f"{event}: {why}")


_FINDER = _Finder()


def pytest_configure(config):
    """Read the corpus directories and install the finder and the audit hook, once per process."""
    root = Path(config.rootpath)
    _STATE.update(rootdir=str(root.resolve()), dirs=corpus_dirs(root), refusals=[], active=True)
    if not _STATE["installed"]:
        sys.meta_path.insert(0, _FINDER)
        sys.addaudithook(_audit)
        _STATE["installed"] = True


def _loaded_corpus_modules() -> list[str]:
    """Name each module in `sys.modules` whose file lies in a corpus directory."""
    found = []
    for name, module in list(sys.modules.items()):
        file = getattr(module, "__file__", None)
        if isinstance(file, str) and _inside(os.path.realpath(file), _STATE["dirs"]):
            found.append(f"{name}: {file}")
    return found


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session, exitstatus):
    """Fail the session if any refusal was recorded or any corpus module is loaded, even if every test passed."""
    if not _STATE["active"]:
        return
    for loaded in _loaded_corpus_modules():
        _STATE["refusals"].append(f"module loaded from the legacy corpus: {loaded}")
    if _STATE["refusals"] and session.exitstatus == 0:
        session.exitstatus = pytest.ExitCode.TESTS_FAILED


def pytest_terminal_summary(terminalreporter):
    """List each recorded refusal under the exception's own name, so a caught refusal is still visible."""
    if _STATE["active"] and _STATE["refusals"]:
        terminalreporter.section("legacy execution")
        for refusal in _STATE["refusals"]:
            terminalreporter.write_line(f"LegacyExecutionError: {refusal}")


def pytest_unconfigure(config):
    """Make the check inert; the audit hook itself cannot be removed from the process."""
    _STATE["active"] = False
