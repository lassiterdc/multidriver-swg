"""Tests of the pair scanner and the call closure, over synthetic packages written under each test's temporary directory.

Each synthetic tree has the toolkit's own layout — `src/{package}/` beside
`tests/registrations/` — so module names are counted from the same root the real tree uses.
Nothing synthetic is imported in this process: the scan and the closure read source text, and
the one test that needs the registry filled by an import runs that import in a child
interpreter. The single real-tree test asserts a relation that holds at any pair count — the
scan, the import-time registry and the expected count agree — never a literal zero.
"""

import ast
import importlib
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from multidriver_swg import pairs
from tests.correspondence import fingerprint, pair_scan, registration_read

pytestmark = pytest.mark.state_check

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC = Path(pairs.__file__).resolve().parents[1]
PKG = "synthpkg"


def _tree(tmp_path: Path, files: dict[str, str]) -> Path:
    """Write `files` (paths relative to a synthetic repository root) and return the synthetic package directory."""
    for relative, source in files.items():
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(textwrap.dedent(source), encoding="utf-8")
    package_dir = tmp_path / "src" / PKG
    package_dir.mkdir(parents=True, exist_ok=True)
    (package_dir / "__init__.py").touch()
    return package_dir


CHAIN = {
    "src/synthpkg/a.py": """
        from synthpkg.b import g

        LIMIT = 10
        UNUSED = 1


        def f(x):
            return g(x) + 1
    """,
    "src/synthpkg/b.py": """
        from synthpkg import c as helpers

        SCALE = 3


        def g(x):
            return helpers.h(x) * SCALE
    """,
    "src/synthpkg/c.py": '''
        def h(x):
            """Return x."""
            return x
    ''',
}


def _f_closure(package_dir: Path) -> pair_scan.Closure:
    """The closure of `synthpkg.a:f` in the synthetic chain."""
    return pair_scan.closure(package_dir / "a.py", "f", package_dir, PKG)


def test_the_scan_lists_each_pair_with_the_site_of_each_side(tmp_path):
    """Property: every literal `pair_side` declaration appears under its pair and side with its path, qualified name and line.

    Class: declarations of both sides of a pair, in different modules, on a function, an
    `async def` and a method, written as a bare or a dotted decorator. Why: the scan is the
    static half of the registry check and the list a record's entries are matched against; a
    side it misses is a pair no check compares. Killed by: reading only module-level functions,
    only the bare decorator name, or only `ast.FunctionDef`. Second correct implementation: a
    scan that walks every node with a qualified-name stack instead of the top level and class
    bodies.
    """
    package_dir = _tree(
        tmp_path,
        {
            "src/synthpkg/old.py": """
                from multidriver_swg.pairs import pair_side


                @pair_side(pair="p", side="pre_refactor")
                def legacy(x):
                    return x


                @pair_side(pair="q", side="pre_refactor")
                async def legacy_async(x):
                    return x
            """,
            "src/synthpkg/new.py": """
                from multidriver_swg import pairs


                class Model:
                    @pairs.pair_side(pair="p", side="current")
                    def run(self, x):
                        return x


                @pairs.pair_side(pair="q", side="current")
                def current_q(x):
                    return x
            """,
        },
    )
    scan = pair_scan.scan_pairs(package_dir, PKG)
    assert scan.findings == []
    assert scan.pairs == {
        "p": {"pre_refactor": ("src/synthpkg/old.py", "legacy", 5), "current": ("src/synthpkg/new.py", "Model.run", 6)},
        "q": {
            "pre_refactor": ("src/synthpkg/old.py", "legacy_async", 10),
            "current": ("src/synthpkg/new.py", "current_q", 11),
        },
    }


def test_the_scan_names_every_declaration_it_cannot_accept(tmp_path):
    """Property: a non-literal pair or side, a positional call, an unknown side, a duplicate side and a lone side are each a finding.

    Class: every `pair_side` declaration the scan cannot list as a complete pair, beside one
    complete pair as the satisfying input. Why: a declaration the scan cannot read still
    registers at import, so passing over it in silence would make the scan and the registry
    disagree with nothing saying why. Killed by: skipping non-literal declarations, keeping the
    later of two sides, or listing a one-sided pair as complete. Second correct implementation: a
    scan that reports each finding with a different message and the same file and line.
    """
    package_dir = _tree(
        tmp_path,
        {
            "src/synthpkg/m.py": """
                from multidriver_swg.pairs import CURRENT, pair_side

                NAME = "n"


                @pair_side(pair=NAME, side="current")
                def by_name(x): ...


                @pair_side(pair="s", side=CURRENT)
                def side_by_name(x): ...


                @pair_side("t", "current")
                def positional(x): ...


                @pair_side(pair="u", side="later")
                def unknown_side(x): ...


                @pair_side(pair="d", side="current")
                def first(x): ...


                @pair_side(pair="d", side="current")
                def second(x): ...


                @pair_side(pair="ok", side="pre_refactor")
                def ok_old(x): ...


                @pair_side(pair="ok", side="current")
                def ok_new(x): ...
            """,
        },
    )
    scan = pair_scan.scan_pairs(package_dir, PKG)
    lines = sorted(int(f.split(":")[1]) for f in scan.findings if f.startswith("src/synthpkg/m.py:"))
    assert lines == [7, 11, 15, 19], scan.findings
    assert any("side current declared twice" in f and "m.py:23" in f and "m.py:27" in f for f in scan.findings)
    assert any(f.startswith("pair d: only its current side") for f in scan.findings), scan.findings
    assert "ok" in scan.pairs and not any("pair ok" in f for f in scan.findings), scan.findings
    assert set(scan.pairs) == {"d", "ok"}


def test_a_changed_body_at_the_end_of_a_call_chain_moves_the_label_and_names_that_function(tmp_path):
    """Property: in `f -> g -> h`, a behaviour change in `h` moves `f`'s combined label, and only `h`'s function label differs.

    Class: changes to a function reached through each resolvable call form — a from-imported
    name (`g`) and a module alias (`helpers.h`) — beside a docstring-only change as the
    satisfying input. Why: a record of `f` is only as current as the code `f` reaches; a change
    two calls away must move the record's fingerprint and say which function moved. Killed by:
    fingerprinting `f` alone, or following only the first call. Second correct implementation: a
    depth-first closure, which reaches the same set of functions in another order.
    """
    package_dir = _tree(tmp_path, CHAIN)
    before = _f_closure(package_dir)
    assert set(before.functions) == {"synthpkg.a:f", "synthpkg.b:g", "synthpkg.c:h"}, before
    (package_dir / "c.py").write_text('def h(x):\n    """Return x unchanged."""\n    return x\n', encoding="utf-8")
    assert _f_closure(package_dir).combined == before.combined
    (package_dir / "c.py").write_text('def h(x):\n    """Return x."""\n    return x + 0\n', encoding="utf-8")
    after = _f_closure(package_dir)
    assert after.combined != before.combined
    assert {k for k in before.functions if before.functions[k] != after.functions[k]} == {"synthpkg.c:h"}


def test_a_changed_module_constant_a_reached_function_reads_moves_the_label(tmp_path):
    """Property: changing a module-level value that a reached function reads moves the combined label; an unread one does not.

    Class: module-level assignments read by a function in the closure (`SCALE`, read by `g`),
    beside an assignment no reached function reads (`UNUSED`). Why: a function's behaviour
    depends on the values it reads as much as on its body, so a record must go stale when one
    of them changes. Killed by: fingerprinting function bodies only. Second correct
    implementation: folding each read binding's label into its reader's function label instead
    of listing it under `names`.
    """
    package_dir = _tree(tmp_path, CHAIN)
    before = _f_closure(package_dir)
    assert "synthpkg.b:SCALE" in before.names and "synthpkg.a:UNUSED" not in before.names, before.names
    a_path = package_dir / "a.py"
    a_path.write_text(a_path.read_text().replace("UNUSED = 1", "UNUSED = 2"), encoding="utf-8")
    assert _f_closure(package_dir).combined == before.combined
    b_path = package_dir / "b.py"
    b_path.write_text(b_path.read_text().replace("SCALE = 3", "SCALE = 4"), encoding="utf-8")
    assert _f_closure(package_dir).combined != before.combined


def test_unresolvable_calls_are_counted_and_listed(tmp_path):
    """Property: a method call on an object and a library call are listed in `unresolved`, and a resolvable call is not.

    Class: calls the closure cannot follow — a method of a parameter, a builtin, a function of a
    third-party or standard-library module — beside a bare-name call of a same-module `def`.
    Why: a closure that dropped what it could not follow would vouch for code it never read; the
    list is the closure's own statement of where it stopped. Killed by: discarding unresolvable
    calls, or listing resolvable ones too. Second correct implementation: recording each
    unresolved call as a `(line, expression)` pair instead of a string.
    """
    package_dir = _tree(
        tmp_path,
        {
            "src/synthpkg/u.py": """
                import math


                def helper(x):
                    return x


                def entry(frame, x):
                    frame.normalise()
                    return math.sqrt(len(x)) + helper(x)
            """,
        },
    )
    result = pair_scan.closure(package_dir / "u.py", "entry", package_dir, PKG)
    calls = sorted(entry.split(": ", 1)[1] for entry in result.unresolved)
    assert calls == ["frame.normalise", "len", "math.sqrt"], result.unresolved
    assert set(result.functions) == {"synthpkg.u:entry", "synthpkg.u:helper"}


T5_REGISTRATION = """
    from synthpkg.target import work as imported_work
    import synthpkg.target as target_alias
    from synthpkg import target

    from tests.correspondence.binding import registers


    @registers(unit="a.py:1-2")
    def by_from_import(x):
        return imported_work(x)


    @registers(unit="a.py:3-4")
    def by_import_as(x):
        return target_alias.work(x)


    @registers(unit="a.py:5-6")
    async def by_from_package_import(x):
        return target.work(x)
"""


def test_a_registered_callable_reaches_a_package_function_by_each_of_the_three_import_forms(tmp_path):
    """Property: the closure of a registered callable contains a package function it calls through each of the three import forms.

    Class: `from {package}.x import f` then `f()`; `import {package}.x as alias` then
    `alias.f()`; `from {package} import x` then `x.f()` — from a registration module outside the
    package, entered through `registered_functions`, one entry an `async def`; plus the relative
    form from inside the package. Why: a departure entry's fingerprint is the closure of the
    function an owning system registers, and that function lives outside the package; if the
    closure cannot reach the package code it calls, the fingerprint covers nothing that can
    change. Killed by: resolving names only within one module, or dropping either alias form.
    Second correct implementation: resolving every import form through a single table of
    `local name -> dotted target` instead of two tables.
    """
    package_dir = _tree(
        tmp_path,
        {
            "src/synthpkg/target.py": "def work(x):\n    return x * 2\n",
            "src/synthpkg/relative.py": "from .target import work\n\n\ndef entry(x):\n    return work(x)\n",
            "tests/registrations/x.py": T5_REGISTRATION,
        },
    )
    registration = tmp_path / "tests" / "registrations" / "x.py"
    functions, findings = registration_read.registered_functions(registration)
    assert findings == [] and len(functions) == 3, (functions, findings)
    reached = {
        unit: "synthpkg.target:work" in pair_scan.closure(registration, node.name, package_dir, PKG).functions
        for unit, node in functions.items()
    }
    reached["relative"] = (
        "synthpkg.target:work" in pair_scan.closure(package_dir / "relative.py", "entry", package_dir, PKG).functions
    )
    assert reached == {"a.py:1-2": True, "a.py:3-4": True, "a.py:5-6": True, "relative": True}, reached


def test_an_async_package_function_is_followed_and_fingerprinted(tmp_path):
    """Property: an `async def` in the package is reached by a call and labelled like any other function.

    Class: `async def` package functions reached from a plain or an async caller. Why: the
    registration reader and the printer both accept `async def`, so a closure that stopped at
    one would leave registered async code uncovered. Killed by: indexing only `ast.FunctionDef`
    definitions. Second correct implementation: indexing definitions by walking the module for
    both node types rather than reading the top level.
    """
    package_dir = _tree(
        tmp_path,
        {
            "src/synthpkg/aio.py": "async def fetch(x):\n    return x\n\n\nasync def entry(x):\n    return await fetch(x)\n"
        },
    )
    result = pair_scan.closure(package_dir / "aio.py", "entry", package_dir, PKG)
    assert set(result.functions) == {"synthpkg.aio:entry", "synthpkg.aio:fetch"} and result.unresolved == [], result


def test_the_denominators_line_counts_pairs_covered_functions_and_unresolved_calls(tmp_path):
    """Property: the denominators line states the scanned pair count, the functions each pair's closures cover, and the unresolved calls.

    Class: any scan and its closures; here one pair whose sides reach one and two functions,
    with one unresolved call. Why: a detector's silence means something only beside the size of
    what it looked at; the line is that size. Killed by: counting closures instead of distinct
    functions, or omitting unresolved calls. Second correct implementation: computing the counts
    from the scan inside `denominators` rather than from closures passed in.
    """
    package_dir = _tree(
        tmp_path,
        {
            "src/synthpkg/pp.py": """
                from multidriver_swg.pairs import pair_side


                def inner(x):
                    return len(x)


                @pair_side(pair="p", side="pre_refactor")
                def old(x):
                    return x


                @pair_side(pair="p", side="current")
                def new(x):
                    return inner(x)
            """,
        },
    )
    scan = pair_scan.scan_pairs(package_dir, PKG)
    line = pair_scan.denominators(scan, pair_scan.pair_closures(scan, package_dir, PKG))
    assert line == "pairs scanned: 1; functions covered: p=3; unresolved calls: 1", line


def test_registry_mismatch_names_each_disagreement_and_nothing_when_all_three_agree():
    """Property: `registry_mismatch` is empty exactly when the scanned sides, the registered sides and the expected count agree.

    Class: a side the scan saw that is not registered, a side registered that the scan missed,
    a count off by one, and the all-agree case. Why: the three sources are written by
    different hands — source text, import-time execution and a constant an owner raises — and a
    pair one of them misses is the gap the registry check exists to show. Killed by: comparing
    pair ids without sides, or skipping the count. Second correct implementation: comparing
    sorted lists of `(pair, side)` instead of sets.
    """
    scan = pair_scan.PairScan(pairs={"p": {"pre_refactor": ("f", "a", 1), "current": ("f", "b", 2)}})
    both = {"p": {"pre_refactor": object(), "current": object()}}
    assert pair_scan.registry_mismatch(scan, both, 1) == []
    assert len(pair_scan.registry_mismatch(scan, {"p": {"current": object()}}, 1)) == 1
    assert len(pair_scan.registry_mismatch(scan, {**both, "q": {"current": object()}}, 1)) == 1
    assert len(pair_scan.registry_mismatch(scan, both, 0)) == 1


CHILD = """
import importlib, json, sys
from pathlib import Path
from multidriver_swg import pairs
from tests.correspondence import pair_scan
package_dir = Path(sys.argv[1])
scan = pair_scan.scan_pairs(package_dir, "synthpkg")
for name in pair_scan.scanned_modules(scan, package_dir):
    importlib.import_module(name)
print(json.dumps(pair_scan.registry_mismatch(scan, pairs.registered_pairs(), int(sys.argv[2]))))
"""


def test_the_registry_check_reports_a_package_whose_expected_count_is_not_raised(tmp_path):
    """Property: in a fresh interpreter, a package registering one complete pair with the expected count left at 0 is reported.

    Class: a registered, fully scanned pair whose owner did not raise the expected count. Why:
    raising the count in the same change is what makes a forgotten registration visible, so the
    check must refuse a count that does not match. Run in a child, because importing the
    synthetic package fills the registry for the rest of the interpreter. The synthetic
    package's expected count is passed in as 0 rather than read from the real package, whose
    count rises the day an owning system registers its first pair. Killed by: a check
    that compares the scan with the registry and ignores the count. Second correct
    implementation: a check that compares the registry's pair count with the constant directly.
    """
    package_dir = _tree(
        tmp_path,
        {
            "src/synthpkg/one.py": """
                from multidriver_swg.pairs import pair_side


                @pair_side(pair="p", side="pre_refactor")
                def old(x):
                    return x


                @pair_side(pair="p", side="current")
                def new(x):
                    return x
            """,
        },
    )
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(tmp_path / "src"), str(SRC), str(REPO_ROOT)])}
    run = subprocess.run([sys.executable, "-c", CHILD, str(package_dir), "0"], env=env, capture_output=True, text=True)
    assert run.returncode == 0, run.stderr
    assert json.loads(run.stdout) == ["1 pair(s) scanned but the expected pair count is 0"], run.stdout


def test_the_real_package_scan_registry_and_expected_count_agree():
    """Property: for the toolkit's own package, the scanned pairs, the import-time registry and the expected count agree.

    Class: the real package at any pair count. Why: this is the registry-equality check itself,
    run on the shipped tree; it compares the three sources with each other and never with a
    literal, so it holds at zero pairs and at any later count. Killed by: a pair registered by a
    call the scan cannot read, a scanned side whose module fails to register it, or a count not
    raised. Second correct implementation: running the same comparison in a child interpreter.
    """
    package_dir = Path(pairs.__file__).resolve().parent
    scan = pair_scan.scan_pairs(package_dir)
    for name in pair_scan.scanned_modules(scan, package_dir):
        importlib.import_module(name)
    assert (
        scan.findings == []
        and pair_scan.registry_mismatch(scan, pairs.registered_pairs(), pairs.EXPECTED_PAIR_COUNT) == []
    )


def test_the_scanner_s_sides_are_the_registry_s_sides():
    """Property: the scanner's restated side names equal `multidriver_swg.pairs.SIDES`.

    Class: the one constant this module restates rather than imports. Why: the scanner cannot
    import the package, so the two must be kept equal; a side added to one would be read as
    unknown by the other. Killed by: editing either tuple alone. Second correct implementation:
    the scanner reading the tuple from `pairs.py`'s syntax tree.
    """
    assert pair_scan.PAIR_SIDES == pairs.SIDES


SETTINGS_READS = {
    "src/synthpkg/settings.py": "SCALE = 2\nOTHER = 5\n",
    "src/synthpkg/relay.py": "from synthpkg.settings import SCALE\n",
    "src/synthpkg/sides.py": """
        import math

        import synthpkg.settings as conf
        from synthpkg import settings
        from synthpkg.relay import SCALE as RELAYED
        from synthpkg.settings import SCALE

        from .settings import SCALE as RELATIVE


        def direct(x):
            return x * SCALE


        def via_alias(x):
            return x * conf.SCALE


        def via_from_package(x):
            return x * settings.SCALE


        def via_reexport(x):
            return x * RELAYED


        def via_relative(x):
            return x * RELATIVE


        def via_library(x):
            return x * math.pi
    """,
}
PACKAGE_READS = ("direct", "via_alias", "via_from_package", "via_reexport", "via_relative")


def _side_closures(package_dir: Path) -> dict[str, pair_scan.Closure]:
    """The closure of every side in the synthetic `sides` module."""
    return {
        name: pair_scan.closure(package_dir / "sides.py", name, package_dir, PKG)
        for name in (*PACKAGE_READS, "via_library")
    }


def test_an_imported_package_constant_is_labelled_where_it_is_defined(tmp_path):
    """Property: a package constant a side imports moves the side's label when its defining value changes, by every read form.

    Class: a constant read as a from-imported name, as `alias.NAME` after `import {package}.x as
    alias`, as `x.NAME` after `from {package} import x`, through a re-export, and by the relative
    form; beside a library import, which keeps its import statement's label, and an edit to a
    binding in the defining module that no side reads, which moves nothing. Why: a constant in a
    sibling module is the commonest place a refactor moves a setting, and a record whose label
    held the import statement alone would vouch for a computation whose input had changed. The
    unread-binding arm is what separates following the import to the one binding read from
    labelling the whole imported module, which passes every other arm here. Killed by: labelling
    an imported name by its import statement, or labelling the imported module as a whole.
    Second correct implementation: resolving every imported name to its defining binding in a
    pre-pass over the package, before any closure is computed.
    """
    package_dir = _tree(tmp_path, SETTINGS_READS)
    before = _side_closures(package_dir)
    assert {name: "synthpkg.settings:SCALE" in before[name].names for name in PACKAGE_READS} == dict.fromkeys(
        PACKAGE_READS, True
    ), {name: sorted(c.names) for name, c in before.items()}
    import_statement = ast.parse("import math").body[0]
    assert before["via_library"].names == {
        "synthpkg.sides:math": fingerprint.label(fingerprint.canonical(import_statement))
    }
    settings_path = package_dir / "settings.py"
    settings_path.write_text("SCALE = 2\nOTHER = 6\n# a comment\n", encoding="utf-8")
    unread_edit = _side_closures(package_dir)
    assert {name: unread_edit[name].combined == before[name].combined for name in before} == dict.fromkeys(before, True)
    settings_path.write_text("SCALE = 3\nOTHER = 5\n", encoding="utf-8")
    read_edit = _side_closures(package_dir)
    moved = {name: read_edit[name].combined != before[name].combined for name in before}
    assert moved == {**dict.fromkeys(PACKAGE_READS, True), "via_library": False}, moved


DECLARATIONS = {
    # read as declarations: each must be a SCANNED pair
    "src/synthpkg/read_plain.py": "from multidriver_swg.pairs import pair_side\n\n\n@pair_side(pair='plain', side='current')\ndef a(x):\n    return x\n",
    "src/synthpkg/read_method.py": "from multidriver_swg.pairs import pair_side\n\n\nclass C:\n    @pair_side(pair='method', side='current')\n    def a(self, x):\n        return x\n",
    "src/synthpkg/read_module_as.py": "import multidriver_swg.pairs as mp\n\n\n@mp.pair_side(pair='module_as', side='current')\ndef a(x):\n    return x\n",
    "src/synthpkg/read_import_as.py": "from multidriver_swg.pairs import pair_side as ps\n\n\n@ps(pair='import_as', side='current')\ndef a(x):\n    return x\n",
    "src/synthpkg/read_from_pkg.py": "from multidriver_swg import pairs\n\n\n@pairs.pair_side(pair='from_pkg', side='current')\ndef a(x):\n    return x\n",
    "src/synthpkg/read_from_pkg_as.py": "from multidriver_swg import pairs as pz\n\n\n@pz.pair_side(pair='from_pkg_as', side='current')\ndef a(x):\n    return x\n",
    "src/synthpkg/read_dotted.py": "import multidriver_swg.pairs\n\n\n@multidriver_swg.pairs.pair_side(pair='dotted', side='current')\ndef a(x):\n    return x\n",
    "src/synthpkg/pairs.py": "def pair_side(*, pair, side):\n    return lambda function: function\n",
    "src/synthpkg/read_relative.py": "from .pairs import pair_side as decl\n\n\n@decl(pair='relative', side='current')\ndef a(x):\n    return x\n",
    # not read: each pair_side(...) call must be a finding at its own line
    "src/synthpkg/u_if.py": "from multidriver_swg.pairs import pair_side\n\nif True:\n\n    @pair_side(pair='u_if', side='current')\n    def a(x):\n        return x\n",
    "src/synthpkg/u_try.py": "from multidriver_swg.pairs import pair_side\n\ntry:\n\n    @pair_side(pair='u_try', side='current')\n    def a(x):\n        return x\n\nexcept ImportError:\n    pass\n",
    "src/synthpkg/u_nested_def.py": "from multidriver_swg.pairs import pair_side\n\n\ndef outer():\n    @pair_side(pair='u_nested_def', side='current')\n    def a(x):\n        return x\n\n    return a\n",
    "src/synthpkg/u_nested_class.py": "from multidriver_swg.pairs import pair_side\n\n\nclass O:\n    class I:\n        @pair_side(pair='u_nested_class', side='current')\n        def a(self, x):\n            return x\n",
    "src/synthpkg/u_by_hand.py": "from multidriver_swg.pairs import pair_side\n\n\ndef a(x):\n    return x\n\n\na = pair_side(pair='u_by_hand', side='current')(a)\n",
    "src/synthpkg/u_wrapped.py": "from multidriver_swg.pairs import pair_side\n\n\ndef keep(d):\n    return d\n\n\n@keep(pair_side(pair='u_wrapped', side='current'))\ndef a(x):\n    return x\n",
    "src/synthpkg/u_conditional.py": "from multidriver_swg.pairs import pair_side\n\n\n@(pair_side(pair='u_conditional', side='current') if True else None)\ndef a(x):\n    return x\n",
    "src/synthpkg/u_wrapped_attr.py": "from multidriver_swg import pairs\n\n\ndef keep(d):\n    return d\n\n\n@keep(pairs.pair_side(pair='u_wrapped_attr', side='current'))\ndef a(x):\n    return x\n",
    "src/synthpkg/u_relative_if.py": "from .pairs import pair_side as decl\n\nif True:\n\n    @decl(pair='u_relative_if', side='current')\n    def a(x):\n        return x\n",
    "src/synthpkg/u_relative_wrapped.py": "from .pairs import pair_side as decl\n\n\ndef keep(d):\n    return d\n\n\n@keep(decl(pair='u_relative_wrapped', side='current'))\ndef a(x):\n    return x\n",
}
READ_PAIRS = {"plain", "method", "module_as", "import_as", "from_pkg", "from_pkg_as", "dotted", "relative"}


def test_every_pair_side_call_the_scan_does_not_read_is_named_by_its_file_and_line(tmp_path):
    """Property: each `pair_side` declaration form is either a scanned pair or, by node identity, a finding at its own line.

    Class: module-level, method, attribute, import-alias, `from {package} import pairs`, dotted and
    relative-alias declarations, each asserted SCANNED by its pair id; and every call that is not
    one of them — under a module-level `if` or `try`, inside a function, on a nested class's
    method, applied by hand, inside another decorator's expression or a conditional decorator, and
    the relative alias under an `if` or wrapped — each asserted a finding at its file and line.
    Why: every one of these registers at import, so a call the scan neither reads nor names is a
    pair the scan, the closure and the detector never see. The wrapped and conditional forms sit
    on a module-level function's decorator list, which separates an identity rule from a
    positional one; the relative alias carries `module="pairs"` at `level=1`, which separates a
    resolved import from a test on the module name's suffix. Killed by: comparing decorator
    positions instead of node identity, recognising `pair_side` only by its written name, or
    ignoring relative imports. Second correct implementation: collecting every `pair_side` call
    first and subtracting the read ones by line and column rather than by node identity.
    """
    package_dir = _tree(tmp_path, DECLARATIONS)
    scan = pair_scan.scan_pairs(package_dir, PKG)
    assert set(scan.pairs) >= READ_PAIRS and not set(scan.pairs) - READ_PAIRS, sorted(scan.pairs)
    unread = sorted(f for f in scan.findings if "outside the decorator list" in f)
    expected = sorted(
        f"src/synthpkg/{Path(path).name}:{next(i for i, line in enumerate(source.splitlines(), 1) if repr(pair) in line)}"
        for path, source in DECLARATIONS.items()
        if (pair := Path(path).stem).startswith("u_")
    )
    assert [f.split(": ", 1)[0] for f in unread] == expected, unread
    assert not any(f.startswith(f"src/synthpkg/read_{p}") for f in scan.findings for p in READ_PAIRS), scan.findings


def test_an_aliased_pair_side_decorator_is_not_followed_into_the_registry(tmp_path):
    """Property: a side declared through an import alias of `pair_side` does not reach `pair_side`'s own code, nor list it.

    Class: a `pair_side` decorator written through any alias the scan recognises, here
    `from .pairs import pair_side as decl` where the package's `pairs.decl` target is a real
    function the closure could follow. Why: the recording decorator only registers its function,
    so following it would put the registry's own code into every pair's closure and move every
    fingerprint when the registry changes; the alias must be treated exactly as the written name
    `pair_side` is. Killed by: stripping recording decorators by written name only. Second
    correct implementation: stripping by resolving each decorator to the package's `pair_side`
    definition rather than by the scan's recognition test.
    """
    package_dir = _tree(tmp_path, DECLARATIONS)
    result = pair_scan.closure(package_dir / "read_relative.py", "a", package_dir, PKG)
    assert set(result.functions) == {"synthpkg.read_relative:a"} and result.unresolved == [], result
