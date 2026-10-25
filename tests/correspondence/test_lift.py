"""Tests of the lift runner: declarations, refusals before execution, line numbers, adapted copies and the lift digest.

Most tests build a small root in `tmp_path`: a corpus record naming one section, stored texts at the
paths the extraction store uses, and lift declarations in `tests/lifts/`. No git repository is needed,
because the runner reads stored texts by path. One test runs the real stored text of a real unit.

Every test class here is a NEW CAPABILITY: no runner existed, so a failure is shown by the wrong
implementation a docstring names.
"""

import hashlib
import sys
import types
from pathlib import Path

import pytest

from tests.correspondence import corpus_record, extent, extraction, fixtures, lift

pytestmark = pytest.mark.state_check

RECORD = 'rule = "corpus-record:1"\n\n[[section]]\nname = "stormy"\ndirectory = "legacy"\n'
HEAD = "from tests.correspondence.lift import declares_lift\n\n\n"
BASIC = (
    HEAD
    + '@declares_lift(unit="mod.py:1-3")\n'
    + "def declare(fixtures):\n"
    + '    """Supply the scale."""\n'
    + "    scale = 3\n"
    + '    return {"scale": scale}, lambda ns: ns["total"]\n'
)
FIXED_DECLARATION = '''@declares_lift(unit="mod.py:1-3")
def declare(fixtures):
    """Supply the scale."""
    scale = 3

    def reduce(ns):
        return {"total": ns["total"]}

    return {"scale": scale}, reduce
'''
PINNED_TREE_SHA256 = "8a9157edc92dbbeed4b669effc471d75b9337846db86afeca0cdd148772a901a"


def _root(
    tmp_path: Path, texts: dict[tuple[str, int, int], str] | None = None, lifts: dict[str, str] | None = None
) -> Path:
    """Build a root holding the record, the stored texts and the lift modules named."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / corpus_record.RECORD_NAME).write_text(RECORD, encoding="utf-8")
    for (file, first, last), text in (
        texts if texts is not None else {("mod.py", 1, 3): "total = scale * 2\n"}
    ).items():
        path = tmp_path / extraction.stored_path("stormy", file, first, last)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    for system, source in (lifts or {"sys_a": BASIC}).items():
        (tmp_path / lift.LIFTS).mkdir(parents=True, exist_ok=True)
        (tmp_path / lift.LIFTS / f"{system}.py").write_text(source, encoding="utf-8")
    return tmp_path


def _declaration(root: Path, system: str = "sys_a") -> lift.LiftDeclaration:
    (declaration,) = [d for d in lift.discover(root) if d.system == system]
    return declaration


def test_a_discovered_declaration_runs_its_stored_text_and_returns_the_reduction(tmp_path):
    """Property: the declared bindings are the namespace the text runs in, and the reduction reads the finished namespace.

    Class: a one-range unit whose text reads one declared name and binds one result. Why it is
    required: this is the lift's whole job. Killed by: ignoring the bindings (a NameError), or
    returning the namespace instead of the reduction. Second correct implementation: building the
    namespace with `types.SimpleNamespace` and `vars()` passes.
    """
    root = _root(tmp_path)
    assert lift.run_lift(_declaration(root)) == 6


def test_texts_run_in_order_in_one_namespace_with_the_shifted_legacy_lines(tmp_path):
    """Property: a unit with two ranges runs them in order in one namespace, and compiled code reports legacy file and lines.

    Class: a split key whose second range reads a name the first binds, with the first range at lines
    10 to 12. Why it is required: a split unit is one unit, and a coverage run names legacy lines.
    Killed by: one namespace per range, or compiling every range from line 1. Second correct
    implementation: concatenating the texts with blank lines to keep the numbers passes.
    """
    source = HEAD + (
        '@declares_lift(unit="mod.py:10-12,20-21")\n'
        "def declare(fixtures):\n"
        '    """Supply the scale."""\n'
        '    return {"scale": 2}, lambda ns: (ns["f"].__code__.co_filename, ns["f"].__code__.co_firstlineno, ns["g"](1))\n'
    )
    texts = {
        ("mod.py", 10, 12): "base = scale\n\ndef f():\n    return base\n",
        ("mod.py", 20, 21): "def g(x):\n    return x + base\n",
    }
    root = _root(tmp_path, texts, {"sys_a": source})
    assert lift.run_lift(_declaration(root)) == ("legacy/mod.py", 12, 3)


def test_the_real_stored_text_runs_unchanged_and_reports_legacy_line_24(tmp_path):
    """Property: the real stored text of `__ref_ams_functions.py:23-30` runs with a stub bound to `stats`, and its function's code reports the legacy path and first line 24.

    Class: the real stored extraction, read from this checkout. Why it is required: lets a coverage
    measurement name legacy lines without translation. Killed by: compiling under the stored copy's own
    path, or without the line shift (first line 2). Second correct implementation: compiling the text
    after prefixing 22 newlines passes. Not established: that the stub's `boxcox` is the library's.
    """
    root = extraction.REPO_ROOT
    stats = types.SimpleNamespace(boxcox=lambda data: (data, 0.5))
    declaration = lift.LiftDeclaration(
        unit="__ref_ams_functions.py:23-30",
        part=None,
        form="unchanged",
        fixtures=(),
        draws="none",
        function=lambda _fixtures: ({"stats": stats}, lambda ns: ns["boxcox_transformation"]),
        system="real",
        path=root / "tests" / "lifts" / "real.py",
        root=root,
    )
    function = lift.run_lift(declaration)
    directory = extent._section(root)["directory"].rstrip("/")
    assert function.__code__.co_filename == f"{directory}/__ref_ams_functions.py"
    assert function.__code__.co_firstlineno == 24

    class Data:
        """A stand-in for the series the legacy function receives."""

        def min(self):
            """Return a positive minimum, so that the function adds no shift."""
            return 1.0

    data, lmbda, shift = function(Data())
    assert (lmbda, shift) == (0.5, 0)


def test_a_name_nothing_binds_is_refused_before_anything_executes(tmp_path):
    """Property: a text that reads an unbound, unbuiltin, undeclared name is refused naming it.

    Class: a two-range unit whose second range reads `missing`. Why it is required: the refusal must
    name the name, whichever range reads it. Killed by: letting the read surface as a bare NameError at
    execution, which names the name only after the first range has run. Second correct implementation:
    compiling all, then checking, then executing passes. Not established: that no statement ran before
    the refusal; the companion test in `test_lift_review.py` carries that property.
    """
    source = HEAD + (
        '@declares_lift(unit="mod.py:1-1,5-5")\n'
        "def declare(fixtures):\n"
        '    """Supply a list."""\n'
        "    seen = []\n"
        '    return {"seen": seen}, lambda ns: seen\n'
    )
    root = _root(
        tmp_path, {("mod.py", 1, 1): "seen.append(1)\n", ("mod.py", 5, 5): "x = missing + 1\n"}, {"sys_a": source}
    )
    declaration = _declaration(root)
    with pytest.raises(lift.LiftError, match="missing"):
        lift.run_lift(declaration)


def test_a_declared_binding_never_read_is_refused_by_name(tmp_path):
    """Property: a binding the text never reads is refused naming it.

    Class: a declaration supplying `scale` and `spare`, a text reading only `scale`. Why it is
    required: an unread binding is a sign of a wrong declaration. Killed by: checking reads only.
    Second correct implementation: comparing the declared set with the read set passes.
    """
    source = BASIC.replace('{"scale": scale}', '{"scale": scale, "spare": 1}')
    root = _root(tmp_path, lifts={"sys_a": source})
    with pytest.raises(lift.LiftError, match="spare"):
        lift.run_lift(_declaration(root))


def test_a_star_import_in_a_text_is_refused(tmp_path):
    """Property: a text with a star-import is refused, since the names it binds cannot be read from the text.

    Class: `from os.path import *`. Why it is required: the name check would otherwise pass names it
    cannot see. Killed by: ignoring the instruction. Second correct implementation: refusing any
    `import *` at the syntax-tree level passes.
    """
    root = _root(tmp_path, {("mod.py", 1, 3): "from os.path import *\ntotal = scale\n"})
    with pytest.raises(lift.LiftError, match="star-import"):
        lift.run_lift(_declaration(root))


def test_names_the_text_binds_itself_and_builtins_are_not_refused(tmp_path):
    """Property: a text may read names it binds (even after use order) and builtins without declaring them.

    Class: a text using `len`, a name bound by an earlier statement, and an import it binds. Why it is
    required: the check must refuse only genuine gaps. Killed by: treating every non-declared name as
    unknown. Second correct implementation: collecting reads and binds from the syntax tree passes.
    """
    text = "import math\nitems = [1, 2, 3]\ntotal = len(items) * scale + math.floor(1.5)\n"
    root = _root(tmp_path, {("mod.py", 1, 3): text})
    assert lift.run_lift(_declaration(root)) == 10


@pytest.mark.parametrize(
    ("body", "name"),
    [
        ("import numpy as np\n\nHELPER = 1\n", "HELPER"),
        ("def helper():\n    return 1\n", "helper"),
    ],
    ids=["constant", "function"],
)
def test_a_declaration_reading_a_module_level_name_is_refused_naming_it(tmp_path, body, name):
    """Property: a declaration that reads a name its module binds at top level is refused at discovery, naming the name.

    Class: a top-level constant and a top-level helper function, each read from the declaration's body.
    Why it is required: the digest covers the declaration only, so a helper outside it could change
    unseen. Killed by: not checking, or checking only the outer function's own names. Second correct
    implementation: resolving the reads against the module's binding list passes.
    """
    source = (
        HEAD
        + body
        + (
            '\n\n@declares_lift(unit="mod.py:1-3")\n'
            "def declare(fixtures):\n"
            f'    return {{"scale": {name}}}, lambda ns: ns["total"]\n'
        )
    )
    root = _root(tmp_path, lifts={"sys_a": source})
    with pytest.raises(lift.LiftError, match=name):
        lift.discover(root)


def test_a_nested_helper_and_a_library_import_are_not_refused(tmp_path):
    """Property: a helper nested inside the declaration, and a library imported from outside the checkout, are admitted.

    Class: a nested function and lambda reading nested names, plus `import numpy as np` read in the
    body. Why it is required: these are the forms the rule tells authors to use. Killed by: refusing
    every import. Second correct implementation: allowing a list of library names passes.
    """
    source = (
        "import numpy as np\n" + HEAD + '@declares_lift(unit="mod.py:1-3")\n'
        "def declare(fixtures):\n"
        "    def helper(x):\n"
        "        return np.float64(x)\n"
        '    return {"scale": helper(3)}, lambda ns: float(ns["total"])\n'
    )
    root = _root(tmp_path, lifts={"sys_a": source})
    assert lift.run_lift(_declaration(root)) == 6.0


def test_findings_name_checkout_imports_relative_imports_and_the_declaration_itself(tmp_path):
    """Property: an import of this checkout's packages, a relative import and a self-reference are findings; a standard-library import is not.

    Class: four module sources checked as text. Why it is required: these are the forms a declaration
    may not use, and a relative import cannot be exercised by running the module. Killed by:
    admitting any import. Second correct implementation: resolving the module with `find_spec` and
    comparing its location with the checkout passes.
    """
    template = "{imp}\n\n\ndef declare(fixtures):\n    return ({name},), None\n"
    root = extraction.REPO_ROOT
    refused = {
        "from tests.correspondence import compare": "compare",
        "from . import sibling": "sibling",
        "import scripts": "scripts",
        "import _refactor_tools": "_refactor_tools",
    }
    for statement, name in refused.items():
        findings = lift.declaration_findings(template.format(imp=statement, name=name), "declare", root)
        assert any(name in f for f in findings), (statement, findings)
    assert lift.declaration_findings(template.format(imp="import math", name="math"), "declare", root) == []
    # Another checkout: the package rule, not the location rule, refuses this checkout's own packages.
    elsewhere = lift.declaration_findings(
        template.format(imp="import multidriver_swg", name="multidriver_swg"), "declare", tmp_path
    )
    assert any("multidriver_swg" in f for f in elsewhere), elsewhere
    assert any(
        "declare" in f
        for f in lift.declaration_findings("def declare(fixtures):\n    return declare\n", "declare", root)
    )


def test_two_declarations_of_one_unit_are_refused_naming_both(tmp_path):
    """Property: discovery refuses two declarations of one (unit, part), naming both functions.

    Class: two functions with the same mark in one module. Why it is required: the runner could not say
    which to run. Killed by: keeping the last. Second correct implementation: keying a mapping and
    raising on a present key passes.
    """
    source = BASIC + "\n\n" + BASIC.split("\n\n\n", 1)[1].replace("def declare", "def declare_again")
    root = _root(tmp_path, lifts={"sys_a": source})
    with pytest.raises(lift.LiftError, match="declare_again"):
        lift.discover(root)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"unit": "no key"},
        {"unit": "a.py:1-2", "form": "other"},
        {"unit": "a.py:1-2", "draws": "maybe"},
        {"unit": "a.py:1-2", "fixtures": [1]},
    ],
)
def test_a_malformed_mark_is_refused(kwargs):
    """Property: a unit that is not a canonical key, an unknown form or draws value, or a non-string fixture is refused.

    Class: four malformed marks. Why it is required: a bad mark would otherwise surface far from its
    cause. Killed by: accepting any value. Second correct implementation: validating in the discovery
    walk instead passes for the same marks.
    """
    with pytest.raises(lift.LiftError):
        lift.declares_lift(**kwargs)


def test_a_declared_fixture_is_loaded_and_passed_by_name(tmp_path):
    """Property: the fixtures a declaration names reach it loaded and digest-checked, as one dict keyed by name.

    Class: a fixture generated into the temporary root by the real fixture machinery. Why it is
    required: lifts run on committed synthetic inputs. Killed by: passing nothing, or a list. Second
    correct implementation: passing keyword arguments named by fixture would pass this one-fixture case.
    """
    source = HEAD + (
        '@declares_lift(unit="mod.py:1-3", fixtures=["numbers"])\n'
        "def declare(fixtures):\n"
        '    return {"scale": int(fixtures["numbers"].sum())}, lambda ns: ns["total"]\n'
    )
    root = _root(tmp_path, lifts={"sys_a": source})
    generators = root / fixtures.GENERATORS
    generators.mkdir(parents=True)
    (generators / "sys_a.py").write_text(
        "import numpy as np\n"
        "from tests.correspondence.fixtures import generates_fixture\n\n\n"
        '@generates_fixture(name="numbers", seed=1)\n'
        "def numbers(rng):\n"
        "    return np.array([1, 2, 3])\n",
        encoding="utf-8",
    )
    fixtures.generate("sys_a", "numbers", root=root)
    assert lift.run_lift(_declaration(root)) == 12


def test_the_digest_moves_with_logic_inputs_and_texts_and_not_with_formatting(tmp_path):
    """Property: the lift digest ignores formatting, a docstring and comments of the declaration, and moves with its logic, its texts, its fixtures' names and the runner's version.

    Class: five variants of one lift against the base. Why it is required: a stored value is keyed on
    this digest, so a reformatting must not read as a stale value and an edit must. Killed by: hashing
    the source text, or omitting the stored-text digests. Second correct implementation: hashing
    `ast.dump` passes on one interpreter and differs across minor versions.
    """
    base = lift.lift_digest(_declaration(_root(tmp_path / "a")))
    assert base.startswith("lift:1:")
    reformatted = BASIC.replace("    scale = 3\n", "    # a comment\n    scale  =  3\n").replace(
        '"""Supply the scale."""', '"""Another docstring."""'
    )
    assert lift.lift_digest(_declaration(_root(tmp_path / "b", lifts={"sys_a": reformatted}))) == base
    logic = BASIC.replace("scale = 3", "scale = 4")
    assert lift.lift_digest(_declaration(_root(tmp_path / "c", lifts={"sys_a": logic}))) != base
    text = lift.lift_digest(_declaration(_root(tmp_path / "d", {("mod.py", 1, 3): "total = scale * 3\n"})))
    assert text != base
    named = BASIC.replace('unit="mod.py:1-3"', 'unit="mod.py:1-3", fixtures=["x"]')
    assert lift.lift_digest(_declaration(_root(tmp_path / "e", lifts={"sys_a": named}))) != base


def test_the_digest_follows_the_runner_version(tmp_path, monkeypatch):
    """Property: raising `RUNNER_VERSION` changes the label's version and its digest.

    Class: the same lift under versions 1 and 2. Why it is required: a change to how the runner builds
    a namespace must move every lift digest. Killed by: leaving the version out of the record. Second
    correct implementation: mixing it into the hash only passes the digest half.
    """
    declaration = _declaration(_root(tmp_path))
    before = lift.lift_digest(declaration)
    monkeypatch.setattr(lift, "RUNNER_VERSION", 2)
    after = lift.lift_digest(declaration)
    assert after.startswith("lift:2:") and after.split(":")[2] != before.split(":")[2]


def test_the_canonical_tree_of_a_fixed_declaration_is_pinned_to_a_committed_digest():
    """Property: the canonical serialisation of one fixed declaration source hashes to a committed value on the running interpreter.

    Class: a source with a docstring, a nested function and a default-free signature. Why it is required:
    `ast.dump` gives different digests on different minor versions; this serialisation must not, and if
    the syntax-tree shape of an interpreter changes it, this named test fails and not every stored value.
    Killed by: serialising positions, empty fields or the docstring. Second correct implementation: any
    serialisation with the same omissions and the same field order passes only by reproducing this value.
    """
    digest = hashlib.sha256(lift.canonical_tree(FIXED_DECLARATION).encode("utf-8")).hexdigest()
    assert digest == PINNED_TREE_SHA256, f"canonical tree digest on Python {sys.version_info[:2]} is {digest}"


def test_run_lift_output_on_a_fixed_synthetic_lift_is_pinned(tmp_path):
    """Property: running one fixed synthetic lift yields a committed value; if it moves, raise `RUNNER_VERSION`.

    Class: a split-free unit at lines 10 to 14 with a comprehension and a function. Why it is required: a
    change to how the runner builds the namespace or shifts lines moves every lift digest, and must read
    as a stale lift and not as a moved environment. Killed by: changing the namespace's contents or the
    line shift without a version change. Second correct implementation: none other than the runner itself.
    """
    text = "total = scale * 2\nsteps = [i * scale for i in range(3)]\n\ndef f(x):\n    return x + total\n"
    source = HEAD + (
        '@declares_lift(unit="mod.py:10-14")\n'
        "def declare(fixtures):\n"
        '    return {"scale": 3}, lambda ns: (ns["total"], ns["steps"], ns["f"](1), ns["f"].__code__.co_filename, ns["f"].__code__.co_firstlineno)\n'
    )
    root = _root(tmp_path, {("mod.py", 10, 14): text}, {"sys_a": source})
    expected = (6, [0, 3, 6], 7, "legacy/mod.py", 13)
    assert lift.run_lift(_declaration(root)) == expected, "the runner's output moved: raise lift.RUNNER_VERSION"


def test_run_lift_records_every_code_object_it_compiles(tmp_path):
    """Property: after a run, `compiled_code_objects()` holds the unit's code object under the legacy path.

    Class: one run of a one-range unit. Why it is required: the check that no test runs a legacy file reads
    this list. Killed by: not recording, or recording before the refusal checks (a refused unit must add none).
    Second correct implementation: recording after execution passes.
    """
    declaration = _declaration(_root(tmp_path))
    before = len(lift.compiled_code_objects())
    lift.run_lift(declaration)
    added = lift.compiled_code_objects()[before:]
    assert [c.co_filename for c in added] == ["legacy/mod.py"]


def _adapted_root(tmp_path: Path, adapted: str, diff: str | None = None) -> Path:
    """Build a root with an adapted lift of `mod.py:1-4` whose copy and diff are those given or computed."""
    stored = "a = 1\nb = scale\nc = 3\ntotal = a + b + c\n"
    source = BASIC.replace('unit="mod.py:1-3"', 'unit="mod.py:1-4", form="adapted"')
    root = _root(tmp_path, {("mod.py", 1, 4): stored}, {"sys_a": source})
    directory = root / lift.ADAPTED / "sys_a" / "mod"
    directory.mkdir(parents=True)
    (directory / "1-4.txt").write_text(adapted, encoding="utf-8")
    computed = lift._adapted_diff(stored.encode(), adapted.encode())
    (directory / "1-4.diff").write_text(computed if diff is None else diff, encoding="utf-8")
    return root


def test_an_adapted_lift_runs_its_copy_under_its_own_path_and_maps_unchanged_lines(tmp_path):
    """Property: an adapted copy is compiled under its own path at its own lines, and `legacy_lines` maps its unchanged lines to legacy numbers, omitting changed ones.

    Class: a copy that changes line 2 and inserts a line before line 4. Why it is required: coverage must
    name legacy lines where the adapted copy still equals the original, and none where it differs. Killed by:
    compiling under the legacy path, or mapping a changed line. Second correct implementation: aligning with
    `difflib.SequenceMatcher` blocks passes.
    """
    adapted = "a = 1\nb = scale + 0\nc = 3\nextra = 0\ntotal = a + b + c\n"
    root = _adapted_root(tmp_path, adapted)
    declaration = _declaration(root)
    assert lift.run_lift(declaration) == 7
    assert lift.compiled_code_objects()[-1].co_filename == "tests/lifts/adapted/sys_a/mod/1-4.txt"
    assert lift.legacy_lines(declaration) == {"1-4": {1: 1, 3: 3, 5: 4}}


def test_an_adapted_lift_whose_stored_diff_is_not_the_diff_is_refused(tmp_path):
    """Property: an edited adapted copy or an edited diff is refused, naming the diff file, before anything runs.

    Class: a diff that does not describe the pair. Why it is required: the reviewed difference is the
    only evidence the adapted copy is a small departure. Killed by: not recomputing the diff. Second
    correct implementation: comparing digests of the recomputed and stored diffs passes.
    """
    root = _adapted_root(tmp_path, "a = 1\nb = scale + 0\nc = 3\ntotal = a + b + c\n", diff="--- stored\n+++ adapted\n")
    with pytest.raises(lift.LiftError, match=r"1-4\.diff"):
        lift.run_lift(_declaration(root))


def test_the_namespace_holds_only_the_declared_bindings(tmp_path):
    """Property: the names a text can see as globals are the declared bindings plus the dunder entries, and nothing else.

    Class: a text that lists its own globals before binding anything. Why it is required: the unit must
    run on declared bindings only, so a leaked helper or import would hide a missing declaration. Killed
    by: seeding the namespace with extra names. Second correct implementation: running with a mapping that
    raises on any undeclared lookup passes.
    """
    text = 'names = sorted(k for k in globals() if not k.startswith("__"))\nunused = scale\n'
    root = _root(tmp_path, {("mod.py", 1, 3): text}, {"sys_a": BASIC.replace('ns["total"]', 'ns["names"]')})
    assert lift.run_lift(_declaration(root)) == ["scale"]
