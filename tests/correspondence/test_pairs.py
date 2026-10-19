"""Tests of the pair registry: the decorator leaves its function alone, refuses a duplicate side, and keeps its registry.

The registry is filled at import and lives for the whole interpreter, so a test that registered
a pair in this process would leave it there for every later test, including any test that
compares the registry with a scan of the package. So every registration here happens either in
a CHILD interpreter, over a module written under the test's temporary directory, or in this
process against a fresh registry swapped in for the test's duration. No test asserts what the
real in-process registry contains, because another test in the session may already have
imported a module that registers.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from multidriver_swg import pairs

pytestmark = pytest.mark.state_check

# The child imports the same package this process imports, from the same checkout.
SRC = Path(pairs.__file__).resolve().parents[1]
HEAD = "from multidriver_swg.pairs import CURRENT, PRE_REFACTOR, pair_side\n\n\n"
CHILD = """
import importlib, json, sys
from multidriver_swg import pairs
try:
    for name in sys.argv[1:]:
        importlib.import_module(name)
except Exception as error:
    print(json.dumps({"error": type(error).__name__, "message": str(error)}))
    raise SystemExit(3)
print(json.dumps({"pairs": {p: sorted(s) for p, s in pairs.registered_pairs().items()}}))
"""


def _child(tmp_path: Path, modules: dict[str, str]) -> dict:
    """Write `modules` under `tmp_path`, import them in that order in a fresh interpreter, and return its report."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    for name, source in modules.items():
        (tmp_path / f"{name}.py").write_text(HEAD + source, encoding="utf-8")
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(tmp_path), str(SRC)])}
    run = subprocess.run([sys.executable, "-c", CHILD, *modules], cwd=tmp_path, env=env, capture_output=True, text=True)
    assert run.returncode in (0, 3), (run.returncode, run.stderr)
    return json.loads(run.stdout.strip().splitlines()[-1])


def _side(pair: str, side: str, name: str) -> str:
    """Source of one decorated function registered as `side` of `pair`."""
    return f"@pair_side(pair={pair!r}, side={side})\ndef {name}(x):\n    return x\n\n\n"


def test_the_decorator_returns_the_function_unchanged_and_records_it(monkeypatch):
    """Property: `pair_side` returns the very function it decorates, and the registry then holds that function.

    Class: any function registered as either side of any pair. Why: registration must not
    change what the function does or which object callers hold; a wrapper would put the
    registry's code on every call path of the new code it is meant only to record. Killed by:
    returning a wrapper, or not recording the function. Second correct implementation: a
    registry keyed by `(pair, side)` tuples that `registered_pairs` regroups by pair. The real
    registry is swapped for an empty one so this registration leaves nothing behind.
    """
    monkeypatch.setattr(pairs, "_REGISTRY", {})

    def legacy(x):
        """A stand-in pre-refactor side."""
        return x

    def current(x):
        """A stand-in current side."""
        return x

    assert pairs.pair_side(pair="p", side=pairs.PRE_REFACTOR)(legacy) is legacy
    assert pairs.pair_side(pair="p", side=pairs.CURRENT)(current) is current
    got = pairs.registered_pairs()
    assert got == {"p": {"pre_refactor": legacy, "current": current}}, got


def test_registered_pairs_is_a_copy_the_caller_may_change(monkeypatch):
    """Property: changing what `registered_pairs` returned changes neither the registry nor a later copy.

    Class: any change a caller makes to the returned mapping, at either level — a pair removed,
    a side replaced. Why: a check that compares the registry with a scan must not be able to
    alter what the next check reads. Killed by: returning the registry itself, or a copy that
    shares the inner per-pair dictionaries. Second correct implementation: a registry stored
    as `(pair, side)` keys that `registered_pairs` regroups into fresh per-pair dictionaries on
    every call.
    """
    monkeypatch.setattr(pairs, "_REGISTRY", {})
    pairs.pair_side(pair="p", side=pairs.CURRENT)(len)
    first = pairs.registered_pairs()
    first["p"]["current"] = print
    first.pop("p")
    assert pairs.registered_pairs() == {"p": {"current": len}}


def test_a_fresh_registry_is_empty(tmp_path):
    """Property: an interpreter that imports only the pair registry has registered no pair.

    Class: the registry as the package ships it, before any registering module is imported.
    Why: a registration made by the package's own import would be a pair no owning system
    declared, and it would count against `EXPECTED_PAIR_COUNT`. Killed by: any `pair_side`
    call reachable from importing `multidriver_swg.pairs`. Second correct implementation: a
    registry created lazily on the first registration instead of at import. Asserted in a
    child, where no other test's import can have filled the registry.
    """
    assert _child(tmp_path, {}) == {"pairs": {}}


def test_a_second_registration_of_one_side_of_one_pair_raises_at_import(tmp_path):
    """Property: importing a module that registers one side of one pair twice raises `DuplicatePairRegistration`.

    Class: two registrations with the same pair id and the same side, in one module or across
    two modules; and, as satisfying inputs, the same side of two pairs and both sides of one
    pair. Why: a later registration would otherwise replace the earlier one silently, and the
    pair would be compared against a function its author did not mean. Killed by: letting the
    later registration win, or refusing by pair alone so that a pair's two sides collide.
    Only the exception's CLASS is asserted, because the class is the registry's contract and
    its message is not: an assertion on wording would refuse a correct registry that quotes or
    orders its message differently. Second correct implementation: a registry whose message repr-quotes the pair,
    the side and both functions, which raises the same class on the same inputs.
    """
    one_module = _child(tmp_path / "one", {"twice": _side("p", "PRE_REFACTOR", "a") + _side("p", "PRE_REFACTOR", "b")})
    assert one_module["error"] == "DuplicatePairRegistration", one_module
    across = _child(tmp_path / "two", {"first": _side("p", "CURRENT", "a"), "second": _side("p", "CURRENT", "b")})
    assert across["error"] == "DuplicatePairRegistration", across
    satisfying = _child(
        tmp_path / "ok",
        {"ok": _side("p", "PRE_REFACTOR", "a") + _side("p", "CURRENT", "b") + _side("q", "PRE_REFACTOR", "c")},
    )
    assert satisfying == {"pairs": {"p": ["current", "pre_refactor"], "q": ["pre_refactor"]}}, satisfying


def test_an_unknown_side_raises_value_error_and_a_positional_call_raises_type_error(tmp_path):
    """Property: a side outside `SIDES` raises a plain `ValueError`, and a positional pair or side raises `TypeError`.

    Class: any side string other than the two named sides, and any call passing pair or side by
    position. Why: a misspelt side would register a function no check pairs with anything, and a
    positional call can swap pair and side unnoticed. Killed by: accepting any side, or making
    the arguments positional-or-keyword. Only the exception's CLASS is asserted, because the
    class is the registry's contract and its message is not. Second correct implementation: validating the side
    inside the returned decorator rather than when the decorator is built, with a message that
    names the side unquoted, which raises the same class at the same import.
    """
    unknown = _child(tmp_path / "unknown", {"unknown": _side("p", "'later'", "a")})
    assert unknown["error"] == "ValueError", unknown
    positional = _child(
        tmp_path / "positional", {"positional": "@pair_side('p', PRE_REFACTOR)\ndef a(x):\n    return x\n"}
    )
    assert positional["error"] == "TypeError", positional


def test_a_child_registration_leaves_this_processs_registry_as_it_was(tmp_path):
    """Property: running a child that registers a pair leaves this process's registry equal to what it was before.

    Class: every child-interpreter registration these tests make. Why: the tests prove the
    registry's refusals in children precisely so that this process's registry, which other
    checks in the session read, is untouched; if a child's registration reached it, that
    isolation would be false. Killed by: running the registering module in this process (an
    import or an exec here). Second correct implementation: a fork of this process instead of a
    fresh interpreter, which leaves the parent's registry untouched in the same way. Compared
    with itself, never with a literal, so it holds however many pairs earlier tests imported.
    """
    before = pairs.registered_pairs()
    child = _child(tmp_path, {"full": _side("isolated", "PRE_REFACTOR", "a") + _side("isolated", "CURRENT", "b")})
    assert child == {"pairs": {"isolated": ["current", "pre_refactor"]}}, child
    assert pairs.registered_pairs() == before
