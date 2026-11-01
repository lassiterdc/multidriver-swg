"""Tests of the side selector: it hands back the registered function of the chosen side and refuses what it cannot honour.

The selector is a lookup over the pair registry, so every test builds its own registry. The real
registry lives for the whole interpreter and other tests compare it with a scan of the package,
so an autouse fixture swaps in an empty registry for each test and restores the real one after
it; no test here registers anything in the real registry, and none asserts what that registry
holds. Each test names the wrong implementation it is red under, because a check that cannot
fail for the reason it claims constrains nothing.
"""

from __future__ import annotations

import contextlib
import dataclasses

import pytest

from multidriver_swg import pairs
from multidriver_swg.dispatch import (
    SideSelection,
    UnknownPairError,
    UnregisteredSideError,
    function_for,
    selected_sides,
    side_for,
)

pytestmark = pytest.mark.state_check

PRE = pairs.PRE_REFACTOR
CUR = pairs.CURRENT


@pytest.fixture(autouse=True)
def fresh_registry(monkeypatch):
    """Run every test of this module against an empty registry that is discarded with the test."""
    monkeypatch.setattr(pairs, "_REGISTRY", {})


def _register(pair: str, *sides: str) -> dict[str, object]:
    """Register one distinct stand-in function per side of `pair` in the swapped-in registry; return them by side."""
    made: dict[str, object] = {}
    for side in sides:

        def stand_in(x, _label=f"{pair}:{side}"):
            """A stand-in function whose identity, not its behaviour, is what a test reads."""
            return _label

        pairs.pair_side(pair=pair, side=side)(stand_in)
        made[side] = stand_in
    return made


def test_the_default_selection_hands_back_the_current_function_itself():
    """Property: the default selection returns the very function registered as the current side.

    Class: any registered pair with both sides, under a selection that sets nothing. Why: a run
    that selected nothing must run the new code, and a caller that receives a wrapper or a copy
    cannot be compared with the function the pair registered. Killed by: defaulting to the
    pre-refactor side, or returning a wrapper around the function. Second correct implementation:
    a selector that reads `registered_pairs()` once per call and indexes it, instead of looking
    each side up through a helper, which returns the same object.
    """
    got = _register("p", PRE, CUR)
    assert function_for("p", SideSelection()) is got[CUR]
    assert side_for("p", SideSelection()) == CUR


def test_a_run_wide_pre_refactor_selection_hands_back_the_pre_refactor_function():
    """Property: a selection whose default side is pre-refactor returns the pre-refactor function of every pair.

    Class: any registered pair with both sides, under a run-wide pre-refactor default; the
    satisfying input is the current default of the previous test, which must not return this one.
    Why: the run-wide value is how a run reproduces the legacy code everywhere. Killed by:
    ignoring `default_side`, or returning the current function whenever a pair has both sides.
    Second correct implementation: a selector that stores the default as an attribute of a plain
    class with `__eq__` instead of a dataclass and looks the side up the same way.
    """
    got = _register("p", PRE, CUR)
    selection = SideSelection(default_side=PRE)
    assert function_for("p", selection) is got[PRE]
    assert function_for("p", selection) is not got[CUR]


def test_a_per_pair_override_switches_only_the_pair_it_names():
    """Property: an override changes the named pair's side and leaves every other pair on the default side.

    Class: two or more registered pairs, one override in either direction (pre-refactor against a
    current default, and current against a pre-refactor default). Why: measuring one issue in one
    old method needs that method beside the current code of everything else. Killed by: applying
    an override to every pair, or ignoring the override. Second correct implementation: a selector
    that builds a complete pair-to-side dict first and indexes it, which gives the same functions.
    """
    p = _register("p", PRE, CUR)
    q = _register("q", PRE, CUR)
    one = SideSelection(per_pair={"p": PRE})
    assert function_for("p", one) is p[PRE]
    assert function_for("q", one) is q[CUR]
    assert selected_sides(one) == {"p": PRE, "q": CUR}
    inverse = SideSelection(default_side=PRE, per_pair={"p": CUR})
    assert function_for("p", inverse) is p[CUR]
    assert function_for("q", inverse) is q[PRE]
    assert selected_sides(inverse) == {"p": CUR, "q": PRE}


def test_an_override_for_an_unregistered_pair_is_refused_by_both_lookups():
    """Property: an override naming a pair the registry does not hold raises `UnknownPairError` naming it.

    Class: any override key absent from the registry, whatever side it asks for and whichever
    pair is looked up; the satisfying input is the same selection over a registry that holds the
    key. Why: a misspelt identifier that was ignored would run the default side for the pair its
    author meant to switch, and the run would say nothing. Killed by: ignoring unknown override
    keys, or refusing only in one of the two lookups. Second correct implementation: validating
    the overrides once when the registry is read, in a helper both lookups call, with a message
    that lists every unknown key instead of the first.
    """
    _register("p", PRE, CUR)
    selection = SideSelection(default_side=PRE, per_pair={"typo": CUR})
    with pytest.raises(UnknownPairError, match="typo"):
        selected_sides(selection)
    with pytest.raises(UnknownPairError, match="typo"):
        function_for("p", selection)
    _register("typo", PRE, CUR)
    assert selected_sides(selection) == {"p": PRE, "typo": CUR}


def test_asking_for_an_unregistered_pair_is_refused_and_a_registered_one_is_not():
    """Property: `function_for` raises `UnknownPairError` for a pair the registry does not hold.

    Class: any pair identifier absent from the registry, including one that differs from a
    registered identifier only in case; the satisfying input is the registered identifier. Why:
    an unknown pair has no function on either side, and returning `None` or the other pair's
    function would be a wrong answer the caller cannot tell from a right one. Killed by: returning
    `None`, or matching identifiers case-insensitively. Second correct implementation: a selector
    that catches `KeyError` from an index and re-raises it as `UnknownPairError`.
    """
    got = _register("p", PRE, CUR)
    with pytest.raises(UnknownPairError, match="'missing'"):
        function_for("missing", SideSelection())
    with pytest.raises(UnknownPairError):
        function_for("P", SideSelection())
    assert function_for("p", SideSelection()) is got[CUR]


def test_a_selected_side_with_no_registered_function_is_refused_without_falling_back():
    """Property: choosing a side a pair has no function for raises `UnregisteredSideError`; the other side is never used.

    Class: a pair registered with only its current side under a pre-refactor default, and a pair
    registered with only its pre-refactor side under the current default, read through
    `function_for` and through `selected_sides`; the satisfying input is a pair with both sides.
    Why: a fallback would run the current code in a run that was asked to reproduce the legacy
    code, and its record would still say the legacy side ran. Killed by: returning the other
    side's function, or omitting the pair from `selected_sides`. Second correct implementation:
    one that tests membership with `in` before indexing instead of catching `KeyError`.
    """
    _register("only_current", CUR)
    _register("only_legacy", PRE)
    wants_legacy = SideSelection(default_side=PRE)
    with pytest.raises(UnregisteredSideError, match="only_current"):
        function_for("only_current", wants_legacy)
    with pytest.raises(UnregisteredSideError, match="only_current"):
        selected_sides(wants_legacy)
    with pytest.raises(UnregisteredSideError, match="only_legacy"):
        function_for("only_legacy", SideSelection())
    both = _register("both", PRE, CUR)
    assert function_for("both", wants_legacy) is both[PRE]


def test_a_side_outside_the_two_sides_is_refused_when_the_selection_is_built():
    """Property: a default side or an override side other than the two registry sides raises `ValueError` naming it.

    Class: any string that is not `pre_refactor` or `current`, given as the default side or as an
    override value; the satisfying input is each of the two sides. Why: a misspelt side would
    otherwise fail at the first lookup, far from the line that wrote it, or select nothing.
    Killed by: accepting any string, or validating only the default. Second correct
    implementation: validating inside `side_for` as well as at construction, which refuses the
    same value at an earlier line.
    """
    with pytest.raises(ValueError, match="legacy"):
        SideSelection(default_side="legacy")
    with pytest.raises(ValueError, match="legacy"):
        SideSelection(per_pair={"p": "legacy"})
    assert SideSelection(default_side=PRE).default_side == PRE
    assert SideSelection(per_pair={"p": CUR}).per_pair["p"] == CUR


def test_a_built_selection_cannot_be_changed_and_ignores_later_changes_to_the_dict_it_was_given():
    """Property: assigning into a selection's overrides raises `TypeError`, and mutating the source dict leaves it as built.

    Class: any change to the selection after construction, through its fields or through the
    dict passed in; the satisfying input is an unmodified selection reading back the same value.
    Why: a selection a run has recorded must still mean the same thing when the run ends. Killed
    by: storing the caller's dict, or a mutable copy. Second correct implementation: storing the
    overrides as a sorted tuple of pairs and answering lookups from it.
    """
    given = {"p": PRE}
    selection = SideSelection(per_pair=given)
    given["p"] = CUR
    given["q"] = PRE
    assert dict(selection.per_pair) == {"p": PRE}
    with pytest.raises(TypeError):
        selection.per_pair["p"] = CUR  # type: ignore[index]
    with pytest.raises(dataclasses.FrozenInstanceError):
        selection.default_side = PRE  # type: ignore[misc]
    assert dict(selection.per_pair) == {"p": PRE}
    assert selection.default_side == CUR


def test_an_empty_registry_selects_nothing_and_knows_no_pair():
    """Property: with no pair registered, `selected_sides` is empty and `function_for` raises `UnknownPairError`.

    Class: the registry as the package ships it before any pair is registered, under the default
    selection and under a run-wide pre-refactor one. Why: a run on the package as it stands has
    no pair to switch, and the selector must say so rather than raise or invent an entry. Killed
    by: raising on an empty registry, or returning a placeholder entry. Second correct
    implementation: building the result from `registered_pairs()` with a comprehension that is
    empty when the registry is.
    """
    assert selected_sides(SideSelection()) == {}
    assert selected_sides(SideSelection(default_side=PRE)) == {}
    with pytest.raises(UnknownPairError):
        function_for("x", SideSelection())


def test_selected_sides_lists_every_registered_pair_in_identifier_order():
    """Property: the result has one entry per registered pair, current-side pairs included, ordered by identifier.

    Class: pairs registered in an order that is not their sorted order, under a run-wide
    pre-refactor selection, a one-override selection and the all-current default. Why: a record
    built from it must list every pair so that an absent pair means an unregistered one, and
    must not depend on import order. Killed by: omitting current-side pairs, or keeping
    registration order. Second correct implementation: building the dict in registration order
    and sorting its items once at the end.
    """
    for pair in ("c", "a", "b"):
        _register(pair, PRE, CUR)
    assert list(selected_sides(SideSelection(default_side=PRE)).items()) == [("a", PRE), ("b", PRE), ("c", PRE)]
    assert list(selected_sides(SideSelection(per_pair={"b": PRE})).items()) == [("a", CUR), ("b", PRE), ("c", CUR)]
    assert list(selected_sides(SideSelection()).items()) == [("a", CUR), ("b", CUR), ("c", CUR)]


def test_the_registry_is_read_at_each_call_and_never_copied_at_import():
    """Property: a lookup sees the registry as it is at the call, so a registry put in place after the import is the one read.

    Class: two different registries consulted by the same selection and the same module in
    sequence. Why: a copy taken when the module was imported would answer from the registry that
    existed then, which in a test is the real one and in a run is whatever had been imported by
    that moment. Killed by: caching `registered_pairs()` at module level or per selection.
    Second correct implementation: calling `pairs.registered_pairs()` through a module attribute
    that tests replace, which reads the same live registry.
    """
    first = _register("p", PRE, CUR)
    assert function_for("p", SideSelection(default_side=PRE)) is first[PRE]
    pairs._REGISTRY.clear()
    second = _register("p", PRE, CUR)
    assert function_for("p", SideSelection(default_side=PRE)) is second[PRE]
    assert second[PRE] is not first[PRE]


def test_side_for_answers_without_consulting_the_registry():
    """Property: `side_for` returns the override or the default side for any identifier, registered or not.

    Class: identifiers the empty registry does not hold, with and without an override. Why: it is
    the pure half of the selector that a record builder may call before the registry is full;
    the checking half is `function_for` and `selected_sides`. Killed by: making `side_for`
    refuse unregistered pairs. Second correct implementation: a selector that resolves the
    override with `dict.get` on a plain dict instead of a read-only mapping.
    """
    selection = SideSelection(default_side=CUR, per_pair={"p": PRE})
    assert side_for("p", selection) == PRE
    assert side_for("anything", selection) == CUR


def test_the_selector_leaves_the_registry_as_it_found_it():
    """Property: no lookup adds, removes or replaces a registration.

    Class: every lookup the module offers, including the ones that raise. Why: the registry is
    shared by the checks that compare it with a scan of the package, and a lookup that wrote to
    it would change what those checks read. Killed by: a lookup that inserts a missing pair or a
    missing side. Second correct implementation: a selector that holds no reference to the
    registry between calls, which leaves it equal by construction.
    """
    _register("p", PRE)
    before = pairs.registered_pairs()
    for call in (
        lambda: function_for("p", SideSelection(default_side=PRE)),
        lambda: function_for("p", SideSelection()),
        lambda: function_for("absent", SideSelection()),
        lambda: selected_sides(SideSelection(per_pair={"absent": PRE})),
        lambda: selected_sides(SideSelection()),
    ):
        with contextlib.suppress(UnknownPairError, UnregisteredSideError):
            call()
    assert pairs.registered_pairs() == before


@pytest.mark.parametrize("registered", [(), ("p",)], ids=["empty-registry", "one-pair-registered"])
@pytest.mark.parametrize("default_side", [PRE, CUR])
@pytest.mark.parametrize("override_side", [PRE, CUR])
def test_an_override_for_an_unregistered_pair_is_refused_whatever_side_and_registry(
    registered, default_side, override_side
):
    """Property: an override key absent from the registry raises `UnknownPairError` naming it, in both lookups.

    Class: every combination of a default side, the side the stray override asks for, and a
    registry that is empty or holds a pair with both sides. The first suite reaches one cell of
    this grid: a non-empty registry, an override side different from the default. Why: a misspelt
    identifier is as likely to carry the same side as the default as a different one, and the
    empty registry is the package as it ships; ignoring the override in either would run the
    default side for the pair its author meant to switch. Killed by: refusing an unknown override
    only when its side differs from the default side, or only when the registry is non-empty.
    Second correct implementation: validating the override keys against `registered_pairs()` in
    a helper that both lookups call before any other work.
    """
    for pair in registered:
        _register(pair, PRE, CUR)
    selection = SideSelection(default_side=default_side, per_pair={"typo": override_side})
    with pytest.raises(UnknownPairError, match="typo"):
        selected_sides(selection)
    with pytest.raises(UnknownPairError, match="typo"):
        function_for("p", selection)


@pytest.mark.parametrize("missing_in", ["first", "middle", "last"])
@pytest.mark.parametrize("missing_side", [PRE, CUR])
def test_a_missing_side_is_refused_by_selected_sides_for_whichever_pair_and_side_is_selected(missing_in, missing_side):
    """Property: `selected_sides` raises `UnregisteredSideError` when any pair lacks the side selected for it.

    Class: three registered pairs, exactly one of which lacks the side the selection chooses for
    it, the lacking pair being first, middle or last in identifier order, and the lacking side
    being each of the two sides (chosen by the default). The first suite reaches only a
    current-only pair under a pre-refactor default, and nothing else, through `selected_sides`.
    Why: a record that listed a side no function was registered for would declare a call that
    could never have happened. Killed by: checking only a pre-refactor selection, checking only
    one pair of the registry, or skipping the check for a pair that runs its current side.
    Second correct implementation: building the full pair-to-function mapping through
    `function_for` first and deriving the sides from it.
    """
    ids = {"first": "a", "middle": "b", "last": "c"}
    lacking = ids[missing_in]
    chosen = CUR if missing_side == CUR else PRE
    for pair in ("a", "b", "c"):
        _register(pair, *(side for side in (PRE, CUR) if not (pair == lacking and side == missing_side)))
    with pytest.raises(UnregisteredSideError, match=lacking):
        selected_sides(SideSelection(default_side=chosen))


@pytest.mark.parametrize("bad_position", [0, 1, 2])
def test_a_misspelt_side_is_refused_at_construction_wherever_it_sits_among_the_overrides(bad_position):
    """Property: a `per_pair` value outside the two sides raises `ValueError` naming it, at any position.

    Class: three overrides whose values are valid except one, which is first, second or third in
    the mapping's order. The first suite gives one override, so first and only position coincide.
    Why: a run-wide selection with several overrides is the normal shape and a misspelling in the
    later ones must fail at the line that wrote it. Killed by: validating only the first
    override, or only the last. Second correct implementation: validating each value in a
    generator expression with `all(...)` and naming the offender from a second pass.
    """
    values = [CUR, PRE, CUR]
    values[bad_position] = "legacy"
    overrides = dict(zip(("a", "b", "c"), values, strict=True))
    with pytest.raises(ValueError, match="legacy"):
        SideSelection(per_pair=overrides)


def test_the_two_refusals_are_value_errors_and_neither_is_a_kind_of_the_other():
    """Property: both refusals derive from `ValueError`, and catching one never catches the other.

    Class: the two exception classes and any `except` clause naming either. Why: a caller that
    handles an unknown pair by registering it must not swallow a missing side, which needs a
    different repair, and a caller handling `ValueError` generally must reach both. Killed by:
    deriving one refusal from the other, or from `Exception` rather than `ValueError`. Second
    correct implementation: two classes each deriving directly from `ValueError` with distinct
    docstrings, which is the shape this test allows and any other base that is a `ValueError`
    subclass and not the sibling.
    """
    assert issubclass(UnknownPairError, ValueError)
    assert issubclass(UnregisteredSideError, ValueError)
    assert not issubclass(UnknownPairError, UnregisteredSideError)
    assert not issubclass(UnregisteredSideError, UnknownPairError)


def test_selected_sides_returns_a_fresh_dict_that_no_later_call_sees_changed():
    """Property: each `selected_sides` call returns a plain `dict` the caller may change without affecting the next call.

    Class: repeated calls with one selection over one registry, the first result being altered
    in place between them. Why: a side record is built from this result and a run that edits
    what it was handed must not change what the next record is built from. Killed by: returning
    a cached dict, or a read-only view of one. Second correct implementation: a dict subclass
    that preserves insertion order, which satisfies `isinstance(result, dict)` and is rebuilt on
    every call.
    """
    _register("a", PRE, CUR)
    _register("b", PRE, CUR)
    selection = SideSelection(default_side=PRE)
    first = selected_sides(selection)
    assert isinstance(first, dict)
    first["a"] = CUR
    first["z"] = PRE
    del first["b"]
    assert selected_sides(selection) == {"a": PRE, "b": PRE}
