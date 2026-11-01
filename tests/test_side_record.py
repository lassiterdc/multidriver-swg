"""Tests of the side record: it lists every registered pair with the side selected, in a stable order, and cannot be altered.

The record is built from the pair registry, so every test builds its own registry. The real
registry lives for the whole interpreter and other tests compare it with a scan of the package,
so an autouse fixture swaps in an empty registry for each test and restores the real one after
it; no test here registers anything in the real registry, and none asserts what that registry
holds. Each test names the wrong implementation it is red under, because a check that cannot
fail for the reason it claims constrains nothing.
"""

from __future__ import annotations

import dataclasses
import json

import pytest

from multidriver_swg import pairs, side_record
from multidriver_swg.dispatch import SideSelection, UnknownPairError, UnregisteredSideError, selected_sides
from multidriver_swg.side_record import SideRecord

pytestmark = pytest.mark.state_check

PRE = pairs.PRE_REFACTOR
CUR = pairs.CURRENT

# Identifiers that differ by case, punctuation and spaces, which a pair may be registered under.
IDENTIFIERS = ("Zeta", "alpha", "B-2", "a.b", "with space", "Alpha", " padded ")

# The six rules of the record's content, in the words the module docstring states them.
CONTENT_RULES = (
    "The record maps each pair identifier, exactly as registered, to the side selected for that pair, "
    "`pre_refactor` or `current`.",
    "The record has one entry for every registered pair, including each pair selected on the current side; "
    "a pair with no entry was not registered.",
    "Entries are ordered by pair identifier, so two records of equal content serialize identically.",
    "A record with no entries is valid and is carried as empty; a run whose provenance carries no side record "
    "is a run that selected no sides, which is a different fact from a run with no pairs.",
    "The record declares the side selected for each pair; it does not attest that a pair's function was called.",
    "The provenance record carries the mapping unaltered and infers no side from any other field.",
)


@pytest.fixture(autouse=True)
def fresh_registry(monkeypatch):
    """Run every test of this module against an empty registry that is discarded with the test."""
    monkeypatch.setattr(pairs, "_REGISTRY", {})


def _register(pair: str, *sides: str) -> None:
    """Register one stand-in function per side of `pair` in the swapped-in registry."""
    for side in sides:

        def stand_in(x):
            """A stand-in whose identity is all a test reads."""
            return x

        pairs.pair_side(pair=pair, side=side)(stand_in)


def _register_all(*identifiers: str) -> None:
    """Register `identifiers` as pairs with both sides, in the order given."""
    for pair in identifiers:
        _register(pair, PRE, CUR)


def test_a_run_wide_pre_refactor_record_lists_every_pair_in_identifier_order():
    """Property: a run-wide pre-refactor selection records every pair as pre-refactor, keys in identifier order.

    Class: pairs registered in an order that is not their sorted order, under a run-wide
    pre-refactor selection; the satisfying input is the all-current record of a later test.
    Why: a record is compared and serialized by a reader who cannot know import order. Killed by:
    keeping registration order, recording the current side, or reading `ran_pre_refactor` from
    anything but the entries. Second correct implementation: a record that stores a sorted tuple
    of items and rebuilds the mapping on demand.
    """
    _register_all("c", "a", "b")
    record = side_record.side_record(SideSelection(default_side=PRE))
    assert list(record.as_mapping().items()) == [("a", PRE), ("b", PRE), ("c", PRE)]
    assert list(record.sides.items()) == [("a", PRE), ("b", PRE), ("c", PRE)]
    assert record.ran_pre_refactor is True


def test_a_one_override_record_lists_the_current_side_pairs_instead_of_omitting_them():
    """Property: with one pair overridden, the record has one pre-refactor entry and the other pairs listed as current.

    Class: three registered pairs and one override, naming each position in turn (first, middle,
    last); the satisfying input is the run-wide record above. Why: a pair with no entry must mean
    an unregistered pair, so a pair run on its current side still has to appear. Killed by:
    recording only the pairs that were switched. Second correct implementation: building the
    record from a loop over the registry instead of from `selected_sides`.
    """
    for overridden in ("a", "b", "c"):
        pairs._REGISTRY.clear()
        _register_all("a", "b", "c")
        record = side_record.side_record(SideSelection(per_pair={overridden: PRE}))
        expected = {pair: PRE if pair == overridden else CUR for pair in ("a", "b", "c")}
        assert record.as_mapping() == expected, (overridden, record.as_mapping())
        assert list(record.as_mapping()) == ["a", "b", "c"]
        assert record.ran_pre_refactor is True


def test_an_all_current_record_lists_every_pair_and_did_not_run_pre_refactor():
    """Property: a record whose every pair was selected on the current side is non-empty and reports no pre-refactor run.

    Class: three registered pairs under the default selection, and under a pre-refactor default
    overridden back to current for every pair. Why: the record must tell a run that used no
    legacy function apart from a run with no pairs, so it lists the pairs. Killed by: returning
    an empty record when nothing was switched, or `ran_pre_refactor` true whenever there are
    entries. Second correct implementation: `ran_pre_refactor` computed as a set intersection of
    the recorded sides with the pre-refactor name.
    """
    _register_all("a", "b", "c")
    for selection in (
        SideSelection(),
        SideSelection(default_side=PRE, per_pair={"a": CUR, "b": CUR, "c": CUR}),
    ):
        record = side_record.side_record(selection)
        assert record.as_mapping() == {"a": CUR, "b": CUR, "c": CUR}
        assert record.ran_pre_refactor is False


def test_an_empty_registry_gives_a_valid_empty_record():
    """Property: with no pair registered the record is empty, its mapping is `{}` and it did not run pre-refactor.

    Class: the registry as the package ships it, under the default and a run-wide pre-refactor
    selection. Why: an empty record is valid and is carried as empty; raising or inventing an
    entry would make a run on the package as it stands fail or lie. Killed by: raising on an
    empty registry, or `ran_pre_refactor` true because the selection's default is pre-refactor.
    Second correct implementation: a record built through a comprehension over the registry that
    is empty when the registry is.
    """
    for selection in (SideSelection(), SideSelection(default_side=PRE)):
        record = side_record.side_record(selection)
        assert record.as_mapping() == {}
        assert len(record.sides) == 0
        assert record.ran_pre_refactor is False


def test_a_record_cannot_be_altered_and_its_mapping_is_a_fresh_dict_each_time():
    """Property: assigning a field raises `FrozenInstanceError`, assigning into `sides` raises `TypeError`, and `as_mapping` is a fresh dict.

    Class: every change a caller can make to a built record: a field, its mapping, and the dict
    `as_mapping` returned; the satisfying input is a second `as_mapping` call, which still reads
    the original content. Why: a record a run has carried must still mean the same thing when
    the run ends. Killed by: a mutable `sides`, or `as_mapping` returning the stored mapping.
    Second correct implementation: storing the entries as a tuple of pairs and rebuilding the
    mapping in `as_mapping` and `sides`.
    """
    _register_all("a", "b")
    record = side_record.side_record(SideSelection(per_pair={"a": PRE}))
    with pytest.raises(dataclasses.FrozenInstanceError):
        record.sides = {}  # type: ignore[misc]
    with pytest.raises(TypeError):
        record.sides["a"] = CUR  # type: ignore[index]
    first = record.as_mapping()
    assert type(first) is dict
    first["a"] = CUR
    first["z"] = PRE
    del first["b"]
    assert record.as_mapping() == {"a": PRE, "b": CUR}
    assert record.as_mapping() is not record.as_mapping()


def test_a_record_ignores_later_changes_to_the_dict_it_was_given():
    """Property: changing the dict a record was built from afterwards changes nothing in the record.

    Class: a mutation of the source dict after construction, adding, replacing and removing
    entries. Why: a caller that built a record and then reused its dict must not alter what a
    run has declared. Killed by: storing the caller's dict, or a view of it. Second correct
    implementation: copying into a tuple of pairs at construction.
    """
    given = {"b": CUR, "a": PRE}
    record = SideRecord(given)
    given["a"] = CUR
    given["z"] = PRE
    del given["b"]
    assert record.as_mapping() == {"a": PRE, "b": CUR}


def test_equal_content_serializes_identically_whatever_order_it_was_built_in():
    """Property: two records of equal content serialize to the same text, however their entries were ordered when built.

    Class: the same pair-to-side content supplied in two different orders, directly and through
    registries filled in different orders; compared as equal records and as the JSON text of
    `as_mapping()`. Why: a provenance record that carries the mapping unaltered is compared as
    text by readers and by checks. Killed by: keeping the order of construction. Second correct
    implementation: sorting only inside `as_mapping` and keeping `sides` in arrival order, which
    this test would catch through the equality arm if `sides` were compared unsorted.
    """
    direct = [SideRecord({"b": CUR, "a": PRE, "c": CUR}), SideRecord({"c": CUR, "a": PRE, "b": CUR})]
    assert direct[0] == direct[1]
    assert json.dumps(direct[0].as_mapping()) == json.dumps(direct[1].as_mapping())
    built = []
    for order in (("c", "a", "b"), ("b", "c", "a")):
        pairs._REGISTRY.clear()
        _register_all(*order)
        built.append(side_record.side_record(SideSelection(per_pair={"a": PRE})))
    assert json.dumps(built[0].as_mapping()) == json.dumps(built[1].as_mapping())
    assert built[0].as_mapping() == direct[0].as_mapping()


def test_the_record_is_captured_when_the_side_is_selected_and_not_rebuilt_later():
    """Property: a pair registered after the record was built does not appear in it, and one removed does not vanish from it.

    Class: a registry changed after `side_record` returned. Why: the record describes the run
    as it chose, while the registry may change afterwards in the same interpreter. Killed by: a
    record that holds a reference to the registry or recomputes its entries from it. Second
    correct implementation: a record that stores a tuple of items taken at construction.
    """
    _register_all("a")
    record = side_record.side_record(SideSelection(default_side=PRE))
    _register_all("b")
    pairs._REGISTRY.pop("a")
    assert record.as_mapping() == {"a": PRE}
    assert record.ran_pre_refactor is True


def test_the_record_refuses_what_the_selector_refuses():
    """Property: an override for an unregistered pair and a selected side with no function both raise instead of being recorded.

    Class: a selection with a stray override key, and a pair registered with only its current
    side under a pre-refactor default; the satisfying input is a complete registry. Why: a side
    must never be recorded that could not have been called. Killed by: building the record from
    the registry alone, ignoring the selection's checks. Second correct implementation: a record
    builder that calls `function_for` for each pair and derives the sides from the selection.
    """
    _register("only_current", CUR)
    with pytest.raises(UnregisteredSideError, match="only_current"):
        side_record.side_record(SideSelection(default_side=PRE))
    with pytest.raises(UnknownPairError, match="typo"):
        side_record.side_record(SideSelection(per_pair={"typo": PRE}))
    assert side_record.side_record(SideSelection()).as_mapping() == {"only_current": CUR}


def test_the_module_docstring_states_the_six_content_rules_and_the_unaltered_carriage():
    """Property: the module docstring contains each of the six content rules as a sentence, and says the mapping is carried unaltered.

    Class: the six rules of the record's content, each searched as a whole sentence with
    whitespace runs collapsed so line wrapping does not matter, and the statement that a
    provenance record carries the result of `as_mapping()` unaltered. Why: a system that builds
    its provenance record from this content reads the rules here, so the docstring must carry
    them in these words. Killed by: dropping or rewording any rule. Second correct implementation:
    the same sentences wrapped at a different width or separated by other prose, which still pass.
    """
    docstring = " ".join((side_record.__doc__ or "").split())
    for rule in CONTENT_RULES:
        assert rule in docstring, rule
    assert "carries the result of `SideRecord.as_mapping()` unaltered" in docstring
    assert docstring.count("R-") == 0


def test_building_a_record_leaves_the_registry_as_it_found_it():
    """Property: building records, including ones that raise, adds, removes and replaces no registration.

    Class: every way of building a record this module offers, successful and refused. Why: the
    registry is shared by the checks that compare it with a scan of the package. Killed by: a
    builder that writes a missing pair or side. Second correct implementation: a builder that
    holds no reference to the registry between calls.
    """
    _register("p", PRE)
    before = pairs.registered_pairs()
    side_record.side_record(SideSelection(default_side=PRE))
    with pytest.raises(UnregisteredSideError):
        side_record.side_record(SideSelection())
    with pytest.raises(UnknownPairError):
        side_record.side_record(SideSelection(per_pair={"absent": PRE}))
    assert pairs.registered_pairs() == before


def test_a_record_keeps_each_identifier_exactly_as_registered_and_in_the_selector_order():
    """Property: record keys are the registered identifiers byte for byte, in the order `selected_sides` lists them.

    Class: identifiers that differ by case, or carry a hyphen, a dot, an interior space or a leading and trailing space, registered in an
    order that is not their sorted order, under a run-wide pre-refactor selection and a one-override
    selection. The first suite uses single lower-case letters only. Why: the identifier is the
    join key between the record, the pair registry and the ledger, so a record that rewrote it
    would name a pair nothing else holds, and its order must agree with the selector it is built
    from. Killed by: normalising an identifier (lower-casing or stripping it) or ordering the keys
    case-insensitively when the record is built. Second correct implementation: a record that
    stores a sorted tuple of items, so long as its order is the selector's order.
    """
    _register_all(*reversed(IDENTIFIERS))
    for selection in (SideSelection(default_side=PRE), SideSelection(per_pair={"B-2": PRE})):
        record = side_record.side_record(selection)
        assert set(record.sides) == set(IDENTIFIERS)
        assert list(record.sides) == list(selected_sides(selection))
        assert list(record.as_mapping()) == list(selected_sides(selection))


def test_a_directly_built_record_orders_its_keys_as_the_selector_orders_the_same_identifiers():
    """Property: a record built from an unordered mapping lists its keys in the order `selected_sides` gives those identifiers.

    Class: the same identifiers (mixed case, punctuation) supplied to `SideRecord` in several
    orders, compared with the selector's order for a registry of those identifiers. Why: a
    provenance record that carries the mapping unaltered is compared as text, and a record built
    directly must serialize as one built through the selector. Killed by: sorting with a key other
    than the identifier (such as case-folding it) in the record while the selector sorts the
    identifier itself. Second correct implementation: the record delegating its ordering to the
    selector's rule, which is plain identifier order.
    """
    _register_all(*IDENTIFIERS)
    expected = list(selected_sides(SideSelection()))
    arrivals = (IDENTIFIERS, tuple(reversed(IDENTIFIERS)), tuple(sorted(IDENTIFIERS, key=str.casefold)))
    for arrival in arrivals:
        record = SideRecord(dict.fromkeys(arrival, CUR))
        assert list(record.as_mapping()) == expected
        assert list(record.sides) == expected


def test_records_of_unequal_content_are_unequal():
    """Property: two records that differ in one side, or in one key, do not compare equal.

    Class: a record against another that differs by exactly one entry, by one side value, and by
    being empty against non-empty. The first suite asserts only that equal content compares equal.
    Why: the equality of two records is how a check says two runs selected the same sides; one that
    ignored the entries would report every pair of runs as identical. Killed by: leaving the
    entries out of the comparison. Second correct implementation: a hand-written `__eq__`
    comparing `as_mapping()` of both.
    """
    base = SideRecord({"a": PRE, "b": CUR})
    assert base == SideRecord({"b": CUR, "a": PRE})
    assert base != SideRecord({"a": CUR, "b": CUR})
    assert base != SideRecord({"a": PRE})
    assert base != SideRecord({"a": PRE, "b": CUR, "c": CUR})
    assert SideRecord({}) != base
