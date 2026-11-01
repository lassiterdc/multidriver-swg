"""Choose, for each registered pair, whether a run calls its `pre_refactor` function or its `current` one.

A pair (see `multidriver_swg.pairs`) holds a legacy function and the function that replaces it.
Something has to hand a caller the function for the side a run chose, and has to say which side
that was for every pair, so that a run which used a legacy function can say so. This module is
that selector. It is a pure lookup over the pair registry: it holds no state of its own, reads
no setting, and registers nothing.

The choice is a `SideSelection`: one side for every pair (`default_side`) plus an override for
named pairs (`per_pair`). A run-wide pre-refactor run is `SideSelection(default_side="pre_refactor")`;
one pair measured against the current code of the rest is `SideSelection(per_pair={"{pair id}":
"pre_refactor"})`. The selection is passed to each call rather than read from a module-level
setting, because a setting held in a module global outlives the run that set it and a later run
in the same interpreter would read it stale.

Which side a run uses, and the key and file format that carry that choice into a run, belong to
the code that reads the run's configuration; that code builds a `SideSelection` at its own
boundary. Nothing here reads a configuration.

Pairs are filled at import time, when a module that carries the `pair_side` decorator is
imported. A call made before such a module is imported sees no such pair, and refuses it as
unknown rather than guessing.

A selected side that no function was registered for is a refusal, never a quiet use of the other
side: falling back would run the current code in a run that was asked to reproduce the legacy
code and report that it had not.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import cast

from multidriver_swg import pairs


class UnknownPairError(ValueError):
    """A pair identifier, asked for or named in a selection's overrides, that the registry does not hold."""


class UnregisteredSideError(ValueError):
    """The side selected for a registered pair has no function registered, and the other side is not used instead."""


@dataclass(frozen=True)
class SideSelection:
    """Which side each pair runs: `default_side` for every pair, replaced by `per_pair` for the pairs it names.

    Both sides are checked against `pairs.SIDES` when the value is built, so a misspelt side
    fails at the line that wrote it and not at the first lookup. `per_pair` is copied into a
    read-only mapping, so a caller changing the dict it passed afterwards cannot change a
    selection that a run has already recorded. Not established: whether a pair named in
    `per_pair` is registered; the registry is filled at import and may not be full when the
    value is built, so that check is made at lookup.
    """

    default_side: str = pairs.CURRENT
    per_pair: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Refuse a side outside `pairs.SIDES`, and replace `per_pair` with a read-only copy of it."""
        _require_side(self.default_side, "default_side")
        for pair, side in self.per_pair.items():
            _require_side(side, f"per_pair[{pair!r}]")
        object.__setattr__(self, "per_pair", MappingProxyType(dict(self.per_pair)))


def side_for(pair: str, selection: SideSelection) -> str:
    """The side `selection` chooses for `pair`: its override if it has one, otherwise its default side.

    A pure function of its two arguments; it does not look at the registry, so it answers for a
    pair that is not registered. `function_for` and `selected_sides` are the calls that check
    the choice against what was registered.
    """
    return selection.per_pair.get(pair, selection.default_side)


def function_for(pair: str, selection: SideSelection) -> Callable[..., object]:
    """The registered function object itself for the side `selection` chooses for `pair`.

    The registry is read at the call, never copied at import, so the registry a caller has put
    in place is the one consulted. Raises `UnknownPairError` when `pair`, or any pair the
    selection overrides, is not registered, and `UnregisteredSideError` when the chosen side of
    `pair` has no function. Neither falls back to the other side.
    """
    registry = pairs.registered_pairs()
    _refuse_unknown_overrides(selection, registry)
    if pair not in registry:
        raise UnknownPairError(f"pair {pair!r} is not registered")
    return _registered_function(pair, side_for(pair, selection), registry)


def selected_sides(selection: SideSelection) -> dict[str, str]:
    """Every registered pair identifier mapped to the side `selection` chooses for it, ordered by identifier.

    Every registered pair appears, including each pair that runs its current side, so a reader
    of the result sees what was not switched as well as what was. The call refuses what
    `function_for` refuses: an override naming an unregistered pair, and a chosen side that has
    no registered function. A side is therefore never declared that could not have been called.
    """
    registry = pairs.registered_pairs()
    _refuse_unknown_overrides(selection, registry)
    chosen: dict[str, str] = {}
    for pair in sorted(registry):
        side = side_for(pair, selection)
        _registered_function(pair, side, registry)
        chosen[pair] = side
    return chosen


def _require_side(side: str, where: str) -> None:
    """Refuse `side` with a message naming it and the place it was given, unless it is one of `pairs.SIDES`."""
    if side not in pairs.SIDES:
        raise ValueError(f"{where} {side!r} is not one of {', '.join(pairs.SIDES)}")


def _refuse_unknown_overrides(selection: SideSelection, registry: Mapping[str, Mapping[str, object]]) -> None:
    """Raise `UnknownPairError` naming each override whose pair `registry` does not hold.

    An override for a pair that is not registered is almost always a misspelt identifier, and
    ignoring it would run the default side for the pair its author meant to switch.
    """
    unknown = sorted(pair for pair in selection.per_pair if pair not in registry)
    if unknown:
        raise UnknownPairError(f"selection overrides pair(s) that are not registered: {', '.join(map(repr, unknown))}")


def _registered_function(pair: str, side: str, registry: Mapping[str, Mapping[str, object]]) -> Callable[..., object]:
    """The function registered as `side` of `pair` in `registry`, or `UnregisteredSideError` naming the pair and side."""
    functions = registry[pair]
    if side not in functions:
        raise UnregisteredSideError(f"pair {pair!r} has no function registered for side {side!r}")
    return cast("Callable[..., object]", functions[side])
