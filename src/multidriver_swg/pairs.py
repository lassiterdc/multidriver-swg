"""Register each new function as one side of a named pair with the legacy function it replaces.

When a function of this package is meant to compute what a legacy function computed, the two
are recorded as a PAIR under one stable identifier: the legacy function is the `pre_refactor`
side and its replacement is the `current` side. A pair is how a later check knows which two
functions to compare, and how a record of their differences knows which functions it describes.

A function joins a pair by decorating it with `pair_side(pair="{pair id}", side="{side}")`.
The decorator records the function in a registry held by this module and returns the function
unchanged, so registration never alters what the function does. The registry fills as modules
that carry the decorator are imported, and `registered_pairs` returns a copy of it.

`EXPECTED_PAIR_COUNT` states how many pairs the package is meant to register. An owning system
raises it in the same change that registers a pair, so a check can compare it with the
registry and with a static scan of the source, and a pair that one of the three misses is
visible.

Both arguments must be written as string literals. The decorator accepts any string at run
time, but a check that reads the source without importing it can only read a literal.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

PRE_REFACTOR = "pre_refactor"
CURRENT = "current"
SIDES = (PRE_REFACTOR, CURRENT)
EXPECTED_PAIR_COUNT = 0  # raised by the owning system in the change that registers a pair

_F = TypeVar("_F", bound=Callable[..., object])

# pair id -> side -> the registered function, filled at import by `pair_side`.
_REGISTRY: dict[str, dict[str, object]] = {}


class DuplicatePairRegistration(ValueError):  # noqa: N818  (the name the pair-record checks are written against)
    """A second function registered as one side of one pair, which would silently replace the first."""


def pair_side(*, pair: str, side: str) -> Callable[[_F], _F]:
    """Register the decorated function as `side` of `pair`; return it unchanged.

    A second registration of one side of one pair raises `DuplicatePairRegistration` when the
    decorator is applied, which is when the module carrying it is imported. Without the refusal
    the later function would replace the earlier one and every comparison of the pair would read
    a function its author did not mean. An unknown side raises `ValueError` when the decorator is
    built, before any function is registered. Both arguments are keyword-only, so a positional
    call raises `TypeError`: a positional pair and side are easy to swap, and the swap would
    register the function under the wrong pair. Not established: a pair identifier that is not
    a string is accepted here; a static scan of the source is what reports a non-literal one.
    """
    if side not in SIDES:
        raise ValueError(f"side {side!r} of pair {pair!r} is not one of {', '.join(SIDES)}")

    def decorate(function: _F) -> _F:
        """Record `function` as `side` of `pair`, refusing a second function for that side."""
        sides = _REGISTRY.setdefault(pair, {})
        if side in sides:
            raise DuplicatePairRegistration(
                f"side {side} of pair {pair} is registered twice: {_qualified(sides[side])} and {_qualified(function)}"
            )
        sides[side] = function
        return function

    return decorate


def registered_pairs() -> dict[str, dict[str, object]]:
    """A copy of the import-time registry, pair id to side to function, that a caller may change freely."""
    return {pair: dict(sides) for pair, sides in _REGISTRY.items()}


def _qualified(function: object) -> str:
    """`{module}:{qualified name}` of a registered function, for naming both sides of a duplicate."""
    return f"{getattr(function, '__module__', '?')}:{getattr(function, '__qualname__', repr(function))}"
