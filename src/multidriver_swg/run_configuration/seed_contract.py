"""Declare the seed contract: the interface every stochastic function of this package implements.

A stochastic function returns a different value on every call, so a run that is meant to be
reproducible has to say where its randomness comes from, and nothing else may decide that on the
caller's behalf. This module states that decision once, so the compute systems of this package
implement one interface rather than each inventing its own: a function that draws a random number
takes a `numpy.random.Generator` as a required argument named `rng`.

The optionality ruling:
"no default, no fallback to an unseeded draw — seeding is always a conscious, user-supplied decision"

The propagation convention:
"the generator is threaded through call signatures, never process-global state"

Two instruments enforce the ruling's two halves and neither covers the other, so a reader should
not read one as covering both. The NO-DEFAULT half is enforced here, at run time, by
`require_seed`, which refuses a function whose `rng` parameter is absent or carries a default. The
NO-FALLBACK half — an `if rng is None:` branch inside a function body — is not reachable by
inspecting a signature, so it is enforced by the source census that certifies the target of zero
unseeded draws. That census is a joint instrument this module names and does not build: a count
over a package that draws nothing today would pass whether or not this declaration exists.

The static half rides on `SeededCallable`: a sibling whose stochastic function omits `rng`, or
types it as optional, fails its type checker.

This module is a declaration and not a draw site. It imports numpy for its type declarations and
makes no draw of its own, binds no module-level generator, and seeds nothing at import.
Deliberately NOT a module-level `numpy.random.Generator` instance offered as a convenience
default: that is the fallback the ruling forbids, and it would make an unseeded draw reachable
from every caller that omitted the argument.

Not established: that a call site passes on the generator it was given rather than a freshly
seeded one. This module declares an interface and checks one signature; what a call site does with
it is the census's question, and the census is not here.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Protocol, TypeAlias

import numpy

SeedGenerator: TypeAlias = numpy.random.Generator

SEED_PARAMETER = "rng"


class SeededCallable(Protocol):
    """The shape of a stochastic function: a required `rng` first, then the function's own arguments.

    `*args` and `**kwargs` stand for a stochastic function's own inputs, which this Protocol does
    not know and must not name. Naming them would refuse a correct implementation for accepting a
    parameter the contract has no view on, which would make the interface an implementation
    requirement rather than a shape.
    """

    def __call__(self, rng: SeedGenerator, *args: object, **kwargs: object) -> object:
        """Draw from `rng` and this call's own arguments; `rng` is required and carries no default."""
        ...


def require_seed(function: Callable[..., object]) -> None:
    """Refuse a function that does not require its seed generator; admit one that does.

    Call this where a stochastic function is registered, so that a function whose generator is
    missing or optional is refused at that moment rather than drawing unseeded at run time, where
    nothing reports it. It refuses a function with no parameter named `SEED_PARAMETER`, and a
    function whose `SEED_PARAMETER` carries a default, because a defaulted generator is one a
    caller can omit — the `seeds = None` form the optionality ruling forbids, and no other check
    in this package reaches it.

    `TypeError` rather than a dedicated exception, because that is Python's own error for a missing
    required argument, and this module is the root interface with no sibling yet that would catch
    a narrower type.

    Not established: that the parameter is annotated `SeedGenerator`. Only absence and a default
    are read here. A sibling that writes `from __future__ import annotations` presents its
    annotation as a string, so a runtime read would compare against text rather than against the
    type; the annotation half of the contract is carried statically by `SeededCallable` and refused
    by a type checker.
    """
    name = getattr(function, "__qualname__", None) or repr(function)
    parameter = inspect.signature(function).parameters.get(SEED_PARAMETER)
    if parameter is None:
        raise TypeError(
            f"{name} has no {SEED_PARAMETER!r} parameter: the seed contract requires a required "
            f"numpy.random.Generator named {SEED_PARAMETER!r}"
        )
    if parameter.default is not inspect.Parameter.empty:
        raise TypeError(
            f"{name} gives its {SEED_PARAMETER!r} parameter the default {parameter.default!r}: the seed "
            "contract requires the caller to supply the generator, so the parameter carries no default"
        )
