"""Declare each compute system's implementation identity, and capture the record of one run.

A run's output is only reproducible if the record of that run says which implementation produced
each part of it. Nothing in the package can reconstruct that afterwards: the inputs a run was given
do not determine which code consumed them, so a record rebuilt from inputs answers a different
question from the one a reader asks — which implementation ran. This module states the declaration
once, so the compute systems of this package implement one interface rather than each inventing its
own, and it captures the record at run time in one place.

The self-declaration ruling:
"every compute system declares its implementation identity into the run's record through a small self-declaration interface it implements; the record is captured at run time, and no central registry is built"

The capture-not-reconstruct convention:
"the record is captured at run time, never rebuilt from inputs afterwards"

Two instruments enforce the interface's two halves, and neither covers the other, so a reader should
not read one as covering both. The SHAPE half — a declaration returns an `ImplementationIdentity`
with the three fields — is enforced statically by `IdentityDeclaration`, so a declaration that
returns the wrong type fails its type checker. The PRESENCE half — that a system declared at all —
is not reachable by typing a declaration that does not exist, so it is enforced at consumption by
`require_complete`, which refuses a record missing an exercised system's identity. That refusal is
the point of the module: a silent non-declaration otherwise passes every check the package has,
because a missing identity looks exactly like a system that produced no output.

No central registry: this module holds no mapping of systems to identities, and the only way an
identity enters a record is a caller passing what a system's own declaration returned. A registry
would make this module the place every system must be edited into, which is the coupling the
self-declaration interface exists to avoid.

Deliberately NOT a plain `dict`-shaped record: a `dict` enforces nothing at construction, so a
record missing its identities would build silently and `require_complete` would be the only thing
between a run and an unnamed producer — with nothing saying the record was meant to carry them.

Not established: that every sibling has adopted this interface. `require_complete` refuses an
incomplete record against the exercised set it is GIVEN; deriving that set is the workflow graph's
work and is not here, so a record is enforceably complete only where the caller supplies the set.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Protocol

from multidriver_swg.run_configuration.seed_contract import SeedGenerator

# An import-visible documentation anchor, not a draw site. The `seeds` field carries `int` values,
# and an `int` is exactly what seeds the contract's generator: `numpy.random.default_rng(seed)`
# takes it as entropy. Binding the class here keeps that relationship in the module's imports, where
# a reader and the import graph both see it, rather than leaving it a prose mention with no
# dependency behind it. Deliberately NOT an instance: a `Generator` advances on every draw, so a
# stored one names a stream already moved, and this module still makes no draw and seeds nothing.
_SEED_GENERATOR = SeedGenerator


class ProvenanceError(Exception):
    """A captured record that does not name every system the run exercised."""


@dataclass(frozen=True)
class ImplementationIdentity:
    """Which implementation produced an output: the system, the implementation, and its code version.

    Frozen, so an identity that has been declared into a record cannot be mutated afterwards and a
    reader comparing two records compares what was actually declared.

    - `system` — the compute system's name.
    - `implementation` — which implementation within that system produced the output.
    - `code_version` — the version of the code that produced it.

    Not established: that the three values are non-empty. A frozen dataclass does not check its
    field values, so an identity with an empty `implementation` constructs here; refusing one would
    be a validation rule this module does not state.
    """

    system: str
    implementation: str
    code_version: str


class IdentityDeclaration(Protocol):
    """The shape of a module-level callable a compute system implements to declare its identity.

    A Protocol rather than a base class, because a compute system is a module and not an instance:
    what it implements is a callable, and a Protocol types that callable without requiring the
    system to inherit from anything.

    Deliberately NOT the enforcement of "declared at all": a Protocol types a declaration that
    exists and says nothing about a system that never wrote one, which is why `require_complete`
    carries the presence half of the contract.
    """

    def __call__(self) -> ImplementationIdentity:
        """Return this system's identity; the record is captured with what this returns."""
        ...


@dataclass(frozen=True)
class ProvenanceRecord:
    """The record of one run: what it was given, the seeds it drew from, and who produced what.

    Frozen, so a captured record cannot be mutated after the run that produced it, and assembled
    only by `capture`, so no function rebuilds one from inputs afterwards.

    - `inputs` — the run's inputs, supplied by the caller at capture time and opaque to this module.
    - `seeds` — the reproducible seed value per producer or mechanism. Each value is an `int`, which
      is what seeds the contract's `SeedGenerator`: `numpy.random.default_rng` takes that `int` as
      entropy. Deliberately NOT the generator object — a `Generator` is stateful and advances on
      every draw, so a record holding one would name a stream already moved, where the `int` names
      the value a rerun reproduces.
    - `identities` — each declared identity, keyed by the system name it belongs to. An identity
      carries its own `code_version`, so the record answers "the code version each identity was
      produced by" without a parallel field.

    Not established: that the record is immutable all the way down. Freezing it stops a field being
    rebound, and the mappings a caller passed stay the caller's own mutable objects, so a caller
    that keeps mutating one after capture changes what a reader of the record sees. Copying them on
    the way in would close that, and would also make `capture` return record that is not the one the
    caller passed, which is a decision this module does not make.
    """

    inputs: Mapping[str, object]
    seeds: Mapping[str, int]
    identities: Mapping[str, ImplementationIdentity]


def capture(
    inputs: Mapping[str, object],
    seeds: Mapping[str, int],
    identities: Mapping[str, ImplementationIdentity],
) -> ProvenanceRecord:
    """Assemble the run's record from what the caller supplies while the run is happening.

    This is the record's only constructor. Capturing rather than reconstructing is the whole point:
    the record is written at run time and held, so a later reader learns which implementation ran
    rather than which implementation's inputs would have produced the same numbers.

    Deliberately NOT a function taking `inputs` alone: a record built from inputs is the
    reconstruction this module exists to prevent, and a second constructor would make that reachable
    from any caller who found it convenient.

    Not established: that the mappings are of the declared value kinds. A frozen dataclass does not
    check its field types at run time, so `seeds` holding a `str` constructs here and is caught only
    by a type checker or by the consumer that reads the value.
    """
    return ProvenanceRecord(inputs=inputs, seeds=seeds, identities=identities)


def require_complete(record: ProvenanceRecord, exercised_systems: Iterable[str]) -> None:
    """Refuse a record that leaves an exercised system's identity undeclared; admit a complete one.

    Call this where a run hands off the output it produced, so a record missing a producer is
    refused at that moment rather than read later by someone who cannot tell a system that produced
    nothing from one that never declared. It TAKES the exercised set as an argument and does not
    derive it: a check that derived its own set could pass by defining the set to match the record,
    which is the vacuous pass this refusal exists to rule out.

    `TypeError` for an argument of the wrong shape — something that is not a record, or an exercised
    set that is not an iterable of system names — and `ProvenanceError` for an incomplete record. The
    split is what lets a caller tell "you called it wrong" from "the run is incomplete", and the
    second is the halt a run must stop on.

    A bare string is refused rather than accepted: it is iterable, so it would be read as one system
    per character and every character would be reported missing, which reads as a record full of
    gaps rather than as a caller who passed a name instead of a set.

    Not established: that the exercised set is the set the run actually exercised. This function
    reads the set it is given and cannot compare it with the workflow graph that produced it.
    """
    if not isinstance(record, ProvenanceRecord):
        raise TypeError(
            f"require_complete was given {type(record).__name__} rather than a ProvenanceRecord: the "
            "completeness check reads a captured record's identities"
        )
    if isinstance(exercised_systems, str | bytes):
        raise TypeError(
            f"require_complete was given the single string {exercised_systems!r} as its exercised set: an "
            "iterable of system names is required, and a bare string iterates its characters"
        )
    try:
        systems = list(exercised_systems)
    except TypeError:
        raise TypeError(
            f"require_complete was given {type(exercised_systems).__name__} as its exercised set: an "
            "iterable of system names is required"
        ) from None

    missing = list(dict.fromkeys(system for system in systems if system not in record.identities))
    if missing:
        raise ProvenanceError(
            "the record names no implementation identity for "
            + ", ".join(repr(system) for system in missing)
            + ": every system the run exercised must declare the implementation that produced its output"
        )
