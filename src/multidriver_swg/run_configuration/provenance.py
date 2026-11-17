"""Declare each compute system's implementation identity, and capture the record of one run.

A run's output is only reproducible if the record of that run says which implementation produced
each part of it. Nothing in the package can reconstruct that afterwards: the inputs a run was given
do not determine which code consumed them, so a record rebuilt from inputs answers a different
question from the one a reader asks — which implementation ran. This module states the declaration
once, so the compute systems of this package implement one interface rather than each inventing its
own, and it captures the record at run time in one place.

The self-declaration ruling:
Each compute system states its own implementation identity in the run's record, through a small interface that the system itself implements; the record is made while the run is happening, and nothing like a central registry is built.

The capture-not-reconstruct convention:
The record is captured while the run happens and is never rebuilt from the inputs afterwards.

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

The PERSISTED FORM. A record held in memory dies with the process, so a producer also writes its own
identity beside the outputs it produced: one file per producer, holding one JSON object and nothing
else, keyed by the literal `implementation-identity:1` and carrying `system`, `implementation` and
`code_version`. `persist_identity` writes that file and `read_identity` is its only reader, so a
consumer never parses the file itself and the form is enforced in one place. Emptiness is refused at
READ rather than in `ImplementationIdentity`: the dataclass deliberately checks no field value, so a
record missing a producer's name would otherwise be written by a producer and read by everyone. The
refusal names the file, the field and the repair, because the reader is looking at a generated file it
did not write.

Not established: that every sibling has adopted this interface. `require_complete` refuses an
incomplete record against the exercised set it is GIVEN; deriving that set is the workflow graph's
work and is not here, so a record is enforceably complete only where the caller supplies the set.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from multidriver_swg.run_configuration.seed_contract import SeedGenerator

# An import-visible documentation anchor, not a draw site. The `seeds` field carries `int` values,
# and an `int` is exactly what seeds the contract's generator: `numpy.random.default_rng(seed)`
# takes it as entropy. Binding the class here keeps that relationship in the module's imports, where
# a reader and the import graph both see it, rather than leaving it a prose mention with no
# dependency behind it. Deliberately NOT an instance: a `Generator` advances on every draw, so a
# stored one names a stream already moved, and this module still makes no draw and seeds nothing.
_SEED_GENERATOR = SeedGenerator

# The persisted form's version literal and its closed key set. The literal is compared as an exact
# string rather than parsed, so a later change to the file's fields is loud instead of silently
# misread by an older reader — the same rule the selection artifact's header follows.
IDENTITY_RULE = "implementation-identity:1"

_IDENTITY_FIELDS = ("format", "system", "implementation", "code_version")


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
    field values, so an identity with an empty `implementation` constructs here; the emptiness rule is
    stated at the persisted form's read, in `read_identity`, rather than at construction, so a value
    held in memory and a value read from a file answer to different rules.
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


def persist_identity(path: str | Path, identity: ImplementationIdentity) -> None:
    """Write `identity` to `path` as the persisted form: one JSON object, the form's four fields, nothing else.

    The file is written WHOLE and is never appended to, so a repeated write replaces the answer rather
    than accumulating answers to one question. The JSON is indented so a reader of the generated file
    can diff two runs' identities line by line, and a trailing newline is written so the file is a
    well-formed text file on every tool that reads one.

    A `TypeError` for an identity that is not one, because that is a caller's mistake rather than a
    defect of the code that produced it; the split is the one `require_complete` draws. NO FIELD VALUE
    IS CHECKED HERE: emptiness is refused by `read_identity`, so the writer stays the plain serializer
    and the rule is stated once, at the boundary every consumer crosses.

    The parent directory is not created. A producer writes this file beside outputs it has already
    written, so the directory exists; creating one here would let a mistyped path silently acquire a
    directory tree that nothing else knows about.

    Deliberately NOT wrapped: an unwritable path. `Path.write_text` raises the operating system's error,
    which names the file, and there is no second fact a wrapper could add.
    """
    if not isinstance(identity, ImplementationIdentity):
        raise TypeError(
            f"persist_identity was given {type(identity).__name__} rather than an ImplementationIdentity: "
            "the persisted form carries the three fields a declaration returns, so an object of another "
            "kind has none of them to write"
        )

    document = {
        "format": IDENTITY_RULE,
        "system": identity.system,
        "implementation": identity.implementation,
        "code_version": identity.code_version,
    }
    Path(path).write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")


def read_identity(path: str | Path) -> ImplementationIdentity:
    """Read the persisted form at `path` and return the identity it carries, refusing one that is not it.

    Every refusal below names the file, because the reader is looking at a GENERATED file and its
    recovery is to find the producer that wrote it:

    - the file is not valid JSON, or its document is not one JSON object — there is no object to read;
    - the document carries a key the form does not name — the writer believed that field was read while
      nothing reads it, and the record's own form is four fields;
    - `format` is absent or is not the literal `IDENTITY_RULE` — the file was written against another
      form, and reading it optimistically is the silent misread the header exists to prevent;
    - `system`, `implementation` or `code_version` is absent, is not text, or is EMPTY — the whole reason
      the persisted form exists is that a run can name its producer later, and an empty field names
      nothing while still looking like a record.

    A field of whitespace-only text is ADMITTED, because the form's rule is emptiness and inventing a
    trimming rule here would refuse a value the writer never agreed was invalid. That is a stated bound
    rather than an oversight.

    Deliberately NOT wrapped: an unreadable path. `Path.read_text` raises the operating system's error,
    which already names the file, and a missing identity file is a missing file at a path the caller
    chose. A file that is not UTF-8 text IS refused here, under this module's class and with the path,
    because the interpreter's `UnicodeDecodeError` names a byte offset and a codec and no file.
    """
    target = Path(path)

    try:
        text = target.read_text(encoding="utf-8")
    except UnicodeDecodeError as error:
        raise ProvenanceError(
            f"identity record {target} is not UTF-8 text, so there is nothing to read: {error}"
        ) from error

    try:
        document = json.loads(text)
    except json.JSONDecodeError as error:
        raise ProvenanceError(
            f"identity record {target} is not valid JSON, so there is nothing to read: {error}"
        ) from error

    if not isinstance(document, dict):
        raise ProvenanceError(
            f"identity record {target} is {document!r}: the persisted form is ONE JSON object carrying "
            f"{', '.join(repr(name) for name in _IDENTITY_FIELDS)}, and an array or a scalar names no "
            "producer"
        )

    for key in document:
        if key not in _IDENTITY_FIELDS:
            raise ProvenanceError(
                f"identity record {target} carries the key {key!r}: the form is exactly "
                f"{', '.join(repr(name) for name in _IDENTITY_FIELDS)}, so a key it does not name is a "
                "field the producer believed was read while nothing reads it"
            )

    if document.get("format") != IDENTITY_RULE:
        raise ProvenanceError(
            f"identity record {target} declares format = {document.get('format')!r}: the persisted form "
            f"must declare format = {IDENTITY_RULE!r}, the version header that makes a later change to the "
            "record's fields loud rather than silently misread"
        )

    values: dict[str, str] = {}
    for name in ("system", "implementation", "code_version"):
        value = document.get(name)
        if not isinstance(value, str) or not value:
            raise ProvenanceError(
                f"identity record {target} field {name!r} is {value!r}: the field is the non-empty text its "
                "producer declared, and an absent or empty one names no producer, which is the one fact "
                "the record exists to carry; write the record with persist_identity"
            )
        values[name] = value

    return ImplementationIdentity(
        system=values["system"],
        implementation=values["implementation"],
        code_version=values["code_version"],
    )
