"""Run an equation's stored implementation, compare the result through a licence, and attribute a red.

A conformance test has to answer three questions that do not belong in its own body: what does the code
actually compute for these inputs, does that agree with the value the manuscript's formula gives, and if
it does not, which equation diverged. This module answers all three in one place.

WHY THE IMPLEMENTATION IS REACHED THROUGH THE LIFT HARNESS AND NOT IMPORTED. Nothing under `src/`
implements the registered equations yet, so the code that executes the published mathematics is the
legacy code — and the correspondence harness forbids importing or running the legacy tree. A unit is
reached only as its stored extraction, compiled and run by `lift.run_lift` in a namespace of declared
bindings. `implementation` is therefore a thin adapter: it finds the declaration, keeps the declaration's
own bindings (its library names, its resolved callees, its `sys` recorder), and substitutes the TEST'S
reduction, so the literal inputs live in the test and never in a shared declaration.

WHY `check` READS THE EQUATION ID FROM THE CALLING TEST'S MARKER. A test that stated its equation id in
a literal argument could state one thing in its decorator and check another, and nothing would notice.
Reading the marker makes the decorator the only statement of the id, so the identifier in the report, in
the ledger lookup and in the test's own selection expression are the same string by construction.

WHY ONLY `AssertionError` BECOMES A DIVERGENCE. A `TypeError` from the comparison surface means the test
handed it a value kind no licence can read — a defect of the test, not a disagreement between the code and
the paper. Wrapping it as a divergence would hand the classifier a red that is not one, and the
divergence record would carry a comparison that never happened.

Not established: that the class an author later records against a divergence is CORRECT. That is a ruling
about which artifact is authoritative, and it lives in `taxonomy.classify` over an authored `Diagnosis`;
this module produces the failure record and never classifies it.
"""

from __future__ import annotations

import contextlib
import dataclasses
from collections.abc import Callable, Iterator, Mapping
from typing import Any

import numpy as np
import pytest

from multidriver_swg.conformance.taxonomy import ConformanceFailure
from tests.conformance import bindings
from tests.correspondence import compare, lift


class EquationCheckError(Exception):
    """Base class for every refusal this module raises, so a caller can catch the family."""


class NotAnEquationMarker(EquationCheckError):  # noqa: N818  (the name the refusals are written against)
    """The calling test carries no conformance marker, or one whose unit names no registered equation."""


class UnboundEquation(EquationCheckError):  # noqa: N818  (the name the refusals are written against)
    """The marker names a registered equation whose ledger binding is not `TEST`, so no comparison is owed."""


class UnknownLiftUnit(EquationCheckError):  # noqa: N818  (the name the refusals are written against)
    """No lift declaration carries the requested unit and part, or more than one does."""


class UnknownOverride(EquationCheckError):  # noqa: N818  (the name the refusals are written against)
    """An override names a key the declaration does not bind, so the runtime could never receive it."""


class ConformanceDivergence(AssertionError):  # noqa: N818  (the name the published interface fixes)
    """One red conformance check, carrying the record a classifier consumes.

    It subclasses `AssertionError` so pytest reports it as an assertion failure and not as an error: the
    test reached its comparison and the comparison refused the value, which is what a divergence is.
    """

    def __init__(self, message: str, failure: ConformanceFailure) -> None:
        """Carry `failure` beside the message, which names the equation and the licence."""
        super().__init__(message)
        self.failure = failure


def check(
    request: pytest.FixtureRequest,
    observed: Any,
    expected: Any,
    licence: compare.Exact | compare.Tolerance,
) -> None:
    """Compare `observed` with `expected` under `licence`, raising `ConformanceDivergence` on a refusal.

    `request` is pytest's fixture of that name; only `request.node.get_closest_marker` is read, which is
    why this module's own tests can drive it with a stand-in whose node answers that one call. The
    equation id is the calling test's own marker `unit`.

    A refusal is turned into a divergence record carrying the ledger's landed binding and reachability, so
    the reader of a red does not have to look them up and cannot look them up differently. An agreement
    returns `None`. A `TypeError` from the comparison propagates unchanged: it is a test-authoring defect
    (a value kind no licence reads), not a paper/code disagreement.
    """
    equation_id = _equation_id(request)
    rows = bindings.load()
    row = rows.get(equation_id)
    if row is None:
        raise NotAnEquationMarker(
            f"the calling test's marker names unit {equation_id!r}, which no registered equation carries; "
            f"the register is keyed {next(iter(rows))}..{next(reversed(rows))}"
        )
    if row["binding"] != "TEST":
        raise UnboundEquation(
            f"{equation_id}: the ledger binds this equation as {row['binding']}, so no conformance test computes it "
            "and there is nothing to compare"
        )
    try:
        compare.assert_equivalent(observed, expected, licence)
    except AssertionError as refused:
        raise ConformanceDivergence(
            f"{equation_id}: the observed value disagrees with the expected value under {licence!r}",
            ConformanceFailure(
                equation_id=equation_id,
                observed=observed,
                expected=expected,
                binding=bindings.binding_of(row),
                reachable=bindings.reachable_of(row),
                diverged=True,
            ),
        ) from refused


def implementation(
    unit: str,
    part: str | None,
    reduce: Callable[[Mapping[str, Any]], Any],
    overrides: Mapping[str, Any] | None = None,
) -> Any:
    """Run the lift declaration of `unit`/`part` with the test's own reduction and literals, and return its value.

    The declaration's own reduction is discarded and the test's `reduce` is called with the finished
    namespace instead, so a test supplies the literal inputs through its reduction's call arguments and
    every declaration can serve many tests. `overrides` replaces declared bindings whose value the test
    owns — a cell unit's inputs are bindings rather than call arguments.

    Refused: a unit/part no declaration carries, and an override key the declaration does not bind (the
    runtime's own refusal names an override the text never reads, which is a different defect and is left
    to it). A refused lift module raises out of `lift.discover()` and is not caught: the discovery runs
    once per session and every module it executes is part of the population a red may come from.
    """
    declaration = _declaration(unit, part)
    extra = dict(overrides or {})

    def wrapped(fixtures: Mapping[str, Any]) -> tuple[dict[str, Any], Callable[[Mapping[str, Any]], Any]]:
        """The declaration's bindings with `extra` applied, together with the test's reduction."""
        declared, _ignored = declaration.function(fixtures)
        unknown = sorted(set(extra) - set(declared))
        if unknown:
            raise UnknownOverride(
                f"lift {unit!r} part {part!r} binds no {', '.join(unknown)}; it binds {', '.join(sorted(declared))}"
            )
        return {**declared, **extra}, reduce

    return lift.run_lift(dataclasses.replace(declaration, function=wrapped))


@contextlib.contextmanager
def seeded_global_numpy(seed: int) -> Iterator[None]:
    """Seed numpy's global generator for the block and restore the state the block found on exit.

    The legacy texts draw through `np.random.*` and through `DataFrame.sample` with no generator argument,
    so numpy's global state is the only channel a test can steer them by. Restoring on exit keeps one
    test's seed from changing what a later test draws — the failure that reads as a flaky red in a file
    the change did not touch.
    """
    state = np.random.get_state()
    try:
        np.random.seed(seed)
        yield
    finally:
        np.random.set_state(state)


def _equation_id(request: pytest.FixtureRequest) -> str:
    """The equation id from the calling test's own `conformance` marker, or a refusal naming the defect."""
    node = getattr(request, "node", None)
    marker = None if node is None else node.get_closest_marker("conformance")
    if marker is None:
        raise NotAnEquationMarker(
            "the calling test carries no conformance marker, so the equation it checks cannot be named"
        )
    unit = marker.kwargs.get("unit")
    if not isinstance(unit, str) or not unit.strip():
        raise NotAnEquationMarker(f"the calling test's conformance marker names no unit; it carries {marker.kwargs!r}")
    return unit


def _declaration(unit: str, part: str | None) -> lift.LiftDeclaration:
    """The one declaration for `unit` and `part`, or a refusal naming both and how many were found."""
    matches = [each for each in _declarations() if each.unit == unit and each.part == part]
    if len(matches) != 1:
        raise UnknownLiftUnit(
            f"lift {unit!r} part {part!r}: exactly one declaration must carry that pair; found {len(matches)}"
        )
    return matches[0]


def _declarations() -> list[lift.LiftDeclaration]:
    """Every lift declaration, discovered once per session.

    Cached because `lift.discover()` executes every module under `tests/lifts/` — seconds of import work
    and, more importantly, the population a refused module raises from. Discovering per call would run
    that population once per equation and make a refused module report itself 21 times over.
    """
    global _DECLARATIONS
    if _DECLARATIONS is None:
        _DECLARATIONS = lift.discover()
    return _DECLARATIONS


_DECLARATIONS: list[lift.LiftDeclaration] | None = None
