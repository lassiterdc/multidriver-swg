"""Check the provenance surface: the identity is frozen, the record is captured rather than rebuilt, and an incomplete record is refused.

A provenance record is worth having only if a missing producer is refused rather than read silently,
so each check here is written against a form a silent non-declaration takes: an identity that can be
mutated after capture, a record a second function rebuilds from inputs, a completeness check that
refuses nothing, and a docstring that stops stating the ruling. Every check names the wrong
implementation it is red under, because a check that cannot fail for the reason it claims constrains
nothing.

Invocation isolation. Every test here is NEW CAPABILITY over a module this turn creates, so the
pre-change tree holds no module to invoke and the tree-substitution arm is unreachable. The
discrimination arm applies: each test's `Killed by:` names a change to what the code DOES on that
test's input, and that is the discrimination this suite is run forward on.

The names are imported from `multidriver_swg.run_configuration` rather than from `provenance`
directly, so collection itself exercises the package's re-export of the five provenance names.
"""

from __future__ import annotations

import ast
import dataclasses
import inspect
import typing
from pathlib import Path

import pytest

import multidriver_swg.run_configuration.provenance as provenance
from multidriver_swg.run_configuration import (
    IdentityDeclaration,
    ImplementationIdentity,
    ProvenanceRecord,
    capture,
    require_complete,
)

# The two statements the ruling is made of, quoted from the record rather than paraphrased: the
# checks below assert these exact sentences appear in the module's docstring.
SELF_DECLARATION_RULING = (
    "every compute system declares its implementation identity into the run's record through a small "
    "self-declaration interface it implements; the record is captured at run time, and no central registry is built"
)
CAPTURE_NOT_RECONSTRUCT = "the record is captured at run time, never rebuilt from inputs afterwards"

# The two halves of the draw scan's own reading, kept beside the checks that use them.
RECORD_NAME = "ProvenanceRecord"
# The two call names a rebuild can reach a record through: the record class itself, or the module's
# only assembler. A rebuild that delegates to `capture` names neither the class nor an annotation,
# so reading the class name alone leaves it invisible while the module still exposes it.
RECORD_BUILDING_CALLS = (RECORD_NAME, "capture")


def _identity(system: str = "swg") -> ImplementationIdentity:
    """One declared identity, so a test states which system it is proving something about."""
    return ImplementationIdentity(system=system, implementation="vine-copula", code_version="0.2.0.dev0")


def _dotted(node: ast.expr) -> str:
    """The dotted name an annotation or an attribute chain reads, or the bare name where it ends in a name.

    A string annotation is returned as its own text, because `from __future__ import annotations`
    turns every annotation into a string at run time and a scan that ignored that form would miss a
    module that spells its return as `"ProvenanceRecord"`.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def _record_builders(source: str) -> set[str]:
    """Names of the functions in `source` that return a `ProvenanceRecord`, construct one, or assemble one.

    Three halves are read because each is blind to a different rebuild: a function annotated
    `-> ProvenanceRecord` is a constructor even when it returns some other function's result, a
    function calling `ProvenanceRecord(...)` is one even with no annotation at all, and a function
    calling `capture(...)` is one because `capture` is the module's only assembler — a rebuild that
    delegates to it names neither the class nor an annotation.
    """
    tree = ast.parse(source)
    builders: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        if node.returns is not None and _dotted(node.returns).rsplit(".", 1)[-1] == RECORD_NAME:
            builders.add(node.name)
        for inner in ast.walk(node):
            if isinstance(inner, ast.Call) and _dotted(inner.func).rsplit(".", 1)[-1] in RECORD_BUILDING_CALLS:
                builders.add(node.name)
                break
    return builders


def _parameter_names(arguments: ast.arguments) -> list[str]:
    """Every parameter name in `arguments`, across all three of Python's parameter lists.

    Reading `args.args` alone leaves a keyword-only or a positional-only `inputs` invisible, and a
    delegating rebuild takes exactly those spellings — so the three lists are read together rather
    than one standing for all of them.
    """
    return [arg.arg for arg in (*arguments.posonlyargs, *arguments.args, *arguments.kwonlyargs)]


def _inputs_only_functions(source: str) -> set[str]:
    """Names of the functions in `source` whose whole parameter list is `inputs`, whatever kind carries it.

    Not established: a rebuild whose parameter list is not exactly `inputs` — `def rebuild(inputs,
    seeds=None)`, say — stays outside this scan. Widening to "carries `inputs`" would name every
    function that takes a record's inputs as one of several arguments, which is not a rebuild.
    """
    tree = ast.parse(source)
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and _parameter_names(node.args) == ["inputs"]
    }


def test_the_identity_is_frozen_and_carries_the_three_fields():
    """Property: `ImplementationIdentity` has exactly the fields `system`, `implementation` and `code_version`; constructing it with all three succeeds; assigning to a field raises `FrozenInstanceError`.

    Class: NEW CAPABILITY. The plausible wrong implementation is an identity built as a plain
    mutable class or a `dict`, which a system could mutate after declaring it, so a record would hold
    whatever the system last wrote rather than what it declared. The check is red under it at the
    `FrozenInstanceError` arm, which a mutable class does not raise.
    Reachability: the input class is any construction of the identity, and the admit arm below
    constructs one with the three fields, so the frozen arm is shown to be reading a type that does
    construct rather than one nothing can build.
    Both-states anchor: the assertions turn on `dataclasses.fields` and on whether assignment raises,
    both properties of the declaration rather than wording this test introduces.
    Killed by: unfreezing the dataclass, renaming a field, or adding a fourth field.
    Second correct implementation: `attrs` with `frozen=True`, or a `NamedTuple` — a `NamedTuple`
    assigns through `object.__setattr__` only and raises `AttributeError` on a plain assignment, so
    it passes the fields arm and is refused only by the `FrozenInstanceError` arm; a tuple-backed
    identity is therefore a different type rather than a different spelling of this one.
    """

    assert [field.name for field in dataclasses.fields(ImplementationIdentity)] == [
        "system",
        "implementation",
        "code_version",
    ]

    identity = _identity()
    assert identity.system == "swg"
    assert identity.implementation == "vine-copula"
    assert identity.code_version == "0.2.0.dev0"

    with pytest.raises(dataclasses.FrozenInstanceError):
        identity.system = "other"


def test_the_declaration_protocol_declares_a_call_returning_an_implementation_identity():
    """Property: `IdentityDeclaration.__call__` takes no argument but `self` and is annotated to return `ImplementationIdentity`, and a conforming declaration's result is what a record holds.

    Class: NEW CAPABILITY. The plausible wrong implementation is a Protocol whose `__call__` is
    annotated `-> object`, or which declares parameters, which reduces the structural half of the
    contract
    to a docstring claim: a sibling's declaration returning a bare string would then satisfy the
    Protocol and a record would hold something that is not an identity. The check is red under it at
    the resolved-annotation arm.
    Reachability: the input class is any declaration, and the conforming callable below is a member
    of it that the Protocol admits, so the assertion is shown to be reading a declaration that
    exists rather than refusing every declaration.
    Both-states anchor: the assertions turn on `inspect.signature` and on the resolved annotation
    object, both properties of the declaration and not wording this test introduces.
    Killed by: annotating the return as anything but `ImplementationIdentity`, or giving `__call__` a
    parameter.
    Second correct implementation: a Protocol declaring `__call__(self) -> ImplementationIdentity`
    with the parameters positional-only, or one whose `__call__` is declared through a
    `Callable[[], ImplementationIdentity]` alias — the test reads the resolved annotation and the
    parameter names, so neither form is refused.
    """

    parameters = inspect.signature(IdentityDeclaration.__call__).parameters
    assert list(parameters) == ["self"], sorted(parameters)

    hint = typing.get_type_hints(IdentityDeclaration.__call__)["return"]
    assert hint is ImplementationIdentity, hint

    def declare() -> ImplementationIdentity:
        """A conforming declaration: no argument, an identity returned."""
        return _identity()

    declared = declare()
    assert isinstance(declared, ImplementationIdentity)
    assert capture(inputs={}, seeds={}, identities={"swg": declared}).identities["swg"] is declared


def test_the_record_is_frozen_and_carries_the_three_fields_and_capture_assembles_it():
    """Property: `ProvenanceRecord` has exactly `inputs`, `seeds` and `identities`; `capture` assembles one from what it is given; assigning to a field raises `FrozenInstanceError`.

    Class: NEW CAPABILITY. The plausible wrong implementation is a `TypedDict` or a plain `dict`,
    which is mutable at run time, so "captured and held" is not enforced and a record with a field
    removed passes construction silently. The check is red under it at the field-name arm and again
    at the frozen arm.
    Reachability: the input class is any record construction; the admit arm builds one through
    `capture` and reads all three fields back, so the frozen arm is reading a type that constructs.
    Both-states anchor: the assertions turn on `dataclasses.fields` and on whether assignment raises,
    both properties of the declaration.
    Killed by: unfreezing the record, renaming a field, dropping a field, or making `capture` drop
    one of the three on the way in.
    Second correct implementation: `attrs` with `frozen=True`, or a dataclass declaring the same
    three fields in a different order — the test reads the names as written, so it pins the order the
    module chose; a different order is a different record and is not admitted here, which is the one
    place this test constrains the implementation.
    """

    record = capture(inputs={"mrms": "path"}, seeds={"rainfall": 24}, identities={"swg": _identity()})

    assert [field.name for field in dataclasses.fields(ProvenanceRecord)] == ["inputs", "seeds", "identities"]
    assert record.inputs == {"mrms": "path"}
    assert record.seeds == {"rainfall": 24}
    assert record.identities == {"swg": _identity()}
    assert isinstance(record, ProvenanceRecord)

    with pytest.raises(dataclasses.FrozenInstanceError):
        record.seeds = {}


def test_require_complete_refuses_a_missing_identity_and_admits_a_complete_record():
    """Property: `require_complete` raises `ProvenanceError` naming each exercised system that has no identity, and returns None when every exercised system is named.

    Class: NEW CAPABILITY. The plausible wrong implementation is a `require_complete` that returns
    None for every input — a completeness check that checks nothing, which is a check that passes by
    refusing nothing. The mirror-image wrong implementation refuses every input, and the
    admit arm below is what catches it, so the refusal is shown to discriminate rather than to refuse
    everything.
    Reachability: the input class is any (record, exercised set) pair, and three members of it are
    exercised — a complete record, a record missing one system, and a record missing two — so both
    the admit and the refuse arms are reached.
    Both-states anchor: the assertions turn on whether `ProvenanceError` is raised and on which
    systems the message names, both consequences of the contract rather than wording this test
    introduces.
    Killed by: returning None unconditionally; refusing a complete record; or building the message
    from the record's identities rather than from the exercised set, which names the systems that
    DID declare.
    Second correct implementation: a check that computes the exercised set as a `set` difference
    rather than a comprehension, or one raising on the first missing system alone — the test asserts
    that the two absent systems are both named, which the set-difference form satisfies; the
    first-only form is refused, because "naming each exercised system" is the contract.
    """

    complete = capture(
        inputs={},
        seeds={"rainfall": 24},
        identities={"swg": _identity("swg"), "triton": _identity("triton")},
    )
    assert require_complete(complete, ["swg", "triton"]) is None

    partial = capture(inputs={}, seeds={"rainfall": 24}, identities={"swg": _identity("swg")})
    with pytest.raises(provenance.ProvenanceError) as missing:
        require_complete(partial, ["swg", "triton", "ssfha"])

    message = str(missing.value)
    assert "'triton'" in message, message
    assert "'ssfha'" in message, message
    assert "'swg'" not in message, message


def test_a_record_naming_only_inputs_and_seeds_is_rejected():
    """Property: a record whose `identities` is empty is refused against a non-empty exercised set.

    Class: NEW CAPABILITY. The plausible wrong implementation is a completeness check that treats
    "the record carries the three fields" as completeness, so a record naming only inputs and seeds
    passes — which is exactly the record the module must refuse, and a check that admits it checks
    nothing. The check is red under it.
    Reachability: the input class is any record with no identities and any non-empty exercised set;
    one member is exercised, and the previous test's admit arm shows a record WITH identities passes,
    so the refusal is attributable to the missing identities rather than to the record type.
    Both-states anchor: the assertion turns on whether the incomplete record is refused, a
    consequence the contract states, not on wording this test introduces.
    Killed by: treating an empty `identities` as complete, or short-circuiting when `identities` is
    empty.
    Second correct implementation: any check that refuses on the missing-system computation — the
    empty mapping produces every exercised system as missing, so a set-difference form and a
    comprehension form both refuse here.
    """

    record = capture(inputs={"mrms": "path"}, seeds={"rainfall": 24}, identities={})

    with pytest.raises(provenance.ProvenanceError) as incomplete:
        require_complete(record, ["swg"])

    assert "'swg'" in str(incomplete.value), str(incomplete.value)


def test_require_complete_splits_a_wrong_call_from_an_incomplete_record():
    """Property: a structurally wrong argument raises `TypeError` and an incomplete record raises `ProvenanceError`, and the two classes are disjoint.

    Class: NEW CAPABILITY. The plausible wrong implementation is one exception for both conditions —
    a single `ValueError`, or a `ProvenanceError` also used for a caller's mistake — which erases the
    distinction between "you called it wrong" and "the run is incomplete", the
    second being the halt a run must stop on. The check is red under it at the disjointness arm.
    Reachability: the input class is any call to `require_complete`; four members are exercised — a
    non-record, a non-iterable set, a bare string, and a structurally right but incomplete pair — so
    both arms are reached from arguments of the right and the wrong form.
    Both-states anchor: the assertions turn on which exception class is raised, a consequence of the
    stated contract rather than wording this test introduces.
    Killed by: raising `TypeError` for an incomplete record, raising `ProvenanceError` for a
    non-iterable set, or making `ProvenanceError` a subclass of `TypeError`.
    Second correct implementation: `ValueError` in place of the contract's `TypeError` is NOT a second
    correct implementation — the contract names `TypeError`, so it is refused. A correct alternative is
    a check that validates its arguments in a different order, or one that raises the `TypeError` for
    a bare string at a different guard; the test asserts classes only, never the order or the message.
    """

    record = capture(inputs={}, seeds={}, identities={})

    with pytest.raises(TypeError):
        require_complete({"inputs": {}}, ["swg"])
    with pytest.raises(TypeError):
        require_complete(record, 7)
    with pytest.raises(TypeError):
        require_complete(record, "swg")
    with pytest.raises(provenance.ProvenanceError):
        require_complete(record, ["swg"])

    assert issubclass(provenance.ProvenanceError, Exception)
    assert not issubclass(provenance.ProvenanceError, TypeError)


def test_no_function_builds_a_record_from_inputs_alone(tmp_path: Path):
    """Property: `capture` is the module's only record constructor, and no function in it takes `inputs` alone.

    Class: NEW CAPABILITY. The plausible wrong implementation is a second, convenience constructor —
    `rebuild(inputs)` returning a record assembled from the inputs — which is the reconstruction the
    module exists to prevent: it would let any caller produce a record that says nothing about which
    implementation ran, while every other check in this module still passes. The check is red under
    it at both arms.
    Reachability: the input class is any module source, and the control below is a member of it that
    DOES build a record, so a clean result over the module is the instrument finding nothing rather
    than not looking.
    Both-states anchor: the assertions turn on the syntax tree's function definitions and call sites,
    properties of the code rather than wording this test introduces.
    Killed by: adding any function that returns or constructs a `ProvenanceRecord` other than
    `capture`, or giving a function the parameter list `(inputs)`.
    Second correct implementation: a module that builds its record through a `classmethod` named
    something else is still refused, because however the constructor is spelled it is a second one;
    a correct alternative is `capture` implemented by calling a private helper that RETURNS the
    record — the test would refuse that too, and the honest form is the one `capture` uses: the
    private helper, where one is needed, returns something other than a record.
    Not established: a rebuild reached by calling a function from another module; the population of
    this scan is this module's source and nothing else.
    """

    source = Path(provenance.__file__ or "").read_text(encoding="utf-8")
    assert _record_builders(source) == {"capture"}, _record_builders(source)
    assert _inputs_only_functions(source) == set(), _inputs_only_functions(source)

    control = tmp_path / "rebuilds.py"
    control.write_text(
        "from multidriver_swg.run_configuration.provenance import ProvenanceRecord\n"
        "\n\n"
        "def rebuild(inputs) -> ProvenanceRecord:\n"
        "    return ProvenanceRecord(inputs=inputs, seeds={}, identities={})\n",
        encoding="utf-8",
    )
    control_source = control.read_text(encoding="utf-8")
    assert _record_builders(control_source) == {"rebuild"}, (
        "the scan found no record builder in a module that builds one"
    )
    assert _inputs_only_functions(control_source) == {"rebuild"}, "the parameter scan found no inputs-only function"


# Three rebuild spellings, each pinning a DIFFERENT arm of the scan: the first two put `inputs` in
# a parameter-kind list `args.args` cannot see, and the third additionally reaches the record through
# a name bound to the class, which the call-name arm cannot see. Kept as module state so the
# parametrized test below states the spelling as its input and not as a fixture detail.
REBUILD_SPELLINGS = {
    "keyword-only": ("def rebuild(*, inputs):\n    return capture(inputs=inputs, seeds={}, identities={})\n"),
    "positional-only": ("def rebuild(inputs, /):\n    return capture(inputs=inputs, seeds={}, identities={})\n"),
    "aliased, keyword-only": (
        "P = ProvenanceRecord\n\n\ndef rebuild(*, inputs):\n    return P(inputs=inputs, seeds={}, identities={})\n"
    ),
}


@pytest.mark.parametrize("spelling", sorted(REBUILD_SPELLINGS))
def test_the_reconstruction_scan_names_a_rebuild_whose_inputs_is_not_a_plain_arg(spelling: str):
    """Property: a function that assembles a record from `inputs` alone is NAMED by the reconstruction scan whatever parameter KIND carries `inputs` and whatever name the construction goes through.

    Class: NEW CAPABILITY. The plausible wrong implementation is what the tree carried before the fix — a
    scan that reads `args.args` only and that names a function only when it writes the record class
    literally or annotates its return — so a rebuild spelled `def rebuild(*, inputs)` and delegating
    to `capture` is invisible to both arms, and "no reconstruction" is then enforced for two spellings
    of the class and asserted for all of it.
    Reachability: the input class is any source defining a function whose whole parameter list is
    `inputs`, and all three members of it below are such sources, so the assertion is shown to be
    reading a module that does rebuild rather than one that does not.
    Both-states anchor: the assertion turns on whether the scan names a function, a property the
    module's own shape of source fixes, and not on wording this test introduces.
    Killed by, measured per member rather than asserted: reverting the parameter union
    (`_parameter_names` to `arguments.args` alone) reds the aliased member and nothing else, because
    that is the one spelling whose construction no call name reaches; reverting the call-name set
    (`RECORD_BUILDING_CALLS` to the record name alone) reds no member, because the parameter union
    carries all three on its own; reverting both reds all three.
    Second correct implementation: a scan that unions `posonlyargs`, `args` and `kwonlyargs` and
    compares the parameter NAMES, and one that reads `ast.arguments` kinds explicitly and refuses a
    rebuild whose every non-`self` parameter resolves to `inputs` — both pass this assertion.
    Not established, both measured against the delivered scan: no member isolates the call-name arm,
    because the only function it names and the parameter union does not is one building a record from
    a parameter list that is NOT exactly `inputs` — outside D-6's predicate, so asserting it here
    would assert past the shape; and a rebuild that is BOTH aliased and parameterised with more than
    `inputs` is named by neither arm (`arm1=[] arm2=[]`, against `arm1=['rebuild']` for the same
    parameter list constructing the class literally), which is the two residuals F1 named in
    conjunction. That case is not a member here and adding it would assert past the predicate.
    """

    source = REBUILD_SPELLINGS[spelling]

    named = _record_builders(source) | _inputs_only_functions(source)
    assert named == {"rebuild"}, (
        f"the scan named no record builder in a {spelling} module that rebuilds one from `inputs` alone: {named}"
    )


def test_the_module_docstring_states_the_ruling_the_convention_and_the_two_enforcers():
    """Property: the module docstring carries the self-declaration ruling and the capture-not-reconstruct convention verbatim, and names both enforcing instruments.

    Class: NEW CAPABILITY. The plausible wrong implementation is a docstring that describes the
    three types and stops there, dropping the two statements the declaration is the deliverable for,
    and naming neither enforcing instrument — so the next editor reads a module with no stated
    rejection of a central registry. The check is red under it.
    Reachability: the input class is any version of the docstring, and the whole of it is searched,
    so every version is reached.
    Both-states anchor: the assertions turn on the presence of the module's own fixed sentences, which
    the module states as its deliverable text, not on wording this test introduces.
    Killed by: deleting or rephrasing either sentence, or ceasing to name `IdentityDeclaration` or
    `require_complete`.
    Second correct implementation: the sentences at any position in any surrounding prose — the test
    asserts containment, so it constrains neither order nor the wording around them.
    """

    docstring = provenance.__doc__ or ""

    assert SELF_DECLARATION_RULING in docstring
    assert CAPTURE_NOT_RECONSTRUCT in docstring
    assert "IdentityDeclaration" in docstring
    assert "require_complete" in docstring
