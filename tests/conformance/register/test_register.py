"""Prove the checked-in equation register carries 36 entries of the declared shape.

Every conformance test downstream reads the register through this package, and the drift
check reads only the spans the register gives it. So a register that silently lost an entry,
lost a field, or recorded an empty span beside a line range would not fail anything: it would
just narrow what the paper/code binding ranges over, and the narrowing would read as a smaller
corpus rather than as a defect. These tests are the only place that assumption is checked.

The positive arms read the real register, so they fail if the checked-in file drifts from the
schema. The refusal arms drive `validate_register` with deliberately broken copies, because a
validation branch that no test can reach is a claim that the loader validates, not evidence.
"""

from __future__ import annotations

import pytest

from tests.conformance.register import (
    EXPECTED_FIELDS,
    EXPECTED_IDS,
    RegisterValidationError,
    UnknownEquationError,
    entries,
    entry,
    validate_register,
)


@pytest.fixture()
def broken_register() -> dict:
    """A validated deep copy of the real register, for the tests that deliberately break it."""
    return entries()


def test_register_carries_the_36_equation_ids() -> None:
    """Property: the register's keys are exactly `E-01`..`E-36`, in order.

    Class: NEW CAPABILITY — the plausible wrong implementation is a register built from a
    filtered § B2 scrape, which skips the CROSS-REPO rows or the three implicit rows and
    still validates as JSON; this test is red under it because the key tuple differs.
    Reachability: a register missing any § B2 row reaches this assertion, and the real file
    is the input. Both-states anchor: the comparison is over the key SET and ORDER the shape
    fixes, not over any wording this change introduces.
    Invocation isolation (B4-4, arm 2): the register and the loader are opened from
    `__file__`, so neither the working directory nor which checkout the run resolves from can
    green or red this test; discrimination shown instead — dropping a row reddens it.
    Kills: dropping a row, renaming a row's id, or reordering the file.
    A second correct implementation: a loader that sorts keys on load still passes, because
    `E-01`..`E-36` sorts identically to its declared order.
    """
    register = entries()
    assert tuple(register) == EXPECTED_IDS


def test_every_entry_carries_exactly_the_four_declared_fields() -> None:
    """Property: each entry's field set equals the four fields the shape declares.

    Class: NEW CAPABILITY — the plausible wrong implementation is a loader that checks the
    types of whatever fields are present and silently tolerates a missing or renamed one;
    red under it because the field SETS differ.
    Reachability: any entry with a dropped, renamed or added field reaches this assertion.
    Both-states anchor: set equality against the declared names, not against the loader's
    error path.
    Invocation isolation (B4-4, arm 2): the entries come from the in-repo register, so no
    invocation change reaches the field sets; discrimination shown instead — adding a fifth
    field to any entry reddens it.
    Kills: dropping `defect_list`, renaming `verbatim_span`, adding an unreviewed field.
    A second correct implementation: a loader that returns entries as other mapping types
    (an `OrderedDict`, a typed dict) still passes, because the assertion compares key sets.
    """
    for equation_id, register_entry in entries().items():
        assert set(register_entry) == set(EXPECTED_FIELDS), equation_id


def test_entry_field_types_match_the_schema() -> None:
    """Property: `section_name` and `verbatim_span` are strings, `line_range` is two ints or
    null, and `defect_list` is a list of strings.

    Class: NEW CAPABILITY — the plausible wrong implementation is a loader that validates the
    top-level shape and trusts the leaf types; red under it on the first entry whose
    `line_range` arrived as a string.
    Reachability: a single-entry type error reaches this loop, and the real register is the
    input. Both-states anchor: the assertions read the values, never the loader's messages.
    Invocation isolation (B4-4, arm 2): the values are read from the in-repo register, so no
    invocation change reaches them; discrimination shown instead — writing one entry's
    `line_range` as a string reddens it.
    Kills: `line_range` written as `"250-250"`, a `defect_list` that becomes a bare string,
    a `section_name` that becomes null.
    A second correct implementation: a loader that emits each entry as a `dict` subclass, or
    one that validates through a typed intermediate and re-emits plain dicts, still passes —
    the assertions read the values (`isinstance(line_range, list)`, the two bounds, each
    note's type) and never the mapping's type or the loader's route to the values.
    """
    for equation_id, register_entry in entries().items():
        assert isinstance(register_entry["section_name"], str), equation_id
        assert isinstance(register_entry["verbatim_span"], str), equation_id
        line_range = register_entry["line_range"]
        if line_range is not None:
            assert isinstance(line_range, list), equation_id
            assert len(line_range) == 2, equation_id
            assert all(isinstance(bound, int) and not isinstance(bound, bool) for bound in line_range), equation_id
        assert isinstance(register_entry["defect_list"], list), equation_id
        assert all(isinstance(note, str) for note in register_entry["defect_list"]), equation_id


def test_empty_span_entries_record_why_the_manuscript_states_no_equation() -> None:
    """Property: the only entries with an empty `verbatim_span` are E-34, E-35 and E-36, and
    each carries `line_range` null and a non-empty `defect_list`.

    Class: NEW CAPABILITY — the plausible wrong implementation is a loader that treats an
    empty span as "no span to check" and lets its line range and note go unrecorded; red
    under it on either half.
    Reachability: an empty-span entry with a line range, or with an empty defect list,
    reaches the assertions. Both-states anchor: the assertions read the entry's own fields.
    Invocation isolation (B4-4, arm 2): the entries are read from the in-repo register, so no
    invocation change reaches them; discrimination shown instead — giving E-34 a line range
    reddens it.
    Kills: an implicit equation given a manufactured line range, or recorded with no note.
    A second correct implementation: a register that lists the three ids in a different
    order still passes, because the assertion compares a set.
    """
    register = entries()
    empty_span_ids = {
        equation_id for equation_id, register_entry in register.items() if register_entry["verbatim_span"] == ""
    }
    assert empty_span_ids == {"E-34", "E-35", "E-36"}
    for equation_id in sorted(empty_span_ids):
        assert register[equation_id]["line_range"] is None, equation_id
        assert register[equation_id]["defect_list"], equation_id


def test_entry_returns_one_entry_and_hands_back_a_copy() -> None:
    """Property: `entry(id)` returns that entry, and mutating the result does not reach the
    next caller.

    Class: NEW CAPABILITY — the plausible wrong implementation is a loader that returns the
    cached dict itself, so one test's edit leaks into the next; red under it.
    Reachability: any caller that mutates a returned entry reaches the aliasing assertion.
    Both-states anchor: the second read is compared with the first, never with a literal.
    Invocation isolation (B4-4, arm 2): the aliasing under test is between two in-process
    reads, so no invocation change reaches it; discrimination shown instead — returning the
    cached dict rather than a copy reddens it.
    Kills: returning the cache, or returning a shallow copy whose `defect_list` is shared.
    A second correct implementation: a loader that re-parses the file on every call passes,
    because the assertions only require that the two reads be equal before the mutation and
    independent after it.
    """
    first = entry("E-04")
    assert first == entries()["E-04"]
    first["defect_list"].append("mutated by a test")
    assert entry("E-04")["defect_list"] != first["defect_list"]


def test_entry_rejects_an_unknown_id() -> None:
    """Property: `entry` raises `UnknownEquationError` naming an id outside `E-01`..`E-36`.

    Class: NEW CAPABILITY — the plausible wrong implementation is a bare dict lookup, which
    raises `KeyError` and says nothing about the register's key set; red under it.
    Reachability: any misspelled or out-of-range id reaches the refusal.
    Both-states anchor: the assertion names the exception type and the id, not the wording
    of a message this change wrote.
    Invocation isolation (B4-4, arm 2): the refusal is raised in-process from the in-repo
    register, so no invocation change reaches it; discrimination shown instead — looking the
    id up in a bare dict raises the uncaught `KeyError` and reddens it.
    Kills: `entry` falling back to `None` for an unknown id.
    A second correct implementation: an exception that also subclasses `KeyError` passes,
    because the assertion is `pytest.raises(UnknownEquationError)`.
    """
    with pytest.raises(UnknownEquationError, match="E-99"):
        entry("E-99")


def test_refuses_a_missing_entry(broken_register: dict) -> None:
    """Property: a register missing one of the 36 rows is refused, naming the missing id.

    Class: NEW CAPABILITY — the plausible wrong implementation is a loader that checks only
    that every PRESENT entry is well formed; red under it because nothing is present to check.
    Reachability: deleting any row reaches the count/set check.
    Both-states anchor: the assertion is on the raised type and the named id.
    Invocation isolation (B4-4, arm 2): the broken copy is built in memory from the in-repo
    register, so no invocation change reaches it; discrimination shown instead — a validator
    that never checks the key count reddens it by accepting the truncated copy.
    Kills: validation over present entries only.
    A second correct implementation: a loader that reports the count without the ids still
    passes only if it names E-07, which is the requirement — a count alone does not.
    """
    del broken_register["E-07"]
    with pytest.raises(RegisterValidationError, match="E-07"):
        validate_register(broken_register)


def test_refuses_an_extra_entry(broken_register: dict) -> None:
    """Property: a register carrying a 37th equation ID is refused, naming the extra id.

    Class: NEW CAPABILITY — the plausible wrong implementation is a loader that validates a
    subset of keys and ignores the rest; red under it.
    Reachability: an added key reaches the set check.
    Both-states anchor: the assertion names the exception and the offending id.
    Invocation isolation (B4-4, arm 2): the extra key is added in memory, so no invocation
    change reaches it; discrimination shown instead — a validator that ignores keys outside
    the ones it found reddens it.
    Kills: accepting an unreviewed row, which is how a register grows without § B2 agreeing.
    A second correct implementation: a loader that fails on the count before the set still
    passes only if its message names E-37.
    """
    broken_register["E-37"] = broken_register["E-01"]
    with pytest.raises(RegisterValidationError, match="E-37"):
        validate_register(broken_register)


def test_refuses_a_wrong_field_type(broken_register: dict) -> None:
    """Property: an entry whose `line_range` is not two integers or null is refused, naming
    the entry.

    Class: NEW CAPABILITY — the plausible wrong implementation is a loader that trusts leaf
    types; red under it.
    Reachability: a string line range reaches the type branch.
    Both-states anchor: the assertion names the exception and E-04's id.
    Invocation isolation (B4-4, arm 2): the bad value is written into an in-memory copy, so
    no invocation change reaches it; discrimination shown instead — a validator that trusts
    leaf types reddens it.
    Kills: a hand-edited register whose line range became a string or a one-element list.
    A second correct implementation: a loader that rejects at the first bad entry it reaches
    passes, because only one entry is broken.
    """
    broken_register["E-04"]["line_range"] = "250"
    with pytest.raises(RegisterValidationError, match="E-04"):
        validate_register(broken_register)


def test_refuses_an_empty_span_that_carries_a_line_range(broken_register: dict) -> None:
    """Property: an entry with an empty `verbatim_span` and a non-null `line_range` is
    refused, naming the entry.

    Class: NEW CAPABILITY — the plausible wrong implementation is a loader that checks the
    empty-span rule by asserting the span is empty and never the line range; red under it.
    Reachability: manufacturing a line range for E-34 reaches the cross-field rule.
    Both-states anchor: the assertion names the exception and E-34's id.
    Invocation isolation (B4-4, arm 2): the manufactured line range is written in memory, so
    no invocation change reaches it; discrimination shown instead — a validator that checks
    only that the span is empty reddens it.
    Kills: an implicit equation given a line range that no span occupies, which would make
    the drift check's line-range assertion meaningless for it.
    A second correct implementation: a loader that checks the defect list before the line
    range still passes, because only the line range is broken here.
    """
    broken_register["E-34"]["line_range"] = [1, 2]
    with pytest.raises(RegisterValidationError, match="E-34"):
        validate_register(broken_register)


def test_refuses_an_entry_missing_a_declared_field(broken_register: dict) -> None:
    """Property: an entry that dropped one of the four declared fields is refused, naming the
    entry and the dropped field.

    Class: NEW CAPABILITY — the plausible wrong implementation is a validator that type-checks
    whatever fields an entry happens to carry and never compares the field SET. Under it a
    dropped field escapes as a bare `KeyError` from the first read that needs it, or reads as
    absent data at the call site; the test is red under that implementation with
    `KeyError: 'defect_list'` (two-arm differential, reproduced by the reviewing seat).
    Reachability: dropping any one of the four fields from any of the 36 entries reaches this
    branch; the fixture builds it from the real register.
    Why required: the module docstring states this is the direction that matters most — "an
    entry that LOST a field would otherwise read as absent data at every call site instead of
    as a broken register" — and until now no test held that claim, because the one field-set
    test reads the checked-in file, which is well formed.
    Both-states anchor: the assertion is the exception type plus the two names the message
    must carry, neither of which this test introduces.
    Invocation isolation (B4-4, arm 2): the broken entry is built in memory from the in-repo
    register, so no change to the working directory or to which checkout the run resolves from
    can green or red it; discrimination shown instead — a field-set-blind validator lets the
    `KeyError` escape, which reddens the `pytest.raises`.
    Kills: a validator that type-checks present fields only.
    A second correct implementation: a validator that reads each expected field through a
    `.get()` sentinel and raises on absence still passes, because the assertion is the type and
    the names, not the order in which the four fields are inspected.
    """
    del broken_register["E-04"]["defect_list"]
    with pytest.raises(RegisterValidationError, match="E-04.*defect_list"):
        validate_register(broken_register)


def test_refuses_an_entry_carrying_an_undeclared_field(broken_register: dict) -> None:
    """Property: an entry that gained a field the schema does not carry is refused, naming the
    entry and the undeclared field.

    Class: NEW CAPABILITY — the plausible wrong implementation is a validator that ignores
    fields outside the four it knows, which accepts the broken register outright; the test is
    red under it with "validation accepted the broken register" (two-arm differential,
    reproduced by the reviewing seat).
    Reachability: adding any key to any entry reaches this branch.
    Why required: the module docstring's first strictness claim is that "an entry that gained a
    field would be an entry whose author believed something the schema does not carry", and
    § 3.2 fixes the value as carrying "exactly" the four fields. A new field therefore means the
    register and the schema have diverged, which is the drift no other test detects.
    Both-states anchor: the assertion is the exception type plus the entry id and the field name.
    Invocation isolation (B4-4, arm 2): the added key is written into an in-memory copy, so no
    invocation change reaches it; discrimination shown instead — an ignore-extras validator
    accepts the copy and the `pytest.raises` fails.
    Kills: a schema widened in code but not in the shape, or an entry grown by hand.
    A second correct implementation: a validator that compares sorted field names and reports
    the difference still passes, because it names the same entry and field.
    """
    broken_register["E-12"]["equation_number"] = 12
    with pytest.raises(RegisterValidationError, match="E-12.*equation_number"):
        validate_register(broken_register)


def test_refuses_an_empty_span_with_no_defect_note(broken_register: dict) -> None:
    """Property: an entry with an empty `verbatim_span` and an empty `defect_list` is refused,
    naming the entry and the field.

    Class: NEW CAPABILITY — the plausible wrong implementation enforces the empty-span rule's
    line-range half and not its note half, which accepts this input; the test is red under it
    with "validation accepted the broken register" (two-arm differential, reproduced by the
    reviewing seat).
    Reachability: emptying the note of any one of the three implicit equations reaches it.
    Why required: § 4 item 2 requires that "for an empty span, `line_range` is `null` and
    `defect_list` is non-empty"; the existing empty-span refusal test covers only the line-range
    half, so this half of a shape-specified rule had no test at all.
    Both-states anchor: the assertion is the exception type plus the entry id and the field name,
    so it does not depend on the wording of the message this rule wrote.
    Invocation isolation (B4-4, arm 2): the emptied note is written into an in-memory copy, so
    no invocation change reaches it; discrimination shown instead — a validator missing this
    rule accepts the copy and the `pytest.raises` fails.
    Kills: an implicit equation recorded with no reason, which would make the three implicit
    rows indistinguishable from rows whose notes were never written.
    A second correct implementation: a validator that reads the note list with a truthiness test
    before the line-range rule still passes, because only the note is broken here.
    """
    broken_register["E-35"]["defect_list"] = []
    with pytest.raises(RegisterValidationError, match="E-35.*defect_list"):
        validate_register(broken_register)
