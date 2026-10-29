"""Tests of the result record's field vocabulary: its two closed value sets and its subject placeholder.

The vocabulary is a declaration, so these tests read the declaration and assert the properties a
consumer relies on rather than the constants' storage form. A value set is closed when a value
outside it cannot be admitted, and the subject placeholder is a reading when it is a value rather
than the absence of one -- both are properties a reader of a record acts on, and neither is held by
the constants' spelling, ordering or container type.
"""

import json

import pytest

from tests.correspondence import record_schema

# Every module under this directory must contribute a node to the State check job's marker
# selection, because `test_divergence_mounts.py` reads the directory and fails on a module that
# contributes none, and that job is the only workflow that runs pytest. Stated here as a
# consequence rather than as convention: dropping this line is not a missing nicety, it is CI
# silently not running this file.
pytestmark = pytest.mark.state_check


def test_the_verdict_vocabulary_admits_three_values_and_no_other():
    """Property: the verdict vocabulary is exactly `pass`, `fail` and `VOID`; a fourth value is not admitted.

    Class (input): the verdict vocabulary as its consumers read it -- the emitter that writes a
    verdict and any reader that branches on one; artifact: `record_schema.VERDICTS`, whose members
    are the only three values a record may carry. Build class: NEW CAPABILITY -- the plausible wrong
    implementation spells the third value lower case (`("pass", "fail", "void")`), which a
    case-sensitive reader would see as an unknown state; a second wrong implementation admits a
    fourth state such as `"skipped"`, which is what a skipped test run would write. This test is red
    under both. Why: a consumer branches on these three values, so a value outside them falls
    through every branch and reads as neither a pass nor a failure. Both-states anchor: the
    assertion turns on set membership, a property present whichever way the set is declared, not on
    the tuple's order or its repr. Invocation isolation: not isolatable from this tree --
    discrimination shown instead: rebinding the module's `VERDICTS` to a renamed member reds this
    test, done off-tree because the working tree is shared. Killed by: re-casing a member, adding a
    member, or moving the vocabulary to a container whose members the comparison cannot read (it
    reads the set of members, so a list or a frozenset of the same three is not killed). Second
    correct implementation: the same three members declared as a `frozenset` or a list.
    """
    assert set(record_schema.VERDICTS) == {"pass", "fail", "VOID"}


def test_the_completeness_marker_admits_three_states_and_no_other():
    """Property: the completeness marker's vocabulary is exactly `pre-collection`, `collected` and `complete`.

    Class (input): the marker as its consumers read it -- the emitter advancing it across the staged
    write and a reader deciding whether a record describes a finished run; artifact:
    `record_schema.COMPLETENESS`, whose members are the only three states a record may carry. Build
    class: NEW CAPABILITY -- the plausible wrong implementation folds `collected` and `complete` into
    one state, which erases the only distinction between a run killed during collection and one that
    finished; a second wrong implementation spells `pre-collection` with an underscore. Red under
    both. Why: the marker is what makes a killed run read as incomplete, so a two-state vocabulary
    that merges the middle state into the last is exactly the defect the marker exists to prevent. Both-states anchor: the assertion turns on set
    membership, not on the sequence's length, order or repr. Invocation isolation: not isolatable
    from this tree -- discrimination shown instead: rebinding `COMPLETENESS` to the merged pair reds
    this test. Killed by: dropping a state, renaming one, or adding a fourth. Second correct
    implementation: the same three strings declared as a list or a frozenset.
    """
    assert set(record_schema.COMPLETENESS) == {"pre-collection", "collected", "complete"}


def test_an_unresolved_subject_is_a_value_not_the_absence_of_one():
    """Property: the subject placeholder is a non-`None` mapping that serializes to `{"state": "unresolved"}`.

    Class (input): every record written before the subject can be resolved, whose
    `subject_resolution` field carries this placeholder; artifact: `record_schema.SUBJECT_UNRESOLVED`,
    which the emitter writes into a record before any subject reading exists. Build
    class: NEW CAPABILITY -- the plausible wrong implementation declares the placeholder as `None`,
    which makes an unresolved reading identical to a record with no such field; a second wrong
    implementation declares it as `{}`, an empty mapping that carries no state to read. Red under
    both (`None` fails the identity check, `{}` fails the state check). Why: a consumer must be able
    to tell "this run's record says the subject did not resolve" from "this record never asked", and
    the record is JSON, so a placeholder that does not survive serialization would arrive at the
    reader as something else entirely. Both-states anchor: the assertion turns on the mapping's
    content and its JSON projection, both properties of any correct declaration of this value, not on
    the dict literal's construction or the object's identity. Invocation isolation: not isolatable
    from this tree -- discrimination shown instead: rebinding `SUBJECT_UNRESOLVED` to `None` reds
    this test. Killed by: declaring the placeholder as `None`, as an empty mapping, or as a state
    string other than `unresolved`. Second correct implementation: the same mapping built by
    `dict(state="unresolved")`, or the same literal behind a named constant for the state string --
    the value's form is fixed by the field definition, so the variation available is in construction,
    not in form. Not established: nothing here distinguishes a differently SHAPED placeholder,
    because the record's field definition fixes this form; changing that form is a schema edit in
    another change, not this test's.
    """
    placeholder = record_schema.SUBJECT_UNRESOLVED
    assert placeholder is not None
    assert placeholder.get("state") == "unresolved"
    assert json.loads(json.dumps(placeholder)) == {"state": "unresolved"}
