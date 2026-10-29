"""Declare the field vocabulary of a run's result record, and nothing else.

A run of this test tree leaves no artifact saying what it ran over. Collection selects tests by
marker, so the tests a run did not execute are absent from its output entirely, and a consumer
reading only that output cannot tell a complete run from one that deselected most of the tree or
stopped before it finished. The result record answers that: one JSON object per run carrying the
run's identity (rootdir, collection scope and two node-id digests), the code path the import
resolved to, the population collection saw before `-m`/`-k` deselection, the tests that survived
it, what deselection removed, where the subject resolved, the checkout it had to resolve inside,
and a verdict.

This module declares that record's names and its two closed value sets, and holds no code that
writes or reads a record. The capture belongs to the collection hook that `inventory.py` already
registers; the emission belongs to a pytest plugin this package will add. Keeping those two out of
the schema is deliberate: a schema that also serialized the record would let its writer and its
readers disagree about one field with neither side wrong, which is the disagreement one
declaration of the names exists to prevent.

Deliberately NOT declared here: a `VERDICT` stash key. The verdict is a record field the emitter
writes from the session's outcome, and it never travels between hooks, so a stash key for it
would be a second place to hold one value -- and a reader finding that key populated would have
no way to tell which of the two was authoritative. The population and the exclusion set do
travel, because collection captures them and the emitter serializes them later, which is why
exactly those two keys are below.

Not established: whether the emitter writes all eight fields at every completeness state. That
is the emitter's own test's subject, not this module's.
"""

from __future__ import annotations

import pytest

# The eight fields a record carries. Two are named in speech with a space (`subject resolution`,
# `declared context`); a JSON object's key cannot hold one, so both are snake_case here, and the
# emitter writes them under these exact names -- a reader that re-derived a spelling would read
# that field as absent, not as unresolved.
FIELDS = (
    "invocation",
    "environment",
    "population",
    "surviving",
    "exclusion",
    "subject_resolution",
    "declared_context",
    "verdict",
)

# Closed value set. `VOID` is upper case and distinct from `fail` because it says the run measured
# nothing at all, where `fail` says it measured and the measurement was bad; a fourth value would
# be one no consumer handles, falling through whatever branches on these three.
VERDICTS = ("pass", "fail", "VOID")

# Closed value set for the completeness marker, which advances as the staged write proceeds. The
# marker is a record key the emitter writes, but it is not one of `FIELDS`: it reports how far the
# record's own write got, and it is what makes a killed run read as incomplete rather than as a
# completed run over an empty population.
COMPLETENESS = ("pre-collection", "collected", "complete")

# The subject-resolution field's placeholder until the sibling instrument that fills it lands. A
# VALUE, never `None`: with `None`, a record whose subject did not resolve would be indistinguishable
# from a record with no subject-resolution field at all -- the first a reading that found nothing,
# the second a schema that never asked.
SUBJECT_UNRESOLVED = {"state": "unresolved"}

# The two fields that travel between hooks: the collection hook captures them and the emitter reads
# them afterwards through the config's stash. Bare `list` in both: every element is a node-id
# string, and the capture's own tests hold that.
POPULATION = pytest.StashKey[list]()
EXCLUDED = pytest.StashKey[list]()
