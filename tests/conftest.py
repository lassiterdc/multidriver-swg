"""The root configuration of the toolkit's test tree.

It registers the markers that scope a test to its family and its owning system, and the
plugins of the pre-refactor correspondence harness: the collection-time refusals, the test
inventory's collection capture, the binding to new code, the proof recorder, the
subject-location check and the result-record emitter. The comparison helper is registered for
assertion rewriting before anything imports it, so a failure inside the helper reads as the
test's own assertion.
"""

import pytest

pytest_plugins = ["pytester"]  # the harness tests drive inner pytest runs

pytest.register_assert_rewrite("tests.correspondence.compare")

from tests.correspondence.binding import new_side  # noqa: E402, F401  (the fixture, exported to every test)

MARKERS = (
    "guard(system): a guard case of one system; runs the whole-ledger checks when that system's tests are selected",
    "equivalence(unit, system, part): an equivalence test of one legacy unit, owned by one system",
    "refactor_reminder(unit, system, part): a failing reminder that a scoped-in unit has no equivalence test yet",
)


def pytest_configure(config):
    """Register the three marker names and the harness plugins on every run of the test tree."""
    for line in MARKERS:
        config.addinivalue_line("markers", line)
    from tests.correspondence import inventory, legacy_execution, markers, proofs, record, subject

    # `record` LAST so that, on the hooks it shares with `subject`, it is dispatched FIRST: pluggy
    # calls equal-priority implementations latest-registered-first, and `subject` may end the session
    # at session start. Registered there, the record still reports the checkout and the VOID verdict
    # that a stopped run should leave behind.
    for plugin in (markers, inventory, proofs, subject, legacy_execution, record):
        config.pluginmanager.register(plugin, f"correspondence-{plugin.__name__.rsplit('.', 1)[-1]}")


def pytest_addoption(parser):
    """Add the proof recorder's two opt-in record options and the result record's one."""
    from tests.correspondence import proofs, record

    proofs.add_options(parser)
    record.add_options(parser)
