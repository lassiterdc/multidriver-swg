"""The root configuration of the toolkit's test tree.

It registers the markers that scope a test to its family and its owning system, and the
plugins of the pre-refactor correspondence harness: the collection-time refusals, the test
inventory's collection capture, the binding to new code, the proof recorder and the
subject-location check. The comparison helper is registered for assertion rewriting before
anything imports it, so a failure inside the helper reads as the test's own assertion.
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
    from tests.correspondence import inventory, legacy_execution, markers, proofs, subject

    for plugin in (markers, inventory, proofs, subject, legacy_execution):
        config.pluginmanager.register(plugin, f"correspondence-{plugin.__name__.rsplit('.', 1)[-1]}")


def pytest_addoption(parser):
    """Add the proof recorder's two opt-in record options."""
    from tests.correspondence import proofs

    proofs.add_options(parser)
