"""The root configuration of the toolkit's test tree.

It registers the markers that scope a test to its family and its owning system, and the
plugins of the pre-refactor correspondence harness, starting with the collection-time
refusals. The comparison helper is registered for assertion rewriting before anything
imports it, so a failure inside the helper reads as the test's own assertion.
"""

import pytest

pytest_plugins = ["pytester"]  # the harness tests drive inner pytest runs

pytest.register_assert_rewrite("tests.correspondence.compare")

MARKERS = (
    "guard(system): a guard case of one system; runs the whole-ledger checks when that system's tests are selected",
    "equivalence(unit, system, part): an equivalence test of one legacy unit, owned by one system",
    "refactor_reminder(unit, system, part): a failing reminder that a scoped-in unit has no equivalence test yet",
)


def pytest_configure(config):
    """Register the three marker names and the harness plugins on every run of the test tree."""
    for line in MARKERS:
        config.addinivalue_line("markers", line)
    from tests.correspondence import inventory, markers

    for plugin in (markers, inventory):
        config.pluginmanager.register(plugin, f"correspondence-{plugin.__name__.rsplit('.', 1)[-1]}")
