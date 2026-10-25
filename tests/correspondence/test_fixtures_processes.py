"""Tests that a fixture generator cannot start a process through multiprocessing's interpreter-starting contexts.

The fixture guard refuses process creation by the audit events Python raises when it starts one. The
`spawn` and `forkserver` contexts of `multiprocessing` start a fresh interpreter through
`_posixsubprocess.fork_exec`, which raises none of those events, so a generator using either context
runs code in a child the guard never sees, and that child can read any file and hand its content back.
These tests ask that such a generator is refused like one calling `subprocess.run`.
"""

from __future__ import annotations

import multiprocessing
from pathlib import Path

import pytest

from tests.correspondence import fixtures

pytestmark = pytest.mark.state_check

STARTS_A_POOL = """\
import multiprocessing
import os

import numpy as np

from tests.correspondence.fixtures import generates_fixture


@generates_fixture(name="pool", seed=1)
def pool(rng):
    with multiprocessing.get_context({context!r}).Pool(1) as workers:
        return np.array([workers.apply(os.getpid)])
"""


def _root_with_pool_generator(tmp_path: Path, context: str) -> Path:
    """A checkout-shaped root whose one generator starts a one-worker pool in `context`."""
    directory = tmp_path / "tests" / "fixture_generators"
    directory.mkdir(parents=True)
    (directory / "a.py").write_text(STARTS_A_POOL.format(context=context), encoding="utf-8")
    return tmp_path


@pytest.mark.parametrize("context", ["spawn", "forkserver"])
def test_a_generator_that_starts_an_interpreter_through_multiprocessing_is_refused(tmp_path, context):
    """Property: a generator starting a `multiprocessing` worker in the `spawn` or `forkserver` context fails the run with the guard's process refusal and writes nothing.

    Class: every route by which a generator starts a process, positioned here at the two routes
    that start a new interpreter without raising `subprocess.Popen`, `os.posix_spawn`, `os.fork` or
    any other event the guard watches. `concurrent.futures.ProcessPoolExecutor` reaches the same
    route wherever its default context is one of these two.
    Why required: the worker is a second interpreter with no hook installed, so whatever file it
    opens and returns becomes fixture content that the anonymization guard, which never scans a
    binary fixture, would not see. The message is matched, not only the missing fixture, because a
    worker that failed for another reason would also leave no fixture.
    Killed by: refusing process creation only through the audit events the guard lists; a guard
    that also refuses the interpreter-starting call these contexts make passes.
    A second correct implementation: replacing `_posixsubprocess.fork_exec` with a refusing stand-in
    while the generator runs, or refusing the creation of a `multiprocessing` context other than
    `fork` for the same span.
    """
    if context not in multiprocessing.get_all_start_methods():
        pytest.skip(f"the {context} start method is not available on this platform")
    root = _root_with_pool_generator(tmp_path, context)
    with pytest.raises(fixtures.FixtureError, match="start a process"):
        fixtures.generate("a", "pool", root=root)
    assert not (root / fixtures.FIXTURES).exists()
