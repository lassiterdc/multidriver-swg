"""Run configuration: the settings a stochastic weather generation run is given, and the seed contract.

This package holds the configuration a run is described by. Its first component is the seed
contract, which decides where a run's randomness comes from: every stochastic function takes a
required `numpy.random.Generator` named `rng`. The names below are re-exported from
`seed_contract` so a sibling imports them from the package; this file carries no logic of its own.
"""

from __future__ import annotations

from multidriver_swg.run_configuration.seed_contract import SeededCallable, SeedGenerator, require_seed

__all__ = ["SeededCallable", "SeedGenerator", "require_seed"]
