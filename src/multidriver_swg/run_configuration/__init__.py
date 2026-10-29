"""Run configuration: the settings a stochastic weather generation run is given, and the seed contract.

This package holds the configuration a run is described by. Its first component is the seed
contract, which decides where a run's randomness comes from: every stochastic function takes a
required `numpy.random.Generator` named `rng`. The names below are re-exported from
`seed_contract` so a sibling imports them from the package; this file carries no logic of its own.
Its second component is the provenance capture surface, re-exported from `provenance`: each compute
system declares its implementation identity into the run's record, and a record that leaves an
exercised system's identity undeclared is refused at consumption.

Its third component is the path-resolution surface, re-exported from `path_resolver`: code names a
path by a logical key `domain.project.kind`, and the resolver returns the machine-specific path an
estate-side binding document supplies, so no tracked toolkit file carries one. An unbound key
refuses with `UnboundPathKeyError` rather than defaulting to any path, because a default would
resolve a missing binding to a machine-specific location, and a reader could not tell that path
apart from one the estate had actually bound.
"""

from __future__ import annotations

from multidriver_swg.run_configuration.path_resolver import (
    EstateNotConfiguredError,
    PathBindings,
    PathKey,
    PathResolutionError,
    UnboundPathKeyError,
    estate_root,
    load_bindings,
    resolve,
)
from multidriver_swg.run_configuration.provenance import (
    IdentityDeclaration,
    ImplementationIdentity,
    ProvenanceError,
    ProvenanceRecord,
    capture,
    require_complete,
)
from multidriver_swg.run_configuration.seed_contract import SeededCallable, SeedGenerator, require_seed

__all__ = [
    "EstateNotConfiguredError",
    "IdentityDeclaration",
    "ImplementationIdentity",
    "PathBindings",
    "PathKey",
    "PathResolutionError",
    "ProvenanceError",
    "ProvenanceRecord",
    "SeededCallable",
    "SeedGenerator",
    "UnboundPathKeyError",
    "capture",
    "estate_root",
    "load_bindings",
    "require_complete",
    "require_seed",
    "resolve",
]
