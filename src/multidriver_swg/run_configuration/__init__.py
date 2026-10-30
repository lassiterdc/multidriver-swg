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

Its fourth component is the selection artifact, re-exported from `selection_artifact`: one versioned,
provenance-stamped document holds a run's TWO selections — the marginal fits, each named by a four-part
fit identifier, and the joint model, whose marginals reference those identifiers — and `load_selection`
is the only route to either, so a consumer that reads the file itself bypasses the form this package
enforces. The refusals a reader will meet are `SelectionArtifactError` for a document that is malformed,
and three leaves it raises where the recovery differs: `UnsupportedSelectionVersionError` for a header
this code does not read, `UnstampedSelectionError` for a selection carrying no usable provenance record,
and `UndescribedDependenceError` for one that names no joint model. Deliberately NOT re-exported are the
module's two constants — the version literal and the component-name tuple — matching how `BINDING_RULE`
and `SEED_PARAMETER` are treated in their own modules: a constant a caller compares against is the
module's, and a package-level alias would invite a comparison against a name this file could change.
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
from multidriver_swg.run_configuration.selection_artifact import (
    DependenceSelection,
    FitIdentifier,
    SelectionArtifact,
    SelectionArtifactError,
    UndescribedDependenceError,
    UnstampedSelectionError,
    UnsupportedSelectionVersionError,
    load_selection,
)

__all__ = [
    "DependenceSelection",
    "EstateNotConfiguredError",
    "FitIdentifier",
    "IdentityDeclaration",
    "ImplementationIdentity",
    "PathBindings",
    "PathKey",
    "PathResolutionError",
    "ProvenanceError",
    "ProvenanceRecord",
    "SeededCallable",
    "SeedGenerator",
    "SelectionArtifact",
    "SelectionArtifactError",
    "UnboundPathKeyError",
    "UndescribedDependenceError",
    "UnstampedSelectionError",
    "UnsupportedSelectionVersionError",
    "capture",
    "estate_root",
    "load_bindings",
    "load_selection",
    "require_complete",
    "require_seed",
    "resolve",
]
