"""Run configuration: the settings a stochastic weather generation run is given, and the seed contract.

This package holds the configuration a run is described by. Its first component is the seed
contract, which decides where a run's randomness comes from: every stochastic function takes a
required `numpy.random.Generator` named `rng`. The names below are re-exported from
`seed_contract` so a sibling imports them from the package; this file carries no logic of its own.
Its second component is the provenance capture surface, re-exported from `provenance`: each compute
system declares its implementation identity into the run's record, and a record that leaves an
exercised system's identity undeclared is refused at consumption. That component also carries the
persisted form: `persist_identity` writes one system's identity beside the outputs it produced, and
`read_identity` is the only reader, refusing a record whose producer fields are absent or empty.

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
and `UndescribedDependenceError` for one that names no joint model.

Its fifth component is the main configuration, re-exported from `main_configuration`: one file, held by
the estate, states this machine's estate root, binding document, project and launch mode, plus the
values that mode reads. `MainConfiguration.load` is its only reader, and every refusal names both the
field and the file, because an operator repairs a file they cannot see from inside the toolkit. The
three path keys the toolkit fixes for a project are constructed from the configuration's project, so no
caller respells `domain.project.kind`.

Its sixth component is the revision gate, re-exported from `revision_gate`: a run executes the package
of a git checkout, at the forty-character commit its project declares, with a clean working tree.
`require_revision_gate` runs those three checks in one fixed order for both call sites that need them,
and `running_revision` is the one place the executed commit is read, so a producer can record the code
version that made its output.

Deliberately NOT re-exported are each module's constants — the version literals, the component-name
tuples and the configuration key sets — matching how `BINDING_RULE` and `SEED_PARAMETER` are treated in
their own modules: a constant a caller compares against is the module's, and a package-level alias would
invite a comparison against a name this file could change.
"""

from __future__ import annotations

from multidriver_swg.run_configuration.main_configuration import (
    JudgementConfiguration,
    LocalConfiguration,
    MainConfiguration,
    MainConfigurationError,
    SlurmConfiguration,
)
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
    persist_identity,
    read_identity,
    require_complete,
)
from multidriver_swg.run_configuration.revision_gate import (
    DeclaredRevision,
    RevisionGateError,
    load_declared_revision,
    require_clean_checkout,
    require_declared_revision,
    require_package_checkout,
    require_revision_gate,
    running_package_directory,
    running_revision,
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
    "DeclaredRevision",
    "DependenceSelection",
    "EstateNotConfiguredError",
    "FitIdentifier",
    "IdentityDeclaration",
    "ImplementationIdentity",
    "JudgementConfiguration",
    "LocalConfiguration",
    "MainConfiguration",
    "MainConfigurationError",
    "PathBindings",
    "PathKey",
    "PathResolutionError",
    "ProvenanceError",
    "ProvenanceRecord",
    "RevisionGateError",
    "SeededCallable",
    "SeedGenerator",
    "SelectionArtifact",
    "SelectionArtifactError",
    "SlurmConfiguration",
    "UnboundPathKeyError",
    "UndescribedDependenceError",
    "UnstampedSelectionError",
    "UnsupportedSelectionVersionError",
    "capture",
    "estate_root",
    "load_bindings",
    "load_declared_revision",
    "load_selection",
    "persist_identity",
    "read_identity",
    "require_clean_checkout",
    "require_complete",
    "require_declared_revision",
    "require_package_checkout",
    "require_revision_gate",
    "require_seed",
    "resolve",
    "running_package_directory",
    "running_revision",
]
