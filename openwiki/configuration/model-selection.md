---
type: configuration
title: Recorded marginal and dependence selection
description: The versioned human-selection TOML contract, four-axis marginal identifiers, joint-model references, provenance stamps, and the workflow consequences of an absent selection.
tags: [configuration, selection, marginals, dependence, provenance]
verified:
  - by: openwiki/0.9.0
    at: 2026-11-20T07:54:33.693Z
sources:
  - id: openwiki-source-643c9664231d31231875d321
    resource: repo://src/multidriver_swg/run_configuration/selection_artifact.py
  - id: openwiki-source-b27a2f93c215033e37aa355a
    resource: repo://src/multidriver_swg/workflow/graph.py
  - id: openwiki-source-f0ef694cda92c207cdb31d2a
    resource: repo://src/multidriver_swg/workflow/pre_run.py
  - id: openwiki-source-e6732b5e2c894519d2f82693
    resource: repo://src/multidriver_swg/workflow/run_start.py
generated: { by: "omp", at: "2026-11-20T07:54:33.693Z" }
---

# Recorded marginal and dependence selection

## A recorded choice, not a fitted model

`run_configuration/selection_artifact.py` validates the document intended to carry a recorded interactive modelling choice. It does not fit distributions, produce the document, or implement the interactive emission surface. The scientific workflow still uses empty-output stubs; accepting selection syntax does not establish scientific validity or completed consumer adoption.

The estate binding `{project}.selection` names the TOML file. `load_selection(path)` requires an explicit path, parses TOML, and constructs `SelectionArtifact`. Its exact rule is `modelled-selection:1`; the only top-level keys are `rule`, `selection`, `dependence`, and `stamp`. Unknown keys fail, and unsupported/absent version headers are refused, not migrated.

## Marginal identifiers: four axes

Every `selection` entry is a string in this order:

```text
event_class.statistic.distribution_family.transformation_chain
```

`FitIdentifier` has exactly those four fields. The first three are non-empty `[A-Za-z0-9_-]+` segments. The last is one or more step names using the same charset, joined by `+`. Internally it is an ordered tuple: `('identity',)` represents no transformation. Programmatic construction refuses a bare string or list as the chain, empty chains, and immediately repeated steps. Non-adjacent recurrence is not the same refusal.

For example, `compound.rainfall_depth.gamma.identity` is a syntactic identifier, not a recommendation or proof of a valid fitted gamma model. Order matters: two distinct transformation orderings remain distinct identifiers. `selection` must be a non-empty array of strings, and duplicate parsed identifiers are rejected rather than merged; document order is preserved.

Goodness-of-fit scores and fitted parameters are not identifier axes. This avoids renaming a curated model choice whenever it is re-fitted. There are important limits:

- Legal segments can still smuggle metric-like text such as `gamma_ks003`; a charset cannot establish semantic discipline.
- Two fits differing only by a transformation's fitted parameter have the same identifier. Hazard scenario and estimation method are also not axes, so such alternatives cannot both be recorded under one spelling.
- The parser has no enumerated vocabulary of valid event classes, statistics, distribution families, or transformations. A well-formed name need not denote a supported model.

## Joint-model selection is separate

A dependence entry is not a marginal identifier. The artifact requires a non-empty `dependence` array, with at most one entry per event class. Every entry has exactly:

| Field | Contract |
| --- | --- |
| `event_class` | One legal name segment |
| `marginals` | Ordered array of at least two distinct selected fit-identifier strings |
| `structure` | One legal name segment describing the structure convention |
| `copula_families` | Ordered family names, exactly `d(d-1)/2` for `d` marginals |

The artifact checks that every marginal reference exists in `selection` and belongs to the entry's event class. It constructs `DependenceSelection` values from those parsed identifiers. Repeated marginal references fail; repeated copula family names are allowed and are the way to express a uniform family at every edge. A single family is not shorthand for all edges.

The edge-count check is structural, not a vine implementation. Which edge a family position denotes depends on the structure convention shared by emitter and consumer; the validator does not enumerate structures or establish that convention. Additional fields such as fitted copula parameters are refused. It also does not require every selected marginal/event class to be used by a dependence entry: references must be sound, but coverage is not exhaustively checked. Multiple alternative structures for one event class cannot be represented in one artifact.

## Provenance stamp

`stamp` contains exactly three tables: `inputs`, `seeds`, and `identities`.

- `inputs` must be a table; it may be empty and its application-specific contents are not interpreted here.
- `seeds` must be non-empty, with integer values and booleans explicitly refused. The implementation does not add a nonnegative-range check.
- `identities` must be non-empty. Each system-keyed entry has exactly `system`, `implementation`, and `code_version`, all strings; the declared system must equal its mapping key.

Unlike persisted identity-file reading, this stamp constructor checks identity field types but not their non-emptiness. It does not compare code-version strings with Git or require a complete exercised-system census. A non-empty stamp is therefore weaker than complete producer provenance.

Before `capture()`, nested input/seed tables are frozen recursively into read-only proxies and arrays into tuples; identities are exposed through a read-only mapping. The artifact does not retain the mutable parsed document, so later edits to the source mapping cannot rewrite the validated selection.

## Selection controls the target set

`prepare_run()` first resolves estate paths and checks whether the selection path is a file. Inspection errors become `PreRunError`, not an invented absent selection. If the configuration names `judgement.automatic_selection`, preparation refuses it: no automatic-selection procedure is provided today.

For the supported human-choice path:

- **File present:** validate the artifact, target every graph output plus the final record, and record judgement `present` with the selection path.
- **File absent:** do not invent a model. Exclude `reads_selection` rules and their transitive consumers, retain attribution, append the final record, and record judgement `kept-absent` with null selection.

The current selection readers are dependence modelling, timeseries synthesis, and generator validation. Their downstream exclusions also remove boundary-condition output and Monte Carlo design storms. The shortened graph retains observed-record assembly, event identification, marginal fitting, frequency assessment, occurrence modelling, interactive decision support, and attribution. These remain stubs; a shortened run does not currently emit a usable human-selection artifact.

`run_start.json` is the versioned `run-start:1` handoff to engine parsing. It carries configuration path, targets, selection path/null, and judgement. Its schema also admits `delegated`, but normal preparation refuses that route. The record checks path/target shape and permitted state names, not existence, scientific meaning, graph membership, or cross-field selection/judgement consistency.

## Errors and repair boundaries

Catch `SelectionArtifactError` for all selection refusals. The loader retains specific subclasses while adding the document path: `UnsupportedSelectionVersionError` identifies a version mismatch, `UnstampedSelectionError` identifies missing/unusable stamp structure or empty seed/identity collections, and `UndescribedDependenceError` identifies missing/unusable dependence blocks or fields. Other content/identifier errors use the root class. Repair the recorded choice or emitter; bypassing it with a default family would erase the human decision the artifact exists to preserve.

## Related pages

- [System boundaries](../architecture/system-boundaries.md)
- [Provenance and randomness](../concepts/provenance-and-randomness.md)
- [Estate paths and revision gates](estate-and-revision.md)
- [Run lifecycle](../workflows/run-lifecycle.md)
