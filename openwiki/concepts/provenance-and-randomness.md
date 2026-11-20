---
type: concept
title: Producer identity, run provenance, reports, and seed contracts
description: How run targets and engine metadata establish cumulative producer provenance, how completion records publish reports, and what the caller-owned random-generator contract actually checks.
tags: [provenance, randomness, workflow, reporting]
verified:
  - by: openwiki/0.9.0
    at: 2026-11-20T07:54:33.693Z
sources:
  - id: openwiki-source-f790ff775c8d4ccbf13b5c9a
    resource: repo://src/multidriver_swg/run_configuration/provenance.py
  - id: openwiki-source-2b12b818de1c72fbacd2ea8c
    resource: repo://src/multidriver_swg/run_configuration/seed_contract.py
  - id: openwiki-source-121b7cb515f2d231a10b2fce
    resource: repo://src/multidriver_swg/side_record.py
  - id: openwiki-source-0a1a55ae1a1203be303d69b5
    resource: repo://src/multidriver_swg/workflow/end_of_run.py
  - id: openwiki-source-9239874f5ebb25aa0025708c
    resource: repo://src/multidriver_swg/workflow/exercised_set.py
  - id: openwiki-source-b27a2f93c215033e37aa355a
    resource: repo://src/multidriver_swg/workflow/graph.py
  - id: openwiki-source-f0ef694cda92c207cdb31d2a
    resource: repo://src/multidriver_swg/workflow/pre_run.py
  - id: openwiki-source-bf6c4c7df02271a2625e7715
    resource: repo://src/multidriver_swg/workflow/report_items.py
  - id: openwiki-source-1d27f6195bd3e6ecb6f6a9b5
    resource: repo://src/multidriver_swg/workflow/Snakefile
  - id: openwiki-source-de9274ba9e42deec3d3631b9
    resource: repo://src/multidriver_swg/workflow/stubs.py
generated: { by: "omp", at: "2026-11-20T07:54:33.693Z" }
---

# Producer identity, run provenance, reports, and seed contracts

## Identity is declared by producers

`ImplementationIdentity` names a system, implementation, and code version. Producers persist that declaration beside their outputs as `implementation_identity.json`; the workflow stubs currently declare `stub` and the running revision. Identity is not inferred from an output's numerical values.

`persist_identity()` writes a whole JSON object with format `implementation-identity:1` and the three identity fields. It checks the object type but does not validate field contents or create parents. `read_identity()` rejects non-UTF-8/non-JSON documents, unknown fields, the wrong format, and missing, non-text, or empty identity values. Whitespace-only strings are admitted. Unreadable-path OS errors are not hidden.

`capture(inputs, seeds, identities)` holds caller-supplied mappings in a frozen `ProvenanceRecord`; freezing prevents field rebinding, **not mutation of those mappings**. `require_complete()` compares identity keys with a separately supplied exercised-system iterable and refuses missing producers. It cannot establish whether that supplied set describes the actual run.

## From target closure to cumulative producers

The normal pre-run step targets all declared outputs when selection exists. Without selection, it removes selection-reading rules and their transitive fixed-edge consumers, retains attribution, and appends the end-of-run record. Thus the retained set includes its fixed prerequisites rather than naming only terminal scientific products.

The Snakefile's final rule waits on every target except its own record. `exercised_set.producing_rules()` reads those exact input targets from the engine's `.snakemake/metadata` store; it does not independently expand the graph or reconstruct the target list. A caller using a custom narrower list receives provenance only for that list.

Metadata must be a JSON object with `record_format_version == 6`, `incomplete is False`, and a rule name present in `GRAPH`. Missing records, malformed data, version changes, or unknown rules raise `ExercisedSetError` naming the target/file. The rule-to-system mapping comes from graph ownership.

`build_provenance()` then reads one persisted identity per producing rule, rejects a system that disagrees with the graph, captures main-configuration path, target list, selection path, and judgement state, and checks completeness. Missing identity files prevent a complete record. Seeds are currently `{}` because these producers draw no randomness.

This producer set is **cumulative**: outputs reused from previous invocations still retain their producers. `invocation_rules()` is a separate informational list, in graph order, based on metadata `endtime >=` the run-start file's modification time. A missing/non-numeric end time is refused. Direct engine reuse of an old run-start record broadens that time window; it is not a fresh-invocation detector independent of run preparation.

## Completion persistence and report ownership

The final record is the directory `end_of_run/record`. Under the normal workflow, `write_end_of_run_record()` receives working directory, targets, and run-start together, writes `provenance.json`, then conditionally writes divergence reports. Supplying only some of those three arguments raises `TypeError`; supplying none supports a divergence-only record without making a provenance claim.

The provenance JSON format is `end-of-run-provenance:1`. Its closed seven-field envelope contains `format`, `inputs`, `seeds`, `identities`, `exercised_systems`, `invocation_rules`, and `side_record`. Identities are keyed by system and serialize implementation/code version; exercised systems are sorted and the side mapping is carried unaltered. The reader validates this envelope and basic container shapes, not every nested input or seed value.

Divergence text and HTML are written only if the supplied side record selects at least one `pre_refactor` side. HTML escapes the text actually written inside a `pre` block, rather than independently generating a second report. Normal command-side selection currently uses `SideSelection()` with no overrides; the main configuration does not expose a legacy-side setting. Selection records describe choices, not proof of function invocation.

The final step writes report artifacts, not the engine's assembled report. The Snakefile flags declared report outputs from graph `report_items`, flags the final directory's HTML under `Divergence report`, and keeps the final rule local to the driver. `ReportItem` admits non-empty headings and case-sensitive `.html`, `.svg`, `.png`, `.jpg`, `.jpeg`, or `.gif` suffixes; the graph additionally requires each item to name its own declared output. The current graph flags the attribution HTML under `Citation`. Empty stub files do not become meaningful scientific reports merely because an engine flag exists.

## Randomness: interface, not a completed adoption audit

Stochastic functions are required to receive a caller-owned `numpy.random.Generator` named `rng`, with no default. Generators should travel through call signatures, not process-global state. Provenance's seed mapping is intended for integer seed values, not stateful generator objects.

`SeededCallable` describes the typed callable shape. `require_seed()` inspects a function signature and raises `TypeError` if `rng` is absent or defaulted. It does not check annotations, inspect function bodies for `if rng is None` fallback, verify that callers propagate the same generator, or establish that all random sites have adopted the contract. The module explicitly identifies those as census/static-analysis responsibilities outside its implementation. It creates no global generator and makes no draw; current workflow bodies are empty-output stubs, not seeded scientific computations.

## Related pages

- [System boundaries](../architecture/system-boundaries.md)
- [Legacy correspondence](legacy-correspondence.md)
- [Model selection](../configuration/model-selection.md)
- [Run lifecycle](../workflows/run-lifecycle.md)
