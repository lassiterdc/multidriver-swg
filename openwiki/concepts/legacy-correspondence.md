---
type: concept
title: Legacy corpus, paired implementations, and deliberate divergence
description: How frozen stormy source relates to import-time implementation pairs, strict side selection, and auditable deliberate departures without implying scientific equivalence.
tags: [legacy, correspondence, provenance, divergence]
verified:
  - by: openwiki/0.9.0
    at: 2026-11-20T07:54:33.693Z
sources:
  - id: openwiki-source-3bd4cb6c4bcb18e7402fe74b
    resource: repo://_old_code_to_refactor/LICENSE
  - id: openwiki-source-58d270f56db6b6279ab1d325
    resource: repo://_old_code_to_refactor/README.md
  - id: openwiki-source-06d6c722b8b86cdeb7a0d39c
    resource: repo://legacy_corpus_record.toml
  - id: openwiki-source-05ccef8d4cf1698187f20464
    resource: repo://pyproject.toml
  - id: openwiki-source-af801c2ddf446f02266690b4
    resource: repo://src/multidriver_swg/dispatch.py
  - id: openwiki-source-5a6619b4eff14b15dbb8c02e
    resource: repo://src/multidriver_swg/divergence_ledger.py
  - id: openwiki-source-ea9543ad3f08971dd655a07b
    resource: repo://src/multidriver_swg/divergence_ledger.toml
  - id: openwiki-source-4454d8231da861a9d43fdabb
    resource: repo://src/multidriver_swg/divergence_report.py
  - id: openwiki-source-f6a0a9a879630fb79950cd94
    resource: repo://src/multidriver_swg/pairs.py
  - id: openwiki-source-121b7cb515f2d231a10b2fce
    resource: repo://src/multidriver_swg/side_record.py
generated: { by: "omp", at: "2026-11-20T07:54:33.693Z" }
---

# Legacy corpus, paired implementations, and deliberate divergence

## Frozen input, not the runtime implementation

`_old_code_to_refactor/` is a manually selected snapshot of 15 scripts from `lassiterdc/stormy`. `legacy_corpus_record.toml` pins upstream revision `c74c8a4052efcbb1345981721532f462090d1a12`, branch `working-ornl-hpc11`, and the `stochastic_storm_rescaling` path. It records file digests, retained/ceded groups, a manifest digest, and a dated verification result of 16 upstream-sourced files being byte-identical. That result is recorded provenance, not a fresh upstream comparison performed by this wiki.

The corpus is not a claim that all scripts execute. The record marks the water-level acquisition, NCEI, and abandoned water-level-generator scripts `DOES-NOT-RUN`, while other files are generally `UNESTABLISHED`, not certified runnable. Stop-site entries distinguish unconditional exits from exits inside functions or loops that require those paths to execute. The README assigns acquisition and preprocessing work to `hydro-fetch` rather than this toolkit.

Licenses have separate scopes: the corpus `LICENSE` is MIT, while `pyproject.toml` declares the toolkit `AGPL-3.0-or-later`. The legacy README also identifies a third-party block whose attribution/license belongs in `NOTICE.md`; do not treat one license declaration as replacing the others.

## Registration and selection

A stable pair identifier associates a `pre_refactor` function with its `current` replacement. `@pair_side(pair="...", side="...")` registers at module import and returns the original callable unchanged. Registration does not discover or import every implementation automatically. A second registration for the same pair/side raises `DuplicatePairRegistration`; invalid side names are refused. Literal decorator arguments support the source-scanning contract documented by the module.

`EXPECTED_PAIR_COUNT` is currently **zero**. This is the declared correspondence baseline, not proof that scientific ports or equivalence checks have been completed. An owning system is expected to raise the count when registering a pair.

`SideSelection` defaults to `current`, with per-pair overrides copied into an immutable mapping. Side spellings are validated at construction; registry membership is checked later because imports populate the registry. `side_for()` only resolves policy. `function_for()` consults the live registry and returns the selected callable. Unknown requested pairs or overrides raise `UnknownPairError`; a missing selected side raises `UnregisteredSideError`. **There is no fallback to the other implementation.** `selected_sides()` validates every registered pair and returns choices sorted by identifier.

## Capturing the run's choice

`side_record(selection)` captures the choices over the registry as it exists at that moment. `SideRecord` copies, sorts, and freezes the mapping; `as_mapping()` returns a fresh dict for persistence. This module itself writes nothing and reads no settings.

An empty mapping means no registered pairs were selected; it is distinct from an omitted record. Every registered pair appears, including those selected on `current`. The record establishes selection, not whether a function was actually called. Despite its name, `ran_pre_refactor` is computed from whether any recorded selection is `pre_refactor`.

## Ledger: agreement and intentional disagreement

The packaged `divergence_ledger.toml` distinguishes:

- **Pair entries:** both implementations' callable/fingerprint descriptions, plus `diverges`. A true flag requires `changed`, `why`, and `judged_correct_when`; a false flag forbids those reason fields, making agreement explicit.
- **Unpaired departure entries:** legacy unit/site, allowed kind (`licensed`, `finding-3`, or `finding-4`), register or triage grounds, replacement status, reason, and correctness criterion. Fingerprints may be added when replacement code is registered. `finding-4` requires a `future_pair`; other kinds cannot carry one.

The current ledger records planned corrections with `new_code = "not yet written"`, including density-product underflow, incorrect goodness-of-fit comparisons, unseeded draws, event-classification inconsistencies, and partial-overlap merging. These entries document intended departures, not implemented fixes.

`load()` parses TOML; `validate()` reports schema findings, including unknown keys, malformed pointers, invalid kinds, and malformed `canon-ast:<version>:<64 lowercase hex>` labels. It does **not** establish that units or register keys exist, or recompute fingerprints against code. The module explicitly delegates semantic resolution to detectors. Detector implementations are not present among the inspected test files; references to them in source are not evidence of their execution here.

## Reporting without overclaiming

`build_report()` validates the ledger before creating a report and raises `LedgerInvalidError` on findings. The report counts ledger pairs and fully registered pairs separately, sorts divergent pairs by identifier and departures by unit/site, and renders explicit empty-state statements. A `finding-4` departure disappears only once its future pair has both sides registered, avoiding duplicate reporting while retaining incomplete pairs' departures.

The report does not prove that ledger identities match registered code and does not decide when to emit itself. Callers choose emission using captured run state; a printed pair count is not an equivalence certificate.

## Related pages

- [System boundaries](../architecture/system-boundaries.md)
- [Provenance and randomness](provenance-and-randomness.md)
- [Release verification](../operations/release-verification.md)
- [Publication conformance](../testing/publication-conformance.md)
