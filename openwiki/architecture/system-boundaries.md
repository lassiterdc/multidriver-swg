---
type: architecture
title: System boundaries and scientific workflow graph
description: The implemented orchestration boundary, twelve-system artifact graph, and distinction between real run infrastructure and unported scientific computation.
tags: [architecture, workflow, scientific-computing]
verified:
  - by: openwiki/0.9.0
    at: 2026-11-20T07:54:33.693Z
sources:
  - id: openwiki-source-58d270f56db6b6279ab1d325
    resource: repo://_old_code_to_refactor/README.md
  - id: openwiki-source-237081be6decbd2758994ca4
    resource: repo://src/multidriver_swg/__init__.py
  - id: openwiki-source-b27a2f93c215033e37aa355a
    resource: repo://src/multidriver_swg/workflow/graph.py
  - id: openwiki-source-1d27f6195bd3e6ecb6f6a9b5
    resource: repo://src/multidriver_swg/workflow/Snakefile
  - id: openwiki-source-de9274ba9e42deec3d3631b9
    resource: repo://src/multidriver_swg/workflow/stubs.py
generated: { by: "omp", at: "2026-11-20T07:54:33.693Z" }
---

# System boundaries and scientific workflow graph

## What is implemented

The package describes a multi-driver stochastic weather generator, but its rendered scientific workflow currently runs **stubs**, not the legacy scientific algorithms. The implemented public module surface includes `conformance`, `dispatch`, `divergence_ledger`, `divergence_report`, `pairs`, `run_configuration`, `side_record`, and `workflow` (`src/multidriver_swg/__init__.py`). An empty scientific output is therefore not evidence of a fitted model or generated forcing.

The workflow separates three owners:

- `workflow/graph.py` owns the fixed rule table, owning-system identifiers, artifact edges, selection-consumer flags, and report-item declarations.
- `workflow/Snakefile` owns engine rendering, run-start validation, dynamic targets, and the final record rule.
- `workflow/stubs.py` owns the current job body: empty outputs plus implementation identity.

## Artifact graph

`GRAPH` is an ordered tuple of frozen `RuleSpec` records. Each output name resolves to `<producing-rule>/<output>`. The following table describes the declared data interfaces, **not implemented scientific calculations**.

| Rule / intended system responsibility | Fixed inputs | Declared output |
| --- | --- | --- |
| `observed_record_assembly` | none | `observed_record.json` |
| `combined_event_identification` | observed record | `event_set.json` |
| `marginal_distribution_fitting` | event set | `candidate_marginals.json` |
| `event_frequency_assessment` | event set | `exceedance_fits.json` |
| `event_occurrence_modelling` | observed record, event set | `occurrence_model.json` |
| `interactive_decision_support` | event set, candidate marginals | `decision_views.html` |
| `dependence_modelling` | candidate marginals | `joint_model.json` |
| `timeseries_synthesis` | joint model, occurrence model, observed record | `synthetic_forcing.json` |
| `boundary_condition_output` | synthetic forcing | `boundary_conditions.json` |
| `generator_validation` | observed record, event set, boundary conditions | `validation_results.json` |
| `monte_carlo_design_storms` | exceedance fits, joint model | `design_storm_realizations.json` |
| `method_attribution` | computed from run targets | `citation_section.html` |

This is a dependency graph, not twelve independent steps: occurrence modelling and dependence modelling converge at synthesis; frequency assessment and dependence modelling converge at design storms. Validation compares the observed/event interfaces with the downstream boundary-condition interface. Every fixed read in the current table is produced by an earlier row.

Owning-system names are explicit fields, generally hyphenated forms of rule names; ownership is not inferred from spelling. `dependence_modelling`, `timeseries_synthesis`, and `generator_validation` set `reads_selection=True`. `judgement_rules()` derives that set from the table; the flag declares a selection dependency, not evidence that the stub reads or applies scientific choices.

## Rendering and dynamic edges

At parse time the Snakefile moves this checkout's `src` to the front of `sys.path` and calls `require_graph_start()`. `all` is the first rule and targets the validated run-start record's target list. A loop renders one rule per table row, deriving inputs, output paths, and report flags.

`method_attribution` deliberately has no fixed reads. Its actual inputs are the run's targets, excluding its own directory and `END_OF_RUN_RECORD`. This permits shortened runs without requesting absent scientific artifacts and avoids a cycle with the final record rule. The latter waits on every other target and writes the end-of-run record directory; it is not a scientific row in `GRAPH`.

The rendering loop binds `system` and `rule_name` into job `params`. The job body reads those bound values rather than the loop variable, which would refer to the last row after rendering. This binding preserves per-job ownership.

## Stub lifecycle and extension boundary

For each declared output, `write_stub_outputs` creates parents and writes zero bytes. It then persists `<rule>/implementation_identity.json` with the supplied system, literal implementation `stub`, and the running revision. Identity is a side artifact, not a declared scientific output. The helper does not independently validate that its arguments name a real rule/owner pair; the renderer supplies that relationship.

A scientific implementation must replace the relevant body and revise its row where interfaces change. The rendering loop is shared infrastructure, not a second place to encode dependencies. `RuleSpec` rejects empty outputs, repeated reads/outputs, self-reads, report items outside the row's outputs, and duplicate report entries. `downstream_of()` follows fixed transitive consumers only; it does not synthesize attribution's run-specific edges.

## Legacy and acquisition boundary

`_old_code_to_refactor/` is a manually selected legacy snapshot, not the active workflow implementation. Its README assigns AORC downloads, NCEI station processing, MRMS-to-subcatchment averaging, and NOAA CO-OPS acquisition to `hydro-fetch`; those acquisition tasks are not to be reimplemented here. The current graph's first empty observed-record output is not an implemented acquisition integration.

## Related pages

- [Legacy correspondence](../concepts/legacy-correspondence.md)
- [Provenance and randomness](../concepts/provenance-and-randomness.md)
- [Model selection](../configuration/model-selection.md)
- [Run lifecycle](../workflows/run-lifecycle.md)
