---
type: workflow
title: From configuration to completed run
description: The persisted handoff from CLI pre-run checks to graph parsing, target selection, ordered rules, and closing provenance/report artifacts.
tags: [workflow, lifecycle, snakemake, provenance]
verified:
  - by: openwiki/0.9.0
    at: 2026-11-20T07:54:33.693Z
sources:
  - id: openwiki-source-642add6b4a8bd8436dd0bc71
    resource: repo://docs/usage.md
  - id: openwiki-source-4848d71ba0714fc9991173c8
    resource: repo://src/multidriver_swg/workflow/cli.py
  - id: openwiki-source-0a1a55ae1a1203be303d69b5
    resource: repo://src/multidriver_swg/workflow/end_of_run.py
  - id: openwiki-source-432e9e79a89827275cf0b537
    resource: repo://src/multidriver_swg/workflow/graph_start.py
  - id: openwiki-source-f0ef694cda92c207cdb31d2a
    resource: repo://src/multidriver_swg/workflow/pre_run.py
  - id: openwiki-source-e6732b5e2c894519d2f82693
    resource: repo://src/multidriver_swg/workflow/run_start.py
  - id: openwiki-source-1d27f6195bd3e6ecb6f6a9b5
    resource: repo://src/multidriver_swg/workflow/Snakefile
  - id: openwiki-source-de9274ba9e42deec3d3631b9
    resource: repo://src/multidriver_swg/workflow/stubs.py
generated: { by: "omp", at: "2026-11-20T07:54:33.693Z" }
---

# From configuration to completed run

## Run success is not weather-generation capability

The packaged Snakefile renders graph rows into jobs that currently call `write_stub_outputs`. Those jobs write empty declared artifacts and persist `implementation = stub` with the running revision. An engine success establishes execution of this graph and its closing rules, not computed rainfall, surge, tide, or scientifically valid forcing.

## 1. Command admission and preparation

The CLI checks Snakemake availability, loads the main configuration, resolves run output and validates the launch plan before calling `prepare_run`. Mode errors therefore occur before preparation writes a new record. Preparation then:

1. Resolves estate bindings for run output, project directory, and selection.
2. Inspects whether the selection path is a file.
3. Refuses any configured `judgement.automatic_selection`: no automatic procedure is provided.
4. Computes the target list once.
5. Reads `toolkit_revision.toml` from the project directory and requires the revision gate.
6. Validates the selection document if present.
7. Writes `run_start.json` in the existing run-output directory.

Binding, revision, selection, or write failures become pre-run refusals. Preparation does not create the output directory. A failure does not imply all records from previous invocations were removed.

## 2. Full versus shortened targets

With a present selection, targets include every graph row's declared outputs. Without selection and with judgement retained, target computation excludes selection-reading rules and their downstream closure, then ensures method attribution remains included. In both cases `end_of_run/record` is appended last.

The pure `compute_targets` helper also treats delegated selection as a full graph, but the actual preparation path refuses delegation before reaching that branch. Selection absence is therefore a supported shorter run, not implicit automatic model choice.

## 3. Persisted handoff

The versioned JSON object has exactly five fields: `format` (`run-start:1`), absolute `main_configuration`, nonempty relative `targets`, absolute-or-null `selection`, and `judgement`. The vocabulary accepts `present`, `kept-absent`, and `delegated`; current preparation emits only the first two.

The reader rejects missing/unknown fields, wrong format, malformed JSON, invalid paths, empty targets, or unknown judgement state. Validation is structural: it does not prove target membership, named file existence, or consistency of every field with operator intent. Writes replace the file rather than append, and are ordinary text writes rather than an atomic state transaction.

## 4. Every engine parse gates again

The Snakefile moves its own source root to the front of `sys.path` before importing workflow code. It then calls `require_graph_start()` before defining rules. This reads the run-start record, reloads its configuration, resolves the project revision declaration through estate bindings, and checks the executing package's checkout, declared commit, and cleanliness through the revision gate.

This backstop also applies to a direct Snakemake invocation; no environment switch disables it. It does not repeat all CLI/pre-run checks, such as selection-form validation or mode binding. Graph refusal occurs after the CLI has launched an engine and therefore belongs to execution failure, unlike pre-run refusal. See [execution and recovery](../operations/execution-and-recovery.md) for bands and interruption behavior.

## 5. Default target and dependency ownership

`all` is deliberately the first rule, so a launch with no explicit target builds the persisted target list rather than merely the first rendered compute rule. Both `all` and `end_of_run` are local rules.

Normal row inputs are resolved through a producer map from artifact name to `<rule>/<output>`. Method attribution instead consumes this invocation's target list, excluding its own outputs and the final record. That exclusion prevents a cycle: the final record already waits on attribution. Shortened runs therefore do not make attribution depend on outputs the run intentionally omitted.

Rows bind system and rule name into `params` before execution; delayed rule bodies do not accidentally read the final loop iteration. Report items are flags on declared outputs selected by row metadata, not a separate inventory in the Snakefile.

## 6. Closing record and engine report

`end_of_run` depends on every target except its own directory output. It passes the working directory, targets, run-start record, and side selection to `write_end_of_run_record`. The workflow supplies the three provenance inputs together; partial sets are rejected. Provenance is written before optional divergence text and escaped HTML. Missing producer identity can prevent successful closure.

The standard CLI path selects current sides for registered pairs. The writer can also accept a side record containing pre-refactor selections; only that branch writes divergence files. Detailed identity and randomness semantics belong in [provenance and randomness](../concepts/provenance-and-randomness.md).

Snakemake builds `report.html` after successful execution from report-flagged artifacts. The directory `end_of_run/record` is the declared final artifact, so an up-to-date invocation need not rerun or rewrite it. Preparation can still replace `run_start.json` on each CLI invocation. A failed attempt may leave partial or older files; their existence alone is not a completion receipt for the latest attempt.

## Related pages

- [System boundaries](../architecture/system-boundaries.md)
- [Estate and revision](../configuration/estate-and-revision.md)
- [Model selection](../configuration/model-selection.md)
- [Execution and recovery](../operations/execution-and-recovery.md)
