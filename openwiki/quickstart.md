---
type: guide
title: Quickstart and task routing
description: Install the current source, prepare external estate inputs, and find the workflow, configuration, provenance, and verification contracts without mistaking stub outputs for simulated weather.
tags: [quickstart, installation, workflow, navigation]
verified:
  - by: openwiki/0.9.0
    at: 2026-11-20T07:54:33.693Z
sources:
  - id: openwiki-source-133836d39d09110782d86224
    resource: repo://docs/estate-bindings.md
  - id: openwiki-source-14586213345f63fc2ffbcb43
    resource: repo://docs/installation.md
  - id: openwiki-source-b98a31da6c93ba1cdb628ccb
    resource: repo://docs/main-configuration.md
  - id: openwiki-source-642add6b4a8bd8436dd0bc71
    resource: repo://docs/usage.md
  - id: openwiki-source-05ccef8d4cf1698187f20464
    resource: repo://pyproject.toml
  - id: openwiki-source-23775c3de52f3ab95a13cb8b
    resource: repo://README.md
  - id: openwiki-source-3c8444a85a2033a86bf58062
    resource: repo://src/multidriver_swg/run_configuration/revision_gate.py
  - id: openwiki-source-f0ef694cda92c207cdb31d2a
    resource: repo://src/multidriver_swg/workflow/pre_run.py
  - id: openwiki-source-de9274ba9e42deec3d3631b9
    resource: repo://src/multidriver_swg/workflow/stubs.py
generated: { by: "omp", at: "2026-11-20T07:54:33.693Z" }
---

# Quickstart and task routing

## Start with the implementation boundary

**The current workflow does not simulate weather.** Its compute rules write empty declared artifacts and record `implementation = "stub"` with the executing revision. Successful graph execution is not computed rainfall, storm surge, tidal phase, or scientifically usable flood forcing. Start with [system boundaries](architecture/system-boundaries.md) to distinguish implemented infrastructure from the intended generator.

This source declares **`0.2.0.dev0`, AGPL-3.0-or-later**. The README identifies the older PyPI release as `0.1.0`, uploaded on 2026-03-01 with an MIT declaration; `pip install multidriver-swg` is not the installation route for the source described here. The legacy `_old_code_to_refactor/` material has a separate MIT licence and attribution notices; do not apply the source toolkit's licence indiscriminately to it.

## Install from the intended checkout

Use Python 3.11 or newer. From the repository root, in an activated environment:

```bash
pip install -e .
multidriver-swg --help
multidriver-swg --version
```

For documentation tools use `pip install -e ".[docs]"`; for the pinned Slurm executor plugin use `pip install -e ".[hpc]"`. The runtime dependencies include NumPy and pinned Snakemake. Release reproduction and the pinned test environment are separate concerns: see [release verification](operations/release-verification.md).

**Installation alone is not run admission.** The imported package must be the source directory of a Git checkout, that checkout must be at the full commit declared by the project, and its working tree must be clean. A copied wheel inside an unrelated clone does not satisfy the source-ownership check. Ignored files do not dirty the gate; tracked edits and non-ignored untracked files do. Reconcile changes without discarding user work. See [estate and revision](configuration/estate-and-revision.md).

## Prepare the external estate first

The toolkit does not supply machine-specific paths or create a default run configuration. Prepare these outside the toolkit checkout:

1. A main TOML configuration with `rule = "main-configuration:1"`, absolute `estate_root`, relative `bindings`, two-segment `project`, and a launch `mode`. For a local run, supply `[local] cores` as a positive integer.
2. The named binding TOML with `rule = "path-bindings:1"` and a flat `[paths]` table. Quote each entire dotted key and use actual absolute paths, not shell variables or brace placeholders.
3. Bind all three project keys: `<domain>.<project>.run_output`, `<domain>.<project>.project_directory`, and `<domain>.<project>.selection`. The angle-bracket spellings here are explanatory, not literal configuration values.
4. In the project directory, provide `toolkit_revision.toml` with `rule = "toolkit-revision:1"` and `commit` equal to the intended forty-character lowercase Git commit. An optional `tag` is descriptive, not revision authority.
5. Ensure the bound run-output directory already exists and is writable: preparation writes `run_start.json` there rather than creating the directory.

The selection **binding** is required even when the selected file is absent. An absent selection permits a shortened graph; a present file is validated. Do not set `judgement.automatic_selection`: preparation refuses it because no automatic-selection procedure is supplied. Follow [model selection](configuration/model-selection.md) before constructing a selection file, and [estate and revision](configuration/estate-and-revision.md) for complete configuration forms.

## Launch and interpret the result

With those inputs prepared:

```bash
multidriver-swg /absolute/path/to/run_configuration.toml
# Equivalent module entrypoint:
python -m multidriver_swg.workflow /absolute/path/to/run_configuration.toml
```

The configuration is the only run argument. `local`, `batch_job`, and `1_job_many_srun_tasks` select launch behavior, not different computation; scheduler settings belong in the configuration. The command uses exit band **0** for completion, **2** for refusal before execution or unaccepted submission, and **3** for execution failure, accepted-job failure, or interruption. Band 0 still means stub-graph success, not generated weather.

Use [run lifecycle](workflows/run-lifecycle.md) for the handoff from preparation through graph parsing and closing artifacts. Use [execution and recovery](operations/execution-and-recovery.md) for exact mode behavior, thread guards, interruption, submission diagnostics, and explicit lock recovery rather than guessing from a partial output directory.

## Find the right contract

| Task | Read |
| --- | --- |
| Understand what exists and which subsystem owns a responsibility | [System boundaries](architecture/system-boundaries.md) |
| Configure estate paths, launch resources, or repair a revision refusal | [Estate and revision](configuration/estate-and-revision.md) |
| Supply model choices or understand a selection-absent run | [Model selection](configuration/model-selection.md) |
| Trace preparation, target selection, graph gates, and completion | [Run lifecycle](workflows/run-lifecycle.md) |
| Run locally or under Slurm; recover from interrupts and locks | [Execution and recovery](operations/execution-and-recovery.md) |
| Understand implementation identities, closing provenance, and seed contracts | [Provenance and randomness](concepts/provenance-and-randomness.md) |
| Compare refactored functions with legacy behavior and deliberate divergence | [Legacy correspondence](concepts/legacy-correspondence.md) |
| Choose focused checks and understand suite boundaries | [Verification map](testing/verification-map.md) |
| Understand publication-math conformance and its reporting boundary | [Publication conformance](testing/publication-conformance.md) |
| Reproduce dependency locks, verify distributions, or prepare a release | [Release verification](operations/release-verification.md) |
