---
type: configuration
title: Estate paths, launch configuration, and revision gates
description: External TOML ownership, logical estate-path resolution, launch-mode requirements, and checkout revision enforcement before preparation and engine parsing.
tags: [configuration, estate, paths, reproducibility]
verified:
  - by: openwiki/0.9.0
    at: 2026-11-20T07:54:33.693Z
sources:
  - id: openwiki-source-133836d39d09110782d86224
    resource: repo://docs/estate-bindings.md
  - id: openwiki-source-b98a31da6c93ba1cdb628ccb
    resource: repo://docs/main-configuration.md
  - id: openwiki-source-7030645839129425893ea6ad
    resource: repo://src/multidriver_swg/run_configuration/main_configuration.py
  - id: openwiki-source-aad4c9e5762f6ebc3cfec001
    resource: repo://src/multidriver_swg/run_configuration/path_resolver.py
  - id: openwiki-source-3c8444a85a2033a86bf58062
    resource: repo://src/multidriver_swg/run_configuration/revision_gate.py
  - id: openwiki-source-432e9e79a89827275cf0b537
    resource: repo://src/multidriver_swg/workflow/graph_start.py
  - id: openwiki-source-f0ef694cda92c207cdb31d2a
    resource: repo://src/multidriver_swg/workflow/pre_run.py
  - id: openwiki-source-1d27f6195bd3e6ecb6f6a9b5
    resource: repo://src/multidriver_swg/workflow/Snakefile
generated: { by: "omp", at: "2026-11-20T07:54:33.693Z" }
---

# Estate paths, launch configuration, and revision gates

## Ownership and resolution chain

Machine-specific configuration belongs to the external estate, not tracked toolkit defaults. The operator supplies a run-configuration path to `MainConfiguration.load(path)`; the toolkit neither creates that file nor chooses a default location. The estate documentation prescribes one configuration per machine/project and a binding file at `config/paths_{machine}.toml`. Those are estate conventions: the loaders accept supplied paths, do not derive a machine name, and do not check hostname-to-filename correspondence.

The normal resolution chain is:

1. Main configuration supplies `estate_root`, relative `bindings`, and `project`.
2. The caller composes `Path(estate_root) / bindings` and loads that TOML document.
3. For `project = "{domain}.{project}"`, the pre-run step resolves `{project}.run_output`, `{project}.project_directory`, and `{project}.selection` through the binding map.
4. `run_output` is the engine working/output directory; `project_directory` contains `toolkit_revision.toml`; `selection` names the model-selection file, which may not yet exist.

Here `{project}` in the three keys means the entire two-segment configuration value. Braced expressions are explanatory placeholders, not deployable path values. No actual machine paths are supplied by this page.

## Strict main configuration

The exact version literal is `main-configuration:1`. Top-level keys are limited to `rule`, `estate_root`, `bindings`, `project`, `mode`, `judgement`, `local`, and `slurm`. Named tables also have closed key sets. Unknown settings fail rather than being silently ignored; loaders do not parse or migrate version headers.

Required scalar fields are the rule, an absolute estate root, a non-empty relative bindings path, a two-segment project, and one of the three modes below. Project segments use the logical-key grammar; `shared` is forbidden as the project segment. These checks establish syntax, not that the estate or project exists. In particular, checking that `bindings` is relative is not a canonical containment/security check against `..` traversal.

| Mode | Required settings | Intended launch boundary |
| --- | --- | --- |
| `local` | `[local] cores` | Local engine workers |
| `batch_job` | `[slurm] partition`, `account`, `time_minutes`, `jobs` | Engine on launching host; scheduler jobs per rule |
| `1_job_many_srun_tasks` | `[slurm] partition`, `account`, `time_minutes`, `cpus_per_task` | Engine/jobs inside one allocation |

All limits are positive integers, not booleans. Any present `[slurm]` table must carry its three connection fields even for a local configuration; omit it if unused. The non-required HPC limit may be present but is still validated. The parser does not establish scheduler account/partition existence or admissible wall time.

`[judgement]` accepts only `automatic_selection`, a procedure name when present. Parsing a valid name does not mean the procedure exists: `prepare_run()` currently refuses automatic-selection delegation because the toolkit supplies none. See [model selection](model-selection.md).

## Binding document and logical keys

Bindings use exact rule `path-bindings:1` and a flat `[paths]` table; these are the only top-level fields. Keys are exactly three dot-separated segments, each matching `[A-Za-z0-9_-]+`, with project segment other than literal `shared`. The grammar checks shape, not an estate vocabulary.

Quote the **entire** dotted key. Unquoted dots create TOML nested tables; the loader explicitly rejects table-valued bindings and tells the operator to quote the key rather than reporting a misleading lookup miss.

Every value must be an absolute string without `$` or `{`. This rejects unresolved shell/template tokens, including `${...}`; the resolver does not expand them. This placeholder refusal belongs to binding values, not a general promise that every text field in every configuration rejects placeholders. Validated paths are exposed read-only.

`resolve()` returns the bound `Path` or raises `UnboundPathKeyError` listing the requested key, document, and available keys. It invents no fallback and does not establish that the returned path exists or contains the intended data. The separate `estate_root()` helper likewise requires an explicit absolute argument rather than consulting environment, hostname, working directory, or defaults.

## Revision authority

The project declaration is `toolkit_revision.toml` with exact rule `toolkit-revision:1`, required `commit`, and optional `tag`; all other keys fail. Commit must be exactly forty lowercase hexadecimal characters. A present tag must be non-empty text. **Only the commit governs**: the tag is descriptive and is never compared or resolved as authority.

`require_revision_gate()` checks, in order:

1. The imported package belongs to a Git checkout, and its resolved directory is precisely that checkout's `src/multidriver_swg`. A wheel installed inside an unrelated clone does not qualify merely because Git finds an enclosing repository.
2. The running full commit equals the declared commit.
3. The checkout has no tracked changes or non-ignored untracked files.

Ignored files do not dirty the gate. Git status explicitly requests normal untracked reporting even if user Git configuration suppresses it. Child Git calls strip repository-location environment variables so inherited `GIT_DIR`/index/worktree settings cannot redirect the comparison to another checkout; Git configuration settings remain available.

## Two enforcement points and repair

Preparation resolves all three keys, loads the project's declaration, and applies the revision gate before writing the run-start record. The Snakefile then calls `require_graph_start()` on every parse, including direct engine invocation. That backstop reads the run-start record, reloads its named configuration and project declaration through estate bindings, then applies the same checkout/commit/clean checks. Failures become Snakemake `WorkflowError`; no skip flag/environment bypass is supplied.

The graph-start backstop is not a replacement for preparation: it does not validate selection contents, outputs, or launch bindings. It checks internal consistency, not whether the operator chose the right estate/declaration.

Repair the named boundary rather than supplying a toolkit default:

- Configuration/grammar error: repair the named field or versioned document.
- Unbound key: add the fully quoted key to the selected machine's bindings.
- Missing declaration: provide the project's `toolkit_revision.toml` with the intended full commit.
- Wrong imported package: launch from that checkout's source, not a copied wheel.
- Commit mismatch: use the declared checkout revision, or deliberately change the estate declaration to the intended revision.
- Dirty checkout: preserve and reconcile user changes before running; the gate is not a request to discard work.

## Related pages

- [Model selection](model-selection.md)
- [Execution and recovery](../operations/execution-and-recovery.md)
- [Quickstart](../quickstart.md)
- [Run lifecycle](../workflows/run-lifecycle.md)
