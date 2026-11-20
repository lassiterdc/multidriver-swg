---
type: operations
title: Local and SLURM execution, exits, and recovery
description: How launch modes bind resources, classify completion, isolate Snakemake settings, and recover interrupted or locked runs.
tags: [execution, slurm, recovery]
verified:
  - by: openwiki/0.9.0
    at: 2026-11-20T07:54:33.693Z
sources:
  - id: openwiki-source-4848d71ba0714fc9991173c8
    resource: repo://src/multidriver_swg/workflow/cli.py
  - id: openwiki-source-dddaeeacffc9b17104ad6989
    resource: repo://src/multidriver_swg/workflow/modes.py
generated: { by: "omp", at: "2026-11-20T07:54:33.693Z" }
---

# Local and SLURM execution, exits, and recovery

## Launch ownership

Run `multidriver-swg CONFIG` (or `python -m multidriver_swg.workflow CONFIG`). The CLI checks engine availability, loads configuration, resolves run output, builds a launch plan, and performs pre-run checks before launching. `launch_plan` owns the mode decision; the CLI executes either an engine plan or a submission plan.

| Mode | Execution and resources |
|---|---|
| `local` | Snakemake on this host with `[local] cores`. |
| `batch_job` | Snakemake on this host using the SLURM executor, configured job limit, partition, account, runtime, and 60-second latency wait. |
| `1_job_many_srun_tasks` | One `sbatch --wait --parsable` submission; the generated script uses one node, one task, configured CPUs and a local Snakemake executor within the allocation. The mode name does not mean this launcher emits an `srun` per rule. |

The executable `launch_plan` branch supports allocation submission even though older `engine_arguments` commentary says the command does not yet call it. The latter intentionally cannot represent a submission as engine arguments.

## Environment and artifacts

Direct engine launches use this Python interpreter and the packaged Snakefile, with run output as working directory, `--rerun-incomplete`, and `report.html` requested after a successful run. Both profile channels are explicitly `none`. Child environments drop every `SNAKEMAKE_*` variable; BLAS guards (`OPENBLAS_NUM_THREADS`, `OMP_NUM_THREADS`, `MKL_NUM_THREADS`) default to `1` only when unset or empty. Explicit operator values survive this environment transformation.

Allocation scripts additionally export all three guards as `1`. The script is `engine_batch.sh` inside run output, not toolkit source. Each plan chooses a fresh `allocation-<uuid>.log`; shell-quoted paths and `tee` capture the allocation's output while retaining scheduler output. A missing log is treated as empty, never replaced by a search through earlier attempts.

## Exit bands and acceptance

| Band | Interpretation |
|---|---|
| `0` | Direct engine returned zero, or an allocation submission printed a job identifier and returned zero. |
| `2` | CLI refusal before execution, or allocation submission without a nonempty identifier on stdout. Configuration/pre-run failures and script write failures use this band. |
| `3` | Started engine returned nonzero, an accepted allocation returned nonzero (including `2`), or launch processing was interrupted after starting. |

Submission classification reads the last nonempty stdout candidate before an optional `;cluster` suffix. It does not query scheduler accounting or independently verify that the candidate is a valid numeric job ID. These are subprocess-output semantics, not a live scheduler status guarantee. OS process-launch failures are not caught by `_run_engine`; do not assume every exceptional failure is normalized into a band.

## Interrupts and locks

The launched process starts a new session. First interrupt forwards SIGINT to its process group and allows 30 seconds for shutdown. Timeout or a second interrupt escalates to SIGTERM, waits up to 10 seconds, then SIGKILL and a bounded pipe drain; the final fallback can wait another 10 seconds while reaping. These bounds constrain waiting, not proof that an escaped or unsignalable process has died. For allocation mode the local process is `sbatch`; there is no explicit `scancel` operation here.

A pre-launch interrupt is a refusal. An interrupt handled after launch returns band `3` and resume advice. `--rerun-incomplete` helps the next invocation handle partial outputs, but never clears locks automatically.

If output contains `LockException`, a failed direct engine prints unlock advice. Allocation failures inspect only the current attempt's log for that name. Check that no engine is still writing before running the exact shell-quoted command printed by the CLI:

```sh
python -m snakemake --snakefile <installed-Snakefile> --directory <run-output> \
  --profile none --workflow-profile none --unlock
```

Unlock is an explicit operator action; it is not cancellation or evidence that a writer has stopped.

## Related pages

- [Estate configuration and revision](../configuration/estate-and-revision.md)
- [Run lifecycle](../workflows/run-lifecycle.md)
- [Verification map](../testing/verification-map.md)
- [Quickstart](../quickstart.md)
