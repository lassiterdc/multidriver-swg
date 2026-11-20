# Real-cluster measurement material

The toolkit's half of a measurement of its command on a cluster. Nothing here holds a machine value: the
partition, the account, the time limit, the paths and the records of a measurement live in the estate, beside
the machine's own configuration. These files are run by the estate's scripts and are also checked locally,
under the `local` mode, by `tests/workflow/test_real_cluster_material.py`.

| File | What it is for |
|---|---|
| `probe.smk` | A workflow file that times one graph-start check, appends a line to `measurement/parses.jsonl` at every parse, and then includes the toolkit's own workflow file unchanged. Its `onstart`, `onsuccess` and `onerror` handlers each append a line to `measurement/handlers.jsonl`. The lines carry the host, the process, the scheduler job id and the time, so a measurement can say how many parses each mode makes, what one costs, and where the handlers run. When `measurement/licence_required.json` exists, it also sets the stub licence's required entries from that file at every parse, so the toolkit's own graph can be made to stop after its licence. |
| `checkpoint_probe.smk` | A small standalone workflow, not the toolkit's graph, whose licence rule is a checkpoint. It shows how the engine behaves at a checkpoint: a run whose selection fixture lacks a pooling entry for a rejected statistic ends after the licence with exit 0, its report and its final output, and a later run with the entry added runs the merge, its consumer and the final rule and not the licence. It logs every parse and every evaluation after the checkpoint. |
| `launch_probe.py` | Starts either workflow with the command's own bindings. It runs the real pre-run step, then takes the command's launch plan and changes only the workflow file. Eight arms: `run`, `noop`, `dryrun`, `fail`, `checkpoint-noruling`, `checkpoint-resume`, `graph-checkpoint-noruling`, `graph-checkpoint-resume`. |
| `collect_measurement.py` | Prints, as Markdown, what a run directory holds: the engine log's map of rule to job, the scheduler's accounting for those jobs, every measurement line with its host, each target and the rule its metadata record names, and the provenance record. It changes nothing. |

## Running the launcher

```bash
python tests/workflow/real_cluster/launch_probe.py CONFIG --arm run
python tests/workflow/real_cluster/collect_measurement.py RUN_DIRECTORY
```

`CONFIG` is this machine's main configuration. The run directory is the run output the configuration's binding
document names. A launch needs the checkout it runs from to be clean and to be the commit the project declares,
because the probe's graph-start check is the toolkit's own.

## What the arms are

- `run` launches the probe on a fresh output.
- `noop` launches it again on that output, so every output is up to date and nothing runs.
- `dryrun` launches it with the engine's dry-run flag.
- `fail` makes the first rule's directory read-only before the launch, so that rule's job fails.
- `checkpoint-noruling` launches the checkpoint workflow over a fixture with no pooling entry.
- `checkpoint-resume` adds the entry and launches it again on the same output.
- `graph-checkpoint-noruling` launches the probe over the toolkit's own graph. It requires the configuration's selection file to be absent, writes `measurement/licence_required.json` naming `compound.rainfall_depth`, and launches, so the run ends after the licence with exit 0, its report and its end-of-run record.
- `graph-checkpoint-resume --selection-from PATH` copies `PATH` to the configuration's selection path and launches on the same output, so the run continues from the merge and does not rebuild the licence.

```bash
python tests/workflow/real_cluster/launch_probe.py CONFIG --arm graph-checkpoint-noruling
python tests/workflow/real_cluster/launch_probe.py CONFIG --arm graph-checkpoint-resume --selection-from PATH
```

The measurement's values and records are written estate-side by the estate's scripts, not here.
