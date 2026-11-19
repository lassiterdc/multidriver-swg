# Usage

Start a run with the console command and one main configuration file:

```bash
multidriver-swg /path/to/run_configuration.toml
```

The command can also be started without a console script: `python -m multidriver_swg.workflow CONFIG`.
`--help` prints the usage line and `--version` prints the distribution name and version. The configuration
is the only argument; every engine option is set by the command.

The main configuration's fields, its mode and the estate it names are documented in
[Main Configuration](main-configuration.md), and the binding document it points at in
[Estate Bindings](estate-bindings.md).

## Exit status

Every ending of the command is one of three bands:

| Band | Meaning |
|---|---|
| `0` | The engine ran and the run completed. |
| `3` | The engine started and the run did not complete: a failed rule, a graph-start refusal raised after the engine began, an accepted submission whose job exited non-zero, or an interrupt. |
| `2` | The command refused to start: a usage error, an invalid configuration, a failed pre-run check (the binding document, the revision gate or the selection artifact), a submission the scheduler refused (no job id on its standard output), a script that cannot be written, or no importable engine. |

A script that reads the status can therefore tell "nothing ran" from "the run failed" without parsing a
message.

An interrupt is band 3: `Ctrl-C`, or the same signal sent to the command alone, reaches the engine, which is
asked to shut down; the command waits for it, then for its jobs, and returns band 3 with one line. A second
interrupt stops the wait and terminates the run's processes. No interrupt ends with a traceback.

## Modes

The configuration's `mode` selects how the run is launched; it changes no rule and no output.

| Mode | What it does | Fields it reads |
|---|---|---|
| `local` | The engine runs on this machine. | `[local] cores` |
| `batch_job` | The engine runs on the host the command runs on and submits one scheduler job per rule. | `[slurm] partition`, `account`, `time_minutes`, `jobs` |
| `1_job_many_srun_tasks` | The command writes `engine_batch.sh` into the run output and submits it with `sbatch --wait --parsable`; the engine runs every rule inside that one allocation. The submission's own output decides the band: no job id is band 2, a job id with exit 0 is band 0, and a job id with any non-zero exit — 2 included — is band 3. | `[slurm] partition`, `account`, `time_minutes`, `cpus_per_task` |

Every value a mode reads comes from the configuration. No mode reads an environment variable, a hostname or
a value from the allocation it happens to run in.

On a shared login node, export the three thread guards before starting a run, or let the command set them
where they are unset: `OPENBLAS_NUM_THREADS=1`, `OMP_NUM_THREADS=1` and `MKL_NUM_THREADS=1`. The engine
imports `numpy`, which otherwise starts one thread per core, and a login node that limits the process count
can refuse to start the engine at all.

## The run's report

A rule's declared output can also be a REPORT ITEM: an artifact the run's report shows, under a heading the
rule names. The citation section of the method-attribution rule is the run's one report item today, under
the heading `Citation`.

An item is a file a browser displays — `.html` or an image (`.svg`, `.png`, `.jpg`, `.jpeg`, `.gif`) — so a
figure or a table meant to be read in the page is written in one of those forms. A file the page cannot
display is refused where the item is declared rather than appearing as a download button, and an item that
names a file its rule does not declare is refused for the same reason: the engine flags a rule's output,
so an item is always one of that rule's own declared outputs.

The report itself is the engine's, built after a successful run from the outputs the rules flag; the command
does not build it and a failed run produces none.

## The end-of-run record

Every completed run ends with one directory in its run output, `end_of_run/record`. It is the run's closing
artifact: the run's `all` target ends with it, the rule that writes it waits on every other target, and
nothing waits on it, so a run that finishes has it and a run that fails does not.

Inside it, `provenance.json` says what produced the run. It names every system whose output the run's closing
rule waits on, the implementation and code version each of them wrote beside its outputs, the rules that ran
in this invocation, and which side of each registered pair the run selected. A run that stopped early names
only the systems of the targets it built. The list of systems is cumulative: an output left from an earlier
invocation counts its producer, so a re-run that rebuilt one step still names every producer, and only the
list of rules that ran in this invocation is smaller. The record is refused, and the run fails, when a
producer left no identity or one that names another system. It carries no seeds yet, because no step draws one.

When the run used the legacy side of a registered pair, the record carries the divergence report: a text
file (`divergence_report.txt`) and an HTML rendering of the same text (`divergence_report.html`). The HTML
file is the run's report item for the divergence, so a report built after such a run shows it under
`Divergence report`. A run whose every pair ran its current side diverged from nothing, so its record holds
neither file and the report shows no divergence item.

The record directory is the rule's declared output, so a second invocation that finds every target up to
date does not rewrite it.

## A run left locked by an interrupted engine

A killed engine leaves lock files that hold only file names, so nothing can tell whether an engine is still
running. The next invocation refuses with band 3 and prints the recourse; the command never unlocks on its
own. If no engine is running, release the lock with the printed invocation, which is:

```bash
python -m snakemake --snakefile <the package's Snakefile> --directory <the run output> \
    --profile none --workflow-profile none --unlock
```

The path printed by the command carries the installed workflow file and the run output filled in, with every
path quoted for a shell, so it can be pasted as it stands.

The allocation mode keeps the same recourse: its script writes this attempt's scheduler and engine output
into a log in the run output named for that attempt, and the command reads that log — never an earlier
attempt's — to find the engine's lock refusal before printing the invocation above.
