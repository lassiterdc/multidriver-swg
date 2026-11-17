# The SLURM shim

Five executables that stand in for the cluster's commands, so the two HPC launch bindings can be exercised
on a laptop. Each reproduces a measured arm of the Rivanna spike rather than an idealised behaviour.

| Command | What it reproduces | Knobs |
|---|---|---|
| `sbatch` | `--parsable`: the id on standard output once a job exists. WITH `--wait` it runs the script or `--wrap` payload and returns that exit code; WITHOUT it (the shape the driver executor submits) it starts the payload in the background and returns 0, as a scheduler does. A refused submission prints nothing and exits 1 | `SHIM_SBATCH_REFUSE`, `SHIM_JOB_ID`, `SHIM_JOB_OUTPUT`, `SHIM_CALL_RECORD` |
| `sacct` | the pipe-separated row the caller's `--format` list asks for (`JobIdRaw,State` for the plugin's status query; the six-column diagnostic row when no format is given), including the `FAILED`/`2:0` values the exit-2 arm leaves | `SHIM_SACCT_STATE`, `SHIM_SACCT_EXIT`, `SHIM_SACCT_EMPTY` |
| `sacctmgr` | the account validation the executor plugin runs before its first submission: one account per line, no second column | `SHIM_ACCOUNT_REFUSE` |
| `srun` | runs its first non-option argument; reports a version with none | — |
| `squeue` | a header with no rows, which is a finished job's listing | — |

The arms are the spike's: a refused partition gives `exit 1` with empty standard output, and an accepted
script that exits 2 gives its job id on standard output and `exit 2`. Those two outputs are what the band
classifier reads, so the shim's job is to reproduce them faithfully and nothing else.
