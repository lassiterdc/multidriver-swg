# Changelog

## [Unreleased]

### Added

- A `multidriver-swg` console command (also `python -m multidriver_swg.workflow`) starts a run from one main configuration file; it runs the pre-run checks, writes the run-start record and starts the engine, reporting one of three exit bands (0 completed, 3 started and not completed, 2 refused to start), and it constructs the engine's environment rather than inheriting it.
- Three launch bindings, all wired: `local` and `batch_job` start the engine on this host, and `1_job_many_srun_tasks` writes `engine_batch.sh` into the run output and submits it with `sbatch --wait --parsable` as one allocation; the submission's band is read from its job id and exit — no id is 2, an id with exit 0 is 0, an id with any non-zero exit is 3 — and its engine output is captured in a per-attempt log in the run output, so a lock refusal prints the same unlock invocation the engine path prints without the command ever clearing the lock.
- The run's report is a field of the rule table: a row declares which of its outputs the report shows and the heading each appears under, the workflow file flags those outputs for the engine's report, and the citation section is the one item the graph declares today. An item is refused unless it names a declared output once and carries a heading, and unless its file is one a browser displays — HTML or an image — so an artifact meant to be read in the page cannot silently become a download button.
- A release now runs the whole test suite at the tagged commit and refuses the tag unless that run was whole, complete and passing on `main`, with both source files recording the tagged version; nothing is built or uploaded until that gate passes.
