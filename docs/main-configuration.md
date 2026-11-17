# Main Configuration

A run needs machine-specific facts: which estate directory it reads, which binding document names
this machine's paths, which project it works on, how it should be launched, and how many cores or
which scheduler account it may use. None of those may live in a tracked toolkit file — a value
written into the public repository publishes a private layout and bakes one machine into every
other machine's copy — so they live in a **run configuration**: one file, held by the estate,
supplied to the run by path.

Nothing in the toolkit creates this file, chooses its location or supplies a default. The loader is
`MainConfiguration.load(path)`, and a run that is given no path has no configuration at all.

This page is the contract that file must satisfy, and the operator step that creates it. It sits
beside [Estate Bindings](estate-bindings.md), which covers the document a boundary of the file
points at.

## Where the file lives

One file per machine and project. A second machine adds its own file and edits none of the others,
and a second project on the same machine adds its own file too, so no two runs share a mutable
configuration.

The file is the estate's, so its location is the estate's choice and its filename is the only place
a machine name appears in it: the configuration carries no machine field, for the same reason the
binding document carries none — a field would be a second statement of one fact, and no surface
could compare the two without the toolkit adopting the estate's naming convention.

The estate's own rule binds every tracked path it adds, so the commit that adds a run configuration
also adds its row to the estate's layout table. Creating the file and declaring it are one act.

## The fields

The file is one TOML document. Its top level holds the five fields below plus up to three tables.
A field or table the form does not name is refused rather than ignored.

| Field | Form | Required | Read by |
|---|---|---|---|
| `rule` | the literal `main-configuration:1` | always | the loader |
| `estate_root` | an absolute path | always | the binding-document lookup |
| `bindings` | the path of this machine's binding document, relative to `estate_root` | always | the binding-document lookup |
| `project` | `domain.project`, two segments of the path-key charset, and the second segment is not `shared` | always | path-key construction |
| `mode` | `local`, `batch_job` or `1_job_many_srun_tasks` | always | the command that launches the run |
| `[judgement] automatic_selection` | a procedure name | only when the run is to choose the models without a human | the step that computes what the run builds |
| `[local] cores` | a positive integer | when `mode = "local"` | the local launch |
| `[slurm] partition` | text | when the `[slurm]` table is present | both HPC launches |
| `[slurm] account` | text | when the `[slurm]` table is present | both HPC launches |
| `[slurm] time_minutes` | a positive integer | when the `[slurm]` table is present | both HPC launches |
| `[slurm] jobs` | a positive integer | when `mode = "batch_job"` | the batch launch |
| `[slurm] cpus_per_task` | a positive integer | when `mode = "1_job_many_srun_tasks"` | the single-allocation launch |

Two properties of that table are worth stating rather than leaving to be inferred.

**A table, once present, carries its own connection fields.** `[slurm]` names `partition`, `account`
and `time_minutes` as the scheduler connection, so a `[slurm]` table that is present at all must
carry all three, whatever the mode. A local run that does not read them omits the table instead of
writing part of it.

**Each HPC mode names the one limit it reads.** `jobs` is required only by `batch_job`, which lets
the engine submit one job per rule, and `cpus_per_task` only by `1_job_many_srun_tasks`, which runs
the engine inside a single allocation. The other limit may be present and is validated when it is;
it is simply not required.

A positive integer is at least `1`, and a TOML boolean is not one: `cores = true` is refused, not
read as `1`. Text fields are refused when empty.

## The version literal

`rule` is compared as an exact literal and is never parsed or migrated. A file declaring another
value was written against another form, and reading it optimistically is the silent misread the
header exists to prevent: adding a field to a later form must make an older file loud rather than
quietly drop the field it does not carry.

## The modes

A mode selects **how the run is launched**; it changes no computation.

- `local` — the engine runs on this machine with `cores` workers.
- `batch_job` — the engine runs on the host the command runs on and submits one scheduler job per
  rule, bounded by `jobs`.
- `1_job_many_srun_tasks` — the command writes a batch script and submits it, and the engine runs
  every job inside that one allocation, using `cpus_per_task` workers.

Every per-machine value a mode reads comes from this file. A mode reads none from the environment,
the hostname or the allocation it happens to run in.

## The three fixed path keys

The toolkit fixes three path keys for a project, and the configuration's `project` field is what
builds them:

| Key | What it names |
|---|---|
| `{project}.run_output` | the engine's working directory: the directory a run writes its outputs, report and end-of-run record beneath |
| `{project}.project_directory` | the estate's `{domain}/{project}/` directory, where the project's own files live |
| `{project}.selection` | the file holding the run's recorded model selection, which may not exist yet |

Each resolves through the machine's binding document, so no value for any of the three appears in
this file, in a tracked toolkit file, or in the source. A key the binding document does not bind is
refused at lookup, naming both the document and the keys it does bind.

## The declared revision

The commit a run must execute is declared by the project, in a separate file in the project
directory:

```toml
rule = "toolkit-revision:1"
commit = "{forty lowercase hexadecimal characters}"
tag = "{the release name the commit was reached by}"
```

| Field | Form | Required |
|---|---|---|
| `rule` | the literal `toolkit-revision:1` | always |
| `commit` | exactly forty lowercase hexadecimal characters | always |
| `tag` | non-empty text | no |

**The commit governs.** `tag` is carried so an operator can see which named release a commit
belonged to, and it is never compared: a tag is a movable name and a commit is not. A tag field the
form does not name is refused, so a mistyped key cannot sit in the file looking effective.

Before a run computes anything, three checks compare the executing toolkit against that declaration:

1. the running package belongs to a git checkout, and that checkout's own source tree is the
   directory the package was imported from. An installed copy inside some unrelated repository fails
   here even though `git rev-parse --show-toplevel` succeeds inside it;
2. the checkout's full commit equals the declared `commit`;
3. the checkout's working tree is clean. A tracked edit changes a module and an untracked file the
   checkout's ignore rules do not cover can add one, so both count. Ignored files — a virtual
   environment, a build directory — do not.

A run that fails any of the three does not start.

## What the file refuses

Every row below stops the configuration from being read at all, and every refusal names the field in
the configuration's own spelling (`slurm.partition`, not `partition`) and the file it read. A
partially-read configuration would leave the run holding a mixture of checked and unchecked values,
so the only safe report is a halt.

| Refused when | Why |
|---|---|
| The file is not valid TOML | There is nothing to read. |
| A key the form does not name, at the top level or inside a named table | A setting its writer believes is live while nothing reads it. |
| `rule` is absent or is not `main-configuration:1` | The version header is what makes a later change to the form loud. |
| `estate_root` is absent, empty or not absolute | A relative root resolves against whatever working directory a run happens to start in. |
| `bindings` is absent, empty or absolute | The field composes with `estate_root`; an absolute value carries a second, uncomposed statement of where the estate is. |
| `project` is absent, empty, is not exactly two dotted segments, carries a character outside the key charset, or names the reserved `shared` project | The project is what the three fixed path keys are built from. |
| `mode` is absent or is not one of the three named modes | A mode selects a launch binding, and no other binding exists. |
| The table or field the chosen mode reads is absent | The run would otherwise fail later with a value nothing supplied. |
| A limit is not a positive integer, or a text field is empty | A scheduler cannot honour a boolean, a text value or a number below one as a limit. |
| `[judgement] automatic_selection` is present but is not non-empty text | It names a procedure, so an empty name names nothing. |

The revision declaration refuses the same two shape defects — a file that is not TOML, and a key or
`rule` the form does not name — plus a `commit` that is not forty lowercase hexadecimal characters
and a `tag` that is present but empty.

## The operator step

1. **Locate the estate root** on this machine.

2. **Create the configuration file** wherever the estate keeps this machine's run configurations,
   and write the fields above. The skeleton below is the shape, with every value a placeholder
   rather than a value.

3. **Create the binding document** the `bindings` field names, beneath the estate root, following
   [Estate Bindings](estate-bindings.md).

4. **Declare the new files in the estate repository, in the same commit.** The estate's layout rule
   cannot see a file that is undeclared inside a directory that already has a row, so an undeclared
   configuration is invisible to the estate's own test and the omission is silent.

5. **Declare the revision** the run must execute, in the project's `toolkit_revision.toml`, and
   check the toolkit out at exactly that commit with a clean tree before starting.

A second machine repeats steps 2 to 5 with its own values and edits no other machine's file.

## A skeleton

Every value is a placeholder. The `[slurm]` table and one of its two limits are shown; a `local`
run writes `[local]` instead, and an HPC run writes the limit its own mode reads.

```toml
rule = "main-configuration:1"
estate_root = "{the absolute path of this machine's estate directory}"
bindings = "{the binding document's path, relative to estate_root}"
project = "{domain}.{project}"
mode = "{local | batch_job | 1_job_many_srun_tasks}"

[judgement]
automatic_selection = "{the procedure name, or omit this table}"

[local]
cores = {a positive integer, for mode = "local"}

[slurm]
partition = "{the partition name}"
account = "{the account the jobs bill to}"
time_minutes = {a positive integer}
jobs = {a positive integer, for mode = "batch_job"}
cpus_per_task = {a positive integer, for mode = "1_job_many_srun_tasks"}
```
