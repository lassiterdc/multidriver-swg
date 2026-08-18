# `_old_code_to_refactor/` — provenance and how to reach the git history

These 15 scripts are a **manual snapshot**, hand-picked out of a larger repo to
separate this project's work from the ensemble-modeling and flood-hazard code it
was tangled with. They are the refactor's input, not its output.

## Where they came from

| | |
|---|---|
| Upstream repo | <https://github.com/lassiterdc/stormy> |
| Branch | `working-ornl-hpc11` |
| Commit pin | `c74c8a4052efcbb1345981721532f462090d1a12` |
| Map | `_old_code_to_refactor/{script}` → `stochastic_storm_rescaling/{script}` |

Verified 2026-08-18: **all 15 files are byte-identical** to their upstream copies
at that commit (`git show` + `cmp`, per file). The snapshot was a clean copy with
no edits, so upstream history is the history of these exact files.

`working-ornl-hpc11` is the branch the work was done on, and this is measured
rather than assumed — it carries 2023 commits dated to 2026-01-03, against 1135
(2024-06-19), 891 (2024-01-25) and 781 (2023-12-01) on the other three branches.

## Getting the history

Deliberately NOT a submodule. stormy is MIT-licensed while this repo is PolyForm
Noncommercial, only 33 of stormy's 648 files are relevant here, and stormy carries
its own third-party submodules — so it is referenced by pin rather than vendored.

```bash
git clone --filter=blob:none --no-checkout https://github.com/lassiterdc/stormy.git /tmp/stormy
git -C /tmp/stormy log --oneline origin/working-ornl-hpc11 -- stochastic_storm_rescaling/{script}
```

The blobless clone is ~1.4 MB and takes seconds. Two caveats worth knowing before
you spend time:

- **Depth is shallow.** 7–10 commits per file, with terse messages (`stuff`,
  `cleaning up`, `rename`). Good for "was this ever finished / when did this
  change"; it carries no design rationale.
- **Content search is impractical on a blobless clone.** `git log -S "{string}"`
  times out, because it lazily fetches per commit across 2023 commits. Metadata
  queries are instant. Take a full clone if you need a pickaxe search.

## Two scripts in here do not run, and history already explains one

- `_c_gen_ncei_hrly_and_daily_data.py` — `ImportError` at line 2
  (`from _inputs import def_inputs_for_c`). Its entire upstream history is ONE
  commit: `8b2637e initial commit (script is incomplete)`. **It was never
  finished.** This is not a regression to diagnose.
- `_a_dwnld_and_process_water-level-data.py` — `NameError`; it calls
  `wlevel_event_selection`, which is defined nowhere. Its tail is superseded in any
  case: the live `surge_event_selection` is `_utils.py:1424`, not the copy at
  `_a_dwnld…:369`. Dead code calling a dead name in a dead tail.

Scope note: AORC download, NCEI station processing, MRMS→subcatchment averaging and
NOAA CO-OPS acquisition were absorbed into `hydro-fetch` on 2026-08-17 and are
**not** to be reimplemented here.
