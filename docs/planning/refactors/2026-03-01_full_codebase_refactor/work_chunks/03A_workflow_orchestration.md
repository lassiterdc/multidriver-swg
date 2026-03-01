# Work Chunk 03A: Workflow Orchestration

**Phase**: 3A — Orchestration
**Last edited**: 2026-03-01

---

## Before Proceeding

- [`full_codebase_refactor.md`](../full_codebase_refactor.md) — master refactor plan
- [`CONTRIBUTING.md`](../../../../CONTRIBUTING.md) — development philosophy

**Prerequisite**: Work chunks 02A–02E complete. **Cross-cutting Decision 3 (human-in-the-loop design) must be resolved before starting this chunk.**

---

## Task Understanding

### Requirements

1. `src/multidriver_swg/runners/` — Runner scripts that wire I/O, config, and core computation together:
   - Phase A runner: data acquisition orchestration
   - Phase B runner: event selection and analysis pipeline
   - Phase C runner: distribution fitting and copula modeling (human-in-the-loop)
   - Phase D runner: stochastic simulation
   - Phase E runner: time series generation and QA/QC

2. Workflow orchestration (form TBD — depends on Decision 3):
   - Option 1: Snakemake workflow with two phases (pre/post human review)
   - Option 2: Script-based runners with config-driven sequencing
   - Option 3: Hybrid (automated phases in Snakemake, interactive phase as Jupyter)

### Key Design Decisions

- **Runner scripts own all I/O** — they call I/O functions to read data, pass it to pure computation functions, then write results
- **Config-driven** — runners receive `PipelineConfig` and execute the pipeline
- **Logging** — runners use Python `logging` module (replacing `print()`)
- **Resume support** — runners check for existing outputs before re-running

### Open Questions (must resolve before implementation)

- How does the human-in-the-loop phase for distribution selection work? (Decision 3)
- Snakemake vs. script-based orchestration?
- If Snakemake, single workflow or multiple workflows?

### Success Criteria

- Full Norfolk pipeline can be executed from config YAML
- Each phase can be run independently
- Human-in-the-loop step clearly defined
- Existing outputs not regenerated unnecessarily

---

## Definition of Done

- [ ] Runner scripts for all pipeline phases
- [ ] Orchestration approach implemented (per Decision 3 resolution)
- [ ] Logging replaces all `print()` statements
- [ ] Resume/skip logic for existing outputs
- [ ] End-to-end pipeline test with Norfolk data
- [ ] **Move this document to `implemented/` once all boxes above are checked**
