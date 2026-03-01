# Work Chunk 03B: CLI Entry Point

**Phase**: 3B — Orchestration (CLI)
**Last edited**: 2026-03-01

---

## Before Proceeding

- [`full_codebase_refactor.md`](../full_codebase_refactor.md) — master refactor plan
- [`CONTRIBUTING.md`](../../../../CONTRIBUTING.md) — development philosophy

**Prerequisite**: Work chunk 03A complete.

---

## Task Understanding

### Requirements

1. CLI entry point for running the pipeline:
   - Accept config YAML path
   - Support running specific phases or the full pipeline
   - Clear progress output

2. Approach TBD — depends on orchestration design (03A):
   - If Snakemake: CLI may be a thin wrapper around `snakemake` invocation
   - If script-based: may use `typer` or `click` for CLI framework

### Success Criteria

- `multidriver-swg run pipeline_config.yaml` executes the pipeline
- `multidriver-swg run pipeline_config.yaml --phase data-acquisition` runs a single phase
- Help text is clear and complete

---

## Definition of Done

- [ ] CLI entry point implemented
- [ ] Phase selection supported
- [ ] Config YAML loading and validation integrated
- [ ] Help text complete
- [ ] **Move this document to `implemented/` once all boxes above are checked**
