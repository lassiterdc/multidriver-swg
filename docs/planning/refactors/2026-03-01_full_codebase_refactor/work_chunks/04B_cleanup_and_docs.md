# Work Chunk 04B: Cleanup and Documentation

**Phase**: 4B — Cleanup
**Last edited**: 2026-03-01

---

## Before Proceeding

- [`full_codebase_refactor.md`](../full_codebase_refactor.md) — master refactor plan
- [`CONTRIBUTING.md`](../../../../CONTRIBUTING.md) — development philosophy

**Prerequisite**: Work chunk 04A complete.

---

## Task Understanding

### Requirements

1. **Config field cleanup**:
   - Audit all `PipelineConfig` fields — remove any unused fields
   - Verify all field descriptions are accurate
   - Ensure no orphaned defaults in `defaults.py`

2. **Architecture documentation**:
   - Update `architecture.md` with final module structure, key modules, workflow phases, config system
   - Update `CLAUDE.md` if needed

3. **Old code archive**:
   - Verify all old functions are superseded by tests
   - Archive `_old_code_to_refactor/` (move or tag for removal)

4. **Code quality pass**:
   - `ruff check` and `ruff format` clean
   - No unused imports, variables, or functions
   - All public functions have docstrings and type hints

### Success Criteria

- `architecture.md` accurately describes the final codebase
- No unused config fields
- `ruff check .` and `ruff format --check .` pass
- All tests pass

---

## Definition of Done

- [ ] Config field audit complete — no unused fields
- [ ] `architecture.md` updated
- [ ] `CLAUDE.md` updated if needed
- [ ] Old code archived or removed
- [ ] `ruff check .` passes
- [ ] `ruff format --check .` passes
- [ ] All tests pass
- [ ] **Move this document to `implemented/` once all boxes above are checked**
