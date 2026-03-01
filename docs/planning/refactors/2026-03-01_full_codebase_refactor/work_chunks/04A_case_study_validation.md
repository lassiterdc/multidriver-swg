# Work Chunk 04A: Case Study Validation

**Phase**: 4A — Validation (End-to-End Norfolk Run)
**Last edited**: 2026-03-01

---

## Before Proceeding

- [`full_codebase_refactor.md`](../full_codebase_refactor.md) — master refactor plan
- [`CONTRIBUTING.md`](../../../../CONTRIBUTING.md) — development philosophy

**Prerequisite**: Work chunks 03A, 03B complete.

---

## Task Understanding

### Requirements

1. Run the complete Norfolk pipeline using the refactored library
2. Compare outputs against old pipeline outputs at key checkpoints:
   - Event selection: same events identified
   - Distribution fitting: same GOF metrics
   - Simulated event statistics: same distributions (statistical test)
   - Generated time series: same format and comparable statistics
3. Validate TRITON-SWMM_toolkit compatibility:
   - Load generated NetCDF with TRITON-SWMM's reader
   - Verify dimensions, variables, units

### Success Criteria

- Full pipeline runs without error on Norfolk data
- Key checkpoint comparisons pass within tolerance
- TRITON-SWMM compatibility confirmed
- All data gaps from chunk 00 tracking checklist resolved

---

## Definition of Done

- [ ] Full Norfolk pipeline executes end-to-end
- [ ] Checkpoint comparisons documented
- [ ] TRITON-SWMM compatibility validated
- [ ] All chunk 00 data tracking items resolved
- [ ] **Move this document to `implemented/` once all boxes above are checked**
