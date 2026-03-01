# Work Chunk 01F: Test Infrastructure and Fixtures

**Phase**: 1F — Foundation (Test Infrastructure)
**Last edited**: 2026-03-01

---

## Before Proceeding

- [`full_codebase_refactor.md`](../full_codebase_refactor.md) — master refactor plan
- [`CONTRIBUTING.md`](../../../../CONTRIBUTING.md) — development philosophy

**Prerequisite**: Work chunks 01A–01E complete.

---

## Task Understanding

### Requirements

1. `tests/conftest.py` — Shared test fixtures:
   - Minimal valid config objects (system + pipeline)
   - Small synthetic datasets for event selection, distribution fitting, time series rescaling
   - Temp directory fixtures for I/O tests
   - Fixture to load old code modules for comparison testing

2. Test utilities for comparison testing pattern:
   - Helper to import functions from `_old_code_to_refactor/` scripts
   - Standard assertion helpers (`assert_dataframes_equal`, `assert_datasets_equal`)

### Key Design Decisions

- **Comparison testing**: Old functions imported directly from `_old_code_to_refactor/` for result comparison
- **Small synthetic data**: Tests must run fast; use small generated datasets, not full Norfolk data
- **Parametrized fixtures**: Config fixtures parametrized over event types ("rain", "surge", "combined")

### Success Criteria

- All Phase 1 tests pass with the fixture infrastructure
- Comparison test pattern demonstrated with at least one old function
- Test suite runs in < 30 seconds

---

## File-by-File Change Plan

### New/Modified Files

| File | Purpose |
|------|---------|
| `tests/conftest.py` | Shared fixtures, config factories, old code import helpers |
| `tests/helpers.py` | Assertion utilities for DataFrames and Datasets |

---

## Definition of Done

- [ ] `tests/conftest.py` with config fixtures, synthetic data, old code import helpers
- [ ] `tests/helpers.py` with comparison assertion utilities
- [ ] At least one demonstration comparison test against old code
- [ ] All Phase 1 tests pass
- [ ] **Move this document to `implemented/` once all boxes above are checked**
