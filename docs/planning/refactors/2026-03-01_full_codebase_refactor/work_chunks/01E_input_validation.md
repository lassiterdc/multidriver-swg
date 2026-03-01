# Work Chunk 01E: Input Validation Layer

**Phase**: 1E — Foundation (Input Validation)
**Last edited**: 2026-03-01

---

## Before Proceeding

- [`full_codebase_refactor.md`](../full_codebase_refactor.md) — master refactor plan
- [`CONTRIBUTING.md`](../../../../CONTRIBUTING.md) — development philosophy

**Prerequisite**: Work chunks 01B, 01C, 01D complete.

---

## Task Understanding

### Requirements

1. `src/multidriver_swg/validation/input_validation.py` — Validate all input data before pipeline execution:
   - File existence checks for all config-referenced paths
   - Data schema validation (expected columns in CSVs, dimensions in NetCDFs)
   - CRS consistency checks across geospatial files
   - Date range consistency (water level data covers the expected period)
   - NOAA station data completeness checks

### Key Design Decisions

- **Fail-fast** — validation runs before any computation; all errors collected and reported at once
- **Config-driven** — validators receive config objects, not raw paths
- **Pydantic model validators** handle structural config validation (01B); this layer handles data content validation
- Existence checks are appropriate for input files (per CONTRIBUTING.md)

### Success Criteria

- Missing input files produce clear error messages listing all missing files (not just the first)
- Schema mismatches (wrong columns, wrong dimensions) detected early
- Validation is a single callable: `validate_inputs(config) -> list[ValidationError]`

---

## Evidence from Codebase

1. `_old_code_to_refactor/*.py` — identify all file reads and what format they expect
2. `src/ss_fha/validation/` — reference implementation from ss-fha

---

## File-by-File Change Plan

### New Files

| File | Purpose |
|------|---------|
| `src/multidriver_swg/validation/__init__.py` | Package init |
| `src/multidriver_swg/validation/input_validation.py` | Input data validation |

---

## Definition of Done

- [ ] `validation/input_validation.py` — validates file existence and data schemas
- [ ] Error accumulation (all issues reported, not just first)
- [ ] Tests pass with mock data (valid + intentionally invalid)
- [ ] **Move this document to `implemented/` once all boxes above are checked**
