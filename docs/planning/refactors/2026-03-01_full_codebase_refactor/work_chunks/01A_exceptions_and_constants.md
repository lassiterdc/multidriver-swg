# Work Chunk 01A: Exceptions and Constants

**Phase**: 1A — Foundation (Exceptions and Constants)
**Last edited**: 2026-03-01

---

## Before Proceeding

Review the following documents before making any edits to plans or writing any code:

- [`docs/planning/refactors/2026-03-01_full_codebase_refactor/full_codebase_refactor.md`](../full_codebase_refactor.md) — master refactor plan
- [`CONTRIBUTING.md`](../../../../CONTRIBUTING.md) — project development philosophy

**Prerequisite**: Work chunk 00 must be complete (variable classification finalized).

---

## Task Understanding

### Requirements

1. `src/multidriver_swg/exceptions.py` — Custom exception hierarchy:
   - `MultiDriverSWGError` (base)
   - `ConfigurationError` — invalid config, missing required fields
   - `DataAcquisitionError` — download failures, API errors
   - `ValidationError` — data validation failures
   - `EventSelectionError` — event selection/classification failures

2. `src/multidriver_swg/constants.py` — True constants from `_inputs.py`:
   - Unit conversions (`mm_per_inch`, `feet_per_meter`)
   - Any physical constants
   - Other constants identified in chunk 00 classification

### Key Design Decisions

- Custom exceptions must preserve context (file paths, return codes) per CONTRIBUTING.md
- Constants must be true constants — never varied by case study or analysis
- No defaults for anything that's case-study-specific

### Success Criteria

- All exceptions have informative `__init__` signatures
- All constants from chunk 00 classification are placed here
- Both modules importable: `from multidriver_swg.exceptions import ConfigurationError`

---

## Evidence from Codebase

Before implementing, inspect:

1. `_old_code_to_refactor/_inputs.py` — extract constants (from chunk 00 classification)
2. `_old_code_to_refactor/_utils.py` — identify `sys.exit()` patterns to replace with exceptions
3. `src/ss_fha/exceptions.py` — reference implementation from ss-fha

---

## File-by-File Change Plan

### New Files

| File | Purpose |
|------|---------|
| `src/multidriver_swg/exceptions.py` | Custom exception hierarchy |
| `src/multidriver_swg/constants.py` | Physical/unit constants |

### Modified Files

| File | Change |
|------|--------|
| `src/multidriver_swg/__init__.py` | Remove placeholder `hello()` function; re-export key exceptions |
| `tests/test_constants.py` | Basic import and value tests |

---

## Risks and Edge Cases

| Risk | Mitigation |
|------|-----------|
| Exception hierarchy too deep / too shallow | Start minimal; add subclasses as needed in later chunks |
| Constants misclassified (should be config) | Chunk 00 classification is the source of truth; flag ambiguous cases |

---

## Validation Plan

```bash
conda run -n multidriver_swg pytest tests/test_constants.py -v
conda run -n multidriver_swg python -c "from multidriver_swg.exceptions import ConfigurationError; print('OK')"
conda run -n multidriver_swg python -c "from multidriver_swg.constants import mm_per_inch; print(f'OK: {mm_per_inch}')"
```

---

## Definition of Done

- [ ] `src/multidriver_swg/exceptions.py` implemented with full exception hierarchy
- [ ] `src/multidriver_swg/constants.py` implemented with all constants from chunk 00
- [ ] `src/multidriver_swg/__init__.py` cleaned up (placeholder removed)
- [ ] All exceptions preserve context per CONTRIBUTING.md
- [ ] Tests pass
- [ ] **Move this document to `implemented/` once all boxes above are checked**
