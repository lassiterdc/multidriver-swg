# Work Chunk 01B: Pydantic Configuration Model and YAML Loader

**Phase**: 1B — Foundation (Config Model)
**Last edited**: 2026-03-01

---

## Before Proceeding

Review the following documents before making any edits to plans or writing any code:

- [`docs/planning/refactors/2026-03-01_full_codebase_refactor/full_codebase_refactor.md`](../full_codebase_refactor.md) — master refactor plan
- [`CONTRIBUTING.md`](../../../../CONTRIBUTING.md) — project development philosophy
- Work chunk 00 — variable classification and YAML schemas (must be complete)

**Prerequisite**: Work chunk 01A must be complete (`multidriver_swg.exceptions` importable).

---

## Task Understanding

### Requirements

1. `src/multidriver_swg/config/model.py` — Pydantic v2 config models:
   - `SystemConfig` — study area parameters (CRS, station IDs, spatial bounds)
   - `PipelineConfig` — full pipeline configuration (thresholds, paths, toggles)
   - Sub-models for each pipeline phase (data acquisition, event selection, copula fitting, etc.)

2. `src/multidriver_swg/config/loader.py` — YAML loading:
   - `load_system_config(yaml_path: Path) -> SystemConfig`
   - `load_pipeline_config(yaml_path: Path) -> PipelineConfig`
   - Template placeholder support (`{{placeholder}}` syntax)
   - System config merge when referenced from pipeline config

3. `src/multidriver_swg/config/defaults.py` — Analysis defaults identified in chunk 00

### Key Design Decisions

- **No defaults for case-study-specific parameters** — station IDs, CRS, thresholds must be explicit
- Config fields identified in chunk 00 classification drive the model hierarchy
- **Human-in-the-loop phases**: Some config fields (distribution selections, copula parameters) will be populated after interactive analysis. The config model should accommodate this — either via optional fields or via separate config files for pre/post-fitting phases.

### Success Criteria

- Minimal valid YAML loads without error
- Missing required fields produce clear error messages
- Norfolk case study YAMLs from chunk 00 load successfully
- All tests pass

---

## Evidence from Codebase

Before implementing, inspect:

1. `_old_code_to_refactor/_inputs.py` — full variable inventory (chunk 00 classification)
2. `src/ss_fha/config/model.py` — reference implementation from ss-fha
3. `src/ss_fha/config/loader.py` — YAML loading pattern
4. `cases/norfolk/` — YAML files from chunk 00

---

## File-by-File Change Plan

### New Files

| File | Purpose |
|------|---------|
| `src/multidriver_swg/config/__init__.py` | Re-export `PipelineConfig`, `load_pipeline_config` |
| `src/multidriver_swg/config/model.py` | Pydantic v2 config models |
| `src/multidriver_swg/config/loader.py` | YAML loading, template filling, system-merge |
| `src/multidriver_swg/config/defaults.py` | Analysis defaults |

### Modified Files

| File | Change |
|------|--------|
| `tests/test_config.py` | Config model and loader tests |

---

## Risks and Edge Cases

| Risk | Mitigation |
|------|-----------|
| Config model complexity grows as phases add fields | Identify all fields upfront in chunk 00; add incrementally |
| Human-in-the-loop fields may not be known at config parse time | Use `Optional` fields with validators that enforce presence when needed |
| Template placeholders clash with YAML syntax | Simple `str.replace()` on raw YAML before parsing |

---

## Validation Plan

```bash
conda run -n multidriver_swg pytest tests/test_config.py -v
# Smoke test: Norfolk YAMLs parse
conda run -n multidriver_swg python -c "
from multidriver_swg.config.loader import load_system_config
cfg = load_system_config('cases/norfolk/system.yaml')
print(f'OK: system.yaml')
"
```

---

## Definition of Done

- [ ] `src/multidriver_swg/config/model.py` implemented with all models
- [ ] `src/multidriver_swg/config/loader.py` implemented
- [ ] `src/multidriver_swg/config/defaults.py` implemented
- [ ] No case-study-specific defaults
- [ ] Norfolk case study YAMLs load without error
- [ ] All tests pass
- [ ] **Move this document to `implemented/` once all boxes above are checked**
