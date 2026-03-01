# Work Chunk 00: Data & Input Inventory

**Phase**: 0 — Pre-Implementation (Config and Data Inventory)
**Last edited**: 2026-03-01

---

## Before Proceeding

Review the following documents before making any edits to plans or writing any code:

- [`docs/planning/refactors/2026-03-01_full_codebase_refactor/full_codebase_refactor.md`](../full_codebase_refactor.md) — master refactor plan; update it if any decisions made here affect the overall plan.
- [`CONTRIBUTING.md`](../../../../CONTRIBUTING.md) — project development philosophy; all implementation decisions must align with it.

**Prerequisite**: None — this is the first task and has no code dependencies.

---

## Purpose

Creating the case study YAML files and data inventory early is a forcing function. It requires making concrete decisions about:

- Which parameters are case-study-specific vs. analysis-method defaults vs. constants
- Which data sources exist, how they are accessed, and what formats they produce
- How the config structure works in practice before any code commits to it
- What the downstream interface contract is (NetCDF schema for TRITON-SWMM)

**No code is written in this chunk.** The deliverables are YAML files, a data inventory, and documentation only.

---

## Task Understanding

### Requirements

1. **Classify every variable in `_inputs.py`** into one of:
   - **Case-study-specific** → goes in `cases/norfolk/system.yaml` or `pipeline_config.yaml`
   - **Analysis default** → goes in `config/defaults.py` with a comment explaining the default
   - **True constant** → goes in `constants.py` (e.g., `mm_per_inch = 25.4`)
   - **Derived/computed** → removed; computed at runtime
   - **Deprecated/unused** → removed (with justification)

2. **`cases/norfolk/system.yaml`** — Norfolk-specific geographic and station parameters:
   - NOAA station ID(s)
   - CRS (coordinate reference system)
   - Watershed/AOI definition
   - Any other site-specific scalars

3. **`cases/norfolk/pipeline_config.yaml`** — Pipeline execution config:
   - Data source paths or download parameters
   - Event selection thresholds
   - Analysis toggles and parameters
   - Output directory configuration

4. **Data source inventory** — Document every external data source:
   - Source (API endpoint, S3 bucket, local file)
   - Format (CSV, NetCDF, zarr, shapefile)
   - How it is currently accessed (download script, manual download, local path)
   - Whether it should be auto-downloaded or user-provided

5. **Output format documentation** — Document the NetCDF time series schema that TRITON-SWMM expects:
   - Inspect TRITON-SWMM_toolkit source for boundary condition reader
   - Document dimensions, variable names, units, time indexing
   - Create a reference schema in the case study README

6. **Naming convention decisions** — Establish canonical names for:
   - Event types: "rain", "surge", "combined" (not "compound" — see Decision 1)
   - Time series variables
   - File naming patterns

### Key Design Decisions from Master Plan

- **"Combined" vs. "compound"** — RESOLVED: "combined" for simulation type, "compound" for phenomenon (Decision 1)
- **No defaults for case-study-specific parameters** — station IDs, CRS, thresholds must be explicit
- **Config fields identified here will be implemented incrementally** in 01A/01B

### Success Criteria

- Every variable in `_inputs.py` is classified with rationale
- Case study YAMLs parse without error via `yaml.safe_load()`
- Data source inventory is complete with access method and format
- Output NetCDF schema for TRITON-SWMM is documented
- All naming conventions are established

---

## Evidence from Codebase

Before implementing, inspect:

1. `_old_code_to_refactor/_inputs.py` — full variable inventory (202 lines)
2. `_old_code_to_refactor/_a_dwnld_and_process_water-level-data.py` — NOAA API parameters
3. `_old_code_to_refactor/_a2_dwnld_aorc_pre_mrms.py` — S3 bucket paths
4. `_old_code_to_refactor/_b_gen_wshed-precip-tseries-from-mrms.py` — MRMS data access
5. `_old_code_to_refactor/_b7_generate_time_series_from_event_stats.py` — output time series format
6. TRITON-SWMM_toolkit source — boundary condition reader (inspect for expected NetCDF schema)

---

## File-by-File Change Plan

### New Files

| File | Purpose |
|------|---------|
| `cases/norfolk/README.md` | Directory purpose, data source inventory, YAML inventory, decisions made |
| `cases/norfolk/system.yaml` | Norfolk-specific geographic/station parameters |
| `cases/norfolk/pipeline_config.yaml` | Pipeline execution config with local paths |

### Modified Files

| File | Change |
|------|--------|
| `docs/planning/refactors/2026-03-01_full_codebase_refactor/full_codebase_refactor.md` | Update Phase 0 section, resolve cross-cutting decisions |
| `docs/planning/refactors/2026-03-01_full_codebase_refactor/work_chunks/README.md` | Update status |
| `CLAUDE.md` | Add Terminology section |

---

## Risks and Edge Cases

| Risk | Mitigation |
|------|-----------|
| YAML schema changes in 01B invalidate these YAMLs | Header comment on each YAML; 01B DoD includes loading them as smoke test |
| TRITON-SWMM output format undocumented | Inspect TRITON-SWMM_toolkit source directly; document findings |
| Some `_inputs.py` variables may be ambiguous (case-study vs. default) | Flag ambiguous cases; resolve during review |
| `_inputs.py` variables are used via `from _inputs import *` — hard to trace usage | Search every old script for each variable name |

---

## Validation

No automated tests. Human review:

- [ ] Every `_inputs.py` variable is classified with rationale
- [ ] Each YAML passes `python -c "import yaml; yaml.safe_load(open('...'))"` without error
- [ ] Each YAML has a provisional-schema header comment
- [ ] Data source inventory is complete
- [ ] Output NetCDF schema documented
- [ ] All naming conventions established
- [ ] Terminology section added to CLAUDE.md

---

## Definition of Done

- [ ] `cases/norfolk/README.md` created with data source inventory
- [ ] `cases/norfolk/system.yaml` created
- [ ] `cases/norfolk/pipeline_config.yaml` created
- [ ] Complete classification of all `_inputs.py` variables (documented in this file or README)
- [ ] Output NetCDF schema for TRITON-SWMM documented
- [ ] Naming conventions established (event types, variables, files)
- [ ] Cross-cutting decisions updated in master plan
- [ ] CLAUDE.md Terminology section added
- [ ] **Move this document to `implemented/` once all boxes above are checked**
