# Work Chunk 02B: Event Selection and Classification

**Phase**: 2B — Core Computation (Event Selection)
**Last edited**: 2026-03-01

---

## Before Proceeding

- [`full_codebase_refactor.md`](../full_codebase_refactor.md) — master refactor plan
- [`CONTRIBUTING.md`](../../../../CONTRIBUTING.md) — development philosophy

**Prerequisite**: Work chunks 01F, 02A complete.

---

## Task Understanding

### Requirements

Extract event selection and classification logic from `_utils.py` and `_b2_event_selection.py` into pure computation modules.

1. `src/multidriver_swg/core/event_selection.py` — Event identification from continuous time series:
   - `event_selection_threshold_or_nstorms()` from `_utils.py` (152 lines) — threshold-based event identification from rainfall and water level time series
   - `surge_event_selection()` from `_utils.py` (117 lines) — surge-specific event detection
   - `compute_event_timeseries_statistics()` from `__ref_ams_functions` or `_utils.py` — compute summary statistics (max intensities over windows, depths, durations) for selected events
   - `classify_events()` — classify events as "rain", "surge", or "combined" based on thresholds

### Key Design Decisions

- **Pure computation** — functions receive time series DataFrames and threshold parameters, return event DataFrames
- **All thresholds as parameters** — `rain_event_threshold_mm`, `surge_event_threshold_ft`, `min_interevent_time`, `max_event_duration_h` come from config
- **Event type terminology** — "rain", "surge", "combined" (not "compound" — Decision 1)
- **No globals** — replace all `from _inputs import *` dependencies with explicit parameters

### Success Criteria

- Event selection produces same events as old code for the same inputs and thresholds
- Classification assigns correct event types
- All threshold parameters are explicit (no globals)
- Comparison tests pass

---

## Evidence from Codebase

1. `_old_code_to_refactor/_utils.py` — `event_selection_threshold_or_nstorms()` (~line 1271), `surge_event_selection()` (~line 1424)
2. `_old_code_to_refactor/_b2_event_selection.py` — event selection pipeline (616 lines)
3. `_old_code_to_refactor/_b3_event_analysis.py` — event classification and combination (668 lines)

---

## File-by-File Change Plan

### New Files

| File | Purpose |
|------|---------|
| `src/multidriver_swg/core/event_selection.py` | Event identification and classification |
| `tests/test_event_selection.py` | Unit tests + comparison tests |

---

## Definition of Done

- [ ] `core/event_selection.py` — event selection and classification functions
- [ ] All thresholds as explicit parameters (no globals)
- [ ] Event types use "rain"/"surge"/"combined" terminology
- [ ] Comparison tests pass against old code outputs
- [ ] No I/O or plotting code
- [ ] **Move this document to `implemented/` once all boxes above are checked**
