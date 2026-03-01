# Work Chunk 02C: Event Analysis and Copula Fitting

**Phase**: 2C — Core Computation (Event Analysis & Copula)
**Last edited**: 2026-03-01

---

## Before Proceeding

- [`full_codebase_refactor.md`](../full_codebase_refactor.md) — master refactor plan
- [`CONTRIBUTING.md`](../../../../CONTRIBUTING.md) — development philosophy

**Prerequisite**: Work chunks 01F, 02A, 02B complete.

---

## Task Understanding

### Requirements

1. `src/multidriver_swg/core/event_analysis.py` — Event analysis post-selection:
   - Combine event data across multiple sources with priority logic (prefer MRMS, fall back to AORC) — from `_b3_event_analysis.py`
   - Compute event summary statistics across multiple time windows
   - Dataset merging and deduplication

2. `src/multidriver_swg/core/copula_fitting.py` — Vine copula fitting:
   - Port from `_b5_fit_vine_copulas.py` (327 lines)
   - Vine copula fitting with `pyvinecopulib`
   - Copula goodness-of-fit testing (Kendall's tau, CVM)
   - Monte Carlo simulation for fit validation
   - Save/load copula parameters (JSON)

### Key Design Decisions

- **Pure computation** — copula fitting receives DataFrames, returns fitted copula objects + GOF metrics
- **Copula parameters serializable** — save to JSON for reproducibility (already done in old code)
- **Human-in-the-loop interface** — the copula fitting module should support: (a) automated fitting with GOF filtering, and (b) accepting user-specified copula parameters from config
- **Multiple event types** — fit separate copulas for each event type ("rain", "surge", "combined")

### Success Criteria

- Vine copula fitting produces valid copula objects
- GOF metrics (Kendall's tau, CVM) computed correctly
- Copula parameters round-trip through JSON
- Comparison tests pass

---

## Evidence from Codebase

1. `_old_code_to_refactor/_b3_event_analysis.py` — event combination and analysis (668 lines)
2. `_old_code_to_refactor/_b5_fit_vine_copulas.py` — vine copula fitting (327 lines)
3. `_old_code_to_refactor/_utils.py` — copula evaluation utilities

---

## File-by-File Change Plan

### New Files

| File | Purpose |
|------|---------|
| `src/multidriver_swg/core/event_analysis.py` | Event data combination and analysis |
| `src/multidriver_swg/core/copula_fitting.py` | Vine copula fitting and evaluation |
| `tests/test_copula_fitting.py` | Copula fitting tests |

---

## Definition of Done

- [ ] `core/event_analysis.py` — event combination, statistics, merging
- [ ] `core/copula_fitting.py` — vine copula fitting, GOF, serialization
- [ ] Copula parameters serialize/deserialize via JSON
- [ ] Tests pass with synthetic data
- [ ] No I/O or plotting in core modules
- [ ] **Move this document to `implemented/` once all boxes above are checked**
