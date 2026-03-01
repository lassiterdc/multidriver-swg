# Work Chunk 02E: QA/QC and Plotting Extraction

**Phase**: 2E — Core Computation (QA/QC and Plotting)
**Last edited**: 2026-03-01

---

## Before Proceeding

- [`full_codebase_refactor.md`](../full_codebase_refactor.md) — master refactor plan
- [`CONTRIBUTING.md`](../../../../CONTRIBUTING.md) — development philosophy

**Prerequisite**: Work chunks 01F, 02A–02D complete.

---

## Task Understanding

### Requirements

1. `src/multidriver_swg/validation/qaqc.py` — QA/QC of simulated events:
   - Port computational QA/QC from `_b7.1_qaqc_simulated_events.py` (767 lines)
   - Two-sample statistical tests (CVM, K-S, Mann-Whitney) comparing observed vs. simulated
   - Event classification threshold validation
   - Reclassification logic for misclassified events

2. `src/multidriver_swg/plotting/` — Plotting modules extracted from throughout the codebase:
   - `distribution_plots.py` — PDF/CDF/QQ plots from `__ref_ams_functions.py:plot_fitted_distribution()`
   - `event_plots.py` — Event statistics visualization, time series comparison plots
   - `comparison_plots.py` — Observed vs. simulated distribution comparisons, p-value distributions

### Key Design Decisions

- **QA/QC computation is separate from QA/QC plotting** — `qaqc.py` computes test statistics; plotting modules render them
- **All plotting functions accept data, not file paths** — no I/O in plotting modules
- **matplotlib style** — consistent figure style across all plots (consider a shared `plotting/style.py`)
- **`toggle_qaqc_plots`** — plotting is optional; tests always skip plotting

### Success Criteria

- QA/QC statistical tests produce correct p-values
- Plotting functions produce valid matplotlib figures
- No I/O in any plotting module
- QA/QC + plotting separated cleanly

---

## Evidence from Codebase

1. `_old_code_to_refactor/_b7.1_qaqc_simulated_events.py` — QA/QC pipeline (767 lines)
2. `_old_code_to_refactor/__ref_ams_functions.py:plot_fitted_distribution()` — distribution plots
3. `_old_code_to_refactor/_utils.py` — statistical testing and plotting functions throughout

---

## File-by-File Change Plan

### New Files

| File | Purpose |
|------|---------|
| `src/multidriver_swg/validation/qaqc.py` | QA/QC statistical tests |
| `src/multidriver_swg/plotting/__init__.py` | Package init |
| `src/multidriver_swg/plotting/distribution_plots.py` | PDF/CDF/QQ plots |
| `src/multidriver_swg/plotting/event_plots.py` | Event statistics visualization |
| `src/multidriver_swg/plotting/comparison_plots.py` | Observed vs. simulated plots |

---

## Definition of Done

- [ ] `validation/qaqc.py` — statistical tests for simulated event validation
- [ ] `plotting/distribution_plots.py` — distribution fitting plots
- [ ] `plotting/event_plots.py` — event statistics visualization
- [ ] `plotting/comparison_plots.py` — observed vs. simulated comparison plots
- [ ] No I/O in any plotting or QA/QC module
- [ ] Tests pass
- [ ] **Move this document to `implemented/` once all boxes above are checked**
