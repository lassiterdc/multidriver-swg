# Work Chunk 02A: Distribution Fitting Module

**Phase**: 2A — Core Computation (Distribution Fitting)
**Last edited**: 2026-03-01

---

## Before Proceeding

- [`full_codebase_refactor.md`](../full_codebase_refactor.md) — master refactor plan
- [`CONTRIBUTING.md`](../../../../CONTRIBUTING.md) — development philosophy

**Prerequisite**: Work chunks 01A, 01F complete.

---

## Task Understanding

### Requirements

Extract and port the distribution fitting functions from `__ref_ams_functions.py` (387 lines) and the marginal selection logic from `_b4_selecting_marginals.py` into `src/multidriver_swg/core/distributions.py`. Pure computation only — no I/O, no plotting.

**Functions to migrate**:

| Old function/object | New location | Notes |
|---------------------|-------------|-------|
| `fit_dist()` | `fit_distribution()` | Core fitting function — wraps scipy.stats; computes AIC, CVM, KS |
| `boxcox_transformation()` / `inverse_boxcox()` | `boxcox_transform()` / `inverse_boxcox()` | Data transformation utilities |
| `normalize_data()` / `inverse_normalize()` | `normalize()` / `inverse_normalize()` | Standard normalization |
| `transform_data_for_fitting()` / `backtransform_data()` | `transform_for_fitting()` / `backtransform()` | Composite transformation pipeline |
| `transform_data_given_transformations()` | `apply_stored_transforms()` | Apply saved transformation params to new data |
| `comp_msdi()` / `comp_madi()` | `compute_msdi()` / `compute_madi()` | Goodness-of-fit metrics |
| `plot_fitted_distribution()` | → `plotting/distribution_plots.py` | **Separate chunk (02E)** |
| Distribution config dicts (`gev`, `weibull_min_dist`, etc.) | `DISTRIBUTION_CATALOG` | Registry of 13 supported distributions |
| `fxs` list | `get_default_distributions()` | Returns the standard set to try |

**Functions from `_b4_selecting_marginals.py`**:
| Old function | New location | Notes |
|--------------|-------------|-------|
| Marginal selection pipeline logic | `select_best_marginals()` | Automated selection based on GOF criteria |
| Goodness-of-fit filtering | `filter_by_gof()` | Filter distributions by CVM/KS p-value thresholds |

### Key Design Decisions

- **Pure computation** — `fit_distribution()` returns fitted parameters and GOF metrics as a dataclass or NamedTuple, not a pandas Series
- **No hardcoded distribution list** — users can pass any `scipy.stats` distribution
- **GOF thresholds as parameters** — not hardcoded `alpha = 0.05`, `cvm_cutoff = 0.2`
- **Typed return values** — replace the pandas Series return with a `FittedDistribution` dataclass
- All `sys.exit()` calls replaced with proper exceptions

### Success Criteria

- All 13 distributions fit and produce correct AIC/CVM/KS values
- Round-trip test: fit → transform → backtransform recovers original data within tolerance
- Comparison test: new `fit_distribution()` produces same results as old `fit_dist()` for the same inputs
- No plotting in this module

---

## Evidence from Codebase

1. `_old_code_to_refactor/__ref_ams_functions.py` — all 387 lines
2. `_old_code_to_refactor/_b4_selecting_marginals.py` — marginal selection pipeline
3. `_old_code_to_refactor/_utils.py` — any distribution-related utilities

---

## File-by-File Change Plan

### New Files

| File | Purpose |
|------|---------|
| `src/multidriver_swg/core/__init__.py` | Package init |
| `src/multidriver_swg/core/distributions.py` | Distribution fitting, transformation, GOF |
| `tests/test_distributions.py` | Unit tests + comparison tests vs old code |

---

## Risks and Edge Cases

| Risk | Mitigation |
|------|-----------|
| `fit_dist` returns `None, None` for invalid fits — callers must handle | New code raises exception or returns sentinel `FittedDistribution` with `is_valid=False` |
| Box-Cox transformation with edge cases (data ≤ 0) | Preserve the scalar shift logic from old code; add explicit handling |
| scipy.stats distribution API may differ for 2-param vs 3-param | Old code already handles this; preserve the branching logic |

---

## Definition of Done

- [ ] `core/distributions.py` — all fitting and transformation functions ported
- [ ] `FittedDistribution` dataclass with GOF metrics, parameters, transformation info
- [ ] `DISTRIBUTION_CATALOG` with 13 pre-configured distributions
- [ ] Comparison tests pass against old `fit_dist()` outputs
- [ ] Round-trip transform/backtransform tests pass
- [ ] No plotting code in this module
- [ ] No hardcoded GOF thresholds
- [ ] **Move this document to `implemented/` once all boxes above are checked**
