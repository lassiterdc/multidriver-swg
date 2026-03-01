# Work Chunk 02D: Stochastic Simulation and Time Series Rescaling

**Phase**: 2D — Core Computation (Simulation & Rescaling)
**Last edited**: 2026-03-01

---

## Before Proceeding

- [`full_codebase_refactor.md`](../full_codebase_refactor.md) — master refactor plan
- [`CONTRIBUTING.md`](../../../../CONTRIBUTING.md) — development philosophy

**Prerequisite**: Work chunks 01F, 02A, 02B, 02C complete.

---

## Task Understanding

### Requirements

1. `src/multidriver_swg/core/simulation.py` — Stochastic event simulation from fitted copulas:
   - Port from `_b6_simulate_from_vincop.py` (769 lines)
   - Poisson process event occurrence simulation
   - Vine copula sampling for correlated event statistics
   - Event-type-specific simulation (rain, surge, combined)

2. `src/multidriver_swg/core/nearest_neighbors.py` — KNN observed event selection:
   - Port `return_df_of_neighbors_w_rank_weighted_slxn_prob()` from `_utils.py` (40 lines)
   - Select nearest observed events to simulated event statistics
   - Rank-weighted random selection among K neighbors

3. `src/multidriver_swg/core/time_series_rescaling.py` — Rescale observed time series:
   - Port from `_b7_generate_time_series_from_event_stats.py` (968 lines) and `_utils.py`
   - `rescale_rain_tseries_to_match_target_depth()` — rescale by adjusting timestep to match depth
   - `rescale_rainfall_timeseries()` — iterative rescaling to match both intensity and depth
   - Surge time series scaling (additive shift to match peak)
   - `generate_randomized_tide_component()` — randomize tidal phase
   - `fill_rain_and_extend_surge_series()` — combine rain + surge time series
   - `reindex_to_buffer_significant_rain_and_surge()` — time series padding

### Key Design Decisions

- **This is the core of the weather generator** — the most complex and performance-critical module
- **All buffer/tolerance parameters explicit** — `timeseries_buffer_around_peaks_h`, `compound_event_time_window_multiplier`, etc. from config
- **No `sys.exit()`** — replace with exceptions; handle "rescaling did not converge" gracefully
- **n_neighbors as a parameter** (currently hardcoded to 7 in old code)
- **Separation**: `simulation.py` generates event statistics; `time_series_rescaling.py` generates the actual time series from those statistics. `nearest_neighbors.py` bridges them.

### Success Criteria

- Simulated event statistics have correct marginal distributions (GOF test)
- Rescaled time series match target depth and intensity within tolerance
- Comparison tests against old code outputs pass
- All parameters explicit (no globals)

---

## Evidence from Codebase

1. `_old_code_to_refactor/_b6_simulate_from_vincop.py` — simulation from copulas (769 lines)
2. `_old_code_to_refactor/_b7_generate_time_series_from_event_stats.py` — time series rescaling (968 lines)
3. `_old_code_to_refactor/_utils.py` — rescaling functions, KNN selection, tide generation, time series extension

---

## File-by-File Change Plan

### New Files

| File | Purpose |
|------|---------|
| `src/multidriver_swg/core/simulation.py` | Stochastic event simulation from copulas |
| `src/multidriver_swg/core/nearest_neighbors.py` | KNN observed event selection |
| `src/multidriver_swg/core/time_series_rescaling.py` | Time series rescaling and generation |
| `tests/test_simulation.py` | Simulation tests |
| `tests/test_time_series_rescaling.py` | Rescaling tests + comparison tests |

---

## Risks and Edge Cases

| Risk | Mitigation |
|------|-----------|
| Rescaling convergence failure (old code has 50-attempt cutoff) | Return convergence status; raise exception if critical; log warning if within tolerance |
| Time series longer than max observed storm duration | Old code breaks loop; new code should handle gracefully with configurable limit |
| KNN selection with very few observed events | Validate n_neighbors ≤ n_observed_events |
| Tidal randomization introduces unrealistic water levels | Preserve the `wlevel_threshold` check from old code |
| Performance — rescaling loop is O(n_events × n_attempts) | Profile; consider parallelization in orchestration layer |

---

## Definition of Done

- [ ] `core/simulation.py` — Poisson event occurrence + copula sampling
- [ ] `core/nearest_neighbors.py` — KNN event selection with rank weighting
- [ ] `core/time_series_rescaling.py` — all rescaling, tide generation, time series extension
- [ ] All buffer/tolerance parameters explicit
- [ ] No `sys.exit()` calls
- [ ] Comparison tests pass against old code
- [ ] Tests pass
- [ ] **Move this document to `implemented/` once all boxes above are checked**
