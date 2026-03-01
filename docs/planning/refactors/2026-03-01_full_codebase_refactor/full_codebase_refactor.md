# Full Codebase Refactor: Interactive Scripts to System-Agnostic Library

## Task Understanding

### Requirements

1. Refactor `_old_code_to_refactor/` (~9,000 lines across 15 scripts + 1 utility module) into a system-agnostic Python library (`multidriver_swg`)
2. Use Pydantic models + YAML configs for all user-defined inputs (replacing `_inputs.py`)
3. Separate I/O, computation, and visualization into distinct layers
4. Support workflow orchestration (Snakemake considered but has open questions — see "Human-in-the-Loop" section below)
5. Phased implementation with each phase independently testable
6. When functions are ported, create permanent comparison tests against old implementations
   - Import old functions directly where possible
   - If direct import is not possible, port old functions with `_` prefix and docstring "PORTED FUNCTION FOR TESTING THE REFACTORING"
   - If discrepancies are found, consider the possibility that the old code has an error

### Assumptions

1. Norfolk is the primary (and initially only) case study
2. Inputs are observational data from public sources (NOAA CO-OPS, MRMS, AORC, NCEI) — no upstream repo outputs
3. The primary output is NetCDF time series consumed by TRITON-SWMM_toolkit as boundary conditions
4. Visualization is important but should be decoupled from computation
5. `_d_build_water_level_generator.py` is deprecated and excluded from refactor scope (confirmed: no other scripts reference its functions or outputs)
6. `heatmapz` package excluded; functionality replaced by seaborn/matplotlib

### Success Criteria

- `pip install multidriver-swg` works
- A new user can configure a YAML, point to data sources, and generate stochastic weather time series
- Each phase has passing tests that validate outputs against known-good reference values
- Old scripts are fully superseded and can be archived
- Output NetCDF format remains compatible with TRITON-SWMM_toolkit

---

## Pipeline Overview

The stochastic weather generator pipeline has a natural sequential flow with one critical human-in-the-loop break point:

```
Phase A: Data Acquisition
   ├── Download NOAA water level/tide data (CO-OPS API)
   ├── Download MRMS/AORC precipitation (S3)
   └── Process NCEI climate station data

Phase B: Event Selection & Analysis
   ├── Extract watershed-scale precipitation time series
   ├── Select rainfall and surge events via thresholds
   ├── Classify events (rain-only, surge-only, compound)
   └── Compute event summary statistics

Phase C: Statistical Modeling  ← HUMAN-IN-THE-LOOP
   ├── Select marginal distributions for event statistics
   ├── Fit vine copulas to multivariate event structure
   └── Validate copula fit (requires expert judgment)

Phase D: Stochastic Simulation
   ├── Simulate event occurrences (Poisson process)
   ├── Simulate event statistics from fitted copulas
   └── QA/QC simulated events against observed distributions

Phase E: Time Series Generation
   ├── Rescale observed time series to match simulated event statistics
   ├── Generate rainfall + water level time series for TRITON-SWMM
   └── QA/QC generated time series
```

### Human-in-the-Loop Design Question

Phase C (statistical modeling) requires expert judgment for:
- Selecting which marginal distributions to fit to each event statistic
- Choosing random variables to include in copula models
- Evaluating goodness-of-fit diagnostics and accepting/rejecting fitted models

**Open question**: How to handle this in an orchestrated workflow. Options to evaluate during implementation:
1. **Two-phase Snakemake**: Pre-fitting workflow (A–B) → manual analysis (Jupyter/interactive) → post-fitting workflow (D–E)
2. **Config-driven**: User specifies distribution choices in YAML after exploratory analysis; pipeline reads choices and fits
3. **Hybrid**: Automated exploratory analysis produces candidate fits; user reviews and confirms selections in config

This decision does not block Phase 0–2 implementation. It must be resolved before Phase 3 (orchestration) work chunks.

---

## Evidence from Codebase

### Old Code Structure (`_old_code_to_refactor/`)

| File | Lines | Role | Refactor target |
|------|------:|------|-----------------|
| `_inputs.py` | 202 | Configuration constants, file paths, thresholds | → Pydantic config model |
| `__ref_ams_functions.py` | 387 | Distribution fitting (GEV, Weibull, etc.) | → `core/distributions.py` |
| `_utils.py` | 2,393 | Shared utilities (60+ functions — monolith) | → decompose across multiple modules |
| `_a_dwnld_and_process_water-level-data.py` | 487 | Download NOAA water levels | → `io/noaa.py` |
| `_a2_dwnld_aorc_pre_mrms.py` | 94 | Download AORC precipitation | → `io/precipitation.py` |
| `_b_gen_wshed-precip-tseries-from-mrms.py` | 333 | Extract MRMS at watershed | → `io/precipitation.py` |
| `_b2_event_selection.py` | 616 | Select events from time series | → `core/event_selection.py` |
| `_b3_event_analysis.py` | 668 | Analyze & classify events | → `core/event_analysis.py` |
| `_b4_selecting_marginals.py` | 316 | Select marginal distributions | → `core/distributions.py` + interactive |
| `_b5_fit_vine_copulas.py` | 327 | Fit vine copulas | → `core/copula_fitting.py` |
| `_b6_simulate_from_vincop.py` | 769 | Simulate from copulas | → `core/simulation.py` |
| `_b7_generate_time_series_from_event_stats.py` | 968 | Rescale time series | → `core/time_series_rescaling.py` |
| `_b7.1_qaqc_simulated_events.py` | 767 | QA/QC simulated events | → `validation/` |
| `_c_gen_ncei_hrly_and_daily_data.py` | 83 | Process NCEI data | → `io/ncei.py` |
| `_d_build_water_level_generator.py` | 601 | Gaussian copula (DEPRECATED) | → excluded |

### Key Observations

- **Hardcoded paths everywhere**: `_inputs.py` builds all paths from `os.getcwd().parents[1]` — completely machine-dependent
- **`from _inputs import *` / `from _utils import *`** in every script — star imports make dependency tracing difficult
- **`sys.exit()` for error handling** (15+ instances) — must be replaced with exceptions
- **I/O, computation, and plotting tangled** in every major function
- **No tests, no docstrings, no type hints**
- **13 pre-configured distribution families** in `__ref_ams_functions.py` — wrapping scipy.stats

### Inter-repo Data Flow

```
[NOAA CO-OPS API] ──(water level CSV)──────────┐
[NOAA MRMS / AORC S3] ──(precip zarr)──────────┤
[NCEI climate data] ──(CSV)─────────────────────┤
                                                ▼
                                [multidriver-swg]
                                                │
                     (NetCDF time series: rainfall + water levels)
                                                ▼
                              [TRITON-SWMM_toolkit]
                          (coupled hydrodynamic simulation)
```

**Interface contract**: The generated NetCDF time series must be compatible with TRITON-SWMM_toolkit's boundary condition reader. The exact schema (dimensions, variable names, units, time indexing) must be documented and preserved.

---

## Implementation Strategy

### Chosen Approach

Bottom-up refactoring following the ss-fha pattern:

1. **Phase 0**: Data/input inventory — create case study YAML, establish naming conventions, document data sources
2. **Phase 1**: Foundation — config model, constants, exceptions, I/O layer, test infrastructure
3. **Phase 2**: Core computation — pure functions extracted from `_utils.py` and `__ref_ams_functions.py`
4. **Phase 3**: Orchestration — runner scripts, workflow coordination, CLI
5. **Phase 4**: Validation & cleanup — case study validation, config field cleanup, archive old code

### Alternatives Considered

- **Top-down (orchestration first)**: Rejected — creates brittle wiring before computation is solid
- **Script-by-script migration**: Rejected — scripts have heavy cross-dependencies via `_utils.py`; decomposing `_utils.py` first is essential
- **Rewrite from scratch**: Rejected — too risky; comparison testing against old code is critical for a stochastic system

---

## Target Architecture

```
src/multidriver_swg/
├── __init__.py
├── constants.py                          # Module-level constants (thresholds, defaults)
├── exceptions.py                         # Custom exception hierarchy
├── config/
│   ├── __init__.py
│   ├── model.py                          # Pydantic v2 config models
│   ├── loader.py                         # YAML loading, template filling
│   └── defaults.py                       # Analysis defaults
├── io/
│   ├── __init__.py
│   ├── noaa.py                           # NOAA CO-OPS water level download
│   ├── precipitation.py                  # MRMS/AORC/NCEI precipitation I/O
│   ├── event_io.py                       # Event summary/time series CSV/NetCDF I/O
│   └── output.py                         # Output NetCDF writer (TRITON-SWMM compat)
├── core/
│   ├── __init__.py
│   ├── event_selection.py                # Threshold-based event identification
│   ├── event_analysis.py                 # Event classification, summary statistics
│   ├── distributions.py                  # Distribution fitting (from __ref_ams_functions)
│   ├── copula_fitting.py                 # Vine copula fitting (pyvinecopulib)
│   ├── simulation.py                     # Stochastic event simulation from copulas
│   ├── time_series_rescaling.py          # Observed → simulated time series rescaling
│   └── nearest_neighbors.py             # KNN event selection (from _utils.py)
├── validation/
│   ├── __init__.py
│   ├── input_validation.py               # Data file existence, schema checks
│   └── qaqc.py                           # QA/QC of simulated events
├── plotting/
│   ├── __init__.py
│   ├── distribution_plots.py             # PDF/CDF/QQ plots for distribution fitting
│   ├── event_plots.py                    # Event statistics visualization
│   └── comparison_plots.py              # Observed vs. simulated comparison plots
└── runners/                              # Orchestration layer (TBD — Snakemake or scripts)
    └── __init__.py

cases/
└── norfolk/
    ├── README.md
    ├── system.yaml                       # Study area config (CRS, station IDs, etc.)
    └── pipeline_config.yaml              # Pipeline config (thresholds, toggles, paths)

tests/
├── conftest.py
├── test_config.py
├── test_event_selection.py
├── test_distributions.py
├── test_copula_fitting.py
├── test_simulation.py
├── test_time_series_rescaling.py
└── test_io.py
```

---

## Tracking Table

| Old file | New location(s) | Status | Phase |
|----------|----------------|--------|-------|
| `_inputs.py` | `config/model.py`, `config/defaults.py`, `constants.py` | Pending | 0, 1A, 1B |
| `__ref_ams_functions.py` | `core/distributions.py` | Pending | 2A |
| `_utils.py` (event selection funcs) | `core/event_selection.py` | Pending | 2B |
| `_utils.py` (rescaling funcs) | `core/time_series_rescaling.py` | Pending | 2D |
| `_utils.py` (KNN funcs) | `core/nearest_neighbors.py` | Pending | 2D |
| `_utils.py` (plotting funcs) | `plotting/` | Pending | 2E |
| `_utils.py` (I/O funcs) | `io/` | Pending | 1D |
| `_a_dwnld_and_process_water-level-data.py` | `io/noaa.py` | Pending | 1D |
| `_a2_dwnld_aorc_pre_mrms.py` | `io/precipitation.py` | Pending | 1D |
| `_b_gen_wshed-precip-tseries-from-mrms.py` | `io/precipitation.py` | Pending | 1D |
| `_b2_event_selection.py` | `core/event_selection.py` | Pending | 2B |
| `_b3_event_analysis.py` | `core/event_analysis.py` | Pending | 2C |
| `_b4_selecting_marginals.py` | `core/distributions.py` + interactive | Pending | 2A |
| `_b5_fit_vine_copulas.py` | `core/copula_fitting.py` | Pending | 2C |
| `_b6_simulate_from_vincop.py` | `core/simulation.py` | Pending | 2D |
| `_b7_generate_time_series_from_event_stats.py` | `core/time_series_rescaling.py` | Pending | 2D |
| `_b7.1_qaqc_simulated_events.py` | `validation/qaqc.py` | Pending | 2E |
| `_c_gen_ncei_hrly_and_daily_data.py` | `io/precipitation.py` | Pending | 1D |
| `_d_build_water_level_generator.py` | **EXCLUDED** (deprecated) | N/A | N/A |

---

## Phased Implementation Plan

### Phase 0: Data & Input Inventory (no code)
| Chunk | Description | Prerequisites |
|-------|-------------|---------------|
| 00 | Case study YAML setup — inventory all data sources, create config templates, make naming/terminology decisions | None |

### Phase 1: Foundation (config, I/O, test infrastructure)
| Chunk | Description | Prerequisites |
|-------|-------------|---------------|
| 01A | Exceptions and constants | None |
| 01B | Pydantic config model and YAML loader | 01A |
| 01C | I/O layer — data acquisition (NOAA, MRMS/AORC, NCEI) | 01A, 01B |
| 01D | I/O layer — event data and output NetCDF | 01A, 01B |
| 01E | Input validation layer | 01B, 01C, 01D |
| 01F | Test infrastructure and fixtures | 01A–01E |

### Phase 2: Core Computation (pure functions)
| Chunk | Description | Prerequisites |
|-------|-------------|---------------|
| 02A | Distribution fitting (`__ref_ams_functions.py` → `core/distributions.py`) | 01A, 01F |
| 02B | Event selection and classification | 01F, 02A |
| 02C | Event analysis and copula fitting | 01F, 02A, 02B |
| 02D | Stochastic simulation and time series rescaling | 01F, 02A, 02B, 02C |
| 02E | QA/QC and plotting extraction | 01F, 02A–02D |

### Phase 3: Orchestration (TBD — depends on human-in-the-loop decision)
| Chunk | Description | Prerequisites |
|-------|-------------|---------------|
| 03A | Runner scripts / workflow orchestration | 02A–02E |
| 03B | CLI entry point | 03A |

### Phase 4: Validation & Cleanup
| Chunk | Description | Prerequisites |
|-------|-------------|---------------|
| 04A | Case study validation (end-to-end Norfolk run) | 03A, 03B |
| 04B | Config field cleanup and architecture.md update | 04A |

---

## Cross-Cutting Decisions

Decisions that affect multiple work chunks are tracked here. Each entry links to the chunks affected and the resolution.

### Decision 1: "Combined" vs. "Compound" terminology — RESOLVED

**Status**: Resolved 2026-03-01

The old code uses "compound" in variable names and file names (e.g., `dir_compound_events`, `f_combined_event_summaries`). The ss-fha refactor resolved this as:
- "combined" = simulation type (both drivers present)
- "compound" = phenomenon (flooding worsened by multiple drivers)

**Affected chunks**: 00, 01B, 02B, 02C
**Recommendation**: Adopt the same convention as ss-fha for consistency across the ecosystem.

### Decision 2: Output NetCDF schema for TRITON-SWMM compatibility

**Status**: Open — to be resolved during chunk 00 (TRITON-SWMM_toolkit source investigation)

The generated time series must be compatible with TRITON-SWMM_toolkit's boundary condition reader. The exact dimensions, variable names, units, and time indexing must be documented.

**Affected chunks**: 01D, 02D, 04A
**Action**: Inspect TRITON-SWMM_toolkit's input reader to determine the expected schema.

### Decision 3: Human-in-the-loop workflow design

**Status**: Open — deferred to Phase 3

See "Human-in-the-Loop Design Question" above.

**Affected chunks**: 03A, 03B

### Decision 4: Event type naming convention

**Status**: Partially resolved — "combined" adopted per Decision 1; canonical strings ("rain", "surge", "combined") to be confirmed in chunk 00

The old code classifies events as "rain", "surge", and "compound" (or "combined"). Need to establish canonical string identifiers used in config, data structures, and file names.

**Affected chunks**: 00, 01B, 02B, 02C, 02D

### Decision 5: Config field inventory from `_inputs.py`

**Status**: Pending — produced by chunk 00

All 200+ lines of `_inputs.py` must be classified as:
- Case-study-specific → YAML config
- Analysis default → `config/defaults.py`
- True constant → `constants.py`
- Deprecated/unused → removed

**Affected chunks**: 00, 01A, 01B
