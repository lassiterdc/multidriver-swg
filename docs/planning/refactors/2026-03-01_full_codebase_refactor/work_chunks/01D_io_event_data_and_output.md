# Work Chunk 01D: I/O Layer — Event Data and Output NetCDF

**Phase**: 1D — Foundation (Event Data and Output I/O)
**Last edited**: 2026-03-01

---

## Before Proceeding

- [`full_codebase_refactor.md`](../full_codebase_refactor.md) — master refactor plan
- [`CONTRIBUTING.md`](../../../../CONTRIBUTING.md) — development philosophy

**Prerequisite**: Work chunks 01A, 01B complete.

---

## Task Understanding

### Requirements

1. `src/multidriver_swg/io/event_io.py` — Event summary and time series I/O:
   - Read/write event summary CSVs
   - Read/write event time series (CSV and NetCDF)
   - Handle event index mapping files
   - Zarr compression utilities (from `_utils.py:define_zarr_compression`)

2. `src/multidriver_swg/io/output.py` — Output NetCDF writer:
   - Write generated time series in the format TRITON-SWMM expects
   - Schema documented in chunk 00 (output NetCDF schema investigation)
   - SWMM .dat format file generation (rainfall forcing files)

### Key Design Decisions

- **Output format is an interface contract** — the NetCDF schema must be compatible with TRITON-SWMM_toolkit
- **Pure I/O** — no computation; reads return DataFrames/Datasets, writes accept them
- **Zarr compression** — use zstd with configurable compression level

### Success Criteria

- Event data round-trips correctly (write → read → compare)
- Output NetCDF matches TRITON-SWMM expected schema
- Zarr compression works with configurable level

---

## Evidence from Codebase

1. `_old_code_to_refactor/_utils.py:define_zarr_compression()` — zarr encoding
2. `_old_code_to_refactor/_b3_event_analysis.py` — event summary/time series NetCDF writing
3. `_old_code_to_refactor/_b7_generate_time_series_from_event_stats.py` — output time series format
4. TRITON-SWMM_toolkit source — boundary condition reader (for output schema)

---

## File-by-File Change Plan

### New Files

| File | Purpose |
|------|---------|
| `src/multidriver_swg/io/event_io.py` | Event summary and time series read/write |
| `src/multidriver_swg/io/output.py` | Output NetCDF writer (TRITON-SWMM compatible) |

### Modified Files

| File | Change |
|------|--------|
| `tests/test_io.py` | Add event I/O and output tests |

---

## Definition of Done

- [ ] `src/multidriver_swg/io/event_io.py` — event summary and time series I/O
- [ ] `src/multidriver_swg/io/output.py` — TRITON-SWMM compatible NetCDF output
- [ ] Output schema matches TRITON-SWMM expectations (documented and tested)
- [ ] Zarr compression utility extracted
- [ ] Tests pass
- [ ] **Move this document to `implemented/` once all boxes above are checked**
