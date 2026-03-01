# Work Chunk 01C: I/O Layer — Data Acquisition

**Phase**: 1C — Foundation (Data Acquisition I/O)
**Last edited**: 2026-03-01

---

## Before Proceeding

- [`full_codebase_refactor.md`](../full_codebase_refactor.md) — master refactor plan
- [`CONTRIBUTING.md`](../../../../CONTRIBUTING.md) — development philosophy

**Prerequisite**: Work chunks 01A, 01B complete.

---

## Task Understanding

### Requirements

1. `src/multidriver_swg/io/noaa.py` — NOAA CO-OPS water level/tide data download:
   - Port from `_a_dwnld_and_process_water-level-data.py`
   - Replace hardcoded station ID with config parameter
   - Replace bare `except: pass` with proper error handling + retry logic
   - Support download of: water levels, tide predictions, hourly heights, high/low tides
   - Timezone handling (source is EST → convert to UTC)

2. `src/multidriver_swg/io/precipitation.py` — Precipitation data I/O:
   - Port AORC S3 download from `_a2_dwnld_aorc_pre_mrms.py`
   - Port MRMS watershed extraction from `_b_gen_wshed-precip-tseries-from-mrms.py`
   - Port NCEI processing from `_c_gen_ncei_hrly_and_daily_data.py`
   - Separate download logic from spatial extraction logic

### Key Design Decisions

- **Pure I/O** — no computation or plotting in these modules
- **Config-driven** — all station IDs, S3 paths, date ranges come from `PipelineConfig`
- **Fail-fast** — download failures raise `DataAcquisitionError` with context
- **Idempotent** — re-running does not re-download existing data (check before download)

### Success Criteria

- NOAA download function works with configurable station ID
- S3 precipitation access works
- All error cases produce `DataAcquisitionError` with actionable messages
- No hardcoded paths or station IDs

---

## Evidence from Codebase

1. `_old_code_to_refactor/_a_dwnld_and_process_water-level-data.py` — NOAA download (487 lines)
2. `_old_code_to_refactor/_a2_dwnld_aorc_pre_mrms.py` — AORC S3 download (94 lines)
3. `_old_code_to_refactor/_b_gen_wshed-precip-tseries-from-mrms.py` — MRMS extraction (333 lines)
4. `_old_code_to_refactor/_c_gen_ncei_hrly_and_daily_data.py` — NCEI processing (83 lines)

---

## File-by-File Change Plan

### New Files

| File | Purpose |
|------|---------|
| `src/multidriver_swg/io/__init__.py` | Package init |
| `src/multidriver_swg/io/noaa.py` | NOAA CO-OPS water level download |
| `src/multidriver_swg/io/precipitation.py` | MRMS/AORC/NCEI precipitation I/O |

### Modified Files

| File | Change |
|------|--------|
| `tests/test_io.py` | I/O layer tests (mocked downloads) |

---

## Risks and Edge Cases

| Risk | Mitigation |
|------|-----------|
| NOAA API rate limits / downtime | Exponential backoff retry; cache downloaded data |
| S3 bucket URL changes | Make bucket URL configurable, not hardcoded |
| Large MRMS datasets overwhelm memory | Dask chunked reads (already used in old code) |
| Timezone confusion (EST/UTC) | All internal storage in UTC; convert on read |

---

## Definition of Done

- [ ] `src/multidriver_swg/io/noaa.py` — NOAA download with configurable station, retry logic
- [ ] `src/multidriver_swg/io/precipitation.py` — MRMS/AORC/NCEI I/O functions
- [ ] All functions use config parameters (no hardcoded station IDs, paths, dates)
- [ ] Error handling uses `DataAcquisitionError` with context
- [ ] Tests pass (mocked API/S3 calls)
- [ ] **Move this document to `implemented/` once all boxes above are checked**
