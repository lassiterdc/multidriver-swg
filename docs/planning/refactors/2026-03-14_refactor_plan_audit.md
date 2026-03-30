---
title: "Refactor Plan Audit: Full Codebase Refactor"
date: 2026-03-14
author: software-engineering-specialist
status: in-progress
target_plan: docs/planning/refactors/2026-03-01_full_codebase_refactor/full_codebase_refactor.md
---

# Refactor Plan Audit

**Target**: `docs/planning/refactors/2026-03-01_full_codebase_refactor/full_codebase_refactor.md` and its 16 work chunks.

**Context**: The refactor has NOT started. No code has been written. The plan is fully modifiable — structural changes, phase reordering, scope adjustments, and architectural overhauls are all on the table.

**Goal**: Evaluate the plan against software engineering best practices and produce actionable recommendations that strengthen the refactor before any implementation begins.

---

## Audit Findings

### 0. Scope and Ambition

**Assessment**: The scale is appropriate for this codebase. A 9,000-line codebase with zero tests, star imports everywhere, `sys.exit()` for error handling, and fully tangled I/O/computation/plotting is not amenable to incremental patching -- the cross-dependencies through `_utils.py` and `from _inputs import *` in every file mean you cannot test or refactor one script without touching the others. The bottom-up approach (decompose `_utils.py` and `_inputs.py` first, then build modules on top) is the correct strategy.

An incremental approach (add tests to old code first, then refactor) is infeasible here because the old code cannot be tested without substantial modification -- the scripts rely on `os.getcwd().parents[1]` for all paths, use `sys.exit()` where exceptions should be raised, and star-import from both `_inputs` and `_utils`, meaning you cannot import any script without triggering side effects and path resolution failures. The plan correctly identifies this by choosing bottom-up extraction over script-by-script migration.

1. `[SUGGESTION]` **Drop chunk 01E (Input Validation Layer) or merge it into 01B.** Pydantic v2 validators already handle structural config validation, and file-existence checks are better placed in the runner layer (03A) rather than in a standalone validation module during Phase 1. At this stage, 01E creates a module with no consumers -- every function it could provide would be called by runners that do not exist until Phase 3. Move the `validate_inputs()` idea into 03A's scope.

2. `[SUGGESTION]` **Consider merging 04A and 04B into a single Phase 4 chunk.** These are both small and sequential. Splitting them adds overhead without reducing risk.

### 1. Architectural Soundness

**Assessment**: The plan has a sound package decomposition and correctly separates I/O, computation, and plotting. Its primary architectural gap is the absence of domain model objects -- the plan treats everything as DataFrames passed between functions, which will make the codebase harder to reason about as it grows.

**Domain Modeling**

The plan mentions a `FittedDistribution` dataclass in chunk 02A but does not establish a domain model layer. The following domain objects are missing from the plan and should be defined in a `src/multidriver_swg/models.py` (or `core/models.py`):

- `Event` -- an identified event with start/end times, event type ("rain"/"surge"/"combined"), and source metadata. Currently, events are rows in DataFrames with implicit column contracts.
- `EventSummaryStatistics` -- the computed statistics for an event (depth, peak intensity over windows, peak surge, lag). The old code builds these as DataFrame rows; a dataclass would make the contract explicit.
- `FittedDistribution` -- already mentioned in 02A; should be defined centrally.
- `CopulaModel` -- wraps a `pyvinecopulib.Vinecop` with metadata (which event type, which variables, GOF metrics, source data hash). Currently, copulas are loaded from JSON with no provenance tracking.
- `SimulatedEvent` -- a simulated event with its statistics and the ID of the observed event used for rescaling.
- `RescaledTimeSeries` -- the final rescaled time series with metadata about convergence (number of iterations, residual error).

These do not require a heavyweight ORM. They are dataclasses (or Pydantic models) that make function signatures unambiguous and enable type checking.

**Error Handling**

The plan defines a custom exception hierarchy in 01A (`ConfigurationError`, `DataAcquisitionError`, `ValidationError`, `EventSelectionError`) which is appropriate. However, the plan does not state an error-handling contract for core computation functions. The old code uses `sys.exit()` for both "this should never happen" assertions and "the rescaling did not converge" recoverable failures. The plan must distinguish between:
- Programming errors (assertions) -- use `assert` or raise `RuntimeError`
- Recoverable failures (e.g., rescaling did not converge within tolerance) -- return a result object with a status field, or raise a specific exception that the runner layer catches
- Data quality issues (e.g., missing values, short records) -- log a warning and continue, or raise `ValidationError`

3. `[BLOCKING]` **Define domain model objects before Phase 2 implementation begins.** Add a `core/models.py` to chunk 01A or create a new chunk 01A.5. Every Phase 2 chunk depends on these types for function signatures. Without them, each chunk will invent its own ad-hoc DataFrame column conventions, and integration will be painful. At minimum: `Event`, `FittedDistribution`, `CopulaModel`, `SimulatedEvent`.

4. `[IMPORTANT]` **Establish an explicit error-handling contract in the master plan.** State that core computation functions raise domain-specific exceptions for unrecoverable errors, return result objects with status fields for recoverable failures (like rescaling non-convergence), and that runners are responsible for catching and logging. This applies across all Phase 2 chunks.

**Package Structure**

5. `[IMPORTANT]` **Split `io/precipitation.py` into at least two modules.** The plan maps three old scripts (`_a2_dwnld_aorc_pre_mrms.py`, `_b_gen_wshed-precip-tseries-from-mrms.py`, `_c_gen_ncei_hrly_and_daily_data.py`) plus parts of `_utils.py` into a single `precipitation.py`. These cover S3 download, spatial extraction from gridded data, NCEI CSV processing, and time series resampling. A single file will become another monolith. Suggested split: `io/aorc.py` (S3 download and spatial extraction), `io/mrms.py` (MRMS processing), `io/ncei.py` (NCEI CSV processing). The master plan's tracking table already lists `io/ncei.py` as a target; the work chunk 01C contradicts this by putting everything in `precipitation.py`.

6. `[SUGGESTION]` **Rename `core/nearest_neighbors.py` to `core/event_matching.py`.** The module does more than KNN -- it bridges simulated statistics to observed events for rescaling. The name should reflect the domain concept, not the algorithm.

**I/O / Computation / Plotting Separation**

The separation holds up well for most of the pipeline. Two places where it will break down:

- `_b7_generate_time_series_from_event_stats.py` (mapped to `core/time_series_rescaling.py`) reads observed time series from xarray Datasets mid-computation (e.g., `ds_6min_water_levels_mrms_res.sel(...)` inside `generate_randomized_tide_component` and `fill_rain_and_extend_surge_series`). The plan must ensure that these I/O calls are lifted into the runner, with the relevant data slices passed into the pure functions.
- `_b3_event_analysis.py` / `_utils.py:determine_combined_event_statistics` reads raw water level and rainfall time series to compute statistics for combined events. This computation requires the full continuous time series, not just event slices. The runner must pre-load these and pass them in.

7. `[IMPORTANT]` **Explicitly document in chunks 02C and 02D which data must be pre-loaded by the runner and passed into core functions.** The old code reads files mid-computation; the plan says "pure computation" but does not specify the function signatures that make this possible. Each Phase 2 chunk should include a "function signatures" section listing inputs and outputs.

**Config Design**

The Pydantic + YAML approach is appropriate for this pipeline. The two-YAML split (`system.yaml` for site parameters, `pipeline_config.yaml` for analysis parameters) is sound.

8. `[SUGGESTION]` **Consider making `pipeline_config.yaml` reference `system.yaml` via an `include` or `system_config_path` field** rather than requiring two separate `load_*` calls. This reduces the chance of mismatched configs.

### 2. Phasing and Dependency Order

**Assessment**: The bottom-up ordering is correct and the dependency graph is mostly sound. There are two dependency issues and one chunk sizing problem.

**Dependency Graph Issues**

The master plan lists 01C and 01D as separate I/O chunks with identical prerequisites (01A, 01B). However, the plan also lists 01D as requiring 01C in the Phase 1 table ("01D: I/O layer -- event data and output NetCDF | Prerequisites: 01A, 01B"). When I cross-reference with the 01D work chunk document, it lists prerequisites as only "01A, 01B" -- not 01C. This is consistent: 01C and 01D are independent I/O chunks that can be implemented in parallel. The master plan's Phase 1 table is misleading because it visually suggests serial ordering. This is not wrong, but should be clarified.

The dependency `02B depends on 02A` is unnecessary. Event selection (`02B`) identifies events from continuous time series using thresholds and rolling sums. It does not use distribution fitting (`02A`). The old code confirms this: `_b2_event_selection.py` imports from `_inputs` and `_utils` but not from `__ref_ams_functions.py`. Remove this dependency -- 02B should depend only on 01F (and transitively on 01A for exceptions/constants).

9. `[IMPORTANT]` **Remove the dependency of 02B on 02A.** Event selection does not require distribution fitting. These chunks can be implemented in parallel, which shortens the critical path. Update the master plan Phase 2 table and the 02B work chunk prerequisites.

10. `[SUGGESTION]` **Clarify in the master plan Phase 1 table that 01C and 01D are parallelizable.** They share prerequisites (01A, 01B) but do not depend on each other.

**Chunk Sizing**

Chunk 02D (Simulation and Rescaling) is too large. It covers three modules (`simulation.py`, `nearest_neighbors.py`, `time_series_rescaling.py`), ports from two major old scripts (`_b6_simulate_from_vincop.py` at 769 lines and `_b7_generate_time_series_from_event_stats.py` at 968 lines), and includes the most complex algorithmic logic in the entire codebase (iterative rescaling with convergence checks, tidal phase randomization, time series padding). This is roughly 1,700 lines of old code to port, compared to ~300-600 lines for other Phase 2 chunks.

11. `[IMPORTANT]` **Split chunk 02D into two chunks: 02D-sim (Poisson simulation + copula sampling + KNN matching) and 02D-rescale (time series rescaling + tide randomization + time series extension).** 02D-rescale depends on 02D-sim. This makes each chunk reviewable and testable independently.

**Phase 0**

Phase 0 (no code) is productive and appropriate. Forcing concrete decisions about config field classification before writing code prevents the common failure mode of a Pydantic model that grows organically and never stabilizes. The TRITON-SWMM output schema investigation is correctly placed here.

**Chunk 01F (Test Infrastructure)**

01F's position at the end of Phase 1 is correct -- it needs the config model, I/O layer, and exception hierarchy to build meaningful fixtures. However, its prerequisite of "01A-01E" is overly strict. Test infrastructure should be built incrementally as each Phase 1 chunk is implemented. A better approach: each Phase 1 chunk writes its own tests, and 01F consolidates shared fixtures, adds comparison test patterns, and adds the old-code import machinery.

12. `[IMPORTANT]` **Redefine 01F as a consolidation and augmentation task, not a from-scratch test infrastructure build.** Each Phase 1 chunk (01A through 01E) should write its own tests inline. 01F should: (a) extract shared fixtures into `conftest.py`, (b) add the old-code import helpers, (c) establish the comparison test pattern with a working example. This avoids the anti-pattern of writing all foundation code without tests and then retrofitting test infrastructure after the fact.

### 3. Risk Management

**Assessment**: The plan identifies reasonable per-chunk risks (API rate limits, timezone confusion, convergence failures) but is missing several systemic risks that are critical for a stochastic simulation pipeline.

13. `[BLOCKING]` **Stochastic reproducibility (seed management).** The old code uses `np.random.RandomState(234)` in `_b6_simulate_from_vincop.py` and `np.random.choice`/`np.random.uniform` without seed control in `_utils.py` (tidal randomization, KNN selection). The plan does not mention seed management anywhere. For a stochastic weather generator, reproducibility is non-negotiable. The plan must: (a) define a seed management strategy (top-level seed in config, per-phase child seeds via `np.random.SeedSequence`), (b) mandate that every function accepting randomness takes an `rng: np.random.Generator` parameter, (c) add this to the config model in 01B. Without this, comparison testing is impossible for stochastic outputs, and users cannot reproduce results.

14. `[IMPORTANT]` **`pyvinecopulib` API stability and version pinning.** The `pyproject.toml` declares no runtime dependencies at all -- not even pandas, numpy, scipy, or pyvinecopulib. The plan must: (a) declare all runtime dependencies in `pyproject.toml` with version bounds, (b) pin `pyvinecopulib` to a specific minor version since it is a specialized library with a small user base and no stability guarantees. This applies to chunk 01A (pyproject.toml setup) or a new "project setup" pre-chunk.

15. `[IMPORTANT]` **Data schema evolution (NOAA API changes).** The old code uses `noaa_coops` to download data. NOAA has changed its API format and endpoints multiple times. The plan should: (a) wrap all NOAA API calls behind an adapter that normalizes the response format, (b) add schema validation on downloaded data (expected columns, units), (c) document the assumed API schema in the data inventory (chunk 00). This applies to chunk 01C.

16. `[SUGGESTION]` **Floating-point determinism across platforms.** Full bitwise reproducibility across platforms is not achievable for scipy distribution fitting and is not worth pursuing. However, the plan should state an explicit tolerance policy: comparison tests use `np.allclose` with specified `rtol` and `atol`, and stochastic comparison tests use statistical bounds rather than exact matching. Document this in 01F.

17. `[IMPORTANT]` **Large-file handling.** The old code works with zarr stores of AORC precipitation data and NetCDF intermediate files. The plan does not address where these live, how they are cached, or how CI tests run without them. Recommendation: (a) large data files live outside the repo in a configurable `data_root` directory specified in `pipeline_config.yaml`, (b) tests use small synthetic data, never full datasets, (c) add a `data_root` field to `PipelineConfig` in chunk 01B.

18. `[SUGGESTION]` **CI/CD for scientific code.** The plan should establish that CI runs unit tests with synthetic data only. Integration tests against real data are run manually or in a separate CI job with access to a data volume. State this in 01F.

**Comparison Testing for a Stochastic System**

The comparison testing strategy (old vs. new) is necessary but not sufficient. For deterministic operations (event selection, distribution fitting, time series resampling with fixed inputs), exact comparison within floating-point tolerance is appropriate. For stochastic operations (copula sampling, Poisson simulation, tidal randomization), comparison testing requires either: (a) fixed seeds and exact match, or (b) statistical distribution tests (KS/CVM) against reference distributions. The plan mentions comparison testing extensively but never specifies which functions are deterministic and which are stochastic, or how tolerance is defined for each case.

19. `[BLOCKING]` **Add a "deterministic vs. stochastic" classification to each Phase 2 chunk.** For each function being ported, state whether it is deterministic (exact comparison with tolerance) or stochastic (requires seed control + statistical comparison). This classification drives the testing strategy.

### 4. Testing Strategy

**Assessment**: The plan's reliance on comparison testing against old code is a reasonable starting strategy for a refactor, but it has a critical limitation: it cannot detect bugs in the old code. The plan acknowledges this ("consider the possibility that the old code has an error") but provides no decision protocol.

**Limitations of comparison testing**:
- If the old code is wrong, passing comparison tests means the new code is also wrong.
- The old code has at least two functions with `sys.exit()` calls that prevent execution entirely (`event_selection_threshold_or_nstorms` at line 1274, `compute_event_timeseries_statistics` at line 1563), suggesting these functions were mid-development. Comparison testing against incomplete functions is meaningless.
- Star imports make it difficult to identify which functions in `_utils.py` are actually used vs. dead code.

**Additional testing approaches needed**:

20. `[IMPORTANT]` **Property-based testing (Hypothesis).** Add Hypothesis strategies for: (a) event selection -- any valid time series should produce events whose time ranges do not overlap and whose statistics are consistent with the input data; (b) distribution fitting -- `fit_distribution` followed by sampling from the fitted distribution should produce data that passes a KS test against the original data; (c) rescaling -- rescaled time series should have the target depth and intensity within tolerance. Establish Hypothesis strategies as part of 01F.

21. `[IMPORTANT]` **Golden-file tests.** For the Norfolk case study, save known-good intermediate outputs (event summaries, fitted distribution parameters, copula parameters) as reference files. Compare against these in CI. This is separate from comparison testing against old code -- golden files represent the accepted output of the *new* code after manual validation. Add golden-file infrastructure to 01F.

22. `[SUGGESTION]` **Statistical distribution tests.** For stochastic outputs, test that simulated event statistics have the expected marginal distributions (KS test against fitted marginals) and the expected correlation structure (Kendall's tau within confidence bounds). The old code already does this in `_b7.1_qaqc_simulated_events.py` -- port these as automated tests, not just QA/QC plots.

**Stochastic output testing**: Use a combination of fixed seeds for regression tests and statistical bounds for distribution-level tests. Fixed-seed tests catch accidental changes to the simulation logic. Statistical-bound tests verify that the generator produces correct distributions regardless of seed.

**Decision protocol for old-code bugs**:

23. `[IMPORTANT]` **Add a decision protocol to the master plan.** When comparison testing reveals a discrepancy: (a) investigate whether the old or new code is correct, (b) if the old code is wrong, document the bug, fix the new code, and update the golden-file reference, (c) if the new code is wrong, fix it and re-run comparison tests. Do not silently match old-code bugs. Add this protocol to the master plan's "Requirements" section.

**Chunk 01F gaps**:

01F establishes fixtures, config factories, and comparison test helpers. It is missing:
- Hypothesis strategy definitions (custom strategies for time series, event summaries, config objects)
- Golden-file loading/comparison utilities
- A convention for marking tests as `@pytest.mark.slow` (integration tests with real data) vs. default (fast synthetic data)
- A fixture for injecting deterministic `np.random.Generator` instances

24. `[IMPORTANT]` **Expand 01F to include Hypothesis strategies, golden-file utilities, and a deterministic RNG fixture.** These patterns must be established early so Phase 2 chunks use them consistently.

### 5. Data Management and Provenance

**Assessment**: The plan has no data management strategy beyond "download data and save it to files." For a pipeline that produces stochastic simulations from observational data, this is a significant gap.

**Caching and versioning**:

25. `[IMPORTANT]` **Add a data caching strategy to chunk 00 and 01C.** Downloaded data should be cached in a `data_root` directory with a structure like `data_root/raw/{source}/{download_date}/`. Re-running the pipeline should not re-download data unless explicitly requested (the plan mentions idempotency in 01C, which is good). Add a manifest file (`data_root/raw/manifest.json`) that records download dates, source URLs, and file checksums.

**Intermediate artifacts**:

26. `[SUGGESTION]` **Define intermediate artifact naming conventions in chunk 00.** The old code uses ad-hoc naming (`a_water-lev_tide_surge.csv`, `6m_wlevel_mrms_event_summaries.csv`). The new code should use a consistent pattern: `{phase}_{content}_{format}.{ext}` (e.g., `b2_event_summaries_mrms.csv`). Alternatively, use the Pydantic config to define output paths for each phase, making them configurable.

**Data lineage**:

The plan does not track which version of downloaded data produced which copula fit. For a scientific pipeline, this is important for reproducibility.

27. `[SUGGESTION]` **Add a run metadata sidecar.** Each pipeline run should produce a `run_metadata.json` recording: config hash, data manifest hash, software version, random seed, and timestamps. Store this alongside outputs. This is not complex to implement and provides basic provenance.

**API download failures**:

01C specifies "exponential backoff retry" and `DataAcquisitionError`, which is appropriate. One gap: partial downloads. If the NOAA download fails mid-year (e.g., years 1927-1990 succeed but 1991 fails), the pipeline should not silently proceed with incomplete data.

28. `[IMPORTANT]` **Add a completeness check after data acquisition.** After downloading, verify that the expected date range is fully covered (no year-sized gaps). Raise `DataAcquisitionError` if coverage is incomplete. Add this to chunk 01C.

**Large files**:

Large files (gridded AORC precipitation zarr stores, intermediate NetCDFs) should not live in the repo. They should live in a `data_root` directory outside the repo, specified via config. The plan's `cases/norfolk/pipeline_config.yaml` should include a `data_root` path.

### 6. Open Decisions and Gaps

**Assessment**: The five cross-cutting decisions are well-framed. Decision 2 (NetCDF schema) and Decision 5 (config field inventory) are correctly placed in Phase 0. Decision 3 (human-in-the-loop) is correctly deferred. Two decisions are missing.

**Missing cross-cutting decisions**:

29. `[BLOCKING]` **Decision 6: DataFrame vs. xarray as the canonical internal data representation.** The old code uses DataFrames for event summaries and xarray Datasets for time series. The plan does not state which representation the new code will use for each data type. This affects every function signature in Phase 2. The plan should decide: (a) event summaries -> DataFrame (or domain model dataclass), (b) continuous time series -> xarray Dataset, (c) event time series -> DataFrame with multi-index (event_id, timestep) or xarray with event_id dimension. Make this decision in chunk 00.

30. `[IMPORTANT]` **Decision 7: Logging strategy.** The old code uses `print()` everywhere. The plan mentions replacing `print()` with `logging` in chunk 03A (runners), but logging configuration (level, format, handlers) is a cross-cutting concern that should be decided in Phase 0 and implemented starting in Phase 1. Every I/O function should log download progress; every core function should log warnings (e.g., "rescaling did not converge within tolerance").

**Human-in-the-loop risk**:

Deferring Decision 3 to Phase 3 is acceptable *if* the Phase 2 core modules are designed to accept user-specified distribution and copula parameters from config. The specific risk: if Phase 2 core modules hardcode an "automated fitting" workflow that selects distributions and fits copulas without user input, then adding a human-in-the-loop step in Phase 3 will require refactoring the Phase 2 modules. The mitigation is already partially in the plan (01B mentions "optional fields" for distribution selections), but it should be made explicit.

31. `[IMPORTANT]` **Add a requirement to 02A and 02C: core fitting functions must accept pre-specified parameters.** `fit_distribution()` should accept an optional `distribution_name` parameter that skips automated selection. `fit_vine_copula()` should accept optional copula family/structure specifications. This ensures the human-in-the-loop path is architecturally supported without committing to a specific workflow design.

**Implicit decisions**:

The plan makes the following implicit decisions without flagging them:

- **Copula serialization format**: JSON (matches old code). This is fine but should be documented.
- **Fail-fast vs. warn-and-continue**: The plan says "fail-fast" for downloads (01C) but does not state the policy for core computation. The rescaling loop in the old code has a 50-attempt cutoff and continues with a warning. The new code should do the same -- fail-fast is wrong for rescaling because some events are inherently difficult to rescale.
- **Return-results vs. mutate-in-place**: The old code mutates DataFrames in place extensively (`.reset_index(drop=True, inplace=True)`, `.rename(..., inplace=True)`). The plan should explicitly mandate "return new objects, never mutate inputs."
- **Event time indexing**: The old code uses both datetime indices and timedelta indices (relative to event start). The plan should pick one convention for event time series.

32. `[IMPORTANT]` **Add these implicit decisions as explicit items in the master plan's "Cross-Cutting Decisions" section.** At minimum: data representation convention, mutability policy, event time indexing convention, and rescaling failure policy.

**Old-code archive transition**:

The plan says old code is "fully superseded" in Phase 4 and can be "archived." However, the comparison testing strategy requires old code to remain importable throughout Phases 2-4. The plan should state:
- Old code remains in `_old_code_to_refactor/` and importable throughout implementation
- Each Phase 2 chunk's comparison tests import specific old functions
- In Phase 4B, after all comparison tests pass, the old code directory is either deleted or moved to `tests/_legacy/` for ongoing regression testing
- If old code is deleted, comparison tests that import it should be converted to golden-file tests

33. `[SUGGESTION]` **Document the old-code transition explicitly.** Add a section to the master plan stating when and how `_old_code_to_refactor/` is retired.

### 7. Architectural Extensibility for Methodological Improvement

**Assessment**: The plan's architecture makes most methodological parameters configurable, which is good. Several extension points require specific architectural attention.

**Event selection thresholds**: Easy to change. The plan explicitly parameterizes `rain_event_threshold_mm`, `surge_event_threshold_ft`, `min_interevent_time`, `max_event_duration_h` in the config. No change needed.

**Copula family selection**: Moderately easy to change. The `pyvinecopulib` API supports specifying copula families, and the plan's 02C chunk stores copula parameters in JSON. However, the plan does not expose copula family constraints in the config model. To make this configurable:

34. `[SUGGESTION]` **Add an optional `copula_families` field to the config model** that accepts a list of allowed bivariate copula families (e.g., `["gaussian", "clayton", "frank"]`). Pass this to `pyvinecopulib.FitControlsVinecop` as the `family_set` parameter. This avoids hardcoding family selection and supports the human-in-the-loop workflow.

**Rescaling convergence criteria**: Currently hardcoded. The old code uses `n_cutoff = 100`, `rtol = 0.01`, `atol = 0.1` in `_b7_generate_time_series_from_event_stats.py`. The plan's 02D chunk says "all buffer/tolerance parameters explicit" but does not list these specific parameters.

35. `[IMPORTANT]` **Add rescaling convergence parameters to the config model.** Specifically: `rescaling_max_iterations`, `rescaling_rtol`, `rescaling_atol`. Document in 02D. These are analysis parameters, not constants.

**KNN neighbor count**: The old code hardcodes `n_neighbors = 7`. The plan's 02D chunk says "n_neighbors as a parameter (currently hardcoded to 7)." This is correctly identified. Ensure it is added to the config model.

**Poisson process assumptions**: The old code assumes a Poisson process for event occurrence with separate rates for rain, surge, and combined events. The architecture should not hardcode Poisson -- make the occurrence model pluggable.

36. `[SUGGESTION]` **Define an occurrence model protocol** (abstract base class or `Protocol`) in `core/simulation.py` with a `simulate_occurrences(n_years, rng) -> pd.Series` method. Provide a `PoissonOccurrenceModel` as the default implementation. This allows future replacement with, e.g., a negative binomial or cluster process without restructuring.

**Tidal phase randomization**: The old code in `generate_randomized_tide_component` uses `n_years_to_look_back_for_tidal_tseries = 4` and `t_window_h_to_shift_tidal_tseries = 2*7*24` (two weeks). These are hardcoded in `_inputs.py`.

37. `[IMPORTANT]` **Add tidal randomization parameters to the config model.** `tidal_lookback_years` and `tidal_shift_window_hours` should be explicit config fields, not buried in `_inputs.py` constants. They are analysis choices, not physical constants.

### 8. Developer Experience and Maintainability

**Assessment**: The plan provides good structural guidance but has documentation gaps that would make onboarding a new contributor difficult.

**Documentation gaps**:

38. `[BLOCKING]` **The plan references `CONTRIBUTING.md` in every work chunk's "Before Proceeding" section, but this file does not exist.** Either create it before implementation begins or remove the references. Since the work chunks cite it as containing "project development philosophy" that "all implementation decisions must align with," it should exist and contain: coding standards, testing requirements, git workflow, and the error-handling contract.

39. `[SUGGESTION]` **Add a "Glossary of Pipeline Concepts" section to the master plan** or to `cases/norfolk/README.md`. Terms like "event selection," "combined event," "marginal selection," "vine copula," "rescaling," and "nearest-neighbor matching" have specific meanings in this pipeline that differ from their general usage. A glossary reduces ambiguity for future contributors.

**Invocation model**:

The right primary invocation model for this pipeline is a Python API with CLI convenience wrappers and optional Jupyter notebooks for the human-in-the-loop phase. Snakemake is overkill for a single-developer project with one case study and a linear pipeline. The plan correctly defers the Snakemake question to Phase 3.

40. `[SUGGESTION]` **Default to script-based runners (Option 2 in Decision 3) unless a specific Snakemake benefit is identified.** Runners that call `config = load_config(yaml_path); result = run_phase_a(config)` are simpler, debuggable, and sufficient for a single case study. Snakemake adds value only if there are many independent runs to parallelize or complex dependency DAGs -- neither applies here.

**Jupyter notebooks**:

Jupyter notebooks should play a role in (a) the human-in-the-loop distribution/copula selection phase and (b) exploratory QA/QC of results. They should not be part of the automated pipeline. The plan does not mention notebooks at all.

41. `[SUGGESTION]` **Add a `notebooks/` directory to the target architecture** with template notebooks for: (a) distribution selection (`marginal_selection.ipynb`), (b) copula diagnostics (`copula_diagnostics.ipynb`), (c) simulation QA/QC (`simulation_qaqc.ipynb`). These are part of the human-in-the-loop workflow and should be first-class deliverables.

**Environment management**:

The plan targets `pip install multidriver-swg` but the architecture doc specifies a conda environment (`multidriver_swg`). The `pyproject.toml` declares no runtime dependencies. This is inconsistent.

42. `[BLOCKING]` **Declare runtime dependencies in `pyproject.toml`.** At minimum: `numpy`, `pandas`, `scipy`, `xarray`, `pydantic>=2`, `pyvinecopulib`, `scikit-learn`, `netcdf4`, `zarr`, `pyyaml`, `noaa-coops`. Non-Python dependencies (GDAL/rasterio for spatial operations, dask for large data) should be documented as requiring conda. The `pip install multidriver-swg` success criterion in the master plan is achievable only if dependencies are declared. The conda environment is the recommended installation method; pip-installability is a packaging quality indicator, not a replacement for conda.

### 9. Verdict

**Top 5 most important changes**, in priority order:

1. **Define domain model objects (Event, FittedDistribution, CopulaModel, SimulatedEvent) before Phase 2.** Without these, Phase 2 chunks will invent incompatible DataFrame column conventions and integration will require rework. (Recommendation 3)

2. **Add seed management to the config model and mandate `rng: np.random.Generator` parameters on all stochastic functions.** Without this, comparison testing of stochastic outputs is impossible and users cannot reproduce results. (Recommendation 13)

3. **Declare runtime dependencies in `pyproject.toml`.** The package currently has zero declared dependencies. This must be fixed before any code is written. (Recommendation 42)

4. **Create `CONTRIBUTING.md`.** Every work chunk references it as mandatory reading, and it does not exist. (Recommendation 38)

5. **Split chunk 02D (Simulation and Rescaling) into two chunks and remove the false 02B->02A dependency.** These changes reduce the critical path and make the largest chunk manageable. (Recommendations 9 and 11)

**Recommendation**: Proceed with modifications. The plan's overall structure, phasing strategy, and decomposition are sound. The issues identified above are fixable without restructuring the plan. Resolve the five BLOCKING items (domain models, seed management, dependencies, CONTRIBUTING.md, DataFrame-vs-xarray decision) and update the work chunks with the IMPORTANT recommendations before beginning Phase 1 implementation.

**Stop-the-line findings**: None. The BLOCKING items are serious but addressable through plan updates before implementation. No issue requires abandoning or fundamentally rethinking the approach.

---

## Appendix A: Specialist Audit Instructions

**Write target**: `docs/planning/refactors/2026-03-14_refactor_plan_audit.md`, section `## Audit Findings`. Replace the placeholder text `*(To be written by the software-engineering-specialist — see Appendix A for instructions.)*` with your full audit.

### Context

You are auditing a refactoring plan for `multidriver-swg`, a stochastic weather generator that produces multi-driver flood forcing (rainfall, storm surge, tidal phase) by resampling and rescaling historic events to match randomly generated event statistics. The pipeline downloads observational data from public APIs, selects and classifies extreme events, fits vine copulas to multivariate event statistics, simulates synthetic events from those copulas, and rescales observed time series to match simulated event statistics — ultimately producing NetCDF boundary conditions for a coupled hydrodynamic model (TRITON-SWMM).

The refactoring has NOT started. No code has been written against the plan. The existing codebase is ~9,000 lines across 15 interactive scripts with no tests, no type hints, star imports everywhere, hardcoded paths, `sys.exit()` for error handling, and tangled I/O/computation/plotting. The plan proposes a bottom-up refactor into a pip-installable library with Pydantic config, separated I/O/computation/plotting layers, and phased implementation with comparison testing against the old code.

The developer has complete flexibility to adapt, restructure, or overhaul the plan based on your recommendations. The fact that the old code has serious problems does not mean the plan is the right solution to those problems — evaluate the plan on its own merits.

### Tone and Format

- Be direct. Flag problems clearly. Do not hedge. If the plan is wrong, say so. If it is right, say that briefly and move on.
- Lead every Assessment with the finding, not with qualifications.
- If a section of the plan is strong, say so in one sentence and move on. Do not pad strong sections.
- Aim for thorough coverage without padding. Quality over length.

### What to Read

Read these files in the order listed. The reading order matters: understand the overall architecture first, then the decomposition, then verify against the actual old code.

1. **Master plan** — read first to understand overall structure and phasing: `docs/planning/refactors/2026-03-01_full_codebase_refactor/full_codebase_refactor.md`
2. **Work chunk index** — read to understand chunk dependency graph: `docs/planning/refactors/2026-03-01_full_codebase_refactor/work_chunks/README.md`
3. **All 16 work chunks** — read to evaluate decomposition and scope of each: `docs/planning/refactors/2026-03-01_full_codebase_refactor/work_chunks/*.md`
4. **Old code entry point** — read for grounding on what the config layer must replace: `_old_code_to_refactor/_inputs.py`
5. **Old utilities monolith** — skim function signatures and structure to verify the plan's decomposition covers everything: `_old_code_to_refactor/_utils.py` (2,393 lines — skim, do not read line-by-line)
6. **Main pipeline scripts** — skim top-level structure and function names in each to verify work chunks cover all functionality: `_old_code_to_refactor/_a_*.py`, `_old_code_to_refactor/_b*.py`, `_old_code_to_refactor/_c_*.py`
7. **Project config**: `pyproject.toml`
8. **Architecture doc**: `~/dev/agentic-workspace/prompts/workspaces/projects/multidriver-swg/architecture.md`

### Audit Framework

Organize your findings into the sections below. For each section, provide:
- **Assessment** (1–2 sentences): Lead with the finding. State what the plan does well or gets wrong.
- **Recommendations** (numbered): Concrete, actionable changes. For each: state *what* to change, *why*, and *where* in the plan it applies. Tag each recommendation: `[BLOCKING]` (must resolve before implementation begins), `[IMPORTANT]` (resolve before or during early phases), or `[SUGGESTION]` (worth considering but not required).

#### 0. Scope and Ambition
- Assess whether the scale of this refactor is appropriate for the codebase size (~9,000 lines), team size (single developer), and project maturity. State whether it should be scaled up, scaled down, or restructured.
- Evaluate whether a more incremental approach (e.g., add tests to old code first, then refactor module-by-module) would reduce risk. Name specific tradeoffs.
- Identify any phases or chunks that could be dropped without compromising the core goal.

#### 1. Architectural Soundness

Assess domain modeling first:
- Identify any missing domain objects (e.g., `Event`, `StormSurgeRecord`, `CopulaModel`, `SimulatedTimeSeries`, `FittedDistribution`). If the plan does not define a domain model layer, state where it should live and what it should contain.
- State the error-handling contract the plan implies or omits. Assess whether library functions should raise exceptions, return Result types, or log-and-continue, and whether the plan is consistent on this.

Then assess structure:
- Evaluate the proposed `src/multidriver_swg/` package decomposition. Name any modules that should be split, merged, or renamed.
- Assess whether the I/O / computation / plotting separation holds up under scrutiny. Name specific places where it breaks down.
- Assess the Pydantic + YAML config design for a pipeline of this type.
- Identify any missing architectural elements (result containers, pipeline state, intermediate artifact management).

#### 2. Phasing and Dependency Order
- Assess whether the bottom-up phase ordering is correct. Identify any circular or unnecessary dependencies between work chunks.
- Verify the dependency graph. Name at least one dependency that is wrong, missing, or unnecessary — if the graph is correct, state that explicitly and explain why.
- Identify any chunks that are too large or too small. State which should be split or merged and why.
- Assess whether the "Phase 0 — no code" approach is productive or risks over-planning before implementation feedback.
- Assess whether chunk 01F (test infrastructure) has the right prerequisites and position in the dependency chain.

#### 3. Risk Management
- Assess whether the identified risks are realistic and well-mitigated.
- Identify which of the following risks are missing or underaddressed in the plan. For each, write a numbered recommendation tagged `[BLOCKING]`, `[IMPORTANT]`, or `[SUGGESTION]`:
  - Stochastic reproducibility (seed management across the pipeline)
  - Floating-point determinism across platforms
  - `pyvinecopulib` API stability and version pinning
  - Data schema evolution (what happens when NOAA changes their API response format?)
  - Large-file handling (gridded precip rasters, intermediate NetCDFs)
  - CI/CD for scientific code (how to test against reference data without committing large files)
- Assess whether the comparison testing strategy (old vs. new) is sufficient for a stochastic system. State how tolerance should be defined.

#### 4. Testing Strategy
- Assess whether "comparison tests against old code" is the right primary testing strategy. Name its limitations.
- Evaluate which additional testing approaches should be used. Address each: property-based testing (hypothesis), golden-file tests, statistical distribution tests (KS/CVM against expected marginals), round-trip tests.
- State how stochastic outputs should be tested — fixed seeds, statistical bounds, distribution-level tests, or a combination.
- Assess how the plan handles the case where comparison testing reveals that the OLD code was wrong. If there is no decision protocol, state what it should be.
- Evaluate what testing approaches chunk 01F includes. Identify testing patterns it should establish (fixtures, factory functions, hypothesis strategies, golden-file patterns) that it does not.

#### 5. Data Management and Provenance
- Assess the plan's strategy for caching and versioning downloaded data. If absent, state what is needed.
- Evaluate the intermediate artifact strategy — storage format, naming conventions, directory structure.
- Assess whether and how data lineage is tracked (which version of downloaded data produced which copula fit).
- Evaluate how API download failures and partial downloads should be handled.
- State whether large files (gridded precip, intermediate NetCDFs) should live in-repo, in a separate data directory, or in a remote store.

#### 6. Open Decisions and Gaps
- Assess whether the 5 cross-cutting decisions are well-framed. Identify any that are missing.
- Evaluate whether the human-in-the-loop design question is adequately deferred or creates downstream risk for Phase 3. Name the specific risk.
- Identify implicit decisions the plan makes without flagging them. Address both data representation choices (DataFrame vs. xarray, event time indexing, copula serialization format) and behavioral choices (fail-fast vs. warn-and-continue, stateless vs. session state, return-results vs. mutate-in-place, logging strategy).
- Assess the old-code archive transition strategy. State at what point in the phasing each old script should be considered "done" and whether `_old_code_to_refactor/` can be deleted at the end or must persist for regression testing.

#### 7. Architectural Extensibility for Methodological Improvement
- Assess each of these extension points: event selection thresholds, copula family selection, rescaling convergence criteria, KNN neighbor count, Poisson process assumptions, tidal phase randomization.
- For each: state whether the current architecture makes it easy or hard to change, and what architectural change (if any) would make it configurable. Do not recommend whether the methodology should change — that is a domain question. Do recommend specific architectural modifications that would make future changes possible without restructuring the library.

#### 8. Developer Experience and Maintainability
- Assess whether a new contributor could understand and extend this codebase based on the plan.
- Identify documentation gaps in the plan.
- Evaluate the right primary invocation model for this pipeline — CLI, Python API, Jupyter, Snakemake, or a combination. Assess the plan's runner/CLI design against this.
- Assess whether Jupyter notebooks should play a role beyond the human-in-the-loop phase.
- Evaluate whether the environment management story is consistent. The plan targets `pip install multidriver-swg` but the architecture doc specifies a conda environment. State how non-Python dependencies should be handled.

#### 9. Verdict

After completing sections 0–8, provide:
1. **Top 5 most important changes** to the plan, in priority order.
2. **Recommendation**: Should implementation proceed as-planned, proceed with modifications, or pause for plan revision?
3. **Stop-the-line findings**: Any issues that would make it dangerous to proceed without resolution. If none, say so explicitly.

Stop after item 3. Do not add a summary, closing remarks, or an offer to discuss further.
