# Development Conventions

Consistent vocabulary and working principles. Reference this document when planning implementations and during QA review. It is a living document — update it when new rules are established or existing ones are clarified.

---

## Part I: Universal Principles

*Portable to any solo Python project with minimal editing.*

### Raise questions rather than make assumptions
When you encounter uncertainty or discrepancies — especially when implementing a pre-written plan that may have stale components — err on the side of caution and ask the developer how to proceed.

### Plan, then implement
Follow a plan-then-implement strategy. If implementing a plan uncovers a need to change it or its success criteria — including deviations from the planned approach, scope changes, or new risks — raise the discrepancy before continuing rather than adapting silently.

### Let's do things right, even if it takes more effort
- Always be on the lookout for better ways of achieving development goals and raise these ideas
- Raise concerns when you suspect the developer is making design decisions that diverge from best practices
- Look for opportunities to make code more efficient (vectorize operations, avoid loops with pandas, etc.)

### Backward compatibility is NOT a priority

**Rationale**: Single developer codebase. Clean code matters more than preserved APIs. Git history is the safety net.

When refactoring:
- ❌ Don't add deprecation warnings
- ❌ Don't keep old APIs "for compatibility"
- ❌ Don't create compatibility shims or aliases
- ✅ Do update all usage sites immediately
- ✅ Do delete obsolete code completely

### Most function arguments should not have defaults

Default function arguments can lead to difficult-to-debug unexpected behavior. Avoid default values unless a default is almost always the correct choice (e.g., `verbose=True`). This is especially true for configuration fields that users populate — the user should make an intentional choice about every input.

### Avoid aliases

Do not create aliases for functions, classes, or variables. An alias is a second name for the same thing — it creates confusion about which name is authoritative and is a form of backward-compatibility shim. If something needs renaming, rename it and update all call sites.

### No cruft/all variables, imports, and function arguments must be used

Unused elements are a signal that implementation may be incomplete. Treat them as an investigation trigger, not just lint to suppress.

If you come across an unused variable, import, or function argument, investigate before removing:
1. Check whether the surrounding implementation is incomplete
2. Find planning documents that touched that function and determine whether implementation is planned
3. If still uncertain, raise the concern with the developer with hypotheses about why it exists
4. The only exception: elements included for a currently-planned implementation, marked with a comment referencing the planning document

Report your observations, hypotheses, and recommendations to the developer.

After investigation and with approval from the developer, remove unused code, dead branches, commented-out blocks, and stale imports.

### Functions have docstrings, type hints, and type checking

Apply this standard to code you write or modify. For existing code in touched scripts, apply organically — accumulate adherence naturally as scripts are touched rather than doing a global retrofit pass.

### Fail-fast

Critical paths must raise exceptions; never silently return `False` or `None` on failure.

### Preserve context in exceptions

Exceptions should include file paths, return codes, and log locations for actionable debugging.

### Prefer log-based completion checks over file existence checks

A file may exist but be corrupt, incomplete, or from a previous failed run. File existence checks can mask errors when log checks are available.

- **Exception**: File existence is appropriate for verifying *input* files before reading them.

### Keep system-agnostic software

System-specific information belongs in user-defined configuration files. Avoid hardcoded paths or machine-specific constants in core code.

### Track project-agnostic utility candidates

When writing utility functions that could plausibly belong in a shared library (e.g., general-purpose file I/O helpers, generic array operations), note them in a dedicated tracking document. Do not extract them immediately — track them so they can be evaluated together.

---

## Part II: Project-Specific Rules

*Project specifics. Replace or extend for a different project.*

### Planning document lifecycle

Planning documents live in `docs/planning/` organized by type (`bugs/`, `features/`, `refactors/`). Each type directory has a `completed/` subdirectory; a `shelved/` subdirectory is created as needed.

- **Active work**: document lives directly in the type directory (e.g., `docs/planning/bugs/2026-02-28_my_fix.md`)
- **Completed**: move to `completed/` within the same type directory (e.g., `docs/planning/bugs/completed/2026-02-28_my_fix.md`)
- **Deprioritized/blocked**: move to `shelved/` within the same type directory (create if it doesn't exist)
- **No longer relevant**: delete

**Naming convention**: all planning docs use a `YYYY-MM-DD_` date prefix corresponding to the creation date, followed by a descriptive `snake_case` name. Exceptions: `README.md`, `utility_package_candidates.md`, and other persistent tracking documents that have no creation lifecycle.

See `docs/planning/README.md` for the full structure.

#### Multi-phase plans

When a plan will be implemented and committed in phases across separate sessions, use a subdirectory instead of a single file:

```
docs/planning/<type>/YYYY-MM-DD_<name>/
├── master.md              ← overview, cross-phase dependencies, decisions log, phase status table
├── 1_<phase1_name>.md     ← phase 1 content only
├── 2_<phase2_name>.md
...
└── implemented/           ← completed phase docs move here
```

- The subdirectory uses the same `YYYY-MM-DD_<name>` naming convention as atomic docs
- Each phase doc is the implementation unit — use `@.prompts/proceed_with_implementation.md` with the phase doc, not `master.md`
- When a phase is complete, move its doc to `implemented/` and update the phase status table in `master.md`
- When all phases are complete, move the entire subdirectory to `completed/` (e.g., `docs/planning/features/completed/2026-02-28_my_plan/`)

#### Completed documents describe final state, not history

A completed document is a **reference**, not an audit trail. Other agents and future sessions may read it as a dependency. It must accurately describe what was built — not what was considered and rejected.

Before closing a document:
- Remove or replace sections describing approaches that were not implemented
- Replace phased implementation plans with a concise "What Was Built" summary
- Keep: requirements, key decisions with rationale, file inventory, Definition of Done
- Drop: superseded alternatives, dead-end phase breakdowns, trade-off tables for rejected approaches

The git history is the audit trail. The completed doc is the reference.

### Recording out-of-scope observations

When implementation work surfaces a real issue that is out of scope for the current task (e.g., pre-existing linting violations noticed while running ruff, an unused import that warrants investigation, a questionable pattern in untouched code), record it in a planning document rather than silently suppressing it or fixing it in-band:

- **Code quality / linting debt** → `docs/planning/bugs/tech_debt_<topic>.md`
- **Potential bug** → `docs/planning/bugs/<date>_<topic>.md`
- **Enhancement idea** → `docs/planning/features/<date>_<topic>.md`

The recording document should include: what was observed, where (file + line), why it was deferred, and enough context to act on it cold. Link to it from the originating plan's definition-of-done checklist so the observation is traceable. Do **not** use `# TODO` comments in source code, `MEMORY.md` bullets, or inline `# noqa` suppressions as the sole record — these are invisible to planning workflows.

### Code style

- **Python**: ≥3.10, target 3.12+
- **Formatter/linter**: `ruff format` and `ruff check` — run before submitting any code. Line length and all style rules are enforced by `pyproject.toml`; write code that will survive `ruff format` unchanged.
- **Type checker**: Pyright/Pylance — address squiggles organically as scripts are touched; do not leave new `# type: ignore` comments unless the issue is a known type checker limitation

### Terminology

<!-- Populate with project-specific terms that have precise meanings in this codebase. -->

### Architecture patterns

<!-- Populate with project-specific patterns (e.g., configuration flow, runner script conventions). -->

---

## Part III: AI Working Norms

*Claude Code conventions for this developer. Light editing needed to port to a different AI tool.*

### Never assume permission to start making edits
Developer permission is required before making any file edits. When you believe the plan is implementation ready, summarize the changes you plan to make and recommend proceeding with implementation.

### Never commit without explicit permission
All commits require prior approval from the developer.

### Never commit Jupyter notebooks
Notebooks in `tests/dev/` are developer testing scratchpads — they contain transient state (`start_from_scratch`, cell outputs, etc.) and should never be staged or committed. If a notebook appears in `git status`, exclude it from the commit.

### Spawning subagents

Available agents and their scopes are catalogued in `~/dev/claude-workspace/README.md`.

Never invoke the Task tool without first confirming with the developer. Subagents start fresh — they do not inherit conversation history. If the root cause spans multiple domains, recommend all relevant specialists rather than picking one. Present the developer with:
1. **Which specialist(s) and why** — what domain question each will answer
2. **The write target** — exact planning doc path and section where findings will be recorded
3. **The full prompt** — developer approves before it runs

#### Write target is required

Specialist findings written only to conversation text are lost across sessions. Every invocation must have a specific planning doc path and section designated before approval is requested.

If no appropriate doc exists yet, recommend one to the developer — which type (`bugs/`, `features/`, `refactors/`), what to name it (`YYYY-MM-DD_name.md`), and which section the specialist will fill in.

#### Constructing the prompt

- **Write target** — tell the specialist exactly where to record findings and in what section
- **Read-only artifact** — a log excerpt, code snippet, or report for context (not to write to)
- **Precise, scoped questions** — answerable from source code or docs; e.g., "Does `--cpu-bind=cores` under `--overlap` serialize srun step admission in SLURM 24.11.5?" not "help me debug this"
- **Efficiency self-assessment** — ask the specialist to note how the prompt or context could be tightened; surfaces inefficiencies for future improvement

#### System-level agents

Agents are project-agnostic — pass `@.prompts/conventions.md` and/or `@.prompts/architecture.md` explicitly when needed. Pass only what the task requires; many specialist questions need no project context at all.

#### Do not use general-purpose agents for directed lookups

Use `Glob`, `Grep`, or `Bash` for verifying file paths, directory structure, or specific values — not an Explore or general-purpose agent. Reserve agents for genuinely open-ended exploration where the answer space is unknown.

### `#user:` comments in planning documents are blocking
In planning documents, all comments prefixed with `#user:` are developer feedback that must ALL be addressed before any implementation can take place. Remove each comment only after written confirmation from the developer that it has been sufficiently addressed. Implications for the entire planning document should be considered when addressing these comments.

### Keep documentation current

When making significant code changes:
- Does this change affect architecture described in `.prompts/architecture.md`? Verify class names, module names, file paths, and config fields still match.
- Does this change affect patterns documented in an active agent file? Update the agent.
- Does this introduce new conventions or update existing ones? Update `.prompts/conventions.md`.
- Are there new gotchas or non-obvious behaviors to document?
- Are there new critical configuration fields to highlight?

### Plan, then implement — toolkit workflow

Follow the plan-then-implement strategy outlined in `.prompts/implementation_plan.md`. Use `.prompts/proceed_with_implementation.md` for preflight checks before starting implementation. Use `.prompts/qaqc_and_commit.md` for post-implementation review.
