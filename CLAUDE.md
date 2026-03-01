# CLAUDE.md

Read these files before beginning any task:

- `CONTRIBUTING.md`
- `architecture.md`

---

## Planning Document Lifecycle

Read `~/dev/claude-workspace/specialist_agent_docs/planning-document-lifecycle.md` for the full lifecycle rules.

---

## Environment

This project uses a conda environment named `multidriver_swg`.

- **Running tools**: Use `conda run -n multidriver_swg <command>` or activate the environment first with `conda activate multidriver_swg`.
- **Copier updates**: When running `copier update` through `conda run`, pass `--defaults` since there is no interactive terminal: `conda run -n multidriver_swg copier update --trust --skip-tasks --defaults`.

---

## Code Style

- **Python**: ≥3.10, target 3.12+
- **Formatter/linter**: `ruff format` and `ruff check` — run before submitting any code. Line length and all style rules are enforced by `pyproject.toml`; write code that will survive `ruff format` unchanged.
- **Type checker**: Pyright/Pylance — address squiggles organically as scripts are touched; do not leave new `# type: ignore` comments unless the issue is a known type checker limitation

---

## Terminology

<!-- Populate with project-specific terms that have precise meanings in this codebase. -->

---

## Architecture Patterns

<!-- Populate with project-specific patterns (e.g., configuration flow, runner script conventions). -->

---

## AI Working Norms

Read `~/dev/claude-workspace/specialist_agent_docs/ai-working-norms.md` for the full protocol.
