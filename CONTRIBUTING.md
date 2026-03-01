# Contributing

## Development setup

1. Fork and clone the repository
2. Create a conda environment or virtual environment
3. Install in development mode: `pip install -e ".[docs]"`
4. Install pre-commit hooks: `pre-commit install`

## Workflow

- Create a feature branch from `main`
- Make changes with tests
- Run `ruff check .` and `ruff format .`
- Run `pytest`
- Submit a pull request

## AI context documentation

This project uses Claude Code with structured prompt files in `.prompts/`. When working with AI assistance:

- `.prompts/conventions.md` — working norms and code design rules
- `.prompts/architecture.md` — project structure and key modules
- Use `@.prompts/proceed_with_implementation.md` before implementing plans
- Use `@.prompts/qaqc_and_commit.md` after implementation

## Documentation

Build docs locally:

```bash
pip install -e ".[docs]"
mkdocs serve
```
