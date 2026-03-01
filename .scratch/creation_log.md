# multidriver-swg Creation Log

**Date**: 2026-02-28
**Template**: `copier-python-template` v1.0.0 (initial generation)
**Post-generation fixes**: Applied manually to align with v1.0.1 changes

## Generation Command

```bash
cd ~/dev
copier copy --trust ~/dev/copier-python-template multidriver-swg
```

First attempt without `--trust` failed (template uses `_tasks`). Re-ran with `--trust`.

## Copier Answers (interactive)

- `project_name`: Multi-Driver Stochastic Weather Generator
- `project_slug`: multidriver-swg (default)
- `package_name`: multidriver_swg (default)
- `author_name`: Daniel Lassiter (default)
- `author_email`: daniel.lassiter@outlook.com (default)
- `github_username`: lassiterdc (default)
- `description`: Stochastic weather generator producing multi-driver flood forcing (rainfall + storm surge + tidal phase) by resampling and rescaling historic events to match randomly generated event statistics.
- `python_version`: 3.11

## Post-Generation Fixes

### 1. Description typo — double period and inconsistent separators

The description was entered with a trailing period (producing `statistics..` in generated files) and used `, ` before `storm surge` instead of ` + `.

**Files fixed** (all instances of the description):
- `.copier-answers.yml`
- `mkdocs.yml`
- `docs/index.md`
- `pyproject.toml`
- `README.md`

### 2. Missing `markdown_extensions` block in `mkdocs.yml`

The template at `v1.0.0` did not include the `markdown_extensions` block (it was added post-tag in commit `1dfbb97`). The block was added manually to the generated `mkdocs.yml` to enable Mermaid diagrams and admonition rendering.

### 3. Template fixes committed as v1.0.1

- `copier.yml`: `python_version` default changed from `3.12` to `3.11`
- `copier.yml`: description help text updated to `(no trailing period)`
- Tagged as `v1.0.1` and pushed
