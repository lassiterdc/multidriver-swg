# Installation

## Create environment

```bash
conda create -n multidriver_swg python=3.11
conda activate multidriver_swg
```

## Install

```bash
pip install -e ".[docs]"
```

!!! note
    The `[docs]` extra installs MkDocs and mkdocstrings for building documentation locally.
    Omit it for a minimal install: `pip install -e .`
