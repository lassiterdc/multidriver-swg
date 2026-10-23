# Multi-Driver Stochastic Weather Generator

Stochastic weather generator producing multi-driver flood forcing (rainfall + storm surge + tidal phase) by resampling and rescaling historic events to match randomly generated event statistics.

## Installation

```bash
pip install multidriver-swg
```

> **Note**: The only release on PyPI is 0.1.0, uploaded on 2026-03-01. It predates this source tree (version `0.2.0.dev0`) and declares the MIT licence, not the AGPL-3.0-or-later licence described below. To use the code this README describes, install from source as shown in [docs/installation.md](docs/installation.md).

## Quick start

```python
import multidriver_swg
```

The package does not yet contain the weather generator described above. The top-level `multidriver_swg` module defines one function, `hello`, a placeholder from the project template. Its two submodules record how new code relates to the pre-refactor scripts in `_old_code_to_refactor/`: `multidriver_swg.pairs` pairs each new function with the legacy function it replaces (`pair_side`, `registered_pairs`), and `multidriver_swg.divergence_ledger` reads and checks the record of deliberate departures from legacy behaviour (`load`, `validate`).

## Documentation

Full documentation at [ReadTheDocs](https://multidriver-swg.readthedocs.io/).

## Licence

The toolkit is licensed under the GNU Affero General Public License, version 3 or
any later version (AGPL-3.0-or-later; `LICENSE`).
That licence does not cover `_old_code_to_refactor/`, which is MIT-licensed
(`_old_code_to_refactor/LICENSE`). Its 15 scripts and its `LICENSE` are
byte-identical copies of files of `stormy@c74c8a4`
(<https://github.com/lassiterdc/stormy>); its `README.md` and `NOTICE.md` are
not. One block of
`_old_code_to_refactor/_d_build_water_level_generator.py` is attributed to a third
party, whose notice is `_old_code_to_refactor/NOTICE.md`. Text copied from that
directory into the test tree carries the same MIT notice beside it, in
`tests/correspondence/extractions/LICENSE`.

## Development

See [docs/installation.md](docs/installation.md) for creating the development environment and installing the package in editable mode.
