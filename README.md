# Multi-Driver Stochastic Weather Generator

Stochastic weather generator producing multi-driver flood forcing (rainfall + storm surge + tidal phase) by resampling and rescaling historic events to match randomly generated event statistics.

## Installation

```bash
pip install multidriver-swg
```

> **Note**: PyPI publishing requires one-time setup of [trusted publishers](https://docs.pypi.org/trusted-publishers/) on pypi.org and test.pypi.org. See the [Copier Update Guide](https://github.com/lassiterdc/copier-python-template/blob/main/COPIER_UPDATE_GUIDE.md) for setup instructions.

## Quick start

```python
import multidriver_swg
```

## Documentation

Full documentation at [ReadTheDocs](https://multidriver-swg.readthedocs.io/).

## Licence

The toolkit is licensed under the PolyForm Noncommercial License 1.0.0 (`LICENSE`).
That licence does not cover `_old_code_to_refactor/`, which is MIT-licensed
(`_old_code_to_refactor/LICENSE`) and a byte-identical copy of files of
`stormy@c74c8a4` (<https://github.com/lassiterdc/stormy>). One block of
`_old_code_to_refactor/_d_build_water_level_generator.py` is attributed to a third
party, whose notice is `_old_code_to_refactor/NOTICE.md`. Text copied from that
directory into the test tree carries the same MIT notice beside it, in
`tests/correspondence/extractions/LICENSE`.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup and contribution guidelines.
