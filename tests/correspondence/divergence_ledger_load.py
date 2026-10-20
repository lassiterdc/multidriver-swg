"""Load the package's divergence-ledger reader by its file path, once per interpreter, without importing the package.

The ledger and its reader live in the package (`src/multidriver_swg/`), so a report run from the
installed package can read them. A check in the test tree must read the same file, but importing
`multidriver_swg.divergence_ledger` normally would first run the package's `__init__`, which will
import the package's dependencies, and a pre-commit hook's interpreter has none of them. So the
test tree loads the reader from its path, and this module is the ONE place it does so.

The module is entered into `sys.modules` under `multidriver_swg.divergence_ledger` before it runs.
Deliberately NOT executed first and registered after: the reader defines a dataclass under
`from __future__ import annotations`, and the dataclass machinery looks its module up in
`sys.modules` while the class is being built, so loading by path without the entry raises
`AttributeError: 'NoneType' object has no attribute '__dict__'`. And a second call returns the
module already there, so every consumer in one interpreter holds one module object and one
`Ledger` class, and an `isinstance` check across two consumers cannot fail on two copies.
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

NAME = "multidriver_swg.divergence_ledger"
SOURCE = Path(__file__).resolve().parents[2] / "src" / "multidriver_swg" / "divergence_ledger.py"


def reader() -> types.ModuleType:
    """The divergence-ledger reader module, loaded from `SOURCE` on the first call and returned as is afterwards."""
    module = sys.modules.get(NAME)
    if module is not None:
        return module
    spec = importlib.util.spec_from_file_location(NAME, SOURCE)
    module = importlib.util.module_from_spec(spec)
    sys.modules[NAME] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        del sys.modules[NAME]
        raise
    return module
