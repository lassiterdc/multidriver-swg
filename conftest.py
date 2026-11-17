"""Put this checkout's source root first on the import path when the package does not resolve to it.

A runner that starts the tests in isolated mode discards PYTHONPATH, and an editable install of another checkout makes an unrelated `multidriver_swg` importable, so without this the toolkit's subject hook stops the session before a test collects; the source root, and the child processes' PYTHONPATH, are set only when `src/` sits beside this file and the package does not already resolve to it, so a tree that already imports this checkout's package, and a tree with no `src/`, are unchanged.
"""

import importlib.util
import os
import sys
from pathlib import Path

_SOURCE_ROOT = Path(__file__).resolve().parent / "src"
_PACKAGE_FILE = _SOURCE_ROOT / "multidriver_swg" / "__init__.py"


def _resolves_to_this_tree() -> bool:
    """Whether the package already imports from this checkout's own source tree, without importing it."""
    spec = importlib.util.find_spec("multidriver_swg")
    if spec is None or spec.origin is None:
        return False
    return Path(spec.origin).resolve() == _PACKAGE_FILE.resolve()


if _SOURCE_ROOT.is_dir() and not _resolves_to_this_tree():
    sys.path.insert(0, str(_SOURCE_ROOT))
    os.environ["PYTHONPATH"] = os.pathsep.join([str(_SOURCE_ROOT), *filter(None, [os.environ.get("PYTHONPATH")])])
