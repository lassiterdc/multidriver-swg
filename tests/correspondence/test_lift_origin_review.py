"""Check that the lift runner admits a module-level import of a built-in or frozen module whatever the working directory.

`lift._admitted_import` is read through `declaration_findings` only for a name the declaration reads and its module
binds at top level; a nested import is never examined. So the end-to-end consequence of the origin rule is visible
through a module-level import, and not through a nested one.
"""

import pytest

from tests.correspondence import lift

pytestmark = pytest.mark.state_check

HEAD = "from tests.correspondence.lift import declares_lift\n\n"
BODY = (
    '@declares_lift(unit="mod.py:1-3")\n'
    "def declare(fixtures):\n"
    '    """Read the module bound above."""\n'
    '    return {{"m": {name}}}, lambda ns: 0\n'
)


@pytest.mark.parametrize("module", ["sys", "os", "itertools"])
@pytest.mark.parametrize("start", ["root", "outside"])
def test_a_declaration_reading_a_module_level_built_in_or_frozen_import_has_no_finding_from_any_directory(
    tmp_path, monkeypatch, module, start
):
    """Property: a declaration that reads a module-level `import {module}` draws no finding, with the working directory under the root or outside it.

    Class: the three modules of the reported defect (`sys`, `os`, `itertools`), imported at the top of the lift module
    and read by the declaration, from the checkout root and from a directory outside it. Why it is required: the
    runner's refusal reaches `_admitted_import` only for a module-level name, so this is the one path by which the
    origin rule changes a verdict a lift author sees; a nested-import test never reaches it and is green
    on the uncorrected function. Killed by: `NON_PATH_ORIGINS = ()`, which refuses the name at the root (the
    knowledge document's measured `module-level os findings=1`). Second correct implementation: classing the module
    with `sys.stdlib_module_names` before the path test passes the same six inputs.
    """
    root, outside = tmp_path / "checkout", tmp_path / "elsewhere"
    root.mkdir()
    outside.mkdir()
    monkeypatch.chdir(root if start == "root" else outside)
    source = HEAD + f"import {module}\n\n\n" + BODY.format(name=module)
    assert lift.declaration_findings(source, "declare", root) == []
