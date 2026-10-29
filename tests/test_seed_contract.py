"""Check the seed contract's declaration: the generator is required, the validator refuses the two forbidden forms, and the module draws nothing.

The contract is worth having only if a sibling cannot quietly opt out of it, so each check here is
written against the shape an opt-out takes: a generator parameter that carries a default, a
validator that refuses nothing, a docstring that stops stating the ruling, and a module that starts
drawing on its own. Every check names the wrong implementation it is red under, because a check
that cannot fail for the reason it claims constrains nothing.
"""

from __future__ import annotations

import ast
import inspect
import typing
from collections.abc import Mapping
from pathlib import Path

import numpy
import pytest

import multidriver_swg.run_configuration.seed_contract as seed_contract
from multidriver_swg.run_configuration import SeededCallable, SeedGenerator, require_seed

# The two statements the ruling is made of, quoted from the contract rather than paraphrased: the
# checks below assert these exact sentences appear in the module's docstring.
OPTIONALITY_RULING = (
    "no default, no fallback to an unseeded draw — seeding is always a conscious, user-supplied decision"
)
PROPAGATION_CONVENTION = "the generator is threaded through call signatures, never process-global state"


def _dotted(node: ast.expr) -> str:
    """The dotted name an attribute chain reads, or the bare name where the chain ends in a name."""
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def _numpy_module_names(tree: ast.AST) -> dict[str, str]:
    """Each name `tree` binds to numpy or a numpy submodule, mapped to the canonical dotted path it stands for.

    The map is keyed by the name the import BINDS, not by the statement's own dotted path, and that
    is what makes the draw scan indifferent to how numpy was imported. `import numpy.random` binds
    the top-level `numpy`, so a draw written `numpy.random.seed(0)` begins at `numpy`; only `as`
    rebinds the whole path. Keying by the statement's path instead leaves that spelling
    unregistered, because no call begins with `numpy.random` as a bare name. A from-import is keyed
    the same way, so `from numpy.random import seed` registers `seed` itself, and it must be
    accepted from any `numpy.*` module rather than from `numpy` alone — the random submodule is the
    one whose members a draw would be reached through.
    """
    names: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "numpy" or alias.name.startswith("numpy."):
                    bound = alias.asname or alias.name.split(".")[0]
                    names[bound] = alias.name if alias.asname else "numpy"
        elif (
            isinstance(node, ast.ImportFrom)
            and node.level == 0
            and node.module is not None
            and (node.module == "numpy" or node.module.startswith("numpy."))
        ):
            for alias in node.names:
                names[alias.asname or alias.name] = f"{node.module}.{alias.name}"
    return names


def _draw_call_lines(source: str) -> list[int]:
    """Line numbers of every call into numpy's random module in `source`, however numpy was imported."""
    tree = ast.parse(source)
    numpy_names = _numpy_module_names(tree)
    lines = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        parts = _dotted(node.func).split(".")
        if parts[0] in numpy_names:
            canonical = ".".join([numpy_names[parts[0]], *parts[1:]])
            if canonical == "numpy.random" or canonical.startswith("numpy.random."):
                lines.append(node.lineno)
    return lines


def _generator_bindings(namespace: Mapping[str, object]) -> set[str]:
    """The names in `namespace` bound to a `numpy.random.Generator` instance."""
    return {name for name, value in namespace.items() if isinstance(value, numpy.random.Generator)}


def test_the_protocol_requires_a_generator_parameter_with_no_default():
    """Property: `SeededCallable.__call__` declares `rng` with no default, annotated `SeedGenerator` and not an optional union.

    Class: NEW CAPABILITY — plausible wrong implementation is a Protocol declaring
    `rng: SeedGenerator | None = None`, which is the legacy `seeds = None` shape and satisfies a
    no-default Protocol structurally. Reachability: any declaration of the Protocol reaches this
    failure, because the whole declaration is read. Both-states anchor: the assertions turn on
    `Parameter.default is Parameter.empty` and on the resolved annotation object, which are
    properties of the declaration and not wording this test introduces. Killed by: giving `rng` a
    default, or annotating it as optional. Second correct implementation: declaring the parameter
    as `numpy.random.Generator` directly rather than through the `SeedGenerator` alias, or making
    it positional-only — the test reads the parameter by name and resolves the annotation, so
    neither form is refused.
    """
    parameters = inspect.signature(SeededCallable.__call__).parameters
    assert "rng" in parameters, sorted(parameters)
    assert parameters["rng"].default is inspect.Parameter.empty

    hint = typing.get_type_hints(SeededCallable.__call__)["rng"]
    assert hint is SeedGenerator, hint
    assert type(None) not in typing.get_args(hint), hint


def test_require_seed_refuses_an_absent_or_defaulted_generator_and_admits_a_required_one():
    """Property: `require_seed` raises `TypeError` naming the function and `rng` for both forbidden forms, and returns None for the required form.

    Class: NEW CAPABILITY — plausible wrong implementation is a `require_seed` that returns None
    for every input, a validator that validates nothing; the mirror-image wrong implementation
    refuses every input, which is what the admit arm is here to catch. Reachability: the input
    class is any callable, and all three of its members are exercised — absent `rng`, defaulted
    `rng`, required `rng` — so the refusals are shown to discriminate rather than to refuse
    everything. Both-states anchor: the assertions turn on whether `TypeError` is raised and on
    whether the message names the function and the parameter, both of which the contract requires
    rather than wording this test introduces. Killed by: returning None unconditionally, or
    refusing the required form. Second correct implementation: a validator that builds the
    parameter mapping once and compares `default is not Parameter.empty`, or one that reads
    `inspect.getfullargspec` instead of `Signature` — the test asserts substrings of the message,
    never its wording.
    """

    def without_seed(count: int) -> int:
        return count

    def with_default(rng: SeedGenerator = None) -> int:
        return 0

    def with_required(rng: SeedGenerator) -> int:
        return 0

    with pytest.raises(TypeError) as absent:
        require_seed(without_seed)
    assert "without_seed" in str(absent.value), str(absent.value)
    assert "rng" in str(absent.value), str(absent.value)

    with pytest.raises(TypeError) as defaulted:
        require_seed(with_default)
    assert "with_default" in str(defaulted.value), str(defaulted.value)
    assert "rng" in str(defaulted.value), str(defaulted.value)

    assert require_seed(with_required) is None


def test_the_module_docstring_states_the_ruling_and_the_propagation_convention():
    """Property: the module docstring carries the optionality ruling and the propagation convention verbatim.

    Class: NEW CAPABILITY — plausible wrong implementation is a docstring that describes the
    interface and stops there, dropping the two statements the declaration is the deliverable for.
    Reachability: any version of the docstring is reached, because the whole of it is searched.
    Both-states anchor: the assertion turns on the presence of the contract's own fixed sentences,
    which the shape fixes as deliverable text, not on wording this test introduces. Killed by:
    deleting or rephrasing either sentence. Second correct implementation: the sentences at any
    position in any surrounding prose — the test asserts containment, so it constrains neither
    order nor wording around them.
    """
    docstring = seed_contract.__doc__ or ""
    assert OPTIONALITY_RULING in docstring
    assert PROPAGATION_CONVENTION in docstring


def test_the_module_draws_no_number_and_binds_no_module_level_generator(tmp_path: Path):
    """Property: `seed_contract` calls nothing in numpy's random module and binds no module-level generator.

    Class: NEW CAPABILITY — plausible wrong implementation is a module-level
    `_DEFAULT_GENERATOR = numpy.random.default_rng()`, or an `numpy.random.seed(0)` at import,
    either of which makes an unseeded or shared draw reachable from a caller that never supplied a
    generator. Reachability: the input class is any source text, and the controls below are members
    of it that DO draw, so a clean result over the module is the instrument finding nothing rather
    than not looking. Both-states anchor: the assertions turn on whether the syntax tree holds a
    call rooted at numpy's random module, and on whether a module global is a `Generator` instance
    — properties of the code, not of this test's wording. Killed by: the module drawing, at any
    indentation and through any import of numpy. Second correct implementation: importing numpy
    under an alias and still never calling into `random`, or holding the `Generator` CLASS (as
    `SeedGenerator` does) rather than an instance — both pass, because the scan resolves aliases
    and the binding arm tests for instances. Not established: a draw made through a name the module
    rebinds locally, or through another module that wraps numpy; the source census over the
    package, which the module docstring names, is the instrument with that wider population.
    """
    source = Path(seed_contract.__file__ or "").read_text(encoding="utf-8")
    assert _draw_call_lines(source) == []

    control = tmp_path / "draws.py"
    control.write_text(
        "import numpy as npy\n\n\ndef draw(count):\n    return npy.random.default_rng(0).normal(size=count)\n",
        encoding="utf-8",
    )
    assert _draw_call_lines(control.read_text(encoding="utf-8")), "the scan found no draw in a file that draws"

    assert _generator_bindings(vars(seed_contract)) == set()
    assert _generator_bindings({"ready": numpy.random.Generator(numpy.random.PCG64(0))}) == {"ready"}


def test_the_draw_scan_finds_a_draw_reached_through_every_numpy_import_spelling():
    """Property: the draw scan reports a draw reached through ANY import that binds a name for numpy's random module, and reports nothing for a same-named function that is not numpy's.

    Class: NEW CAPABILITY — plausible wrong implementations are (a) an import resolver that
    registers only the names an `ast.Import` statement binds at its own dotted depth and accepts an
    `ast.ImportFrom` only where the imported module is exactly `numpy`, so a draw reached through
    `from numpy.random import seed` or through `import numpy.random` is invisible, and (b) a
    resolver that keys by the bound name but canonicalizes every `ast.Import` alias to the
    top-level package, so an `as`-rebound submodule loses its path. Reachability: the input class is
    any source text whose route to numpy's random module is an import binding a name other than the
    bare top-level `numpy`; seven members of it are exercised below, two of which the scan reported
    clean when this test was first written, so the failure is shown to be reachable rather than
    argued.
    Both-states anchor: every assertion turns on whether the scan returns a non-empty list of line
    numbers, a behaviour the scan has in both the current and the repaired code; none asserts on a
    message, a helper name or any other wording a repair would introduce. Why this property is
    required: the module's docstring promises the declaration "seeds nothing at import", and the
    scan is the only instrument that holds that promise — a scan blind to a spelling lets a
    `from numpy.random import seed` at import land green, which is exactly the unseeded draw the
    optionality ruling forbids, and nothing else in this package reads for it. Killed by: (a)
    registering only the names `ast.Import` binds under their own dotted name and accepting
    `ast.ImportFrom` only where its module is exactly `numpy`; (b) canonicalizing an `ast.Import`
    alias to `numpy` unconditionally instead of to the statement's own path when it is
    `as`-rebound. Second correct implementation: a scope-aware resolver built on `symtable` and
    per-scope binding analysis rather than a flat module-level name map; it reports the same seven
    draw-bearing spellings and the same clean result for both negative arms, because the test
    asserts only the verdict and never how the resolver reached it.

    Negative arms inside the test: a module-level function named `seed` that is NOT numpy's, and a
    `numpy.random.seed(0)` call in a source that imports nothing, must both go unflagged — so a
    resolver that flags every call named `seed`, or every source carrying the characters
    `numpy.random`, is shown to be wrong rather than tight.
    """
    spellings = {
        "import numpy as npy": "import numpy as npy\n\n\ndef draw():\n    npy.random.seed(0)\n",
        "from numpy import random": "from numpy import random\n\n\ndef draw():\n    random.seed(0)\n",
        "from numpy.random import seed": "from numpy.random import seed\n\n\ndef draw():\n    seed(0)\n",
        "import numpy.random": "import numpy.random\n\n\ndef draw():\n    numpy.random.seed(0)\n",
        "import numpy.random as nr": "import numpy.random as nr\n\n\ndef draw():\n    nr.seed(0)\n",
        "from numpy import random as rnd": "from numpy import random as rnd\n\n\ndef draw():\n    rnd.seed(0)\n",
        "from numpy.random import seed as s": "from numpy.random import seed as s\n\n\ndef draw():\n    s(0)\n",
    }
    for spelling, source in spellings.items():
        assert _draw_call_lines(source), (
            f"the scan found no draw in a source that reached numpy.random through {spelling!r}"
        )

    not_numpy = {
        "a local function named seed, with no numpy import": (
            "def seed(count):\n    return count\n\n\ndef draw():\n    seed(0)\n"
        ),
        "a numpy.random call in a source that imports nothing": "def draw():\n    numpy.random.seed(0)\n",
    }
    for arm, source in not_numpy.items():
        assert _draw_call_lines(source) == [], f"the scan flagged a source where numpy is not reachable: {arm}"
