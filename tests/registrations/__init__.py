"""The registration modules through which equivalence tests reach new code.

Each owning system writes one module here, named after its slug with hyphens as underscores
(a system `a-system` registers in `a_system.py`), and in it decorates each
function that computes the new side of a legacy unit with
`tests.correspondence.binding.registers(unit="{unit key}")`. A system with no module here has
registered nothing yet, and its tests receive a placeholder that fails their comparison.
"""
