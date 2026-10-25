"""The declarations through which a unit's stored legacy text is run unchanged, or as a reviewed adapted copy.

Each owning system writes one module here, named after its slug with hyphens as underscores (a system
`a-system` declares in `a_system.py`), and in it decorates each function that supplies the names a unit's
stored text reads with `tests.correspondence.lift.declares_lift(unit=...)`. The declaration receives its
loaded fixtures and returns the bindings and the reduction of the finished namespace. A system with no
module here has declared no lift. A hand-adapted copy of a stored text lives in `adapted/{system}/`, as a
text file with its diff beside it.
"""
