"""The generators of the synthetic input fixtures that unit lifts run on.

Each owning system writes one module here, named after its slug with hyphens as underscores (a system
`a-system` generates in `a_system.py`), and in it decorates each function that builds one input with
`tests.correspondence.fixtures.generates_fixture(name=..., seed=...)`. The function receives a
`numpy.random.Generator` seeded with the declared seed and returns a value of a kind
`tests.correspondence.value_codec` stores. `python -m tests.correspondence.fixtures generate --system
{slug} --name {name}` runs one generator once and writes its fixture; nothing in a test run does.
A system with no module here has declared no fixture yet.
"""
