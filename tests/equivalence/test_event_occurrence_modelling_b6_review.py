"""Reviewer test: the declared `draws` of the `_b6_simulate_from_vincop.py` lifts of event-occurrence-modelling match the unit table."""

from tests.correspondence import lift

# (unit, part) -> `draws`, as the approved shape's unit table states for rows 30 to 37 (row 36 has a value and a draw-law test)
TABLE = {
    ("_b6_simulate_from_vincop.py:100-216", None): "none",
    ("_b6_simulate_from_vincop.py:217-233", None): "none",
    ("_b6_simulate_from_vincop.py:384-392", None): "none",
    ("_b6_simulate_from_vincop.py:399-455", None): "none",
    ("_b6_simulate_from_vincop.py:456-478", None): "none",
    ("_b6_simulate_from_vincop.py:479-488", None): "seeded",
    ("_b6_simulate_from_vincop.py:479-488", "draw-law"): "seeded",
    ("_b6_simulate_from_vincop.py:627-745", None): "none",
}


def test_the_declared_draws_of_each_b6_lift_equal_the_unit_table():
    """Property: for every (unit, part) of rows 30 to 37 the lift's declared `draws` equals the table's cell.

    Class: each declaration of the system's `_b6` units; the violating input is the value lift of `479-488`, whose
    seeded `sample(random_state=24)` the table calls `seeded`. Reachability: `lift.discover` lists every declaration.
    Why: the stored-value check keys on `draws`, and the Definition of Done's TO-5 line compares these triples with
    the table by set comparison, so a wrong cell moves a count instead of failing there. The assertion turns on the
    declared value, which the current and a correct declaration both carry. Killed by: leaving `draws` at its default
    on the seeded value lift. Second correct implementation: reading the cell from the text's `random_state` use.
    """
    found = {
        (d.unit, d.part): d.draws
        for d in lift.discover()
        if d.system == "event_occurrence_modelling" and d.unit.startswith("_b6_")
    }
    assert found == TABLE, {
        k: (found.get(k), TABLE.get(k)) for k in set(found) | set(TABLE) if found.get(k) != TABLE.get(k)
    }
