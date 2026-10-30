"""The equivalence tests: one test for each legacy unit whose code ran, comparing the new code with the unit's lift.

A refactor reminder in `tests/reminders/` fails until an equivalence test carrying the same unit key
replaces it. This directory holds those replacements, together with the support every one of them shares
(`_support.py`) and the checks over the whole directory (`test_recorded_rulings.py`, `test_lift_imports.py`).
It is a package so that a module here cannot collide in name with a module of `tests/reminders/`, which has no
`__init__.py`.
"""
