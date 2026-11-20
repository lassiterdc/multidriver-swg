"""Workflow: the run's rule table, and the workflow file that renders it.

This package holds what a run's engine executes. Its first component is the graph, re-exported from
`graph`: `GRAPH` states the run's step order and data dependencies in one place, each row naming the
compute system that owns it, the outputs it reads and the artifacts it declares, and four pure functions
answer the questions a renderer and a target-list calculation ask of it. `RuleSpec` is the row's type,
and `IDENTITY_FILE` names the file every rule writes beside its outputs; the constant is deliberately not
re-exported, matching how the other run-configuration constants stay in the modules that own them.

The package will grow the workflow file that renders the table, the graph-start refusals, the run-start
record and the command; each of those is a later change's surface and none of it is implied here.
"""

from __future__ import annotations

from multidriver_swg.workflow.graph import (
    GRAPH,
    GraphLookupError,
    RuleSpec,
    downstream_of,
    judgement_rules,
    output_classes,
    rule_named,
)

__all__ = [
    "GRAPH",
    "GraphLookupError",
    "RuleSpec",
    "downstream_of",
    "judgement_rules",
    "output_classes",
    "rule_named",
]
