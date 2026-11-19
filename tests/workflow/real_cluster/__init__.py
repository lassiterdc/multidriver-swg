"""Measurement material for running the toolkit's command on a real cluster.

The files here are the toolkit's half of a measurement: a workflow that wraps the toolkit's own workflow file
and records where it parses and where its handlers run, a small workflow whose licence rule is a checkpoint, a
launcher that starts either one with the command's own bindings, and a collector that prints what a run left
behind. They carry no machine value. The values of a measurement and its records belong to the estate.
"""
