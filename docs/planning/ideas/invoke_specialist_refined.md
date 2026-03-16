---
impact: High
urgency: Medium
loe: Medium
risk: Low
priority: 2.18
priority-label: "Core priority"
created: 2026-03-15
description: A skill that refines a specialist prompt through a sequential meta-review loop before executing, producing higher-quality output for high-stakes invocations.
synergies:
  - prompts/instructions/skills/create-skill/SKILL.md  # downstream tool to build this skill
  - prompts/instructions/protocols/subagent-write-target.md  # write-target conventions the skill extends
---

# Invoke Specialist Refined

## Problem

Specialist invocations for high-stakes tasks often produce lower-quality output because the
initial prompt is written without domain review or LLM delivery optimization. This skill fills
that gap by running a three-phase refinement loop before execution: the target specialist
meta-reviews the prompt for domain coverage, the prompt-engineering specialist optimizes it for
LLM delivery, and then the target specialist executes against the refined prompt — with the
refined prompt preserved in the write-target document as a durable record.

## Approach Notes

**Three-phase loop (sequential, non-optional):**
1. User + agent co-draft the initial prompt
2. Target specialist meta-reviews the prompt for domain coverage and missing framework sections
3. Prompt-engineering specialist optimizes for LLM delivery (directive reframing, behavioral
   anchoring, output format constraints, tag embedding)
4. Target specialist executes the refined prompt and writes findings to the write-target document

**Write-target requirement:** Must be a permanent planning or audit doc — never a scratch file.
The skill should reject scratch paths explicitly. The refined prompt lives in an appendix of
that document, creating a durable record of exactly what was asked alongside the output.

**Model gate:** Check current model at invocation. Recommend Opus high-thinking (Claude Code)
or equivalent Codex thinking level. Surface as a prominent recommendation, not a hard block —
the user can override. This skill is intended only for high-stakes, non-trivial specialist
invocations; routine calls should use direct specialist invocation instead.

**Confirmation protocol:** The confirm-before-invoking rule (subagent-write-target.md) applies
to the first invocation. The refinement loop iterations (meta-review → PE → execute) are
continuation of an approved workflow and do not each require separate confirmation.

## Live Example

`docs/planning/refactors/2026-03-14_refactor_plan_audit.md` — Appendix A contains the refined
prompt; the `## Audit Findings` section is the SE specialist's output. The prompt-engineering
step produced the single highest-value change: reframing question bullets as directives.
