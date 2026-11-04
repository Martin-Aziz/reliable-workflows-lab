# Product brief: Reliable Workflows Lab (portfolio exercise)

## Intended user

Backend engineers evaluating how a job system behaves when requests are repeated or workers fail. A secondary audience is engineering interviewers reviewing correctness and trade-offs.

## Problem hypothesis

Developer tutorials often make background processing look trivial, while production failure modes (duplicate effects, lost work, unclear retries) are underexplained. A reproducible failure-oriented demo could help engineers practice this reasoning.

**This is a hypothesis, not validated user research.** There are no claimed customers, pilots, or commercial metrics.

## North-star learning outcome

A developer can explain the difference among request idempotency, exclusive live ownership, at-least-once execution, and exactly-once external effects after running the demo.

## Phase 1 success indicators

- Fresh clone can build and run without external services.
- Job lifecycle observable through HTTP.
- Concurrency and failure invariants backed by tests.
- Limitations prominently documented.

## Next user-discovery questions

1. Which worker failure modes are hardest to debug in your current system?
2. How do you currently prevent duplicate side effects?
3. Would a guided fault-injection playground be useful, or is a reusable library more valuable?
4. Is PostgreSQL-first a sensible default for your workload?

## Prioritization decision

Phase 1 prioritizes verifiable delivery semantics over UI polish and infrastructure breadth. Phase 2 prioritizes durability before adding dashboards. This is a product trade-off to validate, not evidence of user demand.
