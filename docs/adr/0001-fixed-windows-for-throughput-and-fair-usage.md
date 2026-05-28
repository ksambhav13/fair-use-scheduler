# ADR 0001: Fixed Windows for Throughput and Fair Usage Counters

**Status**: Accepted  
**Date**: 2026-05-28

## Context

The Scheduler must track how many tasks a tenant has dispatched within a throughput window (minute/hour/day) and a fair usage window (one dimension higher). Two approaches exist: fixed windows that reset at calendar boundaries, and sliding windows that count over the last N seconds from now.

## Decision

Use fixed windows for both throughput and fair usage counters. Redis keys are structured as `rate:{tenant_id}:throughput:{window_start_epoch}` and `rate:{tenant_id}:fair_usage:{window_start_epoch}`, with TTL set to `2 × window_duration` to allow natural expiry.

## Consequences

**Benefits:**
- Simple Redis implementation: `INCR` + `EXPIRE` on a single key per tenant per window. No sorted sets needed.
- Counter keys are deterministic — any Scheduler instance can compute the current key independently from the clock.
- Counters can be rebuilt from `tasks.dispatched_at` after a Redis flush.

**Drawbacks:**
- Boundary bursting is possible: a tenant can dispatch at the end of one window and immediately at the start of the next, briefly exceeding the intended rate. At per-minute throughput scale this is a small effect and acceptable for a fair-usage policy guardrail (not a hard billing cap).

**Alternatives rejected:**
- Sliding windows (Redis sorted sets): more accurate but significantly more expensive — O(log N) per operation vs O(1), and counter rebuild from DB is more complex. The accuracy gain doesn't justify the cost for a coarse policy control.
