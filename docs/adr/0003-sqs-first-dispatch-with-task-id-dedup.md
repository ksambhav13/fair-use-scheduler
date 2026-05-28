# ADR 0003: SQS-First Dispatch with Task ID as Deduplication Key

**Status**: Accepted  
**Date**: 2026-05-28

## Context

Dispatching a task requires three non-atomic steps: (1) send message to SQS, (2) update task status to `dispatched` in PostgreSQL, (3) increment the Redis consumption counter. Any failure mid-sequence can leave the system in an inconsistent state, most dangerously causing a task to be dispatched twice.

## Decision

Send to SQS first, using the task's UUID as the SQS message deduplication ID. Then update the task status to `dispatched` in PostgreSQL. If the DB update fails, the Scheduler's next loop tick will re-select the same `pending` task and re-send to SQS — the deduplication ID ensures SQS absorbs the duplicate within the 5-minute dedup window. The Redis counter is incremented after the DB update; failures here are tolerated because `tasks.dispatched_at` is the source of truth for counter recovery.

## Consequences

**Benefits:**
- No new infrastructure. The SQS FIFO deduplication feature absorbs the re-dispatch case without an outbox table or relay process.
- Simpler failure path than DB-first (which requires a compensating update back to `pending` on SQS failure).

**Drawbacks:**
- The 5-minute SQS deduplication window is a hard constraint: if the DB update is delayed more than 5 minutes (e.g. prolonged DB outage), the task could be dispatched twice. This is an extreme edge case but a known limitation.
- Workers must be idempotent regardless, as SQS FIFO provides at-least-once (not exactly-once) delivery.

**Alternatives rejected:**
- Outbox pattern: fully reliable and avoids all duplicate risk, but requires a dedicated relay process, an additional DB table, and ongoing operational overhead — complexity not justified at this stage.
- DB-first with compensating update: logically sound but the rollback path (flip back to `pending` on SQS failure) is tricky to implement correctly under partial failures and concurrent scheduler instances.
