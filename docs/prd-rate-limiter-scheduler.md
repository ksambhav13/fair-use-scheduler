# PRD: Multi-Tenant Task Scheduler with Rate Limiting

**Status**: Ready for implementation  
**Date**: 2026-05-28

---

## Problem Statement

Multi-tenant systems often need to execute background tasks on behalf of tenants at controlled rates. Without enforcement, a single high-volume tenant can exhaust downstream capacity, starving other tenants and causing system instability. There is currently no mechanism to control how fast tasks are dispatched per tenant, nor to distinguish between a tenant's sustained baseline usage and abusive burst usage.

---

## Solution

A scheduler that reads tasks from a PostgreSQL task table in insertion order and dispatches them to a worker queue (Amazon SQS FIFO) at a rate governed by each tenant's configuration. Each tenant gets an isolated scheduling loop. Tasks are never rejected — they are delayed until the scheduler's dispatch interval allows them through. When a tenant exceeds their fair usage cap for the current window, the scheduler automatically switches to a lower throttled throughput for the remainder of that window.

---

## User Stories

1. As a tenant, I want my tasks to be executed in the order I submitted them, so that dependent work is not processed out of sequence.
2. As a tenant, I want my tasks to be executed at my configured normal throughput rate, so that I receive consistent, predictable processing capacity.
3. As a tenant, I want tasks to continue being accepted even when I am over limit, so that I never lose work — it just takes longer to process.
4. As a tenant, I want to know my tasks will eventually be processed even if I submit a large burst, so that I can rely on the system for high-volume workloads.
5. As a tenant who has exceeded my fair usage cap, I want my remaining tasks for that window to be processed at the throttled throughput, so that downstream systems are protected while my tasks still make progress.
6. As a tenant, I want the scheduler to automatically return to normal throughput at the start of the next fair usage window, so that I am not permanently penalised for a single burst.
7. As a platform operator, I want each tenant's processing rate to be fully isolated from other tenants, so that a high-volume tenant cannot starve or degrade service for others.
8. As a platform operator, I want to configure normal throughput, throttled throughput, and fair usage cap independently per tenant, so that I can apply different SLAs to different customers.
9. As a platform operator, I want throughput and fair usage to be configurable in per-minute, per-hour, or per-day units, so that I can match the policy to the tenant's workload profile.
10. As a platform operator, I want the scheduler to pick up configuration changes without a restart, so that I can adjust a misbehaving tenant's limits in real time.
11. As a platform operator, I want newly onboarded tenants to start being scheduled automatically without a deployment, so that onboarding is operational self-service.
12. As a platform operator, I want removed tenants' scheduling loops to stop automatically, so that idle loops do not waste resources.
13. As a platform operator, I want multiple scheduler instances to run simultaneously for high availability, so that a single instance failure does not pause all tenant processing.
14. As a platform operator, I want only one scheduler instance to hold the active loop for a given tenant at a time, so that throughput limits are enforced accurately and tasks are not double-dispatched.
15. As a platform operator, I want the scheduler to survive a Redis flush and recover accurate state from the task table, so that a cache failure does not cause permanent data loss or runaway dispatch.
16. As a platform operator, I want tasks dispatched to workers via a reliable queue with at-least-once delivery, so that no task is silently dropped.
17. As a platform operator, I want duplicate dispatch attempts (e.g. after a partial failure) to be absorbed without double-execution, so that worker idempotency is the only remaining correctness requirement.
18. As a worker, I want each dispatched message to carry the task ID and tenant ID, so that I can execute the task and report status back without additional lookups.
19. As a platform operator, I want task status (`pending`, `dispatched`, `completed`, `failed`) and `dispatched_at` to be persisted in the task table, so that I can audit processing history and rebuild Redis state after a failure.
20. As a platform operator, I want the scheduler to batch-fetch pending tasks from the database and drain them in memory, so that per-dispatch DB round-trips are minimised at high throughput.
21. As a platform operator, I want the batch fetch cursor to be persisted in Redis, so that re-scans of already-seen rows are avoided across loop iterations.
22. As a platform operator, I want the scheduler to fall back to a full status-based query if the Redis cursor is lost, so that correctness is preserved after a Redis flush even if performance temporarily degrades.
23. As a platform operator, I want idle tenant loops to back off with a short fixed sleep (1–2 seconds) rather than tight-polling the DB, so that empty tenants do not generate unnecessary database load.

---

## Implementation Decisions

### Schema

**`tasks` table**
- `id` (UUID, primary key)
- `tenant_id` (UUID, foreign key to tenants)
- `payload` (JSONB)
- `status` (enum: `pending`, `dispatched`, `completed`, `failed`)
- `created_at` (timestamp with timezone, set by producer)
- `dispatched_at` (timestamp with timezone, nullable, set by scheduler)

Index: `(tenant_id, created_at) WHERE status = 'pending'` — supports the FIFO batch query efficiently as the table grows.

**`tenant_configs` table**
- `tenant_id` (UUID, primary key)
- `normal_throughput` (integer — tasks per window)
- `throttled_throughput` (integer — tasks per window)
- `throughput_unit` (enum: `MINUTE`, `HOUR`, `DAY`)
- `fair_usage_cap` (integer — tasks per fair usage window)
- `enabled` (boolean)

The fair usage window is always one dimension higher than `throughput_unit`: MINUTE → HOUR, HOUR → DAY.

---

### Modules

**TaskRepository**  
Owns all PostgreSQL access for the task table. Fetches a batch of `pending` tasks in FIFO order for a given tenant using a cursor (`scheduler:{tenantId}:last_fetched_id`) stored in Redis. Falls back to a full status-based FIFO query when the cursor is absent. Marks tasks as `dispatched` with a timestamp. Provides a count query over `dispatched_at` for Redis counter recovery.

Interface: `fetchBatch(tenantId, batchSize)`, `markDispatched(taskId, dispatchedAt)`, `countDispatched(tenantId, windowStart, windowEnd)`

**ConsumptionCounter**  
Owns all Redis counter operations. Computes fixed-window Redis keys deterministically from tenant ID, window unit, and current instant. Increments counters atomically using `INCR`+`EXPIRE`. Exposes a seeding method for recovery after Redis flush.

Interface: `increment(tenantId, windowType)`, `get(tenantId, windowType)`, `seed(tenantId, windowType, count)`

Window key format: `rate:{tenantId}:{windowType}:{epochSeconds_of_window_start}`. TTL set to `2 × window_duration`.

**SchedulerLock**  
Owns the Redis distributed lock per tenant. Uses `SET key value NX PX ttl`. The lock TTL is `2 × dispatch_interval` of the current throughput mode. Exposes refresh (called after each successful dispatch) and release.

Interface: `tryAcquire(tenantId, instanceId)`, `refresh(tenantId, instanceId)`, `release(tenantId, instanceId)`

**TenantConfigLoader**  
Polls `tenant_configs` table on a fixed 60-second interval. On each poll, diffs the loaded config against the previous snapshot and fires change events (added, updated, removed). Provides the current config snapshot synchronously to other components.

Interface: `loadAll()`, `start()`, `stop()`, `addChangeListener(listener)`

**SqsDispatcher**  
Wraps the AWS SQS client. Sends a message to the single FIFO queue using `task.id` as the `MessageDeduplicationId` and `task.tenantId` as the `MessageGroupId`. Message body carries task ID and payload.

Interface: `dispatch(task)` → returns message ID or throws on failure

**TenantSchedulerLoop**  
Core orchestration per tenant. On each iteration:
1. Acquire (or verify holding) the Redis lock.
2. Read the fair usage counter — if over cap, use throttled throughput; else use normal throughput.
3. Read the throughput counter — if at cap for the current window, sleep until window resets; else proceed.
4. Drain next task from in-memory batch (refill from TaskRepository if empty; if no pending tasks, sleep idle backoff).
5. Dispatch via SqsDispatcher.
6. Update task status via TaskRepository.
7. Increment both counters via ConsumptionCounter.
8. Refresh the Redis lock.
9. Sleep for `dispatch_interval` before next iteration.

Interface: `run()` (blocking), `stop()` (signals graceful shutdown after current dispatch completes)

**SchedulerManager**  
Owns the `Map<tenantId, TenantSchedulerLoop>` registry. Subscribes to TenantConfigLoader change events. Starts a new loop thread for each added tenant; signals stop and removes the entry for each removed/disabled tenant. Starts TenantConfigLoader on application startup.

Interface: `start()`, `stop()`

---

### Failure handling

- **Dispatch failure (SQS unavailable)**: loop retries with exponential backoff; does not advance the batch cursor or increment counters until dispatch succeeds.
- **Redis counter loss**: on startup and on detected key absence, TaskRepository's `countDispatched` query is used to seed ConsumptionCounter for the current windows.
- **Redis cursor loss**: TaskRepository falls back to `WHERE status = 'pending' ORDER BY created_at` — already-dispatched tasks are excluded by status, so no double-dispatch occurs.
- **Scheduler instance crash**: Redis lock TTL expires; another running instance acquires the lock and resumes the tenant's loop.

---

### Redis key inventory

| Key | Purpose | TTL |
|---|---|---|
| `scheduler:{tenantId}:lock` | Distributed loop lock | `2 × dispatch_interval` |
| `scheduler:{tenantId}:last_fetched_id` | Batch fetch cursor | 24 hours |
| `rate:{tenantId}:throughput:{windowStart}` | Throughput window counter | `2 × throughput_window_duration` |
| `rate:{tenantId}:fair_usage:{windowStart}` | Fair usage window counter | `2 × fair_usage_window_duration` |

---

## Testing Decisions

A good test verifies observable external behaviour, not internal implementation. Tests should assert on task status changes in the DB, counter values in Redis, and messages appearing in (or absent from) the SQS queue — not on which internal methods were called.

**TaskRepository** — integration tests with Testcontainers (PostgreSQL). Cover: FIFO ordering, cursor-based pagination, cursor fallback when absent, `markDispatched` status transition, `countDispatched` accuracy.

**ConsumptionCounter** — integration tests with Testcontainers (Redis) or embedded Redis. Cover: increment and read within a window, key expiry across window boundaries, seed/recovery path, correct key computation for each window unit.

**TenantSchedulerLoop** — integration tests with real or embedded Redis, Testcontainers PostgreSQL, and a mock SqsDispatcher. Cover: normal throughput respected (N dispatches per window, no more), switch to throttled throughput when fair usage cap is hit, return to normal throughput after window reset, idle backoff when no pending tasks, correct task status transitions end-to-end.

---

## Out of Scope

- Worker implementation — workers consume from SQS independently; this PRD covers only the scheduler side.
- Task ingestion API — external producers write directly to the task table.
- Tenant management UI or API for creating/modifying `tenant_configs`.
- Per-tenant SQS queues or worker pool scaling — all tenants share one SQS FIFO queue.
- Sliding window rate limiting — fixed windows only.
- Dead-letter queue handling for failed worker executions.
- Metrics, dashboards, or alerting on scheduler throughput.

---

## Further Notes

- Workers must be idempotent. SQS FIFO deduplication covers the 5-minute window after dispatch, but at-least-once delivery means duplicate messages outside that window are possible.
- The fair usage policy is a coarse guardrail, not a hard billing cap. Fixed-window boundary bursting (up to `2 × throughput_rate` tasks across a window boundary) is an accepted trade-off for implementation simplicity.
- Three ADRs document the key non-obvious decisions: `docs/adr/0001` (fixed windows), `docs/adr/0002` (Redis lock), `docs/adr/0003` (SQS-first dispatch).
