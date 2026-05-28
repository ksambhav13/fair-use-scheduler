# Architecture: Multi-Tenant Task Scheduler

## Overview

This service is a **scheduler**, not a rate-limiter in the gatekeeper sense. It never rejects tasks. External producers insert tasks into a PostgreSQL table; the scheduler reads them in FIFO order and dispatches them to an Amazon SQS FIFO queue at a rate controlled by per-tenant configuration. Workers consume from SQS independently.

The scheduler enforces two layers of throughput control per tenant:

1. **Throughput limit** — a cap on tasks dispatched per window (minute, hour, or day).
2. **Fair usage policy** — a cap on tasks dispatched per higher-dimension window (hour for per-minute configs, day for per-hour). When breached, the tenant is switched to a lower throttled throughput for the remainder of that window.

---

## System Context

```
┌─────────────────────────────────────────────────────────────┐
│                        rate-limiter                         │
│                                                             │
│  ┌──────────────────┐      ┌──────────────────────────────┐ │
│  │  External        │      │  Scheduler (this service)    │ │
│  │  Producers       │─────▶│                              │ │
│  │  (write tasks    │  DB  │  reads tasks, enforces rate  │ │
│  │   directly to    │      │  limits, dispatches to SQS   │ │
│  │   PostgreSQL)    │      └────────────┬─────────────────┘ │
│  └──────────────────┘                   │ SQS               │
│                                         ▼                   │
│                              ┌──────────────────┐           │
│                              │  Workers         │           │
│                              │  (consume SQS,   │           │
│                              │   execute tasks) │           │
│                              └──────────────────┘           │
└─────────────────────────────────────────────────────────────┘

External state:
  PostgreSQL  — task table, tenant_configs table
  Redis       — counters, locks, batch cursor
  Amazon SQS  — single FIFO queue (tasks.fifo)
```

---

## Component Architecture

```
SchedulerManager
  │  @PostConstruct start()  ←──  TenantConfigLoader (polls DB every 60s)
  │  @PreDestroy   stop()
  │
  │  Map<tenantId, Thread>  (one virtual thread per enabled tenant)
  │
  └──▶ TenantSchedulerLoop  [per tenant]
         │
         ├── TenantConfigLoader    reads live tenant config each iteration
         ├── SchedulerLock         Redis SET NX PX distributed lock
         ├── ConsumptionCounter    Redis INCR+EXPIRE counters
         ├── TaskRepository        PostgreSQL FIFO batch fetch + cursor
         └── SqsDispatcher         AWS SQS FIFO sendMessage
```

### SchedulerManager

Spring `@Component` that owns the `Map<UUID, TenantSchedulerLoop>` registry. On `@PostConstruct`:

1. Registers a change listener on `TenantConfigLoader`.
2. Calls `TenantConfigLoader.start()`, which fires an `ADDED` event for every currently enabled tenant.
3. The change listener starts a virtual thread per added tenant.

On `@PreDestroy`, signals all loops to stop and interrupts their threads.

Config changes (new tenants added, tenants disabled/removed) are handled live without restart.

### TenantConfigLoader

Polls `tenant_configs WHERE enabled = true` every 60 seconds on a virtual-thread executor. On each poll it diffs the fresh snapshot against the previous one and fires `ConfigChangeEvent` records (`ADDED`, `UPDATED`, `REMOVED`) to registered listeners. The current snapshot is exposed via `getConfig(tenantId)` for use inside loops on each iteration.

### TenantSchedulerLoop

The core dispatch loop. Each instance is scoped to one tenant and runs on its own virtual thread. Each loop iteration:

```
1. Reload config from TenantConfigLoader snapshot.
   └─ If absent or disabled → sleep 5s, continue.

2. Compute lock TTL = max(30s, dispatchInterval × lockTtlMultiplier).
   Attempt Redis lock (SET NX PX).
   └─ If not acquired → sleep 1s, retry (another instance holds the loop).

3. Check fair usage counter.
   └─ If fairUsageCount ≥ fairUsageCap → isThrottled = true.
   (Skipped for DAY throughput unit — no higher dimension exists.)

4. Select throughput:
   isThrottled  → throttledThroughput
   !isThrottled → normalThroughput

5. Compute dispatchInterval = windowDuration / currentThroughput.

6. Check throughput counter for current window.
   └─ If throughputCount ≥ currentThroughput → sleep until window resets, continue.

7. Peek next task from in-memory batch (refill from DB if empty).
   └─ If no pending tasks → sleep idleSleepMs (default 1.5s), continue.

8. record start = now()

9. sqsDispatcher.dispatch(task)          ← SQS first (ADR-0003)
10. taskRepository.markDispatched(task)  ← conditional UPDATE WHERE status='PENDING'
    └─ Returns false if another instance already dispatched it → skip counter increments.

11. If dispatched:
    consumptionCounter.incrementThroughput(...)
    consumptionCounter.incrementFairUsage(...)
    schedulerLock.refresh(...)

12. batch.poll()  ← advance past this task regardless of outcome

13. sleep max(0, dispatchInterval - elapsed)
```

### TaskRepository

Wraps `TaskJpaRepository` (Spring Data JPA) and `StringRedisTemplate`. Manages the batch fetch cursor.

**Batch fetch** (`fetchBatch`):
- Reads cursor from Redis key `scheduler:{tenantId}:cursor` (format: `{epochMilli}|{uuid}`).
- With cursor: queries `WHERE status='PENDING' AND (created_at > ? OR (created_at = ? AND id::text > ?))` ordered by `(created_at, id)`.
- Without cursor (key absent or Redis flush): full scan `WHERE status='PENDING' ORDER BY created_at, id`.

**Mark dispatched** (`markDispatched`):
- Issues `UPDATE tasks SET status='DISPATCHED', dispatched_at=? WHERE id=? AND status='PENDING'`.
- Returns `true` only if 1 row was updated (optimistic concurrency — prevents double-counting when SQS dedup absorbs a re-dispatch).
- On success, advances the cursor in Redis (24-hour TTL).

### ConsumptionCounter

Manages four Redis keys per tenant: two counters (throughput window, fair usage window) and their keys are deterministic from the current wall-clock window boundary.

**Key format:**
- `rate:{tenantId}:throughput:{windowStartEpochSec}`
- `rate:{tenantId}:fair_usage:{windowStartEpochSec}`

**Increment** uses a Lua script that atomically increments and sets the TTL only on the first write, avoiding a race between `INCR` and `EXPIRE`:
```lua
local val = redis.call('INCR', KEYS[1])
if val == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return val
```

TTL is set to `2 × window duration` so counter keys survive across the boundary without stale reads.

**Seed** (`seedThroughput`, `seedFairUsage`) writes a count directly with `SET key value EX ttl`. Used during Redis recovery to restore counters from `tasks.dispatched_at`.

### SchedulerLock

Redis distributed lock using `SET NX PX` (acquire), a Lua `GET+EXPIRE` (refresh), and a Lua `GET+DEL` (release). Lua scripts are used for refresh and release to ensure atomicity — a stale instance cannot extend or release a lock it no longer owns.

Lock key: `scheduler:{tenantId}:lock`  
Value: `instanceId` (from `HOSTNAME` env var, default `local`)  
TTL: `max(30s, dispatchInterval × lockTtlMultiplier)`

The lock is refreshed after every successful dispatch. If an instance crashes, the lock expires within TTL seconds and another instance takes over.

### SqsDispatcher

Sends a JSON message to the single SQS FIFO queue with:
- `MessageGroupId` = `tenantId` — preserves per-tenant ordering within SQS.
- `MessageDeduplicationId` = `taskId` — SQS absorbs duplicate sends within 5 minutes.

Message body:
```json
{ "taskId": "...", "tenantId": "...", "payload": "..." }
```

AWS credentials are sourced from `DefaultCredentialsProvider` (env vars → `~/.aws` → EC2/ECS instance role). No credentials are hardcoded or stored in configuration.

---

## Data Model

### `tasks` table

| Column | Type | Notes |
|---|---|---|
| `id` | UUID | Set by producer |
| `tenant_id` | UUID | Foreign key to tenants |
| `payload` | TEXT | Raw JSON, passed through to workers |
| `status` | task_status enum | `PENDING → DISPATCHED → COMPLETED\|FAILED` |
| `created_at` | TIMESTAMPTZ | Set by producer; defines FIFO order |
| `dispatched_at` | TIMESTAMPTZ | Set by scheduler; used for counter recovery |

**Indexes:**
- `idx_tasks_pending_fifo ON (tenant_id, created_at, id) WHERE status='PENDING'` — partial index covering the batch fetch query. Only pending rows are indexed, keeping it compact as the table grows.
- `idx_tasks_dispatched_at ON (tenant_id, dispatched_at) WHERE dispatched_at IS NOT NULL` — used by counter recovery queries (`countDispatched`).

### `tenant_configs` table

| Column | Type | Notes |
|---|---|---|
| `tenant_id` | UUID | Primary key |
| `normal_throughput` | INTEGER | Tasks per throughput window |
| `throttled_throughput` | INTEGER | Tasks per window after fair usage breach |
| `throughput_unit` | throughput_unit enum | `MINUTE`, `HOUR`, or `DAY` |
| `fair_usage_cap` | INTEGER | Tasks per fair usage window |
| `enabled` | BOOLEAN | Disabled tenants have no active loop |

**Fair usage window** is always one dimension higher than `throughput_unit`:
- `MINUTE` → fair usage tracked per `HOUR`
- `HOUR` → fair usage tracked per `DAY`
- `DAY` → no fair usage check (no higher unit)

---

## Redis Key Inventory

| Key | Type | Value | TTL | Owner |
|---|---|---|---|---|
| `scheduler:{tenantId}:lock` | String | `instanceId` | `max(30s, dispatchInterval × multiplier)` | SchedulerLock |
| `scheduler:{tenantId}:cursor` | String | `{epochMilli}\|{uuid}` | 24 hours | TaskRepository |
| `rate:{tenantId}:throughput:{windowStartEpochSec}` | String | integer count | `2 × throughputWindow` | ConsumptionCounter |
| `rate:{tenantId}:fair_usage:{windowStartEpochSec}` | String | integer count | `2 × fairUsageWindow` | ConsumptionCounter |

All keys use tenant UUIDs (not user-controlled strings) to prevent key injection.

---

## Concurrency Model

### Per-instance

Each enabled tenant gets one Java virtual thread (Project Loom). Virtual threads are cheap enough to run one per tenant at scale without a thread pool. The `SchedulerManager` holds `Map<UUID, Thread>` — one entry per active tenant.

### Cross-instance (HA)

Multiple scheduler instances can run simultaneously. Only one instance holds the active loop for a given tenant at a time, enforced by the Redis distributed lock. If the lock holder crashes, the lock expires and another instance acquires it.

```
Instance A          Instance B
    │                   │
    ├─ acquire lock ✓   │
    │  (loop runs)      ├─ tryAcquire → false, sleep 1s
    │                   ├─ tryAcquire → false, sleep 1s
    X  (crash)          │
    │                   │     [TTL expires]
                        ├─ tryAcquire → true ✓
                        │  (loop resumes)
```

### Dispatch atomicity

Three operations must happen per dispatch — none are atomic across all three:

1. `SqsDispatcher.dispatch(task)` — SQS send
2. `TaskRepository.markDispatched(task)` — DB `UPDATE WHERE status='PENDING'`
3. `ConsumptionCounter.increment*` — Redis `INCR`

**Ordering and failure handling (ADR-0003):**

- **SQS first**: if the DB update fails, the loop retries on the next iteration and re-sends to SQS. The `MessageDeduplicationId = taskId` absorbs the duplicate within the 5-minute SQS dedup window.
- **DB second**: `markDispatched` uses `WHERE status='PENDING'` — returns `false` if the task was already dispatched. Counter increments are skipped in that case.
- **Redis last**: counter increment failures are tolerated. The `tasks.dispatched_at` column is the source of truth and can seed counters on recovery.

---

## Failure Modes and Recovery

| Failure | Behaviour | Recovery |
|---|---|---|
| **Redis flush** | Counters and cursor reset to zero | Counters rebuild from `tasks.dispatched_at` via `TaskRepository.countDispatched` + `ConsumptionCounter.seed*`. Cursor falls back to full `WHERE status='PENDING'` scan (already-dispatched tasks excluded by status). |
| **Scheduler instance crash** | Lock TTL expires within `max(30s, dispatchInterval × multiplier)` | Another running instance acquires the lock and resumes the tenant's loop. |
| **SQS unavailable** | `SqsDispatcher.dispatch` throws | Loop catches the exception, logs it, sleeps 1s, retries on the next iteration. No counters are incremented, so the task is retried without consuming throughput budget. |
| **PostgreSQL unavailable** | `fetchBatch` or `markDispatched` throws | Same as SQS — exception caught, 1s sleep, retry. The in-memory batch is not drained, so the same task is re-attempted. |
| **Task already dispatched by another instance** | `markDispatched` returns 0 rows → `false` | Loop skips counter increments, polls the task from the in-memory batch, and moves to the next task. SQS dedup window absorbs the duplicate message. |
| **Tenant disabled mid-loop** | `TenantConfigLoader` fires `UPDATED` event with `enabled=false` | `SchedulerManager` calls `stopLoop(tenantId)`: sets `running=false`, interrupts the thread. |

---

## Throughput Modes

### Normal mode

Active when `fairUsageCount < fairUsageCap` for the current fair usage window.

```
dispatchInterval = windowDurationMs / normalThroughput
```

Example: 10 tasks/minute → dispatch one task every 6 seconds.

### Throttled mode

Active for the remainder of the fair usage window once `fairUsageCount ≥ fairUsageCap`.

```
dispatchInterval = windowDurationMs / throttledThroughput
```

Example: 3 tasks/minute → dispatch one task every 20 seconds.

The tenant automatically returns to normal mode at the start of the next fair usage window (fixed-window reset, not sliding).

### Window cap reached

If `throughputCount ≥ currentThroughput` within the current window (all slots consumed), the loop sleeps until the window boundary:

```
sleepMs = nextWindowStart - now
        = ((now / windowMs) * windowMs + windowMs) - now
```

---

## Configuration Reference

All values are in `application.yml` and overridable via environment variables.

| Property | Env var | Default | Description |
|---|---|---|---|
| `spring.datasource.url` | `DB_URL` | `jdbc:postgresql://localhost:5432/ratelimiter` | PostgreSQL JDBC URL |
| `spring.datasource.username` | `DB_USER` | `postgres` | DB username |
| `spring.datasource.password` | `DB_PASSWORD` | `postgres` | DB password |
| `spring.data.redis.host` | `REDIS_HOST` | `localhost` | Redis host |
| `spring.data.redis.port` | `REDIS_PORT` | `6379` | Redis port |
| `rate-limiter.sqs.queue-url` | `SQS_QUEUE_URL` | — | SQS FIFO queue URL |
| `rate-limiter.sqs.region` | `AWS_REGION` | `us-east-1` | AWS region |
| `rate-limiter.scheduler.batch-size` | — | `50` | Tasks fetched from DB per refill |
| `rate-limiter.scheduler.idle-sleep-ms` | — | `1500` | Sleep when no pending tasks |
| `rate-limiter.scheduler.lock-ttl-multiplier` | — | `2` | Lock TTL = dispatchInterval × multiplier |
| `rate-limiter.scheduler.instance-id` | `HOSTNAME` | `local` | Unique ID for lock ownership |

---

## Key Design Decisions

Three decisions are documented as ADRs in `docs/adr/`:

| ADR | Decision | Why it matters |
|---|---|---|
| `0001` | Fixed windows (not sliding) for all counters | Sliding windows require Redis sorted sets (O(log N) vs O(1)) and are harder to recover. Boundary bursting is a known, accepted trade-off. |
| `0002` | Redis distributed lock per tenant | Preserves the single-loop-per-tenant invariant across multiple instances. Allows HA with automatic failover bounded by the lock TTL. |
| `0003` | SQS-first dispatch + task UUID as dedup ID | Avoids the complexity of an outbox pattern. SQS dedup absorbs re-sends within 5 minutes. Workers must be idempotent for the at-least-once delivery guarantee. |
