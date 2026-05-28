# Rate Limiter — Domain Glossary

## Task
A unit of work belonging to a tenant, inserted into the task table by an external producer. Tasks are processed in FIFO order (insertion order) per tenant. The scheduler controls throughput by how fast it picks tasks — tasks wait in the queue rather than being rescheduled to a future time. A Task has: tenant_id, payload, status (pending → dispatched → completed | failed), and dispatched_at.

## Batch Fetch Cursor
A Redis key (`scheduler:{tenantId}:last_fetched_id`) storing the ID of the last task fetched from the task table. The Scheduler fetches a batch of pending tasks at once and drains them in memory, using the cursor to avoid re-scanning already-seen rows on the next fetch. If the cursor is absent (Redis flush), the Scheduler falls back to a full `status = 'pending'` FIFO query — correctness is preserved because dispatched tasks are excluded by status.

## Tenant
A client of the system. Each tenant has its own throughput configuration and fair usage policy stored in a `tenant_configs` table. The Scheduler loads configs on startup and polls for changes every ~60 seconds, hot-reloading without restart.

## Scheduler
The component that reads the task table and dispatches tasks to the Worker Queue at a controlled rate. Runs a dedicated per-tenant loop — tenants are fully isolated from one another. Enforces throughput limits and fair usage policy. Does not execute task logic itself. Tracks consumption using Redis counters — one counter per tenant per throughput window, one per tenant per fair usage window. Uses a distributed Redis lock per tenant to allow multiple Scheduler instances to run safely (only one instance holds the loop for a given tenant at a time).

## Consumption Counter
A Redis key tracking how many tasks a tenant has had dispatched in a given time window. Used by the Scheduler to enforce Normal Throughput and Fair Usage Policy limits. The task table's `dispatched_at` field serves as the source of truth for rebuilding counters after a Redis flush.

## Dispatch Interval
The minimum time between two consecutive dispatches for a tenant. Calculated as `window_duration / throughput_rate`. The per-tenant loop sleeps this long between dispatches, preventing both bursting and sub-second hammering, while still respecting the window counter cap. When no pending tasks exist, the loop sleeps a fixed 1–2 seconds before retrying.

## Worker Queue
A single Amazon SQS FIFO queue shared across all tenants. The Scheduler dispatches using the task ID as the message deduplication ID (absorbs duplicates within the 5-minute SQS dedup window) and `tenant_id` as the message group ID. Workers consume from this one queue and execute task logic. Workers must be idempotent.

## Normal Throughput
The rate at which a tenant's tasks are executed when the tenant has not exceeded its fair usage policy. Expressed as N tasks per unit of time (minute, hour, or day).

## Throttled Throughput
The reduced rate at which a tenant's tasks are executed after the tenant has exceeded its fair usage policy. Same time-unit dimension as Normal Throughput.

## Fair Usage Policy
A cap defined at a higher time dimension than Throughput. If Throughput is per-minute, Fair Usage is per-hour. If Throughput is per-hour, Fair Usage is per-day. When a tenant's task count within the Fair Usage window exceeds this cap, the Scheduler switches that tenant to Throttled Throughput for the remainder of that window. Both windows are fixed (not sliding) — they reset at calendar boundaries (top of minute, hour, or day). At the start of a new fair usage window, the tenant automatically returns to Normal Throughput.
