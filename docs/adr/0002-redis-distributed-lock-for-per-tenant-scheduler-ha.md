# ADR 0002: Redis Distributed Lock for Per-Tenant Scheduler HA

**Status**: Accepted  
**Date**: 2026-05-28

## Context

The Scheduler runs a dedicated loop per tenant. For high availability, multiple Scheduler instances must be able to run simultaneously without two instances dispatching the same task for the same tenant (which would violate throughput limits and cause duplicate execution).

## Decision

Each Scheduler instance acquires a Redis lock (`SET tenant:{id}:scheduler_lock {instance_id} NX PX {ttl}`) before running a tenant's loop. The lock TTL is set to slightly longer than one dispatch interval. The instance refreshes the lock on each successful dispatch. If the lock holder dies, the TTL expires and another instance acquires it automatically.

## Consequences

**Benefits:**
- Preserves the per-tenant single-loop invariant across multiple Scheduler instances — exactly one instance runs a given tenant's loop at any time.
- Failover is automatic and bounded by the lock TTL (seconds), not a human restart.
- Reuses Redis, which is already required for consumption counters.

**Drawbacks:**
- Lock TTL must be tuned: too short causes unnecessary failovers during slow dispatches (e.g. slow DB or SQS); too long delays failover after a crash.
- If Redis goes down, all Scheduler locks are lost. Instances must detect this and pause dispatching rather than racing without coordination.

**Alternatives rejected:**
- Single Scheduler instance only: simpler but creates a single point of failure with no automatic recovery.
- `SELECT ... FOR UPDATE SKIP LOCKED` on the task table: works for worker pools but doesn't preserve the single-loop-per-tenant constraint that makes dispatch interval enforcement reliable.
