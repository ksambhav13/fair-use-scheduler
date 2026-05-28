CREATE TYPE task_status AS ENUM ('PENDING', 'DISPATCHED', 'COMPLETED', 'FAILED');

CREATE TABLE tasks (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    payload TEXT NOT NULL,
    status task_status NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMPTZ NOT NULL,
    dispatched_at TIMESTAMPTZ
);

CREATE INDEX idx_tasks_pending_fifo ON tasks (tenant_id, created_at, id)
    WHERE status = 'PENDING';

CREATE INDEX idx_tasks_dispatched_at ON tasks (tenant_id, dispatched_at)
    WHERE dispatched_at IS NOT NULL;
