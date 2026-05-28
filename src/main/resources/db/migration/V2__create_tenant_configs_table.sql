CREATE TYPE throughput_unit AS ENUM ('MINUTE', 'HOUR', 'DAY');

CREATE TABLE tenant_configs (
    tenant_id UUID PRIMARY KEY,
    normal_throughput INTEGER NOT NULL,
    throttled_throughput INTEGER NOT NULL,
    throughput_unit throughput_unit NOT NULL,
    fair_usage_cap INTEGER NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE
);
