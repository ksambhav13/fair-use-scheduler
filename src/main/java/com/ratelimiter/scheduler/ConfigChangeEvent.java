package com.ratelimiter.scheduler;

import com.ratelimiter.domain.TenantConfig;
import java.util.UUID;

public record ConfigChangeEvent(UUID tenantId, Type type, TenantConfig config) {
    public enum Type { ADDED, UPDATED, REMOVED }
}
