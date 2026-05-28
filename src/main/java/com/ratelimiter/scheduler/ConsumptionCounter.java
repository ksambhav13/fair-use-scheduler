package com.ratelimiter.scheduler;

import com.ratelimiter.domain.TenantConfig;
import com.ratelimiter.domain.ThroughputUnit;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Component
public class ConsumptionCounter {

    // Sets TTL only on the first increment, preventing a race between INCR and EXPIRE.
    private static final String INCREMENT_SCRIPT = """
            local val = redis.call('INCR', KEYS[1])
            if val == 1 then
                redis.call('EXPIRE', KEYS[1], ARGV[1])
            end
            return val
            """;

    private final StringRedisTemplate redisTemplate;
    private final WindowCalculator windowCalculator;
    private final RedisScript<Long> incrementScript;

    public ConsumptionCounter(StringRedisTemplate redisTemplate, WindowCalculator windowCalculator) {
        this.redisTemplate = redisTemplate;
        this.windowCalculator = windowCalculator;
        this.incrementScript = RedisScript.of(INCREMENT_SCRIPT, Long.class);
    }

    public void incrementThroughput(UUID tenantId, TenantConfig config) {
        String key = throughputKey(tenantId, config.getThroughputUnit());
        long ttlSeconds = config.getThroughputUnit().getDurationSeconds() * 2;
        redisTemplate.execute(incrementScript, List.of(key), String.valueOf(ttlSeconds));
    }

    public void incrementFairUsage(UUID tenantId, TenantConfig config) {
        if (config.getThroughputUnit() == ThroughputUnit.DAY) {
            return;
        }
        ThroughputUnit fairUsageUnit = config.getThroughputUnit().fairUsageUnit();
        String key = fairUsageKey(tenantId, fairUsageUnit);
        long ttlSeconds = fairUsageUnit.getDurationSeconds() * 2;
        redisTemplate.execute(incrementScript, List.of(key), String.valueOf(ttlSeconds));
    }

    public long getThroughputCount(UUID tenantId, TenantConfig config) {
        String key = throughputKey(tenantId, config.getThroughputUnit());
        return parseLong(redisTemplate.opsForValue().get(key));
    }

    public long getFairUsageCount(UUID tenantId, TenantConfig config) {
        if (config.getThroughputUnit() == ThroughputUnit.DAY) {
            return 0;
        }
        ThroughputUnit fairUsageUnit = config.getThroughputUnit().fairUsageUnit();
        String key = fairUsageKey(tenantId, fairUsageUnit);
        return parseLong(redisTemplate.opsForValue().get(key));
    }

    public void seedThroughput(UUID tenantId, TenantConfig config, long count) {
        String key = throughputKey(tenantId, config.getThroughputUnit());
        long ttlSeconds = config.getThroughputUnit().getDurationSeconds() * 2;
        redisTemplate.opsForValue().set(key, String.valueOf(count), Duration.ofSeconds(ttlSeconds));
    }

    public void seedFairUsage(UUID tenantId, TenantConfig config, long count) {
        if (config.getThroughputUnit() == ThroughputUnit.DAY) {
            return;
        }
        ThroughputUnit fairUsageUnit = config.getThroughputUnit().fairUsageUnit();
        String key = fairUsageKey(tenantId, fairUsageUnit);
        long ttlSeconds = fairUsageUnit.getDurationSeconds() * 2;
        redisTemplate.opsForValue().set(key, String.valueOf(count), Duration.ofSeconds(ttlSeconds));
    }

    private String throughputKey(UUID tenantId, ThroughputUnit unit) {
        Instant windowStart = windowCalculator.currentWindowStart(unit);
        return String.format("rate:%s:throughput:%d", tenantId, windowStart.getEpochSecond());
    }

    private String fairUsageKey(UUID tenantId, ThroughputUnit fairUsageUnit) {
        Instant windowStart = windowCalculator.currentWindowStart(fairUsageUnit);
        return String.format("rate:%s:fair_usage:%d", tenantId, windowStart.getEpochSecond());
    }

    private long parseLong(String value) {
        return value == null ? 0L : Long.parseLong(value);
    }
}
