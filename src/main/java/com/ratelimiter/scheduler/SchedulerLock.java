package com.ratelimiter.scheduler;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

@Component
public class SchedulerLock {

    // Refresh TTL only if this instance still holds the lock, preventing a stale instance from extending a lock it lost.
    private static final String REFRESH_SCRIPT = """
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('EXPIRE', KEYS[1], ARGV[2])
            else
                return 0
            end
            """;

    private static final String RELEASE_SCRIPT = """
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            else
                return 0
            end
            """;

    private final StringRedisTemplate redisTemplate;
    private final RedisScript<Long> refreshScript;
    private final RedisScript<Long> releaseScript;

    public SchedulerLock(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
        this.refreshScript = RedisScript.of(REFRESH_SCRIPT, Long.class);
        this.releaseScript = RedisScript.of(RELEASE_SCRIPT, Long.class);
    }

    public boolean tryAcquire(UUID tenantId, String instanceId, long ttlMs) {
        Boolean acquired = redisTemplate.opsForValue()
                .setIfAbsent(lockKey(tenantId), instanceId, Duration.ofMillis(ttlMs));
        return Boolean.TRUE.equals(acquired);
    }

    public boolean refresh(UUID tenantId, String instanceId, long ttlMs) {
        Long result = redisTemplate.execute(
                refreshScript, List.of(lockKey(tenantId)), instanceId, String.valueOf(ttlMs / 1000));
        return Long.valueOf(1L).equals(result);
    }

    public void release(UUID tenantId, String instanceId) {
        redisTemplate.execute(releaseScript, List.of(lockKey(tenantId)), instanceId);
    }

    private String lockKey(UUID tenantId) {
        return "scheduler:" + tenantId + ":lock";
    }
}
