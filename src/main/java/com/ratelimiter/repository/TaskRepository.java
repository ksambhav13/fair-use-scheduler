package com.ratelimiter.repository;

import com.ratelimiter.domain.Task;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Component
public class TaskRepository {

    private static final String CURSOR_KEY_TEMPLATE = "scheduler:%s:cursor";

    private final TaskJpaRepository jpaRepository;
    private final StringRedisTemplate redisTemplate;

    public TaskRepository(TaskJpaRepository jpaRepository, StringRedisTemplate redisTemplate) {
        this.jpaRepository = jpaRepository;
        this.redisTemplate = redisTemplate;
    }

    public List<Task> fetchBatch(UUID tenantId, int batchSize) {
        String cursor = redisTemplate.opsForValue().get(cursorKey(tenantId));
        if (cursor != null) {
            String[] parts = cursor.split("\\|", 2);
            Instant cursorCreatedAt = Instant.ofEpochMilli(Long.parseLong(parts[0]));
            String cursorId = parts[1];
            return jpaRepository.fetchBatchWithCursor(tenantId, cursorCreatedAt, cursorId, batchSize);
        }
        return jpaRepository.fetchBatchWithoutCursor(tenantId, batchSize);
    }

    @Transactional
    public boolean markDispatched(Task task, Instant dispatchedAt) {
        int updated = jpaRepository.markDispatched(task.getId(), dispatchedAt);
        if (updated > 0) {
            String cursorValue = task.getCreatedAt().toEpochMilli() + "|" + task.getId();
            redisTemplate.opsForValue().set(cursorKey(task.getTenantId()), cursorValue, Duration.ofHours(24));
            return true;
        }
        return false;
    }

    public long countDispatched(UUID tenantId, Instant from, Instant to) {
        return jpaRepository.countDispatched(tenantId, from, to);
    }

    private String cursorKey(UUID tenantId) {
        return String.format(CURSOR_KEY_TEMPLATE, tenantId);
    }
}
