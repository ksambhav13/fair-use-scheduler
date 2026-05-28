package com.ratelimiter.repository;

import com.ratelimiter.domain.Task;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface TaskJpaRepository extends JpaRepository<Task, UUID> {

    @Query(value = """
            SELECT * FROM tasks
            WHERE tenant_id = :tenantId AND status = 'PENDING'
              AND (created_at > :cursorCreatedAt
                   OR (created_at = :cursorCreatedAt AND id::text > :cursorId))
            ORDER BY created_at ASC, id::text ASC
            LIMIT :batchSize
            """, nativeQuery = true)
    List<Task> fetchBatchWithCursor(
            @Param("tenantId") UUID tenantId,
            @Param("cursorCreatedAt") Instant cursorCreatedAt,
            @Param("cursorId") String cursorId,
            @Param("batchSize") int batchSize);

    @Query(value = """
            SELECT * FROM tasks
            WHERE tenant_id = :tenantId AND status = 'PENDING'
            ORDER BY created_at ASC, id::text ASC
            LIMIT :batchSize
            """, nativeQuery = true)
    List<Task> fetchBatchWithoutCursor(
            @Param("tenantId") UUID tenantId,
            @Param("batchSize") int batchSize);

    @Modifying
    @org.springframework.transaction.annotation.Transactional
    @Query(value = """
            UPDATE tasks SET status = 'DISPATCHED', dispatched_at = :dispatchedAt
            WHERE id = :id AND status = 'PENDING'
            """, nativeQuery = true)
    int markDispatched(@Param("id") UUID id, @Param("dispatchedAt") Instant dispatchedAt);

    @Query("SELECT COUNT(t) FROM Task t WHERE t.tenantId = :tenantId AND t.dispatchedAt >= :from AND t.dispatchedAt < :to")
    long countDispatched(@Param("tenantId") UUID tenantId, @Param("from") Instant from, @Param("to") Instant to);
}
