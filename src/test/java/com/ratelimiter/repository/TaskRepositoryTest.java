package com.ratelimiter.repository;

import com.ratelimiter.domain.Task;
import com.ratelimiter.domain.TaskStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
class TaskRepositoryTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @SuppressWarnings("resource")
    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private TaskJpaRepository taskJpaRepository;

    @Autowired
    private StringRedisTemplate redisTemplate;

    private UUID tenantId;

    @BeforeEach
    void setUp() {
        tenantId = UUID.randomUUID();
        taskJpaRepository.deleteAll();
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
    }

    @Test
    void fetchBatch_returnsTasksInFifoOrder() {
        Instant base = Instant.now().minusSeconds(100);
        for (int i = 0; i < 5; i++) {
            taskJpaRepository.save(task(tenantId, base.plusSeconds(i)));
        }

        List<Task> result = taskRepository.fetchBatch(tenantId, 5);

        assertThat(result).hasSize(5);
        for (int i = 0; i < 4; i++) {
            assertThat(result.get(i).getCreatedAt()).isBefore(result.get(i + 1).getCreatedAt());
        }
    }

    @Test
    void fetchBatch_withCursor_skipsPreviouslyDispatchedTasks() {
        Instant base = Instant.now().minusSeconds(100);
        List<Task> saved = new java.util.ArrayList<>();
        for (int i = 0; i < 5; i++) {
            saved.add(taskJpaRepository.save(task(tenantId, base.plusSeconds(i))));
        }

        taskRepository.markDispatched(saved.get(0), Instant.now());
        taskRepository.markDispatched(saved.get(1), Instant.now());

        List<Task> result = taskRepository.fetchBatch(tenantId, 5);

        assertThat(result).hasSize(3);
        assertThat(result.get(0).getId()).isEqualTo(saved.get(2).getId());
    }

    @Test
    void fetchBatch_withoutCursor_returnsFromBeginning() {
        Instant base = Instant.now().minusSeconds(100);
        for (int i = 0; i < 3; i++) {
            taskJpaRepository.save(task(tenantId, base.plusSeconds(i)));
        }

        List<Task> result = taskRepository.fetchBatch(tenantId, 10);

        assertThat(result).hasSize(3);
    }

    @Test
    void markDispatched_returnsTrueAndUpdatesStatus() {
        Task saved = taskJpaRepository.save(task(tenantId, Instant.now().minusSeconds(1)));

        boolean result = taskRepository.markDispatched(saved, Instant.now());

        assertThat(result).isTrue();
        Task reloaded = taskJpaRepository.findById(saved.getId()).orElseThrow();
        assertThat(reloaded.getStatus()).isEqualTo(TaskStatus.DISPATCHED);
        assertThat(reloaded.getDispatchedAt()).isNotNull();
    }

    @Test
    void markDispatched_returnsFalseWhenAlreadyDispatched() {
        Task saved = taskJpaRepository.save(task(tenantId, Instant.now().minusSeconds(1)));
        taskRepository.markDispatched(saved, Instant.now());

        boolean secondCall = taskRepository.markDispatched(saved, Instant.now());

        assertThat(secondCall).isFalse();
    }

    @Test
    void countDispatched_returnsAccurateCount() {
        Instant windowStart = Instant.now().minusSeconds(60);
        Instant windowEnd = Instant.now().plusSeconds(60);

        for (int i = 0; i < 3; i++) {
            Task t = taskJpaRepository.save(task(tenantId, Instant.now().minusSeconds(10 + i)));
            taskRepository.markDispatched(t, Instant.now());
        }

        long count = taskRepository.countDispatched(tenantId, windowStart, windowEnd);

        assertThat(count).isEqualTo(3);
    }

    private Task task(UUID tenantId, Instant createdAt) {
        Task t = new Task();
        t.setId(UUID.randomUUID());
        t.setTenantId(tenantId);
        t.setPayload("{\"key\":\"value\"}");
        t.setStatus(TaskStatus.PENDING);
        t.setCreatedAt(createdAt);
        return t;
    }
}
