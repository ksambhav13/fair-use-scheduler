package com.ratelimiter.scheduler;

import com.ratelimiter.config.RateLimiterProperties;
import com.ratelimiter.domain.Task;
import com.ratelimiter.domain.TaskStatus;
import com.ratelimiter.domain.TenantConfig;
import com.ratelimiter.domain.ThroughputUnit;
import com.ratelimiter.repository.TaskJpaRepository;
import com.ratelimiter.repository.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.concurrent.TimeUnit;

@SpringBootTest
@Testcontainers
class TenantSchedulerLoopTest {

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

    @MockBean
    private SqsDispatcher sqsDispatcher;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private TaskJpaRepository taskJpaRepository;

    @Autowired
    private ConsumptionCounter consumptionCounter;

    @Autowired
    private SchedulerLock schedulerLock;

    @Autowired
    private WindowCalculator windowCalculator;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private RateLimiterProperties properties;

    private UUID tenantId;
    private TenantConfig config;
    private TenantConfigLoader stubConfigLoader;

    @BeforeEach
    void setUp() {
        tenantId = UUID.randomUUID();
        taskJpaRepository.deleteAll();
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();

        // 60 tasks/min → 1s dispatch interval; fair usage cap of 3 tasks/hour
        config = new TenantConfig();
        config.setTenantId(tenantId);
        config.setNormalThroughput(60);
        config.setThrottledThroughput(30);
        config.setThroughputUnit(ThroughputUnit.MINUTE);
        config.setFairUsageCap(3);
        config.setEnabled(true);

        stubConfigLoader = Mockito.mock(TenantConfigLoader.class);
        Mockito.when(stubConfigLoader.getConfig(tenantId)).thenReturn(Optional.of(config));
    }

    @Test
    void loop_dispatchesAllPendingTasksInFifoOrder() throws InterruptedException {
        List<Task> tasks = insertTasks(3);

        TenantSchedulerLoop loop = buildLoop();
        Thread thread = Thread.ofVirtual().start(loop);

        await().atMost(5, TimeUnit.SECONDS)
                .until(() -> taskJpaRepository.findAll().stream()
                        .allMatch(t -> t.getStatus() == TaskStatus.DISPATCHED));

        loop.stop();
        thread.interrupt();

        verify(sqsDispatcher, Mockito.times(3)).dispatch(any());

        List<Task> dispatched = taskJpaRepository.findAll().stream()
                .sorted(java.util.Comparator.comparing(Task::getDispatchedAt))
                .toList();
        for (int i = 0; i < 3; i++) {
            assertThat(dispatched.get(i).getId()).isEqualTo(tasks.get(i).getId());
        }
    }

    @Test
    void loop_sleepsWhenNoTasksAvailable() throws InterruptedException {
        TenantSchedulerLoop loop = buildLoop();
        Thread thread = Thread.ofVirtual().start(loop);

        Thread.sleep(3_000);

        loop.stop();
        thread.interrupt();

        verify(sqsDispatcher, never()).dispatch(any());
    }

    @Test
    void loop_doesNotDoubleDispatch_whenTaskAlreadyDispatched() throws InterruptedException {
        Task task = insertTasks(1).get(0);
        // Pre-dispatch the task to simulate concurrent dispatch by another instance
        taskRepository.markDispatched(task, Instant.now());

        AtomicInteger dispatchCount = new AtomicInteger(0);
        Mockito.doAnswer(inv -> { dispatchCount.incrementAndGet(); return null; })
                .when(sqsDispatcher).dispatch(any());

        TenantSchedulerLoop loop = buildLoop();
        Thread thread = Thread.ofVirtual().start(loop);

        Thread.sleep(2_500);

        loop.stop();
        thread.interrupt();

        // SQS may be called (dedup handles it), but markDispatched returns false so counter stays 0
        assertThat(consumptionCounter.getThroughputCount(tenantId, config)).isZero();
    }

    @Test
    void loop_usesThrottledDispatchRate_whenFairUsageCapExceeded() throws InterruptedException {
        // Force throttled mode from the first iteration
        consumptionCounter.seedFairUsage(tenantId, config, config.getFairUsageCap());
        insertTasks(3);

        TenantSchedulerLoop loop = buildLoop();
        Thread thread = Thread.ofVirtual().start(loop);

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> taskJpaRepository.findAll().stream()
                        .allMatch(t -> t.getStatus() == TaskStatus.DISPATCHED));

        loop.stop();
        thread.interrupt();

        List<Task> dispatched = taskJpaRepository.findAll().stream()
                .sorted(java.util.Comparator.comparing(Task::getDispatchedAt))
                .toList();

        // Throttled = 30/min → 2s interval between consecutive dispatches
        long gap = dispatched.get(1).getDispatchedAt().toEpochMilli()
                - dispatched.get(0).getDispatchedAt().toEpochMilli();
        assertThat(gap).isGreaterThanOrEqualTo(1_800);
    }

    private List<Task> insertTasks(int count) {
        Instant base = Instant.now().minusSeconds(100);
        List<Task> tasks = new java.util.ArrayList<>();
        for (int i = 0; i < count; i++) {
            Task t = new Task();
            t.setId(UUID.randomUUID());
            t.setTenantId(tenantId);
            t.setPayload("{\"index\":" + i + "}");
            t.setStatus(TaskStatus.PENDING);
            t.setCreatedAt(base.plusMillis(i * 10L));
            tasks.add(taskJpaRepository.save(t));
        }
        return tasks;
    }

    private TenantSchedulerLoop buildLoop() {
        RateLimiterProperties props = new RateLimiterProperties();
        props.getScheduler().setBatchSize(50);
        props.getScheduler().setIdleSleepMs(500);
        props.getScheduler().setLockTtlMultiplier(2);
        props.getScheduler().setInstanceId("test-" + tenantId);
        props.getSqs().setQueueUrl("https://sqs.us-east-1.amazonaws.com/test/tasks.fifo");

        return new TenantSchedulerLoop(tenantId, taskRepository, consumptionCounter,
                schedulerLock, stubConfigLoader, sqsDispatcher, windowCalculator, props);
    }
}
