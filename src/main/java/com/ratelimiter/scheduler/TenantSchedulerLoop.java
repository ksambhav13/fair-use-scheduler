package com.ratelimiter.scheduler;

import com.ratelimiter.config.RateLimiterProperties;
import com.ratelimiter.domain.Task;
import com.ratelimiter.domain.TenantConfig;
import com.ratelimiter.domain.ThroughputUnit;
import com.ratelimiter.repository.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Instant;
import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;

public class TenantSchedulerLoop implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(TenantSchedulerLoop.class);
    private static final long MIN_LOCK_TTL_MS = 30_000;

    private final UUID tenantId;
    private final TaskRepository taskRepository;
    private final ConsumptionCounter consumptionCounter;
    private final SchedulerLock schedulerLock;
    private final TenantConfigLoader configLoader;
    private final SqsDispatcher sqsDispatcher;
    private final WindowCalculator windowCalculator;
    private final RateLimiterProperties properties;

    private volatile boolean running = true;
    private final Queue<Task> batch = new LinkedList<>();

    public TenantSchedulerLoop(UUID tenantId, TaskRepository taskRepository,
                                ConsumptionCounter consumptionCounter, SchedulerLock schedulerLock,
                                TenantConfigLoader configLoader, SqsDispatcher sqsDispatcher,
                                WindowCalculator windowCalculator, RateLimiterProperties properties) {
        this.tenantId = tenantId;
        this.taskRepository = taskRepository;
        this.consumptionCounter = consumptionCounter;
        this.schedulerLock = schedulerLock;
        this.configLoader = configLoader;
        this.sqsDispatcher = sqsDispatcher;
        this.windowCalculator = windowCalculator;
        this.properties = properties;
    }

    @Override
    public void run() {
        String instanceId = properties.getScheduler().getInstanceId();
        try {
            while (running) {
                try {
                    TenantConfig config = configLoader.getConfig(tenantId).orElse(null);
                    if (config == null || !config.isEnabled()) {
                        Thread.sleep(5_000);
                        continue;
                    }

                    long dispatchIntervalMs = windowCalculator.dispatchIntervalMillis(
                            config.getThroughputUnit(), config.getNormalThroughput());
                    long lockTtlMs = Math.max(MIN_LOCK_TTL_MS,
                            dispatchIntervalMs * properties.getScheduler().getLockTtlMultiplier());

                    if (!schedulerLock.tryAcquire(tenantId, instanceId, lockTtlMs)) {
                        Thread.sleep(1_000);
                        continue;
                    }

                    boolean isThrottled = config.getThroughputUnit() != ThroughputUnit.DAY
                            && consumptionCounter.getFairUsageCount(tenantId, config) >= config.getFairUsageCap();

                    int currentThroughput = isThrottled
                            ? config.getThrottledThroughput()
                            : config.getNormalThroughput();
                    dispatchIntervalMs = windowCalculator.dispatchIntervalMillis(
                            config.getThroughputUnit(), currentThroughput);

                    long throughputCount = consumptionCounter.getThroughputCount(tenantId, config);
                    if (throughputCount >= currentThroughput) {
                        Thread.sleep(windowCalculator.millisUntilNextWindow(config.getThroughputUnit()));
                        continue;
                    }

                    Task task = peekNextTask(config);
                    if (task == null) {
                        Thread.sleep(properties.getScheduler().getIdleSleepMs());
                        continue;
                    }

                    long start = System.currentTimeMillis();

                    // ADR-0003: dispatch to SQS first; DB update follows
                    sqsDispatcher.dispatch(task);
                    boolean dispatched = taskRepository.markDispatched(task, Instant.now());

                    if (dispatched) {
                        consumptionCounter.incrementThroughput(tenantId, config);
                        consumptionCounter.incrementFairUsage(tenantId, config);
                        schedulerLock.refresh(tenantId, instanceId, lockTtlMs);
                    }

                    batch.poll();

                    long remaining = dispatchIntervalMs - (System.currentTimeMillis() - start);
                    if (remaining > 0) {
                        Thread.sleep(remaining);
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("Scheduler loop error for tenant {}", tenantId, e);
                    try {
                        Thread.sleep(1_000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        } finally {
            schedulerLock.release(tenantId, instanceId);
        }
    }

    public void stop() {
        running = false;
    }

    private Task peekNextTask(TenantConfig config) {
        if (batch.isEmpty()) {
            batch.addAll(taskRepository.fetchBatch(tenantId, properties.getScheduler().getBatchSize()));
        }
        return batch.peek();
    }
}
