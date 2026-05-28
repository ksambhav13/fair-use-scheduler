package com.ratelimiter.scheduler;

import com.ratelimiter.config.RateLimiterProperties;
import com.ratelimiter.domain.TenantConfig;
import com.ratelimiter.repository.TaskRepository;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class SchedulerManager {

    private static final Logger log = LoggerFactory.getLogger(SchedulerManager.class);

    private final TenantConfigLoader configLoader;
    private final TaskRepository taskRepository;
    private final ConsumptionCounter consumptionCounter;
    private final SchedulerLock schedulerLock;
    private final SqsDispatcher sqsDispatcher;
    private final WindowCalculator windowCalculator;
    private final RateLimiterProperties properties;

    private final Map<UUID, TenantSchedulerLoop> loops = new ConcurrentHashMap<>();
    private final Map<UUID, Thread> threads = new ConcurrentHashMap<>();

    public SchedulerManager(TenantConfigLoader configLoader, TaskRepository taskRepository,
                             ConsumptionCounter consumptionCounter, SchedulerLock schedulerLock,
                             SqsDispatcher sqsDispatcher, WindowCalculator windowCalculator,
                             RateLimiterProperties properties) {
        this.configLoader = configLoader;
        this.taskRepository = taskRepository;
        this.consumptionCounter = consumptionCounter;
        this.schedulerLock = schedulerLock;
        this.sqsDispatcher = sqsDispatcher;
        this.windowCalculator = windowCalculator;
        this.properties = properties;
    }

    @PostConstruct
    public void start() {
        configLoader.addChangeListener(this::handleConfigChange);
        configLoader.start();
    }

    @PreDestroy
    public void stop() {
        new java.util.HashSet<>(loops.keySet()).forEach(this::stopLoop);
        configLoader.stop();
    }

    private void handleConfigChange(ConfigChangeEvent event) {
        switch (event.type()) {
            case ADDED, UPDATED -> {
                TenantConfig config = event.config();
                if (config != null && config.isEnabled()) {
                    if (!loops.containsKey(event.tenantId())) {
                        startLoop(config);
                    }
                } else {
                    stopLoop(event.tenantId());
                }
            }
            case REMOVED -> stopLoop(event.tenantId());
        }
    }

    private void startLoop(TenantConfig config) {
        TenantSchedulerLoop loop = new TenantSchedulerLoop(
                config.getTenantId(), taskRepository, consumptionCounter,
                schedulerLock, configLoader, sqsDispatcher, windowCalculator, properties);
        loops.put(config.getTenantId(), loop);
        Thread thread = Thread.ofVirtual().start(loop);
        threads.put(config.getTenantId(), thread);
        log.info("Started scheduler loop for tenant {}", config.getTenantId());
    }

    private void stopLoop(UUID tenantId) {
        TenantSchedulerLoop loop = loops.remove(tenantId);
        Thread thread = threads.remove(tenantId);
        if (loop != null) loop.stop();
        if (thread != null) thread.interrupt();
        log.info("Stopped scheduler loop for tenant {}", tenantId);
    }
}
