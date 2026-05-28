package com.ratelimiter.scheduler;

import com.ratelimiter.domain.TenantConfig;
import com.ratelimiter.repository.TenantConfigJpaRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

@Component
public class TenantConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(TenantConfigLoader.class);

    private final TenantConfigJpaRepository jpaRepository;
    private final List<Consumer<ConfigChangeEvent>> listeners = new CopyOnWriteArrayList<>();
    private volatile Map<UUID, TenantConfig> snapshot = Map.of();
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> Thread.ofVirtual().unstarted(r));

    public TenantConfigLoader(TenantConfigJpaRepository jpaRepository) {
        this.jpaRepository = jpaRepository;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::reload, 0, 60, TimeUnit.SECONDS);
    }

    public void stop() {
        scheduler.shutdown();
    }

    public Optional<TenantConfig> getConfig(UUID tenantId) {
        return Optional.ofNullable(snapshot.get(tenantId));
    }

    public Map<UUID, TenantConfig> getAllConfigs() {
        return snapshot;
    }

    public void addChangeListener(Consumer<ConfigChangeEvent> listener) {
        listeners.add(listener);
    }

    private void reload() {
        try {
            Map<UUID, TenantConfig> fresh = new HashMap<>();
            for (TenantConfig c : jpaRepository.findAllByEnabledTrue()) {
                fresh.put(c.getTenantId(), c);
            }

            Map<UUID, TenantConfig> previous = snapshot;

            for (Map.Entry<UUID, TenantConfig> entry : fresh.entrySet()) {
                ConfigChangeEvent.Type type = previous.containsKey(entry.getKey())
                        ? ConfigChangeEvent.Type.UPDATED
                        : ConfigChangeEvent.Type.ADDED;
                fire(new ConfigChangeEvent(entry.getKey(), type, entry.getValue()));
            }
            for (UUID tenantId : previous.keySet()) {
                if (!fresh.containsKey(tenantId)) {
                    fire(new ConfigChangeEvent(tenantId, ConfigChangeEvent.Type.REMOVED, null));
                }
            }

            snapshot = Collections.unmodifiableMap(fresh);
        } catch (Exception e) {
            log.error("Failed to reload tenant configs", e);
        }
    }

    private void fire(ConfigChangeEvent event) {
        for (Consumer<ConfigChangeEvent> listener : listeners) {
            try {
                listener.accept(event);
            } catch (Exception e) {
                log.error("Config change listener failed for tenant {}", event.tenantId(), e);
            }
        }
    }
}
