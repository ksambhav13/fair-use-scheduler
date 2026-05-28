package com.ratelimiter.scheduler;

import com.ratelimiter.domain.TenantConfig;
import com.ratelimiter.domain.ThroughputUnit;
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
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
class ConsumptionCounterTest {

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
    private ConsumptionCounter consumptionCounter;

    @Autowired
    private StringRedisTemplate redisTemplate;

    private UUID tenantId;
    private TenantConfig config;

    @BeforeEach
    void setUp() {
        tenantId = UUID.randomUUID();
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();

        config = new TenantConfig();
        config.setTenantId(tenantId);
        config.setNormalThroughput(10);
        config.setThrottledThroughput(3);
        config.setThroughputUnit(ThroughputUnit.MINUTE);
        config.setFairUsageCap(100);
        config.setEnabled(true);
    }

    @Test
    void getThroughputCount_returnsZeroWhenNoIncrements() {
        assertThat(consumptionCounter.getThroughputCount(tenantId, config)).isZero();
    }

    @Test
    void incrementThroughput_returnsAccumulatedCount() {
        consumptionCounter.incrementThroughput(tenantId, config);
        consumptionCounter.incrementThroughput(tenantId, config);
        consumptionCounter.incrementThroughput(tenantId, config);

        assertThat(consumptionCounter.getThroughputCount(tenantId, config)).isEqualTo(3);
    }

    @Test
    void getFairUsageCount_returnsZeroWhenNoIncrements() {
        assertThat(consumptionCounter.getFairUsageCount(tenantId, config)).isZero();
    }

    @Test
    void incrementFairUsage_accumulatesInHigherDimensionWindow() {
        consumptionCounter.incrementFairUsage(tenantId, config);
        consumptionCounter.incrementFairUsage(tenantId, config);

        assertThat(consumptionCounter.getFairUsageCount(tenantId, config)).isEqualTo(2);
    }

    @Test
    void incrementFairUsage_isNoOpForDayThroughputUnit() {
        config.setThroughputUnit(ThroughputUnit.DAY);

        consumptionCounter.incrementFairUsage(tenantId, config);

        assertThat(consumptionCounter.getFairUsageCount(tenantId, config)).isZero();
    }

    @Test
    void seedThroughput_setsSpecificValue() {
        consumptionCounter.seedThroughput(tenantId, config, 7);

        assertThat(consumptionCounter.getThroughputCount(tenantId, config)).isEqualTo(7);
    }

    @Test
    void seedFairUsage_setsSpecificValue() {
        consumptionCounter.seedFairUsage(tenantId, config, 42);

        assertThat(consumptionCounter.getFairUsageCount(tenantId, config)).isEqualTo(42);
    }

    @Test
    void throughputAndFairUsageCounters_areIndependent() {
        consumptionCounter.incrementThroughput(tenantId, config);
        consumptionCounter.incrementFairUsage(tenantId, config);
        consumptionCounter.incrementFairUsage(tenantId, config);

        assertThat(consumptionCounter.getThroughputCount(tenantId, config)).isEqualTo(1);
        assertThat(consumptionCounter.getFairUsageCount(tenantId, config)).isEqualTo(2);
    }
}
