package com.ratelimiter.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import java.util.UUID;

@Entity
@Table(name = "tenant_configs")
@Getter @Setter @NoArgsConstructor
public class TenantConfig {

    @Id
    @Column(name = "tenant_id")
    private UUID tenantId;

    @Column(name = "normal_throughput", nullable = false)
    private int normalThroughput;

    @Column(name = "throttled_throughput", nullable = false)
    private int throttledThroughput;

    @Enumerated(EnumType.STRING)
    @Column(name = "throughput_unit", columnDefinition = "throughput_unit", nullable = false)
    private ThroughputUnit throughputUnit;

    @Column(name = "fair_usage_cap", nullable = false)
    private int fairUsageCap;

    @Column(nullable = false)
    private boolean enabled = true;
}
