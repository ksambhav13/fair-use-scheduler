package com.ratelimiter.repository;

import com.ratelimiter.domain.TenantConfig;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.UUID;

public interface TenantConfigJpaRepository extends JpaRepository<TenantConfig, UUID> {
    List<TenantConfig> findAllByEnabledTrue();
}
