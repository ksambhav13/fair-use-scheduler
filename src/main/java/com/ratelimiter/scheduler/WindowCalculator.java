package com.ratelimiter.scheduler;

import com.ratelimiter.domain.ThroughputUnit;
import org.springframework.stereotype.Component;
import java.time.Instant;

@Component
public class WindowCalculator {

    public Instant currentWindowStart(ThroughputUnit unit) {
        long nowSec = Instant.now().getEpochSecond();
        long windowStart = (nowSec / unit.getDurationSeconds()) * unit.getDurationSeconds();
        return Instant.ofEpochSecond(windowStart);
    }

    public long millisUntilNextWindow(ThroughputUnit unit) {
        long nowMs = System.currentTimeMillis();
        long windowMs = unit.getDurationSeconds() * 1000L;
        long currentWindowStart = (nowMs / windowMs) * windowMs;
        return (currentWindowStart + windowMs) - nowMs;
    }

    public long dispatchIntervalMillis(ThroughputUnit unit, int throughputRate) {
        return (unit.getDurationSeconds() * 1000L) / throughputRate;
    }
}
