package com.ratelimiter.domain;

public enum ThroughputUnit {
    MINUTE(60), HOUR(3600), DAY(86400);

    private final long durationSeconds;

    ThroughputUnit(long durationSeconds) {
        this.durationSeconds = durationSeconds;
    }

    public long getDurationSeconds() {
        return durationSeconds;
    }

    public ThroughputUnit fairUsageUnit() {
        return switch (this) {
            case MINUTE -> HOUR;
            case HOUR -> DAY;
            case DAY -> throw new IllegalStateException("DAY throughput has no higher fair usage unit");
        };
    }
}
