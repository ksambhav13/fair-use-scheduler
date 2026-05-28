package com.ratelimiter.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("rate-limiter")
public class RateLimiterProperties {

    private Sqs sqs = new Sqs();
    private Scheduler scheduler = new Scheduler();

    public Sqs getSqs() { return sqs; }
    public void setSqs(Sqs sqs) { this.sqs = sqs; }
    public Scheduler getScheduler() { return scheduler; }
    public void setScheduler(Scheduler scheduler) { this.scheduler = scheduler; }

    public static class Sqs {
        private String queueUrl;
        private String region = "us-east-1";

        public String getQueueUrl() { return queueUrl; }
        public void setQueueUrl(String queueUrl) { this.queueUrl = queueUrl; }
        public String getRegion() { return region; }
        public void setRegion(String region) { this.region = region; }
    }

    public static class Scheduler {
        private int batchSize = 50;
        private long idleSleepMs = 1500;
        private int lockTtlMultiplier = 2;
        private String instanceId = "local";

        public int getBatchSize() { return batchSize; }
        public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
        public long getIdleSleepMs() { return idleSleepMs; }
        public void setIdleSleepMs(long idleSleepMs) { this.idleSleepMs = idleSleepMs; }
        public int getLockTtlMultiplier() { return lockTtlMultiplier; }
        public void setLockTtlMultiplier(int lockTtlMultiplier) { this.lockTtlMultiplier = lockTtlMultiplier; }
        public String getInstanceId() { return instanceId; }
        public void setInstanceId(String instanceId) { this.instanceId = instanceId; }
    }
}
