package com.ratelimiter.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

@Configuration
public class SqsConfig {

    @Bean
    public SqsClient sqsClient(RateLimiterProperties properties) {
        return SqsClient.builder()
                .region(Region.of(properties.getSqs().getRegion()))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }
}
