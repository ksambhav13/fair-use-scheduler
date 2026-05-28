package com.ratelimiter.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ratelimiter.config.RateLimiterProperties;
import com.ratelimiter.domain.Task;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import java.util.Map;

@Component
public class SqsDispatcher {

    private final SqsClient sqsClient;
    private final ObjectMapper objectMapper;
    private final String queueUrl;

    public SqsDispatcher(SqsClient sqsClient, ObjectMapper objectMapper, RateLimiterProperties properties) {
        this.sqsClient = sqsClient;
        this.objectMapper = objectMapper;
        this.queueUrl = properties.getSqs().getQueueUrl();
    }

    public void dispatch(Task task) {
        String body;
        try {
            body = objectMapper.writeValueAsString(Map.of(
                    "taskId", task.getId().toString(),
                    "tenantId", task.getTenantId().toString(),
                    "payload", task.getPayload()
            ));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize task " + task.getId(), e);
        }

        sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(body)
                .messageGroupId(task.getTenantId().toString())
                .messageDeduplicationId(task.getId().toString())
                .build());
    }
}
