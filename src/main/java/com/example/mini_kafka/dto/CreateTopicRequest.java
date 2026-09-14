package com.example.mini_kafka.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public class CreateTopicRequest {

    @NotBlank(message = "Topic name cannot be blank")
    private String topicName;

    @Min(
            value = 1,
            message = "Partition count must be at least 1"
    )
    private int partitionCount;

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
    }
}