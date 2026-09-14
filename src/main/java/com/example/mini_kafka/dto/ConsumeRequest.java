package com.example.mini_kafka.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public class ConsumeRequest {

    @NotBlank(message = "Consumer id cannot be blank")
    private String consumerId;

    @NotBlank(message = "Consumer group cannot be blank")
    private String consumerGroup;

    @Min(
            value = 0,
            message = "Partition id cannot be negative"
    )
    private int partitionId;

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }
}