package com.example.mini_kafka.consumer;

import java.util.ArrayList;
import java.util.List;

public class ConsumerGroup {

    private final String groupId;

    private final List<Consumer> consumers =
            new ArrayList<>();

    public ConsumerGroup(String groupId) {
        this.groupId = groupId;
    }

    public void addConsumer(Consumer consumer) {

        if (!consumer.getConsumerGroup().equals(groupId)) {
            throw new IllegalArgumentException(
                    "Consumer does not belong to group: " + groupId
            );
        }

        consumers.add(consumer);
    }

    public Consumer getConsumerForPartition(int partitionId) {

        if (consumers.isEmpty()) {
            throw new IllegalStateException(
                    "Consumer group has no consumers"
            );
        }

        int consumerIndex =
                Math.floorMod(
                        partitionId,
                        consumers.size()
                );

        return consumers.get(consumerIndex);
    }

    public String getGroupId() {
        return groupId;
    }

    public List<Consumer> getConsumers() {
        return new ArrayList<>(consumers);
    }
}