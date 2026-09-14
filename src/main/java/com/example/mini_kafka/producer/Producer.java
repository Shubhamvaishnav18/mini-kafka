package com.example.mini_kafka.producer;

import com.example.mini_kafka.broker.Broker;
import com.example.mini_kafka.broker.Partition;
import com.example.mini_kafka.broker.Topic;

public class Producer {

    private final Broker broker;

    public Producer(Broker broker) {
        this.broker = broker;
    }

    public long send(String topicName, String key, String value) {

        Topic topic = broker.getTopic(topicName);

        int partitionId = getPartitionId(
                key,
                topic.getPartitionCount()
        );

        Partition partition = topic.getPartition(partitionId);

        long offset = partition.append(key, value);

        System.out.println(
                "Message published to topic=" + topicName +
                        ", partition=" + partitionId +
                        ", offset=" + offset
        );

        return offset;
    }

    private int getPartitionId(String key, int partitionCount) {

        return Math.floorMod(
                key.hashCode(),
                partitionCount
        );
    }
}