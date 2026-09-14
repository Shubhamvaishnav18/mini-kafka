package com.example.mini_kafka.consumer;

import com.example.mini_kafka.broker.Broker;
import com.example.mini_kafka.broker.Partition;
import com.example.mini_kafka.broker.Topic;
import com.example.mini_kafka.model.Message;

import java.util.List;

public class Consumer {

    private final String consumerId;
    private final String consumerGroup;
    private final Broker broker;
    private final OffsetManager offsetManager;

    public Consumer(
            String consumerId,
            String consumerGroup,
            Broker broker,
            OffsetManager offsetManager
    ) {
        this.consumerId = consumerId;
        this.consumerGroup = consumerGroup;
        this.broker = broker;
        this.offsetManager = offsetManager;
    }

    public List<Message> consume(
            String topicName,
            int partitionId
    ) {

        Topic topic = broker.getTopic(topicName);

        Partition partition = topic.getPartition(partitionId);

        long offset = offsetManager.getOffset(
                consumerGroup,
                topicName,
                partitionId
        );

        List<Message> messages =
                partition.readFrom(offset);

        for (Message message : messages) {

            offsetManager.commitOffset(
                    consumerGroup,
                    topicName,
                    partitionId,
                    message.getOffset() + 1
            );
        }

        return messages;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }
}