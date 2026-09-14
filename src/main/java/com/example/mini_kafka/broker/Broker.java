package com.example.mini_kafka.broker;

import com.example.mini_kafka.storage.TopicMetadataStore;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Broker {

    private final Map<String, Topic> topics =
            new ConcurrentHashMap<>();

    private final TopicMetadataStore topicMetadataStore;

    public Broker() {

        this.topicMetadataStore =
                new TopicMetadataStore();

        loadExistingTopics();
    }

    public void createTopic(
            String topicName,
            int partitionCount
    ) {

        Topic topic =
                new Topic(
                        topicName,
                        partitionCount
                );

        Topic existingTopic =
                topics.putIfAbsent(
                        topicName,
                        topic
                );

        if (existingTopic != null) {
            throw new IllegalArgumentException(
                    "Topic already exists: " + topicName
            );
        }

        topicMetadataStore.saveTopic(
                topicName,
                partitionCount
        );
    }

    public Topic getTopic(String topicName) {

        Topic topic =
                topics.get(topicName);

        if (topic == null) {
            throw new IllegalArgumentException(
                    "Topic does not exist: " + topicName
            );
        }

        return topic;
    }

    private void loadExistingTopics() {

        Map<String, Integer> storedTopics =
                topicMetadataStore.loadTopics();

        for (
                Map.Entry<String, Integer> entry
                : storedTopics.entrySet()
        ) {

            String topicName =
                    entry.getKey();

            int partitionCount =
                    entry.getValue();

            topics.put(
                    topicName,
                    new Topic(
                            topicName,
                            partitionCount
                    )
            );
        }
    }
}