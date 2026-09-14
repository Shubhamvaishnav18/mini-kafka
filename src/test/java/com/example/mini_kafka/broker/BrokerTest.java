package com.example.mini_kafka.broker;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BrokerTest {

    @Test
    void shouldCreateAndRetrieveTopic() {

        Broker broker = new Broker();

        broker.createTopic("test-topic", 3);

        Topic topic =
                broker.getTopic("test-topic");

        assertNotNull(topic);

        assertEquals(
                3,
                topic.getPartitionCount()
        );
    }
}