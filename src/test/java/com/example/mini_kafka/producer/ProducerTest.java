package com.example.mini_kafka.producer;

import com.example.mini_kafka.broker.Broker;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProducerTest {

    @Test
    void shouldProduceMessagesWithCorrectOffsets() {

        Broker broker = new Broker();

        broker.createTopic("producer-test", 3);

        Producer producer =
                new Producer(broker);

        long firstOffset =
                producer.send(
                        "producer-test",
                        "same-key",
                        "First message"
                );

        long secondOffset =
                producer.send(
                        "producer-test",
                        "same-key",
                        "Second message"
                );

        assertEquals(0, firstOffset);

        assertEquals(1, secondOffset);
    }
}