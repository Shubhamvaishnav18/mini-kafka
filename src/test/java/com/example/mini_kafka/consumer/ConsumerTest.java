package com.example.mini_kafka.consumer;

import com.example.mini_kafka.broker.Broker;
import com.example.mini_kafka.model.Message;
import com.example.mini_kafka.producer.Producer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConsumerTest {

    @Test
    void shouldConsumeMessagesAndCommitOffset() {

        Broker broker = new Broker();

        broker.createTopic("consumer-test", 1);

        Producer producer =
                new Producer(broker);

        OffsetManager offsetManager =
                new OffsetManager();

        Consumer consumer =
                new Consumer(
                        "consumer-1",
                        "test-group",
                        broker,
                        offsetManager
                );

        producer.send(
                "consumer-test",
                "key-1",
                "Message 1"
        );

        producer.send(
                "consumer-test",
                "key-2",
                "Message 2"
        );

        List<Message> messages =
                consumer.consume(
                        "consumer-test",
                        0
                );

        assertEquals(
                2,
                messages.size()
        );

        assertEquals(
                2,
                offsetManager.getOffset(
                        "test-group",
                        "consumer-test",
                        0
                )
        );

        List<Message> secondConsume =
                consumer.consume(
                        "consumer-test",
                        0
                );

        assertEquals(
                0,
                secondConsume.size()
        );
    }
}