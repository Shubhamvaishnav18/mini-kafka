package com.example.mini_kafka.consumer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OffsetManagerTest {

    @Test
    void shouldMaintainSeparateOffsetsForDifferentConsumerGroups() {

        OffsetManager offsetManager =
                new OffsetManager();

        offsetManager.commitOffset(
                "group-1",
                "orders",
                0,
                5
        );

        offsetManager.commitOffset(
                "group-2",
                "orders",
                0,
                10
        );

        assertEquals(
                5,
                offsetManager.getOffset(
                        "group-1",
                        "orders",
                        0
                )
        );

        assertEquals(
                10,
                offsetManager.getOffset(
                        "group-2",
                        "orders",
                        0
                )
        );
    }
}