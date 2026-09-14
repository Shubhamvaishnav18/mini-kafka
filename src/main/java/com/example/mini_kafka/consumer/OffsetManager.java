package com.example.mini_kafka.consumer;

import com.example.mini_kafka.storage.OffsetStore;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OffsetManager {

    private final Map<String, Long> offsets;

    private final OffsetStore offsetStore;

    public OffsetManager() {

        this.offsetStore = new OffsetStore();

        this.offsets = new ConcurrentHashMap<>(
                offsetStore.loadOffsets()
        );
    }

    public long getOffset(
            String consumerGroup,
            String topicName,
            int partitionId
    ) {

        String offsetKey = createOffsetKey(
                consumerGroup,
                topicName,
                partitionId
        );

        return offsets.getOrDefault(
                offsetKey,
                0L
        );
    }

    public synchronized void commitOffset(
            String consumerGroup,
            String topicName,
            int partitionId,
            long nextOffset
    ) {

        String offsetKey = createOffsetKey(
                consumerGroup,
                topicName,
                partitionId
        );

        offsets.compute(
                offsetKey,
                (key, currentOffset) -> {

                    if (currentOffset == null) {
                        return nextOffset;
                    }

                    return Math.max(
                            currentOffset,
                            nextOffset
                    );
                }
        );

        offsetStore.saveOffsets(offsets);
    }

    private String createOffsetKey(
            String consumerGroup,
            String topicName,
            int partitionId
    ) {

        return consumerGroup
                + ":"
                + topicName
                + ":"
                + partitionId;
    }
}