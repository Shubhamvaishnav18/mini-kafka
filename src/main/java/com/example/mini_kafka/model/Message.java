package com.example.mini_kafka.model;

public class Message {

    private final long offset;
    private final String key;
    private final String value;
    private final long timestamp;

    public Message(
            long offset,
            String key,
            String value
    ) {
        this(
                offset,
                key,
                value,
                System.currentTimeMillis()
        );
    }

    public Message(
            long offset,
            String key,
            String value,
            long timestamp
    ) {
        this.offset = offset;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public long getOffset() {
        return offset;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }
}