package com.example.mini_kafka.dto;

public class ConsumeMessageResponse {

    private final long offset;
    private final String key;
    private final String value;
    private final long timestamp;

    public ConsumeMessageResponse(
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