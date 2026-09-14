package com.example.mini_kafka.dto;

public class ProduceMessageResponse {

    private final long offset;

    public ProduceMessageResponse(long offset) {
        this.offset = offset;
    }

    public long getOffset() {
        return offset;
    }
}