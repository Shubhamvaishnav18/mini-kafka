package com.example.mini_kafka.dto;

import jakarta.validation.constraints.NotBlank;

public class ProduceMessageRequest {

    @NotBlank(message = "Key cannot be blank")
    private String key;

    @NotBlank(message = "Value cannot be blank")
    private String value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}