package com.example.mini_kafka.controller;

import com.example.mini_kafka.broker.Broker;
import com.example.mini_kafka.dto.CreateTopicRequest;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import jakarta.validation.Valid;

@RestController
@RequestMapping("/topics")
public class TopicController {

    private final Broker broker;

    public TopicController(Broker broker) {
        this.broker = broker;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public String createTopic(
            @Valid @RequestBody CreateTopicRequest request
    ) {

        broker.createTopic(
                request.getTopicName(),
                request.getPartitionCount()
        );

        return "Topic created successfully";
    }
}