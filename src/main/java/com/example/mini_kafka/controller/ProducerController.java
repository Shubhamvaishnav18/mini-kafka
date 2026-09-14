package com.example.mini_kafka.controller;

import com.example.mini_kafka.dto.ProduceMessageRequest;
import com.example.mini_kafka.dto.ProduceMessageResponse;
import com.example.mini_kafka.producer.Producer;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/topics/{topicName}/messages")
public class ProducerController {

    private final Producer producer;

    public ProducerController(Producer producer) {
        this.producer = producer;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ProduceMessageResponse produce(
            @PathVariable String topicName,
            @Valid @RequestBody ProduceMessageRequest request
    ) {

        long offset = producer.send(
                topicName,
                request.getKey(),
                request.getValue()
        );

        return new ProduceMessageResponse(offset);
    }
}