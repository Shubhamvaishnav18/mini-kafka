package com.example.mini_kafka.controller;

import com.example.mini_kafka.broker.Broker;
import com.example.mini_kafka.consumer.Consumer;
import com.example.mini_kafka.consumer.OffsetManager;
import com.example.mini_kafka.dto.ConsumeRequest;
import com.example.mini_kafka.dto.ConsumeMessageResponse;
import com.example.mini_kafka.model.Message;
import org.springframework.web.bind.annotation.*;
import jakarta.validation.Valid;

import java.util.List;

@RestController
@RequestMapping("/topics/{topicName}/consume")
public class ConsumerController {

    private final Broker broker;
    private final OffsetManager offsetManager;

    public ConsumerController(
            Broker broker,
            OffsetManager offsetManager
    ) {
        this.broker = broker;
        this.offsetManager = offsetManager;
    }

    @PostMapping
    public List<ConsumeMessageResponse> consume(
            @PathVariable String topicName,
            @Valid @RequestBody ConsumeRequest request
    ) {

        Consumer consumer = new Consumer(
                request.getConsumerId(),
                request.getConsumerGroup(),
                broker,
                offsetManager
        );

        List<Message> messages = consumer.consume(
                topicName,
                request.getPartitionId()
        );

        return messages.stream()
                .map(message -> new ConsumeMessageResponse(
                        message.getOffset(),
                        message.getKey(),
                        message.getValue(),
                        message.getTimestamp()
                ))
                .toList();
    }
}