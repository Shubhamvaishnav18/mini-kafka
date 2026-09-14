package com.example.mini_kafka.controller;

import com.example.mini_kafka.broker.Broker;
import com.example.mini_kafka.broker.Partition;
import com.example.mini_kafka.broker.Topic;
import com.example.mini_kafka.dto.ConsumeMessageResponse;
import com.example.mini_kafka.model.Message;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/topics/{topicName}/partitions/{partitionId}/messages")
public class MessageController {

    private final Broker broker;

    public MessageController(Broker broker) {
        this.broker = broker;
    }

    @GetMapping
    public List<ConsumeMessageResponse> getMessages(
            @PathVariable String topicName,
            @PathVariable int partitionId,
            @RequestParam(defaultValue = "0") long offset
    ) {

        Topic topic = broker.getTopic(topicName);

        Partition partition =
                topic.getPartition(partitionId);

        List<Message> messages =
                partition.readFrom(offset);

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