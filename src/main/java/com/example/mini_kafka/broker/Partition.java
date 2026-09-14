package com.example.mini_kafka.broker;

import com.example.mini_kafka.model.Message;
import com.example.mini_kafka.storage.FileMessageStore;
import com.example.mini_kafka.storage.MessageLog;

import java.nio.file.Path;
import java.util.List;

public class Partition {

    private final int id;

    private final MessageLog messageLog;

    public Partition(
            String topicName,
            int id
    ) {

        this.id = id;

        Path filePath = Path.of(
                "data",
                topicName,
                "partition-" + id + ".log"
        );

        FileMessageStore fileMessageStore =
                new FileMessageStore(filePath);

        this.messageLog =
                new MessageLog(fileMessageStore);
    }

    public synchronized long append(String key, String value) {

        long offset =
                messageLog.getNextOffset();

        Message message =
                new Message(
                        offset,
                        key,
                        value
                );

        messageLog.append(message);

        return offset;
    }

    public Message read(long offset) {

        return messageLog.read(offset);
    }

    public List<Message> readFrom(long offset) {

        return messageLog.readFrom(offset);
    }

    public int getId() {

        return id;
    }

    public long getNextOffset() {

        return messageLog.getNextOffset();
    }
}