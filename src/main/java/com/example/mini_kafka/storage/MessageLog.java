package com.example.mini_kafka.storage;

import com.example.mini_kafka.model.Message;

import java.util.ArrayList;
import java.util.List;

public class MessageLog {

    private final List<Message> messages;

    private final FileMessageStore fileMessageStore;

    public MessageLog(FileMessageStore fileMessageStore) {

        this.fileMessageStore = fileMessageStore;

        this.messages = new ArrayList<>(
                fileMessageStore.loadMessages()
        );
    }

    public synchronized void append(Message message) {

        fileMessageStore.append(message);

        messages.add(message);
    }

    public synchronized Message read(long offset) {

        if (offset < 0 || offset >= messages.size()) {
            return null;
        }

        return messages.get((int) offset);
    }

    public synchronized long getNextOffset() {

        return messages.size();
    }

    public synchronized List<Message> readFrom(long offset) {

        if (offset < 0 || offset >= messages.size()) {
            return List.of();
        }

        return new ArrayList<>(
                messages.subList(
                        (int) offset,
                        messages.size()
                )
        );
    }
}