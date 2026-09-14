package com.example.mini_kafka.storage;

import com.example.mini_kafka.model.Message;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class FileMessageStore {

    private final Path filePath;

    public FileMessageStore(Path filePath) {

        this.filePath = filePath;

        try {
            Files.createDirectories(filePath.getParent());

            if (!Files.exists(filePath)) {
                Files.createFile(filePath);
            }

        } catch (IOException exception) {
            throw new RuntimeException(
                    "Failed to create message log file",
                    exception
            );
        }
    }

    public synchronized void append(Message message) {

        try (              // this is try with resource mean now i dont have to manually close the file. BufferedWriter implement autoClosable so i dont need care of it.
                BufferedWriter writer = Files.newBufferedWriter(
                        filePath,
                        StandardOpenOption.APPEND
                )
        ) {

            String encodedKey = encode(message.getKey());
            String encodedValue = encode(message.getValue());

            String line =
                    message.getOffset()
                            + "|"
                            + encodedKey
                            + "|"
                            + encodedValue
                            + "|"
                            + message.getTimestamp();

            writer.write(line);
            writer.newLine();

        } catch (IOException exception) {
            throw new RuntimeException(
                    "Failed to write message",
                    exception
            );
        }
    }

    public List<Message> loadMessages() {

        List<Message> messages = new ArrayList<>();

        try (
                BufferedReader reader =
                        Files.newBufferedReader(filePath)
        ) {

            String line;

            while ((line = reader.readLine()) != null) {

                String[] parts = line.split("\\|");

                if (parts.length != 4) {
                    continue;
                }

                long offset = Long.parseLong(parts[0]);

                String key = decode(parts[1]);

                String value = decode(parts[2]);

                long timestamp =
                        Long.parseLong(parts[3]);

                messages.add(
                        new Message(
                                offset,
                                key,
                                value,
                                timestamp
                        )
                );
            }

        } catch (IOException exception) {
            throw new RuntimeException(
                    "Failed to load messages",
                    exception
            );
        }

        return messages;
    }

    private String encode(String value) {

        return Base64.getEncoder()
                .encodeToString(value.getBytes());
    }

    private String decode(String value) {

        return new String(
                Base64.getDecoder().decode(value)
        );
    }
}