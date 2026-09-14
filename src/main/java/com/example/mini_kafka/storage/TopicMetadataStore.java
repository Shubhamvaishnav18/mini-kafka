package com.example.mini_kafka.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopicMetadataStore {

    private static final Path FILE_PATH =
            Path.of("data", "topics.meta");

    public TopicMetadataStore() {

        try {
            Files.createDirectories(
                    FILE_PATH.getParent()
            );

            if (!Files.exists(FILE_PATH)) {
                Files.createFile(FILE_PATH);
            }

        } catch (IOException exception) {
            throw new RuntimeException(
                    "Failed to initialize topic metadata",
                    exception
            );
        }
    }

    public synchronized void saveTopic(
            String topicName,
            int partitionCount
    ) {

        try {

            String line =
                    topicName
                            + "|"
                            + partitionCount
                            + System.lineSeparator();

            Files.writeString(
                    FILE_PATH,
                    line,
                    java.nio.file.StandardOpenOption.APPEND
            );

        } catch (IOException exception) {
            throw new RuntimeException(
                    "Failed to save topic metadata",
                    exception
            );
        }
    }

    public synchronized Map<String, Integer> loadTopics() {

        Map<String, Integer> topics =
                new HashMap<>();

        try {

            List<String> lines =
                    Files.readAllLines(FILE_PATH);

            for (String line : lines) {

                String[] parts =
                        line.split("\\|");

                if (parts.length != 2) {
                    continue;
                }

                String topicName = parts[0];

                int partitionCount =
                        Integer.parseInt(parts[1]);

                topics.put(
                        topicName,
                        partitionCount
                );
            }

        } catch (IOException exception) {
            throw new RuntimeException(
                    "Failed to load topic metadata",
                    exception
            );
        }

        return topics;
    }
}