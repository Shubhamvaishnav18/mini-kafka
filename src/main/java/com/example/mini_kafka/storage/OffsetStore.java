package com.example.mini_kafka.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OffsetStore {

    private static final Path FILE_PATH =
            Path.of("data", "offsets.meta");

    public OffsetStore() {

        try {
            Files.createDirectories(
                    FILE_PATH.getParent()
            );

            if (!Files.exists(FILE_PATH)) {
                Files.createFile(FILE_PATH);
            }

        } catch (IOException exception) {
            throw new RuntimeException(
                    "Failed to initialize offset storage",
                    exception
            );
        }
    }

    public synchronized Map<String, Long> loadOffsets() {

        Map<String, Long> offsets =
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

                offsets.put(
                        parts[0],
                        Long.parseLong(parts[1])
                );
            }

        } catch (IOException exception) {
            throw new RuntimeException(
                    "Failed to load offsets",
                    exception
            );
        }

        return offsets;
    }

    public synchronized void saveOffsets(
            Map<String, Long> offsets
    ) {

        try {

            List<String> lines =
                    offsets.entrySet()
                            .stream()
                            .map(entry ->
                                    entry.getKey()
                                            + "|"
                                            + entry.getValue()
                            )
                            .toList();

            Files.write(
                    FILE_PATH,
                    lines,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            );

        } catch (IOException exception) {
            throw new RuntimeException(
                    "Failed to save offsets",
                    exception
            );
        }
    }
}