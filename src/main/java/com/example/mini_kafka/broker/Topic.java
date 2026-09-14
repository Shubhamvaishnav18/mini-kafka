package com.example.mini_kafka.broker;

import java.util.ArrayList;
import java.util.List;

public class Topic {

    private final String name;
    private final List<Partition> partitions;

    public Topic(String name, int partitionCount) {

        this.name = name;
        this.partitions = new ArrayList<>();

        for (int i = 0; i < partitionCount; i++) {
            partitions.add(
                    new Partition(name, i)
            );
        }
    }

    public Partition getPartition(int partitionId) {

        if (partitionId < 0 || partitionId >= partitions.size()) {
            throw new IllegalArgumentException("Invalid partition id");
        }

        return partitions.get(partitionId);
    }

    public int getPartitionCount() {
        return partitions.size();
    }

    public String getName() {
        return name;
    }
}