package com.github.rovats.utils.stream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.Assert;
import org.junit.Test;

public class BatchCollectorTest {

    @Test
    public void serialStream() {
        List<Integer> input = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        List<Integer> output = new ArrayList<>();

        input.stream()
                .collect(StreamUtils.batchCollector(3, xs -> output.addAll(xs)));

        assertArrayEquals(input.toArray(), output.toArray());
    }

    @Test
    public void parallelStream() {
        List<Integer> input = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        List<Integer> output = new CopyOnWriteArrayList<>();

        input.parallelStream()
                .collect(StreamUtils.batchCollector(3, xs -> output.addAll(xs)));

        output.sort(Integer::compareTo);
        assertArrayEquals(input.toArray(), output.toArray());
    }

    @Test
    public void exactBatchSize() {
        List<Integer> input = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        List<Integer> output = new ArrayList<>();

        input.stream()
                .collect(StreamUtils.batchCollector(10, xs -> output.addAll(xs)));

        assertArrayEquals(input.toArray(), output.toArray());
    }

    @Test
    public void biggerBatchSize() {
        List<Integer> input = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        List<Integer> output = new ArrayList<>();

        input.stream()
                .collect(StreamUtils.batchCollector(20, xs -> output.addAll(xs)));

        assertArrayEquals(input.toArray(), output.toArray());
    }

    @Test
    public void parallelStrictBatchSizeBatchCollector() {
        List<String> dataSet = Arrays.asList("S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9", "S10");
        
        int dataSize = dataSet.size();
        int batchSize = 3;
        List<String> finalList = dataSet
                                    .stream()
                                    .parallel()
                                    .map(e -> e)
                                    .collect(
                                         StreamUtils.batchCollector(
                                                             batchSize,
                                                             true, // enforce strict batching
                                                             e -> assertTrue(e.size() == batchSize || e.size() == dataSize % batchSize)
                                                             )
                                         );
        Assert.assertTrue(finalList.size() == 0);

        
    }

}