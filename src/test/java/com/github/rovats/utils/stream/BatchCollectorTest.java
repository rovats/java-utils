package com.github.rovats.utils.stream;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertArrayEquals;

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


}