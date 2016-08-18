package com.github.rovats.utils.stream;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
}