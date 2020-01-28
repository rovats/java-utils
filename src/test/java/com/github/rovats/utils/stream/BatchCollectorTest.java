/*
 * 
 * Copyright (C) 2016, 2017, 2018, 2019, 2020 Rohit Vats 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific
 *
 */

package com.github.rovats.utils.stream;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

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
    public void numRecordsProcessed() {
        List<Integer> input = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        List<Integer> output = new ArrayList<>();

        BatchCollector<Integer> batchCollector = StreamUtils.batchCollector(3, xs -> output.addAll(xs));

        input.stream()
                .collect(batchCollector);

        assertEquals(10, batchCollector.getNumRecordsProcessed());
    }
}
