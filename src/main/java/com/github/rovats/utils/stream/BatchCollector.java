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


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.*;
import java.util.stream.Collector;

import static java.util.Objects.requireNonNull;


/**
 * Collects elements in the stream and calls the supplied batch processor
 * after the configured batch size is reached.
 *
 * In case of a parallel stream, the batch processor may be called with
 * elements less than the batch size.
 *
 * The elements are not kept in memory, and the final result will be an
 * empty list.
 *
 * @param <T> Type of the elements being collected
 */
class BatchCollector<T> implements Collector<T, List<T>, List<T>> {

    private final int batchSize;
    private final Consumer<List<T>> batchProcessor;
    private long recordsProcessed = 0;


    /**
     * Constructs the batch collector
     *
     * @param batchSize the batch size after which the batchProcessor should be called
     * @param batchProcessor the batch processor which accepts batches of records to process
     */
    BatchCollector(int batchSize, Consumer<List<T>> batchProcessor) {
        batchProcessor = requireNonNull(batchProcessor);

        this.batchSize = batchSize;
        this.batchProcessor = batchProcessor;
    }

    public Supplier<List<T>> supplier() {
        return ArrayList::new;
    }

    public BiConsumer<List<T>, T> accumulator() {
        return (ts, t) -> {
            ts.add(t);
            if (ts.size() >= batchSize) {
                batchProcessor.accept(ts);
                recordsProcessed += ts.size();
                ts.clear();
            }
        };
    }

    public BinaryOperator<List<T>> combiner() {
        return (ts, ots) -> {
            // process each parallel list without checking for batch size
            // avoids adding all elements of one to another
            // can be modified if a strict batching mode is required
            batchProcessor.accept(ts);
            batchProcessor.accept(ots);
            recordsProcessed += (ts.size() + ots.size());
            return Collections.emptyList();
        };
    }

    public Function<List<T>, List<T>> finisher() {
        return ts -> {
            batchProcessor.accept(ts);
            recordsProcessed += ts.size();
            return Collections.emptyList();
        };
    }

    public Set<Characteristics> characteristics() {
        return Collections.emptySet();
    }

    public long getNumRecordsProcessed() {
        return recordsProcessed;
    }
}
