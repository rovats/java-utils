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


import java.util.List;
import java.util.function.Consumer;

public class StreamUtils {

    /**
     * Creates a new batch collector
     * @param batchSize the batch size after which the batchProcessor should be called
     * @param batchProcessor the batch processor which accepts batches of records to process
     * @param <T> the type of elements being processed
     * @return a batch collector instance
     */
    public static <T> BatchCollector<T> batchCollector(int batchSize, Consumer<List<T>> batchProcessor) {
        return new BatchCollector<T>(batchSize, batchProcessor);
    }
}
