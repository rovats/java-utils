package com.github.rovats.utils.stream;


import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collector;

public class StreamUtils {

    public static <T> Collector<T, List<T>, List<T>> batchCollector(int batchSize, Consumer<List<T>> batchProcessor) {
        return new BatchCollector<T>(batchSize, batchProcessor);
    }
}
