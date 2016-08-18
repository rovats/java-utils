package com.github.rovats.utils.stream;


import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;

class BatchCollector<T> implements Collector<T, List<T>, List<T>> {

    private final int batchSize;
    private final Consumer<List<T>> batchProcessor;

    BatchCollector(int batchSize, Consumer<List<T>> batchProcessor) {
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
            return Collections.emptyList();
        };
    }

    public Function<List<T>, List<T>> finisher() {
        return ts -> {
            batchProcessor.accept(ts);
            return Collections.emptyList();
        };
    }

    public Set<Characteristics> characteristics() {
        return EnumSet.noneOf(Characteristics.class);
    }
}
