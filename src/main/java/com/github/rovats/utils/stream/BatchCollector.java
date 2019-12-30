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
    private final boolean enforceStrictBatching;
    private final Consumer<List<T>> batchProcessor;

    /**
     * Constructs the batch collector
     *
     * @param batchSize
     *            the batch size after which the batchProcessor should be called
     * @param enforceStrictBatching
     *           If true, the collector enforces processing in strict batches of
     *           the specified size
     *  @param batchProcessor
     *            the batch processor which accepts batches of records to
     *            process
     */
    BatchCollector(int batchSize, boolean enforceStrictBatching, Consumer<List<T>> batchProcessor) {
        this.batchSize = batchSize;
        this.enforceStrictBatching = enforceStrictBatching;
        this.batchProcessor = requireNonNull(batchProcessor);
    }

    /**
     * Constructs the batch collector
     *
     * @param batchSize the batch size after which the batchProcessor should be called
     * @param batchProcessor the batch processor which accepts batches of records to process
     */
    BatchCollector(int batchSize, Consumer<List<T>> batchProcessor) {
        this(batchSize, false, batchProcessor);
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
            if (enforceStrictBatching) {
                // implement strict batching mode is required
                List<T> combinedTarget = ts;
                int numOther = ots.size();
                for (int iIdx = 0; iIdx < numOther; iIdx++) {
                    combinedTarget = processCompletedBatch(combinedTarget);
                    combinedTarget.add(ots.get(iIdx));
                }
                return processCompletedBatch(combinedTarget);
            }
            else {
                // process each parallel list without checking for batch size
                // avoids adding all elements of one to another
                batchProcessor.accept(ts);
                batchProcessor.accept(ots);
                return Collections.emptyList();
            }
        };
    }

    /**
     * if the list has reached the 'batchSize', invoke the batchProcessor
     * 
     * @param combinedTarget
     * @return
     */
    protected List<T> processCompletedBatch(List<T> combinedTarget) {
        if (combinedTarget.size() == batchSize) {
            batchProcessor.accept(combinedTarget);
            return new ArrayList<>();
        }
        return combinedTarget;
    }

    public Function<List<T>, List<T>> finisher() {
        return ts -> {
            batchProcessor.accept(ts);
            return Collections.emptyList();
        };
    }

    public Set<Characteristics> characteristics() {
        return Collections.emptySet();
    }
}
