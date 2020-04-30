# java-utils
A collection of useful Java utilities

### Batch Collector
A stream collector that allows batch processing of elements with a given `Consumer` (batch processor).

Use the supplied utility class to get new instances:

```java
List<Integer> input = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
List<Integer> output = new ArrayList<>();

int batchSize = 3;
Consumer<List<Integer>> batchProcessor = xs -> output.addAll(xs);

input.stream()
     .collect(StreamUtils.batchCollector(batchSize, batchProcessor));
```
To fetch the number of records processed by the batch collector:

```
List<Integer> input = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
List<Integer> output = new ArrayList<>();

BatchCollector<Integer> batchCollector = StreamUtils.batchCollector(3, xs -> output.addAll(xs));

input.stream()
     .collect(batchCollector);

assertEquals(10, batchCollector.getNumRecordsProcessed());
```
