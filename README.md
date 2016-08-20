# java-utils [![Build Status](https://snap-ci.com/rovats/java-utils/branch/master/build_image)](https://snap-ci.com/rovats/java-utils/branch/master)
A collection of useful Java utilities

### Batch Collector
A stream collector that allows batch processing of elements with a given `Consumer` (batch processor).

Use the supplied utility class to get new instances:

```java
List<Integer> input = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
List<Integer> output = new ArrayList<>();

input.stream()
     .collect(StreamUtils.batchCollector(3, xs -> output.addAll(xs)));
```
