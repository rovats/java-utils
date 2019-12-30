/**
 * StreamUtilsTest.java
 */
package com.github.rovats.utils.stream;

import org.junit.Assert;
import org.junit.Test;

public class StreamUtilsTest {

    @Test
    public void testBatchCollector() {
        Assert.assertTrue(StreamUtils.batchCollector(100, e -> {}) != null);
    }

}
