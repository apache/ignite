package org.apache.ignite.internal.processors.cache.index;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;

/**
 *
 */
public class H2DynamicIndexingComplexAtomicPartitionedTest extends H2DynamicIndexingComplexTest {
    /**
     * Constructor.
     */
    protected H2DynamicIndexingComplexAtomicPartitionedTest() {
        super(CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC, 0);
    }
}
