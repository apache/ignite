package org.apache.ignite.internal.processors.cache.index;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;

/**
 * Test to check work of DML+DDL operations of transactional partitioned cache with queries initiated from server node.
 */
public class H2DynamicIndexingComplexServerTransactionalPartitionedTest extends H2DynamicIndexingComplexTest {
    /**
     * Constructor.
     */
    public H2DynamicIndexingComplexServerTransactionalPartitionedTest() {
        super(CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL, SRV_IDX);
    }
}
