package org.apache.ignite.internal.processors.cache.index;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;

/**
 * Test to check work of DML+DDL operations of transactional partitioned cache with queries initiated from client node.
 */
public class H2DynamicIndexingComplexClientTransactionalPartitionedTest extends H2DynamicIndexingComplexTest {
    /**
     * Constructor.
     */
    public H2DynamicIndexingComplexClientTransactionalPartitionedTest() {
        super(CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL, CLIENT_IDX);
    }
}
