package org.apache.ignite.internal.processors.cache.index;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;

/**
 * Test to check work of DML+DDL operations of transactional replicated cache with queries initiated from client node.
 */
public class H2DynamicIndexingComplexServerTransactionalReplicatedTest extends H2DynamicIndexingComplexTest {
    /**
     * Constructor.
     */
    public H2DynamicIndexingComplexServerTransactionalReplicatedTest() {
        super(CacheMode.REPLICATED, CacheAtomicityMode.TRANSACTIONAL, SRV_IDX);
    }
}
