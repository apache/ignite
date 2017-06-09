package org.apache.ignite.internal.processors.cache.index;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;

/**
 * Test to check work of DML+DDL operations of atomic replicated cache with queries initiated from server node.
 */
public class H2DynamicIndexingComplexServerAtomicReplicatedTest extends H2DynamicIndexingComplexTest {
    /**
     * Constructor.
     */
    public H2DynamicIndexingComplexServerAtomicReplicatedTest() {
        super(CacheMode.REPLICATED, CacheAtomicityMode.ATOMIC, SRV_IDX);
    }
}
