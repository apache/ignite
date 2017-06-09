package org.apache.ignite.internal.processors.cache.index;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;

/**
 * Test to check work of DML+DDL operations of atomic replicated cache with queries initiated from client node.
 */
public class H2DynamicIndexingComplexClientAtomicReplicatedTest extends H2DynamicIndexingComplexTest {
    /**
     * Constructor.
     */
    public H2DynamicIndexingComplexClientAtomicReplicatedTest() {
        super(CacheMode.REPLICATED, CacheAtomicityMode.ATOMIC, CLIENT_IDX);
    }
}
