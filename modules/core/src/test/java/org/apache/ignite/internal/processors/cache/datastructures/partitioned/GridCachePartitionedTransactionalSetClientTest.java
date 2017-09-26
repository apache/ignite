package org.apache.ignite.internal.processors.cache.datastructures.partitioned;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.processors.cache.datastructures.GridCacheSetClientAbstractTest;

public class GridCachePartitionedTransactionalSetClientTest extends GridCacheSetClientAbstractTest {
    
    @Override protected CacheMode collectionCacheMode() {
        return CacheMode.PARTITIONED;
    }

    @Override protected CacheAtomicityMode collectionCacheAtomicityMode() {
        return CacheAtomicityMode.TRANSACTIONAL;
    }
}
