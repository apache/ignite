package org.apache.ignite.internal.processors.cache.datastructures.replicated;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.processors.cache.datastructures.GridCacheSetClientAbstractTest;

public class GridCacheReplicatedAtomicSetClientTest extends GridCacheSetClientAbstractTest {

    @Override protected CacheMode collectionCacheMode() {
        return CacheMode.REPLICATED;
    }

    @Override protected CacheAtomicityMode collectionCacheAtomicityMode() {
        return CacheAtomicityMode.ATOMIC;
    }
}
