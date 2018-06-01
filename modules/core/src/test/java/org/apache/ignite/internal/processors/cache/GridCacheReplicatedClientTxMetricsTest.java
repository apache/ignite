package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.cache.CacheMode;

import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 *
 */
public class GridCacheReplicatedClientTxMetricsTest extends GridCachePartitionedClientTxMetricsTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return REPLICATED;
    }
}
