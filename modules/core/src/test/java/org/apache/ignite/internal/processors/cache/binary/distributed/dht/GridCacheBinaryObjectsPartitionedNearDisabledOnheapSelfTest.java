package org.apache.ignite.internal.processors.cache.binary.distributed.dht;

public class GridCacheBinaryObjectsPartitionedNearDisabledOnheapSelfTest extends GridCacheBinaryObjectsPartitionedNearDisabledSelfTest {
    /** {@inheritDoc} */
    @Override protected boolean onheapCacheEnabled() {
        return true;
    }
}
