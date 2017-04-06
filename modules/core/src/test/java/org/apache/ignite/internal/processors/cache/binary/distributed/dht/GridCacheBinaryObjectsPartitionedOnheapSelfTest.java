package org.apache.ignite.internal.processors.cache.binary.distributed.dht;

public class GridCacheBinaryObjectsPartitionedOnheapSelfTest extends GridCacheBinaryObjectsPartitionedSelfTest {
    /** {@inheritDoc} */
    @Override protected boolean onheapCacheEnabled() {
        return true;
    }
}
