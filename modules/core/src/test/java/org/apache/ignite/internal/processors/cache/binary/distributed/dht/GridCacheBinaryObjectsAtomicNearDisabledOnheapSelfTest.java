package org.apache.ignite.internal.processors.cache.binary.distributed.dht;

public class GridCacheBinaryObjectsAtomicNearDisabledOnheapSelfTest extends GridCacheBinaryObjectsAtomicNearDisabledSelfTest {
    /** {@inheritDoc} */
    @Override protected boolean onheapCacheEnabled() {
        return true;
    }
}
