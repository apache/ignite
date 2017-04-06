package org.apache.ignite.internal.processors.cache.binary.distributed.dht;

public class GridCacheBinaryObjectsAtomicOnheapSelfTest extends GridCacheBinaryObjectsAtomicSelfTest {
    /** {@inheritDoc} */
    @Override protected boolean onheapCacheEnabled() {
        return true;
    }
}
