package org.apache.ignite.internal.processors.cache.datastructures.local;

import java.util.UUID;
import org.apache.ignite.IgniteMultimap;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.processors.cache.datastructures.GridCacheMultimapApiSelfAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.LOCAL;

public abstract class GridCacheAbstractLocalAtomicMultimapApiSelfTest extends GridCacheMultimapApiSelfAbstractTest {

    /** {@inheritDoc} */
    @Override public void testMultimapCollocationMode() throws Exception {
        String multimapName = UUID.randomUUID().toString();
        final IgniteMultimap<String, String> multimap = grid(0).multimap(multimapName, config(collocated()));

        assertTrue(getMultimapBackingCache(multimap).name().startsWith("datastructures_" + collectionCacheAtomicityMode() + "_" + collectionCacheMode()));
        assertTrue(multimap.collocated());
    }

    /** {@inheritDoc} */
    @Override protected CacheMode collectionCacheMode() {
        return LOCAL;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode collectionCacheAtomicityMode() {
        return ATOMIC;
    }
}
