package org.apache.ignite.yardstick.cache.load;

import org.apache.ignite.IgniteCache;

/**
 *
 */
public class IgniteWALModesLoadIndexedBenchmark extends IgniteWALModesLoadBenchmark {
    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().getOrCreateCache(cacheCfg(true));
    }
}
