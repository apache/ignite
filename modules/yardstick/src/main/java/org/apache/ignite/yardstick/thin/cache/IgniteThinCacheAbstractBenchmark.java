package org.apache.ignite.yardstick.thin.cache;

import org.apache.ignite.client.ClientCache;
import org.apache.ignite.yardstick.IgniteThinAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

/**
 *
 * Abstract class for thin client benchmarks which use cache.
 */
public abstract class IgniteThinCacheAbstractBenchmark<K, V> extends IgniteThinAbstractBenchmark {
    /** Cache. */
    protected ClientCache<K, V> cache;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        cache = cache();
    }

    /**
     * Each benchmark must determine which cache will be used.
     *
     * @return ClientCache Cache to use.
     */
    protected abstract ClientCache<K, V> cache();
}
