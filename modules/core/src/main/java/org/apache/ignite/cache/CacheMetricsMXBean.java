/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.cache;

import org.gridgain.grid.cache.GridCache;

import javax.cache.management.*;

/**
 * Implementation of {@link CacheStatisticsMXBean}.
 */
public class CacheMetricsMXBean implements CacheStatisticsMXBean {

    /**
     * Grid cache.
     */
    private final GridCache cache;

    /**
     * Constructor.
     *
     * @param cache GridCache
     */
    public CacheMetricsMXBean(GridCache cache) {
        this.cache = cache;
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        cache.resetMetrics();
    }

    /** {@inheritDoc} */
    @Override public long getCacheHits() {
        return cache.metrics().hits();
    }

    /** {@inheritDoc} */
    @Override public float getCacheHitPercentage() {
        return cache.metrics().getCacheHitPercentage();
    }

    /** {@inheritDoc} */
    @Override public long getCacheMisses() {
        return cache.metrics().getCacheMisses();
    }

    /** {@inheritDoc} */
    @Override public float getCacheMissPercentage() {
        return cache.metrics().getCacheMissPercentage();
    }

    /** {@inheritDoc} */
    @Override public long getCacheGets() {
        return cache.metrics().getCacheGets();
    }

    /** {@inheritDoc} */
    @Override public long getCachePuts() {
        return cache.metrics().getCachePuts();
    }

    /** {@inheritDoc} */
    @Override public long getCacheRemovals() {
        return cache.metrics().getCacheRemovals();
    }

    /** {@inheritDoc} */
    @Override public long getCacheEvictions() {
        return cache.metrics().getCacheEvictions();
    }

    /** {@inheritDoc} */
    @Override public float getAverageGetTime() {
        return cache.metrics().getAverageGetTime();
    }

    /** {@inheritDoc} */
    @Override public float getAveragePutTime() {
        return cache.metrics().getAveragePutTime();
    }

    /** {@inheritDoc} */
    @Override public float getAverageRemoveTime() {
        return cache.metrics().getAverageRemoveTime();
    }
}
