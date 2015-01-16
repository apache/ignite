/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache;

import org.gridgain.grid.cache.*;

import javax.cache.management.*;

/**
 * Implementation of {@link CacheStatisticsMXBean}.
 */
public class CacheMetricsMXBean implements CacheStatisticsMXBean {
    /** Grid cache. */
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
