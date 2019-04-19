/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.evict.PageEvictionTracker;

/**
 * Data region provides access to objects configured with {@link DataRegionConfiguration} configuration.
 */
public class DataRegion {
    /** */
    private final PageMemory pageMem;

    /** */
    private final DataRegionMetricsImpl memMetrics;

    /** */
    private final DataRegionConfiguration cfg;

    /** */
    private final PageEvictionTracker evictionTracker;

    /**
     * @param pageMem PageMemory instance.
     * @param memMetrics DataRegionMetrics instance.
     * @param cfg Configuration of given DataRegion.
     * @param evictionTracker Eviction tracker.
     */
    public DataRegion(
        PageMemory pageMem,
        DataRegionConfiguration cfg,
        DataRegionMetricsImpl memMetrics,
        PageEvictionTracker evictionTracker
    ) {
        this.pageMem = pageMem;
        this.memMetrics = memMetrics;
        this.cfg = cfg;
        this.evictionTracker = evictionTracker;
    }

    /**
     *
     */
    public PageMemory pageMemory() {
        return pageMem;
    }

    /**
     * @return Config.
     */
    public DataRegionConfiguration config() {
        return cfg;
    }

    /**
     * @return Memory Metrics.
     */
    public DataRegionMetricsImpl memoryMetrics() {
        return memMetrics;
    }

    /**
     *
     */
    public PageEvictionTracker evictionTracker() {
        return evictionTracker;
    }
}
