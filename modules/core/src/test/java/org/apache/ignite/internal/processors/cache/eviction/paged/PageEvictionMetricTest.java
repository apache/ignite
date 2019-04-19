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
package org.apache.ignite.internal.processors.cache.eviction.paged;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class PageEvictionMetricTest extends PageEvictionAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return setEvictionMode(DataPageEvictionMode.RANDOM_LRU, super.getConfiguration(gridName));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPageEvictionMetric() throws Exception {
        checkPageEvictionMetric(CacheAtomicityMode.ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10738")
    @Test
    public void testPageEvictionMetricMvcc() throws Exception {
        checkPageEvictionMetric(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkPageEvictionMetric(CacheAtomicityMode atomicityMode) throws Exception {
        IgniteEx ignite = startGrid(0);

        DataRegionMetricsImpl metrics =
            ignite.context().cache().context().database().dataRegion(null).memoryMetrics();

        metrics.enableMetrics();

        CacheConfiguration<Object, Object> cfg = cacheConfig("evict-metric", null,
            CacheMode.PARTITIONED, atomicityMode, CacheWriteSynchronizationMode.PRIMARY_SYNC);

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(cfg);

        for (int i = 1; i <= ENTRIES; i++) {
            // Row size is between PAGE_SIZE / 2 and PAGE_SIZE. Enforces "one row - one page".
            cache.put(i, new TestObject(PAGE_SIZE / 6));

            if (i % (ENTRIES / 10) == 0)
                System.out.println(">>> Entries put: " + i);
        }

        assertTrue(metrics.getEvictionRate() > 0);
    }
}
