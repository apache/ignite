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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test for cluster wide offheap cache metrics.
 */
public class OffheapCacheMetricsForClusterGroupSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** Client count */
    private static final int CLIENT_CNT = 3;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // start grids
        for (int i = 0; i < GRID_CNT; i++)
            startGrid("server-" + i);

        // start clients
        for (int i = 0; i < CLIENT_CNT; i++)
            startClientGrid("client-" + i);
    }

    @Test
    public void testGetOffHeapPrimaryEntriesCount() throws Exception {
        String cacheName = "testGetOffHeapPrimaryEntriesCount";
        IgniteCache<Integer, Integer> cache = grid("client-0").createCache(cacheConfiguration(cacheName));

        for (int i = 0; i < 1000; i++)
            cache.put(i, i);

        awaitMetricsUpdate(1);

        assertGetOffHeapPrimaryEntriesCount(cacheName, 1000);

        for (int j = 0; j < 1000; j++)
            cache.get(j);

        awaitMetricsUpdate(1);

        assertGetOffHeapPrimaryEntriesCount(cacheName, 1000);

        cache = grid("client-1").cache(cacheName);

        for (int j = 0; j < 1000; j++)
            cache.get(j);

        awaitMetricsUpdate(1);

        assertGetOffHeapPrimaryEntriesCount(cacheName, 1000);
    }

    private void assertGetOffHeapPrimaryEntriesCount(String cacheName, int count) throws Exception {
        long localPrimary = 0L;
        long localBackups = 0L;

        for (int i = 0; i < GRID_CNT; i++) {
            IgniteCache<Integer, Integer> cache = grid("server-" + i).cache(cacheName);
            assertEquals(count, cache.metrics().getOffHeapPrimaryEntriesCount());
            assertEquals(count, cache.mxBean().getOffHeapPrimaryEntriesCount());
            assertEquals(count, cache.metrics().getOffHeapBackupEntriesCount());
            assertEquals(count, cache.mxBean().getOffHeapBackupEntriesCount());

            localPrimary += cache.localMxBean().getOffHeapPrimaryEntriesCount();
            localBackups += cache.localMxBean().getOffHeapPrimaryEntriesCount();
        }

        assertEquals(count, localPrimary);
        assertEquals(count, localBackups);

        for (int i = 0; i < CLIENT_CNT; i++) {
            IgniteCache<Integer, Integer> cache = grid("client-" + i).cache(cacheName);
            assertEquals(count, cache.metrics().getOffHeapPrimaryEntriesCount());
            assertEquals(count, cache.mxBean().getOffHeapPrimaryEntriesCount());
            assertEquals(count, cache.metrics().getOffHeapBackupEntriesCount());
            assertEquals(count, cache.mxBean().getOffHeapBackupEntriesCount());
            assertEquals(0L, cache.localMxBean().getOffHeapPrimaryEntriesCount());
            assertEquals(0L, cache.localMxBean().getOffHeapBackupEntriesCount());
        }
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    private static CacheConfiguration<Integer, Integer> cacheConfiguration(String cacheName) {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>(cacheName);

        cfg.setBackups(1);
        cfg.setStatisticsEnabled(true);
        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        return cfg;
    }
}
