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
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import java.util.concurrent.CountDownLatch;

import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;

/**
 * Test for cluster wide offheap cache metrics.
 */
public class OffheapCacheMetricsForClusterGroupSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** Client count */
    private static final int CLIENT_CNT = 3;

    /** Grid client mode */
    private boolean clientMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClientMode(clientMode);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // start grids
        clientMode = false;
        for (int i = 0; i < GRID_CNT; i++)
            startGrid("server-" + i);

        // start clients
        clientMode = true;
        for (int i = 0; i < CLIENT_CNT; i++)
            startGrid("client-" + i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    public void testGetOffHeapPrimaryEntriesCount() throws Exception {
        String cacheName = "testGetOffHeapPrimaryEntriesCount";
        IgniteCache<Integer, Integer> cache = grid("client-0").createCache(cacheConfiguration(cacheName));

        for (int i = 0; i < 1000; i++)
            cache.put(i, i);

        awaitMetricsUpdate();

        assertGetOffHeapPrimaryEntriesCount(cacheName, 1000);

        for (int j = 0; j < 1000; j++)
            cache.get(j);

        awaitMetricsUpdate();

        assertGetOffHeapPrimaryEntriesCount(cacheName, 1000);

        cache = grid("client-1").cache(cacheName);

        for (int j = 0; j < 1000; j++)
            cache.get(j);

        awaitMetricsUpdate();

        assertGetOffHeapPrimaryEntriesCount(cacheName, 1000);
    }

    /**
     * Wait for {@link EventType#EVT_NODE_METRICS_UPDATED} event will be receieved.
     */
    private void awaitMetricsUpdate() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch((GRID_CNT + 1) * 2);

        IgnitePredicate<Event> lsnr = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event ignore) {
                latch.countDown();

                return true;
            }
        };

        for (int i = 0; i < GRID_CNT; i++)
            grid("server-" + i).events().localListen(lsnr, EVT_NODE_METRICS_UPDATED);

        latch.await();
    }

    private void assertGetOffHeapPrimaryEntriesCount(String cacheName, int count) throws Exception {
        for (int i = 0; i < GRID_CNT; i++) {
            IgniteCache<Integer, Integer> cache = grid("server-" + i).cache(cacheName);
            assertEquals(count, cache.metrics().getOffHeapPrimaryEntriesCount());
        }

        for (int i = 0; i < CLIENT_CNT; i++) {
            IgniteCache<Integer, Integer> cache = grid("client-" + i).cache(cacheName);
            assertEquals(count, cache.metrics().getOffHeapPrimaryEntriesCount());
        }
    }

    private static CacheConfiguration<Integer, Integer> cacheConfiguration(String cacheName) {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>(cacheName);

        cfg.setBackups(1);
        cfg.setStatisticsEnabled(true);
        cfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);
        cfg.setOffHeapMaxMemory(1024 * 1024 * 1024);
        return cfg;
    }
}