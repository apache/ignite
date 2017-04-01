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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Collection;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.TouchedExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.PAX;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED;

/**
 * Tests preloading of expired entries.
 */
public abstract class GridCacheExpiredEntriesPreloadAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int GRID_CNT = 2;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setRebalanceMode(SYNC);
        cfg.setCacheStoreFactory(null);
        cfg.setWriteThrough(false);
        cfg.setReadThrough(false);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testExpiredEntriesPreloading() throws Exception {
        IgniteCache<String, Integer> cache0 = jcache(0);

        final int KEYS_NUM = 3;

        for (int i = 0; i < KEYS_NUM; i++)
            cache0.put(String.valueOf(i), 0);

        final ExpiryPolicy expiry = new TouchedExpiryPolicy(new Duration(MILLISECONDS, 100L));

        IgniteCache cache = grid(0).cache(null).withExpiryPolicy(expiry);

        for (int i = 0; i < KEYS_NUM; i++)
            cache.put(String.valueOf(i), i);

        // Allow entries to expire.
        U.sleep(1000);

        // Ensure entries expiration.
        for (int i = 0; i < KEYS_NUM; i++)
            assert cache0.get(String.valueOf(i)) == null;

        // Start another node.
        Ignite g1 = startGrid(1);

        final GridCacheAdapter<String, Integer> cache1 = ((IgniteKernal)g1).context().cache().internalCache();

        cache1.preloader().syncFuture().get();

        Collection<Event> evts = g1.events().localQuery(F.<Event>alwaysTrue(), EVT_CACHE_REBALANCE_OBJECT_LOADED);

        assertEquals("Expected all entries are preloaded.", KEYS_NUM, evts.size());

        boolean rmv = GridTestUtils.waitForCondition(new PAX() {
            @Override public boolean applyx() {
                return cache1.isEmpty();
            }
        }, 10_000);

        assertTrue("Expired entries were not removed.", rmv);
    }
}