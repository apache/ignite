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

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.events.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;

import javax.cache.expiry.*;
import java.util.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.apache.ignite.events.IgniteEventType.*;

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

        cfg.setPreloadMode(SYNC);
        cfg.setStore(null);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testExpiredEntriesPreloading() throws Exception {
        GridCache<String, Integer> cache0 = cache(0);

        final int KEYS_NUM = 3;

        for (int i = 0; i < KEYS_NUM; i++)
            cache0.put(String.valueOf(i), 0);

        final ExpiryPolicy expiry = new TouchedExpiryPolicy(new Duration(MILLISECONDS, 100L));

        IgniteCache cache = grid(0).jcache(null).withExpiryPolicy(expiry);

        for (int i = 0; i < KEYS_NUM; i++)
            cache.put(String.valueOf(i), i);

        // Allow entries to expire.
        U.sleep(1000);

        // Ensure entries expiration.
        for (int i = 0; i < KEYS_NUM; i++)
            assert cache0.get(String.valueOf(i)) == null;

        // Start another node.
        Ignite g1 = startGrid(1);

        final GridCacheAdapter<String, Integer> cache1 = ((GridKernal)g1).context().cache().internalCache();

        cache1.preloader().syncFuture().get();

        Collection<IgniteEvent> evts = g1.events().localQuery(F.<IgniteEvent>alwaysTrue(), EVT_CACHE_PRELOAD_OBJECT_LOADED);

        assertEquals("Expected all entries are preloaded.", KEYS_NUM, evts.size());

        boolean rmv = GridTestUtils.waitForCondition(new PAX() {
            @Override public boolean applyx() {
                return cache1.isEmpty();
            }
        }, 10_000);

        assertTrue("Expired entries were not removed.", rmv);
    }
}
