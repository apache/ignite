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
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.colocated.GridDhtColocatedCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheEntry;
import org.apache.ignite.internal.processors.cache.local.GridLocalCacheEntry;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Cache map entry self test.
 */
public class CacheOffheapMapEntrySelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        // No-op.
    }

    /**
     * @param gridName Grid name.
     * @param atomicityMode Atomicity mode.
     * @param cacheMode Cache mode.
     * @param cacheName Cache name.
     * @return Cache configuration.
     * @throws Exception If failed.
     */
    private CacheConfiguration cacheConfiguration(String gridName,
        CacheAtomicityMode atomicityMode,
        CacheMode cacheMode,
        String cacheName)
        throws Exception
    {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setCacheMode(cacheMode);
        cfg.setAtomicityMode(atomicityMode);
        cfg.setName(cacheName);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheMapEntry() throws Exception {
        checkCacheMapEntry(ATOMIC, LOCAL, GridLocalCacheEntry.class);

        checkCacheMapEntry(TRANSACTIONAL, LOCAL, GridLocalCacheEntry.class);

        checkCacheMapEntry(ATOMIC, PARTITIONED, GridNearCacheEntry.class);

        checkCacheMapEntry(TRANSACTIONAL, PARTITIONED, GridNearCacheEntry.class);

        checkCacheMapEntry(ATOMIC, REPLICATED, GridDhtAtomicCacheEntry.class);

        checkCacheMapEntry(TRANSACTIONAL, REPLICATED, GridDhtColocatedCacheEntry.class);
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @param cacheMode Cache mode.
     * @param entryCls Class of cache map entry.
     * @throws Exception If failed.
     */
    private void checkCacheMapEntry(CacheAtomicityMode atomicityMode,
        CacheMode cacheMode,
        Class<?> entryCls)
        throws Exception
    {
        log.info("Test cache [atomicityMode=" + atomicityMode + ", cacheMode=" + cacheMode + ']');

        CacheConfiguration cfg = cacheConfiguration(grid(0).name(),
            atomicityMode,
            cacheMode,
            "Cache");

        IgniteCache jcache = grid(0).getOrCreateCache(cfg);

        try {
            GridCacheAdapter<Integer, String> cache = ((IgniteKernal)grid(0)).internalCache(jcache.getName());

            Integer key = primaryKey(grid(0).cache(DEFAULT_CACHE_NAME));

            cache.put(key, "val");

            GridCacheEntryEx entry = cache.entryEx(key);

            entry.unswap(true);

            assertNotNull(entry);

            assertEquals(entry.getClass(), entryCls);
        }
        finally {
            jcache.destroy();
        }
    }
}