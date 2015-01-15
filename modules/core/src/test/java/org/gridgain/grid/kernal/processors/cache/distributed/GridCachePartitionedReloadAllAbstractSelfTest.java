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
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicWriteOrderMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Check reloadAll() on partitioned cache.
 */
public abstract class GridCachePartitionedReloadAllAbstractSelfTest extends GridCommonAbstractTest {
    /** Amount of nodes in the grid. */
    private static final int GRID_CNT = 4;

    /** Amount of backups in partitioned cache. */
    private static final int BACKUP_CNT = 1;

    /** IP finder. */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Map where dummy cache store values are stored. */
    private final Map<Integer, String> map = new ConcurrentHashMap8<>();

    /** Collection of caches, one per grid node. */
    private List<GridCache<Integer, String>> caches;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setDistributionMode(nearEnabled() ? NEAR_PARTITIONED : PARTITIONED_ONLY);

        cc.setCacheMode(cacheMode());

        cc.setAtomicityMode(atomicityMode());

        cc.setBackups(BACKUP_CNT);

        cc.setWriteSynchronizationMode(FULL_SYNC);

        cc.setStore(cacheStore());

        cc.setAtomicWriteOrderMode(atomicWriteOrderMode());

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @return Cache mode.
     */
    protected GridCacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @return Atomicity mode.
     */
    protected GridCacheAtomicityMode atomicityMode() {
        return GridCacheAtomicityMode.TRANSACTIONAL;
    }

    /**
     * @return Write order mode for atomic cache.
     */
    protected GridCacheAtomicWriteOrderMode atomicWriteOrderMode() {
        return CLOCK;
    }

    /**
     * @return {@code True} if near cache is enabled.
     */
    protected abstract boolean nearEnabled();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        caches = new ArrayList<>(GRID_CNT);

        for (int i = 0; i < GRID_CNT; i++)
            caches.add(startGrid(i).<Integer, String>cache(null));

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        map.clear();

        caches = null;
    }

    /**
     * Create new cache store.
     *
     * @return Write through storage emulator.
     */
    protected CacheStore<?, ?> cacheStore() {
        return new CacheStoreAdapter<Integer, String>() {
            @IgniteInstanceResource
            private Ignite g;

            @Override public void loadCache(IgniteBiInClosure<Integer, String> c,
                Object... args) {
                X.println("Loading all on: " + caches.indexOf(g.<Integer, String>cache(null)));

                for (Map.Entry<Integer, String> e : map.entrySet())
                    c.apply(e.getKey(), e.getValue());
            }

            @Override public String load(Integer key) {
                X.println("Loading on: " + caches.indexOf(g.<Integer, String>cache(null)) + " key=" + key);

                return map.get(key);
            }

            @Override public void put(Integer key, @Nullable String val) {
                fail("Should not be called within the test.");
            }

            @Override public void remove(Integer key) {
                fail("Should not be called within the test.");
            }
        };
    }

    /**
     * Ensure that reloadAll() with disabled near cache reloads data only on a node
     * on which reloadAll() has been called.
     *
     * @throws Exception If test failed.
     */
    public void testReloadAll() throws Exception {
        // Fill caches with values.
        for (GridCache<Integer, String> cache : caches) {
            Iterable<Integer> keys = primaryKeysForCache(cache, 100);

            info("Values [cache=" + caches.indexOf(cache) + ", size=" + F.size(keys.iterator()) +  ", keys=" + keys + "]");

            for (Integer key : keys)
                map.put(key, "val" + key);
        }

        Collection<GridCache<Integer, String>> emptyCaches = new ArrayList<>(caches);

        for (GridCache<Integer, String> cache : caches) {
            info("Reloading cache: " + caches.indexOf(cache));

            // Check data is reloaded only on the nodes on which reloadAll() has been called.
            if (!nearEnabled()) {
                for (GridCache<Integer, String> eCache : emptyCaches)
                    assertEquals("Non-null values found in cache [cache=" + caches.indexOf(eCache) +
                        ", size=" + eCache.size() + ", size=" + eCache.size() +
                        ", entrySetSize=" + eCache.entrySet().size() + "]",
                        0, eCache.size());
            }

            cache.reloadAll(map.keySet());

            for (Integer key : map.keySet()) {
                GridCacheEntry entry = cache.entry(key);

                if (entry.primary() || entry.backup() || nearEnabled())
                    assertEquals(map.get(key), cache.peek(key));
                else
                    assertNull(cache.peek(key));
            }

            emptyCaches.remove(cache);
        }
    }

    /**
     * Create list of keys for which the given cache is primary.
     *
     * @param cache Cache.
     * @param cnt Keys count.
     * @return Collection of keys for which given cache is primary.
     */
    private Iterable<Integer> primaryKeysForCache(GridCacheProjection<Integer,String> cache, int cnt) {
        Collection<Integer> found = new ArrayList<>(cnt);

        for (int i = 0; i < 10000; i++) {
            if (cache.entry(i).primary()) {
                found.add(i);

                if (found.size() == cnt)
                    return found;
            }
        }

        throw new IllegalStateException("Unable to find " + cnt + " keys as primary for cache.");
    }
}
