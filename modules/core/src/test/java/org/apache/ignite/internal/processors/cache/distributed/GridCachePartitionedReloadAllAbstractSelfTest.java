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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.cache.integration.CompletionListenerFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.CLOCK;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

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
    private List<IgniteCache<Integer, String>> caches;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        CacheConfiguration cc = defaultCacheConfiguration();

        if (!nearEnabled())
            cc.setNearConfiguration(null);

        cc.setCacheMode(cacheMode());

        cc.setAtomicityMode(atomicityMode());

        cc.setBackups(BACKUP_CNT);

        cc.setWriteSynchronizationMode(FULL_SYNC);

        CacheStore store = cacheStore();

        if (store != null) {
            cc.setCacheStoreFactory(singletonFactory(store));
            cc.setReadThrough(true);
            cc.setWriteThrough(true);
            cc.setLoadPreviousValue(true);
        }
        else
            cc.setCacheStoreFactory(null);

        cc.setAtomicWriteOrderMode(atomicWriteOrderMode());

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @return Cache mode.
     */
    protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @return Atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.TRANSACTIONAL;
    }

    /**
     * @return Write order mode for atomic cache.
     */
    protected CacheAtomicWriteOrderMode atomicWriteOrderMode() {
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
                X.println("Loading all on: " + caches.indexOf(((IgniteKernal)g).<Integer, String>getCache(null)));

                for (Map.Entry<Integer, String> e : map.entrySet())
                    c.apply(e.getKey(), e.getValue());
            }

            @Override public String load(Integer key) {
                X.println("Loading on: " + caches.indexOf(((IgniteKernal)g)
                    .<Integer, String>getCache(null)) + " key=" + key);

                return map.get(key);
            }

            @Override public void write(javax.cache.Cache.Entry<? extends Integer, ? extends String> e) {
                fail("Should not be called within the test.");
            }

            @Override public void delete(Object key) {
                fail("Should not be called within the test.");
            }
        };
    }

    /**
     * @throws Exception If test failed.
     */
    public void testReloadAll() throws Exception {
        // Fill caches with values.
        for (IgniteCache<Integer, String> cache : caches) {
            Iterable<Integer> keys = primaryKeys(cache, 100);

            info("Values [cache=" + caches.indexOf(cache) + ", size=" + F.size(keys.iterator()) +  ", keys=" + keys + "]");

            for (Integer key : keys)
                map.put(key, "val" + key);
        }

        CompletionListenerFuture fut = new CompletionListenerFuture();

        caches.get(0).loadAll(map.keySet(), false, fut);

        fut.get();

        Affinity aff = ignite(0).affinity(null);

        for (IgniteCache<Integer, String> cache : caches) {
            for (Integer key : map.keySet()) {
                if (aff.isPrimaryOrBackup(grid(caches.indexOf(cache)).localNode(), key))
                    assertEquals(map.get(key), cache.localPeek(key));
                else
                    assertNull(cache.localPeek(key));
            }
        }
    }
}