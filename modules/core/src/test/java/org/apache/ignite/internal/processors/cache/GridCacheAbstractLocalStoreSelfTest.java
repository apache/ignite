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

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.processors.cache.store.CacheLocalStore;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public abstract class GridCacheAbstractLocalStoreSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    public static final TestLocalStore<Integer, Integer> LOCAL_STORE_1 = new TestLocalStore<>();

    /** */
    public static final TestLocalStore<Integer, Integer> LOCAL_STORE_2 = new TestLocalStore<>();

    /** */
    public static final TestLocalStore<Integer, Integer> LOCAL_STORE_3 = new TestLocalStore<>();

    /** */
    public static final int KEYS = 1000;

    /** */
    public static final String BACKUP_CACHE = "backup";

    /**
     *
     */
    public GridCacheAbstractLocalStoreSelfTest() {
        super(false /* doesn't start grid */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cacheCfg = cache(gridName, null, 0);

        CacheConfiguration cacheBackupCfg = cache(gridName, BACKUP_CACHE, 2);

        cfg.setCacheConfiguration(cacheCfg, cacheBackupCfg);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(spi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        LOCAL_STORE_1.clear();
        LOCAL_STORE_2.clear();
        LOCAL_STORE_3.clear();
    }

    /**
     * @param gridName Grid name.
     * @param cacheName Cache name.
     * @param backups Number of backups.
     * @return Configuration.
     */
    @SuppressWarnings("unchecked")
    private CacheConfiguration cache(String gridName, String cacheName, int backups) {
        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setName(cacheName);
        cacheCfg.setCacheMode(getCacheMode());
        cacheCfg.setAtomicityMode(getAtomicMode());
        cacheCfg.setNearConfiguration(nearConfiguration());
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setRebalanceMode(SYNC);

        if (gridName.endsWith("1"))
            cacheCfg.setCacheStoreFactory(singletonFactory(LOCAL_STORE_1));
        else if (gridName.endsWith("2"))
            cacheCfg.setCacheStoreFactory(singletonFactory(LOCAL_STORE_2));
        else
            cacheCfg.setCacheStoreFactory(singletonFactory(LOCAL_STORE_3));

        cacheCfg.setWriteThrough(true);
        cacheCfg.setReadThrough(true);
        cacheCfg.setBackups(backups);
        cacheCfg.setOffHeapMaxMemory(0);
        cacheCfg.setSwapEnabled(true);

        if (isOffHeapTieredMode())
            cacheCfg.setMemoryMode(OFFHEAP_TIERED);

        return cacheCfg;
    }

    /**
     * @return Distribution mode.
     */
    protected abstract NearCacheConfiguration nearConfiguration();

    /**
     * @return Cache atomicity mode.
     */
    protected abstract CacheAtomicityMode getAtomicMode();

    /**
     * @return Cache mode.
     */
    protected abstract CacheMode getCacheMode();

    /**
     * @return {@code True} if {@link CacheMemoryMode#OFFHEAP_TIERED} memory mode should be used.
     */
    protected boolean isOffHeapTieredMode() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testEvict() throws Exception {
        Ignite ignite1 = startGrid(1);

        IgniteCache<Object, Object> cache = ignite1.cache(null).withExpiryPolicy(new CreatedExpiryPolicy(
            new Duration(TimeUnit.MILLISECONDS, 100L)));

        // Putting entry.
        for (int i = 0; i < KEYS; i++)
            cache.put(i, i);

        // Wait when entry
        U.sleep(200);

        // Check that entry is evicted from cache, but local store does contain it.
        for (int i = 0; i < KEYS; i++) {
            cache.localEvict(Arrays.asList(i));

            assertNull(cache.localPeek(i));

            assertEquals(i, (int)LOCAL_STORE_1.load(i).get1());

            assertEquals(i, cache.get(i));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryNode() throws Exception {
        Ignite ignite1 = startGrid(1);

        IgniteCache<Object, Object> cache = ignite1.cache(null);

        // Populate cache and check that local store has all value.
        for (int i = 0; i < KEYS; i++)
            cache.put(i, i);

        checkLocalStore(ignite1, LOCAL_STORE_1);

        final AtomicInteger evtCnt = new AtomicInteger(0);

        if (getCacheMode() != REPLICATED) {
            ignite1.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    evtCnt.incrementAndGet();

                    return true;
                }
            }, EventType.EVT_CACHE_REBALANCE_PART_UNLOADED);
        }

        final Ignite ignite2 = startGrid(2);

        if (getCacheMode() != REPLICATED) {
            boolean wait = GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    // Partition count which must be transferred to 2'nd node.
                    int parts = ignite2.affinity(null).allPartitions(ignite2.cluster().localNode()).length;

                    return evtCnt.get() >= parts;
                }
            }, 5000);

            assertTrue(wait);
        }

        assertEquals(Ignition.allGrids().size(), 2);

        checkLocalStore(ignite1, LOCAL_STORE_1);
        checkLocalStore(ignite2, LOCAL_STORE_2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBackupNode() throws Exception {
        Ignite ignite1 = startGrid(1);

        IgniteCache<Object, Object> cache = ignite1.cache(BACKUP_CACHE);

        for (int i = 0; i < KEYS; i++)
            cache.put(i, i);

        for (int i = 0; i < KEYS; i++)
            assertEquals(LOCAL_STORE_1.load(i).get1().intValue(), i);

        // Start 2'nd node.
        Ignite ignite2 = startGrid(2);

        assertEquals(2, Ignition.allGrids().size());

        checkLocalStoreForBackup(ignite2, LOCAL_STORE_2);

        // Start 3'nd node.
        Ignite ignite3 = startGrid(3);

        assertEquals(Ignition.allGrids().size(), 3);

        for (int i = 0; i < KEYS; i++)
            cache.put(i, i * 3);

        checkLocalStoreForBackup(ignite2, LOCAL_STORE_2);
        checkLocalStoreForBackup(ignite3, LOCAL_STORE_3);

        // Stop 3'nd node.
        stopGrid(3, true);

        assertEquals(Ignition.allGrids().size(), 2);

        checkLocalStoreForBackup(ignite2, LOCAL_STORE_2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSwap() throws Exception {
        Ignite ignite1 = startGrid(1);

        IgniteCache<Object, Object> cache = ignite1.cache(null);

        // Populate cache and check that local store has all value.
        for (int i = 0; i < KEYS; i++)
            cache.put(i, i);

        checkLocalStore(ignite1, LOCAL_STORE_1);

        // Push entry to swap.
        for (int i = 0; i < KEYS; i++)
            cache.localEvict(Lists.newArrayList(i));

        for (int i = 0; i < KEYS; i++)
            assertNull(cache.localPeek(i, CachePeekMode.ONHEAP));

        final AtomicInteger evtCnt = new AtomicInteger(0);

        if (getCacheMode() != REPLICATED) {
            ignite1.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    evtCnt.incrementAndGet();

                    return true;
                }
            }, EventType.EVT_CACHE_REBALANCE_PART_UNLOADED);
        }

        final Ignite ignite2 = startGrid(2);

        if (getCacheMode() != REPLICATED) {
            boolean wait = GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    // Partition count which must be transferred to 2'nd node.
                    int parts = ignite2.affinity(null).allPartitions(ignite2.cluster().localNode()).length;

                    return evtCnt.get() >= parts;
                }
            }, 5000);

            assertTrue(wait);
        }

        assertEquals(Ignition.allGrids().size(), 2);

        checkLocalStore(ignite1, LOCAL_STORE_1);
        checkLocalStore(ignite2, LOCAL_STORE_2);
    }

    /**
     * Checks that local stores contains only primary entry.
     *
     * @param ignite Ignite.
     * @param store Store.
     */
    private void checkLocalStore(Ignite ignite, CacheStore<Integer, IgniteBiTuple<Integer, ?>> store) {
        for (int i = 0; i < KEYS; i++) {
            if (ignite.affinity(null).isPrimary(ignite.cluster().localNode(), i))
                assertEquals(store.load(i).get1().intValue(), i);
            else if (!ignite.affinity(null).isPrimaryOrBackup(ignite.cluster().localNode(), i))
                assertNull(store.load(i));
        }
    }

    /**
     * Checks that local stores contains only primary entry.
     *
     * @param ignite Ignite.
     * @param store Store.
     */
    private void checkLocalStoreForBackup(Ignite ignite, CacheStore<Integer, IgniteBiTuple<Integer, ?>> store) {
        for (int i = 0; i < KEYS; i++) {
            if (ignite.affinity(BACKUP_CACHE).isBackup(ignite.cluster().localNode(), i))
                assertEquals(store.load(i).get1().intValue(), i);
            else if (!ignite.affinity(BACKUP_CACHE).isPrimaryOrBackup(ignite.cluster().localNode(), i))
                assertNull(store.load(i).get1());
        }
    }

    /**
     *
     */
    @CacheLocalStore
    public static class TestLocalStore<K, V> implements CacheStore<K, IgniteBiTuple<V, ?>> {
        /** */
        private Map<K, IgniteBiTuple<V, ?>> map = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<K, IgniteBiTuple<V, ?>> clo, @Nullable Object... args)
            throws CacheLoaderException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void sessionEnd(boolean commit) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public IgniteBiTuple<V, ?> load(K key) throws CacheLoaderException {
            return map.get(key);
        }

        /** {@inheritDoc} */
        @Override public Map<K, IgniteBiTuple<V, ?>> loadAll(Iterable<? extends K> keys) throws CacheLoaderException {
            Map<K, IgniteBiTuple<V, ?>> res = new HashMap<>();

            for (K key : keys) {
                IgniteBiTuple<V, ?> val = map.get(key);

                if (val != null)
                    res.put(key, val);
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends K, ? extends IgniteBiTuple<V, ?>> entry)
            throws CacheWriterException {
            map.put(entry.getKey(), entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<? extends K, ? extends IgniteBiTuple<V, ?>>> entries)
            throws CacheWriterException {
            for (Cache.Entry<? extends K, ? extends IgniteBiTuple<V, ?>> e : entries)
                map.put(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            map.remove(key);
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> keys) throws CacheWriterException {
            for (Object key : keys)
                map.remove(key);
        }

        /**
         * Clear store.
         */
        public void clear(){
            map.clear();
        }
    }
}