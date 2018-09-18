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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.store.CacheLocalStore;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LOCAL_STORE_KEEPS_PRIMARY_ONLY;
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
    public static final TestLocalStore<Integer, Integer> LOCAL_STORE_4 = new TestLocalStore<>();

    /** */
    public static final TestLocalStore<Integer, Integer> LOCAL_STORE_5 = new TestLocalStore<>();

    /** */
    public static final TestLocalStore<Integer, Integer> LOCAL_STORE_6 = new TestLocalStore<>();

    /** */
    public static final int KEYS = 1025;

    /** */
    public static final String BACKUP_CACHE_1 = "backup_1";

    /** */
    public static final String BACKUP_CACHE_2 = "backup_2";

    /** */
    public static volatile boolean near = false;

    /**
     *
     */
    public GridCacheAbstractLocalStoreSelfTest() {
        super(false /* doesn't start grid */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cacheCfg = cache(igniteInstanceName, DEFAULT_CACHE_NAME, 0);

        cacheCfg.setAffinity(new RendezvousAffinityFunction());

        CacheConfiguration cacheBackup1Cfg = cache(igniteInstanceName, BACKUP_CACHE_1, 1);

        cacheBackup1Cfg.setAffinity(new RendezvousAffinityFunction());

        CacheConfiguration cacheBackup2Cfg = cache(igniteInstanceName, BACKUP_CACHE_2, 2);

        cacheBackup2Cfg.setAffinity(new RendezvousAffinityFunction());

        cfg.setCacheConfiguration(cacheCfg, cacheBackup1Cfg, cacheBackup2Cfg);

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
        LOCAL_STORE_4.clear();
        LOCAL_STORE_5.clear();
        LOCAL_STORE_6.clear();
    }

    /**
     * @param igniteInstanceName Ignite instance name.
     * @param cacheName Cache name.
     * @param backups Number of backups.
     * @return Configuration.
     */
    @SuppressWarnings("unchecked")
    private CacheConfiguration cache(String igniteInstanceName, String cacheName, int backups) {
        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setName(cacheName);
        cacheCfg.setCacheMode(getCacheMode());
        cacheCfg.setAtomicityMode(getAtomicMode());
        cacheCfg.setNearConfiguration(nearConfiguration());
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);

        cacheCfg.setRebalanceMode(SYNC);

        cacheCfg.setCacheStoreFactory(new StoreFactory());

        cacheCfg.setWriteThrough(true);
        cacheCfg.setReadThrough(true);
        cacheCfg.setBackups(backups);

        return cacheCfg;
    }

    /**
     * @return Distribution mode.
     */
    protected NearCacheConfiguration nearConfiguration() {
        return near ? new NearCacheConfiguration() : null;
    }

    /**
     * @return Cache atomicity mode.
     */
    protected abstract CacheAtomicityMode getAtomicMode();

    /**
     * @return Cache mode.
     */
    protected abstract CacheMode getCacheMode();

    /**
     * @throws Exception If failed.
     */
    public void testEvict() throws Exception {
        Ignite ignite1 = startGrid(1);

        IgniteCache<Object, Object> cache = ignite1.cache(DEFAULT_CACHE_NAME).withExpiryPolicy(new CreatedExpiryPolicy(
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

        IgniteCache<Object, Object> cache = ignite1.cache(DEFAULT_CACHE_NAME);

        // Populate cache and check that local store has all value.
        for (int i = 0; i < KEYS; i++)
            cache.put(i, i);

        checkLocalStore(ignite1, LOCAL_STORE_1, DEFAULT_CACHE_NAME);

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
                    int parts = ignite2.affinity(DEFAULT_CACHE_NAME).allPartitions(ignite2.cluster().localNode()).length;

                    return evtCnt.get() >= parts;
                }
            }, 5000);

            assertTrue(wait);
        }

        assertEquals(Ignition.allGrids().size(), 2);

        checkLocalStore(ignite1, LOCAL_STORE_1, DEFAULT_CACHE_NAME);
        checkLocalStore(ignite2, LOCAL_STORE_2, DEFAULT_CACHE_NAME);
    }


    /**
     * @throws Exception If failed.
     */
    public void testBackupRestorePrimary() throws Exception {
        testBackupRestore();
    }

    /**
     * @throws Exception If failed.
     */
    public void testBackupRestore() throws Exception {
        final IgniteEx ignite1 = startGrid(1);
        Ignite ignite2 = startGrid(2);

        awaitPartitionMapExchange();

        // We need a backup key on grid 1, so we must wait for late affinity assignment to change.
        AffinityTopologyVersion waitTopVer = new AffinityTopologyVersion(2, 1);

        grid(1).context().cache().context().exchange().affinityReadyFuture(waitTopVer).get();
        grid(2).context().cache().context().exchange().affinityReadyFuture(waitTopVer).get();

        final String name = BACKUP_CACHE_2;

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                AffinityTopologyVersion topVer = ignite1.context().cache().context().cacheContext(CU.cacheId(name))
                    .affinity().affinityTopologyVersion();

                return topVer.topologyVersion() == 2 && topVer.minorTopologyVersion() == 1;

//                return ignite1.affinity(name).primaryPartitions(ignite1.cluster().localNode()).length <
//                    ignite1.affinity(name).partitions();
            }
        }, 10_000));

        int key1 = -1;
        int key2 = -1;

        for (int i = 0; i < KEYS; i++) {
            if (ignite1.affinity(name).isPrimary(ignite1.cluster().localNode(), i)) {
                key1 = i;

                break;
            }
        }

        for (int i = 0; i < KEYS; i++) {
            if (!ignite1.affinity(name).isPrimary(ignite1.cluster().localNode(), i)) {
                key2 = i;

                break;
            }
        }

        assertTrue(key1 >= 0);
        assertTrue(key2 >= 0);
        assertNotSame(key1, key2);

        assertEquals(0, ignite1.cache(name).size());

        assertEquals(0, LOCAL_STORE_1.map.size());
        assertEquals(0, LOCAL_STORE_2.map.size());

        IgniteCache<Integer, Integer> cache = ignite1.cache(name).withAllowAtomicOpsInTx();

        try (Transaction tx = ignite1.transactions().txStart()) {
            cache.put(key1, key1);
            cache.put(key2, key2);

            Map<Integer, Integer> m = new HashMap<>();

            for (int i = KEYS; i < KEYS + 100; i++)
                m.put(i, i);

            cache.putAll(m);

            tx.commit();
        }

        assertEquals(102, LOCAL_STORE_1.map.size());
        assertEquals(102, LOCAL_STORE_2.map.size());

        stopGrid(1);

        assertEquals(1, G.allGrids().size());

        assertEquals(key1, ignite2.cache(name).get(key1));
        assertEquals(key2, ignite2.cache(name).get(key2));

        for (int i = KEYS; i < KEYS + 100; i++)
            assertEquals(i, ignite2.cache(name).get(i));

        assertEquals(102, LOCAL_STORE_1.map.size());
        assertEquals(102, LOCAL_STORE_2.map.size());

        assert GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return ignite(2).cache(name).size() == 102;
            }
        }, 5000);

        stopGrid(2);

        assertEquals(0, G.allGrids().size());

        assertEquals(102, LOCAL_STORE_1.map.size());
        assertEquals(102, LOCAL_STORE_2.map.size());

        // Node restart. One node should have everything at local store.
        ignite2 = startGrid(2);

        assertEquals(1, G.allGrids().size());

        assertEquals(key1, ignite2.cache(name).get(key1));
        assertEquals(key2, ignite2.cache(name).get(key2));

        for (int i = KEYS; i < KEYS + 100; i++)
            assertEquals(i, ignite2.cache(name).get(i));

        assertEquals(102, ignite2.cache(name).size());

        assertEquals(102, LOCAL_STORE_1.map.size());
        assertEquals(102, LOCAL_STORE_2.map.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalStoreCorrespondsAffinityWithBackups() throws Exception {
        testLocalStoreCorrespondsAffinity(BACKUP_CACHE_2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalStoreCorrespondsAffinityWithBackup() throws Exception {
        testLocalStoreCorrespondsAffinity(BACKUP_CACHE_1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalStoreCorrespondsAffinityNoBackups() throws Exception {
        testLocalStoreCorrespondsAffinity(DEFAULT_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    private void testLocalStoreCorrespondsAffinity(String name) throws Exception {
        near = true;

        try {

            for (int i = 1; i <= 6; i++)
                startGrid(i);

            assertTrue(((IgniteCacheProxy)grid(1).cache(name)).context().isNear() ||
                getCacheMode()  == REPLICATED);

            awaitPartitionMapExchange();

            Random rn = new Random();

            for (int i = 1; i <= 6; i++)
                assertEquals(0, grid(i).cache(name).localSize(CachePeekMode.NEAR));

            for (int i = 0; i < KEYS; i++) {
                Ignite ignite = grid(rn.nextInt(6) + 1);

                IgniteCache<Integer, Integer> cache = ignite.cache(name).withAllowAtomicOpsInTx();

                try (Transaction tx = ignite.transactions().txStart()) {
                    cache.put(i, i);

                    for (int j = 0; j < 5; j++)
                        cache.get(rn.nextInt(KEYS));

                    Map<Integer, Integer> m = new HashMap<>(5);

                    for (int j = 0; j < 5; j++) {
                        Integer key = rn.nextInt(KEYS);

                        m.put(key, key);
                    }

                    cache.putAll(m);

                    tx.commit();
                }
            }

            for (int i = 1; i <= 6; i++) {
                assertTrue(grid(i).cache(name).localSize(CachePeekMode.NEAR) > 0 ||
                    getCacheMode()  == REPLICATED);
            }

            checkLocalStore(grid(1), LOCAL_STORE_1, name);
            checkLocalStore(grid(2), LOCAL_STORE_2, name);
            checkLocalStore(grid(3), LOCAL_STORE_3, name);
            checkLocalStore(grid(4), LOCAL_STORE_4, name);
            checkLocalStore(grid(5), LOCAL_STORE_5, name);
            checkLocalStore(grid(6), LOCAL_STORE_6, name);

            int fullStoreSize = LOCAL_STORE_1.map.size() +
                LOCAL_STORE_2.map.size() +
                LOCAL_STORE_3.map.size() +
                LOCAL_STORE_4.map.size() +
                LOCAL_STORE_5.map.size() +
                LOCAL_STORE_6.map.size();

            CacheConfiguration ccfg = grid(1).cache(name).getConfiguration(CacheConfiguration.class);

            assertEquals(
                getCacheMode()  == REPLICATED ?
                    KEYS * 6 :
                    ccfg.getBackups() * KEYS + KEYS,
                fullStoreSize);

        }
        finally {
            near = false;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalStoreWithNearKeysPrimary() throws Exception {
        try {
            System.setProperty(IGNITE_LOCAL_STORE_KEEPS_PRIMARY_ONLY, "true");

            testLocalStoreWithNearKeys();
        }
        finally {
            System.setProperty(IGNITE_LOCAL_STORE_KEEPS_PRIMARY_ONLY, "false");
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalStoreWithNearKeysPrimaryAndBackups() throws Exception {
        testLocalStoreWithNearKeys();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalStoreWithNearKeys() throws Exception {
        if (getCacheMode() == REPLICATED)
            return;

        near = true;

        try {
            for (int i = 1; i <= 3; i++)
                startGrid(i);

            awaitPartitionMapExchange();

            Ignite ignite = grid(1);

            int k = 0;

            for (int i = 1; i <= 3; i++) {
                int kP = -1;
                int kB = -1;
                int kN = -1;

                while (kP == -1 || kB == -1 || kN == -1) {
                    if (ignite.affinity(BACKUP_CACHE_1).isPrimary(grid(1).cluster().localNode(), k))
                        kP = k;

                    if (ignite.affinity(BACKUP_CACHE_1).isBackup(grid(1).cluster().localNode(), k) &&
                        ignite.affinity(BACKUP_CACHE_1).isPrimary(grid(2).cluster().localNode(), k))
                        kB = k;

                    if (!ignite.affinity(BACKUP_CACHE_1).isPrimaryOrBackup(grid(1).cluster().localNode(), k) &&
                        ignite.affinity(BACKUP_CACHE_1).isPrimary(grid(3).cluster().localNode(), k))
                        kN = k;

                    k++;
                }

                assertTrue(kP != kB && kB != kN && kN != kP);

                Map<Integer, Integer> m = new HashMap<>(3);

                m.put(kP, kP);
                m.put(kB, kB);
                m.put(kN, kN);

                IgniteCache<Integer, Integer> cache = grid(i).cache(BACKUP_CACHE_1).withAllowAtomicOpsInTx();

                try (Transaction tx = grid(i).transactions().txStart()) {
                    cache.putAll(m);

                    tx.commit();
                }

                boolean locStoreBackups = !IgniteSystemProperties.getBoolean(IGNITE_LOCAL_STORE_KEEPS_PRIMARY_ONLY);

                checkLocalStore(grid(1), LOCAL_STORE_1, BACKUP_CACHE_1, m.keySet(), locStoreBackups);
                checkLocalStore(grid(2), LOCAL_STORE_2, BACKUP_CACHE_1, m.keySet(), locStoreBackups);
                checkLocalStore(grid(3), LOCAL_STORE_3, BACKUP_CACHE_1, m.keySet(), locStoreBackups);
            }

            grid(1).cache(BACKUP_CACHE_1).removeAll();

            Random rn = new Random();

            for (int i = 1; i <= 3; i++) {
                IgniteCache<Integer, Integer> cache = grid(i).cache(BACKUP_CACHE_1)
                    .withSkipStore().withAllowAtomicOpsInTx();

                try (Transaction tx = grid(i).transactions().txStart()) {
                    Map<Integer, Integer> m = new HashMap<>(3);

                    for (int j = 0; j < 50; j++) {
                        m.put(rn.nextInt(1000), 1000);
                    }

                    cache.putAll(m);

                    tx.commit();
                }

                assertTrue(LOCAL_STORE_1.map.isEmpty());
                assertTrue(LOCAL_STORE_2.map.isEmpty());
                assertTrue(LOCAL_STORE_3.map.isEmpty());
            }
        }
        finally {
            near = false;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testBackupNode() throws Exception {
        Ignite ignite1 = startGrid(1);

        IgniteCache<Object, Object> cache = ignite1.cache(BACKUP_CACHE_2);

        for (int i = 0; i < KEYS; i++)
            cache.put(i, i);

        for (int i = 0; i < KEYS; i++) {
            assertEquals(LOCAL_STORE_1.load(i).get1().intValue(), i);
            assertNull(LOCAL_STORE_2.load(i));
            assertNull(LOCAL_STORE_3.load(i));
        }

        startGrid(2);

        assertEquals(2, Ignition.allGrids().size());

        for (int i = 0; i < KEYS; i++) {
            assertEquals(LOCAL_STORE_1.load(i).get1().intValue(), i);
            assertEquals(LOCAL_STORE_2.load(i).get1().intValue(), i);
            assertNull(LOCAL_STORE_3.load(i));
        }

        startGrid(3);

        assertEquals(Ignition.allGrids().size(), 3);

        for (int i = 0; i < KEYS; i++)
            cache.put(i, i * 3);

        for (int i = 0; i < KEYS; i++) {
            assertEquals(LOCAL_STORE_1.load(i).get1().intValue(), i * 3);
            assertEquals(LOCAL_STORE_2.load(i).get1().intValue(), i * 3);
            assertEquals(LOCAL_STORE_3.load(i).get1().intValue(), i * 3);
        }

        // Stop 3'nd node.
        stopGrid(3, true);

        for (int i = 0; i < KEYS; i++)
            cache.put(i, i * 7);

        assertEquals(Ignition.allGrids().size(), 2);

        for (int i = 0; i < KEYS; i++) {
            assertEquals(LOCAL_STORE_1.load(i).get1().intValue(), i * 7);
            assertEquals(LOCAL_STORE_2.load(i).get1().intValue(), i * 7);
            assertEquals(LOCAL_STORE_3.load(i).get1().intValue(), i * 3);
        }

        startGrid(3);

        assertEquals(Ignition.allGrids().size(), 3);

        for (int i = 0; i < KEYS; i++) {
            assertEquals(LOCAL_STORE_1.load(i).get1().intValue(), i * 7);
            assertEquals(LOCAL_STORE_2.load(i).get1().intValue(), i * 7);
            assertEquals(LOCAL_STORE_3.load(i).get1().intValue(), i * 7);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSwap() throws Exception {
        Ignite ignite1 = startGrid(1);

        IgniteCache<Object, Object> cache = ignite1.cache(DEFAULT_CACHE_NAME);

        // Populate cache and check that local store has all value.
        for (int i = 0; i < KEYS; i++)
            cache.put(i, i);

        checkLocalStore(ignite1, LOCAL_STORE_1, DEFAULT_CACHE_NAME);

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
                    int parts = ignite2.affinity(DEFAULT_CACHE_NAME).allPartitions(ignite2.cluster().localNode()).length;

                    return evtCnt.get() >= parts;
                }
            }, 5000);

            assertTrue(wait);
        }

        assertEquals(Ignition.allGrids().size(), 2);

        checkLocalStore(ignite1, LOCAL_STORE_1, DEFAULT_CACHE_NAME);
        checkLocalStore(ignite2, LOCAL_STORE_2, DEFAULT_CACHE_NAME);
    }

    /**
     * Checks that local stores contains only primary entry.
     *  @param ignite Ignite.
     * @param store Store.
     * @param name Cache name.
     */
    private void checkLocalStore(Ignite ignite, CacheStore<Integer, IgniteBiTuple<Integer, ?>> store, String name) {
        for (int i = 0; i < KEYS; i++) {
            if (ignite.affinity(name).isPrimaryOrBackup(ignite.cluster().localNode(), i))
                assertEquals(store.load(i).get1().intValue(), i);
            else
                assertNull(store.load(i));
        }
    }

    /**
     * Checks that local stores contains primary and backup entries.
     *  @param ignite Ignite.
     * @param store Store.
     * @param name Cache name.
     * @param keys keys.
     */
    private void checkLocalStore(Ignite ignite, CacheStore<Integer, IgniteBiTuple<Integer, ?>> store, String name,
        Set<Integer> keys) {
        checkLocalStore(ignite, store, name, keys, true);
    }

    /**
     * Checks that local stores contains primary and backup or only primary entries.
     *
     * @param ignite Ignite.
     * @param store Store.
     * @param name Cache name.
     * @param keys keys.
     */
    private void checkLocalStore(Ignite ignite, CacheStore<Integer, IgniteBiTuple<Integer, ?>> store, String name,
        Set<Integer> keys, boolean withBackups) {
        for (int key : keys) {
            if (withBackups) {
                if (ignite.affinity(name).isPrimaryOrBackup(ignite.cluster().localNode(), key))
                    assertEquals(store.load(key).get1().intValue(), key);
                else
                    assertNull(store.load(key));
            }
            else {
                if (ignite.affinity(name).isPrimary(ignite.cluster().localNode(), key))
                    assertEquals(store.load(key).get1().intValue(), key);
                else
                    assertNull(store.load(key));
            }
        }
    }

    /**
     *
     */
    @CacheLocalStore
    public static class TestLocalStore<K, V> implements CacheStore<K, IgniteBiTuple<V, ?>> {
        /** */
        private final Map<K, IgniteBiTuple<V, ?>> map = new ConcurrentHashMap<>();

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

    /**
     *
     */
    static class StoreFactory implements Factory<CacheStore> {
        /** */
        @IgniteInstanceResource
        private Ignite node;

        @Override public CacheStore create() {
            String igniteInstanceName = node.configuration().getIgniteInstanceName();

            if (igniteInstanceName.endsWith("1"))
                return LOCAL_STORE_1;
            else if (igniteInstanceName.endsWith("2"))
                return LOCAL_STORE_2;
            else if (igniteInstanceName.endsWith("3"))
                return LOCAL_STORE_3;
            else if (igniteInstanceName.endsWith("4"))
                return LOCAL_STORE_4;
            else if (igniteInstanceName.endsWith("5"))
                return LOCAL_STORE_5;
            else
                return LOCAL_STORE_6;
        }
    }
}