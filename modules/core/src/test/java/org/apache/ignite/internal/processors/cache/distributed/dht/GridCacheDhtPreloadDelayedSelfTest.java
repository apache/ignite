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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;

/**
 * Test cases for partitioned cache {@link GridDhtPreloader preloader}.
 */
public class GridCacheDhtPreloadDelayedSelfTest extends GridCommonAbstractTest {
    /** Key count. */
    private static final int KEY_CNT = 100;

    /** Preload delay. */
    private static final int PRELOAD_DELAY = 5000;

    /** Preload mode. */
    private CacheRebalanceMode preloadMode = ASYNC;

    /** Preload delay. */
    private long delay = -1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(persistenceEnabled())).
                setWalSegmentSize(4 * 1024 * 1024));

        assert preloadMode != null;

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cc.setRebalanceMode(preloadMode);
        cc.setRebalanceDelay(delay);
        cc.setAffinity(new RendezvousAffinityFunction(false, 128));
        cc.setBackups(1);
        cc.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(cc);

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /**
     * @return {@code True} if a persistent grid is required.
     */
    protected boolean persistenceEnabled() {
        return false;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testManualPreload() throws Exception {
        delay = -1;

        Ignite g0 = startGrid(0);

        if (persistenceEnabled())
            g0.cluster().state(ClusterState.ACTIVE);

        int cnt = KEY_CNT;

        IgniteCache<String, Integer> c0 = g0.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < cnt; i++)
            c0.put(Integer.toString(i), i);

        Ignite g1 = startGrid(1);

        if (persistenceEnabled())
            resetBaselineTopology();

        Ignite g2 = startGrid(2);

        if (persistenceEnabled())
            resetBaselineTopology();

        IgniteCache<String, Integer> c1 = g1.cache(DEFAULT_CACHE_NAME);
        IgniteCache<String, Integer> c2 = g2.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < cnt; i++)
            assertNull(c1.localPeek(Integer.toString(i), CachePeekMode.ONHEAP));

        for (int i = 0; i < cnt; i++)
            assertNull(c2.localPeek(Integer.toString(i), CachePeekMode.ONHEAP));

        final CountDownLatch l1 = new CountDownLatch(1);
        final CountDownLatch l2 = new CountDownLatch(1);

        g1.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                l1.countDown();

                return true;
            }
        }, EventType.EVT_CACHE_REBALANCE_STOPPED);

        g2.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                l2.countDown();

                return true;
            }
        }, EventType.EVT_CACHE_REBALANCE_STOPPED);

        info("Beginning to wait for cache1 repartition.");

        GridDhtCacheAdapter<String, Integer> d0 = dht(0);
        GridDhtCacheAdapter<String, Integer> d1 = dht(1);
        GridDhtCacheAdapter<String, Integer> d2 = dht(2);

        doSleep(1000);

        // Force preload.
        c1.rebalance();

        l1.await();

        info("Cache1 is repartitioned.");

        checkMaps(false, d0, d1, d2);

        info("Beginning to wait for cache2 repartition.");

        // Force preload.
        c2.rebalance();

        l2.await();

        info("Cache2 is repartitioned.");

        checkMaps(true, d0, d1, d2);

        checkCache(c0, cnt);
        checkCache(c1, cnt);
        checkCache(c2, cnt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDelayedPreload() throws Exception {
        delay = PRELOAD_DELAY;

        Ignite g0 = startGrid(0);

        if (persistenceEnabled())
            g0.cluster().state(ClusterState.ACTIVE);

        int cnt = KEY_CNT;

        IgniteCache<String, Integer> c0 = g0.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < cnt; i++)
            c0.put(Integer.toString(i), i);

        Ignite g1 = startGrid(1);

        if (persistenceEnabled())
            resetBaselineTopology();

        Ignite g2 = startGrid(2);

        if (persistenceEnabled())
            resetBaselineTopology();

        IgniteCache<String, Integer> c1 = g1.cache(DEFAULT_CACHE_NAME);
        IgniteCache<String, Integer> c2 = g2.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < cnt; i++)
            assertNull(c1.localPeek(Integer.toString(i), CachePeekMode.ONHEAP));

        for (int i = 0; i < cnt; i++)
            assertNull(c2.localPeek(Integer.toString(i), CachePeekMode.ONHEAP));

        final CountDownLatch l1 = new CountDownLatch(1);
        final CountDownLatch l2 = new CountDownLatch(1);

        g1.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                l1.countDown();

                return true;
            }
        }, EventType.EVT_CACHE_REBALANCE_STOPPED);

        g2.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                l2.countDown();

                return true;
            }
        }, EventType.EVT_CACHE_REBALANCE_STOPPED);

        U.sleep(1000);

        GridDhtCacheAdapter<String, Integer> d0 = dht(0);
        GridDhtCacheAdapter<String, Integer> d1 = dht(1);
        GridDhtCacheAdapter<String, Integer> d2 = dht(2);

        // Partitions maps are not always equal because state is not send after moving to EVICTED state.

        info("Beginning to wait for caches repartition.");

        assert l1.await(PRELOAD_DELAY * 3 / 2, MILLISECONDS);

        assert l2.await(PRELOAD_DELAY * 3 / 2, MILLISECONDS);

        U.sleep(1000);

        info("Caches are repartitioned.");

        checkMaps(true, d0, d1, d2);

        checkCache(c0, cnt);
        checkCache(c1, cnt);
        checkCache(c2, cnt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAutomaticPreload() throws Exception {
        delay = 0;
        preloadMode = CacheRebalanceMode.SYNC;

        Ignite g0 = startGrid(0);

        if (persistenceEnabled())
            g0.cluster().state(ClusterState.ACTIVE);

        int cnt = KEY_CNT;

        IgniteCache<String, Integer> c0 = g0.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < cnt; i++)
            c0.put(Integer.toString(i), i);

        Ignite g1 = startGrid(1);

        if (persistenceEnabled())
            resetBaselineTopology();

        Ignite g2 = startGrid(2);

        if (persistenceEnabled())
            resetBaselineTopology();

        IgniteCache<String, Integer> c1 = g1.cache(DEFAULT_CACHE_NAME);
        IgniteCache<String, Integer> c2 = g2.cache(DEFAULT_CACHE_NAME);

        GridDhtCacheAdapter<String, Integer> d0 = dht(0);
        GridDhtCacheAdapter<String, Integer> d1 = dht(1);
        GridDhtCacheAdapter<String, Integer> d2 = dht(2);

        checkMaps(true, d0, d1, d2);

        checkCache(c0, cnt);
        checkCache(c1, cnt);
        checkCache(c2, cnt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAutomaticPreloadWithEmptyCache() throws Exception {
        preloadMode = SYNC;

        delay = 0;

        Collection<Ignite> ignites = new ArrayList<>();

        try {
            for (int i = 0; i < 5; i++) {
                ignites.add(startGrid(i));

                if (persistenceEnabled()) {
                    if (i == 0)
                        grid(0).cluster().state(ClusterState.ACTIVE);
                    else
                        resetBaselineTopology();
                }

                awaitPartitionMapExchange(true, true, null, false);

                for (Ignite g : ignites) {
                    info(">>> Checking affinity for grid: " + g.name());

                    GridDhtPartitionTopology top = topology(g);

                    GridDhtPartitionFullMap fullMap = top.partitionMap(true);

                    for (Map.Entry<UUID, GridDhtPartitionMap> fe : fullMap.entrySet()) {
                        UUID nodeId = fe.getKey();

                        GridDhtPartitionMap m = fe.getValue();

                        for (Map.Entry<Integer, GridDhtPartitionState> e : m.entrySet()) {
                            int p = e.getKey();
                            GridDhtPartitionState state = e.getValue();

                            Collection<ClusterNode> nodes = affinityNodes(g, p);

                            Collection<UUID> nodeIds = U.nodeIds(nodes);

                            assert nodeIds.contains(nodeId) : "Invalid affinity mapping [nodeId=" + nodeId +
                                ", part=" + p + ", state=" + state + ", igniteInstanceName=" +
                                G.ignite(nodeId).name() + ", affNames=" + U.nodes2names(nodes) +
                                ", affIds=" + nodeIds + ']';
                        }
                    }
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testManualPreloadSyncMode() throws Exception {
        preloadMode = CacheRebalanceMode.SYNC;
        delay = -1;

        try {
            IgniteEx crd = startGrid(0);

            if (persistenceEnabled())
                crd.cluster().state(ClusterState.ACTIVE);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPreloadManyNodes() throws Exception {
        delay = 0;
        preloadMode = ASYNC;

        final int cnt = persistenceEnabled() ? 4 : 9;

        startGridsMultiThreaded(cnt);

        if (persistenceEnabled())
            grid(0).cluster().state(ClusterState.ACTIVE);

        U.sleep(2000);

        try {
            delay = -1;
            preloadMode = ASYNC;

            Ignite g = startGrid(cnt);

            if (persistenceEnabled())
                resetBaselineTopology();

            info(">>> Starting manual preload");

            long start = System.currentTimeMillis();

            g.cache(DEFAULT_CACHE_NAME).rebalance().get();

            info(">>> Finished preloading of empty cache in " + (System.currentTimeMillis() - start) + "ms.");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param g Grid.
     * @return Topology.
     */
    private GridDhtPartitionTopology topology(Ignite g) {
        GridCacheAdapter<Integer, String> internalCache = ((IgniteKernal)g).internalCache(DEFAULT_CACHE_NAME);

        return internalCache.isNear() ? ((GridNearCacheAdapter<Integer, String>)internalCache).dht().topology() :
            internalCache.context().dht().topology();
    }

    /**
     * @param g Grid.
     * @return Affinity.
     */
    private Affinity<Object> affinity(Ignite g) {
        return g.affinity(DEFAULT_CACHE_NAME);
    }

    /**
     * @param g Grid.
     * @param p Partition.
     * @return Affinity nodes.
     */
    private Collection<ClusterNode> affinityNodes(Ignite g, int p) {
        return affinity(g).mapPartitionToPrimaryAndBackups(p);
    }

    /**
     * Checks if keys are present.
     *
     * @param c Cache.
     * @param keyCnt Key count.
     */
    private void checkCache(IgniteCache<String, Integer> c, int keyCnt) {
        Ignite g = c.unwrap(Ignite.class);

        for (int i = 0; i < keyCnt; i++) {
            String key = Integer.toString(i);

            if (affinity(c).isPrimaryOrBackup(g.cluster().localNode(), key))
                assertEquals(Integer.valueOf(i), c.localPeek(key));
        }
    }

    /**
     * Checks maps for equality.
     *
     * @param strict Strict check flag.
     * @param caches Maps to compare.
     * @throws Exception If failed.
     */
    @SafeVarargs
    private final void checkMaps(final boolean strict, final GridDhtCacheAdapter<String, Integer>... caches)
        throws Exception {
        if (caches.length < 2)
            return;

        GridTestUtils.retryAssert(log, 50, 500, new CAX() {
            @Override public void applyx() {
                info("Checking partition maps.");

                for (int i = 0; i < caches.length; i++)
                    info("Partition map for node " + i + ": " + caches[i].topology().partitionMap(false).toFullString());

                GridDhtPartitionFullMap orig = caches[0].topology().partitionMap(true);

                for (int i = 1; i < caches.length; i++) {
                    GridDhtPartitionFullMap cmp = caches[i].topology().partitionMap(true);

                    assert orig.keySet().equals(cmp.keySet());

                    for (Map.Entry<UUID, GridDhtPartitionMap> entry : orig.entrySet()) {
                        UUID nodeId = entry.getKey();

                        GridDhtPartitionMap nodeMap = entry.getValue();

                        GridDhtPartitionMap cmpMap = cmp.get(nodeId);

                        assert cmpMap != null;

                        assert nodeMap.keySet().equals(cmpMap.keySet());

                        for (Map.Entry<Integer, GridDhtPartitionState> nodeEntry : nodeMap.entrySet()) {
                            GridDhtPartitionState state = cmpMap.get(nodeEntry.getKey());

                            assert state != null;
                            assert state != GridDhtPartitionState.EVICTED;
                            assert !strict || state == GridDhtPartitionState.OWNING : "Invalid partition state: " + state;
                            assert state == nodeEntry.getValue();
                        }
                    }
                }
            }
        });

    }
}
