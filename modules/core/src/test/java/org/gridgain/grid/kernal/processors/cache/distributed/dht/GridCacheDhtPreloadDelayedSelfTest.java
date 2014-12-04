/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;

/**
 * Test cases for partitioned cache {@link GridDhtPreloader preloader}.
 *
 * Forum example <a href="http://www.gridgainsystems.com/jiveforums/thread.jspa?threadID=1449">
 * http://www.gridgainsystems.com/jiveforums/thread.jspa?threadID=1449</a>
 */
public class GridCacheDhtPreloadDelayedSelfTest extends GridCommonAbstractTest {
    /** Key count. */
    private static final int KEY_CNT = 100;

    /** Preload delay. */
    private static final int PRELOAD_DELAY = 5000;

    /** Preload mode. */
    private GridCachePreloadMode preloadMode = ASYNC;

    /** Preload delay. */
    private long delay = -1;

    /** IP finder. */
    private GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        assert preloadMode != null;

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cc.setPreloadMode(preloadMode);
        cc.setPreloadPartitionedDelay(delay);
        cc.setAffinity(new GridCacheConsistentHashAffinityFunction(false, 128));
        cc.setBackups(1);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setDistributionMode(NEAR_PARTITIONED);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);
        disco.setMaxMissedHeartbeats(Integer.MAX_VALUE);

        c.setDiscoverySpi(disco);
        c.setCacheConfiguration(cc);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** @throws Exception If failed. */
    public void testManualPreload() throws Exception {
        delay = -1;

        Ignite g0 = startGrid(0);

        int cnt = KEY_CNT;

        GridCache<String, Integer> c0 = g0.cache(null);

        for (int i = 0; i < cnt; i++)
            c0.put(Integer.toString(i), i);

        Ignite g1 = startGrid(1);
        Ignite g2 = startGrid(2);

        GridCache<String, Integer> c1 = g1.cache(null);
        GridCache<String, Integer> c2 = g2.cache(null);

        for (int i = 0; i < cnt; i++)
            assertNull(c1.peek(Integer.toString(i)));

        for (int i = 0; i < cnt; i++)
            assertNull(c2.peek(Integer.toString(i)));

        final CountDownLatch l1 = new CountDownLatch(1);
        final CountDownLatch l2 = new CountDownLatch(1);

        g1.events().localListen(new IgnitePredicate<IgniteEvent>() {
            @Override public boolean apply(IgniteEvent evt) {
                l1.countDown();

                return true;
            }
        }, IgniteEventType.EVT_CACHE_PRELOAD_STOPPED);

        g2.events().localListen(new IgnitePredicate<IgniteEvent>() {
            @Override public boolean apply(IgniteEvent evt) {
                l2.countDown();

                return true;
            }
        }, IgniteEventType.EVT_CACHE_PRELOAD_STOPPED);

        info("Beginning to wait for cache1 repartition.");

        GridDhtCacheAdapter<String, Integer> d0 = dht(0);
        GridDhtCacheAdapter<String, Integer> d1 = dht(1);
        GridDhtCacheAdapter<String, Integer> d2 = dht(2);

        checkMaps(false, d0, d1, d2);

        // Force preload.
        c1.forceRepartition();

        l1.await();

        info("Cache1 is repartitioned.");

        checkMaps(false, d0, d1, d2);

        info("Beginning to wait for cache2 repartition.");

        // Force preload.
        c2.forceRepartition();

        l2.await();

        info("Cache2 is repartitioned.");

        checkMaps(true, d0, d1, d2);

        checkCache(c0, cnt);
        checkCache(c1, cnt);
        checkCache(c2, cnt);
    }

    /** @throws Exception If failed. */
    public void _testDelayedPreload() throws Exception { // TODO GG-9141
        delay = PRELOAD_DELAY;

        Ignite g0 = startGrid(0);

        int cnt = KEY_CNT;

        GridCache<String, Integer> c0 = g0.cache(null);

        for (int i = 0; i < cnt; i++)
            c0.put(Integer.toString(i), i);

        Ignite g1 = startGrid(1);
        Ignite g2 = startGrid(2);

        GridCache<String, Integer> c1 = g1.cache(null);
        GridCache<String, Integer> c2 = g2.cache(null);

        for (int i = 0; i < cnt; i++)
            assertNull(c1.peek(Integer.toString(i)));

        for (int i = 0; i < cnt; i++)
            assertNull(c2.peek(Integer.toString(i)));

        final CountDownLatch l1 = new CountDownLatch(1);
        final CountDownLatch l2 = new CountDownLatch(1);

        g1.events().localListen(new IgnitePredicate<IgniteEvent>() {
            @Override public boolean apply(IgniteEvent evt) {
                l1.countDown();

                return true;
            }
        }, IgniteEventType.EVT_CACHE_PRELOAD_STOPPED);

        g2.events().localListen(new IgnitePredicate<IgniteEvent>() {
            @Override public boolean apply(IgniteEvent evt) {
                l2.countDown();

                return true;
            }
        }, IgniteEventType.EVT_CACHE_PRELOAD_STOPPED);

        U.sleep(1000);

        GridDhtCacheAdapter<String, Integer> d0 = dht(0);
        GridDhtCacheAdapter<String, Integer> d1 = dht(1);
        GridDhtCacheAdapter<String, Integer> d2 = dht(2);

        info("Beginning to wait for caches repartition.");

        checkMaps(false, d0, d1, d2);

        assert l1.await(PRELOAD_DELAY * 3 / 2, TimeUnit.MILLISECONDS);

        assert l2.await(PRELOAD_DELAY * 3 / 2, TimeUnit.MILLISECONDS);

        U.sleep(1000);

        info("Caches are repartitioned.");

        checkMaps(true, d0, d1, d2);

        checkCache(c0, cnt);
        checkCache(c1, cnt);
        checkCache(c2, cnt);
    }

    /** @throws Exception If failed. */
    public void testAutomaticPreload() throws Exception {
        delay = 0;
        preloadMode = GridCachePreloadMode.SYNC;

        Ignite g0 = startGrid(0);

        int cnt = KEY_CNT;

        GridCache<String, Integer> c0 = g0.cache(null);

        for (int i = 0; i < cnt; i++)
            c0.put(Integer.toString(i), i);

        Ignite g1 = startGrid(1);
        Ignite g2 = startGrid(2);

        GridCache<String, Integer> c1 = g1.cache(null);
        GridCache<String, Integer> c2 = g2.cache(null);

        GridDhtCacheAdapter<String, Integer> d0 = dht(0);
        GridDhtCacheAdapter<String, Integer> d1 = dht(1);
        GridDhtCacheAdapter<String, Integer> d2 = dht(2);

        checkMaps(true, d0, d1, d2);

        checkCache(c0, cnt);
        checkCache(c1, cnt);
        checkCache(c2, cnt);
    }

    /** @throws Exception If failed. */
    public void testAutomaticPreloadWithEmptyCache() throws Exception {
        preloadMode = SYNC;

        delay = 0;

        Collection<Ignite> ignites = new ArrayList<>();

        try {
            for (int i = 0; i < 5; i++) {
                ignites.add(startGrid(i));

                awaitPartitionMapExchange();

                for (Ignite g : ignites) {
                    info(">>> Checking affinity for grid: " + g.name());

                    GridDhtPartitionTopology<Integer, String> top = topology(g);

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
                                ", part=" + p + ", state=" + state + ", grid=" + G.grid(nodeId).name() +
                                ", affNames=" + U.nodes2names(nodes) + ", affIds=" + nodeIds + ']';
                        }
                    }
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testManualPreloadSyncMode() throws Exception {
        preloadMode = GridCachePreloadMode.SYNC;
        delay = -1;

        try {
            startGrid(0);
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testPreloadManyNodes() throws Exception {
        delay = 0;
        preloadMode = ASYNC;

        startGridsMultiThreaded(9);

        U.sleep(2000);

        try {
            delay = -1;
            preloadMode = ASYNC;

            Ignite g = startGrid(9);

            info(">>> Starting manual preload");

            long start = System.currentTimeMillis();

            g.cache(null).forceRepartition().get();

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
    private GridDhtPartitionTopology<Integer, String> topology(Ignite g) {
        return ((GridNearCacheAdapter<Integer, String>)((GridKernal)g).<Integer, String>internalCache()).dht().topology();
    }

    /**
     * @param g Grid.
     * @return Affinity.
     */
    private GridCacheAffinity<Object> affinity(Ignite g) {
        return g.cache(null).affinity();
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
    private void checkCache(GridCache<String, Integer> c, int keyCnt) {
        Ignite g = c.gridProjection().grid();

        for (int i = 0; i < keyCnt; i++) {
            String key = Integer.toString(i);

            if (c.affinity().isPrimaryOrBackup(g.cluster().localNode(), key))
                assertEquals(Integer.valueOf(i), c.peek(key));
        }
    }

    /**
     * Checks maps for equality.
     *
     * @param strict Strict check flag.
     * @param caches Maps to compare.
     */
    private void checkMaps(final boolean strict, final GridDhtCacheAdapter<String, Integer>... caches)
        throws GridInterruptedException {
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
