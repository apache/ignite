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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.affinity.rendezvous.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.configuration.CacheConfiguration.*;
import static org.apache.ignite.configuration.DeploymentMode.*;
import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.*;

/**
 * Test cases for partitioned cache {@link GridDhtPreloader preloader}.
 */
public class GridCacheDhtPreloadSelfTest extends GridCommonAbstractTest {
    /** Flag to print preloading events. */
    private static final boolean DEBUG = false;

    /** */
    private static final long TEST_TIMEOUT = 5 * 60 * 1000;

    /** Default backups. */
    private static final int DFLT_BACKUPS = 1;

    /** Partitions. */
    private static final int DFLT_PARTITIONS = 521;

    /** Preload batch size. */
    private static final int DFLT_BATCH_SIZE = DFLT_REBALANCE_BATCH_SIZE;

    /** Number of key backups. Each test method can set this value as required. */
    private int backups = DFLT_BACKUPS;

    /** Preload mode. */
    private CacheRebalanceMode preloadMode = ASYNC;

    /** */
    private int preloadBatchSize = DFLT_BATCH_SIZE;

    /** Number of partitions. */
    private int partitions = DFLT_PARTITIONS;

    /** IP finder. */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     *
     */
    public GridCacheDhtPreloadSelfTest() {
        super(false /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);
        cfg.setCacheConfiguration(cacheConfiguration(gridName));
        cfg.setDeploymentMode(CONTINUOUS);

        return cfg;
    }

    /**
     * Gets cache configuration for grid with given name.
     *
     * @param gridName Grid name.
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration(String gridName) {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setRebalanceBatchSize(preloadBatchSize);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setRebalanceMode(preloadMode);
        cacheCfg.setAffinity(new CacheRendezvousAffinityFunction(false, partitions));
        cacheCfg.setBackups(backups);

        return cacheCfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
//        resetLog4j(Level.DEBUG, true,
//            // Categories.
//            GridDhtPreloader.class.getPackage().getName(),
//            GridDhtPartitionTopologyImpl.class.getName(),
//            GridDhtLocalPartition.class.getName());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        backups = DFLT_BACKUPS;
        partitions = DFLT_PARTITIONS;
        preloadMode = ASYNC;
        preloadBatchSize = DFLT_BATCH_SIZE;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /**
     * @param c Cache.
     * @return {@code True} if synchronous preloading.
     */
    private boolean isSync(GridCache<?, ?> c) {
        return c.configuration().getRebalanceMode() == SYNC;
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivePartitionTransferSyncSameCoordinator() throws Exception {
        preloadMode = SYNC;

        checkActivePartitionTransfer(1000, 4, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivePartitionTransferAsyncSameCoordinator() throws Exception {
        checkActivePartitionTransfer(1000, 4, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivePartitionTransferSyncChangingCoordinator() throws Exception {
        preloadMode = SYNC;

        checkActivePartitionTransfer(1000, 4, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivePartitionTransferAsyncChangingCoordinator() throws Exception {
        checkActivePartitionTransfer(1000, 4, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivePartitionTransferSyncRandomCoordinator() throws Exception {
        preloadMode = SYNC;

        checkActivePartitionTransfer(1000, 4, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivePartitionTransferAsyncRandomCoordinator() throws Exception {
        checkActivePartitionTransfer(1000, 4, false, true);
    }

    /**
     * @param keyCnt Key count.
     * @param nodeCnt Node count.
     * @param sameCoord Same coordinator flag.
     * @param shuffle Shuffle flag.
     * @throws Exception If failed.
     */
    private void checkActivePartitionTransfer(int keyCnt, int nodeCnt, boolean sameCoord, boolean shuffle)
        throws Exception {
//        resetLog4j(Level.DEBUG, true,
//            // Categories.
//            GridDhtPreloader.class.getPackage().getName(),
//            GridDhtPartitionTopologyImpl.class.getName(),
//            GridDhtLocalPartition.class.getName());

        try {
            Ignite ignite1 = startGrid(0);

            IgniteCache<Integer, String> cache1 = ignite1.jcache(null);

            putKeys(cache1, keyCnt);
            checkKeys(cache1, keyCnt, F.asList(ignite1));

            List<Ignite> ignites = new ArrayList<>(nodeCnt + 1);

            startGrids(nodeCnt, 1, ignites);

            // Check all nodes.
            for (Ignite g : ignites) {
                IgniteCache<Integer, String> c = g.jcache(null);

                checkKeys(c, keyCnt, ignites);
            }

            if (shuffle)
                Collections.shuffle(ignites);

            if (sameCoord)
                // Add last.
                ignites.add(ignite1);
            else
                // Add first.
                ignites.add(0, ignite1);

            if (!sameCoord && shuffle)
                Collections.shuffle(ignites);

            checkActiveState(ignites);

            info(">>> Finished checking nodes [keyCnt=" + keyCnt + ", nodeCnt=" + nodeCnt + ", grids=" +
                U.grids2names(ignites) + ']');

            Collection<IgniteFuture<?>> futs = new LinkedList<>();

            Ignite last = F.last(ignites);

            for (Iterator<Ignite> it = ignites.iterator(); it.hasNext(); ) {
                Ignite g = it.next();

                if (!it.hasNext()) {
                    assert last == g;

                    break;
                }

                checkActiveState(ignites);

                final UUID nodeId = g.cluster().localNode().id();

                it.remove();

                futs.add(waitForLocalEvent(last.events(), new P1<Event>() {
                    @Override public boolean apply(Event e) {
                        CacheRebalancingEvent evt = (CacheRebalancingEvent)e;

                        ClusterNode node = evt.discoveryNode();

                        return evt.type() == EVT_CACHE_REBALANCE_STOPPED && node.id().equals(nodeId) &&
                            evt.discoveryEventType() == EVT_NODE_LEFT;
                    }
                }, EVT_CACHE_REBALANCE_STOPPED));

                info("Before grid stop [name=" + g.name() + ", fullTop=" + top2string(ignites));

                stopGrid(g.name());

                info("After grid stop [name=" + g.name() + ", fullTop=" + top2string(ignites));

                // Check all left nodes.
                checkActiveState(ignites);
            }

            info("Waiting for preload futures: " + F.view(futs, new IgnitePredicate<IgniteFuture<?>>() {
                @Override public boolean apply(IgniteFuture<?> fut) {
                    return !fut.isDone();
                }
            }));

            X.waitAll(futs);

            info("Finished waiting for preload futures.");

            assert last != null;

            IgniteCache<Integer, String> lastCache = last.jcache(null);

            GridDhtCacheAdapter<Integer, String> dht = dht(lastCache);

            CacheAffinity<Integer> aff = affinity(lastCache);

            info("Finished waiting for all exchange futures...");

            for (int i = 0; i < keyCnt; i++) {
                if (aff.mapPartitionToPrimaryAndBackups(aff.partition(i)).contains(last.cluster().localNode())) {
                    GridDhtPartitionTopology top = dht.topology();

                    for (GridDhtLocalPartition p : top.localPartitions()) {
                        Collection<ClusterNode> moving = top.moving(p.id());

                        assert moving.isEmpty() : "Nodes with partition in moving state [part=" + p +
                            ", moving=" + moving + ']';

                        assert OWNING == p.state() : "Invalid partition state for partition [part=" + p + ", map=" +
                            top.partitionMap(false) + ']';
                    }
                }
            }

            checkActiveState(ignites);
        }
        catch (Error | Exception e) {
            error("Test failed.", e);

            throw e;
        } finally {
            stopAllGrids();
        }
    }

    /**
     * @param grids Grids.
     */
    private void checkActiveState(Iterable<Ignite> grids) {
        // Check that nodes don't have non-active information about other nodes.
        for (Ignite g : grids) {
            IgniteCache<Integer, String> c = g.jcache(null);

            GridDhtCacheAdapter<Integer, String> dht = dht(c);

            GridDhtPartitionFullMap allParts = dht.topology().partitionMap(false);

            for (GridDhtPartitionMap parts : allParts.values()) {
                if (!parts.nodeId().equals(g.cluster().localNode().id())) {
                    for (Map.Entry<Integer, GridDhtPartitionState> e : parts.entrySet()) {
                        int p = e.getKey();

                        GridDhtPartitionState state = e.getValue();

                        assert state == OWNING || state == MOVING || state == RENTING :
                            "Invalid state [grid=" + g.name() + ", part=" + p + ", state=" + state +
                                ", parts=" + parts + ']';

                        assert state.active();
                    }
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultiplePartitionBatchesSyncPreload() throws Exception {
        preloadMode = SYNC;
        preloadBatchSize = 100;
        partitions = 2;

        checkNodes(1000, 1, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultiplePartitionBatchesAsyncPreload() throws Exception {
        preloadBatchSize = 100;
        partitions = 2;

        checkNodes(1000, 1, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultipleNodesSyncPreloadSameCoordinator() throws Exception {
        preloadMode = SYNC;

        checkNodes(1000, 4, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultipleNodesAsyncPreloadSameCoordinator() throws Exception {
        checkNodes(1000, 4, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultipleNodesSyncPreloadChangingCoordinator() throws Exception {
        preloadMode = SYNC;

        checkNodes(1000, 4, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultipleNodesAsyncPreloadChangingCoordinator() throws Exception {
        checkNodes(1000, 4, false, false);
    }


    /**
     * @throws Exception If failed.
     */
    public void testMultipleNodesSyncPreloadRandomCoordinator() throws Exception {
        preloadMode = SYNC;

        checkNodes(1000, 4, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultipleNodesAsyncPreloadRandomCoordinator() throws Exception {
        checkNodes(1000, 4, false, true);
    }

    /**
     * @param cnt Number of grids.
     * @param startIdx Start node index.
     * @param list List of started grids.
     * @throws Exception If failed.
     */
    private void startGrids(int cnt, int startIdx, Collection<Ignite> list) throws Exception {
        for (int i = 0; i < cnt; i++) {
            final Ignite g = startGrid(startIdx++);

            if (DEBUG)
                g.events().localListen(new IgnitePredicate<Event>() {
                    @Override public boolean apply(Event evt) {
                        info("\n>>> Preload event [grid=" + g.name() + ", evt=" + evt + ']');

                        return true;
                    }
                }, EVTS_CACHE_REBALANCE);

            list.add(g);
        }
    }

    /**
     * @param grids Grids to stop.
     */
    private void stopGrids(Iterable<Ignite> grids) {
        for (Ignite g : grids)
            stopGrid(g.name());
    }

    /**
     * @param keyCnt Key count.
     * @param nodeCnt Node count.
     * @param sameCoord Same coordinator flag.
     * @param shuffle Shuffle flag.
     * @throws Exception If failed.
     */
    private void checkNodes(int keyCnt, int nodeCnt, boolean sameCoord, boolean shuffle)
        throws Exception {
//        resetLog4j(Level.DEBUG, true,
//            // Categories.
//            GridDhtPreloader.class.getPackage().getName(),
//            GridDhtPartitionTopologyImpl.class.getName(),
//            GridDhtLocalPartition.class.getName());

        try {
            Ignite ignite1 = startGrid(0);

            IgniteCache<Integer, String> cache1 = ignite1.jcache(null);

            putKeys(cache1, keyCnt);
            checkKeys(cache1, keyCnt, F.asList(ignite1));

            List<Ignite> ignites = new ArrayList<>(nodeCnt + 1);

            startGrids(nodeCnt, 1, ignites);

            // Check all nodes.
            for (Ignite g : ignites) {
                IgniteCache<Integer, String> c = g.jcache(null);

                checkKeys(c, keyCnt, ignites);
            }

            if (shuffle)
                Collections.shuffle(ignites);

            if (sameCoord)
                // Add last.
                ignites.add(ignite1);
            else
                // Add first.
                ignites.add(0, ignite1);

            if (!sameCoord && shuffle)
                Collections.shuffle(ignites);

            info(">>> Finished checking nodes [keyCnt=" + keyCnt + ", nodeCnt=" + nodeCnt + ", grids=" +
                U.grids2names(ignites) + ']');

            Ignite last = null;

            for (Iterator<Ignite> it = ignites.iterator(); it.hasNext(); ) {
                Ignite g = it.next();

                if (!it.hasNext()) {
                    last = g;

                    break;
                }

                final UUID nodeId = g.cluster().localNode().id();

                it.remove();

                Collection<IgniteFuture<?>> futs = new LinkedList<>();

                for (Ignite gg : ignites)
                    futs.add(waitForLocalEvent(gg.events(), new P1<Event>() {
                            @Override public boolean apply(Event e) {
                                CacheRebalancingEvent evt = (CacheRebalancingEvent)e;

                                ClusterNode node = evt.discoveryNode();

                                return evt.type() == EVT_CACHE_REBALANCE_STOPPED && node.id().equals(nodeId) &&
                                    evt.discoveryEventType() == EVT_NODE_LEFT;
                            }
                        }, EVT_CACHE_REBALANCE_STOPPED));


                info("Before grid stop [name=" + g.name() + ", fullTop=" + top2string(ignites));

                stopGrid(g.name());

                info(">>> Waiting for preload futures [leftNode=" + g.name() + ", remaining=" + U.grids2names(ignites) + ']');

                X.waitAll(futs);

                info("After grid stop [name=" + g.name() + ", fullTop=" + top2string(ignites));

                // Check all left nodes.
                for (Ignite gg : ignites) {
                    IgniteCache<Integer, String> c = gg.jcache(null);

                    checkKeys(c, keyCnt, ignites);
                }
            }

            assert last != null;

            IgniteCache<Integer, String> lastCache = last.jcache(null);

            GridDhtCacheAdapter<Integer, String> dht = dht(lastCache);

            CacheAffinity<Integer> aff = affinity(lastCache);

            for (int i = 0; i < keyCnt; i++) {
                if (aff.mapPartitionToPrimaryAndBackups(aff.partition(i)).contains(last.cluster().localNode())) {
                    GridDhtPartitionTopology top = dht.topology();

                    for (GridDhtLocalPartition p : top.localPartitions()) {
                        Collection<ClusterNode> moving = top.moving(p.id());

                        assert moving.isEmpty() : "Nodes with partition in moving state [part=" + p +
                            ", moving=" + moving + ']';

                        assert OWNING == p.state() : "Invalid partition state for partition [part=" + p + ", map=" +
                            top.partitionMap(false) + ']';
                    }
                }
            }
        }
        catch (Error | Exception e) {
            error("Test failed.", e);

            throw e;
        } finally {
            stopAllGrids();
        }
    }

    /**
     * @param c Cache.
     * @param cnt Key count.
     */
    private void putKeys(IgniteCache<Integer, String> c, int cnt) {
        for (int i = 0; i < cnt; i++)
            c.put(i, Integer.toString(i));
    }

    /**
     * @param cache Cache.
     * @param cnt Key count.
     * @param grids Grids.
     */
    private void checkKeys(IgniteCache<Integer, String> cache, int cnt, Iterable<Ignite> grids) {
        CacheAffinity<Integer> aff = affinity(cache);

        Ignite ignite = cache.unwrap(Ignite.class);

        ClusterNode loc = ignite.cluster().localNode();

        boolean sync = cache.getConfiguration(CacheConfiguration.class).getRebalanceMode() == SYNC;

        for (int i = 0; i < cnt; i++) {
            Collection<ClusterNode> nodes = ignite.cluster().nodes();

            Collection<ClusterNode> affNodes = aff.mapPartitionToPrimaryAndBackups(aff.partition(i));

            assert !affNodes.isEmpty();

            if (affNodes.contains(loc)) {
                String val = sync ? cache.localPeek(i, CachePeekMode.ONHEAP) : cache.get(i);

                ClusterNode primaryNode = F.first(affNodes);

                assert primaryNode != null;

                boolean primary = primaryNode.equals(loc);

                assert Integer.toString(i).equals(val) : "Key check failed [grid=" + ignite.name() +
                    ", cache=" + cache.getName() + ", key=" + i + ", expected=" + i + ", actual=" + val +
                    ", part=" + aff.partition(i) + ", primary=" + primary + ", affNodes=" + U.nodeIds(affNodes) +
                    ", locId=" + loc.id() + ", allNodes=" + U.nodeIds(nodes) + ", allParts=" + top2string(grids) + ']';
            }
        }
    }

    /**
     * @param grids Grids
     * @return String representation of all partitions and their state.
     */
    @SuppressWarnings( {"ConstantConditions"})
    private String top2string(Iterable<Ignite> grids) {
        Map<String, String> map = new HashMap<>();

        for (Ignite g : grids) {
            IgniteCache<Integer, String> c = g.jcache(null);

            GridDhtCacheAdapter<Integer, String> dht = dht(c);

            GridDhtPartitionFullMap fullMap = dht.topology().partitionMap(false);

            map.put(g.name(), DEBUG ? fullMap.toFullString() : fullMap.toString());
        }

        return "Grid partition maps [" + map.toString() + ']';
    }
}
