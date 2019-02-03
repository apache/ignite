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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.CacheConfiguration.DFLT_REBALANCE_BATCH_SIZE;
import static org.apache.ignite.configuration.DeploymentMode.CONTINUOUS;
import static org.apache.ignite.events.EventType.EVTS_CACHE_REBALANCE;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.RENTING;

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

    /**
     *
     */
    public GridCacheDhtPreloadSelfTest() {
        super(false /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(igniteInstanceName));
        cfg.setDeploymentMode(CONTINUOUS);

        return cfg;
    }

    /**
     * Gets cache configuration for grid with given name.
     *
     * @param igniteInstanceName Ignite instance name.
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration(String igniteInstanceName) {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setRebalanceBatchSize(preloadBatchSize);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setRebalanceMode(preloadMode);
        cacheCfg.setAffinity(new RendezvousAffinityFunction(false, partitions));
        cacheCfg.setBackups(backups);
        cacheCfg.setOnheapCacheEnabled(onheapCacheEnabled());

        return cacheCfg;
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
     * @return {@code True} if on-heap cache is enabled.
     */
    protected boolean onheapCacheEnabled() {
        return false;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivePartitionTransferSyncSameCoordinator() throws Exception {
        preloadMode = SYNC;

        checkActivePartitionTransfer(1000, 4, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivePartitionTransferAsyncSameCoordinator() throws Exception {
        checkActivePartitionTransfer(1000, 4, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivePartitionTransferSyncChangingCoordinator() throws Exception {
        preloadMode = SYNC;

        checkActivePartitionTransfer(1000, 4, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivePartitionTransferAsyncChangingCoordinator() throws Exception {
        checkActivePartitionTransfer(1000, 4, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivePartitionTransferSyncRandomCoordinator() throws Exception {
        preloadMode = SYNC;

        checkActivePartitionTransfer(1000, 4, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
        try {
            Ignite ignite1 = startGrid(0);

            IgniteCache<Integer, String> cache1 = ignite1.cache(DEFAULT_CACHE_NAME);

            putKeys(cache1, keyCnt);
            checkKeys(cache1, keyCnt, F.asList(ignite1));

            List<Ignite> ignites = new ArrayList<>(nodeCnt + 1);

            startGrids(nodeCnt, 1, ignites);

            // Check all nodes.
            for (Ignite g : ignites) {
                IgniteCache<Integer, String> c = g.cache(DEFAULT_CACHE_NAME);

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

            Ignite last = F.last(ignites);

            for (Iterator<Ignite> it = ignites.iterator(); it.hasNext(); ) {
                Ignite g = it.next();

                if (!it.hasNext()) {
                    assert last == g;

                    break;
                }

                checkActiveState(ignites);

                it.remove();

                info("Before grid stop [name=" + g.name() + ", fullTop=" + top2string(ignites));

                stopGrid(g.name());

                info("After grid stop [name=" + g.name() + ", fullTop=" + top2string(ignites));

                // Check all left nodes.
                checkActiveState(ignites);

                awaitPartitionMapExchange(); // Need wait, otherwise test logic is broken if EVT_NODE_FAILED exchanges are merged.
            }

            info("Finished waiting for preload futures.");

            assert last != null;

            IgniteCache<Integer, String> lastCache = last.cache(DEFAULT_CACHE_NAME);

            GridDhtCacheAdapter<Integer, String> dht = dht(lastCache);

            Affinity<Integer> aff = affinity(lastCache);

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
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param grids Grids.
     */
    private void checkActiveState(Iterable<Ignite> grids) {
        // Check that nodes don't have non-active information about other nodes.
        for (Ignite g : grids) {
            IgniteCache<Integer, String> c = g.cache(DEFAULT_CACHE_NAME);

            GridDhtCacheAdapter<Integer, String> dht = dht(c);

            GridDhtPartitionFullMap allParts = dht.topology().partitionMap(false);

            for (GridDhtPartitionMap parts : allParts.values()) {
                if (!parts.nodeId().equals(g.cluster().localNode().id())) {
                    for (Map.Entry<Integer, GridDhtPartitionState> e : parts.entrySet()) {
                        int p = e.getKey();

                        GridDhtPartitionState state = e.getValue();

                        assert state == OWNING || state == MOVING || state == RENTING :
                            "Invalid state [igniteInstanceName=" + g.name() + ", part=" + p + ", state=" + state +
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
    @Test
    public void testMultiplePartitionBatchesSyncPreload() throws Exception {
        preloadMode = SYNC;
        preloadBatchSize = 100;
        partitions = 2;

        checkNodes(1000, 1, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultiplePartitionBatchesAsyncPreload() throws Exception {
        preloadBatchSize = 100;
        partitions = 2;

        checkNodes(1000, 1, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleNodesSyncPreloadSameCoordinator() throws Exception {
        preloadMode = SYNC;

        checkNodes(1000, 4, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleNodesAsyncPreloadSameCoordinator() throws Exception {
        checkNodes(1000, 4, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleNodesSyncPreloadChangingCoordinator() throws Exception {
        preloadMode = SYNC;

        checkNodes(1000, 4, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleNodesAsyncPreloadChangingCoordinator() throws Exception {
        checkNodes(1000, 4, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleNodesSyncPreloadRandomCoordinator() throws Exception {
        preloadMode = SYNC;

        checkNodes(1000, 4, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
                        info("\n>>> Preload event [igniteInstanceName=" + g.name() + ", evt=" + evt + ']');

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
        try {
            Ignite ignite1 = startGrid(0);

            IgniteCache<Integer, String> cache1 = ignite1.cache(DEFAULT_CACHE_NAME);

            putKeys(cache1, keyCnt);
            checkKeys(cache1, keyCnt, F.asList(ignite1));

            List<Ignite> ignites = new ArrayList<>(nodeCnt + 1);

            startGrids(nodeCnt, 1, ignites);

            // Check all nodes.
            for (Ignite g : ignites) {
                IgniteCache<Integer, String> c = g.cache(DEFAULT_CACHE_NAME);

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

                info("Before grid stop [name=" + g.name() + ", fullTop=" + top2string(ignites));

                stopGrid(g.name());

                info(">>> Waiting for preload futures [leftNode=" + g.name() + ", remaining=" + U.grids2names(ignites) + ']');

                awaitPartitionMapExchange();

                info("After grid stop [name=" + g.name() + ", fullTop=" + top2string(ignites));

                // Check all left nodes.
                for (Ignite gg : ignites) {
                    IgniteCache<Integer, String> c = gg.cache(DEFAULT_CACHE_NAME);

                    checkKeys(c, keyCnt, ignites);
                }
            }

            assert last != null;

            IgniteCache<Integer, String> lastCache = last.cache(DEFAULT_CACHE_NAME);

            GridDhtCacheAdapter<Integer, String> dht = dht(lastCache);

            Affinity<Integer> aff = affinity(lastCache);

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
        }
        finally {
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
        Affinity<Integer> aff = affinity(cache);

        Ignite ignite = cache.unwrap(Ignite.class);

        ClusterNode loc = ignite.cluster().localNode();

        boolean sync = cache.getConfiguration(CacheConfiguration.class).getRebalanceMode() == SYNC;

        for (int i = 0; i < cnt; i++) {
            Collection<ClusterNode> nodes = ignite.cluster().nodes();

            Collection<ClusterNode> affNodes = aff.mapPartitionToPrimaryAndBackups(aff.partition(i));

            assert !affNodes.isEmpty();

            if (affNodes.contains(loc)) {
                String val = sync ? cache.localPeek(i) : cache.get(i);

                ClusterNode primaryNode = F.first(affNodes);

                assert primaryNode != null;

                boolean primary = primaryNode.equals(loc);

                assertEquals("Key check failed [igniteInstanceName=" + ignite.name() +
                        ", cache=" + cache.getName() + ", key=" + i + ", expected=" + i + ", actual=" + val +
                        ", part=" + aff.partition(i) + ", primary=" + primary + ", affNodes=" + U.nodeIds(affNodes) +
                        ", locId=" + loc.id() + ", allNodes=" + U.nodeIds(nodes) + ", allParts=" + top2string(grids) + ']',
                    Integer.toString(i), val);
            }
        }
    }

    /**
     * @param grids Grids
     * @return String representation of all partitions and their state.
     */
    private String top2string(Iterable<Ignite> grids) {
        Map<String, String> map = new HashMap<>();

        for (Ignite g : grids) {
            IgniteCache<Integer, String> c = g.cache(DEFAULT_CACHE_NAME);

            GridDhtCacheAdapter<Integer, String> dht = dht(c);

            GridDhtPartitionFullMap fullMap = dht.topology().partitionMap(false);

            map.put(g.name(), DEBUG ? fullMap.toFullString() : fullMap.toString());
        }

        return "Grid partition maps [" + map.toString() + ']';
    }
}
