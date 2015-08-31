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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.NONE;
import static org.apache.ignite.configuration.DeploymentMode.CONTINUOUS;
import static org.apache.ignite.events.EventType.EVTS_CACHE_REBALANCE;

/**
 * Test cases for partitioned cache {@link GridDhtPreloader preloader}.
 */
public class GridCacheDhtPreloadDisabledSelfTest extends GridCommonAbstractTest {
    /** Flat to print preloading events. */
    private static final boolean DEBUG = false;

    /** */
    private static final long TEST_TIMEOUT = 5 * 60 * 1000;

    /** Default backups. */
    private static final int DFLT_BACKUPS = 1;

    /** Partitions. */
    private static final int DFLT_PARTITIONS = 521;

    /** Number of key backups. Each test method can set this value as required. */
    private int backups = DFLT_BACKUPS;

    /** Number of partitions. */
    private int partitions = DFLT_PARTITIONS;

    /** IP finder. */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     *
     */
    public GridCacheDhtPreloadDisabledSelfTest() {
        super(false /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC);
        cacheCfg.setRebalanceMode(NONE);
        cacheCfg.setAffinity(new RendezvousAffinityFunction(false, partitions));
        cacheCfg.setBackups(backups);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        //cacheCfg.setRebalanceThreadPoolSize(1);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);
        cfg.setCacheConfiguration(cacheCfg);
        cfg.setDeploymentMode(CONTINUOUS);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        backups = DFLT_BACKUPS;
        partitions = DFLT_PARTITIONS;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /**
     * @param i Grid index.
     * @return Topology.
     */
    private GridDhtPartitionTopology topology(int i) {
        return near(grid(i).cache(null)).dht().topology();
    }

    /** @throws Exception If failed. */
    public void testSamePartitionMap() throws Exception {
        backups = 1;
        partitions = 10;

        int nodeCnt = 4;

        startGridsMultiThreaded(nodeCnt);

        try {
            for (int p = 0; p < partitions; p++) {
                List<Collection<ClusterNode>> mappings = new ArrayList<>(nodeCnt);

                for (int i = 0; i < nodeCnt; i++) {
                    Collection<ClusterNode> nodes = topology(i).nodes(p, AffinityTopologyVersion.NONE);
                    List<ClusterNode> owners = topology(i).owners(p);

                    int size = backups + 1;

                    assert owners.size() == size : "Size mismatch [nodeIdx=" + i + ", p=" + p + ", size=" + size +
                        ", owners=" + F.nodeIds(owners) + ']';
                    assert nodes.size() == size : "Size mismatch [nodeIdx=" + i + ", p=" + p + ", size=" + size +
                        ", nodes=" + F.nodeIds(nodes) + ']';

                    assert F.eqNotOrdered(nodes, owners);
                    assert F.eqNotOrdered(owners, nodes);

                    mappings.add(owners);
                }

                for (int i = 0; i < mappings.size(); i++) {
                    Collection<ClusterNode> m1 = mappings.get(i);

                    for (int j = 0; j != i && j < mappings.size(); j++) {
                        Collection<ClusterNode> m2 = mappings.get(j);

                        assert F.eqNotOrdered(m1, m2) : "Mappings are not equal [m1=" + F.nodeIds(m1) + ", m2=" +
                            F.nodeIds(m2) + ']';
                        assert F.eqNotOrdered(m2, m1) : "Mappings are not equal [m1=" + F.nodeIds(m1) + ", m2=" +
                            F.nodeIds(m2) + ']';
                    }
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testDisabledPreloader() throws Exception {
        try {
            Ignite ignite1 = startGrid(0);

            IgniteCache<Integer, String> cache1 = ignite1.cache(null);

            int keyCnt = 10;

            putKeys(cache1, keyCnt);

            for (int i = 0; i < keyCnt; i++) {
                assertNull(near(cache1).peekEx(i));
                assertNotNull((dht(cache1).peekEx(i)));

                assertEquals(Integer.toString(i), cache1.localPeek(i, CachePeekMode.ONHEAP));
            }

            int nodeCnt = 3;

            Collection<Ignite> ignites = new ArrayList<>(nodeCnt);

            startGrids(nodeCnt, 1, ignites);

            // Check all nodes.
            for (Ignite g : ignites) {
                IgniteCache<Integer, String> c = g.cache(null);

                for (int i = 0; i < keyCnt; i++)
                    assertNull(c.localPeek(i, CachePeekMode.ONHEAP));
            }

            Collection<Integer> keys = new LinkedList<>();

            for (int i = 0; i < keyCnt; i++)
                if (ignite1.affinity(null).mapKeyToNode(i).equals(ignite1.cluster().localNode()))
                    keys.add(i);

            info(">>> Finished checking nodes [keyCnt=" + keyCnt + ", nodeCnt=" + nodeCnt + ", grids=" +
                U.grids2names(ignites) + ']');

            for (Iterator<Ignite> it = ignites.iterator(); it.hasNext(); ) {
                Ignite g = it.next();

                it.remove();

                stopGrid(g.name());

                // Check all nodes.
                for (Ignite gg : ignites) {
                    IgniteCache<Integer, String> c = gg.cache(null);

                    for (int i = 0; i < keyCnt; i++)
                        assertNull(c.localPeek(i, CachePeekMode.ONHEAP));
                }
            }

            for (Integer i : keys)
                assertEquals(i.toString(), cache1.localPeek(i, CachePeekMode.ONHEAP));
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

    /** @param grids Grids to stop. */
    private void stopGrids(Iterable<Ignite> grids) {
        for (Ignite g : grids)
            stopGrid(g.name());
    }

    /**
     * @param c Cache.
     * @param cnt Key count.
     */
    private void putKeys(Cache<Integer, String> c, int cnt) {
        for (int i = 0; i < cnt; i++)
            c.put(i, Integer.toString(i));
    }
}