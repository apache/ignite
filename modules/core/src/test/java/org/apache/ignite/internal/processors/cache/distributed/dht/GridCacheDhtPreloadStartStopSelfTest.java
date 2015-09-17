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

import java.util.Collection;
import java.util.LinkedList;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.configuration.CacheConfiguration.DFLT_REBALANCE_BATCH_SIZE;
import static org.apache.ignite.configuration.DeploymentMode.CONTINUOUS;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;

/**
 * Test cases for partitioned cache {@link GridDhtPreloader preloader}.
 */
public class GridCacheDhtPreloadStartStopSelfTest extends GridCommonAbstractTest {
    /** */
    private static final long TEST_TIMEOUT = 5 * 60 * 1000;

    /** Default backups. */
    private static final int DFLT_BACKUPS = 1;

    /** Partitions. */
    private static final int DFLT_PARTITIONS = 521;

    /** Preload batch size. */
    private static final int DFLT_BATCH_SIZE = DFLT_REBALANCE_BATCH_SIZE;

    /** Default cache count. */
    private static final int DFLT_CACHE_CNT = 10;

    /** Number of key backups. Each test method can set this value as required. */
    private int backups = DFLT_BACKUPS;

    /** Preload mode. */
    private CacheRebalanceMode preloadMode = ASYNC;

    /** */
    private int preloadBatchSize = DFLT_BATCH_SIZE;

    /** Number of partitions. */
    private int partitions = DFLT_PARTITIONS;

    /** */
    private int cacheCnt = DFLT_CACHE_CNT;

    /** IP finder. */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     *
     */
    public GridCacheDhtPreloadStartStopSelfTest() {
        super(false /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration[] cacheCfgs = new CacheConfiguration[cacheCnt];

        for (int i = 0; i < cacheCnt; i++) {
            CacheConfiguration cacheCfg = defaultCacheConfiguration();

            cacheCfg.setName("partitioned-" + i);

            cacheCfg.setCacheMode(PARTITIONED);
            cacheCfg.setRebalanceBatchSize(preloadBatchSize);
            cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            cacheCfg.setRebalanceMode(preloadMode);
            cacheCfg.setAffinity(new RendezvousAffinityFunction(false, partitions));
            cacheCfg.setBackups(backups);
            cacheCfg.setAtomicityMode(TRANSACTIONAL);

            cacheCfgs[i] = cacheCfg;
        }

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);
        cfg.setCacheConfiguration(cacheCfgs);
        cfg.setDeploymentMode(CONTINUOUS);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        backups = DFLT_BACKUPS;
        partitions = DFLT_PARTITIONS;
        preloadMode = ASYNC;
        preloadBatchSize = DFLT_BATCH_SIZE;
        cacheCnt = DFLT_CACHE_CNT;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /**
     * @param c Cache.
     * @return {@code True} if synchronoous preloading.
     */
    private boolean isSync(IgniteCache<?, ?> c) {
        return c.getConfiguration(CacheConfiguration.class).getRebalanceMode() == SYNC;
    }

    /**
     * @param cnt Number of grids.
     * @param startIdx Start node index.
     * @param list List of started grids.
     * @throws Exception If failed.
     */
    private void startGrids(int cnt, int startIdx, Collection<Ignite> list) throws Exception {
        for (int i = 0; i < cnt; i++)
            list.add(startGrid(startIdx++));
    }

    /** @param grids Grids to stop. */
    private void stopGrids(Iterable<Ignite> grids) {
        for (Ignite g : grids)
            stopGrid(g.name());
    }

    /** @throws Exception If failed. */
    public void testDeadlock() throws Exception {
        info("Testing deadlock...");

        Collection<Ignite> ignites = new LinkedList<>();

        int gridCnt = 3;

        startGrids(gridCnt, 1, ignites);

        info("Grids started: " + gridCnt);

        stopGrids(ignites);
    }

    /**
     * @param keyCnt Key count.
     * @param nodeCnt Node count.
     * @throws Exception If failed.
     */
    private void checkNodes(int keyCnt, int nodeCnt) throws Exception {
        try {
            Ignite g1 = startGrid(0);

            IgniteCache<Integer, String> c1 = g1.cache(null);

            putKeys(c1, keyCnt);
            checkKeys(c1, keyCnt);

            Collection<Ignite> ignites = new LinkedList<>();

            startGrids(nodeCnt, 1, ignites);

            // Check all nodes.
            for (Ignite g : ignites) {
                IgniteCache<Integer, String> c = g.cache(null);

                checkKeys(c, keyCnt);
            }

            info(">>> Finished checking nodes [keyCnt=" + keyCnt + ", nodeCnt=" + nodeCnt + ']');

            stopGrids(ignites);

            GridDhtCacheAdapter<Integer, String> dht = dht(c1);

            info(">>> Waiting for preload futures...");

            GridCachePartitionExchangeManager<Object, Object> exchMgr
                = ((IgniteKernal)g1).context().cache().context().exchange();

            // Wait for exchanges to complete.
            for (IgniteInternalFuture<?> fut : exchMgr.exchangeFutures())
                fut.get();

            Affinity<Integer> aff = affinity(c1);

            for (int i = 0; i < keyCnt; i++) {
                if (aff.mapPartitionToPrimaryAndBackups(aff.partition(i)).contains(g1.cluster().localNode())) {
                    GridDhtPartitionTopology top = dht.topology();

                    for (GridDhtLocalPartition p : top.localPartitions())
                        assertEquals("Invalid partition state for partition: " + p, OWNING, p.state());
                }
            }
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
     * @param c Cache.
     * @param cnt Key count.
     */
    private void checkKeys(IgniteCache<Integer, String> c, int cnt) {
        Affinity<Integer> aff = affinity(c);

        boolean sync = isSync(c);

        Ignite ignite = c.unwrap(Ignite.class);

        for (int i = 0; i < cnt; i++) {
            if (aff.mapPartitionToPrimaryAndBackups(aff.partition(i)).contains(ignite.cluster().localNode())) {
                String val = sync ? c.localPeek(i, CachePeekMode.ONHEAP) : c.get(i);

                assertEquals("Key check failed [grid=" + ignite.name() + ", cache=" + c.getName() + ", key=" + i + ']',
                    Integer.toString(i), val);
            }
        }
    }
}