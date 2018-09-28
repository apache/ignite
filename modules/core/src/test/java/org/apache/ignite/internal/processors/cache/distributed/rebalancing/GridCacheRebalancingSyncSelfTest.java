/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.lang.GridAbsPredicateX;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheRebalanceMode.NONE;

/**
 *
 */
public class GridCacheRebalancingSyncSelfTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int TEST_SIZE = 100_000;

    /** */
    private static final long TOPOLOGY_STILLNESS_TIME = 30_000L;

    /** partitioned cache name. */
    protected static final String CACHE_NAME_DHT_PARTITIONED = "cacheP";

    /** partitioned cache 2 name. */
    protected static final String CACHE_NAME_DHT_PARTITIONED_2 = "cacheP2";

    /** replicated cache name. */
    protected static final String CACHE_NAME_DHT_REPLICATED = "cacheR";

    /** replicated cache 2 name. */
    protected static final String CACHE_NAME_DHT_REPLICATED_2 = "cacheR2";

    /** */
    private volatile boolean concurrentStartFinished;

    /** */
    private volatile boolean concurrentStartFinished2;

    /** */
    private volatile boolean concurrentStartFinished3;

    /**
     * Time in milliseconds of last received {@link GridDhtPartitionsSingleMessage}
     * or {@link GridDhtPartitionsFullMessage} using {@link CollectingCommunicationSpi}.
     */
    private static volatile long lastPartMsgTime;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration iCfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)iCfg.getDiscoverySpi()).setIpFinder(ipFinder);
        ((TcpDiscoverySpi)iCfg.getDiscoverySpi()).setForceServerMode(true);

        TcpCommunicationSpi commSpi = new CollectingCommunicationSpi();

        commSpi.setLocalPort(GridTestUtils.getNextCommPort(getClass()));
        commSpi.setTcpNoDelay(true);

        iCfg.setCommunicationSpi(commSpi);

        if (getTestIgniteInstanceName(10).equals(igniteInstanceName))
            iCfg.setClientMode(true);

        CacheConfiguration<Integer, Integer> cachePCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cachePCfg.setName(CACHE_NAME_DHT_PARTITIONED);
        cachePCfg.setCacheMode(CacheMode.PARTITIONED);
        cachePCfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        cachePCfg.setBackups(1);
        cachePCfg.setRebalanceBatchSize(1);
        cachePCfg.setRebalanceBatchesPrefetchCount(1);
        cachePCfg.setRebalanceOrder(2);

        CacheConfiguration<Integer, Integer> cachePCfg2 = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cachePCfg2.setName(CACHE_NAME_DHT_PARTITIONED_2);
        cachePCfg2.setCacheMode(CacheMode.PARTITIONED);
        cachePCfg2.setRebalanceMode(CacheRebalanceMode.SYNC);
        cachePCfg2.setBackups(1);
        cachePCfg2.setRebalanceOrder(2);
        cachePCfg2.setRebalanceDelay(5000);

        CacheConfiguration<Integer, Integer> cacheRCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cacheRCfg.setName(CACHE_NAME_DHT_REPLICATED);
        cacheRCfg.setCacheMode(CacheMode.REPLICATED);
        cacheRCfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        cacheRCfg.setRebalanceBatchSize(1);
        cacheRCfg.setRebalanceBatchesPrefetchCount(Integer.MAX_VALUE);
        ((TcpCommunicationSpi)iCfg.getCommunicationSpi()).setSharedMemoryPort(-1);//Shmem fail fix for Integer.MAX_VALUE.

        CacheConfiguration<Integer, Integer> cacheRCfg2 = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cacheRCfg2.setName(CACHE_NAME_DHT_REPLICATED_2);
        cacheRCfg2.setCacheMode(CacheMode.REPLICATED);
        cacheRCfg2.setRebalanceMode(CacheRebalanceMode.SYNC);
        cacheRCfg2.setRebalanceOrder(4);

        iCfg.setCacheConfiguration(cachePCfg, cachePCfg2, cacheRCfg, cacheRCfg2);

        iCfg.setRebalanceThreadPoolSize(3);

        return iCfg;
    }

    /**
     * @param ignite Ignite.
     * @param from Start from key.
     * @param iter Iteration.
     */
    protected void generateData(Ignite ignite, int from, int iter) {
        generateData(ignite, CACHE_NAME_DHT_PARTITIONED, from, iter);
        generateData(ignite, CACHE_NAME_DHT_PARTITIONED_2, from, iter);
        generateData(ignite, CACHE_NAME_DHT_REPLICATED, from, iter);
        generateData(ignite, CACHE_NAME_DHT_REPLICATED_2, from, iter);
    }

    /**
     * @param ignite Ignite.
     * @param name Cache name.
     * @param from Start from key.
     * @param iter Iteration.
     */
    protected void generateData(Ignite ignite, String name, int from, int iter) {
        for (int i = from; i < from + TEST_SIZE; i++) {
            if ((i + 1) % (TEST_SIZE / 10) == 0)
                log.info("Prepared " + (i + 1) * 100 / (TEST_SIZE) + "% entries. [count=" + TEST_SIZE +
                    ", iteration=" + iter + ", cache=" + name + "]");

            ignite.cache(name).put(i, i + name.hashCode() + iter);
        }
    }

    /**
     * @param ignite Ignite.
     * @param from Start from key.
     * @param iter Iteration.
     */
    protected void checkData(Ignite ignite, int from, int iter) {
        checkData(ignite, CACHE_NAME_DHT_PARTITIONED, from, iter);
        checkData(ignite, CACHE_NAME_DHT_PARTITIONED_2, from, iter);
        checkData(ignite, CACHE_NAME_DHT_REPLICATED, from, iter);
        checkData(ignite, CACHE_NAME_DHT_REPLICATED_2, from, iter);
    }

    /**
     * @param ignite Ignite.
     * @param from Start from key.
     * @param iter Iteration.
     * @param name Cache name.
     */
    protected void checkData(Ignite ignite, String name, int from, int iter) {
        for (int i = from; i < from + TEST_SIZE; i++) {
            if ((i + 1) % (TEST_SIZE / 10) == 0)
                log.info("<" + name + "> Checked " + (i + 1) * 100 / (TEST_SIZE) + "% entries. [count=" + TEST_SIZE +
                    ", iteration=" + iter + ", cache=" + name + "]");

            assertEquals("Value does not match [key=" + i + ", cache=" + name + ']',
                ignite.cache(name).get(i), i + name.hashCode() + iter);
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        GridTestUtils.runGC(); // Clean heap before rebalancing.
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleRebalancing() throws Exception {
        IgniteKernal ignite = (IgniteKernal)startGrid(0);

        generateData(ignite, 0, 0);

        log.info("Preloading started.");

        long start = System.currentTimeMillis();

        startGrid(1);

        awaitPartitionMapExchange(true, true, null, true);

        checkPartitionMapExchangeFinished();

        awaitPartitionMessagesAbsent();

        stopGrid(0);

        awaitPartitionMapExchange(true, true, null, true);

        checkPartitionMapExchangeFinished();

        awaitPartitionMessagesAbsent();

        startGrid(2);

        awaitPartitionMapExchange(true, true, null, true);

        checkPartitionMapExchangeFinished();

        awaitPartitionMessagesAbsent();

        stopGrid(2);

        awaitPartitionMapExchange(true, true, null, true);

        checkPartitionMapExchangeFinished();

        awaitPartitionMessagesAbsent();

        long spend = (System.currentTimeMillis() - start) / 1000;

        checkData(grid(1), 0, 0);

        log.info("Spend " + spend + " seconds to rebalance entries.");
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadRebalancing() throws Exception {
        final Ignite ignite = startGrid(0);

        startGrid(1);

        generateData(ignite, CACHE_NAME_DHT_PARTITIONED, 0, 0);

        log.info("Preloading started.");

        long start = System.currentTimeMillis();

        concurrentStartFinished = false;

        Thread t1 = new Thread() {
            @Override public void run() {
                Random rdm = new Random();

                while (!concurrentStartFinished) {
                    for (int i = 0; i < TEST_SIZE; i++) {
                        if (i % (TEST_SIZE / 10) == 0)
                            log.info("Prepared " + i * 100 / (TEST_SIZE) + "% entries (" + TEST_SIZE + ").");

                        int ii = rdm.nextInt(TEST_SIZE);

                        ignite.cache(CACHE_NAME_DHT_PARTITIONED).put(ii, ii + CACHE_NAME_DHT_PARTITIONED.hashCode());
                    }
                }
            }
        };

        Thread t2 = new Thread() {
            @Override public void run() {
                while (!concurrentStartFinished)
                    checkData(ignite, CACHE_NAME_DHT_PARTITIONED, 0, 0);
            }
        };

        t1.start();
        t2.start();

        startGrid(2);
        startGrid(3);

        stopGrid(2);

        startGrid(4);

        awaitPartitionMapExchange(true, true, null);

        concurrentStartFinished = true;

        checkSupplyContextMapIsEmpty();

        t1.join();
        t2.join();

        long spend = (System.currentTimeMillis() - start) / 1000;

        info("Time to rebalance entries: " + spend);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    protected void checkSupplyContextMapIsEmpty() throws Exception {
        for (Ignite g : G.allGrids()) {
            for (GridCacheAdapter c : ((IgniteEx)g).context().cache().internalCaches()) {
                Object supplier = U.field(c.preloader(), "supplier");

                final Map map = U.field(supplier, "scMap");

                GridTestUtils.waitForCondition(new PA() {
                    @Override public boolean apply() {
                        synchronized (map) {
                            return map.isEmpty();
                        }
                    }
                }, 15_000);

                synchronized (map) {
                    assertTrue("Map is not empty [cache=" + c.name() +
                        ", node=" + g.name() +
                        ", map=" + map + ']', map.isEmpty());
                }
            }
        }
    }

    /**
     *
     */
    protected void checkPartitionMapExchangeFinished() {
        for (Ignite g : G.allGrids()) {
            IgniteKernal g0 = (IgniteKernal)g;

            for (IgniteCacheProxy<?, ?> c : g0.context().cache().jcaches()) {
                CacheConfiguration cfg = c.context().config();

                if (cfg.getCacheMode() != LOCAL && cfg.getRebalanceMode() != NONE) {
                    GridDhtCacheAdapter<?, ?> dht = dht(c);

                    GridDhtPartitionTopology top = dht.topology();

                    List<GridDhtLocalPartition> locs = top.localPartitions();

                    for (GridDhtLocalPartition loc : locs) {
                        GridDhtPartitionState actl = loc.state();

                        boolean res = GridDhtPartitionState.OWNING.equals(actl);

                        if (!res)
                            printPartitionState(c);

                        assertTrue("Wrong local partition state part=" +
                            loc.id() + ", should be OWNING [state=" + actl +
                            "], node=" + g0.name() + " cache=" + c.getName(), res);

                        Collection<ClusterNode> affNodes =
                            g0.affinity(cfg.getName()).mapPartitionToPrimaryAndBackups(loc.id());

                        assertTrue(affNodes.contains(g0.localNode()));
                    }

                    for (Ignite remote : G.allGrids()) {
                        IgniteKernal remote0 = (IgniteKernal)remote;

                        IgniteCacheProxy<?, ?> remoteC = remote0.context().cache().jcache(cfg.getName());

                        GridDhtCacheAdapter<?, ?> remoteDht = dht(remoteC);

                        GridDhtPartitionTopology remoteTop = remoteDht.topology();

                        GridDhtPartitionMap pMap = remoteTop.partitionMap(true).get(((IgniteKernal)g).getLocalNodeId());

                        assertEquals(pMap.size(), locs.size());

                        for (Map.Entry entry : pMap.entrySet()) {
                            assertTrue("Wrong remote partition state part=" + entry.getKey() +
                                    ", should be OWNING [state=" + entry.getValue() + "], node="
                                    + remote.name() + " cache=" + c.getName(),
                                entry.getValue() == GridDhtPartitionState.OWNING);
                        }

                        for (GridDhtLocalPartition loc : locs)
                            assertTrue(pMap.containsKey(loc.id()));
                    }
                }
            }
        }

        log.info("checkPartitionMapExchangeFinished finished");
    }

    /**
     * Method checks for {@link GridDhtPartitionsSingleMessage} or {@link GridDhtPartitionsFullMessage}
     * not received within {@code TOPOLOGY_STILLNESS_TIME} bound.
     *
     * @throws Exception If failed.
     */
    protected void awaitPartitionMessagesAbsent() throws Exception {
        log.info("Checking GridDhtPartitions*Message absent (it will take up to " +
            TOPOLOGY_STILLNESS_TIME + " ms) ... ");

        // Start waiting new messages from current point of time.
        lastPartMsgTime = U.currentTimeMillis();

        assertTrue("Should not have partition Single or Full messages within bound " +
                TOPOLOGY_STILLNESS_TIME + " ms.",
            GridTestUtils.waitForCondition(
                new GridAbsPredicateX() {
                    @Override public boolean applyx() {
                        return lastPartMsgTime + TOPOLOGY_STILLNESS_TIME < U.currentTimeMillis();
                    }
                },
                2 * TOPOLOGY_STILLNESS_TIME // 30 sec to gain stable topology and 30 sec of silence.
            )
        );
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testComplexRebalancing() throws Exception {
        final Ignite ignite = startGrid(0);

        generateData(ignite, 0, 0);

        log.info("Preloading started.");

        long start = System.currentTimeMillis();

        concurrentStartFinished = false;
        concurrentStartFinished2 = false;
        concurrentStartFinished3 = false;

        Thread t1 = new Thread() {
            @Override public void run() {
                try {
                    startGrid(1);
                    startGrid(2);

                    while (!concurrentStartFinished2)
                        U.sleep(10);

                    awaitPartitionMapExchange();

                    //New cache should start rebalancing.
                    CacheConfiguration<Integer, Integer> cacheRCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

                    cacheRCfg.setName(CACHE_NAME_DHT_PARTITIONED + "_NEW");
                    cacheRCfg.setCacheMode(CacheMode.PARTITIONED);
                    cacheRCfg.setRebalanceMode(CacheRebalanceMode.SYNC);
                    cacheRCfg.setRebalanceBatchesPrefetchCount(1);

                    grid(0).getOrCreateCache(cacheRCfg);

                    while (!concurrentStartFinished3)
                        U.sleep(10);

                    concurrentStartFinished = true;
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        Thread t2 = new Thread() {
            @Override public void run() {
                try {
                    startGrid(3);
                    startGrid(4);

                    concurrentStartFinished2 = true;
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        Thread t3 = new Thread() {
            @Override public void run() {
                generateData(ignite, 0, 1);

                concurrentStartFinished3 = true;
            }
        };

        t1.start();
        t2.start();// Should cancel t1 rebalancing.
        t3.start();

        t1.join();
        t2.join();
        t3.join();

        awaitPartitionMapExchange(true, true, null);

        checkSupplyContextMapIsEmpty();

        checkData(grid(4), 0, 1);

        final Ignite ignite3 = grid(3);

        Thread t4 = new Thread() {
            @Override public void run() {
                generateData(ignite3, 0, 2);

            }
        };

        t4.start();

        stopGrid(1);

        awaitPartitionMapExchange(true, true, null);

        checkSupplyContextMapIsEmpty();

        stopGrid(0);

        awaitPartitionMapExchange(true, true, null);

        checkSupplyContextMapIsEmpty();

        stopGrid(2);

        awaitPartitionMapExchange(true, true, null);

        checkPartitionMapExchangeFinished();

        awaitPartitionMessagesAbsent();

        checkSupplyContextMapIsEmpty();

        t4.join();

        stopGrid(3);

        awaitPartitionMapExchange();

        checkSupplyContextMapIsEmpty();

        long spend = (System.currentTimeMillis() - start) / 1000;

        checkData(grid(4), 0, 2);

        log.info("Spend " + spend + " seconds to rebalance entries.");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     *
     */
    private static class CollectingCommunicationSpi extends TcpCommunicationSpi {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void sendMessage(final ClusterNode node, final Message msg,
            final IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            final Object msg0 = ((GridIoMessage)msg).message();

            if (msg0 instanceof GridDhtPartitionsSingleMessage ||
                msg0 instanceof GridDhtPartitionsFullMessage) {
                lastPartMsgTime = U.currentTimeMillis();

                log.info("Last seen time of GridDhtPartitionsSingleMessage or GridDhtPartitionsFullMessage updated " +
                    "[lastPartMsgTime=" + lastPartMsgTime +
                    ", node=" + node.id() + ']');
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}
