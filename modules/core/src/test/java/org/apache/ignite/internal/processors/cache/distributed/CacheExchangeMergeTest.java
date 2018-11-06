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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.TestDelayingCommunicationSpi;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsAbstractMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleRequest;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.eclipse.jetty.util.ConcurrentHashSet;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_EXCHANGE_HISTORY_SIZE;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.testframework.GridTestUtils.mergeExchangeWaitVersion;

/**
 *
 */
public class CacheExchangeMergeTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final long WAIT_SECONDS = 15;

    /** */
    private ThreadLocal<Boolean> client = new ThreadLocal<>();

    /** */
    private boolean testSpi;

    /** */
    private boolean testDelaySpi;

    /** */
    private static String[] cacheNames = {"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"};

    /** */
    private boolean cfgCache = true;

    /** */
    private IgniteClosure<String, Boolean> clientC;

    /** */
    private static ExecutorService executor;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        if (testSpi)
            cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());
        else if (testDelaySpi)
            cfg.setCommunicationSpi(new TestDelayExchangeMessagesSpi());

        Boolean clientMode = client.get();

        if (clientMode == null && clientC != null)
            clientMode = clientC.apply(igniteInstanceName);

        if (clientMode != null) {
            cfg.setClientMode(clientMode);

            client.set(null);
        }

        if (cfgCache) {
            cfg.setCacheConfiguration(
                cacheConfiguration("c1", ATOMIC, PARTITIONED, 0),
                cacheConfiguration("c2", ATOMIC, PARTITIONED, 1),
                cacheConfiguration("c3", ATOMIC, PARTITIONED, 2),
                cacheConfiguration("c4", ATOMIC, PARTITIONED, 10),
                cacheConfiguration("c5", ATOMIC, REPLICATED, 0),
                cacheConfiguration("c6", TRANSACTIONAL, PARTITIONED, 0),
                cacheConfiguration("c7", TRANSACTIONAL, PARTITIONED, 1),
                cacheConfiguration("c8", TRANSACTIONAL, PARTITIONED, 2),
                cacheConfiguration("c9", TRANSACTIONAL, PARTITIONED, 10),
                cacheConfiguration("c10", TRANSACTIONAL, REPLICATED, 0)
            );
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        if (executor != null)
            executor.shutdown();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @param name Cache name.
     * @param atomicityMode Cache atomicity mode.
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String name,
        CacheAtomicityMode atomicityMode,
        CacheMode cacheMode,
        int backups)
    {
        CacheConfiguration ccfg = new CacheConfiguration(name);

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setCacheMode(cacheMode);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDelayExchangeMessages() throws Exception {
        testDelaySpi = true;

        System.setProperty(IgniteSystemProperties.IGNITE_EXCHANGE_MERGE_DELAY, "2000");

        try {
            final int srvs = 6;
            final int clients = 3;

            startGridsMultiThreaded(srvs);

            for (int i = 0; i < clients; i++) {
                client.set(true);

                startGrid(srvs + i);
            }

            final int initNodes = srvs + clients;

            final AtomicInteger stopIdx = new AtomicInteger();

            IgniteInternalFuture stopFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    Thread.sleep(ThreadLocalRandom.current().nextLong(500) + 1);

                    stopGrid(stopIdx.incrementAndGet());

                    return null;
                }
            }, 3, "stop-srv");

            final AtomicInteger startIdx = new AtomicInteger(initNodes);

            IgniteInternalFuture startFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int nodeIdx = startIdx.incrementAndGet();

                    if (rnd.nextInt(3) == 0) {
                        log.info("Start client: " + nodeIdx);

                        client.set(true);
                    }
                    else
                        log.info("Start server: " + nodeIdx);

                    startGrid(nodeIdx);

                    if (rnd.nextBoolean()) {
                        log.info("Stop started node: " + nodeIdx);

                        stopGrid(nodeIdx);
                    }

                    return null;
                }
            }, 5, "start-node");

            stopFut.get();
            startFut.get();

            checkCaches();
        }
        finally {
            System.clearProperty(IgniteSystemProperties.IGNITE_EXCHANGE_MERGE_DELAY);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMergeStartRandomClientsServers() throws Exception {
        for (int iter = 0; iter < 3; iter++) {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            final int srvs = rnd.nextInt(3) + 1;
            final int clients = rnd.nextInt(3);

            log.info("Iteration [iter=" + iter + ", srvs=" + srvs + ", clients=" + clients + ']');

            Ignite srv0 = startGrids(srvs);

            for (int i = 0; i < clients; i++) {
                client.set(true);

                startGrid(srvs + i);
            }

            final int threads = 8;

            final int initNodes = srvs + clients;

            mergeExchangeWaitVersion(srv0, initNodes + threads);

            final AtomicInteger idx = new AtomicInteger(initNodes);

            IgniteInternalFuture fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int nodeIdx = idx.incrementAndGet();

                    if (rnd.nextInt(3) == 0) {
                        log.info("Start client: " + nodeIdx);

                        client.set(true);
                    }
                    else
                        log.info("Start server: " + nodeIdx);

                    startGrid(nodeIdx);

                    return null;
                }
            }, threads, "test-thread");

            fut.get();

            checkCaches();

            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMergeStartStopRandomClientsServers() throws Exception {
        for (int iter = 0; iter < 3; iter++) {
            log.info("Iteration: " + iter);

            final int srvs = 5;
            final int clients = 5;

            Ignite srv0 = startGrids(srvs);

            for (int i = 0; i < clients; i++) {
                client.set(true);

                startGrid(srvs + i);
            }

            final int threads = 8;

            final int initNodes = srvs + clients;

            mergeExchangeWaitVersion(srv0, initNodes + threads);

            final AtomicInteger idx = new AtomicInteger(initNodes);

            final ConcurrentHashSet<Integer> stopNodes = new ConcurrentHashSet<>();

            IgniteInternalFuture fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    if (rnd.nextBoolean()) {
                        Integer stopIdx;

                        for (;;) {
                            stopIdx = rnd.nextInt(initNodes - 1) + 1;

                            if (stopNodes.add(stopIdx))
                                break;
                        }

                        log.info("Stop node: " + stopIdx);

                        stopGrid(getTestIgniteInstanceName(stopIdx), true, false);
                    }
                    else {
                        int nodeIdx = idx.incrementAndGet();

                        if (rnd.nextInt(5) == 0) {
                            log.info("Start client: " + nodeIdx);

                            client.set(true);
                        }
                        else
                            log.info("Start server: " + nodeIdx);

                        startGrid(nodeIdx);
                    }

                    return null;
                }
            }, threads, "test-thread");

            fut.get();

            checkCaches();

            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentStartServers() throws Exception {
        concurrentStart(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentStartServersAndClients() throws Exception {
        concurrentStart(true);
    }

    /**
     * @param withClients If {@code true} also starts client nodes.
     * @throws Exception If failed.
     */
    private void concurrentStart(final boolean withClients) throws Exception {
        for (int i = 0; i < 5; i++) {
            log.info("Iteration: " + i);

            startGrid(0);

            final AtomicInteger idx = new AtomicInteger(1);

            IgniteInternalFuture fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    if (withClients)
                        client.set(ThreadLocalRandom.current().nextBoolean());

                    int nodeIdx = idx.getAndIncrement();

                    Ignite node = startGrid(nodeIdx);

                    checkNodeCaches(node, nodeIdx * 1000, 1000);

                    return null;
                }
            }, 10, "start-node");

            fut.get();

            checkCaches();

            startGrid(1000);

            checkCaches();

            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMergeServerAndClientJoin1() throws Exception {
        final IgniteEx srv0 = startGrid(0);

        mergeExchangeWaitVersion(srv0, 3);

        IgniteInternalFuture<?> fut1 = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(1);

                return null;
            }
        }, 1, "start-srv");

        waitForExchangeStart(srv0, 2);

        IgniteInternalFuture<?> fut2 = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                client.set(true);

                startGrid(2);

                return null;
            }
        }, 1, "start-client");

        fut1.get();
        fut2.get();

        checkCaches();

        checkExchanges(srv0, 1, 3);
        checkExchanges(ignite(1), 3);
        checkExchanges(ignite(2), 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartCacheOnJoinAndJoinMerge_2_nodes() throws Exception {
        startCacheOnJoinAndJoinMerge1(2, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartCacheOnJoinAndJoinMerge_4_nodes() throws Exception {
        startCacheOnJoinAndJoinMerge1(4, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartCacheOnJoinAndJoinMerge_WithClients() throws Exception {
        startCacheOnJoinAndJoinMerge1(5, true);
    }

    /**
     * @param nodes Number of nodes to start.
     * @param withClients If {@code true} starts both servers and clients.
     * @throws Exception If failed.
     */
    private void startCacheOnJoinAndJoinMerge1(int nodes, boolean withClients) throws Exception {
        cfgCache = false;

        final IgniteEx srv0 = startGrid(0);

        mergeExchangeWaitVersion(srv0, nodes + 1);

        if (withClients) {
            clientC = new IgniteClosure<String, Boolean>() {
                @Override public Boolean apply(String nodeName) {
                    return getTestIgniteInstanceIndex(nodeName) % 2 == 0;
                }
            };
        }

        cfgCache = true;

        IgniteInternalFuture fut = startGridsAsync(srv0, 1, nodes);

        fut.get();

        checkCaches();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMergeAndHistoryCleanup() throws Exception {
        final int histSize = 5;

        String oldHistVal = System.getProperty(IGNITE_EXCHANGE_HISTORY_SIZE);

        System.setProperty(IGNITE_EXCHANGE_HISTORY_SIZE, String.valueOf(histSize));

        try {
            final Ignite srv0 = startGrid(0);

            int topVer = 1;

            for (int i = 0; i < 3; i++) {
                mergeExchangeWaitVersion(srv0, topVer + 3);

                startGridsAsync(srv0, topVer, 3).get();

                topVer += 3;
            }

            checkHistorySize(histSize);

            awaitPartitionMapExchange();

            checkHistorySize(histSize);

            mergeExchangeWaitVersion(srv0, topVer + 2);

            stopGrid(1);
            stopGrid(2);

            checkHistorySize(histSize);

            awaitPartitionMapExchange();

            checkHistorySize(histSize);
        }
        finally {
            if (oldHistVal != null)
                System.setProperty(IGNITE_EXCHANGE_HISTORY_SIZE, oldHistVal);
            else
                System.clearProperty(IGNITE_EXCHANGE_HISTORY_SIZE);
        }
    }

    /**
     * @param histSize History size.
     */
    private void checkHistorySize(int histSize) {
        List<Ignite> nodes = G.allGrids();

        assertTrue(nodes.size() > 0);

        for (Ignite node : nodes) {
            List<GridDhtPartitionsExchangeFuture> exchFuts =
                    ((IgniteEx)node).context().cache().context().exchange().exchangeFutures();

            assertTrue("Unexpected size: " + exchFuts.size(), exchFuts.size() > 0 && exchFuts.size() <= histSize);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartCacheOnJoinAndMergeWithFail() throws Exception {
        cfgCache = false;

        final Ignite srv0 = startGrids(2);

        mergeExchangeWaitVersion(srv0, 5);

        cfgCache = true;

        IgniteInternalFuture fut = startGridsAsync(srv0, 2, 2);

        stopGrid(1);

        fut.get();

        checkCaches();

        checkExchanges(srv0, 1, 2, 3, 5);
        checkExchanges(ignite(2), 3, 5);
        checkExchanges(ignite(3), 5);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartCacheOnJoinAndCoordinatorFailed1() throws Exception {
        cfgCache = false;

        final Ignite srv0 = startGrids(2);

        mergeExchangeWaitVersion(srv0, 5);

        cfgCache = true;

        IgniteInternalFuture fut = startGridsAsync(srv0, 2, 2);

        stopGrid(0);

        fut.get();

        checkCaches();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartCacheOnJoinAndCoordinatorFailed2() throws Exception {
        cfgCache = false;

        final Ignite srv0 = startGrid(0);

        mergeExchangeWaitVersion(srv0, 3);

        cfgCache = true;

        IgniteInternalFuture fut = startGridsAsync(srv0, 1, 2);

        stopGrid(0);

        fut.get();

        checkCaches();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMergeServersJoin1() throws Exception {
        IgniteEx srv0 = startGrid(0);

        mergeExchangeWaitVersion(srv0, 3);

        final AtomicInteger idx = new AtomicInteger(1);

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(idx.getAndIncrement());

                return null;
            }
        }, 2, "start-node");

        fut.get();

        checkCaches();

        checkExchanges(srv0, 1, 3);
        checkExchanges(ignite(1), 3);
        checkExchanges(ignite(2), 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMergeServerJoin1ClientsInTopology() throws Exception {
        IgniteEx srv0 = startGrid(0);

        client.set(true);

        startGrid(1);

        client.set(true);

        startGrid(2);

        mergeExchangeWaitVersion(srv0, 5);

        final AtomicInteger idx = new AtomicInteger(3);

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(idx.getAndIncrement());

                return null;
            }
        }, 2, "start-node");

        fut.get();

        checkCaches();

        checkExchanges(srv0, 1, 2, 3, 5);
        checkExchanges(ignite(1), 2, 3, 5);
        checkExchanges(ignite(2), 3, 5);
        checkExchanges(ignite(3), 5);
        checkExchanges(ignite(4), 5);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMergeAndNewCoordinator() throws Exception {
        final Ignite srv0 = startGrids(3);

        mergeExchangeWaitVersion(srv0, 6);

        IgniteInternalFuture fut = startGridsAsync(srv0, 3, 3);

        fut.get();

        checkCaches();

        stopGrid(0);

        checkCaches();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMergeServersFail1_1() throws Exception {
        mergeServersFail1(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMergeServersFail1_2() throws Exception {
        mergeServersFail1(true);
    }

    /**
     * @param waitRebalance Wait for rebalance end before start tested topology change.
     * @throws Exception If failed.
     */
    private void mergeServersFail1(boolean waitRebalance) throws Exception {
        final Ignite srv0 = startGrids(5);

        if (waitRebalance)
            awaitPartitionMapExchange();

        final List<DiscoveryEvent> mergedEvts = new ArrayList<>();

        mergeExchangeWaitVersion(srv0, 8, mergedEvts);

        UUID grid3Id = grid(3).localNode().id();
        UUID grid2Id = grid(2).localNode().id();

        stopGrid(getTestIgniteInstanceName(4), true, false);
        stopGrid(getTestIgniteInstanceName(3), true, false);
        stopGrid(getTestIgniteInstanceName(2), true, false);

        checkCaches();

        awaitPartitionMapExchange();

        assertTrue("Unexpected number of merged disco events: " + mergedEvts.size(), mergedEvts.size() == 2);

        for (DiscoveryEvent discoEvt : mergedEvts) {
            ClusterNode evtNode = discoEvt.eventNode();

            assertTrue("eventNode is null for DiscoEvent " + discoEvt, evtNode != null);

            assertTrue("Unexpected eventNode ID: "
                    + evtNode.id() + " while expecting " + grid2Id + " or " + grid3Id,
                evtNode.id().equals(grid2Id) || evtNode.id().equals(grid3Id));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMergeServersAndClientsFail1() throws Exception {
        mergeServersAndClientsFail(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMergeServersAndClientsFail2() throws Exception {
        mergeServersAndClientsFail(true);
    }


    /**
     * @param waitRebalance Wait for rebalance end before start tested topology change.
     * @throws Exception If failed.
     */
    private void mergeServersAndClientsFail(boolean waitRebalance) throws Exception {
        clientC = new IgniteClosure<String, Boolean>() {
            @Override public Boolean apply(String nodeName) {
                return nodeName.equals(getTestIgniteInstanceName(2)) || nodeName.equals(getTestIgniteInstanceName(3));
            }
        };

        final Ignite srv0 = startGrids(6);

        if (waitRebalance)
            awaitPartitionMapExchange();

        mergeExchangeWaitVersion(srv0, 10);

        stopGrid(getTestIgniteInstanceName(1), true, false);
        stopGrid(getTestIgniteInstanceName(2), true, false);
        stopGrid(getTestIgniteInstanceName(3), true, false);
        stopGrid(getTestIgniteInstanceName(4), true, false);

        checkAffinity();

        mergeExchangeWaitVersion(srv0, 12);

        IgniteInternalFuture fut = startGridsAsync(srv0, 6, 2);

        fut.get();

        checkCaches();
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinExchangeCoordinatorChange_NoMerge_1() throws Exception {
        for (CoordinatorChangeMode mode : CoordinatorChangeMode.values()) {
            exchangeCoordinatorChangeNoMerge(4, true, mode);

            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinExchangeCoordinatorChange_NoMerge_2() throws Exception {
        for (CoordinatorChangeMode mode : CoordinatorChangeMode.values()) {
            exchangeCoordinatorChangeNoMerge(8, true, mode);

            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailExchangeCoordinatorChange_NoMerge_1() throws Exception {
        for (CoordinatorChangeMode mode : CoordinatorChangeMode.values()) {
            exchangeCoordinatorChangeNoMerge(5, false, mode);

            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailExchangeCoordinatorChange_NoMerge_2() throws Exception {
        for (CoordinatorChangeMode mode : CoordinatorChangeMode.values()) {
            exchangeCoordinatorChangeNoMerge(8, false, mode);

            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMergeJoinExchangesCoordinatorChange1_4_servers() throws Exception {
        for (CoordinatorChangeMode mode : CoordinatorChangeMode.values()) {
            mergeJoinExchangesCoordinatorChange1(4, mode);

            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMergeJoinExchangesCoordinatorChange1_8_servers() throws Exception {
        for (CoordinatorChangeMode mode : CoordinatorChangeMode.values()) {
            mergeJoinExchangesCoordinatorChange1(8, mode);

            stopAllGrids();
        }
    }

    /**
     * @param srvs Number of server nodes.
     * @param mode Test mode.
     * @throws Exception If failed.
     */
    private void mergeJoinExchangesCoordinatorChange1(final int srvs, CoordinatorChangeMode mode)
        throws Exception
    {
        log.info("Test mergeJoinExchangesCoordinatorChange1 [srvs=" + srvs + ", mode=" + mode + ']');

        testSpi = true;

        Ignite srv0 = startGrids(srvs);

        mergeExchangeWaitVersion(srv0, 6);

        CountDownLatch latch = blockExchangeFinish(srvs, mode);

        IgniteInternalFuture<?> fut = startGridsAsync(srv0, srvs, 2);

        if (latch != null && !latch.await(WAIT_SECONDS, TimeUnit.SECONDS))
            fail("Failed to wait for expected messages.");

        stopGrid(getTestIgniteInstanceName(0), true, false);

        fut.get();

        checkCaches();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMergeJoinExchangesCoordinatorChange2_4_servers() throws Exception {
        mergeJoinExchangeCoordinatorChange2(4, 2, F.asList(1, 2, 3, 4), F.asList(5));

        stopAllGrids();

        mergeJoinExchangeCoordinatorChange2(4, 2, F.asList(1, 2, 3, 5), F.asList(4));
    }

    /**
     * @param srvs Number of server nodes.
     * @param startNodes Number of nodes to start.
     * @param blockNodes Nodes which do not receive messages.
     * @param waitMsgNodes Nodes which should receive messages.
     * @throws Exception If failed.
     */
    private void mergeJoinExchangeCoordinatorChange2(final int srvs,
        final int startNodes,
        List<Integer> blockNodes,
        List<Integer> waitMsgNodes) throws Exception
    {
        testSpi = true;

        Ignite srv0 = startGrids(srvs);

        mergeExchangeWaitVersion(srv0, srvs + startNodes);

        CountDownLatch latch = blockExchangeFinish(srv0, srvs + 1, blockNodes, waitMsgNodes);

        IgniteInternalFuture<?> fut = startGridsAsync(srv0, srvs, startNodes);

        if (latch != null && !latch.await(WAIT_SECONDS, TimeUnit.SECONDS))
            fail("Failed to wait for expected messages.");

        stopGrid(getTestIgniteInstanceName(0), true, false);

        fut.get();

        checkCaches();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMergeExchangeCoordinatorChange4() throws Exception {
        testSpi = true;

        final int srvs = 4;

        Ignite srv0 = startGrids(srvs);

        mergeExchangeWaitVersion(srv0, 6);

        final AtomicInteger idx = new AtomicInteger(srvs);

        CountDownLatch latch = blockExchangeFinish(srv0, 5, F.asList(1, 2, 3, 4), F.asList(5));

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(idx.getAndIncrement());

                return null;
            }
        }, 2, "start-node");

        if (latch != null && !latch.await(WAIT_SECONDS, TimeUnit.SECONDS))
            fail("Failed to wait for expected messages.");

        stopGrid(getTestIgniteInstanceName(0), true, false);

        fut.get();

        checkCaches();
    }

    /**
     * @param srvs Number of servers.
     * @param join If {@code true} starts new node, otherwise stops node.
     * @param mode Tested scenario.
     * @throws Exception If failed.
     */
    private void exchangeCoordinatorChangeNoMerge(int srvs, final boolean join, CoordinatorChangeMode mode) throws Exception {
        log.info("Test mergeJoinExchangeCoordinatorChange [nodes=" + srvs + ", mode=" + mode + ']');

        testSpi = true;

        final int nodes = srvs;

        startGrids(nodes);

        CountDownLatch latch = blockExchangeFinish(srvs, mode);

        IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable() {
            @Override public Object call() throws Exception {
                if (join)
                    startGrid(nodes);
                else
                    stopGrid(nodes - 1);

                return null;
            }
        });

        waitForExchangeStart(ignite(0), nodes + 1);

        if (latch != null && !latch.await(WAIT_SECONDS, TimeUnit.SECONDS))
            fail("Failed to wait for expected messages.");

        stopGrid(0);

        fut.get();

        checkCaches();
    }

    /**
     * @param srvs Number of server nodes.
     * @param mode Test scenario.
     * @return Awaited state latch.
     * @throws Exception If failed.
     */
    private CountDownLatch blockExchangeFinish(int srvs, CoordinatorChangeMode mode) throws Exception {
        Ignite crd = ignite(0);

        long topVer = srvs + 1;

        switch (mode) {
            case NOBODY_RCVD: {
                blockExchangeFinish(crd, topVer);

                break;
            }

            case NEW_CRD_RCDV: {
                List<Integer> finishNodes = F.asList(1);

                return blockExchangeFinish(crd, topVer, blockNodes(srvs, finishNodes), finishNodes);
            }

            case NON_CRD_RCVD: {
                assert srvs > 2 : srvs;

                List<Integer> finishNodes = F.asList(2);

                return blockExchangeFinish(crd, topVer, blockNodes(srvs, finishNodes), finishNodes);
            }

            default:
                fail();
        }

        return null;
    }

    /**
     * @param srvs Number of servers.
     * @param waitNodes Nodes which should receive message.
     * @return Blocked nodes indexes.
     */
    private List<Integer> blockNodes(int srvs, List<Integer> waitNodes) {
        List<Integer> block = new ArrayList<>();

        for (int i = 0; i < srvs + 1; i++) {
            if (!waitNodes.contains(i))
                block.add(i);
        }

        return block;
    }

    /**
     * @param crd Exchange coordinator.
     * @param topVer Exchange topology version.
     */
    private void blockExchangeFinish(Ignite crd, long topVer) {
        final AffinityTopologyVersion topVer0 = new AffinityTopologyVersion(topVer);

        TestRecordingCommunicationSpi.spi(crd).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                if (msg instanceof GridDhtPartitionsFullMessage) {
                    GridDhtPartitionsFullMessage msg0 = (GridDhtPartitionsFullMessage)msg;

                    return msg0.exchangeId() != null && msg0.exchangeId().topologyVersion().equals(topVer0);
                }

                return false;
            }
        });
    }

    /**
     * @param crd Exchange coordinator.
     * @param topVer Exchange topology version.
     * @param blockNodes Nodes which do not receive messages.
     * @param waitMsgNodes Nodes which should receive messages.
     * @return Awaited state latch.
     */
    private CountDownLatch blockExchangeFinish(Ignite crd,
        long topVer,
        final List<Integer> blockNodes,
        final List<Integer> waitMsgNodes)
    {
        log.info("blockExchangeFinish [crd=" + crd.cluster().localNode().id() +
            ", block=" + blockNodes +
            ", wait=" + waitMsgNodes + ']');

        final AffinityTopologyVersion topVer0 = new AffinityTopologyVersion(topVer);

        final CountDownLatch latch = new CountDownLatch(waitMsgNodes.size());

        TestRecordingCommunicationSpi.spi(crd).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                if (msg instanceof GridDhtPartitionsFullMessage) {
                    GridDhtPartitionsFullMessage msg0 = (GridDhtPartitionsFullMessage)msg;

                    if (msg0.exchangeId() == null || msg0.exchangeId().topologyVersion().compareTo(topVer0) < 0)
                        return false;

                    String name = node.attribute(IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME);

                    assert name != null : node;

                    for (Integer idx : blockNodes) {
                        if (name.equals(getTestIgniteInstanceName(idx)))
                            return true;
                    }

                    for (Integer idx : waitMsgNodes) {
                        if (name.equals(getTestIgniteInstanceName(idx))) {
                            log.info("Coordinators sends awaited message [node=" + node.id() + ']');

                            latch.countDown();
                        }
                    }
                }

                return false;
            }
        });

        return latch;
    }

    /**
     * @throws Exception If failed.
     */
    private void checkCaches() throws Exception {
        checkAffinity();

        checkCaches0();

        checkAffinity();

        awaitPartitionMapExchange();

        checkTopologiesConsistency();

        checkCaches0();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkCaches0() throws Exception {
        List<Ignite> nodes = G.allGrids();

        assertTrue(nodes.size() > 0);

        for (Ignite node : nodes)
            checkNodeCaches(node);
    }

    /**
     * Checks that after exchange all nodes have consistent state about partition owners.
     *
     * @throws Exception If failed.
     */
    private void checkTopologiesConsistency() throws Exception {
        List<Ignite> nodes = G.allGrids();

        IgniteEx crdNode = null;

        for (Ignite node : nodes) {
            ClusterNode locNode = node.cluster().localNode();

            if (crdNode == null || locNode.order() < crdNode.localNode().order())
                crdNode = (IgniteEx) node;
        }

        for (Ignite node : nodes) {
            IgniteEx node0 = (IgniteEx) node;

            if (node0.localNode().id().equals(crdNode.localNode().id()))
                continue;

            for (IgniteInternalCache cache : node0.context().cache().caches()) {
                int partitions = cache.context().affinity().partitions();
                for (int p = 0; p < partitions; p++) {
                    List<ClusterNode> crdOwners = crdNode.cachex(cache.name()).cache().context().topology().owners(p);

                    List<ClusterNode> owners = cache.context().topology().owners(p);

                    assertEquals(crdOwners, owners);
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void checkAffinity() throws Exception {
        List<Ignite> nodes = G.allGrids();

        ClusterNode crdNode = null;

        for (Ignite node : nodes) {
            ClusterNode locNode = node.cluster().localNode();

            if (crdNode == null || locNode.order() < crdNode.order())
                crdNode = locNode;
        }

        AffinityTopologyVersion topVer = ((IgniteKernal)grid(crdNode)).
            context().cache().context().exchange().readyAffinityVersion();

        Map<String, List<List<ClusterNode>>> affMap = new HashMap<>();

        for (Ignite node : nodes) {
            IgniteKernal node0 = (IgniteKernal)node;

            for (IgniteInternalCache cache : node0.context().cache().caches()) {
                List<List<ClusterNode>> aff = affMap.get(cache.name());
                List<List<ClusterNode>> aff0 = cache.context().affinity().assignments(topVer);

                if (aff != null)
                    assertEquals(aff, aff0);
                else
                    affMap.put(cache.name(), aff0);
            }
        }
    }

    /**
     * @param node Node.
     * @param startKey Start key.
     * @param keyRange Keys range.
     */
    private void checkNodeCaches(Ignite node, int startKey, int keyRange) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (String cacheName : cacheNames) {
            String err = "Invalid value [node=" + node.name() +
                ", client=" + node.configuration().isClientMode() +
                ", order=" + node.cluster().localNode().order() +
                ", cache=" + cacheName + ']';

            IgniteCache<Object, Object> cache = node.cache(cacheName);

            for (int i = 0; i < 10; i++) {
                Integer key = rnd.nextInt(keyRange) + startKey;

                cache.put(key, i);

                Object val = cache.get(key);

                assertEquals(err, i, val);
            }
        }
    }

    /**
     * @param node Node.
     * @throws Exception If failed.
     */
    private void checkNodeCaches(final Ignite node) throws Exception {
        log.info("Check node caches [node=" + node.name() + ']');

        List<Future<?>> futs = new ArrayList<>();

        for (final String cacheName : cacheNames) {
            final IgniteCache<Object, Object> cache = node.cache(cacheName);

            futs.add(executor.submit(new Runnable() {
                @Override public void run() {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    assertNotNull("No cache [node=" + node.name() +
                            ", client=" + node.configuration().isClientMode() +
                            ", order=" + node.cluster().localNode().order() +
                            ", cache=" + cacheName + ']', cache);

                    String err = "Invalid value [node=" + node.name() +
                            ", client=" + node.configuration().isClientMode() +
                            ", order=" + node.cluster().localNode().order() +
                            ", cache=" + cacheName + ']';

                    for (int i = 0; i < 5; i++) {
                        Integer key = rnd.nextInt(20_000);

                        cache.put(key, i);

                        Object val = cache.get(key);

                        assertEquals(err, i, val);
                    }

                    for (int i = 0; i < 5; i++) {
                        Map<Integer, Integer> map = new TreeMap<>();

                        for (int j = 0; j < 10; j++) {
                            Integer key = rnd.nextInt(20_000);

                            map.put(key, i);
                        }

                        cache.putAll(map);

                        Map<Object, Object> res = cache.getAll(map.keySet());

                        for (Map.Entry<Integer, Integer> e : map.entrySet())
                            assertEquals(err, e.getValue(), res.get(e.getKey()));
                    }

                    if (cache.getConfiguration(CacheConfiguration.class).getAtomicityMode() == TRANSACTIONAL) {
                        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                            for (TransactionIsolation isolation : TransactionIsolation.values())
                                checkNodeCaches(err, node, cache, concurrency, isolation);
                        }
                    }
                }
            }));
        }

        for (Future<?> fut : futs)
            fut.get();
    }

    /**
     * @param err Error message.
     * @param node Node.
     * @param cache Cache.
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     */
    private void checkNodeCaches(
        String err,
        Ignite node,
        IgniteCache<Object, Object> cache,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        Map<Object, Object> map = new HashMap<>();

        try {
            try (Transaction tx = node.transactions().txStart(concurrency, isolation)) {
                for (int i = 0; i < 5; i++) {
                    Integer key = rnd.nextInt(20_000);

                    cache.put(key, i);

                    Object val = cache.get(key);

                    assertEquals(i, val);

                    map.put(key, val);
                }

                tx.commit();
            }
        }
        catch (ClusterTopologyException e) {
            info("Tx failed, ignore: " + e);

            return;
        }

        for (Map.Entry<Object, Object> e : map.entrySet())
            assertEquals(err, e.getValue(), cache.get(e.getKey()));
    }

    /**
     * @param node Node.
     * @param vers Expected exchange versions.
     */
    private void checkExchanges(Ignite node, long... vers) {
        IgniteKernal node0 = (IgniteKernal)node;

        List<AffinityTopologyVersion> expVers = new ArrayList<>();

        for (long ver : vers)
            expVers.add(new AffinityTopologyVersion(ver));

        List<AffinityTopologyVersion> doneVers = new ArrayList<>();

        List<GridDhtPartitionsExchangeFuture> futs =
            node0.context().cache().context().exchange().exchangeFutures();

        for (int i = futs.size() - 1; i >= 0; i--) {
            GridDhtPartitionsExchangeFuture fut = futs.get(i);

            if (fut.exchangeDone() && fut.firstEvent().type() != EVT_DISCOVERY_CUSTOM_EVT) {
                AffinityTopologyVersion resVer = fut.topologyVersion();

                if (resVer != null)
                    doneVers.add(resVer);
            }
        }

        assertEquals(expVers, doneVers);

        for (CacheGroupContext grpCtx : node0.context().cache().cacheGroups()) {
            for (AffinityTopologyVersion ver : grpCtx.affinity().cachedVersions()) {
                if (ver.minorTopologyVersion() > 0)
                    continue;

                assertTrue("Unexpected version [ver=" + ver + ", exp=" + expVers + ']',
                    expVers.contains(ver));
            }
        }
    }

    /**
     * @param node Node.
     * @param topVer Exchange version.
     * @throws Exception If failed.
     */
    private void waitForExchangeStart(final Ignite node, final long topVer) throws Exception {
        final GridCachePartitionExchangeManager exch = ((IgniteKernal)node).context().cache().context().exchange();

        boolean wait = GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return exch.lastTopologyFuture().initialVersion().topologyVersion() >= topVer;
            }
        }, 15_000);

        assertTrue(wait);
    }

    /**
     * Sequentially starts nodes so that node name is consistent with node order.
     *
     * @param node Some existing node.
     * @param startIdx Start node index.
     * @param cnt Number of nodes.
     * @return Start future.
     * @throws Exception If failed.
     */
    private IgniteInternalFuture startGridsAsync(Ignite node, int startIdx, int cnt) throws Exception {
        GridCompoundFuture fut = new GridCompoundFuture();

        for (int i = 0; i < cnt; i++) {
            final CountDownLatch latch = new CountDownLatch(1);

            node.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    log.info("Got event: " + ((DiscoveryEvent)evt).eventNode().id());

                    latch.countDown();

                    return false;
                }
            }, EventType.EVT_NODE_JOINED);

            final int nodeIdx = startIdx + i;

            IgniteInternalFuture fut0 = GridTestUtils.runAsync(new Callable() {
                @Override public Object call() throws Exception {
                    log.info("Start new node: " + nodeIdx);

                    startGrid(nodeIdx);

                    return null;
                }
            }, "start-node-" + nodeIdx);

            if (!latch.await(WAIT_SECONDS, TimeUnit.SECONDS))
                fail();

            fut.add(fut0);
        }

        fut.markInitialized();

        return fut;
    }

    /**
     *
     */
    enum CoordinatorChangeMode {
        /**
         * Coordinator failed, did not send full message.
         */
        NOBODY_RCVD,

        /**
         * Coordinator failed, but new coordinator received full message
         * and finished exchange.
         */
        NEW_CRD_RCDV,

        /**
         * Coordinator failed, but one of servers (not new coordinator) received full message.
         */
        NON_CRD_RCVD
    }

    /**
     *
     */

    static class TestDelayExchangeMessagesSpi extends TestDelayingCommunicationSpi {
        /** {@inheritDoc} */
        @Override protected boolean delayMessage(Message msg, GridIoMessage ioMsg) {
            if (msg instanceof GridDhtPartitionsAbstractMessage)
                return ((GridDhtPartitionsAbstractMessage)msg).exchangeId() != null || (msg instanceof GridDhtPartitionsSingleRequest);

            return false;
        }
    }
}
