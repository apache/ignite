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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteClientReconnectAbstractTest;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteClusterReadOnlyException;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.cluster.ClusterState.lesserOf;
import static org.apache.ignite.testframework.GridTestUtils.assertActive;
import static org.apache.ignite.testframework.GridTestUtils.assertInactive;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 *
 */
public class IgniteClusterActivateDeactivateTest extends GridCommonAbstractTest {
    /** */
    static final String CACHE_NAME_PREFIX = "cache-";

    /** Non-persistent data region name. */
    private static final String NO_PERSISTENCE_REGION = "no-persistence-region";

    /** */
    private static final int DEFAULT_CACHES_COUNT = 2;

    /** */
    private ClusterState stateOnStart;

    /** */
    CacheConfiguration[] ccfgs;

    /** */
    private boolean testSpi;

    /** */
    private boolean testReconnectSpi;

    /** */
    private Class[] testSpiRecord;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (testReconnectSpi) {
            TcpDiscoverySpi spi = new IgniteClientReconnectAbstractTest.TestTcpDiscoverySpi();

            cfg.setDiscoverySpi(spi);

            spi.setJoinTimeout(2 * 60_000);
        }

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(sharedStaticIpFinder);

        cfg.setConsistentId(igniteInstanceName);

        if (stateOnStart != null)
            cfg.setClusterStateOnStart(stateOnStart);

        if (ccfgs != null) {
            cfg.setCacheConfiguration(ccfgs);

            ccfgs = null;
        }

        DataStorageConfiguration memCfg = new DataStorageConfiguration();
        memCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setMaxSize(150L * 1024 * 1024)
            .setPersistenceEnabled(persistenceEnabled()));
        memCfg.setWalSegments(2);
        memCfg.setWalSegmentSize(512 * 1024);

        memCfg.setDataRegionConfigurations(new DataRegionConfiguration()
            .setMaxSize(150L * 1024 * 1024)
            .setName(NO_PERSISTENCE_REGION)
            .setPersistenceEnabled(false));

        if (persistenceEnabled())
            memCfg.setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);
        cfg.setFailureDetectionTimeout(60_000);

        if (testSpi) {
            TestRecordingCommunicationSpi spi = new TestRecordingCommunicationSpi();

            if (testSpiRecord != null)
                spi.record(testSpiRecord);

            cfg.setCommunicationSpi(spi);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @return {@code True} if test with persistence.
     */
    protected boolean persistenceEnabled() {
        return false;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEnableReadOnlyFromActivateSimple_SingleNode() throws Exception {
        changeActiveClusterStateSimple(1, 0, 0, ACTIVE, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEnableReadOnlyFromActivateSimple_5_Servers() throws Exception {
        changeActiveClusterStateSimple(5, 0, 0, ACTIVE, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEnableReadOnlyFromActivateSimple_5_Servers2() throws Exception {
        changeActiveClusterStateSimple(5, 0, 4, ACTIVE, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEnableReadOnlyFromActivateSimple_5_Servers_5_Clients() throws Exception {
        changeActiveClusterStateSimple(5, 4, 0, ACTIVE, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEnableReadOnlyFromActivateSimple_5_Servers_5_Clients_FromClient() throws Exception {
        changeActiveClusterStateSimple(5, 4, 6, ACTIVE, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDisableReadOnlyFromActivateSimple_SingleNode() throws Exception {
        changeActiveClusterStateSimple(1, 0, 0, ACTIVE_READ_ONLY, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDisableReadOnlyFromActivateSimple_5_Servers() throws Exception {
        changeActiveClusterStateSimple(5, 0, 0, ACTIVE_READ_ONLY, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDisableReadOnlyFromActivateSimple_5_Servers2() throws Exception {
        changeActiveClusterStateSimple(5, 0, 4, ACTIVE_READ_ONLY, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDisableReadOnlyFromActivateSimple_5_Servers_5_Clients() throws Exception {
        changeActiveClusterStateSimple(5, 4, 0, ACTIVE_READ_ONLY, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDisableReadOnlyFromActivateSimple_5_Servers_5_Clients_FromClient() throws Exception {
        changeActiveClusterStateSimple(5, 4, 6, ACTIVE_READ_ONLY, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateSimple_SingleNode() throws Exception {
        activateSimple(1, 0, 0, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateInReadOnlySimple_SingleNode() throws Exception {
        activateSimple(1, 0, 0, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateSimple_5_Servers() throws Exception {
        activateSimple(5, 0, 0, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateInReadOnlySimple_5_Servers() throws Exception {
        activateSimple(5, 0, 0, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateSimple_5_Servers2() throws Exception {
        activateSimple(5, 0, 4, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateInReadOnlySimple_5_Servers2() throws Exception {
        activateSimple(5, 0, 4, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateSimple_5_Servers_5_Clients() throws Exception {
        activateSimple(5, 4, 0, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateInReadOnlySimple_5_Servers_5_Clients() throws Exception {
        activateSimple(5, 4, 0, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateSimple_5_Servers_5_Clients_FromClient() throws Exception {
        activateSimple(5, 4, 6, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateInReadOnlySimple_5_Servers_5_Clients_FromClient() throws Exception {
        activateSimple(5, 4, 6, ACTIVE_READ_ONLY);
    }

    /**
     * @param srvs Number of servers.
     * @param clients Number of clients.
     * @param changeFrom Index of node stating activation.
     * @param state Activation state.
     * @throws Exception If failed.
     */
    private void activateSimple(int srvs, int clients, int changeFrom, ClusterState state) throws Exception {
        assertActive(state);

        changeStateSimple(srvs, clients, changeFrom, INACTIVE, state);
    }

    /**
     * @param srvs Number of servers.
     * @param clients Number of clients.
     * @param changeFrom Index of node stating deactivation.
     * @param initialState Initial cluster state.
     * @throws Exception If failed.
     */
    private void deactivateSimple(int srvs, int clients, int changeFrom, ClusterState initialState) throws Exception {
        assertActive(initialState);

        changeStateSimple(srvs, clients, changeFrom, initialState, INACTIVE);
    }

    /**
     * @param srvs Number of servers.
     * @param clients Number of clients.
     * @param deactivateFrom Index of node stating deactivation.
     * @param initialState Initial cluster state.
     * @param targetState Targer cluster state.
     * @throws Exception If failed.
     */
    private void changeActiveClusterStateSimple(
        int srvs,
        int clients,
        int deactivateFrom,
        ClusterState initialState,
        ClusterState targetState
    ) throws Exception {
        assertActive(initialState);
        assertActive(targetState);

        assertNotSame(initialState, targetState);

        changeStateSimple(srvs, clients, deactivateFrom, initialState, targetState);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReActivateSimple_5_Servers_4_Clients_FromClient() throws Exception {
        reactivateSimple(5, 4, 6, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReActivateInReadOnlySimple_5_Servers_4_Clients_FromClient() throws Exception {
        reactivateSimple(5, 4, 6, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReActivateSimple_5_Servers_4_Clients_FromServer() throws Exception {
        reactivateSimple(5, 4, 0, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReActivateInReadOnlySimple_5_Servers_4_Clients_FromServer() throws Exception {
        reactivateSimple(5, 4, 0, ACTIVE_READ_ONLY);
    }

    /**
     * @param srvs Number of servers.
     * @param clients Number of clients.
     * @param activateFrom Index of node stating activation.
     * @param state Activation state.
     * @throws Exception If failed.
     */
    private void reactivateSimple(int srvs, int clients, int activateFrom, ClusterState state) throws Exception {
        activateSimple(srvs, clients, activateFrom, state);

        if (state == ACTIVE)
            rolloverSegmentAtLeastTwice(activateFrom);

        for (int i = 0; i < srvs + clients; i++)
            checkCachesOnNode(i, DEFAULT_CACHES_COUNT);

        ignite(activateFrom).cluster().state(INACTIVE);
        ignite(activateFrom).cluster().state(state);

        if (state == ACTIVE)
            rolloverSegmentAtLeastTwice(activateFrom);

        for (int i = 0; i < srvs + clients; i++)
            checkCachesOnNode(i, DEFAULT_CACHES_COUNT);
    }

    /**
     * Work directory have 2 segments by default. This method do full circle.
     */
    private void rolloverSegmentAtLeastTwice(int activateFrom) {
        for (int c = 0; c < DEFAULT_CACHES_COUNT; c++) {
            IgniteCache<Object, Object> cache = ignite(activateFrom).cache(CACHE_NAME_PREFIX + c);

            //this should be enough including free-,meta- page and etc.
            for (int i = 0; i < 1000; i++)
                cache.put(i, i);
        }
    }

    /**
     * @param nodes Number of nodes.
     * @param caches Number of caches.
     */
    final void checkCaches(int nodes, int caches) throws InterruptedException {
        checkCaches(nodes, caches, true);
    }

    /**
     * @param nodes Number of nodes.
     * @param caches Number of caches.
     */
    final void checkCaches(int nodes, int caches, boolean awaitExchange) throws InterruptedException {
        if (awaitExchange)
            awaitPartitionMapExchange();

        ClusterState state = ignite(0).cluster().state();

        assertActive(state);

        for (int i = 0; i < nodes; i++) {
            for (int c = 0; c < caches; c++) {
                IgniteCache<Integer, Integer> cache = ignite(i).cache(CACHE_NAME_PREFIX + c);

                for (int j = 0; j < 10; j++) {
                    Integer key = ThreadLocalRandom.current().nextInt(1000);
                    Integer value = j;

                    if (state == ACTIVE)
                        cache.put(key, j);
                    else
                        assertThrowsWithCause(() -> cache.put(key, value), IgniteClusterReadOnlyException.class);

                    assertEquals(state == ACTIVE ? value : null, cache.get(key));
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWhileActivate1_Server() throws Exception {
        joinWhileActivate1(false, false, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWhileActivateInReadOnly1_Server() throws Exception {
        joinWhileActivate1(false, false, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWhileActivate1_WithCache_Server() throws Exception {
        joinWhileActivate1(false, true, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWhileActivateInReadOnly1_WithCache_Server() throws Exception {
        joinWhileActivate1(false, true, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWhileActivate1_Client() throws Exception {
        joinWhileActivate1(true, false, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWhileActivateInReadOnly1_Client() throws Exception {
        joinWhileActivate1(true, false, ACTIVE_READ_ONLY);
    }

    /**
     * @param srvs Number of servers.
     * @param clients Number of clients.
     * @param stateChangeFrom Index of node initiating changes.
     * @param initialState Cluster state on start nodes.
     * @param targetState State of started cluster.
     * @param blockMsgNodes Nodes whcis block exchange messages.
     * @return State change future.
     * @throws Exception If failed.
     */
    private IgniteInternalFuture<?> startNodesAndBlockStatusChange(
        int srvs,
        int clients,
        final int stateChangeFrom,
        final ClusterState initialState,
        final ClusterState targetState,
        int... blockMsgNodes
    ) throws Exception {
        assertNotSame(initialState, targetState);

        if (!persistenceEnabled())
            stateOnStart = initialState;

        testSpi = true;

        startWithCaches1(srvs, clients);

        AffinityTopologyVersion affTopVer = new AffinityTopologyVersion(srvs + clients);

        if (ClusterState.active(initialState)) {
            ignite(0).cluster().state(initialState);

            awaitPartitionMapExchange();

            affTopVer = grid(0).cachex(CU.UTILITY_CACHE_NAME).context().topology().readyTopologyVersion();

            assertEquals(srvs + clients, affTopVer.topologyVersion());
        }

        if (blockMsgNodes.length == 0)
            blockMsgNodes = new int[] {1};

        final AffinityTopologyVersion STATE_CHANGE_TOP_VER = affTopVer.nextMinorVersion();

        List<TestRecordingCommunicationSpi> spis = new ArrayList<>();

        for (int idx : blockMsgNodes) {
            TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(ignite(idx));

            spis.add(spi);

            blockExchangeSingleMessage(spi, STATE_CHANGE_TOP_VER);
        }

        IgniteInternalFuture<?> stateChangeFut = runAsync(() -> ignite(stateChangeFrom).cluster().state(targetState));

        for (TestRecordingCommunicationSpi spi : spis)
            spi.waitForBlocked();

        U.sleep(500);

        assertFalse(stateChangeFut.isDone());

        return stateChangeFut;
    }

    /**
     * @param spi SPI.
     * @param topVer Exchange topology version.
     */
    private void blockExchangeSingleMessage(TestRecordingCommunicationSpi spi, final AffinityTopologyVersion topVer) {
        spi.blockMessages((IgniteBiPredicate<ClusterNode, Message>)(clusterNode, msg) -> {
            if (msg instanceof GridDhtPartitionsSingleMessage) {
                GridDhtPartitionsSingleMessage pMsg = (GridDhtPartitionsSingleMessage)msg;

                if (pMsg.exchangeId() != null && pMsg.exchangeId().topologyVersion().equals(topVer))
                    return true;
            }

            return false;
        });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWhileDeactivate1_Server() throws Exception {
        joinWhileDeactivate1(false, false, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWhileDeactivateFromReadOnly1_Server() throws Exception {
        joinWhileDeactivate1(false, false, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWhileDeactivate1_WithCache_Server() throws Exception {
        joinWhileDeactivate1(false, true, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWhileDeactivateFromReadOnly1_WithCache_Server() throws Exception {
        joinWhileDeactivate1(false, true, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWhileDeactivate1_Client() throws Exception {
        joinWhileDeactivate1(true, false, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWhileDeactivateFromReadOnly1_Client() throws Exception {
        joinWhileDeactivate1(true, false, ACTIVE_READ_ONLY);
    }

    /**
     * @param startClient If {@code true} joins client node, otherwise server.
     * @param withNewCache If {@code true} joining node has new cache in configuration.
     * @param state Target cluster state.
     * @throws Exception If failed.
     */
    private void joinWhileActivate1(boolean startClient, boolean withNewCache, ClusterState state) throws Exception {
        joinWhileClusterStateChange(startClient, withNewCache, INACTIVE, state);
    }

    /**
     * @param startClient If {@code true} joins client node, otherwise server.
     * @param withNewCache If {@code true} joining node has new cache in configuration.
     * @param state Initial cluster state.
     * @throws Exception If failed.
     */
    private void joinWhileDeactivate1(boolean startClient, boolean withNewCache, ClusterState state) throws Exception {
        joinWhileClusterStateChange(startClient, withNewCache, state, INACTIVE);
    }

    /**
     * @param startClient If {@code true} joins client node, otherwise server.
     * @param withNewCache If {@code true} joining node has new cache in configuration.
     * @param initialState Initial cluster state.
     * @param targetState Target cluster state.
     * @throws Exception If failed.
     */
    private void joinWhileClusterStateChange(
        boolean startClient,
        boolean withNewCache,
        ClusterState initialState,
        ClusterState targetState
    ) throws Exception {
        checkStatesAreDifferent(initialState, targetState);

        int nodesCnt = 2;

        IgniteInternalFuture<?> activeFut = startNodesAndBlockStatusChange(nodesCnt, 0, 0, initialState, targetState);

        ccfgs = withNewCache ? cacheConfigurations2() : cacheConfigurations1();

        final int numberOfCaches = ccfgs.length;

        IgniteInternalFuture<?> startFut = startNodeAsync(nodesCnt++, startClient);

        TestRecordingCommunicationSpi.spi(ignite(1)).stopBlock();

        activeFut.get();
        startFut.get();

        if (ClusterState.active(targetState))
            checkCachesOnNode(nodesCnt - 1, DEFAULT_CACHES_COUNT);
        else {
            checkNoCaches(nodesCnt);

            ignite(nodesCnt - 1).cluster().state(initialState);

            for (int c = 0; c < DEFAULT_CACHES_COUNT; c++)
                checkCache(ignite(nodesCnt - 1), CACHE_NAME_PREFIX + c, true);
        }

        if (withNewCache) {
            for (int i = 0; i < nodesCnt; i++)
                checkCachesOnNode(i, numberOfCaches);
        }

        awaitPartitionMapExchange();

        checkCaches(nodesCnt, numberOfCaches);

        startGrid(nodesCnt++, false);

        checkCaches(nodesCnt, numberOfCaches);

        startGrid(nodesCnt++, true);

        checkCaches(nodesCnt, numberOfCaches);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentJoinAndActivate() throws Exception {
        testConcurrentJoinAndActivate(ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentJoinAndActivateInReadOnly() throws Exception {
        testConcurrentJoinAndActivate(ACTIVE_READ_ONLY);
    }

    /** */
    private void testConcurrentJoinAndActivate(ClusterState activateState) throws Exception {
        assertActive(activateState);

        for (int iter = 0; iter < 3; iter++) {
            log.info("Iteration: " + iter);

            stateOnStart = INACTIVE;

            final int START_NODES = 3;

            startWithCaches1(START_NODES, 0);

            final int numberOfCaches = cacheConfigurations1().length;

            final CyclicBarrier b = new CyclicBarrier(START_NODES + 1);

            IgniteInternalFuture<Void> fut1 = runAsync(() -> {
                b.await();

                U.sleep(ThreadLocalRandom.current().nextLong(100) + 1);

                ignite(0).cluster().state(activateState);

                return null;
            });

            final AtomicInteger nodeIdx = new AtomicInteger(START_NODES);

            IgniteInternalFuture<Long> fut2 = GridTestUtils.runMultiThreadedAsync((Callable<Void>)() -> {
                int idx = nodeIdx.getAndIncrement();

                b.await();

                startGrid(idx);

                return null;
            }, START_NODES, "start-node");

            fut1.get();
            fut2.get();

            checkCaches(2 * START_NODES, numberOfCaches);

            afterTest();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateSimple_SingleNode() throws Exception {
        deactivateSimple(1, 0, 0, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateFromReadOnlySimple_SingleNode() throws Exception {
        deactivateSimple(1, 0, 0, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateSimple_5_Servers() throws Exception {
        deactivateSimple(5, 0, 0, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateFromReadOnlySimple_5_Servers() throws Exception {
        deactivateSimple(5, 0, 0, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateSimple_5_Servers2() throws Exception {
        deactivateSimple(5, 0, 4, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateFromReadOnlySimple_5_Servers2() throws Exception {
        deactivateSimple(5, 0, 4, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateSimple_5_Servers_5_Clients() throws Exception {
        deactivateSimple(5, 4, 0, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateFromReadOnlySimple_5_Servers_5_Clients() throws Exception {
        deactivateSimple(5, 4, 0, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateSimple_5_Servers_5_Clients_FromClient() throws Exception {
        deactivateSimple(5, 4, 6, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateFromReadOnlySimple_5_Servers_5_Clients_FromClient() throws Exception {
        deactivateSimple(5, 4, 6, ACTIVE_READ_ONLY);
    }

    /**
     * @param srvs Number of servers.
     * @param clients Number of clients.
     * @param changeFrom Index of node starting cluster state change from {@code initialState} to {@code targetState}.
     * @param initialState Initial cluster state.
     * @param targetState Target cluster state.
     * @throws Exception If failed.
     */
    private void changeStateSimple(
        int srvs,
        int clients,
        int changeFrom,
        ClusterState initialState,
        ClusterState targetState
    ) throws Exception {
        assertNotSame(initialState, targetState);

        stateOnStart = initialState;

        int nodesCnt = srvs + clients;

        startWithCaches1(srvs, clients);

        if (persistenceEnabled() && ClusterState.active(initialState))
            grid(0).cluster().state(initialState);

        checkClusterState(nodesCnt, initialState);

        if (!ClusterState.active(initialState))
            checkNoCaches(nodesCnt);

        ignite(changeFrom).cluster().state(initialState); // Should be no-op.

        checkClusterState(nodesCnt, initialState);

        ignite(changeFrom).cluster().state(targetState);

        checkClusterState(nodesCnt, targetState);

        if (ClusterState.active(targetState)) {
            for (int i = 0; i < nodesCnt; i++)
                checkCachesOnNode(i, DEFAULT_CACHES_COUNT);

            checkCaches(nodesCnt, DEFAULT_CACHES_COUNT);
        }
        else
            checkNoCaches(nodesCnt);

        startNodeAndCheckCaches(nodesCnt++, false, DEFAULT_CACHES_COUNT);
        startNodeAndCheckCaches(nodesCnt++, true, DEFAULT_CACHES_COUNT);

        if (!ClusterState.active(targetState)) {
            checkNoCaches(nodesCnt);

            checkClusterState(nodesCnt, targetState);

            ignite(changeFrom).cluster().state(initialState);

            checkClusterState(nodesCnt, initialState);

            for (int i = 0; i < nodesCnt; i++) {
                if (ignite(i).configuration().isClientMode())
                    checkCache(ignite(i), CU.UTILITY_CACHE_NAME, true);
                else
                    checkCachesOnNode(i, DEFAULT_CACHES_COUNT);
            }
        }
    }

    /** */
    private void startNodeAndCheckCaches(int nodeIdx, boolean client, int cachesCount) throws Exception {
        startGrid(nodeIdx, client);

        ClusterState state = grid(0).cluster().state();

        if (ClusterState.active(state)) {
            checkCachesOnNode(nodeIdx, cachesCount, !client);

            checkCaches(nodeIdx + 1, cachesCount);
        }
    }

    /**
     * @param srvs Number of servers.
     * @param clients Number of clients.
     * @throws Exception If failed.
     */
    private void startWithCaches1(int srvs, int clients) throws Exception {
        for (int i = 0; i < srvs + clients; i++) {
            ccfgs = cacheConfigurations1();

            startGrid(i, i >= srvs);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnectClusterActive() throws Exception {
        testClientReconnect(ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnectClusterActiveReadOnly() throws Exception {
        testClientReconnect(ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnectClusterInactive() throws Exception {
        testClientReconnect(INACTIVE);
    }

    /** */
    private void testClientReconnect(ClusterState initialState) throws Exception {
        testReconnectSpi = true;

        stateOnStart = initialState;

        ccfgs = cacheConfigurations1();

        final int SRVS = 3;
        final int CLIENTS = 3;
        int nodesCnt = SRVS + CLIENTS;

        startWithCaches1(SRVS, CLIENTS);

        if (persistenceEnabled() && ClusterState.active(initialState))
            ignite(0).cluster().state(initialState);

        Ignite srv = ignite(0);
        Ignite client = ignite(SRVS);

        if (ClusterState.active(initialState)) {
            checkCache(client, CU.UTILITY_CACHE_NAME, true);

            checkCaches(nodesCnt);
        }
        else
            checkNoCaches(nodesCnt);

        IgniteClientReconnectAbstractTest.reconnectClientNode(log, client, srv, null);

        if (!ClusterState.active(initialState)) {
            checkNoCaches(nodesCnt);

            srv.cluster().state(ACTIVE);

            checkCache(client, CU.UTILITY_CACHE_NAME, true);
        }

        checkCaches(nodesCnt);

        startGrid(nodesCnt++, false);
        startGrid(nodesCnt++, true);

        checkCaches(nodesCnt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnectClusterDeactivated() throws Exception {
        clientReconnectClusterState(ACTIVE, INACTIVE, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnectClusterDeactivatedFromReadOnly() throws Exception {
        clientReconnectClusterState(ACTIVE_READ_ONLY, INACTIVE, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnectClusterDeactivateInProgress() throws Exception {
        clientReconnectClusterState(ACTIVE, INACTIVE, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnectClusterDeactivateFromReadOnlyInProgress() throws Exception {
        clientReconnectClusterState(ACTIVE_READ_ONLY, INACTIVE, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnectClusterActivated() throws Exception {
        clientReconnectClusterState(INACTIVE, ACTIVE, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnectClusterActivatedReadOnly() throws Exception {
        clientReconnectClusterState(INACTIVE, ACTIVE_READ_ONLY, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnectClusterActivateInProgress() throws Exception {
        clientReconnectClusterState(INACTIVE, ACTIVE, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnectClusterActivateReadOnlyInProgress() throws Exception {
        clientReconnectClusterState(INACTIVE, ACTIVE_READ_ONLY, true);
    }

    /**
     * @param initialState Initial cluster state.
     * @param targetState Cluster state after transition.
     * @param transition If {@code true} client reconnects while cluster state transition is in progress.
     * @throws Exception If failed.
     */
    private void clientReconnectClusterState(
        ClusterState initialState,
        ClusterState targetState,
        final boolean transition
    ) throws Exception {
        assertNotSame(initialState, targetState);

        testReconnectSpi = true;
        testSpi = transition;
        stateOnStart = initialState;

        final int SRVS = 3;
        final int CLIENTS = 3;
        int nodesCnt = SRVS + CLIENTS;

        startWithCaches1(SRVS, CLIENTS);

        final Ignite srv = ignite(0);
        IgniteEx client = grid(SRVS);

        if (persistenceEnabled() && ClusterState.active(initialState))
            ignite(0).cluster().state(initialState);

        if (ClusterState.active(initialState)) {
            checkCache(client, CU.UTILITY_CACHE_NAME, true);

            checkCaches(nodesCnt);

            // Wait for late affinity assignment to finish.
            awaitPartitionMapExchange();
        }
        else
            checkNoCaches(nodesCnt);

        final AffinityTopologyVersion STATE_CHANGE_TOP_VER = new AffinityTopologyVersion(nodesCnt + 1, 1);

        final TestRecordingCommunicationSpi spi1 = transition ? TestRecordingCommunicationSpi.spi(ignite(1)) : null;

        final AtomicReference<IgniteInternalFuture> stateFut = new AtomicReference<>();

        IgniteClientReconnectAbstractTest.reconnectClientNode(log, client, srv, () -> {
            if (transition) {
                blockExchangeSingleMessage(spi1, STATE_CHANGE_TOP_VER);

                stateFut.set(runAsync(() -> srv.cluster().state(targetState), initialState + "->" + targetState));

                try {
                    U.sleep(500);
                }
                catch (IgniteInterruptedCheckedException e) {
                    U.error(log, e);
                }
            }
            else
                srv.cluster().state(targetState);
        });

        if (transition) {
            assertFalse(stateFut.get().isDone());

            assertTrue(client.context().state().clusterState().transition());

            // Public API method would block forever because we blocked the exchange message.
            assertEquals(lesserOf(initialState, targetState), client.context().state().publicApiState(false));

            spi1.waitForBlocked();

            spi1.stopBlock();

            stateFut.get().get();
        }

        if (!ClusterState.active(targetState)) {
            checkNoCaches(nodesCnt);

            ignite(0).cluster().state(initialState);

            checkClusterState(nodesCnt, initialState);
        }

        checkCache(client, CU.UTILITY_CACHE_NAME, true);

        checkCaches(nodesCnt);

        checkCache(client, CACHE_NAME_PREFIX + 0, true);

        startGrid(nodesCnt++, false);
        startGrid(nodesCnt++, true);

        checkCaches(nodesCnt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInactiveTopologyChanges() throws Exception {
        checkInactiveTopologyChanges(ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInactiveTopologyChangesReadOnly() throws Exception {
        checkInactiveTopologyChanges(ACTIVE_READ_ONLY);
    }

    /** */
    private void checkInactiveTopologyChanges(ClusterState state) throws Exception {
        assertActive(state);

        testSpi = true;

        testSpiRecord = new Class[] {GridDhtPartitionsSingleMessage.class, GridDhtPartitionsFullMessage.class};

        stateOnStart = INACTIVE;

        final int SRVS = 4;
        final int CLIENTS = 4;
        int nodesCnt = SRVS + CLIENTS;

        startWithCaches1(SRVS, CLIENTS);

        checkRecordedMessages(false);

        for (int i = 0; i < 2; i++) {
            stopGrid(i);

            startGrid(i, false);
        }

        checkRecordedMessages(false);

        for (int i = 0; i < 2; i++) {
            stopGrid(SRVS + i);

            startGrid(SRVS + i, true);
        }

        checkRecordedMessages(false);

        ignite(0).cluster().state(state);

        checkCaches(nodesCnt);

        checkRecordedMessages(true);

        startGrid(nodesCnt++, false);
        startGrid(nodesCnt++, true);

        checkRecordedMessages(true);

        checkCaches(nodesCnt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateFailover1() throws Exception {
        stateChangeFailover1(INACTIVE, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateWithReadOnlyFailover1() throws Exception {
        stateChangeFailover1(INACTIVE, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateFailover1() throws Exception {
        stateChangeFailover1(ACTIVE, INACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateFromReadOnlyFailover1() throws Exception {
        stateChangeFailover1(ACTIVE_READ_ONLY, INACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEnableReadOnlyFailover1() throws Exception {
        stateChangeFailover1(ACTIVE, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDisableReadOnlyFailover1() throws Exception {
        stateChangeFailover1(ACTIVE_READ_ONLY, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateFailover2() throws Exception {
        stateChangeFailover2(INACTIVE, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateWithReadOnlyFailover2() throws Exception {
        stateChangeFailover2(INACTIVE, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateFailover2() throws Exception {
        stateChangeFailover2(ACTIVE, INACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateFromReadOnlyFailover2() throws Exception {
        stateChangeFailover2(ACTIVE_READ_ONLY, INACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEnableReadOnlyFailover2() throws Exception {
        stateChangeFailover2(ACTIVE, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDisableReadOnlyFailover2() throws Exception {
        stateChangeFailover2(ACTIVE_READ_ONLY, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateFailover3() throws Exception {
        stateChangeFailover3(INACTIVE, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateWithReadOnlyFailover3() throws Exception {
        stateChangeFailover3(INACTIVE, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateFailover3() throws Exception {
        stateChangeFailover3(ACTIVE, INACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateFromReadOnlyFailover3() throws Exception {
        stateChangeFailover3(ACTIVE_READ_ONLY, INACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEnableReadOnlyFailover3() throws Exception {
        stateChangeFailover3(ACTIVE, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDisableReadOnlyFailover3() throws Exception {
        stateChangeFailover3(ACTIVE_READ_ONLY, ACTIVE);
    }

    /**
     * @param initialState Initial cluster state.
     * @param targetState Target cluster state.
     * @throws Exception If failed.
     */
    private void stateChangeFailover1(ClusterState initialState, ClusterState targetState) throws Exception {
        stateChangeFailover(initialState, targetState, 1, 1, 4);
    }

    /**
     * @param initialState Initial cluster state.
     * @param targetState Target cluster state.
     * @throws Exception If failed.
     */
    private void stateChangeFailover2(ClusterState initialState, ClusterState targetState) throws Exception {
        stateChangeFailover(initialState, targetState, 2, 0, 1, 4);
    }

    /**
     * @param initialState Initial cluster state.
     * @param targetState Target cluster state.
     * @throws Exception If failed.
     */
    private void stateChangeFailover3(ClusterState initialState, ClusterState targetState) throws Exception {
        assertNotSame(initialState, targetState);

        testReconnectSpi = true;

        final int servers = 4;
        final int clients = 0;
        int nodesCnt = servers + clients;

        startNodesAndBlockStatusChange(servers, clients, 0, initialState, targetState);

        IgniteInternalFuture<?> startFut1 = startNodeAsync(nodesCnt++, false);
        IgniteInternalFuture<?> startFut2 = startNodeAsync(nodesCnt++, false);

        final int expNodesCnt = nodesCnt;

        assertTrue(waitForCondition(() -> grid(0).cluster().nodes().size() == expNodesCnt, 30000L));

        // Stop all nodes participating in state change and not allow last node to finish exchange.
        for (int i = 0; i < servers; i++)
            ((IgniteDiscoverySpi)ignite(i).configuration().getDiscoverySpi()).simulateNodeFailure();

        for (int i = 0; i < servers; i++)
            stopGrid(getTestIgniteInstanceName(i), true, false);

        startFut1.get();
        startFut2.get();

        for (int i = servers; i < nodesCnt; i++)
            assertEquals(ignite(i).name(), INACTIVE, ignite(i).cluster().state());

        ignite(servers).cluster().state(ClusterState.active(initialState) ? initialState : targetState);

        doFinalChecks(servers, nodesCnt);
    }

    /**
     * @param initialState Initial cluster state.
     * @param targetState Target cluster state.
     * @param startExtraNodes Number of started server nodes during blocked status change.
     * @param restartNodes Indexes of ignite instances for restart.
     * @throws Exception If failed.
     */
    private void stateChangeFailover(
        ClusterState initialState,
        ClusterState targetState,
        int startExtraNodes,
        int... restartNodes
    ) throws Exception {
        assertNotSame(initialState, targetState);

        assertTrue(Arrays.toString(restartNodes) + " doesn't contain element 1", U.containsIntArray(restartNodes, 1));
        assertTrue(Arrays.toString(restartNodes) + " doesn't contain element 4", U.containsIntArray(restartNodes, 4));

        final int servers = 4;
        final int clients = 4;
        int nodesCnt = servers + clients;

        // Nodes 1 and 4 do not reply to coordinator.
        IgniteInternalFuture<?> fut = startNodesAndBlockStatusChange(servers, clients, 3, initialState, targetState, 1, 4);

        List<IgniteInternalFuture<?>> startFuts = new ArrayList<>();

        // Start more nodes while transition is in progress.
        for (int i = 0; i < startExtraNodes; i++)
            startFuts.add(startNodeAsync(nodesCnt++, false));

        final int exceptedNodesCnt = nodesCnt;

        assertTrue(waitForCondition(() -> grid(0).cluster().nodes().size() == exceptedNodesCnt, 30000L));

        for (int idx : restartNodes)
            stopGrid(getTestIgniteInstanceName(idx), true, false);

        fut.get();

        for (IgniteInternalFuture<?> startFut : startFuts)
            startFut.get();

        for (int idx : restartNodes)
            startGrid(idx, idx >= servers & idx < (servers + clients));

        if (!ClusterState.active(targetState)) {
            checkNoCaches(nodesCnt);

            ignite(0).cluster().state(initialState);
        }

        checkCaches(nodesCnt);
    }

    /** */
    protected void doFinalChecks(int startNodes, int nodesCnt) throws Exception {
        for (int i = 0; i < startNodes; i++)
            startGrid(i);

        checkCaches(nodesCnt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClusterStateNotWaitForDeactivation() throws Exception {
        checkClusterStateNotWaitForDeactivation(ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReadOnlyClusterStateNotWaitForDeactivation() throws Exception {
        checkClusterStateNotWaitForDeactivation(ACTIVE_READ_ONLY);
    }

    /** */
    private void checkClusterStateNotWaitForDeactivation(ClusterState initialState) throws Exception {
        assertActive(initialState);

        testSpi = true;

        final int nodes = 2;

        IgniteEx crd = startGrids(nodes);

        crd.cluster().state(initialState);

        AffinityTopologyVersion curTopVer = crd.context().discovery().topologyVersionEx();

        AffinityTopologyVersion deactivationTopVer = new AffinityTopologyVersion(
            curTopVer.topologyVersion(),
            curTopVer.minorTopologyVersion() + 1
        );

        for (int gridIdx = 0; gridIdx < nodes; gridIdx++)
            blockExchangeSingleMessage(TestRecordingCommunicationSpi.spi(grid(gridIdx)), deactivationTopVer);

        IgniteInternalFuture deactivationFut = runAsync(() -> crd.cluster().state(INACTIVE));

        // Wait for deactivation start.
        assertTrue(GridTestUtils.waitForCondition(
            () -> {
                DiscoveryDataClusterState clusterState = crd.context().state().clusterState();

                return clusterState.transition() && !ClusterState.active(clusterState.state());
            },
            getTestTimeout()
        ));

        // Check that deactivation transition wait is not happened.
        ClusterState state = crd.context().state().publicApiState(true);

        assertInactive(state);

        for (int gridIdx = 0; gridIdx < nodes; gridIdx++)
            TestRecordingCommunicationSpi.spi(grid(gridIdx)).stopBlock();

        deactivationFut.get();
    }

    /**
     * @param exp If {@code true} there should be recorded messages.
     */
    private void checkRecordedMessages(boolean exp) {
        for (Ignite node : G.allGrids()) {
            List<Object> recorded = TestRecordingCommunicationSpi.spi(node).recordedMessages(false);

            if (exp)
                assertFalse(F.isEmpty(recorded));
            else
                assertTrue(F.isEmpty(recorded));
        }
    }

    /**
     * @return Cache configurations.
     */
    final CacheConfiguration[] cacheConfigurations1() {
        CacheConfiguration[] ccfgs = new CacheConfiguration[2];

        ccfgs[0] = cacheConfiguration(CACHE_NAME_PREFIX + 0, ATOMIC);
        ccfgs[1] = cacheConfiguration(CACHE_NAME_PREFIX + 1, TRANSACTIONAL);

        return ccfgs;
    }

    /**
     * @return Cache configurations.
     */
    final CacheConfiguration[] cacheConfigurations2() {
        CacheConfiguration[] ccfgs = new CacheConfiguration[5];

        ccfgs[0] = cacheConfiguration(CACHE_NAME_PREFIX + 0, ATOMIC);
        ccfgs[1] = cacheConfiguration(CACHE_NAME_PREFIX + 1, TRANSACTIONAL);
        ccfgs[2] = cacheConfiguration(CACHE_NAME_PREFIX + 2, ATOMIC);
        ccfgs[3] = cacheConfiguration(CACHE_NAME_PREFIX + 3, TRANSACTIONAL);
        ccfgs[4] = cacheConfiguration(CACHE_NAME_PREFIX + 4, TRANSACTIONAL);

        ccfgs[4].setDataRegionName(NO_PERSISTENCE_REGION);
        ccfgs[4].setDiskPageCompression(null);

        return ccfgs;
    }

    /**
     * @param name Cache name.
     * @param atomicityMode Atomicity mode.
     * @return Cache configuration.
     */
    protected final CacheConfiguration cacheConfiguration(String name, CacheAtomicityMode atomicityMode) {
        CacheConfiguration ccfg = new CacheConfiguration(name);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setBackups(1);

        return ccfg;
    }

    /**
     * @param cacheName Cache name.
     * @param node Node.
     * @param exp {@code True} if expect that cache is started on node.
     */
    void checkCache(Ignite node, String cacheName, boolean exp) throws IgniteCheckedException {
        GridTestUtils.waitForCondition(
            () -> ((IgniteEx)node).context().cache().context().exchange().lastTopologyFuture() != null,
            1000
        );

        ((IgniteEx)node).context().cache().context().exchange().lastTopologyFuture().get();

        ((IgniteEx)node).context().state().publicApiState(true);

        GridCacheAdapter cache = ((IgniteEx)node).context().cache().internalCache(cacheName);

        if (exp)
            assertNotNull("Cache not found [cache=" + cacheName + ", node=" + node.name() + ']', cache);
        else
            assertNull("Unexpected cache found [cache=" + cacheName + ", node=" + node.name() + ']', cache);
    }

    /**
     * @param nodes Number of nodes.
     */
    final void checkNoCaches(int nodes) {
        for (int i = 0; i < nodes; i++) {
            assertEquals(INACTIVE, grid(i).context().state().publicApiState(true));

            GridCacheProcessor cache = ignite(i).context().cache();

            assertTrue(cache.caches().isEmpty());
            assertTrue(cache.internalCaches().stream().allMatch(c -> c.context().isRecoveryMode()));
        }
    }

    /** */
    private void checkClusterState(int nodesCnt, ClusterState state) {
        for (int i = 0; i < nodesCnt; i++)
            assertEquals(ignite(i).name(), state, ignite(i).cluster().state());
    }

    /** */
    protected void checkCachesOnNode(int nodeNumber, int cachesCnt) throws IgniteCheckedException {
        checkCachesOnNode(nodeNumber, cachesCnt, true);
    }

    /** */
    protected void checkCachesOnNode(int nodeNumber, int cachesCnt, boolean expUserCaches) throws IgniteCheckedException {
        for (int c = 0; c < cachesCnt; c++)
            checkCache(ignite(nodeNumber), CACHE_NAME_PREFIX + c, expUserCaches);

        checkCache(ignite(nodeNumber), CU.UTILITY_CACHE_NAME, true);
    }

    /**
     * @param nodes Expected nodes number.
     */
    private void checkCaches(int nodes) throws InterruptedException {
        checkCaches(nodes, 2, false);
    }

    /** */
    private static void checkStatesAreDifferent(ClusterState state1, ClusterState state2) {
        assertTrue(state1 + " " + state2, ClusterState.active(state1) != ClusterState.active(state2));
    }

    /** */
    protected void startGrid(int nodeNumber, boolean client) throws Exception {
        startGrid(nodeNumber, client, null);
    }

    /** */
    protected void startGrid(int nodeNumber, boolean client, CacheConfiguration[] cacheConfigs) throws Exception {
        if (cacheConfigs != null)
            this.ccfgs = cacheConfigs;

        if (client)
            startClientGrid(nodeNumber);
        else
            startGrid(nodeNumber);
    }

    /** */
    private IgniteInternalFuture<?> startNodeAsync(int nodeNumber, boolean client) {
        return runAsync(() -> {
            try {
                if (client)
                    startClientGrid(nodeNumber);
                else
                    startGrid(nodeNumber);
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }, "start" + "-" + (client ? "client" : "server") + "-node" + nodeNumber);
    }
}
