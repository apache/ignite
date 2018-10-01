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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteClientReconnectAbstractTest;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
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
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteClusterActivateDeactivateTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    static final String CACHE_NAME_PREFIX = "cache-";

    /** Non-persistent data region name. */
    private static final String NO_PERSISTENCE_REGION = "no-persistence-region";
    /** */
    private static final int DEFAULT_CACHES_COUNT = 2;

    /** */
    boolean client;

    /** */
    private boolean active = true;

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

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setClientMode(client);

        cfg.setActiveOnStart(active);

        if (ccfgs != null) {
            cfg.setCacheConfiguration(ccfgs);

            ccfgs = null;
        }

        DataStorageConfiguration memCfg = new DataStorageConfiguration();
        memCfg.setPageSize(4 * 1024);
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
    public void testActivateSimple_SingleNode() throws Exception {
        activateSimple(1, 0, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateSimple_5_Servers() throws Exception {
        activateSimple(5, 0, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateSimple_5_Servers2() throws Exception {
        activateSimple(5, 0, 4);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateSimple_5_Servers_5_Clients() throws Exception {
        activateSimple(5, 4, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateSimple_5_Servers_5_Clients_FromClient() throws Exception {
        activateSimple(5, 4, 6);
    }

    /**
     * @param srvs Number of servers.
     * @param clients Number of clients.
     * @param activateFrom Index of node stating activation.
     * @throws Exception If failed.
     */
    private void activateSimple(int srvs, int clients, int activateFrom) throws Exception {
        active = false;

        final int CACHES = 2;

        for (int i = 0; i < srvs + clients; i++) {
            client = i >= srvs;

            ccfgs = cacheConfigurations1();

            startGrid(i);

            checkNoCaches(i);
        }

        for (int i = 0; i < srvs + clients; i++)
            assertFalse(ignite(i).cluster().active());

        ignite(activateFrom).cluster().active(false); // Should be no-op.

        ignite(activateFrom).cluster().active(true);

        for (int i = 0; i < srvs + clients; i++)
            assertTrue(ignite(i).cluster().active());

        for (int i = 0; i < srvs + clients; i++) {
            for (int c = 0; c < DEFAULT_CACHES_COUNT; c++)
                checkCache(ignite(i), CACHE_NAME_PREFIX + c, true);

            checkCache(ignite(i), CU.UTILITY_CACHE_NAME, true);
        }

        checkCaches(srvs + clients, CACHES);

        client = false;

        startGrid(srvs + clients);

        for (int c = 0; c < DEFAULT_CACHES_COUNT; c++)
            checkCache(ignite(srvs + clients), CACHE_NAME_PREFIX + c, true);

        checkCaches(srvs + clients + 1, CACHES);

        client = true;

        startGrid(srvs + clients + 1);

        for (int c = 0; c < DEFAULT_CACHES_COUNT; c++)
            checkCache(ignite(srvs + clients + 1), CACHE_NAME_PREFIX + c, false);

        checkCaches(srvs + clients + 2, CACHES);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReActivateSimple_5_Servers_4_Clients_FromClient() throws Exception {
        reactivateSimple(5, 4, 6);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReActivateSimple_5_Servers_4_Clients_FromServer() throws Exception {
        reactivateSimple(5, 4, 0);
    }

    /**
     * @param srvs Number of servers.
     * @param clients Number of clients.
     * @param activateFrom Index of node stating activation.
     * @throws Exception If failed.
     */
    public void reactivateSimple(int srvs, int clients, int activateFrom) throws Exception {
        activateSimple(srvs, clients, activateFrom);

        rolloverSegmentAtLeastTwice(activateFrom);

        for (int i = 0; i < srvs + clients; i++) {
            for (int c = 0; c < DEFAULT_CACHES_COUNT; c++)
                checkCache(ignite(i), CACHE_NAME_PREFIX + c, true);

            checkCache(ignite(i), CU.UTILITY_CACHE_NAME, true);
        }

        ignite(activateFrom).cluster().active(false);

        ignite(activateFrom).cluster().active(true);

        rolloverSegmentAtLeastTwice(activateFrom);

        for (int i = 0; i < srvs + clients; i++) {
            for (int c = 0; c < DEFAULT_CACHES_COUNT; c++)
                checkCache(ignite(i), CACHE_NAME_PREFIX + c, true);

            checkCache(ignite(i), CU.UTILITY_CACHE_NAME, true);
        }
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
    final void checkCaches(int nodes, int caches) {
        for (int i = 0; i < nodes; i++) {
            for (int c = 0; c < caches; c++) {
                IgniteCache<Integer, Integer> cache = ignite(i).cache(CACHE_NAME_PREFIX + c);

                for (int j = 0; j < 10; j++) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    Integer key = rnd.nextInt(1000);

                    cache.put(key, j);

                    assertEquals((Integer)j, cache.get(key));
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinWhileActivate1_Server() throws Exception {
        joinWhileActivate1(false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinWhileActivate1_WithCache_Server() throws Exception {
        joinWhileActivate1(false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinWhileActivate1_Client() throws Exception {
        joinWhileActivate1(true, false);
    }

    /**
     * @param startClient If {@code true} joins client node, otherwise server.
     * @param withNewCache If {@code true} joining node has new cache in configuration.
     * @throws Exception If failed.
     */
    private void joinWhileActivate1(final boolean startClient, final boolean withNewCache) throws Exception {
        IgniteInternalFuture<?> activeFut = startNodesAndBlockStatusChange(2, 0, 0, false);

        IgniteInternalFuture<?> startFut = GridTestUtils.runAsync((Callable<Void>)() -> {
            client = startClient;

            ccfgs = withNewCache ? cacheConfigurations2() : cacheConfigurations1();

            startGrid(2);

            return null;
        });

        TestRecordingCommunicationSpi spi1 = TestRecordingCommunicationSpi.spi(ignite(1));

        spi1.stopBlock();

        activeFut.get();
        startFut.get();

        for (int c = 0; c < DEFAULT_CACHES_COUNT; c++)
            checkCache(ignite(2), CACHE_NAME_PREFIX + c, true);

        if (withNewCache) {
            for (int i = 0; i < 3; i++) {
                for (int c = 0; c < 4; c++)
                    checkCache(ignite(i), CACHE_NAME_PREFIX + c, true);
            }
        }

        awaitPartitionMapExchange();

        checkCaches(3, withNewCache ? 4 : 2);

        client = false;

        startGrid(3);

        checkCaches(4, withNewCache ? 4 : 2);

        client = true;

        startGrid(4);

        checkCaches(5, withNewCache ? 4 : 2);
    }

    /**
     * @param srvs Number of servers.
     * @param clients Number of clients.
     * @param stateChangeFrom Index of node initiating changes.
     * @param initiallyActive If {@code true} start cluster in active state (otherwise in inactive).
     * @param blockMsgNodes Nodes whcis block exchange messages.
     * @return State change future.
     * @throws Exception If failed.
     */
    private IgniteInternalFuture<?> startNodesAndBlockStatusChange(
        int srvs,
        int clients,
        final int stateChangeFrom,
        final boolean initiallyActive,
        int... blockMsgNodes
    ) throws Exception {
        active = initiallyActive;
        testSpi = true;

        startWithCaches1(srvs, clients);

        int minorVer = 1;

        if (initiallyActive && persistenceEnabled()) {
            ignite(0).cluster().active(true);

            minorVer++;
        }

        if (blockMsgNodes.length == 0)
            blockMsgNodes = new int[] {1};

        final AffinityTopologyVersion STATE_CHANGE_TOP_VER = new AffinityTopologyVersion(srvs + clients, minorVer);

        List<TestRecordingCommunicationSpi> spis = new ArrayList<>();

        for (int idx : blockMsgNodes) {
            TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(ignite(idx));

            spis.add(spi);

            blockExchangeSingleMessage(spi, STATE_CHANGE_TOP_VER);
        }

        IgniteInternalFuture<?> stateChangeFut = GridTestUtils.runAsync(() ->
            ignite(stateChangeFrom).cluster().active(!initiallyActive)
        );

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
    public void testJoinWhileDeactivate1_Server() throws Exception {
        joinWhileDeactivate1(false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinWhileDeactivate1_WithCache_Server() throws Exception {
        joinWhileDeactivate1(false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinWhileDeactivate1_Client() throws Exception {
        joinWhileDeactivate1(true, false);
    }

    /**
     * @param startClient If {@code true} joins client node, otherwise server.
     * @param withNewCache If {@code true} joining node has new cache in configuration.
     * @throws Exception If failed.
     */
    private void joinWhileDeactivate1(final boolean startClient, final boolean withNewCache) throws Exception {
        IgniteInternalFuture<?> activeFut = startNodesAndBlockStatusChange(2, 0, 0, true);

        IgniteInternalFuture<?> startFut = GridTestUtils.runAsync((Callable<Void>)() -> {
            client = startClient;

            ccfgs = withNewCache ? cacheConfigurations2() : cacheConfigurations1();

            startGrid(2);

            return null;
        });

        TestRecordingCommunicationSpi spi1 = TestRecordingCommunicationSpi.spi(ignite(1));

        spi1.stopBlock();

        activeFut.get();
        startFut.get();

        checkNoCaches(3);

        ignite(2).cluster().active(true);

        for (int c = 0; c < DEFAULT_CACHES_COUNT; c++)
            checkCache(ignite(2), CACHE_NAME_PREFIX + c, true);

        if (withNewCache) {
            for (int i = 0; i < 3; i++) {
                for (int c = 0; c < 4; c++)
                    checkCache(ignite(i), CACHE_NAME_PREFIX + c, true);
            }
        }

        awaitPartitionMapExchange();

        checkCaches(3, withNewCache ? 4 : 2);

        client = false;

        startGrid(3);

        checkCaches(4, withNewCache ? 4 : 2);

        client = true;

        startGrid(4);

        checkCaches(5, withNewCache ? 4 : 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentJoinAndActivate() throws Exception {
        for (int iter = 0; iter < 3; iter++) {
            log.info("Iteration: " + iter);

            active = false;

            for (int i = 0; i < 3; i++) {
                ccfgs = cacheConfigurations1();

                startGrid(i);
            }

            final int START_NODES = 3;

            final CyclicBarrier b = new CyclicBarrier(START_NODES + 1);

            IgniteInternalFuture<Void> fut1 = GridTestUtils.runAsync(() -> {
                b.await();

                U.sleep(ThreadLocalRandom.current().nextLong(100) + 1);

                ignite(0).cluster().active(true);

                return null;
            });

            final AtomicInteger nodeIdx = new AtomicInteger(3);

            IgniteInternalFuture<Long> fut2 = GridTestUtils.runMultiThreadedAsync((Callable<Void>)() -> {
                int idx = nodeIdx.getAndIncrement();

                b.await();

                startGrid(idx);

                return null;
            }, START_NODES, "start-node");

            fut1.get();
            fut2.get();

            checkCaches(6, 2);

            afterTest();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeactivateSimple_SingleNode() throws Exception {
        deactivateSimple(1, 0, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeactivateSimple_5_Servers() throws Exception {
        deactivateSimple(5, 0, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeactivateSimple_5_Servers2() throws Exception {
        deactivateSimple(5, 0, 4);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeactivateSimple_5_Servers_5_Clients() throws Exception {
        deactivateSimple(5, 4, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeactivateSimple_5_Servers_5_Clients_FromClient() throws Exception {
        deactivateSimple(5, 4, 6);
    }

    /**
     * @param srvs Number of servers.
     * @param clients Number of clients.
     * @param deactivateFrom Index of node stating deactivation.
     * @throws Exception If failed.
     */
    private void deactivateSimple(int srvs, int clients, int deactivateFrom) throws Exception {
        active = true;

        final int CACHES = 2;

        for (int i = 0; i < srvs + clients; i++) {
            client = i >= srvs;

            ccfgs = cacheConfigurations1();

            startGrid(i);
        }

        if (persistenceEnabled())
            ignite(deactivateFrom).cluster().active(true);

        ignite(deactivateFrom).cluster().active(true); // Should be no-op.

        checkCaches(srvs + clients, CACHES);

        for (int i = 0; i < srvs + clients; i++)
            assertTrue(ignite(i).cluster().active());

        ignite(deactivateFrom).cluster().active(false);

        for (int i = 0; i < srvs + clients; i++)
            assertFalse(ignite(i).cluster().active());

        checkNoCaches(srvs + clients);

        client = false;

        startGrid(srvs + clients);

        checkNoCaches(srvs + clients + 1);

        client = true;

        startGrid(srvs + clients + 1);

        checkNoCaches(srvs + clients + 2);

        for (int i = 0; i < srvs + clients + 2; i++)
            assertFalse(ignite(i).cluster().active());

        ignite(deactivateFrom).cluster().active(true);

        for (int i = 0; i < srvs + clients + 2; i++) {
            assertTrue(ignite(i).cluster().active());

            checkCache(ignite(i), CU.UTILITY_CACHE_NAME, true);
        }

        for (int i = 0; i < srvs; i++) {
            for (int c = 0; c < DEFAULT_CACHES_COUNT; c++)
                checkCache(ignite(i), CACHE_NAME_PREFIX + c, true);
        }

        checkCaches1(srvs + clients + 2);
    }

    /**
     * @param srvs Number of servers.
     * @param clients Number of clients.
     * @throws Exception If failed.
     */
    private void startWithCaches1(int srvs, int clients) throws Exception {
        for (int i = 0; i < srvs + clients; i++) {
            ccfgs = cacheConfigurations1();

            client = i >= srvs;

            startGrid(i);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientReconnectClusterActive() throws Exception {
        testReconnectSpi = true;

        ccfgs = cacheConfigurations1();

        final int SRVS = 3;
        final int CLIENTS = 3;

        startWithCaches1(SRVS, CLIENTS);

        if (persistenceEnabled())
            ignite(0).cluster().active(true);

        Ignite srv = ignite(0);
        Ignite client = ignite(SRVS);

        checkCache(client, CU.UTILITY_CACHE_NAME, true);

        checkCaches1(SRVS + CLIENTS);

        IgniteClientReconnectAbstractTest.reconnectClientNode(log, client, srv, null);

        checkCaches1(SRVS + CLIENTS);

        this.client = false;

        startGrid(SRVS + CLIENTS);

        this.client = true;

        startGrid(SRVS + CLIENTS + 1);

        checkCaches1(SRVS + CLIENTS + 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientReconnectClusterInactive() throws Exception {
        testReconnectSpi = true;

        active = false;

        final int SRVS = 3;
        final int CLIENTS = 3;

        startWithCaches1(SRVS, CLIENTS);

        Ignite srv = ignite(0);
        Ignite client = ignite(SRVS);

        checkNoCaches(SRVS + CLIENTS);

        IgniteClientReconnectAbstractTest.reconnectClientNode(log, client, srv, null);

        checkNoCaches(SRVS + CLIENTS);

        ignite(0).cluster().active(true);

        checkCache(client, CU.UTILITY_CACHE_NAME, true);

        checkCaches1(SRVS + CLIENTS);

        this.client = false;

        startGrid(SRVS + CLIENTS);

        this.client = true;

        startGrid(SRVS + CLIENTS + 1);

        checkCaches1(SRVS + CLIENTS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientReconnectClusterDeactivated() throws Exception {
        clientReconnectClusterDeactivated(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientReconnectClusterDeactivateInProgress() throws Exception {
        clientReconnectClusterDeactivated(true);
    }

    /**
     * @param transition If {@code true} client reconnects while cluster state transition is in progress.
     * @throws Exception If failed.
     */
    private void clientReconnectClusterDeactivated(final boolean transition) throws Exception {
        testReconnectSpi = true;
        testSpi = transition;

        final int SRVS = 3;
        final int CLIENTS = 3;

        startWithCaches1(SRVS, CLIENTS);

        final Ignite srv = ignite(0);
        IgniteEx client = grid(SRVS);

        if (persistenceEnabled())
            ignite(0).cluster().active(true);

        checkCache(client, CU.UTILITY_CACHE_NAME, true);

        checkCaches1(SRVS + CLIENTS);

        // Wait for late affinity assignment to finish.
        grid(0).context().cache().context().exchange().affinityReadyFuture(
            new AffinityTopologyVersion(SRVS + CLIENTS, 1)).get();

        final AffinityTopologyVersion STATE_CHANGE_TOP_VER = new AffinityTopologyVersion(SRVS + CLIENTS + 1, 1);

        final TestRecordingCommunicationSpi spi1 = transition ? TestRecordingCommunicationSpi.spi(ignite(1)) : null;

        final AtomicReference<IgniteInternalFuture> stateFut = new AtomicReference<>();

        IgniteClientReconnectAbstractTest.reconnectClientNode(log, client, srv, () -> {
            if (transition) {
                blockExchangeSingleMessage(spi1, STATE_CHANGE_TOP_VER);

                stateFut.set(GridTestUtils.runAsync(() -> srv.cluster().active(false),
                    "deactivate"));

                try {
                    U.sleep(500);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else
                srv.cluster().active(false);
        });

        if (transition) {
            assertFalse(stateFut.get().isDone());

            // Public API method would block forever because we blocked the exchange message.
            assertFalse(client.context().state().publicApiActiveState(false));

            spi1.waitForBlocked();

            spi1.stopBlock();

            stateFut.get().get();
        }

        checkNoCaches(SRVS + CLIENTS);

        ignite(0).cluster().active(true);

        checkCache(client, CU.UTILITY_CACHE_NAME, true);

        assertTrue(client.cluster().active());

        checkCaches1(SRVS + CLIENTS);

        checkCache(client, CACHE_NAME_PREFIX + 0, true);

        this.client = false;

        startGrid(SRVS + CLIENTS);

        this.client = true;

        startGrid(SRVS + CLIENTS + 1);

        checkCaches1(SRVS + CLIENTS + 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientReconnectClusterActivated() throws Exception {
        clientReconnectClusterActivated(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientReconnectClusterActivateInProgress() throws Exception {
        clientReconnectClusterActivated(true);
    }

    /**
     * @param transition If {@code true} client reconnects while cluster state transition is in progress.
     * @throws Exception If failed.
     */
    private void clientReconnectClusterActivated(final boolean transition) throws Exception {
        testReconnectSpi = true;
        testSpi = transition;

        active = false;

        final int SRVS = 3;
        final int CLIENTS = 3;

        startWithCaches1(SRVS, CLIENTS);

        final Ignite srv = ignite(0);
        IgniteEx client = grid(SRVS);

        checkNoCaches(SRVS + CLIENTS);

        final AffinityTopologyVersion STATE_CHANGE_TOP_VER = new AffinityTopologyVersion(SRVS + CLIENTS + 1, 1);

        final TestRecordingCommunicationSpi spi1 = transition ? TestRecordingCommunicationSpi.spi(ignite(1)) : null;

        final AtomicReference<IgniteInternalFuture> stateFut = new AtomicReference<>();

        IgniteClientReconnectAbstractTest.reconnectClientNode(log, client, srv, () -> {
            if (transition) {
                blockExchangeSingleMessage(spi1, STATE_CHANGE_TOP_VER);

                stateFut.set(GridTestUtils.runAsync(() -> srv.cluster().active(true),
                    "activate"));

                try {
                    U.sleep(500);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else
                srv.cluster().active(true);
        });

        if (transition) {
            assertFalse(stateFut.get().isDone());

            assertTrue(client.context().state().clusterState().transition());

            spi1.waitForBlocked();

            spi1.stopBlock();

            stateFut.get().get();
        }

        checkCache(client, CU.UTILITY_CACHE_NAME, true);

        checkCaches1(SRVS + CLIENTS);

        checkCache(client, CACHE_NAME_PREFIX + 0, true);

        this.client = false;

        startGrid(SRVS + CLIENTS);

        this.client = true;

        startGrid(SRVS + CLIENTS + 1);

        checkCaches1(SRVS + CLIENTS + 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInactiveTopologyChanges() throws Exception {
        testSpi = true;

        testSpiRecord = new Class[] {GridDhtPartitionsSingleMessage.class, GridDhtPartitionsFullMessage.class};

        active = false;

        final int SRVS = 4;
        final int CLIENTS = 4;

        startWithCaches1(SRVS, CLIENTS);

        checkRecordedMessages(false);

        for (int i = 0; i < 2; i++) {
            stopGrid(i);

            client = false;

            startGrid(i);
        }

        checkRecordedMessages(false);

        for (int i = 0; i < 2; i++) {
            stopGrid(SRVS + i);

            client = true;

            startGrid(SRVS + i);
        }

        checkRecordedMessages(false);

        ignite(0).cluster().active(true);

        checkCaches1(SRVS + CLIENTS);

        checkRecordedMessages(true);

        client = false;

        startGrid(SRVS + CLIENTS);

        client = true;

        startGrid(SRVS + CLIENTS + 1);

        checkRecordedMessages(true);

        checkCaches1(SRVS + CLIENTS + 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateFailover1() throws Exception {
        stateChangeFailover1(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeactivateFailover1() throws Exception {
        stateChangeFailover1(false);
    }

    /**
     * @param activate If {@code true} tests activation, otherwise deactivation.
     * @throws Exception If failed.
     */
    private void stateChangeFailover1(boolean activate) throws Exception {
        // Nodes 1 and 4 do not reply to coordinator.
        IgniteInternalFuture<?> fut = startNodesAndBlockStatusChange(4, 4, 3, !activate, 1, 4);

        client = false;

        // Start one more node while transition is in progress.
        IgniteInternalFuture<Void> startFut = GridTestUtils.runAsync(() -> {
            startGrid(8);

            return null;
        }, "start-node");

        U.sleep(500);

        stopGrid(getTestIgniteInstanceName(1), true, false);
        stopGrid(getTestIgniteInstanceName(4), true, false);

        fut.get();

        startFut.get();

        client = false;

        startGrid(1);

        client = true;

        startGrid(4);

        if (!activate) {
            checkNoCaches(9);

            ignite(0).cluster().active(true);
        }

        checkCaches1(9);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateFailover2() throws Exception {
        stateChangeFailover2(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeactivateFailover2() throws Exception {
        stateChangeFailover2(false);
    }

    /**
     * @param activate If {@code true} tests activation, otherwise deactivation.
     * @throws Exception If failed.
     */
    private void stateChangeFailover2(boolean activate) throws Exception {
        // Nodes 1 and 4 do not reply to coordinator.
        IgniteInternalFuture<?> fut = startNodesAndBlockStatusChange(4, 4, 3, !activate, 1, 4);

        client = false;

        // Start more nodes while transition is in progress.
        IgniteInternalFuture<Void> startFut1 = GridTestUtils.runAsync(() -> {
            startGrid(8);

            return null;
        }, "start-node1");

        IgniteInternalFuture<Void> startFut2 = GridTestUtils.runAsync(() -> {
            startGrid(9);

            return null;
        }, "start-node2");

        U.sleep(500);

        // Stop coordinator.
        stopGrid(getTestIgniteInstanceName(0), true, false);

        stopGrid(getTestIgniteInstanceName(1), true, false);
        stopGrid(getTestIgniteInstanceName(4), true, false);

        fut.get();

        startFut1.get();
        startFut2.get();

        client = false;

        startGrid(0);
        startGrid(1);

        client = true;

        startGrid(4);

        if (!activate) {
            checkNoCaches(10);

            ignite(0).cluster().active(true);
        }

        checkCaches1(10);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateFailover3() throws Exception {
        stateChangeFailover3(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeactivateFailover3() throws Exception {
        stateChangeFailover3(false);
    }

    /**
     * @param activate If {@code true} tests activation, otherwise deactivation.
     * @throws Exception If failed.
     */
    private void stateChangeFailover3(boolean activate) throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-8220");

        testReconnectSpi = true;

        startNodesAndBlockStatusChange(4, 0, 0, !activate);

        client = false;

        IgniteInternalFuture startFut1 = GridTestUtils.runAsync((Callable)() -> {
            startGrid(4);

            return null;
        }, "start-node1");

        IgniteInternalFuture startFut2 = GridTestUtils.runAsync((Callable)() -> {
            startGrid(5);

            return null;
        }, "start-node2");

        U.sleep(1000);

        // Stop all nodes participating in state change and not allow last node to finish exchange.
        for (int i = 0; i < 4; i++)
            ((IgniteDiscoverySpi)ignite(i).configuration().getDiscoverySpi()).simulateNodeFailure();

        for (int i = 0; i < 4; i++)
            stopGrid(getTestIgniteInstanceName(i), true, false);

        startFut1.get();
        startFut2.get();

        assertFalse(ignite(4).cluster().active());
        assertFalse(ignite(5).cluster().active());

        ignite(4).cluster().active(true);

        for (int i = 0; i < 4; i++)
            startGrid(i);

        checkCaches1(6);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClusterStateNotWaitForDeactivation() throws Exception {
        testSpi = true;

        final int nodes = 2;

        IgniteEx crd = (IgniteEx)startGrids(nodes);

        crd.cluster().active(true);

        AffinityTopologyVersion curTopVer = crd.context().discovery().topologyVersionEx();

        AffinityTopologyVersion deactivationTopVer = new AffinityTopologyVersion(
            curTopVer.topologyVersion(),
            curTopVer.minorTopologyVersion() + 1
        );

        for (int gridIdx = 0; gridIdx < nodes; gridIdx++) {
            TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(gridIdx));

            blockExchangeSingleMessage(spi, deactivationTopVer);
        }

        IgniteInternalFuture deactivationFut = GridTestUtils.runAsync(() -> crd.cluster().active(false));

        // Wait for deactivation start.
        GridTestUtils.waitForCondition(() -> {
            DiscoveryDataClusterState clusterState = crd.context().state().clusterState();

            return clusterState.transition() && !clusterState.active();
        }, getTestTimeout());

        // Check that deactivation transition wait is not happened.
        Assert.assertFalse(crd.context().state().publicApiActiveState(true));

        for (int gridIdx = 0; gridIdx < nodes; gridIdx++) {
            TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(gridIdx));

            spi.stopBlock();
        }

        deactivationFut.get();
    }

    /**
     * @param exp If {@code true} there should be recorded messages.
     */
    private void checkRecordedMessages(boolean exp) {
        for (Ignite node : G.allGrids()) {
            List<Object> recorded =
                TestRecordingCommunicationSpi.spi(node).recordedMessages(false);

            if (exp)
                assertFalse(F.isEmpty(recorded));
            else
                assertTrue(F.isEmpty(recorded));
        }
    }

    /**
     * @param nodes Expected nodes number.
     */
    private void checkCaches1(int nodes) {
        checkCaches(nodes, 2);
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
        ((IgniteEx)node).context().cache().context().exchange().lastTopologyFuture().get();

        ((IgniteEx)node).context().state().publicApiActiveState(true);

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
            grid(i).context().state().publicApiActiveState(true);

            GridCacheProcessor cache = ((IgniteEx)ignite(i)).context().cache();

            assertTrue(cache.caches().isEmpty());
            assertTrue(cache.internalCaches().isEmpty());
        }
    }
}
