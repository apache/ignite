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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAffinityAssignmentRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessageV2;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeLeftMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 *
 */
public class CacheNoAffinityExchangeTest extends GridCommonAbstractTest {
    /** */
    private volatile boolean startClientCaches;

    /** Tx cache name from client static configuration. */
    private static final String PARTITIONED_TX_CLIENT_CACHE_NAME = "p-tx-client-cache";

    /** */
    private final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder().setShared(true);

    /** */
    private final TcpDiscoveryIpFinder CLIENT_IP_FINDER = new TcpDiscoveryVmIpFinder()
        .setAddresses(Collections.singleton("127.0.0.1:47500"));

    /** Custom communication SPI that can be used by the client node. */
    private volatile TestRecordingCommunicationSpi clientCommSpi;

    /** Stores errors that triggered failure handler. */
    private final Map<String, Throwable> errs = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.ENTRY_LOCK);

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setDiscoverySpi(new TestDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setClusterStateOnStart(INACTIVE);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setMaxSize(200 * 1024 * 1024)));

        if (cfg.isClientMode()) {
            TestRecordingCommunicationSpi customSpi = clientCommSpi;
            if (customSpi != null)
                cfg.setCommunicationSpi(customSpi);

            // It is necessary to ensure that client always connects to grid(0).
            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(CLIENT_IP_FINDER);

            if (startClientCaches) {
                CacheConfiguration<Integer, Integer> txCfg = new CacheConfiguration<Integer, Integer>()
                    .setName(PARTITIONED_TX_CLIENT_CACHE_NAME)
                    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                    .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                    .setAffinity(new RendezvousAffinityFunction(false, 32))
                    .setBackups(2);

                cfg.setCacheConfiguration(txCfg);
            }
        }

        cfg.setFailureHandler(new AbstractFailureHandler() {
            /** {@inheritDoc} */
            @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                errs.put(ignite.configuration().getIgniteInstanceName(), failureCtx.error());
                return false;
            }
        });

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        startClientCaches = false;

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoAffinityChangeOnClientJoin() throws Exception {
        Ignite ig = startGrids(4);

        ig.cluster().state(ACTIVE);

        IgniteCache<Integer, Integer> atomicCache = ig.createCache(new CacheConfiguration<Integer, Integer>()
                .setName("atomic").setAtomicityMode(CacheAtomicityMode.ATOMIC));

        IgniteCache<Integer, Integer> txCache = ig.createCache(new CacheConfiguration<Integer, Integer>()
                .setName("tx").setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        assertTrue(GridTestUtils.waitForCondition(() ->
                new AffinityTopologyVersion(4, 3).equals(grid(3).context().discovery().topologyVersionEx()),
            5_000));

        TestDiscoverySpi discoSpi = (TestDiscoverySpi)grid(2).context().discovery().getInjectedDiscoverySpi();

        CountDownLatch latch = new CountDownLatch(1);

        discoSpi.latch = latch;

        startClientGrid(4);

        assertTrue(GridTestUtils.waitForCondition(() ->
                new AffinityTopologyVersion(5, 0).equals(grid(0).context().discovery().topologyVersionEx()) &&
                    new AffinityTopologyVersion(5, 0).equals(grid(1).context().discovery().topologyVersionEx()) &&
                    new AffinityTopologyVersion(4, 3).equals(grid(2).context().discovery().topologyVersionEx()) &&
                    new AffinityTopologyVersion(4, 3).equals(grid(3).context().discovery().topologyVersionEx()),
            10_000));

        for (int k = 0; k < 100; k++) {
            atomicCache.put(k, k);
            txCache.put(k, k);

            Lock lock = txCache.lock(k);
            lock.lock();
            lock.unlock();
        }

        for (int k = 0; k < 100; k++) {
            assertEquals(Integer.valueOf(k), atomicCache.get(k));
            assertEquals(Integer.valueOf(k), txCache.get(k));
        }

        assertEquals(new AffinityTopologyVersion(5, 0), grid(0).context().discovery().topologyVersionEx());
        assertEquals(new AffinityTopologyVersion(5, 0), grid(1).context().discovery().topologyVersionEx());
        assertEquals(new AffinityTopologyVersion(4, 3), grid(2).context().discovery().topologyVersionEx());
        assertEquals(new AffinityTopologyVersion(4, 3), grid(3).context().discovery().topologyVersionEx());

        latch.countDown();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoAffinityChangeOnClientLeft() throws Exception {
        Ignite ig = startGrids(4);

        ig.cluster().state(ACTIVE);

        IgniteCache<Integer, Integer> atomicCache = ig.createCache(new CacheConfiguration<Integer, Integer>()
            .setName("atomic").setAtomicityMode(CacheAtomicityMode.ATOMIC));

        IgniteCache<Integer, Integer> txCache = ig.createCache(new CacheConfiguration<Integer, Integer>()
            .setName("tx").setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        assertTrue(GridTestUtils.waitForCondition(() ->
            new AffinityTopologyVersion(4, 3).equals(grid(3).context().discovery().topologyVersionEx()),
            5_000));

        startClientGrid(4);

        TestDiscoverySpi discoSpi = (TestDiscoverySpi)grid(2).context().discovery().getInjectedDiscoverySpi();

        CountDownLatch latch = new CountDownLatch(1);

        discoSpi.latch = latch;

        stopGrid(4);

        assertTrue(GridTestUtils.waitForCondition(() ->
                new AffinityTopologyVersion(6, 0).equals(grid(0).context().discovery().topologyVersionEx()) &&
                    new AffinityTopologyVersion(6, 0).equals(grid(1).context().discovery().topologyVersionEx()) &&
                    new AffinityTopologyVersion(5, 0).equals(grid(2).context().discovery().topologyVersionEx()) &&
                    new AffinityTopologyVersion(5, 0).equals(grid(3).context().discovery().topologyVersionEx()),
            10_000));

        for (int k = 0; k < 100; k++) {
            atomicCache.put(k, k);
            txCache.put(k, k);

            Lock lock = txCache.lock(k);
            lock.lock();
            lock.unlock();
        }

        for (int k = 0; k < 100; k++) {
            assertEquals(Integer.valueOf(k), atomicCache.get(k));
            assertEquals(Integer.valueOf(k), txCache.get(k));
        }

        assertEquals(new AffinityTopologyVersion(6, 0), grid(0).context().discovery().topologyVersionEx());
        assertEquals(new AffinityTopologyVersion(6, 0), grid(1).context().discovery().topologyVersionEx());
        assertEquals(new AffinityTopologyVersion(5, 0), grid(2).context().discovery().topologyVersionEx());
        assertEquals(new AffinityTopologyVersion(5, 0), grid(3).context().discovery().topologyVersionEx());

        latch.countDown();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoAffinityChangeOnClientLeftWithMergedExchanges() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_EXCHANGE_MERGE_DELAY, "1000");

        try {
            Ignite ig = startGridsMultiThreaded(4);

            ig.cluster().state(ACTIVE);

            IgniteCache<Integer, Integer> atomicCache = ig.createCache(new CacheConfiguration<Integer, Integer>()
                .setName("atomic").setAtomicityMode(CacheAtomicityMode.ATOMIC).setCacheMode(CacheMode.REPLICATED));

            IgniteCache<Integer, Integer> txCache = ig.createCache(new CacheConfiguration<Integer, Integer>()
                .setName("tx").setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL).setCacheMode(CacheMode.REPLICATED));

            Ignite client = startClientGrid("client");

            stopGrid(1);
            stopGrid(2);
            stopGrid(3);

            awaitPartitionMapExchange();

            atomicCache.put(-1, -1);
            txCache.put(-1, -1);

            TestRecordingCommunicationSpi.spi(ig).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    return msg instanceof GridDhtPartitionSupplyMessageV2;
                }
            });

            startGridsMultiThreaded(1, 3);

            awaitPartitionMapExchange();

            CountDownLatch latch = new CountDownLatch(1);
            for (Ignite ignite : G.allGrids()) {
                if (ignite.cluster().localNode().order() == 9) {
                    TestDiscoverySpi discoSpi =
                        (TestDiscoverySpi)((IgniteEx)ignite).context().discovery().getInjectedDiscoverySpi();

                    discoSpi.latch = latch;

                    break;
                }
            }

            client.close();

            for (int k = 0; k < 100; k++) {
                atomicCache.put(k, k);
                txCache.put(k, k);

                Lock lock = txCache.lock(k);
                lock.lock();
                lock.unlock();
            }

            for (int k = 0; k < 100; k++) {
                assertEquals(Integer.valueOf(k), atomicCache.get(k));
                assertEquals(Integer.valueOf(k), txCache.get(k));
            }

            latch.countDown();
        }
        finally {
            System.clearProperty(IgniteSystemProperties.IGNITE_EXCHANGE_MERGE_DELAY);
        }
    }

    /**
     * Checks case when number of client events is greater than affinity history size.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_AFFINITY_HISTORY_SIZE, value = "10")
    public void testMulipleClientLeaveJoin() throws Exception {
        doTestMulipleClientLeaveJoin();
    }

    /**
     * Checks case when number of client events is so big that history consists only from client event versions.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_AFFINITY_HISTORY_SIZE, value = "2")
    public void testMulipleClientLeaveJoinLinksLimitOverflow() throws Exception {
        doTestMulipleClientLeaveJoin();
    }

    /**
     * Tests that multiple client events won't fail transactions due to affinity assignment history expiration.
     *
     * @throws Exception If failed.
     */
    public void doTestMulipleClientLeaveJoin() throws Exception {
        Ignite ig = startGrids(2);

        ig.cluster().state(ACTIVE);

        IgniteEx stableClient = startClientGrid(2);

        IgniteCache<Integer, Integer> stableClientTxCacheProxy = stableClient.createCache(
            new CacheConfiguration<Integer, Integer>()
                .setName("tx")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(1)
                .setAffinity(new RendezvousAffinityFunction(false, 32)));

        awaitPartitionMapExchange();

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < 10; i++) {
                    try {
                        startClientGrid(3);

                        stopGrid(3);
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });

        CountDownLatch clientTxLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> loadFut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try (Transaction tx = stableClient.transactions().txStart()) {
                    ThreadLocalRandom r = ThreadLocalRandom.current();

                    stableClientTxCacheProxy.put(r.nextInt(100), r.nextInt());

                    try {
                        clientTxLatch.await();
                    }
                    catch (InterruptedException e) {
                        throw new IgniteInterruptedException(e);
                    }

                    tx.commit();
                }
            }
        });

        fut.get();

        clientTxLatch.countDown();

        loadFut.get();
    }

     /**
      * @throws Exception If failed.
      */
    @Test
    public void testAffinityChangeOnClientConnectWithStaticallyConfiguredCaches() throws Exception {
        Ignite ig = startGrids(2);

        ig.cluster().state(ACTIVE);

        TestDiscoverySpi discoSpi = (TestDiscoverySpi)grid(1).context().discovery().getInjectedDiscoverySpi();

        CountDownLatch latch = new CountDownLatch(1);

        discoSpi.latch = latch;

        startClientCaches = true;

        Ignite client = startClientGrid(2);

        assertTrue(GridTestUtils.waitForCondition(() -> {
            AffinityTopologyVersion topVer0 = grid(0).context().discovery().topologyVersionEx();
            AffinityTopologyVersion topVer1 = grid(1).context().discovery().topologyVersionEx();

            return topVer0.topologyVersion() == 3 && topVer1.topologyVersion() == 2;
        }, 10_000));

        final IgniteCache<Integer, Integer> txCache = client.cache(PARTITIONED_TX_CLIENT_CACHE_NAME);

        final AtomicBoolean updated = new AtomicBoolean();

        GridTestUtils.runAsync(() -> {
            for (int i = 0; i < 32; ++i)
                txCache.put(i, i);

            updated.set(true);
        });

        assertFalse(GridTestUtils.waitForCondition(updated::get, 5_000));

        latch.countDown();

        assertTrue(GridTestUtils.waitForCondition(updated::get, 5_000));

        for (int i = 0; i < 32; ++i)
            assertEquals(Integer.valueOf(i), txCache.get(i));

        assertEquals("Expected major topology version is 3.",
            3, grid(1).context().discovery().topologyVersionEx().topologyVersion());
    }

    /**
     * Tests the "long" joining of the client node to the cluster
     * when the message from the client is handled after affinity assignments are wiped from the history.
     *
     * Expected result: client node should reconnect to the cluster, server nodes should not be affected in any way.
     *
     * @throws Exception If Failed.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_AFFINITY_HISTORY_SIZE, value = "2")
    public void testNoAffinityOnJoiningClientNode() throws Exception {
        Ignite ig = startGrids(1);

        ig.cluster().state(ACTIVE);

        clientCommSpi = new TestRecordingCommunicationSpi();

        clientCommSpi.blockMessages(
            GridDhtPartitionsSingleMessage.class,
            ig.configuration().getIgniteInstanceName());

        // Start client node and block initial PME in order to guarantee that single message will be processed
        // when affinity history for the corresponding topology version is already cleaned.
        IgniteInternalFuture<?> startClientFut = GridTestUtils.runAsync(() -> {
            try {
                startClientCaches = true;

                startClientGrid(1);
            }
            catch (Exception e) {
                throw new RuntimeException("Unexpected exception.", e);
            }
        });

        // Wait for initial PME.
        clientCommSpi.waitForBlocked();

        startClientCaches = false;

        // Start new server nodes in order to clean the history of affinity assignments.
        for (int i = 2; i < 5; i++) {
            startGrid(i);
            stopGrid(i, true);
        }

        // Send single message from the client.
        clientCommSpi.stopBlock();

        // Make sure that client node successfully started.
        startClientFut.get(getTestTimeout());

        awaitPartitionMapExchange();

        StringBuilder failures = new StringBuilder();
        errs.forEach((k, v) -> failures.append("nodeId=").append(k).append(", err=").append(v).append(U.nl()));

        assertTrue(
            "Failure handler should not be triggered " + failures.toString(),
            errs.isEmpty());
    }

    /**
     * Tests getting a cache on the client node when the history of affinity assignments is not enough.
     *
     * Expected result: client request should be rejected with the cause, server nodes should not be affected in any way.
     *
     * @throws Exception If Failed.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_AFFINITY_HISTORY_SIZE, value = "2")
    public void testNoAffinityOnClientCacheStart() throws Exception {
        Ignite ig = startGrids(1);

        ig.cluster().state(ACTIVE);

        ig.getOrCreateCache("client-cache");

        startClientCaches = false;

        IgniteEx client = startClientGrid(1);
        TestRecordingCommunicationSpi clientSpi = TestRecordingCommunicationSpi.spi(client);
        clientSpi.blockMessages((node, msg) -> msg instanceof GridDhtAffinityAssignmentRequest);

        // Block creating client cache in order to guarantee that GridDhtAffinityAssignmentRequest message
        // will be processed when affinity history for the corresponding topology version is already cleaned.
        IgniteInternalFuture<?> startClientCacheFut = GridTestUtils.runAsync(() -> {
            client.cache("client-cache");
        });

        clientSpi.waitForBlocked();

        startClientCaches = false;

        // Start new server nodes in order to clean the history of affinity assignments.
        for (int i = 2; i < 5; i++) {
            startGrid(i);
            stopGrid(i, true);
        }

        // Send GridDhtAffinityAssignmentRequest from the client.
        clientSpi.stopBlock();

        assertThrows(log, () -> startClientCacheFut.get(), IgniteCheckedException.class, null);

        awaitPartitionMapExchange();

        StringBuilder failures = new StringBuilder();
        errs.forEach((k, v) -> failures.append("nodeId=").append(k).append(", err=").append(v).append(U.nl()));

        assertTrue(
            "Failure handler should not be triggered " + failures.toString(),
            errs.isEmpty());

        // Make sure that the next call is successful.
        assertNotNull(client.cache("client-cache"));
    }

    /**
     *
     */
    public static class TestDiscoverySpi extends TcpDiscoverySpi {
        /** */
        private volatile CountDownLatch latch;

        /** {@inheritDoc} */
        @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
            if (msg instanceof TcpDiscoveryNodeAddFinishedMessage
                || msg instanceof TcpDiscoveryNodeLeftMessage
                || msg instanceof TcpDiscoveryNodeFailedMessage) {
                CountDownLatch latch0 = latch;

                if (latch0 != null)
                    try {
                        latch0.await();
                    }
                    catch (InterruptedException ex) {
                        throw new IgniteException(ex);
                    }
            }

            super.startMessageProcess(msg);
        }
    }

}
