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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridNodeOrderComparator;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityFunctionContextImpl;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessageV2;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_GRID_NAME;

/**
 *
 */
public class CacheLateAffinityAssignmentTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** */
    private boolean forceSrvMode;

    /** */
    private static final String CACHE_NAME1 = "testCache1";

    /** */
    private static final String CACHE_NAME2 = "testCache2";

    /** */
    private IgniteClosure<String, CacheConfiguration[]> cacheC;

    /** */
    private IgnitePredicate<ClusterNode> cacheNodeFilter;

    /** */
    private IgniteClosure<String, TestRecordingCommunicationSpi> spiC;

    /** */
    private IgniteClosure<String, Boolean> clientC;

    /** Expected ideal affinity assignments. */
    private Map<Long, Map<Integer, List<List<ClusterNode>>>> idealAff = new HashMap<>();

    /** */
    private boolean skipCheckOrder;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setLateAffinityAssignment(true);

        TestRecordingCommunicationSpi commSpi;

        if (spiC != null)
            commSpi = spiC.apply(gridName);
        else
            commSpi = new TestRecordingCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        TestTcpDiscoverySpi discoSpi = new TestTcpDiscoverySpi();

        discoSpi.setForceServerMode(forceSrvMode);
        discoSpi.setIpFinder(ipFinder);
        discoSpi.setMaxMissedClientHeartbeats(100);
        discoSpi.setNetworkTimeout(60_000);

        cfg.setDiscoverySpi(discoSpi);

        CacheConfiguration[] ccfg;

        if (cacheC != null)
            ccfg = cacheC.apply(gridName);
        else
            ccfg = new CacheConfiguration[]{cacheConfiguration()};

        if (ccfg != null)
            cfg.setCacheConfiguration(ccfg);

        if (clientC != null) {
            client = clientC.apply(gridName);

            discoSpi.setJoinTimeout(30_000);
        }

        cfg.setClientMode(client);

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(CACHE_NAME1);
        ccfg.setNodeFilter(cacheNodeFilter);
        ccfg.setAffinity(affinityFunction(null));
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicWriteOrderMode(PRIMARY);
        ccfg.setBackups(0);

        return ccfg;
    }

    /**
     * @param parts Number of partitions.
     * @return Affinity function.
     */
    protected AffinityFunction affinityFunction(@Nullable Integer parts) {
        return new RendezvousAffinityFunction(false,
            parts == null ? RendezvousAffinityFunction.DFLT_PARTITION_COUNT : parts);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try {
            checkCaches();
        }
        finally {
            stopAllGrids();
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * Checks that new joined primary is not assigned immediately.
     *
     * @throws Exception If failed.
     */
    public void testDelayedAffinityCalculation() throws Exception {
        Ignite ignite0 = startServer(0, 1);

        checkAffinity(1, topVer(1, 0), true);

        GridCacheContext cctx = ((IgniteKernal)ignite0).context().cache().internalCache(CACHE_NAME1).context();

        AffinityFunction func = cctx.config().getAffinity();

        AffinityFunctionContext ctx = new GridAffinityFunctionContextImpl(
            new ArrayList<>(ignite0.cluster().nodes()),
            null,
            null,
            topVer(1, 0),
            cctx.config().getBackups());

        List<List<ClusterNode>> calcAff1_0 = func.assignPartitions(ctx);

        startServer(1, 2);

        ctx = new GridAffinityFunctionContextImpl(
            new ArrayList<>(ignite0.cluster().nodes()),
            calcAff1_0,
            null,
            topVer(1, 0),
            cctx.config().getBackups());

        List<List<ClusterNode>> calcAff2_0 = func.assignPartitions(ctx);

        checkAffinity(2, topVer(2, 0), false);

        List<List<ClusterNode>> aff2_0 = affinity(ignite0, topVer(2, 0), CACHE_NAME1);

        for (int p = 0; p < calcAff1_0.size(); p++) {
            List<ClusterNode> a1 = calcAff1_0.get(p);
            List<ClusterNode> a2 = calcAff2_0.get(p);

            List<ClusterNode> a = aff2_0.get(p);

            // Primary did not change.
            assertEquals(a1.get(0), a.get(0));

            // New primary is backup.
            if (!a1.get(0).equals(a2.get(0)))
                assertTrue(a.contains(a2.get(0)));
        }

        checkAffinity(2, topVer(2, 1), true);

        List<List<ClusterNode>> aff2_1 = affinity(ignite0, topVer(2, 1), CACHE_NAME1);

        assertEquals(calcAff2_0, aff2_1);
    }

    /**
     * Simple test, node join.
     *
     * @throws Exception If failed.
     */
    public void testAffinitySimpleSequentialStart() throws Exception {
        startServer(0, 1);

        startServer(1, 2);

        checkAffinity(2, topVer(2, 0), false);

        checkAffinity(2, topVer(2, 1), true);

        startServer(2, 3);

        checkAffinity(3, topVer(3, 0), false);

        checkAffinity(3, topVer(3, 1), true);

        awaitPartitionMapExchange();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinitySimpleSequentialStartNoCacheOnCoordinator() throws Exception {
        cacheC = new IgniteClosure<String, CacheConfiguration[]>() {
            @Override public CacheConfiguration[] apply(String gridName) {
                if (gridName.equals(getTestGridName(0)))
                    return null;

                return new CacheConfiguration[]{cacheConfiguration()};
            }
        };

        cacheNodeFilter = new CacheNodeFilter(F.asList(getTestGridName(0)));

        testAffinitySimpleSequentialStart();

        assertNull(((IgniteKernal)ignite(0)).context().cache().internalCache(CACHE_NAME1));
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinitySimpleNoCacheOnCoordinator1() throws Exception {
        cacheC = new IgniteClosure<String, CacheConfiguration[]>() {
            @Override public CacheConfiguration[] apply(String gridName) {
                if (gridName.equals(getTestGridName(1)))
                    return null;

                return new CacheConfiguration[]{cacheConfiguration()};
            }
        };

        cacheNodeFilter = new CacheNodeFilter(F.asList(getTestGridName(1)));

        startServer(0, 1);

        startServer(1, 2);

        checkAffinity(2, topVer(2, 1), true);

        startServer(2, 3);

        startServer(3, 4);

        Map<String, List<List<ClusterNode>>> aff = checkAffinity(4, topVer(4, 1), true);

        stopGrid(0); // Kill coordinator, now coordinator node1 without cache.

        boolean primaryChanged = calculateAffinity(5, false, aff);

        checkAffinity(3, topVer(5, 0), !primaryChanged);

        if (primaryChanged)
            checkAffinity(3, topVer(5, 1), true);

        assertNull(((IgniteKernal)ignite(1)).context().cache().internalCache(CACHE_NAME1));
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinitySimpleNoCacheOnCoordinator2() throws Exception {
        cacheC = new IgniteClosure<String, CacheConfiguration[]>() {
            @Override public CacheConfiguration[] apply(String gridName) {
                if (gridName.equals(getTestGridName(1)) || gridName.equals(getTestGridName(2)))
                    return null;

                return new CacheConfiguration[]{cacheConfiguration()};
            }
        };

        cacheNodeFilter = new CacheNodeFilter(F.asList(getTestGridName(1), getTestGridName(2)));

        startServer(0, 1);
        startServer(1, 2);
        startServer(2, 3);
        startServer(3, 4);

        for (int i = 0; i < 4; i++) {
            TestRecordingCommunicationSpi spi =
                (TestRecordingCommunicationSpi)ignite(i).configuration().getCommunicationSpi();

            // Prevent exchange finish while node0 or node1 is coordinator.
            spi.blockMessages(GridDhtPartitionsSingleMessage.class, ignite(0).name());
            spi.blockMessages(GridDhtPartitionsSingleMessage.class, ignite(1).name());
        }

        stopGrid(0);
        stopGrid(1);

        calculateAffinity(5);
        calculateAffinity(6);

        checkAffinity(2, topVer(6, 0), true);

        assertNull(((IgniteKernal)ignite(2)).context().cache().internalCache(CACHE_NAME1));
        assertNotNull(((IgniteKernal)ignite(3)).context().cache().internalCache(CACHE_NAME1));

        assertNotNull(ignite(2).cache(CACHE_NAME1));

        checkAffinity(2, topVer(6, 0), true);

        startServer(4, 7);

        checkAffinity(3, topVer(7, 0), false);

        checkAffinity(3, topVer(7, 1), true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateCloseClientCacheOnCoordinator1() throws Exception {
        cacheC = new IgniteClosure<String, CacheConfiguration[]>() {
            @Override public CacheConfiguration[] apply(String gridName) {
                return null;
            }
        };

        cacheNodeFilter = new CacheNodeFilter(F.asList(getTestGridName(0)));

        Ignite ignite0 = startServer(0, 1);

        ignite0.createCache(cacheConfiguration());

        ignite0.cache(CACHE_NAME1);

        ignite0.cache(CACHE_NAME1).close();

        startServer(1, 2);

        startServer(2, 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateCloseClientCacheOnCoordinator2() throws Exception {
        cacheC = new IgniteClosure<String, CacheConfiguration[]>() {
            @Override public CacheConfiguration[] apply(String gridName) {
                if (gridName.equals(getTestGridName(0)))
                    return null;

                return new CacheConfiguration[]{cacheConfiguration()};
            }
        };

        cacheNodeFilter = new CacheNodeFilter(F.asList(getTestGridName(0)));

        Ignite ignite0 = startServer(0, 1);

        int topVer = 1;

        int nodes = 1;

        for (int i = 0;  i < 3; i++) {
            log.info("Iteration [iter=" + i + ", topVer=" + topVer + ']');

            topVer++;

            startServer(nodes++, topVer);

            checkAffinity(nodes, topVer(topVer, 1), true);

            ignite0.cache(CACHE_NAME1);

            checkAffinity(nodes, topVer(topVer, 2), true);

            topVer++;

            startServer(nodes++, topVer);

            checkAffinity(nodes, topVer(topVer, 1), true);

            ignite0.cache(CACHE_NAME1).close();

            checkAffinity(nodes, topVer(topVer, 2), true);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheDestroyAndCreate1() throws Exception {
        cacheDestroyAndCreate(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheDestroyAndCreate2() throws Exception {
        cacheDestroyAndCreate(false);
    }

    /**
     * @param cacheOnCrd If {@code false} does not create cache on coordinator.
     * @throws Exception If failed.
     */
    private void cacheDestroyAndCreate(boolean cacheOnCrd) throws Exception {
        if (!cacheOnCrd)
            cacheNodeFilter = new CacheNodeFilter(Collections.singletonList(getTestGridName(0)));

        startServer(0, 1);

        startServer(1, 2);

        startServer(2, 3);

        checkAffinity(3, topVer(3, 1), true);

        startClient(3, 4);

        checkAffinity(4, topVer(4, 0), true);

        CacheConfiguration ccfg = cacheConfiguration();
        ccfg.setName(CACHE_NAME2);

        ignite(1).createCache(ccfg);

        calculateAffinity(4);

        checkAffinity(4, topVer(4, 1), true);

        ignite(1).destroyCache(CACHE_NAME2);

        idealAff.get(4L).remove(CU.cacheId(CACHE_NAME2));

        ccfg = cacheConfiguration();
        ccfg.setName(CACHE_NAME2);
        ccfg.setAffinity(affinityFunction(10));

        ignite(1).createCache(ccfg);

        calculateAffinity(4);

        checkAffinity(4, topVer(4, 3), true);

        checkCaches();

        ignite(1).destroyCache(CACHE_NAME2);

        idealAff.get(4L).remove(CU.cacheId(CACHE_NAME2));

        ccfg = cacheConfiguration();
        ccfg.setName(CACHE_NAME2);
        ccfg.setAffinity(affinityFunction(20));

        ignite(1).createCache(ccfg);

        calculateAffinity(4);

        checkAffinity(4, topVer(4, 5), true);
    }

    /**
     * Simple test, node leaves.
     *
     * @throws Exception If failed.
     */
    public void testAffinitySimpleNodeLeave() throws Exception {
        startServer(0, 1);

        startServer(1, 2);

        checkAffinity(2, topVer(2, 0), false);

        checkAffinity(2, topVer(2, 1), true);

        stopNode(1, 3);

        checkAffinity(1, topVer(3, 0), true);

        checkNoExchange(1, topVer(3, 1));

        awaitPartitionMapExchange();
    }

    /**
     * Simple test, node leaves.
     *
     * @throws Exception If failed.
     */
    public void testAffinitySimpleNodeLeaveClientAffinity() throws Exception {
        startServer(0, 1);

        startServer(1, 2);

        checkAffinity(2, topVer(2, 1), true);

        startClient(2, 3);

        checkAffinity(3, topVer(3, 0), true);

        stopNode(1, 4);

        checkAffinity(2, topVer(4, 0), true);

        awaitPartitionMapExchange();
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeLeaveExchangeWaitAffinityMessage() throws Exception {
        Ignite ignite0 = startServer(0, 1);

        startServer(1, 2);

        startServer(2, 3);

        checkAffinity(3, topVer(3, 1), true);

        checkOrderCounters(3, topVer(3, 1));

        startClient(3, 4);

        checkAffinity(4, topVer(4, 0), true);

        TestTcpDiscoverySpi discoSpi = (TestTcpDiscoverySpi)ignite0.configuration().getDiscoverySpi();

        discoSpi.blockCustomEvent();

        stopGrid(1);

        List<IgniteInternalFuture<?>> futs = affFutures(3, topVer(5, 0));

        U.sleep(1000);

        for (IgniteInternalFuture<?> fut : futs)
            assertFalse(fut.isDone());

        discoSpi.stopBlock();

        checkAffinity(3, topVer(5, 0), false);

        checkOrderCounters(3, topVer(5, 0));
    }

    /**
     * Simple test, client node joins/leaves.
     *
     * @throws Exception If failed.
     */
    public void testAffinitySimpleClientNodeEvents1() throws Exception {
        affinitySimpleClientNodeEvents(1);
    }

    /**
     * Simple test, client node joins/leaves.
     *
     * @throws Exception If failed.
     */
    public void testAffinitySimpleClientNodeEvents2() throws Exception {
        affinitySimpleClientNodeEvents(3);
    }

    /**
     * Simple test, client node joins/leaves.
     *
     * @param srvs Number of server nodes.
     * @throws Exception If failed.
     */
    private void affinitySimpleClientNodeEvents(int srvs) throws Exception {
        long topVer = 0;

        for (int i = 0; i < srvs; i++)
            startServer(i, ++topVer);

        if (srvs == 1)
            checkAffinity(srvs, topVer(srvs, 0), true);
        else
            checkAffinity(srvs, topVer(srvs, 1), true);

        startClient(srvs, ++topVer);

        checkAffinity(srvs + 1, topVer(srvs + 1, 0), true);

        stopNode(srvs, ++topVer);

        checkAffinity(srvs, topVer(srvs + 2, 0), true);
    }

    /**
     * Wait for rebalance, 2 nodes join.
     *
     * @throws Exception If failed.
     */
    public void testDelayAssignmentMultipleJoin1() throws Exception {
        delayAssignmentMultipleJoin(2);
    }

    /**
     * Wait for rebalance, 4 nodes join.
     *
     * @throws Exception If failed.
     */
    public void testDelayAssignmentMultipleJoin2() throws Exception {
        delayAssignmentMultipleJoin(4);
    }

    /**
     * @param joinCnt Number of joining nodes.
     * @throws Exception If failed.
     */
    private void delayAssignmentMultipleJoin(int joinCnt) throws Exception {
        Ignite ignite0 = startServer(0, 1);

        TestRecordingCommunicationSpi spi =
            (TestRecordingCommunicationSpi)ignite0.configuration().getCommunicationSpi();

        blockSupplySend(spi, CACHE_NAME1);

        int majorVer = 1;

        for (int i = 0; i < joinCnt; i++) {
            majorVer++;

            startServer(i + 1, majorVer);

            checkAffinity(majorVer, topVer(majorVer, 0), false);
        }

        List<IgniteInternalFuture<?>> futs = affFutures(majorVer, topVer(majorVer, 1));

        U.sleep(1000);

        for (IgniteInternalFuture<?> fut : futs)
            assertFalse(fut.isDone());

        spi.stopBlock();

        checkAffinity(majorVer, topVer(majorVer, 1), true);

        for (IgniteInternalFuture<?> fut : futs)
            assertTrue(fut.isDone());

        awaitPartitionMapExchange();
    }

    /**
     * Wait for rebalance, client node joins.
     *
     * @throws Exception If failed.
     */
    public void testDelayAssignmentClientJoin() throws Exception {
        Ignite ignite0 = startServer(0, 1);

        TestRecordingCommunicationSpi spi =
            (TestRecordingCommunicationSpi)ignite0.configuration().getCommunicationSpi();

        blockSupplySend(spi, CACHE_NAME1);

        startServer(1, 2);

        startClient(2, 3);

        checkAffinity(3, topVer(3, 0), false);

        spi.stopBlock();

        checkAffinity(3, topVer(3, 1), true);
    }

    /**
     * Wait for rebalance, client node leaves.
     *
     * @throws Exception If failed.
     */
    public void testDelayAssignmentClientLeave() throws Exception {
        Ignite ignite0 = startServer(0, 1);

        startClient(1, 2);

        checkAffinity(2, topVer(2, 0), true);

        TestRecordingCommunicationSpi spi =
            (TestRecordingCommunicationSpi)ignite0.configuration().getCommunicationSpi();

        blockSupplySend(spi, CACHE_NAME1);

        startServer(2, 3);

        checkAffinity(3, topVer(3, 0), false);

        stopNode(1, 4);

        checkAffinity(2, topVer(4, 0), false);

        spi.stopBlock();

        checkAffinity(2, topVer(4, 1), true);
    }

    /**
     * Wait for rebalance, client cache is started.
     *
     * @throws Exception If failed.
     */
    public void testDelayAssignmentClientCacheStart() throws Exception {
        Ignite ignite0 = startServer(0, 1);

        TestRecordingCommunicationSpi spi =
                (TestRecordingCommunicationSpi)ignite0.configuration().getCommunicationSpi();

        blockSupplySend(spi, CACHE_NAME1);

        startServer(1, 2);

        startServer(2, 3);

        cacheC = new IgniteClosure<String, CacheConfiguration[]>() {
            @Override public CacheConfiguration[] apply(String nodeName) {
                return null;
            }
        };

        Ignite client = startClient(3, 4);

        checkAffinity(4, topVer(4, 0), false);

        assertNotNull(client.cache(CACHE_NAME1));

        checkAffinity(4, topVer(4, 1), false);

        spi.stopBlock();

        checkAffinity(4, topVer(4, 2), true);
    }

    /**
     * Wait for rebalance, cache is started.
     *
     * @throws Exception If failed.
     */
    public void testDelayAssignmentCacheStart() throws Exception {
        Ignite ignite0 = startServer(0, 1);

        TestRecordingCommunicationSpi spi =
            (TestRecordingCommunicationSpi)ignite0.configuration().getCommunicationSpi();

        blockSupplySend(spi, CACHE_NAME1);

        startServer(1, 2);

        startServer(2, 3);

        checkAffinity(3, topVer(3, 0), false);

        CacheConfiguration ccfg = cacheConfiguration();

        ccfg.setName(CACHE_NAME2);

        ignite0.createCache(ccfg);

        calculateAffinity(3);

        checkAffinity(3, topVer(3, 1), false);

        spi.stopBlock();

        checkAffinity(3, topVer(3, 2), true);
    }

    /**
     * Wait for rebalance, cache is destroyed.
     *
     * @throws Exception If failed.
     */
    public void testDelayAssignmentCacheDestroy() throws Exception {
        Ignite ignite0 = startServer(0, 1);

        CacheConfiguration ccfg = cacheConfiguration();

        ccfg.setName(CACHE_NAME2);

        ignite0.createCache(ccfg);

        TestRecordingCommunicationSpi spi =
            (TestRecordingCommunicationSpi)ignite0.configuration().getCommunicationSpi();

        blockSupplySend(spi, CACHE_NAME2);

        startServer(1, 2);

        startServer(2, 3);

        checkAffinity(3, topVer(3, 0), false);

        ignite0.destroyCache(CACHE_NAME2);

        checkAffinity(3, topVer(3, 1), false);

        checkAffinity(3, topVer(3, 2), true);

        spi.stopBlock();
    }

    /**
     * Simple test, stop random node.
     *
     * @throws Exception If failed.
     */
    public void testAffinitySimpleStopRandomNode() throws Exception {
        final int ITERATIONS = 3;

        for (int iter = 0; iter < 3; iter++) {
            log.info("Iteration: " + iter);

            final int NODES = 5;

            for (int i = 0 ; i < NODES; i++)
                startServer(i, i + 1);

            int majorVer = NODES;

            checkAffinity(majorVer, topVer(majorVer, 1), true);

            Set<Integer> stopOrder = new HashSet<>();

            while (stopOrder.size() != NODES - 1)
                stopOrder.add(ThreadLocalRandom.current().nextInt(NODES));

            int nodes = NODES;

            for (Integer idx : stopOrder) {
                log.info("Stop node: " + idx);

                majorVer++;

                stopNode(idx, majorVer);

                checkAffinity(--nodes, topVer(majorVer, 0), false);

                awaitPartitionMapExchange();
            }

            if (iter < ITERATIONS - 1) {
                stopAllGrids();

                idealAff.clear();
            }
        }
    }

    /**
     * Wait for rebalance, coordinator leaves, 2 nodes.
     *
     * @throws Exception If failed.
     */
    public void testDelayAssignmentCoordinatorLeave1() throws Exception {
        Ignite ignite0 = startServer(0, 1);

        TestRecordingCommunicationSpi spi =
            (TestRecordingCommunicationSpi)ignite0.configuration().getCommunicationSpi();

        blockSupplySend(spi, CACHE_NAME1);

        startServer(1, 2);

        stopNode(0, 3);

        checkAffinity(1, topVer(3, 0), true);

        checkNoExchange(1, topVer(3, 1));

        awaitPartitionMapExchange();
    }

    /**
     * Wait for rebalance, coordinator leaves, 3 nodes.
     *
     * @throws Exception If failed.
     */
    public void testDelayAssignmentCoordinatorLeave2() throws Exception {
        Ignite ignite0 = startServer(0, 1);

        Ignite ignite1 = startServer(1, 2);

        checkAffinity(2, topVer(2, 1), true);

        TestRecordingCommunicationSpi spi0 =
            (TestRecordingCommunicationSpi)ignite0.configuration().getCommunicationSpi();
        TestRecordingCommunicationSpi spi1 =
            (TestRecordingCommunicationSpi)ignite1.configuration().getCommunicationSpi();

        blockSupplySend(spi0, CACHE_NAME1);
        blockSupplySend(spi1, CACHE_NAME1);

        startServer(2, 3);

        stopNode(0, 4);

        checkAffinity(2, topVer(4, 0), false);

        spi1.stopBlock();

        checkAffinity(2, topVer(4, 1), true);
    }

    /**
     * Coordinator leaves during node leave exchange.
     *
     * @throws Exception If failed.
     */
    public void testNodeLeftExchangeCoordinatorLeave1() throws Exception {
        nodeLeftExchangeCoordinatorLeave(3);
    }

    /**
     * Coordinator leaves during node leave exchange.
     *
     * @throws Exception If failed.
     */
    public void testNodeLeftExchangeCoordinatorLeave2() throws Exception {
        nodeLeftExchangeCoordinatorLeave(5);
    }

    /**
     * @param nodes Number of nodes.
     * @throws Exception If failed.
     */
    private void nodeLeftExchangeCoordinatorLeave(int nodes) throws Exception {
        assert nodes > 2 : nodes;

        long topVer = 0;

        for (int i = 0; i < nodes; i++)
            startServer(i, ++topVer);

        Ignite ignite1 = grid(1);

        checkAffinity(nodes, topVer(nodes, 1), true);

        TestRecordingCommunicationSpi spi1 =
            (TestRecordingCommunicationSpi)ignite1.configuration().getCommunicationSpi();

        // Prevent exchange finish while node0 is coordinator.
        spi1.blockMessages(GridDhtPartitionsSingleMessage.class, ignite(0).name());

        stopNode(2, ++topVer); // New exchange started.

        stopGrid(0); // Stop coordinator while exchange in progress.

        Map<String, List<List<ClusterNode>>> aff = checkAffinity(nodes - 2, topVer(topVer, 0), false);

        topVer++;

        boolean primaryChanged = calculateAffinity(nodes + 2, false, aff);

        checkAffinity(nodes - 2, topVer(topVer, 0), !primaryChanged);

        if (primaryChanged)
            checkAffinity(nodes - 2, topVer(topVer, 1), true);

        awaitPartitionMapExchange();
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinExchangeBecomeCoordinator() throws Exception {
        long topVer = 0;

        final int NODES = 3;

        for (int i = 0; i < NODES; i++)
            startServer(i, ++topVer);

        checkAffinity(NODES, topVer(topVer, 1), true);

        for (int i = 0; i < NODES; i++) {
            TestRecordingCommunicationSpi spi =
                    (TestRecordingCommunicationSpi)ignite(i).configuration().getCommunicationSpi();

            spi.blockMessages(new IgnitePredicate<GridIoMessage>() {
                @Override public boolean apply(GridIoMessage msg) {
                    Message msg0 = msg.message();

                    return msg0.getClass().equals(GridDhtPartitionsSingleMessage.class) ||
                        msg0.getClass().equals(GridDhtPartitionsFullMessage.class);
                }
            });
        }

        final CountDownLatch latch = new CountDownLatch(1);

        IgniteInternalFuture<?> stopFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                latch.await();

                U.sleep(5000);

                for (int i = 0; i < NODES; i++)
                    stopGrid(i);

                return null;
            }
        }, "stop-thread");


        latch.countDown();

        Ignite node = startGrid(NODES);

        assertEquals(NODES + 1, node.cluster().localNode().order());

        stopFut.get();

        for (int i = 0; i < NODES + 1; i++)
            calculateAffinity(++topVer);

        checkAffinity(1, topVer(topVer, 0), true);

        for (int i = 0; i < NODES; i++)
            startServer(i, ++topVer);

        checkAffinity(NODES + 1, topVer(topVer, 1), true);
    }

    /**
     * Wait for rebalance, send affinity change message, but affinity already changed (new node joined).
     *
     * @throws Exception If failed.
     */
    public void testDelayAssignmentAffinityChanged() throws Exception {
        Ignite ignite0 = startServer(0, 1);

        TestTcpDiscoverySpi discoSpi0 =
            (TestTcpDiscoverySpi)ignite0.configuration().getDiscoverySpi();
        TestRecordingCommunicationSpi commSpi0 =
            (TestRecordingCommunicationSpi)ignite0.configuration().getCommunicationSpi();

        startClient(1, 2);

        checkAffinity(2, topVer(2, 0), true);

        discoSpi0.blockCustomEvent();

        startServer(2, 3);

        checkAffinity(3, topVer(3, 0), false);

        discoSpi0.waitCustomEvent();

        blockSupplySend(commSpi0, CACHE_NAME1);

        startServer(3, 4);

        discoSpi0.stopBlock();

        checkAffinity(4, topVer(4, 0), false);

        checkNoExchange(4, topVer(4, 1));

        commSpi0.stopBlock();

        checkAffinity(4, topVer(4, 1), true);
    }

    /**
     * Wait for rebalance, cache is destroyed and created again.
     *
     * @throws Exception If failed.
     */
    public void testDelayAssignmentCacheDestroyCreate() throws Exception {
        Ignite ignite0 = startServer(0, 1);

        CacheConfiguration ccfg = cacheConfiguration();

        ccfg.setName(CACHE_NAME2);

        ignite0.createCache(ccfg);

        TestTcpDiscoverySpi discoSpi0 =
            (TestTcpDiscoverySpi)ignite0.configuration().getDiscoverySpi();
        TestRecordingCommunicationSpi spi =
            (TestRecordingCommunicationSpi)ignite0.configuration().getCommunicationSpi();

        blockSupplySend(spi, CACHE_NAME2);

        discoSpi0.blockCustomEvent();

        startServer(1, 2);

        startGrid(3);

        checkAffinity(3, topVer(3, 0), false);

        spi.stopBlock();

        discoSpi0.waitCustomEvent();

        ignite0.destroyCache(CACHE_NAME2);

        ccfg = cacheConfiguration();
        ccfg.setName(CACHE_NAME2);
        ccfg.setAffinity(affinityFunction(10));

        ignite0.createCache(ccfg);

        discoSpi0.stopBlock();

        checkAffinity(3, topVer(3, 1), false);
        checkAffinity(3, topVer(3, 2), false);

        idealAff.get(2L).remove(CU.cacheId(CACHE_NAME2));

        calculateAffinity(3);

        checkAffinity(3, topVer(3, 3), true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientCacheStartClose() throws Exception {
        cacheC = new IgniteClosure<String, CacheConfiguration[]>() {
            @Override public CacheConfiguration[] apply(String gridName) {
                if (gridName.equals(getTestGridName(1)))
                    return null;

                return new CacheConfiguration[]{cacheConfiguration()};
            }
        };

        startServer(0, 1);

        Ignite client = startClient(1, 2);

        checkAffinity(2, topVer(2, 0), true);

        IgniteCache cache = client.cache(CACHE_NAME1);

        checkAffinity(2, topVer(2, 1), true);

        cache.close();

        checkAffinity(2, topVer(2, 2), true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheStartDestroy() throws Exception {
        startGridsMultiThreaded(3, false);

        for (int i = 0; i < 3; i++)
            calculateAffinity(i + 1);

        checkAffinity(3, topVer(3, 1), true);

        Ignite client = startClient(3, 4);

        checkAffinity(4, topVer(4, 0), true);

        CacheConfiguration ccfg = cacheConfiguration();

        ccfg.setName(CACHE_NAME2);

        ignite(0).createCache(ccfg);

        calculateAffinity(4);

        checkAffinity(4, topVer(4, 1), true);

        client.cache(CACHE_NAME2);

        checkAffinity(4, topVer(4, 2), true);

        client.destroyCache(CACHE_NAME2);

        checkAffinity(4, topVer(4, 3), true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInitCacheReceivedOnJoin() throws Exception {
        cacheC = new IgniteClosure<String, CacheConfiguration[]>() {
            @Override public CacheConfiguration[] apply(String s) {
                return null;
            }
        };

        startServer(0, 1);

        startServer(1, 2);

        checkAffinity(2, topVer(2, 1), true);

        cacheC = new IgniteClosure<String, CacheConfiguration[]>() {
            @Override public CacheConfiguration[] apply(String s) {
                return new CacheConfiguration[]{cacheConfiguration()};
            }
        };

        startServer(2, 3);

        checkAffinity(3, topVer(3, 0), false);

        checkAffinity(3, topVer(3, 1), true);

        cacheC = new IgniteClosure<String, CacheConfiguration[]>() {
            @Override public CacheConfiguration[] apply(String s) {
                CacheConfiguration ccfg = cacheConfiguration();

                ccfg.setName(CACHE_NAME2);

                return new CacheConfiguration[]{ccfg};
            }
        };

        startClient(3, 4);

        checkAffinity(4, topVer(4, 0), true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientStartFirst1() throws Exception {
        clientStartFirst(1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientStartFirst2() throws Exception {
        clientStartFirst(3);
    }

    /**
     * @param clients Number of client nodes.
     * @throws Exception If failed.
     */
    private void clientStartFirst(int clients) throws Exception {
        forceSrvMode = true;

        int topVer = 0;

        for (int i = 0; i < clients; i++)
            startClient(topVer, ++topVer);

        cacheC = new IgniteClosure<String, CacheConfiguration[]>() {
            @Override public CacheConfiguration[] apply(String nodeName) {
                return null;
            }
        };

        startServer(topVer, ++topVer);

        checkAffinity(topVer, topVer(topVer, 0), true);

        startServer(topVer, ++topVer);

        checkAffinity(topVer, topVer(topVer, 0), false);

        checkAffinity(topVer, topVer(topVer, 1), true);

        stopNode(clients, ++topVer);

        checkAffinity(clients + 1, topVer(topVer, 0), true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandomOperations() throws Exception {
        forceSrvMode = true;

        final int MAX_SRVS = 10;
        final int MAX_CLIENTS = 10;
        final int MAX_CACHES = 15;

        List<String> srvs = new ArrayList<>();
        List<String> clients = new ArrayList<>();

        int srvIdx = 0;
        int clientIdx = 0;
        int cacheIdx = 0;

        List<String> caches = new ArrayList<>();

        long seed = System.currentTimeMillis();

        Random rnd = new Random(seed);

        log.info("Random seed: " + seed);

        long topVer = 0;

        for (int i = 0; i < 100; i++) {
            int op = i == 0 ? 0 : rnd.nextInt(7);

            log.info("Iteration [iter=" + i + ", op=" + op + ']');

            switch (op) {
                case 0: {
                    if (srvs.size() < MAX_SRVS) {
                        srvIdx++;

                        String srvName = "server-" + srvIdx;

                        log.info("Start server: " + srvName);

                        if (rnd.nextBoolean()) {
                            cacheIdx++;

                            String cacheName = "join-cache-" + cacheIdx;

                            log.info("Cache for joining node: " + cacheName);

                            cacheClosure(rnd, caches, cacheName, srvs, srvIdx);
                        }
                        else
                            cacheClosure(rnd, caches, null, srvs, srvIdx);

                        startNode(srvName, ++topVer, false);

                        srvs.add(srvName);
                    }
                    else
                        log.info("Skip start server.");

                    break;
                }

                case 1: {
                    if (srvs.size() > 1) {
                        String srvName = srvs.get(rnd.nextInt(srvs.size()));

                        log.info("Stop server: " + srvName);

                        stopNode(srvName, ++topVer);

                        srvs.remove(srvName);
                    }
                    else
                        log.info("Skip stop server.");

                    break;
                }

                case 2: {
                    if (clients.size() < MAX_CLIENTS) {
                        clientIdx++;

                        String clientName = "client-" + clientIdx;

                        log.info("Start client: " + clientName);

                        if (rnd.nextBoolean()) {
                            cacheIdx++;

                            String cacheName = "join-cache-" + cacheIdx;

                            log.info("Cache for joining node: " + cacheName);

                            cacheClosure(rnd, caches, cacheName, srvs, srvIdx);
                        }
                        else
                            cacheClosure(rnd, caches, null, srvs, srvIdx);

                        startNode(clientName, ++topVer, true);

                        clients.add(clientName);
                    }
                    else
                        log.info("Skip start client.");

                    break;
                }

                case 3: {
                    if (clients.size() > 1) {
                        String clientName = clients.get(rnd.nextInt(clients.size()));

                        log.info("Stop client: " + clientName);

                        stopNode(clientName, ++topVer);

                        clients.remove(clientName);
                    }
                    else
                        log.info("Skip stop client.");

                    break;
                }

                case 4: {
                    if (caches.size() > 0) {
                        String cacheName = caches.get(rnd.nextInt(caches.size()));

                        Ignite node = randomNode(rnd, srvs, clients);

                        log.info("Destroy cache [cache=" + cacheName + ", node=" + node.name() + ']');

                        node.destroyCache(cacheName);

                        caches.remove(cacheName);
                    }
                    else
                        log.info("Skip destroy cache.");

                    break;
                }

                case 5: {
                    if (caches.size() < MAX_CACHES) {
                        cacheIdx++;

                        String cacheName = "cache-" + cacheIdx;

                        Ignite node = randomNode(rnd, srvs, clients);

                        log.info("Create cache [cache=" + cacheName + ", node=" + node.name() + ']');

                        node.createCache(randomCacheConfiguration(rnd, cacheName, srvs, srvIdx));

                        calculateAffinity(topVer);

                        caches.add(cacheName);
                    }
                    else
                        log.info("Skip create cache.");

                    break;
                }

                case 6: {
                    if (caches.size() > 0) {
                        for (int j = 0; j < 3; j++) {
                            String cacheName = caches.get(rnd.nextInt(caches.size()));

                            for (int k = 0; k < 3; k++) {
                                Ignite node = randomNode(rnd, srvs, clients);

                                log.info("Get/closes cache [cache=" + cacheName + ", node=" + node.name() + ']');

                                node.cache(cacheName).close();
                            }
                        }
                    }
                    else
                        log.info("Skip get/close cache.");

                    break;
                }

                default:
                    fail();
            }

            IgniteKernal node = (IgniteKernal)grid(srvs.get(0));

            checkAffinity(srvs.size() + clients.size(),
                node.context().cache().context().exchange().readyAffinityVersion(),
                false);
        }

        srvIdx++;

        String srvName = "server-" + srvIdx;

        log.info("Start server: " + srvName);

        cacheClosure(rnd, caches, null, srvs, srvIdx);

        startNode(srvName, ++topVer, false);

        srvs.add(srvName);

        checkAffinity(srvs.size() + clients.size(), topVer(topVer, 1), true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentStartStaticCaches() throws Exception {
        concurrentStartStaticCaches(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentStartStaticCachesWithClientNodes() throws Exception {
        concurrentStartStaticCaches(true);
    }

    /**
     * @param withClients If {@code true} also starts client nodes.
     * @throws Exception If failed.
     */
    private void concurrentStartStaticCaches(boolean withClients) throws Exception {
        cacheC = new IgniteClosure<String, CacheConfiguration[]>() {
            @Override public CacheConfiguration[] apply(String gridName) {
                int caches = getTestGridIndex(gridName) + 1;

                CacheConfiguration[] ccfgs = new CacheConfiguration[caches];

                for (int i = 0; i < caches; i++) {
                    CacheConfiguration ccfg = cacheConfiguration();

                    ccfg.setName("cache-" + i);

                    ccfgs[i] = ccfg;
                }

                return ccfgs;
            }
        };

        if (withClients) {
            clientC = new IgniteClosure<String, Boolean>() {
                @Override public Boolean apply(String gridName) {
                    int idx = getTestGridIndex(gridName);

                    return idx % 3 == 2;
                }
            };
        }

        int ITERATIONS = 3;

        int NODES = withClients ? 8 : 5;

        for (int i = 0; i < ITERATIONS; i++) {
            log.info("Iteration: " + i);

            startGridsMultiThreaded(NODES);

            for (int t = 0; t < NODES; t++)
                calculateAffinity(t + 1, true, null);

            if (withClients) {
                skipCheckOrder = true;

                checkAffinity(NODES, topVer(NODES, 0), false);
            }
            else
                checkAffinity(NODES, topVer(NODES, 1), true);

            if (i < ITERATIONS - 1) {
                checkCaches();

                awaitPartitionMapExchange();

                stopAllGrids();

                idealAff.clear();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testServiceReassign() throws Exception {
        skipCheckOrder = true;

        Ignite ignite0 = startServer(0, 1);

        IgniteServices svcs = ignite0.services();

        for (int i = 0; i < 10; i++)
            svcs.deployKeyAffinitySingleton("service-" + i, new TestServiceImpl(i), CACHE_NAME1, i);

        startServer(1, 2);

        startServer(2, 3);

        Map<String, List<List<ClusterNode>>> assignments = checkAffinity(3, topVer(3, 1), true);

        checkServicesDeploy(ignite(0), assignments.get(CACHE_NAME1));

        stopGrid(0);

        boolean primaryChanged = calculateAffinity(4, false, assignments);

        assignments = checkAffinity(2, topVer(4, 0), !primaryChanged);

        if (primaryChanged)
            checkAffinity(2, topVer(4, 1), true);

        checkServicesDeploy(ignite(1), assignments.get(CACHE_NAME1));
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoForceKeysRequests() throws Exception {
        cacheC = new IgniteClosure<String, CacheConfiguration[]>() {
            @Override public CacheConfiguration[] apply(String s) {
                return null;
            }
        };

        final AtomicBoolean fail = new AtomicBoolean();

        spiC = new IgniteClosure<String, TestRecordingCommunicationSpi>() {
            @Override public TestRecordingCommunicationSpi apply(String s) {
                TestRecordingCommunicationSpi spi = new TestRecordingCommunicationSpi();

                spi.blockMessages(new IgnitePredicate<GridIoMessage>() {
                    @Override public boolean apply(GridIoMessage msg) {
                        Message msg0 = msg.message();

                        if (msg0 instanceof GridDhtForceKeysRequest || msg0 instanceof GridDhtForceKeysResponse) {
                            fail.set(true);

                            U.dumpStack(log, "Unexpected message: " + msg0);
                        }

                        return false;
                    }
                });

                return spi;
            }
        };

        final int SRVS = 3;

        for (int i = 0; i < SRVS; i++)
            startGrid(i);

        client = true;

        startGrid(SRVS);

        client = false;

        final List<CacheConfiguration> ccfgs = new ArrayList<>();

        ccfgs.add(cacheConfiguration("tc1", TRANSACTIONAL, 0));
        ccfgs.add(cacheConfiguration("tc2", TRANSACTIONAL, 1));
        ccfgs.add(cacheConfiguration("tc3", TRANSACTIONAL, 2));

        for (CacheConfiguration ccfg : ccfgs)
            ignite(0).createCache(ccfg);

        final int NODES = SRVS + 1;

        final AtomicInteger nodeIdx = new AtomicInteger();

        final long stopTime = System.currentTimeMillis() + 60_000;

        IgniteInternalFuture<?> updateFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                int idx = nodeIdx.getAndIncrement();

                Ignite node = grid(idx);

                List<IgniteCache<Object, Object>> caches = new ArrayList<>();

                for (CacheConfiguration ccfg : ccfgs)
                    caches.add(node.cache(ccfg.getName()));

                while (!fail.get() && System.currentTimeMillis() < stopTime) {
                    for (IgniteCache<Object, Object> cache : caches)
                        cacheOperations(cache);
                }

                return null;
            }
        }, NODES, "update-thread");

        IgniteInternalFuture<?> srvRestartFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                while (!fail.get() && System.currentTimeMillis() < stopTime) {
                    Ignite node = startGrid(NODES);

                    List<IgniteCache<Object, Object>> caches = new ArrayList<>();

                    for (CacheConfiguration ccfg : ccfgs)
                        caches.add(node.cache(ccfg.getName()));

                    for (int i = 0; i < 2; i++) {
                        for (IgniteCache<Object, Object> cache : caches)
                            cacheOperations(cache);
                    }

                    U.sleep(500);

                    stopGrid(NODES);

                    U.sleep(500);
                }

                return null;
            }
        }, "srv-restart");

        srvRestartFut.get();
        updateFut.get();

        assertFalse("Unexpected messages.", fail.get());
    }

    /**
     * @param cache Cache
     */
    private void cacheOperations(IgniteCache<Object, Object> cache) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        final int KEYS = 10_000;

        try {
            cache.get(rnd.nextInt(KEYS));

            cache.put(rnd.nextInt(KEYS), rnd.nextInt(10));

            cache.getAndPut(rnd.nextInt(KEYS), rnd.nextInt(10));

            cache.remove(rnd.nextInt(KEYS));

            cache.getAndRemove(rnd.nextInt(KEYS));

            cache.remove(rnd.nextInt(KEYS), rnd.nextInt(10));

            cache.putIfAbsent(rnd.nextInt(KEYS), rnd.nextInt(10));

            cache.replace(rnd.nextInt(KEYS), rnd.nextInt(10));

            cache.replace(rnd.nextInt(KEYS), rnd.nextInt(10), rnd.nextInt(10));

            cache.invoke(rnd.nextInt(KEYS), new TestEntryProcessor(rnd.nextInt(10)));

            if (cache.getConfiguration(CacheConfiguration.class).getAtomicityMode() == TRANSACTIONAL) {
                IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

                for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                    for (TransactionIsolation isolation : TransactionIsolation.values()) {
                        try (Transaction tx = txs.txStart(concurrency, isolation)) {
                            Integer key = rnd.nextInt(KEYS);

                            cache.getAndPut(key, rnd.nextInt(10));

                            cache.invoke(key + 1, new TestEntryProcessor(rnd.nextInt(10)));

                            cache.get(key + 2);

                            tx.commit();
                        }
                    }
                }
            }
        }
        catch (Exception e) {
            log.info("Cache operation failed: " + e);
        }
    }

    /**
     * @param name Cache name.
     * @param atomicityMode Cache atomicity mode.
     * @param backups Number of backups.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String name, CacheAtomicityMode atomicityMode, int backups) {
        CacheConfiguration ccfg = cacheConfiguration();

        ccfg.setName(name);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setBackups(backups);

        return ccfg;
    }

    /**
     * @param ignite Node.
     * @param affinity Affinity.
     * @throws Exception If failed.
     */
    private void checkServicesDeploy(Ignite ignite, final List<List<ClusterNode>> affinity) throws Exception {
        Affinity<Object> aff = ignite.affinity(CACHE_NAME1);

        for (int i = 0; i < 10; i++) {
            final int part = aff.partition(i);

            final String srvcName = "service-" + i;

            final ClusterNode srvcNode = affinity.get(part).get(0);

            boolean wait = GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    TestService srvc = grid(srvcNode).services().service(srvcName);

                    if (srvc == null)
                        return false;

                    assertEquals(srvcNode, srvc.serviceNode());

                    return true;
                }
            }, 5000);

            assertTrue(wait);
        }
    }

    /**
     * @param rnd Random generator.
     * @param srvs Server.
     * @param clients Clients.
     * @return Random node.
     */
    private Ignite randomNode(Random rnd, List<String> srvs, List<String> clients) {
        String name = null;

        if (rnd.nextBoolean()) {
            if (clients.size() > 0)
                name = clients.get(rnd.nextInt(clients.size()));
        }

        if (name == null)
            name = srvs.get(rnd.nextInt(srvs.size()));

        Ignite node = grid(name);

        assert  node != null;

        return node;
    }

    /**
     * @param rnd Random generator.
     * @param caches Caches list.
     * @param cacheName Cache name.
     * @param srvs Server nodes.
     * @param srvIdx Current servers index.
     */
    private void cacheClosure(Random rnd, List<String> caches, String cacheName, List<String> srvs, int srvIdx) {
        if (cacheName != null) {
            final CacheConfiguration ccfg = randomCacheConfiguration(rnd, cacheName, srvs, srvIdx);

            cacheC = new IgniteClosure<String, CacheConfiguration[]>() {
                @Override public CacheConfiguration[] apply(String s) {
                    return new CacheConfiguration[]{ccfg};
                }
            };

            caches.add(cacheName);
        }
        else {
            cacheC = new IgniteClosure<String, CacheConfiguration[]>() {
                @Override public CacheConfiguration[] apply(String s) {
                    return null;
                }
            };
        }
    }

    /**
     * @param rnd Random generator.
     * @param name Cache name.
     * @param srvs Server nodes.
     * @param srvIdx Current servers index.
     * @return Cache configuration.
     */
    private CacheConfiguration randomCacheConfiguration(Random rnd, String name, List<String> srvs, int srvIdx) {
        CacheConfiguration ccfg = cacheConfiguration();

        ccfg.setAtomicityMode(rnd.nextBoolean() ? TRANSACTIONAL : ATOMIC);
        ccfg.setBackups(rnd.nextInt(10));
        ccfg.setRebalanceMode(rnd.nextBoolean() ? SYNC : ASYNC);
        ccfg.setAffinity(affinityFunction(rnd.nextInt(2048) + 10));
        ccfg.setStartSize(128);

        if (rnd.nextBoolean()) {
            Set<String> exclude = new HashSet<>();

            for (int i = 0; i < 10; i++) {
                if (i % 2 == 0 && srvs.size() > 0)
                    exclude.add(srvs.get(rnd.nextInt(srvs.size())));
                else
                    exclude.add("server-" + (srvIdx + rnd.nextInt(10)));
            }

            ccfg.setNodeFilter(new CacheNodeFilter(exclude));
        }

        ccfg.setName(name);

        return ccfg;
    }

    /**
     * @param node Node.
     * @param topVer Topology version.
     * @param cache Cache name.
     * @return Affinity assignments.
     */
    private List<List<ClusterNode>> affinity(Ignite node, AffinityTopologyVersion topVer, String cache) {
        GridCacheContext cctx = ((IgniteKernal)node).context().cache().internalCache(cache).context();

        return cctx.affinity().assignments(topVer);
    }

    /**
     * @param spi SPI.
     * @param cacheName Cache name.
     */
    private void blockSupplySend(TestRecordingCommunicationSpi spi, final String cacheName) {
        spi.blockMessages(new IgnitePredicate<GridIoMessage>() {
            @Override public boolean apply(GridIoMessage ioMsg) {
                if (!ioMsg.message().getClass().equals(GridDhtPartitionSupplyMessageV2.class))
                    return false;

                GridDhtPartitionSupplyMessageV2 msg = (GridDhtPartitionSupplyMessageV2)ioMsg.message();

                return msg.cacheId() == CU.cacheId(cacheName);
            }
        });
    }

    /**
     * @param expNodes Expected nodes number.
     * @param topVer Topology version.
     * @return Affinity futures.
     */
    private List<IgniteInternalFuture<?>> affFutures(int expNodes, AffinityTopologyVersion topVer) {
        List<Ignite> nodes = G.allGrids();

        assertEquals(expNodes, nodes.size());

        List<IgniteInternalFuture<?>> futs = new ArrayList<>(nodes.size());

        for (Ignite node : nodes) {
            IgniteInternalFuture<?>
                fut = ((IgniteKernal)node).context().cache().context().exchange().affinityReadyFuture(topVer);

            futs.add(fut);
        }

        return futs;
    }

    /**
     * @param major Major version.
     * @param minor Minor version.
     * @return Topology version.
     */
    private static AffinityTopologyVersion topVer(long major, int minor) {
        return new AffinityTopologyVersion(major, minor);
    }

    /**
     *
     */
    private void checkCaches() {
        List<Ignite> nodes = G.allGrids();

        assertFalse(nodes.isEmpty());

        for (Ignite node : nodes) {
            Collection<String> cacheNames = node.cacheNames();

            assertFalse(cacheNames.isEmpty());

            for (String cacheName : cacheNames) {
                try {
                    IgniteCache<Object, Object> cache = node.cache(cacheName);

                    assertNotNull(cache);

                    Long val = System.currentTimeMillis();

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    for (int i = 0; i < 100; i++) {
                        int key = rnd.nextInt(100_000);

                        cache.put(key, val);

                        assertEquals(val, cache.get(key));

                        cache.remove(key);

                        assertNull(cache.get(key));
                    }
                }
                catch (Exception e) {
                    assertTrue("Unexpected error: " + e, X.hasCause(e, ClusterTopologyServerNotFoundException.class));

                    Affinity<Object> aff = node.affinity(cacheName);

                    assert aff.partitions() > 0;

                    for (int p = 0; p > aff.partitions(); p++) {
                        Collection<ClusterNode> partNodes = aff.mapPartitionToPrimaryAndBackups(p);

                        assertTrue(partNodes.isEmpty());
                    }
                }
            }
        }
    }

    /**
     * @param expNode Expected nodes number.
     * @param topVer Topology version.
     * @throws Exception If failed.
     */
    private void checkNoExchange(int expNode, AffinityTopologyVersion topVer) throws Exception {
        List<IgniteInternalFuture<?>> futs = affFutures(expNode, topVer);

        U.sleep(1000);

        for (IgniteInternalFuture<?> fut : futs)
            assertFalse(fut.isDone());
    }

    /**
     * @param expNodes Expected nodes number.
     * @param topVer Topology version.
     * @throws Exception If failed.
     */
    private void checkOrderCounters(int expNodes, AffinityTopologyVersion topVer) throws Exception {
        List<Ignite> nodes = G.allGrids();

        Long order = null;

        for (Ignite node : nodes) {
            IgniteKernal node0 = (IgniteKernal)node;

            if (node0.configuration().isClientMode())
                continue;

            IgniteInternalFuture<?> fut = node0.context().cache().context().exchange().affinityReadyFuture(topVer);

            if (fut != null)
                fut.get();

            AtomicLong orderCntr = GridTestUtils.getFieldValue(node0.context().cache().context().versions(), "order");

            log.info("Order [node=" + node0.name() + ", order=" + orderCntr.get() + ']');

            if (order == null)
                order = orderCntr.get();
            else
                assertEquals(order, (Long)orderCntr.get());
        }

        assertEquals(expNodes, nodes.size());
    }

    /**
     * @param expNodes Expected nodes number.
     * @param topVer Topology version.
     * @param expIdeal If {@code true} expect ideal affinity assignment.
     * @throws Exception If failed.
     * @return Affinity assignments.
     */
    @SuppressWarnings("unchecked")
    private Map<String, List<List<ClusterNode>>> checkAffinity(int expNodes,
        AffinityTopologyVersion topVer,
        boolean expIdeal) throws Exception {
        List<Ignite> nodes = G.allGrids();

        Map<String, List<List<ClusterNode>>> aff = new HashMap<>();

        for (Ignite node : nodes) {
            log.info("Check node: " + node.name());

            IgniteKernal node0 = (IgniteKernal)node;

            IgniteInternalFuture<?> fut = node0.context().cache().context().exchange().affinityReadyFuture(topVer);

            if (fut != null)
                fut.get();

            for (GridCacheContext cctx : node0.context().cache().context().cacheContexts()) {
                if (cctx.startTopologyVersion() != null && cctx.startTopologyVersion().compareTo(topVer) > 0)
                    continue;

                List<List<ClusterNode>> aff1 = aff.get(cctx.name());
                List<List<ClusterNode>> aff2 = cctx.affinity().assignments(topVer);

                if (aff1 == null)
                    aff.put(cctx.name(), aff2);
                else
                    assertAffinity(aff1, aff2, node, cctx.name(), topVer);

                if (expIdeal) {
                    List<List<ClusterNode>> ideal = idealAssignment(topVer, cctx.cacheId());

                    assertAffinity(ideal, aff2, node, cctx.name(), topVer);

                    Affinity<Object> cacheAff = node.affinity(cctx.name());

                    for (int i = 0; i < 10; i++) {
                        int part = cacheAff.partition(i);

                        List<ClusterNode> partNodes = ideal.get(part);

                        if (partNodes.isEmpty()) {
                            try {
                                cacheAff.mapKeyToNode(i);

                                fail();
                            }
                            catch (IgniteException ignore) {
                                // No-op.
                            }
                        }
                        else {
                            ClusterNode primary = cacheAff.mapKeyToNode(i);

                            assertEquals(primary, partNodes.get(0));
                        }
                    }

                    for (int p = 0; p < ideal.size(); p++) {
                        List<ClusterNode> exp = ideal.get(p);
                        Collection<ClusterNode> partNodes = cacheAff.mapPartitionToPrimaryAndBackups(p);

                        assertEqualsCollections(exp, partNodes);
                    }
                }
            }
        }

        assertEquals(expNodes, nodes.size());

        if (!skipCheckOrder)
            checkOrderCounters(expNodes, topVer);

        return aff;
    }

    /**
     * @param aff1 Affinity 1.
     * @param aff2 Affinity 2.
     * @param node Node.
     * @param cacheName Cache name.
     * @param topVer Topology version.
     */
    private void assertAffinity(List<List<ClusterNode>> aff1,
        List<List<ClusterNode>> aff2,
        Ignite node,
        String cacheName,
        AffinityTopologyVersion topVer) {
        assertEquals(aff1.size(), aff2.size());

        if (!aff1.equals(aff2)) {
            for (int i = 0; i < aff1.size(); i++) {
                assertEquals("Wrong affinity [node=" + node.name() +
                    ", topVer=" + topVer +
                    ", cache=" + cacheName +
                    ", part=" + i + ']',
                    F.nodeIds(aff1.get(i)), F.nodeIds(aff2.get(i)));
            }

            fail();
        }
    }

    /**
     * @param idx Node index.
     * @param topVer New topology version.
     * @return Started node.
     * @throws Exception If failed.
     */
    private Ignite startClient(int idx, long topVer) throws Exception {
        client = true;

        Ignite ignite = startGrid(idx);

        assertTrue(ignite.configuration().isClientMode());

        client = false;

        calculateAffinity(topVer);

        return ignite;
    }

    /**
     * @param idx Node index.
     * @param topVer New topology version.
     * @throws Exception If failed.
     * @return Started node.
     */
    private Ignite startServer(int idx, long topVer) throws Exception {
        Ignite node = startGrid(idx);

        assertFalse(node.configuration().isClientMode());

        calculateAffinity(topVer);

        return node;
    }

    /**
     * @param name Node name.
     * @param topVer Topology version.
     * @param client Client flag.
     * @throws Exception If failed.
     */
    private void startNode(String name, long topVer, boolean client) throws Exception {
        this.client = client;

        startGrid(name);

        calculateAffinity(topVer);
    }

    /**
     * @param name Node name.
     * @param topVer Topology version.
     * @throws Exception If failed.
     */
    private void stopNode(String name, long topVer) throws Exception {
        stopGrid(name);

        calculateAffinity(topVer);
    }

    /**
     * @param idx Node index.
     * @param topVer New topology version.
     * @throws Exception If failed.
     */
    private void stopNode(int idx, long topVer) throws Exception {
        stopNode(getTestGridName(idx), topVer);
    }

    /**
     * @param topVer Topology version.
     * @param cacheId Cache ID.
     * @return Ideal assignment.
     */
    private List<List<ClusterNode>> idealAssignment(AffinityTopologyVersion topVer, Integer cacheId) {
        Map<Integer, List<List<ClusterNode>>> assignments = idealAff.get(topVer.topologyVersion());

        assert assignments != null : "No assignments [topVer=" + topVer + ']';

        List<List<ClusterNode>> cacheAssignments = assignments.get(cacheId);

        assert cacheAssignments != null : "No cache assignments [topVer=" + topVer + ", cache=" + cacheId + ']';

        return cacheAssignments;
    }

    /**
     * @param topVer Topology version.
     * @throws Exception If failed.
     */
    private void calculateAffinity(long topVer) throws Exception {
        calculateAffinity(topVer, false, null);
    }

    /**
     * @param topVer Topology version.
     * @param filterByRcvd If {@code true} filters caches by 'receivedFrom' property.
     * @param cur Optional current affinity.
     * @throws Exception If failed.
     * @return {@code True} if some primary node changed comparing to given affinity.
     */
    private boolean calculateAffinity(long topVer,
        boolean filterByRcvd,
        @Nullable Map<String, List<List<ClusterNode>>> cur) throws Exception {
        List<Ignite> all = G.allGrids();

        IgniteKernal ignite = (IgniteKernal)Collections.min(all, new Comparator<Ignite>() {
            @Override public int compare(Ignite n1, Ignite n2) {
                return Long.compare(n1.cluster().localNode().order(), n2.cluster().localNode().order());
            }
        });

        assert all.size() > 0;

        Map<Integer, List<List<ClusterNode>>> assignments = idealAff.get(topVer);

        if (assignments == null)
            idealAff.put(topVer, assignments = new HashMap<>());

        GridKernalContext ctx = ignite.context();

        GridCacheSharedContext cctx = ctx.cache().context();

        AffinityTopologyVersion topVer0 = new AffinityTopologyVersion(topVer);

        cctx.discovery().topologyFuture(topVer).get();

        List<GridDhtPartitionsExchangeFuture> futs = cctx.exchange().exchangeFutures();

        DiscoveryEvent evt = null;

        long stopTime = System.currentTimeMillis() + 10_000;

        boolean primaryChanged = false;

        do {
            for (int i = futs.size() - 1; i >= 0; i--) {
                GridDhtPartitionsExchangeFuture fut = futs.get(i);

                if (fut.topologyVersion().equals(topVer0)) {
                    evt = fut.discoveryEvent();

                    break;
                }
            }

            if (evt == null) {
                U.sleep(500);

                futs = cctx.exchange().exchangeFutures();
            }
            else
                break;
        } while (System.currentTimeMillis() < stopTime);

        assertNotNull("Failed to find exchange future:", evt);

        List<ClusterNode> allNodes = ctx.discovery().serverNodes(topVer0);

        for (DynamicCacheDescriptor cacheDesc : ctx.cache().cacheDescriptors()) {
            if (assignments.get(cacheDesc.cacheId()) != null)
                continue;

            if (filterByRcvd && cacheDesc.receivedFrom() != null &&
                ctx.discovery().node(topVer0, cacheDesc.receivedFrom()) == null)
                continue;

            AffinityFunction func = cacheDesc.cacheConfiguration().getAffinity();

            func = cctx.cache().clone(func);

            cctx.kernalContext().resource().injectGeneric(func);

            List<ClusterNode> affNodes = new ArrayList<>();

            IgnitePredicate<ClusterNode> filter = cacheDesc.cacheConfiguration().getNodeFilter();

            for (ClusterNode n : allNodes) {
                if (!CU.clientNode(n) && (filter == null || filter.apply(n)))
                    affNodes.add(n);
            }

            Collections.sort(affNodes, GridNodeOrderComparator.INSTANCE);

            AffinityFunctionContext affCtx = new GridAffinityFunctionContextImpl(
                affNodes,
                previousAssignment(topVer, cacheDesc.cacheId()),
                evt,
                topVer0,
                cacheDesc.cacheConfiguration().getBackups());

            List<List<ClusterNode>> assignment = func.assignPartitions(affCtx);

            if (cur != null) {
                List<List<ClusterNode>> prev = cur.get(cacheDesc.cacheConfiguration().getName());

                assertEquals(prev.size(), assignment.size());

                if (!primaryChanged) {
                    for (int p = 0; p < prev.size(); p++) {
                        List<ClusterNode> nodes0 = prev.get(p);
                        List<ClusterNode> nodes1 = assignment.get(p);

                        if (nodes0.size() > 0 && nodes1.size() > 0) {
                            ClusterNode p0 = nodes0.get(0);
                            ClusterNode p1 = nodes1.get(0);

                            if (allNodes.contains(p0) && !p0.equals(p1)) {
                                primaryChanged = true;

                                log.info("Primary changed [cache=" + cacheDesc.cacheConfiguration().getName() +
                                    ", part=" + p +
                                    ", prev=" + F.nodeIds(nodes0) +
                                    ", new=" + F.nodeIds(nodes1) + ']');

                                break;
                            }
                        }
                    }
                }
            }

            assignments.put(cacheDesc.cacheId(), assignment);
        }

        return primaryChanged;
    }

    /**
     * @param topVer Topology version.
     * @param cacheId Cache ID.
     * @return Previous assignment.
     */
    @Nullable private List<List<ClusterNode>> previousAssignment(long topVer, Integer cacheId) {
        if (topVer == 1)
            return null;

        Map<Integer, List<List<ClusterNode>>> assignments = idealAff.get(topVer - 1);

        assertNotNull(assignments);

        return assignments.get(cacheId);
    }

    /**
     *
     */
    interface TestService {
        /**
         * @return Node.
         */
        ClusterNode serviceNode();
    }

    /**
     *
     */
    private static class TestServiceImpl implements Service, TestService {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private int key;

        /**
         * @param key Key.
         */
        public TestServiceImpl(int key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            ignite.log().info("Execute service [key=" + key + ", node=" + ignite.name() + ']');
        }

        /** {@inheritDoc} */
        @Override public ClusterNode serviceNode() {
            return ignite.cluster().localNode();
        }
    }

    /**
     *
     */
    static class CacheNodeFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private Collection<String> excludeNodes;

        /**
         * @param excludeNodes Nodes names.
         */
        public CacheNodeFilter(Collection<String> excludeNodes) {
            this.excludeNodes = excludeNodes;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode clusterNode) {
            String name = clusterNode.attribute(ATTR_GRID_NAME).toString();

            return !excludeNodes.contains(name);
        }
    }

    /**
     *
     */
    static class TestTcpDiscoverySpi extends TcpDiscoverySpi {
        /** */
        private boolean blockCustomEvt;

        /** */
        private final Object mux = new Object();

        /** */
        private List<DiscoverySpiCustomMessage> blockedMsgs = new ArrayList<>();

        /** {@inheritDoc} */
        @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException {
            synchronized (mux) {
                if (blockCustomEvt) {
                    DiscoveryCustomMessage msg0 = GridTestUtils.getFieldValue(msg, "delegate");

                    if (msg0 instanceof CacheAffinityChangeMessage) {
                        log.info("Block custom message: " + msg0);

                        blockedMsgs.add(msg);

                        mux.notifyAll();

                        return;
                    }
                }
            }

            super.sendCustomEvent(msg);
        }

        /**
         *
         */
        public void blockCustomEvent() {
            synchronized (mux) {
                assert blockedMsgs.isEmpty() : blockedMsgs;

                blockCustomEvt = true;
            }
        }

        /**
         * @throws InterruptedException If interrupted.
         */
        public void waitCustomEvent() throws InterruptedException {
            synchronized (mux) {
                while (blockedMsgs.isEmpty())
                    mux.wait();
            }
        }

        /**
         *
         */
        public void stopBlock() {
            List<DiscoverySpiCustomMessage> msgs;

            synchronized (this) {
                msgs = new ArrayList<>(blockedMsgs);

                blockCustomEvt = false;

                blockedMsgs.clear();
            }

            for (DiscoverySpiCustomMessage msg : msgs) {
                log.info("Resend blocked message: " + msg);

                super.sendCustomEvent(msg);
            }
        }
    }

    /**
     *
     */
    static class TestEntryProcessor implements EntryProcessor<Object, Object, Object> {
        /** */
        private Object val;

        /**
         * @param val Value.
         */
        public TestEntryProcessor(Object val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Object, Object> e, Object... args) {
            e.setValue(val);

            return null;
        }
    }
}
