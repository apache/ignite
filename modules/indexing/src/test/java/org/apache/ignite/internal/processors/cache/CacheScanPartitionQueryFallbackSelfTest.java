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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterGroupEmptyCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.query.CacheQuery;
import org.apache.ignite.internal.processors.cache.query.CacheQueryFuture;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryAdapter;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryRequest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests partition scan query fallback.
 */
public class CacheScanPartitionQueryFallbackSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** Keys count. */
    private static final int KEYS_CNT = 5000;

    /** Ip finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Backups. */
    private int backups;

    /** Cache mode. */
    private CacheMode cacheMode;

    /** Client mode. */
    private volatile boolean clientMode;

    /** Expected first node ID. */
    private static UUID expNodeId;

    /** Expected fallback node ID. */
    private static UUID expFallbackNodeId;

    /** Communication SPI factory. */
    private CommunicationSpiFactory commSpiFactory;

    /** Latch. */
    private static CountDownLatch latch;

    /** Test entries. */
    private Map<Integer, Map<Integer, Integer>> entries = new HashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClientMode(clientMode);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(IP_FINDER);
        discoSpi.setForceServerMode(true);
        cfg.setDiscoverySpi(discoSpi);

        cfg.setCommunicationSpi(commSpiFactory.create());

        CacheConfiguration ccfg = defaultCacheConfiguration();
        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setBackups(backups);
        ccfg.setNearConfiguration(null);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * Scan should perform on the local node.
     *
     * @throws Exception If failed.
     */
    public void testScanLocal() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        backups = 0;
        commSpiFactory = new TestLocalCommunicationSpiFactory();

        try {
            Ignite ignite = startGrids(GRID_CNT);

            IgniteCacheProxy<Integer, Integer> cache = fillCache(ignite);

            int part = anyLocalPartition(cache.context());

            CacheQuery<Map.Entry<Integer, Integer>> qry = cache.context().queries().createScanQuery(null, part, false);

            doTestScanQuery(qry);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Scan should perform on the remote node.
     *
     * @throws Exception If failed.
     */
    public void testScanRemote() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        backups = 0;
        commSpiFactory = new TestRemoteCommunicationSpiFactory();

        try {
            Ignite ignite = startGrids(GRID_CNT);

            IgniteCacheProxy<Integer, Integer> cache = fillCache(ignite);

            IgniteBiTuple<Integer, UUID> tup = remotePartition(cache.context());

            int part = tup.get1();

            expNodeId = tup.get2();

            CacheQuery<Map.Entry<Integer, Integer>> qry = cache.context().queries().createScanQuery(null, part, false);

            doTestScanQuery(qry);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Scan should activate fallback mechanism when new nodes join topology and rebalancing happens in parallel with
     * scan query.
     *
     * @throws Exception In case of error.
     */
    public void testScanFallbackOnRebalancing() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        clientMode = false;
        backups = 1;
        commSpiFactory = new TestFallbackOnRebalancingCommunicationSpiFactory();

        try {
            Ignite ignite = startGrids(GRID_CNT);

            final IgniteCacheProxy<Integer, Integer> cache = fillCache(ignite);

            final AtomicBoolean done = new AtomicBoolean(false);

            final AtomicInteger idx = new AtomicInteger(GRID_CNT);

            IgniteInternalFuture fut1 = multithreadedAsync(
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        int id = idx.getAndIncrement();

                        while (!done.get()) {
                            startGrid(id);
                            Thread.sleep(3000);

                            stopGrid(id);

                            if (done.get())
                                return null;

                            Thread.sleep(3000);
                        }

                        return null;
                    }
                }, GRID_CNT);

            final AtomicInteger nodeIdx = new AtomicInteger();

            IgniteInternalFuture fut2 = multithreadedAsync(
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        int nodeId = nodeIdx.getAndIncrement();

                        IgniteCacheProxy<Integer, Integer> cache = (IgniteCacheProxy<Integer, Integer>)
                            grid(nodeId).<Integer, Integer>cache(null);

                        while (!done.get()) {
                            IgniteBiTuple<Integer, UUID> tup = remotePartition(cache.context());

                            int part = tup.get1();

                            try {
                                CacheQuery<Map.Entry<Integer, Integer>> qry = cache.context().queries().createScanQuery(
                                    null, part, false);

                                doTestScanQuery(qry);
                            }
                            catch (ClusterGroupEmptyCheckedException e) {
                                log.warning("Invalid partition: " + part, e);
                            }
                        }

                        return null;
                    }
                }, GRID_CNT);

            Thread.sleep(60 * 1000); // Test for one minute

            done.set(true);

            fut2.get();
            fut1.get();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Scan should try first remote node and fallbacks to second remote node.
     *
     * @throws Exception If failed.
     */
    public void testScanFallback() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        backups = 1;
        commSpiFactory = new TestFallbackCommunicationSpiFactory();

        final Set<Integer> candidates = new TreeSet<>();

        final AtomicBoolean test = new AtomicBoolean(false);

        for(int j = 0; j < 2; j++) {
            clientMode = true;

            latch = new CountDownLatch(1);

            try {
                final Ignite ignite0 = startGrid(0);

                clientMode = false;

                final IgniteEx ignite1 = startGrid(1);
                final IgniteEx ignite2 = startGrid(2);
                startGrid(3);

                if (test.get()) {
                    expNodeId = ignite1.localNode().id();
                    expFallbackNodeId = ignite2.localNode().id();
                }

                final IgniteCacheProxy<Integer, Integer> cache = fillCache(ignite0);

                if (!test.get()) {
                    candidates.addAll(localPartitions(ignite1));

                    candidates.retainAll(localPartitions(ignite2));
                }

                Runnable run = new Runnable() {
                    @Override public void run() {
                        try {
                            startGrid(4);
                            startGrid(5);

                            awaitPartitionMapExchange();

                            if (!test.get()) {
                                candidates.removeAll(localPartitions(ignite1));

                                F.retain(candidates, false, localPartitions(ignite2));
                            }

                            latch.countDown();
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                };

                int part;
                CacheQuery<Map.Entry<Integer, Integer>> qry = null;

                if (test.get()) {
                    part = F.first(candidates);

                    qry = cache.context().queries().createScanQuery(null, part, false);
                }

                new Thread(run).start();

                if (test.get())
                    doTestScanQuery(qry);
                else
                    latch.await();
            }
            finally {
                test.set(true);

                stopAllGrids();
            }
        }
    }

    /**
     * @param ignite Ignite.
     */
    protected IgniteCacheProxy<Integer, Integer> fillCache(Ignite ignite) {
        IgniteCacheProxy<Integer, Integer> cache =
            (IgniteCacheProxy<Integer, Integer>)ignite.<Integer, Integer>cache(null);

        for (int i = 0; i < KEYS_CNT; i++) {
            cache.put(i, i);

            int part = cache.context().affinity().partition(i);

            Map<Integer, Integer> partEntries = entries.get(part);

            if (partEntries == null)
                entries.put(part, partEntries = new HashMap<>());

            partEntries.put(i, i);
        }

        return cache;
    }

    /**
     * @param qry Query.
     */
    protected void doTestScanQuery(
        CacheQuery<Map.Entry<Integer, Integer>> qry) throws IgniteCheckedException {
        CacheQueryFuture<Map.Entry<Integer, Integer>> fut = qry.execute();

        Collection<Map.Entry<Integer, Integer>> expEntries = fut.get();

        for (Map.Entry<Integer, Integer> e : expEntries) {
            Map<Integer, Integer> map = entries.get(((GridCacheQueryAdapter)qry).partition());

            if (map == null)
                assertTrue(expEntries.isEmpty());
            else
                assertEquals(map.get(e.getKey()), e.getValue());
        }
    }

    /**
     * @param cctx Cctx.
     */
    private static int anyLocalPartition(GridCacheContext<?, ?> cctx) {
        return F.first(cctx.topology().localPartitions()).id();
    }

    /**
     * @param cctx Cctx.
     */
    private IgniteBiTuple<Integer, UUID> remotePartition(final GridCacheContext cctx) {
        ClusterNode node = F.first(cctx.kernalContext().grid().cluster().forRemotes().nodes());

        GridCacheAffinityManager affMgr = cctx.affinity();

        AffinityTopologyVersion topVer = affMgr.affinityTopologyVersion();

        Set<Integer> parts = affMgr.primaryPartitions(node.id(), topVer);

        return new IgniteBiTuple<>(F.first(parts), node.id());
    }

    /**
     * @param ignite Ignite.
     */
    private Set<Integer> localPartitions(Ignite ignite) {
        GridCacheContext cctx = ((IgniteCacheProxy)ignite.cache(null)).context();

        Collection<GridDhtLocalPartition> owningParts = F.view(cctx.topology().localPartitions(),
            new IgnitePredicate<GridDhtLocalPartition>() {
                @Override public boolean apply(GridDhtLocalPartition part) {
                    return part.state() == GridDhtPartitionState.OWNING;
                }
            });

        return new HashSet<>(F.transform(owningParts, new IgniteClosure<GridDhtLocalPartition, Integer>() {
            @Override public Integer apply(GridDhtLocalPartition part) {
                return part.id();
            }
        }));
    }

    /**
     * Factory for tests specific communication SPI.
     */
    private interface CommunicationSpiFactory {
        /**
         * Creates communication SPI instance.
         */
        TcpCommunicationSpi create();
    }

    /**
     *
     */
    private static class TestLocalCommunicationSpiFactory implements CommunicationSpiFactory {
        /** {@inheritDoc} */
        @Override public TcpCommunicationSpi create() {
            return new TcpCommunicationSpi() {
                @Override public void sendMessage(ClusterNode node, Message msg,
                    IgniteInClosure<IgniteException> ackClosure) throws IgniteSpiException {
                    Object origMsg = ((GridIoMessage)msg).message();

                    if (origMsg instanceof GridCacheQueryRequest)
                        fail(); //should use local node

                    super.sendMessage(node, msg, ackClosure);
                }
            };
        }
    }

    /**
     *
     */
    private static class TestRemoteCommunicationSpiFactory implements CommunicationSpiFactory {
        /** {@inheritDoc} */
        @Override public TcpCommunicationSpi create() {
            return new TcpCommunicationSpi() {
                @Override public void sendMessage(ClusterNode node, Message msg,
                    IgniteInClosure<IgniteException> ackClosure) throws IgniteSpiException {
                    Object origMsg = ((GridIoMessage)msg).message();

                    if (origMsg instanceof GridCacheQueryRequest)
                        assertEquals(expNodeId, node.id());

                    super.sendMessage(node, msg, ackClosure);
                }
            };
        }
    }

    /**
     *
     */
    private static class TestFallbackCommunicationSpiFactory implements CommunicationSpiFactory {
        /** {@inheritDoc} */
        @Override public TcpCommunicationSpi create() {
            return new TcpCommunicationSpi() {
                @Override public void sendMessage(ClusterNode node, Message msg,
                    IgniteInClosure<IgniteException> ackClosure) throws IgniteSpiException {
                    Object origMsg = ((GridIoMessage)msg).message();

                    if (origMsg instanceof GridCacheQueryRequest) {
                        if (latch.getCount() > 0)
                            assertEquals(expNodeId, node.id());
                        else
                            assertEquals(expFallbackNodeId, node.id());

                        try {
                            latch.await();
                        }
                        catch (InterruptedException e) {
                            throw new IgniteSpiException(e);
                        }
                    }

                    super.sendMessage(node, msg, ackClosure);
                }
            };
        }
    }

    /**
     *
     */
    private static class TestFallbackOnRebalancingCommunicationSpiFactory implements CommunicationSpiFactory {
        /** {@inheritDoc} */
        @Override public TcpCommunicationSpi create() {
            return new TcpCommunicationSpi();
        }
    }
}