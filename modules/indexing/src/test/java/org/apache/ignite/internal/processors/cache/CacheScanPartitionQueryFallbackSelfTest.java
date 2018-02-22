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
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryRequest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
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
    private static final int KEYS_CNT = 50 * RendezvousAffinityFunction.DFLT_PARTITION_COUNT;

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

    /** Communication SPI factory. */
    private CommunicationSpiFactory commSpiFactory;

    /** Test entries. */
    private Map<Integer, Map<Integer, Integer>> entries = new HashMap<>();

    /** */
    private boolean syncRebalance;

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

        if (syncRebalance)
            ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);

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

            QueryCursor<Cache.Entry<Integer, Integer>> qry =
                cache.query(new ScanQuery<Integer, Integer>().setPartition(part));

            doTestScanQuery(qry, part);
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

            QueryCursor<Cache.Entry<Integer, Integer>> qry =
                cache.query(new ScanQuery<Integer, Integer>().setPartition(part));

            doTestScanQuery(qry, part);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testScanFallbackOnRebalancing() throws Exception {
        scanFallbackOnRebalancing(false);
    }

    /**
     * @param cur If {@code true} tests query cursor.
     * @throws Exception In case of error.
     */
    private void scanFallbackOnRebalancing(final boolean cur) throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        clientMode = false;
        backups = 2;
        commSpiFactory = new TestFallbackOnRebalancingCommunicationSpiFactory();
        syncRebalance = true;

        try {
            Ignite ignite = startGrids(GRID_CNT);

            fillCache(ignite);

            final AtomicBoolean done = new AtomicBoolean(false);

            final AtomicInteger idx = new AtomicInteger(GRID_CNT);

            IgniteInternalFuture fut1 = multithreadedAsync(
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        int id = idx.getAndIncrement();

                        while (!done.get()) {
                            startGrid(id);

                            Thread.sleep(3000);

                            info("Will stop grid: " + getTestGridName(id));

                            stopGrid(id);

                            if (done.get())
                                return null;

                            Thread.sleep(3000);
                        }

                        return null;
                    }
                }, 2);

            final AtomicInteger nodeIdx = new AtomicInteger();

            IgniteInternalFuture fut2 = multithreadedAsync(
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        int nodeId = nodeIdx.getAndIncrement();

                        IgniteCache<Integer, Integer> cache = grid(nodeId).cache(null);

                        int cntr = 0;

                        while (!done.get()) {
                            int part = ThreadLocalRandom.current().nextInt(ignite(nodeId).affinity(null).partitions());

                            if (cntr++ % 100 == 0)
                                info("Running query [node=" + nodeId + ", part=" + part + ']');

                            try (QueryCursor<Cache.Entry<Integer, Integer>> cur0 =
                                     cache.query(new ScanQuery<Integer, Integer>(part))) {

                                if (cur)
                                    doTestScanQueryCursor(cur0, part);
                                else
                                    doTestScanQuery(cur0, part);
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
     * Scan should activate fallback mechanism when new nodes join topology and rebalancing happens in parallel with
     * scan query.
     *
     * @throws Exception In case of error.
     */
    public void testScanFallbackOnRebalancingCursor1() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        clientMode = false;
        backups = 1;
        commSpiFactory = new TestFallbackOnRebalancingCommunicationSpiFactory();

        try {
            Ignite ignite = startGrids(GRID_CNT);

            fillCache(ignite);

            final AtomicBoolean done = new AtomicBoolean(false);

            IgniteInternalFuture fut1 = multithreadedAsync(
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        for (int i = 0; i < 5; i++) {
                            startGrid(GRID_CNT + i);

                            U.sleep(500);
                        }

                        done.set(true);

                        return null;
                    }
                }, 1);

            final AtomicInteger nodeIdx = new AtomicInteger();

            IgniteInternalFuture fut2 = multithreadedAsync(
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        int nodeId = nodeIdx.getAndIncrement();

                        IgniteCache<Integer, Integer> cache = grid(nodeId).cache(null);

                        int cntr = 0;

                        while (!done.get()) {
                            int part = ThreadLocalRandom.current().nextInt(ignite(nodeId).affinity(null).partitions());

                            if (cntr++ % 100 == 0)
                                info("Running query [node=" + nodeId + ", part=" + part + ']');

                            try (QueryCursor<Cache.Entry<Integer, Integer>> cur =
                                     cache.query(new ScanQuery<Integer, Integer>(part).setPageSize(5))) {

                                doTestScanQueryCursor(cur, part);
                            }
                        }

                        return null;
                    }
                }, GRID_CNT);

            fut1.get();
            fut2.get();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanFallbackOnRebalancingCursor2() throws Exception {
        scanFallbackOnRebalancing(true);
    }

    /**
     * @param ignite Ignite.
     * @return Cache.
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
     * @param part Partition.
     */
    protected void doTestScanQuery(QueryCursor<Cache.Entry<Integer, Integer>> qry, int part) {
        Collection<Cache.Entry<Integer, Integer>> qryEntries = qry.getAll();

        Map<Integer, Integer> map = entries.get(part);

        for (Cache.Entry<Integer, Integer> e : qryEntries)
            assertEquals(map.get(e.getKey()), e.getValue());

        assertEquals("Invalid number of entries for partition: " + part, map.size(), qryEntries.size());
    }

    /**
     * @param cur Query cursor.
     * @param part Partition number.
     */
    protected void doTestScanQueryCursor(
        QueryCursor<Cache.Entry<Integer, Integer>> cur, int part) {

        Map<Integer, Integer> map = entries.get(part);

        assert map != null;

        int cnt = 0;

        for (Cache.Entry<Integer, Integer> e : cur) {
            assertEquals(map.get(e.getKey()), e.getValue());

            cnt++;
        }

        assertEquals("Invalid number of entries for partition: " + part, map.size(), cnt);
    }

    /**
     * @param cctx Cctx.
     * @return Local partition.
     */
    private static int anyLocalPartition(GridCacheContext<?, ?> cctx) {
        return F.first(cctx.topology().localPartitions()).id();
    }

    /**
     * @param cctx Cctx.
     * @return Remote partition.
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
     * @return Local partitions.
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
         * @return Communication SPI instance.
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
                    IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
                    Object origMsg = ((GridIoMessage)msg).message();

                    if (origMsg instanceof GridCacheQueryRequest)
                        fail(); //should use local node

                    super.sendMessage(node, msg, ackC);
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
                    IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
                    Object origMsg = ((GridIoMessage)msg).message();

                    if (origMsg instanceof GridCacheQueryRequest)
                        assertEquals(expNodeId, node.id());

                    super.sendMessage(node, msg, ackC);
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