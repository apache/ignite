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
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.ExchangeContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsAbstractMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.mvcc.MvccProcessor;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class GridExchangeFreeSwitchTest extends GridCommonAbstractTest {
    /** */
    private boolean persistence;

    /** */
    private static final String CACHE_NAME = "testCache";

    /** */
    private IgniteClosure<String, CacheConfiguration[]> cacheC;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        cfg.setCacheConfiguration(cacheC != null ?
            cacheC.apply(igniteInstanceName) : new CacheConfiguration[] {cacheConfiguration()});

        DataStorageConfiguration cfg1 = new DataStorageConfiguration();

        DataRegionConfiguration drCfg = new DataRegionConfiguration();

        if (persistence) {
            drCfg.setPersistenceEnabled(true);

            cfg.setActiveOnStart(false);
            cfg.setAutoActivationEnabled(false);
        }

        cfg1.setDefaultDataRegionConfiguration(drCfg);

        cfg.setDataStorageConfiguration(cfg1);

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(CACHE_NAME);
        ccfg.setAffinity(affinityFunction(null));
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setBackups(0);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
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
        stopAllGrids();
    }

    /**
     * Checks Partition Exchange happen in case of baseline auto-adjust (in-memory cluster).
     * It's not possible to perform switch since primaries may change.
     */
    @Test
    public void testNonBaselineNodeLeftOnFullyRebalancedCluster() throws Exception {
        testNodeLeftOnFullyRebalancedCluster();
    }

    /**
     * Checks Partition Exchange is absent in case of fixed baseline.
     * It's possible to perform switch since primaries can't change.
     */
    @Test
    public void testBaselineNodeLeftOnFullyRebalancedCluster() throws Exception {
        persistence = true;

        try {
            testNodeLeftOnFullyRebalancedCluster();
        }
        finally {
            persistence = false;
        }
    }

    /**
     * Checks node left PME absent/present on fully rebalanced topology (Latest PME == LAS).
     */
    private void testNodeLeftOnFullyRebalancedCluster() throws Exception {
        int nodes = 10;

        Ignite ignite = startGridsMultiThreaded(nodes, true);

        ignite.cluster().active(true);

        AtomicLong cnt = new AtomicLong();

        for (int i = 0; i < nodes; i++) {
            TestRecordingCommunicationSpi spi =
                (TestRecordingCommunicationSpi)ignite(i).configuration().getCommunicationSpi();

            spi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    if (msg.getClass().equals(GridDhtPartitionsSingleMessage.class) &&
                        ((GridDhtPartitionsAbstractMessage)msg).exchangeId() != null)
                        cnt.incrementAndGet();

                    if (!persistence)
                        return false;

                    return msg.getClass().equals(GridDhtPartitionsSingleMessage.class) ||
                        msg.getClass().equals(GridDhtPartitionsFullMessage.class);
                }
            });
        }

        Random r = new Random();

        while (nodes > 1) {
            G.allGrids().get(r.nextInt(nodes--)).close(); // Stopping random node.

            awaitPartitionMapExchange(true, true, null, true);

            assertEquals(persistence ? 0 /*PME absent*/ : (nodes - 1) /*regular PME*/, cnt.get());

            IgniteEx alive = (IgniteEx)G.allGrids().get(0);

            assertTrue(alive.context().cache().context().exchange().lastFinishedFuture().rebalanced());

            cnt.set(0);
        }
    }

    /**
     *
     */
    @Test
    public void testNoTransactionsWaitAtNodeLeft() throws Exception {
        persistence = true;

        String cacheName = "three-partitioned";

        try {
            cacheC = new IgniteClosure<String, CacheConfiguration[]>() {
                @Override public CacheConfiguration[] apply(String igniteInstanceName) {
                    CacheConfiguration ccfg = new CacheConfiguration();

                    ccfg.setName(cacheName);
                    ccfg.setWriteSynchronizationMode(FULL_SYNC);
                    ccfg.setBackups(1);
                    ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
                    ccfg.setAffinity(new Map4PartitionsTo4NodesAffinityFunction());

                    return new CacheConfiguration[] {ccfg};
                }
            };

            int nodes = 4;

            startGridsMultiThreaded(nodes, true).cluster().active(true);

            AtomicLong cnt = new AtomicLong();

            for (int i = 0; i < nodes; i++) {
                TestRecordingCommunicationSpi spi =
                    (TestRecordingCommunicationSpi)ignite(i).configuration().getCommunicationSpi();

                spi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                    @Override public boolean apply(ClusterNode node, Message msg) {
                        if (msg.getClass().equals(GridDhtPartitionsSingleMessage.class) &&
                            ((GridDhtPartitionsAbstractMessage)msg).exchangeId() != null)
                            cnt.incrementAndGet();

                        return msg.getClass().equals(GridDhtPartitionsSingleMessage.class) ||
                            msg.getClass().equals(GridDhtPartitionsFullMessage.class);
                    }
                });
            }

            Random r = new Random();

            Ignite candidate;
            MvccProcessor proc;

            do {
                candidate = G.allGrids().get(r.nextInt(nodes));

                proc = ((IgniteEx)candidate).context().coordinators();
            }
            // MVCC coordinator fail always breaks transactions, excluding.
            while (proc.mvccEnabled() && proc.currentCoordinator().local());

            Ignite failed = candidate;

            int multiplicator = 3;
            int key_bound = 1_000_000;

            CountDownLatch readyLatch = new CountDownLatch(4 * multiplicator);
            CountDownLatch failedLatch = new CountDownLatch(1);

            IgniteCache<Integer, Integer> failedCache = failed.getOrCreateCache(cacheName);

            IgniteInternalFuture<?> nearThenNearFut = multithreadedAsync(() -> {
                try {
                    List<Integer> keys = nearKeys(failedCache, 2, r.nextInt(key_bound));

                    Integer key0 = keys.get(0);
                    Integer key1 = keys.get(1);

                    Ignite primary = primaryNode(key0, cacheName);

                    assertNotSame(failed, primary);

                    IgniteCache<Integer, Integer> primaryCache = primary.getOrCreateCache(cacheName);

                    try (Transaction tx = primary.transactions().txStart()) {
                        primaryCache.put(key0, key0);

                        readyLatch.countDown();
                        failedLatch.await();

                        primaryCache.put(key1, key1);

                        tx.commit();
                    }

                    assertEquals(key0, primaryCache.get(key0));
                    assertEquals(key1, primaryCache.get(key1));
                }
                catch (Exception e) {
                    fail("Should not happen [exception=" + e + "]");
                }
            }, multiplicator);

            IgniteInternalFuture<?> primaryThenPrimaryFut = multithreadedAsync(() -> {
                try {
                    List<Integer> keys = primaryKeys(failedCache, 2, r.nextInt(key_bound));

                    Integer key0 = keys.get(0);
                    Integer key1 = keys.get(1);

                    Ignite backup = backupNode(key0, cacheName);

                    assertNotSame(failed, backup);

                    IgniteCache<Integer, Integer> backupCache = backup.getOrCreateCache(cacheName);

                    try (Transaction tx = backup.transactions().txStart()) {
                        backupCache.put(key0, key0);

                        readyLatch.countDown();
                        failedLatch.await();

                        try {
                            backupCache.put(key1, key1);

                            fail("Should not happen");
                        }
                        catch (Exception ignored) {
                            // Transaction broken because of primary left.
                        }
                    }
                }
                catch (Exception e) {
                    fail("Should not happen [exception=" + e + "]");
                }
            }, multiplicator);

            IgniteInternalFuture<?> nearThenPrimaryFut = multithreadedAsync(() -> {
                try {
                    Integer key0 = nearKeys(failedCache, 1, r.nextInt(key_bound)).get(0);
                    Integer key1 = primaryKeys(failedCache, 1, r.nextInt(key_bound)).get(0);

                    Ignite primary = primaryNode(key0, cacheName);

                    assertNotSame(failed, primary);

                    IgniteCache<Integer, Integer> primaryCache = primary.getOrCreateCache(cacheName);

                    try (Transaction tx = primary.transactions().txStart()) {
                        primaryCache.put(key0, key0);

                        readyLatch.countDown();
                        failedLatch.await();

                        try {
                            primaryCache.put(key1, key1);

                            fail("Should not happen");
                        }
                        catch (Exception ignored) {
                            // Transaction broken because of primary left.
                        }
                    }
                }
                catch (Exception e) {
                    fail("Should not happen [exception=" + e + "]");
                }
            }, multiplicator);

            IgniteInternalFuture<?> nearThenBackupFut = multithreadedAsync(() -> {
                try {
                    Integer key0 = nearKeys(failedCache, 1, r.nextInt(key_bound)).get(0);
                    Integer key1 = backupKeys(failedCache, 1, r.nextInt(key_bound)).get(0);

                    Ignite primary = primaryNode(key0, cacheName);

                    assertNotSame(failed, primary);

                    IgniteCache<Integer, Integer> primaryCache = primary.getOrCreateCache(cacheName);

                    try (Transaction tx = primary.transactions().txStart()) {
                        primaryCache.put(key0, key0);

                        readyLatch.countDown();
                        failedLatch.await();

                        primaryCache.put(key1, key1);

                        tx.commit();
                    }

                    assertEquals(key0, primaryCache.get(key0));
                    assertEquals(key1, primaryCache.get(key1));
                }
                catch (Exception e) {
                    fail("Should not happen [exception=" + e + "]");
                }
            }, multiplicator);

            readyLatch.await();

            failed.close(); // Stopping node.

            awaitPartitionMapExchange();

            failedLatch.countDown();

            nearThenNearFut.get();
            primaryThenPrimaryFut.get();
            nearThenPrimaryFut.get();
            nearThenBackupFut.get();

            int pmeFreeCnt = 0;

            for (Ignite ignite : G.allGrids()) {
                assertEquals(nodes + 1, ignite.cluster().topologyVersion());

                ExchangeContext ctx =
                    ((IgniteEx)ignite).context().cache().context().exchange().lastFinishedFuture().context();

                if (ctx.exchangeFreeSwitch())
                    pmeFreeCnt++;
            }

            assertEquals(nodes - 1, pmeFreeCnt);

            assertEquals(0, cnt.get());
        }
        finally {
            persistence = false;
        }
    }

    /**
     *
     */
    private static class Map4PartitionsTo4NodesAffinityFunction extends RendezvousAffinityFunction {
        /**
         * Default constructor.
         */
        public Map4PartitionsTo4NodesAffinityFunction() {
            super(false, 4);
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            List<List<ClusterNode>> res = new ArrayList<>(4);

            // Partitions by owners (node{parts}): 0{0,3}, 1{0,1}, 2{1,2}, 3{2,3}
            if (affCtx.currentTopologySnapshot().size() == 4) {
                List<ClusterNode> p0 = new ArrayList<>();

                p0.add(affCtx.currentTopologySnapshot().get(0));
                p0.add(affCtx.currentTopologySnapshot().get(1));

                List<ClusterNode> p1 = new ArrayList<>();

                p1.add(affCtx.currentTopologySnapshot().get(1));
                p1.add(affCtx.currentTopologySnapshot().get(2));

                List<ClusterNode> p2 = new ArrayList<>();

                p2.add(affCtx.currentTopologySnapshot().get(2));
                p2.add(affCtx.currentTopologySnapshot().get(3));

                List<ClusterNode> p3 = new ArrayList<>();

                p3.add(affCtx.currentTopologySnapshot().get(3));
                p3.add(affCtx.currentTopologySnapshot().get(0));

                res.add(p0);
                res.add(p1);
                res.add(p2);
                res.add(p3);
            }

            return res;
        }
    }
}
