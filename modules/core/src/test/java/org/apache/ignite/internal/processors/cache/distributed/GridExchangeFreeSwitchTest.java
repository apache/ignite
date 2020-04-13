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
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.ExchangeContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsAbstractMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.mvcc.MvccProcessor;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PME_FREE_SWITCH_DISABLED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.IgniteFeatures.PME_FREE_SWITCH;
import static org.apache.ignite.internal.IgniteFeatures.allNodesSupports;
import static org.apache.ignite.internal.IgniteFeatures.nodeSupports;

/**
 *
 */
public class GridExchangeFreeSwitchTest extends GridCommonAbstractTest {
    /** Persistence flag. */
    private boolean persistence;

    /** Cache name. */
    private static final String CACHE_NAME = "testCache";

    /** Cache configuration closure. */
    private IgniteClosure<String, CacheConfiguration[]> cacheC;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        cfg.setCacheConfiguration(cacheC != null ?
            cacheC.apply(igniteInstanceName) : new CacheConfiguration[] {cacheConfiguration()});

        cfg.setClusterStateOnStart(ClusterState.INACTIVE);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        DataRegionConfiguration drCfg = new DataRegionConfiguration();

        drCfg.setPersistenceEnabled(persistence);

        dsCfg.setDefaultDataRegionConfiguration(drCfg);

        cfg.setDataStorageConfiguration(dsCfg);

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
     * Checks PME happen in case of baseline auto-adjust (in-memory cluster). It's not possible to perform switch since
     * primaries may change.
     */
    @Test
    public void testNonBaselineNodeLeftOnFullyRebalancedCluster() throws Exception {
        testNodeLeftOnFullyRebalancedCluster(PmeFreeSwitchDisabledNode.NONE);
    }

    /**
     * Checks PME is absent in case of fixed baseline. It's possible to perform switch since primaries can't change.
     */
    @Test
    public void testBaselineNodeLeftOnFullyRebalancedCluster() throws Exception {
        testBaselineNodeLeftOnFullyRebalancedCluster(PmeFreeSwitchDisabledNode.NONE);
    }

    /**
     * Checks PME is absent/present with all nodes except first one supports PME-free switch.
     */
    @Test
    public void testBaselineNodeLeftOnFullyRebalancedClusterPmeFreeDisabledFirstNode() throws Exception {
        testBaselineNodeLeftOnFullyRebalancedCluster(PmeFreeSwitchDisabledNode.FIRST);
    }

    /**
     * Checks PME is absent/present with all nodes except midlle one supports PME-free switch.
     */
    @Test
    public void testBaselineNodeLeftOnFullyRebalancedClusterPmeFreeDisabledMiddleNode() throws Exception {
        testBaselineNodeLeftOnFullyRebalancedCluster(PmeFreeSwitchDisabledNode.MIDDLE);
    }

    /**
     * Checks PME is absent/present with all nodes except last one supports PME-free switch.
     */
    @Test
    public void testBaselineNodeLeftOnFullyRebalancedClusterPmeFreeDisabledLastNode() throws Exception {
        testBaselineNodeLeftOnFullyRebalancedCluster(PmeFreeSwitchDisabledNode.LAST);
    }

    /**
     * Checks PME is absent/present in case of persistence enabled.
     */
    private void testBaselineNodeLeftOnFullyRebalancedCluster(PmeFreeSwitchDisabledNode order) throws Exception {
        persistence = true;

        try {
            testNodeLeftOnFullyRebalancedCluster(order);
        }
        finally {
            persistence = false;
        }
    }

    /**
     * Starts node with PME-free feature explicitly disabled.
     */
    private void startNodeWithPmeFreeSwitchDisabled() throws Exception {
        try {
            System.setProperty(IGNITE_PME_FREE_SWITCH_DISABLED, "true");

            Ignite ignite = startGrid(G.allGrids().size());

            assertFalse(nodeSupports(ignite.cluster().localNode(), PME_FREE_SWITCH));
        }
        finally {
            System.clearProperty(IGNITE_PME_FREE_SWITCH_DISABLED);
        }
    }

    /**
     * Checks node left PME absent/present on fully rebalanced topology (Latest PME == LAA).
     */
    private void testNodeLeftOnFullyRebalancedCluster(PmeFreeSwitchDisabledNode disabled) throws Exception {
        int nodes = 10;

        switch (disabled) {
            case FIRST:
                startNodeWithPmeFreeSwitchDisabled();

                startGridsMultiThreaded(1, nodes - 1);

                break;

            case MIDDLE:
                startGridsMultiThreaded(0, (nodes / 2) - 1);

                startNodeWithPmeFreeSwitchDisabled();

                int started = G.allGrids().size();

                startGridsMultiThreaded(started, nodes - started);

                break;

            case LAST:
                startGridsMultiThreaded(0, nodes - 1);

                startNodeWithPmeFreeSwitchDisabled();

                break;

            case NONE:
                startGridsMultiThreaded(0, nodes);

                break;

            default:
                throw new UnsupportedOperationException();
        }

        assertEquals(nodes, G.allGrids().size());

        assertEquals(ClusterState.INACTIVE, grid(0).cluster().state());

        grid(0).cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        AtomicLong singleCnt = new AtomicLong();
        AtomicLong fullCnt = new AtomicLong();

        startPmeMessagesCounting(nodes, singleCnt, fullCnt);

        Random r = new Random();

        while (nodes > 1) {
            G.allGrids().get(r.nextInt(nodes--)).close(); // Stopping random node.

            awaitPartitionMapExchange(true, true, null, true);

            IgniteEx alive = (IgniteEx)G.allGrids().get(0);

            assertTrue(alive.context().cache().context().exchange().lastFinishedFuture().rebalanced());

            boolean pmeFreeSwitch = persistence && allNodesSupports(alive.cluster().nodes(), PME_FREE_SWITCH);

            assertEquals(pmeFreeSwitch ? 0 : (nodes - 1), singleCnt.get());
            assertEquals(pmeFreeSwitch ? 0 : (nodes - 1), fullCnt.get());

            singleCnt.set(0);
            fullCnt.set(0);
        }
    }

    /**
     * @param nodes Nodes.
     * @param singleCnt Counter for GridDhtPartitionsSingleMessage.
     * @param fullCnt Counter for GridDhtPartitionsFullMessage.
     */
    private void startPmeMessagesCounting(int nodes, AtomicLong singleCnt, AtomicLong fullCnt) {
        for (int i = 0; i < nodes; i++) {
            TestRecordingCommunicationSpi spi =
                (TestRecordingCommunicationSpi)ignite(i).configuration().getCommunicationSpi();

            spi.closure(new IgniteBiInClosure<ClusterNode, Message>() {
                @Override public void apply(ClusterNode node, Message msg) {
                    if (msg.getClass().equals(GridDhtPartitionsSingleMessage.class) &&
                        ((GridDhtPartitionsAbstractMessage)msg).exchangeId() != null)
                        singleCnt.incrementAndGet();

                    if (msg.getClass().equals(GridDhtPartitionsFullMessage.class) &&
                        ((GridDhtPartitionsAbstractMessage)msg).exchangeId() != null)
                        fullCnt.incrementAndGet();
                }
            });
        }
    }

    /**
     *
     */
    @Test
    public void testNoTransactionsWaitAtNodeLeftWithZeroBackupsAndLossIgnore() throws Exception {
        testNoTransactionsWaitAtNodeLeft(0, PartitionLossPolicy.IGNORE);
    }

    /**
     *
     */
    @Test
    public void testNoTransactionsWaitAtNodeLeftWithZeroBackupsAndLossSafe() throws Exception {
        testNoTransactionsWaitAtNodeLeft(0, PartitionLossPolicy.READ_WRITE_SAFE);
    }

    /**
     *
     */
    @Test
    public void testNoTransactionsWaitAtNodeLeftWithSingleBackup() throws Exception {
        testNoTransactionsWaitAtNodeLeft(1, PartitionLossPolicy.IGNORE);
    }

    /**
     * Checks that transaction can be continued on node left and there is no waiting for it's completion in case
     * baseline was not changed and cluster was fully rebalanced.
     */
    private void testNoTransactionsWaitAtNodeLeft(int backups, PartitionLossPolicy lossPlc) throws Exception {
        persistence = true;

        String cacheName = "partitioned";

        try {
            cacheC = new IgniteClosure<String, CacheConfiguration[]>() {
                @Override public CacheConfiguration[] apply(String igniteInstanceName) {
                    CacheConfiguration ccfg = new CacheConfiguration();

                    ccfg.setName(cacheName);
                    ccfg.setWriteSynchronizationMode(FULL_SYNC);
                    ccfg.setBackups(backups);
                    ccfg.setPartitionLossPolicy(lossPlc);
                    ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
                    ccfg.setAffinity(new Map4PartitionsTo4NodesAffinityFunction());

                    return new CacheConfiguration[] {ccfg};
                }
            };

            int nodes = 4;

            startGridsMultiThreaded(nodes);

            AtomicLong singleCnt = new AtomicLong();
            AtomicLong fullCnt = new AtomicLong();

            startPmeMessagesCounting(nodes, singleCnt, fullCnt);

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

            AtomicInteger key_from = new AtomicInteger();

            CountDownLatch readyLatch = new CountDownLatch((backups > 0 ? 4 : 2) * multiplicator);
            CountDownLatch failedLatch = new CountDownLatch(1);

            IgniteCache<Integer, Integer> failedCache = failed.getOrCreateCache(cacheName);

            IgniteInternalFuture<?> nearThenNearFut = multithreadedAsync(() -> {
                try {
                    List<Integer> keys = nearKeys(failedCache, 2, key_from.addAndGet(100));

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

            IgniteInternalFuture<?> primaryThenPrimaryFut = backups > 0 ? multithreadedAsync(() -> {
                try {
                    List<Integer> keys = primaryKeys(failedCache, 2, key_from.addAndGet(100));

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
            }, multiplicator) : new GridFinishedFuture<>();

            IgniteInternalFuture<?> nearThenPrimaryFut = multithreadedAsync(() -> {
                try {
                    Integer key0 = nearKeys(failedCache, 1, key_from.addAndGet(100)).get(0);
                    Integer key1 = primaryKeys(failedCache, 1, key_from.addAndGet(100)).get(0);

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

            IgniteInternalFuture<?> nearThenBackupFut = backups > 0 ? multithreadedAsync(() -> {
                try {
                    Integer key0 = nearKeys(failedCache, 1, key_from.addAndGet(100)).get(0);
                    Integer key1 = backupKeys(failedCache, 1, key_from.addAndGet(100)).get(0);

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
            }, multiplicator) : new GridFinishedFuture<>();

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

            assertEquals(0, singleCnt.get());
            assertEquals(0, fullCnt.get());
        }
        finally {
            persistence = false;
        }
    }

    /**
     *
     */
    @Test
    public void testOnlyAffectedNodesWaitsForRecovery() throws Exception {
        persistence = true;

        String partCacheName = "partitioned";
        String replCacheName = "replicated";

        try {
            cacheC = new IgniteClosure<String, CacheConfiguration[]>() {
                @Override public CacheConfiguration[] apply(String igniteInstanceName) {
                    CacheConfiguration partitionedCcfg = new CacheConfiguration();

                    partitionedCcfg.setName(partCacheName);
                    partitionedCcfg.setWriteSynchronizationMode(FULL_SYNC);
                    partitionedCcfg.setBackups(2);
                    partitionedCcfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
                    partitionedCcfg.setAffinity(new Map4PartitionsTo4NodesAffinityFunction());

                    CacheConfiguration replicatedCcfg = new CacheConfiguration();

                    replicatedCcfg.setName(replCacheName);
                    replicatedCcfg.setWriteSynchronizationMode(FULL_SYNC);
                    replicatedCcfg.setCacheMode(CacheMode.REPLICATED);
                    replicatedCcfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

                    return new CacheConfiguration[] {partitionedCcfg, replicatedCcfg};
                }
            };

            int nodes = 4;

            startGridsMultiThreaded(nodes);

            for (int i = 0; i < nodes; i++) {
                TestRecordingCommunicationSpi spi =
                    (TestRecordingCommunicationSpi)ignite(i).configuration().getCommunicationSpi();

                spi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                    @Override public boolean apply(ClusterNode node, Message msg) {
                        return msg.getClass().equals(GridCacheTxRecoveryRequest.class);
                    }
                });
            }

            CountDownLatch partLatch = new CountDownLatch(1);
            CountDownLatch replLatch = new CountDownLatch(1);
            CountDownLatch failedLatch = new CountDownLatch(1);

            Random r = new Random();

            Ignite failed = grid(r.nextInt(nodes));

            IgniteCache<Integer, Integer> partCache = failed.getOrCreateCache(partCacheName);
            IgniteCache<Integer, Integer> replCache = failed.getOrCreateCache(replCacheName);

            Integer partKey = primaryKey(partCache);
            Integer replKey = primaryKey(replCache);

            Ignite primary = primaryNode(partKey, partCacheName);

            List<Ignite> backupNodes = backupNodes(partKey, partCacheName);
            List<Ignite> nearNodes = new ArrayList<>(G.allGrids());

            nearNodes.remove(primary);
            nearNodes.removeAll(backupNodes);

            assertEquals(failed, primary);
            assertFalse(backupNodes.isEmpty());
            assertFalse(nearNodes.isEmpty());
            assertTrue(Collections.disjoint(backupNodes, nearNodes));
            assertEquals(nodes - 1, backupNodes.size() + nearNodes.size());

            IgniteInternalFuture<?> partFut = multithreadedAsync(() -> {
                try {
                    checkActiveTransactionsCount(primary, 0, backupNodes, 0, nearNodes, 0);

                    Transaction tx = primary.transactions().txStart();

                    checkActiveTransactionsCount(primary, 1, backupNodes, 0, nearNodes, 0);

                    partCache.put(partKey, 1);

                    ((TransactionProxyImpl<?, ?>)tx).tx().prepare(true);

                    checkActiveTransactionsCount(primary, 1, backupNodes, 1, nearNodes, 0);

                    partLatch.countDown();
                    failedLatch.await();
                }
                catch (Exception e) {
                    fail("Should not happen [exception=" + e + "]");
                }
            }, 1);

            IgniteInternalFuture<?> replFut = multithreadedAsync(() -> {
                try {
                    partLatch.await(); // Waiting for partitioned cache tx preparation.

                    checkActiveTransactionsCount(primary, 1, backupNodes, 1, nearNodes, 0);

                    Transaction tx = primary.transactions().txStart();

                    checkActiveTransactionsCount(primary, 2, backupNodes, 1, nearNodes, 0);

                    replCache.put(replKey, 1);

                    ((TransactionProxyImpl<?, ?>)tx).tx().prepare(true);

                    checkActiveTransactionsCount(primary, 2, backupNodes, 2, nearNodes, 1);

                    replLatch.countDown();
                    failedLatch.await();
                }
                catch (Exception e) {
                    fail("Should not happen [exception=" + e + "]");
                }
            }, 1);

            partLatch.await();
            replLatch.await();

            checkActiveTransactionsCount(failed, 2, backupNodes, 2, nearNodes, 1);

            failed.close(); // Stopping node.

            assertTrue(GridTestUtils.waitForCondition( // Waiting for recovery start.
                () -> {
                    for (Ignite ignite : G.allGrids()) {
                        TestRecordingCommunicationSpi spi =
                            (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

                        if (!spi.hasBlockedMessages())
                            return false;
                    }

                    return true;
                }, 5000));

            failedLatch.countDown();

            partFut.get();
            replFut.get();

            checkActiveTransactionsCount(null /*stopped*/, 0, backupNodes, 2, nearNodes, 1);

            AtomicReference<IgniteInternalTx> replTx = new AtomicReference<>();

            for (Ignite near : nearNodes) { // Near nodes contain only replicated txs.
                Collection<IgniteInternalTx> txs =
                    ((IgniteEx)near).context().cache().context().tm().activeTransactions();

                IgniteInternalTx tx = txs.iterator().next();

                assertEquals(1, txs.size());
                assertTrue(tx.state() == TransactionState.PREPARED);
                assertTrue(tx.writeEntries().stream().allMatch(
                    entry -> entry.context().shared().cacheContext(entry.cacheId()).isReplicated()));
                assertTrue(replTx.get() == null || replTx.get().nearXidVersion().equals(tx.nearXidVersion()));

                replTx.set(tx);
            }

            CountDownLatch backupCreateLatch = new CountDownLatch(backupNodes.size());
            CountDownLatch backupPutLatch = new CountDownLatch(backupNodes.size());
            CountDownLatch backupCommitLatch = new CountDownLatch(backupNodes.size());
            CountDownLatch nearCreateLatch = new CountDownLatch(nearNodes.size());
            CountDownLatch nearPutLatch = new CountDownLatch(nearNodes.size());
            CountDownLatch nearCommitLatch = new CountDownLatch(nearNodes.size());

            BiConsumer<Ignite, T3<CountDownLatch, CountDownLatch, CountDownLatch>> txRun = // Counts tx's creations and preparations.
                (Ignite ignite, T3</*create*/CountDownLatch, /*put*/CountDownLatch, /*commit*/CountDownLatch> latches) -> {
                    try {
                        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(replCacheName);

                        try (Transaction tx = ignite.transactions().txStart()) {
                            latches.get1().countDown(); // Create.

                            cache.put(primaryKeys(cache, 100).get(99), 2);

                            latches.get2().countDown(); // Put.

                            tx.commit();

                            latches.get3().countDown(); // Commit.
                        }
                    }
                    catch (Exception e) {
                        fail("Should not happen [exception=" + e + "]");
                    }
                };

            List<IgniteInternalFuture<?>> futs = new ArrayList<>();

            for (Ignite backup : backupNodes)
                futs.add(multithreadedAsync(() ->
                    txRun.accept(backup, new T3<>(backupCreateLatch, backupPutLatch, backupCommitLatch)), 1));

            for (Ignite near : nearNodes)
                futs.add(multithreadedAsync(() ->
                    txRun.accept(near, new T3<>(nearCreateLatch, nearPutLatch, nearCommitLatch)), 1));

            checkUpcomingTransactionsState( // Switch in progress cluster-wide.
                backupCreateLatch, 0, // Started.
                backupPutLatch, backupNodes.size(),
                backupCommitLatch, backupNodes.size(),
                nearCreateLatch, 0, // Started.
                nearPutLatch, nearNodes.size(),
                nearCommitLatch, nearNodes.size());

            checkActiveTransactionsCount(
                null, 0,
                backupNodes, 2 /* prepared replicated + prepard partitioned */ + 1 /* started txRun per node */,
                nearNodes, 1 /* prepared replicated */ + 1 /* started txRun per node */);

            for (Ignite ignite : G.allGrids()) {
                TestRecordingCommunicationSpi spi =
                    (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

                spi.stopBlock(true, t -> { // Replicated recovery.
                    Message msg = t.get2().message();

                    return ((GridCacheTxRecoveryRequest)msg).nearXidVersion().equals(replTx.get().nearXidVersion());
                });
            }

            checkUpcomingTransactionsState(
                backupCreateLatch, 0, // Started.
                backupPutLatch, backupNodes.size(),
                backupCommitLatch, backupNodes.size(),
                nearCreateLatch, 0, // Started.
                nearPutLatch, 0, // Near nodes able to start transactions once replicated caches recovered,
                nearCommitLatch, nearNodes.size()); // But not able to commit transactions since their backups still in switch.

            checkActiveTransactionsCount(
                null, 0,
                backupNodes, 1 /* prepared partitioned */ + 1 /* started txRun per node */,
                nearNodes, 1 /* started txRun per node */);

            for (Ignite ignite : G.allGrids()) {
                TestRecordingCommunicationSpi spi =
                    (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

                spi.stopBlock(true, t -> { // Partitioned recovery.
                    return true;
                });
            }

            checkUpcomingTransactionsState( // Switches finished cluster-wide, all transactions can be committed.
                backupCreateLatch, 0,
                backupPutLatch, 0,
                backupCommitLatch, 0,
                nearCreateLatch, 0,
                nearPutLatch, 0,
                nearCommitLatch, 0);

            // Final check that active transactions are absent.
            checkActiveTransactionsCount(null, 0, backupNodes, 0, nearNodes, 0);

            for (IgniteInternalFuture<?> fut : futs)
                fut.get();
        }
        finally {
            persistence = false;
        }
    }

    /**
     *
     */
    private void checkActiveTransactionsCount(
        Ignite primary,
        int primaryCnt,
        List<Ignite> backupNodes,
        int backupCnt,
        List<Ignite> nearNodes,
        int nearCnt) {
        Function<Ignite, Collection<IgniteInternalTx>> txs =
            ignite -> ((IgniteEx)ignite).context().cache().context().tm().activeTransactions();

        if (primary != null)
            assertEquals(primaryCnt, txs.apply(primary).size());

        for (Ignite backup : backupNodes)
            assertEquals(backupCnt, txs.apply(backup).size());

        for (Ignite near : nearNodes)
            assertEquals(nearCnt, txs.apply(near).size());
    }

    /**
     *
     */
    private void checkUpcomingTransactionsState(
        CountDownLatch backupCreateLatch,
        int backupCreateCnt,
        CountDownLatch backupPutLatch,
        int backupPutCnt,
        CountDownLatch backupCommitLatch,
        int backupCommitCnt,
        CountDownLatch nearCreateLatch,
        int nearCreateCnt,
        CountDownLatch nearPutLatch,
        int nearPutCnt,
        CountDownLatch nearCommitLatch,
        int nearCommitCnt) throws InterruptedException {
        checkTransactionsState(backupCreateLatch, backupCreateCnt);
        checkTransactionsState(backupPutLatch, backupPutCnt);
        checkTransactionsState(backupCommitLatch, backupCommitCnt);
        checkTransactionsState(nearCreateLatch, nearCreateCnt);
        checkTransactionsState(nearPutLatch, nearPutCnt);
        checkTransactionsState(nearCommitLatch, nearCommitCnt);
    }

    /**
     *
     */
    private void checkTransactionsState(CountDownLatch latch, int cnt) throws InterruptedException {
        if (cnt > 0)
            assertEquals(cnt, latch.getCount()); // Switch in progress.
        else
            latch.await(); // Switch finished (finishing).
    }

    /**
     *
     */
    @Test
    public void testLateAffinityAssignmentOnBackupLeftAndJoin() throws Exception {
        String cacheName = "single-partitioned";

        cacheC = new IgniteClosure<String, CacheConfiguration[]>() {
            @Override public CacheConfiguration[] apply(String igniteInstanceName) {
                CacheConfiguration ccfg = new CacheConfiguration();

                ccfg.setName(cacheName);
                ccfg.setAffinity(new Map1PartitionTo2NodesAffinityFunction());

                return new CacheConfiguration[] {ccfg};
            }
        };

        persistence = true;

        try {
            startGrid(0); // Primary partition holder.
            startGrid(1); // Backup partition holder.

            grid(0).cluster().active(true);

            grid(1).close(); // Stopping backup partition holder.

            grid(0).getOrCreateCache(cacheName).put(1, 1); // Updating primary partition to cause rebalance.

            startGrid(1); // Restarting backup partition holder.

            awaitPartitionMapExchange();

            AffinityTopologyVersion topVer = grid(0).context().discovery().topologyVersionEx();

            assertEquals(topVer.topologyVersion(), 4);
            assertEquals(topVer.minorTopologyVersion(), 1); // LAA happen on backup partition holder restart.

            GridDhtPartitionsExchangeFuture fut4 = null;
            GridDhtPartitionsExchangeFuture fut41 = null;

            for (GridDhtPartitionsExchangeFuture fut : grid(0).context().cache().context().exchange().exchangeFutures()) {
                AffinityTopologyVersion ver = fut.topologyVersion();

                if (ver.topologyVersion() == 4) {
                    if (ver.minorTopologyVersion() == 0)
                        fut4 = fut;
                    else if (ver.minorTopologyVersion() == 1)
                        fut41 = fut;
                }
            }

            assertFalse(fut4.rebalanced()); // Backup partition holder restart cause non-rebalanced state.

            assertTrue(fut41.rebalanced()); // LAA.

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

            int backups = affCtx.backups();

            if (affCtx.currentTopologySnapshot().size() == 4) {
                List<ClusterNode> p0 = new ArrayList<>();
                List<ClusterNode> p1 = new ArrayList<>();
                List<ClusterNode> p2 = new ArrayList<>();
                List<ClusterNode> p3 = new ArrayList<>();

                p0.add(affCtx.currentTopologySnapshot().get(0));
                p1.add(affCtx.currentTopologySnapshot().get(1));
                p2.add(affCtx.currentTopologySnapshot().get(2));
                p3.add(affCtx.currentTopologySnapshot().get(3));

                if (backups >= 1) {
                    p0.add(affCtx.currentTopologySnapshot().get(1));
                    p1.add(affCtx.currentTopologySnapshot().get(2));
                    p2.add(affCtx.currentTopologySnapshot().get(3));
                    p3.add(affCtx.currentTopologySnapshot().get(0));
                }

                if (backups == 2) {
                    p0.add(affCtx.currentTopologySnapshot().get(2));
                    p1.add(affCtx.currentTopologySnapshot().get(3));
                    p2.add(affCtx.currentTopologySnapshot().get(0));
                    p3.add(affCtx.currentTopologySnapshot().get(1));
                }

                res.add(p0);
                res.add(p1);
                res.add(p2);
                res.add(p3);
            }

            return res;
        }
    }

    /**
     *
     */
    private static class Map1PartitionTo2NodesAffinityFunction extends RendezvousAffinityFunction {
        /**
         * Default constructor.
         */
        public Map1PartitionTo2NodesAffinityFunction() {
            super(false, 1);
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            List<List<ClusterNode>> res = new ArrayList<>(2);

            List<ClusterNode> p0 = new ArrayList<>();

            p0.add(affCtx.currentTopologySnapshot().get(0));

            if (affCtx.currentTopologySnapshot().size() == 2)
                p0.add(affCtx.currentTopologySnapshot().get(1));

            res.add(p0);

            return res;
        }
    }

    /**
     * Specifies node to start with IGNITE_PME_FREE_SWITCH_DISABLED JVM option.
     */
    private enum PmeFreeSwitchDisabledNode {
        /** First. */
        FIRST,

        /** Middle. */
        MIDDLE,

        /** Last. */
        LAST,

        /** None. */
        NONE
    }
}
