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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccTxRecoveryTest.NodeMode.CLIENT;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccTxRecoveryTest.NodeMode.SERVER;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccTxRecoveryTest.TxEndResult.COMMIT;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccTxRecoveryTest.TxEndResult.ROLLBAK;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.PREPARED;
import static org.apache.ignite.transactions.TransactionState.PREPARING;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;

/** */
public class CacheMvccTxRecoveryTest extends CacheMvccAbstractTest {
    /** */
    public enum TxEndResult {
        /** */ COMMIT,
        /** */ ROLLBAK
    }

    /** */
    public enum NodeMode {
        /** */ SERVER,
        /** */ CLIENT
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        throw new RuntimeException("Is not supposed to be used");
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRecoveryCommitNearFailure1() throws Exception {
        checkRecoveryNearFailure(COMMIT, CLIENT);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRecoveryCommitNearFailure2() throws Exception {
        checkRecoveryNearFailure(COMMIT, SERVER);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRecoveryRollbackNearFailure1() throws Exception {
        checkRecoveryNearFailure(ROLLBAK, CLIENT);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRecoveryRollbackNearFailure2() throws Exception {
        checkRecoveryNearFailure(ROLLBAK, SERVER);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRecoveryCommitPrimaryFailure1() throws Exception {
        checkRecoveryPrimaryFailure(COMMIT, false);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRecoveryRollbackPrimaryFailure1() throws Exception {
        checkRecoveryPrimaryFailure(ROLLBAK, false);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRecoveryCommitPrimaryFailure2() throws Exception {
        checkRecoveryPrimaryFailure(COMMIT, true);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRecoveryRollbackPrimaryFailure2() throws Exception {
        checkRecoveryPrimaryFailure(ROLLBAK, true);
    }

    /** */
    private void checkRecoveryNearFailure(TxEndResult endRes, NodeMode nearNodeMode) throws Exception {
        int gridCnt = 4;
        int baseCnt = gridCnt - 1;

        boolean commit = endRes == COMMIT;

        startGridsMultiThreaded(baseCnt);

        // tweak client/server near
        client = nearNodeMode == CLIENT;

        IgniteEx nearNode = startGrid(baseCnt);

        IgniteCache<Object, Object> cache = nearNode.getOrCreateCache(basicCcfg()
            .setBackups(1));

        Affinity<Object> aff = nearNode.affinity(DEFAULT_CACHE_NAME);

        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            if (aff.isPrimary(grid(0).localNode(), i) && aff.isBackup(grid(1).localNode(), i)) {
                keys.add(i);
                break;
            }
        }

        for (int i = 0; i < 100; i++) {
            if (aff.isPrimary(grid(1).localNode(), i) && aff.isBackup(grid(2).localNode(), i)) {
                keys.add(i);
                break;
            }
        }

        assert keys.size() == 2;

        TestRecordingCommunicationSpi nearComm
            = (TestRecordingCommunicationSpi)nearNode.configuration().getCommunicationSpi();

        if (!commit)
            nearComm.blockMessages(GridNearTxPrepareRequest.class, grid(1).name());

        GridTestUtils.runAsync(() -> {
            // run in separate thread to exclude tx from thread-local map
            GridNearTxLocal nearTx
                = ((TransactionProxyImpl)nearNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)).tx();

            for (Integer k : keys)
                cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(k));

            List<IgniteInternalTx> txs = IntStream.range(0, baseCnt)
                .mapToObj(i -> txsOnNode(grid(i), nearTx.xidVersion()))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

            IgniteInternalFuture<?> prepareFut = nearTx.prepareNearTxLocal();

            if (commit)
                prepareFut.get();
            else
                assertConditionEventually(() -> txs.stream().anyMatch(tx -> tx.state() == PREPARED));

            // drop near
            nearNode.close();

            assertConditionEventually(() -> txs.stream().allMatch(tx -> tx.state() == (commit ? COMMITTED : ROLLED_BACK)));

            return null;
        }).get();

        if (commit) {
            assertConditionEventually(() -> {
                int rowsCnt = grid(0).cache(DEFAULT_CACHE_NAME)
                    .query(new SqlFieldsQuery("select * from Integer")).getAll().size();
                return rowsCnt == keys.size();
            });
        }
        else {
            int rowsCnt = G.allGrids().get(0).cache(DEFAULT_CACHE_NAME)
                .query(new SqlFieldsQuery("select * from Integer")).getAll().size();

            assertEquals(0, rowsCnt);
        }

        assertPartitionCountersAreConsistent(keys, grids(baseCnt, i -> true));
    }

    /** */
    private void checkRecoveryPrimaryFailure(TxEndResult endRes, boolean mvccCrd) throws Exception {
        int gridCnt = 4;
        int baseCnt = gridCnt - 1;

        boolean commit = endRes == COMMIT;

        startGridsMultiThreaded(baseCnt);

        client = true;

        IgniteEx nearNode = startGrid(baseCnt);

        IgniteCache<Object, Object> cache = nearNode.getOrCreateCache(basicCcfg()
            .setBackups(1));

        Affinity<Object> aff = nearNode.affinity(DEFAULT_CACHE_NAME);

        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            if (aff.isPrimary(grid(0).localNode(), i) && aff.isBackup(grid(1).localNode(), i)) {
                keys.add(i);
                break;
            }
        }

        for (int i = 0; i < 100; i++) {
            if (aff.isPrimary(grid(1).localNode(), i) && aff.isBackup(grid(2).localNode(), i)) {
                keys.add(i);
                break;
            }
        }

        assert keys.size() == 2;

        int victim, victimBackup;

        if (mvccCrd) {
            victim = 0;
            victimBackup = 1;
        }
        else {
            victim = 1;
            victimBackup = 2;
        }

        TestRecordingCommunicationSpi victimComm = (TestRecordingCommunicationSpi)grid(victim).configuration().getCommunicationSpi();

        if (commit)
            victimComm.blockMessages(GridNearTxFinishResponse.class, nearNode.name());
        else
            victimComm.blockMessages(GridDhtTxPrepareRequest.class, grid(victimBackup).name());

        GridNearTxLocal nearTx
            = ((TransactionProxyImpl)nearNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)).tx();

        for (Integer k : keys)
            cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(k));

        List<IgniteInternalTx> txs = IntStream.range(0, baseCnt)
            .filter(i -> i != victim)
            .mapToObj(i -> txsOnNode(grid(i), nearTx.xidVersion()))
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

        IgniteInternalFuture<IgniteInternalTx> commitFut = nearTx.commitAsync();

        if (commit)
            assertConditionEventually(() -> txs.stream().allMatch(tx -> tx.state() == COMMITTED));
        else
            assertConditionEventually(() -> txs.stream().anyMatch(tx -> tx.state() == PREPARED));

        // drop victim
        grid(victim).close();

        awaitPartitionMapExchange();

        assertConditionEventually(() -> txs.stream().allMatch(tx -> tx.state() == (commit ? COMMITTED : ROLLED_BACK)));

        assert victimComm.hasBlockedMessages();

        if (commit) {
            assertConditionEventually(() -> {
                int rowsCnt = G.allGrids().get(0).cache(DEFAULT_CACHE_NAME)
                    .query(new SqlFieldsQuery("select * from Integer")).getAll().size();
                return rowsCnt == keys.size();
            });
        }
        else {
            int rowsCnt = G.allGrids().get(0).cache(DEFAULT_CACHE_NAME)
                .query(new SqlFieldsQuery("select * from Integer")).getAll().size();

            assertEquals(0, rowsCnt);
        }

        assertTrue(commitFut.isDone());

        assertPartitionCountersAreConsistent(keys, grids(baseCnt, i -> i != victim));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRecoveryCommit() throws Exception {
        startGridsMultiThreaded(2);

        client = true;

        IgniteEx ign = startGrid(2);

        IgniteCache<Object, Object> cache = ign.getOrCreateCache(basicCcfg());

        AtomicInteger keyCntr = new AtomicInteger();

        ArrayList<Integer> keys = new ArrayList<>();

        ign.cluster().forServers().nodes()
            .forEach(node -> keys.add(keyForNode(ign.affinity(DEFAULT_CACHE_NAME), keyCntr, node)));

        GridTestUtils.runAsync(() -> {
            // run in separate thread to exclude tx from thread-local map
            Transaction tx = ign.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

            for (Integer k : keys)
                cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(k));

            ((TransactionProxyImpl)tx).tx().prepareNearTxLocal().get();

            return null;
        }).get();

        // drop near
        stopGrid(2, true);

        IgniteEx srvNode = grid(0);

        assertConditionEventually(
            () -> srvNode.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("select * from Integer")).getAll().size() == 2
        );

        assertPartitionCountersAreConsistent(keys, G.allGrids());
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testCountersNeighborcastServerFailed() throws Exception {
        // Reopen https://issues.apache.org/jira/browse/IGNITE-10766 if starts failing

        int srvCnt = 4;

        startGridsMultiThreaded(srvCnt);

        client = true;

        IgniteEx ign = startGrid(srvCnt);

        IgniteCache<Object, Object> cache = ign.getOrCreateCache(basicCcfg()
            .setBackups(2));

        ArrayList<Integer> keys = new ArrayList<>();

        int vid = 3;

        IgniteEx victim = grid(vid);

        Affinity<Object> aff = ign.affinity(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100; i++) {
            if (aff.isPrimary(victim.localNode(), i) && !aff.isBackup(grid(0).localNode(), i)) {
                keys.add(i);
                break;
            }
        }

        for (int i = 0; i < 100; i++) {
            if (aff.isPrimary(victim.localNode(), i) && !aff.isBackup(grid(1).localNode(), i)) {
                keys.add(i);
                break;
            }
        }

        assert keys.size() == 2 && !keys.contains(99);

        // prevent prepare on one backup
        ((TestRecordingCommunicationSpi)victim.configuration().getCommunicationSpi())
            .blockMessages(GridDhtTxPrepareRequest.class, grid(0).name());

        GridNearTxLocal nearTx = ((TransactionProxyImpl)ign.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)).tx();

        for (Integer k : keys)
            cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(k));

        List<IgniteInternalTx> txs = IntStream.range(0, srvCnt)
            .mapToObj(this::grid)
            .filter(g -> g != victim)
            .map(g -> txsOnNode(g, nearTx.xidVersion()))
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

        nearTx.commitAsync();

        // await tx partially prepared
        assertConditionEventually(() -> txs.stream().anyMatch(tx -> tx.state() == PREPARED));

        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);

        IgniteInternalFuture<Object> backgroundTxFut = GridTestUtils.runAsync(() -> {
            try (Transaction ignored = ign.transactions().txStart()) {
                boolean upd = false;

                for (int i = 100; i < 200; i++) {
                    if (!aff.isPrimary(victim.localNode(), i)) {
                        cache.put(i, 11);
                        upd = true;
                        break;
                    }
                }

                assert upd;

                latch1.countDown();

                latch2.await(getTestTimeout(), TimeUnit.MILLISECONDS);
            }

            return null;
        });

        latch1.await(getTestTimeout(), TimeUnit.MILLISECONDS);

        // drop primary
        victim.close();

        // do all assertions before rebalance
        assertConditionEventually(() -> txs.stream().allMatch(tx -> tx.state() == ROLLED_BACK));

        List<IgniteEx> liveNodes = grids(srvCnt, i -> i != vid);

        assertPartitionCountersAreConsistent(keys, liveNodes);

        latch2.countDown();

        backgroundTxFut.get(getTestTimeout());

        assertTrue(liveNodes.stream()
            .map(node -> node.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("select * from Integer")).getAll())
            .allMatch(Collection::isEmpty));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testTxRecoveryWithLostFullMessageOnJoiningBackupNode() throws Exception {
        CountDownLatch success = new CountDownLatch(1);

        int joiningBackupNodeId = 2;

        IgniteEx crd = startGrid(0);

        IgniteEx partOwner = startGrid(1);

        IgniteCache<Object, Object> cache = partOwner.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL)
            .setCacheMode(PARTITIONED)
            .setIndexedTypes(Integer.class, Integer.class)
            .setBackups(2));

        // prevent FullMassage on joining backup node
        ((TestRecordingCommunicationSpi)crd.configuration().getCommunicationSpi())
            .blockMessages(GridDhtPartitionsFullMessage.class, getTestIgniteInstanceName(joiningBackupNodeId));

        new Thread(() -> {
            try {
                startGrid(joiningBackupNodeId);
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            success.countDown();
        }).start();

        assertTrue(GridTestUtils.waitForCondition(() -> crd.cluster().nodes().size() == 3, 10_000));

        ArrayList<Integer> keys = new ArrayList<>();

        Affinity<Object> aff = crd.affinity(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100; i++) {
            if (aff.isPrimary(partOwner.localNode(), i)) {
                keys.add(i);
                break;
            }
        }

        ((TestRecordingCommunicationSpi)partOwner.configuration().getCommunicationSpi())
            .blockMessages(GridDhtTxPrepareRequest.class, getTestIgniteInstanceName(joiningBackupNodeId));

        GridNearTxLocal nearTx = ((TransactionProxyImpl)partOwner.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)).tx();

        for (Integer k : keys)
            cache.put(k, k);

        nearTx.commitAsync();

        // Checks that PartitionCountersNeighborcastRequest will be sent after primary node left.
        IgniteTxManager tm = crd.context().cache().context().tm();
        assertTrue(GridTestUtils.waitForCondition(() -> !tm.activeTransactions().isEmpty(), 10_000));
        assertTrue(GridTestUtils.waitForCondition(() -> tm.activeTransactions().iterator().next().state().equals(PREPARED), 10_000));

        // Primary node left.
        partOwner.close();

        // Node with backup fetch lost FullMessage and starts.
        ((TestRecordingCommunicationSpi)crd.configuration().getCommunicationSpi()).stopBlock();

        success.await();

        awaitPartitionMapExchange();

        assertEquals(2, crd.cluster().nodes().size());
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testUpdateCountersGapIsClosed() throws Exception {
        int srvCnt = 3;

        startGridsMultiThreaded(srvCnt);

        client = true;

        IgniteEx ign = startGrid(srvCnt);

        IgniteCache<Object, Object> cache = ign.getOrCreateCache(
            basicCcfg().setBackups(2));

        int vid = 1;

        IgniteEx victim = grid(vid);

        ArrayList<Integer> keys = new ArrayList<>();

        Integer part = null;

        Affinity<Object> aff = ign.affinity(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 2000; i++) {
            int p = aff.partition(i);
            if (aff.isPrimary(victim.localNode(), i)) {
                if (part == null) part = p;
                if (p == part) keys.add(i);
                if (keys.size() == 2) break;
            }
        }

        assert keys.size() == 2;

        Transaction txA = ign.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

        // prevent first transaction prepare on backups
        ((TestRecordingCommunicationSpi)victim.configuration().getCommunicationSpi())
            .blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                final AtomicInteger limiter = new AtomicInteger();

                @Override public boolean apply(ClusterNode node, Message msg) {
                    if (msg instanceof GridDhtTxPrepareRequest)
                        return limiter.getAndIncrement() < 2;

                    return false;
                }
            });

        cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(keys.get(0)));

        txA.commitAsync();

        GridCacheVersion aXidVer = ((TransactionProxyImpl)txA).tx().xidVersion();

        assertConditionEventually(() -> txsOnNode(victim, aXidVer).stream()
            .anyMatch(tx -> tx.state() == PREPARING));

        GridTestUtils.runAsync(() -> {
            try (Transaction txB = ign.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(keys.get(1)));

                txB.commit();
            }
        }).get();

        long victimUpdCntr = updateCounter(victim.cachex(DEFAULT_CACHE_NAME).context(), keys.get(0));

        List<IgniteEx> backupNodes = grids(srvCnt, i -> i != vid);

        List<IgniteInternalTx> backupTxsA = backupNodes.stream()
            .map(node -> txsOnNode(node, aXidVer))
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

        // drop primary
        victim.close();

        assertConditionEventually(() -> backupTxsA.stream().allMatch(tx -> tx.state() == ROLLED_BACK));

        backupNodes.stream()
            .map(node -> node.cache(DEFAULT_CACHE_NAME))
            .forEach(c -> {
                assertEquals(1, c.query(new SqlFieldsQuery("select * from Integer")).getAll().size());
            });

        backupNodes.forEach(node -> {
            for (Integer k : keys)
                assertEquals(victimUpdCntr, updateCounter(node.cachex(DEFAULT_CACHE_NAME).context(), k));
        });
    }

    /** */
    private static CacheConfiguration<Object, Object> basicCcfg() {
        return new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT)
            .setCacheMode(PARTITIONED)
            .setIndexedTypes(Integer.class, Integer.class);
    }

    /** */
    private static List<IgniteInternalTx> txsOnNode(IgniteEx node, GridCacheVersion xidVer) {
        List<IgniteInternalTx> txs = node.context().cache().context().tm().activeTransactions().stream()
            .peek(tx -> assertEquals(xidVer, tx.nearXidVersion()))
            .collect(Collectors.toList());

        assert !txs.isEmpty();

        return txs;
    }

    /** */
    private static void assertConditionEventually(GridAbsPredicate p)
        throws IgniteInterruptedCheckedException {
        if (!GridTestUtils.waitForCondition(p, 5_000))
            fail();
    }

    /** */
    private List<IgniteEx> grids(int cnt, IntPredicate p) {
        return IntStream.range(0, cnt).filter(p).mapToObj(this::grid).collect(Collectors.toList());
    }

    /** */
    private void assertPartitionCountersAreConsistent(Iterable<Integer> keys, Iterable<? extends Ignite> nodes) {
        for (Integer key : keys) {
            long cntr0 = -1;

            for (Ignite n : nodes) {
                IgniteEx node = ((IgniteEx)n);

                if (node.affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(node.localNode(), key)) {
                    long cntr = updateCounter(node.cachex(DEFAULT_CACHE_NAME).context(), key);
//                    System.err.println(node.localNode().consistentId() + " " + key + " -> " + cntr);
                    if (cntr0 == -1)
                        cntr0 = cntr;

                    assertEquals(cntr0, cntr);
                }
            }
        }
    }

    /** */
    private static long updateCounter(GridCacheContext<?, ?> cctx, Object key) {
        return dataStore(cctx, key)
            .map(IgniteCacheOffheapManager.CacheDataStore::updateCounter)
            .get();
    }

    /** */
    private static Optional<IgniteCacheOffheapManager.CacheDataStore> dataStore(
        GridCacheContext<?, ?> cctx, Object key) {
        int p = cctx.affinity().partition(key);
        IgniteCacheOffheapManager offheap = cctx.offheap();
        return StreamSupport.stream(offheap.cacheDataStores().spliterator(), false)
            .filter(ds -> ds.partId() == p)
            .findFirst();
    }
}
