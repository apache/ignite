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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.PREPARED;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;

/** */
// t0d0 near client node
public class CacheMvccTxRecoveryTest extends CacheMvccAbstractTest {
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
    public void testRecoveryCommitNearFailure() throws Exception {
        checkRecoveryNearFailure(true);
    }

    /**
     * @throws Exception if failed.
     */
    public void testRecoveryRollbackNearFailure() throws Exception {
        checkRecoveryNearFailure(false);
    }

    /**
     * @throws Exception if failed.
     */
    public void testRecoveryCommitPrimaryFailure1() throws Exception {
        checkRecoveryPrimaryFailure1(true, false);
    }

    /**
     * @throws Exception if failed.
     */
    public void testRecoveryRollbackPrimaryFailure1() throws Exception {
        checkRecoveryPrimaryFailure1(false, false);
    }

    /**
     * @throws Exception if failed.
     */
    public void testRecoveryCommitPrimaryFailure2() throws Exception {
        checkRecoveryPrimaryFailure1(true, true);
    }

    /**
     * @throws Exception if failed.
     */
    public void testRecoveryRollbackPrimaryFailure2() throws Exception {
        checkRecoveryPrimaryFailure1(false, true);
    }

    /** */
    private void checkRecoveryNearFailure(boolean commit) throws Exception {
        int gridCnt = 4;
        int baseCnt = gridCnt - 1;

        startGridsMultiThreaded(baseCnt);

        // tweak client/server near
        client = false;

        IgniteEx nearNode = startGrid(baseCnt);

        IgniteCache<Object, Object> cache = nearNode.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT)
            .setCacheMode(PARTITIONED)
            .setBackups(1)
            .setIndexedTypes(Integer.class, Integer.class));

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

        TestRecordingCommunicationSpi nearComm = (TestRecordingCommunicationSpi)nearNode.configuration().getCommunicationSpi();

        if (!commit)
            nearComm.blockMessages(GridNearTxPrepareRequest.class, grid(1).name());

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

        for (Integer k : keys) {
            long cntr = -1;

            for (int i = 0; i < baseCnt; i++) {
                IgniteEx g = grid(i);

                if (g.affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(g.localNode(), k)) {
                    long c = updateCounter(g.cachex(DEFAULT_CACHE_NAME).context(), k);

                    if (cntr == -1)
                        cntr = c;

                    assertEquals(cntr, c);
                }
            }
        }
    }

    /** */
    private void checkRecoveryPrimaryFailure1(boolean commit, boolean mvccCrd) throws Exception {
        int gridCnt = 4;
        int baseCnt = gridCnt - 1;

        startGridsMultiThreaded(baseCnt);

        client = true;

        IgniteEx nearNode = startGrid(baseCnt);

        IgniteCache<Object, Object> cache = nearNode.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT)
            .setCacheMode(PARTITIONED)
            .setBackups(1)
            .setIndexedTypes(Integer.class, Integer.class));

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

        for (Integer k : keys) {
            long cntr = -1;

            for (int i = 0; i < baseCnt; i++) {
                if (i == victim)
                    continue;

                IgniteEx g = grid(i);

                if (g.affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(g.localNode(), k)) {
                    long c = updateCounter(g.cachex(DEFAULT_CACHE_NAME).context(), k);

                    if (cntr == -1)
                        cntr = c;

                    assertEquals(cntr, c);
                }
            }
        }
    }

//    /**
//     * @throws Exception if failed.
//     */
//    public void testRecoveryCommit() throws Exception {
//        fail();
//        // tx is commited
//        // mvcc tracker is closed
//        // partition counters are consistent
//        startGridsMultiThreaded(2);
//
//        client = true;
//
//        IgniteEx ign = startGrid(2);
//
//        IgniteCache<Object, Object> cache = ign.getOrCreateCache(new CacheConfiguration<>("test")
//            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT)
//            .setCacheMode(PARTITIONED)
//            .setIndexedTypes(Integer.class, Integer.class));
//
//        AtomicInteger keyCntr = new AtomicInteger();
//
//        ArrayList<Integer> keys = new ArrayList<>();
//
//        ign.cluster().forServers().nodes()
//            .forEach(node -> keys.add(keyForNode(ign.affinity("test"), keyCntr, node)));
//
//        Transaction tx = ign.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);
//
//        for (Integer k : keys)
//            cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(k));
//
//        ((TransactionProxyImpl)tx).tx().prepareNearTxLocal();
//
//        // drop near
//        stopGrid(2, true);
//
//        // t0d0 fail if condition is still unmet
//        IgniteEx srvNode = grid(0);
//
//        // t0d0 check "select count(1)" consistency
//        System.err.println(srvNode.cache("test").query(new SqlFieldsQuery("select * from Integer")).getAll().size());
//
//        assertConditionEventually(
//            () -> srvNode.cache("test").query(new SqlFieldsQuery("select * from Integer")).getAll().size() == 2
//        );
//
//        for (int i = 0; i < 2; i++) {
//            for (Integer k : keys) {
//                dataStore(grid(i).cachex("test").context(), k)
//                    .ifPresent(ds -> System.err.println(k + " -> " + ds.updateCounter()));
//            }
//        }
//    }
//
//    /**
//     * @throws Exception if failed.
//     */
//    public void testRecoveryRollbackInconsistentBackups() throws Exception {
//        fail();
//        // tx is rolled back
//        // mvcc tracker is closed
//        // partition counters are consistent
//        int srvCnt = 3;
//
//        startGridsMultiThreaded(srvCnt);
//
//        client = true;
//
//        IgniteEx ign = startGrid(srvCnt);
//
//        IgniteCache<Object, Object> cache = ign.getOrCreateCache(new CacheConfiguration<>("test")
//            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT)
//            .setCacheMode(PARTITIONED)
//            .setBackups(2)
//            .setIndexedTypes(Integer.class, Integer.class));
//
//        ArrayList<Integer> keys = new ArrayList<>();
//
//        Affinity<Object> aff = ign.affinity("test");
//
//        int victim = 2;
//        int missedPrepare = 1;
//
//        for (int i = 0; i < 100; i++) {
//            if (aff.isPrimary(grid(victim).localNode(), i) && aff.isBackup(grid(missedPrepare).localNode(), i)) {
//                keys.add(i);
//                break;
//            }
//        }
//
//        // t0d0 choose node visely and messages to block
//        // t0d0 check problem with visibility inconsistense on different nodes in various cases
//        ((TestCommunicationSpi)grid(victim).configuration().getCommunicationSpi())
//            .blockDhtPrepare(grid(missedPrepare).localNode().id());
//
//        Transaction nearTx = ign.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);
//
//        for (Integer k : keys)
//            cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(k));
//
//        List<IgniteInternalTx> txs = IntStream.range(0, srvCnt)
//            .filter(i -> i != victim)
//            .mapToObj(i -> grid(i).context().cache().context().tm().activeTransactions().iterator().next())
//            .collect(Collectors.toList());
//
//        ((TransactionProxyImpl)nearTx).tx().prepareNearTxLocal();
//
//        // drop near
//        stopGrid(srvCnt, true);
//        // drop primary
//        // t0d0 update started voting on mvcc coordinator
//        stopGrid(victim, true);
//
//        for (int i = 0; i < srvCnt; i++) {
//            if (i == victim) continue;
//
//            for (Integer k : keys) {
//                dataStore(grid(i).cachex("test").context(), k)
//                    .ifPresent(ds -> System.err.println(k + " -> " + ds.updateCounter()));
//            }
//        }
//
//        assertConditionEventually(() -> txs.stream().allMatch(tx -> tx.state() == ROLLED_BACK));
//
//        TimeUnit.SECONDS.sleep(5);
//
//        for (int i = 0; i < srvCnt; i++) {
//            if (i == victim) continue;
//
//            for (Integer k : keys) {
//                dataStore(grid(i).cachex("test").context(), k)
//                    .ifPresent(ds -> System.err.println(k + " -> " + ds.updateCounter()));
//            }
//
//            System.err.println(grid(i).cache("test").query(new SqlFieldsQuery("select * from Integer").setLocal(true)).getAll());
//        }
//    }
//
//    /**
//     * @throws Exception if failed.
//     */
//    public void testRecoveryOrphanedRemoteTx() throws Exception {
//        fail();
//        // tx is rolled back
//        // mvcc tracker is closed
//        // partition counters are consistent
//        int srvCnt = 2;
//
//        startGridsMultiThreaded(srvCnt);
//
//        client = true;
//
//        IgniteEx ign = startGrid(srvCnt);
//
//        IgniteCache<Object, Object> cache = ign.getOrCreateCache(new CacheConfiguration<>("test")
//            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT)
//            .setCacheMode(PARTITIONED)
//            .setBackups(1)
//            .setIndexedTypes(Integer.class, Integer.class));
//
//        ArrayList<Integer> keys = new ArrayList<>();
//
//        for (int i = 0; i < 1000; i++) {
//            Affinity<Object> aff = ign.affinity("test");
//            List<ClusterNode> nodes = new ArrayList<>(aff.mapKeyToPrimaryAndBackups(i));
//            ClusterNode primary = nodes.get(0);
//            ClusterNode backup = nodes.get(1);
//            if (grid(0).localNode().equals(primary) && grid(1).localNode().equals(backup))
//                keys.add(i);
//            if (grid(1).localNode().equals(primary) && grid(0).localNode().equals(backup))
//                keys.add(i);
//            if (keys.size() == 2)
//                break;
//        }
//
//        assert keys.size() == 2;
//
//        TestCommunicationSpi comm = (TestCommunicationSpi)ign.configuration().getCommunicationSpi();
//
//        // t0d0 choose node visely and messages to block
//        comm.blockNearPrepare(grid(1).localNode().id());
//
//        Transaction nearTx = ign.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);
//
//        for (Integer k : keys)
//            cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(k));
//
//        List<IgniteInternalTx> txs = IntStream.range(0, srvCnt)
//            .mapToObj(i -> grid(i).context().cache().context().tm().activeTransactions().iterator().next())
//            .collect(Collectors.toList());
//
//        ((TransactionProxyImpl)nearTx).tx().prepareNearTxLocal();
//
//        // drop near
//        stopGrid(srvCnt, true);
//
//        assertConditionEventually(() -> txs.stream().allMatch(tx -> tx.state() == ROLLED_BACK));
//    }

    /** */
    private static CacheConfiguration<Object, Object> basicCcfg() {
        return new CacheConfiguration<>("test")
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT)
            .setCacheMode(PARTITIONED)
            .setIndexedTypes(Integer.class, Integer.class);
    }

    /** */
    private static List<IgniteInternalTx> txsOnNode(IgniteEx node, GridCacheVersion xidVer) {
        List<IgniteInternalTx> txs = node.context().cache().context().tm().activeTransactions().stream()
            .peek(tx -> assertEquals(xidVer, tx.nearXidVersion()))
            .collect(Collectors.toList());

        assertFalse(txs.isEmpty());

        return txs;
    }

    /** */
    private static void assertConditionEventually(GridAbsPredicate p)
        throws IgniteInterruptedCheckedException {
        if (!GridTestUtils.waitForCondition(p, 5_000))
            fail();
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
