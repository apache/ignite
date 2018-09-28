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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/** */
// t0d0 SFU mappings
// t0d0 run all tests in this class
// t0d0 include not all nodes into expected result
// t0d0 cover local backups in tests
public class CacheMvccTxRecoveryTest extends CacheMvccAbstractTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        throw new RuntimeException("Is not supposed to be used");
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCommunicationSpi(new TestCommunicationSpi());

        return cfg;
    }

    /**
     * @throws Exception if failed.
     */
    public void testAllTxNodesAreTrackedFastUpdate() throws Exception {
        startGridsMultiThreaded(2);

        client = true;

        IgniteEx ign = startGrid(2);

        IgniteCache<Object, Object> cache = ign.createCache(basicCcfg().setBackups(1));

        AtomicInteger keyCntr = new AtomicInteger();

        Integer key1 = keyForNode(ign.affinity(cache.getName()), keyCntr, grid(0).localNode());
        Integer key2 = keyForNode(ign.affinity(cache.getName()), keyCntr, grid(1).localNode());

        try (Transaction userTx = ign.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(key1));
            cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(key2));

            GridNearTxLocal nearTx = ((TransactionProxyImpl)userTx).tx();

            nearTx.prepareNearTxLocal().get();

            ImmutableMap<UUID, Set<UUID>> txNodes = ImmutableMap.of(
                grid(0).localNode().id(), Collections.singleton(grid(1).localNode().id()),
                grid(1).localNode().id(), Collections.singleton(grid(0).localNode().id())
            );

            Iterables.concat(txsOnNode(grid(0), nearTx.nearXidVersion()), txsOnNode(grid(1), nearTx.nearXidVersion()))
                .forEach(tx -> assertEquals(txNodes, repack(tx.transactionNodes())));
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testAllTxNodesAreTrackedCachePut() throws Exception {
        startGridsMultiThreaded(2);

        client = true;

        IgniteEx ign = startGrid(2);

        IgniteCache<Object, Object> cache = ign.createCache(basicCcfg().setBackups(1));

        AtomicInteger keyCntr = new AtomicInteger();

        Integer key1 = keyForNode(ign.affinity(cache.getName()), keyCntr, grid(0).localNode());
        Integer key2 = keyForNode(ign.affinity(cache.getName()), keyCntr, grid(1).localNode());

        try (Transaction userTx = ign.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.put(key1, 42);
            cache.put(key2, 42);

            GridNearTxLocal nearTx = ((TransactionProxyImpl)userTx).tx();

            nearTx.prepareNearTxLocal().get();

            ImmutableMap<UUID, Set<UUID>> txNodes = ImmutableMap.of(
                grid(0).localNode().id(), Collections.singleton(grid(1).localNode().id()),
                grid(1).localNode().id(), Collections.singleton(grid(0).localNode().id())
            );

            Iterables.concat(txsOnNode(grid(0), nearTx.nearXidVersion()), txsOnNode(grid(1), nearTx.nearXidVersion()))
                .forEach(tx -> assertEquals(txNodes, repack(tx.transactionNodes())));
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testAllTxNodesAreTrackedCursorUpdate() throws Exception {
        startGridsMultiThreaded(2);

        client = true;

        IgniteEx ign = startGrid(2);

        IgniteCache<Object, Object> cache = ign.createCache(basicCcfg().setBackups(1));

        AtomicInteger keyCntr = new AtomicInteger();

        Integer key1 = keyForNode(ign.affinity(cache.getName()), keyCntr, grid(0).localNode());
        Integer key2 = keyForNode(ign.affinity(cache.getName()), keyCntr, grid(1).localNode());

        // data initialization
        cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(key1));
        cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(key2));

        try (Transaction userTx = ign.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            // t0d0 why broadcast?
            cache.query(new SqlFieldsQuery("update Integer set _val = _val + 1 where _key = ?").setArgs(key1));
            cache.query(new SqlFieldsQuery("update Integer set _val = _val + 1 where _key = ?").setArgs(key2));

            GridNearTxLocal nearTx = ((TransactionProxyImpl)userTx).tx();

            nearTx.prepareNearTxLocal().get();

            ImmutableMap<UUID, Set<UUID>> txNodes = ImmutableMap.of(
                grid(0).localNode().id(), Collections.singleton(grid(1).localNode().id()),
                grid(1).localNode().id(), Collections.singleton(grid(0).localNode().id())
            );

            Iterables.concat(txsOnNode(grid(0), nearTx.nearXidVersion()), txsOnNode(grid(1), nearTx.nearXidVersion()))
                .forEach(tx -> assertEquals(txNodes, repack(tx.transactionNodes())));
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testAllTxNodesAreTrackedBroadcast() throws Exception {
        startGridsMultiThreaded(2);

        client = true;

        IgniteEx ign = startGrid(2);

        IgniteCache<Object, Object> cache = ign.createCache(basicCcfg().setBackups(1));

        AtomicInteger keyCntr = new AtomicInteger();

        Integer key1 = keyForNode(ign.affinity(cache.getName()), keyCntr, grid(0).localNode());
        Integer key2 = keyForNode(ign.affinity(cache.getName()), keyCntr, grid(1).localNode());

        // data initialization
        cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(key1));
        cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(key2));

        try (Transaction userTx = ign.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.query(new SqlFieldsQuery("update Integer set _val = _val + 1").setArgs(key1));

            GridNearTxLocal nearTx = ((TransactionProxyImpl)userTx).tx();

            nearTx.prepareNearTxLocal().get();

            ImmutableMap<UUID, Set<UUID>> txNodes = ImmutableMap.of(
                grid(0).localNode().id(), Collections.singleton(grid(1).localNode().id()),
                grid(1).localNode().id(), Collections.singleton(grid(0).localNode().id())
            );

            Iterables.concat(txsOnNode(grid(0), nearTx.nearXidVersion()), txsOnNode(grid(1), nearTx.nearXidVersion()))
                .forEach(tx -> assertEquals(txNodes, repack(tx.transactionNodes())));
        }
    }
//
//    /**
//     * @throws Exception if failed.
//     */
//    public void testRecoveryRollback() throws Exception {
//        // tx is rolled back
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
//        TestCommunicationSpi comm = (TestCommunicationSpi)ign.configuration().getCommunicationSpi();
//
//        comm.blockNearPrepare(grid(1).localNode().id());
//
//        Transaction nearTx = ign.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);
//
//        for (Integer k : keys)
//            cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(k));
//
//        IgniteInternalTx tx0 = grid(0).context().cache().context().tm().activeTransactions().iterator().next();
//        IgniteInternalTx tx1 = grid(1).context().cache().context().tm().activeTransactions().iterator().next();
//
//        ((TransactionProxyImpl)nearTx).tx().prepareNearTxLocal();
//
//        // drop near
//        stopGrid(2, true);
//
//        assertConditionEventually(
//            () -> tx0.state() == ROLLED_BACK && tx1.state() == ROLLED_BACK,
//            "Transaction rollback is expected");
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
//    public void testRecoveryCommit() throws Exception {
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
//            () -> srvNode.cache("test").query(new SqlFieldsQuery("select * from Integer")).getAll().size() == 2,
//            "Transaction commit is expected");
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
//        AtomicInteger keyCntr = new AtomicInteger();
//
//        ArrayList<Integer> keys = new ArrayList<>();
//
////        ign.cluster().forServers().nodes()
////            .forEach(node -> keys.add(keyForNode(ign.affinity("test"), keyCntr, node)));
//
////        keys.add(primaryKey(grid(0).cache("test")));
//        keys.add(1);
//
//        TestCommunicationSpi comm = (TestCommunicationSpi)grid(0).configuration().getCommunicationSpi();
//
//        // t0d0 choose node visely and messages to block
//        comm.blockNearPrepare(grid(1).localNode().id());
//
//        Transaction nearTx = ign.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);
//
//        for (Integer k : keys)
//            cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(k));
//
////        List<IgniteInternalTx> txs = IntStream.range(0, srvCnt)
////            .mapToObj(i -> grid(i).context().cache().context().tm().activeTransactions().iterator().next())
////            .collect(Collectors.toList());
//
//        ((TransactionProxyImpl)nearTx).tx().prepareNearTxLocal();
//
//        // drop near
//        stopGrid(srvCnt, true);
//        // drop primary
//        stopGrid(0, true);
//
////        assertConditionEventually(
////            () -> txs.stream().allMatch(tx -> tx.state() == ROLLED_BACK),
////            "Transaction rollback is expected");
//
//        TimeUnit.SECONDS.sleep(2);
//
//        for (int i = 0; i < srvCnt; i++) {
//            if (i == 0) continue;
//
//            for (Integer k : keys) {
//                dataStore(grid(i).cachex("test").context(), k)
//                    .ifPresent(ds -> System.err.println(k + " -> " + ds.updateCounter()));
//            }
//        }
//        System.err.println(grid(1).cache("test").query(new SqlFieldsQuery("select * from Integer").setLocal(true)).getAll());
//        System.err.println(grid(2).cache("test").query(new SqlFieldsQuery("select * from Integer").setLocal(true)).getAll());
//    }
//
    /** */
    private static CacheConfiguration<Object, Object> basicCcfg() {
        return new CacheConfiguration<>("test")
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT)
            .setCacheMode(PARTITIONED)
            .setIndexedTypes(Integer.class, Integer.class);
    }

    /** */
    private static Map<UUID, Set<UUID>> repack(Map<UUID, Collection<UUID>> orig) {
        ImmutableMap.Builder<UUID, Set<UUID>> builder = ImmutableMap.builder();

        orig.forEach((primary, backups) -> {
            builder.put(primary, new HashSet<>(backups));
        });

        return builder.build();
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
    private static void assertConditionEventually(GridAbsPredicate p, String errMsg)
        throws IgniteInterruptedCheckedException {
        if (!GridTestUtils.waitForCondition(p, 5_000))
            fail(errMsg);
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

    /** */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** */
        @LoggerResource
        private IgniteLogger log;
        /** */
        private UUID blockedNodeId;
        /** */
        private List<T2<ClusterNode, GridIoMessage>> blockedMsgs = new ArrayList<>();

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClo)
            throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                Object msg0 = ((GridIoMessage)msg).message();

                if (msg0 instanceof GridNearTxPrepareRequest || msg0 instanceof GridDhtTxPrepareRequest)
                    System.err.println("PREPARE MSG " + msg0.getClass().getSimpleName() + " for " + node.id());

                if (msg0 instanceof GridDhtTxPrepareRequest) {
                    synchronized (this) {
                        if (node.id().equals(blockedNodeId)) {
                            log.info("Block message: " + msg0);

                            blockedMsgs.add(new T2<>(node, (GridIoMessage)msg));

                            return;
                        }
                    }
                }
            }

            super.sendMessage(node, msg, ackClo);
        }

        /** */
        synchronized void blockNearPrepare(UUID nodeId) {
            assert blockedNodeId == null;

            blockedNodeId = nodeId;
        }

        /** */
        synchronized void unblock() {
            blockedNodeId = null;

            for (T2<ClusterNode, GridIoMessage> msg : blockedMsgs) {
                log.info("Send blocked message: " + msg.get2().message());

                sendMessage(msg.get1(), msg.get2());
            }
        }
    }
}
