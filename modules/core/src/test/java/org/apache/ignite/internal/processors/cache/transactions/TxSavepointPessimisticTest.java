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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.GridNearUnlockRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtUnlockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests savepoint support for pessimistic transactions.
 */
public class TxSavepointPessimisticTest extends GridCommonAbstractTest {
    /** Enables {@link TestRecordingCommunicationSpi} for selected tests only. */
    private volatile boolean useRecordingCommunicationSpi;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (useRecordingCommunicationSpi)
            cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRollbackToSavepoint() throws Exception {
        Ignite ignite = startGrid(0);
        IgniteCache<Integer, Integer> cache = transactionalCache(ignite, 1);

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.put(1, 1);
            tx.savepoint("sp");
            cache.put(2, 2);
            tx.rollbackToSavepoint("sp");
            cache.put(3, 3);
            tx.commit();
        }

        assertEquals(Integer.valueOf(1), cache.get(1));
        assertNull(cache.get(2));
        assertEquals(Integer.valueOf(3), cache.get(3));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRollbackToSavepointReleasesLockForPutRemoveEntry() throws Exception {
        Ignite ignite = startGrid(0);
        IgniteCache<Integer, Integer> cache = transactionalCache(ignite, 1);

        int key = 42;

        CountDownLatch savepointRolledBackLatch = new CountDownLatch(1);
        CountDownLatch finishFirstTxLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 30_000, 1)) {
                tx.savepoint("sp");

                // Real client flow: key is created and then removed inside the same tx segment.
                cache.put(key, 1);
                cache.remove(key);

                tx.rollbackToSavepoint("sp");

                savepointRolledBackLatch.countDown();

                assertTrue(finishFirstTxLatch.await(10, TimeUnit.SECONDS));

                tx.commit();
            }
        });

        assertTrue(savepointRolledBackLatch.await(10, TimeUnit.SECONDS));

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 3_000, 0)) {
            cache.put(key, 22);
            tx.commit();
        }

        finishFirstTxLatch.countDown();

        fut.get(10_000);

        assertEquals(Integer.valueOf(22), cache.get(key));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOverwriteAndReleaseSavepoint() throws Exception {
        Ignite ignite = startGrid(0);
        IgniteCache<Integer, Integer> cache = transactionalCache(ignite, 1);

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.put(1, 1);
            tx.savepoint("sp");
            cache.put(1, 2);
            tx.savepoint("sp", true);
            cache.put(1, 3);
            tx.rollbackToSavepoint("sp");
            tx.commit();
        }

        assertEquals(Integer.valueOf(2), cache.get(1));

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.savepoint("sp");
            tx.releaseSavepoint("sp");

            GridTestUtils.assertThrowsAnyCause(log,
                () -> {
                    tx.rollbackToSavepoint("sp");

                    return null;
                },
                IllegalArgumentException.class,
                "No such savepoint");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSavepointRejectedForOptimisticTx() throws Exception {
        Ignite ignite = startGrid(0);

        try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
            GridTestUtils.assertThrowsAnyCause(log,
                () -> {
                    tx.savepoint("sp");

                    return null;
                },
                IgniteCheckedException.class,
                "Savepoints are supported only for PESSIMISTIC transactions.");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRollbackToSavepointReleasesLockForNewEntry() throws Exception {
        Ignite ignite = startGrid(0);
        IgniteCache<Integer, Integer> cache = transactionalCache(ignite, 1);

        CountDownLatch savepointRolledBackLatch = new CountDownLatch(1);
        CountDownLatch finishFirstTxLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(1, 1);
                tx.savepoint("sp");
                cache.put(2, 2);
                tx.rollbackToSavepoint("sp");

                savepointRolledBackLatch.countDown();

                assertTrue(finishFirstTxLatch.await(10, TimeUnit.SECONDS));

                tx.commit();
            }
        });

        assertTrue(savepointRolledBackLatch.await(10, TimeUnit.SECONDS));

        try (Transaction tx = ignite.transactions().txStart(
            PESSIMISTIC,
            TransactionIsolation.REPEATABLE_READ,
            3_000,
            0
        )) {
            cache.put(2, 22);
            tx.commit();
        }

        finishFirstTxLatch.countDown();

        fut.get();

        assertEquals(Integer.valueOf(22), cache.get(2));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRollbackToSavepointReleasesRemoteDhtLock() throws Exception {
        Ignite ignite0 = startGrid(0);
        Ignite ignite1 = startGrid(1);

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache0 = transactionalCache(ignite0, 0);
        IgniteCache<Integer, Integer> cache1 = ignite1.cache(DEFAULT_CACHE_NAME);

        int node0Key = primaryKey(cache0);
        int node1Key = primaryKey(cache1);

        CountDownLatch savepointRolledBackLatch = new CountDownLatch(1);
        CountDownLatch finishFirstTxLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache0.put(node0Key, 1);

                tx.savepoint("sp");

                cache0.put(node1Key, 1);

                tx.rollbackToSavepoint("sp");

                savepointRolledBackLatch.countDown();

                assertTrue(finishFirstTxLatch.await(10, TimeUnit.SECONDS));

                tx.commit();
            }
        });

        assertTrue(savepointRolledBackLatch.await(10, TimeUnit.SECONDS));

        try (Transaction tx = ignite1.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 10_000, 1)) {
            cache1.put(node1Key, 22);
            tx.commit();
        }

        finishFirstTxLatch.countDown();

        fut.get();

        assertEquals(Integer.valueOf(22), cache0.get(node1Key));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRollbackToSavepointReleasesRemoteDhtLockAcquireAgain() throws Exception {
        Ignite ignite0 = startGrid(0);
        Ignite ignite1 = startGrid(1);
        Ignite ignite2 = startGrid(2);

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache0 = transactionalCache(ignite0, 1);
        IgniteCache<Integer, Integer> cache1 = ignite1.cache(DEFAULT_CACHE_NAME);

        int node0Key = primaryKey(cache0);
        int node1Key = keyForPrimaryAndBackup(ignite0, ignite1, ignite2);

        CountDownLatch savepointRolledBackLatch = new CountDownLatch(1);
        CountDownLatch finishFirstTxLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 30_000, 2)) {
                cache0.put(node0Key, 1);

                tx.savepoint("sp");

                cache0.put(node1Key, 1);

                tx.rollbackToSavepoint("sp");

                savepointRolledBackLatch.countDown();

                assertTrue(finishFirstTxLatch.await(10, TimeUnit.SECONDS));

                cache0.put(node1Key, 2);

                tx.commit();
            }
        });

        try (Transaction tx = ignite1.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 10_000, 1)) {
            cache1.put(node1Key, 22);
            tx.commit();
        }

        assertFalse(fut.isDone());
        assertEquals(Integer.valueOf(22), cache0.get(node1Key));

        finishFirstTxLatch.countDown();

        fut.get(10_000);
        assertEquals(Integer.valueOf(2), cache0.get(node1Key));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDhtEntriesAfterRollbackToSavepoint() throws Exception {
        Ignite ignite0 = startGrid(0);
        Ignite ignite1 = startGrid(1);
        Ignite ignite2 = startGrid(2);

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache0 = transactionalCache(ignite0, 1);

        int keepTxAliveKey = primaryKey(cache0);
        int dhtKey = keyForPrimaryAndBackup(ignite0, ignite1, ignite2);

        // Materialize DHT entries on primary/backup before tx starts.
        cache0.put(dhtKey, -1);

        CountDownLatch dhtKeyWrittenLatch = new CountDownLatch(1);
        CountDownLatch proceedRollbackLatch = new CountDownLatch(1);
        CountDownLatch rollbackDoneLatch = new CountDownLatch(1);
        CountDownLatch finishTxLatch = new CountDownLatch(1);
        GridCacheVersion[] nearVerRef = new GridCacheVersion[1];

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 30_000, 2)) {
                nearVerRef[0] = ((TransactionProxyImpl<?, ?>)tx).tx().nearXidVersion();

                cache0.put(keepTxAliveKey, 1);

                tx.savepoint("sp");

                cache0.put(dhtKey, 1);

                dhtKeyWrittenLatch.countDown();

                assertTrue(proceedRollbackLatch.await(10, TimeUnit.SECONDS));

                tx.rollbackToSavepoint("sp");

                rollbackDoneLatch.countDown();

                assertTrue(finishTxLatch.await(10, TimeUnit.SECONDS));

                tx.commit();
            }
        });

        assertTrue(dhtKeyWrittenLatch.await(10, TimeUnit.SECONDS));
        assertNotNull(nearVerRef[0]);

        proceedRollbackLatch.countDown();

        assertTrue(rollbackDoneLatch.await(10, TimeUnit.SECONDS));

        assertNoTxStateKeyOnNode(ignite1, nearVerRef[0], dhtKey);
        assertNoTxStateKeyOnNode(ignite2, nearVerRef[0], dhtKey);

        finishTxLatch.countDown();

        fut.get(10_000);

        assertNoActiveTx(ignite1, nearVerRef[0]);
        assertNoActiveTx(ignite2, nearVerRef[0]);
    }

    /**
     * TODO: PVD: Reve this test after debug.
     * Same scenario as {@link #testDhtEntriesAfterRollbackToSavepoint()} but with detailed
     * transaction communication logging captured via {@link TestRecordingCommunicationSpi}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDhtEntriesAfterRollbackToSavepointCommunicationLog() throws Exception {
        useRecordingCommunicationSpi = true;

        Ignite ignite0;
        Ignite ignite1;
        Ignite ignite2;

        try {
            ignite0 = startGrid(0);
            ignite1 = startGrid(1);
            ignite2 = startGrid(2);
        }
        finally {
            useRecordingCommunicationSpi = false;
        }

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache0 = transactionalCache(ignite0, 1);

        int keepTxAliveKey = primaryKey(cache0);
        int dhtKey = keyForPrimaryAndBackup(ignite0, ignite1, ignite2);

        List<String> txCommLog = new CopyOnWriteArrayList<>();

        // Materialize DHT entries on primary/backup before tx starts.
        cache0.put(dhtKey, -1);

        CountDownLatch dhtKeyWrittenLatch = new CountDownLatch(1);
        CountDownLatch proceedRollbackLatch = new CountDownLatch(1);
        CountDownLatch rollbackDoneLatch = new CountDownLatch(1);
        CountDownLatch finishTxLatch = new CountDownLatch(1);
        GridCacheVersion[] nearVerRef = new GridCacheVersion[1];

        installTxCommunicationLogger(ignite0, txCommLog);
        installTxCommunicationLogger(ignite1, txCommLog);
        installTxCommunicationLogger(ignite2, txCommLog);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 30_000, 2)) {
                nearVerRef[0] = ((TransactionProxyImpl<?, ?>)tx).tx().nearXidVersion();

                cache0.put(keepTxAliveKey, 1);
//                cache0.put(dhtKey, 2);

                tx.savepoint("sp");

                cache0.put(dhtKey, 1);

                dhtKeyWrittenLatch.countDown();

                assertTrue(proceedRollbackLatch.await(10, TimeUnit.SECONDS));

                tx.rollbackToSavepoint("sp");

                rollbackDoneLatch.countDown();

                assertTrue(finishTxLatch.await(10, TimeUnit.SECONDS));

                tx.commit();
            }
        });

        assertTrue(dhtKeyWrittenLatch.await(10, TimeUnit.SECONDS));
        assertNotNull(nearVerRef[0]);

        proceedRollbackLatch.countDown();

        assertTrue(rollbackDoneLatch.await(10, TimeUnit.SECONDS));

//        assertNoTxStateKeyOnNode(ignite1, nearVerRef[0], dhtKey);
//        assertNoTxStateKeyOnNode(ignite2, nearVerRef[0], dhtKey);

        finishTxLatch.countDown();

        fut.get(10_000);

        txCommLog.forEach(line -> info("TX-COMM " + line));

        assertFalse("Expected transaction communication to be captured", txCommLog.isEmpty());
        assertTrue("Expected savepoint unlock communication in log: " + txCommLog,
            txCommLog.stream().anyMatch(line ->
                line.contains(GridNearUnlockRequest.class.getSimpleName()) ||
                    line.contains(GridDhtUnlockRequest.class.getSimpleName())));
    }

    /**
     * @param nearNode Node where near tx is started.
     * @param primaryNode Node that should be primary.
     * @param backupNode Node that should be backup.
     * @return Key mapped to given primary and backup.
     */
    private int keyForPrimaryAndBackup(Ignite nearNode, Ignite primaryNode, Ignite backupNode) {
        for (int key = 0; key < 50_000; key++) {
            Collection<ClusterNode> mapping = nearNode.affinity(DEFAULT_CACHE_NAME).mapKeyToPrimaryAndBackups(key);

            if (mapping.size() < 2)
                continue;

            ClusterNode primary = mapping.iterator().next();
            ClusterNode backup = mapping.stream().skip(1).findFirst().orElse(null);

            if (primary.id().equals(primaryNode.cluster().localNode().id()) &&
                backup.id().equals(backupNode.cluster().localNode().id()))
                return key;
        }

        throw new AssertionError("Failed to find key for requested primary/backup mapping.");
    }

    /**
     * @param ignite Node.
     * @param backups Backups count.
     * @return Transactional cache.
     */
    private IgniteCache<Integer, Integer> transactionalCache(Ignite ignite, int backups) {
        CacheConfiguration<?, ?> ccfg =
            new CacheConfiguration<>(DEFAULT_CACHE_NAME).setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
//            defaultCacheConfiguration()
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(CacheMode.PARTITIONED)
//                .setCacheMode(CacheMode.REPLICATED)
            .setBackups(backups);

        return (IgniteCache<Integer, Integer>)ignite.getOrCreateCache(ccfg);
    }

    /**
     * Enables transaction message logging for a node via {@link TestRecordingCommunicationSpi}.
     *
     * @param ignite Node.
     * @param txCommLog Shared communication log.
     */
    private void installTxCommunicationLogger(Ignite ignite, List<String> txCommLog) {
        String srcNode = ignite.name();
        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(ignite);

        spi.closure((dstNode, msg) -> {
            if (isTxCommunicationMessage(msg))
                txCommLog.add("PVD::" + srcNode + " -> " + dstNode.consistentId() + " : " + txMessageDetails(msg));
        });

        spi.record((node, msg) -> isTxCommunicationMessage(msg));
    }

    /**
     * @param msg Message.
     * @return {@code True} if message is transaction-related for this scenario.
     */
    private boolean isTxCommunicationMessage(Message msg) {
        return msg instanceof GridNearLockRequest ||
            msg instanceof GridNearLockResponse ||
            msg instanceof GridDhtLockRequest ||
            msg instanceof GridDhtLockResponse ||
            msg instanceof GridNearUnlockRequest ||
            msg instanceof GridDhtUnlockRequest ||
            msg instanceof GridNearTxPrepareRequest ||
            msg instanceof GridNearTxPrepareResponse ||
            msg instanceof GridNearTxFinishRequest ||
            msg instanceof GridDhtTxFinishRequest;
    }

    /**
     * @param msg Message.
     * @return Human-readable details for tx communication logs.
     */
    private String txMessageDetails(Message msg) {
        if (msg instanceof GridDhtUnlockRequest) {
            GridDhtUnlockRequest req = (GridDhtUnlockRequest)msg;

            return msg.getClass().getSimpleName() + "[keys=" + (req.keys() == null ? 0 : req.keys().size()) +
                ", forSavepoint=" + req.forSavepoint() + ']';
        }

        if (msg instanceof GridNearUnlockRequest) {
            GridNearUnlockRequest req = (GridNearUnlockRequest)msg;

            return msg.getClass().getSimpleName() + "[keys=" + (req.keys() == null ? 0 : req.keys().size()) +
                ", forSavepoint=" + req.forSavepoint() + ']';
        }

        return msg.getClass().getSimpleName();
    }

    /**
     * Asserts that no active transaction with the given near version contains the specified key
     * in its write-set or entry map on the provided node.
     *
     * @param ignite Node to inspect.
     * @param nearVer Near transaction version.
     * @param key Key that must be absent from tx state.
     * @throws Exception If waiting fails.
     */
    private void assertNoTxStateKeyOnNode(Ignite ignite, GridCacheVersion nearVer, int key) throws Exception {
        assertTrue(GridTestUtils.waitForCondition(() -> {
            GridCacheContext<?, ?> cctx = ((IgniteKernal)ignite).internalCache(DEFAULT_CACHE_NAME).context();
            IgniteTxKey txKey = cctx.txKey(cctx.toCacheKeyObject(key));

            for (IgniteInternalTx tx : cctx.tm().activeTransactions()) {
                if (!nearVer.equals(tx.nearXidVersion()))
                    continue;

                if (tx.hasWriteKey(txKey) || tx.entry(txKey) != null)
                    return false;
            }

            return true;
        }, 10_000));
    }

    /**
     * Asserts that no active transaction remains with the near version on the node.
     *
     * @param ignite Node to inspect.
     * @param nearVer Near transaction version.
     * @throws Exception If waiting fails.
     */
    private void assertNoActiveTx(Ignite ignite, GridCacheVersion nearVer) throws Exception {
        assertTrue(GridTestUtils.waitForCondition(() -> {
            GridCacheContext<?, ?> cctx = ((IgniteKernal)ignite).internalCache(DEFAULT_CACHE_NAME).context();
            for (IgniteInternalTx tx : cctx.tm().activeTransactions()) {
                if (nearVer.equals(tx.nearXidVersion()))
                    return false;
            }

            return true;
        }, 10_000));
    }
}
