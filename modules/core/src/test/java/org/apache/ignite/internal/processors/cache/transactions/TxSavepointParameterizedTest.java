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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionException;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests for transaction savepoint API.
 */
@RunWith(Parameterized.class)
public class TxSavepointParameterizedTest extends GridCommonAbstractTest {
    /** */
    private static Ignite ignite0;

    /** */
    private static Ignite ignite1;

    /** */
    private static Ignite ignite2;

    /** */
    private static Ignite ignite3;

    /** */
    private static Ignite client;

    /** */
    @Parameter(0)
    public boolean initKeies;

    /** */
    @Parameter(1)
    public boolean useNearCache;

    /** */
    @Parameter(2)
    public int backups;

    /** */
    @Parameter(3)
    public boolean spKeyOnTxInitiator;

    /** */
    @Parameter(4)
    public boolean replicated;

    /** */
    @Parameter(5)
    public TransactionIsolation transactionIsolation;

    /**
     * Returns data for test.
     * @return Test parameters.
     */
    @Parameters(name = "initKeies={0}, useNearCache={1}, backups={2}, spKeyOnTxInitiator={3}, replicated={4}, txIsolation={5}")
    public static Collection<Object[]> testData() {
        return List.of(new Object[][] {
            // READ_COMMITTED
            // backups = 0
            {true, true, 0, true, false, READ_COMMITTED},
            {true, true, 0, false, false, READ_COMMITTED},
            {true, false, 0, true, false, READ_COMMITTED},
            {true, false, 0, false, false, READ_COMMITTED},
            {false, true, 0, true, false, READ_COMMITTED},
            {false, true, 0, false, false, READ_COMMITTED},
            {false, false, 0, true, false, READ_COMMITTED},
            {false, false, 0, false, false, READ_COMMITTED},

            // backups = 1
            {true, true, 1, true, false, READ_COMMITTED},
            {true, true, 1, false, false, READ_COMMITTED},
            {true, false, 1, true, false, READ_COMMITTED},
            {true, false, 1, false, false, READ_COMMITTED},
            {false, true, 1, true, false, READ_COMMITTED},
            {false, true, 1, false, false, READ_COMMITTED},
            {false, false, 1, true, false, READ_COMMITTED},
            {false, false, 1, false, false, READ_COMMITTED},

            // backups = 2
            {true, true, 2, true, false, READ_COMMITTED},
            {true, true, 2, false, false, READ_COMMITTED},
            {true, false, 2, true, false, READ_COMMITTED},
            {true, false, 2, false, false, READ_COMMITTED},
            {false, true, 2, true, false, READ_COMMITTED},
            {false, true, 2, false, false, READ_COMMITTED},
            {false, false, 2, true, false, READ_COMMITTED},
            {false, false, 2, false, false, READ_COMMITTED},

            // replicated cache.
            {true, true, 0, true, true, READ_COMMITTED},
            {true, false, 0, true, true, READ_COMMITTED},
            {false, true, 0, true, true, READ_COMMITTED},
            {false, false, 0, true, true, READ_COMMITTED},

            // REPEATABLE_READ
            // backups = 0
            {true, true, 0, true, false, REPEATABLE_READ},
            {true, true, 0, false, false, REPEATABLE_READ},
            {true, false, 0, true, false, REPEATABLE_READ},
            {true, false, 0, false, false, REPEATABLE_READ},
            {false, true, 0, true, false, REPEATABLE_READ},
            {false, true, 0, false, false, REPEATABLE_READ},
            {false, false, 0, true, false, REPEATABLE_READ},
            {false, false, 0, false, false, REPEATABLE_READ},

            // backups = 1
            {true, true, 1, true, false, REPEATABLE_READ},
            {true, true, 1, false, false, REPEATABLE_READ},
            {true, false, 1, true, false, REPEATABLE_READ},
            {true, false, 1, false, false, REPEATABLE_READ},
            {false, true, 1, true, false, REPEATABLE_READ},
            {false, true, 1, false, false, REPEATABLE_READ},
            {false, false, 1, true, false, REPEATABLE_READ},
            {false, false, 1, false, false, REPEATABLE_READ},

            // backups = 2
            {true, true, 2, true, false, REPEATABLE_READ},
            {true, true, 2, false, false, REPEATABLE_READ},
            {true, false, 2, true, false, REPEATABLE_READ},
            {true, false, 2, false, false, REPEATABLE_READ},
            {false, true, 2, true, false, REPEATABLE_READ},
            {false, true, 2, false, false, REPEATABLE_READ},
            {false, false, 2, true, false, REPEATABLE_READ},
            {false, false, 2, false, false, REPEATABLE_READ},

            // replicated cache.
            {true, true, 0, true, true, REPEATABLE_READ},
            {true, false, 0, true, true, REPEATABLE_READ},
            {false, true, 0, true, true, REPEATABLE_READ},
            {false, false, 0, true, true, REPEATABLE_READ},
        });
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite0 = startGrid(0);
        ignite1 = startGrid(1);
        ignite2 = startGrid(2);
        ignite3 = startGrid(3);
        client = startClientGrid();

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite0.destroyCache(DEFAULT_CACHE_NAME);

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRollbackToSavepointReleasesRemoteDhtLockAcquireAgain() throws Exception {
        IgniteCache<Integer, Integer> cache0 = transactionalCache(ignite0);

        int node0Key = primaryKey(cache0);
        int node1Key = keyForPrimaryAndBackup(ignite0, ignite1);

        if (initKeies) {
            cache0.put(node0Key, -1);
            cache0.put(node1Key, -1);
        }

        CountDownLatch savepointRolledBackLatch = new CountDownLatch(1);
        CountDownLatch finishFirstTxLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = startTransaction(ignite0, 2)) {
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

        updateKeyFormPrimary(node1Key);

        assertFalse(fut.isDone());

        // TODO: IGNITE-28612 Entry visibility violation in transactional replication cache with one backup and near.
        if (initKeies && useNearCache && backups == 1 && spKeyOnTxInitiator && !replicated) {
            assertTrue(GridTestUtils.waitForCondition(() ->
                Integer.valueOf(42).equals(cache0.get(node1Key)), 10_000));
        }

        assertEquals(Integer.valueOf(42), cache0.get(node1Key));

        finishFirstTxLatch.countDown();

        fut.get(10_000);

        assertEquals(Integer.valueOf(2), cache0.get(node1Key));
    }

    /**
     * Starts a transaction with the given size.
     * @param ignite Transaction initiator.
     * @param txSize Transaction size.
     * @return Transaction.
     */
    private Transaction startTransaction(Ignite ignite, int txSize) {
        return ignite.transactions().txStart(PESSIMISTIC, transactionIsolation, 30_000, txSize);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOverwriteAndReleaseSavepoint() throws Exception {
        IgniteCache<Integer, Integer> cache0 = transactionalCache(ignite0);

        int key = keyForPrimaryAndBackup(ignite0, ignite1);

        if (initKeies) {
            cache0.put(key, -1);
        }

        try (Transaction tx = startTransaction(ignite0, 1)) {
            cache0.put(key, 1);

            tx.savepoint("sp");

            cache0.put(key, 2);

            tx.savepoint("sp", true);

            cache0.put(key, 3);

            tx.rollbackToSavepoint("sp");

            tx.commit();
        }

        assertEquals(Integer.valueOf(2), cache0.get(key));

        try (Transaction tx = startTransaction(ignite0, 1)) {
            tx.savepoint("sp");

            tx.releaseSavepoint("sp");

            GridTestUtils.assertThrowsAnyCause(log,
                () -> {
                    tx.rollbackToSavepoint("sp");

                    return null;
                },
                TransactionException.class,
                "Savepoint does not exist");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRollbackToSavepointReleasesLockForPutRemoveEntry() throws Exception {
        IgniteCache<Integer, Integer> cache0 = transactionalCache(ignite0);

        int key = keyForPrimaryAndBackup(ignite0, ignite1);

        if (initKeies) {
            cache0.put(key, -1);
        }

        CountDownLatch savepointRolledBackLatch = new CountDownLatch(1);
        CountDownLatch finishFirstTxLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = startTransaction(ignite0, 1)) {
                tx.savepoint("sp");

                cache0.put(key, 1);
                cache0.remove(key);

                tx.rollbackToSavepoint("sp");

                savepointRolledBackLatch.countDown();

                assertTrue(finishFirstTxLatch.await(10, TimeUnit.SECONDS));

                tx.commit();
            }
        });

        assertTrue(savepointRolledBackLatch.await(10, TimeUnit.SECONDS));

        updateKeyFormPrimary(key);

        finishFirstTxLatch.countDown();

        fut.get(10_000);

        assertEquals(Integer.valueOf(42), cache0.get(key));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRollbackToSavepoint() {
        IgniteCache<Integer, Integer> cache0 = transactionalCache(ignite0);

        int key1 = keyForPrimaryAndBackup(ignite0, ignite1);
        int key2 = keyForPrimaryAndBackup(ignite0, ignite2);
        int key3 = keyForPrimaryAndBackup(ignite0, ignite3);

        if (initKeies) {
            cache0.put(key1, -1);
            cache0.put(key2, -1);
            cache0.put(key3, -1);
        }

        try (Transaction tx = startTransaction(ignite0, 3)) {
            cache0.put(key1, 1);

            tx.savepoint("sp");

            cache0.put(key2, 2);

            tx.rollbackToSavepoint("sp");

            cache0.put(key3, 3);

            tx.commit();
        }

        assertEquals(Integer.valueOf(1), cache0.get(key1));

        if (initKeies)
            assertEquals(Integer.valueOf(-1), cache0.get(key2));
        else
            assertNull(cache0.get(key2));

        assertEquals(Integer.valueOf(3), cache0.get(key3));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientInitiator() throws Exception {
        // A client node can not be a primary node for any key.
        if (spKeyOnTxInitiator)
            return;

        IgniteCache<Integer, Integer> cache = transactionalCache(client);

        int key1 = keyForPrimaryAndBackup(client, ignite0);
        int key2 = keyForPrimaryAndBackup(client, ignite1);

        if (initKeies) {
            cache.put(key1, -1);
            cache.put(key2, -1);
        }

        GridCacheVersion[] nearVerRef = new GridCacheVersion[1];

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = startTransaction(client, 2)) {
                nearVerRef[0] = ((TransactionProxyImpl<?, ?>)tx).tx().nearXidVersion();

                Integer val1 = cache.get(key1);
                Integer val2 = cache.get(key2);

                if (initKeies) {
                    assertEquals(-1, val1.intValue());
                    assertEquals(-1, val2.intValue());
                }
                else {
                    assertNull(val1);
                    assertNull(val2);
                }

                cache.put(key1, 1);
                cache.put(key2, 1);

                tx.savepoint("sp");

                cache.put(key1, 2);
                cache.put(key2, 2);

                tx.rollbackToSavepoint("sp");

                val1 = cache.get(key1);
                val2 = cache.get(key2);

                assertEquals(1, val1.intValue());
                assertEquals(1, val2.intValue());

                tx.commit();
            }
        });

        fut.get(10_000);

        Integer val1 = cache.get(key1);
        Integer val2 = cache.get(key2);

        assertEquals(1, val1.intValue());
        assertEquals(1, val2.intValue());

        assertNoActiveTx(nearVerRef[0]);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRollbackAllKey() throws Exception {
        IgniteCache<Integer, Integer> cache0 = transactionalCache(ignite0);

        int keyOnInitiator = primaryKey(cache0);
        int keyOnOtherNode = keyForPrimaryAndBackup(ignite0, ignite1);

        if (initKeies) {
            cache0.put(keyOnInitiator, -1);
            cache0.put(keyOnOtherNode, -1);
        }

        CountDownLatch rollbackDoneLatch = new CountDownLatch(1);
        CountDownLatch finishTxLatch = new CountDownLatch(1);
        GridCacheVersion[] nearVerRef = new GridCacheVersion[1];

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = startTransaction(ignite0, 2)) {
                nearVerRef[0] = ((TransactionProxyImpl<?, ?>)tx).tx().nearXidVersion();

                tx.savepoint("sp");

                Integer val1 = cache0.get(keyOnInitiator);
                Integer val2 = cache0.get(keyOnOtherNode);

                if (initKeies) {
                    assertEquals(-1, val1.intValue());
                    assertEquals(-1, val2.intValue());
                }
                else {
                    assertNull(val1);
                    assertNull(val2);
                }

                cache0.put(keyOnInitiator, 1);
                cache0.put(keyOnOtherNode, 1);

                tx.rollbackToSavepoint("sp");

                rollbackDoneLatch.countDown();

                assertTrue(finishTxLatch.await(10, TimeUnit.SECONDS));

                val1 = cache0.get(keyOnInitiator);
                val2 = cache0.get(keyOnOtherNode);

                assertEquals(42, val1.intValue());
                assertEquals(42, val2.intValue());

                tx.commit();
            }
        });

        assertTrue(rollbackDoneLatch.await(10, TimeUnit.SECONDS));

        assertNoTxStateKeyOnNode(nearVerRef[0], keyOnInitiator);
        assertNoTxStateKeyOnNode(nearVerRef[0], keyOnOtherNode);

        updateKeyFormPrimary(keyOnInitiator);
        updateKeyFormPrimary(keyOnOtherNode);

        finishTxLatch.countDown();

        fut.get(10_000);

        assertNoActiveTx(nearVerRef[0]);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDhtEntriesAfterRollbackToSavepoint() throws Exception {
        IgniteCache<Integer, Integer> cache0 = transactionalCache(ignite0);

        int keepTxAliveKey = primaryKey(cache0);
        int dhtKey = keyForPrimaryAndBackup(ignite0, ignite1);

        if (initKeies) {
            cache0.put(keepTxAliveKey, -1);
            cache0.put(dhtKey, -1);
        }

        CountDownLatch dhtKeyWrittenLatch = new CountDownLatch(1);
        CountDownLatch proceedRollbackLatch = new CountDownLatch(1);
        CountDownLatch rollbackDoneLatch = new CountDownLatch(1);
        CountDownLatch finishTxLatch = new CountDownLatch(1);
        GridCacheVersion[] nearVerRef = new GridCacheVersion[1];

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = startTransaction(ignite0, 2)) {
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

        assertNoTxStateKeyOnNode(nearVerRef[0], dhtKey);

        finishTxLatch.countDown();

        fut.get(10_000);

        assertNoActiveTx(nearVerRef[0]);
    }

    /**
     * Updates a key from its primary node.
     *
     * @param key Key to update.
     */
    private void updateKeyFormPrimary(int key) {
        Ignite updateNode = primaryNode(key, DEFAULT_CACHE_NAME);

        updateNode.cache(DEFAULT_CACHE_NAME).put(key, 42);
    }

    /**
     * Asserts that no active transaction with the given near version contains the specified key
     * in its write-set or entry map on any node.
     *
     * @param nearVer Near transaction version.
     * @param key Key that must be absent from tx state.
     * @throws Exception If waiting fails.
     */
    private void assertNoTxStateKeyOnNode(GridCacheVersion nearVer, int key) throws Exception {
        for (Ignite ignite : G.allGrids()) {
            if (ignite.configuration().isClientMode())
                continue;

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
    }

    /**
     * Asserts that no active transaction remains with the near version.
     *
     * @param nearVer Near transaction version.
     * @throws Exception If waiting fails.
     */
    private void assertNoActiveTx(GridCacheVersion nearVer) throws Exception {
        for (Ignite ignite : G.allGrids()) {
            if (ignite.configuration().isClientMode())
                continue;

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

    /**
     * Creates transactional cache.
     *
     * @param ignite Node.
     * @return Transactional cache.
     */
    private IgniteCache<Integer, Integer> transactionalCache(Ignite ignite) {
        CacheConfiguration<?, ?> ccfg =
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setNearConfiguration(useNearCache ? new NearCacheConfiguration<>() : null)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setCacheMode(replicated ? CacheMode.REPLICATED : CacheMode.PARTITIONED)
                .setBackups(backups);

        return (IgniteCache<Integer, Integer>)ignite.createCache(ccfg);
    }

    /**
     * Gets key mapped to given primary and backups.
     * Primary is determined by primaryNode parameter, backup nodes are determined by spKeyOnTxInitiator parameter.
     *
     * @param txInitiator Transaction initiator.
     * @param primaryNode Node that should be primary.
     * @return Key mapped to given primary and backups.
     */
    private int keyForPrimaryAndBackup(Ignite txInitiator, Ignite primaryNode) {
        Ignite backupNode1 = spKeyOnTxInitiator && primaryNode != txInitiator ?
            txInitiator : G.allGrids().stream()
                          .filter(n -> n != txInitiator && n != primaryNode).findFirst().orElseThrow();
        Ignite backupNode2 = G.allGrids().stream()
            .filter(n -> n != txInitiator && n != primaryNode && n != backupNode1).findFirst().orElseThrow();

        for (int key = 0; key < 50_000; key++) {
            Collection<ClusterNode> mapping = txInitiator.affinity(DEFAULT_CACHE_NAME).mapKeyToPrimaryAndBackups(key);

            if (!replicated)
                assertEquals(backups + 1, mapping.size());

            ClusterNode primary = mapping.iterator().next();

            if (replicated && primary.id().equals(primaryNode.cluster().localNode().id()))
                return key;

            ClusterNode backup1 = mapping.stream().skip(1).findFirst().orElse(null);
            ClusterNode backup2 = mapping.stream().skip(2).findFirst().orElse(null);

            if (primary.id().equals(primaryNode.cluster().localNode().id()) &&
                (backups < 1 || backup1.id().equals(backupNode1.cluster().localNode().id())) &&
                (backups < 2 || backup2.id().equals(backupNode2.cluster().localNode().id())))
                return key;
        }

        throw new AssertionError("Failed to find key for requested primary/backup mapping.");
    }
}
