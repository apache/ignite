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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests savepoint support for pessimistic transactions.
 */
public class TxSavepointPessimisticTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "tx-savepoint-cache";

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
        IgniteCache<Integer, Integer> cache = transactionalCache(ignite);

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
    public void testOverwriteAndReleaseSavepoint() throws Exception {
        Ignite ignite = startGrid(0);
        IgniteCache<Integer, Integer> cache = transactionalCache(ignite);

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
        IgniteCache<Integer, Integer> cache = transactionalCache(ignite);

        CountDownLatch savepointRolledBackLatch = new CountDownLatch(1);
        CountDownLatch finishFirstTxLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(1, 1);
                tx.savepoint("sp");
                cache.put(2, 2);
                tx.rollbackToSavepoint("sp");

                savepointRolledBackLatch.countDown();

                assertTrue(finishFirstTxLatch.await(5, TimeUnit.SECONDS));

                tx.commit();
            }
        });

        assertTrue(savepointRolledBackLatch.await(5, TimeUnit.SECONDS));

        try (Transaction tx = ignite.transactions().txStart(
            TransactionConcurrency.PESSIMISTIC,
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

        IgniteCache<Integer, Integer> cache0 = transactionalCache(ignite0, 0);
        IgniteCache<Integer, Integer> cache1 = ignite1.cache(CACHE_NAME);

        List<Integer> node0PrimaryKeys = primaryKeys(cache0, 1);

        int keepTxAliveKey = node0PrimaryKeys.get(0);
        int remotePrimaryKey = primaryKey(cache1);

        CountDownLatch savepointRolledBackLatch = new CountDownLatch(1);
        CountDownLatch finishFirstTxLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache0.put(keepTxAliveKey, 1);

                tx.savepoint("sp");

                cache0.put(remotePrimaryKey, 1);

                tx.rollbackToSavepoint("sp");

                savepointRolledBackLatch.countDown();

                assertTrue(finishFirstTxLatch.await(5, TimeUnit.SECONDS));

                tx.commit();
            }
        });

        assertTrue(savepointRolledBackLatch.await(5, TimeUnit.SECONDS));

        try (Transaction tx = ignite1.transactions().txStart(
            TransactionConcurrency.PESSIMISTIC,
            TransactionIsolation.REPEATABLE_READ,
            3_000,
            0
        )) {
            cache1.put(remotePrimaryKey, 22);
            tx.commit();
        }

        finishFirstTxLatch.countDown();

        fut.get();

        assertEquals(Integer.valueOf(22), cache0.get(remotePrimaryKey));
    }

    /**
     * @param ignite Node.
     * @return Transactional cache.
     */
    private IgniteCache<Integer, Integer> transactionalCache(Ignite ignite) {
        return transactionalCache(ignite, 1);
    }

    /**
     * @param ignite Node.
     * @param backups Backups count.
     * @return Transactional cache.
     */
    private IgniteCache<Integer, Integer> transactionalCache(Ignite ignite, int backups) {
        CacheConfiguration<Integer, Integer> ccfg = defaultCacheConfiguration();

        ccfg.setName(CACHE_NAME);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setBackups(backups);

        return ignite.getOrCreateCache(ccfg);
    }
}
