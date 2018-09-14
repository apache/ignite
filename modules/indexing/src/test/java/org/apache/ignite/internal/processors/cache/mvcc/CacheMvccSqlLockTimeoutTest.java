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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionState;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/** */
public class CacheMvccSqlLockTimeoutTest extends CacheMvccAbstractTest {
    /** */
    private static final int TIMEOUT_MILLIS = 200;

    /** */
    private UnaryOperator<IgniteConfiguration> cfgCustomizer = UnaryOperator.identity();

    // t0d0 write concurrent test
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        throw new RuntimeException("Is not used in current test");
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return cfgCustomizer.apply(super.getConfiguration(gridName));
    }

    /**
     * @throws Exception if failed.
     */
    public void testLockTimeoutsForPartitionedCache() throws Exception {
        checkLockTimeouts(
            baseCacheConfig()
                .setCacheMode(PARTITIONED)
                .setBackups(1)
        );
    }

    /**
     * @throws Exception if failed.
     */
    public void testLockTimeoutsForReplicatedCache() throws Exception {
        checkLockTimeouts(baseCacheConfig().setCacheMode(REPLICATED));
    }

    /**
     * @throws Exception if failed.
     */
    public void testLockTimeoutsAfterDefaultTxTimeoutForPartitionedCache() throws Exception {
        checkLockTimeoutsAfterDefaultTxTimeout(
            baseCacheConfig()
                .setCacheMode(PARTITIONED)
                .setBackups(1)
        );
    }

    /**
     * @throws Exception if failed.
     */
    public void testLockTimeoutsAfterDefaultTxTimeoutForReplicatedCache() throws Exception {
        checkLockTimeoutsAfterDefaultTxTimeout(baseCacheConfig().setCacheMode(REPLICATED));
    }

    /** */
    private CacheConfiguration<?, ?> baseCacheConfig() {
        return new CacheConfiguration<>("test")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(PARTITIONED)
            .setBackups(1)
            .setSqlSchema("PUBLIC")
            .setIndexedTypes(Integer.class, Integer.class);
    }

    /** */
    private void checkLockTimeouts(CacheConfiguration<?, ?> ccfg) throws Exception {
        startGridsMultiThreaded(2);

        IgniteEx ignite = grid(0);

        ignite.createCache(ccfg);

        AtomicInteger keyCntr = new AtomicInteger();

        int nearKey = keyForNode(ignite.affinity("test"), keyCntr, ignite.localNode());
        int otherKey = keyForNode(ignite.affinity("test"), keyCntr, grid(1).localNode());

        TimeoutChecker timeoutChecker = new TimeoutChecker(ignite, "test");

        timeoutChecker.checkScenario(TimeoutMode.STMT, TxStartMode.EXPLICIT, nearKey);

        timeoutChecker.checkScenario(TimeoutMode.STMT, TxStartMode.EXPLICIT, otherKey);

        timeoutChecker.checkScenario(TimeoutMode.STMT, TxStartMode.IMPLICIT, nearKey);

        timeoutChecker.checkScenario(TimeoutMode.STMT, TxStartMode.IMPLICIT, otherKey);

        // explicit tx timeout has no sense for implicit transaction
        timeoutChecker.checkScenario(TimeoutMode.TX, TxStartMode.EXPLICIT, nearKey);

        timeoutChecker.checkScenario(TimeoutMode.TX, TxStartMode.EXPLICIT, otherKey);
    }

    /** */
    private void checkLockTimeoutsAfterDefaultTxTimeout(CacheConfiguration<?, ?> ccfg) throws Exception {
        cfgCustomizer = cfg ->
            cfg.setTransactionConfiguration(new TransactionConfiguration().setDefaultTxTimeout(TIMEOUT_MILLIS));

        startGridsMultiThreaded(2);

        IgniteEx ignite = grid(0);

        ignite.createCache(ccfg);

        AtomicInteger keyCntr = new AtomicInteger();

        int nearKey = keyForNode(ignite.affinity("test"), keyCntr, ignite.localNode());
        int otherKey = keyForNode(ignite.affinity("test"), keyCntr, grid(1).localNode());

        TimeoutChecker timeoutChecker = new TimeoutChecker(ignite, "test");

        timeoutChecker.checkScenario(TimeoutMode.TX_DEFAULT, TxStartMode.EXPLICIT, nearKey);

        timeoutChecker.checkScenario(TimeoutMode.TX_DEFAULT, TxStartMode.EXPLICIT, otherKey);

        timeoutChecker.checkScenario(TimeoutMode.TX_DEFAULT, TxStartMode.IMPLICIT, nearKey);

        timeoutChecker.checkScenario(TimeoutMode.TX_DEFAULT, TxStartMode.IMPLICIT, otherKey);
    }

    /** */
    private static class TimeoutChecker {
        /** */
        final Ignite ignite;
        /** */
        final String cacheName;

        /** */
        TimeoutChecker(Ignite ignite, String cacheName) {
            this.ignite = ignite;
            this.cacheName = cacheName;
        }

        /** */
        void checkScenario(TimeoutMode timeoutMode, TxStartMode txStartMode, int key) throws Exception {
            // 999 is used as bound to enforce query execution with obtaining cursor before enlist
            assert key <= 999;

            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 60_000, 1)) {
                ignite.cache(cacheName).query(new SqlFieldsQuery("merge into Integer(_key, _val) values(?, 1)")
                    .setArgs(key));

                tx.commit();
            }

            ensureTimeIsOut("insert into Integer(_key, _val) values(?, 42)", key, timeoutMode, txStartMode);
            ensureTimeIsOut("merge into Integer(_key, _val) values(?, 42)", key, timeoutMode, txStartMode);
            ensureTimeIsOut("update Integer set _val = 42 where _key = ?", key, timeoutMode, txStartMode);
            ensureTimeIsOut("update Integer set _val = 42 where _key = ? or _key > 999", key, timeoutMode, txStartMode);
            ensureTimeIsOut("delete from Integer where _key = ?", key, timeoutMode, txStartMode);
            ensureTimeIsOut("delete from Integer where _key = ? or _key > 999", key, timeoutMode, txStartMode);

            // SELECT ... FOR UPDATE locking entries has no meaning for implicit transaction
            if (txStartMode != TxStartMode.IMPLICIT) {
                ensureTimeIsOut("select * from Integer where _key = ? for update", key, timeoutMode, txStartMode);
                ensureTimeIsOut(
                    "select * from Integer where _key = ? or _key > 999 for update", key, timeoutMode, txStartMode);
            }
        }

        /** */
        void ensureTimeIsOut(String sql, int key, TimeoutMode timeoutMode, TxStartMode txStartMode) throws Exception {
            assert txStartMode == TxStartMode.EXPLICIT || timeoutMode != TimeoutMode.TX;

            IgniteCache<?, ?> cache = ignite.cache(cacheName);

            int oldVal = (Integer)cache
                .query(new SqlFieldsQuery("select _val from Integer where _key = ?").setArgs(key))
                .getAll().get(0).get(0);

            try (Transaction tx1 = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 60_000, 1)) {
                cache.query(new SqlFieldsQuery("update Integer set _val = 42 where _key = ?").setArgs(key));

                try {
                    CompletableFuture.runAsync(() -> {
                        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setArgs(key);

                        try (Transaction tx2 = txStartMode == TxStartMode.EXPLICIT ? startTx(timeoutMode): null) {
                            if (timeoutMode == TimeoutMode.STMT)
                                qry.setTimeout(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

                            cache.query(qry).getAll();

                            if (tx2 != null)
                                tx2.commit();
                        }
                    }).get();

                    fail("Timeout exception should be thrown");
                }
                catch (ExecutionException ee) {
                    // t0d0 remove
                    ee.printStackTrace();
                    assertTrue(X.hasCause(ee, IgniteTxTimeoutCheckedException.class)
                        || X.hasCause(ee, IgniteTxRollbackCheckedException.class)
                        || msgContains(ee, "Failed to acquire lock within provided timeout for transaction")
                        || msgContains(ee, "Failed to finish transaction because it has been rolled back"));
                }

                assertEquals(TransactionState.ACTIVE, tx1.state());

                tx1.rollback();
            }

            int newVal = (Integer)cache
                .query(new SqlFieldsQuery("select _val from Integer where _key = ?").setArgs(key))
                .getAll().get(0).get(0);

            assertEquals(oldVal, newVal);
        }

        /** */
        private Transaction startTx(TimeoutMode timeoutMode) {
            return timeoutMode == TimeoutMode.TX
                ? ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, TIMEOUT_MILLIS, 1)
                : ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);
        }

        /** */
        private static boolean msgContains(Throwable e, String str) {
            return e.getMessage() != null && e.getMessage().contains(str);
        }
    }

    /** */
    private enum TimeoutMode {
        /** */
        TX,
        /** */
        TX_DEFAULT,
        /** */
        STMT
    }

    /** */
    private enum TxStartMode {
        /** */
        EXPLICIT,
        /** */
        IMPLICIT
    }
}
