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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DUMP_TX_COLLISIONS_INTERVAL;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/** Tests tx key contention detection functional. */
public class TxWithKeyContentionSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(20 * 1024 * 1024)
                )
        );

        return cfg;
    }

    /** */
    protected CacheConfiguration<Integer, Integer> getCacheConfiguration(boolean nearCache) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAffinity(new RendezvousAffinityFunction(false, 16))
            .setBackups(2)
            .setStatisticsEnabled(true);

        if (nearCache)
            ccfg.setNearConfiguration(new NearCacheConfiguration<>());

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Tests transactional payload.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_TX_COLLISIONS_INTERVAL, value = "30000")
    public void testPessimisticRepeatableReadCheckContentionTxMetric() throws Exception {
        runKeyCollisionsMetric(PESSIMISTIC, REPEATABLE_READ, false);
    }

    /**
     * Tests transactional payload with near cache enabled.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_TX_COLLISIONS_INTERVAL, value = "30000")
    public void testPessimisticRepeatableReadCheckContentionTxMetricNear() throws Exception {
        runKeyCollisionsMetric(PESSIMISTIC, REPEATABLE_READ, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_TX_COLLISIONS_INTERVAL, value = "30000")
    public void testPessimisticReadCommitedCheckContentionTxMetric() throws Exception {
        runKeyCollisionsMetric(PESSIMISTIC, READ_COMMITTED, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_TX_COLLISIONS_INTERVAL, value = "30000")
    public void testPessimisticReadCommitedCheckContentionTxMetricNear() throws Exception {
        runKeyCollisionsMetric(PESSIMISTIC, READ_COMMITTED, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_TX_COLLISIONS_INTERVAL, value = "30000")
    public void testOptimisticReadCommittedCheckContentionTxMetric() throws Exception {
        runKeyCollisionsMetric(OPTIMISTIC, READ_COMMITTED, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_TX_COLLISIONS_INTERVAL, value = "30000")
    public void testOptimisticReadCommittedCheckContentionTxMetricNear() throws Exception {
        runKeyCollisionsMetric(OPTIMISTIC, READ_COMMITTED, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_TX_COLLISIONS_INTERVAL, value = "30000")
    public void testOptimisticRepeatableReadCheckContentionTxMetric() throws Exception {
        runKeyCollisionsMetric(OPTIMISTIC, REPEATABLE_READ, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_TX_COLLISIONS_INTERVAL, value = "30000")
    public void testOptimisticRepeatableReadCheckContentionTxMetricNear() throws Exception {
        runKeyCollisionsMetric(OPTIMISTIC, REPEATABLE_READ, true);
    }

    /** Tests metric correct results while tx collisions occured.
     *
     * @param concurrency Concurrency level.
     * @param isolation Isolation level.
     * @throws Exception If failed.
     */
    private void runKeyCollisionsMetric(TransactionConcurrency concurrency, TransactionIsolation isolation, boolean nearCache)
            throws Exception {
        Ignite ig = startGridsMultiThreaded(3);

        int contCnt = (int)U.staticField(IgniteTxManager.class, "COLLISIONS_QUEUE_THRESHOLD") * 2;

        Ignite cl = startClientGrid();

        IgniteCache<Integer, Integer> clientCache = cl.createCache(getCacheConfiguration(nearCache));

        final Integer keyId = primaryKey(ig.cache(DEFAULT_CACHE_NAME));

        IgniteTransactions transactions = cl.transactions();

        AtomicBoolean doTest = new AtomicBoolean(true);

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(
            () -> {
                while (doTest.get()) {
                    try (Transaction tx = transactions.txStart(concurrency, isolation)) {
                        clientCache.put(keyId, 0);

                        tx.commit();
                    }
                }
            },
            contCnt,
            "threadName");

        try {
            assertTrue(GridTestUtils.waitForCondition(
                () -> checkMetrics(ig),
                getTestTimeout()));
        }
        finally {
            doTest.set(false);

            fut.get();
        }
    }

    /**
     * Checks if the transaction collision metrics contain the string "queueSize" for the given Ignite instance.
     *
     * @param ig Ignite instance.
     * @return {@code true} if the metrics contain "queueSize"; otherwise {@code false}.
     */
    private static boolean checkMetrics(Ignite ig) {
        IgniteTxManager srvTxMgr = ((IgniteEx)ig).context().cache().context().tm();

        try {
            U.invoke(IgniteTxManager.class, srvTxMgr, "collectTxCollisionsInfo");
        }
        catch (IgniteCheckedException e) {
            fail(e.toString());
        }

        return ig.cache(DEFAULT_CACHE_NAME)
            .localMetrics()
            .getTxKeyCollisions()
            .contains("queueSize");
    }
}
