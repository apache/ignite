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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.GridKernalState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 */
public class TxCompletionOnNodeStopTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 4;

    /** Tx size. */
    private static final int TX_SIZE = 100;

    /** Tx modes. */
    @SuppressWarnings("unchecked")
    private static final T2<TransactionConcurrency, TransactionIsolation>[] txModes = new T2[] {
        new T2<>(PESSIMISTIC, READ_COMMITTED),
        new T2<>(PESSIMISTIC, REPEATABLE_READ),
        new T2<>(OPTIMISTIC, READ_COMMITTED),
        new T2<>(OPTIMISTIC, REPEATABLE_READ),
        new T2<>(OPTIMISTIC, SERIALIZABLE)
    };

    /** Wait for condition timeout. */
    private static final long OPERATIONS_TIMEOUT = 5_000;

    /** Ignite to be stopped index. */
    private static final int IGNITE_IDX = 111;

    /** Small tx on stop timeout. */
    private static boolean smallTxOnStopTimeout;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (smallTxOnStopTimeout) {
            cfg.getTransactionConfiguration()
                .setTxOnStopTimeout(100);
        }

        return optimize(cfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxOnStopTimeout() throws Exception {
        smallTxOnStopTimeout = true;

        IgniteCache cache = null;

        try {
            final CountDownLatch latch = new CountDownLatch(1);

            final Ignite ignite = startGrid(IGNITE_IDX);

            cache = createCache(FULL_SYNC, PARTITIONED, false);

            awaitPartitionMapExchange();

            IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(() -> {
                IgniteCache<Object, Object> cache0 = ignite.cache(DEFAULT_CACHE_NAME);

                try (Transaction tx = ignite.transactions().txStart()) {
                    latch.countDown();

                    for (int i = 0; U.currentTimeMillis() - tx.startTime() < OPERATIONS_TIMEOUT; i++)
                        cache0.put(i, String.valueOf(i));

                    fail();
                }
                catch (Exception e) {
                    if (!X.hasCause(e, IllegalStateException.class, NodeStoppingException.class)) {

                        U.error(log(), "Unexpected exception", e);

                        fail();
                    }
                }

                return null;
            });

            latch.await();

            stopGrid(IGNITE_IDX, false);

            fut.get();
        }
        finally {
            if (cache != null)
                cache.destroy();

            smallTxOnStopTimeout = false;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransactionsCompletionOnStop() throws Exception {
        for (CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values()) {
            doTestsTxCompletionOnStop(syncMode, REPLICATED, false);
            doTestsTxCompletionOnStop(syncMode, PARTITIONED, false);
            doTestsTxCompletionOnStop(syncMode, PARTITIONED, true);
        }
    }

    /**
     * @param syncMode Sync mode.
     * @param cacheMode Cache mode.
     * @throws Exception If failed.
     */
    private void doTestsTxCompletionOnStop(CacheWriteSynchronizationMode syncMode,
        CacheMode cacheMode, boolean near) throws Exception {
        IgniteCache cache = null;

        try {
            cache = createCache(syncMode, cacheMode, near);

            log().info(String.format(">>> Start tests with syncMode=%s, cacheMode=%s, near=%s.",
                syncMode, cacheMode, near));

            doTestTxCompletionOnStop(false);
            doTestTxCompletionOnStop(true);
        }
        finally {
            if (cache != null)
                cache.destroy();
        }
    }

    /**
     * @param syncMode Sync mode.
     * @param cacheMode Cache mode.
     * @return Created cache.
     */
    @SuppressWarnings("unchecked")
    private IgniteCache createCache(CacheWriteSynchronizationMode syncMode, CacheMode cacheMode, boolean near) {
        CacheConfiguration cfg = defaultCacheConfiguration()
            .setAtomicityMode(TRANSACTIONAL)
            .setWriteSynchronizationMode(syncMode)
            .setCacheMode(cacheMode)
            .setBackups(1)
            .setNearConfiguration(near ? new NearCacheConfiguration() : null);

        return ignite(0).createCache(cfg);
    }

    /**
     * @param client Client.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void doTestTxCompletionOnStop(boolean client) throws Exception {
        log().info(String.format(">>> Start test with client=%s.", client));

        final CountDownLatch txsStartLatch = new CountDownLatch(txModes.length * 2);
        final CountDownLatch stopGridLatch = new CountDownLatch(1);

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(IGNITE_IDX))
            .setClientMode(client);

        final IgniteEx ignite = startGrid(cfg);

        final IgniteCacheProxy cache = (IgniteCacheProxy)ignite.cache(DEFAULT_CACHE_NAME);

        if (client && cache.context().config().getNearConfiguration() != null)
            ignite.createNearCache(DEFAULT_CACHE_NAME, new NearCacheConfiguration<>());

        awaitPartitionMapExchange();

        IgniteInternalFuture<Object> stoppingFut = GridTestUtils.runAsync(() -> {
            txsStartLatch.await();

            try {
                log().info("Grid stopping started.");

                stopGrid(IGNITE_IDX, false);

                log().info("Grid stopped.");
            }
            catch (Exception e) {
                U.error(log(), "Unexpected exception", e);

                fail();
            }

            stopGridLatch.countDown();

            return null;
        });

        final AtomicInteger threadCnt = new AtomicInteger();

        IgniteInternalFuture<Long> txsFut = GridTestUtils.runMultiThreadedAsync(() -> {
            int threadNum = threadCnt.getAndIncrement();

            int add = threadNum * TX_SIZE;

            T2<TransactionConcurrency, TransactionIsolation> txMode =
                txModes[threadNum >= txModes.length ? threadNum - txModes.length : threadNum];

            log().info(String.format(">>> Start TX concurrency=%s, isolation=%s, threadNum=%s",
                txMode.get1(), txMode.get2(), threadNum));

            try (Transaction tx = ignite.transactions()
                .txStart(txMode.get1(), txMode.get2(), OPERATIONS_TIMEOUT, TX_SIZE)) {
                for (int i = 0; i < TX_SIZE; i++) {
                    if (i == TX_SIZE / 2) {
                        txsStartLatch.countDown();

                        boolean preStopping = GridTestUtils.waitForCondition(() ->
                            ignite.context().gateway().getState() == GridKernalState.PRE_STOPPING, OPERATIONS_TIMEOUT);

                        assertTrue(preStopping);
                    }

                    cache.put(i + add, String.valueOf(i + add));
                }

                if (threadNum < txModes.length)
                    tx.commit();
                else
                    tx.rollback();

                stopGridLatch.await(); // In order to call close() after grid stopped.
            }

            return null;
        }, txModes.length * 2, "tx-thread");

        txsFut.get();

        stoppingFut.get();

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache0 = ignite(0).cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = ignite(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (int i = 0; i < TX_SIZE * txModes.length * 2; i++)
                assertEquals((i / TX_SIZE) < txModes.length ? String.valueOf(i) : null, cache0.get(i));

            tx.commit();
        }

        cache0.clear();
    }
}
