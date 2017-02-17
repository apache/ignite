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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_TX_DEADLOCK_DETECTION_MAX_ITERS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TX_DEADLOCK_DETECTION_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class TxDeadlockDetectionNoHangsTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 3;

    /** Cache. */
    private static final String CACHE = "cache";

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setName(CACHE);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setNearConfiguration(null);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGridsMultiThreaded(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        GridTestUtils.setFieldValue(null, TxDeadlockDetection.class, "DEADLOCK_TIMEOUT", (int)(getTestTimeout() * 2));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        GridTestUtils.setFieldValue(null, TxDeadlockDetection.class, "DEADLOCK_TIMEOUT",
            getInteger(IGNITE_TX_DEADLOCK_DETECTION_TIMEOUT, 60000));
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60 * 1000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoHangsPessimistic() throws Exception {
        assertTrue(grid(0).context().cache().context().tm().deadlockDetectionEnabled());

        doTest(PESSIMISTIC);

        try {
            GridTestUtils.setFieldValue(null, IgniteTxManager.class, "DEADLOCK_MAX_ITERS", 0);

            assertFalse(grid(0).context().cache().context().tm().deadlockDetectionEnabled());

            doTest(PESSIMISTIC);
        }
        finally {
            GridTestUtils.setFieldValue(null, IgniteTxManager.class, "DEADLOCK_MAX_ITERS",
                IgniteSystemProperties.getInteger(IGNITE_TX_DEADLOCK_DETECTION_MAX_ITERS, 1000));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoHangsOptimistic() throws Exception {
        assertTrue(grid(0).context().cache().context().tm().deadlockDetectionEnabled());

        doTest(OPTIMISTIC);

        try {
            GridTestUtils.setFieldValue(null, IgniteTxManager.class, "DEADLOCK_MAX_ITERS", 0);

            assertFalse(grid(0).context().cache().context().tm().deadlockDetectionEnabled());

            doTest(OPTIMISTIC);
        }
        finally {
            GridTestUtils.setFieldValue(null, IgniteTxManager.class, "DEADLOCK_MAX_ITERS",
                IgniteSystemProperties.getInteger(IGNITE_TX_DEADLOCK_DETECTION_MAX_ITERS, 1000));
        }
    }

    /**
     * @param concurrency Concurrency.
     * @throws IgniteCheckedException If failed.
     */
    private void doTest(final TransactionConcurrency concurrency) throws IgniteCheckedException {
        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<Long> restartFut = null;

        try {
            restartFut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
                @Override public void run() {
                    while (!stop.get()) {
                        try {
                            U.sleep(500);

                            startGrid(NODES_CNT);

                            awaitPartitionMapExchange();

                            U.sleep(500);

                            stopGrid(NODES_CNT);
                        }
                        catch (Exception ignored) {
                            // No-op.
                        }
                    }
                }
            }, 1, "restart-thread");

            long stopTime = System.currentTimeMillis() + 2 * 60_000L;

            for (int i = 0; System.currentTimeMillis() < stopTime; i++) {
                boolean detectionEnabled = grid(0).context().cache().context().tm().deadlockDetectionEnabled();

                log.info(">>> Iteration " + i + " (detection is " + (detectionEnabled ? "enabled" : "disabled") + ')');

                final AtomicInteger threadCnt = new AtomicInteger();

                IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
                    @Override public void run() {
                        int threadNum = threadCnt.getAndIncrement();

                        Ignite ignite = ignite(threadNum % NODES_CNT);

                        IgniteCache<Integer, Integer> cache = ignite.cache(CACHE);

                        try (Transaction tx = ignite.transactions().txStart(concurrency, REPEATABLE_READ, 500, 0)) {
                            ThreadLocalRandom rnd = ThreadLocalRandom.current();

                            for (int i = 0; i < 50; i++) {
                                int key = rnd.nextInt(50);

                                if (log.isDebugEnabled()) {
                                    log.info(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode() +
                                        ", tx=" + tx + ", key=" + key + ']');
                                }

                                cache.put(key, 0);
                            }

                            tx.commit();
                        }
                        catch (Exception e) {
                            log.info("Ignore error: " + e);
                        }
                    }
                }, NODES_CNT * 3, "tx-thread");

                fut.get();
            }
        }
        finally {
            stop.set(true);

            if (restartFut != null)
                restartFut.get();

            checkDetectionFutures();
        }
    }

    /**
     *
     */
    private void checkDetectionFutures() {
        for (int i = 0; i < NODES_CNT ; i++) {
            Ignite ignite = ignite(i);

            IgniteTxManager txMgr = ((IgniteKernal)ignite).context().cache().context().tm();

            Collection<IgniteInternalFuture<?>> futs = txMgr.deadlockDetectionFutures();

            assertTrue(futs.isEmpty());
        }
    }
}
