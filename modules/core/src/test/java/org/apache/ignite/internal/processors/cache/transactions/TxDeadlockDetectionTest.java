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

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.jsr166.ThreadLocalRandom8;

import static org.apache.ignite.internal.util.typedef.X.hasCause;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class TxDeadlockDetectionTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 3;

    /** Cache. */
    private static final String CACHE = "cache";

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (isDebug()) {
            TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

            discoSpi.failureDetectionTimeoutEnabled(false);

            cfg.setDiscoverySpi(discoSpi);
        }

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setName(CACHE);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setNearConfiguration(null);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

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

    /**
     * @throws Exception If failed.
     */
    public void testNoHangs() throws Exception {
        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<Long> restartFut = null;

        try {
            restartFut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
                @Override public void run() {
                    while (!stop.get()) {
                        try {
                            U.sleep(500);

                            startGrid(NODES_CNT);

                            U.sleep(500);

                            stopGrid(NODES_CNT);
                        }
                        catch (Exception e) {
                            // No-op.
                        }
                    }
                }
            }, 1, "restart-thread");

            long stopTime = System.currentTimeMillis() + 2 * 60_000L;

            for (int i = 0; System.currentTimeMillis() < stopTime; i++) {
                log.info(">>> Iteration " + i);

                final AtomicInteger threadCnt = new AtomicInteger();

                IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
                    @Override public void run() {
                        int threadNum = threadCnt.getAndIncrement();

                        Ignite ignite = ignite(threadNum % NODES_CNT);

                        IgniteCache<Integer, Integer> cache = ignite.cache(CACHE);

                        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 500, 0)) {
                            ThreadLocalRandom8 rnd = ThreadLocalRandom8.current();

                            for (int i = 0; i < 500; i++) {
                                int key = rnd.nextInt(100);

                                if (log.isDebugEnabled()) {
                                    log.info(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode() +
                                        ", tx=" + tx + ", key=" + key + ']');
                                }

                                cache.put(key, 0);
                            }

                            tx.commit();
                        }
                        catch (Exception e) {
                            e.printStackTrace();
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
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoDeadlockSimple() throws Exception {
        final AtomicInteger threadCnt = new AtomicInteger();

        final AtomicBoolean deadlock = new AtomicBoolean();

        final AtomicBoolean timedOut = new AtomicBoolean();

        final CyclicBarrier barrier = new CyclicBarrier(2);

        final int timeout = 500;

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                int threadNum = threadCnt.getAndIncrement();

                Ignite ignite = ignite(threadNum);

                IgniteCache<Integer, Integer> cache = ignite.cache(CACHE);

                try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, timeout, 0)) {
                    int key = 42;

                    if (log.isDebugEnabled())
                        log.debug(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode() +
                            ", tx=" + tx + ", key=" + key + ']');

                    cache.put(key, 0);

                    barrier.await(timeout + 100, TimeUnit.MILLISECONDS);

                    tx.commit();
                }
                catch (Exception e) {
                    if (hasCause(e, TransactionTimeoutException.class))
                        timedOut.set(true);

                    if (hasCause(e, TransactionDeadlockException.class))
                        deadlock.set(true);
                }
            }
        }, 2, "tx-thread");

        fut.get();

        assertTrue(timedOut.get());

        assertFalse(deadlock.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoDeadlock() throws Exception {
        for (int i = 2; i <= 10; i++) {
            final int threads = i;

            log.info(">>> Test with " + threads + " transactions.");

            final AtomicInteger threadCnt = new AtomicInteger();

            final AtomicBoolean deadlock = new AtomicBoolean();

            final AtomicBoolean timedOut = new AtomicBoolean();

            final CyclicBarrier barrier = new CyclicBarrier(threads);

            final int timeout = 500;

            IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
                @Override public void run() {
                    int threadNum = threadCnt.incrementAndGet();

                    Ignite ignite = ignite(threadNum % NODES_CNT);

                    IgniteCache<Integer, Integer> cache = ignite.cache(CACHE);

                    try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, timeout, 0)) {
                        int key1 = threadNum;

                        log.info(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode() +
                            ", tx=" + tx + ", key=" + key1 + ']');

                        cache.put(key1, 0);

                        barrier.await();

                        if (threadNum == threads) {
                            log.info(">>> Performs sleep. [node=" + ((IgniteKernal)ignite).localNode() +
                                ", tx=" + tx + ']');

                            U.sleep(timeout * 2);
                        }
                        else {
                            int key2 = threadNum + 1;

                            log.info(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode() +
                                ", tx=" + tx + ", key2=" + key2 + ']');

                            cache.put(key2, 1);
                        }

                        tx.commit();
                    }
                    catch (Exception e) {
                        if (hasCause(e, TransactionTimeoutException.class))
                            timedOut.set(true);

                        if (hasCause(e, TransactionDeadlockException.class))
                            deadlock.set(true);
                    }
                }
            }, threads, "tx-thread");

            fut.get();

            assertTrue(timedOut.get());

            assertFalse(deadlock.get());
        }
    }
}
