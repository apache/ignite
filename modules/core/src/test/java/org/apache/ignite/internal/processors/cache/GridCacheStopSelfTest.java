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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests correct cache stopping.
 */
public class GridCacheStopSelfTest extends GridCommonAbstractTest {
    /** */
    private CacheAtomicityMode atomicityMode = TRANSACTIONAL;

    /** */
    private boolean replicated;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME)
            .setAtomicityMode(atomicityMode);

        if (replicated)
            ccfg.setCacheMode(REPLICATED);
        else
            ccfg.setBackups(1);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopExplicitTransactions() throws Exception {
        atomicityMode = TRANSACTIONAL;

        testStop(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopImplicitTransactions() throws Exception {
        atomicityMode = TRANSACTIONAL;

        testStop(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopExplicitTransactionsReplicated() throws Exception {
        atomicityMode = TRANSACTIONAL;
        replicated = true;

        testStop(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopImplicitTransactionsReplicated() throws Exception {
        atomicityMode = TRANSACTIONAL;
        replicated = true;

        testStop(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopExplicitMvccTransactions() throws Exception {
        atomicityMode = TRANSACTIONAL_SNAPSHOT;

        testStop(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopImplicitMvccTransactions() throws Exception {
        atomicityMode = TRANSACTIONAL_SNAPSHOT;

        testStop(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopExplicitMvccTransactionsReplicated() throws Exception {
        atomicityMode = TRANSACTIONAL_SNAPSHOT;
        replicated = true;

        testStop(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopImplicitMvccTransactionsReplicated() throws Exception {
        atomicityMode = TRANSACTIONAL_SNAPSHOT;
        replicated = true;

        testStop(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopAtomic() throws Exception {
        atomicityMode = ATOMIC;

        testStop(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopMultithreaded() throws Exception {
        try {
            startGrid(0);

            for (int i = 0; i < 5; i++) {
                log.info("Iteration: " + i);

                startGridsMultiThreaded(1, 3);

                final AtomicInteger threadIdx = new AtomicInteger(0);

                final IgniteInternalFuture<?> fut1 = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        int idx = threadIdx.getAndIncrement();

                        IgniteKernal node = (IgniteKernal)ignite(idx % 3 + 1);

                        IgniteCache<Integer, Integer> cache = node.cache(DEFAULT_CACHE_NAME);

                        while (true) {
                            try {
                                cacheOperations(node, cache);
                            }
                            catch (Exception ignored) {
                                if (node.isStopping())
                                    break;
                            }
                        }

                        return null;
                    }
                }, 20, "tx-node-stop-thread");

                IgniteInternalFuture<?> fut2 = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        IgniteKernal node = (IgniteKernal)ignite(0);

                        IgniteCache<Integer, Integer> cache = node.cache(DEFAULT_CACHE_NAME);

                        while (!fut1.isDone()) {
                            try {
                                cacheOperations(node, cache);
                            }
                            catch (Exception ignore) {
                                // No-op.
                            }
                        }

                        return null;
                    }
                }, 2, "tx-thread");

                Thread.sleep(3000);

                final AtomicInteger nodeIdx = new AtomicInteger(1);

                GridTestUtils.runMultiThreaded(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        int idx = nodeIdx.getAndIncrement();

                        log.info("Stop node: " + idx);

                        ignite(idx).close();

                        return null;
                    }
                }, 3, "stop-node");

                fut1.get();
                fut2.get();
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param node Node.
     * @param cache Cache.
     */
    @SuppressWarnings("unchecked")
    private void cacheOperations(Ignite node, IgniteCache<Integer, Integer> cache) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        Integer key = rnd.nextInt(1000);

        cache.put(key, key);

        cache.get(key);

        if (cache.getConfiguration(CacheConfiguration.class).getAtomicityMode() != TRANSACTIONAL_SNAPSHOT) {
            try (Transaction tx = node.transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
                cache.put(key, key);

                tx.commit();
            }
        }

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.put(key, key);

            tx.commit();
        }
    }

    /**
     * @param startTx If {@code true} starts transactions.
     * @throws Exception If failed.
     */
    private void testStop(final boolean startTx) throws Exception {
        for (int i = 0; i < 10; i++) {
            startGrid(0);

            final int PUT_THREADS = 50;

            final CountDownLatch stopLatch = new CountDownLatch(1);

            final CountDownLatch readyLatch = new CountDownLatch(PUT_THREADS);

            final IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

            assertNotNull(cache);

            CacheConfiguration ccfg = cache.getConfiguration(CacheConfiguration.class);

            assertEquals(atomicityMode, ccfg.getAtomicityMode());
            assertEquals(replicated ? REPLICATED : PARTITIONED, ccfg.getCacheMode());

            Collection<IgniteInternalFuture<?>> putFuts = new ArrayList<>();

            for (int j = 0; j < PUT_THREADS; j++) {
                final int key = j;

                putFuts.add(GridTestUtils.runAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        try {
                            if (startTx) {
                                TransactionConcurrency concurrency =
                                    atomicityMode != TRANSACTIONAL_SNAPSHOT && (key % 2 == 0) ? OPTIMISTIC : PESSIMISTIC;

                                try (Transaction tx = grid(0).transactions().txStart(concurrency, REPEATABLE_READ)) {
                                    cache.put(key, key);

                                    readyLatch.countDown();

                                    stopLatch.await();

                                    tx.commit();
                                }
                            }
                            else {
                                readyLatch.countDown();

                                stopLatch.await();

                                cache.put(key, key);
                            }
                        }
                        catch (CacheException | IgniteException | IllegalStateException e) {
                            log.info("Ignore error: " + e);
                        }

                        return null;
                    }
                }, "cache-thread"));
            }

            readyLatch.await();

            stopLatch.countDown();

            stopGrid(0);

            for (IgniteInternalFuture<?> fut : putFuts)
                fut.get();

            try {
                cache.put(1, 1);
            }
            catch (IllegalStateException e) {
                if (!X.hasCause(e, CacheStoppedException.class)) {
                    e.printStackTrace();

                    fail("Unexpected exception: " + e);
                }
            }
        }
    }
}
