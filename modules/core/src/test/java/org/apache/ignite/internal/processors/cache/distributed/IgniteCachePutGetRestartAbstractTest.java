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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractTest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Test for specific user scenario.
 */
public abstract class IgniteCachePutGetRestartAbstractTest extends IgniteCacheAbstractTest {
    /** */
    private static final int ENTRY_CNT = 1000;

    /** */
    private Integer expVal = 0;

    /** */
    private final Object mux = new Object();

    /** */
    private volatile CountDownLatch latch = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (gridName.equals(getTestGridName(gridCount() - 1)))
            cfg.setClientMode(true);

        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setRebalanceMode(SYNC);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxPutGetRestart() throws Exception {
        int clientGrid = gridCount() - 1;

        assertTrue(ignite(clientGrid).configuration().isClientMode());

        final IgniteTransactions txs = ignite(clientGrid).transactions();

        final IgniteCache<Integer, Integer> cache = jcache(clientGrid);

        updateCache(cache, txs);

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> updateFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Thread.currentThread().setName("update-thread");

                assertTrue(latch.await(30_000, TimeUnit.MILLISECONDS));

                int iter = 0;

                while (!stop.get()) {
                    log.info("Start update: " + iter);

                    synchronized (mux) {
                        updateCache(cache, txs);
                    }

                    log.info("End update: " + iter++);
                }

                log.info("Update iterations: " + iter);

                return null;
            }
        });

        IgniteInternalFuture<?> restartFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Thread.currentThread().setName("restart-thread");

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (!stop.get()) {
                    assertTrue(latch.await(30_000, TimeUnit.MILLISECONDS));

                    int node = rnd.nextInt(0, gridCount() - 1);

                    log.info("Stop node: " + node);

                    stopGrid(node);

                    U.sleep(100);

                    log.info("Start node: " + node);

                    startGrid(node);

                    latch = new CountDownLatch(1);

                    U.sleep(100);
                }

                return null;
            }
        });

        long endTime = System.currentTimeMillis() + 2 * 60_000;

        try {
            int iter = 0;

            while (System.currentTimeMillis() < endTime && !updateFut.isDone() && !restartFut.isDone()) {
                try {
                    log.info("Start get: " + iter);

                    synchronized (mux) {
                        readCache(cache, txs);
                    }

                    log.info("End get: " + iter++);
                }
                finally {
                    latch.countDown();
                }
            }

            log.info("Get iterations: " + iter);
        }
        finally {
            latch.countDown();

            stop.set(true);
        }

        updateFut.get();

        restartFut.get();

        readCache(cache, txs);
    }

    /**
     * @param cache Cache.
     * @param txs Transactions.
     */
    private void readCache(IgniteCache<Integer, Integer> cache, IgniteTransactions txs) {
        try (Transaction tx = txs.txStart(OPTIMISTIC, REPEATABLE_READ)) {
            for (int i = 0; i < ENTRY_CNT; i++)
                assertNotNull(cache.get(i));
        }
    }

    /**
     * @param cache Cache.
     * @param txs Transactions.
     */
    private void updateCache(IgniteCache<Integer, Integer> cache, IgniteTransactions txs) {
        int val = expVal + 1;

        try {
            try (Transaction tx = txs.txStart(OPTIMISTIC, REPEATABLE_READ)) {
                for (int i = 0; i < ENTRY_CNT; i++)
                    cache.put(i, val);

                tx.commit();

                expVal = val;

                log.info("Updated cache, new value: " + val);
            }
        }
        catch (IgniteException e) {
            log.error("Update failed: " + e, e);
        }
    }
}