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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.junit.Test;

import static org.apache.ignite.internal.util.typedef.X.hasCause;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class TxDeadlockDetectionUnmasrhalErrorsTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (isDebug()) {
            TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

            discoSpi.failureDetectionTimeoutEnabled(false);

            cfg.setDiscoverySpi(discoSpi);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);

        startClientGrid(1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeadlockCacheObjectContext() throws Exception {
        IgniteCache<Integer, Integer> cache0 = null;
        IgniteCache<Integer, Integer> cache1 = null;
        try {
            cache0 = getCache(ignite(0), "cache0");
            cache1 = getCache(ignite(0), "cache1");

            IgniteCache<Integer, Integer> clientCache0 = grid(1).cache("cache0");

            awaitPartitionMapExchange();

            final CyclicBarrier barrier = new CyclicBarrier(2);

            final CountDownLatch latch = new CountDownLatch(1);

            final AtomicInteger threadCnt = new AtomicInteger();

            final AtomicBoolean deadlock = new AtomicBoolean();

            IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
                @Override public void run() {
                    int threadNum = threadCnt.getAndIncrement();

                    Ignite ignite = ignite(0);

                    IgniteCache<Integer, Integer> cache1 = ignite.cache("cache" + (threadNum == 0 ? 0 : 1));

                    IgniteCache<Integer, Integer> cache2 = ignite.cache("cache" + (threadNum == 0 ? 1 : 0));

                    try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 1000, 0)) {
                        int key1 = threadNum == 0 ? 0 : 1;

                        log.info(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode() +
                            ", tx=" + tx + ", key=" + key1 + ", cache=" + cache1.getName() + ']');

                        cache1.put(key1, 0);

                        barrier.await();

                        int key2 = threadNum == 0 ? 1 : 0;

                        log.info(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode() +
                            ", tx=" + tx + ", key=" + key2 + ", cache=" + cache2.getName() + ']');

                        latch.countDown();

                        cache2.put(key2, 1);

                        tx.commit();

                        log.info(">>> Commit done");
                    }
                    catch (Throwable e) {
                        // At least one stack trace should contain TransactionDeadlockException.
                        if (hasCause(e, TransactionTimeoutException.class) &&
                            hasCause(e, TransactionDeadlockException.class)
                            ) {
                            if (deadlock.compareAndSet(false, true))
                                U.error(log, "At least one stack trace should contain " +
                                    TransactionDeadlockException.class.getSimpleName(), e);
                        }
                    }
                }
            }, 2, "tx-thread");

            latch.await();

            Ignite client = grid(1);

            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED, 500, 0)) {
                clientCache0.put(0, 3);
                clientCache0.put(1, 3);

                tx.commit();

                log.info(">>> Commit done");
            }
            catch (CacheException e) {
                assertTrue(X.hasCause(e, TransactionTimeoutException.class));
            }
            catch (Throwable e) {
                log.error("Unexpected exception occurred", e);

                fail();
            }

            fut.get();

            assertTrue(deadlock.get());

            for (int i = 0; i < NODES_CNT; i++) {
                Ignite ignite = ignite(i);

                IgniteTxManager txMgr = ((IgniteKernal)ignite).context().cache().context().tm();

                Collection<IgniteInternalFuture<?>> futs = txMgr.deadlockDetectionFutures();

                assertTrue(futs.isEmpty());
            }

            //assertNotNull(grid(1).context().cache().context().cacheContext(cacheId));
        }
        finally {
            if (cache0 != null)
                cache0.destroy();

            if (cache1 != null)
                cache1.destroy();
        }
    }

    /**
     * @param ignite Ignite.
     * @param name Name.
     */
    private IgniteCache<Integer, Integer> getCache(Ignite ignite, String name) {
        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setName(name);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setBackups(0);
        ccfg.setNearConfiguration(null);

        return ignite.getOrCreateCache(ccfg);
    }
}
