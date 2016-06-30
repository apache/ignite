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

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
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

import static org.apache.ignite.internal.util.typedef.X.hasCause;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class TxPessimisticDeadlockDetectionCrossCacheTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 2;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        if (isDebug()) {
            TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

            discoSpi.failureDetectionTimeoutEnabled(false);

            cfg.setDiscoverySpi(discoSpi);
        }

        CacheConfiguration ccfg0 = defaultCacheConfiguration();

        ccfg0.setName("cache0");
        ccfg0.setCacheMode(CacheMode.PARTITIONED);
        ccfg0.setBackups(1);
        ccfg0.setNearConfiguration(null);

        CacheConfiguration ccfg1 = defaultCacheConfiguration();

        ccfg1.setName("cache1");
        ccfg1.setCacheMode(CacheMode.PARTITIONED);
        ccfg1.setBackups(1);
        ccfg1.setNearConfiguration(null);

        cfg.setCacheConfiguration(ccfg0, ccfg1);

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
    public void testDeadlock() throws Exception {
        final CyclicBarrier barrier = new CyclicBarrier(2);

        final AtomicInteger threadCnt = new AtomicInteger();

        final AtomicBoolean deadlock = new AtomicBoolean();

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                int threadNum = threadCnt.getAndIncrement();

                Ignite ignite = ignite(0);

                IgniteCache<Integer, Integer> cache1 = ignite.cache("cache" + (threadNum == 0 ? 0 : 1));

                IgniteCache<Integer, Integer> cache2 = ignite.cache("cache" + (threadNum == 0 ? 1 : 0));

                try (Transaction tx =
                         ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 500, 0)
                ) {
                    int key1 = primaryKey(cache1);

                    log.info(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode() +
                        ", tx=" + tx + ", key=" + key1 + ", cache=" + cache1.getName() + ']');

                    cache1.put(key1, 0);

                    barrier.await();

                    int key2 = primaryKey(cache2);

                    log.info(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode() +
                        ", tx=" + tx + ", key=" + key2 + ", cache=" + cache2.getName() + ']');

                    cache2.put(key2, 1);

                    tx.commit();
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

        fut.get();

        assertTrue(deadlock.get());

        for (int i = 0; i < NODES_CNT ; i++) {
            Ignite ignite = ignite(i);

            IgniteTxManager txMgr = ((IgniteKernal)ignite).context().cache().context().tm();

            ConcurrentMap<Long, TxDeadlockDetection.TxDeadlockFuture> futs =
                GridTestUtils.getFieldValue(txMgr, IgniteTxManager.class, "deadlockDetectFuts");

            assertTrue(futs.isEmpty());
        }
    }
}
