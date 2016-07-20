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
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionTimeoutException;

import static org.apache.ignite.internal.util.typedef.X.hasCause;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class TxOptimisticDeadlockDetectionCrossCacheTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 2;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (isDebug()) {
            TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

            discoSpi.failureDetectionTimeoutEnabled(false);

            cfg.setDiscoverySpi(discoSpi);
        }

        TcpCommunicationSpi commSpi = new TestCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

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
        // Sometimes boh transactions perform commit, so we repeat attempt.
        while (!doTestDeadlock()) {}
    }

    /**
     * @throws Exception If failed.
     */
    private boolean doTestDeadlock() throws Exception {
        TestCommunicationSpi.init(2);

        final CyclicBarrier barrier = new CyclicBarrier(2);

        final AtomicInteger threadCnt = new AtomicInteger();

        final AtomicBoolean deadlock = new AtomicBoolean();

        final AtomicInteger commitCnt = new AtomicInteger();

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                int threadNum = threadCnt.getAndIncrement();

                Ignite ignite = ignite(0);

                IgniteCache<Integer, Integer> cache1 = ignite.cache("cache" + (threadNum == 0 ? 0 : 1));

                IgniteCache<Integer, Integer> cache2 = ignite.cache("cache" + (threadNum == 0 ? 1 : 0));

                try (Transaction tx =
                         ignite.transactions().txStart(OPTIMISTIC, REPEATABLE_READ, 500, 0)
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

                    commitCnt.incrementAndGet();
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

        if (commitCnt.get() == 2)
            return false;

        assertTrue(deadlock.get());

        for (int i = 0; i < NODES_CNT ; i++) {
            Ignite ignite = ignite(i);

            IgniteTxManager txMgr = ((IgniteKernal)ignite).context().cache().context().tm();

            Collection<IgniteInternalFuture<?>> futs = txMgr.deadlockDetectionFutures();

            assertTrue(futs.isEmpty());
        }

        return true;
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** Tx count. */
        private static volatile int TX_CNT;

        /** Tx ids. */
        private static final Set<GridCacheVersion> TX_IDS = new GridConcurrentHashSet<>();

        /**
         * @param txCnt Tx count.
         */
        private static void init(int txCnt) {
            TX_CNT = txCnt;
            TX_IDS.clear();
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(
            final ClusterNode node,
            final Message msg,
            final IgniteInClosure<IgniteException> ackC
        ) throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                Message msg0 = ((GridIoMessage)msg).message();

                if (msg0 instanceof GridNearTxPrepareRequest) {
                    final GridNearTxPrepareRequest req = (GridNearTxPrepareRequest)msg0;

                    GridCacheVersion txId = req.version();

                    if (TX_IDS.contains(txId)) {
                        while (TX_IDS.size() < TX_CNT) {
                            try {
                                U.sleep(50);
                            }
                            catch (IgniteInterruptedCheckedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
                else if (msg0 instanceof GridNearTxPrepareResponse) {
                    GridNearTxPrepareResponse res = (GridNearTxPrepareResponse)msg0;

                    GridCacheVersion txId = res.version();

                    TX_IDS.add(txId);
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }

}
