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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
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
    private static final int NODES_CNT = 3;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

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
        doTestDeadlock();
    }

    /**
     * @throws Exception If failed.
     */
    private boolean doTestDeadlock() throws Exception {
        TestCommunicationSpi.init(log, 2);

        final AtomicInteger threadCnt = new AtomicInteger();

        final AtomicBoolean deadlock = new AtomicBoolean();

        final AtomicInteger commitCnt = new AtomicInteger();

        awaitPartitionMapExchange();

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                int threadNum = threadCnt.getAndIncrement();

                Ignite ignite = ignite(0);

                IgniteCache<Integer, Integer> cache1 = cacheForThread(ignite, threadNum, 0);
                IgniteCache<Integer, Integer> cache2 = cacheForThread(ignite, threadNum, 1);

                try (Transaction tx =
                         ignite.transactions().txStart(OPTIMISTIC, REPEATABLE_READ, 5000, 0)
                ) {
                    int key1 = remoteKeyForThread(threadNum, 0);

                    log.info(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode() +
                        ", tx=" + tx + ", key=" + key1 + ", cache=" + cache1.getName() + ']');

                    cache1.put(key1, 0);

                    int key2 = remoteKeyForThread(threadNum, 1);

                    log.info(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode() +
                        ", tx=" + tx + ", key=" + key2 + ", cache=" + cache2.getName() + ']');

                    cache2.put(key2, 1);

                    tx.commit();

                    commitCnt.incrementAndGet();
                }
                catch (Throwable e) {
                    log.error("Got exception", e);

                    // At least one stack trace should contain TransactionDeadlockException.
                    if (hasCause(e, TransactionTimeoutException.class) &&
                        hasCause(e, TransactionDeadlockException.class)
                        ) {
                        if (deadlock.compareAndSet(false, true))
                            log.info("Successfully set deadlock flag");
                        else
                            log.info("Deadlock flag already set");
                    }
                    else
                        log.info("Unexpected exception");
                }
            }
        }, 2, "tx-thread");

        fut.get();

        assertFalse("Commits must fail", commitCnt.get() == 2);

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
     * @param ignite Ignite.
     * @param threadNum Thread number.
     * @param cacheNum Cache number.
     */
    private IgniteCache<Integer, Integer> cacheForThread(Ignite ignite, int threadNum, int cacheNum) {
        return ignite.cache("cache" + (threadNum == cacheNum ? 0 : 1));
    }

    /**
     * @param threadNum Thread number.
     * @param keyNum Key number.
     */
    private int remoteKeyForThread(int threadNum, int keyNum) throws IgniteCheckedException {
        Ignite ignite = threadNum == keyNum ? grid(1) : grid(2);

        return primaryKey(cacheForThread(ignite, threadNum, keyNum));
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** Tx count. */
        private static volatile int TX_CNT;

        /** Tx ids. */
        private static final Set<GridCacheVersion> TX_IDS = new GridConcurrentHashSet<>();

        /** Logger. */
        private static volatile IgniteLogger log;

        /**
         * @param txCnt Tx count.
         */
        private static void init(IgniteLogger log, int txCnt) {
            TX_CNT = txCnt;
            TX_IDS.clear();
            TestCommunicationSpi.log = log;
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

                    log.info("Request for tx: " + txId);

                    if (TX_IDS.contains(txId)) {
                        log.info("Start waiting for tx: " + txId);

                        while (TX_IDS.size() < TX_CNT) {
                            try {
                                U.sleep(50);
                            }
                            catch (IgniteInterruptedCheckedException e) {
                                e.printStackTrace();
                            }
                        }

                        log.info("Finish waiting for tx: " + txId);
                    }
                }
                else if (msg0 instanceof GridNearTxPrepareResponse) {
                    GridNearTxPrepareResponse res = (GridNearTxPrepareResponse)msg0;

                    GridCacheVersion txId = res.version();

                    log.info("Response for tx: " + txId);

                    TX_IDS.add(txId);
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}
