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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionMetrics;
import org.apache.ignite.transactions.TransactionTimeoutException;

import static org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion.NONE;
import static org.apache.ignite.internal.util.typedef.X.hasCause;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Transactional cache metrics test.
 */
public abstract class GridCacheTransactionalAbstractMetricsSelfTest extends GridCacheAbstractMetricsSelfTest {
    /** */
    private static final int TX_CNT = 3;

    /** */
    private static final long DELAY_MILLIS = 30L;

    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Max await deadlock detection - 10 seconds. */
    private static final long MAX_AWAIT = 10_000L;

    /** Startup node in client mode flag. */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(igniteInstanceName);

        configuration.setClientMode(client);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        if (isDebug()) {
            discoSpi.failureDetectionTimeoutEnabled(false);

            configuration.setDiscoverySpi(discoSpi);
        }

        CommunicationSpi commSpi = new TestCommunicationSpi();

        configuration.setCommunicationSpi(commSpi);
        configuration.setClientMode(client);
        configuration.setDiscoverySpi(discoSpi);

        return configuration;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        client = true;

        for (int i = gridCount(); i < gridCount() * 2; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        int gridsCnt = gridCount();

        // Reset statistics on client nodes.
        for (int i = gridsCnt; i < gridsCnt * 2; i++) {
            Ignite g = grid(i);

            g.transactions().resetMetrics();

            g.cache(DEFAULT_CACHE_NAME).localMxBean().clear();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticReadCommittedCommits() throws Exception {
        testCommits(OPTIMISTIC, READ_COMMITTED, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticReadCommittedCommitsNoData() throws Exception {
        testCommits(OPTIMISTIC, READ_COMMITTED, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticRepeatableReadCommits() throws Exception {
        testCommits(OPTIMISTIC, REPEATABLE_READ, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticRepeatableReadCommitsNoData() throws Exception {
        testCommits(OPTIMISTIC, REPEATABLE_READ, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticSerializableCommits() throws Exception {
        testCommits(OPTIMISTIC, SERIALIZABLE, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticSerializableCommitsNoData() throws Exception {
        testCommits(OPTIMISTIC, SERIALIZABLE, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticReadCommittedCommits() throws Exception {
        testCommits(PESSIMISTIC, READ_COMMITTED, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticReadCommittedCommitsNoData() throws Exception {
        testCommits(PESSIMISTIC, READ_COMMITTED, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticRepeatableReadCommits() throws Exception {
        testCommits(PESSIMISTIC, REPEATABLE_READ, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticRepeatableReadCommitsNoData() throws Exception {
        testCommits(PESSIMISTIC, REPEATABLE_READ, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticSerializableCommits() throws Exception {
        testCommits(PESSIMISTIC, SERIALIZABLE, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticSerializableCommitsNoData() throws Exception {
        testCommits(PESSIMISTIC, SERIALIZABLE, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticReadCommittedRollbacks() throws Exception {
        testRollbacks(OPTIMISTIC, READ_COMMITTED, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticReadCommittedRollbacksNoData() throws Exception {
        testRollbacks(OPTIMISTIC, READ_COMMITTED, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticRepeatableReadRollbacks() throws Exception {
        testRollbacks(OPTIMISTIC, REPEATABLE_READ, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticRepeatableReadRollbacksNoData() throws Exception {
        testRollbacks(OPTIMISTIC, REPEATABLE_READ, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticSerializableRollbacks() throws Exception {
        testRollbacks(OPTIMISTIC, SERIALIZABLE, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticSerializableRollbacksNoData() throws Exception {
        testRollbacks(OPTIMISTIC, SERIALIZABLE, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticReadCommittedRollbacks() throws Exception {
        testRollbacks(PESSIMISTIC, READ_COMMITTED, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticReadCommittedRollbacksNoData() throws Exception {
        testRollbacks(PESSIMISTIC, READ_COMMITTED, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticRepeatableReadRollbacks() throws Exception {
        testRollbacks(PESSIMISTIC, REPEATABLE_READ, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticRepeatableReadRollbacksNoData() throws Exception {
        testRollbacks(PESSIMISTIC, REPEATABLE_READ, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticSerializableRollbacks() throws Exception {
        testRollbacks(PESSIMISTIC, SERIALIZABLE, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticSerializableRollbacksNoData() throws Exception {
        testRollbacks(PESSIMISTIC, SERIALIZABLE, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticReadCommittedTimeouts() throws Exception {
        testRollbacksDueToTimeout(OPTIMISTIC, READ_COMMITTED, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticReadCommittedTimeoutsNoData() throws Exception {
        testRollbacksDueToTimeout(OPTIMISTIC, READ_COMMITTED, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticRepeatableReadTimeouts() throws Exception {
        testRollbacksDueToTimeout(OPTIMISTIC, REPEATABLE_READ, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticRepeatableReadTimeoutsNoData() throws Exception {
        testRollbacksDueToTimeout(OPTIMISTIC, REPEATABLE_READ, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticSerializableTimeouts() throws Exception {
        testRollbacksDueToTimeout(OPTIMISTIC, SERIALIZABLE, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticSerializableTimeoutsNoData() throws Exception {
        testRollbacksDueToTimeout(OPTIMISTIC, SERIALIZABLE, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticReadCommittedTimeouts() throws Exception {
        testRollbacksDueToTimeout(PESSIMISTIC, READ_COMMITTED, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticReadCommittedTimeoutsNoData() throws Exception {
        testRollbacksDueToTimeout(PESSIMISTIC, READ_COMMITTED, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticRepeatableReadTimeouts() throws Exception {
        testRollbacksDueToTimeout(PESSIMISTIC, REPEATABLE_READ, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticRepeatableReadTimeoutsNoData() throws Exception {
        testRollbacksDueToTimeout(PESSIMISTIC, REPEATABLE_READ, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticSerializableTimeoutsNoData() throws Exception {
        testRollbacksDueToTimeout(PESSIMISTIC, SERIALIZABLE, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticSerializableTimeouts() throws Exception {
        testRollbacksDueToTimeout(PESSIMISTIC, SERIALIZABLE, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticDeadlocks() throws Exception {
        testRollbacksOnDeadlock(TransactionConcurrency.PESSIMISTIC, false);

    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticDeadlocks() throws Exception {
        testRollbacksOnDeadlock(TransactionConcurrency.OPTIMISTIC, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticDeadlocksOnClient() throws Exception {
        testRollbacksOnDeadlock(TransactionConcurrency.PESSIMISTIC, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticDeadlocksOnClient() throws Exception {
        testRollbacksOnDeadlock(TransactionConcurrency.OPTIMISTIC, true);
    }

    /**
     * @param concurrency Concurrency control.
     * @param isolation Isolation level.
     * @param put Put some data if {@code true}.
     * @throws Exception If failed.
     */
    private void testCommits(TransactionConcurrency concurrency, TransactionIsolation isolation, boolean put)
        throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < TX_CNT; i++) {
            Transaction tx = grid(0).transactions().txStart(concurrency, isolation);

            if (put)
                for (int j = 0; j < keyCount(); j++)
                    cache.put(j, j);

            // Waiting 30 ms for metrics. U.currentTimeMillis() method has 10 ms discretization.
            U.sleep(DELAY_MILLIS);

            tx.commit();
        }

        for (int i = 0; i < gridCount(); i++) {
            TransactionMetrics metrics = grid(i).transactions().metrics();
            CacheMetrics cacheMetrics = grid(i).cache(DEFAULT_CACHE_NAME).localMetrics();

            if (i == 0) {
                assertEquals(TX_CNT, metrics.txCommits());

                if (put) {
                    assertEquals(TX_CNT, cacheMetrics.getCacheTxCommits());
                    assert cacheMetrics.getAverageTxCommitTime() > 0;
                }
            }
            else {
                assertEquals(0, metrics.txCommits());
                assertEquals(0, cacheMetrics.getCacheTxCommits());
            }

            assertEquals(0, metrics.txRollbacks());
            assertEquals(0, cacheMetrics.getCacheTxRollbacks());
        }
    }

    /**
     * @param concurrency Concurrency control.
     * @param isolation Isolation level.
     * @param put Put some data if {@code true}.
     * @throws Exception If failed.
     */
    private void testRollbacks(TransactionConcurrency concurrency, TransactionIsolation isolation,
        boolean put) throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < TX_CNT; i++) {
            Transaction tx = grid(0).transactions().txStart(concurrency, isolation);

            if (put)
                for (int j = 0; j < keyCount(); j++)
                    cache.put(j, j);

            // Waiting 30 ms for metrics. U.currentTimeMillis() method has 10 ms discretization.
            U.sleep(DELAY_MILLIS);

            tx.rollback();
        }

        for (int i = 0; i < gridCount(); i++) {
            TransactionMetrics metrics = grid(i).transactions().metrics();
            CacheMetrics cacheMetrics = grid(i).cache(DEFAULT_CACHE_NAME).localMetrics();

            assertEquals(0, metrics.txCommits());
            assertEquals(0, cacheMetrics.getCacheTxCommits());

            if (i == 0) {
                assertEquals(TX_CNT, metrics.txRollbacks());

                if (put) {
                    assertEquals(TX_CNT, cacheMetrics.getCacheTxRollbacks());
                    assert cacheMetrics.getAverageTxRollbackTime() > 0;
                }
            }
            else {
                assertEquals(0, metrics.txRollbacks());
                assertEquals(0, cacheMetrics.getCacheTxRollbacks());
            }
        }
    }

    /**
     * @param concurrency Concurrency control.
     * @param isolation Isolation level.
     * @param put Put some data if {@code true}.
     * @throws Exception If failed.
     */
    private void testRollbacksDueToTimeout(TransactionConcurrency concurrency, TransactionIsolation isolation,
        boolean put) throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < TX_CNT; i++) {
            try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation, 150, keyCount())) {
                if (put) {
                    for (int j = 0; j < keyCount(); j++)
                        cache.put(j, j);
                }

                U.sleep(300);

                tx.commit();
            }
            catch (CacheException | TransactionTimeoutException ignore) {
                assertTrue(X.hasCause(ignore, TransactionTimeoutException.class));
            }
        }

        for (int i = 0; i < gridCount(); i++) {
            TransactionMetrics metrics = grid(i).transactions().metrics();
            CacheMetrics cacheMetrics = grid(i).cache(DEFAULT_CACHE_NAME).localMetrics();

            assertEquals(0, metrics.txCommits());
            assertEquals(0, cacheMetrics.getCacheTxCommits());

            if (i == 0) {
                assertEquals(TX_CNT, metrics.txRollbacks());
                assertEquals(TX_CNT, metrics.txRollbacksOnTimeout());

                if (put) {
                    assertEquals(TX_CNT, cacheMetrics.getCacheTxRollbacks());
                    assertEquals(TX_CNT, cacheMetrics.getCacheTxRollbacksOnTimeout());
                }
            }
            else {
                assertEquals(0, metrics.txRollbacks());
                assertEquals(0, metrics.txRollbacksOnTimeout());
                assertEquals(0, cacheMetrics.getCacheTxRollbacks());
            }
        }
    }

    /**
     * @param concurrency Transaction concurrency level.
     * @param clientTx Initiate transactions on client node.
     * @throws Exception If failed.
     */
    private void testRollbacksOnDeadlock(final TransactionConcurrency concurrency,
        final boolean clientTx) throws Exception {

        final AtomicInteger threadCnt = new AtomicInteger();

        final boolean loc = cacheMode() == CacheMode.LOCAL;

        final int txCnt = loc ? 2 : gridCount();

        final CyclicBarrier barrier = new CyclicBarrier(txCnt);

        final List<List<Integer>> keySets = generateKeys(txCnt, loc);

        final AtomicInteger txDeadlockErrCnt = new AtomicInteger();

        if (concurrency == TransactionConcurrency.OPTIMISTIC) {
            // Transactions will be sync using discovery spi (for optimistic tx deadlock).
            ((TestCommunicationSpi)grid(0).configuration().getCommunicationSpi()).syncTxOnPrepare(txCnt);
        }

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                int threadIdx = threadCnt.getAndIncrement();

                Ignite ignite = loc ? ignite(0) : ignite(threadIdx + (clientTx ? txCnt : 0));

                IgniteCache<Object, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

                List<Integer> keys = keySets.get(threadIdx);

                int txTimeout = 1000 + txCnt * 200;

                try (Transaction tx = ignite.transactions().txStart(concurrency, REPEATABLE_READ, txTimeout, 0)) {
                    Integer key = keys.get(0);

                    cache.put(key, 0);

                    // Sync transactions at this point (for pessimistic tx deadlock).
                    U.awaitQuiet(barrier);

                    key = keys.get(1);

                    ClusterNode primaryNode =
                        ((IgniteCacheProxy)cache).context().affinity().primaryByKey(key, NONE);

                    List<Integer> primaryKeys = primaryKeys(
                        grid(primaryNode).cache(DEFAULT_CACHE_NAME), 5, key + (100 * (threadIdx + 1)));

                    Map<Object, Integer> entries = new LinkedHashMap<>();

                    for (Integer k : primaryKeys) {
                        entries.put(k, 1);
                        entries.put(k + 13, 2);
                    }

                    entries.put(key, 0);

                    cache.putAll(entries);

                    tx.commit();
                }
                catch (RuntimeException e) {
                    if (hasCause(e, TransactionTimeoutException.class) &&
                        hasCause(e, TransactionDeadlockException.class))
                        txDeadlockErrCnt.incrementAndGet();

                    throw e;
                }
            }
        }, txCnt, "tx-thread");

        try {
            fut.get(MAX_AWAIT);
        }
        catch (IgniteCheckedException ignore) {
            // Exception will be thrown after all futures will be completed.
        }

        awaitMetricsUpdate();

        int txRollbacksDueToDeadlock = 0;

        if (loc)
            txRollbacksDueToDeadlock = grid(clientTx ? 1 : 0).transactions().metrics().txRollbackOnDeadlock();
        else {
            for (int i = 0; i < txCnt; i++)
                txRollbacksDueToDeadlock += grid(i + (clientTx ? txCnt : 0)).transactions().metrics().txRollbackOnDeadlock();

            // At least one transaction was rolled back due to deadlock on non LOCAL cache.
            assertTrue("No rollbacks due to deadlock detected.", txRollbacksDueToDeadlock > 0);
        }

        // Workaround for LOCAL cache - no warranty that deadlock happened.
        assertEquals(txDeadlockErrCnt.intValue(), txRollbacksDueToDeadlock);

        // On client node commits/rollbacks doesn't counts in cache metrics.
        if (!clientTx) {
            long cacheRollbacksDueToDeadlock = grid(0).cache(DEFAULT_CACHE_NAME).metrics().getCacheTxRollbacksOnDeadlock();

            assertEquals(txRollbacksDueToDeadlock, cacheRollbacksDueToDeadlock);
        }

    }

    /**
     * @param nodesCnt Nodes count.
     * @param loc Generate keys for {@code LOCAL} cache.
     */
    private List<List<Integer>> generateKeys(int nodesCnt, boolean loc) throws IgniteCheckedException {
        List<List<Integer>> keySets = new ArrayList<>();

        if (loc) {
            List<Integer> keys = primaryKeys(ignite(0).cache(DEFAULT_CACHE_NAME), 2);

            keySets.add(new ArrayList<>(keys));

            Collections.reverse(keys);

            keySets.add(keys);
        }
        else {
            for (int i = 0; i < nodesCnt; i++) {
                List<Integer> keys = new ArrayList<>(2);

                keys.add(primaryKey(ignite(i).cache(DEFAULT_CACHE_NAME)));
                keys.add(primaryKey(ignite((i + 1) % nodesCnt).cache(DEFAULT_CACHE_NAME)));

                keySets.add(keys);
            }
        }

        return keySets;
    }

    /**
     * Wait for {@link EventType#EVT_NODE_METRICS_UPDATED} event will be received.
     */
    private void awaitMetricsUpdate() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch((gridCount() + 2) * 2);

        IgnitePredicate<Event> lsnr = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event ignore) {
                latch.countDown();

                return true;
            }
        };

        for (int i = 0; i < gridCount(); i++)
            grid(i).events().localListen(lsnr, EventType.EVT_NODE_METRICS_UPDATED);

        latch.await();
    }

    /** */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** Tx count. */
        private static volatile int TX_CNT;

        /** Tx ids. */
        private static final Set<GridCacheVersion> TX_IDS = new GridConcurrentHashSet<>();

        /**
         * @param txCnt Tx count.
         */
        private void syncTxOnPrepare(int txCnt) {
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

                    if (TX_IDS.contains(txId) && TX_IDS.size() < TX_CNT) {
                        GridTestUtils.runAsync(new Callable<Void>() {
                            @Override public Void call() throws Exception {
                                while (TX_IDS.size() < TX_CNT) {
                                    try {
                                        U.sleep(50);
                                    }
                                    catch (IgniteInterruptedCheckedException e) {
                                        e.printStackTrace();
                                    }
                                }

                                TestCommunicationSpi.super.sendMessage(node, msg, ackC);

                                return null;
                            }
                        });

                        return;
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
