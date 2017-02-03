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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
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
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
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

        TcpCommunicationSpi commSpi = new TestCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

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

        startGridsMultiThreaded(NODES_CNT);
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
                log.info(">>> Iteration " + i);

                final AtomicInteger threadCnt = new AtomicInteger();

                IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
                    @Override public void run() {
                        int threadNum = threadCnt.getAndIncrement();

                        Ignite ignite = ignite(threadNum % NODES_CNT);

                        IgniteCache<Integer, Integer> cache = ignite.cache(CACHE);

                        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 700, 0)) {
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

            checkDetectionFuts();
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

        final long timeout = 500;

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

                    barrier.await(timeout + 1000, TimeUnit.MILLISECONDS);

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

        checkDetectionFuts();
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

            final long timeout = 500;

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

                            U.sleep(timeout * 3);
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

            checkDetectionFuts();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailedTxLocksRequest() throws Exception {
        doTestFailedMessage(TxLocksRequest.class);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailedTxLocksResponse() throws Exception {
        doTestFailedMessage(TxLocksResponse.class);
    }

    /**
     * @param failCls Failing message class.
     * @throws Exception If failed.
     */
    private void doTestFailedMessage(Class failCls) throws Exception {
        try {
            final int txCnt = 2;

            final CyclicBarrier barrier = new CyclicBarrier(txCnt);

            final AtomicInteger threadCnt = new AtomicInteger();

            final AtomicBoolean deadlock = new AtomicBoolean();

            final AtomicBoolean timeout = new AtomicBoolean();

            TestCommunicationSpi.failCls = failCls;

            IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
                @Override public void run() {
                    int num = threadCnt.getAndIncrement();

                    Ignite ignite = ignite(num);

                    IgniteCache<Object, Integer> cache = ignite.cache(CACHE);

                    try (Transaction tx =
                             ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, num == 0 ? 500 : 1500, 0)
                    ) {
                        int key1 = primaryKey(ignite((num + 1) % txCnt).cache(CACHE));

                        log.info(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode() +
                            ", tx=" + tx + ", key=" + key1 + ']');

                        cache.put(new TestKey(key1), 1);

                        barrier.await();

                        int key2 = primaryKey(cache);

                        log.info(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode() +
                            ", tx=" + tx + ", key=" + key2 + ']');

                        cache.put(new TestKey(key2), 2);

                        tx.commit();
                    }
                    catch (Exception e) {
                        timeout.compareAndSet(false, hasCause(e, TransactionTimeoutException.class));

                        deadlock.compareAndSet(false, hasCause(e, TransactionDeadlockException.class));
                    }
                }
            }, txCnt, "tx-thread");

            fut.get();

            assertFalse(deadlock.get());

            assertTrue(timeout.get());

            checkDetectionFuts();
        }
        finally {
            TestCommunicationSpi.failCls = null;
            TestKey.failSer = false;
        }
    }

    /**
     *
     */
    private void checkDetectionFuts() {
        for (int i = 0; i < NODES_CNT ; i++) {
            Ignite ignite = ignite(i);

            IgniteTxManager txMgr = ((IgniteKernal)ignite).context().cache().context().tm();

            Collection<IgniteInternalFuture<?>> futs = txMgr.deadlockDetectionFutures();

            assertTrue(futs.isEmpty());
        }
    }

    /**
     *
     */
    private static class TestKey implements Externalizable {
        /** Fail request. */
        private static volatile boolean failSer = false;

        /** Id. */
        private int id;

        /**
         * Default constructor (required by Externalizable).
         */
        public TestKey() {
            // No-op.
        }

        /**
         * @param id Id.
         */
        public TestKey(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(id);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            if (failSer) {
                TestCommunicationSpi.failCls = null;
                failSer = false;

                throw new IOException();
            }

            id = in.readInt();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            TestKey key = (TestKey)o;

            return id == key.id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** Fail response. */
        private static volatile Class failCls;

        /** {@inheritDoc} */
        @Override public void sendMessage(
            ClusterNode node,
            Message msg,
            IgniteInClosure<IgniteException> ackC
        ) throws IgniteSpiException {
            if (failCls != null && msg instanceof GridIoMessage &&
                ((GridIoMessage)msg).message().getClass() == failCls)
                TestKey.failSer = true;

            super.sendMessage(node, msg, ackC);
        }
    }
}
