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

package org.apache.ignite.internal;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicDeferredUpdateResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.persistence.DatabaseLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Test exchange manager warnings.
 */
public class GridCachePartitionExchangeManagerWarningsTest extends GridCommonAbstractTest {
    /** Long running operations timeout. */
    private static final String LONG_OPERATIONS_DUMP_TIMEOUT = "1000";

    /** Atomic cache name. */
    private static final String CACHE_NAME = "TEST_CACHE";

    /** Transactional cache name. */
    private static final String TX_CACHE_NAME = "TX_TEST_CACHE";

    /** Lifecycle bean. */
    private LifecycleBean lifecycleBean;

    /** */
    private String oldLongOpsDumpTimeout;

    /** */
    private ListeningTestLogger testLog;

    /** */
    private volatile Supplier<TcpCommunicationSpi> spiFactory = CustomTcpCommunicationSpi::new;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        oldLongOpsDumpTimeout = System.getProperty(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        if (oldLongOpsDumpTimeout != null)
            System.setProperty(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, oldLongOpsDumpTimeout);
        else
            System.clearProperty(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (testLog != null)
            testLog.clearListeners();

        spiFactory = CustomTcpCommunicationSpi::new;

        testLog = null;

        lifecycleBean = null;

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(spiFactory.get());

        if (testLog != null)
            cfg.setGridLogger(testLog);

        cfg.setLifecycleBeans(lifecycleBean);

        CacheConfiguration atomicCfg = new CacheConfiguration(CACHE_NAME)
            .setBackups(1)
            .setAtomicityMode(ATOMIC);
        CacheConfiguration txCfg = new CacheConfiguration(TX_CACHE_NAME)
            .setBackups(1)
            .setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(atomicCfg, txCfg);

        return cfg;
    }

    @Test
    public void testSingleMessageErrorWarnings () throws Exception {
        String expectedLogMsg = "Failed to send local partitions";

        LogListener logListener = LogListener.matches(expectedLogMsg).build();
        testLog = new ListeningTestLogger(log, logListener);

        IgniteConfiguration cfg0 = getConfiguration(getTestIgniteInstanceName(0));
        TestRecordingCommunicationSpi spi0 = new TestRecordingCommunicationSpi();
        cfg0.setCommunicationSpi(spi0);
        cfg0.setGridLogger(testLog);
        startGrid(cfg0);

        IgniteConfiguration cfg1 = getConfiguration(getTestIgniteInstanceName(1));
        TestRecordingCommunicationSpi spi1 = new TestRecordingCommunicationSpi();
        cfg1.setCommunicationSpi(spi1);
        cfg1.setGridLogger(testLog);

        IgniteEx node1 = startGrid(cfg1);

        node1.cluster().state(ClusterState.ACTIVE);
        awaitPartitionMapExchange();

        spi1.blockMessages((node, msg) -> msg instanceof GridDhtPartitionsSingleMessage);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            node1.context().cache().context().exchange().refreshPartitions();
        });

        assertTrue("Message not blocked", spi1.waitForBlocked(1, 5_000));

        stopGrid(0);

        spi1.stopBlock();

        fut.get(5_000);

        U.sleep(500);

        assertTrue("Expected log not found",
                GridTestUtils.waitForCondition(logListener::check, 3000));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, value = LONG_OPERATIONS_DUMP_TIMEOUT)
    public void testLongRunningCacheFutures() throws Exception {
        long timeout = Long.parseLong(LONG_OPERATIONS_DUMP_TIMEOUT);
        int longRunFuturesCnt = 1000;
        String logSubstr = "future";

        LogListener minWarnings = LogListener.matches(logSubstr).atLeast(1).build();
        LogListener maxWarnings = LogListener.matches(logSubstr).atMost(longRunFuturesCnt - 1).build();

        testLog = new ListeningTestLogger(log, minWarnings, maxWarnings);

        startGrids(2);

        Ignite client = startClientGrid(3);
        try (IgniteDataStreamer<Integer, Integer> streamer = client.dataStreamer(CACHE_NAME)) {
            streamer.allowOverwrite(true);

            for (int i = 0; i < longRunFuturesCnt; i++)
                streamer.addData(i, i);
        }

        doSleep(timeout * 2);

        stopAllGrids();

        assertTrue("Warnings were not found", minWarnings.check());
        assertTrue("Too much warnings in the logs", maxWarnings.check());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, value = LONG_OPERATIONS_DUMP_TIMEOUT)
    public void testLongRunningTransactions() throws Exception {
        long timeout = Long.parseLong(LONG_OPERATIONS_DUMP_TIMEOUT);
        int transactions = 100;
        String logSubstr = "transaction";

        LogListener minWarnings = LogListener.matches(logSubstr).atLeast(1).build();
        LogListener maxWarnings = LogListener.matches(logSubstr).atMost(transactions - 1).build();

        testLog = new ListeningTestLogger(log, minWarnings, maxWarnings);

        ExecutorService excSvc = Executors.newFixedThreadPool(transactions);

        try (Ignite srv1 = startGrid(0)) {
            CountDownLatch txStarted = new CountDownLatch(transactions);

            CountDownLatch stopTx = new CountDownLatch(1);

            for (int i = 0; i < transactions; i++)
                excSvc.submit(new AsyncTransaction(srv1, TX_CACHE_NAME, i, txStarted, stopTx));

            if (!txStarted.await(10_000, TimeUnit.MILLISECONDS))
                fail("Unable to start transactions");

            doSleep(timeout * 2);

            stopTx.countDown();
        }
        finally {
            excSvc.shutdown();

            if (!excSvc.awaitTermination(10_000, TimeUnit.MILLISECONDS))
                fail("Unable to wait for thread pool termination.");
        }

        assertTrue("Warnings were not found", minWarnings.check());
        assertTrue("Too much warnings in the logs", maxWarnings.check());
    }

    /** */
    @Test
    public void testDumpLongRunningOperationsWaitForFullyInitializedExchangeManager() throws Exception {
        long waitingTimeout = 5_000;

        PrintStream errStream = System.err;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch waitLatch = new CountDownLatch(1);

        try {
            // GridCachePartitionExchangeManager#dumpLongRunningOperations() uses diagnostic log,
            // which can be non-initialized, and so, error messgaes are logged into standard error output stream.
            ByteArrayOutputStream testOut = new ByteArrayOutputStream(16 * 1024);
            System.setErr(new PrintStream(testOut));

            AtomicReference<IgniteInternalFuture<?>> dumpOpsFut = new AtomicReference<>();
            IgniteInternalFuture<Ignite> startFut = null;

            // Lyficycle bean allows to register DatabaseLifecycleListener and trigger dumpLongRunningOperations
            // before GridCachePartitionExchangeManager is started.
            lifecycleBean = new LifecycleBean() {
                /** Ignite instance. */
                @IgniteInstanceResource
                IgniteEx ignite;

                /** {@inheritDoc} */
                @Override public void onLifecycleEvent(LifecycleEventType evt) throws IgniteException {
                    if (evt == LifecycleEventType.BEFORE_NODE_START) {
                        ignite.context().internalSubscriptionProcessor()
                            .registerDatabaseListener(new DatabaseLifecycleListener() {
                                @Override public void onInitDataRegions(
                                    IgniteCacheDatabaseSharedManager mgr
                                ) throws IgniteCheckedException {
                                    dumpOpsFut.set(
                                        GridTestUtils.runAsync(
                                            () -> ignite.context().cache().context().exchange().dumpLongRunningOperations(1_000)));

                                    // Let's allow to check that dumpLongRunningOperations is triggered.
                                    startLatch.countDown();

                                    // Wait for the check
                                    try {
                                        if (!waitLatch.await(waitingTimeout * 3, TimeUnit.MILLISECONDS))
                                            throw new IgniteCheckedException("Failed to wait for a check of dumpLongRunningOperations");
                                    }
                                    catch (InterruptedException e) {
                                        throw new IgniteCheckedException(e);
                                    }
                                }
                            });
                    }
                }
            };

            startFut = GridTestUtils.runAsync(new Callable<Ignite>() {
                @Override public Ignite call() throws Exception {
                    return startGrid(0);
                }
            });

            assertTrue("Server node did not start in " + waitingTimeout + " ms.",
                startLatch.await(waitingTimeout, TimeUnit.MILLISECONDS));

            // Check that dumpLongRunningOperations did not produce any error.
            if (GridTestUtils.waitForCondition(() -> dumpOpsFut.get().isDone(), waitingTimeout)) {
                // Check that error output stream does not contain NullPointerException.
                String output = testOut.toString();

                assertTrue("Unexpected error [err=" + output + ']', output.isEmpty());
            }

            // Unblock starting the node.
            waitLatch.countDown();

            assertTrue(
                "Dumping log running operations is not completed yet.",
                GridTestUtils.waitForCondition(() -> dumpOpsFut.get().isDone(), waitingTimeout));

            // Check that error output stream does not contain any error.
            String output = testOut.toString();

            assertTrue("Unexpected error [err=" + output + ']', output.isEmpty());

            startFut.get(waitingTimeout, TimeUnit.MILLISECONDS);
        }
        finally {
            startLatch.countDown();
            waitLatch.countDown();

            System.setErr(errStream);
        }
    }

    /**
     * Async tx runnable.
     */
    private static class AsyncTransaction implements Runnable {
        /** */
        private final Ignite ignite;

        /** */
        private final String cacheName;

        /** */
        private final Integer key;

        /** */
        private final CountDownLatch startLatch;

        /** */
        private final CountDownLatch canStopLatch;

        /**
         * @param ignite Ignite instance.
         * @param cacheName Cache name.
         * @param key Cache key.
         * @param startLatch Latch to synchronize all transactions.
         * @param canStopLatch Latch to finish transaction.
         */
        public AsyncTransaction(Ignite ignite, String cacheName, Integer key, CountDownLatch startLatch, CountDownLatch canStopLatch) {
            this.ignite = ignite;
            this.cacheName = cacheName;
            this.key = key;
            this.startLatch = startLatch;
            this.canStopLatch = canStopLatch;
        }

        /**
         * Start transaction.
         */
        @Override public void run() {
            IgniteTransactions transactions = ignite.transactions();

            try (Transaction tx = transactions.txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE)) {
                IgniteCache<Integer, Integer> cache = ignite.cache(cacheName);

                Integer val = cache.get(key);

                startLatch.countDown();

                cache.put(key, val == null ? 1 : val + 1);

                try {
                    canStopLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();

                    Thread.currentThread().interrupt();
                }

                tx.rollback();
            }
        }
    }

    /**
     * Custom communication SPI for simulating long running cache futures.
     */
    private static class CustomTcpCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        public CustomTcpCommunicationSpi() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg,
            IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                // Skip response from backup node.
                if (((GridIoMessage)msg).message() instanceof GridDhtAtomicDeferredUpdateResponse)
                    return;
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}
