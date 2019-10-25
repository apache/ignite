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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicDeferredUpdateResponse;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT;

/**
 * Test exchange manager warnings.
 */
public class GridCachePartitionExchangeManagerWarningsTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME = "TEST_CACHE";

    /** */
    private String oldLongOpsDumpTimeout;

    /** */
    private CustomTestLogger testLog;

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

        testLog = null;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLongRunningCacheFutures() throws Exception {
        long timeout = 1000;

        System.setProperty(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, Long.toString(timeout));

        testLog = new CustomTestLogger(false, log, "future");

        int longRunFuturesCnt = 1000;

        try (Ignite srv1 = start("srv-1", false, false)) {
            try (Ignite srv2 = start("srv-2", false, false)) {
                try (Ignite client = start("client", true, false)) {
                    try (IgniteDataStreamer<Integer, Integer> streamer = client.dataStreamer(CACHE_NAME)) {
                        streamer.allowOverwrite(true);

                        for (int i = 0; i < longRunFuturesCnt; i++)
                            streamer.addData(i, i);
                    }

                    Thread.sleep(timeout * 2);
                }
            }
        }

        assertTrue("Warnings were not found", testLog.warningsTotal() > 0);

        assertTrue("Too much warnings in the logs: " + testLog.warningsTotal(),
            testLog.warningsTotal() < longRunFuturesCnt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLongRunningTransactions() throws Exception {
        long timeout = 1000;

        System.setProperty(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, Long.toString(timeout));

        testLog = new CustomTestLogger(false, log, "transaction");

        int transactions = 100;

        ExecutorService excSvc = Executors.newFixedThreadPool(transactions);

        try (Ignite srv1 = start("srv", false, true)) {
            CountDownLatch txStarted = new CountDownLatch(transactions);

            CountDownLatch stopTx = new CountDownLatch(1);

            for (int i = 0; i < transactions; i++)
                excSvc.submit(new AsyncTransaction(srv1, CACHE_NAME, i, txStarted, stopTx));

            if (!txStarted.await(10000, TimeUnit.MILLISECONDS))
                fail("Unable to start transactions");

            Thread.sleep(timeout * 2);

            stopTx.countDown();
        }
        finally {
            excSvc.shutdown();

            if (!excSvc.awaitTermination(10000, TimeUnit.MILLISECONDS))
                fail("Unable to wait for thread pool termination.");
        }

        assertTrue("Warnings were not found", testLog.warningsTotal() > 0);

        assertTrue("Too much warnings in the logs: " + testLog.warningsTotal(),
            testLog.warningsTotal() < transactions);
    }

    /**
     * Start Ignite node.
     *
     * @param instanceName Ignite instance name.
     * @param clientMode Client mode flag.
     * @param transactional Transactional cache flag.
     * @throws Exception If failed.
     */
    private Ignite start(String instanceName, boolean clientMode, boolean transactional) throws Exception {
        return Ignition.start(getConfiguration(instanceName, clientMode, transactional));
    }

    /**
     * Create Ignite configuration.
     *
     * @param instanceName Ignite instance name.
     * @param clientMode Client mode flag.
     * @param transactional Transactional cache flag.
     * @throws Exception If failed.
     */
    private IgniteConfiguration getConfiguration(String instanceName, boolean clientMode, boolean transactional) throws Exception {
        CacheConfiguration cacheCfg = new CacheConfiguration(CACHE_NAME)
            .setBackups(1)
            .setAtomicityMode(transactional ? CacheAtomicityMode.TRANSACTIONAL : CacheAtomicityMode.ATOMIC);

        IgniteConfiguration cfg = getConfiguration()
            .setCacheConfiguration(cacheCfg)
            .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER))
            .setCommunicationSpi(new CustomTcpCommunicationSpi())
            .setIgniteInstanceName(instanceName)
            .setClientMode(clientMode);

        if (testLog != null)
            cfg.setGridLogger(testLog);

        return cfg;
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
     * Custom logger for counting warning messages.
     */
    private static class CustomTestLogger extends ListeningTestLogger {
        /** */
        private final AtomicInteger warningsTotal = new AtomicInteger();

        /** */
        private final String substr;

        /**
         * @param dbg If set to {@code true}, enables debug and trace log messages processing.
         * @param echo Logger to echo all messages, limited by {@code dbg} flag.
         * @param substr Substring to filter warning messages.
         */
        public CustomTestLogger(boolean dbg, @Nullable IgniteLogger echo, String substr) {
            super(dbg, echo);

            this.substr = substr;
        }

        /** {@inheritDoc} */
        @Override public void warning(String msg, @Nullable Throwable t) {
            super.warning(msg, t);

            if (substr == null || msg.toLowerCase().contains(substr.toLowerCase()))
                warningsTotal.incrementAndGet();
        }

        /**
         * @return Total number of warnings.
         */
        public int warningsTotal() {
            return warningsTotal.get();
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
