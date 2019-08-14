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

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TransactionMetricsMxBeanImpl;
import org.apache.ignite.internal.TransactionsMXBeanImpl;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.mxbean.TransactionMetricsMxBean;
import org.apache.ignite.mxbean.TransactionsMXBean;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.MessageOrderLogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_TRANSACTION_TIME_DUMP_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TRANSACTION_TIME_DUMP_SAMPLES_COEFFICIENT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TRANSACTION_TIME_DUMP_SAMPLES_PER_SECOND_LIMIT;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class GridTransactionsSystemUserTimeMetricsTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "test";

    /** */
    private static final String CLIENT = "client";

    /** */
    private static final String CLIENT_2 = CLIENT + "2";

    /** */
    private static final long USER_DELAY = 1000;

    /** */
    private static final long SYSTEM_DELAY = 1000;

    /** */
    private static final int TX_COUNT_FOR_LOG_THROTTLING_CHECK = 4;

    /** */
    private static final long LONG_TRAN_TIMEOUT = Math.min(SYSTEM_DELAY, USER_DELAY);

    /** */
    private static final long LONG_OP_TIMEOUT = 500;

    /** */
    private static final String TRANSACTION_TIME_DUMP_REGEX = ".*?ransaction time dump .*";

    /** */
    private static final String TRANSACTION_TIME_DUMPS_SKIPPED_REGEX =
        "Transaction time dumps skipped because of log throttling: " + TX_COUNT_FOR_LOG_THROTTLING_CHECK / 2;

    /** */
    private LogListener logTxDumpLsnr = new MessageOrderLogListener(TRANSACTION_TIME_DUMP_REGEX);

    /** */
    private final TransactionDumpListener transactionDumpLsnr = new TransactionDumpListener(TRANSACTION_TIME_DUMP_REGEX);

    /** */
    private final TransactionDumpListener transactionDumpsSkippedLsnr =
        new TransactionDumpListener(TRANSACTION_TIME_DUMPS_SKIPPED_REGEX);
    /** */
    private final ListeningTestLogger testLog = new ListeningTestLogger(false, log());

    /** */
    private volatile boolean slowPrepare;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        testLog.registerListener(logTxDumpLsnr);
        testLog.registerListener(transactionDumpLsnr);
        testLog.registerListener(transactionDumpsSkippedLsnr);

        cfg.setGridLogger(testLog);

        boolean isClient = igniteInstanceName.contains(CLIENT);

        cfg.setClientMode(isClient);

        if (!isClient) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);

            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setBackups(1);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);

            cfg.setCacheConfiguration(ccfg);
        }

        cfg.setCommunicationSpi(new TestCommunicationSpi());

        return cfg;
    }

    /**
     * Setting long op timeout to small value to make this tests faster
     */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        changeProperty(IGNITE_LONG_TRANSACTION_TIME_DUMP_THRESHOLD, String.valueOf(LONG_TRAN_TIMEOUT));
        changeProperty(IGNITE_TRANSACTION_TIME_DUMP_SAMPLES_COEFFICIENT, String.valueOf(1.0f));
        changeProperty(IGNITE_TRANSACTION_TIME_DUMP_SAMPLES_PER_SECOND_LIMIT, "5");
        changeProperty(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, String.valueOf(LONG_OP_TIMEOUT));
    }

    /**
     * Returning long operations timeout to its former value.
     */
    @Override protected void afterTestsStopped() throws Exception {
        restoreProperty(IGNITE_LONG_TRANSACTION_TIME_DUMP_THRESHOLD);
        restoreProperty(IGNITE_TRANSACTION_TIME_DUMP_SAMPLES_COEFFICIENT);
        restoreProperty(IGNITE_TRANSACTION_TIME_DUMP_SAMPLES_PER_SECOND_LIMIT);
        restoreProperty(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT);

        super.afterTestsStopped();
    }

    /** */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    public void testTransactionsSystemUserTime() throws Exception {
        Ignite ignite = startGrids(2);

        Ignite client = startGrid(CLIENT);

        IgniteLogger oldLog = GridTestUtils.getFieldValue(IgniteTxAdapter.class, "log");

        GridTestUtils.setFieldValue(IgniteTxAdapter.class, "log", testLog);

        try {
            assertTrue(client.configuration().isClientMode());

            IgniteCache<Integer, Integer> cache = client.getOrCreateCache(CACHE_NAME);

            cache.put(1, 1);

            Callable<Object> txCallable = () -> {
                Integer val = cache.get(1);

                cache.put(1, val + 1);

                return null;
            };

            TransactionMetricsMxBean tmMxMetricsBean = getMxBean(
                CLIENT,
                "TransactionMetrics",
                TransactionMetricsMxBean.class,
                TransactionMetricsMxBeanImpl.class
            );

            //slow user
            slowPrepare = false;

            doInTransaction(client, () -> {
                Integer val = cache.get(1);

                doSleep(USER_DELAY);

                cache.put(1, val + 1);

                return null;
            });

            assertEquals(2, cache.get(1).intValue());

            assertTrue(tmMxMetricsBean.getTotalNodeUserTime() >= USER_DELAY);
            assertTrue(tmMxMetricsBean.getTotalNodeSystemTime() < LONG_TRAN_TIMEOUT);

            //slow prepare
            slowPrepare = true;

            doInTransaction(client, txCallable);

            assertTrue(logTxDumpLsnr.check());

            assertEquals(3, cache.get(1).intValue());

            assertTrue(tmMxMetricsBean.getTotalNodeSystemTime() >= SYSTEM_DELAY);

            String sysTimeHisto = tmMxMetricsBean.getNodeSystemTimeHistogram();
            String userTimeHisto = tmMxMetricsBean.getNodeUserTimeHistogram();

            assertNotNull(sysTimeHisto);
            assertNotNull(userTimeHisto);

            assertTrue(!sysTimeHisto.isEmpty());
            assertTrue(!userTimeHisto.isEmpty());

            logTxDumpLsnr.reset();

            //checking settings changing via JMX with second client
            Ignite client2 = startGrid(CLIENT_2);

            TransactionsMXBean tmMxBean = getMxBean(
                CLIENT,
                "Transactions",
                TransactionsMXBean.class,
                TransactionsMXBeanImpl.class
            );

            tmMxBean.setLongTransactionTimeDumpThreshold(0);
            tmMxBean.setTransactionTimeDumpSamplesCoefficient(0.0);

            doInTransaction(client2, txCallable);

            assertFalse(logTxDumpLsnr.check());

            //testing dumps limit

            doSleep(1000);

            transactionDumpLsnr.reset();

            transactionDumpsSkippedLsnr.reset();

            tmMxBean.setTransactionTimeDumpSamplesCoefficient(1.0);

            tmMxBean.setTransactionTimeDumpSamplesPerSecondLimit(TX_COUNT_FOR_LOG_THROTTLING_CHECK / 2);

            slowPrepare = false;

            for (int i = 0; i < TX_COUNT_FOR_LOG_THROTTLING_CHECK; i++)
                doInTransaction(client, txCallable);

            assertEquals(TX_COUNT_FOR_LOG_THROTTLING_CHECK / 2, transactionDumpLsnr.value());

            //testing skipped message in log

            doSleep(1000);

            doInTransaction(client, txCallable);

            assertTrue(transactionDumpsSkippedLsnr.check());

            U.log(log, sysTimeHisto);
            U.log(log, userTimeHisto);
        }
        finally {
            GridTestUtils.setFieldValue(IgniteTxAdapter.class, "log", oldLog);
        }
    }

    /**
     *
     */
    private class TestCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                Object msg0 = ((GridIoMessage)msg).message();

                if (slowPrepare && msg0 instanceof GridNearTxPrepareRequest)
                    doSleep(SYSTEM_DELAY);
            }

            super.sendMessage(node, msg, ackClosure);
        }
    }

    /**
     *
     */
    private static class TransactionDumpListener extends LogListener {
        /** */
        private final AtomicInteger counter = new AtomicInteger(0);

        /** */
        private final String regex;

        /** */
        private TransactionDumpListener(String regex) {
            this.regex = regex;
        }

        /** {@inheritDoc} */
        @Override public boolean check() {
            return value() > 0;
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            counter.set(0);
        }

        /** {@inheritDoc} */
        @Override public void accept(String s) {
            if (s.matches(regex))
                counter.incrementAndGet();
        }

        /** */
        int value() {
            return counter.get();
        }
    }
}
