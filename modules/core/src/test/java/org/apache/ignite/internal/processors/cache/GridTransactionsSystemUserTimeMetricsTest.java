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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.MBeanException;
import javax.management.ReflectionException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TransactionsMXBeanImpl;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccTxSnapshotRequest;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxAdapter;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.mxbean.TransactionsMXBean;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.MessageOrderLogListener;
import org.apache.ignite.testframework.junits.SystemPropertiesList;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_TRANSACTION_TIME_DUMP_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TRANSACTION_TIME_DUMP_SAMPLES_COEFFICIENT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TRANSACTION_TIME_DUMP_SAMPLES_PER_SECOND_LIMIT;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.transactions.TransactionMetricsAdapter.METRIC_SYSTEM_TIME_HISTOGRAM;
import static org.apache.ignite.internal.processors.cache.transactions.TransactionMetricsAdapter.METRIC_TOTAL_SYSTEM_TIME;
import static org.apache.ignite.internal.processors.cache.transactions.TransactionMetricsAdapter.METRIC_TOTAL_USER_TIME;
import static org.apache.ignite.internal.processors.cache.transactions.TransactionMetricsAdapter.METRIC_USER_TIME_HISTOGRAM;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.TX_METRICS;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
@SystemPropertiesList(value = {
    @WithSystemProperty(key = IGNITE_LONG_TRANSACTION_TIME_DUMP_THRESHOLD, value = "999"),
    @WithSystemProperty(key = IGNITE_TRANSACTION_TIME_DUMP_SAMPLES_COEFFICIENT, value = "1.0"),
    @WithSystemProperty(key = IGNITE_TRANSACTION_TIME_DUMP_SAMPLES_PER_SECOND_LIMIT, value = "5"),
    @WithSystemProperty(key = IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, value = "500")
})
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
    private static final long EPSILON = 300;

    /** */
    private static final String TRANSACTION_TIME_DUMP_REGEX = ".*?ransaction time dump .*?totalTime=[0-9]{1,4}, " +
            "systemTime=[0-9]{1,4}, userTime=[0-9]{1,4}, cacheOperationsTime=[0-9]{1,4}.*";

    /** */
    private static final String ROLLBACK_TIME_DUMP_REGEX =
        ".*?Long transaction time dump .*?cacheOperationsTime=[0-9]{1,4}.*?rollbackTime=[0-9]{1,4}.*";

    /** */
    private LogListener logTxDumpLsnr = new MessageOrderLogListener(TRANSACTION_TIME_DUMP_REGEX);

    /** */
    private TransactionDumpListener transactionDumpLsnr = new TransactionDumpListener(TRANSACTION_TIME_DUMP_REGEX);

    /** */
    private LogListener rollbackDumpLsnr = new MessageOrderLogListener(ROLLBACK_TIME_DUMP_REGEX);

    /** */
    private static CommonLogProxy testLog = new CommonLogProxy(null);

    /** */
    private final ListeningTestLogger listeningTestLog = new ListeningTestLogger(false, log());

    /** */
    private static IgniteLogger oldLog;

    /** Flag which is set to true if we need to slow system time. */
    private volatile boolean slowSystem;

    /** Flag which is set to true if we need to simulate transaction failure. */
    private volatile boolean simulateFailure;

    /** */
    private static boolean gridStarted = false;

    /** */
    private Ignite client;

    /** */
    private IgniteCache<Integer, Integer> cache;

    /** */
    private Callable<Object> txCallable = () -> {
        Integer val = cache.get(1);

        cache.put(1, val + 1);

        return null;
    };

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(testLog);

        boolean isClient = igniteInstanceName.contains(CLIENT);

        if (!isClient) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);

            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setBackups(1);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);

            cfg.setCacheConfiguration(ccfg);
        }

        cfg.setMetricExporterSpi(new JmxMetricExporterSpi());

        cfg.setCommunicationSpi(new TestCommunicationSpi());

        return cfg;
    }

    /** */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        oldLog = GridTestUtils.getFieldValue(IgniteTxAdapter.class, "log");

        GridTestUtils.setFieldValue(IgniteTxAdapter.class, "log", testLog);
    }

    /** */
    @Override protected void afterTestsStopped() throws Exception {
        GridTestUtils.setFieldValue(IgniteTxAdapter.class, "log", oldLog);

        oldLog = null;

        gridStarted = false;

        stopAllGrids();

        super.afterTestsStopped();
    }

    /** */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        testLog.setImpl(listeningTestLog);

        listeningTestLog.registerListener(logTxDumpLsnr);
        listeningTestLog.registerListener(transactionDumpLsnr);
        listeningTestLog.registerListener(rollbackDumpLsnr);

        if (!gridStarted) {
            startGrids(2);

            gridStarted = true;
        }

        client = startClientGrid(CLIENT);

        cache = client.getOrCreateCache(CACHE_NAME);

        cache.put(1, 1);

        applyJmxParameters(1000L, 0.0, 5);
    }

    /** */
    @Override protected void afterTest() throws Exception {
        stopGrid(CLIENT);

        super.afterTest();
    }

    /**
     * Applies JMX parameters to client node in runtime. Parameters are spreading through the cluster, so this method
     * allows to change system/user time tracking without restarting the cluster.
     *
     * @param threshold Long transaction time dump threshold.
     * @param coefficient Transaction time dump samples coefficient.
     * @param limit Transaction time dump samples per second limit.
     * @return Transaction MX bean.
     * @throws Exception If failed.
     */
    private TransactionsMXBean applyJmxParameters(Long threshold, Double coefficient, Integer limit) throws Exception {
        TransactionsMXBean tmMxBean = getMxBean(
            CLIENT,
            "Transactions",
            TransactionsMXBeanImpl.class,
            TransactionsMXBean.class);

        if (threshold != null)
            tmMxBean.setLongTransactionTimeDumpThreshold(threshold);

        if (coefficient != null)
            tmMxBean.setTransactionTimeDumpSamplesCoefficient(coefficient);

        if (limit != null)
            tmMxBean.setTransactionTimeDumpSamplesPerSecondLimit(limit);

        return tmMxBean;
    }

    /**
     * Allows to make N asynchronous transactions executing {@link #txCallable} in separate thread pool,
     * with given delay on user time for each transaction.
     *
     * @param client Client.
     * @param txCnt Transactions count.
     * @param userDelay User delay for each transaction.
     */
    private void doAsyncTransactions(Ignite client, int txCnt, long userDelay) {
        ExecutorService executorSrvc = Executors.newFixedThreadPool(txCnt);

        for (int i = 0; i < txCnt; i++) {
            executorSrvc.submit(() -> {
                try {
                    doInTransaction(client, () -> {
                        doSleep(userDelay);

                        txCallable.call();

                        return null;
                    });
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        executorSrvc.shutdown();
    }

    /**
     * Allows to run a transaction which executes {@link #txCallable} with given system delay, user delay and
     * mode.
     *
     * @param client Client.
     * @param sysDelay System delay.
     * @param userDelay User delay.
     * @param mode Mode, see {@link TxTestMode}.
     * @throws Exception If failed.
     */
    private void doTransaction(Ignite client, boolean sysDelay, boolean userDelay, TxTestMode mode) throws Exception {
        if (sysDelay)
            slowSystem = true;

        if (mode == TxTestMode.FAIL)
            simulateFailure = true;

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            if (userDelay)
                doSleep(USER_DELAY);

            txCallable.call();

            if (mode == TxTestMode.ROLLBACK)
                tx.rollback();
            else
                tx.commit();
        }

        slowSystem = false;
        simulateFailure = false;
    }

    /**
     * Allows to run a transaction which executes {@link #txCallable} with given system delay, user delay and
     * mode, also measures it's start time, completion time, gets MX bean with metrics and gives it in a result.
     *
     * @param sysDelay  System delay.
     * @param userDelay User delay.
     * @param mode Mode, see {@link TxTestMode}.
     * @return Result, see {@link ClientTxTestResult}.
     * @throws Exception If failed.
     */
    private ClientTxTestResult measureClientTransaction(boolean sysDelay, boolean userDelay, TxTestMode mode) throws Exception {
        logTxDumpLsnr.reset();
        rollbackDumpLsnr.reset();

        long startTime = System.currentTimeMillis();

        try {
            doTransaction(client, sysDelay, userDelay, mode);
        }
        catch (Exception e) {
            // Giving a time for transaction to rollback.
            doSleep(500);
        }

        long completionTime = System.currentTimeMillis();

        ClientTxTestResult res = new ClientTxTestResult(startTime, completionTime,
            metricRegistry(CLIENT, null, TX_METRICS));

        return res;
    }

    /**
     * Checks that histogram long array is not null and is not empty.
     *
     * @param histogram Array.
     * @param txCnt Total count of transactions, that should be presented in histogram.
     */
    private void checkHistogram(long[] histogram, long txCnt) {
        assertNotNull(histogram);

        long cnt = LongStream.of(histogram).sum();

        assertEquals("Must be " + txCnt + " transaction(s), actually were: " + cnt + ". Histogram: " + histogram, txCnt, cnt);
    }

    /**
     * Checks if metrics have correct values with given delay mode.
     *
     * @param res Should contains the result of transaction completion - start time, completion time and MX bean
     * from which metrics can be received.
     * @param userDelayMode If true, we are checking metrics after transaction with user delay. Otherwise,
     * we are checking metrics after transaction with system delay.
     * @throws MBeanException If getting of metric attribute failed.
     * @throws AttributeNotFoundException If getting of metric attribute failed.
     * @throws ReflectionException If getting of metric attribute failed.
     */
    private void checkTxDelays(ClientTxTestResult res, boolean userDelayMode)
        throws MBeanException, AttributeNotFoundException, ReflectionException {
        long userTime = (Long)res.mBean.getAttribute(METRIC_TOTAL_USER_TIME);
        long sysTime = (Long)res.mBean.getAttribute(METRIC_TOTAL_SYSTEM_TIME);

        if (userDelayMode) {
            assertTrue(userTime >= USER_DELAY);
            assertTrue(userTime < res.completionTime - res.startTime - sysTime + EPSILON);
            assertTrue(sysTime >= 0);
            assertTrue(sysTime < EPSILON);
        }
        else {
            assertTrue(userTime >= 0);
            assertTrue(userTime < EPSILON);
            assertTrue(sysTime >= SYSTEM_DELAY);
            assertTrue(sysTime < res.completionTime - res.startTime - userTime + EPSILON);
        }

        checkHistogram((long[])res.mBean.getAttribute(METRIC_SYSTEM_TIME_HISTOGRAM), 2);
        checkHistogram((long[])res.mBean.getAttribute(METRIC_USER_TIME_HISTOGRAM), 2);
    }

    /**
     * Test user time and system time with user delay on committed transaction.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUserDelayOnCommittedTx() throws Exception {
        ClientTxTestResult res = measureClientTransaction(false, true, TxTestMode.COMMIT);

        assertTrue(logTxDumpLsnr.check());

        checkTxDelays(res, true);
    }

    /**
     * Test user time and system time with user delay on rolled back transaction.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUserDelayOnRolledBackTx() throws Exception {
        ClientTxTestResult res = measureClientTransaction(false, true, TxTestMode.ROLLBACK);

        assertTrue(rollbackDumpLsnr.check());

        checkTxDelays(res, true);
    }

    /**
     * Test user time and system time with user delay on failed transaction.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUserDelayOnFailedTx() throws Exception {
        ClientTxTestResult res = measureClientTransaction(false, true, TxTestMode.FAIL);

        assertTrue(rollbackDumpLsnr.check());

        checkTxDelays(res, true);
    }

    /**
     * Test user time and system time with system delay on committed transaction.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSystemDelayOnCommittedTx() throws Exception {
        ClientTxTestResult res = measureClientTransaction(true, false, TxTestMode.COMMIT);

        assertTrue(logTxDumpLsnr.check());

        checkTxDelays(res, false);
    }

    /**
     * Test user time and system time with system delay on rolled back transaction.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSystemDelayOnRolledBackTx() throws Exception {
        ClientTxTestResult res = measureClientTransaction(true, false, TxTestMode.ROLLBACK);

        assertTrue(rollbackDumpLsnr.check());

        checkTxDelays(res, false);
    }

    /**
     * Test user time and system time with system delay on failed transaction.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSystemDelayOnFailedTx() throws Exception {
        ClientTxTestResult res = measureClientTransaction(true, false, TxTestMode.FAIL);

        assertTrue(rollbackDumpLsnr.check());

        checkTxDelays(res, false);
    }

    /**
     * Test that changing of JMX parameters spreads on cluster correctly.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testJmxParametersSpreading() throws Exception {
        startClientGrid(CLIENT_2);

        try {
            TransactionsMXBean tmMxBean = getMxBean(
                CLIENT,
                "Transactions",
                TransactionsMXBeanImpl.class,
                TransactionsMXBean.class);

            TransactionsMXBean tmMxBean2 = getMxBean(
                CLIENT_2,
                "Transactions",
                TransactionsMXBeanImpl.class,
                TransactionsMXBean.class);

            int oldLimit = tmMxBean.getTransactionTimeDumpSamplesPerSecondLimit();
            long oldThreshold = tmMxBean.getLongTransactionTimeDumpThreshold();
            double oldCoefficient = tmMxBean.getTransactionTimeDumpSamplesCoefficient();

            try {
                int newLimit = 1234;
                long newThreshold = 99999;
                double newCoefficient = 0.01;

                tmMxBean.setTransactionTimeDumpSamplesPerSecondLimit(newLimit);
                tmMxBean2.setLongTransactionTimeDumpThreshold(newThreshold);
                tmMxBean.setTransactionTimeDumpSamplesCoefficient(newCoefficient);

                assertEquals(newLimit, tmMxBean2.getTransactionTimeDumpSamplesPerSecondLimit());
                assertEquals(newThreshold, tmMxBean.getLongTransactionTimeDumpThreshold());
                assertTrue(tmMxBean2.getTransactionTimeDumpSamplesCoefficient() - newCoefficient < 0.0001);
            }
            finally {
                tmMxBean.setTransactionTimeDumpSamplesPerSecondLimit(oldLimit);
                tmMxBean.setLongTransactionTimeDumpThreshold(oldThreshold);
                tmMxBean.setTransactionTimeDumpSamplesCoefficient(oldCoefficient);
            }
        }
        finally {
            // CLIENT grid is stopped in afterTest.
            stopGrid(CLIENT_2);
        }
    }

    /**
     * Tests that tx time dumps appear in log correctly and after tx completion. Also checks that LRT dump
     * now contains information about current system and user time.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLongTransactionDumpLimit() throws Exception {
        logTxDumpLsnr.reset();
        transactionDumpLsnr.reset();

        int txCnt = 10;

        List<String> txLogLines = new LinkedList<>();

        txLogLines.add("First 10 long running transactions \\[total=" + txCnt + "\\]");

        for (int i = 0; i < txCnt; i++)
            txLogLines.add(".*?>>> Transaction .*? systemTime=[0-4]{1,4}, userTime=[0-4]{1,4}.*");

        LogListener lrtLogLsnr = new MessageOrderLogListener(txLogLines.toArray(new String[0]));

        listeningTestLog.registerListener(lrtLogLsnr);

        applyJmxParameters(5000L, null, txCnt);

        doAsyncTransactions(client, txCnt, 5200);

        doSleep(3000);

        assertFalse(logTxDumpLsnr.check());

        doSleep(3000);

        assertTrue(logTxDumpLsnr.check());
        assertTrue(transactionDumpLsnr.check());
        assertTrue(lrtLogLsnr.check());

        assertEquals(txCnt, transactionDumpLsnr.value());
    }

    /**
     * Tests transactions sampling with dumping 100% of transactions in log.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSamplingCoefficient() throws Exception {
        logTxDumpLsnr.reset();
        transactionDumpLsnr.reset();

        int txCnt = 10;

        applyJmxParameters(null, 1.0, txCnt);

        // Wait for a second to reset hit counter.
        doSleep(1000);

        for (int i = 0; i < txCnt; i++)
            doInTransaction(client, txCallable);

        assertTrue(logTxDumpLsnr.check());
        assertTrue(transactionDumpLsnr.check());

        assertEquals(txCnt, transactionDumpLsnr.value());
    }

    /**
     * Tests transactions sampling with dumping 0% of transactions in log.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNoSamplingCoefficient() throws Exception {
        logTxDumpLsnr.reset();

        applyJmxParameters(null, 0.0, 10);

        int txCnt = 10;

        for (int i = 0; i < txCnt; i++)
            doInTransaction(client, txCallable);

        assertFalse(logTxDumpLsnr.check());
    }

    /**
     * Tests transactions sampling with dumping 100% of transactions in log but limited by 2 dump records per second.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSamplingLimit() throws Exception {
        logTxDumpLsnr.reset();
        transactionDumpLsnr.reset();

        int txCnt = 10;
        int txDumpCnt = 2;

        LogListener transactionDumpsSkippedLsnr = LogListener
                .matches("Transaction time dumps skipped because of log throttling: " + (txCnt - txDumpCnt))
                .build();

        listeningTestLog.registerListener(transactionDumpsSkippedLsnr);

        applyJmxParameters(null, 1.0, txDumpCnt);

        // Wait for a second to reset hit counter.
        doSleep(1000);

        for (int i = 0; i < txCnt; i++)
            doInTransaction(client, txCallable);

        // Wait for a second to reset hit counter.
        doSleep(1000);

        // One more sample to print information about skipped previous samples.
        doInTransaction(client, txCallable);

        assertTrue(logTxDumpLsnr.check());
        assertTrue(transactionDumpLsnr.check());
        assertTrue(transactionDumpsSkippedLsnr.check());

        assertEquals(txDumpCnt + 1, transactionDumpLsnr.value());
    }

    /**
     * Tests transactions sampling with dumping 100% of transactions in log and no threshold timeout.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSamplingNoThreshold() throws Exception {
        logTxDumpLsnr.reset();
        transactionDumpLsnr.reset();

        int txCnt = 10;

        applyJmxParameters(0L, 1.0, txCnt);

        // Wait for a second to reset hit counter.
        doSleep(1000);

        for (int i = 0; i < txCnt; i++)
            doInTransaction(client, txCallable);

        assertTrue(logTxDumpLsnr.check());
        assertTrue(transactionDumpLsnr.check());

        assertEquals(txCnt, transactionDumpLsnr.value());
    }

    /**
     * Tests transactions sampling with dumping 100% of transactions in log, no threshold timeout but with limit of 5
     * transactions per second.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSamplingNoThresholdWithLimit() throws Exception {
        logTxDumpLsnr.reset();

        int txCnt = 10;

        applyJmxParameters(0L, 0.0, 5);

        for (int i = 0; i < txCnt; i++)
            doInTransaction(client, txCallable);

        assertFalse(logTxDumpLsnr.check());
    }

    /**
     * Test communication SPI, allowing to simulate system delay on lock and transaction failure on prepare.
     */
    private class TestCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                Object msg0 = ((GridIoMessage)msg).message();

                if (msg0 instanceof GridNearLockRequest || msg0 instanceof MvccTxSnapshotRequest) {
                    if (slowSystem) {
                        slowSystem = false;

                        doSleep(SYSTEM_DELAY);
                    }
                }

                if (msg0 instanceof GridNearTxPrepareRequest) {
                    if (simulateFailure) {
                        simulateFailure = false;

                        throw new RuntimeException("Simulating prepare failure.");
                    }
                }
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

    /**
     * Enum to define transaction test mode.
     */
    enum TxTestMode {
        /** If transaction should be committed. */
        COMMIT,

        /** If transaction should be rolled back. */
        ROLLBACK,

        /** If transaction should fail. */
        FAIL
    }

    /**
     * Result of running of a test transaction.
     */
    private static class ClientTxTestResult {
        /** Start time. */
        final long startTime;

        /** Completion time. */
        final long completionTime;

        /** MX bean to receive metrics. */
        final DynamicMBean mBean;

        /** */
        public ClientTxTestResult(long startTime, long completionTime, DynamicMBean mBean) {
            this.startTime = startTime;
            this.completionTime = completionTime;
            this.mBean = mBean;
        }
    }

    /** */
    private static class CommonLogProxy implements IgniteLogger {
        /** */
        private IgniteLogger impl;

        /** */
        public CommonLogProxy(IgniteLogger impl) {
            this.impl = impl;
        }

        /** */
        public void setImpl(IgniteLogger impl) {
            this.impl = impl;
        }

        /** {@inheritDoc} */
        @Override public IgniteLogger getLogger(Object ctgr) {
            return impl.getLogger(ctgr);
        }

        /** {@inheritDoc} */
        @Override public void trace(String msg) {
            impl.trace(msg);
        }

        /** {@inheritDoc} */
        @Override public void debug(String msg) {
            impl.debug(msg);
        }

        /** {@inheritDoc} */
        @Override public void info(String msg) {
            impl.info(msg);
        }

        /** {@inheritDoc} */
        @Override public void warning(String msg, Throwable e) {
            impl.warning(msg, e);
        }

        /** {@inheritDoc} */
        @Override public void error(String msg, Throwable e) {
            impl.error(msg, e);
        }

        /** {@inheritDoc} */
        @Override public boolean isTraceEnabled() {
            return impl.isTraceEnabled();
        }

        /** {@inheritDoc} */
        @Override public boolean isDebugEnabled() {
            return impl.isDebugEnabled();
        }

        /** {@inheritDoc} */
        @Override public boolean isInfoEnabled() {
            return impl.isInfoEnabled();
        }

        /** {@inheritDoc} */
        @Override public boolean isQuiet() {
            return impl.isQuiet();
        }

        /** {@inheritDoc} */
        @Override public String fileName() {
            return impl.fileName();
        }
    }
}
