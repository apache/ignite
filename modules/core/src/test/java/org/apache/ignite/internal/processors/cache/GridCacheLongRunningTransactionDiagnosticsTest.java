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

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridJobExecuteRequest;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.TransactionsMXBeanImpl;
import org.apache.ignite.mxbean.TransactionsMXBean;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Test class for diagnostics of long running transactions.
 */
public class GridCacheLongRunningTransactionDiagnosticsTest extends GridCommonAbstractTest  {
    /** */
    private static final long LONG_OP_TIMEOUT = 500;

    /** */
    private static final long TX_TIMEOUT = LONG_OP_TIMEOUT * 2;

    /** */
    private static final String CACHE_NAME = "test";

    /** */
    private static String longOpTimeoutCommon;

    /** */
    private final LogListener dumpLsnr = LogListener.matches("Dumping the near node thread that started transaction").build();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        boolean isClient = "client".equals(igniteInstanceName);
        if (!isClient) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);

            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setBackups(2);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);

            cfg.setCacheConfiguration(ccfg);
        }

        ListeningTestLogger testLog = new ListeningTestLogger(false, log);

        testLog.registerListener(dumpLsnr);

        cfg.setGridLogger(testLog);

        return cfg;
    }

    /**
     * Setting long op timeout to small value to make this tests faster
     */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        longOpTimeoutCommon = System.getProperty(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT);

        System.setProperty(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, String.valueOf(LONG_OP_TIMEOUT));
    }

    /**
     * Returning long operations timeout to its former value.
     */
    @Override protected void afterTestsStopped() throws Exception {
        if (longOpTimeoutCommon != null)
            System.setProperty(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, longOpTimeoutCommon);
        else
            System.clearProperty(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT);

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Tests long transaction scenario.
     *
     * @throws Exception if grids start failed.
     */
    @Test
    public void testLrt() throws Exception {
        startGridsMultiThreaded(2);

        imitateLongTransaction(true);
    }

    /**
     * Tests transaction mx bean and its ability to turn on and off thread dump requests
     * from local node to near node.
     *
     * @throws Exception if grids start failed.
     */
    @Test
    public void testLrtChangeSetting() throws Exception {
        startGridsMultiThreaded(2);

        TransactionsMXBean tMXBean0 = txMXBean(0);
        TransactionsMXBean tMXBean1 = txMXBean(1);

        assertTrue(tMXBean0.getTxOwnerDumpRequestsAllowed());
        assertTrue(tMXBean1.getTxOwnerDumpRequestsAllowed());

        tMXBean0.setTxOwnerDumpRequestsAllowed(false);

        assertFalse(tMXBean0.getTxOwnerDumpRequestsAllowed());
        assertFalse(tMXBean1.getTxOwnerDumpRequestsAllowed());

        imitateLongTransaction(false);
    }

    /**
     * Creates a client node and imitates a long running transaction by this client.
     *
     * @param shouldRcvThreadDumpReq whether client node (transaction owner) should
     *                                       receive dump request from local node.
     * @throws Exception if failed.
     */
    private void imitateLongTransaction(boolean shouldRcvThreadDumpReq) throws Exception {
        dumpLsnr.reset();

        final int val = 0;

        final IgniteEx client = startClientGrid("client");

        assertTrue(client.configuration().isClientMode());

        client.getOrCreateCache(CACHE_NAME);

        StringBuilder taskNameContainer = new StringBuilder();

        client.context().io().addMessageListener(
            GridTopic.TOPIC_JOB,
            (nodeId, msg, plc) -> taskNameContainer.append(((GridJobExecuteRequest) msg).getTaskName())
        );

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, TX_TIMEOUT, 1)) {
            client.cache(CACHE_NAME).put(val, 0);

            doSleep(TX_TIMEOUT * 2);

            tx.rollback();
        }

        assertEquals(
            shouldRcvThreadDumpReq,
            FetchActiveTxOwnerTraceClosure.class.getName().equals(taskNameContainer.toString())
        );

        assertEquals(shouldRcvThreadDumpReq, dumpLsnr.check());
    }

    /** */
    private TransactionsMXBean txMXBean(int igniteInt) throws Exception {
        return getMxBean(getTestIgniteInstanceName(igniteInt), "Transactions",
            TransactionsMXBeanImpl.class, TransactionsMXBean.class);
    }
}
