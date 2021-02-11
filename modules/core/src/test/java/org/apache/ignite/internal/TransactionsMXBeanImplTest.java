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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.mxbean.TransactionsMXBean;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.testframework.LogListener.matches;

/**
 *
 */
public class TransactionsMXBeanImplTest extends GridCommonAbstractTest {
    /** Listener log messages. */
    private static ListeningTestLogger testLog;

    /** Client node. */
    private boolean clientNode;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        testLog = new ListeningTestLogger(false, log);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        testLog.clearListeners();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        return super.getConfiguration(name)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setGridLogger(testLog)
            .setClientMode(clientNode)
            .setCacheConfiguration(
                new CacheConfiguration<>()
                    .setName(DEFAULT_CACHE_NAME)
                    .setAffinity(new RendezvousAffinityFunction(false, 32))
                    .setBackups(1)
                    .setAtomicityMode(TRANSACTIONAL)
                    .setRebalanceMode(CacheRebalanceMode.ASYNC)
                    .setWriteSynchronizationMode(FULL_SYNC)
            );
    }

    /**
     *
     */
    @Test
    public void testBasic() throws Exception {
        IgniteEx ignite = startGrid(0);

        TransactionsMXBean bean = txMXBean(0);

        ignite.transactions().txStart();

        ignite.cache(DEFAULT_CACHE_NAME).put(0, 0);

        String res = bean.getActiveTransactions(null, null, null, null, null, null, null, null, false, false);

        assertEquals("1", res);

        res = bean.getActiveTransactions(null, null, null, null, null, null, null, null, true, false);

        assertTrue(res.indexOf("Tx:") > 0);

        res = bean.getActiveTransactions(null, null, null, null, null, null, null, null, false, true);

        assertEquals("1", res);

        doSleep(500);

        res = bean.getActiveTransactions(null, null, null, null, null, null, null, null, false, false);

        assertEquals("0", res);
    }

    /**
     * Test for changing lrt timeout and their appearance before default
     * timeout through MXBean.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, value = "60000")
    public void testLongOperationsDumpTimeoutPositive() throws Exception {
        checkLongOperationsDumpTimeoutViaTxMxBean(60_000, 100, 10_000, true);
    }

    /**
     * Test to disable the LRT by setting timeout to 0.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, value = "100")
    public void testLongOperationsDumpTimeoutZero() throws Exception {
        checkLongOperationsDumpTimeoutViaTxMxBean(100, 0, 1_000, false);
    }

    /**
     * Test to disable the LRT by setting timeout to -1.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, value = "100")
    public void testLongOperationsDumpTimeoutNegative() throws Exception {
        checkLongOperationsDumpTimeoutViaTxMxBean(100, -1, 1_000, false);
    }

    /**
     * Test to verify the correct change of "Long operations dump timeout." in
     * an immutable cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, value = "100")
    public void testChangeLongOperationsDumpTimeoutOnImmutableCluster() throws Exception {
        Map<IgniteEx, TransactionsMXBean> allNodes = new HashMap<>();

        for (int i = 0; i < 2; i++)
            allNodes.put(startGrid(i), txMXBean(i));

        clientNode = true;

        for (int i = 2; i < 4; i++)
            allNodes.put(startGrid(i), txMXBean(i));

        //check for default value
        checkLongOperationsDumpTimeoutViaTxMxBean(allNodes, 100L);

        //check update value via server node
        long newTimeout = 200L;
        updateLongOperationsDumpTimeoutViaTxMxBean(allNodes, node -> !node.configuration().isClientMode(), newTimeout);
        checkLongOperationsDumpTimeoutViaTxMxBean(allNodes, newTimeout);

        //check update value via client node
        newTimeout = 300L;
        updateLongOperationsDumpTimeoutViaTxMxBean(allNodes, node -> node.configuration().isClientMode(), newTimeout);
        checkLongOperationsDumpTimeoutViaTxMxBean(allNodes, newTimeout);
    }

    /**
     * Test to verify the correct change of "Long operations dump timeout." in
     * an mutable cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, value = "100")
    public void testChangeLongOperationsDumpTimeoutOnMutableCluster() throws Exception {
        Map<IgniteEx, TransactionsMXBean> node0 = singletonMap(startGrid(0), txMXBean(0));
        Map<IgniteEx, TransactionsMXBean> node1 = singletonMap(startGrid(1), txMXBean(1));

        Map<IgniteEx, TransactionsMXBean> allNodes = new HashMap<>();
        allNodes.putAll(node0);
        allNodes.putAll(node1);

        long newTimeout = 200L;
        updateLongOperationsDumpTimeoutViaTxMxBean(allNodes, node -> true, newTimeout);
        checkLongOperationsDumpTimeoutViaTxMxBean(allNodes, newTimeout);
        allNodes.clear();

        stopGrid(1);
        node1 = singletonMap(startGrid(1), txMXBean(1));

        //check that default value in restart node
        long defTimeout = 100L;
        checkLongOperationsDumpTimeoutViaTxMxBean(node0, newTimeout);
        checkLongOperationsDumpTimeoutViaTxMxBean(node1, defTimeout);

        newTimeout = 300L;
        updateLongOperationsDumpTimeoutViaTxMxBean(node0, node -> true, newTimeout);

        Map<IgniteEx, TransactionsMXBean> node2 = singletonMap(startGrid(2), txMXBean(2));

        //check that default value in new node
        checkLongOperationsDumpTimeoutViaTxMxBean(node0, newTimeout);
        checkLongOperationsDumpTimeoutViaTxMxBean(node1, newTimeout);
        checkLongOperationsDumpTimeoutViaTxMxBean(node2, defTimeout);
    }

    /**
     * Search for the first node by the predicate and change
     * "Long operations dump timeout." through TransactionsMXBean.
     *
     * @param nodes Nodes with TransactionsMXBean's.
     * @param nodePred Predicate to search for a node.
     * @param newTimeout New timeout.
     */
    private void updateLongOperationsDumpTimeoutViaTxMxBean(
        Map<IgniteEx, TransactionsMXBean> nodes,
        Predicate<? super IgniteEx> nodePred,
        long newTimeout
    ) {
        assertNotNull(nodes);
        assertNotNull(nodePred);

        nodes.entrySet().stream()
            .filter(e -> nodePred.test(e.getKey()))
            .findAny().get().getValue().setLongOperationsDumpTimeout(newTimeout);
    }

    /**
     * Checking the value of "Long operations dump timeout." on all nodes
     * through the TransactionsMXBean.
     *
     * @param nodes Nodes with TransactionsMXBean's.
     * @param checkTimeout Checked timeout.
     */
    private void checkLongOperationsDumpTimeoutViaTxMxBean(Map<IgniteEx, TransactionsMXBean> nodes, long checkTimeout) {
        assertNotNull(nodes);

        nodes.forEach((node, txMxBean) -> assertEquals(checkTimeout, txMxBean.getLongOperationsDumpTimeout()));
    }

    /**
     * Checking changes and receiving lrt through MXBean.
     *
     * @param defTimeout Default lrt timeout.
     * @param newTimeout New lrt timeout.
     * @param waitTimeTx Waiting time for a lrt.
     * @param expectTx Expect or not a lrt to log.
     * @throws Exception If failed.
     */
    private void checkLongOperationsDumpTimeoutViaTxMxBean(
        long defTimeout,
        long newTimeout,
        long waitTimeTx,
        boolean expectTx
    ) throws Exception {
        IgniteEx ignite = startGrid(0);

        TransactionsMXBean txMXBean = txMXBean(0);

        assertEquals(defTimeout, txMXBean.getLongOperationsDumpTimeout());

        Transaction tx = ignite.transactions().txStart();

        LogListener lrtLogLsnr = matches("First 10 long running transactions [total=1]").build();
        LogListener txLogLsnr = matches(((TransactionProxyImpl)tx).tx().xidVersion().toString()).build();

        testLog.registerListener(lrtLogLsnr);
        testLog.registerListener(txLogLsnr);

        txMXBean.setLongOperationsDumpTimeout(newTimeout);

        assertEquals(newTimeout, ignite.context().cache().context().tm().longOperationsDumpTimeout());

        if (expectTx)
            assertTrue(waitForCondition(() -> lrtLogLsnr.check() && txLogLsnr.check(), waitTimeTx));
        else
            assertFalse(waitForCondition(() -> lrtLogLsnr.check() && txLogLsnr.check(), waitTimeTx));
    }

    /** */
    private TransactionsMXBean txMXBean(int igniteInt) throws Exception {
        return getMxBean(getTestIgniteInstanceName(igniteInt), "Transactions",
            TransactionsMXBeanImpl.class, TransactionsMXBean.class);
    }
}
