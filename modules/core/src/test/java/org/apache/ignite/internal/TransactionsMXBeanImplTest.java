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

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.mxbean.TransactionsMXBean;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DUMP_TX_COLLISIONS_INTERVAL;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_TRANSACTION_TIME_DUMP_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TRANSACTION_TIME_DUMP_SAMPLES_COEFFICIENT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TRANSACTION_TIME_DUMP_SAMPLES_PER_SECOND_LIMIT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TX_OWNER_DUMP_REQUESTS_ALLOWED;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.testframework.LogListener.matches;

/**
 *
 */
public class TransactionsMXBeanImplTest extends GridCommonAbstractTest {
    /** Prefix of key for distributed meta storage. */
    private static final String DIST_CONF_PREFIX = "distrConf-";

    /** Listener log messages. */
    private static ListeningTestLogger testLog;

    /** Client node. */
    private boolean clientNode;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        testLog = new ListeningTestLogger(log);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
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
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                    ))
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

        ignite.cluster().state(ACTIVE);

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
     * Test to verify the correct change of {@link TransactionsMXBean#getLongOperationsDumpTimeout()}
     * in an immutable cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, value = "100")
    public void testChangeLongOperationsDumpTimeoutOnImmutableCluster() throws Exception {
        Map<IgniteEx, TransactionsMXBean> allNodes = startGridAndActivate(2);
        Map<IgniteEx, TransactionsMXBean> clientNodes = new HashMap<>();
        Map<IgniteEx, TransactionsMXBean> srvNodes = new HashMap<>(allNodes);

        clientNode = true;

        for (int i = 2; i < 4; i++) {
            IgniteEx igniteEx = startGrid(i);

            TransactionsMXBean transactionsMXBean = txMXBean(i);

            allNodes.put(igniteEx, transactionsMXBean);
            clientNodes.put(igniteEx, transactionsMXBean);
        }

        //check for default value
        checkPropertyValueViaTxMxBean(allNodes, 100L, TransactionsMXBean::getLongOperationsDumpTimeout);

        //create property update latches for client nodes
        Map<IgniteEx, List<CountDownLatch>> updateLatches = new HashMap<>();

        clientNodes.keySet().forEach(ignite -> updateLatches.put(ignite, F.asList(new CountDownLatch(1), new CountDownLatch(1))));

        clientNodes.forEach((igniteEx, bean) -> igniteEx.context().distributedMetastorage().listen(
            (key) -> key.startsWith(DIST_CONF_PREFIX),
            (String key, Serializable oldVal, Serializable newVal) -> {
                if ((long)newVal == 200)
                    updateLatches.get(igniteEx).get(0).countDown();
                if ((long)newVal == 300)
                    updateLatches.get(igniteEx).get(1).countDown();
            }
        ));

        long newTimeout = 200L;

        //update value via server node
        updatePropertyViaTxMxBean(allNodes, TransactionsMXBean::setLongOperationsDumpTimeout, newTimeout);

        //check new value in server nodes
        checkPropertyValueViaTxMxBean(srvNodes, newTimeout, TransactionsMXBean::getLongOperationsDumpTimeout);

        //check new value in client nodes
        for (List<CountDownLatch> list : updateLatches.values()) {
            CountDownLatch countDownLatch = list.get(0);

            countDownLatch.await(100, TimeUnit.MILLISECONDS);
        }

        newTimeout = 300L;

        //update value via server node
        updatePropertyViaTxMxBean(clientNodes, TransactionsMXBean::setLongOperationsDumpTimeout, newTimeout);

        //check new value in server nodes
        checkPropertyValueViaTxMxBean(srvNodes, newTimeout, TransactionsMXBean::getLongOperationsDumpTimeout);

        //check new value on client nodes
        for (List<CountDownLatch> list : updateLatches.values()) {
            CountDownLatch countDownLatch = list.get(1);

            countDownLatch.await(100, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Test to verify the correct change of {@link TransactionsMXBean#getLongOperationsDumpTimeout()}
     * in an mutable cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, value = "100")
    public void testChangeLongOperationsDumpTimeoutOnMutableCluster() throws Exception {
        Map<IgniteEx, TransactionsMXBean> allNodes = startGridAndActivate(2);

        long newTimeout = 200L;

        updatePropertyViaTxMxBean(allNodes, TransactionsMXBean::setLongOperationsDumpTimeout, newTimeout);

        checkPropertyValueViaTxMxBean(allNodes, newTimeout, TransactionsMXBean::getLongOperationsDumpTimeout);

        stopAllGrids();

        allNodes = startGridAndActivate(2);

        //check that new value after restart
        checkPropertyValueViaTxMxBean(allNodes, newTimeout, TransactionsMXBean::getLongOperationsDumpTimeout);

        newTimeout = 300L;

        updatePropertyViaTxMxBean(allNodes, TransactionsMXBean::setLongOperationsDumpTimeout, newTimeout);

        allNodes.putAll(singletonMap(startGrid(2), txMXBean(2)));

        //check that last value in new node
        checkPropertyValueViaTxMxBean(allNodes, newTimeout, TransactionsMXBean::getLongOperationsDumpTimeout);
    }

    /**
     * Test to verify the correct change of {@link TransactionsMXBean#getTxOwnerDumpRequestsAllowed()}
     * in an mutable cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_TX_OWNER_DUMP_REQUESTS_ALLOWED, value = "false")
    public void testChangeTxOwnerDumpRequestsAllowed() throws Exception {
        checkPropertyChangingViaTxMxBean(false, true, TransactionsMXBean::getTxOwnerDumpRequestsAllowed,
            TransactionsMXBean::setTxOwnerDumpRequestsAllowed);
    }

    /**
     * Test to verify the correct change of {@link TransactionsMXBean#getLongTransactionTimeDumpThreshold()}
     * in an mutable cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_LONG_TRANSACTION_TIME_DUMP_THRESHOLD, value = "0")
    public void testChangeLongTransactionTimeDumpThreshold() throws Exception {
        checkPropertyChangingViaTxMxBean(0L, 999L, TransactionsMXBean::getLongTransactionTimeDumpThreshold,
            TransactionsMXBean::setLongTransactionTimeDumpThreshold);
    }

    /**
     * Test to verify the correct change of {@link TransactionsMXBean#getTransactionTimeDumpSamplesCoefficient()}
     * in an mutable cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_TRANSACTION_TIME_DUMP_SAMPLES_COEFFICIENT, value = "0.0")
    public void testChangeTransactionTimeDumpSamplesCoefficient() throws Exception {
        checkPropertyChangingViaTxMxBean(0.0, 1.0, TransactionsMXBean::getTransactionTimeDumpSamplesCoefficient,
            TransactionsMXBean::setTransactionTimeDumpSamplesCoefficient);
    }

    /**
     * Test to verify the correct change of {@link TransactionsMXBean#getTransactionTimeDumpSamplesPerSecondLimit()}
     * in an mutable cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_TRANSACTION_TIME_DUMP_SAMPLES_PER_SECOND_LIMIT, value = "5")
    public void testChangeLongTransactionTimeDumpSamplesPerSecondLimit() throws Exception {
        checkPropertyChangingViaTxMxBean(5, 10, TransactionsMXBean::getTransactionTimeDumpSamplesPerSecondLimit,
            TransactionsMXBean::setTransactionTimeDumpSamplesPerSecondLimit);
    }

    /**
     * Test to verify the correct change of {@link TransactionsMXBean#getTxKeyCollisionsInterval()}
     * in an mutable cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_TX_COLLISIONS_INTERVAL, value = "1100")
    public void testChangeCollisionsDumpInterval() throws Exception {
        checkPropertyChangingViaTxMxBean(1100, 1200, TransactionsMXBean::getTxKeyCollisionsInterval,
                TransactionsMXBean::setTxKeyCollisionsInterval);
    }

    /**
     * Test to verify the correct change of property in an mutable cluster.
     *
     * @param defVal Default value.
     * @param newVal New value.
     * @param getter Getter of property.
     * @param setter Setter of property.
     * @param <T> Type of property.
     * @throws Exception If failed.
     */
    private <T> void checkPropertyChangingViaTxMxBean(T defVal, T newVal, Function<TransactionsMXBean, T> getter,
        BiConsumer<TransactionsMXBean, T> setter
    ) throws Exception {
        Map<IgniteEx, TransactionsMXBean> allNodes = startGridAndActivate(2);

        //check for default value
        checkPropertyValueViaTxMxBean(allNodes, defVal, getter);

        updatePropertyViaTxMxBean(allNodes, setter, newVal);

        //check new value
        checkPropertyValueViaTxMxBean(allNodes, newVal, getter);

        stopAllGrids();

        allNodes = startGridAndActivate(2);

        //check that new value after restart
        checkPropertyValueViaTxMxBean(allNodes, newVal, getter);

        allNodes.putAll(singletonMap(startGrid(2), txMXBean(2)));

        //check that last value after adding new node
        checkPropertyValueViaTxMxBean(allNodes, newVal, getter);
    }

    /**
     * Start grid and activate.
     *
     * @param cnt Nodes count.
     * @return Map with started nodes.
     * @throws Exception If anything failed.
     */
    private Map<IgniteEx, TransactionsMXBean> startGridAndActivate(int cnt) throws Exception {
        Map<IgniteEx, TransactionsMXBean> nodes = new HashMap<>();
        IgniteEx ignite = null;

        for (int i = 0; i < cnt; i++) {
            ignite = startGrid(i);

            nodes.put(ignite, txMXBean(i));
        }

        if (ignite != null)
            ignite.cluster().state(ACTIVE);

        return nodes;
    }

    /**
     * Search for the first node and change property through TransactionsMXBean.
     *
     * @param nodes Nodes with TransactionsMXBean's.
     * @param setter new value setter.
     */
    private <T> void updatePropertyViaTxMxBean(
        Map<IgniteEx, TransactionsMXBean> nodes,
        BiConsumer<TransactionsMXBean, T> setter,
        T val
    ) {
        assertNotNull(nodes);

        setter.accept(nodes.entrySet().stream()
            .findAny().get().getValue(), val);
    }

    /**
     * Checking the value of property on nodes through the TransactionsMXBean.
     *
     * @param nodes Nodes with TransactionsMXBean's.
     * @param checkedVal Checked value.
     */
    private static <T> void checkPropertyValueViaTxMxBean(Map<IgniteEx, TransactionsMXBean> nodes, T checkedVal,
        Function<TransactionsMXBean, T> getter) {
        assertNotNull(nodes);

        nodes.forEach((node, txMxBean) -> assertEquals(checkedVal, getter.apply(txMxBean)));
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
        IgniteEx ignite1 = startGrid(1);

        ignite.cluster().state(ACTIVE);

        TransactionsMXBean txMXBean = txMXBean(0);
        TransactionsMXBean txMXBean1 = txMXBean(1);

        assertEquals(defTimeout, txMXBean.getLongOperationsDumpTimeout());
        assertEquals(defTimeout, txMXBean1.getLongOperationsDumpTimeout());

        Transaction tx = ignite.transactions().txStart();

        LogListener lrtLogLsnr = matches("First 10 long running transactions [total=1]").build();
        LogListener txLogLsnr = matches(((TransactionProxyImpl)tx).tx().xidVersion().toString()).build();

        testLog.registerListener(lrtLogLsnr);
        testLog.registerListener(txLogLsnr);

        txMXBean.setLongOperationsDumpTimeout(newTimeout);

        assertEquals(newTimeout, ignite.context().cache().context().tm().longOperationsDumpTimeout());
        assertEquals(newTimeout, ignite1.context().cache().context().tm().longOperationsDumpTimeout());

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
