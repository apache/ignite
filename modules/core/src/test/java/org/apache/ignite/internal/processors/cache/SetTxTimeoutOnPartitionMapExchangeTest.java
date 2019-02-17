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

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TransactionsMXBeanImpl;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.TransactionsMXBean;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class SetTxTimeoutOnPartitionMapExchangeTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Wait condition timeout. */
    private static final long WAIT_CONDITION_TIMEOUT = 10_000L;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        return cfg;
    }

    /**
     *
     */
    public void testDefaultTxTimeoutOnPartitionMapExchange() throws Exception {
        IgniteEx ig1 = startGrid(1);
        IgniteEx ig2 = startGrid(2);

        TransactionConfiguration txCfg1 = ig1.configuration().getTransactionConfiguration();
        TransactionConfiguration txCfg2 = ig2.configuration().getTransactionConfiguration();

        final long expDfltTimeout = TransactionConfiguration.TX_TIMEOUT_ON_PARTITION_MAP_EXCHANGE;

        assertEquals(expDfltTimeout, txCfg1.getTxTimeoutOnPartitionMapExchange());
        assertEquals(expDfltTimeout, txCfg2.getTxTimeoutOnPartitionMapExchange());
    }

    /**
     *
     */
    public void testJmxSetTxTimeoutOnPartitionMapExchange() throws Exception {
        startGrid(1);
        startGrid(2);

        TransactionsMXBean mxBean1 = txMXBean(1);
        TransactionsMXBean mxBean2 = txMXBean(2);

        final long expTimeout1 = 20_000L;
        final long expTimeout2 = 30_000L;

        mxBean1.setTxTimeoutOnPartitionMapExchange(expTimeout1);
        assertTxTimeoutOnPartitionMapExchange(expTimeout1);
        assertEquals(expTimeout1, mxBean1.getTxTimeoutOnPartitionMapExchange());

        mxBean2.setTxTimeoutOnPartitionMapExchange(expTimeout2);
        assertTxTimeoutOnPartitionMapExchange(expTimeout2);
        assertEquals(expTimeout2, mxBean2.getTxTimeoutOnPartitionMapExchange());
    }

    /**
     *
     */
    public void testClusterSetTxTimeoutOnPartitionMapExchange() throws Exception {
        Ignite ig1 = startGrid(1);
        Ignite ig2 = startGrid(2);

        final long expTimeout1 = 20_000L;
        final long expTimeout2 = 30_000L;

        ig1.cluster().setTxTimeoutOnPartitionMapExchange(expTimeout1);
        assertTxTimeoutOnPartitionMapExchange(expTimeout1);

        ig2.cluster().setTxTimeoutOnPartitionMapExchange(expTimeout2);
        assertTxTimeoutOnPartitionMapExchange(expTimeout2);
    }

    /**
     *
     */
    private TransactionsMXBean txMXBean(int igniteInt) throws Exception {
        ObjectName mbeanName = U.makeMBeanName(getTestIgniteInstanceName(igniteInt), "Transactions",
            TransactionsMXBeanImpl.class.getSimpleName());

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (!mbeanSrv.isRegistered(mbeanName))
            fail("MBean is not registered: " + mbeanName.getCanonicalName());

        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, TransactionsMXBean.class, true);
    }

    /**
     * Checking the transaction timeout on all grids.
     *
     * @param expTimeout Expected timeout.
     * @throws IgniteInterruptedCheckedException If failed.
     */
    private void assertTxTimeoutOnPartitionMapExchange(final long expTimeout)
        throws IgniteInterruptedCheckedException {

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (Ignite ignite : G.allGrids()) {
                    long actualTimeout = ignite.configuration()
                        .getTransactionConfiguration().getTxTimeoutOnPartitionMapExchange();

                    if (actualTimeout != expTimeout) {
                        log.warning(String.format(
                            "Wrong transaction timeout on partition map exchange [grid=%s, timeout=%d, expected=%d]",
                            ignite.name(), actualTimeout, expTimeout));

                        return false;
                    }
                }

                return true;

            }
        }, WAIT_CONDITION_TIMEOUT));
    }
}
