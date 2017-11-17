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

package org.apache.ignite.spi.communication.tcp;

import java.lang.management.ManagementFactory;
import java.util.Map;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridJobExecuteRequest;
import org.apache.ignite.internal.GridJobExecuteResponse;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for TcpCommunicationSpi statistics.
 */
public class TcpCommunicationStatisticsTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Mutex. */
    final private Object mux = new Object();

    /**
     * CommunicationSPI synchronized by {@code mux}
     */
    private class SynchronizedCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
            synchronized (mux) {
                super.sendMessage(node, msg);
            }
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg,
            IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            synchronized (mux) {
                super.sendMessage(node, msg, ackC);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER).setForceServerMode(true));

        TcpCommunicationSpi spi = new SynchronizedCommunicationSpi();

        spi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(spi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * Gets TcpCommunicationSpiMBean for given node.
     *
     * @param nodeIdx Node index.
     * @return MBean instance.
     */
     private TcpCommunicationSpiMBean mbean(int nodeIdx) throws MalformedObjectNameException {
        ObjectName mbeanName = U.makeMBeanName(getTestIgniteInstanceName(nodeIdx), "SPIs",
            SynchronizedCommunicationSpi.class.getSimpleName());

        MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        if (mbeanServer.isRegistered(mbeanName))
            return MBeanServerInvocationHandler.newProxyInstance(mbeanServer, mbeanName, TcpCommunicationSpiMBean.class,
                true);
        else
            fail("MBean is not registered: " + mbeanName.getCanonicalName());

        return null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStatistics() throws Exception {
        ClusterGroup clusterGroupNode1 = grid(0).cluster().forNodeId(grid(1).localNode().id());

        grid(0).compute(clusterGroupNode1).call(new IgniteCallable<Boolean>() {
            @Override public Boolean call() throws Exception {
                return Boolean.TRUE;
            }
        });

        synchronized (mux) {
            TcpCommunicationSpiMBean mbean0 = mbean(0);
            TcpCommunicationSpiMBean mbean1 = mbean(1);

            Map<String, Long> msgsSentByNode0 = mbean0.getSentMessagesByNode();
            Map<String, Long> msgsSentByNode1 = mbean1.getSentMessagesByNode();
            Map<String, Long> msgsReceivedByNode0 = mbean0.getReceivedMessagesByNode();
            Map<String, Long> msgsReceivedByNode1 = mbean1.getReceivedMessagesByNode();

            String nodeId0 = grid(0).localNode().id().toString();
            String nodeId1 = grid(1).localNode().id().toString();

            assertEquals(msgsReceivedByNode0.get(nodeId1).longValue(), mbean0.getReceivedMessagesCount());
            assertEquals(msgsReceivedByNode1.get(nodeId0).longValue(), mbean1.getReceivedMessagesCount());
            assertEquals(msgsSentByNode0.get(nodeId1).longValue(), mbean0.getSentMessagesCount());
            assertEquals(msgsSentByNode1.get(nodeId0).longValue(), mbean1.getSentMessagesCount());

            assertEquals(mbean0.getSentMessagesCount(), mbean1.getReceivedMessagesCount());
            assertEquals(mbean1.getSentMessagesCount(), mbean0.getReceivedMessagesCount());

            Map<String, Long> msgsSentByType0 = mbean0.getSentMessagesByType();
            Map<String, Long> msgsSentByType1 = mbean1.getSentMessagesByType();
            Map<String, Long> msgsReceivedByType0 = mbean0.getReceivedMessagesByType();
            Map<String, Long> msgsReceivedByType1 = mbean1.getReceivedMessagesByType();

            assertEquals(1, msgsSentByType0.get(GridJobExecuteRequest.class.getSimpleName()).longValue());
            assertEquals(1, msgsSentByType1.get(GridJobExecuteResponse.class.getSimpleName()).longValue());
            assertEquals(1, msgsReceivedByType0.get(GridJobExecuteResponse.class.getSimpleName()).longValue());
            assertEquals(1, msgsReceivedByType1.get(GridJobExecuteRequest.class.getSimpleName()).longValue());
        }
    }

}
