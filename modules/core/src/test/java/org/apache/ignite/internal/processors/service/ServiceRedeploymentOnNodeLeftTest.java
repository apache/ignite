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

package org.apache.ignite.internal.processors.service;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpiInternalListener;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;

/** */
public class ServiceRedeploymentOnNodeLeftTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());
        cfg.setDiscoverySpi(new TestTcpDiscoverySpi()
            .setIpFinder(((TcpDiscoverySpi)cfg.getDiscoverySpi()).getIpFinder()));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testReassignedServiceRedeployment() throws Exception {
        startGrids(4);

        assignServiceToNode(2);

        grid(0).services().deploy(new ServiceConfiguration()
            .setName("service")
            .setService(new TestService())
            .setTotalCount(1)
            .setNodeFilter(new TestNodeFilter()));

        assertEquals("test", grid(3).services().serviceProxy("service", Supplier.class, false, 5_000).get());

        assignServiceToNode(1);

        spi(grid(1)).blockMessages((n, msg) -> msg instanceof ServiceSingleNodeDeploymentResultBatch);

        interceptDiscoveryMessage(0, ServiceClusterDeploymentResultBatch.class, () -> {
            assignServiceToNode(3);

            return true; // Proceed with message sending.
        });

        stopGrid(2);

        spi(grid(1)).waitForBlocked();

        stopGrid(1);

        assertEquals("test", grid(3).services().serviceProxy("service", Supplier.class, false, 5_000).get());
    }

    /** */
    @Test
    public void testCoordinatorChangeReassignedServiceRedeployment() throws Exception {
        startGrids(5);

        assignServiceToNode(2);

        grid(0).services().deploy(new ServiceConfiguration()
            .setName("service")
            .setService(new TestService())
            .setTotalCount(1)
            .setNodeFilter(new TestNodeFilter()));

        assertEquals("test", grid(3).services().serviceProxy("service", Supplier.class, false, 5_000).get());

        assignServiceToNode(1);

        spi(grid(1)).blockMessages((n, msg) -> msg instanceof ServiceSingleNodeDeploymentResultBatch);

        CountDownLatch allSingleMsgReceivedOnCoordinatorLatch = new CountDownLatch(1);

        interceptDiscoveryMessage(0, ServiceClusterDeploymentResultBatch.class, () -> {
            assignServiceToNode(4);

            allSingleMsgReceivedOnCoordinatorLatch.countDown();

            return false; // Block message sending.
        });

        stopGrid(2);

        spi(grid(1)).waitForBlocked();

        stopGrid(1);

        assertTrue(allSingleMsgReceivedOnCoordinatorLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS));

        stopGrid(0);

        assertEquals("test", grid(3).services().serviceProxy("service", Supplier.class, false, 5_000).get());
    }

    /** */
    private void invokeOnDiscoveryMessage(int nodeIdx, Class<?> msgCls, Runnable action) {
        interceptDiscoveryMessage(nodeIdx, msgCls, () -> {
            action.run();

            return true;
        });
    }

    /** */
    private void interceptDiscoveryMessage(int nodeIdx, Class<?> msgCls, Supplier<Boolean> interceptor) {
        TestTcpDiscoverySpi discoSpi = (TestTcpDiscoverySpi)grid(nodeIdx).configuration().getDiscoverySpi();

        discoSpi.setInternalListener(new IgniteDiscoverySpiInternalListener() {
            @Override public boolean beforeSendCustomEvent(DiscoverySpi spi, IgniteLogger log, DiscoverySpiCustomMessage msg) {
                if (msgCls.isAssignableFrom(msg.getClass())) {
                    try {
                        return interceptor.get();
                    }
                    finally {
                        discoSpi.setInternalListener(null);
                    }
                }

                return true;
            }
        });
    }

    /** Service reassignment is not deterministic - it use random to choose service holder among nodes. */
    private void assignServiceToNode(int idx) {
        TestNodeFilter.serviceHolderNodeId = grid(idx).context().localNodeId();
    }

    /** */
    private static class TestNodeFilter implements IgnitePredicate<ClusterNode> {
        /** */
        static UUID serviceHolderNodeId;

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return node.id().equals(serviceHolderNodeId);
        }
    }

    /** */
    private static class TestService implements Supplier<String>, Service {
        /** {@inheritDoc} */
        @Override public String get() {
            return "test";
        }
    }
}
