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

package org.apache.ignite.spi.discovery.tcp;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVT_NODE_SEGMENTED;
import static org.apache.ignite.internal.managers.discovery.GridDiscoveryManager.DISCO_METRICS;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests TcpDiscoverySpiMBean.
 */
public class TcpDiscoverySpiMBeanTest extends GridCommonAbstractTest {
    /** */
    private final GridStringLogger strLog = new GridStringLogger();

    /** */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        TcpDiscoverySpi tcpSpi = new TcpDiscoverySpi();
        tcpSpi.setIpFinder(IP_FINDER);
        cfg.setDiscoverySpi(tcpSpi);

        cfg.setGridLogger(strLog);

        return cfg;
    }

    /**
     * Tests TcpDiscoverySpiMBean#getCurrentTopologyVersion() and TcpDiscoverySpiMBean#dumpRingStructure().
     *
     * @throws Exception if fails.
     */
    @Test
    public void testMBean() throws Exception {
        int cnt = 3;
        int cliIdx = cnt - 1;

        startGrids(cnt - 1);
        startClientGrid(cliIdx);

        ClusterNode crd = U.oldest(grid(0).context().discovery().aliveServerNodes(), null);

        assertNotNull(crd);

        try {
            for (int i = 0; i < cnt; i++) {
                IgniteEx grid = grid(i);

                MetricRegistry discoReg = grid.context().metric().registry(DISCO_METRICS);

                TcpDiscoverySpiMBean bean = getMxBean(grid.context().igniteInstanceName(), "SPIs",
                    TcpDiscoverySpi.class, TcpDiscoverySpiMBean.class);

                assertNotNull(bean);

                assertEquals(grid.cluster().topologyVersion(), bean.getCurrentTopologyVersion());
                assertEquals(grid.cluster().topologyVersion(),
                    discoReg.<LongMetric>findMetric("CurrentTopologyVersion").value());

                if (i != cliIdx) {
                    assertEquals(crd.id(), bean.getCoordinator());
                    assertEquals(crd.id(), discoReg.<ObjectMetric<UUID>>findMetric("Coordinator").value());
                }
                else {
                    assertNull(bean.getCoordinator());
                    assertNull(discoReg.findMetric("Coordinator"));
                }

                if (grid.localNode().id().equals(bean.getCoordinator())) {
                    assertTrue(bean.getCoordinatorSinceTimestamp() > 0);
                    assertTrue(discoReg.<LongMetric>findMetric("CoordinatorSince").value() > 0);
                }
                else {
                    assertEquals(0, bean.getCoordinatorSinceTimestamp());

                    if (i == cliIdx)
                        assertNull(discoReg.findMetric("CoordinatorSince"));
                    else
                        assertEquals(0L, discoReg.<LongMetric>findMetric("CoordinatorSince").value());
                }

                // `getNodesJoined` returns count of joined nodes since local node startup.
                assertEquals((cnt - 1) - i, bean.getNodesJoined());
                assertEquals((cnt - 1) - i, discoReg.<IntMetric>findMetric("JoinedNodes").value());

                assertEquals(0L, bean.getNodesFailed());
                assertEquals(0, discoReg.<IntMetric>findMetric("FailedNodes").value());

                assertEquals(0L, bean.getNodesLeft());
                assertEquals(0, discoReg.<IntMetric>findMetric("LeftNodes").value());

                assertTrue(bean.getTotalReceivedMessages() > 0);
                assertTrue(discoReg.<IntMetric>findMetric("TotalReceivedMessages").value() > 0);

                assertTrue(bean.getTotalProcessedMessages() > 0);
                assertTrue(discoReg.<IntMetric>findMetric("TotalProcessedMessages").value() > 0);

                if (i != cliIdx) {
                    assertTrue(bean.getPendingMessagesRegistered() > 0);
                    assertTrue(discoReg.<IntMetric>findMetric("PendingMessagesRegistered").value() > 0);
                }
                else {
                    assertEquals(0, bean.getPendingMessagesRegistered());
                    assertEquals(0, discoReg.<IntMetric>findMetric("PendingMessagesRegistered").value());
                }

                assertEquals(0, bean.getPendingMessagesDiscarded());

                bean.dumpRingStructure();
                assertTrue(strLog.toString().contains("TcpDiscoveryNodesRing"));

                assertFalse(bean.getProcessedMessages().isEmpty());
                assertFalse(bean.getReceivedMessages().isEmpty());
                assertTrue(bean.getMaxMessageProcessingTime() >= 0);
                assertEquals(i == cliIdx, bean.isClientMode());
            }

            stopGrid(0);

            crd = U.oldest(grid(1).context().discovery().aliveServerNodes(), null);

            for (int i = 1; i < cnt; i++) {
                IgniteEx grid = grid(i);

                MetricRegistry discoReg = grid.context().metric().registry(DISCO_METRICS);

                TcpDiscoverySpiMBean bean = getMxBean(grid.context().igniteInstanceName(), "SPIs",
                    TcpDiscoverySpi.class, TcpDiscoverySpiMBean.class);

                assertNotNull(bean);

                assertEquals(grid.cluster().topologyVersion(), bean.getCurrentTopologyVersion());
                assertEquals(grid.cluster().topologyVersion(),
                    discoReg.<LongMetric>findMetric("CurrentTopologyVersion").value());

                if (i != cliIdx) {
                    assertEquals(crd.id(), bean.getCoordinator());
                    assertEquals(crd.id(), discoReg.<ObjectMetric<UUID>>findMetric("Coordinator").value());
                }

                if (grid.localNode().id().equals(crd.id())) {
                    assertTrue(bean.getCoordinatorSinceTimestamp() > 0);
                    assertTrue(discoReg.<LongMetric>findMetric("CoordinatorSince").value() > 0);
                }

                assertTrue(waitForCondition(
                    () -> bean.getNodesLeft() == 1 && discoReg.<IntMetric>findMetric("LeftNodes").value() == 1,
                    getTestTimeout()));
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Tests TcpDiscoverySpiMBean#excludeNode.
     *
     * @throws Exception if fails.
     */
    @Test
    public void testNodeExclusion() throws Exception {
        try {
            int srvCnt = 2;

            IgniteEx grid0 = startGrids(srvCnt);

            IgniteEx client;

            client = startClientGrid("client");

            TcpDiscoverySpiMBean bean = getMxBean(grid0.context().igniteInstanceName(), "SPIs",
                TcpDiscoverySpi.class, TcpDiscoverySpiMBean.class);

            assertEquals(grid0.cluster().forServers().nodes().size(), srvCnt);

            assertEquals(grid0.cluster().forClients().nodes().size(), 1);

            UUID clientId = client.localNode().id();

            bean.excludeNode(clientId.toString());

            assertTrue(GridTestUtils.waitForCondition(() ->
                grid0.cluster().forClients().nodes().size() == 1, 5_000));

            assertTrue(GridTestUtils.waitForCondition(() ->
                grid0.cluster().forClients().node(clientId) == null, 5_000));

            assertTrue(GridTestUtils.waitForCondition(() ->
                strLog.toString().contains("Node excluded, node="), 5_000));

            bean.excludeNode(new UUID(0, 0).toString());

            bean.excludeNode("fakeUUID");

            assertEquals(grid0.cluster().forServers().nodes().size(), srvCnt);

            ClusterNode node = grid0.cluster().forServers().nodes().stream().filter(n -> n.id() != grid0.localNode().id())
                .findFirst().get();

            assertNotNull(node);

            final CountDownLatch cnt = new CountDownLatch(1);

            Ignite segmentedNode = G.allGrids().stream().filter(id -> id.cluster().localNode().id().equals(node.id()))
                .findAny().get();

            assertNotNull(segmentedNode);

            segmentedNode.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    cnt.countDown();

                    return false;
                }
            }, EVT_NODE_SEGMENTED);

            bean.excludeNode(node.id().toString());

            assertTrue(GridTestUtils.waitForCondition(() ->
                grid0.cluster().forServers().nodes().size() == srvCnt - 1, 5_000));

            assertTrue("Next node have to be failed within failureDetectionTimeout",
                cnt.await(grid0.configuration().getFailureDetectionTimeout() + 3000, MILLISECONDS));

            bean.excludeNode(grid0.localNode().id().toString());
        }
        finally {
            stopAllGrids();
        }
    }
}
