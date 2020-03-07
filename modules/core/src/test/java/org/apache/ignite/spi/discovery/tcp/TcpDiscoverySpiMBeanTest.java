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

import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVT_NODE_SEGMENTED;

/**
 * Tests TcpDiscoverySpiMBean.
 */
public class TcpDiscoverySpiMBeanTest extends GridCommonAbstractTest {
    /** */
    private GridStringLogger strLog = new GridStringLogger();

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
        startGrids(3);

        try {
            for (int i = 0; i < 3; i++) {
                IgniteEx grid = grid(i);

                TcpDiscoverySpiMBean bean = getMxBean(grid.context().igniteInstanceName(), "SPIs",
                    TcpDiscoverySpi.class, TcpDiscoverySpiMBean.class);

                assertNotNull(bean);
                assertEquals(grid.cluster().topologyVersion(), bean.getCurrentTopologyVersion());

                bean.dumpRingStructure();
                assertTrue(strLog.toString().contains("TcpDiscoveryNodesRing"));
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

            assertTrue(strLog.toString().contains("Node excluded, node="));

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
