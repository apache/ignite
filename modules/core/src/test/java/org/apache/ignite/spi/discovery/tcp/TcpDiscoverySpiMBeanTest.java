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
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

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

        if ("client".equals(igniteInstanceName))
            cfg.setClientMode(true);

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

        MBeanServer srv = ManagementFactory.getPlatformMBeanServer();

        try {
            for (int i = 0; i < 3; i++) {
                IgniteEx grid = grid(i);

                ObjectName spiName = U.makeMBeanName(grid.context().igniteInstanceName(), "SPIs",
                        TcpDiscoverySpi.class.getSimpleName());

                TcpDiscoverySpiMBean bean = JMX.newMBeanProxy(srv, spiName, TcpDiscoverySpiMBean.class);

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

    /** */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /**
     * Tests TcpDiscoverySpiMBean#excludeNode.
     *
     * @throws Exception if fails.
     */
    @Test
    public void testNodeExclusion() throws Exception {
        int srvCnt = 2;

        IgniteEx grid0 = (IgniteEx)startGrids(srvCnt);

        IgniteEx client = null;

        client = startGrid("client");

        MBeanServer srv = ManagementFactory.getPlatformMBeanServer();

        ObjectName spiName = U.makeMBeanName(grid0.context().igniteInstanceName(), "SPIs",
            TcpDiscoverySpi.class.getSimpleName());

        TcpDiscoverySpiMBean bean = JMX.newMBeanProxy(srv, spiName, TcpDiscoverySpiMBean.class);

        assertEquals(grid0.cluster().forServers().nodes().size(), srvCnt);

        assertEquals(grid0.cluster().forClients().nodes().size(), 1);

        bean.excludeNode(client.localNode().id().toString());

        assertTrue(GridTestUtils.waitForCondition(() ->
            grid0.cluster().forClients().nodes().isEmpty(), 5_000));

        assertTrue(strLog.toString().contains("Node excluded, node="));

        assertEquals(grid0.cluster().forClients().nodes().size(), 0);

        bean.excludeNode(new UUID(0, 0).toString());

        bean.excludeNode("fakeUUID");

        assertEquals(grid0.cluster().forServers().nodes().size(), srvCnt);

        ClusterNode node = grid0.cluster().forServers().nodes().stream().filter(n -> n.id() != grid0.localNode().id())
            .findFirst().get();

        assertNotNull(node);

        bean.excludeNode(node.id().toString());

        assertTrue(GridTestUtils.waitForCondition(() ->
            grid0.cluster().forServers().nodes().size() == srvCnt - 1, 5_000));

        bean.excludeNode(grid0.localNode().id().toString());
    }
}
