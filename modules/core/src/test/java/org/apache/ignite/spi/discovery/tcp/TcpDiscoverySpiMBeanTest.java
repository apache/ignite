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

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

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
}
