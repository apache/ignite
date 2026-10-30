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

package org.apache.ignite.internal.managers.discovery;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.managers.discovery.GridDiscoveryManager.DISCO_METRICS;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests the JOINED_CLIENT_NODES metric in the discovery metrics registry.
 */
public class GridDiscoveryManagerClientMetricTest extends GridCommonAbstractTest {
    /** */
    private static final String JOINED_CLIENT_NODES = "JoinedClientNodes";

    /** */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(spi);

        return cfg;
    }

    /**
     * Verifies that the JOINED_CLIENT_NODES counter increments
     * when client nodes join the topology.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testClientNodesJoinedMetric() throws Exception {
        int srvCnt = 2;

        try {
            startGrids(srvCnt);

            // Initially no clients have joined.
            for (int i = 0; i < srvCnt; i++) {
                IgniteEx grid = grid(i);

                MetricRegistry discoReg = grid.context().metric().registry(DISCO_METRICS);

                assertNotNull(discoReg);

                IntMetric ctr = discoReg.findMetric(JOINED_CLIENT_NODES);

                assertNotNull("Metric " + JOINED_CLIENT_NODES + " not found", ctr);

                assertEquals(0, ctr.value());
            }

            // Start one client node.
            startClientGrid("client-0");

            // Both servers should see 1 client joined.
            for (int i = 0; i < srvCnt; i++) {
                IgniteEx grid = grid(i);

                MetricRegistry discoReg = grid.context().metric().registry(DISCO_METRICS);

                IntMetric ctr = discoReg.findMetric(JOINED_CLIENT_NODES);

                assertTrue(waitForCondition(() -> ctr.value() == 1, getTestTimeout()));
            }

            // Start a second client node.
            startClientGrid("client-1");

            // Both servers should now see 2 clients joined.
            for (int i = 0; i < srvCnt; i++) {
                IgniteEx grid = grid(i);

                MetricRegistry discoReg = grid.context().metric().registry(DISCO_METRICS);

                IntMetric ctr = discoReg.findMetric(JOINED_CLIENT_NODES);

                assertTrue(waitForCondition(() -> ctr.value() == 2, getTestTimeout()));
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Verifies that the JOINED_CLIENT_NODES metric is accessible
     * via the metric registry and exposes proper name and description.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testMetricMetadata() throws Exception {
        try {
            IgniteEx grid = startGrid(0);

            MetricRegistry discoReg = grid.context().metric().registry(DISCO_METRICS);

            IntMetric ctr = discoReg.findMetric(JOINED_CLIENT_NODES);

            assertNotNull(ctr);

            assertEquals(DISCO_METRICS + "." + JOINED_CLIENT_NODES, ctr.name());

            assertEquals("Joined client nodes count.", ctr.description());
        }
        finally {
            stopAllGrids();
        }
    }
}
