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

package org.apache.ignite.spi.discovery.datacenter;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class MultiDataCenterClientRoutingTest extends GridCommonAbstractTest {
    /** */
    private static final String DC_ID_0 = "DC0";

    /** */
    private static final String DC_ID_1 = "DC1";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        System.clearProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        // Enforce different mac adresses to emulate distributed environment by default.
        cfg.setUserAttributes(Collections.singletonMap(
            IgniteNodeAttributes.ATTR_MACS_OVERRIDE, UUID.randomUUID().toString()));

        return cfg;
    }

    /** */
    @Test
    public void testConnectionToProperDc() throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        boolean bool = rnd.nextBoolean();

        int nodes = 20;

        for (int i = 0; i < nodes; i += 2) {
            System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, bool ? DC_ID_0 : DC_ID_1);

            startGrid(i);

            System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, bool ? DC_ID_1 : DC_ID_0);

            startGrid(i + 1);
        }

        awaitPartitionMapExchange();

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, bool ? DC_ID_0 : DC_ID_1);

        IgniteEx client = startClientGrid();

        UUID routerId = ((TcpDiscoveryNode)client.localNode()).clientRouterNodeId();

        List<ClusterNode> routers = client.cluster().nodes().stream()
            .filter(node -> node.id().equals(routerId))
            .collect(Collectors.toList());

        assertTrue(routers.size() == 1);
        assertEquals(bool ? DC_ID_0 : DC_ID_1, routers.get(0).dataCenterId());
    }

    /** */
    @Test
    public void testConnectionToAnyDcWhenNoOtherOption() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_0);

        startGrid(0);

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_1);

        IgniteEx client = startClientGrid();

        UUID routerId = ((TcpDiscoveryNode)client.localNode()).clientRouterNodeId();

        List<ClusterNode> routers = client.cluster().nodes().stream()
            .filter(node -> node.id().equals(routerId))
            .collect(Collectors.toList());

        assertTrue(routers.size() == 1);
        assertEquals(DC_ID_0, routers.get(0).dataCenterId());
    }

    /** */
    @Test
    public void testConnectionToUnconfiguredDcWhenNoOtherOption() throws Exception {
        startGrid(0);

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_1);

        IgniteEx client = startClientGrid();

        UUID routerId = ((TcpDiscoveryNode)client.localNode()).clientRouterNodeId();

        List<ClusterNode> routers = client.cluster().nodes().stream()
            .filter(node -> node.id().equals(routerId))
            .collect(Collectors.toList());

        assertTrue(routers.size() == 1);
        assertNull(routers.get(0).dataCenterId());
    }
}
