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

package org.apache.ignite.internal.client.thin;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.configuration.ClientConnectorConfiguration.DFLT_PORT;

/**
 * Test endpoints discovery by thin client.
 */
public class ThinClientEnpointsDiscoveryTest extends ThinClientAbstractPartitionAwarenessTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testEndpointsDiscovery() throws Exception {
        startGrids(3);

        // Set only subset of nodes to connect, but wait for init of all nodes channels (other nodes should be discovered).
        initClient(getClientConfiguration(0, 4), 0, 1, 2);

        stopGrid(0);

        detectTopologyChange();

        // Address of stopped node removed.
        assertTrue(GridTestUtils.waitForCondition(() -> channels[0].isClosed(), WAIT_TIMEOUT));

        channels[0] = null;

        startGrid(0);

        startGrid(3);

        detectTopologyChange();

        // Addresses of new nodes discovered.
        awaitChannelsInit(0, 3);
    }

    /** */
    @Test
    public void testEndpointsDiscoveryDisabled() throws Exception {
        startGrids(2);

        // Set only subset of nodes to connect, but wait for init of all nodes channels (other nodes should be discovered).
        initClient(getClientConfiguration(0).setClusterDiscoveryEnabled(false), 0);

        Thread.sleep(300);

        assertNull(channels[1]);
        assertNull(channels[2]);
        assertNull(channels[3]);
    }

    /** */
    @Test
    public void testDiscoveryAfterAllNodesFailed() throws Exception {
        startGrids(2);

        awaitPartitionMapExchange();

        initClient(getClientConfiguration(0), 0, 1);

        Integer key = primaryKey(grid(1).cache(PART_CACHE_NAME));

        // Any request to cache through any channel to initialize cache's partitions map.
        client.cache(PART_CACHE_NAME).get(0);

        assertOpOnChannel(null, ClientOperation.CACHE_PARTITIONS);
        assertOpOnChannel(null, ClientOperation.CACHE_GET);

        stopGrid(0);

        // Send request through channel 1 to ensure that channel 0 is closed due to discovered topology change
        // (not by failure on channel 0).
        client.cache(PART_CACHE_NAME).put(key, key);

        assertOpOnChannel(channels[1], ClientOperation.CACHE_PUT);

        assertTrue(GridTestUtils.waitForCondition(() -> channels[0].isClosed(), WAIT_TIMEOUT));

        channels[0] = null;

        // At this moment we know only address of node 1.
        stopGrid(1);

        try {
            detectTopologyChange();

            fail();
        }
        catch (ClientConnectionException ignore) {
            // Expected.
        }

        startGrid(0);

        // We should be able to connect to node 0 again.
        detectTopologyChange();

        awaitChannelsInit(0);
    }

    /** */
    @Test
    public void testUnreachableAddressDiscoveredDoesNotPreventClientInit() throws Exception {
        try (ServerSocket sock = new ServerSocket()) {
            sock.bind(new InetSocketAddress("127.0.0.1", 0));

            ArrayList<String> addrs = new ArrayList<>();
            addrs.add("127.0.0.1:" + sock.getLocalPort());

            IgniteEx server = startGrid(0);
            ClusterNode serverNode = server.cluster().localNode();

            // Override node attributes - set local port of the "fake server" socket which does not work.
            Map<String, Object> attrsFiltered = serverNode.attributes();
            Map<String, Object> attrsSealed = GridTestUtils.getFieldValue(attrsFiltered, "map");
            Map<String, Object> attrs = GridTestUtils.getFieldValue(attrsSealed, "m");
            attrs.put(ClientListenerProcessor.CLIENT_LISTENER_PORT, sock.getLocalPort());

            // Config has good server address, client discovery returns unreachable address.
            // We expect the client to connect to the good address and ignore the unreachable one.
            ClientConfiguration ccfg = new ClientConfiguration()
                    .setTimeout(2000)
                    .setAddresses("127.0.0.1:" + DFLT_PORT);

            IgniteClient client = Ignition.startClient(ccfg);

            Collection<String> cacheNames = client.cacheNames();
            assertFalse(cacheNames.isEmpty());
        }
    }
}
