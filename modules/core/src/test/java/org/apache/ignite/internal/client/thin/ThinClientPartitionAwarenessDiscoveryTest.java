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

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.client.ClientAddressFinder;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.junit.Test;

import static org.apache.ignite.configuration.ClientConnectorConfiguration.DFLT_PORT;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.CLIENT_LISTENER_PORT;

/**
 * Test partition awareness of thin client on changed topology.
 */
public class ThinClientPartitionAwarenessDiscoveryTest extends ThinClientAbstractPartitionAwarenessTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Test that client use channels to all running nodes while new nodes start.
     */
    @Test
    public void testClientDiscoveryNodesJoin() throws Exception {
        for (int i = 0; i < MAX_CLUSTER_SIZE; ++i) {
            startGrid(i);
            awaitPartitionMapExchange();

            int[] workChannels = IntStream.rangeClosed(0, i).toArray();

            if (i == 0)
                initClient(getClientConfigurationWithDiscovery(), workChannels);
            else {
                detectTopologyChange();
                awaitChannelsInit(workChannels);
            }

            testPartitionAwareness(workChannels);
        }
    }

    /**
     * Test that client use channels to all running nodes while nodes stop.
     */
    @Test
    public void testClientDiscoveryNodesLeave() throws Exception {
        startGrids(MAX_CLUSTER_SIZE);
        awaitPartitionMapExchange();

        initClient(getClientConfigurationWithDiscovery(), 0, 1, 2, 3);
        detectTopologyChange();

        for (int i = MAX_CLUSTER_SIZE - 1; i != 0; i--) {
            int[] workChannels = IntStream.range(0, i).toArray();

            channels[i] = null;
            stopGrid(i);
            awaitPartitionMapExchange();
            detectTopologyChange();

            testPartitionAwareness(workChannels);
        }
    }

    /**
     * Test that client use channels to configured nodes only while more nodes run.
     */
    @Test
    public void testClientDiscoveryFilterNodeJoin() throws Exception {
        startGrids(MAX_CLUSTER_SIZE - 1);
        awaitPartitionMapExchange();

        initClient(getClientConfigurationWithDiscovery(3), 0, 1, 2);

        startGrid(3);

        awaitPartitionMapExchange();
        detectTopologyChange();

        testPartitionAwareness(0, 1, 2);
    }

    /**
     * Checks that each request goes to right node.
     */
    private void testPartitionAwareness(int... chIdxs) {
        ClientCache<Object, Object> clientCache = client.cache(PART_CACHE_NAME);
        IgniteInternalCache<Object, Object> igniteCache = grid(0).context().cache().cache(PART_CACHE_NAME);

        Map<TestTcpClientChannel, Boolean> channelHits = Arrays.stream(chIdxs).boxed()
            .collect(Collectors.toMap(idx -> channels[idx], idx -> false));

        for (int i = 0; i < KEY_CNT; i++) {
            TestTcpClientChannel opCh = affinityChannel(i, igniteCache);

            clientCache.put(i, i);

            if (i == 0)
                assertOpOnChannel(null, ClientOperation.CACHE_PARTITIONS);

            assertOpOnChannel(opCh, ClientOperation.CACHE_PUT);

            if (opCh != null) {
                assertTrue(channelHits.containsKey(opCh));

                channelHits.compute(opCh, (c, old) -> true);
            }
        }

        assertFalse(channelHits.containsValue(false));
    }

    /**
     * Provide ClientConfiguration with addrResolver that find all alive nodes.
     */
    private ClientConfiguration getClientConfigurationWithDiscovery(int... excludeIdx) {
        Set<Integer> exclude = Arrays.stream(excludeIdx).boxed().collect(Collectors.toSet());

        ClientAddressFinder addrFinder = () ->
            IgnitionEx.allGrids().stream().map(node -> {
                int port = (Integer)node.cluster().localNode().attributes().get(CLIENT_LISTENER_PORT);

                if (exclude.contains(port - DFLT_PORT))
                    return null;

                return "127.0.0.1:" + port;
            })
                .filter(Objects::nonNull)
                .toArray(String[]::new);

        return new ClientConfiguration()
            .setAddressesFinder(addrFinder)
            .setPartitionAwarenessEnabled(true);
    }
}
