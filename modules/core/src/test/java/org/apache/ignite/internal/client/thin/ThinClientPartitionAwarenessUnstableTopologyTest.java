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

import org.apache.ignite.client.ClientCache;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.mxbean.ClientProcessorMXBean;
import org.junit.Test;

/**
 * Test partition awareness of thin client on unstable topology.
 */
public class ThinClientPartitionAwarenessUnstableTopologyTest extends ThinClientAbstractPartitionAwarenessTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Test that join of the new node is detected by the client and affects partition awareness.
     */
    @Test
    public void testPartitionAwarenessOnNodeJoin() throws Exception {
        startGrids(3);

        awaitPartitionMapExchange();

        initClient(getClientConfiguration(1, 2, 3), 1, 2);

        // Test partition awareness before node join.
        testPartitionAwareness(true);

        assertNull(channels[3]);

        startGrid(3);

        awaitPartitionMapExchange();

        // Send non-affinity request to detect topology change.
        ClientCache<Object, Object> cache = client.getOrCreateCache(PART_CACHE_NAME);

        awaitChannelsInit(3);

        assertOpOnChannel(dfltCh, ClientOperation.CACHE_GET_OR_CREATE_WITH_NAME);

        Integer key = primaryKey(grid(3).cache(PART_CACHE_NAME));

        assertNotNull("Not found key for node 3", key);

        cache.put(key, 0);

        // Cache partitions are requested from default channel, since it's first (and currently the only) channel
        // which detects new topology.
        assertOpOnChannel(dfltCh, ClientOperation.CACHE_PARTITIONS);

        assertOpOnChannel(channels[3], ClientOperation.CACHE_PUT);

        // Test partition awareness after node join.
        testPartitionAwareness(false);
    }

    /**
     * Test that node left event affects partition awareness.
     */
    @Test
    public void testPartitionAwarenessOnNodeLeft() throws Exception {
        startGrids(4);

        awaitPartitionMapExchange();

        initClient(getClientConfiguration(1, 2, 3), 1, 2, 3);

        // Test partition awareness before node left.
        testPartitionAwareness(true);

        stopGrid(3);

        channels[3] = null;

        awaitPartitionMapExchange();

        // Next request will also detect topology change.
        initDefaultChannel();

        // Test partition awareness after node join.
        testPartitionAwareness(true);
    }

    /**
     * Test connection restore to affinity nodes.
     */
    @Test
    public void testConnectionLoss() throws Exception {
        startGrids(2);

        awaitPartitionMapExchange();

        initClient(getClientConfiguration(0, 1), 0, 1);

        // Test partition awareness before connection to node lost.
        testPartitionAwareness(true);

        // Choose node to disconnect (not default node).
        int disconnectNodeIdx = dfltCh == channels[0] ? 1 : 0;

        // Drop all thin connections from the node.
        getMxBean(grid(disconnectNodeIdx).name(), "Clients",
            ClientListenerProcessor.class, ClientProcessorMXBean.class).dropAllConnections();

        channels[disconnectNodeIdx] = null;

        // Send request to disconnected node.
        ClientCache<Object, Object> cache = client.cache(PART_CACHE_NAME);

        Integer key = primaryKey(grid(disconnectNodeIdx).cache(PART_CACHE_NAME));

        assertNotNull("Not found key for node " + disconnectNodeIdx, key);

        cache.put(key, 0);

        // Request goes to default channel, since affinity node is disconnected.
        assertOpOnChannel(dfltCh, ClientOperation.CACHE_PUT);

        cache.put(key, 0);

        // Connection to disconnected node should be restored after retry.
        assertOpOnChannel(channels[disconnectNodeIdx], ClientOperation.CACHE_PUT);

        // Test partition awareness.
        testPartitionAwareness(false);
    }

    /**
     * Test that partition awareness works when reconnecting to the new cluster (with lower topology version)
     */
    @Test
    public void testPartitionAwarenessOnClusterRestart() throws Exception {
        startGrids(3);

        awaitPartitionMapExchange();

        initClient(getClientConfiguration(0, 1, 2), 0, 1, 2);

        // Test partition awareness before cluster restart.
        testPartitionAwareness(true);

        stopAllGrids();

        for (int i = 0; i < channels.length; i++)
            channels[i] = null;

        // Start 2 grids, so topology version of the new cluster will be less then old cluster.
        startGrids(2);

        awaitPartitionMapExchange();

        // Send any request to failover.
        try {
            client.cache(REPL_CACHE_NAME).put(0, 0);
        }
        catch (Exception expected) {
            // No-op.
        }

        initDefaultChannel();

        awaitChannelsInit(0, 1);

        testPartitionAwareness(true);
    }

    /**
     * Checks that each request goes to right node.
     *
     * @param partReq Next operation should request partitions map.
     */
    private void testPartitionAwareness(boolean partReq) {
        ClientCache<Object, Object> clientCache = client.cache(PART_CACHE_NAME);
        IgniteInternalCache<Object, Object> igniteCache = grid(0).context().cache().cache(PART_CACHE_NAME);

        for (int i = 0; i < KEY_CNT; i++) {
            TestTcpClientChannel opCh = affinityChannel(i, igniteCache);

            clientCache.put(i, i);

            if (partReq) {
                assertOpOnChannel(dfltCh, ClientOperation.CACHE_PARTITIONS);

                partReq = false;
            }

            assertOpOnChannel(opCh, ClientOperation.CACHE_PUT);
        }
    }
}
