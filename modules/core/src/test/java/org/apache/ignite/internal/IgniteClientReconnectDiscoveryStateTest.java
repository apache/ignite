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

package org.apache.ignite.internal;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.Event;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;

import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;

/**
 *
 */
public class IgniteClientReconnectDiscoveryStateTest extends IgniteClientReconnectAbstractTest {
    /** {@inheritDoc} */
    @Override protected int serverCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected int clientCount() {
        return 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnect() throws Exception {
        final Ignite client = ignite(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        long topVer = 4;

        IgniteCluster cluster = client.cluster();

        cluster.nodeLocalMap().put("locMapKey", 10);

        Map<Integer, Integer> nodeCnt = new HashMap<>();

        nodeCnt.put(1, 1);
        nodeCnt.put(2, 2);
        nodeCnt.put(3, 3);
        nodeCnt.put(4, 4);

        for (Map.Entry<Integer, Integer> e : nodeCnt.entrySet()) {
            Collection<ClusterNode> nodes = cluster.topology(e.getKey());

            assertNotNull("No nodes for topology: " + e.getKey(), nodes);
            assertEquals((int)e.getValue(), nodes.size());
        }

        ClusterNode locNode = cluster.localNode();

        assertEquals(topVer, locNode.order());

        TestTcpDiscoverySpi srvSpi = spi(clientRouter(client));

        final CountDownLatch reconnectLatch = new CountDownLatch(1);

        client.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED) {
                    info("Disconnected: " + evt);

                    IgniteFuture<?> fut = client.cluster().clientReconnectFuture();

                    assertNotNull(fut);
                    assertFalse(fut.isDone());
                }
                else if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                    info("Reconnected: " + evt);

                    reconnectLatch.countDown();
                }

                return true;
            }
        }, EVT_CLIENT_NODE_DISCONNECTED, EVT_CLIENT_NODE_RECONNECTED);

        srvSpi.failNode(client.cluster().localNode().id(), null);

        waitReconnectEvent(reconnectLatch);

        topVer += 2; // Client failed and rejoined.

        locNode = cluster.localNode();

        assertEquals(topVer, locNode.order());
        assertEquals(topVer, cluster.topologyVersion());

        nodeCnt.put(5, 3);
        nodeCnt.put(6, 4);

        for (Map.Entry<Integer, Integer> e : nodeCnt.entrySet()) {
            Collection<ClusterNode> nodes = cluster.topology(e.getKey());

            assertNotNull("No nodes for topology: " + e.getKey(), nodes);
            assertEquals((int)e.getValue(), nodes.size());
        }

        assertEquals(10, cluster.nodeLocalMap().get("locMapKey"));
    }
}