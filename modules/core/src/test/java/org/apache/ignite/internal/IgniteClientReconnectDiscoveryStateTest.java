/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.junit.Test;

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
    @Test
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

        if (tcpDiscovery()) {
            nodeCnt.put(4, 4);

            for (Map.Entry<Integer, Integer> e : nodeCnt.entrySet()) {
                Collection<ClusterNode> nodes = cluster.topology(e.getKey());

                assertNotNull("No nodes for topology: " + e.getKey(), nodes);
                assertEquals((int)e.getValue(), nodes.size());
            }
        }

        ClusterNode locNode = cluster.localNode();

        assertEquals(topVer, locNode.order());

        DiscoverySpi srvSpi = ignite(0).configuration().getDiscoverySpi();

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

        if (tcpDiscovery())
            nodeCnt.put(5, 3);
        else
            nodeCnt.clear();

        nodeCnt.put(6, 4);

        for (Map.Entry<Integer, Integer> e : nodeCnt.entrySet()) {
            Collection<ClusterNode> nodes = cluster.topology(e.getKey());

            assertNotNull("No nodes for topology: " + e.getKey(), nodes);
            assertEquals((int)e.getValue(), nodes.size());
        }

        assertEquals(10, cluster.nodeLocalMap().get("locMapKey"));
    }
}
