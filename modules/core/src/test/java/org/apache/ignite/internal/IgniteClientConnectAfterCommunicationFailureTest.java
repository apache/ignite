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

import java.util.Arrays;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests client to be able restore connection to cluster on subsequent attempts after communication problems.
 */
@RunWith(JUnit4.class)
public class IgniteClientConnectAfterCommunicationFailureTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(new TcpDiscoveryMulticastIpFinder());

        cfg.setNetworkTimeout(500);
        cfg.setCommunicationSpi(new TcpCommunicationSpi(gridName.contains("block")));

        if (gridName.contains("client")) {
            cfg.setClientMode(true);
        }

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnects() throws Exception {
        Ignite srv1 = startGrid("server1");
        Ignite srv2 = startGrid("server2");
        startGrid("client-block");

        assertEquals(1, srv2.cluster().forClients().nodes().size());
        assertEquals(1, srv1.cluster().forClients().nodes().size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientThreadsSuspended() throws Exception {
        Ignite srv1 = startGrid("server1");
        Ignite srv2 = startGrid("server2");
        Ignite client = startGrid("client");

        boolean blockedAnything = false;

        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.getName().contains("%client%")) {
                thread.suspend();
                blockedAnything = true;
            }
        }

        Thread.sleep(10000);

        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.getName().contains("%client%"))
                thread.resume();
        }

        for (int j = 0; j < 10; j++) {
            boolean topOk = true;

            for (Ignite node : Arrays.asList(srv1, srv2, client)) {
                if (node.cluster().nodes().size() != 3) {
                    U.warn(log, "Grid size is incorrect (will re-run check in 1000 ms) " +
                        "[name=" + node.name() + ", size=" + node.cluster().nodes().size() + ']');

                    topOk = false;

                    break;
                }
            }

            if (topOk)
                return;
            else
                Thread.sleep(1000);
        }

        assertTrue(blockedAnything);
        assertEquals(1, srv2.cluster().forClients().nodes().size());
        assertEquals(1, srv1.cluster().forClients().nodes().size());
    }

    /**
     * Will never connect with the first node id, normal operation after.
     */
    private class TcpCommunicationSpi extends org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi {
        /**
         * Whether this instance should actually block.
         */
        private final boolean isBlocking;

        /**
         * Local node ID that is prevented from creating connections.
         */
        private volatile UUID blockedNodeId = null;

        /**
         *
         * @param isBlocking Whether this instance should actually block.
         */
        public TcpCommunicationSpi(boolean isBlocking) {
            this.isBlocking = isBlocking;
        }

        /** {@inheritDoc} */
        @Override protected GridCommunicationClient createTcpClient(ClusterNode node, int connIdx)
            throws IgniteCheckedException {
            if (blockHandshakeOnce(getLocalNode().id())) {
                throw new IgniteCheckedException("Node is blocked");
            }

            return super.createTcpClient(node, connIdx);
        }

        /** Check if this connection is blocked. */
        private boolean blockHandshakeOnce(UUID nodeId) {
            if (isBlocking && (blockedNodeId == null || blockedNodeId.equals(nodeId))) {
                blockedNodeId = nodeId;
                return true;
            }
            return false;
        }
    }
}
