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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests client to be able restore connection to cluster on subsequent attempts after communication problems.
 */
public class IgniteClientConnectAfterCommunicationFailureTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName)
            .setNetworkTimeout(500)
            .setCommunicationSpi(new TcpCommunicationSpi(gridName.contains("block")));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnects() throws Exception {
        Ignite srv1 = startGrid("server1");
        Ignite srv2 = startGrid("server2");
        startClientGrid("client-block");

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
        Ignite client = startClientGrid("client");

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
        private final AtomicBoolean isBlocking;

        /**
         *
         * @param isBlocking Whether this instance should actually block.
         */
        public TcpCommunicationSpi(boolean isBlocking) {
            this.isBlocking = new AtomicBoolean(isBlocking);
        }

        /** {@inheritDoc} */
        @Override protected GridCommunicationClient createTcpClient(ClusterNode node, int connIdx)
            throws IgniteCheckedException {
            if (blockHandshakeOnce())
                throw new IgniteCheckedException("Node is blocked");

            return super.createTcpClient(node, connIdx);
        }

        /** Check if this connection is blocked. */
        private boolean blockHandshakeOnce() {
            return isBlocking.compareAndSet(true, false);
        }
    }
}
