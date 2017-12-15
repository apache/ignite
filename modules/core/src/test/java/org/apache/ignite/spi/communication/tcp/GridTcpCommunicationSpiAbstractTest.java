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

package org.apache.ignite.spi.communication.tcp;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.GridAbstractCommunicationSelfTest;
import org.apache.ignite.spi.communication.GridTestMessage;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Test for {@link TcpCommunicationSpi}
 */
abstract class GridTcpCommunicationSpiAbstractTest extends GridAbstractCommunicationSelfTest<CommunicationSpi> {
    /** */
    private static final int SPI_COUNT = 2;

    /** */
    public static final int IDLE_CONN_TIMEOUT = 2000;

    /** */
    private final boolean useShmem;

    /**
     * @param useShmem Use shared mem flag.
     */
    protected GridTcpCommunicationSpiAbstractTest(boolean useShmem) {
        this.useShmem = useShmem;
    }

    /** {@inheritDoc} */
    @Override protected CommunicationSpi getSpi(int idx) {
        TcpCommunicationSpi spi = new TcpCommunicationSpi();

        if (!useShmem)
            spi.setSharedMemoryPort(-1);

        spi.setLocalPort(GridTestUtils.getNextCommPort(getClass()));
        spi.setIdleConnectionTimeout(IDLE_CONN_TIMEOUT);
        spi.setTcpNoDelay(tcpNoDelay());

        return spi;
    }

    /**
     * @return Value of property '{@link TcpCommunicationSpi#isTcpNoDelay()}'.
     */
    protected abstract boolean tcpNoDelay();

    /** {@inheritDoc} */
    @Override protected int getSpiCount() {
        return SPI_COUNT;
    }

    /** {@inheritDoc} */
    @Override public void testSendToManyNodes() throws Exception {
        super.testSendToManyNodes();

        // Test idle clients remove.
        for (CommunicationSpi spi : spis.values()) {
            ConcurrentMap<UUID, GridCommunicationClient> clients = U.field(spi, "clients");

            assertEquals(getSpiCount() - 1, clients.size());

            clients.put(UUID.randomUUID(), F.first(clients.values()));
        }
    }

    public void testConnectionCheck() {
        for (Map.Entry<UUID, CommunicationSpi<Message>> entry : spis.entrySet()) {
            UUID id = entry.getKey();

            TcpCommunicationSpi spi = (TcpCommunicationSpi)entry.getValue();

            List<ClusterNode> checkNodes = new ArrayList<>();

            for (ClusterNode node : nodes) {
                if (!id.equals(node.id()))
                    checkNodes.add(node);
            }

            spi.checkConnection(checkNodes);

            break;
//            for (ClusterNode node : nodes) {
//                synchronized (mux) {
//                    if (!msgDestMap.containsKey(entry.getKey()))
//                        msgDestMap.put(entry.getKey(), new HashSet<UUID>());
//
//                    msgDestMap.get(entry.getKey()).add(node.id());
//                }
//
//                entry.getValue().sendMessage(node, new GridTestMessage(entry.getKey(), msgId++, 0));
//            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (CommunicationSpi spi : spis.values()) {
            ConcurrentMap<UUID, GridCommunicationClient[]> clients = U.field(spi, "clients");

            for (int i = 0; i < 20; i++) {
                GridCommunicationClient client0 = null;

                for (GridCommunicationClient[] clients0 : clients.values()) {
                    for (GridCommunicationClient client : clients0) {
                        if (client != null) {
                            client0 = client;

                            break;
                        }
                    }

                    if (client0 != null)
                        break;
                }

                if (client0 == null)
                    return;

                info("Check failed for SPI [igniteInstanceName=" +
                    GridTestUtils.getFieldValue(spi, IgniteSpiAdapter.class, "igniteInstanceName") +
                    ", client=" + client0 +
                    ", spi=" + spi + ']');

                U.sleep(1000);
            }

            fail("Failed to wait when clients are closed.");
        }
    }
}