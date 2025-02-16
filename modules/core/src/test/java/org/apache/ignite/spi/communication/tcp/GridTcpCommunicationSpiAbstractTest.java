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
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.function.Supplier;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.communication.CommunicationListener;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.GridAbstractCommunicationSelfTest;
import org.apache.ignite.spi.communication.GridTestMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Test for {@link TcpCommunicationSpi}
 */
abstract class GridTcpCommunicationSpiAbstractTest extends GridAbstractCommunicationSelfTest<CommunicationSpi<Message>> {
    /** */
    private static long msgId = 1;

    /** */
    private static final int SPI_COUNT = 3;

    /** */
    public static final int IDLE_CONN_TIMEOUT = 2000;

    /** {@inheritDoc} */
    @Override protected CommunicationSpi<Message> getSpi(int idx) {
        TcpCommunicationSpi spi = new TcpCommunicationSpi();

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
    @Override protected CommunicationListener<Message> createMessageListener(UUID nodeId) {
        return new MessageListener(nodeId);
    }

    /** {@inheritDoc} */
    @Override protected Map<Short, Supplier<Message>> customMessageTypes() {
        return Collections.singletonMap(GridTestMessage.DIRECT_TYPE, GridTestMessage::new);
    }

    /** */
    @Test
    public void testSendToOneNode() throws Exception {
        info(">>> Starting send to one node test. <<<");

        MessageListener.msgDestMap.clear();

        for (Map.Entry<UUID, CommunicationSpi<Message>> entry : spis.entrySet()) {
            for (ClusterNode node : nodes) {
                synchronized (MessageListener.mux) {
                    if (!MessageListener.msgDestMap.containsKey(entry.getKey()))
                        MessageListener.msgDestMap.put(entry.getKey(), new HashSet<>());

                    MessageListener.msgDestMap.get(entry.getKey()).add(node.id());
                }

                entry.getValue().sendMessage(node, new GridTestMessage(entry.getKey(), msgId++, 0));
            }
        }

        long now = System.currentTimeMillis();
        long endTime = now + getMaxTransmitMessagesTime();

        synchronized (MessageListener.mux) {
            while (now < endTime && !MessageListener.msgDestMap.isEmpty()) {
                MessageListener.mux.wait(endTime - now);

                now = System.currentTimeMillis();
            }

            if (!MessageListener.msgDestMap.isEmpty()) {
                for (Map.Entry<UUID, Set<UUID>> entry : MessageListener.msgDestMap.entrySet()) {
                    error("Failed to receive all messages [sender=" + entry.getKey() +
                        ", dest=" + entry.getValue() + ']');
                }
            }

            assert MessageListener.msgDestMap.isEmpty() : "Some messages were not received.";
        }
    }

    /** */
    @Test
    public void testSendToManyNodes() throws Exception {
        MessageListener.msgDestMap.clear();

        // Send message from each SPI to all SPI's, including itself.
        for (Map.Entry<UUID, CommunicationSpi<Message>> entry : spis.entrySet()) {
            UUID sndId = entry.getKey();

            CommunicationSpi<Message> commSpi = entry.getValue();

            for (ClusterNode node : nodes) {
                synchronized (MessageListener.mux) {
                    if (!MessageListener.msgDestMap.containsKey(sndId))
                        MessageListener.msgDestMap.put(sndId, new HashSet<>());

                    MessageListener.msgDestMap.get(sndId).add(node.id());
                }

                commSpi.sendMessage(node, new GridTestMessage(sndId, msgId++, 0));
            }
        }

        long now = System.currentTimeMillis();
        long endTime = now + getMaxTransmitMessagesTime();

        synchronized (MessageListener.mux) {
            while (now < endTime && !MessageListener.msgDestMap.isEmpty()) {
                MessageListener.mux.wait(endTime - now);

                now = System.currentTimeMillis();
            }

            if (!MessageListener.msgDestMap.isEmpty()) {
                for (Map.Entry<UUID, Set<UUID>> entry : MessageListener.msgDestMap.entrySet()) {
                    error("Failed to receive all messages [sender=" + entry.getKey() +
                        ", dest=" + entry.getValue() + ']');
                }
            }

            assert MessageListener.msgDestMap.isEmpty() : "Some messages were not received.";
        }

        // Test idle clients remove.
        for (CommunicationSpi<Message> spi : spis.values()) {
            ConcurrentMap<UUID, GridCommunicationClient[]> clients = GridTestUtils.getFieldValue(spi, "clientPool", "clients");

            assertEquals(getSpiCount() - 1, clients.size());

            clients.put(UUID.randomUUID(), F.first(clients.values()));
        }
    }

    /**
     *
     */
    @Test
    public void testCheckConnection1() {
        for (int i = 0; i < 100; i++) {
            for (Map.Entry<UUID, CommunicationSpi<Message>> entry : spis.entrySet()) {
                TcpCommunicationSpi spi = (TcpCommunicationSpi)entry.getValue();

                List<ClusterNode> checkNodes = new ArrayList<>(nodes);

                assert checkNodes.size() > 1;

                IgniteFuture<BitSet> fut = spi.checkConnection(checkNodes);

                BitSet res = fut.get();

                for (int n = 0; n < checkNodes.size(); n++)
                    assertTrue(res.get(n));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCheckConnection2() throws Exception {
        final int THREADS = spis.size();

        final CyclicBarrier b = new CyclicBarrier(THREADS);

        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        for (Map.Entry<UUID, CommunicationSpi<Message>> entry : spis.entrySet()) {
            final TcpCommunicationSpi spi = (TcpCommunicationSpi)entry.getValue();

            futs.add(GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    List<ClusterNode> checkNodes = new ArrayList<>(nodes);

                    assert checkNodes.size() > 1;

                    b.await();

                    for (int i = 0; i < 100; i++) {
                        IgniteFuture<BitSet> fut = spi.checkConnection(checkNodes);

                        BitSet res = fut.get();

                        for (int n = 0; n < checkNodes.size(); n++)
                            assertTrue(res.get(n));
                    }

                    return null;
                }
            }));
        }

        for (IgniteInternalFuture<?> f : futs)
            f.get();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (CommunicationSpi<Message> spi : spis.values()) {
            ConcurrentMap<UUID, GridCommunicationClient[]> clients = GridTestUtils.getFieldValue(spi, "clientPool", "clients");

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

    /** */
    private static class MessageListener implements CommunicationListener<Message> {
        /** */
        private static final Map<UUID, Set<UUID>> msgDestMap = new HashMap<>();

        /** */
        private static final Object mux = new Object();

        /** */
        private final UUID locNodeId;

        /**
         * @param locNodeId Local node ID.
         */
        MessageListener(UUID locNodeId) {
            assert locNodeId != null;

            this.locNodeId = locNodeId;
        }

        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Message msg, IgniteRunnable msgC) {
            msgC.run();

            if (msg instanceof GridTestMessage) {
                GridTestMessage testMsg = (GridTestMessage)msg;

                if (!testMsg.getSourceNodeId().equals(nodeId))
                    fail("Listener nodeId not equals to message nodeId.");

                synchronized (mux) {
                    // Get list of all recipients for the message.
                    Set<UUID> recipients = msgDestMap.get(testMsg.getSourceNodeId());

                    if (recipients != null) {
                        // Remove this node from a list of recipients.
                        if (!recipients.remove(locNodeId))
                            fail("Received unknown message [locNodeId=" + locNodeId + ", msg=" + testMsg + ']');

                        // If all recipients received their messages,
                        // remove source nodes from sent messages map.
                        if (recipients.isEmpty())
                            msgDestMap.remove(testMsg.getSourceNodeId());

                        if (msgDestMap.isEmpty())
                            mux.notifyAll();
                    }
                    else
                        fail("Received unknown message [locNodeId=" + locNodeId + ", msg=" + testMsg + ']');
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void onDisconnected(UUID nodeId) {
            // No-op.
        }
    }
}
