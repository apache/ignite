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

package org.apache.ignite.spi.communication;

import java.net.BindException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.util.typedef.CO;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.testframework.GridSpiTestContext;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.IgniteMock;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MACS;

/**
 * Super class for all communication self tests.
 * @param <T> Type of communication SPI.
 */
@SuppressWarnings({"JUnitAbstractTestClassNamingConvention"})
public abstract class GridAbstractCommunicationSelfTest<T extends CommunicationSpi> extends GridSpiAbstractTest<T> {
    /** */
    private static long msgId = 1;

    /** */
    private static final Collection<IgniteTestResources> spiRsrcs = new ArrayList<>();

    /** */
    private static final Map<UUID, Set<UUID>> msgDestMap = new HashMap<>();

    /** */
    protected static final Map<UUID, CommunicationSpi<Message>> spis = new HashMap<>();

    /** */
    protected static final Collection<ClusterNode> nodes = new ArrayList<>();

    /** */
    private static final Object mux = new Object();

    /** */
    protected boolean useSsl = false;

    /**
     *
     */
    static {
        GridIoMessageFactory.registerCustom(GridTestMessage.DIRECT_TYPE, new CO<Message>() {
            @Override public Message apply() {
                return new GridTestMessage();
            }
        });
    }

    /** */
    @SuppressWarnings({"deprecation"})
    private class MessageListener implements CommunicationListener<Message> {
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
            info("Received message [locNodeId=" + locNodeId + ", nodeId=" + nodeId +
                ", msg=" + msg + ']');

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

    /** */
    protected GridAbstractCommunicationSelfTest() {
        super(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSendToOneNode() throws Exception {
        info(">>> Starting send to one node test. <<<");

        msgDestMap.clear();

        for (Entry<UUID, CommunicationSpi<Message>> entry : spis.entrySet()) {
            for (ClusterNode node : nodes) {
                synchronized (mux) {
                    if (!msgDestMap.containsKey(entry.getKey()))
                        msgDestMap.put(entry.getKey(), new HashSet<UUID>());

                    msgDestMap.get(entry.getKey()).add(node.id());
                }

                entry.getValue().sendMessage(node, new GridTestMessage(entry.getKey(), msgId++, 0));
            }
        }

        long now = System.currentTimeMillis();
        long endTime = now + getMaxTransmitMessagesTime();

        synchronized (mux) {
            while (now < endTime && !msgDestMap.isEmpty()) {
                mux.wait(endTime - now);

                now = System.currentTimeMillis();
            }

            if (!msgDestMap.isEmpty()) {
                for (Entry<UUID, Set<UUID>> entry : msgDestMap.entrySet()) {
                    error("Failed to receive all messages [sender=" + entry.getKey() +
                        ", dest=" + entry.getValue() + ']');
                }
            }

            assert msgDestMap.isEmpty() : "Some messages were not received.";
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("WaitWithoutCorrespondingNotify")
    public void testSendToManyNodes() throws Exception {
        msgDestMap.clear();

        // Send message from each SPI to all SPI's, including itself.
        for (Entry<UUID, CommunicationSpi<Message>> entry : spis.entrySet()) {
            UUID sndId = entry.getKey();

            CommunicationSpi<Message> commSpi = entry.getValue();

            for (ClusterNode node : nodes) {
                synchronized (mux) {
                    if (!msgDestMap.containsKey(sndId))
                        msgDestMap.put(sndId, new HashSet<UUID>());

                    msgDestMap.get(sndId).add(node.id());
                }

                commSpi.sendMessage(node, new GridTestMessage(sndId, msgId++, 0));
            }
        }

        long now = System.currentTimeMillis();
        long endTime = now + getMaxTransmitMessagesTime();

        synchronized (mux) {
            while (now < endTime && !msgDestMap.isEmpty()) {
                mux.wait(endTime - now);

                now = System.currentTimeMillis();
            }

            if (!msgDestMap.isEmpty()) {
                for (Entry<UUID, Set<UUID>> entry : msgDestMap.entrySet()) {
                    error("Failed to receive all messages [sender=" + entry.getKey() +
                        ", dest=" + entry.getValue() + ']');
                }
            }

            assert msgDestMap.isEmpty() : "Some messages were not received.";
        }
    }

    /**
     * @param idx Node index.
     * @return Spi.
     */
    protected abstract CommunicationSpi<Message> getSpi(int idx);

    /**
     * @return Spi count.
     */
    protected int getSpiCount() {
        return 2;
    }

    /**
     * @return Max time for message delivery.
     */
    protected int getMaxTransmitMessagesTime() {
        return 20000;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 0; i < 3; i++) {
            try {
                startSpis();

                break;
            }
            catch (IgniteCheckedException e) {
                if (e.hasCause(BindException.class)) {
                    if (i < 2) {
                        info("Failed to start SPIs because of BindException, will retry after delay.");

                        afterTestsStopped();

                        U.sleep(30_000);
                    }
                    else
                        throw e;
                }
                else
                    throw e;
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void startSpis() throws Exception {
        U.setWorkDirectory(null, U.getIgniteHome());

        spis.clear();
        nodes.clear();
        spiRsrcs.clear();

        Map<ClusterNode, GridSpiTestContext> ctxs = new HashMap<>();

        for (int i = 0; i < getSpiCount(); i++) {
            CommunicationSpi<Message> spi = getSpi(i);

            GridTestUtils.setFieldValue(spi, IgniteSpiAdapter.class, "gridName", "grid-" + i);

            IgniteTestResources rsrcs = new IgniteTestResources();

            GridTestNode node = new GridTestNode(rsrcs.getNodeId());

            node.order(i);

            GridSpiTestContext ctx = initSpiContext();

            ctx.setLocalNode(node);

            info(">>> Initialized context: nodeId=" + ctx.localNode().id());

            spiRsrcs.add(rsrcs);

            rsrcs.inject(spi);

            if (useSsl) {
                IgniteMock ignite = GridTestUtils.getFieldValue(spi, IgniteSpiAdapter.class, "ignite");

                IgniteConfiguration cfg = ignite.configuration()
                    .setSslContextFactory(GridTestUtils.sslFactory());

                ignite.setStaticCfg(cfg);
            }

            spi.setListener(new MessageListener(rsrcs.getNodeId()));

            node.setAttributes(spi.getNodeAttributes());
            node.setAttribute(ATTR_MACS, F.concat(U.allLocalMACs(), ", "));

            nodes.add(node);

            spi.spiStart(getTestGridName() + (i + 1));

            spis.put(rsrcs.getNodeId(), spi);

            spi.onContextInitialized(ctx);

            ctxs.put(node, ctx);
        }

        // For each context set remote nodes.
        for (Entry<ClusterNode, GridSpiTestContext> e : ctxs.entrySet()) {
            for (ClusterNode n : nodes) {
                if (!n.equals(e.getKey()))
                    e.getValue().remoteNodes().add(n);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        for (CommunicationSpi<Message> spi : spis.values()) {
            spi.onContextDestroyed();

            spi.setListener(null);

            spi.spiStop();
        }

        for (IgniteTestResources rsrcs : spiRsrcs)
            rsrcs.stopThreads();
    }
}