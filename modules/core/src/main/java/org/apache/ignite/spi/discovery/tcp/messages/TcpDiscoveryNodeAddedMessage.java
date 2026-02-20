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

package org.apache.ignite.spi.discovery.tcp.messages;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.internal.DiscoveryDataPacket;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.jetbrains.annotations.Nullable;

/**
 * Message telling nodes that new node should be added to topology.
 * When newly added node receives the message it connects to its next and finishes
 * join process.
 * <br>
 * Requires pre- and post- marshalling.
 * @see #prepareMarshal(Marshaller)
 * @see #finishUnmarshal(Marshaller, ClassLoader)
 */
@TcpDiscoveryEnsureDelivery
@TcpDiscoveryRedirectToClient
public class TcpDiscoveryNodeAddedMessage extends TcpDiscoveryAbstractTraceableMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(value = 6, method = "gridDiscoveryData")
    private DiscoveryDataPacket dataPacket;

    /** Start time of the first grid node. */
    @Order(7)
    private long gridStartTime;

    /** Topology snapshots history. */
    @Order(value = 8, method = "topologyHistoryMessages")
    private @Nullable Map<Long, ClusterNodeCollectionMessage> topHistMsgs;

    /** {@link TcpDiscoveryAbstractMessage} pending messages from previous node which is a {@link Message}. */
    @Order(value = 9, method = "pendingMessages")
    private Map<Integer, Message> pendingMsgs;

    /** Added node. */
    private TcpDiscoveryNode node;

    /** Marshalled {@link #node}. */
    @Order(10)
    private byte[] nodeBytes;

    /** Current topology. Initialized by coordinator. */
    @GridToStringInclude
    private @Nullable Collection<TcpDiscoveryNode> top;

    /** Marshalled {@link #top}. */
    @Order(value = 11, method = "topologyBytes")
    private @Nullable byte[] topBytes;

    /** */
    @GridToStringInclude
    private transient Collection<TcpDiscoveryNode> clientTop;

    /**
     * TODO: Remove after refactoring of discovery messages serialization https://issues.apache.org/jira/browse/IGNITE-25883
     * Java-serializable pending messages from previous node.
     */
    private @Nullable Map<Integer, TcpDiscoveryAbstractMessage> serializablePendingMsgs;

    /** Marshalled {@link #serializablePendingMsgs}. */
    @Order(value = 12, method = "serializablePendingMessagesBytes")
    private byte[] serializablePendingMsgsBytes;

    /** Constructor for {@link DiscoveryMessageFactory}. */
    public TcpDiscoveryNodeAddedMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param creatorNodeId Creator node ID.
     * @param node Node to add to topology.
     * @param dataPacket container for collecting discovery data across the cluster.
     * @param gridStartTime Start time of the first grid node.
     */
    public TcpDiscoveryNodeAddedMessage(
        UUID creatorNodeId,
        TcpDiscoveryNode node,
        DiscoveryDataPacket dataPacket,
        long gridStartTime
    ) {
        super(creatorNodeId);

        assert node != null;
        assert gridStartTime > 0;

        this.node = node;
        this.dataPacket = dataPacket;
        this.gridStartTime = gridStartTime;
    }

    /**
     * @param msg Message.
     */
    public TcpDiscoveryNodeAddedMessage(TcpDiscoveryNodeAddedMessage msg) {
        super(msg);

        node = msg.node;
        nodeBytes = msg.nodeBytes;

        pendingMsgs = msg.pendingMsgs;
        serializablePendingMsgs = msg.serializablePendingMsgs;
        serializablePendingMsgsBytes = msg.serializablePendingMsgsBytes;

        topology(msg.topology());

        clientTop = msg.clientTop;
        topHistMsgs = msg.topHistMsgs;
        dataPacket = msg.dataPacket;
        gridStartTime = msg.gridStartTime;
    }

    /**
     * TODO: Revise after refactoring of TcpDiscoveryNode serialization https://issues.apache.org/jira/browse/IGNITE-27899
     * @param marsh marshaller.
     */
    public void prepareMarshal(Marshaller marsh) {
        if (!F.isEmpty(topHistMsgs))
            topHistMsgs.values().forEach(m -> m.prepareMarshal(marsh));

        if (node != null && nodeBytes == null) {
            try {
                nodeBytes = U.marshal(marsh, node);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to marshal cluster node.", e);
            }
        }

        if (top != null && topBytes == null) {
            try {
                topBytes = U.marshal(marsh, top);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to marshal topology nodes.", e);
            }
        }

        if (serializablePendingMsgs != null && serializablePendingMsgsBytes == null) {
            try {
                serializablePendingMsgsBytes = U.marshal(marsh, serializablePendingMsgs);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to marshal serializable pending messages.", e);
            }
        }
    }

    /**
     * TODO: Revise after refactoring of TcpDiscoveryNode serialization https://issues.apache.org/jira/browse/IGNITE-27899
     * @param marsh Marshaller.
     * @param clsLdr Class loader.
     */
    public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) {
        if (!F.isEmpty(topHistMsgs))
            topHistMsgs.values().forEach(m -> m.finishUnmarshal(marsh, clsLdr));

        if (nodeBytes != null && node == null) {
            try {
                node = U.unmarshal(marsh, nodeBytes, clsLdr);

                nodeBytes = null;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to unmarshal cluster node.", e);
            }
        }

        if (topBytes != null && top == null) {
            try {
                top = U.unmarshal(marsh, topBytes, clsLdr);

                topBytes = null;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to unmarshal topology nodes.", e);
            }
        }

        if (serializablePendingMsgsBytes != null && serializablePendingMsgs == null) {
            try {
                serializablePendingMsgs = U.unmarshal(marsh, serializablePendingMsgsBytes, clsLdr);

                serializablePendingMsgsBytes = null;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to unmarshal serializable pending messages.", e);
            }
        }

        if (F.isEmpty(pendingMsgs))
            return;

        for (Message msg : pendingMsgs.values()) {
            if (msg instanceof TcpDiscoveryNodeAddedMessage)
                ((TcpDiscoveryNodeAddedMessage)msg).finishUnmarshal(marsh, clsLdr);
        }
    }

    /**
     * Gets newly added node.
     *
     * @return New node.
     */
    public TcpDiscoveryNode node() {
        return node;
    }

    /** @return Serialized {@link #node}. */
    public byte[] nodeBytes() {
        return nodeBytes;
    }

    /** @param nodeBytes Serialized {@link #node}. */
    public void nodeBytes(byte[] nodeBytes) {
        this.nodeBytes = nodeBytes;
    }

    /**
     * @return Pending messages.
     * @see #messages()
     */
    public Map<Integer, Message> pendingMessages() {
        return pendingMsgs;
    }

    /**
     * @param pendingMsgs Pending messages.
     * @see #messages(List)
     */
    public void pendingMessages(Map<Integer, Message> pendingMsgs) {
        this.pendingMsgs = pendingMsgs;
    }

    /**
     * TODO: revise after refactoring of discovery messages serialization https://issues.apache.org/jira/browse/IGNITE-25883
     * Gets pending messages sent to new node by its previous.
     *
     * @return Pending messages from previous node.
     */
    @Nullable public List<TcpDiscoveryAbstractMessage> messages() {
        assert serializablePendingMsgs == null;

        if (F.isEmpty(pendingMsgs) && F.isEmpty(serializablePendingMsgs))
            return Collections.emptyList();

        int totalSz = (F.isEmpty(pendingMsgs) ? 0 : pendingMsgs.size())
            + (F.isEmpty(serializablePendingMsgs) ? 0 : serializablePendingMsgs.size());

        List<TcpDiscoveryAbstractMessage> res = new ArrayList<>(totalSz);

        for (int i = 0; i < totalSz; ++i) {
            Message m = F.isEmpty(pendingMsgs) ? null : pendingMsgs.get(i);

            if (m == null) {
                TcpDiscoveryAbstractMessage sm = serializablePendingMsgs.get(i);
                assert sm != null;

                res.add(sm);
            }
            else {
                assert serializablePendingMsgs == null || serializablePendingMsgs.get(i) == null;
                assert m instanceof TcpDiscoveryAbstractMessage;

                res.add((TcpDiscoveryAbstractMessage)m);
            }
        }

        return res;
    }

    /**
     * TODO: revise after refactoring of discovery messages serialization https://issues.apache.org/jira/browse/IGNITE-25883
     * Sets pending messages to send to new node.
     *
     * @param msgs Pending messages to send to new node.
     */
    public void messages(@Nullable List<TcpDiscoveryAbstractMessage> msgs) {
        serializablePendingMsgsBytes = null;

        if (F.isEmpty(msgs)) {
            serializablePendingMsgs = null;
            pendingMsgs = null;

            return;
        }

        // Keeps the original message order as in a list.
        int idx = 0;

        for (TcpDiscoveryAbstractMessage m : msgs) {
            if (m instanceof Message) {
                if (pendingMsgs == null)
                    pendingMsgs = U.newHashMap(msgs.size());

                pendingMsgs.put(idx++, (Message)m);

                continue;
            }

            if (serializablePendingMsgs == null)
                serializablePendingMsgs = U.newHashMap(msgs.size());

            serializablePendingMsgs.put(idx++, m);
        }
    }

    /** @return Bytes of {@link #serializablePendingMsgs}. */
    public @Nullable byte[] serializablePendingMessagesBytes() {
        return serializablePendingMsgsBytes;
    }

    /** @param serializablePendingMsgsBytes Bytes of {@link #serializablePendingMsgs}. */
    public void serializablePendingMessagesBytes(@Nullable byte[] serializablePendingMsgsBytes) {
        this.serializablePendingMsgsBytes = serializablePendingMsgsBytes;
    }

    /**
     * Gets topology.
     *
     * @return Current topology.
     */
    @Nullable public Collection<TcpDiscoveryNode> topology() {
        return top;
    }

    /**
     * Sets topology.
     *
     * @param top Current topology.
     */
    public void topology(@Nullable Collection<TcpDiscoveryNode> top) {
        this.top = top;
        topBytes = null;
    }

    /** @return Serialized {@link #top}. */
    public @Nullable byte[] topologyBytes() {
        return topBytes;
    }

    /** @param topBytes Serialized {@link #top}. */
    public void topologyBytes(@Nullable byte[] topBytes) {
        this.topBytes = topBytes;
    }

    /**
     * @param top Topology at the moment when client joined.
     */
    public void clientTopology(Collection<TcpDiscoveryNode> top) {
        assert top != null && !top.isEmpty() : top;

        clientTop = top;
    }

    /**
     * @return Topology at the moment when client joined.
     */
    public Collection<TcpDiscoveryNode> clientTopology() {
        return clientTop;
    }

    /** @return Topology history messages. */
    public @Nullable Map<Long, ClusterNodeCollectionMessage> topologyHistoryMessages() {
        return topHistMsgs;
    }

    /** @param topHistMsgs Topology history messages. */
    public void topologyHistoryMessages(@Nullable Map<Long, ClusterNodeCollectionMessage> topHistMsgs) {
        this.topHistMsgs = topHistMsgs;
    }

    /**
     * Gets topology snapshots history.
     *
     * @return Map with topology snapshots history.
     */
    public @Nullable Map<Long, Collection<ClusterNode>> topologyHistory() {
        if (topHistMsgs == null)
            return null;

        Map<Long, Collection<ClusterNode>> res = U.newHashMap(topHistMsgs.size());

        topHistMsgs.forEach((nodeId, msgs) -> {
            Collection<ClusterNode> clusterNodeImpls = msgs.clusterNodeMessages().stream()
                .map(m -> {
                    assert m.clusterMetricsMessage() != null;

                    TcpDiscoveryNode tcpDiscoNode = new TcpDiscoveryNode(
                        m.id(),
                        m.consistentId(),
                        m.order(),
                        m.local(),
                        m.client(),
                        m.addresses(),
                        m.hostNames(),
                        m.attributes(),
                        new IgniteProductVersion(m.productVersionMessage()),
                        new ClusterMetricsSnapshot(m.clusterMetricsMessage())
                    );

                    return tcpDiscoNode;
                }).collect(Collectors.toList());

            res.put(nodeId, clusterNodeImpls);
        });

        return res;
    }

    /**
     * Sets topology snapshots history.
     *
     * @param topHist Map with topology snapshots history.
     */
    public void topologyHistory(@Nullable Map<Long, Collection<ClusterNode>> topHist) {
        if (F.isEmpty(topHist)) {
            topHistMsgs = null;

            return;
        }

        topHistMsgs = U.newHashMap(topHist.size());

        topHist.forEach((nodeId, clusterNodes) -> {
            Collection<ClusterNodeMessage> clusterNodeImpls = clusterNodes.stream().map(ClusterNodeMessage::new)
                .collect(Collectors.toList());

            topHistMsgs.put(nodeId, new ClusterNodeCollectionMessage(clusterNodeImpls));
        });
    }

    /**
     * @return {@link DiscoveryDataPacket} carried by this message.
     */
    public DiscoveryDataPacket gridDiscoveryData() {
        return dataPacket;
    }

    /** @param dataPacket Data packet data. */
    public void gridDiscoveryData(DiscoveryDataPacket dataPacket) {
        this.dataPacket = dataPacket;
    }

    /**
     * Clears discovery data to minimize message size.
     */
    public void clearDiscoveryData() {
        dataPacket = null;
    }

    /**
     * Clears unmarshalled discovery data to minimize message size.
     * These data are used only on "collect" stage and are not part of persistent state.
     */
    public void clearUnmarshalledDiscoveryData() {
        if (dataPacket != null)
            dataPacket.clearUnmarshalledJoiningNodeData();
    }

    /**
     * @return First grid node start time.
     */
    public long gridStartTime() {
        return gridStartTime;
    }

    /** @param gridStartTime First grid node start time. */
    public void gridStartTime(long gridStartTime) {
        this.gridStartTime = gridStartTime;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 20;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryNodeAddedMessage.class, this, "super", super.toString());
    }
}
