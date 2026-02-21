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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.spi.discovery.tcp.internal.DiscoveryDataPacket;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.jetbrains.annotations.Nullable;

/**
 * TODO: Revise serialization of the {@link TcpDiscoveryNode} fields after https://issues.apache.org/jira/browse/IGNITE-27899
 * Message telling nodes that new node should be added to topology.
 * When newly added node receives the message it connects to its next and finishes
 * join process.
 */
@TcpDiscoveryEnsureDelivery
@TcpDiscoveryRedirectToClient
public class TcpDiscoveryNodeAddedMessage extends TcpDiscoveryAbstractTraceableMessage implements TcpDiscoveryMarshallableMessage {
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

    /** Message to hold collection of pending {@link TcpDiscoveryAbstractMessage}. */
    @Order(value = 9, method = "pendingMessagesTransferMessage")
    private TcpDiscoveryCollectionMessage pendingMsgsMsg;

    /** Added node. */
    private TcpDiscoveryNode node;

    /** Marshalled {@link #node}. */
    @Order(10)
    @GridToStringExclude
    private byte[] nodeBytes;

    /** Current topology. Initialized by coordinator. */
    @GridToStringInclude
    private @Nullable Collection<TcpDiscoveryNode> top;

    /** Marshalled {@link #top}. */
    @Order(value = 11, method = "topologyBytes")
    @GridToStringExclude
    private @Nullable byte[] topBytes;

    /** */
    @GridToStringInclude
    private transient Collection<TcpDiscoveryNode> clientTop;

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

        top = msg.top;
        topBytes = msg.topBytes;

        pendingMsgsMsg = msg.pendingMsgsMsg;

        clientTop = msg.clientTop;
        topHistMsgs = msg.topHistMsgs;
        dataPacket = msg.dataPacket;
        gridStartTime = msg.gridStartTime;
    }

    /** @param marsh marshaller. */
    @Override public void prepareMarshal(Marshaller marsh) {
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
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) {
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
     * Gets pending messages sent to new node by its previous.
     *
     * @return Pending messages from previous node.
     */
    public Collection<TcpDiscoveryAbstractMessage> pendingMessages() {
        return pendingMsgsMsg == null ? Collections.emptyList() : pendingMsgsMsg.messages();
    }

    /**
     * Sets pending messages.
     *
     * @param msgs Pending messages.
     */
    public void pendingMessages(@Nullable Collection<TcpDiscoveryAbstractMessage> msgs) {
        pendingMsgsMsg = F.isEmpty(msgs) ? null : new TcpDiscoveryCollectionMessage(msgs);
    }

    /** @return Message to transfer the pending messages. */
    public @Nullable TcpDiscoveryCollectionMessage pendingMessagesTransferMessage() {
        return pendingMsgsMsg;
    }

    /** @param pendingMsgsMsg Message to transfer the pending messages. */
    public void pendingMessagesTransferMessage(@Nullable TcpDiscoveryCollectionMessage pendingMsgsMsg) {
        this.pendingMsgsMsg = pendingMsgsMsg;
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
    public Map<Long, Collection<ClusterNode>> topologyHistory() {
        if (F.isEmpty(topHistMsgs))
            return Collections.emptyMap();

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
        return 21;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryNodeAddedMessage.class, this, "super", super.toString());
    }
}
