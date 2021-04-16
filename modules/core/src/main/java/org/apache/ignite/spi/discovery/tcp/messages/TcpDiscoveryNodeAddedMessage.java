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
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.internal.DiscoveryDataPacket;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.jetbrains.annotations.Nullable;

/**
 * Message telling nodes that new node should be added to topology.
 * When newly added node receives the message it connects to its next and finishes
 * join process.
 */
@TcpDiscoveryEnsureDelivery
@TcpDiscoveryRedirectToClient
public class TcpDiscoveryNodeAddedMessage extends TcpDiscoveryAbstractTraceableMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Added node. */
    private final TcpDiscoveryNode node;

    /** */
    private DiscoveryDataPacket dataPacket;

    /** Pending messages from previous node. */
    private Collection<TcpDiscoveryAbstractMessage> msgs;

    /** Discarded message ID. */
    private IgniteUuid discardMsgId;

    /** Discarded message ID. */
    private IgniteUuid discardCustomMsgId;

    /** Current topology. Initialized by coordinator. */
    @GridToStringInclude
    private Collection<TcpDiscoveryNode> top;

    /** */
    @GridToStringInclude
    private transient Collection<TcpDiscoveryNode> clientTop;

    /** Topology snapshots history. */
    private Map<Long, Collection<ClusterNode>> topHist;

    /** Start time of the first grid node. */
    private final long gridStartTime;

    /**
     * Constructor.
     *
     * @param creatorNodeId Creator node ID.
     * @param node Node to add to topology.
     * @param dataPacket container for collecting discovery data across the cluster.
     * @param gridStartTime Start time of the first grid node.
     */
    public TcpDiscoveryNodeAddedMessage(UUID creatorNodeId,
        TcpDiscoveryNode node,
        DiscoveryDataPacket dataPacket,
        long gridStartTime)
    {
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

        this.node = msg.node;
        this.msgs = msg.msgs;
        this.discardMsgId = msg.discardMsgId;
        this.discardCustomMsgId = msg.discardCustomMsgId;
        this.top = msg.top;
        this.clientTop = msg.clientTop;
        this.topHist = msg.topHist;
        this.dataPacket = msg.dataPacket;
        this.gridStartTime = msg.gridStartTime;
    }

    /**
     * Gets newly added node.
     *
     * @return New node.
     */
    public TcpDiscoveryNode node() {
        return node;
    }

    /**
     * Gets pending messages sent to new node by its previous.
     *
     * @return Pending messages from previous node.
     */
    @Nullable public Collection<TcpDiscoveryAbstractMessage> messages() {
        return msgs;
    }

    /**
     * Gets discarded message ID.
     *
     * @return Discarded message ID.
     */
    @Nullable public IgniteUuid discardedMessageId() {
        return discardMsgId;
    }

    /**
     * Gets discarded custom message ID.
     *
     * @return Discarded message ID.
     */
    @Nullable public IgniteUuid discardedCustomMessageId() {
        return discardCustomMsgId;
    }

    /**
     * Sets pending messages to send to new node.
     *
     * @param msgs Pending messages to send to new node.
     * @param discardMsgId Discarded message ID.
     * @param discardCustomMsgId Discarded custom message ID.
     */
    public void messages(
        @Nullable Collection<TcpDiscoveryAbstractMessage> msgs,
        @Nullable IgniteUuid discardMsgId,
        @Nullable IgniteUuid discardCustomMsgId
    ) {
        this.msgs = msgs;
        this.discardMsgId = discardMsgId;
        this.discardCustomMsgId = discardCustomMsgId;
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
    }

    /**
     * @param top Topology at the moment when client joined.
     */
    public void clientTopology(Collection<TcpDiscoveryNode> top) {
        assert top != null && !top.isEmpty() : top;

        this.clientTop = top;
    }

    /**
     * @return Topology at the moment when client joined.
     */
    public Collection<TcpDiscoveryNode> clientTopology() {
        return clientTop;
    }

    /**
     * Gets topology snapshots history.
     *
     * @return Map with topology snapshots history.
     */
    public Map<Long, Collection<ClusterNode>> topologyHistory() {
        return topHist;
    }

    /**
     * Sets topology snapshots history.
     *
     * @param topHist Map with topology snapshots history.
     */
    public void topologyHistory(@Nullable Map<Long, Collection<ClusterNode>> topHist) {
        this.topHist = topHist;
    }

    /**
     * @return {@link DiscoveryDataPacket} carried by this message.
     */
    public DiscoveryDataPacket gridDiscoveryData() {
        return dataPacket;
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryNodeAddedMessage.class, this, "super", super.toString());
    }
}
