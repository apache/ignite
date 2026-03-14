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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MarshallableMessage;
import org.apache.ignite.spi.discovery.tcp.internal.DiscoveryDataPacket;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.jetbrains.annotations.Nullable;

/**
 * TODO: Use NodeMessage for {@link TcpDiscoveryNode} and {@link ClusterNode} after https://issues.apache.org/jira/browse/IGNITE-27899
 * Message telling nodes that new node should be added to topology.
 * When newly added node receives the message it connects to its next and finishes
 * join process.
 */
@TcpDiscoveryEnsureDelivery
@TcpDiscoveryRedirectToClient
public class TcpDiscoveryNodeAddedMessage extends TcpDiscoveryAbstractTraceableMessage implements MarshallableMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Added node. */
    private TcpDiscoveryNode node;

    /** Marshalled {@link #node}. */
    @Order(0)
    @GridToStringExclude
    byte[] nodeBytes;

    /** */
    @Order(1)
    DiscoveryDataPacket dataPacket;

    /** Pending messages containner. */
    @Order(2)
    @Nullable TcpDiscoveryCollectionMessage pendingMsgsMsg;

    /** Current topology. Initialized by coordinator. */
    @GridToStringInclude
    private Collection<TcpDiscoveryNode> top;

    /** Marshalled {@link #top}. */
    @Order(3)
    @GridToStringExclude
    @Nullable byte[] topBytes;

    /** */
    @GridToStringInclude
    private transient Collection<TcpDiscoveryNode> clientTop;

    /** Topology snapshots history. */
    private Map<Long, Collection<ClusterNode>> topHist;

    /** Marshalled {@link #topHist}. */
    @Order(4)
    @GridToStringExclude
    @Nullable byte[] topHistBytes;

    /** Start time of the first grid node. */
    @Order(5)
    long gridStartTime;

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
        pendingMsgsMsg = msg.pendingMsgsMsg;
        top = msg.top;
        topBytes = msg.topBytes;
        clientTop = msg.clientTop;
        topHist = msg.topHist;
        topHistBytes = msg.topHistBytes;
        dataPacket = msg.dataPacket;
        gridStartTime = msg.gridStartTime;
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
    public @Nullable Collection<TcpDiscoveryAbstractMessage> messages() {
        return pendingMsgsMsg == null ? null : pendingMsgsMsg.messages();
    }

    /**
     * Sets pending messages to send to new node.
     *
     * @param msgs Pending messages to send to new node.
     */
    public void messages(@Nullable Collection<TcpDiscoveryAbstractMessage> msgs) {
        pendingMsgsMsg = F.isEmpty(msgs) ? null : new TcpDiscoveryCollectionMessage(msgs);
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
        topHistBytes = null;
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

    /** @param marsh marshaller. */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        nodeBytes = U.marshal(marsh, node);

        if (top != null)
            topBytes = U.marshal(marsh, top);

        if (topHist != null)
            topHistBytes = U.marshal(marsh, topHist);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
        node = U.unmarshal(marsh, nodeBytes, clsLdr);

        if (topBytes != null)
            top = U.unmarshal(marsh, topBytes, clsLdr);

        if (topHistBytes != null)
            topHist = U.unmarshal(marsh, topHistBytes, clsLdr);

        nodeBytes = null;
        topBytes = null;
        topHistBytes = null;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 29;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryNodeAddedMessage.class, this, "super", super.toString());
    }
}
