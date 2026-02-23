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

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
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

    /** Added node. */
    @Order(6)
    TcpDiscoveryNodeMessage nodeMsg;

    /** */
    @Order(value = 7, method = "gridDiscoveryData")
    private DiscoveryDataPacket dataPacket;

    /** Message to hold collection of pending {@link TcpDiscoveryAbstractMessage}. */
    @Order(8)
    TcpDiscoveryCollectionMessage msgs;

    /** Current topology. Initialized by coordinator. */
    @GridToStringInclude
    @Order(9)
    @Nullable ClusterNodeCollectionMessage top;

    /** */
    @GridToStringInclude
    private transient Collection<TcpDiscoveryNode> clientTop;

    /** Topology snapshots history. */
    @Order(10)
    @Nullable Map<Long, ClusterNodeCollectionMessage> topHistMsgs;

    /** Start time of the first grid node. */
    @Order(value = 11, method = "gridStartTime")
    private long gridStartTime;

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
    public TcpDiscoveryNodeAddedMessage(UUID creatorNodeId,
        TcpDiscoveryNode node,
        DiscoveryDataPacket dataPacket,
        long gridStartTime
    ) {
        super(creatorNodeId);

        assert node != null;
        assert gridStartTime > 0;

        nodeMsg = new TcpDiscoveryNodeMessage(node);
        this.dataPacket = dataPacket;
        this.gridStartTime = gridStartTime;
    }

    /**
     * @param msg Message.
     */
    public TcpDiscoveryNodeAddedMessage(TcpDiscoveryNodeAddedMessage msg) {
        super(msg);

        nodeMsg = msg.nodeMsg;
        top = msg.top;
        msgs = msg.msgs;
        clientTop = msg.clientTop;
        topHistMsgs = msg.topHistMsgs;
        dataPacket = msg.dataPacket;
        gridStartTime = msg.gridStartTime;
    }

    /**
     * Gets newly added node.
     *
     * @return New node.
     */
    public TcpDiscoveryNode node() {
        return new TcpDiscoveryNode(nodeMsg);
    }

    /**
     * Gets pending messages sent to new node by its previous.
     *
     * @return Pending messages from previous node.
     */
    public Collection<TcpDiscoveryAbstractMessage> messages() {
        return msgs == null ? Collections.emptyList() : msgs.messages();
    }

    /**
     * Sets pending messages.
     *
     * @param msgs Pending messages.
     */
    public void messages(@Nullable Collection<TcpDiscoveryAbstractMessage> msgs) {
        this.msgs = F.isEmpty(msgs) ? null : new TcpDiscoveryCollectionMessage(msgs);
    }

    /**
     * Gets topology.
     *
     * @return Current topology.
     */
    @Nullable public Collection<TcpDiscoveryNode> topology() {
        return top.clusterNodeMessages().stream().map(TcpDiscoveryNode::new).collect(Collectors.toList());
    }

    /**
     * Sets topology.
     *
     * @param top Current topology.
     */
    public void topology(@Nullable Collection<TcpDiscoveryNode> top) {
        this.top = top == null ? null : ClusterNodeCollectionMessage.of(top);
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
        if (F.isEmpty(topHistMsgs))
            return Collections.emptyMap();

        return topHistMsgs.entrySet().stream()
            .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), e.getValue().clusterNodes()))
            .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
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
            Collection<TcpDiscoveryNodeMessage> clusterNodeImpls = clusterNodes.stream().map(TcpDiscoveryNodeMessage::new)
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

    /** @param dataPacket {@link DiscoveryDataPacket} carried by this message. */
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
    @Override public void prepareMarshal(Marshaller marsh) {
        nodeMsg.prepareMarshal(marsh);

        if (msgs != null)
            msgs.prepareMarshal(marsh);

        if (top != null)
            top.prepareMarshal(marsh);

        if (!F.isEmpty(topHistMsgs))
            topHistMsgs.values().forEach(t -> t.prepareMarshal(marsh));
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) {
        nodeMsg.finishUnmarshal(marsh, clsLdr);

        if (msgs != null)
            msgs.finishUnmarshal(marsh, clsLdr);

        if (top != null)
            top.finishUnmarshal(marsh, clsLdr);

        if (!F.isEmpty(topHistMsgs))
            topHistMsgs.values().forEach(t -> t.finishUnmarshal(marsh, clsLdr));
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
