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
import org.apache.ignite.plugin.extensions.communication.Message;
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
public class TcpDiscoveryNodeAddedMessage extends TcpDiscoveryAbstractTraceableMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Added node. */
    @Order(6)
    public TcpDiscoveryNodeMessage nodeMsg;

    /** */
    @Order(7)
    public DiscoveryDataPacket dataPacket;

    /** Pending messages containner. */
    @Order(8)
    public @Nullable TcpDiscoveryCollectionMessage pendingMsgsMsg;

    /** Current topology. Initialized by coordinator. */
    @GridToStringInclude
    @Order(9)
    public @Nullable Collection<TcpDiscoveryNodeMessage> topMsgs;

    /** */
    @GridToStringInclude
    public transient Collection<TcpDiscoveryNode> clientTop;

    /** Topology snapshots history. */
    @Order(10)
    public Map<Long, ClusterNodeCollectionMessage> topHistMsgs;

    /** Start time of the first grid node. */
    @Order(11)
    public long gridStartTime;

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
        pendingMsgsMsg = msg.pendingMsgsMsg;
        topMsgs = msg.topMsgs;
        clientTop = msg.clientTop;
        topHistMsgs = msg.topHistMsgs;
        dataPacket = msg.dataPacket;
        gridStartTime = msg.gridStartTime;
    }

    /**
     * Sets pending messages to send to new node.
     *
     * @param msgs Pending messages to send to new node.
     */
    public void messages(@Nullable Collection<TcpDiscoveryAbstractMessage> msgs) {
        assert F.isEmpty(msgs) || msgs.stream().noneMatch(m -> m == this)
            : "Adding current message to its pending messages may issue infinite write/read message cycles and stack overflow.";

        pendingMsgsMsg = F.isEmpty(msgs) ? null : new TcpDiscoveryCollectionMessage(msgs);
    }

    /**
     * Sets topology.
     *
     * @param top Current topology.
     */
    public void topology(@Nullable Collection<TcpDiscoveryNode> top) {
        topMsgs = F.isEmpty(top) ? null : top.stream().map(TcpDiscoveryNodeMessage::new).collect(Collectors.toList());
    }

    /**
     * Gets topology.
     *
     * @return Current topology.
     */
    public Collection<TcpDiscoveryNode> topology() {
        return F.isEmpty(topMsgs)
            ? Collections.emptyList()
            : topMsgs.stream().map(TcpDiscoveryNode::new).collect(Collectors.toList());
    }

    /**
     * Gets topology history.
     *
     * @return Current toipology history.
     */
    public Map<Long, Collection<ClusterNode>> topologyHistory() {
        if (F.isEmpty(topHistMsgs))
            return Collections.emptyMap();

        Map<Long, Collection<ClusterNode>> res = U.newHashMap(topHistMsgs.size());

        topHistMsgs.forEach((i, nodesMsg) -> {
            Collection<ClusterNode> col = nodesMsg.clusterNodeMsgs.stream().map(m -> (ClusterNode)m).collect(Collectors.toList());

            res.put(i, col);
        });

        return res;
    }

    /**
     * Sets topology snapshots history.
     *
     * @param hist Map with topology snapshots history.
     */
    public void topologyHistory(@Nullable Map<Long, Collection<ClusterNode>> hist) {
        if (F.isEmpty(hist)) {
            topHistMsgs = null;

            return;
        }

        topHistMsgs = hist.entrySet().stream().map(e -> new AbstractMap.SimpleEntry<>(
                e.getKey(), new ClusterNodeCollectionMessage(e.getValue())))
            .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
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

    /** {@inheritDoc} */
    @Override public short directType() {
        return 24;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryNodeAddedMessage.class, this, "super", super.toString());
    }
}
