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

import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Message telling nodes that new node should be added to topology.
 * When newly added node receives the message it connects to its next and finishes
 * join process.
 */
@TcpDiscoveryEnsureDelivery
@TcpDiscoveryRedirectToClient
public class TcpDiscoveryNodeAddedMessage extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Added node. */
    private TcpDiscoveryNode node;

    /** Pending messages from previous node. */
    private Collection<TcpDiscoveryAbstractMessage> msgs;

    /** Discarded message ID. */
    private IgniteUuid discardMsgId;

    /** Current topology. Initialized by coordinator. */
    @GridToStringInclude
    private Collection<TcpDiscoveryNode> top;

    /** Topology snapshots history. */
    private Map<Long, Collection<ClusterNode>> topHist;

    /** Discovery data from new node. */
    private Map<Integer, Object> newNodeDiscoData;

    /** Discovery data from old nodes. */
    private Map<UUID, Map<Integer, Object>> oldNodesDiscoData;

    /** Start time of the first grid node. */
    private long gridStartTime;

    /**
     * Public default no-arg constructor for {@link Externalizable} interface.
     */
    public TcpDiscoveryNodeAddedMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param creatorNodeId Creator node ID.
     * @param node Node to add to topology.
     * @param newNodeDiscoData New Node discovery data.
     * @param gridStartTime Start time of the first grid node.
     */
    public TcpDiscoveryNodeAddedMessage(UUID creatorNodeId, TcpDiscoveryNode node,
        Map<Integer, Object> newNodeDiscoData,
        long gridStartTime)
    {
        super(creatorNodeId);

        assert node != null;
        assert gridStartTime > 0;

        this.node = node;
        this.newNodeDiscoData = newNodeDiscoData;
        this.gridStartTime = gridStartTime;

        oldNodesDiscoData = new LinkedHashMap<>();
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
     * Sets pending messages to send to new node.
     *
     * @param msgs Pending messages to send to new node.
     * @param discardMsgId Discarded message ID.
     */
    public void messages(@Nullable Collection<TcpDiscoveryAbstractMessage> msgs, @Nullable IgniteUuid discardMsgId) {
        this.msgs = msgs;
        this.discardMsgId = discardMsgId;
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
     * Gets topology snapshots history.
     *
     * @return Map with topology snapshots history.
     */
    @Nullable public Map<Long, Collection<ClusterNode>> topologyHistory() {
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
     * @return Discovery data from new node.
     */
    public Map<Integer, Object> newNodeDiscoveryData() {
        return newNodeDiscoData;
    }

    /**
     * @return Discovery data from old nodes.
     */
    public Map<UUID, Map<Integer, Object>> oldNodesDiscoveryData() {
        return oldNodesDiscoData;
    }

    /**
     * @param discoData Discovery data to add.
     */
    public void addDiscoveryData(UUID nodeId, Map<Integer, Object> discoData) {
        // Old nodes disco data may be null if message
        // makes more than 1 pass due to stopping of the nodes in topology.
        if (oldNodesDiscoData != null)
            oldNodesDiscoData.put(nodeId, discoData);
    }

    /**
     * Clears discovery data to minimize message size.
     */
    public void clearDiscoveryData() {
        newNodeDiscoData = null;
        oldNodesDiscoData = null;
    }

    /**
     * @return First grid node start time.
     */
    public long gridStartTime() {
        return gridStartTime;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(node);
        U.writeCollection(out, msgs);
        U.writeGridUuid(out, discardMsgId);
        U.writeCollection(out, top);
        U.writeMap(out, topHist);
        out.writeLong(gridStartTime);
        U.writeMap(out, newNodeDiscoData);

        out.writeInt(oldNodesDiscoData != null ? oldNodesDiscoData.size() : -1);

        if (oldNodesDiscoData != null) {
            for (Map.Entry<UUID, Map<Integer, Object>> entry : oldNodesDiscoData.entrySet()) {
                U.writeUuid(out, entry.getKey());

                U.writeMap(out, entry.getValue());
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        node = (TcpDiscoveryNode)in.readObject();
        msgs = U.readCollection(in);
        discardMsgId = U.readGridUuid(in);
        top = U.readCollection(in);
        topHist = U.readTreeMap(in);
        gridStartTime = in.readLong();
        newNodeDiscoData = U.readMap(in);

        int oldNodesDiscoDataSize = in.readInt();

        if (oldNodesDiscoDataSize >= 0) {
            oldNodesDiscoData = new LinkedHashMap<>(oldNodesDiscoDataSize);

            for (int i = 0; i < oldNodesDiscoDataSize; i++) {
                UUID nodeId = U.readUuid(in);

                oldNodesDiscoData.put(nodeId, U.<Integer, Object>readMap(in));
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryNodeAddedMessage.class, this, "super", super.toString());
    }
}
