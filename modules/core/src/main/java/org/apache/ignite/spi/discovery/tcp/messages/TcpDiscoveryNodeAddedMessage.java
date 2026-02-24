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
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
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
    public TcpDiscoveryNode node;

    /** Marshalled {@link #node}. */
    @Order(6)
    @GridToStringExclude
    byte[] nodeBytes;

    /** */
    @Order(7)
    public DiscoveryDataPacket dataPacket;

    /** Pending messages containner. */
    @Order(8)
    public @Nullable TcpDiscoveryCollectionMessage pendingMsgsMsg;

    /** Current topology. Initialized by coordinator. */
    @GridToStringInclude
    public @Nullable Collection<TcpDiscoveryNode> top;

    /** Marshalled {@link #top}. */
    @Order(9)
    @GridToStringExclude
    @Nullable byte[] topBytes;

    /** */
    @GridToStringInclude
    public transient Collection<TcpDiscoveryNode> clientTop;

    /** Topology snapshots history. */
    public Map<Long, Collection<ClusterNode>> topHist;

    /** Marshalled {@link #topHist}. */
    @Order(10)
    @GridToStringExclude
    @Nullable byte[] topHistBytes;

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
    public TcpDiscoveryNodeAddedMessage(UUID creatorNodeId,
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

        this.node = msg.node;
        this.nodeBytes = msg.nodeBytes;
        this.pendingMsgsMsg = msg.pendingMsgsMsg;
        this.top = msg.top;
        this.topBytes = msg.topBytes;
        this.clientTop = msg.clientTop;
        this.topHist = msg.topHist;
        this.topHistBytes = msg.topHistBytes;
        this.dataPacket = msg.dataPacket;
        this.gridStartTime = msg.gridStartTime;
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
     * Sets topology snapshots history.
     *
     * @param topHist Map with topology snapshots history.
     */
    public void topologyHistory(@Nullable Map<Long, Collection<ClusterNode>> topHist) {
        this.topHist = topHist;
        topHistBytes = null;
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

        if (topHist != null && topHistBytes == null) {
            try {
                topHistBytes = U.marshal(marsh, topHist);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to marshal topology history.", e);
            }
        }

        if(pendingMsgsMsg != null)
            pendingMsgsMsg.prepareMarshal(marsh);
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

        if (topHistBytes != null && topHist == null) {
            try {
                topHist = U.unmarshal(marsh, topHistBytes, clsLdr);

                topHistBytes = null;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to unmarshal topology history.", e);
            }
        }

        if(pendingMsgsMsg != null)
            pendingMsgsMsg.finishUnmarshal(marsh, clsLdr);
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
