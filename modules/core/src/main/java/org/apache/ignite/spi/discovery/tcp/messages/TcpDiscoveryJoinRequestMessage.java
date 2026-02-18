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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.internal.DiscoveryDataPacket;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;

import static org.apache.ignite.internal.util.lang.ClusterNodeFunc.eqNodes;

/**
 * Initial message sent by a node that wants to enter topology.
 * Sent to random node during SPI start. Then forwarded directly to coordinator.
 */
public class TcpDiscoveryJoinRequestMessage extends TcpDiscoveryAbstractTraceableMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** New node that wants to join the topology. */
    private TcpDiscoveryNode node;

    /** Serialized {@link #node}. */
    // TODO Remove the field after completing https://issues.apache.org/jira/browse/IGNITE-27899.
    @Order(6)
    private byte[] nodeBytes;

    /** Discovery data container. */
    @Order(value = 7, method = "gridDiscoveryData")
    private DiscoveryDataPacket dataPacket;

    /** Constructor. */
    public TcpDiscoveryJoinRequestMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param node New node that wants to join.
     * @param dataPacket Discovery data.
     */
    public TcpDiscoveryJoinRequestMessage(TcpDiscoveryNode node, DiscoveryDataPacket dataPacket) {
        super(node.id());

        this.node = node;
        this.dataPacket = dataPacket;
    }

    /**
     * Gets new node that wants to join the topology.
     *
     * @return Node that wants to join the topology.
     */
    public TcpDiscoveryNode node() {
        return node;
    }

    /**
     * @return Serialized node.
     */
    public byte[] nodeBytes() {
        return nodeBytes;
    }

    /**
     * @param nodeBytes Serialized node.
     */
    public void nodeBytes(byte[] nodeBytes) {
        this.nodeBytes = nodeBytes;
    }

    /** @return Discovery data container that collects data from all cluster nodes. */
    public DiscoveryDataPacket gridDiscoveryData() {
        return dataPacket;
    }

    /**
     * @param dataPacket Discovery data container that collects data from all cluster nodes.
     */
    public void gridDiscoveryData(DiscoveryDataPacket dataPacket) {
        this.dataPacket = dataPacket;
    }

    /**
     * @return Responded flag.
     */
    public boolean responded() {
        return getFlag(RESPONDED_FLAG_POS);
    }

    /**
     * @param responded Responded flag.
     */
    public void responded(boolean responded) {
        setFlag(RESPONDED_FLAG_POS, responded);
    }

    /**
     * @param marsh Marshaller.
     */
    public void prepareMarshal(Marshaller marsh) {
        if (node != null && nodeBytes == null) {
            try {
                nodeBytes = U.marshal(marsh, node);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to marshal node", e);
            }
        }
    }

    /**
     * @param marsh Marshaller.
     * @param clsLdr Class loader.
     */
    public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) {
        if (nodeBytes != null && node == null) {
            try {
                node = U.unmarshal(marsh, nodeBytes, clsLdr);

                nodeBytes = null;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to unmarshal node", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        // NOTE!
        // Do not call super. As IDs will differ, but we can ignore this.

        if (!(obj instanceof TcpDiscoveryJoinRequestMessage))
            return false;

        TcpDiscoveryJoinRequestMessage other = (TcpDiscoveryJoinRequestMessage)obj;

        return eqNodes(other.node, node);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryJoinRequestMessage.class, this, "super", super.toString());
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 20;
    }
}
