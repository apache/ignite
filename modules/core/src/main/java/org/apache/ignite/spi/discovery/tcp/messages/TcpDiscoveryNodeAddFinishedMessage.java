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

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.spi.discovery.tcp.internal.DiscoveryDataPacket;
import org.jetbrains.annotations.Nullable;

/**
 * Sent by coordinator across the ring to finish node add process.
 */
@TcpDiscoveryEnsureDelivery
@TcpDiscoveryRedirectToClient
public class TcpDiscoveryNodeAddFinishedMessage extends TcpDiscoveryAbstractTraceableMessage implements TcpDiscoveryMarshallableMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Added node ID. */
    @Order(6)
    UUID nodeId;

    /**
     * Client node can not get discovery data from TcpDiscoveryNodeAddedMessage, we have to pass discovery data in
     * TcpDiscoveryNodeAddFinishedMessage.
     */
    @Order(7)
    @GridToStringExclude DiscoveryDataPacket clientDiscoData;

    /** */
    @GridToStringExclude
    private Map<String, Object> clientNodeAttrs;

    /** Serialized client node attributes. */
    @Order(8)
    @Nullable byte[] clientNodeAttrsBytes;

    /** Constructor. */
    public TcpDiscoveryNodeAddFinishedMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param creatorNodeId ID of the creator node (coordinator).
     * @param nodeId Added node ID.
     */
    public TcpDiscoveryNodeAddFinishedMessage(UUID creatorNodeId, UUID nodeId) {
        super(creatorNodeId);

        this.nodeId = nodeId;
    }

    /**
     * @param msg Message.
     */
    public TcpDiscoveryNodeAddFinishedMessage(TcpDiscoveryNodeAddFinishedMessage msg) {
        super(msg);

        nodeId = msg.nodeId;
        clientDiscoData = msg.clientDiscoData;
        clientNodeAttrs = msg.clientNodeAttrs;
    }

    /**
     * Gets ID of the node added.
     *
     * @return ID of the node added.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId ID of the node added.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return Discovery data for joined client.
     */
    public DiscoveryDataPacket clientDiscoData() {
        return clientDiscoData;
    }

    /**
     * @param clientDiscoData Discovery data for joined client.
     */
    public void clientDiscoData(DiscoveryDataPacket clientDiscoData) {
        this.clientDiscoData = clientDiscoData;

        assert clientDiscoData == null || !clientDiscoData.hasDataFromNode(nodeId);
    }

    /**
     * @return Client node attributes.
     */
    public Map<String, Object> clientNodeAttributes() {
        return clientNodeAttrs;
    }

    /**
     * @param clientNodeAttrs New client node attributes.
     */
    public void clientNodeAttributes(Map<String, Object> clientNodeAttrs) {
        this.clientNodeAttrs = clientNodeAttrs;
    }

    /**
     * @return Serialized client node attributes.
     */
    public @Nullable byte[] clientNodeAttributesBytes() {
        return clientNodeAttrsBytes;
    }

    /**
     * @param clientNodeAttrsBytes Serialized client node attributes.
     */
    public void clientNodeAttributesBytes(@Nullable byte[] clientNodeAttrsBytes) {
        this.clientNodeAttrsBytes = clientNodeAttrsBytes;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) {
        if (clientNodeAttrs != null && clientNodeAttrsBytes == null) {
            try {
                clientNodeAttrsBytes = U.marshal(marsh, clientNodeAttrs);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to marshal client node attributes", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) {
        if (F.isEmpty(clientNodeAttrsBytes))
            clientNodeAttrs = null;
        else {
            try {
                clientNodeAttrs = U.unmarshal(marsh, clientNodeAttrsBytes, clsLdr);

                clientNodeAttrsBytes = null;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to unmarshal client node attributes", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryNodeAddFinishedMessage.class, this, "super", super.toString());
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 19;
    }
}
