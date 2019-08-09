/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.ignite.internal.processors.tracing.messages.SpanContainer;
import org.apache.ignite.internal.processors.tracing.messages.TraceableMessage;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.tcp.internal.DiscoveryDataPacket;

/**
 * Sent by coordinator across the ring to finish node add process.
 */
@TcpDiscoveryEnsureDelivery
@TcpDiscoveryRedirectToClient
public class TcpDiscoveryNodeAddFinishedMessage extends TcpDiscoveryAbstractMessage implements TraceableMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Added node ID. */
    private final UUID nodeId;

    /**
     * Client node can not get discovery data from TcpDiscoveryNodeAddedMessage, we have to pass discovery data in
     * TcpDiscoveryNodeAddFinishedMessage
     */
    @GridToStringExclude
    private DiscoveryDataPacket clientDiscoData;

    /** */
    @GridToStringExclude
    private Map<String, Object> clientNodeAttrs;

    /** Span container. */
    private SpanContainer spanContainer = new SpanContainer();

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
        spanContainer = msg.spanContainer;
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

    /** {@inheritDoc} */
    @Override public SpanContainer spanContainer() {
        return spanContainer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryNodeAddFinishedMessage.class, this, "super", super.toString());
    }
}
