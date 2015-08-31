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
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Sent by coordinator across the ring to finish node add process.
 */
@TcpDiscoveryEnsureDelivery
@TcpDiscoveryRedirectToClient
public class TcpDiscoveryNodeAddFinishedMessage extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Added node ID. */
    private final UUID nodeId;

    /**
     * Client node can not get discovery data from TcpDiscoveryNodeAddedMessage, we have to pass discovery data in
     * TcpDiscoveryNodeAddFinishedMessage
     */
    @GridToStringExclude
    private Map<UUID, Map<Integer, byte[]>> clientDiscoData;

    /** */
    @GridToStringExclude
    private Map<String, Object> clientNodeAttrs;

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
    public Map<UUID, Map<Integer, byte[]>> clientDiscoData() {
        return clientDiscoData;
    }

    /**
     * @param clientDiscoData Discovery data for joined client.
     */
    public void clientDiscoData(@Nullable Map<UUID, Map<Integer, byte[]>> clientDiscoData) {
        this.clientDiscoData = clientDiscoData;

        assert clientDiscoData == null || !clientDiscoData.containsKey(nodeId);
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
    @Override public String toString() {
        return S.toString(TcpDiscoveryNodeAddFinishedMessage.class, this, "super", super.toString());
    }
}