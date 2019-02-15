/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.spi.discovery.tcp.messages;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.tcp.internal.DiscoveryDataPacket;

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
    private DiscoveryDataPacket clientDiscoData;

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
    @Override public String toString() {
        return S.toString(TcpDiscoveryNodeAddFinishedMessage.class, this, "super", super.toString());
    }
}
