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

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Handshake response.
 */
public class TcpDiscoveryHandshakeResponse extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long order;

    /**
     * Constructor.
     *
     * @param creatorNodeId Creator node ID.
     * @param locNodeOrder Local node order.
     */
    public TcpDiscoveryHandshakeResponse(UUID creatorNodeId, long locNodeOrder) {
        super(creatorNodeId);

        order = locNodeOrder;
    }

    /**
     * @return DiscoveryDataPacketCompression flag.
     */
    public boolean isDiscoveryDataPacketCompression() {
        return getFlag(COMPRESS_DATA_PACKET);
    }

    /**
     * @param discoveryDataPacketCompression DiscoveryDataPacketCompression flag.
     */
    public void setDiscoveryDataPacketCompression(boolean discoveryDataPacketCompression) {
        setFlag(COMPRESS_DATA_PACKET, discoveryDataPacketCompression);
    }

    /**
     * Gets previous node alive flag.<br>
     * {@code True} means node has connectivity to it's previous node in a ring.
     *
     * @return previous node alive flag.
     */
    public boolean previousNodeAlive() {
        return getFlag(CHANGE_TOPOLOGY_FLAG_POS);
    }

    /**
     * Sets topology change flag.<br>
     * {@code True} means node has connectivity to it's previous node in a ring.
     *
     * @param prevNodeAlive previous node alive flag.
     */
    public void previousNodeAlive(boolean prevNodeAlive) {
        setFlag(CHANGE_TOPOLOGY_FLAG_POS, prevNodeAlive);
    }

    /**
     * Gets order of the node sent the response.
     *
     * @return Order of the node sent the response.
     */
    public long order() {
        return order;
    }

    /**
     * Sets order of the node sent the response.
     *
     * @param order Order of the node sent the response.
     */
    public void order(long order) {
        this.order = order;
    }

    /**
     * @return {@code True} if server supports client message acknowledge.
     */
    public boolean clientAck() {
        return getFlag(CLIENT_ACK_FLAG_POS);
    }

    /**
     * @param clientAck {@code True} if server supports client message acknowledge.
     */
    public void clientAck(boolean clientAck) {
        setFlag(CLIENT_ACK_FLAG_POS, clientAck);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryHandshakeResponse.class, this, "super", super.toString(),
            "isPreviousNodeAlive", previousNodeAlive());
    }
}