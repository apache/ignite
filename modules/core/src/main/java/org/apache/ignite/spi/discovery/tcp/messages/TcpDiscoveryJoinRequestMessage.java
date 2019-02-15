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

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.tcp.internal.DiscoveryDataPacket;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;

/**
 * Initial message sent by a node that wants to enter topology.
 * Sent to random node during SPI start. Then forwarded directly to coordinator.
 */
public class TcpDiscoveryJoinRequestMessage extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** New node that wants to join the topology. */
    private final TcpDiscoveryNode node;

    /** Discovery data container. */
    private final DiscoveryDataPacket dataPacket;

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
     *
     */
    public DiscoveryDataPacket gridDiscoveryData() {
        return dataPacket;
    }

    /**
     * @return {@code true} flag.
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

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        // NOTE!
        // Do not call super. As IDs will differ, but we can ignore this.

        if (!(obj instanceof TcpDiscoveryJoinRequestMessage))
            return false;

        TcpDiscoveryJoinRequestMessage other = (TcpDiscoveryJoinRequestMessage)obj;

        return F.eqNodes(other.node, node);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryJoinRequestMessage.class, this, "super", super.toString());
    }
}
