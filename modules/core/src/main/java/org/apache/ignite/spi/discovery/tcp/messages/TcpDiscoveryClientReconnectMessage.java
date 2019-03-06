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

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Message telling that client node is reconnecting to topology.
 */
@TcpDiscoveryEnsureDelivery
public class TcpDiscoveryClientReconnectMessage extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** New router nodeID. */
    private final UUID routerNodeId;

    /** Last message ID. */
    private final IgniteUuid lastMsgId;

    /** Pending messages. */
    @GridToStringExclude
    private Collection<TcpDiscoveryAbstractMessage> msgs;

    /**
     * @param creatorNodeId Creator node ID.
     * @param routerNodeId New router node ID.
     * @param lastMsgId Last message ID.
     */
    public TcpDiscoveryClientReconnectMessage(UUID creatorNodeId, UUID routerNodeId, IgniteUuid lastMsgId) {
        super(creatorNodeId);

        this.routerNodeId = routerNodeId;
        this.lastMsgId = lastMsgId;
    }

    /**
     * @return New router node ID.
     */
    public UUID routerNodeId() {
        return routerNodeId;
    }

    /**
     * @return Last message ID.
     */
    public IgniteUuid lastMessageId() {
        return lastMsgId;
    }

    /**
     * @param msgs Pending messages.
     */
    public void pendingMessages(Collection<TcpDiscoveryAbstractMessage> msgs) {
        this.msgs = msgs;
    }

    /**
     * @return Pending messages.
     */
    public Collection<TcpDiscoveryAbstractMessage> pendingMessages() {
        return msgs;
    }

    /**
     * @param success Success flag.
     */
    public void success(boolean success) {
        setFlag(CLIENT_RECON_SUCCESS_FLAG_POS, success);
    }

    /**
     * @return Success flag.
     */
    public boolean success() {
        return getFlag(CLIENT_RECON_SUCCESS_FLAG_POS);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        // NOTE!
        // Do not call super. As IDs will differ, but we can ignore this.

        if (!(obj instanceof TcpDiscoveryClientReconnectMessage))
            return false;

        TcpDiscoveryClientReconnectMessage other = (TcpDiscoveryClientReconnectMessage)obj;

        return F.eq(creatorNodeId(), other.creatorNodeId()) &&
            F.eq(routerNodeId, other.routerNodeId) &&
            F.eq(lastMsgId, other.lastMsgId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryClientReconnectMessage.class, this, "super", super.toString());
    }
}