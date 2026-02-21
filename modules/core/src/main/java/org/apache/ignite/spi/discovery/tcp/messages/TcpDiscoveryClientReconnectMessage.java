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
import java.util.Collections;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Message telling that client node is reconnecting to topology.
 */
@TcpDiscoveryEnsureDelivery
public class TcpDiscoveryClientReconnectMessage extends TcpDiscoveryAbstractMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** New router nodeID. */
    @Order(5)
    private UUID routerNodeId;

    /** Last message ID. */
    @Order(value = 6, method = "lastMessageId")
    private IgniteUuid lastMsgId;

    /** Pending messages. */
    @Order(value = 7, method = "pendingMessagesTransferMessage")
    @Nullable private TcpDiscoveryCollectionMessage pendingMsgsMsg;

    /** Constructor for {@link DiscoveryMessageFactory}. */
    public TcpDiscoveryClientReconnectMessage() {
        // No-op.
    }

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

    /** @param routerNodeId New router node ID. */
    public void routerNodeId(UUID routerNodeId) {
        this.routerNodeId = routerNodeId;
    }

    /**
     * @return Last message ID.
     */
    public IgniteUuid lastMessageId() {
        return lastMsgId;
    }

    /** @param lastMsgId Last message ID. */
    public void lastMessageId(IgniteUuid lastMsgId) {
        this.lastMsgId = lastMsgId;
    }

    /**
     * @param msgs Pending messages.
     */
    public void pendingMessages(@Nullable Collection<TcpDiscoveryAbstractMessage> msgs) {
        pendingMsgsMsg = msgs == null ? null : new TcpDiscoveryCollectionMessage(msgs);
    }

    /**
     * @return Pending messages.
     */
    public Collection<TcpDiscoveryAbstractMessage> pendingMessages() {
        return pendingMsgsMsg == null ? Collections.emptyList() : pendingMsgsMsg.messages();
    }

    /** @return Message to transfer the pending messages. */
    public @Nullable TcpDiscoveryCollectionMessage pendingMessagesTransferMessage() {
        return pendingMsgsMsg;
    }

    /** @param pendingMsgsMsg Message to transfer the pending messages. */
    public void pendingMessagesTransferMessage(@Nullable TcpDiscoveryCollectionMessage pendingMsgsMsg) {
        this.pendingMsgsMsg = pendingMsgsMsg;
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

        return Objects.equals(creatorNodeId(), other.creatorNodeId()) &&
            Objects.equals(routerNodeId, other.routerNodeId) &&
            Objects.equals(lastMsgId, other.lastMsgId);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 20;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryClientReconnectMessage.class, this, "super", super.toString());
    }
}
