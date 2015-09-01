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
import java.util.UUID;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Message telling that client node is reconnecting to topology.
 */
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
    @Override public String toString() {
        return S.toString(TcpDiscoveryClientReconnectMessage.class, this, "super", super.toString());
    }
}