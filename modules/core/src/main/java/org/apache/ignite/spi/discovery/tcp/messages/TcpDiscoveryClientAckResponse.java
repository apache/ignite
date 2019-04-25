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

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 *
 */
public class TcpDiscoveryClientAckResponse extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final IgniteUuid msgId;

    /**
     * @param creatorNodeId Creator node ID.
     * @param msgId Message ID to ack.
     */
    public TcpDiscoveryClientAckResponse(UUID creatorNodeId, IgniteUuid msgId) {
        super(creatorNodeId);

        this.msgId = msgId;
    }

    /**
     * @return Acknowledged message ID.
     */
    public IgniteUuid messageId() {
        return msgId;
    }

    /** {@inheritDoc} */
    @Override public boolean traceLogLevel() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean highPriority() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryClientAckResponse.class, this, "super", super.toString());
    }
}