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
 * Message sent by coordinator when some operation handling is over. All receiving
 * nodes should discard this and all preceding messages in local buffers.
 */
public class TcpDiscoveryDiscardMessage extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** ID of the message to discard (this and all preceding). */
    private final IgniteUuid msgId;

    /** True if this is discard ID for custom event message. */
    private final boolean customMsgDiscard;

    /**
     * Constructor.
     *
     * @param creatorNodeId Creator node ID.
     * @param msgId Message ID.
     * @param customMsgDiscard Flag indicating whether the ID to discard is for a custom message or not.
     */
    public TcpDiscoveryDiscardMessage(UUID creatorNodeId, IgniteUuid msgId, boolean customMsgDiscard) {
        super(creatorNodeId);

        this.msgId = msgId;
        this.customMsgDiscard = customMsgDiscard;
    }

    /**
     * Gets message ID to discard (this and all preceding).
     *
     * @return Message ID.
     */
    public IgniteUuid msgId() {
        return msgId;
    }

    /**
     * Flag indicating whether the ID to discard is for a custom message or not.
     *
     * @return Custom message flag.
     */
    public boolean customMessageDiscard() {
        return customMsgDiscard;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryDiscardMessage.class, this, "super", super.toString());
    }
}