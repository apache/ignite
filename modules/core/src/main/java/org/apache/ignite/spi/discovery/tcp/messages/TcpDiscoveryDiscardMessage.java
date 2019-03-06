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