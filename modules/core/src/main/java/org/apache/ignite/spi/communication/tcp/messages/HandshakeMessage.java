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

package org.apache.ignite.spi.communication.tcp.messages;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.internal.IgniteCodeGeneratingFail;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;

/**
 * Handshake message.
 */
@IgniteCodeGeneratingFail
public class HandshakeMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Message body size in bytes. */
    private static final int MESSAGE_SIZE = 32;

    /** Full message size (with message type) in bytes. */
    public static final int MESSAGE_FULL_SIZE = MESSAGE_SIZE + DIRECT_TYPE_SIZE;

    /** */
    private UUID nodeId;

    /** */
    private long rcvCnt;

    /** */
    private long connectCnt;

    /**
     * Default constructor required by {@link Message}.
     */
    public HandshakeMessage() {
        // No-op.
    }

    /**
     * @param nodeId Node ID.
     * @param connectCnt Connect count.
     * @param rcvCnt Number of received messages.
     */
    public HandshakeMessage(UUID nodeId, long connectCnt, long rcvCnt) {
        assert nodeId != null;
        assert rcvCnt >= 0 : rcvCnt;

        this.nodeId = nodeId;
        this.connectCnt = connectCnt;
        this.rcvCnt = rcvCnt;
    }

    /**
     * @return Connection index.
     */
    public int connectionIndex() {
        return 0;
    }

    /**
     * @return Connect count.
     */
    public long connectCount() {
        return connectCnt;
    }

    /**
     * @return Number of received messages.
     */
    public long received() {
        return rcvCnt;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        if (buf.remaining() < MESSAGE_FULL_SIZE)
            return false;

        TcpCommunicationSpi.writeMessageType(buf, directType());

        byte[] bytes = U.uuidToBytes(nodeId);

        assert bytes.length == 16 : bytes.length;

        buf.put(bytes);

        buf.putLong(rcvCnt);

        buf.putLong(connectCnt);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        if (buf.remaining() < MESSAGE_SIZE)
            return false;

        byte[] nodeIdBytes = new byte[NodeIdMessage.MESSAGE_SIZE];

        buf.get(nodeIdBytes);

        nodeId = U.bytesToUuid(nodeIdBytes, 0);

        rcvCnt = buf.getLong();

        connectCnt = buf.getLong();

        return true;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TcpCommunicationSpi.HANDSHAKE_MSG_TYPE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HandshakeMessage.class, this);
    }
}
