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

package org.apache.ignite.spi.communication.tcp.messages;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;

import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.HANDSHAKE_WAIT_MSG_TYPE;

/**
 * Message requesting to wait until node's SPI context initialize.
 */
public class HandshakeWaitMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Full message size (with message type) in bytes. */
    public static final int MESSAGE_FULL_SIZE = DIRECT_TYPE_SIZE;

    /**
     * Default constructor required by {@link Message}.
     */
    public HandshakeWaitMessage() {
        // No-op.
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

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return HANDSHAKE_WAIT_MSG_TYPE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HandshakeWaitMessage.class, this);
    }

}
