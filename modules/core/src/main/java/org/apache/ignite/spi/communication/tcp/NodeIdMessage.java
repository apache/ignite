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

package org.apache.ignite.spi.communication.tcp;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Node ID message.
 */
public class NodeIdMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Message body size (with message type) in bytes. */
    static final int MESSAGE_SIZE = 16;

    /** Full message size (with message type) in bytes. */
    public static final int MESSAGE_FULL_SIZE = MESSAGE_SIZE + DIRECT_TYPE_SIZE;

    /** */
    byte[] nodeIdBytes;

    /** */
    byte[] nodeIdBytesWithType;

    /** */
    public NodeIdMessage() {
        // No-op.
    }

    /**
     * @param nodeId Node ID.
     */
    NodeIdMessage(UUID nodeId) {
        assert nodeId != null;

        nodeIdBytes = U.uuidToBytes(nodeId);

        assert nodeIdBytes.length == MESSAGE_SIZE : "Node ID size must be " + MESSAGE_SIZE;

        nodeIdBytesWithType = new byte[MESSAGE_FULL_SIZE];

        nodeIdBytesWithType[0] = (byte)(TcpCommunicationSpi.NODE_ID_MSG_TYPE & 0xFF);
        nodeIdBytesWithType[1] = (byte)((TcpCommunicationSpi.NODE_ID_MSG_TYPE >> 8) & 0xFF);

        System.arraycopy(nodeIdBytes, 0, nodeIdBytesWithType, 2, nodeIdBytes.length);
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        assert nodeIdBytes.length == MESSAGE_SIZE;

        if (buf.remaining() < MESSAGE_FULL_SIZE)
            return false;

        TcpCommunicationSpi.writeMessageType(buf, directType());

        buf.put(nodeIdBytes);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        if (buf.remaining() < MESSAGE_SIZE)
            return false;

        nodeIdBytes = new byte[MESSAGE_SIZE];

        buf.get(nodeIdBytes);

        return true;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TcpCommunicationSpi.NODE_ID_MSG_TYPE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(NodeIdMessage.class, this);
    }
}
