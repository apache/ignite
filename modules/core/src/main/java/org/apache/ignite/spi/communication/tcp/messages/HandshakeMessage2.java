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

package org.apache.ignite.spi.communication.tcp.messages;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.internal.IgniteCodeGeneratingFail;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Updated handshake message.
 */
@IgniteCodeGeneratingFail
public class HandshakeMessage2 extends HandshakeMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Message size in bytes including {@link HandshakeMessage} fields. */
    public static final int HANDSHAKE2_MESSAGE_SIZE = MESSAGE_FULL_SIZE + 4;

    /** */
    private int connIdx;

    /**
     *
     */
    public HandshakeMessage2() {
        // No-op.
    }

    /**
     * @param nodeId Node ID.
     * @param connectCnt Connect count.
     * @param rcvCnt Number of received messages.
     * @param connIdx Connection index.
     */
    public HandshakeMessage2(UUID nodeId, long connectCnt, long rcvCnt, int connIdx) {
        super(nodeId, connectCnt, rcvCnt);

        this.connIdx = connIdx;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -44;
    }

    /** {@inheritDoc} */
    @Override public int connectionIndex() {
        return connIdx;
    }

    /** {@inheritDoc} */
    @Override public int getMessageSize() {
        return HANDSHAKE2_MESSAGE_SIZE;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        if (!super.writeTo(buf, writer))
            return false;

        if (buf.remaining() < 4)
            return false;

        buf.putInt(connIdx);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        if (!super.readFrom(buf, reader))
            return false;

        if (buf.remaining() < 4)
            return false;

        connIdx = buf.getInt();

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HandshakeMessage2.class, this, super.toString());
    }
}
