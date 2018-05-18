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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Updated handshake message.
 */
public class HandshakeMessage3 extends HandshakeMessage2 {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private boolean compressionFlag;

    /**
     * Default constructor required by {@link Message}.
     */
    public HandshakeMessage3() {
        // No-op.
    }

    /**
     * @param nodeId Node ID.
     * @param connectCnt Connect count.
     * @param rcvCnt Number of received messages.
     * @param connIdx Connection index.
     * @param compressionFlag Compression flag.
     */
    public HandshakeMessage3(UUID nodeId, long connectCnt, long rcvCnt, int connIdx, boolean compressionFlag) {
        super(nodeId, connectCnt, rcvCnt, connIdx);

        this.compressionFlag = compressionFlag;
    }

    /**
     * @return Compression flag.
     */
    public boolean compressionFlag() {
        return compressionFlag;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 136;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        if (!super.writeTo(buf, writer))
            return false;

        if (buf.remaining() < 1)
            return false;

        buf.put((byte)(compressionFlag ? 1 : 0));

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        if (!super.readFrom(buf, reader))
            return false;

        if (buf.remaining() < 1)
            return false;

        compressionFlag = buf.get() == 1;

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HandshakeMessage3.class, this);
    }
}
