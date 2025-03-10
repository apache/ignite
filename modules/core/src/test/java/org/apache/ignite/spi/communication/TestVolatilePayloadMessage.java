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

package org.apache.ignite.spi.communication;

import java.nio.ByteBuffer;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/** */
public class TestVolatilePayloadMessage implements Message {
    /** */
    public static final short DIRECT_TYPE = 210;

    /** */
    private int idx;

    /** */
    private byte[] payload;

    /** */
    private int payloadLen;

    /** */
    public TestVolatilePayloadMessage() {
        // No-op.
    }

    /** */
    public TestVolatilePayloadMessage(int idx, byte[] payload) {
        this.idx = idx;
        this.payload = payload;
        this.payloadLen = payload.length;
    }

    /** */
    public int index() {
        return idx;
    }

    /**
     * @return Network payload.
     */
    public byte[] payload() {
        return payload;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeInt(null, idx))
                    return false;

                writer.incrementState();
            case 1:
                if (!writer.writeInt(null, payloadLen))
                    return false;

                writer.incrementState();
            case 2:
                if (!writer.writeByteArray(null, payload))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        switch (reader.state()) {
            case 0:
                idx = reader.readInt(null);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                payloadLen = reader.readInt(null);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                if (buf.remaining() < payloadLen)
                    return false;

                payload = reader.readByteArray(null);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return DIRECT_TYPE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }
}
