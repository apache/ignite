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

package org.apache.ignite.internal.processors.odbc;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.IgniteCodeGeneratingFail;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/** */
@IgniteCodeGeneratingFail
public class ClientMessage implements Message, Externalizable {
    /** */
    private static final long serialVersionUID = -4609408156037304495L;

    /** Limit handshake size to 64 MiB. */
    private static final int MAX_HANDSHAKE_SIZE = 64 * 1024 * 1024;

    /** First 3 bytes in handshake are either 1 1 0 (handshake = 1, major version = 1)... */
    private static final int HANDSHAKE_HEADER = 1 + (1 << 8);

    /** ...or 1 2 0 (handshake = 1, major version = 2). */
    private static final int HANDSHAKE_HEADER2 = 1 + (2 << 8);

    /** */
    private final boolean isFirstMessage;

    /** */
    private byte[] data;

    /** */
    private BinaryOutputStream stream;

    /** */
    private int cnt = -4;

    /** */
    private int msgSize;

    /** */
    private int firstMessageHeader;

    /** */
    public ClientMessage() {
        isFirstMessage = false;
    }

    /** */
    public ClientMessage(boolean isFirstMessage) {
        this.isFirstMessage = isFirstMessage;
    }

    /** */
    public ClientMessage(byte[] data) {
        //noinspection AssignmentOrReturnOfFieldWithMutableType
        this.data = data;
        isFirstMessage = false;
    }

    /** */
    public ClientMessage(BinaryOutputStream stream) {
        this.stream = stream;
        isFirstMessage = false;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter ignored) {
        assert stream != null || data != null;

        byte[] data = stream != null ? stream.array() : this.data;
        int msgSize = stream != null ? stream.position() : data.length;

        if (cnt < 0) {
            for (; cnt < 0 && buf.hasRemaining(); cnt++)
                buf.put((byte)((msgSize >> (8 * (4 + cnt))) & 0xFF));

            if (cnt < 0)
                return false;
        }

        assert cnt >= 0;
        assert msgSize > 0;

        int remaining = buf.remaining();

        if (remaining > 0) {
            int missing = msgSize - cnt;

            if (missing > 0) {
                int len = Math.min(missing, remaining);

                buf.put(data, cnt, len);

                cnt += len;
            }
        }

        if (cnt == msgSize) {
            cnt = -4;

            if (stream != null) {
                U.closeQuiet(stream);

                stream = null;
            }

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        throw new UnsupportedOperationException();
    }

    /**
     * Reads this message from provided byte buffer.
     *
     * @param buf Byte buffer.
     * @return Whether message was fully read.
     */
    public boolean readFrom(ByteBuffer buf) throws IOException {
        if (cnt < 0) {
            for (; cnt < 0 && buf.hasRemaining(); cnt++)
                msgSize |= (buf.get() & 0xFF) << (8 * (4 + cnt));

            if (cnt < 0)
                return false;

            if (msgSize <= 0)
                throw new IOException("Message size must be greater than 0: " + msgSize);

            if (isFirstMessage && msgSize > MAX_HANDSHAKE_SIZE)
                throw new IOException("Client handshake size limit exceeded: " + msgSize + " > " + MAX_HANDSHAKE_SIZE);
        }

        assert cnt >= 0;
        assert msgSize > 0;

        int remaining = buf.remaining();

        if (remaining > 0) {
            int missing = msgSize - cnt;

            if (missing > 0) {
                int len = Math.min(missing, remaining);

                if (isFirstMessage && data == null) {
                    // Sanity check: first 3 bytes in handshake are always 1 1 0 (handshake = 1, major version = 1).
                    // Do not allocate the buffer before validating the header to protect us from garbage data sent by unrelated application
                    // connecting on our port by accident.
                    while (len > 0 && cnt < 3) {
                        firstMessageHeader |= (buf.get() & 0xFF) << (8 * cnt);
                        cnt++;
                        len--;
                    }

                    if (cnt < 3)
                        return false;

                    if (firstMessageHeader != HANDSHAKE_HEADER && firstMessageHeader != HANDSHAKE_HEADER2)
                        throw new IOException("Handshake header check failed: " + firstMessageHeader);

                    // Header is valid, create buffer and set first bytes.
                    data = new byte[msgSize];
                    data[0] = 1;
                    data[1] = (byte)(firstMessageHeader >> 8);
                }

                if (data == null)
                    data = new byte[msgSize];

                buf.get(data, cnt, len);

                cnt += len;
            }
        }

        if (cnt == msgSize) {
            cnt = -4;
            msgSize = 0;

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return Short.MIN_VALUE;
    }

    /**
     * @return Message payload.
     */
    public byte[] payload() {
        if (stream != null) {
            data = stream.arrayCopy();
            U.closeQuiet(stream);
            stream = null;
        }

        //noinspection AssignmentOrReturnOfFieldWithMutableType
        return data;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        byte[] data = payload();
        out.writeInt(data.length);
        out.write(data);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        data = new byte[in.readInt()];
        in.read(data, 0, data.length);
    }
}
