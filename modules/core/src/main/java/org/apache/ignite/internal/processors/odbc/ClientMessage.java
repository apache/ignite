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
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

@IgniteCodeGeneratingFail
public class ClientMessage implements Message, Externalizable {
    /** */
    private static final long serialVersionUID = -4609408156037304495L;

    /** */
    private byte[] data;

    /** */
    private BinaryHeapOutputStream stream;

    /** */
    private int cnt = -4;

    /** */
    private int msgSize;

    /** */
    public ClientMessage() {}

    /** */
    public ClientMessage(byte[] data) {
        this.data = data;
    }

    /** */
    public ClientMessage(BinaryHeapOutputStream stream) {
        this.stream = stream;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter ignored) {
        assert stream != null || data != null;

        byte[] data = stream != null ? stream.array() : this.data;
        int msgSize = stream != null ? stream.position() : data.length;

        if (cnt < 0) {
            for (; cnt < 0 && buf.hasRemaining(); cnt++)
                buf.put((byte) ((msgSize >> (8 * (4 + cnt))) & 0xFF));

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
        if (cnt < 0) {
            for (; cnt < 0 && buf.hasRemaining(); cnt++)
                msgSize |= (buf.get() & 0xFF) << (8 * (4 + cnt));

            if (cnt < 0)
                return false;

            data = new byte[msgSize];
        }

        assert data != null;
        assert cnt >= 0;
        assert msgSize > 0;

        int remaining = buf.remaining();

        if (remaining > 0) {
            int missing = msgSize - cnt;

            if (missing > 0) {
                int len = Math.min(missing, remaining);

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

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op
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
