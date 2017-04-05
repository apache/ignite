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

package org.apache.ignite.internal.managers.communication;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import java.nio.ByteBuffer;

/**
 *
 */
public class IgniteIoTestMessage implements Message {
    /** */
    private static byte FLAG_PROC_FROM_NIO = 1;

    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long id;

    /** */
    private byte flags;

    /** */
    private boolean req;

    /** */
    private byte payload[];

    /**
     *
     */
    IgniteIoTestMessage() {
        // No-op.
    }

    /**
     * @param id Message ID.
     * @param req Request flag.
     * @param payload Payload.
     */
    IgniteIoTestMessage(long id, boolean req, byte[] payload) {
        this.id = id;
        this.req = req;
        this.payload = payload;
    }

    /**
     * @return {@code True} if message should be processed from NIO thread
     * (otherwise message is submitted to system pool).
     */
    boolean processFromNioThread() {
        return isFlag(FLAG_PROC_FROM_NIO);
    }

    /**
     * @param procFromNioThread {@code True} if message should be processed from NIO thread.
     */
    void processFromNioThread(boolean procFromNioThread) {
        setFlag(procFromNioThread, FLAG_PROC_FROM_NIO);
    }

    /**
     * @param flags Flags.
     */
    public void flags(byte flags) {
        this.flags = flags;
    }

    /**
     * @return Flags.
     */
    public byte flags() {
        return flags;
    }

    /**
     * Sets flag mask.
     *
     * @param flag Set or clear.
     * @param mask Mask.
     */
    private void setFlag(boolean flag, int mask) {
        flags = flag ? (byte)(flags | mask) : (byte)(flags & ~mask);
    }

    /**
     * Reads flag mask.
     *
     * @param mask Mask to read.
     * @return Flag value.
     */
    private boolean isFlag(int mask) {
        return (flags & mask) != 0;
    }

    /**
     * @return {@code true} if this is request.
     */
    public boolean request() {
        return req;
    }

    /**
     * @return ID.
     */
    public long id() {
        return id;
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
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeLong("id", id))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeByteArray("payload", payload))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeBoolean("req", req))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                id = reader.readLong("id");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                payload = reader.readByteArray("payload");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                req = reader.readBoolean("req");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(IgniteIoTestMessage.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -43;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteIoTestMessage.class, this);
    }
}
