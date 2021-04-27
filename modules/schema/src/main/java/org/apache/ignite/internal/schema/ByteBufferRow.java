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

package org.apache.ignite.internal.schema;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;

/**
 * Heap byte buffer-based row.
 */
public class ByteBufferRow implements BinaryRow {
    /** Row buffer. */
    private final ByteBuffer buf;

    /**
     * @param data Array representation of the row.
     */
    public ByteBufferRow(byte[] data) {
        this(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN));
    }

    /**
     * Constructor.
     *
     * @param buf Buffer representing the row.
     */
    public ByteBufferRow(ByteBuffer buf) {
        assert buf.order() == ByteOrder.LITTLE_ENDIAN;

        this.buf = buf;
    }

    /** {@inheritDoc} */
    @Override public int schemaVersion() {
        return Short.toUnsignedInt(readShort(SCHEMA_VERSION_OFFSET));
    }

    /** {@inheritDoc} */
    @Override public boolean hasValue() {
        short flags = readShort(FLAGS_FIELD_OFFSET);

        return (flags & (RowFlags.NULL_VALUE | RowFlags.TOMBSTONE)) == 0;
    }

    /** {@inheritDoc} */
    @Override public int hash() {
        return readInteger(KEY_HASH_FIELD_OFFSET);
    }

    /** {@inheritDoc} */
    @Override public void writeTo(OutputStream stream) throws IOException {
        WritableByteChannel channel = Channels.newChannel(stream);

        channel.write(buf);

        buf.rewind();
    }

    /** {@inheritDoc} */
    @Override public byte readByte(int off) {
        return (byte)(buf.get(off) & 0xFF);
    }

    /** {@inheritDoc} */
    @Override public short readShort(int off) {
        return (short)(buf.getShort(off) & 0xFFFF);
    }

    /** {@inheritDoc} */
    @Override public int readInteger(int off) {
        return buf.getInt(off);
    }

    /** {@inheritDoc} */
    @Override public long readLong(int off) {
        return buf.getLong(off);
    }

    /** {@inheritDoc} */
    @Override public float readFloat(int off) {
        return buf.getFloat(off);
    }

    /** {@inheritDoc} */
    @Override public double readDouble(int off) {
        return buf.getDouble(off);
    }

    /** {@inheritDoc} */
    @Override public byte[] readBytes(int off, int len) {
        try {
            byte[] res = new byte[len];

            buf.position(off);

            buf.get(res, 0, res.length);

            return res;
        }
        finally {
            buf.position(0);
        }
    }

    /** {@inheritDoc} */
    @Override public String readString(int off, int len) {
        return new String(buf.array(), off, len, StandardCharsets.UTF_8);
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer keySlice() {
        final int off = KEY_CHUNK_OFFSET;
        final int len = readInteger(off);

        try {
            return buf.limit(off + len).position(off).slice();
        }
        finally {
            buf.position(0); // Reset bounds.
            buf.limit(buf.capacity());
        }
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer valueSlice() {
        int off = KEY_CHUNK_OFFSET + readInteger(KEY_CHUNK_OFFSET);
        int len = hasValue() ? readInteger(off) : 0;

        try {
            return buf.limit(off + len).position(off).slice();
        }
        finally {
            buf.position(0); // Reset bounds.
            buf.limit(buf.capacity());
        }
    }
}
