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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.jetbrains.annotations.NotNull;

/**
 * Byte buffer backed data input.
 */
public class ByteBufferBackedDataInputImpl implements ByteBufferBackedDataInput {
    /** Buffer. */
    private ByteBuffer buf;

    /**
     * @param buf New buffer.
     */
    public ByteBufferBackedDataInput buffer(ByteBuffer buf) {
        this.buf = buf;

        return this;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer buffer() {
        return buf;
    }

    /** {@inheritDoc} */
    @Override public void ensure(int requested) throws IOException {
        if (buf.remaining() < requested)
            throw new IOException("Requested size is greater than buffer: " + requested);
    }

    /** {@inheritDoc} */
    @Override public void readFully(@NotNull byte[] b) throws IOException {
        ensure(b.length);

        buf.get(b);
    }

    /** {@inheritDoc} */
    @Override public void readFully(@NotNull byte[] b, int off, int len) throws IOException {
        ensure(b.length);

        buf.get(b, off, len);
    }

    /**
     * {@inheritDoc}
     */
    @Override public int skipBytes(int n) throws IOException {
        ensure(n);

        buf.position(buf.position() + n);

        return n;
    }

    /**
     * {@inheritDoc}
     */
    @Override public boolean readBoolean() throws IOException {
        return readByte() == 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override public byte readByte() throws IOException {
        ensure(1);

        return buf.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override public int readUnsignedByte() throws IOException {
        return readByte() & 0xFF;
    }

    /**
     * {@inheritDoc}
     */
    @Override public short readShort() throws IOException {
        ensure(2);

        return buf.getShort();
    }

    /**
     * {@inheritDoc}
     */
    @Override public int readUnsignedShort() throws IOException {
        return readShort() & 0xFFFF;
    }

    /**
     * {@inheritDoc}
     */
    @Override public char readChar() throws IOException {
        ensure(2);

        return buf.getChar();
    }

    /**
     * {@inheritDoc}
     */
    @Override public int readInt() throws IOException {
        ensure(4);

        return buf.getInt();
    }

    /**
     * {@inheritDoc}
     */
    @Override public long readLong() throws IOException {
        ensure(8);

        return buf.getLong();
    }

    /**
     * {@inheritDoc}
     */
    @Override public float readFloat() throws IOException {
        ensure(4);

        return buf.getFloat();
    }

    /**
     * {@inheritDoc}
     */
    @Override public double readDouble() throws IOException {
        ensure(8);

        return buf.getDouble();
    }

    /**
     * {@inheritDoc}
     */
    @Override public String readLine() throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override public String readUTF() throws IOException {
        throw new UnsupportedOperationException();
    }
}
