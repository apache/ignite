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

package org.apache.ignite.internal.processors.cache.persistence.wal.io;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferExpander;
import org.jetbrains.annotations.NotNull;

/**
 * File input, backed by byte buffer file input.
 * This class allows to read data by chunks from file and then read primitives
 */
public class SimpleFileInput implements FileInput {
    /**
     * Buffer for reading blocks of data into.
     * <b>Note:</b> biggest block requested from this input can't be longer than buffer capacity
     */
    private ByteBuffer buf;

    /** I/O interface for read/write operations with file */
    protected FileIO io;

    /** */
    private long pos;

    /** */
    private ByteBufferExpander expBuf;

    /**
     * @param io FileIO to read from.
     * @param buf Buffer for reading blocks of data into.
     */
    public SimpleFileInput(FileIO io, ByteBufferExpander buf) throws IOException {
        assert io != null;

        this.io = io;
        this.buf = buf.buffer();

        expBuf = buf;

        pos = io.position();

        clearBuffer();
    }

    /** {@inheritDoc} */
    @Override public FileIO io() {
        return io;
    }

    /**
     * Clear buffer.
     */
    private void clearBuffer() {
        buf.clear();
        buf.limit(0);

        assert buf.remaining() == 0; // Buffer is empty.
    }

    /** {@inheritDoc} */
    @Override public void seek(long pos) throws IOException {
        if (pos > io.size())
            throw new EOFException();

        io.position(pos);

        this.pos = pos;

        clearBuffer();
    }

    /**
     * @return Underlying buffer.
     */
    @Override public ByteBuffer buffer() {
        return buf;
    }


    /** {@inheritDoc} */
    @Override public void ensure(int requested) throws IOException {
        int available = buf.remaining();

        if (available >= requested)
            return;

        if (buf.capacity() < requested) {
            if (expBuf == null)
                throw new IOException("Requested size is greater than buffer: " + requested);

            buf = expBuf.expand(requested);

            assert available == buf.remaining();
        }

        buf.compact();

        do {
            int read = io.read(buf);

            if (read == -1)
                throw new EOFException("EOF at position [" + io.position() + "] expected to read [" + requested + "] bytes");

            available += read;

            pos += read;
        }
        while (available < requested);

        buf.flip();
    }

    /**
     * @return Position in the stream.
     */
    @Override public long position() {
        return pos - buf.remaining();
    }

    /**
     * {@inheritDoc}
     */
    @Override public void readFully(@NotNull byte[] b) throws IOException {
        ensure(b.length);

        buf.get(b);
    }

    /**
     * {@inheritDoc}
     */
    @Override public void readFully(@NotNull byte[] b, int off, int len) throws IOException {
        ensure(len);

        buf.get(b, off, len);
    }

    /**
     * {@inheritDoc}
     */
    @Override public int skipBytes(int n) throws IOException {
        if (buf.remaining() >= n)
            buf.position(buf.position() + n);
        else
            seek(pos + n);

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

    /** {@inheritDoc} */
    @Override public String readUTF() throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * @param skipCheck If CRC check should be skipped.
     * @return autoclosable fileInput, after its closing crc will be calculated and compared with saved one
     */
    @Override public Crc32CheckingFileInput startRead(boolean skipCheck) {
        return new Crc32CheckingFileInput(this, skipCheck);
    }
}
