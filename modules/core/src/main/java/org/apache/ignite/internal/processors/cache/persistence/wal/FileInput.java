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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.PureJavaCrc32;
import org.jetbrains.annotations.NotNull;

/**
 * File input.
 */
public final class FileInput implements ByteBufferBackedDataInput {
    /** */
    private ByteBuffer buf;

    /** */
    private FileChannel ch;

    /** */
    private long pos;

    /**
     * @param ch  Channel.
     * @param buf Buffer.
     */
    public FileInput(FileChannel ch, ByteBuffer buf) throws IOException {
        assert ch != null;

        this.ch = ch;
        this.buf = buf;

        pos = ch.position();

        clearBuffer();
    }

    /**
     * Clear buffer.
     */
    private void clearBuffer() {
        buf.clear();
        buf.limit(0);

        assert buf.remaining() == 0; // Buffer is empty.
    }

    /**
     * @param pos Position in bytes from file begin.
     */
    public void seek(long pos) throws IOException {
        if (pos > ch.size())
            throw new EOFException();

        ch.position(pos);

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

        if (buf.capacity() < requested)
            throw new IOException("Requested size is greater than buffer: " + requested);

        buf.compact();

        do {
            int read = ch.read(buf);

            if (read == -1)
                throw new EOFException();

            available += read;

            pos += read;
        }
        while (available < requested);

        buf.flip();
    }

    /**
     * @return Position in the stream.
     */
    public long position() {
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

    /**
     * {@inheritDoc}
     */
    @Override public String readUTF() throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * @param skipCheck If CRC check should be skipped.
     * @return autoclosable fileInput, after its closing crc32 will be calculated and compared with saved one
     */
    public Crc32CheckingFileInput startRead(boolean skipCheck) {
        return new Crc32CheckingFileInput(buf.position(), skipCheck);
    }

    /**
     * Checking of CRC32.
     */
    public class Crc32CheckingFileInput implements ByteBufferBackedDataInput, AutoCloseable {
        /** */
        private final PureJavaCrc32 crc32 = new PureJavaCrc32();

        /** Last calc position. */
        private int lastCalcPosition;

        /** Skip crc check. */
        private boolean skipCheck;

        /**
         * @param position Position.
         */
        public Crc32CheckingFileInput(int position, boolean skipCheck) {
            this.lastCalcPosition = position;
            this.skipCheck = skipCheck;
        }

        /** {@inheritDoc} */
        @Override public void ensure(int requested) throws IOException {
            int available = buf.remaining();

            if (available >= requested)
                return;

            updateCrc();

            FileInput.this.ensure(requested);

            lastCalcPosition = 0;
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            updateCrc();

            int val = crc32.getValue();

            int writtenCrc =  this.readInt();

            if ((val ^ writtenCrc) != 0 && !skipCheck) {
                // If it last message we will skip it (EOF will be thrown).
                ensure(5);

                throw new IgniteDataIntegrityViolationException(
                    "val: " + val + " writtenCrc: " + writtenCrc
                );
            }
        }

        /**
         *
         */
        private void updateCrc() {
            if (skipCheck)
                return;

            int oldPos = buf.position();

            buf.position(lastCalcPosition);

            crc32.update(buf, oldPos - lastCalcPosition);

            lastCalcPosition = oldPos;
        }

        /** {@inheritDoc} */
        @Override public int skipBytes(int n) throws IOException {
            throw new UnsupportedOperationException();
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

        /** {@inheritDoc} */
        @Override public ByteBuffer buffer() {
            return FileInput.this.buffer();
        }
    }
}
