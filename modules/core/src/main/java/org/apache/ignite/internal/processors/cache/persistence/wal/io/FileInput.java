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

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferBackedDataInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.PureJavaCrc32;
import org.jetbrains.annotations.NotNull;

/**
 * File input, backed by byte buffer file input.
 * This class allows to read data by chunks from file and then read primitives.
 */
public interface FileInput extends ByteBufferBackedDataInput {
    /**
     * File I/O.
     */
    FileIO io();

    /**
     * @param pos Position in bytes from file begin.
     */
    void seek(long pos) throws IOException;

    /**
     * @return Position in the stream.
     */
    long position();

    /**
     * @param skipCheck If CRC check should be skipped.
     * @return autoclosable fileInput, after its closing crc32 will be calculated and compared with saved one
     */
    SimpleFileInput.Crc32CheckingFileInput startRead(boolean skipCheck);

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

        /** */
        private FileInput delegate;

        /**
         */
        public Crc32CheckingFileInput(FileInput delegate, boolean skipCheck) {
            this.delegate = delegate;
            this.lastCalcPosition = delegate.buffer().position();
            this.skipCheck = skipCheck;
        }

        /** {@inheritDoc} */
        @Override public void ensure(int requested) throws IOException {
            int available = buffer().remaining();

            if (available >= requested)
                return;

            updateCrc();

            delegate.ensure(requested);

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

            int oldPos = buffer().position();

            buffer().position(lastCalcPosition);

            crc32.update(delegate.buffer(), oldPos - lastCalcPosition);

            lastCalcPosition = oldPos;
        }

        /** {@inheritDoc} */
        @Override public int skipBytes(int n) throws IOException {
            ensure(n);

            int skipped = Math.min(buffer().remaining(), n);

            buffer().position(buffer().position() + skipped);

            return skipped;
        }

        /**
         * {@inheritDoc}
         */
        @Override public void readFully(@NotNull byte[] b) throws IOException {
            ensure(b.length);

            buffer().get(b);
        }

        /**
         * {@inheritDoc}
         */
        @Override public void readFully(@NotNull byte[] b, int off, int len) throws IOException {
            ensure(len);

            buffer().get(b, off, len);
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

            return buffer().get();
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

            return buffer().getShort();
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

            return buffer().getChar();
        }

        /**
         * {@inheritDoc}
         */
        @Override public int readInt() throws IOException {
            ensure(4);

            return buffer().getInt();
        }

        /**
         * {@inheritDoc}
         */
        @Override public long readLong() throws IOException {
            ensure(8);

            return buffer().getLong();
        }

        /**
         * {@inheritDoc}
         */
        @Override public float readFloat() throws IOException {
            ensure(4);

            return buffer().getFloat();
        }

        /**
         * {@inheritDoc}
         */
        @Override public double readDouble() throws IOException {
            ensure(8);

            return buffer().getDouble();
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
            return delegate.buffer();
        }
    }
}
