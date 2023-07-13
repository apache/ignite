/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;

/**
 * Checking of CRC32.
 */
public class Crc32CheckingDataInput extends ByteBufferBackedDataInputImpl implements AutoCloseable {
    /** */
    private final FastCrc crc = new FastCrc();

    /**
     * Last calc position.
     */
    private int lastCalcPosition;

    /**
     * Skip crc check.
     */
    private final boolean skipCheck;

    /** */
    private final ByteBufferBackedDataInput delegate;

    /** */
    public Crc32CheckingDataInput(ByteBufferBackedDataInput delegate, boolean skipCheck) {
        this.delegate = delegate;

        lastCalcPosition = buffer().position();

        this.skipCheck = skipCheck;
    }

    /**
     * {@inheritDoc}
     */
    @Override public void ensure(int requested) throws IOException {
        int available = buffer().remaining();

        if (available >= requested)
            return;

        updateCrc();

        delegate.ensure(requested);

        lastCalcPosition = 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override public void close() throws Exception {
        updateCrc();

        int val = crc.getValue();

        int writtenCrc = readInt();

        if ((val ^ writtenCrc) != 0 && !skipCheck) {
            // If it is a last message we will skip it (EOF will be thrown).
            ensure(5);

            throw new IgniteDataIntegrityViolationException(
                "val: " + val + " writtenCrc: " + writtenCrc
            );
        }
    }

    /** */
    private void updateCrc() {
        if (skipCheck)
            return;

        int oldPos = buffer().position();

        buffer().position(lastCalcPosition);

        crc.update(delegate.buffer(), oldPos - lastCalcPosition);

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
    @Override public void readFully(byte[] b) throws IOException {
        ensure(b.length);

        buffer().get(b);
    }

    /**
     * {@inheritDoc}
     */
    @Override public void readFully(byte[] b, int off, int len) throws IOException {
        ensure(len);

        buffer().get(b, off, len);
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
    @Override public short readShort() throws IOException {
        ensure(2);

        return buffer().getShort();
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

    /** {@inheritDoc} */
    @Override public ByteBuffer buffer() {
        return delegate.buffer();
    }
}
