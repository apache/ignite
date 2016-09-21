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

package org.apache.ignite.internal.processors.hadoop.shuffle.streams;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;

/**
 * Data input stream.
 */
public class HadoopDataInStream extends InputStream implements DataInput {
    /** */
    private final HadoopOffheapBuffer buf = new HadoopOffheapBuffer(0, 0);

    /** */
    private final GridUnsafeMemory mem;

    /**
     * @param mem Memory.
     */
    public HadoopDataInStream(GridUnsafeMemory mem) {
        assert mem != null;

        this.mem = mem;
    }

    /**
     * @return Buffer.
     */
    public HadoopOffheapBuffer buffer() {
        return buf;
    }

    /**
     * @param size Size.
     * @return Old pointer.
     */
    protected long move(long size) throws IOException {
        long ptr = buf.move(size);

        assert ptr != 0;

        return ptr;
    }

    /** {@inheritDoc} */
    @Override public int read() throws IOException {
        return readUnsignedByte();
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] b, int off, int len) throws IOException {
        readFully(b, off, len);

        return len;
    }

    /** {@inheritDoc} */
    @Override public long skip(long n) throws IOException {
        move(n);

        return n;
    }

    /** {@inheritDoc} */
    @Override public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    /** {@inheritDoc} */
    @Override public void readFully(byte[] b, int off, int len) throws IOException {
        mem.readBytes(move(len), b, off, len);
    }

    /** {@inheritDoc} */
    @Override public int skipBytes(int n) throws IOException {
        move(n);

        return n;
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean() throws IOException {
        byte res = readByte();

        if (res == 1)
            return true;

        assert res == 0 : res;

        return false;
    }

    /** {@inheritDoc} */
    @Override public byte readByte() throws IOException {
        return mem.readByte(move(1));
    }

    /** {@inheritDoc} */
    @Override public int readUnsignedByte() throws IOException {
        return readByte() & 0xff;
    }

    /** {@inheritDoc} */
    @Override public short readShort() throws IOException {
        return mem.readShort(move(2));
    }

    /** {@inheritDoc} */
    @Override public int readUnsignedShort() throws IOException {
        return readShort() & 0xffff;
    }

    /** {@inheritDoc} */
    @Override public char readChar() throws IOException {
        return (char)readShort();
    }

    /** {@inheritDoc} */
    @Override public int readInt() throws IOException {
        return mem.readInt(move(4));
    }

    /** {@inheritDoc} */
    @Override public long readLong() throws IOException {
        return mem.readLong(move(8));
    }

    /** {@inheritDoc} */
    @Override public float readFloat() throws IOException {
        return mem.readFloat(move(4));
    }

    /** {@inheritDoc} */
    @Override public double readDouble() throws IOException {
        return mem.readDouble(move(8));
    }

    /** {@inheritDoc} */
    @Override public String readLine() throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String readUTF() throws IOException {
        byte[] bytes = new byte[readInt()];

        if (bytes.length != 0)
            readFully(bytes);

        return new String(bytes, StandardCharsets.UTF_8);
    }
}