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

package org.apache.ignite.internal.processors.hadoop.shuffle.direct;

import org.apache.ignite.internal.util.GridUnsafe;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.apache.ignite.internal.util.GridUnsafe.BYTE_ARR_OFF;

/**
 * Hadoop data input used for direct communication.
 */
public class HadoopDirectDataInput extends InputStream implements DataInput {
    /** Data buffer. */
    private final byte[] buf;

    /** Position. */
    private int pos;

    /**
     * Constructor.
     *
     * @param buf Buffer.
     */
    public HadoopDirectDataInput(byte[] buf) {
        this.buf = buf;
    }

    /** {@inheritDoc} */
    @Override public int read() throws IOException {
        return (int)readByte() & 0xFF;
    }

    /** {@inheritDoc} */
    @Override public void readFully(@NotNull byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    /** {@inheritDoc} */
    @Override public void readFully(@NotNull byte[] b, int off, int len) throws IOException {
        System.arraycopy(buf, pos, b, off, len);

        pos += len;
    }

    /** {@inheritDoc} */
    @Override public int skipBytes(int n) throws IOException {
        pos += n;

        return n;
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean() throws IOException {
        return readByte() == 1;
    }

    /** {@inheritDoc} */
    @Override public byte readByte() throws IOException {
        byte res = GridUnsafe.getByte(buf, BYTE_ARR_OFF + pos);

        pos += 1;

        return res;
    }

    /** {@inheritDoc} */
    @Override public int readUnsignedByte() throws IOException {
        return readByte() & 0xff;
    }

    /** {@inheritDoc} */
    @Override public short readShort() throws IOException {
        short res = GridUnsafe.getShort(buf, BYTE_ARR_OFF + pos);

        pos += 2;

        return res;
    }

    /** {@inheritDoc} */
    @Override public int readUnsignedShort() throws IOException {
        return readShort() & 0xffff;
    }

    /** {@inheritDoc} */
    @Override public char readChar() throws IOException {
        char res = GridUnsafe.getChar(buf, BYTE_ARR_OFF + pos);

        pos += 2;

        return res;
    }

    /** {@inheritDoc} */
    @Override public int readInt() throws IOException {
        int res = GridUnsafe.getInt(buf, BYTE_ARR_OFF + pos);

        pos += 4;

        return res;
    }

    /** {@inheritDoc} */
    @Override public long readLong() throws IOException {
        long res = GridUnsafe.getLong(buf, BYTE_ARR_OFF + pos);

        pos += 8;

        return res;
    }

    /** {@inheritDoc} */
    @Override public float readFloat() throws IOException {
        float res = GridUnsafe.getFloat(buf, BYTE_ARR_OFF + pos);

        pos += 4;

        return res;
    }

    /** {@inheritDoc} */
    @Override public double readDouble() throws IOException {
        double res = GridUnsafe.getDouble(buf, BYTE_ARR_OFF + pos);

        pos += 8;

        return res;
    }

    /** {@inheritDoc} */
    @Override public String readLine() throws IOException {
        // TODO: Create ticket!
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @NotNull @Override public String readUTF() throws IOException {
        byte[] bytes = new byte[readShort()];

        if (bytes.length != 0)
            readFully(bytes);

        return new String(bytes, StandardCharsets.UTF_8);
    }
}
