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

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UTFDataFormatException;
import java.nio.charset.StandardCharsets;

import static org.apache.ignite.internal.util.GridUnsafe.BYTE_ARR_OFF;

/**
 * Hadoop data output for direct communication.
 */
public class HadoopDirectDataOutput extends OutputStream implements DataOutput {
    /** Flush size. */
    private final int flushSize;

    /** Data buffer. */
    private byte[] buf;

    /** Buffer size. */
    private int bufSize;

    /** Position. */
    private int pos;

    /**
     * Constructor.
     *
     * @param flushSize Flush size.
     */
    public HadoopDirectDataOutput(int flushSize) {
        this(flushSize, flushSize);
    }

    /**
     * Constructor.
     *
     * @param flushSize Flush size.
     * @param allocSize Allocation size.
     */
    public HadoopDirectDataOutput(int flushSize, int allocSize) {
        this.flushSize = flushSize;

        buf = new byte[allocSize];
        bufSize = allocSize;
    }

    /** {@inheritDoc} */
    @Override public void write(@NotNull byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    /** {@inheritDoc} */
    @Override public void write(@NotNull byte[] b, int off, int len) throws IOException {
        int writePos = ensure(len);

        System.arraycopy(b, off, buf, writePos, len);
    }

    /** {@inheritDoc} */
    @Override public void write(int val) throws IOException {
        writeByte(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(boolean val) throws IOException {
        writeByte(val ? (byte)1 : (byte)0);
    }

    /** {@inheritDoc} */
    @Override public void writeByte(int val) throws IOException {
        int writePos = ensure(1);

        buf[writePos] = (byte)val;
    }

    /** {@inheritDoc} */
    @Override public void writeShort(int val) throws IOException {
        int writePos = ensure(2);

        GridUnsafe.putShort(buf, BYTE_ARR_OFF + writePos, (short)val);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(int val) throws IOException {
        int writePos = ensure(2);

        GridUnsafe.putChar(buf, BYTE_ARR_OFF + writePos, (char)val);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int val) throws IOException {
        int writePos = ensure(4);

        GridUnsafe.putInt(buf, BYTE_ARR_OFF + writePos, val);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long val) throws IOException {
        int writePos = ensure(8);

        GridUnsafe.putLong(buf, BYTE_ARR_OFF + writePos, val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(float val) throws IOException {
        int writePos = ensure(4);

        GridUnsafe.putFloat(buf, BYTE_ARR_OFF + writePos, val);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(double val) throws IOException {
        int writePos = ensure(8);

        GridUnsafe.putDouble(buf, BYTE_ARR_OFF + writePos, val);
    }

    /** {@inheritDoc} */
    @Override public void writeBytes(@NotNull String str) throws IOException {
        for(int i = 0; i < str.length(); ++i)
            write((byte)str.charAt(i));
    }

    /** {@inheritDoc} */
    @Override public void writeChars(@NotNull String str) throws IOException {
        for (int i = 0; i < str.length(); ++i)
            writeChar(str.charAt(i));
    }

    /** {@inheritDoc} */
    @Override public void writeUTF(@NotNull String str) throws IOException {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);

        int len = bytes.length;

        if (len > 65535)
            throw new UTFDataFormatException("UTF8 form of string is longer than 65535 bytes: " + str);

        writeShort((short)len);
        write(bytes);
    }

    /**
     * @return Buffer.
     */
    public byte[] buffer() {
        return buf;
    }

    /**
     * @return Buffer length (how much memory is allocated).
     */
    public int bufferLength() {
        return bufSize;
    }

    /**
     * @return Position.
     */
    public int position() {
        return pos;
    }

    /**
     * @return Whether buffer is ready for flush.
     */
    public boolean readyForFlush() {
        return pos >= flushSize;
    }

    /**
     * Reset the stream.
     */
    public void reset() {
        pos = 0;
    }

    /**
     * Ensure that the given amount of bytes is available within the stream, then shift the position.
     *
     * @param cnt Count.
     * @return Position
     */
    private int ensure(int cnt) {
        int pos0 = pos;

        if (pos0 + cnt > bufSize)
            grow(pos0 + cnt);

        pos += cnt;

        return pos0;
    }

    /**
     * Grow array up to the given count.
     *
     * @param cnt Count.
     */
    private void grow(int cnt) {
        int bufSize0 = (int)(bufSize * 1.1);

        if (bufSize0 < cnt)
            bufSize0 = cnt;

        byte[] buf0 = new byte[bufSize0];

        System.arraycopy(buf, 0, buf0, 0, pos);

        buf = buf0;
        bufSize = bufSize0;
    }
}
