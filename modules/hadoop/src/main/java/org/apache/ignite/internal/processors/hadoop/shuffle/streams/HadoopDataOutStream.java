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

import java.io.DataOutput;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;

import static org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory.BYTE_ARR_OFF;
import static org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory.UNSAFE;

/**
 * Data output stream.
 */
public class HadoopDataOutStream extends OutputStream implements DataOutput {
    /** */
    private final HadoopOffheapBuffer buf = new HadoopOffheapBuffer(0, 0);

    /** */
    private final GridUnsafeMemory mem;

    /**
     * @param mem Memory.
     */
    public HadoopDataOutStream(GridUnsafeMemory mem) {
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
     * @return Old pointer or {@code 0} if move was impossible.
     */
    public long move(long size) {
        return buf.move(size);
    }

    /** {@inheritDoc} */
    @Override public void write(int b) {
        writeByte(b);
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] b) {
        write(b, 0, b.length);
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] b, int off, int len) {
        UNSAFE.copyMemory(b, BYTE_ARR_OFF + off, null, move(len), len);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(boolean v) {
        writeByte(v ? 1 : 0);
    }

    /** {@inheritDoc} */
    @Override public void writeByte(int v) {
        mem.writeByte(move(1), (byte)v);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(int v) {
        mem.writeShort(move(2), (short)v);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(int v) {
        writeShort(v);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int v) {
        mem.writeInt(move(4), v);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long v) {
        mem.writeLong(move(8), v);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(float v) {
        mem.writeFloat(move(4), v);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(double v) {
        mem.writeDouble(move(8), v);
    }

    /** {@inheritDoc} */
    @Override public void writeBytes(String s) {
        writeUTF(s);
    }

    /** {@inheritDoc} */
    @Override public void writeChars(String s) {
        writeUTF(s);
    }

    /** {@inheritDoc} */
    @Override public void writeUTF(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);

        writeInt(b.length);
        write(b);
    }
}