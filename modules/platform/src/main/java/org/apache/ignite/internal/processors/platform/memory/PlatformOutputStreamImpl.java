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

package org.apache.ignite.internal.processors.platform.memory;

import static org.apache.ignite.internal.processors.platform.memory.PlatformMemoryUtils.*;

/**
 * Interop output stream implementation.
 */
public class PlatformOutputStreamImpl implements PlatformOutputStream {
    /** Underlying memory chunk. */
    protected final PlatformMemory mem;

    /** Pointer. */
    protected long data;

    /** Maximum capacity. */
    protected int cap;

    /** Current position. */
    protected int pos;

    /**
     * Constructor.
     *
     * @param mem Underlying memory chunk.
     */
    public PlatformOutputStreamImpl(PlatformMemory mem) {
        this.mem = mem;

        data = mem.data();
        cap = mem.capacity();
    }

    /** {@inheritDoc} */
    @Override public void writeByte(byte val) {
        ensureCapacity(pos + 1);

        UNSAFE.putByte(data + pos++, val);
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(byte[] val) {
        copyAndShift(val, BYTE_ARR_OFF, val.length);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(boolean val) {
        writeByte(val ? (byte) 1 : (byte) 0);
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(boolean[] val) {
        copyAndShift(val, BOOLEAN_ARR_OFF, val.length);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(short val) {
        ensureCapacity(pos + 2);

        UNSAFE.putShort(data + pos, val);

        shift(2);
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(short[] val) {
        copyAndShift(val, SHORT_ARR_OFF, val.length << 1);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(char val) {
        ensureCapacity(pos + 2);

        UNSAFE.putChar(data + pos, val);

        shift(2);
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(char[] val) {
        copyAndShift(val, CHAR_ARR_OFF, val.length << 1);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int val) {
        ensureCapacity(pos + 4);

        UNSAFE.putInt(data + pos, val);

        shift(4);
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(int[] val) {
        copyAndShift(val, INT_ARR_OFF, val.length << 2);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int pos, int val) {
        ensureCapacity(pos + 4);

        UNSAFE.putInt(data + pos, val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(float val) {
        writeInt(Float.floatToIntBits(val));
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(float[] val) {
        copyAndShift(val, FLOAT_ARR_OFF, val.length << 2);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long val) {
        ensureCapacity(pos + 8);

        UNSAFE.putLong(data + pos, val);

        shift(8);
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(long[] val) {
        copyAndShift(val, LONG_ARR_OFF, val.length << 3);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(double val) {
        writeLong(Double.doubleToLongBits(val));
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(double[] val) {
        copyAndShift(val, DOUBLE_ARR_OFF, val.length << 3);
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] arr, int off, int len) {
        copyAndShift(arr, BYTE_ARR_OFF + off, len);
    }

    /** {@inheritDoc} */
    @Override public void write(long addr, int cnt) {
        copyAndShift(null, addr, cnt);
    }

    /** {@inheritDoc} */
    @Override public int position() {
        return pos;
    }

    /** {@inheritDoc} */
    @Override public void position(int pos) {
        ensureCapacity(pos);

        this.pos = pos;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public byte[] array() {
        assert false;

        throw new UnsupportedOperationException("Should not be called.");
    }

    /** {@inheritDoc} */
    @Override public byte[] arrayCopy() {
        assert false;

        throw new UnsupportedOperationException("Should not be called.");
    }

    /** {@inheritDoc} */
    @Override public long offheapPointer() {
        assert false;

        throw new UnsupportedOperationException("Should not be called.");
    }

    /** {@inheritDoc} */
    @Override public boolean hasArray() {
        assert false;

        throw new UnsupportedOperationException("Should not be called.");
    }

    /** {@inheritDoc} */
    @Override public void synchronize() {
        PlatformMemoryUtils.length(mem.pointer(), pos);
    }

    /**
     * Ensure capacity.
     *
     * @param reqCap Required byte count.
     */
    protected void ensureCapacity(int reqCap) {
        if (reqCap > cap) {
            int newCap = cap << 1;

            if (newCap < reqCap)
                newCap = reqCap;

            mem.reallocate(newCap);

            assert mem.capacity() >= newCap;

            data = mem.data();
            cap = newCap;
        }
    }

    /**
     * Shift position.
     *
     * @param cnt Byte count.
     */
    protected void shift(int cnt) {
        pos += cnt;
    }

    /**
     * Copy source object to the stream shifting position afterwards.
     *
     * @param src Source.
     * @param off Offset.
     * @param len Length.
     */
    private void copyAndShift(Object src, long off, int len) {
        ensureCapacity(pos + len);

        UNSAFE.copyMemory(src, off, null, data + pos, len);

        shift(len);
    }
}
