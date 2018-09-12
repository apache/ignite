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

import org.apache.ignite.internal.util.GridUnsafe;

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

        GridUnsafe.putByte(data + pos++, val);
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(byte[] val) {
        copyAndShift(val, GridUnsafe.BYTE_ARR_OFF, val.length);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(boolean val) {
        writeByte(val ? (byte) 1 : (byte) 0);
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(boolean[] val) {
        copyAndShift(val, GridUnsafe.BOOLEAN_ARR_OFF, val.length);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(short val) {
        ensureCapacity(pos + 2);

        GridUnsafe.putShort(data + pos, val);

        shift(2);
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(short[] val) {
        copyAndShift(val, GridUnsafe.SHORT_ARR_OFF, val.length << 1);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(char val) {
        ensureCapacity(pos + 2);

        GridUnsafe.putChar(data + pos, val);

        shift(2);
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(char[] val) {
        copyAndShift(val, GridUnsafe.CHAR_ARR_OFF, val.length << 1);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int val) {
        ensureCapacity(pos + 4);

        GridUnsafe.putInt(data + pos, val);

        shift(4);
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(int[] val) {
        copyAndShift(val, GridUnsafe.INT_ARR_OFF, val.length << 2);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(int pos, short val) {
        ensureCapacity(pos + 2);

        GridUnsafe.putShort(data + pos, val);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int pos, int val) {
        ensureCapacity(pos + 4);

        GridUnsafe.putInt(data + pos, val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(float val) {
        writeInt(Float.floatToIntBits(val));
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(float[] val) {
        copyAndShift(val, GridUnsafe.FLOAT_ARR_OFF, val.length << 2);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long val) {
        ensureCapacity(pos + 8);

        GridUnsafe.putLong(data + pos, val);

        shift(8);
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(long[] val) {
        copyAndShift(val, GridUnsafe.LONG_ARR_OFF, val.length << 3);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(double val) {
        writeLong(Double.doubleToLongBits(val));
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(double[] val) {
        copyAndShift(val, GridUnsafe.DOUBLE_ARR_OFF, val.length << 3);
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] arr, int off, int len) {
        copyAndShift(arr, GridUnsafe.BYTE_ARR_OFF + off, len);
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

        unsafePosition(pos);
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
    @Override public long rawOffheapPointer() {
        return data;
    }

    /** {@inheritDoc} */
    @Override public boolean hasArray() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void unsafeEnsure(int cap) {
        ensureCapacity(pos + cap);
    }

    /** {@inheritDoc} */
    @Override public void unsafePosition(int pos) {
        this.pos = pos;
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteByte(byte val) {
        GridUnsafe.putByte(data + pos++, val);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteBoolean(boolean val) {
        unsafeWriteByte(val ? (byte) 1 : (byte) 0);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteShort(short val) {
        GridUnsafe.putShort(data + pos, val);

        shift(2);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteShort(int pos, short val) {
        GridUnsafe.putShort(data + pos, val);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteChar(char val) {
        GridUnsafe.putChar(data + pos, val);

        shift(2);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteInt(int val) {
        GridUnsafe.putInt(data + pos, val);

        shift(4);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteInt(int pos, int val) {
        GridUnsafe.putInt(data + pos, val);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteLong(long val) {
        GridUnsafe.putLong(data + pos, val);

        shift(8);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteFloat(float val) {
        unsafeWriteInt(Float.floatToIntBits(val));
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteDouble(double val) {
        unsafeWriteLong(Double.doubleToLongBits(val));
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

        GridUnsafe.copyHeapOffheap(src, off, data + pos, len);

        shift(len);
    }

    /** {@inheritDoc} */
    @Override public int capacity() {
        return cap;
    }
}