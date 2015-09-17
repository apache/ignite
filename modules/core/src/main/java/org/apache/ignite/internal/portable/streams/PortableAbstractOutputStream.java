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

package org.apache.ignite.internal.portable.streams;

/**
 * Base portable output stream.
 */
public abstract class PortableAbstractOutputStream extends PortableAbstractStream
    implements PortableOutputStream {
    /** Minimal capacity when it is reasonable to start doubling resize. */
    private static final int MIN_CAP = 256;

    /** {@inheritDoc} */
    @Override public void writeByte(byte val) {
        ensureCapacity(pos + 1);

        writeByteAndShift(val);
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(byte[] val) {
        ensureCapacity(pos + val.length);

        copyAndShift(val, BYTE_ARR_OFF, val.length);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(boolean val) {
        writeByte(val ? BYTE_ONE : BYTE_ZERO);
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(boolean[] val) {
        ensureCapacity(pos + val.length);

        copyAndShift(val, BOOLEAN_ARR_OFF, val.length);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(short val) {
        ensureCapacity(pos + 2);

        if (!LITTLE_ENDIAN)
            val = Short.reverseBytes(val);

        writeShortFast(val);

        shift(2);
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(short[] val) {
        int cnt = val.length << 1;

        ensureCapacity(pos + cnt);

        if (LITTLE_ENDIAN)
            copyAndShift(val, SHORT_ARR_OFF, cnt);
        else {
            for (short item : val)
                writeShortFast(Short.reverseBytes(item));

            shift(cnt);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeChar(char val) {
        ensureCapacity(pos + 2);

        if (!LITTLE_ENDIAN)
            val = Character.reverseBytes(val);

        writeCharFast(val);

        shift(2);
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(char[] val) {
        int cnt = val.length << 1;

        ensureCapacity(pos + cnt);

        if (LITTLE_ENDIAN)
            copyAndShift(val, CHAR_ARR_OFF, cnt);
        else {
            for (char item : val)
                writeCharFast(Character.reverseBytes(item));

            shift(cnt);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int val) {
        ensureCapacity(pos + 4);

        if (!LITTLE_ENDIAN)
            val = Integer.reverseBytes(val);

        writeIntFast(val);

        shift(4);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int pos, int val) {
        ensureCapacity(pos + 4);

        writeIntPositioned(pos, val);
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(int[] val) {
        int cnt = val.length << 2;

        ensureCapacity(pos + cnt);

        if (LITTLE_ENDIAN)
            copyAndShift(val, INT_ARR_OFF, cnt);
        else {
            for (int item : val)
                writeIntFast(Integer.reverseBytes(item));

            shift(cnt);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(float val) {
        writeInt(Float.floatToIntBits(val));
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(float[] val) {
        int cnt = val.length << 2;

        ensureCapacity(pos + cnt);

        if (LITTLE_ENDIAN)
            copyAndShift(val, FLOAT_ARR_OFF, cnt);
        else {
            for (float item : val) {
                writeIntFast(Integer.reverseBytes(Float.floatToIntBits(item)));

                shift(4);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long val) {
        ensureCapacity(pos + 8);

        if (!LITTLE_ENDIAN)
            val = Long.reverseBytes(val);

        writeLongFast(val);

        shift(8);
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(long[] val) {
        int cnt = val.length << 3;

        ensureCapacity(pos + cnt);

        if (LITTLE_ENDIAN)
            copyAndShift(val, LONG_ARR_OFF, cnt);
        else {
            for (long item : val)
                writeLongFast(Long.reverseBytes(item));

            shift(cnt);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(double val) {
        writeLong(Double.doubleToLongBits(val));
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(double[] val) {
        int cnt = val.length << 3;

        ensureCapacity(pos + cnt);

        if (LITTLE_ENDIAN)
            copyAndShift(val, DOUBLE_ARR_OFF, cnt);
        else {
            for (double item : val) {
                writeLongFast(Long.reverseBytes(Double.doubleToLongBits(item)));

                shift(8);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] arr, int off, int len) {
        ensureCapacity(pos + len);

        copyAndShift(arr, BYTE_ARR_OFF + off, len);
    }

    /** {@inheritDoc} */
    @Override public void write(long addr, int cnt) {
        ensureCapacity(pos + cnt);

        copyAndShift(null, addr, cnt);
    }

    /** {@inheritDoc} */
    @Override public void position(int pos) {
        ensureCapacity(pos);

        this.pos = pos;
    }

    /** {@inheritDoc} */
    @Override public long offheapPointer() {
        return 0;
    }

    /**
     * Calculate new capacity.
     *
     * @param curCap Current capacity.
     * @param reqCap Required capacity.
     * @return New capacity.
     */
    protected static int capacity(int curCap, int reqCap) {
        int newCap;

        if (reqCap < MIN_CAP)
            newCap = MIN_CAP;
        else {
            newCap = curCap << 1;

            if (newCap < reqCap)
                newCap = reqCap;
        }

        return newCap;
    }

    /**
     * Write next byte to the stream.
     *
     * @param val Value.
     */
    protected abstract void writeByteAndShift(byte val);

    /**
     * Copy source object to the stream shift position afterwards.
     *
     * @param src Source.
     * @param off Offset.
     * @param len Length.
     */
    protected abstract void copyAndShift(Object src, long off, int len);

    /**
     * Write short value (fast path).
     *
     * @param val Short value.
     */
    protected abstract void writeShortFast(short val);

    /**
     * Write char value (fast path).
     *
     * @param val Char value.
     */
    protected abstract void writeCharFast(char val);

    /**
     * Write int value (fast path).
     *
     * @param val Int value.
     */
    protected abstract void writeIntFast(int val);

    /**
     * Write long value (fast path).
     *
     * @param val Long value.
     */
    protected abstract void writeLongFast(long val);

    /**
     * Write int value to the given position.
     *
     * @param pos Position.
     * @param val Value.
     */
    protected abstract void writeIntPositioned(int pos, int val);

    /**
     * Ensure capacity.
     *
     * @param cnt Required byte count.
     */
    protected abstract void ensureCapacity(int cnt);
}