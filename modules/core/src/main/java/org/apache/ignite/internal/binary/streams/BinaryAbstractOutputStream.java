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

package org.apache.ignite.internal.binary.streams;

import org.apache.ignite.internal.util.GridUnsafe;

import static org.apache.ignite.internal.util.GridUnsafe.BIG_ENDIAN;

/**
 * Base binary output stream.
 */
public abstract class BinaryAbstractOutputStream extends BinaryAbstractStream
    implements BinaryOutputStream {
    /** Minimal capacity when it is reasonable to start doubling resize. */
    private static final int MIN_CAP = 256;

    /**
     * The maximum size of array to allocate.
     * Some VMs reserve some header words in an array.
     * Attempts to allocate larger arrays may result in
     * OutOfMemoryError: Requested array size exceeds VM limit
     * @see java.util.ArrayList#MAX_ARRAY_SIZE
     */
    protected static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    /** {@inheritDoc} */
    @Override public void writeByte(byte val) {
        ensureCapacity(pos + 1);

        writeByteAndShift(val);
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(byte[] val) {
        ensureCapacity(pos + val.length);

        copyAndShift(val, GridUnsafe.BYTE_ARR_OFF, val.length);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(boolean val) {
        writeByte(val ? BYTE_ONE : BYTE_ZERO);
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(boolean[] val) {
        ensureCapacity(pos + val.length);

        copyAndShift(val, GridUnsafe.BOOLEAN_ARR_OFF, val.length);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(short val) {
        ensureCapacity(pos + 2);

        writeShortFast(val);

        shift(2);
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(short[] val) {
        int cnt = val.length << 1;

        ensureCapacity(pos + cnt);

        if (BIG_ENDIAN) {
            for (short item : val) {
                writeShortFast(item);

                shift(2);
            }
        }
        else
            copyAndShift(val, GridUnsafe.SHORT_ARR_OFF, cnt);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(char val) {
        ensureCapacity(pos + 2);

        writeCharFast(val);

        shift(2);
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(char[] val) {
        int cnt = val.length << 1;

        ensureCapacity(pos + cnt);

        if (BIG_ENDIAN) {
            for (char item : val) {
                writeCharFast(item);

                shift(2);
            }
        }
        else
            copyAndShift(val, GridUnsafe.CHAR_ARR_OFF, cnt);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int val) {
        ensureCapacity(pos + 4);

        writeIntFast(val);

        shift(4);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(int pos, short val) {
        ensureCapacity(pos + 2);

        unsafeWriteShort(pos, val);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int pos, int val) {
        ensureCapacity(pos + 4);

        unsafeWriteInt(pos, val);
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(int[] val) {
        int cnt = val.length << 2;

        ensureCapacity(pos + cnt);

        if (BIG_ENDIAN) {
            for (int item : val) {
                writeIntFast(item);

                shift(4);
            }
        }
        else
            copyAndShift(val, GridUnsafe.INT_ARR_OFF, cnt);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(float val) {
        writeInt(Float.floatToIntBits(val));
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(float[] val) {
        int cnt = val.length << 2;

        ensureCapacity(pos + cnt);

        if (BIG_ENDIAN) {
            for (float item : val) {
                writeIntFast(Float.floatToIntBits(item));

                shift(4);
            }
        }
        else
            copyAndShift(val, GridUnsafe.FLOAT_ARR_OFF, cnt);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long val) {
        ensureCapacity(pos + 8);

        writeLongFast(val);

        shift(8);
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(long[] val) {
        int cnt = val.length << 3;

        ensureCapacity(pos + cnt);

        if (BIG_ENDIAN) {
            for (long item : val) {
                writeLongFast(item);

                shift(8);
            }
        }
        else
            copyAndShift(val, GridUnsafe.LONG_ARR_OFF, cnt);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(double val) {
        writeLong(Double.doubleToLongBits(val));
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(double[] val) {
        int cnt = val.length << 3;

        ensureCapacity(pos + cnt);

        if (BIG_ENDIAN) {
            for (double item : val) {
                writeLongFast(Double.doubleToLongBits(item));

                shift(8);
            }
        }
        else
            copyAndShift(val, GridUnsafe.DOUBLE_ARR_OFF, cnt);
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] arr, int off, int len) {
        ensureCapacity(pos + len);

        copyAndShift(arr, GridUnsafe.BYTE_ARR_OFF + off, len);
    }

    /** {@inheritDoc} */
    @Override public void write(long addr, int cnt) {
        ensureCapacity(pos + cnt);

        copyAndShift(null, addr, cnt);
    }

    /** {@inheritDoc} */
    @Override public void position(int pos) {
        ensureCapacity(pos);

        unsafePosition(pos);
    }

    /** {@inheritDoc} */
    @Override public long offheapPointer() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long rawOffheapPointer() {
        return 0;
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
    @Override public void unsafeWriteBoolean(boolean val) {
        unsafeWriteByte(val ? BYTE_ONE : BYTE_ZERO);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteFloat(float val) {
        unsafeWriteInt(Float.floatToIntBits(val));
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteDouble(double val) {
        unsafeWriteLong(Double.doubleToLongBits(val));
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
        else if (reqCap > MAX_ARRAY_SIZE)
            throw new IllegalArgumentException("Required capacity exceeds allowed. Required:" + reqCap);
        else {
            newCap = Math.max(curCap, MIN_CAP);

            while (newCap < reqCap) {
                newCap = newCap << 1;

                if (newCap < 0)
                    newCap = MAX_ARRAY_SIZE;
            }
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
     * Ensure capacity.
     *
     * @param cnt Required byte count.
     */
    protected abstract void ensureCapacity(int cnt);
}
