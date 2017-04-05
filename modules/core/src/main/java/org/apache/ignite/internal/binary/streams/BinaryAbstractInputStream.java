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

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.util.GridUnsafe;

import static org.apache.ignite.internal.util.GridUnsafe.BIG_ENDIAN;

/**
 * Binary abstract input stream.
 */
public abstract class BinaryAbstractInputStream extends BinaryAbstractStream
    implements BinaryInputStream {
    /** Length of data inside array. */
    protected int len;

    /** {@inheritDoc} */
    @Override public byte readByte() {
        ensureEnoughData(1);

        return readByteAndShift();
    }

    /** {@inheritDoc} */
    @Override public byte[] readByteArray(int cnt) {
        ensureEnoughData(cnt);

        byte[] res = new byte[cnt];

        copyAndShift(res, GridUnsafe.BYTE_ARR_OFF, cnt);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean() {
        return readByte() == BYTE_ONE;
    }

    /** {@inheritDoc} */
    @Override public boolean[] readBooleanArray(int cnt) {
        ensureEnoughData(cnt);

        boolean[] res = new boolean[cnt];

        copyAndShift(res, GridUnsafe.BOOLEAN_ARR_OFF, cnt);

        return res;
    }

    /** {@inheritDoc} */
    @Override public short readShort() {
        ensureEnoughData(2);

        short res = readShortFast();

        shift(2);

        return res;
    }

    /** {@inheritDoc} */
    @Override public short[] readShortArray(int cnt) {
        int len = cnt << 1;

        ensureEnoughData(len);

        short[] res = new short[cnt];

        copyAndShift(res, GridUnsafe.SHORT_ARR_OFF, len);

        if (BIG_ENDIAN) {
            for (int i = 0; i < res.length; i++)
                res[i] = Short.reverseBytes(res[i]);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public char readChar() {
        ensureEnoughData(2);

        char res = readCharFast();

        shift(2);

        return res;
    }

    /** {@inheritDoc} */
    @Override public char[] readCharArray(int cnt) {
        int len = cnt << 1;

        ensureEnoughData(len);

        char[] res = new char[cnt];

        copyAndShift(res, GridUnsafe.CHAR_ARR_OFF, len);

        if (BIG_ENDIAN) {
            for (int i = 0; i < res.length; i++)
                res[i] = Character.reverseBytes(res[i]);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public int readInt() {
        ensureEnoughData(4);

        int res = readIntFast();

        shift(4);

        return res;
    }

    /** {@inheritDoc} */
    @Override public int[] readIntArray(int cnt) {
        int len = cnt << 2;

        ensureEnoughData(len);

        int[] res = new int[cnt];

        copyAndShift(res, GridUnsafe.INT_ARR_OFF, len);

        if (BIG_ENDIAN) {
            for (int i = 0; i < res.length; i++)
                res[i] = Integer.reverseBytes(res[i]);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public byte readBytePositioned(int pos) {
        int delta = pos + 1 - this.pos;

        if (delta > 0)
            ensureEnoughData(delta);

        return readBytePositioned0(pos);
    }

    /** {@inheritDoc} */
    @Override public short readShortPositioned(int pos) {
        int delta = pos + 2 - this.pos;

        if (delta > 0)
            ensureEnoughData(delta);

        return readShortPositioned0(pos);
    }

    /** {@inheritDoc} */
    @Override public int readIntPositioned(int pos) {
        int delta = pos + 4 - this.pos;

        if (delta > 0)
            ensureEnoughData(delta);

        return readIntPositioned0(pos);
    }

    /** {@inheritDoc} */
    @Override public float readFloat() {
        return Float.intBitsToFloat(readInt());
    }

    /** {@inheritDoc} */
    @Override public float[] readFloatArray(int cnt) {
        int len = cnt << 2;

        ensureEnoughData(len);

        float[] res = new float[cnt];

        if (BIG_ENDIAN) {
            for (int i = 0; i < res.length; i++) {
                int x = readIntFast();

                shift(4);

                res[i] = Float.intBitsToFloat(x);
            }
        }
        else
            copyAndShift(res, GridUnsafe.FLOAT_ARR_OFF, len);

        return res;
    }

    /** {@inheritDoc} */
    @Override public long readLong() {
        ensureEnoughData(8);

        long res = readLongFast();

        shift(8);

        return res;
    }

    /** {@inheritDoc} */
    @Override public long[] readLongArray(int cnt) {
        int len = cnt << 3;

        ensureEnoughData(len);

        long[] res = new long[cnt];

        copyAndShift(res, GridUnsafe.LONG_ARR_OFF, len);

        if (BIG_ENDIAN) {
            for (int i = 0; i < res.length; i++)
                res[i] = Long.reverseBytes(res[i]);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public double readDouble() {
        return Double.longBitsToDouble(readLong());
    }

    /** {@inheritDoc} */
    @Override public double[] readDoubleArray(int cnt) {
        int len = cnt << 3;

        ensureEnoughData(len);

        double[] res = new double[cnt];

        if (BIG_ENDIAN) {
            for (int i = 0; i < res.length; i++) {
                long x = readLongFast();

                shift(8);

                res[i] = Double.longBitsToDouble(x);
            }
        }
        else
            copyAndShift(res, GridUnsafe.DOUBLE_ARR_OFF, len);

        return res;
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] arr, int off, int len) {
        if (len > remaining())
            len = remaining();

        copyAndShift(arr, GridUnsafe.BYTE_ARR_OFF + off, len);

        return len;
    }

    /** {@inheritDoc} */
    @Override public void position(int pos) {
        if (remaining() + this.pos < pos)
            throw new BinaryObjectException("Position is out of bounds: " + pos);
        else
            this.pos = pos;
    }

    /** {@inheritDoc} */
    @Override public long offheapPointer() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long rawOffheapPointer() {
        return 0;
    }

    /**
     * Ensure that there is enough data.
     *
     * @param cnt Length.
     */
    protected void ensureEnoughData(int cnt) {
        if (remaining() < cnt)
            throw new BinaryObjectException("Not enough data to read the value [position=" + pos +
                ", requiredBytes=" + cnt + ", remainingBytes=" + remaining() + ']');
    }

    /**
     * Read next byte from the stream and perform shift.
     *
     * @return Next byte.
     */
    protected abstract byte readByteAndShift();

    /**
     * Copy data to target object shift position afterwards.
     *
     * @param target Target.
     * @param off Offset.
     * @param len Length.
     */
    protected abstract void copyAndShift(Object target, long off, int len);

    /**
     * Read short value (fast path).
     *
     * @return Short value.
     */
    protected abstract short readShortFast();

    /**
     * Read char value (fast path).
     *
     * @return Char value.
     */
    protected abstract char readCharFast();

    /**
     * Read int value (fast path).
     *
     * @return Int value.
     */
    protected abstract int readIntFast();

    /**
     * Read long value (fast path).
     *
     * @return Long value.
     */
    protected abstract long readLongFast();

    /**
     * Internal routine for positioned byte value read.
     *
     * @param pos Position.
     * @return Int value.
     */
    protected abstract byte readBytePositioned0(int pos);

    /**
     * Internal routine for positioned short value read.
     *
     * @param pos Position.
     * @return Int value.
     */
    protected abstract short readShortPositioned0(int pos);

    /**
     * Internal routine for positioned int value read.
     *
     * @param pos Position.
     * @return Int value.
     */
    protected abstract int readIntPositioned0(int pos);
}
