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

import org.apache.ignite.portable.*;

/**
 * Portable abstract input stream.
 */
public abstract class PortableAbstractInputStream extends PortableAbstractStream
    implements PortableInputStream {
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

        copyAndShift(res, BYTE_ARR_OFF, cnt);

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

        copyAndShift(res, BOOLEAN_ARR_OFF, cnt);

        return res;
    }

    /** {@inheritDoc} */
    @Override public short readShort() {
        ensureEnoughData(2);

        short res = readShortFast();

        shift(2);

        if (!LITTLE_ENDIAN)
            res = Short.reverseBytes(res);

        return res;
    }

    /** {@inheritDoc} */
    @Override public short[] readShortArray(int cnt) {
        int len = cnt << 1;

        ensureEnoughData(len);

        short[] res = new short[cnt];

        copyAndShift(res, SHORT_ARR_OFF, len);

        if (!LITTLE_ENDIAN) {
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

        if (!LITTLE_ENDIAN)
            res = Character.reverseBytes(res);

        return res;
    }

    /** {@inheritDoc} */
    @Override public char[] readCharArray(int cnt) {
        int len = cnt << 1;

        ensureEnoughData(len);

        char[] res = new char[cnt];

        copyAndShift(res, CHAR_ARR_OFF, len);

        if (!LITTLE_ENDIAN) {
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

        if (!LITTLE_ENDIAN)
            res = Integer.reverseBytes(res);

        return res;
    }

    /** {@inheritDoc} */
    @Override public int[] readIntArray(int cnt) {
        int len = cnt << 2;

        ensureEnoughData(len);

        int[] res = new int[cnt];

        copyAndShift(res, INT_ARR_OFF, len);

        if (!LITTLE_ENDIAN) {
            for (int i = 0; i < res.length; i++)
                res[i] = Integer.reverseBytes(res[i]);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public int readInt(int pos) {
        int delta = pos + 4 - this.pos;

        if (delta > 0)
            ensureEnoughData(delta);

        return readIntPositioned(pos);
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

        if (LITTLE_ENDIAN)
            copyAndShift(res, FLOAT_ARR_OFF, len);
        else {
            for (int i = 0; i < res.length; i++) {
                int x = readIntFast();

                shift(4);

                res[i] = Float.intBitsToFloat(Integer.reverseBytes(x));
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public long readLong() {
        ensureEnoughData(8);

        long res = readLongFast();

        shift(8);

        if (!LITTLE_ENDIAN)
            res = Long.reverseBytes(res);

        return res;
    }

    /** {@inheritDoc} */
    @Override public long[] readLongArray(int cnt) {
        int len = cnt << 3;

        ensureEnoughData(len);

        long[] res = new long[cnt];

        copyAndShift(res, LONG_ARR_OFF, len);

        if (!LITTLE_ENDIAN) {
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

        if (LITTLE_ENDIAN)
            copyAndShift(res, DOUBLE_ARR_OFF, len);
        else {
            for (int i = 0; i < res.length; i++) {
                long x = readLongFast();

                shift(8);

                res[i] = Double.longBitsToDouble(Long.reverseBytes(x));
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] arr, int off, int len) {
        if (len > remaining())
            len = remaining();

        copyAndShift(arr, BYTE_ARR_OFF + off, len);

        return len;
    }

    /** {@inheritDoc} */
    @Override public void position(int pos) {
        if (remaining() + this.pos < pos)
            throw new PortableException("Position is out of bounds: " + pos);
        else
            this.pos = pos;
    }

    /** {@inheritDoc} */
    @Override public long offheapPointer() {
        return 0;
    }

    /**
     * Ensure that there is enough data.
     *
     * @param cnt Length.
     */
    protected void ensureEnoughData(int cnt) {
        if (remaining() < cnt)
            throw new PortableException("Not enough data to read the value [position=" + pos +
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
     * Internal routine for positioned int value read.
     *
     * @param pos Position.
     * @return Int value.
     */
    protected abstract int readIntPositioned(int pos);
}
