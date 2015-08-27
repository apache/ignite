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

import org.apache.ignite.*;

import static org.apache.ignite.internal.processors.platform.memory.PlatformMemoryUtils.*;

/**
 * Interop input stream implementation.
 */
public class PlatformInputStreamImpl implements PlatformInputStream {
    /** Underlying memory. */
    private final PlatformMemory mem;

    /** Real data pointer */
    private long data;

    /** Amount of available data. */
    private int len;

    /** Current position. */
    private int pos;

    /** Heap-copied data. */
    private byte[] dataCopy;

    /**
     * Constructor.
     *
     * @param mem Underlying memory chunk.
     */
    public PlatformInputStreamImpl(PlatformMemory mem) {
        this.mem = mem;

        data = mem.data();
        len = mem.length();
    }

    /** {@inheritDoc} */
    @Override public byte readByte() {
        ensureEnoughData(1);

        return UNSAFE.getByte(data + pos++);
    }

    /** {@inheritDoc} */
    @Override public byte[] readByteArray(int cnt) {
        byte[] res = new byte[cnt];

        copyAndShift(res, BYTE_ARR_OFF, cnt);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean() {
        return readByte() == 1;
    }

    /** {@inheritDoc} */
    @Override public boolean[] readBooleanArray(int cnt) {
        boolean[] res = new boolean[cnt];

        copyAndShift(res, BOOLEAN_ARR_OFF, cnt);

        return res;
    }

    /** {@inheritDoc} */
    @Override public short readShort() {
        ensureEnoughData(2);

        short res = UNSAFE.getShort(data + pos);

        shift(2);

        return res;
    }

    /** {@inheritDoc} */
    @Override public short[] readShortArray(int cnt) {
        int len = cnt << 1;

        short[] res = new short[cnt];

        copyAndShift(res, SHORT_ARR_OFF, len);

        return res;
    }

    /** {@inheritDoc} */
    @Override public char readChar() {
        ensureEnoughData(2);

        char res = UNSAFE.getChar(data + pos);

        shift(2);

        return res;
    }

    /** {@inheritDoc} */
    @Override public char[] readCharArray(int cnt) {
        int len = cnt << 1;

        char[] res = new char[cnt];

        copyAndShift(res, CHAR_ARR_OFF, len);

        return res;
    }

    /** {@inheritDoc} */
    @Override public int readInt() {
        ensureEnoughData(4);

        int res = UNSAFE.getInt(data + pos);

        shift(4);

        return res;
    }

    /** {@inheritDoc} */
    @Override public int readInt(int pos) {
        int delta = pos + 4 - this.pos;

        if (delta > 0)
            ensureEnoughData(delta);

        return UNSAFE.getInt(data + pos);
    }

    /** {@inheritDoc} */
    @Override public int[] readIntArray(int cnt) {
        int len = cnt << 2;

        int[] res = new int[cnt];

        copyAndShift(res, INT_ARR_OFF, len);

        return res;
    }

    /** {@inheritDoc} */
    @Override public float readFloat() {
        ensureEnoughData(4);

        float res = UNSAFE.getFloat(data + pos);

        shift(4);

        return res;
    }

    /** {@inheritDoc} */
    @Override public float[] readFloatArray(int cnt) {
        int len = cnt << 2;

        float[] res = new float[cnt];

        copyAndShift(res, FLOAT_ARR_OFF, len);

        return res;
    }

    /** {@inheritDoc} */
    @Override public long readLong() {
        ensureEnoughData(8);

        long res = UNSAFE.getLong(data + pos);

        shift(8);

        return res;
    }

    /** {@inheritDoc} */
    @Override public long[] readLongArray(int cnt) {
        int len = cnt << 3;

        long[] res = new long[cnt];

        copyAndShift(res, LONG_ARR_OFF, len);

        return res;
    }

    /** {@inheritDoc} */
    @Override public double readDouble() {
        ensureEnoughData(8);

        double res = UNSAFE.getDouble(data + pos);

        shift(8);

        return res;
    }

    /** {@inheritDoc} */
    @Override public double[] readDoubleArray(int cnt) {
        int len = cnt << 3;

        double[] res = new double[cnt];

        copyAndShift(res, DOUBLE_ARR_OFF, len);

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
    @Override public int remaining() {
        return len - pos;
    }

    /** {@inheritDoc} */
    @Override public int position() {
        return pos;
    }

    /** {@inheritDoc} */
    @Override public void position(int pos) {
        if (pos > len)
            throw new IgniteException("Position is out of bounds: " + pos);
        else
            this.pos = pos;
    }

    /** {@inheritDoc} */
    @Override public byte[] array() {
        return arrayCopy();
    }

    /** {@inheritDoc} */
    @Override public byte[] arrayCopy() {
        if (dataCopy == null) {
            dataCopy = new byte[len];

            UNSAFE.copyMemory(null, data, dataCopy, BYTE_ARR_OFF, dataCopy.length);
        }

        return dataCopy;
    }

    /** {@inheritDoc} */
    @Override public long offheapPointer() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean hasArray() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void synchronize() {
        data = mem.data();
        len = mem.length();
    }

    /**
     * Ensure there is enough data in the stream.
     *
     * @param cnt Amount of byte expected to be available.
     */
    private void ensureEnoughData(int cnt) {
        if (remaining() < cnt)
            throw new IgniteException("Not enough data to read the value [position=" + pos +
                ", requiredBytes=" + cnt + ", remainingBytes=" + remaining() + ']');
    }

    /**
     * Copy required amount of data and shift position.
     *
     * @param target Target to copy data to.
     * @param off Offset.
     * @param cnt Count.
     */
    private void copyAndShift(Object target, long off, int cnt) {
        ensureEnoughData(cnt);

        UNSAFE.copyMemory(null, data + pos, target, off, cnt);

        shift(cnt);
    }

    /**
     * Shift position to the right.
     *
     * @param cnt Amount of bytes.
     */
    private void shift(int cnt) {
        pos += cnt;

        assert pos <= len;
    }
}
