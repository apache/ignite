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

import static org.apache.ignite.internal.processors.platform.memory.PlatformMemoryUtils.UNSAFE;

/**
 * Interop output stream implementation working with BIG ENDIAN architecture.
 */
public class PlatformBigEndianOutputStreamImpl extends PlatformOutputStreamImpl {
    /**
     * Constructor.
     *
     * @param mem Underlying memory chunk.
     */
    public PlatformBigEndianOutputStreamImpl(PlatformMemory mem) {
        super(mem);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(short val) {
        super.writeShort(Short.reverseBytes(val));
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(short[] val) {
        int cnt = val.length << 1;

        ensureCapacity(pos + cnt);

        long startPos = data + pos;

        for (short item : val) {
            UNSAFE.putShort(startPos, Short.reverseBytes(item));

            startPos += 2;
        }

        shift(cnt);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(char val) {
        super.writeChar(Character.reverseBytes(val));
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(char[] val) {
        int cnt = val.length << 1;

        ensureCapacity(pos + cnt);

        long startPos = data + pos;

        for (char item : val) {
            UNSAFE.putChar(startPos, Character.reverseBytes(item));

            startPos += 2;
        }

        shift(cnt);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int val) {
        super.writeInt(Integer.reverseBytes(val));
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(int[] val) {
        int cnt = val.length << 2;

        ensureCapacity(pos + cnt);

        long startPos = data + pos;

        for (int item : val) {
            UNSAFE.putInt(startPos, Integer.reverseBytes(item));

            startPos += 4;
        }

        shift(cnt);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int pos, int val) {
        super.writeInt(pos, Integer.reverseBytes(val));
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(float[] val) {
        int cnt = val.length << 2;

        ensureCapacity(pos + cnt);

        long startPos = data + pos;

        for (float item : val) {
            UNSAFE.putInt(startPos, Integer.reverseBytes(Float.floatToIntBits(item)));

            startPos += 4;
        }

        shift(cnt);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long val) {
        super.writeLong(Long.reverseBytes(val));
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(long[] val) {
        int cnt = val.length << 3;

        ensureCapacity(pos + cnt);

        long startPos = data + pos;

        for (long item : val) {
            UNSAFE.putLong(startPos, Long.reverseBytes(item));

            startPos += 8;
        }

        shift(cnt);
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(double[] val) {
        int cnt = val.length << 3;

        ensureCapacity(pos + cnt);

        long startPos = data + pos;

        for (double item : val) {
            UNSAFE.putLong(startPos, Long.reverseBytes(Double.doubleToLongBits(item)));

            startPos += 8;
        }

        shift(cnt);
    }
}
