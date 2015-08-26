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

/**
 * Interop input stream implementation working with BIG ENDIAN architecture.
 */
public class PlatformBigEndianInputStreamImpl extends PlatformInputStreamImpl {
    /**
     * Constructor.
     *
     * @param mem Memory chunk.
     */
    public PlatformBigEndianInputStreamImpl(PlatformMemory mem) {
        super(mem);
    }

    /** {@inheritDoc} */
    @Override public short readShort() {
        return Short.reverseBytes(super.readShort());
    }

    /** {@inheritDoc} */
    @Override public short[] readShortArray(int cnt) {
        short[] res = super.readShortArray(cnt);

        for (int i = 0; i < cnt; i++)
            res[i] = Short.reverseBytes(res[i]);

        return res;
    }

    /** {@inheritDoc} */
    @Override public char readChar() {
        return Character.reverseBytes(super.readChar());
    }

    /** {@inheritDoc} */
    @Override public char[] readCharArray(int cnt) {
        char[] res = super.readCharArray(cnt);

        for (int i = 0; i < cnt; i++)
            res[i] = Character.reverseBytes(res[i]);

        return res;
    }

    /** {@inheritDoc} */
    @Override public int readInt() {
        return Integer.reverseBytes(super.readInt());
    }

    /** {@inheritDoc} */
    @Override public int readInt(int pos) {
        return Integer.reverseBytes(super.readInt(pos));
    }

    /** {@inheritDoc} */
    @Override public int[] readIntArray(int cnt) {
        int[] res = super.readIntArray(cnt);

        for (int i = 0; i < cnt; i++)
            res[i] = Integer.reverseBytes(res[i]);

        return res;
    }

    /** {@inheritDoc} */
    @Override public float readFloat() {
        return Float.intBitsToFloat(Integer.reverseBytes(Float.floatToIntBits(super.readFloat())));
    }

    /** {@inheritDoc} */
    @Override public float[] readFloatArray(int cnt) {
        float[] res = super.readFloatArray(cnt);

        for (int i = 0; i < cnt; i++)
            res[i] = Float.intBitsToFloat(Integer.reverseBytes(Float.floatToIntBits(res[i])));

        return res;
    }

    /** {@inheritDoc} */
    @Override public long readLong() {
        return Long.reverseBytes(super.readLong());
    }

    /** {@inheritDoc} */
    @Override public long[] readLongArray(int cnt) {
        long[] res = super.readLongArray(cnt);

        for (int i = 0; i < cnt; i++)
            res[i] = Long.reverseBytes(res[i]);

        return res;
    }

    /** {@inheritDoc} */
    @Override public double readDouble() {
        return Double.longBitsToDouble(Long.reverseBytes(Double.doubleToLongBits(super.readDouble())));
    }

    /** {@inheritDoc} */
    @Override public double[] readDoubleArray(int cnt) {
        double[] res = super.readDoubleArray(cnt);

        for (int i = 0; i < cnt; i++)
            res[i] = Double.longBitsToDouble(Long.reverseBytes(Double.doubleToLongBits(res[i])));

        return res;
    }
}
