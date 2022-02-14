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

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Input stream over {@link ByteBuffer}.
 */
public class BinaryByteBufferInputStream implements BinaryInputStream {
    /** */
    private final ByteBuffer buf;

    /**
     * @param buf Buffer to wrap.
     * @return Stream.
     */
    public static BinaryByteBufferInputStream create(ByteBuffer buf) {
        return new BinaryByteBufferInputStream(buf);
    }

    /**
     * @param buf Buffer to get data from.
     */
    BinaryByteBufferInputStream(ByteBuffer buf) {
        this.buf = buf;
    }

    /** {@inheritDoc} */
    @Override public byte readByte() {
        return buf.get();
    }

    /** {@inheritDoc} */
    @Override public byte[] readByteArray(int cnt) {
        byte[] data = new byte[cnt];

        buf.get(data);

        return data;
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] arr, int off, int cnt) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean() {
        return readByte() == 1;
    }

    /** {@inheritDoc} */
    @Override public boolean[] readBooleanArray(int cnt) {
        boolean[] res = new boolean[cnt];

        for (int i = 0; i < cnt; i++)
            res[i] = buf.get() != 0;

        return res;
    }

    /** {@inheritDoc} */
    @Override public short readShort() {
        return buf.getShort();
    }

    /** {@inheritDoc} */
    @Override public short[] readShortArray(int cnt) {
        short[] res = new short[cnt];

        for (int i = 0; i < cnt; i++)
            res[i] = buf.getShort();

        return res;
    }

    /** {@inheritDoc} */
    @Override public char readChar() {
        return buf.getChar();
    }

    /** {@inheritDoc} */
    @Override public char[] readCharArray(int cnt) {
        char[] res = new char[cnt];

        for (int i = 0; i < cnt; i++)
            res[i] = buf.getChar();

        return res;
    }

    /** {@inheritDoc} */
    @Override public int readInt() {
        return buf.getInt();
    }

    /** {@inheritDoc} */
    @Override public int[] readIntArray(int cnt) {
        int[] res = new int[cnt];

        for (int i = 0; i < cnt; i++)
            res[i] = buf.getInt();

        return res;
    }

    /** {@inheritDoc} */
    @Override public float readFloat() {
        return buf.getFloat();
    }

    /** {@inheritDoc} */
    @Override public float[] readFloatArray(int cnt) {
        float[] res = new float[cnt];

        for (int i = 0; i < cnt; i++)
            res[i] = buf.getFloat();

        return res;
    }

    /** {@inheritDoc} */
    @Override public long readLong() {
        return buf.getLong();
    }

    /** {@inheritDoc} */
    @Override public long[] readLongArray(int cnt) {
        long[] res = new long[cnt];

        for (int i = 0; i < cnt; i++)
            res[i] = buf.getLong();

        return res;
    }

    /** {@inheritDoc} */
    @Override public double readDouble() {
        return buf.getDouble();
    }

    /** {@inheritDoc} */
    @Override public double[] readDoubleArray(int cnt) {
        double[] res = new double[cnt];

        for (int i = 0; i < cnt; i++)
            res[i] = buf.getDouble();

        return res;
    }

    /** {@inheritDoc} */
    @Override public int remaining() {
        return buf.remaining();
    }

    /** {@inheritDoc} */
    @Override public byte readBytePositioned(int pos) {
        return buf.get(pos);
    }

    /** {@inheritDoc} */
    @Override public short readShortPositioned(int pos) {
        return buf.getShort(pos);
    }

    /** {@inheritDoc} */
    @Override public int readIntPositioned(int pos) {
        return buf.getInt(pos);
    }

    /** {@inheritDoc} */
    @Override public int position() {
        return buf.position();
    }

    /** {@inheritDoc} */
    @Override public void position(int pos) {
        buf.position(pos);
    }

    /** {@inheritDoc} */
    @Override public long rawOffheapPointer() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int capacity() {
        return buf.capacity();
    }

    /** {@inheritDoc} */
    @Override public byte[] array() {
        return buf.array();
    }

    /** {@inheritDoc} */
    @Override public byte[] arrayCopy() {
        byte[] arr = buf.array();

        return Arrays.copyOf(arr, arr.length);
    }

    /** {@inheritDoc} */
    @Override public long offheapPointer() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean hasArray() {
        return false;
    }
}
