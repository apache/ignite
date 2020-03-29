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
import org.apache.ignite.binary.BinaryObjectException;

/**
 *
 */
public class BinaryByteBufferInputStream implements BinaryInputStream {
    /** */
    private ByteBuffer buf;

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
        ensureHasData(1);

        return buf.get();
    }

    /** {@inheritDoc} */
    @Override public byte[] readByteArray(int cnt) {
        ensureHasData(cnt);

        byte[] data = new byte[cnt];

        buf.get(data);

        return data;
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] arr, int off, int cnt) {
        ensureHasData(cnt);

        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean() {
        ensureHasData(1);

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean[] readBooleanArray(int cnt) {
        ensureHasData(cnt);

        boolean[] res = new boolean[cnt];

        for (int i = 0; i < cnt; i++)
            res[i] = buf.get() != 0;

        return res;
    }

    /** {@inheritDoc} */
    @Override public short readShort() {
        ensureHasData(2);

        return buf.getShort();
    }

    /** {@inheritDoc} */
    @Override public short[] readShortArray(int cnt) {
        ensureHasData(2 * cnt);

        short[] res = new short[cnt];

        for (int i = 0; i < cnt; i++)
            res[i] = buf.getShort();

        return res;
    }

    /** {@inheritDoc} */
    @Override public char readChar() {
        ensureHasData(2);

        return buf.getChar();
    }

    /** {@inheritDoc} */
    @Override public char[] readCharArray(int cnt) {
        ensureHasData(2 * cnt);

        char[] res = new char[cnt];

        for (int i = 0; i < cnt; i++)
            res[i] = buf.getChar();

        return res;
    }

    /** {@inheritDoc} */
    @Override public int readInt() {
        ensureHasData(4);

        return buf.getInt();
    }

    /** {@inheritDoc} */
    @Override public int[] readIntArray(int cnt) {
        ensureHasData(4 * cnt);

        int[] res = new int[cnt];

        for (int i = 0; i < cnt; i++)
            res[i] = buf.getInt();

        return res;
    }

    /** {@inheritDoc} */
    @Override public float readFloat() {
        ensureHasData(4);

        return buf.getFloat();
    }

    /** {@inheritDoc} */
    @Override public float[] readFloatArray(int cnt) {
        ensureHasData(4 * cnt);

        float[] res = new float[cnt];

        for (int i = 0; i < cnt; i++)
            res[i] = buf.getFloat();

        return res;
    }

    /** {@inheritDoc} */
    @Override public long readLong() {
        ensureHasData(8);

        return buf.getLong();
    }

    /** {@inheritDoc} */
    @Override public long[] readLongArray(int cnt) {
        ensureHasData(8 * cnt);

        long[] res = new long[cnt];

        for (int i = 0; i < cnt; i++)
            res[i] = buf.getLong();

        return res;
    }

    /** {@inheritDoc} */
    @Override public double readDouble() {
        ensureHasData(8);

        return buf.getDouble();
    }

    /** {@inheritDoc} */
    @Override public double[] readDoubleArray(int cnt) {
        ensureHasData(8 * cnt);

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
        int oldPos = buf.position();

        buf.position(pos);

        ensureHasData(1);

        byte res = buf.get();

        buf.position(oldPos);

        return res;
    }

    /** {@inheritDoc} */
    @Override public short readShortPositioned(int pos) {
        int oldPos = buf.position();

        buf.position(pos);

        ensureHasData(2);

        short res = buf.getShort();

        buf.position(oldPos);

        return res;
    }

    /** {@inheritDoc} */
    @Override public int readIntPositioned(int pos) {
        int oldPos = buf.position();

        buf.position(pos);

        ensureHasData(4);

        byte res = buf.get();

        buf.position(oldPos);

        return res;
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
        return buf.array();
    }

    /** {@inheritDoc} */
    @Override public long offheapPointer() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean hasArray() {
        return false;
    }

    /**
     * @param cnt Remaining bytes.
     */
    private void ensureHasData(int cnt) {
        if (buf.remaining() < cnt)
            throw new BinaryObjectException("Not enough data to read the value " +
                "[requiredBytes=" + cnt + ", remainingBytes=" + buf.remaining() + ']');
    }
}
