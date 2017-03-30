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

import java.util.Arrays;
import org.apache.ignite.internal.util.GridUnsafe;

import static org.apache.ignite.internal.util.GridUnsafe.BIG_ENDIAN;

/**
 * Binary heap input stream.
 */
public final class BinaryHeapInputStream extends BinaryAbstractInputStream {
    /**
     * Create stream with pointer set at the given position.
     *
     * @param data Data.
     * @param pos Position.
     * @return Stream.
     */
    public static BinaryHeapInputStream create(byte[] data, int pos) {
        assert pos < data.length;

        BinaryHeapInputStream stream = new BinaryHeapInputStream(data);

        stream.pos = pos;

        return stream;
    }

    /** Data. */
    private byte[] data;

    /**
     * Constructor.
     *
     * @param data Data.
     */
    public BinaryHeapInputStream(byte[] data) {
        this.data = data;

        len = data.length;
    }

    /**
     * @return Copy of this stream.
     */
    public BinaryHeapInputStream copy() {
        BinaryHeapInputStream in = new BinaryHeapInputStream(Arrays.copyOf(data, data.length));

        in.position(pos);

        return in;
    }

    /**
     * Method called from JNI to resize stream.
     *
     * @param len Required length.
     * @return Underlying byte array.
     */
    public byte[] resize(int len) {
        if (data.length < len) {
            byte[] data0 = new byte[len];

            System.arraycopy(data, 0, data0, 0, data.length);

            data = data0;
        }

        return data;
    }

    /** {@inheritDoc} */
    @Override public int remaining() {
        return data.length - pos;
    }

    /** {@inheritDoc} */
    @Override public int capacity() {
        return data.length;
    }

    /** {@inheritDoc} */
    @Override public byte[] array() {
        return data;
    }

    /** {@inheritDoc} */
    @Override public byte[] arrayCopy() {
        byte[] res = new byte[len];

        System.arraycopy(data, 0, res, 0, len);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean hasArray() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected byte readByteAndShift() {
        return data[pos++];
    }

    /** {@inheritDoc} */
    @Override protected void copyAndShift(Object target, long off, int len) {
        GridUnsafe.copyMemory(data, GridUnsafe.BYTE_ARR_OFF + pos, target, off, len);

        shift(len);
    }

    /** {@inheritDoc} */
    @Override protected short readShortFast() {
        long off = GridUnsafe.BYTE_ARR_OFF + pos;

        return BIG_ENDIAN ? GridUnsafe.getShortLE(data, off) : GridUnsafe.getShort(data, off);

    }

    /** {@inheritDoc} */
    @Override protected char readCharFast() {
        long off = GridUnsafe.BYTE_ARR_OFF + pos;

        return BIG_ENDIAN ? GridUnsafe.getCharLE(data, off) : GridUnsafe.getChar(data, off);
    }

    /** {@inheritDoc} */
    @Override protected int readIntFast() {
        long off = GridUnsafe.BYTE_ARR_OFF + pos;

        return BIG_ENDIAN ? GridUnsafe.getIntLE(data, off) : GridUnsafe.getInt(data, off);
    }

    /** {@inheritDoc} */
    @Override protected long readLongFast() {
        long off = GridUnsafe.BYTE_ARR_OFF + pos;

        return BIG_ENDIAN ? GridUnsafe.getLongLE(data, off) : GridUnsafe.getLong(data, off);
    }

    /** {@inheritDoc} */
    @Override protected byte readBytePositioned0(int pos) {
        return GridUnsafe.getByte(data, GridUnsafe.BYTE_ARR_OFF + pos);
    }

    /** {@inheritDoc} */
    @Override protected short readShortPositioned0(int pos) {
        long off = GridUnsafe.BYTE_ARR_OFF + pos;

        return BIG_ENDIAN ? GridUnsafe.getShortLE(data, off) : GridUnsafe.getShort(data, off);

    }

    /** {@inheritDoc} */
    @Override protected int readIntPositioned0(int pos) {
        long off = GridUnsafe.BYTE_ARR_OFF + pos;

        return BIG_ENDIAN ? GridUnsafe.getIntLE(data, off) : GridUnsafe.getInt(data, off);
    }
}
