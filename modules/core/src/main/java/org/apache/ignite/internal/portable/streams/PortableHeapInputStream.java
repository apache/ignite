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

import java.util.Arrays;

/**
 * Portable off-heap input stream.
 */
public final class PortableHeapInputStream extends PortableAbstractInputStream {
    /** Data. */
    private byte[] data;

    /**
     * Constructor.
     *
     * @param data Data.
     */
    public PortableHeapInputStream(byte[] data) {
        this.data = data;

        len = data.length;
    }

    /**
     * @return Copy of this stream.
     */
    public PortableHeapInputStream copy() {
        PortableHeapInputStream in = new PortableHeapInputStream(Arrays.copyOf(data, data.length));

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

            UNSAFE.copyMemory(data, BYTE_ARR_OFF, data0, BYTE_ARR_OFF, data.length);

            data = data0;
        }

        return data;
    }

    /** {@inheritDoc} */
    @Override public int remaining() {
        return data.length - pos;
    }

    /** {@inheritDoc} */
    @Override public byte[] array() {
        return data;
    }

    /** {@inheritDoc} */
    @Override public byte[] arrayCopy() {
        byte[] res = new byte[len];

        UNSAFE.copyMemory(data, BYTE_ARR_OFF, res, BYTE_ARR_OFF, res.length);

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
        UNSAFE.copyMemory(data, BYTE_ARR_OFF + pos, target, off, len);

        shift(len);
    }

    /** {@inheritDoc} */
    @Override protected short readShortFast() {
        return UNSAFE.getShort(data, BYTE_ARR_OFF + pos);
    }

    /** {@inheritDoc} */
    @Override protected char readCharFast() {
        return UNSAFE.getChar(data, BYTE_ARR_OFF + pos);
    }

    /** {@inheritDoc} */
    @Override protected int readIntFast() {
        return UNSAFE.getInt(data, BYTE_ARR_OFF + pos);
    }

    /** {@inheritDoc} */
    @Override protected long readLongFast() {
        return UNSAFE.getLong(data, BYTE_ARR_OFF + pos);
    }

    /** {@inheritDoc} */
    @Override protected int readIntPositioned(int pos) {
        int res = UNSAFE.getInt(data, BYTE_ARR_OFF + pos);

        if (!LITTLE_ENDIAN)
            res = Integer.reverseBytes(res);

        return res;
    }
}