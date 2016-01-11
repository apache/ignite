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

/**
 * Binary off-heap input stream.
 */
public class BinaryOffheapInputStream extends BinaryAbstractInputStream {
    /** Pointer. */
    private final long ptr;

    /** Capacity. */
    private final int cap;

    /** */
    private boolean forceHeap;

    /**
     * Constructor.
     *
     * @param ptr Pointer.
     * @param cap Capacity.
     */
    public BinaryOffheapInputStream(long ptr, int cap) {
        this(ptr, cap, false);
    }

    /**
     * Constructor.
     *
     * @param ptr Pointer.
     * @param cap Capacity.
     * @param forceHeap If {@code true} method {@link #offheapPointer} returns 0 and unmarshalling will
     *        create heap-based objects.
     */
    public BinaryOffheapInputStream(long ptr, int cap, boolean forceHeap) {
        this.ptr = ptr;
        this.cap = cap;
        this.forceHeap = forceHeap;

        len = cap;
    }

    /** {@inheritDoc} */
    @Override public int remaining() {
        return cap - pos;
    }

    /** {@inheritDoc} */
    @Override public byte[] array() {
        return arrayCopy();
    }

    /** {@inheritDoc} */
    @Override public byte[] arrayCopy() {
        byte[] res = new byte[len];

        GridUnsafe.copyMemory(null, ptr, res, GridUnsafe.BYTE_ARR_OFF, res.length);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean hasArray() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected byte readByteAndShift() {
        return GridUnsafe.getByte(ptr + pos++);
    }

    /** {@inheritDoc} */
    @Override protected void copyAndShift(Object target, long off, int len) {
        GridUnsafe.copyMemory(null, ptr + pos, target, off, len);

        shift(len);
    }

    /** {@inheritDoc} */
    @Override protected short readShortFast() {
        return GridUnsafe.getShort(ptr + pos);
    }

    /** {@inheritDoc} */
    @Override protected char readCharFast() {
        return GridUnsafe.getChar(ptr + pos);
    }

    /** {@inheritDoc} */
    @Override protected int readIntFast() {
        return GridUnsafe.getInt(ptr + pos);
    }

    /** {@inheritDoc} */
    @Override protected long readLongFast() {
        return GridUnsafe.getLong(ptr + pos);
    }

    /** {@inheritDoc} */
    @Override protected byte readBytePositioned0(int pos) {
        return GridUnsafe.getByte(ptr + pos);
    }

    /** {@inheritDoc} */
    @Override protected short readShortPositioned0(int pos) {
        short res = GridUnsafe.getShort(ptr + pos);

        if (!LITTLE_ENDIAN)
            res = Short.reverseBytes(res);

        return res;
    }

    /** {@inheritDoc} */
    @Override protected int readIntPositioned0(int pos) {
        int res = GridUnsafe.getInt(ptr + pos);

        if (!LITTLE_ENDIAN)
            res = Integer.reverseBytes(res);

        return res;
    }

    /** {@inheritDoc} */
    @Override public long offheapPointer() {
        return forceHeap ? 0 : ptr;
    }
}
