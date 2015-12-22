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
 * Binary heap output stream.
 */
public final class BinaryHeapOutputStream extends BinaryAbstractOutputStream {
    /** Allocator. */
    private final BinaryMemoryAllocatorChunk chunk;

    /** Data. */
    private byte[] data;

    /**
     * Constructor.
     *
     * @param cap Initial capacity.
     */
    public BinaryHeapOutputStream(int cap) {
        this(cap, BinaryMemoryAllocator.INSTANCE.chunk());
    }

    /**
     * Constructor.
     *
     * @param cap Capacity.
     * @param chunk Chunk.
     */
    public BinaryHeapOutputStream(int cap, BinaryMemoryAllocatorChunk chunk) {
        this.chunk = chunk;

        data = chunk.allocate(cap);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        chunk.release(data, pos);
    }

    /** {@inheritDoc} */
    @Override public void ensureCapacity(int cnt) {
        if (cnt > data.length) {
            int newCap = capacity(data.length, cnt);

            data = chunk.reallocate(data, newCap);
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] array() {
        return data;
    }

    /** {@inheritDoc} */
    @Override public byte[] arrayCopy() {
        byte[] res = new byte[pos];

        GridUnsafe.copyMemory(data, GridUnsafe.BYTE_ARR_OFF, res, GridUnsafe.BYTE_ARR_OFF, pos);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean hasArray() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void writeByteAndShift(byte val) {
        data[pos++] = val;
    }

    /** {@inheritDoc} */
    @Override protected void copyAndShift(Object src, long off, int len) {
        GridUnsafe.copyMemory(src, off, data, GridUnsafe.BYTE_ARR_OFF + pos, len);

        shift(len);
    }

    /** {@inheritDoc} */
    @Override protected void writeShortFast(short val) {
        GridUnsafe.putShortAligned(data, GridUnsafe.BYTE_ARR_OFF + pos, val);
    }

    /** {@inheritDoc} */
    @Override protected void writeCharFast(char val) {
        GridUnsafe.putCharAligned(data, GridUnsafe.BYTE_ARR_OFF + pos, val);
    }

    /** {@inheritDoc} */
    @Override protected void writeIntFast(int val) {
        GridUnsafe.putIntAligned(data, GridUnsafe.BYTE_ARR_OFF + pos, val);
    }

    /** {@inheritDoc} */
    @Override protected void writeLongFast(long val) {
        GridUnsafe.putLongAligned(data, GridUnsafe.BYTE_ARR_OFF + pos, val);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteByte(byte val) {
        GridUnsafe.putByte(data, GridUnsafe.BYTE_ARR_OFF + pos++, val);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteShort(short val) {
        if (!LITTLE_ENDIAN)
            val = Short.reverseBytes(val);

        GridUnsafe.putShortAligned(data, GridUnsafe.BYTE_ARR_OFF + pos, val);

        shift(2);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteShort(int pos, short val) {
        if (!LITTLE_ENDIAN)
            val = Short.reverseBytes(val);

        GridUnsafe.putShortAligned(data, GridUnsafe.BYTE_ARR_OFF + pos, val);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteChar(char val) {
        if (!LITTLE_ENDIAN)
            val = Character.reverseBytes(val);

        GridUnsafe.putCharAligned(data, GridUnsafe.BYTE_ARR_OFF + pos, val);

        shift(2);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteInt(int val) {
        if (!LITTLE_ENDIAN)
            val = Integer.reverseBytes(val);

        GridUnsafe.putIntAligned(data, GridUnsafe.BYTE_ARR_OFF + pos, val);

        shift(4);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteInt(int pos, int val) {
        if (!LITTLE_ENDIAN)
            val = Integer.reverseBytes(val);

        GridUnsafe.putIntAligned(data, GridUnsafe.BYTE_ARR_OFF + pos, val);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteLong(long val) {
        if (!LITTLE_ENDIAN)
            val = Long.reverseBytes(val);

        GridUnsafe.putLongAligned(data, GridUnsafe.BYTE_ARR_OFF + pos, val);

        shift(8);
    }
}
