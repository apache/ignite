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

/**
 * Portable offheap output stream.
 */
public class PortableOffheapOutputStream extends PortableAbstractOutputStream {
    /** Pointer. */
    private long ptr;

    /** Length of bytes that cen be used before resize is necessary. */
    private int cap;

    /**
     * Constructor.
     *
     * @param cap Capacity.
     */
    public PortableOffheapOutputStream(int cap) {
        this(0, cap);
    }

    /**
     * Constructor.
     *
     * @param ptr Pointer to existing address.
     * @param cap Capacity.
     */
    public PortableOffheapOutputStream(long ptr, int cap) {
        this.ptr = ptr == 0 ? allocate(cap) : ptr;

        this.cap = cap;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        release(ptr);
    }

    /** {@inheritDoc} */
    @Override public void ensureCapacity(int cnt) {
        if (cnt > cap) {
            int newCap = capacity(cap, cnt);

            ptr = reallocate(ptr, newCap);

            cap = newCap;
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] array() {
        return arrayCopy();
    }

    /** {@inheritDoc} */
    @Override public byte[] arrayCopy() {
        byte[] res = new byte[pos];

        UNSAFE.copyMemory(null, ptr, res, BYTE_ARR_OFF, pos);

        return res;
    }

    /**
     * @return Pointer.
     */
    public long pointer() {
        return ptr;
    }

    /**
     * @return Capacity.
     */
    public int capacity() {
        return cap;
    }

    /** {@inheritDoc} */
    @Override protected void writeByteAndShift(byte val) {
        UNSAFE.putByte(ptr + pos++, val);
    }

    /** {@inheritDoc} */
    @Override protected void copyAndShift(Object src, long offset, int len) {
        UNSAFE.copyMemory(src, offset, null, ptr + pos, len);

        shift(len);
    }

    /** {@inheritDoc} */
    @Override protected void writeShortFast(short val) {
        UNSAFE.putShort(ptr + pos, val);
    }

    /** {@inheritDoc} */
    @Override protected void writeCharFast(char val) {
        UNSAFE.putChar(ptr + pos, val);
    }

    /** {@inheritDoc} */
    @Override protected void writeIntFast(int val) {
        UNSAFE.putInt(ptr + pos, val);
    }

    /** {@inheritDoc} */
    @Override protected void writeLongFast(long val) {
        UNSAFE.putLong(ptr + pos, val);
    }

    /** {@inheritDoc} */
    @Override protected void writeIntPositioned(int pos, int val) {
        if (!LITTLE_ENDIAN)
            val = Integer.reverseBytes(val);

        UNSAFE.putInt(ptr + pos, val);
    }

    /** {@inheritDoc} */
    @Override public boolean hasArray() {
        return false;
    }

    /**
     * Allocate memory.
     *
     * @param cap Capacity.
     * @return Pointer.
     */
    protected long allocate(int cap) {
        return UNSAFE.allocateMemory(cap);
    }

    /**
     * Reallocate memory.
     *
     * @param ptr Old pointer.
     * @param cap Capacity.
     * @return New pointer.
     */
    protected long reallocate(long ptr, int cap) {
        return UNSAFE.reallocateMemory(ptr, cap);
    }

    /**
     * Release memory.
     *
     * @param ptr Pointer.
     */
    protected void release(long ptr) {
        UNSAFE.freeMemory(ptr);
    }
}
