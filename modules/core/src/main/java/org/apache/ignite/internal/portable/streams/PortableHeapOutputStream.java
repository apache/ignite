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

import static org.apache.ignite.internal.portable.PortableThreadLocalMemoryAllocator.*;

/**
 * Portable heap output stream.
 */
public final class PortableHeapOutputStream extends PortableAbstractOutputStream {
    /** Default capacity. */
    private static final int DFLT_CAP = 1024;

    /** Allocator. */
    private final PortableMemoryAllocator alloc;

    /** Data. */
    private byte[] data;

    /**
     * Constructor.
     */
    public PortableHeapOutputStream() {
        this(DFLT_CAP, DFLT_ALLOC);
    }

    /**
     * Constructor.
     *
     * @param cap Initial capacity.
     */
    public PortableHeapOutputStream(int cap) {
        this(cap, THREAD_LOCAL_ALLOC);
    }

    /**
     * Constructor.
     *
     * @param cap Initial capacity.
     * @param alloc Allocator.
     */
    public PortableHeapOutputStream(int cap, PortableMemoryAllocator alloc) {
        data = alloc.allocate(cap);

        this.alloc = alloc;
    }

    /**
     * Constructor.
     *
     * @param data Data.
     */
    public PortableHeapOutputStream(byte[] data) {
        this(data, DFLT_ALLOC);
    }

    /**
     * Constructor.
     *
     * @param data Data.
     * @param alloc Allocator.
     */
    public PortableHeapOutputStream(byte[] data, PortableMemoryAllocator alloc) {
        this.data = data;
        this.alloc = alloc;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        alloc.release(data, pos);
    }

    /** {@inheritDoc} */
    @Override public void ensureCapacity(int cnt) {
        if (cnt > data.length) {
            int newCap = capacity(data.length, cnt);

            data = alloc.reallocate(data, newCap);
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] array() {
        return data;
    }

    /** {@inheritDoc} */
    @Override public byte[] arrayCopy() {
        byte[] res = new byte[pos];

        UNSAFE.copyMemory(data, BYTE_ARR_OFF, res, BYTE_ARR_OFF, pos);

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
        UNSAFE.copyMemory(src, off, data, BYTE_ARR_OFF + pos, len);

        shift(len);
    }

    /** {@inheritDoc} */
    @Override protected void writeShortFast(short val) {
        UNSAFE.putShort(data, BYTE_ARR_OFF + pos, val);
    }

    /** {@inheritDoc} */
    @Override protected void writeCharFast(char val) {
        UNSAFE.putChar(data, BYTE_ARR_OFF + pos, val);
    }

    /** {@inheritDoc} */
    @Override protected void writeIntFast(int val) {
        UNSAFE.putInt(data, BYTE_ARR_OFF + pos, val);
    }

    /** {@inheritDoc} */
    @Override protected void writeLongFast(long val) {
        UNSAFE.putLong(data, BYTE_ARR_OFF + pos, val);
    }

    /** {@inheritDoc} */
    @Override protected void writeIntPositioned(int pos, int val) {
        if (!LITTLE_ENDIAN)
            val = Integer.reverseBytes(val);

        UNSAFE.putInt(data, BYTE_ARR_OFF + pos, val);
    }
}
