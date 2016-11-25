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

package org.apache.ignite.internal.processors.hadoop.shuffle.mem.heap;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.internal.processors.hadoop.shuffle.mem.MemoryManager;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;

/**
 * Base class for all multimaps.
 */
public class HeapMemoryManager extends MemoryManager {

    ReentrantLock lock = new ReentrantLock();

    /** */
    private final int pageSize;

    /** */
    private final List<HeapPage> allPages = new ArrayList<>();


    /**
     * @param mem Memory.
     * @param pageSize Page size.
     */
    public HeapMemoryManager(GridUnsafeMemory mem, int pageSize) {
        assert mem != null;
        assert pageSize > 0;

        this.pageSize = pageSize;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        lock.lock();

        try {
            allPages.clear();
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public int pageSize() {
        return pageSize;
    }

    /** {@inheritDoc} */
    @Override public long allocate(long size) {
        lock.lock();
        try {
            HeapPage p = new HeapPage(allPages.size(), (int)size);

            allPages.add(p);

            return p.ptr();
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void copyMemory(long srcPtr, long destPtr, long len) {
        HeapPage src = ptrToPage(srcPtr);
        HeapPage dst = ptrToPage(srcPtr);

        GridUnsafe.copyMemory(src.buf(), (int)srcPtr, dst.buf(), (int)destPtr, len);
    }

    /** {@inheritDoc} */
    @Override public Bytes bytes(long ptr, long len) {
        return new Bytes(ptrToPage(ptr).buf(), (int)GridUnsafe.BYTE_ARR_OFF + (int)ptr, (int)len);
    }

    /** {@inheritDoc} */
    @Override public long readLongVolatile(long ptr) {
        HeapPage p = ptrToPage(ptr);

        return GridUnsafe.getLongVolatile(p.buf(), GridUnsafe.BYTE_ARR_OFF + ptr & 0xffffffff);
    }

    /** {@inheritDoc} */
    @Override public void writeLongVolatile(long ptr, long v) {
        HeapPage p = ptrToPage(ptr);

        GridUnsafe.putLongVolatile(p.buf(), GridUnsafe.BYTE_ARR_OFF + ptr & 0xffffffff, v);
    }

    /** {@inheritDoc} */
    @Override public boolean casLong(long ptr, long exp, long v) {
        HeapPage p = ptrToPage(ptr);

        return GridUnsafe.compareAndSwapLong(p.buf(), GridUnsafe.BYTE_ARR_OFF + ptr & 0xffffffff, exp, v);
    }

    /** {@inheritDoc} */
    @Override public long readLong(long ptr) {
        HeapPage p = ptrToPage(ptr);

        return GridUnsafe.getLong(p.buf(), GridUnsafe.BYTE_ARR_OFF + ptr & 0xffffffff);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long ptr, long v) {
        HeapPage p = ptrToPage(ptr);

        GridUnsafe.putLong(p.buf(), GridUnsafe.BYTE_ARR_OFF + ptr & 0xffffffff, v);
    }

    /** {@inheritDoc} */
    @Override public int readInt(long ptr) {
        HeapPage p = ptrToPage(ptr);

        return GridUnsafe.getInt(p.buf(), GridUnsafe.BYTE_ARR_OFF + ptr & 0xffffffff);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(long ptr, int v) {
        HeapPage p = ptrToPage(ptr);

        GridUnsafe.putInt(p.buf(), GridUnsafe.BYTE_ARR_OFF + ptr & 0xffffffff, v);
    }

    /** {@inheritDoc} */
    @Override public float readFloat(long ptr) {
        HeapPage p = ptrToPage(ptr);

        return GridUnsafe.getFloat(p.buf(), GridUnsafe.BYTE_ARR_OFF + ptr & 0xffffffff);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(long ptr, float v) {
        HeapPage p = ptrToPage(ptr);

        GridUnsafe.putFloat(p.buf(), GridUnsafe.BYTE_ARR_OFF + ptr & 0xffffffff, v);
    }

    /** {@inheritDoc} */
    @Override public double readDouble(long ptr) {
        HeapPage p = ptrToPage(ptr);

        return GridUnsafe.getDouble(p.buf(), GridUnsafe.BYTE_ARR_OFF + ptr & 0xffffffff);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(long ptr, double v) {
        HeapPage p = ptrToPage(ptr);

        GridUnsafe.putDouble(p.buf(), GridUnsafe.BYTE_ARR_OFF + ptr & 0xffffffff, v);
    }

    /** {@inheritDoc} */
    @Override public short readShort(long ptr) {
        HeapPage p = ptrToPage(ptr);

        return GridUnsafe.getShort(p.buf(), GridUnsafe.BYTE_ARR_OFF + ptr & 0xffffffff);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(long ptr, short v) {
        HeapPage p = ptrToPage(ptr);

        GridUnsafe.putShort(p.buf(), GridUnsafe.BYTE_ARR_OFF + ptr & 0xffffffff, v);
    }

    /** {@inheritDoc} */
    @Override public byte readByte(long ptr) {
        HeapPage p = ptrToPage(ptr);

        return GridUnsafe.getByte(p.buf(), GridUnsafe.BYTE_ARR_OFF + ptr & 0xffffffff);
    }

    /** {@inheritDoc} */
    @Override public void writeByte(long ptr, byte v) {
        HeapPage p = ptrToPage(ptr);

        GridUnsafe.putByte(p.buf(), GridUnsafe.BYTE_ARR_OFF + ptr & 0xffffffff, v);
    }

    /** {@inheritDoc} */
    @Override public void writeBytes(long ptr, byte[] arr, int off, int len) {
        HeapPage p = ptrToPage(ptr);

        GridUnsafe.copyMemory(arr, GridUnsafe.BYTE_ARR_OFF + off,
            p.buf(), GridUnsafe.BYTE_ARR_OFF + ptr & 0xffffffff, len);
    }

    /** {@inheritDoc} */
    @Override public byte[] readBytes(long ptr, byte[] arr, int off, int len) {
        HeapPage p = ptrToPage(ptr);

        GridUnsafe.copyMemory(p.buf(), GridUnsafe.BYTE_ARR_OFF + ptr & 0xffffffff,
            arr, GridUnsafe.BYTE_ARR_OFF + off, len);

        return arr;
    }

    /**
     * @param ptr Pointer.
     * @return Bytes.
     */
    private HeapPage ptrToPage(long ptr) {
        return allPages.get((int)(ptr >> 32));
    }
}