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

package org.apache.ignite.internal.processors.hadoop.shuffle.mem.offheap;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.processors.hadoop.shuffle.mem.MemoryManager;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;

/**
 * Off-heap implementation of memory manager.
 */
public class OffheapMemoryManager extends MemoryManager {
    /** */
    private final GridUnsafeMemory mem;

    /** */
    private final int pageSize;

    /** */
    private final Collection<OffheapPage> allPages = new ConcurrentLinkedQueue<>();

    private AtomicBoolean closed = new AtomicBoolean();

    /**
     * @param mem Memory.
     * @param pageSize Page size.
     */
    public OffheapMemoryManager(GridUnsafeMemory mem, int pageSize) {
        assert mem != null;
        assert pageSize > 0;

        this.mem = mem;
        this.pageSize = pageSize;

        System.out.println("Create manager. mem=#" + Integer.toHexString(System.identityHashCode(this)));
    }

    /**
     * @param page Page.
     */
    private void deallocate(OffheapPage page) {
        assert page != null;

        mem.release(page.ptr(), page.size());
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (closed.compareAndSet(false, true)) {

            System.out.println("Begin close manager. mem=#" + Integer.toHexString(System.identityHashCode(this)));

            for (OffheapPage page : allPages)
                deallocate(page);

            System.out.println("Manager is closed. mem=#" + Integer.toHexString(System.identityHashCode(this)));
        }
        else
            assert false : "Already closed";

    }

    /** {@inheritDoc} */
    @Override public int pageSize() {
        return pageSize;
    }

    /** {@inheritDoc} */
    @Override public long allocate(long size) {
        long ptr = mem.allocate(size, true);

        allPages.add(new OffheapPage(ptr, size));

        return ptr;
    }

    /** {@inheritDoc} */
    @Override public void copyMemory(long srcPtr, long destPtr, long len) {
        mem.copyMemory(srcPtr, destPtr, len);
    }

    /** {@inheritDoc} */
    @Override public void copyMemory(byte[] srcBuf, int srcOff, long destPtr, long len) {
        GridUnsafe.copyMemory(srcBuf, GridUnsafe.BYTE_ARR_OFF + srcOff, null, destPtr, len);
    }

    /** {@inheritDoc} */
    @Override public void copyMemory(long srcPtr, byte[] dstBuf, int dstOff, long len) {
        GridUnsafe.copyMemory(null, srcPtr, dstBuf, GridUnsafe.BYTE_ARR_OFF + dstOff, len);
    }

    /** {@inheritDoc} */
    @Override public Bytes bytes(long ptr, long len) {
        byte [] buf = new byte[(int)len];

        return new Bytes(mem.readBytes(ptr, buf, 0, (int)len), 0, (int)len);
    }

    /** {@inheritDoc} */
    @Override public long readLongVolatile(long ptr) {
        return mem.readLongVolatile(ptr);
    }

    /** {@inheritDoc} */
    @Override public void writeLongVolatile(long ptr, long v) {
        mem.writeLongVolatile(ptr, v);
    }

    /** {@inheritDoc} */
    @Override public boolean casLong(long ptr, long exp, long v) {
        return mem.casLong(ptr, exp, v);
    }

    /** {@inheritDoc} */
    @Override public long readLong(long ptr) {
        return mem.readLong(ptr);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long ptr, long v) {
        mem.writeLong(ptr, v);
    }

    /** {@inheritDoc} */
    @Override public int readInt(long ptr) {
        return mem.readInt(ptr);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(long ptr, int v) {
        mem.writeInt(ptr, v);
    }

    /** {@inheritDoc} */
    @Override public float readFloat(long ptr) {
        return mem.readFloat(ptr);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(long ptr, float v) {
        mem.writeFloat(ptr, v);
    }

    /** {@inheritDoc} */
    @Override public double readDouble(long ptr) {
        return mem.readDouble(ptr);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(long ptr, double v) {
        mem.writeDouble(ptr, v);
    }

    /** {@inheritDoc} */
    @Override public short readShort(long ptr) {
        return mem.readShort(ptr);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(long ptr, short v) {
        mem.writeShort(ptr, v);
    }

    /** {@inheritDoc} */
    @Override public byte readByte(long ptr) {
        return mem.readByte(ptr);
    }

    /** {@inheritDoc} */
    @Override public void writeByte(long ptr, byte v) {
        mem.writeByte(ptr, v);
    }

    /** {@inheritDoc} */
    @Override public void writeBytes(long ptr, byte[] arr, int off, int len) {
        mem.writeBytes(ptr, arr, off, len);
    }

    /** {@inheritDoc} */
    @Override public byte[] readBytes(long ptr, byte[] arr, int off, int len) {
        return mem.readBytes(ptr, arr, off, len);
    }
}