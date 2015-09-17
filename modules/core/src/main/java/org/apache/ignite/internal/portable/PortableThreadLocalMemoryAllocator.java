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

package org.apache.ignite.internal.portable;

import org.apache.ignite.internal.portable.streams.PortableMemoryAllocator;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;
import sun.misc.Unsafe;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_MARSHAL_BUFFERS_RECHECK;

/**
 * Thread-local memory allocator.
 */
public class PortableThreadLocalMemoryAllocator implements PortableMemoryAllocator {
    /** Memory allocator instance. */
    public static final PortableThreadLocalMemoryAllocator THREAD_LOCAL_ALLOC =
        new PortableThreadLocalMemoryAllocator();

    /** Holders. */
    private static final ThreadLocal<ByteArrayHolder> holders = new ThreadLocal<>();

    /** Unsafe instance. */
    protected static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** Array offset: byte. */
    protected static final long BYTE_ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

    /**
     * Ensures singleton.
     */
    private PortableThreadLocalMemoryAllocator() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public byte[] allocate(int size) {
        ByteArrayHolder holder = holders.get();

        if (holder == null)
            holders.set(holder = new ByteArrayHolder());

        if (holder.acquired)
            return new byte[size];

        holder.acquired = true;

        if (holder.data == null || size > holder.data.length)
            holder.data = new byte[size];

        return holder.data;
    }

    /** {@inheritDoc} */
    @Override public byte[] reallocate(byte[] data, int size) {
        ByteArrayHolder holder = holders.get();

        assert holder != null;

        byte[] newData = new byte[size];

        if (holder.data == data)
            holder.data = newData;

        UNSAFE.copyMemory(data, BYTE_ARR_OFF, newData, BYTE_ARR_OFF, data.length);

        return newData;
    }

    /** {@inheritDoc} */
    @Override public void release(byte[] data, int maxMsgSize) {
        ByteArrayHolder holder = holders.get();

        assert holder != null;

        if (holder.data != data)
            return;

        holder.maxMsgSize = maxMsgSize;
        holder.acquired = false;

        holder.shrink();
    }

    /** {@inheritDoc} */
    @Override public long allocateDirect(int size) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long reallocateDirect(long addr, int size) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void releaseDirect(long addr) {
        // No-op
    }

    /**
     * Checks whether a thread-local array is acquired or not.
     * The function is used by Unit tests.
     *
     * @return {@code true} if acquired {@code false} otherwise.
     */
    public boolean isThreadLocalArrayAcquired() {
        ByteArrayHolder holder = holders.get();

        return holder != null && holder.acquired;
    }

    /**
     * Thread-local byte array holder.
     */
    private static class ByteArrayHolder {
        /** */
        private static final Long CHECK_FREQ = Long.getLong(IGNITE_MARSHAL_BUFFERS_RECHECK, 10000);

        /** Data array */
        private byte[] data;

        /** Max message size detected between checks. */
        private int maxMsgSize;

        /** Last time array size is checked. */
        private long lastCheck = U.currentTimeMillis();

        /** Whether the holder is acquired or not. */
        private boolean acquired;

        /**
         * Shrinks array size if needed.
         */
        private void shrink() {
            long now = U.currentTimeMillis();

            if (now - lastCheck >= CHECK_FREQ) {
                int halfSize = data.length >> 1;

                if (maxMsgSize < halfSize)
                    data = new byte[halfSize];

                lastCheck = now;
            }
        }
    }
}