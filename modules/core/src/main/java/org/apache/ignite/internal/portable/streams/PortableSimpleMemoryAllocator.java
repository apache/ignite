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

import org.apache.ignite.internal.util.*;

import sun.misc.*;

/**
 * Naive implementation of portable memory allocator.
 */
public class PortableSimpleMemoryAllocator implements PortableMemoryAllocator {
    /** Unsafe. */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** Array offset: byte. */
    protected static final long BYTE_ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

    /** {@inheritDoc} */
    @Override public byte[] allocate(int size) {
        return new byte[size];
    }

    /** {@inheritDoc} */
    @Override public byte[] reallocate(byte[] data, int size) {
        byte[] newData = new byte[size];

        UNSAFE.copyMemory(data, BYTE_ARR_OFF, newData, BYTE_ARR_OFF, data.length);

        return newData;
    }

    /** {@inheritDoc} */
    @Override public void release(byte[] data, int maxMsgSize) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public long allocateDirect(int size) {
        return UNSAFE.allocateMemory(size);
    }

    /** {@inheritDoc} */
    @Override public long reallocateDirect(long addr, int size) {
        return UNSAFE.reallocateMemory(addr, size);
    }

    /** {@inheritDoc} */
    @Override public void releaseDirect(long addr) {
        UNSAFE.freeMemory(addr);
    }
}
