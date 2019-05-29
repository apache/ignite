/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.store;

import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LongStore;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager.MemoryCalculator;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 *
 */
public class OffHeapLongStore implements LongStore {
    /** */
    private static final long OVERHEAD_SIZE = 16 + 4 + 4 + 8 + 8;
    /** */
    private final int size;
    /** */
    private final int capacity;
    /** */
    private final long ptr;
    /** */
    private final MemoryCalculator memCalc;

    /**
     *
     */
    public OffHeapLongStore(int capacity, MemoryCalculator memCalc) {
        this.capacity = capacity * 2;
        this.size = this.capacity * 8;
        this.ptr = allocate(size);
        this.memCalc = memCalc;

        memCalc.onOffHeapAllocated(size + OVERHEAD_SIZE);
    }

    /**
     *
     */
    private long allocate(int size) {
        long ptr = GridUnsafe.allocateMemory(size);

        GridUnsafe.setMemory(ptr, size, (byte)0);

        return ptr;
    }

    /** {@inheritDoc} */
    @Override public int capacity() {
        return capacity / 2;
    }

    /** {@inheritDoc} */
    @Override public long getByIndex(int idx) {
        return GridUnsafe.getLong(ptr + offset(idx));
    }

    /** {@inheritDoc} */
    @Override public void setByIndex(int idx, long val) {
        GridUnsafe.putLong(ptr + offset(idx), val);
    }

    /**
     *
     */
    private long offset(long headIdx) {
        return headIdx * 8;
    }

    /** {@inheritDoc} */
    @Override public long[] copy() {
        long[] arr = new long[capacity];

        GridUnsafe.copyMemory(null, ptr, arr, GridUnsafe.LONG_ARR_OFF, size);

        return arr;
    }

    /** {@inheritDoc} */
    @Override public void free() {
        GridUnsafe.freeMemory(ptr);

        memCalc.onOffHeapFree(size + OVERHEAD_SIZE);
    }
}
