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

/**
 *
 */
public class HeapLongStore implements LongStore {
    /** */
    private static final int OVERHEAD_SIZE = 8 + 16 + 8 + 8;
    /** */
    private long[] arr;
    /** */
    private final MemoryCalculator memoryCalc;

    /** */
    public HeapLongStore(int capacity, MemoryCalculator memoryCalc) {
        this.arr = new long[capacity * 2];
        this.memoryCalc = memoryCalc;

        memoryCalc.onHeapAllocated(arr.length * 8 + OVERHEAD_SIZE);
    }

    /** {@inheritDoc} */
    @Override public int capacity() {
        return arr.length / 2;
    }

    /** {@inheritDoc} */
    @Override public long getByIndex(int idx) {
        return arr[idx];
    }

    /** {@inheritDoc} */
    @Override public void setByIndex(int idx, long val) {
        arr[idx] = val;
    }

    /** {@inheritDoc} */
    @Override public long[] copy() {
        return arr.clone();
    }

    /** {@inheritDoc} */
    @Override public void free() {
        memoryCalc.onOffHeapFree(arr.length * 8 + OVERHEAD_SIZE);

        arr = null;
    }
}
