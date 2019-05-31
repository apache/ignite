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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.store;

import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageMetaInfoStore;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager.MemoryCalculator;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker.LOCK_OP_MASK;

/**
 *
 */
public class HeapPageMetaInfoStore implements PageMetaInfoStore {
    /**
     *
     */
    private static final int OVERHEAD_SIZE = 8 + 16 + 8 + 8;
    /**
     *
     */
    private static final int STEP = 4;
    /**
     *
     */
    private long[] arr;
    /**
     *
     */
    private final MemoryCalculator memoryCalc;

    /**
     *
     */
    public HeapPageMetaInfoStore(int capacity, @Nullable MemoryCalculator memoryCalc) {
        this.arr = new long[capacity * STEP];
        this.memoryCalc = memoryCalc;

        if (memoryCalc != null)
            memoryCalc.onHeapAllocated(arr.length * 8 + OVERHEAD_SIZE);
    }

    HeapPageMetaInfoStore(long[] arr) {
        this.arr = arr;
        memoryCalc = null;
    }

    /** {@inheritDoc} */
    @Override public int capacity() {
        return arr.length / STEP;
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] != 0)
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void add(int itemIdx, int op, int structureId, long pageId, long pageAddrHeader, long pageAddr) {
        arr[STEP * itemIdx] = pageId;
        arr[STEP * itemIdx + 1] = pageAddrHeader;
        arr[STEP * itemIdx + 2] = pageAddr;
        arr[STEP * itemIdx + 3] = meta(structureId, op);
    }

    /** {@inheritDoc} */
    @Override public void remove(int itemIdx) {
        arr[STEP * itemIdx] = 0;
        arr[STEP * itemIdx + 1] = 0;
        arr[STEP * itemIdx + 2] = 0;
        arr[STEP * itemIdx + 3] = 0;
    }

    /** {@inheritDoc} */
    @Override public int getOperation(int itemIdx) {
        long structureIdAndOp = arr[STEP * itemIdx + 3];

        return (int)((structureIdAndOp >> 32));
    }

    /** {@inheritDoc} */
    @Override public int getStructureId(int itemIdx) {
        long structureIdAndOp = arr[STEP * itemIdx + 3];

        return (int)(structureIdAndOp);
    }

    /** {@inheritDoc} */
    @Override public long getPageId(int itemIdx) {
        return arr[STEP * itemIdx];
    }

    /** {@inheritDoc} */
    @Override public long getPageAddrHeader(int itemIdx) {
        return arr[STEP * itemIdx + 1];
    }

    /** {@inheritDoc} */
    @Override public long getPageAddr(int itemIdx) {
        return arr[STEP * itemIdx + 2];
    }

    /** {@inheritDoc} */
    @Override public PageMetaInfoStore copy() {
        return new HeapPageMetaInfoStore(arr.clone());
    }

    /** {@inheritDoc} */
    @Override public void free() {
        if (memoryCalc != null)
            memoryCalc.onHeapFree(arr.length * 8 + OVERHEAD_SIZE);

        arr = null;
    }

    /**
     * Build long from two int.
     *
     * @param structureId Structure id.
     * @param op Operation.
     */
    private long meta(int structureId, int op) {
        long major = ((long)op) << 32;

        long minor = structureId & 0xFFFFFFFFL;

        return major | minor;
    }
}
