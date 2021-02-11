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

import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager.MemoryCalculator;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageMetaInfoStore;
import org.apache.ignite.internal.util.GridUnsafe;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker.LOCK_OP_MASK;

/**
 *
 */
public class OffHeapPageMetaInfoStore implements PageMetaInfoStore {
    /**
     *
     */
    private static final long OVERHEAD_SIZE = 16 + 4 + 4 + 8 + 8;

    /**
     *
     */
    private static final int PAGE_ID_OFFSET = 0;

    /**
     *
     */
    private static final int PAGE_HEADER_ADDRESS_OFFSET = 8;

    /**
     *
     */
    private static final int PAGE_ADDRESS_OFFSET = 16;

    /**
     *
     */
    private static final int PAGE_META_OFFSET = 24;

    /**
     *
     */
    private static final int ITEM_SIZE = 4;

    /**
     *
     */
    private final int size;

    /**
     *
     */
    private final int capacity;

    /**
     *
     */
    private final long ptr;

    /**
     *
     */
    private final MemoryCalculator memCalc;

    /**
     *
     */
    public OffHeapPageMetaInfoStore(int capacity, @Nullable MemoryCalculator memCalc) {
        this.capacity = capacity;
        this.size = this.capacity * (8 * ITEM_SIZE);
        this.ptr = allocate(size);
        this.memCalc = memCalc;

        if (memCalc != null) {
            memCalc.onHeapAllocated(OVERHEAD_SIZE);
            memCalc.onOffHeapAllocated(size);
        }
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
        return capacity;
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        for (int i = 0; i < size; i++) {
            if (GridUnsafe.getByte(ptr + i) != 0)
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void add(int itemIdx, int op, int structureId, long pageId, long pageAddrHeader, long pageAddr) {
        GridUnsafe.putLong(offset(itemIdx) + PAGE_ID_OFFSET, pageId);
        GridUnsafe.putLong(offset(itemIdx) + PAGE_HEADER_ADDRESS_OFFSET, pageAddrHeader);
        GridUnsafe.putLong(offset(itemIdx) + PAGE_ADDRESS_OFFSET, pageAddr);
        GridUnsafe.putLong(offset(itemIdx) + PAGE_META_OFFSET, join(structureId, op));
    }

    /** {@inheritDoc} */
    @Override public void remove(int itemIdx) {
        GridUnsafe.putLong(offset(itemIdx) + PAGE_ID_OFFSET, 0);
        GridUnsafe.putLong(offset(itemIdx) + PAGE_HEADER_ADDRESS_OFFSET, 0);
        GridUnsafe.putLong(offset(itemIdx) + PAGE_ADDRESS_OFFSET, 0);
        GridUnsafe.putLong(offset(itemIdx) + PAGE_META_OFFSET, 0);
    }

    /** {@inheritDoc} */
    @Override public int getOperation(int itemIdx) {
        long structureIdAndOp = GridUnsafe.getLong(offset(itemIdx) + PAGE_META_OFFSET);

        return (int)((structureIdAndOp >> 32) & LOCK_OP_MASK);
    }

    /** {@inheritDoc} */
    @Override public int getStructureId(int itemIdx) {
        long structureIdAndOp = GridUnsafe.getLong(offset(itemIdx) + PAGE_META_OFFSET);

        return (int)(structureIdAndOp);
    }

    /** {@inheritDoc} */
    @Override public long getPageId(int itemIdx) {
        return GridUnsafe.getLong(offset(itemIdx) + PAGE_ID_OFFSET);
    }

    /** {@inheritDoc} */
    @Override public long getPageAddrHeader(int itemIdx) {
        return GridUnsafe.getLong(offset(itemIdx) + PAGE_HEADER_ADDRESS_OFFSET);
    }

    /** {@inheritDoc} */
    @Override public long getPageAddr(int itemIdx) {
        return GridUnsafe.getLong(offset(itemIdx) + PAGE_ADDRESS_OFFSET);
    }

    /**
     *
     */
    private long offset(long itemIdx) {
        long offset = ptr + itemIdx * 8 * ITEM_SIZE;

        assert offset >= ptr && offset <= ((ptr + size) - 8 * ITEM_SIZE) : "offset=" + (offset - ptr) + ", size=" + size;

        return offset;
    }

    /** {@inheritDoc} */
    @Override public PageMetaInfoStore copy() {
        long[] arr = new long[capacity * 4];

        GridUnsafe.copyMemory(null, ptr, arr, GridUnsafe.LONG_ARR_OFF, size);

        return new HeapPageMetaInfoStore(arr);
    }

    /** {@inheritDoc} */
    @Override public void free() {
        GridUnsafe.freeMemory(ptr);

        if (memCalc != null) {
            memCalc.onHeapFree(OVERHEAD_SIZE);
            memCalc.onOffHeapFree(size);
        }
    }

    /**
     * Build long from two int.
     *
     * @param structureId Structure id.
     * @param op Operation.
     */
    private long join(int structureId, int op) {
        long major = ((long)op) << 32;

        long minor = structureId & 0xFFFFFFFFL;

        return major | minor;
    }
}
