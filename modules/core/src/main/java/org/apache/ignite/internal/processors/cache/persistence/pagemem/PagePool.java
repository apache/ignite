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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.apache.ignite.internal.util.offheap.GridOffHeapOutOfMemoryException;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class PagePool {
    /** Relative pointer chunk index mask. */
    static final long SEGMENT_INDEX_MASK = 0xFFFFFF0000000000L;

    /** Address mask to avoid ABA problem. */
    private static final long ADDRESS_MASK = 0xFFFFFFFFFFFFFFL;

    /** Counter increment to avoid ABA problem. */
    private static final long COUNTER_INC = ADDRESS_MASK + 1;

    /** Counter mask to avoid ABA problem. */
    private static final long COUNTER_MASK = ~ADDRESS_MASK;

    /** Segment index. */
    protected final int idx;

    /** Direct memory region. */
    protected final DirectMemoryRegion region;

    /** Pool pages counter. */
    protected final AtomicInteger pagesCntr = new AtomicInteger();

    /** */
    protected long lastAllocatedIdxPtr;

    /** Pointer to the address of the free page list. */
    protected long freePageListPtr;

    /** Pages base. */
    protected long pagesBase;

    /** System page size. */
    private final int sysPageSize;

    /** Instance of RW Lock Updater */
    private OffheapReadWriteLock rwLock;

    /**
     * @param idx Index.
     * @param region Region
     */
    protected PagePool(
        int idx,
        DirectMemoryRegion region,
        int sysPageSize,
        OffheapReadWriteLock rwLock
    ) {
        this.idx = idx;
        this.region = region;
        this.sysPageSize = sysPageSize;
        this.rwLock = rwLock;

        long base = (region.address() + 7) & ~0x7;

        freePageListPtr = base;

        base += 8;

        lastAllocatedIdxPtr = base;

        base += 8;

        // Align page start by
        pagesBase = base;

        GridUnsafe.putLong(freePageListPtr, PageMemoryImpl.INVALID_REL_PTR);
        GridUnsafe.putLong(lastAllocatedIdxPtr, 0L);
    }

    /**
     * Allocates a new free page.
     *
     * @param tag Tag to initialize page RW lock.
     * @return Relative pointer to the allocated page.
     * @throws GridOffHeapOutOfMemoryException If failed to allocate new free page.
     */
    public long borrowOrAllocateFreePage(int tag) throws GridOffHeapOutOfMemoryException {
        long relPtr = borrowFreePage();

        if (relPtr == PageMemoryImpl.INVALID_REL_PTR)
            relPtr = allocateFreePage(tag);

        if (relPtr != PageMemoryImpl.INVALID_REL_PTR && pagesCntr != null)
            pagesCntr.incrementAndGet();

        return relPtr;
    }

    /**
     * @return Relative pointer to a free page that was borrowed from the allocated pool.
     */
    private long borrowFreePage() {
        while (true) {
            long freePageRelPtrMasked = GridUnsafe.getLongVolatile(null, freePageListPtr);

            long freePageRelPtr = freePageRelPtrMasked & ADDRESS_MASK;

            if (freePageRelPtr != PageMemoryImpl.INVALID_REL_PTR) {
                long freePageAbsPtr = absolute(freePageRelPtr);

                long nextFreePageRelPtr = GridUnsafe.getLongVolatile(null, freePageAbsPtr) & ADDRESS_MASK;

                // nextFreePageRelPtr may be invalid because a concurrent thread may have already polled this value
                // and used it.
                long cnt = ((freePageRelPtrMasked & COUNTER_MASK) + COUNTER_INC) & COUNTER_MASK;

                if (GridUnsafe.compareAndSwapLong(null, freePageListPtr, freePageRelPtrMasked, nextFreePageRelPtr | cnt)) {
                    GridUnsafe.putLongVolatile(null, freePageAbsPtr, PageHeader.PAGE_MARKER);

                    return freePageRelPtr;
                }
            }
            else
                return PageMemoryImpl.INVALID_REL_PTR;
        }
    }

    /**
     * @param tag Tag to initialize page RW lock.
     * @return Relative pointer of the allocated page.
     * @throws GridOffHeapOutOfMemoryException If failed to allocate new free page.
     */
    private long allocateFreePage(int tag) throws GridOffHeapOutOfMemoryException {
        long limit = region.address() + region.size();

        while (true) {
            long lastIdx = GridUnsafe.getLongVolatile(null, lastAllocatedIdxPtr);

            // Check if we have enough space to allocate a page.
            if (pagesBase + (lastIdx + 1) * sysPageSize > limit)
                return PageMemoryImpl.INVALID_REL_PTR;

            if (GridUnsafe.compareAndSwapLong(null, lastAllocatedIdxPtr, lastIdx, lastIdx + 1)) {
                long absPtr = pagesBase + lastIdx * sysPageSize;

                assert (lastIdx & SEGMENT_INDEX_MASK) == 0L;

                long relative = relative(lastIdx);

                assert relative != PageMemoryImpl.INVALID_REL_PTR;

                PageHeader.initNew(absPtr, relative);

                rwLock.init(absPtr + PageMemoryImpl.PAGE_LOCK_OFFSET, tag);

                return relative;
            }
        }
    }

    /**
     * @param relPtr Relative pointer to free.
     * @return Resulting number of pages in pool if pages counter is enabled, 0 otherwise.
     */
    public int releaseFreePage(long relPtr) {
        long absPtr = absolute(relPtr);

        assert !PageHeader.isAcquired(absPtr) : "Release pinned page: " + PageHeader.fullPageId(absPtr);

        int resCntr = 0;

        if (pagesCntr != null)
            resCntr = pagesCntr.decrementAndGet();

        while (true) {
            long freePageRelPtrMasked = GridUnsafe.getLongVolatile(null, freePageListPtr);

            long freePageRelPtr = freePageRelPtrMasked & PageMemoryImpl.RELATIVE_PTR_MASK;

            GridUnsafe.putLongVolatile(null, absPtr, freePageRelPtr);

            long cnt = freePageRelPtrMasked & COUNTER_MASK;

            long relPtrWithCnt = (relPtr & ADDRESS_MASK) | cnt;

            if (GridUnsafe.compareAndSwapLong(null, freePageListPtr, freePageRelPtrMasked, relPtrWithCnt))
                return resCntr;
        }
    }

    /**
     * @param relativePtr Relative pointer.
     * @return Absolute pointer.
     */
    long absolute(long relativePtr) {
        int segIdx = (int)((relativePtr >> 40) & 0xFFFF);

        assert segIdx == idx : "expected=" + idx + ", actual=" + segIdx + ", relativePtr=" + U.hexLong(relativePtr);

        long pageIdx = relativePtr & ~SEGMENT_INDEX_MASK;

        long off = pageIdx * sysPageSize;

        return pagesBase + off;
    }

    /**
     * @param pageIdx Page index in the pool.
     * @return Relative pointer.
     */
    long relative(long pageIdx) {
        return pageIdx | ((long)idx) << 40;
    }

    /**
     * @return Max number of pages in the pool.
     */
    public int pages() {
        return (int)((region.size() - (pagesBase - region.address())) / sysPageSize);
    }

    /**
     * @return Number of pages in the list.
     */
    public int size() {
        return pagesCntr.get();
    }
}
