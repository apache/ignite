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

package org.apache.ignite.internal.pagememory.impl;

import static org.apache.ignite.internal.util.GridUnsafe.wrapPointer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.configuration.schemas.store.PageMemoryDataRegionConfiguration;
import org.apache.ignite.configuration.schemas.store.PageMemoryDataRegionView;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.mem.DirectMemoryProvider;
import org.apache.ignite.internal.pagememory.mem.DirectMemoryRegion;
import org.apache.ignite.internal.pagememory.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteSystemProperties;

/**
 * Page header structure is described by the following diagram.
 *
 * <p>When page is not allocated (in a free list):
 * <pre>
 * +--------+--------+---------------------------------------------+
 * |8 bytes |8 bytes |     PAGE_SIZE + PAGE_OVERHEAD - 16 bytes    |
 * +--------+--------+---------------------------------------------+
 * |Next ptr|Rel ptr |              Empty                          |
 * +--------+--------+---------------------------------------------+
 * </pre>
 * <p/>
 * When page is allocated and is in use:
 * <pre>
 * +--------+--------+--------+--------+---------------------------+
 * |8 bytes |8 bytes |8 bytes |8 bytes |        PAGE_SIZE          |
 * +--------+--------+--------+--------+---------------------------+
 * | Marker |Page ID |Pin CNT |  Lock  |        Page data          |
 * +--------+--------+--------+--------+---------------------------+
 * </pre>
 *
 * <p>Note that first 8 bytes of page header are used either for page marker or for next relative pointer depending
 * on whether the page is in use or not.
 */
public class PageMemoryNoStoreImpl implements PageMemory {
    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(PageMemoryNoStoreImpl.class);

    /** Ignite page memory concurrency level. */
    private static final String IGNITE_OFFHEAP_LOCK_CONCURRENCY_LEVEL = "IGNITE_OFFHEAP_LOCK_CONCURRENCY_LEVEL";

    /** Marker bytes that signify beginning of used page in memory. */
    public static final long PAGE_MARKER = 0xBEEAAFDEADBEEF01L;

    /** Full relative pointer mask. */
    private static final long RELATIVE_PTR_MASK = 0xFFFFFFFFFFFFFFL;

    /** Invalid relative pointer value. */
    private static final long INVALID_REL_PTR = RELATIVE_PTR_MASK;

    /** Address mask to avoid ABA problem. */
    private static final long ADDRESS_MASK = 0xFFFFFFFFFFFFFFL;

    /** Counter mask to avoid ABA problem. */
    private static final long COUNTER_MASK = ~ADDRESS_MASK;

    /** Counter increment to avoid ABA problem. */
    private static final long COUNTER_INC = ADDRESS_MASK + 1;

    /** Page ID offset. */
    public static final int PAGE_ID_OFFSET = 8;

    /** Page pin counter offset. */
    public static final int LOCK_OFFSET = 16;

    /**
     * Need a 8-byte pointer for linked list, 8 bytes for internal needs (flags),
     * 4 bytes cache ID, 8 bytes timestamp.
     */
    public static final int PAGE_OVERHEAD = LOCK_OFFSET + OffheapReadWriteLock.LOCK_SIZE;

    /** Number of bits required to store segment index. */
    private static final int SEG_BITS = 4;

    /** Number of bits required to store segment index. */
    private static final int SEG_CNT = (1 << SEG_BITS);

    /** Number of bits left to store page index. */
    private static final int IDX_BITS = PageIdUtils.PAGE_IDX_SIZE - SEG_BITS;

    /** Segment mask. */
    private static final int SEG_MASK = ~(-1 << SEG_BITS);

    /** Index mask. */
    private static final int IDX_MASK = ~(-1 << IDX_BITS);

    /** Page size. */
    private final int sysPageSize;

    /** Direct memory allocator. */
    private final DirectMemoryProvider directMemoryProvider;

    /** Data region configuration view. */
    private final PageMemoryDataRegionView dataRegionCfg;

    /** Head of the singly linked list of free pages. */
    private final AtomicLong freePageListHead = new AtomicLong(INVALID_REL_PTR);

    /** Segments array. */
    private volatile Segment[] segments;

    /** Lock for segments changes. */
    private final Object segmentsLock = new Object();

    /** Total number of pages loaded into memory. */
    private final AtomicInteger allocatedPages = new AtomicInteger();

    /** Offheap read write lock instance. */
    private final OffheapReadWriteLock rwLock;

    /** Concurrency level. */
    private final int lockConcLvl = IgniteSystemProperties.getInteger(
            IGNITE_OFFHEAP_LOCK_CONCURRENCY_LEVEL,
            Integer.highestOneBit(Runtime.getRuntime().availableProcessors() * 4)
    );

    /** Total number of pages may be allocated for this instance. */
    private final int totalPages;

    /** Flag for enabling of acquired pages tracking. */
    private final boolean trackAcquiredPages;

    /** Page IO registry. */
    private final PageIoRegistry ioRegistry;

    /**
     * {@code False} if memory was not started or already stopped and is not supposed for any usage.
     */
    private volatile boolean started;

    /**
     * Constructor.
     *
     * @param directMemoryProvider Memory allocator to use.
     * @param dataRegionCfg Data region configuration.
     * @param ioRegistry IO registry.
     */
    public PageMemoryNoStoreImpl(
            DirectMemoryProvider directMemoryProvider,
            PageMemoryDataRegionConfiguration dataRegionCfg,
            PageIoRegistry ioRegistry
    ) {
        this.directMemoryProvider = directMemoryProvider;
        this.ioRegistry = ioRegistry;
        this.trackAcquiredPages = false;
        this.dataRegionCfg = (PageMemoryDataRegionView) dataRegionCfg.value();

        int pageSize = this.dataRegionCfg.pageSize();

        sysPageSize = pageSize + PAGE_OVERHEAD;

        assert sysPageSize % 8 == 0 : sysPageSize;

        totalPages = (int) (this.dataRegionCfg.maxSize() / sysPageSize);

        rwLock = new OffheapReadWriteLock(lockConcLvl);
    }

    @Override
    public void start() throws IgniteInternalException {
        synchronized (segmentsLock) {
            if (started) {
                return;
            }

            started = true;

            long startSize = dataRegionCfg.initSize();
            long maxSize = dataRegionCfg.maxSize();

            long[] chunks = new long[SEG_CNT];

            chunks[0] = startSize;

            long total = startSize;

            long allocChunkSize = Math.max((maxSize - startSize) / (SEG_CNT - 1), 256L * 1024 * 1024);

            int lastIdx = 0;

            for (int i = 1; i < SEG_CNT; i++) {
                long allocSize = Math.min(allocChunkSize, maxSize - total);

                if (allocSize <= 0) {
                    break;
                }

                chunks[i] = allocSize;

                total += allocSize;

                lastIdx = i;
            }

            if (lastIdx != SEG_CNT - 1) {
                chunks = Arrays.copyOf(chunks, lastIdx + 1);
            }

            if (segments == null) {
                directMemoryProvider.initialize(chunks);
            }

            addSegment(null);
        }
    }

    @Override
    public void stop(boolean deallocate) throws IgniteInternalException {
        synchronized (segmentsLock) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Stopping page memory.");
            }

            started = false;

            directMemoryProvider.shutdown(deallocate);

            if (directMemoryProvider instanceof Closeable) {
                try {
                    ((Closeable) directMemoryProvider).close();
                } catch (IOException e) {
                    throw new IgniteInternalException(e);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer pageBuffer(long pageAddr) {
        return wrapPointer(pageAddr, pageSize());
    }

    /** {@inheritDoc} */
    @Override public long allocatePage(int grpId, int partId, byte flags) {
        assert started;

        long relPtr = borrowFreePage();
        long absPtr = 0;

        if (relPtr != INVALID_REL_PTR) {
            int pageIdx = PageIdUtils.pageIndex(relPtr);

            Segment seg = segment(pageIdx);

            absPtr = seg.absolute(pageIdx);
        } else {
            // No segments contained a free page.
            Segment[] seg0 = segments;
            Segment allocSeg = seg0[seg0.length - 1];

            while (allocSeg != null) {
                relPtr = allocSeg.allocateFreePage(flags);

                if (relPtr != INVALID_REL_PTR) {
                    absPtr = allocSeg.absolute(PageIdUtils.pageIndex(relPtr));

                    allocatedPages.incrementAndGet();

                    break;
                } else {
                    allocSeg = addSegment(seg0);
                }
            }
        }

        if (relPtr == INVALID_REL_PTR) {
            IgniteOutOfMemoryException oom = new IgniteOutOfMemoryException("Out of memory in data region ["
                    + "name=" + dataRegionCfg.name()
                    + ", initSize=" + IgniteUtils.readableSize(dataRegionCfg.initSize(), false)
                    + ", maxSize=" + IgniteUtils.readableSize(dataRegionCfg.maxSize(), false)
                    + ", persistenceEnabled=" + dataRegionCfg.persistent() + "] Try the following:\n"
                    + "  ^-- Increase maximum off-heap memory size (DataRegionConfiguration.maxSize)\n"
                    + "  ^-- Enable Ignite persistence (DataRegionConfiguration.persistenceEnabled)\n"
                    + "  ^-- Enable eviction or expiration policies"
            );

            //TODO Fail node with failure handler.

            throw oom;
        }

        assert (relPtr & ~PageIdUtils.PAGE_IDX_MASK) == 0 : IgniteUtils.hexLong(relPtr & ~PageIdUtils.PAGE_IDX_MASK);

        // Assign page ID according to flags and partition ID.
        long pageId = PageIdUtils.pageId(partId, flags, (int) relPtr);

        writePageId(absPtr, pageId);

        GridUnsafe.zeroMemory(absPtr + PAGE_OVERHEAD, sysPageSize - PAGE_OVERHEAD);

        return pageId;
    }

    /** {@inheritDoc} */
    @Override public boolean freePage(int grpId, long pageId) {
        assert started;

        releaseFreePage(pageId);

        return true;
    }

    /** {@inheritDoc} */
    @Override public int pageSize() {
        return sysPageSize - PAGE_OVERHEAD;
    }

    /** {@inheritDoc} */
    @Override public int systemPageSize() {
        return sysPageSize;
    }

    /** {@inheritDoc} */
    @Override public int realPageSize(int grpId) {
        return pageSize();
    }

    /** {@inheritDoc} */
    @Override public long loadedPages() {
        return allocatedPages.get();
    }

    /**
     * Returns a total number of pages may be allocated for this instance.
     */
    public int totalPages() {
        return totalPages;
    }

    /**
     * Return a total number of acquired pages.
     */
    public long acquiredPages() {
        long total = 0;

        for (Segment seg : segments) {
            seg.readLock().lock();

            try {
                int acquired = seg.acquiredPages();

                assert acquired >= 0;

                total += acquired;
            } finally {
                seg.readLock().unlock();
            }
        }

        return total;
    }

    /**
     * Writes page ID to the page at the given absolute position.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @param pageId Page ID to write.
     */
    private void writePageId(long absPtr, long pageId) {
        GridUnsafe.putLong(absPtr + PAGE_ID_OFFSET, pageId);
    }

    /**
     * Returns the segment that contains given page.
     *
     * @param pageIdx Page index.
     * @return Segment.
     */
    private Segment segment(int pageIdx) {
        int segIdx = segmentIndex(pageIdx);

        return segments[segIdx];
    }

    /**
     * Extracts a segment index from the full page index.
     *
     * @param pageIdx Page index to extract segment index from.
     * @return Segment index.
     */
    private int segmentIndex(long pageIdx) {
        return (int) ((pageIdx >> IDX_BITS) & SEG_MASK);
    }

    /**
     * Creates a full page index.
     *
     * @param segIdx Segment index.
     * @param pageIdx Page index inside of the segment.
     * @return Full page index.
     */
    private long fromSegmentIndex(int segIdx, long pageIdx) {
        long res = 0;

        res = (res << SEG_BITS) | (segIdx & SEG_MASK);
        res = (res << IDX_BITS) | (pageIdx & IDX_MASK);

        return res;
    }

    // *** PageSupport methods ***

    /** {@inheritDoc} */
    @Override public long acquirePage(int cacheId, long pageId) {
        return acquirePage(cacheId, pageId, IoStatisticsHolderNoOp.INSTANCE);
    }

    /** {@inheritDoc} */
    @Override public long acquirePage(int cacheId, long pageId, IoStatisticsHolder statHolder) {
        assert started;

        int pageIdx = PageIdUtils.pageIndex(pageId);

        Segment seg = segment(pageIdx);

        long absPtr = seg.acquirePage(pageIdx);

        statHolder.trackLogicalRead(absPtr + PAGE_OVERHEAD);

        return absPtr;
    }

    /** {@inheritDoc} */
    @Override public void releasePage(int cacheId, long pageId, long page) {
        assert started;

        if (trackAcquiredPages) {
            Segment seg = segment(PageIdUtils.pageIndex(pageId));

            seg.onPageRelease();
        }
    }

    /** {@inheritDoc} */
    @Override public long readLock(int cacheId, long pageId, long page) {
        assert started;

        if (rwLock.readLock(page + LOCK_OFFSET, PageIdUtils.tag(pageId))) {
            return page + PAGE_OVERHEAD;
        }

        return 0L;
    }

    /** {@inheritDoc} */
    @Override public long readLockForce(int cacheId, long pageId, long page) {
        assert started;

        if (rwLock.readLock(page + LOCK_OFFSET, -1)) {
            return page + PAGE_OVERHEAD;
        }

        return 0L;
    }

    /** {@inheritDoc} */
    @Override public void readUnlock(int cacheId, long pageId, long page) {
        assert started;

        rwLock.readUnlock(page + LOCK_OFFSET);
    }

    /** {@inheritDoc} */
    @Override public long writeLock(int cacheId, long pageId, long page) {
        assert started;

        if (rwLock.writeLock(page + LOCK_OFFSET, PageIdUtils.tag(pageId))) {
            return page + PAGE_OVERHEAD;
        }

        return 0L;
    }

    /** {@inheritDoc} */
    @Override public long tryWriteLock(int cacheId, long pageId, long page) {
        assert started;

        if (rwLock.tryWriteLock(page + LOCK_OFFSET, PageIdUtils.tag(pageId))) {
            return page + PAGE_OVERHEAD;
        }

        return 0L;
    }

    /** {@inheritDoc} */
    @Override
    public void writeUnlock(
            int cacheId,
            long pageId,
            long page,
            boolean dirtyFlag
    ) {
        assert started;

        long actualId = PageIo.getPageId(page + PAGE_OVERHEAD);

        rwLock.writeUnlock(page + LOCK_OFFSET, PageIdUtils.tag(actualId));
    }

    /** {@inheritDoc} */
    @Override public boolean isDirty(int cacheId, long pageId, long page) {
        // always false for page no store.
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public PageIoRegistry ioRegistry() {
        return ioRegistry;
    }

    /**
     * Converts page index into a sequence number.
     *
     * @param pageIdx Page index.
     * @return Total page sequence number.
     */
    public int pageSequenceNumber(int pageIdx) {
        Segment seg = segment(pageIdx);

        return seg.sequenceNumber(pageIdx);
    }

    /**
     * Converts sequential page number to the page index.
     *
     * @param seqNo Page sequence number.
     * @return Page index.
     */
    public int pageIndex(int seqNo) {
        Segment[] segs = segments;

        int low = 0;
        int high = segs.length - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;

            Segment seg = segs[mid];

            int cmp = seg.containsPageBySequence(seqNo);

            if (cmp < 0) {
                high = mid - 1;
            } else if (cmp > 0) {
                low = mid + 1;
            } else {
                return seg.pageIndex(seqNo);
            }
        }

        throw new IgniteInternalException("Allocated page must always be present in one of the segments [seqNo=" + seqNo
                + ", segments=" + Arrays.toString(segs) + ']');
    }

    /**
     * Adds a page to the free pages list.
     *
     * @param pageId Page ID to release.
     */
    private void releaseFreePage(long pageId) {
        int pageIdx = PageIdUtils.pageIndex(pageId);

        // Clear out flags and file ID.
        long relPtr = PageIdUtils.pageId(0, (byte) 0, pageIdx);

        Segment seg = segment(pageIdx);

        long absPtr = seg.absolute(pageIdx);

        // Second, write clean relative pointer instead of page ID.
        writePageId(absPtr, relPtr);

        // Third, link the free page.
        while (true) {
            long freePageRelPtrMasked = freePageListHead.get();

            long freePageRelPtr = freePageRelPtrMasked & RELATIVE_PTR_MASK;

            GridUnsafe.putLong(absPtr, freePageRelPtr);

            if (freePageListHead.compareAndSet(freePageRelPtrMasked, relPtr)) {
                allocatedPages.decrementAndGet();

                return;
            }
        }
    }

    /**
     * Returns a relative pointer to a free page that was borrowed from the allocated pool.
     */
    private long borrowFreePage() {
        while (true) {
            long freePageRelPtrMasked = freePageListHead.get();

            long freePageRelPtr = freePageRelPtrMasked & ADDRESS_MASK;

            // no free pages available
            if (freePageRelPtr == INVALID_REL_PTR) {
                return INVALID_REL_PTR;
            }

            int pageIdx = PageIdUtils.pageIndex(freePageRelPtr);

            Segment seg = segment(pageIdx);

            long freePageAbsPtr = seg.absolute(pageIdx);
            long nextFreePageRelPtr = GridUnsafe.getLong(freePageAbsPtr) & ADDRESS_MASK;
            long cnt = ((freePageRelPtrMasked & COUNTER_MASK) + COUNTER_INC) & COUNTER_MASK;

            if (freePageListHead.compareAndSet(freePageRelPtrMasked, nextFreePageRelPtr | cnt)) {
                GridUnsafe.putLong(freePageAbsPtr, PAGE_MARKER);

                allocatedPages.incrementAndGet();

                return freePageRelPtr;
            }
        }
    }

    /**
     * Attempts to add a new memory segment.
     *
     * @param oldRef Old segments array. If this method observes another segments array, it will allocate a new
     *      segment (if possible). If the array has already been updated, it will return the last element in the
     *      new array.
     * @return Added segment, if successfull, {@code null} if failed to add.
     */
    private synchronized Segment addSegment(Segment[] oldRef) {
        if (segments == oldRef) {
            DirectMemoryRegion region = directMemoryProvider.nextRegion();

            // No more memory is available.
            if (region == null) {
                return null;
            }

            if (oldRef != null) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("Allocated next memory segment [plcName=" + dataRegionCfg.name()
                            + ", chunkSize=" + IgniteUtils.readableSize(region.size(), true) + ']');
                }
            }

            Segment[] newRef = new Segment[oldRef == null ? 1 : oldRef.length + 1];

            if (oldRef != null) {
                System.arraycopy(oldRef, 0, newRef, 0, oldRef.length);
            }

            Segment lastSeg = oldRef == null ? null : oldRef[oldRef.length - 1];

            Segment allocated = new Segment(newRef.length - 1, region, lastSeg == null ? 0 : lastSeg.sumPages());

            allocated.init();

            newRef[newRef.length - 1] = allocated;

            segments = newRef;
        }

        // Only this synchronized method writes to segments, so it is safe to read twice.
        return segments[segments.length - 1];
    }

    private class Segment extends ReentrantReadWriteLock {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Segment index. */
        private final int idx;

        /** Direct memory chunk. */
        private final DirectMemoryRegion region;

        /** Last allocated page index. */
        private long lastAllocatedIdxPtr;

        /** Base address for all pages. */
        private long pagesBase;

        /** Capacity of all previous segments combined. */
        private final int pagesInPrevSegments;

        /** Segments capacity. */
        private int maxPages;

        /** Total number of currently acquired pages. */
        private final AtomicInteger acquiredPages;

        /**
         * Constructor.
         *
         * @param idx Index.
         * @param region Memory region to use.
         * @param pagesInPrevSegments Number of pages in previously allocated segments.
         */
        private Segment(int idx, DirectMemoryRegion region, int pagesInPrevSegments) {
            this.idx = idx;
            this.region = region;
            this.pagesInPrevSegments = pagesInPrevSegments;

            acquiredPages = new AtomicInteger();
        }

        /**
         * Initializes page memory segment.
         */
        private void init() {
            long base = region.address();

            lastAllocatedIdxPtr = base;

            base += 8;

            // Align by 8 bytes.
            pagesBase = (base + 7) & ~0x7;

            GridUnsafe.putLong(lastAllocatedIdxPtr, 0);

            long limit = region.address() + region.size();

            maxPages = (int) ((limit - pagesBase) / sysPageSize);
        }

        /**
         * Acquires a page from the segment..
         *
         * @param pageIdx Page index.
         * @return Page absolute pointer.
         */
        private long acquirePage(int pageIdx) {
            long absPtr = absolute(pageIdx);

            assert absPtr % 8 == 0 : absPtr;

            if (trackAcquiredPages) {
                acquiredPages.incrementAndGet();
            }

            return absPtr;
        }

        private void onPageRelease() {
            acquiredPages.decrementAndGet();
        }

        /**
         * Returns absolute pointer to the page with given index.
         *
         * @param pageIdx Page index.
         * @return Absolute pointer.
         */
        private long absolute(int pageIdx) {
            pageIdx &= IDX_MASK;

            long off = ((long) pageIdx) * sysPageSize;

            return pagesBase + off;
        }

        /**
         * Converts page index into a sequence number.
         *
         * @param pageIdx Page index with encoded segment.
         * @return Absolute page sequence number.
         */
        private int sequenceNumber(int pageIdx) {
            pageIdx &= IDX_MASK;

            return pagesInPrevSegments + pageIdx;
        }

        /**
         * Returns a page sequence number upper bound.
         */
        private int sumPages() {
            return pagesInPrevSegments + maxPages;
        }

        /**
         * Returns a total number of currently acquired pages.
         */
        private int acquiredPages() {
            return acquiredPages.get();
        }

        /**
         * Allocates new page in the segment.
         *
         * @param tag Tag to initialize RW lock.
         * @return Relative pointer of the allocated page or {@link #INVALID_REL_PTR} if segment is overflown.
         */
        private long allocateFreePage(int tag) {
            long limit = region.address() + region.size();

            while (true) {
                long lastIdx = GridUnsafe.getLongVolatile(null, lastAllocatedIdxPtr);

                // Check if we have enough space to allocate a page.
                if (pagesBase + (lastIdx + 1) * sysPageSize > limit) {
                    return INVALID_REL_PTR;
                }

                if (GridUnsafe.compareAndSwapLong(null, lastAllocatedIdxPtr, lastIdx, lastIdx + 1)) {
                    long absPtr = pagesBase + lastIdx * sysPageSize;

                    assert lastIdx <= PageIdUtils.MAX_PAGE_NUM : lastIdx;

                    long pageIdx = fromSegmentIndex(idx, lastIdx);

                    assert pageIdx != INVALID_REL_PTR;

                    writePageId(absPtr, pageIdx);

                    GridUnsafe.putLong(absPtr, PAGE_MARKER);

                    rwLock.init(absPtr + LOCK_OFFSET, tag);

                    return pageIdx;
                }
            }
        }

        /**
         * Checks if given sequence number belongs to the current segment.
         *
         * @param seqNo Page sequence number.
         * @return {@code 0} if this segment contains the page with the given sequence number,
         *      {@code -1} if one of the previous segments contains the page with the given sequence number,
         *      {@code 1} if one of the next segments contains the page with the given sequence number.
         */
        public int containsPageBySequence(int seqNo) {
            if (seqNo < pagesInPrevSegments) {
                return -1;
            } else if (seqNo < pagesInPrevSegments + maxPages) {
                return 0;
            } else {
                return 1;
            }
        }

        /**
         * Converts sequential page number to the page index.
         *
         * @param seqNo Page sequence number.
         * @return Page index
         */
        public int pageIndex(int seqNo) {
            return PageIdUtils.pageIndex(fromSegmentIndex(idx, seqNo - pagesInPrevSegments));
        }
    }
}
