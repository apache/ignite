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

package org.apache.ignite.internal.pagemem.impl;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.apache.ignite.internal.util.offheap.GridOffHeapOutOfMemoryException;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_OFFHEAP_LOCK_CONCURRENCY_LEVEL;
import static org.apache.ignite.internal.util.GridUnsafe.wrapPointer;

/**
 * Page header structure is described by the following diagram.
 *
 * When page is not allocated (in a free list):
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
 * Note that first 8 bytes of page header are used either for page marker or for next relative pointer depending
 * on whether the page is in use or not.
 */
@SuppressWarnings({"LockAcquiredButNotSafelyReleased", "FieldAccessedSynchronizedAndUnsynchronized"})
public class PageMemoryNoStoreImpl implements PageMemory {
    /** */
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
    private int sysPageSize;

    /** */
    private final IgniteLogger log;

    /** Direct memory allocator. */
    private final DirectMemoryProvider directMemoryProvider;

    /** Name of DataRegion this PageMemory is associated with. */
    private final DataRegionConfiguration dataRegionCfg;

    /** Object to collect memory usage metrics. */
    private final DataRegionMetricsImpl memMetrics;

    /** */
    private AtomicLong freePageListHead = new AtomicLong(INVALID_REL_PTR);

    /** Segments array. */
    private volatile Segment[] segments;

    /** */
    private final AtomicInteger allocatedPages = new AtomicInteger();

    /** */
    private AtomicInteger selector = new AtomicInteger();

    /** */
    private OffheapReadWriteLock rwLock;

    /** Concurrency lvl. */
    private final int lockConcLvl = IgniteSystemProperties.getInteger(
        IGNITE_OFFHEAP_LOCK_CONCURRENCY_LEVEL,
        IgniteUtils.nearestPow2(Runtime.getRuntime().availableProcessors() * 4)
    );

    /** */
    private final int totalPages;

    /** */
    private final boolean trackAcquiredPages;

    /** Shared context. */
    private final GridCacheSharedContext<?, ?> ctx;

    /**
     * @param log Logger.
     * @param directMemoryProvider Memory allocator to use.
     * @param sharedCtx Cache shared context.
     * @param pageSize Page size.
     * @param dataRegionCfg Data region configuration.
     * @param memMetrics Memory Metrics.
     * @param trackAcquiredPages If {@code true} tracks number of allocated pages (for tests purpose only).
     */
    public PageMemoryNoStoreImpl(
        IgniteLogger log,
        DirectMemoryProvider directMemoryProvider,
        GridCacheSharedContext<?, ?> sharedCtx,
        int pageSize,
        DataRegionConfiguration dataRegionCfg,
        DataRegionMetricsImpl memMetrics,
        boolean trackAcquiredPages
    ) {
        assert log != null || sharedCtx != null;
        assert pageSize % 8 == 0;

        this.log = sharedCtx != null ? sharedCtx.logger(PageMemoryNoStoreImpl.class) : log;
        this.directMemoryProvider = directMemoryProvider;
        this.trackAcquiredPages = trackAcquiredPages;
        this.memMetrics = memMetrics;
        this.dataRegionCfg = dataRegionCfg;
        this.ctx = sharedCtx;

        sysPageSize = pageSize + PAGE_OVERHEAD;

        assert sysPageSize % 8 == 0 : sysPageSize;

        totalPages = (int)(dataRegionCfg.getMaxSize() / sysPageSize);

        rwLock = new OffheapReadWriteLock(lockConcLvl);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        long startSize = dataRegionCfg.getInitialSize();
        long maxSize = dataRegionCfg.getMaxSize();

        long[] chunks = new long[SEG_CNT];

        chunks[0] = startSize;

        long total = startSize;

        long allocChunkSize = Math.max((maxSize - startSize) / (SEG_CNT - 1), 256L * 1024 * 1024);

        int lastIdx = 0;

        for (int i = 1; i < SEG_CNT; i++) {
            long allocSize = Math.min(allocChunkSize, maxSize - total);

            if (allocSize <= 0)
                break;

            chunks[i] = allocSize;

            total += allocSize;

            lastIdx = i;
        }

        if (lastIdx != SEG_CNT - 1)
            chunks = Arrays.copyOf(chunks, lastIdx + 1);

        if (segments == null)
            directMemoryProvider.initialize(chunks);

        addSegment(null);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("OverlyStrongTypeCast")
    @Override public void stop() throws IgniteException {
        if (log.isDebugEnabled())
            log.debug("Stopping page memory.");

        directMemoryProvider.shutdown();

        if (directMemoryProvider instanceof Closeable) {
            try {
                ((Closeable)directMemoryProvider).close();
            }
            catch (IOException e) {
                throw new IgniteException(e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer pageBuffer(long pageAddr) {
        return wrapPointer(pageAddr, pageSize());
    }

    /** {@inheritDoc} */
    @Override public long allocatePage(int grpId, int partId, byte flags) {
        long relPtr = borrowFreePage();
        long absPtr = 0;

        if (relPtr != INVALID_REL_PTR) {
            int pageIdx = PageIdUtils.pageIndex(relPtr);

            Segment seg = segment(pageIdx);

            absPtr = seg.absolute(pageIdx);
        }

        // No segments contained a free page.
        if (relPtr == INVALID_REL_PTR) {
            Segment[] seg0 = segments;
            Segment allocSeg = seg0[seg0.length - 1];

            while (allocSeg != null) {
                relPtr = allocSeg.allocateFreePage(flags);

                if (relPtr != INVALID_REL_PTR) {
                    if (relPtr != INVALID_REL_PTR) {
                        absPtr = allocSeg.absolute(PageIdUtils.pageIndex(relPtr));

                        break;
                    }
                }
                else
                    allocSeg = addSegment(seg0);
            }
        }

        if (relPtr == INVALID_REL_PTR) {
            IgniteOutOfMemoryException oom = new IgniteOutOfMemoryException("Out of memory in data region [" +
                "name=" + dataRegionCfg.getName() +
                ", initSize=" + U.readableSize(dataRegionCfg.getInitialSize(), false) +
                ", maxSize=" + U.readableSize(dataRegionCfg.getMaxSize(), false) +
                ", persistenceEnabled=" + dataRegionCfg.isPersistenceEnabled() + "] Try the following:" + U.nl() +
                "  ^-- Increase maximum off-heap memory size (DataRegionConfiguration.maxSize)" + U.nl() +
                "  ^-- Enable Ignite persistence (DataRegionConfiguration.persistenceEnabled)" + U.nl() +
                "  ^-- Enable eviction or expiration policies"
            );

            if (ctx != null)
                ctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, oom));

            throw oom;
        }


        assert (relPtr & ~PageIdUtils.PAGE_IDX_MASK) == 0 : U.hexLong(relPtr & ~PageIdUtils.PAGE_IDX_MASK);

        // Assign page ID according to flags and partition ID.
        long pageId = PageIdUtils.pageId(partId, flags, (int)relPtr);

        writePageId(absPtr, pageId);

        // TODO pass an argument to decide whether the page should be cleaned.
        GridUnsafe.setMemory(absPtr + PAGE_OVERHEAD, sysPageSize - PAGE_OVERHEAD, (byte)0);

        return pageId;
    }

    /** {@inheritDoc} */
    @Override public boolean freePage(int cacheId, long pageId) {
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

    /**
     * @return Next index.
     */
    private int nextRoundRobinIndex() {
        while (true) {
            int idx = selector.get();

            int nextIdx = idx + 1;

            if (nextIdx >= segments.length)
                nextIdx = 0;

            if (selector.compareAndSet(idx, nextIdx))
                return nextIdx;
        }
    }

    /** {@inheritDoc} */
    @Override public long loadedPages() {
        return allocatedPages.get();
    }

    /**
     * @return Total number of pages may be allocated for this instance.
     */
    public int totalPages() {
        return totalPages;
    }

    /**
     * @return Total number of acquired pages.
     */
    public long acquiredPages() {
        long total = 0;

        for (Segment seg : segments) {
            seg.readLock().lock();

            try {
                int acquired = seg.acquiredPages();

                assert acquired >= 0;

                total += acquired;
            }
            finally {
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
     * @param pageIdx Page index.
     * @return Segment.
     */
    private Segment segment(int pageIdx) {
        int segIdx = segmentIndex(pageIdx);

        return segments[segIdx];
    }

    /**
     * @param pageIdx Page index to extract segment index from.
     * @return Segment index.
     */
    private int segmentIndex(long pageIdx) {
        return (int)((pageIdx >> IDX_BITS) & SEG_MASK);
    }

    /**
     * @param segIdx Segment index.
     * @param pageIdx Page index.
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
        int pageIdx = PageIdUtils.pageIndex(pageId);

        Segment seg = segment(pageIdx);

        return seg.acquirePage(pageIdx);
    }

    /** {@inheritDoc} */
    @Override public void releasePage(int cacheId, long pageId, long page) {
        if (trackAcquiredPages) {
            Segment seg = segment(PageIdUtils.pageIndex(pageId));

            seg.onPageRelease();
        }
    }

    /** {@inheritDoc} */
    @Override public long readLock(int cacheId, long pageId, long page) {
        if (rwLock.readLock(page + LOCK_OFFSET, PageIdUtils.tag(pageId)))
            return page + PAGE_OVERHEAD;

        return 0L;
    }

    /** {@inheritDoc} */
    @Override public long readLockForce(int cacheId, long pageId, long page) {
        if (rwLock.readLock(page + LOCK_OFFSET, -1))
            return page + PAGE_OVERHEAD;

        return 0L;
    }

    /** {@inheritDoc} */
    @Override public void readUnlock(int cacheId, long pageId, long page) {
        rwLock.readUnlock(page + LOCK_OFFSET);
    }

    /** {@inheritDoc} */
    @Override public long writeLock(int cacheId, long pageId, long page) {
        if (rwLock.writeLock(page + LOCK_OFFSET, PageIdUtils.tag(pageId)))
            return page + PAGE_OVERHEAD;

        return 0L;
    }

    /** {@inheritDoc} */
    @Override public long tryWriteLock(int cacheId, long pageId, long page) {
        if (rwLock.tryWriteLock(page + LOCK_OFFSET, PageIdUtils.tag(pageId)))
            return page + PAGE_OVERHEAD;

        return 0L;
    }

    /** {@inheritDoc} */
    @Override public void writeUnlock(
        int cacheId,
        long pageId,
        long page,
        Boolean walPlc,
        boolean dirtyFlag
    ) {
        long actualId = PageIO.getPageId(page + PAGE_OVERHEAD);

        rwLock.writeUnlock(page + LOCK_OFFSET, PageIdUtils.tag(actualId));
    }

    /** {@inheritDoc} */
    @Override public boolean isDirty(int cacheId, long pageId, long page) {
        // always false for page no store.
        return false;
    }

    /**
     * @param pageIdx Page index.
     * @return Total page sequence number.
     */
    public int pageSequenceNumber(int pageIdx) {
        Segment seg = segment(pageIdx);

        return seg.sequenceNumber(pageIdx);
    }

    /**
     * @param seqNo Page sequence number.
     * @return Page index.
     */
    public int pageIndex(int seqNo) {
        Segment[] segs = segments;

        int low = 0, high = segs.length - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;

            Segment seg = segs[mid];

            int cmp = seg.containsPageBySequence(seqNo);

            if (cmp < 0)
                high = mid - 1;
            else if (cmp > 0)
                low = mid + 1;
            else
                return seg.pageIndex(seqNo);
        }

        throw new IgniteException("Allocated page must always be present in one of the segments [seqNo=" + seqNo +
            ", segments=" + Arrays.toString(segs) + ']');
    }

    /**
     * @param pageId Page ID to release.
     */
    private void releaseFreePage(long pageId) {
        int pageIdx = PageIdUtils.pageIndex(pageId);

        // Clear out flags and file ID.
        long relPtr = PageIdUtils.pageId(0, (byte)0, pageIdx);

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

                memMetrics.updateTotalAllocatedPages(-1L);

                return;
            }
        }
    }

    /**
     * @return Relative pointer to a free page that was borrowed from the allocated pool.
     */
    private long borrowFreePage() {
        while (true) {
            long freePageRelPtrMasked = freePageListHead.get();

            long freePageRelPtr = freePageRelPtrMasked & ADDRESS_MASK;

            if (freePageRelPtr != INVALID_REL_PTR) {
                int pageIdx = PageIdUtils.pageIndex(freePageRelPtr);

                Segment seg = segment(pageIdx);

                long freePageAbsPtr = seg.absolute(pageIdx);
                long nextFreePageRelPtr = GridUnsafe.getLong(freePageAbsPtr) & ADDRESS_MASK;
                long cnt = ((freePageRelPtrMasked & COUNTER_MASK) + COUNTER_INC) & COUNTER_MASK;

                if (freePageListHead.compareAndSet(freePageRelPtrMasked, nextFreePageRelPtr | cnt)) {
                    GridUnsafe.putLong(freePageAbsPtr, PAGE_MARKER);

                    allocatedPages.incrementAndGet();

                    memMetrics.updateTotalAllocatedPages(1L);

                    return freePageRelPtr;
                }
            }
            else
                return INVALID_REL_PTR;
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
            if (region == null)
                return null;

            if (oldRef != null) {
                if (log.isInfoEnabled())
                    log.info("Allocated next memory segment [plcName=" + dataRegionCfg.getName() +
                        ", chunkSize=" + U.readableSize(region.size(), true) + ']');
            }

            Segment[] newRef = new Segment[oldRef == null ? 1 : oldRef.length + 1];

            if (oldRef != null)
                System.arraycopy(oldRef, 0, newRef, 0, oldRef.length);

            Segment lastSeg = oldRef == null ? null : oldRef[oldRef.length - 1];

            Segment allocated = new Segment(newRef.length - 1, region, lastSeg == null ? 0 : lastSeg.sumPages());

            allocated.init();

            newRef[newRef.length - 1] = allocated;

            segments = newRef;
        }

        // Only this synchronized method writes to segments, so it is safe to read twice.
        return segments[segments.length - 1];
    }

    /**
     *
     */
    private class Segment extends ReentrantReadWriteLock {
        /** */
        private static final long serialVersionUID = 0L;

        /** Segment index. */
        private int idx;

        /** Direct memory chunk. */
        private DirectMemoryRegion region;

        /** Last allocated page index. */
        private long lastAllocatedIdxPtr;

        /** Base address for all pages. */
        private long pagesBase;

        /** */
        private int pagesInPrevSegments;

        /** */
        private int maxPages;

        /** */
        private final AtomicInteger acquiredPages;

        /**
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

            maxPages = (int)((limit - pagesBase) / sysPageSize);
        }

        /**
         * @param pageIdx Page index.
         * @return Page absolute pointer.
         */
        private long acquirePage(int pageIdx) {
            long absPtr = absolute(pageIdx);

            assert absPtr % 8 == 0 : absPtr;

            if (trackAcquiredPages)
                acquiredPages.incrementAndGet();

            return absPtr;
        }

        /**
         */
        private void onPageRelease() {
            acquiredPages.decrementAndGet();
        }

        /**
         * @param pageIdx Page index.
         * @return Absolute pointer.
         */
        private long absolute(int pageIdx) {
            pageIdx &= IDX_MASK;

            long off = ((long)pageIdx) * sysPageSize;

            return pagesBase + off;
        }

        /**
         * @param pageIdx Page index with encoded segment.
         * @return Absolute page sequence number.
         */
        private int sequenceNumber(int pageIdx) {
            pageIdx &= IDX_MASK;

            return pagesInPrevSegments + pageIdx;
        }

        /**
         * @return Page sequence number upper bound.
         */
        private int sumPages() {
            return pagesInPrevSegments + maxPages;
        }

        /**
         * @return Total number of currently acquired pages.
         */
        private int acquiredPages() {
            return acquiredPages.get();
        }

        /**
         * @param tag Tag to initialize RW lock.
         * @return Relative pointer of the allocated page.
         * @throws GridOffHeapOutOfMemoryException If failed to allocate.
         */
        private long allocateFreePage(int tag) throws GridOffHeapOutOfMemoryException {
            long limit = region.address() + region.size();

            while (true) {
                long lastIdx = GridUnsafe.getLongVolatile(null, lastAllocatedIdxPtr);

                // Check if we have enough space to allocate a page.
                if (pagesBase + (lastIdx + 1) * sysPageSize > limit)
                    return INVALID_REL_PTR;

                if (GridUnsafe.compareAndSwapLong(null, lastAllocatedIdxPtr, lastIdx, lastIdx + 1)) {
                    long absPtr = pagesBase + lastIdx * sysPageSize;

                    assert lastIdx <= PageIdUtils.MAX_PAGE_NUM : lastIdx;

                    long pageIdx = fromSegmentIndex(idx, lastIdx);

                    assert pageIdx != INVALID_REL_PTR;

                    writePageId(absPtr, pageIdx);

                    GridUnsafe.putLong(absPtr, PAGE_MARKER);

                    rwLock.init(absPtr + LOCK_OFFSET, tag);

                    allocatedPages.incrementAndGet();

                    memMetrics.updateTotalAllocatedPages(1L);

                    return pageIdx;
                }
            }
        }

        /**
         * @param seqNo Page sequence number.
         * @return {@code 0} if this segment contains the page with the given sequence number,
         *      {@code -1} if one of the previous segments contains the page with the given sequence number,
         *      {@code 1} if one of the next segments contains the page with the given sequence number.
         */
        public int containsPageBySequence(int seqNo) {
            if (seqNo < pagesInPrevSegments)
                return -1;
            else if (seqNo < pagesInPrevSegments + maxPages)
                return 0;
            else
                return 1;
        }

        /**
         * @param seqNo Page sequence number.
         * @return Page index
         */
        public int pageIndex(int seqNo) {
            return PageIdUtils.pageIndex(fromSegmentIndex(idx, seqNo - pagesInPrevSegments));
        }
    }

    /** {@inheritDoc} */
    @Override public int checkpointBufferPagesCount() {
        return 0;
    }
}
