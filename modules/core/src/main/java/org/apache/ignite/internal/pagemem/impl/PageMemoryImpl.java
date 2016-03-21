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
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.mem.DirectMemory;
import org.apache.ignite.internal.mem.DirectMemoryFragment;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.OutOfMemoryException;
import org.apache.ignite.internal.pagemem.DirectMemoryUtils;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.offheap.GridOffHeapOutOfMemoryException;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lifecycle.LifecycleAware;
import sun.misc.JavaNioAccess;
import sun.misc.SharedSecrets;

/**
 *
 */
@SuppressWarnings({"LockAcquiredButNotSafelyReleased", "FieldAccessedSynchronizedAndUnsynchronized"})
public class  PageMemoryImpl implements PageMemory {
    /** Relative pointer chunk index mask. */
    private static final long CHUNK_INDEX_MASK = 0xFFFFFF0000000000L;

    /** Full relative pointer mask. */
    private static final long RELATIVE_PTR_MASK = 0xFFFFFFFFFFFFFFL;

    /** Dirty flag. */
    private static final long DIRTY_FLAG = 0x0100000000000000L;

    /** Invalid relative pointer value. */
    private static final long INVALID_REL_PTR = RELATIVE_PTR_MASK;

    /** Address mask to avoid ABA problem. */
    private static final long ADDRESS_MASK = 0xFFFFFFFFFFFFFFL;

    /** Counter mask to avoid ABA problem. */
    private static final long COUNTER_MASK = ~ADDRESS_MASK;

    /** Counter increment to avoid ABA problem. */
    private static final long COUNTER_INC = ADDRESS_MASK + 1;

    /** Page relative pointer. Does not change once a page is allocated. */
    public static final int RELATIVE_PTR_OFFSET = 8;

    /** Page ID offset  */
    public static final int PAGE_ID_OFFSET = 16;

    /** Need a 8-byte pointer for linked list and 8 bytes for internal needs. */
    public static final int PAGE_OVERHEAD = 24;

    /** Page size. */
    private int sysPageSize;

    /** Page store manager. */
    private IgnitePageStoreManager storeMgr;

    /** Direct byte buffer factory. */
    private JavaNioAccess nioAccess;

    // TODO mem should be replaced with platform-aware memory util.
    /** */
    private final DirectMemoryUtils mem = new DirectMemoryUtils();

    /** */
    private final IgniteLogger log;

    /** Direct memory allocator. */
    private final DirectMemoryProvider directMemoryProvider;

    /** Segments array. */
    private Segment[] segments;

    /** Current chunk from which new pages should be allocated. */
    private volatile Chunk currentChunk;

    /** All used chunks. */
    private final List<Chunk> chunks;

    /** Pointer to the address of the free page list. */
    private long freePageListPtr;

    /** */
    private long lastAllocatedPageIdPtr;

    /** */
    private long dbMetaPageIdPtr;

    /** Pages marked as dirty since the last checkpoint. */
    private Collection<Long> dirtyPages = new GridConcurrentHashSet<>();

    /** Pages captured for the checkpoint process. */
    private Collection<Long> checkpointPages;

    /**
     * @param log Logger to use.
     * @param directMemoryProvider Memory allocator to use.
     * @param pageSize Page size.
     * @param segments Number of segments.
     */
    public PageMemoryImpl(
        IgniteLogger log,
        DirectMemoryProvider directMemoryProvider,
        IgnitePageStoreManager storeMgr,
        int pageSize,
        int segments
    ) {
        if (segments == 0)
            segments = Runtime.getRuntime().availableProcessors() * 8;

        this.log = log;
        this.directMemoryProvider = directMemoryProvider;
        this.storeMgr = storeMgr;
        this.segments = new Segment[segments];

        chunks = new ArrayList<>();
        sysPageSize = pageSize + PAGE_OVERHEAD;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        if (directMemoryProvider instanceof LifecycleAware)
            ((LifecycleAware)directMemoryProvider).start();

        try {
            DirectMemory memory = directMemoryProvider.memory();

            nioAccess = SharedSecrets.getJavaNioAccess();

            if (memory.restored())
                initExisting(memory);
            else
                initNew(memory);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to initialize DirectBuffer class internals.", e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("OverlyStrongTypeCast")
    @Override public void stop() throws IgniteException {
        if (directMemoryProvider instanceof LifecycleAware)
            ((LifecycleAware)directMemoryProvider).stop();

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
    @Override public long allocatePage(int cacheId, int partId, byte flags) throws IgniteCheckedException {
        if (storeMgr != null)
            return storeMgr.allocatePage(cacheId, partId, flags);

        long pageId, absPtr;

        long relPtr = borrowFreePage();

        if (relPtr != INVALID_REL_PTR) {
            absPtr = absolute(relPtr);

            pageId = readPageId(absPtr);
        }
        else {
            while (true) {
                pageId = mem.readLong(lastAllocatedPageIdPtr);

                if (mem.compareAndSwapLong(lastAllocatedPageIdPtr, pageId, pageId + 1))
                    break;
            }

            relPtr = allocateFreePage();

            absPtr = absolute(relPtr);

            writePageId(absPtr, pageId);
        }

        // TODO pass an argument to decide whether the page should be cleaned.
        mem.setMemory(absPtr + PAGE_OVERHEAD, sysPageSize - PAGE_OVERHEAD, (byte)0);

        Segment seg = segment(pageId);

        seg.writeLock().lock();

        try {
            seg.loadedPages.put(pageId, relPtr);
        }
        finally {
            seg.writeLock().unlock();
        }

        return pageId;
    }


    /** {@inheritDoc} */
    @Override public boolean freePage(int cacheId, long pageId) throws IgniteCheckedException {
        Segment seg = segment(pageId);

        seg.writeLock().lock();

        try {
            if (seg.acquiredPages.get(pageId) != null)
                return false;

            long relPtr = seg.loadedPages.get(pageId, INVALID_REL_PTR);

            if (relPtr == INVALID_REL_PTR)
                return false;

            if (isDirty(absolute(relPtr)))
                return false;

            seg.loadedPages.remove(pageId);

            releaseFreePage(relPtr);
        }
        finally {
            seg.writeLock().unlock();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public Page metaPage() throws IgniteCheckedException {
        return page(mem.readLong(dbMetaPageIdPtr));
    }

    /** {@inheritDoc} */
    @Override public Page page(long pageId) throws IgniteCheckedException {
        Segment seg = segment(pageId);

        seg.readLock().lock();

        try {
            PageImpl page = seg.acquiredPages.get(pageId);

            if (page != null) {
                page.acquireReference();

                return page;
            }
        }
        finally {
            seg.readLock().unlock();
        }

        seg.writeLock().lock();

        try {
            // Double-check.
            PageImpl page = seg.acquiredPages.get(pageId);

            if (page != null) {
                page.acquireReference();

                return page;
            }

            long relPtr = seg.loadedPages.get(pageId, INVALID_REL_PTR);

            if (relPtr == INVALID_REL_PTR) {
                if (storeMgr == null)
                    throw new IllegalStateException("The page with the given page ID was not allocated: " +
                        U.hexLong(pageId));

                relPtr = borrowOrAllocateFreePage();

                long absPtr = absolute(relPtr);

                // We can clear dirty flag after the page has been allocated.
                setDirty(pageId, absPtr, false);

                seg.loadedPages.put(pageId, relPtr);

                page = new PageImpl(pageId, absPtr, this);

                if (storeMgr != null)
                    storeMgr.read(CU.cacheId("partitioned"), pageId, wrapPointer(absPtr + PAGE_OVERHEAD, pageSize())); // TODO cacheID
            }
            else
                page = new PageImpl(pageId, absolute(relPtr), this);

            seg.acquiredPages.put(pageId, page);

            page.acquireReference();

            return page;
        }
        finally {
            seg.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void releasePage(Page p) {
        PageImpl page = (PageImpl)p;

        Segment seg = segment(page.id());

        seg.writeLock().lock();

        try {
            if (page.releaseReference())
                seg.acquiredPages.remove(page.id());
        }
        finally {
            seg.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public int pageSize() {
        return sysPageSize - PAGE_OVERHEAD;
    }

    /** {@inheritDoc} */
    @Override public Collection<Long> beginCheckpoint() throws IgniteException {
        if (checkpointPages != null)
            throw new IgniteException("Failed to begin checkpoint (it is already in progress).");

        checkpointPages = dirtyPages;

        dirtyPages = new GridConcurrentHashSet<>();

        return checkpointPages;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void finishCheckpoint() {
        assert checkpointPages != null : "Checkpoint has not been started.";

        // Split page IDs by segment.
        Collection<Long>[] segCols = new Collection[segments.length];

        for (Long pageId : checkpointPages) {
            int segIdx = segmentIndex(pageId);

            Collection<Long> col = segCols[segIdx];

            if (col == null) {
                col = new ArrayList<>(checkpointPages.size() / segments.length + 1);

                segCols[segIdx] = col;
            }

            col.add(pageId);
        }

        // Lock segment by segment and flush changes.
        for (int i = 0; i < segCols.length; i++) {
            Collection<Long> col = segCols[i];

            if (col == null)
                continue;

            Segment seg = segments[i];

            seg.writeLock().lock();

            try {
                for (long pageId : col) {
                    PageImpl page = seg.acquiredPages.get(pageId);

                    if (page != null) {
                        // We are in the segment write lock, so it is safe to remove the page if use counter is
                        // equal to 0.
                        if (page.flushCheckpoint(log))
                            seg.acquiredPages.remove(pageId);
                    }
                    // else page was not modified since the checkpoint started.
                    else {
                        checkpointPages.remove(pageId);

                        long relPtr = seg.loadedPages.get(pageId, INVALID_REL_PTR);

                        assert relPtr != INVALID_REL_PTR;

                        setDirty(pageId, absolute(relPtr), false);
                    }
                }
            }
            finally {
                seg.writeLock().unlock();
            }
        }

        checkpointPages = null;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer getForCheckpoint(long pageId) {
        Segment seg = segment(pageId);

        seg.readLock().lock();

        try {
            long relPtr = seg.loadedPages.get(pageId, INVALID_REL_PTR);

            assert relPtr != INVALID_REL_PTR : "Failed to get page checkpoint data (page has been evicted) " +
                "[pageId=" + U.hexLong(pageId) + ']';

            return wrapPointer(absolute(relPtr) + PAGE_OVERHEAD, pageSize());
        }
        finally {
            seg.readLock().unlock();
        }
    }

    /**
     * @param ptr Pointer to wrap.
     * @param len Memory location length.
     * @return Wrapped buffer.
     */
    ByteBuffer wrapPointer(long ptr, int len) {
        ByteBuffer buf = nioAccess.newDirectByteBuffer(ptr, len, null);

        buf.order(ByteOrder.nativeOrder());

        return buf;
    }

    /**
     * @param pageId Page ID to check if it was added to the checkpoint list.
     * @return {@code True} if it was added to the checkpoint list.
     */
    boolean isInCheckpoint(long pageId) {
        Collection<Long> pages0 = checkpointPages;

        return pages0 != null && pages0.contains(pageId);
    }

    /**
     * @param pageId Page ID to clear.
     */
    void clearCheckpoint(long pageId) {
        assert checkpointPages != null;

        checkpointPages.remove(pageId);
    }

    /**
     * @param relativePtr Relative pointer.
     * @return Absolute pointer.
     */
    long absolute(long relativePtr) {
        int chunkIdx = (int)((relativePtr >> 40) & 0xFFFF);

        long pageIdx = relativePtr & ~CHUNK_INDEX_MASK;

        long offset = pageIdx * sysPageSize;

        Chunk chunk = chunks.get(chunkIdx);

        return chunk.pagesBase + offset;
    }

    /**
     * @param chunk Chunk index.
     * @param pageIdx Page index in the chunk.
     * @return Relative pointer.
     */
    long relative(int chunk, long pageIdx) {
        return pageIdx | ((long)chunk) << 40;
    }

    /**
     * Reads relative pointer from the page at the given absolute position.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @return Relative pointer written to the page.
     */
    long readRelative(long absPtr) {
        return mem.readLong(absPtr + RELATIVE_PTR_OFFSET) & RELATIVE_PTR_MASK;
    }

    /**
     * Writes relative pointer to the page at the given absolute position.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @param relPtr Relative pointer to write.
     */
    void writeRelative(long absPtr, long relPtr) {
        mem.writeLong(absPtr + RELATIVE_PTR_OFFSET, relPtr & RELATIVE_PTR_MASK);
    }

    /**
     * Reads page ID from the page at the given absolute position.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @return Page ID written to the page.
     */
    long readPageId(long absPtr) {
        return mem.readLong(absPtr + PAGE_ID_OFFSET);
    }

    /**
     * Writes page ID to the page at the given absolute position.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @param pageId Page ID to write.
     */
    void writePageId(long absPtr, long pageId) {
        mem.writeLong(absPtr + PAGE_ID_OFFSET, pageId);
    }

    /**
     * @param absPtr Absolute pointer.
     * @return {@code True} if page is dirty.
     */
    boolean isDirty(long absPtr) {
        long relPtrWithFlags = mem.readLong(absPtr + RELATIVE_PTR_OFFSET);

        return (relPtrWithFlags & DIRTY_FLAG) != 0;
    }

    /**
     * Gets the number of active pages across all segments. Used for test purposes only.
     *
     * @return Number of active pages.
     */
    public int activePagesCount() {
        int total = 0;

        for (Segment seg : segments)
            total += seg.acquiredPages.size();

        return total;
    }

    /**
     * This method must be called in synchronized context.
     *
     * @param absPtr Absolute pointer.
     * @param dirty {@code True} dirty flag.
     */
    void setDirty(long pageId, long absPtr, boolean dirty) {
        long relPtrWithFlags = mem.readLong(absPtr + RELATIVE_PTR_OFFSET);

        boolean wasDirty = (relPtrWithFlags & DIRTY_FLAG) != 0;

        if (dirty)
            relPtrWithFlags |= DIRTY_FLAG;
        else
            relPtrWithFlags &= ~DIRTY_FLAG;

        mem.writeLong(absPtr + RELATIVE_PTR_OFFSET, relPtrWithFlags);

        if (!wasDirty && dirty)
            dirtyPages.add(pageId);
    }

    /**
     * Attempts to restore page memory state based on the memory chunks returned by the allocator.
     */
    private void initExisting(DirectMemory memory) {
        DirectMemoryFragment meta = memory.fragments().get(0);

        long base = meta.address();

        int segs = mem.readInt(base);

        base +=4;

        int sysPageS = mem.readInt(base);

        base += 4;

        long memPerSegment = mem.readLong(base);

        base += 8;

        freePageListPtr = base;

        base += 8;

        lastAllocatedPageIdPtr = base;

        base += 8;

        dbMetaPageIdPtr = base;

        base += 8;

        if (sysPageSize != sysPageS || segs != segments.length) {
            U.quietAndWarn(log, "Saved memory state setting differ from configured settings " +
                "(configured settings will be ignored) " +
                "[configuredPageSize=" + sysPageSize + ", pageSize=" + sysPageS +
                ", configuredSegments=" + segments.length + ", segments=" + segs + ']');

            sysPageSize = sysPageS;
            segments = new Segment[segs];
        }

        log.info("Allocating segment tables at offset: " + U.hexLong(base - meta.address()));

        for (int i = 0; i < segments.length; i++) {
            segments[i] = new Segment(base, memPerSegment, false);

            base += memPerSegment;
        }

        synchronized (this) {
            for (int i = 0; i < memory.fragments().size(); i++) {
                DirectMemoryFragment fr = memory.fragments().get(i);

                long offset = i == 0 ? (base - fr.address()) : 0;

                Chunk chunk = new Chunk(i, fr, offset, false);

                chunks.add(chunk);

                if (i == 0)
                    currentChunk = chunk;
            }
        }
    }

    /**
     *
     */
    private void initNew(DirectMemory memory) throws IgniteCheckedException {
        long totalMemory = 0;

        for (DirectMemoryFragment fr : memory.fragments())
            totalMemory += fr.size();

        int pages = (int)(totalMemory / sysPageSize);

        long memPerSegment = requiredSegmentMemory(pages / segments.length);

        long metaSize = memPerSegment * segments.length + 40;

        DirectMemoryFragment meta = memory.fragments().get(0);

        if (meta.size() < metaSize)
            throw new IllegalStateException("Failed to initialize page memory (first memory fragment must " +
                "be at least " + metaSize + " bytes) [allocatedSize=" + meta.size() + ']');

        long base = meta.address();

        mem.writeInt(base, segments.length);

        base += 4;

        mem.writeInt(base, sysPageSize);

        base += 4;

        mem.writeLong(base, memPerSegment);

        base += 8;

        freePageListPtr = base;

        base += 8;

        lastAllocatedPageIdPtr = base;

        base += 8;

        dbMetaPageIdPtr = base;

        base += 8;

        log.info("Allocating segment tables at offset: " + U.hexLong(base - meta.address()));

        for (int i = 0; i < segments.length; i++) {
            segments[i] = new Segment(base, memPerSegment, true);

            base += memPerSegment;
        }

        assert (base - meta.address()) == metaSize : "Invalid offset [base=" + U.hexLong(base) +
            ", addr=" + U.hexLong(meta.address()) + ", metaSize=" + U.hexLong(metaSize) + ']';

        mem.writeLong(freePageListPtr, INVALID_REL_PTR);
        mem.writeLong(lastAllocatedPageIdPtr, 1);

        synchronized (this) {
            for (int i = 0; i < memory.fragments().size(); i++) {
                DirectMemoryFragment fr = memory.fragments().get(i);

                long offset = i == 0 ? (base - fr.address()) : 0;

                Chunk chunk = new Chunk(i, fr, offset, true);

                chunks.add(chunk);

                if (i == 0)
                    currentChunk = chunk;
            }
        }

        if (storeMgr == null) {
            mem.writeLong(dbMetaPageIdPtr, allocatePage(0, -1, FLAG_META));

            Page dbMetaPage = metaPage();

            try {
                ByteBuffer buf = dbMetaPage.getForWrite();

                boolean ok = false;

                try {
                    while (buf.remaining() >= 8)
                        buf.putLong(0);

                    ok = true;
                }
                finally {
                    dbMetaPage.releaseWrite(ok);
                }
            }
            finally {
                releasePage(dbMetaPage);
            }
        }
        else
            mem.writeLong(dbMetaPageIdPtr, storeMgr.metaRoot());
    }

    /**
     * Requests next memory chunk from the system allocator.
     */
    private void requestNextChunk() {
        assert Thread.holdsLock(this);

        int curIdx = currentChunk.idx;

        // If current chunk is the last one, fail.
        if (curIdx == chunks.size() - 1)
            throw new OutOfMemoryException();

        Chunk chunk = chunks.get(curIdx + 1);

        if (log.isInfoEnabled())
            log.info("Switched to the next page memory chunk [idx=" + chunk.idx +
                ", base=0x" + U.hexLong(chunk.fr.address()) + ", len=" + chunk.size() + ']');

        currentChunk = chunk;
    }

    /**
     * @param pageId Page ID to get segment for.
     * @return Segment.
     */
    private Segment segment(long pageId) {
        int idx = segmentIndex(pageId);

        return segments[idx];
    }

    /**
     * @param pageId Page ID.
     * @return Segment index.
     */
    private int segmentIndex(long pageId) {
        return U.safeAbs(U.hash(pageId)) % segments.length;
    }

    /**
     * Allocates a new free page.
     *
     * @return Relative pointer to the allocated page.
     * @throws GridOffHeapOutOfMemoryException
     */
    private long borrowOrAllocateFreePage() throws GridOffHeapOutOfMemoryException {
        long relPtr = borrowFreePage();

        return relPtr != INVALID_REL_PTR ? relPtr : allocateFreePage();
    }

    /**
     * @return Relative pointer to a free page that was borrowed from the allocated pool.
     */
    private long borrowFreePage() {
        while (true) {
            long freePageRelPtrMasked = mem.readLong(freePageListPtr);

            long freePageRelPtr = freePageRelPtrMasked & ADDRESS_MASK;
            long cnt = ((freePageRelPtrMasked & COUNTER_MASK) + COUNTER_INC) & COUNTER_MASK;

            if (freePageRelPtr != INVALID_REL_PTR) {
                long nextFreePageRelPtr = mem.readLong(absolute(freePageRelPtr)) & ADDRESS_MASK;

                if (mem.compareAndSwapLong(freePageListPtr, freePageRelPtrMasked, nextFreePageRelPtr | cnt))
                    return freePageRelPtr;

            }
            else
                return INVALID_REL_PTR;
        }
    }

    /**
     * Allocates a page from the next memory chunk.
     *
     * @return Relative pointer to the allocated page.
     */
    private long allocateFreePage() {
        while (true) {
            Chunk chunk = currentChunk;

            long relPtr = chunk.allocateFreePage();

            if (relPtr == INVALID_REL_PTR) {
                synchronized (this) {
                    Chunk full = currentChunk;

                    if (chunk == full)
                        requestNextChunk();
                }
            }
            else
                return relPtr;
        }
    }

    /**
     * @param relPtr Relative pointer to free.
     */
    private void releaseFreePage(long relPtr) {
        while (true) {
            long freePageRelPtrMasked = mem.readLong(freePageListPtr);

            long freePageRelPtr = freePageRelPtrMasked & RELATIVE_PTR_MASK;

            mem.writeLong(absolute(relPtr), freePageRelPtr);

            if (mem.compareAndSwapLong(freePageListPtr, freePageRelPtrMasked, relPtr))
                return;
        }
    }

    /**
     *
     */
    private class Segment extends ReentrantReadWriteLock {
        /** Page ID to relative pointer map. */
        private final LongLongHashMap loadedPages;

        /** */
        private final Map<Long, PageImpl> acquiredPages;

        /**
         * @param ptr Pointer for the page IDs map.
         * @param len Length of the allocated memory.
         */
        private Segment(long ptr, long len, boolean clear) {
            loadedPages = new LongLongHashMap(mem, ptr, len, clear);

            acquiredPages = new HashMap<>(16, 0.75f);
        }
    }

    /**
     * Gets an estimate for the amount of memory required to store the given number of page IDs
     * in a segment table.
     *
     * @param pages Number of pages to store.
     * @return Memory size estimate.
     */
    private static long requiredSegmentMemory(int pages) {
        return LongLongHashMap.requiredMemory(pages) + 8;
    }

    /**
     * Convenience memory chunk wrapper.
     */
    private class Chunk {
        /** Chunk index. */
        private int idx;

        /** Direct memory chunk. */
        private DirectMemoryFragment fr;

        /** Base address for all pages. */
        private long pagesBase;

        /** Last allocated page offset relative to the memory base. */
        private long lastAllocatedIdx;

        /**
         * @param idx Chunk index.
         * @param fr Memory to work with.
         */
        private Chunk(int idx, DirectMemoryFragment fr, long offset, boolean clear) {
            this.idx = idx;
            this.fr = fr;

            long base = fr.address() + offset;

            if (clear)
                mem.writeInt(base, idx);
            else {
                int idx0 = mem.readInt(base);

                if (idx0 != idx)
                    throw new IllegalStateException("Failed to restore page memory chunk state " +
                        "(relative index mismatch) [idx=" + idx + ", readIdx=" + idx0 + ']');
            }

            lastAllocatedIdx = base + 4;

            long start = base + 12;

            if (clear)
                mem.writeLong(lastAllocatedIdx, 0);

            pagesBase = start;
        }

        /**
         * @return Relative pointer of the allocated page.
         * @throws GridOffHeapOutOfMemoryException
         */
        private long allocateFreePage() throws GridOffHeapOutOfMemoryException {
            long limit = fr.address() + fr.size();

            while (true) {
                long lastIdx = mem.readLong(lastAllocatedIdx);

                // Check if we have enough space to allocate a page.
                if (pagesBase + (lastIdx + 1) * sysPageSize > limit)
                    return INVALID_REL_PTR;

                if (mem.compareAndSwapLong(lastAllocatedIdx, lastIdx, lastIdx + 1)) {
                    long absPtr = pagesBase + lastIdx * sysPageSize;

                    assert (absPtr & CHUNK_INDEX_MASK) == 0L;

                    long relative = relative(idx, lastIdx);

                    assert relative != INVALID_REL_PTR;

                    writeRelative(absPtr, relative);

                    return relative;
                }
            }
        }

        /**
         * @return Chunk size.
         */
        private long size() {
            return fr.address() - pagesBase + fr.size();
        }
    }
}
