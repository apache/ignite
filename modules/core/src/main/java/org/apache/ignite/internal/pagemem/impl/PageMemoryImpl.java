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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.mem.DirectMemory;
import org.apache.ignite.internal.mem.DirectMemoryFragment;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.OutOfMemoryException;
import org.apache.ignite.internal.pagemem.DirectMemoryUtils;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.StorageException;
import org.apache.ignite.internal.pagemem.wal.record.PageWrapperRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.offheap.GridOffHeapOutOfMemoryException;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.jsr166.ConcurrentHashMap8;
import sun.misc.JavaNioAccess;
import sun.misc.SharedSecrets;
import sun.nio.ch.DirectBuffer;

/**
 * Page header structure is described by the following diagram.
 *
 * When page is not allocated (in a free list):
 * <pre>
 * +--------+------------------------------------------------------+
 * |8 bytes |         PAGE_SIZE + PAGE_OVERHEAD - 8 bytes          |
 * +--------+------------------------------------------------------+
 * |Next ptr|                      Page data                       |
 * +--------+------------------------------------------------------+
 * </pre>
 * <p/>
 * When page is allocated and is in use:
 * <pre>
 * +--------+--------+--------+----+--------+----------------------+
 * |8 bytes |8 bytes |8 bytes |4 b |8 bytes |       PAGE_SIZE      |
 * +--------+--------+--------+----+--------+----------------------+
 * | Marker |Rel ptr |Page ID |C ID| Tstamp |       Page data      |
 * +--------+--------+--------+----+--------+----------------------+
 * </pre>
 *
 * Note that first 8 bytes of page header are used either for page marker or for next relative pointer depending
 * on whether the page is in use or not.
 */
@SuppressWarnings({"LockAcquiredButNotSafelyReleased", "FieldAccessedSynchronizedAndUnsynchronized"})
public class PageMemoryImpl implements PageMemory {
    /** */
    public static final long PAGE_MARKER = 0x0000000000000001L;

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

    /** Page cache ID offset. */
    public static final int PAGE_CACHE_ID_OFFSET = 24;

    /** Page access timestamp */
    public static final int PAGE_TIMESTAMP_OFFSET = 28;

    /**
     * Need a 8-byte pointer for linked list, 8 bytes for internal needs (flags),
     * 4 bytes cache ID, 8 bytes timestamp.
     */
    public static final int PAGE_OVERHEAD = 36;

    /** Number of random pages that will be picked for eviction. */
    public static final int RANDOM_PAGES_EVICT_NUM = 5;

    /** Page size. */
    private int sysPageSize;

    /** Page store manager. */
    private IgnitePageStoreManager storeMgr;

    /** */
    private IgniteWriteAheadLogManager walMgr;

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
    private Collection<FullPageId> dirtyPages = new GridConcurrentHashSet<>();

    /** Pages captured for the checkpoint process. */
    private Collection<FullPageId> checkpointPages;

    /**
     * @param directMemoryProvider Memory allocator to use.
     * @param sharedCtx Cache shared context.
     * @param pageSize Page size.
     * @param segments Number of segments.
     */
    public PageMemoryImpl(
        IgniteLogger log,
        DirectMemoryProvider directMemoryProvider,
        GridCacheSharedContext<?, ?> sharedCtx,
        int pageSize,
        int segments
    ) {
        assert log != null || sharedCtx != null;

        if (segments == 0)
            segments = Runtime.getRuntime().availableProcessors() * 8;

        this.log = sharedCtx != null ? sharedCtx.logger(PageMemoryImpl.class) : log;
        this.segments = new Segment[segments];
        this.directMemoryProvider = directMemoryProvider;

        if (sharedCtx != null) {
            storeMgr = sharedCtx.pageStore();
            walMgr = sharedCtx.wal();
        }

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
        if (log.isDebugEnabled())
            log.debug("Stopping page memory.");

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
    @Override public FullPageId allocatePage(int cacheId, int partId, byte flags) throws IgniteCheckedException {
        if (storeMgr != null)
            return storeMgr.allocatePage(cacheId, partId, flags);

        long pageId, absPtr;

        long relPtr = borrowFreePage();

        if (relPtr != INVALID_REL_PTR) {
            absPtr = absolute(relPtr);

            pageId = readPageId(absPtr);

            long idx = PageIdUtils.pageIndex(pageId);

            // Reassign page ID according to flags and partition ID.
            pageId = PageIdUtils.pageId(partId, flags, idx);
        }
        else {
            while (true) {
                pageId = mem.readLong(lastAllocatedPageIdPtr);

                if (mem.compareAndSwapLong(lastAllocatedPageIdPtr, pageId, pageId + 1))
                    break;
            }

            // Assign page ID according to flags and partition ID.
            pageId = PageIdUtils.pageId(partId, flags, pageId);

            relPtr = allocateFreePage();

            if (relPtr == INVALID_REL_PTR)
                throw new OutOfMemoryException();

            absPtr = absolute(relPtr);
        }

        writePageId(absPtr, pageId);
        writePageCacheId(absPtr, cacheId);
        writeCurrentTimestamp(absPtr);

        // TODO pass an argument to decide whether the page should be cleaned.
        mem.setMemory(absPtr + PAGE_OVERHEAD, sysPageSize - PAGE_OVERHEAD, (byte)0);

        FullPageId fullId = new FullPageId(pageId, cacheId);

        Segment seg = segment(fullId);

        seg.writeLock().lock();

        try {
            seg.loadedPages.put(fullId, relPtr);
        }
        finally {
            seg.writeLock().unlock();
        }

        return fullId;
    }

    /** {@inheritDoc} */
    @Override public boolean freePage(FullPageId fullId) throws IgniteCheckedException {
        Segment seg = segment(fullId);

        seg.writeLock().lock();

        try {
            if (seg.acquiredPages.get(fullId) != null)
                return false;

            long relPtr = seg.loadedPages.get(fullId, INVALID_REL_PTR);

            if (relPtr == INVALID_REL_PTR)
                return false;

            if (isDirty(absolute(relPtr)))
                return false;

            seg.loadedPages.remove(fullId);

            releaseFreePage(relPtr);
        }
        finally {
            seg.writeLock().unlock();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public Page metaPage() throws IgniteCheckedException {
        return page(new FullPageId(mem.readLong(dbMetaPageIdPtr), 0));
    }

    /** {@inheritDoc} */
    @Override public Page page(FullPageId fullId) throws IgniteCheckedException {
        Segment seg = segment(fullId);

        seg.readLock().lock();

        try {
            long relPtr = seg.loadedPages.get(fullId, INVALID_REL_PTR);

            // The page is loaded to the memory.
            if (relPtr != INVALID_REL_PTR) {
                while (true) {
                    PageImpl page = seg.acquiredPages.get(fullId);

                    if (page != null) {
                        if (page.acquireReference())
                            return page;
                        else
                            seg.acquiredPages.remove(fullId, page);
                    }

                    page = new PageImpl(fullId, absolute(relPtr), this);
                    page.acquireReference();

                    PageImpl old = seg.acquiredPages.putIfAbsent(fullId, page);

                    if (old != null) {
                        if (old.acquireReference())
                            return old;
                        else
                            seg.acquiredPages.remove(fullId, old);
                    }
                    else
                        return page;
                }
            }
        }
        finally {
            seg.readLock().unlock();
        }

        seg.writeLock().lock();

        try {
            // Double-check.
            PageImpl page = seg.acquiredPages.get(fullId);

            if (page != null) {
                if (page.acquireReference())
                    return page;
                else
                    seg.acquiredPages.remove(fullId, page);
            }

            long relPtr = seg.loadedPages.get(fullId, INVALID_REL_PTR);

            if (relPtr == INVALID_REL_PTR) {
                if (storeMgr == null)
                    throw new IllegalStateException("The page with the given page ID was not allocated: " +
                        fullId);

                relPtr = borrowOrAllocateFreePage();

                if (relPtr == INVALID_REL_PTR)
                    relPtr = evictPage(seg);

                long absPtr = absolute(relPtr);

                writeFullPageId(absPtr, fullId);
                writeCurrentTimestamp(absPtr);

                // We can clear dirty flag after the page has been allocated.
                setDirty(fullId, absPtr, false, false);

                seg.loadedPages.put(fullId, relPtr);

                page = new PageImpl(fullId, absPtr, this);

                if (storeMgr != null)
                    storeMgr.read(fullId.cacheId(), fullId.pageId(), wrapPointer(absPtr + PAGE_OVERHEAD, pageSize()));
            }
            else
                page = new PageImpl(fullId, absolute(relPtr), this);

            page.acquireReference();

            Page old = seg.acquiredPages.put(fullId, page);

            assert old == null;

            return page;
        }
        finally {
            seg.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void releasePage(Page p) {
        PageImpl page = (PageImpl)p;

        Segment seg = segment(page.fullId());

        seg.readLock().lock();

        try {
            if (page.releaseReference())
                seg.acquiredPages.remove(page.fullId(), page);
        }
        finally {
            seg.readLock().unlock();
        }
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
    @Override public Collection<FullPageId> beginCheckpoint() throws IgniteException {
        if (checkpointPages != null)
            throw new IgniteException("Failed to begin checkpoint (it is already in progress).");

        checkpointPages = dirtyPages;

        dirtyPages = new GridConcurrentHashSet<>();

        return checkpointPages;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "TooBroadScope"})
    @Override public void finishCheckpoint() {
        assert checkpointPages != null : "Checkpoint has not been started.";

        // Split page IDs by segment.
        Collection<FullPageId>[] segCols = new Collection[segments.length];

        for (FullPageId pageId : checkpointPages) {
            int segIdx = segmentIndex(pageId);

            Collection<FullPageId> col = segCols[segIdx];

            if (col == null) {
                col = new ArrayList<>(checkpointPages.size() / segments.length + 1);

                segCols[segIdx] = col;
            }

            col.add(pageId);
        }

        // Lock segment by segment and flush changes.
        for (int i = 0; i < segCols.length; i++) {
            Collection<FullPageId> col = segCols[i];

            if (col == null)
                continue;

            Collection<PageImpl> activePages = null;

            Segment seg = segments[i];

            seg.writeLock().lock();

            try {
                if (!seg.acquiredPages.isEmpty())
                    activePages = new ArrayList<>(seg.acquiredPages.size());

                for (FullPageId pageId : col) {
                    PageImpl page = seg.acquiredPages.get(pageId);

                    if (page != null) {
                        // We are in the segment write lock, so we can assert that the reference is acquired.
                        boolean acquired = page.acquireReference();

                        assert acquired : "Failed to acquire reference for dirty checkpoint page: " + page;

                        assert activePages != null;

                        activePages.add(page);
                    }
                    // else page was not modified since the checkpoint started.
                    else {
                        clearCheckpoint(pageId);

                        long relPtr = seg.loadedPages.get(pageId, INVALID_REL_PTR);

                        assert relPtr != INVALID_REL_PTR;

                        setDirty(pageId, absolute(relPtr), false, true);
                    }
                }
            }
            finally {
                seg.writeLock().unlock();
            }

            // Must release active pages outside of segment write lock.
            if (activePages != null) {
                for (PageImpl page : activePages) {
                    boolean releasedLast = page.flushCheckpoint(log);

                    assert !releasedLast : "Released page that was modified during checkpoint: " + page;

                    releasePage(page);
                }
            }
        }

        checkpointPages = null;
    }

    /** {@inheritDoc} */
    @Override public boolean getForCheckpoint(FullPageId pageId, ByteBuffer tmpBuf) {
        assert tmpBuf.remaining() == pageSize();

        Segment seg = segment(pageId);

        seg.readLock().lock();

        try {
            long relPtr = seg.loadedPages.get(pageId, INVALID_REL_PTR);

            assert relPtr != INVALID_REL_PTR : "Failed to get page checkpoint data (page has been evicted) " +
                "[pageId=" + pageId + ']';

            long absPtr = absolute(relPtr);

            if (tmpBuf.isDirect()) {
                long tmpPtr = ((DirectBuffer)tmpBuf).address();

                GridUnsafe.copyMemory(absPtr + PAGE_OVERHEAD, tmpPtr, pageSize());
            }
            else {
                byte[] arr = tmpBuf.array();

                assert arr != null;
                assert arr.length == pageSize();

                GridUnsafe.copyMemory(null, absPtr + PAGE_OVERHEAD, arr, GridUnsafe.BYTE_ARR_OFF, pageSize());
            }
        }
        finally {
            seg.readLock().unlock();
        }

        return true;
    }

    /**
     * @return Total number of loaded pages in memory.
     */
    public long loadedPages() {
        long total = 0;

        for (Segment seg : segments) {
            seg.readLock().lock();

            try {
                total += seg.loadedPages.size();
            }
            finally {
                seg.readLock().unlock();
            }
        }

        return total;
    }

    /**
     * @return Total number of acquired pages.
     */
    public long acquiredPages() {
        long total = 0;

        for (Segment seg : segments) {
            seg.readLock().lock();

            try {
                total += seg.acquiredPages.size();
            }
            finally {
                seg.readLock().unlock();
            }
        }

        return total;
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
    boolean isInCheckpoint(FullPageId pageId) {
        Collection<FullPageId> pages0 = checkpointPages;

        return pages0 != null && pages0.contains(pageId);
    }

    /**
     * @param fullPageId Page ID to clear.
     */
    void clearCheckpoint(FullPageId fullPageId) {
        assert checkpointPages != null;

        checkpointPages.remove(fullPageId);
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
     * Reads cache ID from the page at the given absolute pointer.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @return Cache ID written to the page.
     */
    int readPageCacheId(final long absPtr) {
        return mem.readInt(absPtr + PAGE_CACHE_ID_OFFSET);
    }

    /**
     * Writes cache ID from the page at the given absolute pointer.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @param cacheId Cache ID to write.
     */
    void writePageCacheId(final long absPtr, final int cacheId) {
        mem.writeInt(absPtr + PAGE_CACHE_ID_OFFSET, cacheId);
    }

    /**
     * Reads page ID and cache ID from the page at the given absolute pointer.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @return Full page ID written to the page.
     */
    FullPageId readFullPageId(final long absPtr) {
        return new FullPageId(readPageId(absPtr), readPageCacheId(absPtr));
    }

    /**
     * Writes page ID and cache ID from the page at the given absolute pointer.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @param fullPageId Full page ID to write.
     */
    void writeFullPageId(final long absPtr, final FullPageId fullPageId) {
        writePageId(absPtr, fullPageId.pageId());
        writePageCacheId(absPtr, fullPageId.cacheId());
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
     * @param forceAdd If this flag is {@code true}, then the page will be added to the dirty set regardless whether
     *      the old flag was dirty or not.
     */
    void setDirty(FullPageId pageId, long absPtr, boolean dirty, boolean forceAdd) {
        long relPtrWithFlags = mem.readLong(absPtr + RELATIVE_PTR_OFFSET);

        boolean wasDirty = (relPtrWithFlags & DIRTY_FLAG) != 0;

        if (dirty)
            relPtrWithFlags |= DIRTY_FLAG;
        else
            relPtrWithFlags &= ~DIRTY_FLAG;

        mem.writeLong(absPtr + RELATIVE_PTR_OFFSET, relPtrWithFlags);

        if (dirty && (!wasDirty || forceAdd))
            dirtyPages.add(pageId);
        else if (!dirty && wasDirty)
            dirtyPages.remove(pageId);
    }

    /**
     *
     */
    void beforeReleaseWrite(FullPageId pageId, ByteBuffer buf) {
        if (walMgr != null) {
            assert buf.remaining() == sysPageSize : "remaining=" + buf.remaining() + ", sysPageSize=" + sysPageSize;

            try {
                walMgr.log(new PageWrapperRecord(pageId, buf));
            }
            catch (IgniteCheckedException | StorageException e) {
                // TODO ignite-db.
                throw new IgniteException(e);
            }
        }
    }

    /**
     * Volatile write for current timestamp to page in {@code absAddr} address.
     *
     * @param absPtr Absolute page address.
     */
    void writeCurrentTimestamp(final long absPtr) {
        mem.writeLongVolatile(absPtr + PAGE_TIMESTAMP_OFFSET, U.currentTimeMillis());
    }

    /**
     * Read for timestamp from page in {@code absAddr} address.
     *
     * @param absPtr Absolute page address.
     * @return Timestamp.
     */
    long readTimestamp(final long absPtr) {
        return mem.readLong(absPtr + PAGE_TIMESTAMP_OFFSET);
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
            mem.writeLong(dbMetaPageIdPtr, allocatePage(0, -1, FLAG_META).pageId());

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
    private boolean requestNextChunk() {
        assert Thread.holdsLock(this);

        int curIdx = currentChunk.idx;

        // If current chunk is the last one, fail.
        if (curIdx == chunks.size() - 1)
            return false;

        Chunk chunk = chunks.get(curIdx + 1);

        if (log.isInfoEnabled())
            log.info("Switched to the next page memory chunk [idx=" + chunk.idx +
                ", base=0x" + U.hexLong(chunk.fr.address()) + ", len=" + chunk.size() + ']');

        currentChunk = chunk;

        return true;
    }

    /**
     * @param fullId Page ID to get segment for.
     * @return Segment.
     */
    private Segment segment(FullPageId fullId) {
        int idx = segmentIndex(fullId);

        return segments[idx];
    }

    /**
     * @param pageId Page ID.
     * @return Segment index.
     */
    private int segmentIndex(FullPageId pageId) {
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
                long freePageAbsPtr = absolute(freePageRelPtr);

                long nextFreePageRelPtr = mem.readLong(freePageAbsPtr) & ADDRESS_MASK;

                if (mem.compareAndSwapLong(freePageListPtr, freePageRelPtrMasked, nextFreePageRelPtr | cnt)) {
                    GridUnsafe.putLong(freePageAbsPtr, PAGE_MARKER);

                    return freePageRelPtr;
                }
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

                    if (chunk == full && !requestNextChunk())
                        return INVALID_REL_PTR;
                }
            }
            else
                return relPtr;
        }
    }

    /**
     * Evict random oldest page from memory to storage.
     *
     * @param seg Currently locked segment.
     * @return Relative addres for evicted page.
     * @throws IgniteCheckedException
     */
    private long evictPage(final Segment seg) throws IgniteCheckedException {
        final ThreadLocalRandom rnd = ThreadLocalRandom.current();

        final int cap = seg.loadedPages.capacity();

        if (seg.acquiredPages.size() >= seg.loadedPages.size())
            throw new OutOfMemoryException("No not acquired pages left for segment. Unable to evict.");

        // With big number of random picked pages we may fall into infinite loop, because
        // every time the same page may be found.
        Set<Long> ignored = null;

        long relEvictAddr = INVALID_REL_PTR;

        int iterations = 0;

        while (true) {
            long cleanAddr = INVALID_REL_PTR;
            long cleanTs = Long.MAX_VALUE;
            long dirtyTs = Long.MAX_VALUE;
            long dirtyAddr = INVALID_REL_PTR;

            for (int i = 0; i < RANDOM_PAGES_EVICT_NUM; i++) {
                // We need to lookup for pages only in current segment for thread safety,
                // so peeking random memory will lead to checking for found page segment.
                // It's much faster to check available pages for segment right away.
                final long rndAddr = seg.loadedPages.getNearestAt(rnd.nextInt(cap), INVALID_REL_PTR);

                assert rndAddr != INVALID_REL_PTR;

                if (relEvictAddr == rndAddr || (ignored != null && ignored.contains(rndAddr))) {
                    i--;

                    continue;
                }

                final long absPageAddr = absolute(rndAddr);

                final long pageTs = readTimestamp(absPageAddr);

                final boolean dirty = isDirty(absPageAddr);

                if (pageTs < cleanTs && !dirty) {
                    cleanAddr = rndAddr;

                    cleanTs = pageTs;
                } else if (pageTs < dirtyTs && dirty) {
                    dirtyAddr = rndAddr;

                    dirtyTs = pageTs;
                }

                relEvictAddr = cleanAddr == INVALID_REL_PTR ? dirtyAddr : cleanAddr;
            }

            assert relEvictAddr != INVALID_REL_PTR;

            final long absEvictAddr = absolute(relEvictAddr);

            final FullPageId fullPageId = readFullPageId(absEvictAddr);

            if (!prepareEvict(seg, fullPageId, absEvictAddr)) {
                if (++iterations > 2) {
                    if (ignored == null)
                        ignored = new HashSet<>();

                    ignored.add(relEvictAddr);
                }

                continue;
            }

            seg.loadedPages.remove(fullPageId);

            return relEvictAddr;
        }
    }

    /**
     * Prepares a page for eviction, if needed.
     *
     * @param seg Segment being operated on.
     * @param fullPageId Candidate page full ID.
     * @param absPtr Absolute pointer of the page to evict.
     * @return {@code True} if it is ok to evict this page, {@code false} if another page should be selected.
     * @throws IgniteCheckedException If failed to write page to the underlying store during eviction.
     */
    private boolean prepareEvict(Segment seg, FullPageId fullPageId, long absPtr) throws IgniteCheckedException {
        assert seg.writeLock().isHeldByCurrentThread();

        final long metaPageId = mem.readLong(dbMetaPageIdPtr);

        if (fullPageId.pageId() == metaPageId && fullPageId.cacheId() == 0)
            return false;

        if (seg.acquiredPages.containsKey(fullPageId))
            return false;

        Collection<FullPageId> cpPages = checkpointPages;

        if (isDirty(absPtr)) {
            // Can evict a dirty page only if should be written by a checkpoint.
            if (cpPages != null && cpPages.contains(fullPageId)) {
                assert storeMgr != null;

                storeMgr.write(fullPageId.cacheId(), fullPageId.pageId(),
                    wrapPointer(absPtr + PAGE_OVERHEAD, pageSize()));

                setDirty(fullPageId, absPtr, false, true);

                cpPages.remove(fullPageId);

                return true;
            }

            return false;
        }
        else
            // Page was not modified, ok to evict.
            return true;
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
        /** */
        private static final long serialVersionUID = 0L;

        /** Page ID to relative pointer map. */
        private final FullPageIdTable loadedPages;

        /** */
        private final ConcurrentMap<FullPageId, PageImpl> acquiredPages;

        /**
         * @param ptr Pointer for the page IDs map.
         * @param len Length of the allocated memory.
         */
        private Segment(long ptr, long len, boolean clear) {
            loadedPages = new FullPageIdTable(mem, ptr, len, clear,
                storeMgr == null // if null evictions won't be used
                    ? FullPageIdTable.AddressingStrategy.QUADRATIC
                    : FullPageIdTable.AddressingStrategy.LINEAR);

            acquiredPages = new ConcurrentHashMap8<>(16, 0.75f);
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
        return FullPageIdTable.requiredMemory(pages) + 8;
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

                    assert (lastIdx & CHUNK_INDEX_MASK) == 0L;

                    long relative = relative(idx, lastIdx);

                    assert relative != INVALID_REL_PTR;

                    writeRelative(absPtr, relative);

                    GridUnsafe.putLong(absPtr, PAGE_MARKER);

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

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Chunk.class, this, "size", size());
        }
    }
}
