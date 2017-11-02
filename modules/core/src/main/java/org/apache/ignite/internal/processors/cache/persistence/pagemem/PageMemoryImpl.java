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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.InitNewPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PageDeltaRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointLockStateChecker;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.TrackingPageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.internal.util.lang.GridInClosure3X;
import org.apache.ignite.internal.util.lang.GridPredicate3;
import org.apache.ignite.internal.util.offheap.GridOffHeapOutOfMemoryException;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;
import sun.misc.JavaNioAccess;
import sun.misc.SharedSecrets;
import sun.nio.ch.DirectBuffer;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

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
 * +------------------+--------+--------+----+----+--------+--------+----------------------+
 * |     8 bytes      |8 bytes |8 bytes |4 b |4 b |8 bytes |8 bytes |       PAGE_SIZE      |
 * +------------------+--------+--------+----+----+--------+--------+----------------------+
 * | Marker/Timestamp |Rel ptr |Page ID |C ID|PIN | LOCK   |TMP BUF |       Page data      |
 * +------------------+--------+--------+----+----+--------+--------+----------------------+
 * </pre>
 *
 * Note that first 8 bytes of page header are used either for page marker or for next relative pointer depending
 * on whether the page is in use or not.
 */
@SuppressWarnings({"LockAcquiredButNotSafelyReleased", "FieldAccessedSynchronizedAndUnsynchronized"})
public class PageMemoryImpl implements PageMemoryEx {
    /** */
    public static final long PAGE_MARKER = 0x0000000000000001L;

    /** Relative pointer chunk index mask. */
    private static final long SEGMENT_INDEX_MASK = 0xFFFFFF0000000000L;

    /** Full relative pointer mask. */
    private static final long RELATIVE_PTR_MASK = 0xFFFFFFFFFFFFFFL;

    /** Dirty flag. */
    private static final long DIRTY_FLAG = 0x0100000000000000L;

    /** Invalid relative pointer value. */
    private static final long INVALID_REL_PTR = RELATIVE_PTR_MASK;

    /** */
    private static final long OUTDATED_REL_PTR = INVALID_REL_PTR + 1;

    /** Address mask to avoid ABA problem. */
    private static final long ADDRESS_MASK = 0xFFFFFFFFFFFFFFL;

    /** Counter mask to avoid ABA problem. */
    private static final long COUNTER_MASK = ~ADDRESS_MASK;

    /** Counter increment to avoid ABA problem. */
    private static final long COUNTER_INC = ADDRESS_MASK + 1;

    /** Page relative pointer. Does not change once a page is allocated. */
    public static final int RELATIVE_PTR_OFFSET = 8;

    /** Page ID offset */
    public static final int PAGE_ID_OFFSET = 16;

    /** Page cache ID offset. */
    public static final int PAGE_CACHE_ID_OFFSET = 24;

    /** Page pin counter offset. */
    public static final int PAGE_PIN_CNT_OFFSET = 28;

    /** Page lock offset. */
    public static final int PAGE_LOCK_OFFSET = 32;

    /** Page lock offset. */
    public static final int PAGE_TMP_BUF_OFFSET = 40;

    /**
     * 8b Marker/timestamp
     * 8b Relative pointer
     * 8b Page ID
     * 4b Cache ID
     * 4b Pin count
     * 8b Lock
     * 8b Temporary buffer
     */
    public static final int PAGE_OVERHEAD = 48;

    /** Number of random pages that will be picked for eviction. */
    public static final int RANDOM_PAGES_EVICT_NUM = 5;

    /** Tracking io. */
    private static final TrackingPageIO trackingIO = TrackingPageIO.VERSIONS.latest();

    /** Page size. */
    private final int sysPageSize;

    /** Shared context. */
    private final GridCacheSharedContext<?, ?> sharedCtx;

    /** State checker. */
    private final CheckpointLockStateChecker stateChecker;

    /** Number of used pages in checkpoint buffer. */
    private final AtomicInteger cpBufPagesCntr = new AtomicInteger(0);

    /** */
    private ExecutorService asyncRunner = new ThreadPoolExecutor(
        0,
        Runtime.getRuntime().availableProcessors(),
        30L,
        TimeUnit.SECONDS,
        new ArrayBlockingQueue<Runnable>(Runtime.getRuntime().availableProcessors()));

    /** Page store manager. */
    private IgnitePageStoreManager storeMgr;

    /** */
    private IgniteWriteAheadLogManager walMgr;

    /** Direct byte buffer factory. */
    private JavaNioAccess nioAccess;

    /** */
    private final IgniteLogger log;

    /** Direct memory allocator. */
    private final DirectMemoryProvider directMemoryProvider;

    /** Segments array. */
    private Segment[] segments;

    /** */
    private PagePool checkpointPool;

    /** */
    private OffheapReadWriteLock rwLock;

    /** Flush dirty page closure. When possible, will be called by evictPage(). */
    private final GridInClosure3X<FullPageId, ByteBuffer, Integer> flushDirtyPage;

    /**
     * Flush dirty page closure. When possible, will be called by evictPage().
     * {@code Null} if page tracking functionality is disabled
     * */
    @Nullable private final GridInClosure3X<Long, FullPageId, PageMemoryEx> changeTracker;

    /** Pages write throttle. */
    private PagesWriteThrottle writeThrottle;

    /** Write throttle enabled flag. */
    private boolean throttleEnabled;

    /**  */
    private boolean pageEvictWarned;

    /** */
    private long[] sizes;

    /** */
    private DataRegionMetricsImpl memMetrics;

    /**
     * @param directMemoryProvider Memory allocator to use.
     * @param sharedCtx Cache shared context.
     * @param pageSize Page size.
     * @param flushDirtyPage Callback invoked when a dirty page is evicted.
     * @param changeTracker Callback invoked to track changes in pages.
     * @param throttleEnabled Write throttle enabled flag.
     */
    public PageMemoryImpl(
        DirectMemoryProvider directMemoryProvider,
        long[] sizes,
        GridCacheSharedContext<?, ?> sharedCtx,
        int pageSize,
        GridInClosure3X<FullPageId, ByteBuffer, Integer> flushDirtyPage,
        @Nullable GridInClosure3X<Long, FullPageId, PageMemoryEx> changeTracker,
        CheckpointLockStateChecker stateChecker,
        DataRegionMetricsImpl memMetrics,
        boolean throttleEnabled
    ) {
        assert sharedCtx != null;

        log = sharedCtx.logger(PageMemoryImpl.class);

        this.sharedCtx = sharedCtx;
        this.directMemoryProvider = directMemoryProvider;
        this.sizes = sizes;
        this.flushDirtyPage = flushDirtyPage;
        this.changeTracker = changeTracker;
        this.stateChecker = stateChecker;
        this.throttleEnabled = throttleEnabled;

        storeMgr = sharedCtx.pageStore();
        walMgr = sharedCtx.wal();

        assert storeMgr != null;
        assert walMgr != null;

        sysPageSize = pageSize + PAGE_OVERHEAD;

        rwLock = new OffheapReadWriteLock(128);

        this.memMetrics = memMetrics;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        directMemoryProvider.initialize(sizes);

        List<DirectMemoryRegion> regions = new ArrayList<>(sizes.length);

        while (true) {
            DirectMemoryRegion reg = directMemoryProvider.nextRegion();

            if (reg == null)
                break;

            regions.add(reg);
        }

        nioAccess = SharedSecrets.getJavaNioAccess();

        int regs = regions.size();

        segments = new Segment[regs - 1];

        DirectMemoryRegion cpReg = regions.get(regs - 1);

        checkpointPool = new PagePool(regs - 1, cpReg, cpBufPagesCntr);

        long checkpointBuf = cpReg.size();

        long totalAllocated = 0;
        int pages = 0;
        long totalTblSize = 0;

        for (int i = 0; i < regs - 1; i++) {
            assert i < segments.length;

            DirectMemoryRegion reg = regions.get(i);

            totalAllocated += reg.size();

            segments[i] = new Segment(i, regions.get(i), checkpointPool.pages() / segments.length, throttleEnabled);

            pages += segments[i].pages();
            totalTblSize += segments[i].tableSize();
        }

        initWriteThrottle();

        if (log.isInfoEnabled())
            log.info("Started page memory [memoryAllocated=" + U.readableSize(totalAllocated, false) +
                ", pages=" + pages +
                ", tableSize=" + U.readableSize(totalTblSize, false) +
                ", checkpointBuffer=" + U.readableSize(checkpointBuf, false) +
                ']');
    }

    /**
     *
     */
    private void initWriteThrottle() {
        if (!(sharedCtx.database() instanceof GridCacheDatabaseSharedManager)) {
            log.error("Write throttle can't start. Unexpected class of database manager: " +
                sharedCtx.database().getClass());

            throttleEnabled = false;
        }

        if (throttleEnabled)
            writeThrottle = new PagesWriteThrottle(this, (GridCacheDatabaseSharedManager)sharedCtx.database());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("OverlyStrongTypeCast")
    @Override public void stop() throws IgniteException {
        if (log.isDebugEnabled())
            log.debug("Stopping page memory.");

        U.shutdownNow(getClass(), asyncRunner, log);

        if (segments != null) {
            for (Segment seg : segments)
                seg.close();
        }

        directMemoryProvider.shutdown();
    }

    /** {@inheritDoc} */
    @Override public void releasePage(int cacheId, long pageId, long page) {
        Segment seg = segment(cacheId, pageId);

        seg.readLock().lock();

        try {
            seg.releasePage(page);
        }
        finally {
            seg.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public long readLock(int cacheId, long pageId, long page) {
        return readLockPage(page, new FullPageId(pageId, cacheId), false);
    }

    /** {@inheritDoc} */
    @Override public void readUnlock(int cacheId, long pageId, long page) {
        readUnlockPage(page);
    }

    /** {@inheritDoc} */
    @Override public long writeLock(int cacheId, long pageId, long page) {
        return writeLock(cacheId, pageId, page, false);
    }

    /** {@inheritDoc} */
    @Override public long writeLock(int cacheId, long pageId, long page, boolean restore) {
        return writeLockPage(page, new FullPageId(pageId, cacheId), !restore);
    }

    /** {@inheritDoc} */
    @Override public long tryWriteLock(int cacheId, long pageId, long page) {
        return tryWriteLockPage(page, new FullPageId(pageId, cacheId), true);
    }

    /** {@inheritDoc} */
    @Override public void writeUnlock(int cacheId, long pageId, long page, Boolean walPlc,
        boolean dirtyFlag) {
        writeUnlock(cacheId, pageId, page, walPlc, dirtyFlag, false);
    }

    /** {@inheritDoc} */
    @Override public void writeUnlock(int cacheId, long pageId, long page, Boolean walPlc,
        boolean dirtyFlag, boolean restore) {
        writeUnlockPage(page, new FullPageId(pageId, cacheId), walPlc, dirtyFlag, restore);
    }

    /** {@inheritDoc} */
    @Override public boolean isDirty(int cacheId, long pageId, long page) {
        return isDirty(page);
    }

    /** {@inheritDoc} */
    @Override public long allocatePage(int cacheId, int partId, byte flags) throws IgniteCheckedException {
        assert flags == PageIdAllocator.FLAG_DATA && partId <= PageIdAllocator.MAX_PARTITION_ID ||
            flags == PageIdAllocator.FLAG_IDX && partId == PageIdAllocator.INDEX_PARTITION :
            "flags = " + flags + ", partId = " + partId;

        long pageId = storeMgr.allocatePage(cacheId, partId, flags);

        memMetrics.incrementTotalAllocatedPages();

        assert PageIdUtils.pageIndex(pageId) > 0; //it's crucial for tracking pages (zero page is super one)

        // We need to allocate page in memory for marking it dirty to save it in the next checkpoint.
        // Otherwise it is possible that on file will be empty page which will be saved at snapshot and read with error
        // because there is no crc inside them.
        Segment seg = segment(cacheId, pageId);

        FullPageId fullId = new FullPageId(pageId, cacheId);

        seg.writeLock().lock();

        boolean isTrackingPage = changeTracker != null && trackingIO.trackingPageFor(pageId, pageSize()) == pageId;

        try {
            long relPtr = seg.borrowOrAllocateFreePage(pageId);

            if (relPtr == INVALID_REL_PTR)
                relPtr = seg.evictPage();

            long absPtr = seg.absolute(relPtr);

            GridUnsafe.setMemory(absPtr + PAGE_OVERHEAD, pageSize(), (byte)0);

            PageHeader.fullPageId(absPtr, fullId);
            PageHeader.writeTimestamp(absPtr, U.currentTimeMillis());
            rwLock.init(absPtr + PAGE_LOCK_OFFSET, PageIdUtils.tag(pageId));

            assert GridUnsafe.getInt(absPtr + PAGE_OVERHEAD + 4) == 0; //TODO GG-11480

            assert !PageHeader.isAcquired(absPtr) :
                "Pin counter must be 0 for a new page [relPtr=" + U.hexLong(relPtr) +
                    ", absPtr=" + U.hexLong(absPtr) + ']';

            setDirty(fullId, absPtr, true, true);

            if (isTrackingPage) {
                long pageAddr = absPtr + PAGE_OVERHEAD;

                // We are inside segment write lock, so no other thread can pin this tracking page yet.
                // We can modify page buffer directly.
                if (PageIO.getType(pageAddr) == 0) {
                    trackingIO.initNewPage(pageAddr, pageId, pageSize());

                    if (!sharedCtx.wal().isAlwaysWriteFullPages())
                        sharedCtx.wal().log(
                            new InitNewPageRecord(
                                cacheId,
                                pageId,
                                trackingIO.getType(),
                                trackingIO.getVersion(), pageId
                            )
                        );
                    else
                        sharedCtx.wal().log(new PageSnapshot(fullId, absPtr + PAGE_OVERHEAD, pageSize()));
                }
            }

            seg.loadedPages.put(cacheId, PageIdUtils.effectivePageId(pageId), relPtr, seg.partTag(cacheId, partId));
        }
        finally {
            seg.writeLock().unlock();
        }

        //we have allocated 'tracking' page, we need to allocate regular one
        return isTrackingPage ? allocatePage(cacheId, partId, flags) : pageId;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer pageBuffer(long pageAddr) {
        return wrapPointer(pageAddr, pageSize());
    }

    /** {@inheritDoc} */
    @Override public boolean freePage(int cacheId, long pageId) throws IgniteCheckedException {
        assert false : "Free page should be never called directly when persistence is enabled.";

        return false;
    }

    /** {@inheritDoc} */
    @Override public long metaPageId(int cacheId) throws IgniteCheckedException {
        return storeMgr.metaPageId(cacheId);
    }

    /** {@inheritDoc} */
    @Override public long partitionMetaPageId(int cacheId, int partId) throws IgniteCheckedException {
        return PageIdUtils.pageId(partId, PageIdAllocator.FLAG_DATA, 0);
    }

    /** {@inheritDoc} */
    @Override public long acquirePage(int cacheId, long pageId) throws IgniteCheckedException {
        return acquirePage(cacheId, pageId, false);
    }

    /** {@inheritDoc} */
    @Override public long acquirePage(int cacheId, long pageId, boolean restore) throws IgniteCheckedException {
        FullPageId fullId = new FullPageId(pageId, cacheId);

        int partId = PageIdUtils.partId(pageId);

        Segment seg = segment(cacheId, pageId);

        seg.readLock().lock();

        try {
            long relPtr = seg.loadedPages.get(
                cacheId,
                PageIdUtils.effectivePageId(pageId),
                seg.partTag(cacheId, partId),
                INVALID_REL_PTR,
                INVALID_REL_PTR
            );

            // The page is loaded to the memory.
            if (relPtr != INVALID_REL_PTR) {
                long absPtr = seg.absolute(relPtr);

                seg.acquirePage(absPtr);

                return absPtr;
            }
        }
        finally {
            seg.readLock().unlock();
        }

        seg.writeLock().lock();

        try {
            // Double-check.
            long relPtr = seg.loadedPages.get(
                cacheId,
                PageIdUtils.effectivePageId(pageId),
                seg.partTag(cacheId, partId),
                INVALID_REL_PTR,
                OUTDATED_REL_PTR
            );

            long absPtr;

            if (relPtr == INVALID_REL_PTR) {
                relPtr = seg.borrowOrAllocateFreePage(pageId);

                if (relPtr == INVALID_REL_PTR)
                    relPtr = seg.evictPage();

                absPtr = seg.absolute(relPtr);

                PageHeader.fullPageId(absPtr, fullId);
                PageHeader.writeTimestamp(absPtr, U.currentTimeMillis());

                assert !PageHeader.isAcquired(absPtr) :
                    "Pin counter must be 0 for a new page [relPtr=" + U.hexLong(relPtr) +
                        ", absPtr=" + U.hexLong(absPtr) + ']';

                // We can clear dirty flag after the page has been allocated.
                setDirty(fullId, absPtr, false, false);

                seg.loadedPages.put(
                    cacheId,
                    PageIdUtils.effectivePageId(pageId),
                    relPtr,
                    seg.partTag(cacheId, partId)
                );

                long pageAddr = absPtr + PAGE_OVERHEAD;

                if (!restore) {
                    try {
                        ByteBuffer buf = wrapPointer(pageAddr, pageSize());

                        storeMgr.read(cacheId, pageId, buf);
                    }
                    catch (IgniteDataIntegrityViolationException ignore) {
                        U.warn(log, "Failed to read page (data integrity violation encountered, will try to " +
                            "restore using existing WAL) [fullPageId=" + fullId + ']');

                        tryToRestorePage(fullId, absPtr);

                        seg.acquirePage(absPtr);

                        return absPtr;
                    }
                }
                else {
                    GridUnsafe.setMemory(absPtr + PAGE_OVERHEAD, pageSize(), (byte)0);

                    // Must init page ID in order to ensure RWLock tag consistency.
                    PageIO.setPageId(pageAddr, pageId);
                }

                rwLock.init(absPtr + PAGE_LOCK_OFFSET, PageIdUtils.tag(pageId));
            }
            else if (relPtr == OUTDATED_REL_PTR) {
                assert PageIdUtils.pageIndex(pageId) == 0 : fullId;

                relPtr = refreshOutdatedPage(seg, cacheId, pageId, false);

                absPtr = seg.absolute(relPtr);

                long pageAddr = absPtr + PAGE_OVERHEAD;

                GridUnsafe.setMemory(absPtr + PAGE_OVERHEAD, pageSize(), (byte)0);

                PageHeader.fullPageId(absPtr, fullId);
                PageHeader.writeTimestamp(absPtr, U.currentTimeMillis());
                PageIO.setPageId(pageAddr, pageId);

                assert !PageHeader.isAcquired(absPtr) :
                    "Pin counter must be 0 for a new page [relPtr=" + U.hexLong(relPtr) +
                        ", absPtr=" + U.hexLong(absPtr) + ']';

                rwLock.init(absPtr + PAGE_LOCK_OFFSET, PageIdUtils.tag(pageId));
            }
            else
                absPtr = seg.absolute(relPtr);

            seg.acquirePage(absPtr);

            return absPtr;
        }
        finally {
            seg.writeLock().unlock();
        }
    }

    /**
     * @param seg Segment.
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param rmv {@code True} if page should be removed.
     * @return Relative pointer to refreshed page.
     */
    private long refreshOutdatedPage(Segment seg, int cacheId, long pageId, boolean rmv) {
        assert seg.writeLock().isHeldByCurrentThread();

        int tag = seg.partTag(cacheId, PageIdUtils.partId(pageId));

        long relPtr = seg.loadedPages.refresh(cacheId, PageIdUtils.effectivePageId(pageId), tag);

        long absPtr = seg.absolute(relPtr);

        GridUnsafe.setMemory(absPtr + PAGE_OVERHEAD, pageSize(), (byte)0);

        PageHeader.dirty(absPtr, false);

        long tmpBufPtr = PageHeader.tempBufferPointer(absPtr);

        if (tmpBufPtr != INVALID_REL_PTR) {
            GridUnsafe.setMemory(checkpointPool.absolute(tmpBufPtr) + PAGE_OVERHEAD, pageSize(), (byte)0);

            PageHeader.tempBufferPointer(absPtr, INVALID_REL_PTR);

            // We pinned the page when allocated the temp buffer, release it now.
            PageHeader.releasePage(absPtr);

            checkpointPool.releaseFreePage(tmpBufPtr);
        }

        if (rmv)
            seg.loadedPages.remove(cacheId, PageIdUtils.effectivePageId(pageId), tag);

        if (seg.segCheckpointPages != null)
            seg.segCheckpointPages.remove(new FullPageId(pageId, cacheId));

        if (seg.dirtyPages != null)
            seg.dirtyPages.remove(new FullPageId(pageId, cacheId));

        return relPtr;
    }

    /**
     * @param fullId Full id.
     * @param absPtr Absolute pointer.
     */
    private void tryToRestorePage(FullPageId fullId, long absPtr) throws IgniteCheckedException {
        Long tmpAddr = null;

        try {
            ByteBuffer curPage = null;
            ByteBuffer lastValidPage = null;

            try (WALIterator it = walMgr.replay(null)) {
                for (IgniteBiTuple<WALPointer, WALRecord> tuple : it) {
                    switch (tuple.getValue().type()) {
                        case PAGE_RECORD:
                            PageSnapshot snapshot = (PageSnapshot)tuple.getValue();

                            if (snapshot.fullPageId().equals(fullId)) {
                                if (tmpAddr == null) {
                                    assert snapshot.pageData().length <= pageSize() : snapshot.pageData().length;

                                    tmpAddr = GridUnsafe.allocateMemory(pageSize());
                                }

                                if (curPage == null)
                                    curPage = wrapPointer(tmpAddr, pageSize());

                                PageUtils.putBytes(tmpAddr, 0, snapshot.pageData());
                            }

                            break;

                        case CHECKPOINT_RECORD:
                            CheckpointRecord rec = (CheckpointRecord)tuple.getValue();

                            assert !rec.end();

                            if (curPage != null) {
                                lastValidPage = curPage;
                                curPage = null;
                            }

                            break;

                        case MEMORY_RECOVERY: // It means that previous checkpoint was broken.
                            curPage = null;

                            break;

                        default:
                            if (tuple.getValue() instanceof PageDeltaRecord) {
                                PageDeltaRecord deltaRecord = (PageDeltaRecord)tuple.getValue();

                                if (curPage != null
                                    && deltaRecord.pageId() == fullId.pageId()
                                    && deltaRecord.groupId() == fullId.groupId()) {
                                    assert tmpAddr != null;

                                    deltaRecord.applyDelta(this, tmpAddr);
                                }
                            }
                    }
                }
            }

            ByteBuffer restored = curPage == null ? lastValidPage : curPage;

            if (restored == null)
                throw new AssertionError(String.format(
                    "Page is broken. Can't restore it from WAL. (grpId = %d, pageId = %X).",
                    fullId.groupId(), fullId.pageId()
                ));

            long pageAddr = writeLockPage(absPtr, fullId, false);

            try {
                pageBuffer(pageAddr).put(restored);
            }
            finally {
                writeUnlockPage(absPtr, fullId, null, true, false);
            }
        }
        finally {
            if (tmpAddr != null)
                GridUnsafe.freeMemory(tmpAddr);
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
    @Override public boolean safeToUpdate() {
        for (Segment segment : segments)
            if (!segment.safeToUpdate())
                return false;

        return true;
    }

    /**
     * @param dirtyRatioThreshold Throttle threshold.
     */
    boolean shouldThrottle(double dirtyRatioThreshold) {
        for (Segment segment : segments) {
            if (segment.shouldThrottle(dirtyRatioThreshold))
                return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public GridMultiCollectionWrapper<FullPageId> beginCheckpoint() throws IgniteException {
        Collection[] collections = new Collection[segments.length];

        for (int i = 0; i < segments.length; i++) {
            Segment seg = segments[i];

            if (seg.segCheckpointPages != null)
                throw new IgniteException("Failed to begin checkpoint (it is already in progress).");

            collections[i] = seg.segCheckpointPages = seg.dirtyPages;

            seg.dirtyPages = new GridConcurrentHashSet<>();
        }

        memMetrics.resetDirtyPages();

        return new GridMultiCollectionWrapper<>(collections);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "TooBroadScope"})
    @Override public void finishCheckpoint() {
        for (Segment seg : segments)
            seg.segCheckpointPages = null;

        if (throttleEnabled)
            writeThrottle.onFinishCheckpoint();
    }

    /** {@inheritDoc} */
    @Override public Integer getForCheckpoint(FullPageId fullId, ByteBuffer outBuf, CheckpointMetricsTracker tracker) {
        assert outBuf.remaining() == pageSize();

        Segment seg = segment(fullId.groupId(), fullId.pageId());

        long absPtr = 0;

        long relPtr;

        int tag;

        boolean tmpBuffer = false;

        seg.readLock().lock();

        try {
            if (!isInCheckpoint(fullId))
                return null;

            tag = seg.partTag(fullId.groupId(), PageIdUtils.partId(fullId.pageId()));

            relPtr = seg.loadedPages.get(
                fullId.groupId(),
                PageIdUtils.effectivePageId(fullId.pageId()),
                tag,
                INVALID_REL_PTR,
                OUTDATED_REL_PTR
            );

            // Page may have been cleared during eviction. We have nothing to do in this case.
            if (relPtr == INVALID_REL_PTR)
                return null;

            if (relPtr != OUTDATED_REL_PTR) {
                absPtr = seg.absolute(relPtr);

                // Pin the page until page will not be copied.
                if (PageHeader.tempBufferPointer(absPtr) == INVALID_REL_PTR)
                    PageHeader.acquirePage(absPtr);
                else
                    tmpBuffer = true;
            }
        }
        finally {
            seg.readLock().unlock();
        }

        if (relPtr == OUTDATED_REL_PTR) {
            seg.writeLock().lock();

            try {
                // Double-check.
                relPtr = seg.loadedPages.get(
                    fullId.groupId(),
                    PageIdUtils.effectivePageId(fullId.pageId()),
                    seg.partTag(
                        fullId.groupId(),
                        PageIdUtils.partId(fullId.pageId())
                    ),
                    INVALID_REL_PTR,
                    OUTDATED_REL_PTR
                );

                if (relPtr == INVALID_REL_PTR)
                    return null;

                if (relPtr == OUTDATED_REL_PTR) {
                    relPtr = refreshOutdatedPage(
                        seg,
                        fullId.groupId(),
                        PageIdUtils.effectivePageId(fullId.pageId()),
                        true
                    );

                    seg.pool.releaseFreePage(relPtr);
                }

                return null;
            }
            finally {
                seg.writeLock().unlock();
            }
        }
        else
            return copyPageForCheckpoint(absPtr, fullId, outBuf, tmpBuffer, tracker) ? tag : null;
    }

    /**
     * @param absPtr Absolute ptr.
     * @param fullId Full id.
     * @param tmpBuf Tmp buffer.
     */
    private boolean copyPageForCheckpoint(
        long absPtr,
        FullPageId fullId,
        ByteBuffer tmpBuf,
        boolean tmpBuffer,
        CheckpointMetricsTracker tracker
    ) {
        assert absPtr != 0;
        assert PageHeader.isAcquired(absPtr);

        rwLock.writeLock(absPtr + PAGE_LOCK_OFFSET, OffheapReadWriteLock.TAG_LOCK_ALWAYS);

        try {
            long tmpRelPtr = PageHeader.tempBufferPointer(absPtr);

            boolean success = clearCheckpoint(fullId);

            assert success : "Page was pin when we resolve abs pointer, it can not be evicted";

            if (tmpRelPtr != INVALID_REL_PTR){
                PageHeader.tempBufferPointer(absPtr, INVALID_REL_PTR);

                long tmpAbsPtr = checkpointPool.absolute(tmpRelPtr);

                copyInBuffer(tmpAbsPtr, tmpBuf);

                GridUnsafe.setMemory(tmpAbsPtr + PAGE_OVERHEAD, pageSize(), (byte)0);

                if (tracker != null)
                    tracker.onCowPageWritten();

                checkpointPool.releaseFreePage(tmpRelPtr);

                // We pinned the page when allocated the temp buffer, release it now.
                PageHeader.releasePage(absPtr);

                // Need release again because we pin page when resolve abs pointer,
                // and page did not have tmp buffer page.
                if (!tmpBuffer)
                    PageHeader.releasePage(absPtr);

            }
            else {
                copyInBuffer(absPtr, tmpBuf);

                PageHeader.dirty(absPtr, false);

                // We pinned the page when resolve abs pointer.
                PageHeader.releasePage(absPtr);
            }

            return true;
        }
        finally {
            rwLock.writeUnlock(absPtr + PAGE_LOCK_OFFSET, OffheapReadWriteLock.TAG_LOCK_ALWAYS);
        }
    }

    /**
     * @param absPtr Absolute ptr.
     * @param tmpBuf Tmp buffer.
     */
    private void copyInBuffer(long absPtr, ByteBuffer tmpBuf) {
        if (tmpBuf.isDirect()) {
            long tmpPtr = ((DirectBuffer)tmpBuf).address();

            GridUnsafe.copyMemory(absPtr + PAGE_OVERHEAD, tmpPtr, pageSize());

            assert GridUnsafe.getInt(absPtr + PAGE_OVERHEAD + 4) == 0; //TODO GG-11480
            assert GridUnsafe.getInt(tmpPtr + 4) == 0; //TODO GG-11480
        }
        else {
            byte[] arr = tmpBuf.array();

            assert arr != null;
            assert arr.length == pageSize();

            GridUnsafe.copyMemory(null, absPtr + PAGE_OVERHEAD, arr, GridUnsafe.BYTE_ARR_OFF, pageSize());
        }
    }

    /** {@inheritDoc} */
    @Override public int invalidate(int cacheId, int partId) {
        int tag = 0;

        for (Segment seg : segments) {
            seg.writeLock().lock();

            try {
                int newTag = seg.incrementPartTag(cacheId, partId);

                if (tag == 0)
                    tag = newTag;

                assert tag == newTag;
            }
            finally {
                seg.writeLock().unlock();
            }
        }

        return tag;
    }

    /** {@inheritDoc} */
    @Override public void onCacheGroupDestroyed(int grpId) {
        for (Segment seg : segments) {
            seg.writeLock().lock();

            try {
                seg.resetPartTags(grpId);
            }
            finally {
                seg.writeLock().unlock();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Void> clearAsync(
        GridPredicate3<Integer, Long, Integer> pred,
        boolean cleanDirty
    ) {
        CountDownFuture completeFut = new CountDownFuture(segments.length);

        for (Segment seg : segments) {
            Runnable clear = new ClearSegmentRunnable(seg, pred, cleanDirty, completeFut, pageSize());

            try {
                asyncRunner.execute(clear);
            }
            catch (RejectedExecutionException ignore) {
                clear.run();
            }
        }

        return completeFut;
    }

    /** {@inheritDoc} */
    @Override public long loadedPages() {
        long total = 0;

        Segment[] segments = this.segments;

        if (segments != null) {
            for (Segment seg : segments) {
                if (seg == null)
                    break;

                seg.readLock().lock();

                try {
                    if (seg.closed)
                        continue;

                    total += seg.loadedPages.size();
                }
                finally {
                    seg.readLock().unlock();
                }
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
                if (seg.closed)
                    continue;

                total += seg.acquiredPages();
            }
            finally {
                seg.readLock().unlock();
            }
        }

        return total;
    }

    /**
     * @param absPtr Absolute pointer to read lock.
     * @param force Force flag.
     * @return Pointer to the page read buffer.
     */
    private long readLockPage(long absPtr, FullPageId fullId, boolean force) {
        int tag = force ? -1 : PageIdUtils.tag(fullId.pageId());

        boolean locked = rwLock.readLock(absPtr + PAGE_LOCK_OFFSET, tag);

        if (!locked)
            return 0;

        PageHeader.writeTimestamp(absPtr, U.currentTimeMillis());

        assert GridUnsafe.getInt(absPtr + PAGE_OVERHEAD + 4) == 0; //TODO GG-11480

        return absPtr + PAGE_OVERHEAD;
    }

    /** {@inheritDoc} */
    @Override public long readLockForce(int cacheId, long pageId, long page) {
        return readLockPage(page, new FullPageId(pageId, cacheId), true);
    }

    /**
     * @param absPtr Absolute pointer to unlock.
     */
    void readUnlockPage(long absPtr) {
        rwLock.readUnlock(absPtr + PAGE_LOCK_OFFSET);
    }

    /**
     * Checks if a page has temp copy buffer.
     *
     * @param absPtr Absolute pointer.
     * @return {@code True} if a page has temp buffer.
     */
    public boolean hasTempCopy(long absPtr) {
        return PageHeader.tempBufferPointer(absPtr) != INVALID_REL_PTR;
    }

    /**
     * @param absPtr Absolute pointer.
     * @return Pointer to the page write buffer or {@code 0} if page was not locked.
     */
    long tryWriteLockPage(long absPtr, FullPageId fullId, boolean checkTag) {
        int tag = checkTag ? PageIdUtils.tag(fullId.pageId()) : OffheapReadWriteLock.TAG_LOCK_ALWAYS;

        if (!rwLock.tryWriteLock(absPtr + PAGE_LOCK_OFFSET, tag))
            return 0;

        return postWriteLockPage(absPtr, fullId);
    }

    /**
     * @param absPtr Absolute pointer.
     * @return Pointer to the page write buffer.
     */
    private long writeLockPage(long absPtr, FullPageId fullId, boolean checkTag) {
        int tag = checkTag ? PageIdUtils.tag(fullId.pageId()) : OffheapReadWriteLock.TAG_LOCK_ALWAYS;

        boolean locked = rwLock.writeLock(absPtr + PAGE_LOCK_OFFSET, tag);

        return locked ? postWriteLockPage(absPtr, fullId) : 0;
    }

    /**
     * @param absPtr Absolute pointer.
     * @return Pointer to the page write buffer.
     */
    private long postWriteLockPage(long absPtr, FullPageId fullId) {
        PageHeader.writeTimestamp(absPtr, U.currentTimeMillis());

        // Create a buffer copy if the page is scheduled for a checkpoint.
        if (isInCheckpoint(fullId) && PageHeader.tempBufferPointer(absPtr) == INVALID_REL_PTR) {
            long tmpRelPtr = checkpointPool.borrowOrAllocateFreePage(fullId.pageId());

            if (tmpRelPtr == INVALID_REL_PTR) {
                rwLock.writeUnlock(absPtr + PAGE_LOCK_OFFSET, OffheapReadWriteLock.TAG_LOCK_ALWAYS);

                throw new IgniteException(
                    "Failed to allocate temporary buffer for checkpoint " +
                        "(increase checkpointPageBufferSize configuration property)");
            }

            // Pin the page until checkpoint is not finished.
            PageHeader.acquirePage(absPtr);

            long tmpAbsPtr = checkpointPool.absolute(tmpRelPtr);

            GridUnsafe.copyMemory(
                null,
                absPtr + PAGE_OVERHEAD,
                null,
                tmpAbsPtr + PAGE_OVERHEAD,
                pageSize()
            );

            PageHeader.dirty(absPtr, false);
            PageHeader.tempBufferPointer(absPtr, tmpRelPtr);

            assert GridUnsafe.getInt(absPtr + PAGE_OVERHEAD + 4) == 0; //TODO GG-11480
            assert GridUnsafe.getInt(tmpAbsPtr + PAGE_OVERHEAD + 4) == 0; //TODO GG-11480
        }

        assert GridUnsafe.getInt(absPtr + PAGE_OVERHEAD + 4) == 0; //TODO GG-11480

        return absPtr + PAGE_OVERHEAD;
    }

    /**
     * @param page Page pointer.
     * @param walPlc Full page WAL record policy.
     */
    private void writeUnlockPage(
        long page,
        FullPageId fullId,
        Boolean walPlc,
        boolean markDirty,
        boolean restore
    ) {
        boolean dirty = isDirty(page);

        //if page is for restore, we shouldn't mark it as changed
        if (!restore && markDirty && !dirty && changeTracker != null)
            changeTracker.apply(page, fullId, this);

        boolean pageWalRec = markDirty && walPlc != FALSE && (walPlc == TRUE || !dirty);

        assert GridUnsafe.getInt(page + PAGE_OVERHEAD + 4) == 0; //TODO GG-11480

        if (markDirty)
            setDirty(fullId, page, markDirty, false);

        beforeReleaseWrite(fullId, page + PAGE_OVERHEAD, pageWalRec);

        long pageId = PageIO.getPageId(page + PAGE_OVERHEAD);

        try {
            rwLock.writeUnlock(page + PAGE_LOCK_OFFSET, PageIdUtils.tag(pageId));

            if (throttleEnabled && !restore && markDirty && !dirty)
                writeThrottle.onMarkDirty(isInCheckpoint(fullId));
        }
        catch (AssertionError ex) {
            StringBuilder sb = new StringBuilder(sysPageSize * 2);

            for (int i = 0; i < systemPageSize(); i += 8)
                sb.append(U.hexLong(GridUnsafe.getLong(page + i)));

            U.error(log, "Failed to unlock page [fullPageId=" + fullId + ", binPage=" + sb + ']');

            throw ex;
        }
    }

    /**
     * @param absPtr Absolute pointer to the page.
     * @return {@code True} if write lock acquired for the page.
     */
    boolean isPageWriteLocked(long absPtr) {
        return rwLock.isWriteLocked(absPtr + PAGE_LOCK_OFFSET);
    }

    /**
     * @param absPtr Absolute pointer to the page.
     * @return {@code True} if read lock acquired for the page.
     */
    boolean isPageReadLocked(long absPtr) {
        return rwLock.isReadLocked(absPtr + PAGE_LOCK_OFFSET);
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
        Segment seg = segment(pageId.groupId(), pageId.pageId());

        Collection<FullPageId> pages0 = seg.segCheckpointPages;

        return pages0 != null && pages0.contains(pageId);
    }

    /**
     * @param fullPageId Page ID to clear.
     * @return {@code True} if remove successfully.
     */
    boolean clearCheckpoint(FullPageId fullPageId) {
        Segment seg = segment(fullPageId.groupId(), fullPageId.pageId());

        Collection<FullPageId> pages0 = seg.segCheckpointPages;

        assert pages0 != null;

        return pages0.remove(fullPageId);
    }

    /**
     * @param absPtr Absolute pointer.
     * @return {@code True} if page is dirty.
     */
    boolean isDirty(long absPtr) {
        return PageHeader.dirty(absPtr);
    }

    /**
     * Gets the number of active pages across all segments. Used for test purposes only.
     *
     * @return Number of active pages.
     */
    public int activePagesCount() {
        int total = 0;

        for (Segment seg : segments)
            total += seg.acquiredPages();

        return total;
    }

    /**
     * Number of used pages in checkpoint buffer.
     */
    public int checkpointBufferPagesCount() {
        return cpBufPagesCntr.get();
    }

    /**
     * Number of used pages in checkpoint buffer.
     */
    public int checkpointBufferPagesSize() {
        return checkpointPool.pages();
    }

    /**
     * This method must be called in synchronized context.
     *
     * @param absPtr Absolute pointer.
     * @param dirty {@code True} dirty flag.
     * @param forceAdd If this flag is {@code true}, then the page will be added to the dirty set regardless whether the
     * old flag was dirty or not.
     */
    void setDirty(FullPageId pageId, long absPtr, boolean dirty, boolean forceAdd) {
        boolean wasDirty = PageHeader.dirty(absPtr, dirty);

        if (dirty) {
            if (!wasDirty || forceAdd) {
                boolean added = segment(pageId.groupId(), pageId.pageId()).dirtyPages.add(pageId);

                if (added)
                    memMetrics.incrementDirtyPages();
            }
        }
        else {
            boolean rmv = segment(pageId.groupId(), pageId.pageId()).dirtyPages.remove(pageId);

            if (rmv)
                memMetrics.decrementDirtyPages();
        }
    }

    /**
     *
     */
    void beforeReleaseWrite(FullPageId pageId, long ptr, boolean pageWalRec) {
        if (walMgr != null && (pageWalRec || walMgr.isAlwaysWriteFullPages())) {
            try {
                walMgr.log(new PageSnapshot(pageId, ptr, pageSize()));
            }
            catch (IgniteCheckedException e) {
                // TODO ignite-db.
                throw new IgniteException(e);
            }
        }
    }

    /**
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @return Segment.
     */
    private Segment segment(int grpId, long pageId) {
        int idx = segmentIndex(grpId, pageId, segments.length);

        return segments[idx];
    }

    /**
     * @param pageId Page ID.
     * @return Segment index.
     */
    public static int segmentIndex(int grpId, long pageId, int segments) {
        pageId = PageIdUtils.effectivePageId(pageId);

        // Take a prime number larger than total number of partitions.
        int hash = U.hash(pageId * 65537 + grpId);

        return U.safeAbs(hash) % segments;
    }

    /**
     *
     */
    private class PagePool {
        /** Segment index. */
        protected final int idx;

        /** Direct memory region. */
        protected final DirectMemoryRegion region;

        /** Pool pages counter. */
        protected final AtomicInteger pagesCntr;

        /** */
        protected long lastAllocatedIdxPtr;

        /** Pointer to the address of the free page list. */
        protected long freePageListPtr;

        /** Pages base. */
        protected long pagesBase;

        /**
         * @param idx Index.
         * @param region Region
         * @param pagesCntr Pages counter.
         */
        protected PagePool(int idx, DirectMemoryRegion region, AtomicInteger pagesCntr) {
            this.idx = idx;
            this.region = region;
            this.pagesCntr = pagesCntr;

            long base = (region.address() + 7) & ~0x7;

            freePageListPtr = base;

            base += 8;

            lastAllocatedIdxPtr = base;

            base += 8;

            // Align page start by
            pagesBase = base;

            GridUnsafe.putLong(freePageListPtr, INVALID_REL_PTR);
            GridUnsafe.putLong(lastAllocatedIdxPtr, 1L);
        }

        /**
         * Allocates a new free page.
         *
         * @param pageId Page ID to to initialize.
         * @return Relative pointer to the allocated page.
         * @throws GridOffHeapOutOfMemoryException If failed to allocate new free page.
         */
        private long borrowOrAllocateFreePage(long pageId) throws GridOffHeapOutOfMemoryException {
            if (pagesCntr != null)
                pagesCntr.getAndIncrement();

            long relPtr = borrowFreePage();

            return relPtr != INVALID_REL_PTR ? relPtr : allocateFreePage(pageId);
        }

        /**
         * @return Relative pointer to a free page that was borrowed from the allocated pool.
         */
        private long borrowFreePage() {
            while (true) {
                long freePageRelPtrMasked = GridUnsafe.getLong(freePageListPtr);

                long freePageRelPtr = freePageRelPtrMasked & ADDRESS_MASK;

                if (freePageRelPtr != INVALID_REL_PTR) {
                    long freePageAbsPtr = absolute(freePageRelPtr);

                    long nextFreePageRelPtr = GridUnsafe.getLong(freePageAbsPtr) & ADDRESS_MASK;

                    long cnt = ((freePageRelPtrMasked & COUNTER_MASK) + COUNTER_INC) & COUNTER_MASK;

                    if (GridUnsafe.compareAndSwapLong(null, freePageListPtr, freePageRelPtrMasked, nextFreePageRelPtr | cnt)) {
                        GridUnsafe.putLong(freePageAbsPtr, PAGE_MARKER);

                        return freePageRelPtr;
                    }
                }
                else
                    return INVALID_REL_PTR;
            }
        }

        /**
         * @param pageId Page ID.
         * @return Relative pointer of the allocated page.
         * @throws GridOffHeapOutOfMemoryException If failed to allocate new free page.
         */
        private long allocateFreePage(long pageId) throws GridOffHeapOutOfMemoryException {
            long limit = region.address() + region.size();

            while (true) {
                long lastIdx = GridUnsafe.getLong(lastAllocatedIdxPtr);

                // Check if we have enough space to allocate a page.
                if (pagesBase + (lastIdx + 1) * sysPageSize > limit)
                    return INVALID_REL_PTR;

                if (GridUnsafe.compareAndSwapLong(null, lastAllocatedIdxPtr, lastIdx, lastIdx + 1)) {
                    long absPtr = pagesBase + lastIdx * sysPageSize;

                    assert (lastIdx & SEGMENT_INDEX_MASK) == 0L;

                    long relative = relative(lastIdx);

                    assert relative != INVALID_REL_PTR;

                    PageHeader.initNew(absPtr, relative);

                    rwLock.init(absPtr + PAGE_LOCK_OFFSET, PageIdUtils.tag(pageId));

                    return relative;
                }
            }
        }

        /**
         * @param relPtr Relative pointer to free.
         */
        private void releaseFreePage(long relPtr) {
            long absPtr = absolute(relPtr);

            assert !PageHeader.isAcquired(absPtr) : "Release pinned page: " + PageHeader.fullPageId(absPtr);

            if (pagesCntr != null)
                pagesCntr.getAndDecrement();

            while (true) {
                long freePageRelPtrMasked = GridUnsafe.getLong(freePageListPtr);

                long freePageRelPtr = freePageRelPtrMasked & RELATIVE_PTR_MASK;

                GridUnsafe.putLong(absPtr, freePageRelPtr);

                if (GridUnsafe.compareAndSwapLong(null, freePageListPtr, freePageRelPtrMasked, relPtr))
                    return;
            }
        }

        /**
         * @param relativePtr Relative pointer.
         * @return Absolute pointer.
         */
        long absolute(long relativePtr) {
            int segIdx = (int)((relativePtr >> 40) & 0xFFFF);

            assert segIdx == idx : "expected=" + idx + ", actual=" + segIdx;

            long pageIdx = relativePtr & ~SEGMENT_INDEX_MASK;

            long off = pageIdx * sysPageSize;

            return pagesBase + off;
        }

        /**
         * @param pageIdx Page index in the pool.
         * @return Relative pointer.
         */
        private long relative(long pageIdx) {
            return pageIdx | ((long)idx) << 40;
        }

        /**
         * @return Max number of pages in the pool.
         */
        private int pages() {
            return (int)((region.size() - (pagesBase - region.address())) / sysPageSize);
        }
    }

    /**
     *
     */
    private class Segment extends ReentrantReadWriteLock {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private static final double FULL_SCAN_THRESHOLD = 0.4;

        /** Page ID to relative pointer map. */
        private FullPageIdTable loadedPages;

        /** */
        private long acquiredPagesPtr;

        /** */
        private PagePool pool;

        /** */
        private long memPerTbl;

        /** Pages marked as dirty since the last checkpoint. */
        private Collection<FullPageId> dirtyPages = new GridConcurrentHashSet<>();

        /** */
        private volatile Collection<FullPageId> segCheckpointPages;

        /** */
        private final int maxDirtyPages;

        /** Maps partition (cacheId, partId) to its tag. Tag is 1-based incrementing partition file counter */
        private final Map<T2<Integer, Integer>, Integer> partTagMap = new HashMap<>();

        /** */
        private boolean closed;

        /**
         * @param region Memory region.
         * @param throttlingEnabled Write throttling enabled flag.
         */
        private Segment(int idx, DirectMemoryRegion region, int cpPoolPages, boolean throttlingEnabled) {
            long totalMemory = region.size();

            int pages = (int)(totalMemory / sysPageSize);

            memPerTbl = requiredSegmentTableMemory(pages);

            acquiredPagesPtr = region.address();

            GridUnsafe.putIntVolatile(null, acquiredPagesPtr, 0);

            loadedPages = new FullPageIdTable(region.address() + 8, memPerTbl, true);

            DirectMemoryRegion poolRegion = region.slice(memPerTbl + 8);

            pool = new PagePool(idx, poolRegion, null);

            maxDirtyPages = throttlingEnabled ? pool.pages() * 3 / 4 : Math.min(pool.pages() * 2 / 3, cpPoolPages);
        }

        /**
         * Closes the segment.
         */
        private void close() {
            writeLock().lock();

            try {
                closed = true;
            }
            finally {
                writeLock().unlock();
            }
        }

        /**
         *
         */
        private boolean safeToUpdate() {
            return dirtyPages.size() < maxDirtyPages;
        }

        /**
         * @param dirtyRatioThreshold Throttle threshold.
         */
        private boolean shouldThrottle(double dirtyRatioThreshold) {
            return ((double)dirtyPages.size()) / pages() > dirtyRatioThreshold;
        }

        /**
         * @return Max number of pages this segment can allocate.
         */
        private int pages() {
            return pool.pages();
        }

        /**
         * @return Memory allocated for pages table.
         */
        private long tableSize() {
            return memPerTbl;
        }

        /**
         * @param absPtr Page absolute address to acquire.
         */
        private void acquirePage(long absPtr) {
            PageHeader.acquirePage(absPtr);

            updateAtomicInt(acquiredPagesPtr, 1);
        }

        /**
         * @param absPtr Page absolute address to release.
         */
        private void releasePage(long absPtr) {
            PageHeader.releasePage(absPtr);

            updateAtomicInt(acquiredPagesPtr, -1);
        }

        /**
         * @return Total number of acquired pages.
         */
        private int acquiredPages() {
            return GridUnsafe.getInt(acquiredPagesPtr);
        }

        /**
         * @param pageId Page ID.
         * @return Page relative pointer.
         */
        private long borrowOrAllocateFreePage(long pageId) {
            return pool.borrowOrAllocateFreePage(pageId);
        }

        /**
         * Prepares a page for eviction, if needed.
         *
         * @param fullPageId Candidate page full ID.
         * @param absPtr Absolute pointer of the page to evict.
         * @return {@code True} if it is ok to evict this page, {@code false} if another page should be selected.
         * @throws IgniteCheckedException If failed to write page to the underlying store during eviction.
         */
        private boolean prepareEvict(FullPageId fullPageId, long absPtr) throws IgniteCheckedException {
            assert writeLock().isHeldByCurrentThread();

            // Do not evict cache meta pages.
            if (fullPageId.pageId() == storeMgr.metaPageId(fullPageId.groupId()))
                return false;

            if (PageHeader.isAcquired(absPtr))
                return false;

            Collection<FullPageId> cpPages = segCheckpointPages;

            if (isDirty(absPtr)) {
                // Can evict a dirty page only if should be written by a checkpoint.
                // These pages does not have tmp buffer.
                if (cpPages != null && cpPages.contains(fullPageId)) {
                    assert storeMgr != null;

                    memMetrics.updatePageReplaceRate(U.currentTimeMillis() - PageHeader.readTimestamp(absPtr));

                    flushDirtyPage.applyx(
                        fullPageId,
                        wrapPointer(absPtr + PAGE_OVERHEAD, pageSize()),
                        partTag(
                            fullPageId.groupId(),
                            PageIdUtils.partId(fullPageId.pageId())
                        )
                    );

                    setDirty(fullPageId, absPtr, false, true);

                    cpPages.remove(fullPageId);

                    return true;
                }

                return false;
            }
            else {
                memMetrics.updatePageReplaceRate(U.currentTimeMillis() - PageHeader.readTimestamp(absPtr));

                // Page was not modified, ok to evict.
                return true;
            }
        }

        /**
         * Evict random oldest page from memory to storage.
         *
         * @return Relative address for evicted page.
         * @throws IgniteCheckedException If failed to evict page.
         */
        private long evictPage() throws IgniteCheckedException {
            assert getWriteHoldCount() > 0;

            if (!pageEvictWarned) {
                pageEvictWarned = true;

                U.warn(log, "Page evictions started, this will affect storage performance (consider increasing " +
                    "DataStorageConfiguration#setPageCacheSize).");
            }

            final ThreadLocalRandom rnd = ThreadLocalRandom.current();

            final int cap = loadedPages.capacity();

            if (acquiredPages() >= loadedPages.size())
                throw new IgniteOutOfMemoryException("Failed to evict page from segment (all pages are acquired).");

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
                    ++iterations;

                    if (iterations > pool.pages() * FULL_SCAN_THRESHOLD)
                        break;

                    // We need to lookup for pages only in current segment for thread safety,
                    // so peeking random memory will lead to checking for found page segment.
                    // It's much faster to check available pages for segment right away.
                    EvictCandidate nearest = loadedPages.getNearestAt(rnd.nextInt(cap), INVALID_REL_PTR);

                    assert nearest != null && nearest.relativePointer() != INVALID_REL_PTR;

                    long rndAddr = nearest.relativePointer();

                    int partTag = nearest.tag();

                    final long absPageAddr = absolute(rndAddr);

                    FullPageId fullId = PageHeader.fullPageId(absPageAddr);

                    // Check page mapping consistency.
                    assert fullId.equals(nearest.fullId()) : "Invalid page mapping [tableId=" + nearest.fullId() +
                        ", actual=" + fullId + ", nearest=" + nearest;

                    boolean outdated = partTag < partTag(fullId.groupId(), PageIdUtils.partId(fullId.pageId()));

                    if (outdated)
                        return refreshOutdatedPage(this, fullId.groupId(), fullId.pageId(), true);

                    boolean pinned = PageHeader.isAcquired(absPageAddr);

                    boolean skip = ignored != null && ignored.contains(rndAddr);

                    if (relEvictAddr == rndAddr || pinned || skip) {
                        i--;

                        continue;
                    }

                    final long pageTs = PageHeader.readTimestamp(absPageAddr);

                    final boolean dirty = isDirty(absPageAddr);

                    if (pageTs < cleanTs && !dirty) {
                        cleanAddr = rndAddr;

                        cleanTs = pageTs;
                    }
                    else if (pageTs < dirtyTs && dirty) {
                        dirtyAddr = rndAddr;

                        dirtyTs = pageTs;
                    }

                    relEvictAddr = cleanAddr == INVALID_REL_PTR ? dirtyAddr : cleanAddr;
                }

                assert relEvictAddr != INVALID_REL_PTR;

                final long absEvictAddr = absolute(relEvictAddr);

                final FullPageId fullPageId = PageHeader.fullPageId(absEvictAddr);

                if (!prepareEvict(fullPageId, absEvictAddr)) {
                    if (iterations > 10) {
                        if (ignored == null)
                            ignored = new HashSet<>();

                        ignored.add(relEvictAddr);
                    }

                    if (iterations > pool.pages() * FULL_SCAN_THRESHOLD)
                        return tryToFindSequentially(cap);

                    continue;
                }

                loadedPages.remove(
                    fullPageId.groupId(),
                    PageIdUtils.effectivePageId(fullPageId.pageId()),
                    partTag(
                        fullPageId.groupId(),
                        PageIdUtils.partId(fullPageId.pageId())
                    )
                );

                return relEvictAddr;
            }
        }

        /**
         * Will scan all segment pages to find one to evict it
         *
         * @param cap Capacity.
         */
        private long tryToFindSequentially(int cap) throws IgniteCheckedException {
            assert getWriteHoldCount() > 0;

            long prevAddr = INVALID_REL_PTR;
            int pinnedCnt = 0;
            int failToPrepare = 0;

            for (int i = 0; i < cap; i++) {
                final EvictCandidate nearest = loadedPages.getNearestAt(i, INVALID_REL_PTR);

                assert nearest != null && nearest.relativePointer() != INVALID_REL_PTR;

                final long addr = nearest.relativePointer();

                int partTag = nearest.tag();

                final long absPageAddr = absolute(addr);

                FullPageId fullId = PageHeader.fullPageId(absPageAddr);

                if (partTag < partTag(fullId.groupId(), PageIdUtils.partId(fullId.pageId())))
                    return refreshOutdatedPage(this, fullId.groupId(), fullId.pageId(), true);

                boolean pinned = PageHeader.isAcquired(absPageAddr);

                if (pinned)
                    pinnedCnt++;

                if (addr == prevAddr || pinned)
                    continue;

                final long absEvictAddr = absolute(addr);

                final FullPageId fullPageId = PageHeader.fullPageId(absEvictAddr);

                if (prepareEvict(fullPageId, absEvictAddr)) {
                    loadedPages.remove(
                        fullPageId.groupId(),
                        PageIdUtils.effectivePageId(fullPageId.pageId()),
                        partTag(
                            fullPageId.groupId(),
                            PageIdUtils.partId(fullPageId.pageId())
                        )
                    );

                    return addr;
                }
                else
                    failToPrepare++;

                prevAddr = addr;
            }

            throw new IgniteOutOfMemoryException("Failed to find a page for eviction [segmentCapacity=" + cap +
                ", loaded=" + loadedPages.size() +
                ", maxDirtyPages=" + maxDirtyPages +
                ", dirtyPages=" + dirtyPages.size() +
                ", cpPages=" + (segCheckpointPages == null ? 0 : segCheckpointPages.size()) +
                ", pinnedInSegment=" + pinnedCnt +
                ", failedToPrepare=" + failToPrepare +
                ']');
        }

        /**
         * Delegate to the corresponding page pool.
         *
         * @param relPtr Relative pointer.
         * @return Absolute pointer.
         */
        private long absolute(long relPtr) {
            return pool.absolute(relPtr);
        }

        /**
         * @param grpId Cache group ID.
         * @param partId Partition ID.
         * @return Partition tag. Growing 1 based partition file version
         */
        private int partTag(int grpId, int partId) {
            assert getReadHoldCount() > 0 || getWriteHoldCount() > 0;

            Integer tag = partTagMap.get(new T2<>(grpId, partId));

            if (tag == null)
                return 1;

            return tag;
        }

        /**
         * @param grpId Cache group ID.
         * @param partId Partition ID.
         * @return New partition tag.
         */
        private int incrementPartTag(int grpId, int partId) {
            assert getWriteHoldCount() > 0;

            T2<Integer, Integer> t = new T2<>(grpId, partId);

            Integer tag = partTagMap.get(t);

            if (tag == null) {
                partTagMap.put(t, 2);

                return 2;
            }
            else if (tag == Integer.MAX_VALUE) {
                U.warn(log, "Partition tag overflow [grpId=" + grpId + ", partId=" + partId + "]");

                partTagMap.put(t, 0);

                return 0;
            }
            else {
                partTagMap.put(t, tag + 1);

                return tag + 1;
            }
        }

        /**
         * @param grpId Cache group id.
         */
        private void resetPartTags(int grpId) {
            assert getWriteHoldCount() > 0;

            Iterator<T2<Integer, Integer>> iter = partTagMap.keySet().iterator();

            while (iter.hasNext()) {
                T2<Integer, Integer> t = iter.next();

                if (t.get1() == grpId)
                    iter.remove();
            }
        }
    }

    /**
     * Gets an estimate for the amount of memory required to store the given number of page IDs
     * in a segment table.
     *
     * @param pages Number of pages to store.
     * @return Memory size estimate.
     */
    private static long requiredSegmentTableMemory(int pages) {
        return FullPageIdTable.requiredMemory(pages) + 8;
    }

    /**
     * @param ptr Pointer to update.
     * @param delta Delta.
     */
    private static int updateAtomicInt(long ptr, int delta) {
        while (true) {
            int old = GridUnsafe.getInt(ptr);

            int updated = old + delta;

            if (GridUnsafe.compareAndSwapInt(null, ptr, old, updated))
                return updated;
        }
    }

    /**
     * @param ptr Pointer to update.
     * @param delta Delta.
     */
    private static long updateAtomicLong(long ptr, long delta) {
        while (true) {
            long old = GridUnsafe.getLong(ptr);

            long updated = old + delta;

            if (GridUnsafe.compareAndSwapLong(null, ptr, old, updated))
                return updated;
        }
    }

    /**
     *
     */
    private static class PageHeader {
        /**
         * @param absPtr Absolute pointer to initialize.
         * @param relative Relative pointer to write.
         */
        private static void initNew(long absPtr, long relative) {
            relative(absPtr, relative);

            tempBufferPointer(absPtr, INVALID_REL_PTR);

            GridUnsafe.putLong(absPtr, PAGE_MARKER);
            GridUnsafe.putInt(absPtr + PAGE_PIN_CNT_OFFSET, 0);
        }

        /**
         * @param absPtr Absolute pointer.
         * @return Dirty flag.
         */
        private static boolean dirty(long absPtr) {
            return flag(absPtr, DIRTY_FLAG);
        }

        /**
         * @param absPtr Page absolute pointer.
         * @param dirty Dirty flag.
         * @return Previous value of dirty flag.
         */
        private static boolean dirty(long absPtr, boolean dirty) {
            return flag(absPtr, DIRTY_FLAG, dirty);
        }

        /**
         * @param absPtr Absolute pointer.
         * @param flag Flag mask.
         * @return Flag value.
         */
        private static boolean flag(long absPtr, long flag) {
            assert (flag & 0xFFFFFFFFFFFFFFL) == 0;
            assert Long.bitCount(flag) == 1;

            long relPtrWithFlags = GridUnsafe.getLong(absPtr + RELATIVE_PTR_OFFSET);

            return (relPtrWithFlags & flag) != 0;
        }

        /**
         * Sets flag.
         *
         * @param absPtr Absolute pointer.
         * @param flag Flag mask.
         * @param set New flag value.
         * @return Previous flag value.
         */
        private static boolean flag(long absPtr, long flag, boolean set) {
            assert (flag & 0xFFFFFFFFFFFFFFL) == 0;
            assert Long.bitCount(flag) == 1;

            long relPtrWithFlags = GridUnsafe.getLong(absPtr + RELATIVE_PTR_OFFSET);

            boolean was = (relPtrWithFlags & flag) != 0;

            if (set)
                relPtrWithFlags |= flag;
            else
                relPtrWithFlags &= ~flag;

            GridUnsafe.putLong(absPtr + RELATIVE_PTR_OFFSET, relPtrWithFlags);

            return was;
        }

        /**
         * @param absPtr Page pointer.
         * @return If page is pinned.
         */
        private static boolean isAcquired(long absPtr) {
            return GridUnsafe.getInt(absPtr + PAGE_PIN_CNT_OFFSET) > 0;
        }

        /**
         * @param absPtr Absolute pointer.
         */
        private static void acquirePage(long absPtr) {
            updateAtomicInt(absPtr + PAGE_PIN_CNT_OFFSET, 1);
        }

        /**
         * @param absPtr Absolute pointer.
         */
        private static int releasePage(long absPtr) {
            return updateAtomicInt(absPtr + PAGE_PIN_CNT_OFFSET, -1);
        }

        /**
         * Reads relative pointer from the page at the given absolute position.
         *
         * @param absPtr Absolute memory pointer to the page header.
         * @return Relative pointer written to the page.
         */
        private static long readRelative(long absPtr) {
            return GridUnsafe.getLong(absPtr + RELATIVE_PTR_OFFSET) & RELATIVE_PTR_MASK;
        }

        /**
         * Writes relative pointer to the page at the given absolute position.
         *
         * @param absPtr Absolute memory pointer to the page header.
         * @param relPtr Relative pointer to write.
         */
        private static void relative(long absPtr, long relPtr) {
            GridUnsafe.putLong(absPtr + RELATIVE_PTR_OFFSET, relPtr & RELATIVE_PTR_MASK);
        }

        /**
         * Volatile write for current timestamp to page in {@code absAddr} address.
         *
         * @param absPtr Absolute page address.
         */
        private static void writeTimestamp(final long absPtr, long tstamp) {
            tstamp >>= 8;

            GridUnsafe.putLongVolatile(null, absPtr, (tstamp << 8) | 0x01);
        }

        /**
         * Read for timestamp from page in {@code absAddr} address.
         *
         * @param absPtr Absolute page address.
         * @return Timestamp.
         */
        private static long readTimestamp(final long absPtr) {
            long markerAndTs = GridUnsafe.getLong(absPtr);

            // Clear last byte as it is occupied by page marker.
            return markerAndTs & ~0xFF;
        }

        /**
         * @param absPtr Page absolute pointer.
         * @param tmpRelPtr Temp buffer relative pointer.
         */
        private static void tempBufferPointer(long absPtr, long tmpRelPtr) {
            GridUnsafe.putLong(absPtr + PAGE_TMP_BUF_OFFSET, tmpRelPtr);
        }

        /**
         * @param absPtr Page absolute pointer.
         * @return Temp buffer relative pointer.
         */
        private static long tempBufferPointer(long absPtr) {
            return GridUnsafe.getLong(absPtr + PAGE_TMP_BUF_OFFSET);
        }

        /**
         * Reads page ID from the page at the given absolute position.
         *
         * @param absPtr Absolute memory pointer to the page header.
         * @return Page ID written to the page.
         */
        private static long readPageId(long absPtr) {
            return GridUnsafe.getLong(absPtr + PAGE_ID_OFFSET);
        }

        /**
         * Writes page ID to the page at the given absolute position.
         *
         * @param absPtr Absolute memory pointer to the page header.
         * @param pageId Page ID to write.
         */
        private static void pageId(long absPtr, long pageId) {
            GridUnsafe.putLong(absPtr + PAGE_ID_OFFSET, pageId);
        }

        /**
         * Reads cache ID from the page at the given absolute pointer.
         *
         * @param absPtr Absolute memory pointer to the page header.
         * @return Cache ID written to the page.
         */
        private static int readPageCacheId(final long absPtr) {
            return GridUnsafe.getInt(absPtr + PAGE_CACHE_ID_OFFSET);
        }

        /**
         * Writes cache ID from the page at the given absolute pointer.
         *
         * @param absPtr Absolute memory pointer to the page header.
         * @param cacheId Cache ID to write.
         */
        private static void pageCacheId(final long absPtr, final int cacheId) {
            GridUnsafe.putInt(absPtr + PAGE_CACHE_ID_OFFSET, cacheId);
        }

        /**
         * Reads page ID and cache ID from the page at the given absolute pointer.
         *
         * @param absPtr Absolute memory pointer to the page header.
         * @return Full page ID written to the page.
         */
        private static FullPageId fullPageId(final long absPtr) {
            return new FullPageId(readPageId(absPtr), readPageCacheId(absPtr));
        }

        /**
         * Writes page ID and cache ID from the page at the given absolute pointer.
         *
         * @param absPtr Absolute memory pointer to the page header.
         * @param fullPageId Full page ID to write.
         */
        private static void fullPageId(final long absPtr, final FullPageId fullPageId) {
            pageId(absPtr, fullPageId.pageId());

            pageCacheId(absPtr, fullPageId.groupId());
        }
    }

    /**
     *
     */
    private static class ClearSegmentRunnable implements Runnable {
        /** */
        private Segment seg;

        /** */
        private GridPredicate3<Integer, Long, Integer> clearPred;

        /** */
        private CountDownFuture doneFut;

        /** */
        private int pageSize;

        /** */
        private boolean rmvDirty;

        /**
         * @param seg Segment.
         * @param clearPred Clear predicate.
         * @param doneFut Completion future.
         */
        private ClearSegmentRunnable(
            Segment seg,
            GridPredicate3<Integer, Long, Integer> clearPred,
            boolean rmvDirty,
            CountDownFuture doneFut,
            int pageSize
        ) {
            this.seg = seg;
            this.clearPred = clearPred;
            this.rmvDirty = rmvDirty;
            this.doneFut = doneFut;
            this.pageSize = pageSize;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            int cap = seg.loadedPages.capacity();

            int chunkSize = 1000;
            GridLongList ptrs = new GridLongList(chunkSize);

            try {
                for (int base = 0; base < cap; ) {
                    int boundary = Math.min(cap, base + chunkSize);

                    seg.writeLock().lock();

                    try {
                        while (base < boundary) {
                            long ptr = seg.loadedPages.clearAt(base, clearPred, INVALID_REL_PTR);

                            if (ptr != INVALID_REL_PTR)
                                ptrs.add(ptr);

                            base++;
                        }
                    }
                    finally {
                        seg.writeLock().unlock();
                    }

                    for (int i = 0; i < ptrs.size(); i++) {
                        long relPtr = ptrs.get(i);

                        long absPtr = seg.pool.absolute(relPtr);

                        if (rmvDirty) {
                            FullPageId fullId = PageHeader.fullPageId(absPtr);

                            seg.dirtyPages.remove(fullId);
                        }

                        GridUnsafe.setMemory(absPtr + PAGE_OVERHEAD, pageSize, (byte)0);

                        seg.pool.releaseFreePage(relPtr);
                    }

                    ptrs.clear();
                }

                doneFut.onDone((Void)null);
            }
            catch (Throwable e) {
                doneFut.onDone(e);
            }
        }
    }
}
