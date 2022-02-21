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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.PageReplacementMode;
import org.apache.ignite.events.EventType;
import org.apache.ignite.events.PageReplacementStartedEvent;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.InitNewPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PageDeltaRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointLockStateChecker;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.PageStoreWriter;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.TrackingPageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.apache.ignite.internal.processors.compress.CompressionProcessor;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.internal.util.lang.GridInClosure3X;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.spi.encryption.noop.NoopEncryptionSpi;
import org.apache.ignite.thread.IgniteThreadFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DELAYED_REPLACED_PAGE_WRITE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_LOADED_PAGES_BACKWARD_SHIFT_MAP;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.pagemem.FullPageId.NULL_PAGE;
import static org.apache.ignite.internal.processors.cache.persistence.pagemem.PagePool.SEGMENT_INDEX_MASK;
import static org.apache.ignite.internal.util.GridUnsafe.wrapPointer;

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
@SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
public class PageMemoryImpl implements PageMemoryEx {
    /** Full relative pointer mask. */
    public static final long RELATIVE_PTR_MASK = 0xFFFFFFFFFFFFFFL;

    /** Invalid relative pointer value. */
    static final long INVALID_REL_PTR = RELATIVE_PTR_MASK;

    /** Pointer which means that this page is outdated (for example, cache was destroyed, partition eviction'd happened */
    static final long OUTDATED_REL_PTR = INVALID_REL_PTR + 1;

    /** Page lock offset. */
    public static final int PAGE_LOCK_OFFSET = 32;

    /**
     * 8b Marker/timestamp
     * 8b Relative pointer
     * 8b Page ID
     * 4b Cache group ID
     * 4b Pin count
     * 8b Lock
     * 8b Temporary buffer
     */
    public static final int PAGE_OVERHEAD = 48;

    /** Try again tag. */
    public static final int TRY_AGAIN_TAG = -1;

    /** @see IgniteSystemProperties#IGNITE_DELAYED_REPLACED_PAGE_WRITE */
    public static final boolean DFLT_DELAYED_REPLACED_PAGE_WRITE = true;

    /** @see IgniteSystemProperties#IGNITE_LOADED_PAGES_BACKWARD_SHIFT_MAP */
    public static final boolean DFLT_LOADED_PAGES_BACKWARD_SHIFT_MAP = true;

    /** Tracking io. */
    private static final TrackingPageIO trackingIO = TrackingPageIO.VERSIONS.latest();

    /** Checkpoint pool overflow error message. */
    public static final String CHECKPOINT_POOL_OVERFLOW_ERROR_MSG = "Failed to allocate temporary buffer for checkpoint " +
        "(increase checkpointPageBufferSize configuration property)";

    /** Page size. */
    private final int sysPageSize;

    /** Encrypted page size. */
    private final int encPageSize;

    /** Shared context. */
    private final GridCacheSharedContext<?, ?> ctx;

    /** Checkpoint lock state provider. */
    private final CheckpointLockStateChecker stateChecker;

    /** Use new implementation of loaded pages table:  'Robin Hood hashing: backward shift deletion'. */
    private final boolean useBackwardShiftMap =
        IgniteSystemProperties.getBoolean(IGNITE_LOADED_PAGES_BACKWARD_SHIFT_MAP, DFLT_LOADED_PAGES_BACKWARD_SHIFT_MAP);

    /** Page replacement policy factory. */
    private final PageReplacementPolicyFactory pageReplacementPolicyFactory;

    /** */
    private final ExecutorService asyncRunner;

    /** Page manager. */
    private final PageReadWriteManager pmPageMgr;

    /** */
    private final IgniteWriteAheadLogManager walMgr;

    /** */
    private final GridEncryptionManager encMgr;

    /** */
    private final boolean encryptionDisabled;

    /** */
    private final IgniteLogger log;

    /** Direct memory allocator. */
    private final DirectMemoryProvider directMemoryProvider;

    /** Segments array. */
    private volatile Segment[] segments;

    /** @see #safeToUpdate() */
    private final AtomicBoolean safeToUpdate = new AtomicBoolean(true);

    /** Lock for segments changes. */
    private final Object segmentsLock = new Object();

    /** */
    private PagePool checkpointPool;

    /** */
    private final OffheapReadWriteLock rwLock;

    /** Flush dirty page closure. When possible, will be called by evictPage(). */
    private final PageStoreWriter flushDirtyPage;

    /**
     * Delayed page replacement (rotation with disk) tracker.
     * Because other thread may require exactly the same page to be loaded from store, reads are protected by locking.
     * {@code Null} if delayed write functionality is disabled.
     */
    @Nullable private final DelayedPageReplacementTracker delayedPageReplacementTracker;

    /**
     * Callback invoked to track changes in pages.
     * {@code Null} if page tracking functionality is disabled
     * */
    @Nullable private final GridInClosure3X<Long, FullPageId, PageMemoryEx> changeTracker;

    /** Pages write throttle. */
    private PagesWriteThrottlePolicy writeThrottle;

    /** Write throttle type. */
    private final ThrottlingPolicy throttlingPlc;

    /** Checkpoint progress provider. Null disables throttling. */
    @Nullable private final IgniteOutClosure<CheckpointProgress> cpProgressProvider;

    /** Field updater. */
    private static final AtomicIntegerFieldUpdater<PageMemoryImpl> pageReplacementWarnedFieldUpdater =
        AtomicIntegerFieldUpdater.newUpdater(PageMemoryImpl.class, "pageReplacementWarned");

    /** Flag indicating page replacement started (rotation with disk), allocating new page requires freeing old one. */
    private volatile int pageReplacementWarned;

    /** */
    private final long[] sizes;

    /** Memory metrics to track dirty pages count and page replace rate. */
    private final DataRegionMetricsImpl dataRegionMetrics;

    /**
     * {@code False} if memory was not started or already stopped and is not supposed for any usage.
     */
    private volatile boolean started;

    /**
     * @param directMemoryProvider Memory allocator to use.
     * @param sizes segments sizes, last is checkpoint pool size.
     * @param ctx Cache shared context.
     * @param pmPageMgr Page store manager.
     * @param pageSize Page size.
     * @param flushDirtyPage write callback invoked when a dirty page is removed for replacement.
     * @param changeTracker Callback invoked to track changes in pages.
     * @param stateChecker Checkpoint lock state provider. Used to ensure lock is held by thread, which modify pages.
     * @param dataRegionMetrics Memory metrics to track dirty pages count and page replace rate.
     * @param throttlingPlc Write throttle enabled and its type. Null equal to none.
     * @param cpProgressProvider checkpoint progress, base for throttling. Null disables throttling.
     */
    public PageMemoryImpl(
        DirectMemoryProvider directMemoryProvider,
        long[] sizes,
        GridCacheSharedContext<?, ?> ctx,
        PageReadWriteManager pmPageMgr,
        int pageSize,
        PageStoreWriter flushDirtyPage,
        @Nullable GridInClosure3X<Long, FullPageId, PageMemoryEx> changeTracker,
        CheckpointLockStateChecker stateChecker,
        DataRegionMetricsImpl dataRegionMetrics,
        @Nullable ThrottlingPolicy throttlingPlc,
        IgniteOutClosure<CheckpointProgress> cpProgressProvider
    ) {
        assert ctx != null;
        assert pageSize > 0;
        assert dataRegionMetrics != null;

        log = ctx.logger(PageMemoryImpl.class);

        this.ctx = ctx;
        this.directMemoryProvider = directMemoryProvider;
        this.sizes = sizes;
        this.flushDirtyPage = flushDirtyPage;
        delayedPageReplacementTracker =
            getBoolean(IGNITE_DELAYED_REPLACED_PAGE_WRITE, DFLT_DELAYED_REPLACED_PAGE_WRITE)
                ? new DelayedPageReplacementTracker(pageSize, flushDirtyPage, log, sizes.length - 1) :
                null;
        this.changeTracker = changeTracker;
        this.stateChecker = stateChecker;
        this.throttlingPlc = throttlingPlc != null ? throttlingPlc : ThrottlingPolicy.CHECKPOINT_BUFFER_ONLY;
        this.cpProgressProvider = cpProgressProvider;

        this.pmPageMgr = pmPageMgr;
        walMgr = ctx.wal();
        encMgr = ctx.kernalContext().encryption();
        encryptionDisabled = ctx.gridConfig().getEncryptionSpi() instanceof NoopEncryptionSpi;

        assert pmPageMgr != null;
        assert walMgr != null;
        assert encMgr != null;

        sysPageSize = pageSize + PAGE_OVERHEAD;

        encPageSize = CU.encryptedPageSize(pageSize, ctx.kernalContext().config().getEncryptionSpi());

        rwLock = new OffheapReadWriteLock(128);

        this.dataRegionMetrics = dataRegionMetrics;

        asyncRunner = new ThreadPoolExecutor(
            0,
            Runtime.getRuntime().availableProcessors(),
            30L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(Runtime.getRuntime().availableProcessors()),
            new IgniteThreadFactory(ctx.igniteInstanceName(), "page-mem-op"));

        DataRegionConfiguration memCfg = getDataRegionConfiguration();

        PageReplacementMode pageReplacementMode = memCfg == null ? DataRegionConfiguration.DFLT_PAGE_REPLACEMENT_MODE :
                memCfg.getPageReplacementMode();

        switch (pageReplacementMode) {
            case RANDOM_LRU:
                pageReplacementPolicyFactory = new RandomLruPageReplacementPolicyFactory();

                break;
            case SEGMENTED_LRU:
                pageReplacementPolicyFactory = new SegmentedLruPageReplacementPolicyFactory();

                break;
            case CLOCK:
                pageReplacementPolicyFactory = new ClockPageReplacementPolicyFactory();

                break;
            default:
                throw new IgniteException("Unexpected page replacement mode: " + pageReplacementMode);
        }
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        synchronized (segmentsLock) {
            if (started)
                return;

            started = true;

            directMemoryProvider.initialize(sizes);

            List<DirectMemoryRegion> regions = new ArrayList<>(sizes.length);

            while (true) {
                DirectMemoryRegion reg = directMemoryProvider.nextRegion();

                if (reg == null)
                    break;

                regions.add(reg);
            }

            int regs = regions.size();

            Segment[] segments = new Segment[regs - 1];

            DirectMemoryRegion cpReg = regions.get(regs - 1);

            checkpointPool = new PagePool(regs - 1, cpReg, sysPageSize, rwLock);

            long checkpointBuf = cpReg.size();

            long totalAllocated = 0;
            int pages = 0;
            long totalTblSize = 0;
            long totalReplSize = 0;

            for (int i = 0; i < regs - 1; i++) {
                assert i < segments.length;

                DirectMemoryRegion reg = regions.get(i);

                totalAllocated += reg.size();

                segments[i] = new Segment(i, regions.get(i), checkpointPool.pages() / segments.length, throttlingPlc);

                pages += segments[i].pages();
                totalTblSize += segments[i].tableSize();
                totalReplSize += segments[i].replacementSize();
            }

            initWriteThrottle();

            this.segments = segments;

            if (log.isInfoEnabled()) {
                log.info("Started page memory [memoryAllocated=" + U.readableSize(totalAllocated, false) +
                    ", pages=" + pages +
                    ", tableSize=" + U.readableSize(totalTblSize, false) +
                    ", replacementSize=" + U.readableSize(totalReplSize, false) +
                    ", checkpointBuffer=" + U.readableSize(checkpointBuf, false) +
                    ']');
            }
        }
    }

    /**
     * Resolves instance of {@link PagesWriteThrottlePolicy} according to chosen throttle policy.
     */
    private void initWriteThrottle() {
        if (throttlingPlc == ThrottlingPolicy.SPEED_BASED)
            writeThrottle = new PagesWriteSpeedBasedThrottle(this, cpProgressProvider, stateChecker, log);
        else if (throttlingPlc == ThrottlingPolicy.TARGET_RATIO_BASED)
            writeThrottle = new PagesWriteThrottle(this, cpProgressProvider, stateChecker, false, log);
        else if (throttlingPlc == ThrottlingPolicy.CHECKPOINT_BUFFER_ONLY)
            writeThrottle = new PagesWriteThrottle(this, null, stateChecker, true, log);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean deallocate) throws IgniteException {
        synchronized (segmentsLock) {
            if (!started)
                return;

            if (log.isDebugEnabled())
                log.debug("Stopping page memory.");

            U.shutdownNow(getClass(), asyncRunner, log);

            if (segments != null) {
                for (Segment seg : segments)
                    seg.close();
            }

            started = false;

            directMemoryProvider.shutdown(deallocate);
        }
    }

    /** {@inheritDoc} */
    @Override public void releasePage(int grpId, long pageId, long page) {
        assert started;

        Segment seg = segment(grpId, pageId);

        seg.readLock().lock();

        try {
            seg.releasePage(page);
        }
        finally {
            seg.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public long readLock(int grpId, long pageId, long page) {
        assert started;

        return readLock(page, pageId, false);
    }

    /** {@inheritDoc} */
    @Override public void readUnlock(int grpId, long pageId, long page) {
        assert started;

        readUnlockPage(page);
    }

    /** {@inheritDoc} */
    @Override public long writeLock(int grpId, long pageId, long page) {
        assert started;

        return writeLock(grpId, pageId, page, false);
    }

    /** {@inheritDoc} */
    @Override public long writeLock(int grpId, long pageId, long page, boolean restore) {
        assert started;

        return writeLockPage(page, new FullPageId(pageId, grpId), !restore);
    }

    /** {@inheritDoc} */
    @Override public long tryWriteLock(int grpId, long pageId, long page) {
        assert started;

        return tryWriteLockPage(page, new FullPageId(pageId, grpId), true);
    }

    /** {@inheritDoc} */
    @Override public void writeUnlock(int grpId, long pageId, long page, Boolean walPlc,
        boolean dirtyFlag) {
        assert started;

        writeUnlock(grpId, pageId, page, walPlc, dirtyFlag, false);
    }

    /** {@inheritDoc} */
    @Override public void writeUnlock(int grpId, long pageId, long page, Boolean walPlc,
        boolean dirtyFlag, boolean restore) {
        assert started;

        writeUnlockPage(page, new FullPageId(pageId, grpId), walPlc, dirtyFlag, restore);
    }

    /** {@inheritDoc} */
    @Override public boolean isDirty(int grpId, long pageId, long page) {
        assert started;

        return isDirty(page);
    }

    /** {@inheritDoc} */
    @Override public long allocatePage(int grpId, int partId, byte flags) throws IgniteCheckedException {
        assert flags != PageIdAllocator.FLAG_IDX && partId <= PageIdAllocator.MAX_PARTITION_ID ||
            flags == PageIdAllocator.FLAG_IDX && partId == PageIdAllocator.INDEX_PARTITION :
            "flags = " + flags + ", partId = " + partId;

        assert started;
        assert stateChecker.checkpointLockIsHeldByThread();

        if (isThrottlingEnabled())
            writeThrottle.onMarkDirty(false);

        long pageId = pmPageMgr.allocatePage(grpId, partId, flags);

        assert PageIdUtils.pageIndex(pageId) > 0; //it's crucial for tracking pages (zero page is super one)

        // We need to allocate page in memory for marking it dirty to save it in the next checkpoint.
        // Otherwise it is possible that on file will be empty page which will be saved at snapshot and read with error
        // because there is no crc inside them.
        Segment seg = segment(grpId, pageId);

        seg.writeLock().lock();

        boolean isTrackingPage = changeTracker != null &&
            PageIdUtils.pageIndex(trackingIO.trackingPageFor(pageId, realPageSize(grpId))) == PageIdUtils.pageIndex(pageId);

        if (isTrackingPage && PageIdUtils.flag(pageId) == PageIdAllocator.FLAG_AUX)
            pageId = PageIdUtils.pageId(PageIdUtils.partId(pageId), PageIdAllocator.FLAG_DATA, PageIdUtils.pageIndex(pageId));

        FullPageId fullId = new FullPageId(pageId, grpId);

        try {
            long relPtr = seg.loadedPages.get(
                grpId,
                PageIdUtils.effectivePageId(pageId),
                seg.partGeneration(grpId, partId),
                INVALID_REL_PTR,
                OUTDATED_REL_PTR
            );

            if (relPtr == OUTDATED_REL_PTR) {
                relPtr = seg.refreshOutdatedPage(grpId, pageId, false);

                seg.pageReplacementPolicy.onRemove(relPtr);
            }

            if (relPtr == INVALID_REL_PTR)
                relPtr = seg.borrowOrAllocateFreePage(pageId);

            if (relPtr == INVALID_REL_PTR)
                relPtr = seg.removePageForReplacement();

            long absPtr = seg.absolute(relPtr);

            GridUnsafe.setMemory(absPtr + PAGE_OVERHEAD, pageSize(), (byte)0);

            PageHeader.fullPageId(absPtr, fullId);
            PageHeader.writeTimestamp(absPtr, U.currentTimeMillis());
            rwLock.init(absPtr + PAGE_LOCK_OFFSET, PageIdUtils.tag(pageId));

            assert PageIO.getCrc(absPtr + PAGE_OVERHEAD) == 0; //TODO GG-11480

            assert !PageHeader.isAcquired(absPtr) :
                "Pin counter must be 0 for a new page [relPtr=" + U.hexLong(relPtr) +
                    ", absPtr=" + U.hexLong(absPtr) + ", pinCntr=" + PageHeader.pinCount(absPtr) + ']';

            setDirty(fullId, absPtr, true, true);

            if (isTrackingPage) {
                long pageAddr = absPtr + PAGE_OVERHEAD;

                // We are inside segment write lock, so no other thread can pin this tracking page yet.
                // We can modify page buffer directly.
                if (PageIO.getType(pageAddr) == 0) {
                    PageMetrics metrics = dataRegionMetrics.cacheGrpPageMetrics(grpId);

                    trackingIO.initNewPage(pageAddr, pageId, realPageSize(grpId), metrics);

                    if (!ctx.wal().disabled(fullId.groupId())) {
                        if (!ctx.wal().isAlwaysWriteFullPages())
                            ctx.wal().log(
                                new InitNewPageRecord(
                                    grpId,
                                    pageId,
                                    trackingIO.getType(),
                                    trackingIO.getVersion(), pageId
                                )
                            );
                        else {
                            ctx.wal().log(new PageSnapshot(fullId, absPtr + PAGE_OVERHEAD, pageSize(),
                                realPageSize(fullId.groupId())));
                        }
                    }
                }
            }

            seg.pageReplacementPolicy.onMiss(relPtr);

            seg.loadedPages.put(grpId, PageIdUtils.effectivePageId(pageId), relPtr, seg.partGeneration(grpId, partId));
        }
        catch (IgniteOutOfMemoryException oom) {
            DataRegionConfiguration dataRegionCfg = getDataRegionConfiguration();

            IgniteOutOfMemoryException e = new IgniteOutOfMemoryException("Out of memory in data region [" +
                "name=" + dataRegionCfg.getName() +
                ", initSize=" + U.readableSize(dataRegionCfg.getInitialSize(), false) +
                ", maxSize=" + U.readableSize(dataRegionCfg.getMaxSize(), false) +
                ", persistenceEnabled=" + dataRegionCfg.isPersistenceEnabled() + "] Try the following:" + U.nl() +
                "  ^-- Increase maximum off-heap memory size (DataRegionConfiguration.maxSize)" + U.nl() +
                "  ^-- Enable eviction or expiration policies"
            );

            e.initCause(oom);

            ctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
        finally {
            seg.writeLock().unlock();
        }

        // Finish replacement only when an exception wasn't thrown otherwise it possible to corrupt B+Tree.
        if (delayedPageReplacementTracker != null)
            delayedPageReplacementTracker.delayedPageWrite().finishReplacement();

        //we have allocated 'tracking' page, we need to allocate regular one
        return isTrackingPage ? allocatePage(grpId, partId, flags) : pageId;
    }

    /**
     * @return Data region configuration.
     */
    private DataRegionConfiguration getDataRegionConfiguration() {
        DataStorageConfiguration memCfg = ctx.kernalContext().config().getDataStorageConfiguration();

        assert memCfg != null;

        String dataRegionName = dataRegionMetrics.getName();

        if (memCfg.getDefaultDataRegionConfiguration().getName().equals(dataRegionName))
            return memCfg.getDefaultDataRegionConfiguration();

        DataRegionConfiguration[] dataRegions = memCfg.getDataRegionConfigurations();

        if (dataRegions != null) {
            for (DataRegionConfiguration reg : dataRegions) {
                if (reg != null && reg.getName().equals(dataRegionName))
                    return reg;
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer pageBuffer(long pageAddr) {
        return wrapPointer(pageAddr, pageSize());
    }

    /** {@inheritDoc} */
    @Override public boolean freePage(int grpId, long pageId) {
        assert false : "Free page should be never called directly when persistence is enabled.";

        return false;
    }

    /** {@inheritDoc} */
    @Override public long partitionMetaPageId(int grpId, int partId) {
        assert started;

        return PageIdUtils.pageId(partId, PageIdAllocator.FLAG_DATA, 0);
    }

    /** {@inheritDoc} */
    @Override public long acquirePage(int grpId, long pageId) throws IgniteCheckedException {
        return acquirePage(grpId, pageId, IoStatisticsHolderNoOp.INSTANCE, false);
    }

    /** {@inheritDoc} */
    @Override public long acquirePage(int grpId, long pageId,
        IoStatisticsHolder statHolder) throws IgniteCheckedException {
        assert started;

        return acquirePage(grpId, pageId, statHolder, false);
    }

    /** {@inheritDoc} */
    @Override public long acquirePage(int grpId, long pageId, AtomicBoolean pageAllocated) throws IgniteCheckedException {
        return acquirePage(grpId, pageId, IoStatisticsHolderNoOp.INSTANCE, false, pageAllocated);
    }

    /** {@inheritDoc} */
    @Override public long acquirePage(int grpId, long pageId, IoStatisticsHolder statHolder,
        boolean restore) throws IgniteCheckedException {
        return acquirePage(grpId, pageId, statHolder, restore, null);
    }

    /**
     * @param grpId Group id.
     * @param pageId Page id.
     * @param statHolder Stat holder.
     * @param restore Restore.
     * @param pageAllocated Page allocated.
     */
    private long acquirePage(int grpId, long pageId, IoStatisticsHolder statHolder,
        boolean restore, @Nullable AtomicBoolean pageAllocated) throws IgniteCheckedException {
        assert started;

        int partId = PageIdUtils.partId(pageId);

        Segment seg = segment(grpId, pageId);

        seg.readLock().lock();

        try {
            long relPtr = seg.loadedPages.get(
                grpId,
                PageIdUtils.effectivePageId(pageId),
                seg.partGeneration(grpId, partId),
                INVALID_REL_PTR,
                INVALID_REL_PTR
            );

            // The page is loaded to the memory.
            if (relPtr != INVALID_REL_PTR) {
                long absPtr = seg.absolute(relPtr);

                seg.acquirePage(absPtr);

                seg.pageReplacementPolicy.onHit(relPtr);

                statHolder.trackLogicalRead(absPtr + PAGE_OVERHEAD);

                return absPtr;
            }
        }
        finally {
            seg.readLock().unlock();
        }

        FullPageId fullId = new FullPageId(pageId, grpId);

        seg.writeLock().lock();

        long lockedPageAbsPtr = -1;
        boolean readPageFromStore = false;

        try {
            // Double-check.
            long relPtr = seg.loadedPages.get(
                grpId,
                fullId.effectivePageId(),
                seg.partGeneration(grpId, partId),
                INVALID_REL_PTR,
                OUTDATED_REL_PTR
            );

            long absPtr;

            if (relPtr == INVALID_REL_PTR) {
                relPtr = seg.borrowOrAllocateFreePage(pageId);

                if (pageAllocated != null)
                    pageAllocated.set(true);

                if (relPtr == INVALID_REL_PTR)
                    relPtr = seg.removePageForReplacement();

                absPtr = seg.absolute(relPtr);

                PageHeader.fullPageId(absPtr, fullId);
                PageHeader.writeTimestamp(absPtr, U.currentTimeMillis());

                assert !PageHeader.isAcquired(absPtr) :
                    "Pin counter must be 0 for a new page [relPtr=" + U.hexLong(relPtr) +
                        ", absPtr=" + U.hexLong(absPtr) + ']';

                // We can clear dirty flag after the page has been allocated.
                setDirty(fullId, absPtr, false, false);

                seg.pageReplacementPolicy.onMiss(relPtr);

                seg.loadedPages.put(
                    grpId,
                    fullId.effectivePageId(),
                    relPtr,
                    seg.partGeneration(grpId, partId)
                );

                long pageAddr = absPtr + PAGE_OVERHEAD;

                if (!restore) {
                    if (delayedPageReplacementTracker != null)
                        delayedPageReplacementTracker.waitUnlock(fullId);

                    readPageFromStore = true;
                }
                else {
                    GridUnsafe.setMemory(absPtr + PAGE_OVERHEAD, pageSize(), (byte)0);

                    // Must init page ID in order to ensure RWLock tag consistency.
                    PageIO.setPageId(pageAddr, pageId);
                }

                rwLock.init(absPtr + PAGE_LOCK_OFFSET, PageIdUtils.tag(pageId));

                if (readPageFromStore) {
                    boolean locked = rwLock.writeLock(absPtr + PAGE_LOCK_OFFSET, OffheapReadWriteLock.TAG_LOCK_ALWAYS);

                    assert locked : "Page ID " + fullId + " expected to be locked";

                    lockedPageAbsPtr = absPtr;
                }
            }
            else if (relPtr == OUTDATED_REL_PTR) {
                assert PageIdUtils.pageIndex(pageId) == 0 : fullId;

                relPtr = seg.refreshOutdatedPage(grpId, pageId, false);

                absPtr = seg.absolute(relPtr);

                long pageAddr = absPtr + PAGE_OVERHEAD;

                GridUnsafe.setMemory(pageAddr, pageSize(), (byte)0);

                PageHeader.fullPageId(absPtr, fullId);
                PageHeader.writeTimestamp(absPtr, U.currentTimeMillis());
                PageIO.setPageId(pageAddr, pageId);

                assert !PageHeader.isAcquired(absPtr) :
                    "Pin counter must be 0 for a new page [relPtr=" + U.hexLong(relPtr) +
                        ", absPtr=" + U.hexLong(absPtr) + ']';

                rwLock.init(absPtr + PAGE_LOCK_OFFSET, PageIdUtils.tag(pageId));

                seg.pageReplacementPolicy.onRemove(relPtr);
                seg.pageReplacementPolicy.onMiss(relPtr);
            }
            else {
                absPtr = seg.absolute(relPtr);

                seg.pageReplacementPolicy.onHit(relPtr);
            }

            seg.acquirePage(absPtr);

            if (!readPageFromStore)
                statHolder.trackLogicalRead(absPtr + PAGE_OVERHEAD);

            return absPtr;
        }
        catch (IgniteOutOfMemoryException oom) {
            ctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, oom));

            throw oom;
        }
        finally {
            seg.writeLock().unlock();

            if (delayedPageReplacementTracker != null)
                delayedPageReplacementTracker.delayedPageWrite().finishReplacement();

            if (readPageFromStore) {
                assert lockedPageAbsPtr != -1 : "Page is expected to have a valid address [pageId=" + fullId +
                    ", lockedPageAbsPtr=" + U.hexLong(lockedPageAbsPtr) + ']';

                assert isPageWriteLocked(lockedPageAbsPtr) : "Page is expected to be locked: [pageId=" + fullId + "]";

                long pageAddr = lockedPageAbsPtr + PAGE_OVERHEAD;

                ByteBuffer buf = wrapPointer(pageAddr, pageSize());

                long actualPageId = 0;

                try {
                    pmPageMgr.read(grpId, pageId, buf, false);

                    statHolder.trackPhysicalAndLogicalRead(pageAddr);

                    actualPageId = PageIO.getPageId(buf);

                    dataRegionMetrics.onPageRead();

                    if (PageIO.isIndexPage(PageIO.getType(buf)))
                        dataRegionMetrics.cacheGrpPageMetrics(grpId).indexPages().increment();
                }
                catch (IgniteDataIntegrityViolationException e) {
                    U.warn(log, "Failed to read page (data integrity violation encountered, will try to " +
                        "restore using existing WAL) [fullPageId=" + fullId + ']', e);

                    buf.rewind();

                    tryToRestorePage(fullId, buf);

                    // Mark the page as dirty because it has been restored.
                    setDirty(fullId, lockedPageAbsPtr, true, false);

                    // And save the page snapshot in the WAL.
                    beforeReleaseWrite(fullId, pageAddr, true);

                    statHolder.trackPhysicalAndLogicalRead(pageAddr);

                    dataRegionMetrics.onPageRead();
                }
                finally {
                    rwLock.writeUnlock(lockedPageAbsPtr + PAGE_LOCK_OFFSET,
                        actualPageId == 0 ? OffheapReadWriteLock.TAG_LOCK_ALWAYS : PageIdUtils.tag(actualPageId));
                }
            }
        }
    }

    /** */
    private void releaseCheckpointBufferPage(long tmpBufPtr) {
        int resCntr = checkpointPool.releaseFreePage(tmpBufPtr);

        if (resCntr == checkpointBufferPagesSize() / 2 && writeThrottle != null)
            writeThrottle.tryWakeupThrottledThreads();
    }

    /**
     * Restores page from WAL page snapshot & delta records.
     *
     * @param fullId Full page ID.
     * @param buf Destination byte buffer. Note: synchronization to provide ByteBuffer safety should be done outside
     * this method.
     *
     * @throws IgniteCheckedException If failed to start WAL iteration, if incorrect page type observed in data, etc.
     * @throws StorageException If it was not possible to restore page, page not found in WAL.
     */
    private void tryToRestorePage(FullPageId fullId, ByteBuffer buf) throws IgniteCheckedException {
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
                                    assert snapshot.pageDataSize() <= pageSize() : snapshot.pageDataSize();

                                    tmpAddr = GridUnsafe.allocateMemory(pageSize());
                                }

                                if (curPage == null)
                                    curPage = wrapPointer(tmpAddr, pageSize());

                                PageUtils.putBytes(tmpAddr, 0, snapshot.pageData());

                                if (PageIO.getCompressionType(tmpAddr) != CompressionProcessor.UNCOMPRESSED_PAGE) {
                                    int realPageSize = realPageSize(snapshot.groupId());

                                    assert snapshot.pageDataSize() < realPageSize : snapshot.pageDataSize();

                                    ctx.kernalContext().compress().decompressPage(curPage, realPageSize);
                                }
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
                throw new StorageException(String.format(
                    "Page is broken. Can't restore it from WAL. (grpId = %d, pageId = %X).",
                    fullId.groupId(), fullId.pageId()
                ));

            buf.put(restored);
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
    @Override public int realPageSize(int grpId) {
        if (encryptionDisabled || encMgr.getActiveKey(grpId) == null)
            return pageSize();

        return encPageSize;
    }

    /** {@inheritDoc} */
    @Override public boolean safeToUpdate() {
        if (segments != null)
            return safeToUpdate.get();

        return true;
    }

    /**
     * @param dirtyRatioThreshold Throttle threshold.
     */
    boolean shouldThrottle(double dirtyRatioThreshold) {
        if (segments == null)
            return false;

        for (Segment segment : segments) {
            if (segment.shouldThrottle(dirtyRatioThreshold))
                return true;
        }

        return false;
    }

    /**
     * @return Max dirty ratio from the segments.
     */
    double getDirtyPagesRatio() {
        if (segments == null)
            return 0;

        double res = 0;

        for (Segment segment : segments)
            res = Math.max(res, segment.getDirtyPagesRatio());

        return res;
    }

    /**
     * @return Total pages can be placed in all segments.
     */
    @Override public long totalPages() {
        if (segments == null)
            return 0;

        long res = 0;

        for (Segment segment : segments)
            res += segment.pages();

        return res;
    }

    /** {@inheritDoc} */
    @Override public GridMultiCollectionWrapper<FullPageId> beginCheckpoint(
        IgniteInternalFuture allowToReplace
    ) throws IgniteException {
        if (segments == null)
            return new GridMultiCollectionWrapper<>(Collections.emptyList());

        Collection[] collections = new Collection[segments.length];

        for (int i = 0; i < segments.length; i++) {
            Segment seg = segments[i];

            if (seg.checkpointPages != null)
                throw new IgniteException("Failed to begin checkpoint (it is already in progress).");

            Collection<FullPageId> dirtyPages = seg.dirtyPages;
            collections[i] = dirtyPages;

            seg.checkpointPages = new CheckpointPages(dirtyPages, allowToReplace);

            seg.resetDirtyPages();
        }

        safeToUpdate.set(true);

        dataRegionMetrics.resetDirtyPages();

        if (throttlingPlc != ThrottlingPolicy.DISABLED)
            writeThrottle.onBeginCheckpoint();

        return new GridMultiCollectionWrapper<>(collections);
    }

    /**
     * @return {@code True} if throttling is enabled.
     */
    private boolean isThrottlingEnabled() {
        return throttlingPlc != ThrottlingPolicy.CHECKPOINT_BUFFER_ONLY && throttlingPlc != ThrottlingPolicy.DISABLED;
    }

    /** {@inheritDoc} */
    @Override public void finishCheckpoint() {
        if (segments == null)
            return;

        synchronized (segmentsLock) {
            for (Segment seg : segments)
                seg.checkpointPages = null;
        }

        if (throttlingPlc != ThrottlingPolicy.DISABLED)
            writeThrottle.onFinishCheckpoint();
    }

    /** {@inheritDoc} */
    @Override public PageReadWriteManager pageManager() {
        return pmPageMgr;
    }

    /** {@inheritDoc} */
    @Override public void checkpointWritePage(
        FullPageId fullId,
        ByteBuffer buf,
        PageStoreWriter pageStoreWriter,
        CheckpointMetricsTracker metricsTracker
    ) throws IgniteCheckedException {
        assert buf.remaining() == pageSize();

        Segment seg = segment(fullId.groupId(), fullId.pageId());

        long absPtr = 0;

        long relPtr;

        int tag;

        boolean pageSingleAcquire = false;

        seg.readLock().lock();

        try {
            if (!isInCheckpoint(fullId))
                return;

            relPtr = resolveRelativePointer(seg, fullId, tag = generationTag(seg, fullId));

            // Page may have been cleared during eviction. We have nothing to do in this case.
            if (relPtr == INVALID_REL_PTR)
                return;

            if (relPtr != OUTDATED_REL_PTR) {
                absPtr = seg.absolute(relPtr);

                // Pin the page until page will not be copied. This helpful to prevent page replacement of this page.
                if (PageHeader.tempBufferPointer(absPtr) == INVALID_REL_PTR)
                    PageHeader.acquirePage(absPtr);
                else
                    pageSingleAcquire = true;
            }
        }
        finally {
            seg.readLock().unlock();
        }

        if (relPtr == OUTDATED_REL_PTR) {
            seg.writeLock().lock();

            try {
                // Double-check.
                relPtr = resolveRelativePointer(seg, fullId, generationTag(seg, fullId));

                if (relPtr == INVALID_REL_PTR)
                    return;

                if (relPtr == OUTDATED_REL_PTR) {
                    relPtr = seg.refreshOutdatedPage(
                        fullId.groupId(),
                        fullId.effectivePageId(),
                        true
                    );

                    seg.pageReplacementPolicy.onRemove(relPtr);

                    seg.pool.releaseFreePage(relPtr);
                }

                return;
            }
            finally {
                seg.writeLock().unlock();
            }
        }

        copyPageForCheckpoint(absPtr, fullId, buf, tag, pageSingleAcquire, pageStoreWriter, metricsTracker);
    }

    /**
     * @param absPtr Absolute ptr.
     * @param fullId Full id.
     * @param buf Buffer for copy page content for future write via {@link PageStoreWriter}.
     * @param pageSingleAcquire Page is acquired only once. We don't pin the page second time (until page will not be
     * copied) in case checkpoint temporary buffer is used.
     * @param pageStoreWriter Checkpoint page write context.
     */
    private void copyPageForCheckpoint(
        long absPtr,
        FullPageId fullId,
        ByteBuffer buf,
        Integer tag,
        boolean pageSingleAcquire,
        PageStoreWriter pageStoreWriter,
        CheckpointMetricsTracker tracker
    ) throws IgniteCheckedException {
        assert absPtr != 0;
        assert PageHeader.isAcquired(absPtr) || !isInCheckpoint(fullId);

        // Exception protection flag.
        // No need to write if exception occurred.
        boolean canWrite = false;

        boolean locked = rwLock.tryWriteLock(absPtr + PAGE_LOCK_OFFSET, OffheapReadWriteLock.TAG_LOCK_ALWAYS);

        if (!locked) {
            // We release the page only once here because this page will be copied sometime later and
            // will be released properly then.
            if (!pageSingleAcquire)
                PageHeader.releasePage(absPtr);

            buf.clear();

            if (isInCheckpoint(fullId))
                pageStoreWriter.writePage(fullId, buf, TRY_AGAIN_TAG);

            return;
        }

        if (!clearCheckpoint(fullId)) {
            rwLock.writeUnlock(absPtr + PAGE_LOCK_OFFSET, OffheapReadWriteLock.TAG_LOCK_ALWAYS);

            if (!pageSingleAcquire)
                PageHeader.releasePage(absPtr);

            return;
        }

        try {
            long tmpRelPtr = PageHeader.tempBufferPointer(absPtr);

            if (tmpRelPtr != INVALID_REL_PTR) {
                PageHeader.tempBufferPointer(absPtr, INVALID_REL_PTR);

                long tmpAbsPtr = checkpointPool.absolute(tmpRelPtr);

                copyInBuffer(tmpAbsPtr, buf);

                PageHeader.fullPageId(tmpAbsPtr, NULL_PAGE);

                GridUnsafe.setMemory(tmpAbsPtr + PAGE_OVERHEAD, pageSize(), (byte)0);

                if (tracker != null)
                    tracker.onCowPageWritten();

                releaseCheckpointBufferPage(tmpRelPtr);

                // Need release again because we pin page when resolve abs pointer,
                // and page did not have tmp buffer page.
                if (!pageSingleAcquire)
                    PageHeader.releasePage(absPtr);
            }
            else {
                copyInBuffer(absPtr, buf);

                PageHeader.dirty(absPtr, false);
            }

            assert PageIO.getType(buf) != 0 : "Invalid state. Type is 0! pageId = " + U.hexLong(fullId.pageId());
            assert PageIO.getVersion(buf) != 0 : "Invalid state. Version is 0! pageId = " + U.hexLong(fullId.pageId());

            canWrite = true;
        }
        finally {
            rwLock.writeUnlock(absPtr + PAGE_LOCK_OFFSET, OffheapReadWriteLock.TAG_LOCK_ALWAYS);

            if (canWrite) {
                buf.rewind();

                pageStoreWriter.writePage(fullId, buf, tag);

                dataRegionMetrics.onPageWritten();

                buf.rewind();
            }

            // We pinned the page either when allocated the temp buffer, or when resolved abs pointer.
            // Must release the page only after write unlock.
            PageHeader.releasePage(absPtr);
        }
    }

    /**
     * @param absPtr Absolute ptr.
     * @param buf Tmp buffer.
     */
    private void copyInBuffer(long absPtr, ByteBuffer buf) {
        if (buf.isDirect()) {
            long tmpPtr = GridUnsafe.bufferAddress(buf);

            GridUnsafe.copyMemory(absPtr + PAGE_OVERHEAD, tmpPtr, pageSize());

            assert PageIO.getCrc(absPtr + PAGE_OVERHEAD) == 0; //TODO GG-11480
            assert PageIO.getCrc(tmpPtr) == 0; //TODO GG-11480
        }
        else {
            byte[] arr = buf.array();

            assert arr != null;
            assert arr.length == pageSize();

            GridUnsafe.copyMemory(null, absPtr + PAGE_OVERHEAD, arr, GridUnsafe.BYTE_ARR_OFF, pageSize());
        }
    }

    /**
     * Get current prartition generation tag.
     *
     * @param seg Segment.
     * @param fullId Full page id.
     * @return Current partition generation tag.
     */
    private int generationTag(Segment seg, FullPageId fullId) {
        return seg.partGeneration(
            fullId.groupId(),
            PageIdUtils.partId(fullId.pageId())
        );
    }

    /**
     * Resolver relative pointer via {@link LoadedPagesMap}.
     *
     * @param seg Segment.
     * @param fullId Full page id.
     * @param reqVer Required version.
     * @return Relative pointer.
     */
    private long resolveRelativePointer(Segment seg, FullPageId fullId, int reqVer) {
        return seg.loadedPages.get(
            fullId.groupId(),
            fullId.effectivePageId(),
            reqVer,
            INVALID_REL_PTR,
            OUTDATED_REL_PTR
        );
    }

    /** {@inheritDoc} */
    @Override public int invalidate(int grpId, int partId) {
        synchronized (segmentsLock) {
            if (!started)
                return 0;

            int tag = 0;

            for (Segment seg : segments) {
                seg.writeLock().lock();

                try {
                    int newTag = seg.incrementPartGeneration(grpId, partId);

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
    }

    /** {@inheritDoc} */
    @Override public void onCacheGroupDestroyed(int grpId) {
        for (Segment seg : segments) {
            seg.writeLock().lock();

            try {
                seg.resetGroupPartitionsGeneration(grpId);
            }
            finally {
                seg.writeLock().unlock();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Void> clearAsync(
        LoadedPagesMap.KeyPredicate pred,
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
        if (segments == null)
            return 0L;

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
     * @param fullPageId Full page ID to check.
     * @return {@code true} if the page is contained in the loaded pages table, {@code false} otherwise.
     */
    public boolean hasLoadedPage(FullPageId fullPageId) {
        int grpId = fullPageId.groupId();
        long pageId = fullPageId.effectivePageId();
        int partId = PageIdUtils.partId(pageId);

        Segment seg = segment(grpId, pageId);

        seg.readLock().lock();

        try {
            long res =
                seg.loadedPages.get(grpId, pageId, seg.partGeneration(grpId, partId), INVALID_REL_PTR, INVALID_REL_PTR);

            return res != INVALID_REL_PTR;
        }
        finally {
            seg.readLock().unlock();
        }
    }

    /**
     * @param absPtr Absolute pointer to read lock.
     * @param pageId Page ID.
     * @param force Force flag.
     * @return Pointer to the page read buffer.
     */
    private long readLock(long absPtr, long pageId, boolean force) {
        return readLock(absPtr, pageId, force, true);
    }

    /** {@inheritDoc} */
    @Override public long readLock(long absPtr, long pageId, boolean force, boolean touch) {
        assert started;

        int tag = force ? -1 : PageIdUtils.tag(pageId);

        boolean locked = rwLock.readLock(absPtr + PAGE_LOCK_OFFSET, tag);

        if (!locked)
            return 0;

        if (touch)
            PageHeader.writeTimestamp(absPtr, U.currentTimeMillis());

        assert PageIO.getCrc(absPtr + PAGE_OVERHEAD) == 0; //TODO GG-11480

        return absPtr + PAGE_OVERHEAD;
    }

    /** {@inheritDoc} */
    @Override public long readLockForce(int grpId, long pageId, long page) {
        assert started;

        return readLock(page, pageId, true);
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
            long tmpRelPtr = checkpointPool.borrowOrAllocateFreePage(PageIdUtils.tag(fullId.pageId()));

            if (tmpRelPtr == INVALID_REL_PTR) {
                rwLock.writeUnlock(absPtr + PAGE_LOCK_OFFSET, OffheapReadWriteLock.TAG_LOCK_ALWAYS);

                throw new IgniteException(CHECKPOINT_POOL_OVERFLOW_ERROR_MSG + ": " + dataRegionMetrics.getName());
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

            assert PageIO.getType(tmpAbsPtr + PAGE_OVERHEAD) != 0 : "Invalid state. Type is 0! pageId = " + U.hexLong(fullId.pageId());
            assert PageIO.getVersion(tmpAbsPtr + PAGE_OVERHEAD) != 0
                : "Invalid state. Version is 0! pageId = " + U.hexLong(fullId.pageId());

            PageHeader.dirty(absPtr, false);
            PageHeader.tempBufferPointer(absPtr, tmpRelPtr);
            // info for checkpoint buffer cleaner.
            PageHeader.fullPageId(tmpAbsPtr, fullId);

            assert PageIO.getCrc(absPtr + PAGE_OVERHEAD) == 0; //TODO GG-11480
            assert PageIO.getCrc(tmpAbsPtr + PAGE_OVERHEAD) == 0; //TODO GG-11480
        }

        assert PageIO.getCrc(absPtr + PAGE_OVERHEAD) == 0; //TODO GG-11480

        return absPtr + PAGE_OVERHEAD;
    }

    /**
     * @param page Page pointer.
     * @param fullId full page ID.
     * @param walPlc Full page WAL record policy
     * @param markDirty set dirty flag to page
     * @param restore restore flag
     */
    private void writeUnlockPage(
        long page,
        FullPageId fullId,
        Boolean walPlc,
        boolean markDirty,
        boolean restore
    ) {
        boolean wasDirty = isDirty(page);

        try {
            //if page is for restore, we shouldn't mark it as changed
            if (!restore && markDirty && !wasDirty && changeTracker != null)
                changeTracker.apply(page, fullId, this);

            boolean pageWalRec = markDirty && walPlc != FALSE && (walPlc == TRUE || !wasDirty);

            assert PageIO.getCrc(page + PAGE_OVERHEAD) == 0; //TODO GG-11480

            if (markDirty)
                setDirty(fullId, page, true, false);

            beforeReleaseWrite(fullId, page + PAGE_OVERHEAD, pageWalRec);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
        // Always release the lock.
        finally {
            long pageId = PageIO.getPageId(page + PAGE_OVERHEAD);

            try {
                assert pageId != 0 : U.hexLong(PageHeader.readPageId(page));

                rwLock.writeUnlock(page + PAGE_LOCK_OFFSET, PageIdUtils.tag(pageId));

                assert PageIO.getVersion(page + PAGE_OVERHEAD) != 0 : dumpPage(pageId, fullId.groupId());
                assert PageIO.getType(page + PAGE_OVERHEAD) != 0 : U.hexLong(pageId);

                if (throttlingPlc != ThrottlingPolicy.DISABLED && !restore && markDirty && !wasDirty)
                    writeThrottle.onMarkDirty(isInCheckpoint(fullId));
            }
            catch (AssertionError ex) {
                U.error(log, "Failed to unlock page [fullPageId=" + fullId +
                    ", binPage=" + U.toHexString(page, systemPageSize()) + ']');

                throw ex;
            }
        }
    }

    /**
     * Prepares page details for assertion.
     * @param pageId Page id.
     * @param grpId Group id.
     */
    @NotNull private String dumpPage(long pageId, int grpId) {
        int pageIdx = PageIdUtils.pageIndex(pageId);
        int partId = PageIdUtils.partId(pageId);
        long off = (long)(pageIdx + 1) * pageSize();

        return U.hexLong(pageId) + " (grpId=" + grpId + ", pageIdx=" + pageIdx + ", partId=" + partId + ", offH=" +
            Long.toHexString(off) + ")";
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
     * @param pageId Page ID to check if it was added to the checkpoint list.
     * @return {@code True} if it was added to the checkpoint list.
     */
    boolean isInCheckpoint(FullPageId pageId) {
        Segment seg = segment(pageId.groupId(), pageId.pageId());

        CheckpointPages pages0 = seg.checkpointPages;

        return pages0 != null && pages0.contains(pageId);
    }

    /**
     * @param fullPageId Page ID to clear.
     * @return {@code True} if remove successfully.
     */
    boolean clearCheckpoint(FullPageId fullPageId) {
        Segment seg = segment(fullPageId.groupId(), fullPageId.pageId());

        CheckpointPages pages0 = seg.checkpointPages;

        assert pages0 != null;

        return pages0.markAsSaved(fullPageId);
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
        if (segments == null)
            return 0;

        int total = 0;

        for (Segment seg : segments)
            total += seg.acquiredPages();

        return total;
    }

    /** {@inheritDoc} */
    @Override public int checkpointBufferPagesCount() {
        return checkpointPool == null ? 0 : checkpointPool.size();
    }

    /**
     * Number of used pages in checkpoint buffer.
     */
    public int checkpointBufferPagesSize() {
        return checkpointPool == null ? 0 : checkpointPool.pages();
    }

    /**
     * This method must be called in synchronized context.
     *
     * @param pageId full page ID.
     * @param absPtr Absolute pointer.
     * @param dirty {@code True} dirty flag.
     * @param forceAdd If this flag is {@code true}, then the page will be added to the dirty set regardless whether the
     * old flag was dirty or not.
     */
    private void setDirty(FullPageId pageId, long absPtr, boolean dirty, boolean forceAdd) {
        boolean wasDirty = PageHeader.dirty(absPtr, dirty);

        if (dirty) {
            assert stateChecker.checkpointLockIsHeldByThread();

            if (!wasDirty || forceAdd) {
                Segment seg = segment(pageId.groupId(), pageId.pageId());

                if (seg.dirtyPages.add(pageId)) {
                    long dirtyPagesCnt = seg.dirtyPagesCntr.incrementAndGet();

                    if (dirtyPagesCnt >= seg.maxDirtyPages)
                        safeToUpdate.set(false);

                    dataRegionMetrics.incrementDirtyPages();
                }
            }
        }
        else {
            Segment seg = segment(pageId.groupId(), pageId.pageId());

            if (seg.dirtyPages.remove(pageId)) {
                seg.dirtyPagesCntr.decrementAndGet();

                dataRegionMetrics.decrementDirtyPages();
            }
        }
    }

    /**
     *
     */
    void beforeReleaseWrite(FullPageId pageId, long ptr, boolean pageWalRec) throws IgniteCheckedException {
        boolean walIsNotDisabled = walMgr != null && !walMgr.disabled(pageId.groupId());
        boolean pageRecOrAlwaysWriteFullPage = walMgr != null && (pageWalRec || walMgr.isAlwaysWriteFullPages());

        if (pageRecOrAlwaysWriteFullPage && walIsNotDisabled)
            walMgr.log(new PageSnapshot(pageId, ptr, pageSize(), realPageSize(pageId.groupId())));
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

    /** {@inheritDoc} */
    @Override public boolean shouldThrottle() {
        return writeThrottle.shouldThrottle();
    }

    /** {@inheritDoc} */
    @Override public DataRegionMetricsImpl metrics() {
        return dataRegionMetrics;
    }

    /**
     * Get arbitrary page from cp buffer.
     */
    @Override public FullPageId pullPageFromCpBuffer() {
        long idx = GridUnsafe.getLong(checkpointPool.lastAllocatedIdxPtr);

        long lastIdx = ThreadLocalRandom.current().nextLong(idx / 2, idx);

        while (--lastIdx > 1) {
            assert (lastIdx & SEGMENT_INDEX_MASK) == 0L;

            long relative = checkpointPool.relative(lastIdx);

            long freePageAbsPtr = checkpointPool.absolute(relative);

            FullPageId pageToReplace = PageHeader.fullPageId(freePageAbsPtr);

            if (pageToReplace.pageId() == NULL_PAGE.pageId() || pageToReplace.groupId() == NULL_PAGE.groupId())
                continue;

            if (!isInCheckpoint(pageToReplace))
                continue;

            return pageToReplace;
        }

        return NULL_PAGE;
    }

    /**
     * Gets a collection of all pages currently marked as dirty. Will create a collection copy.
     *
     * @return Collection of all page IDs marked as dirty.
     */
    @TestOnly
    public Collection<FullPageId> dirtyPages() {
        if (segments == null)
            return Collections.emptySet();

        Collection<FullPageId> res = new HashSet<>((int)loadedPages());

        for (Segment seg : segments)
            res.addAll(seg.dirtyPages);

        return res;
    }

    /**
     *
     */
    class Segment extends ReentrantReadWriteLock {
        /** */
        private static final long serialVersionUID = 0L;

        /** Pointer to acquired pages integer counter. */
        private static final int ACQUIRED_PAGES_SIZEOF = 4;

        /** Padding to read from word beginning. */
        private static final int ACQUIRED_PAGES_PADDING = 4;

        /** Page ID to relative pointer map. */
        private final LoadedPagesMap loadedPages;

        /** Pointer to acquired pages integer counter. */
        private final long acquiredPagesPtr;

        /** */
        private final PagePool pool;

        /** */
        private final PageReplacementPolicy pageReplacementPolicy;

        /** Bytes required to store {@link #loadedPages}. */
        private final long memPerTbl;

        /** Bytes required to store {@link #pageReplacementPolicy} service data. */
        private long memPerRepl;

        /** Pages marked as dirty since the last checkpoint. */
        private volatile Collection<FullPageId> dirtyPages = new GridConcurrentHashSet<>();

        /** Atomic size counter for {@link #dirtyPages}. Used for {@link PageMemoryImpl#safeToUpdate()} calculation. */
        private final AtomicLong dirtyPagesCntr = new AtomicLong();

        /** Wrapper of pages of current checkpoint. */
        private volatile CheckpointPages checkpointPages;

        /** */
        private final long maxDirtyPages;

        /** Initial partition generation. */
        private static final int INIT_PART_GENERATION = 1;

        /** Maps partition (grpId, partId) to its generation. Generation is 1-based incrementing partition counter. */
        private final Map<GroupPartitionId, Integer> partGenerationMap = new HashMap<>();

        /** */
        private boolean closed;

        /**
         * @param region Memory region.
         * @param throttlingPlc policy determine if write throttling enabled and its type.
         */
        private Segment(int idx, DirectMemoryRegion region, int cpPoolPages, ThrottlingPolicy throttlingPlc) {
            long totalMemory = region.size();

            int pages = (int)(totalMemory / sysPageSize);

            acquiredPagesPtr = region.address();

            GridUnsafe.putIntVolatile(null, acquiredPagesPtr, 0);

            int ldPagesMapOffInRegion = ACQUIRED_PAGES_SIZEOF + ACQUIRED_PAGES_PADDING;

            long ldPagesAddr = region.address() + ldPagesMapOffInRegion;

            memPerTbl = useBackwardShiftMap
                ? RobinHoodBackwardShiftHashMap.requiredMemory(pages)
                : FullPageIdTable.requiredMemory(pages);

            loadedPages = useBackwardShiftMap
                ? new RobinHoodBackwardShiftHashMap(ldPagesAddr, memPerTbl)
                : new FullPageIdTable(ldPagesAddr, memPerTbl, true);

            pages = (int)((totalMemory - memPerTbl - ldPagesMapOffInRegion) / sysPageSize);

            memPerRepl = pageReplacementPolicyFactory.requiredMemory(pages);

            DirectMemoryRegion poolRegion = region.slice(memPerTbl + memPerRepl + ldPagesMapOffInRegion);

            pool = new PagePool(idx, poolRegion, sysPageSize, rwLock);

            pageReplacementPolicy = pageReplacementPolicyFactory.create(this,
                    region.address() + memPerTbl + ldPagesMapOffInRegion, pool.pages());

            maxDirtyPages = throttlingPlc != ThrottlingPolicy.DISABLED
                ? pool.pages() * 3L / 4
                : Math.min(pool.pages() * 2L / 3, cpPoolPages);
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
         * @param dirtyRatioThreshold Throttle threshold.
         */
        private boolean shouldThrottle(double dirtyRatioThreshold) {
            return getDirtyPagesRatio() > dirtyRatioThreshold;
        }

        /**
         * @return dirtyRatio to be compared with Throttle threshold.
         */
        private double getDirtyPagesRatio() {
            return dirtyPagesCntr.doubleValue() / pages();
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
         * @return Memory allocated for page replacement service data.
         */
        private long replacementSize() {
            return memPerRepl;
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
            return pool.borrowOrAllocateFreePage(PageIdUtils.tag(pageId));
        }

        /**
         * Clear dirty pages collection and reset counter.
         */
        private void resetDirtyPages() {
            dirtyPages = new GridConcurrentHashSet<>();

            dirtyPagesCntr.set(0);
        }

        /**
         * Prepares a page removal for page replacement, if needed.
         *
         * @param fullPageId Candidate page full ID.
         * @param absPtr Absolute pointer of the page to evict.
         * @return {@code True} if it is ok to replace this page, {@code false} if another page should be selected.
         * @throws IgniteCheckedException If failed to write page to the underlying store during eviction.
         */
        public boolean tryToRemovePage(FullPageId fullPageId, long absPtr) throws IgniteCheckedException {
            assert writeLock().isHeldByCurrentThread();

            // Do not evict cache meta pages.
            if (fullPageId.pageId() == META_PAGE_ID)
                return false;

            if (PageHeader.isAcquired(absPtr))
                return false;

            clearRowCache(fullPageId, absPtr);

            if (isDirty(absPtr)) {
                CheckpointPages checkpointPages = this.checkpointPages;
                // Can evict a dirty page only if it has been written by a checkpoint.
                // These pages do not have a tmp buffer.
                if (checkpointPages == null || !checkpointPages.allowToSave(fullPageId))
                    return false;

                assert pmPageMgr != null;

                PageStoreWriter saveDirtyPage = delayedPageReplacementTracker != null
                    ? delayedPageReplacementTracker.delayedPageWrite() : flushDirtyPage;

                saveDirtyPage.writePage(
                    fullPageId,
                    wrapPointer(absPtr + PAGE_OVERHEAD, pageSize()),
                    partGeneration(
                        fullPageId.groupId(),
                        PageIdUtils.partId(fullPageId.pageId())
                    )
                );

                setDirty(fullPageId, absPtr, false, true);

                checkpointPages.markAsSaved(fullPageId);
            }

            dataRegionMetrics.updatePageReplaceRate(U.currentTimeMillis() - PageHeader.readTimestamp(absPtr));

            loadedPages.remove(fullPageId.groupId(), fullPageId.effectivePageId());

            if (PageIO.isIndexPage(PageIO.getType(absPtr + PAGE_OVERHEAD))) {
                int grpId = fullPageId.groupId();
                dataRegionMetrics.cacheGrpPageMetrics(grpId).indexPages().decrement();
            }

            return true;
        }

        /**
         * @param fullPageId Full page ID to remove all links placed on the page from row cache.
         * @param absPtr Absolute pointer of the page to evict.
         * @throws IgniteCheckedException On error.
         */
        private void clearRowCache(FullPageId fullPageId, long absPtr) throws IgniteCheckedException {
            assert writeLock().isHeldByCurrentThread();

            if (ctx.kernalContext().query() == null || !ctx.kernalContext().query().moduleEnabled())
                return;

            long pageAddr = PageMemoryImpl.this.readLock(absPtr, fullPageId.pageId(), true, false);

            try {
                if (PageIO.getType(pageAddr) != PageIO.T_DATA)
                    return;

                GridQueryRowCacheCleaner cleaner = ctx.kernalContext().indexProcessor()
                    .rowCacheCleaner(fullPageId.groupId());

                if (cleaner == null)
                    return;

                DataPageIO io = DataPageIO.VERSIONS.forPage(pageAddr);

                io.forAllItems(pageAddr, new DataPageIO.CC<Void>() {
                    @Override public Void apply(long link) {
                        cleaner.remove(link);

                        return null;
                    }
                });
            }
            finally {
                readUnlockPage(absPtr);
            }
        }

        /**
         * @param grpId Cache group ID.
         * @param pageId Page ID.
         * @param rmv {@code True} if page should be removed.
         * @return Relative pointer to refreshed page.
         */
        public long refreshOutdatedPage(int grpId, long pageId, boolean rmv) {
            assert writeLock().isHeldByCurrentThread();

            int tag = partGeneration(grpId, PageIdUtils.partId(pageId));

            long relPtr = loadedPages.refresh(grpId, PageIdUtils.effectivePageId(pageId), tag);

            long absPtr = absolute(relPtr);

            GridUnsafe.setMemory(absPtr + PAGE_OVERHEAD, pageSize(), (byte)0);

            PageHeader.dirty(absPtr, false);

            long tmpBufPtr = PageHeader.tempBufferPointer(absPtr);

            if (tmpBufPtr != INVALID_REL_PTR) {
                GridUnsafe.setMemory(checkpointPool.absolute(tmpBufPtr) + PAGE_OVERHEAD, pageSize(), (byte)0);

                PageHeader.tempBufferPointer(absPtr, INVALID_REL_PTR);

                // We pinned the page when allocated the temp buffer, release it now.
                PageHeader.releasePage(absPtr);

                releaseCheckpointBufferPage(tmpBufPtr);
            }

            if (rmv)
                loadedPages.remove(grpId, PageIdUtils.effectivePageId(pageId));

            CheckpointPages cpPages = checkpointPages;

            if (cpPages != null)
                cpPages.markAsSaved(new FullPageId(pageId, grpId));

            Collection<FullPageId> dirtyPages = this.dirtyPages;

            if (dirtyPages != null) {
                if (dirtyPages.remove(new FullPageId(pageId, grpId)))
                    dirtyPagesCntr.decrementAndGet();
            }

            return relPtr;
        }

        /**
         * Removes random oldest page for page replacement from memory to storage.
         *
         * @return Relative address for removed page, now it can be replaced by allocated or reloaded page.
         * @throws IgniteCheckedException If failed to evict page.
         */
        private long removePageForReplacement() throws IgniteCheckedException {
            assert getWriteHoldCount() > 0;

            if (pageReplacementWarned == 0) {
                if (pageReplacementWarnedFieldUpdater.compareAndSet(PageMemoryImpl.this, 0, 1)) {
                    String msg = "Page replacements started, pages will be rotated with disk, this will affect " +
                        "storage performance (consider increasing DataRegionConfiguration#setMaxSize for " +
                        "data region): " + dataRegionMetrics.getName();

                    U.warn(log, msg);

                    if (ctx.gridEvents().isRecordable(EventType.EVT_PAGE_REPLACEMENT_STARTED)) {
                        ctx.kernalContext().closure().runLocalSafe(new GridPlainRunnable() {
                            @Override public void run() {
                                ctx.gridEvents().record(new PageReplacementStartedEvent(
                                    ctx.localNode(),
                                    msg,
                                    dataRegionMetrics.getName()));
                            }
                        });
                    }
                }
            }

            if (acquiredPages() >= loadedPages.size())
                throw oomException("all pages are acquired");

            return pageReplacementPolicy.replace();
        }

        /**
         * Creates out of memory exception with additional information.
         *
         * @param reason Reason.
         */
        public IgniteOutOfMemoryException oomException(String reason) {
            DataRegionConfiguration dataRegionCfg = getDataRegionConfiguration();

            return new IgniteOutOfMemoryException("Failed to find a page for eviction (" + reason + ") [" +
                "segmentCapacity=" + loadedPages.capacity() +
                ", loaded=" + loadedPages.size() +
                ", maxDirtyPages=" + maxDirtyPages +
                ", dirtyPages=" + dirtyPagesCntr +
                ", cpPages=" + (checkpointPages() == null ? 0 : checkpointPages().size()) +
                ", pinned=" + acquiredPages() +
                ']' + U.nl() + "Out of memory in data region [" +
                "name=" + dataRegionCfg.getName() +
                ", initSize=" + U.readableSize(dataRegionCfg.getInitialSize(), false) +
                ", maxSize=" + U.readableSize(dataRegionCfg.getMaxSize(), false) +
                ", persistenceEnabled=" + dataRegionCfg.isPersistenceEnabled() + "] Try the following:" + U.nl() +
                "  ^-- Increase maximum off-heap memory size (DataRegionConfiguration.maxSize)" + U.nl() +
                "  ^-- Enable eviction or expiration policies"
            );
        }

        /**
         * Delegate to the corresponding page pool.
         *
         * @param relPtr Relative pointer.
         * @return Absolute pointer.
         */
        public long absolute(long relPtr) {
            return pool.absolute(relPtr);
        }

        /**
         * Delegate to the corresponding page pool.
         *
         * @param pageIdx Page index.
         * @return Relative pointer.
         */
        public long relative(long pageIdx) {
            return pool.relative(pageIdx);
        }

        /**
         * Delegate to the corresponding page pool.
         *
         * @param relPtr Relative pointer.
         * @return Page index in the pool.
         */
        public long pageIndex(long relPtr) {
            return pool.pageIndex(relPtr);
        }

        /**
         * @param grpId Cache group ID.
         * @param partId Partition ID.
         * @return Partition generation. Growing, 1-based partition version. Changed
         */
        public int partGeneration(int grpId, int partId) {
            assert getReadHoldCount() > 0 || getWriteHoldCount() > 0;

            Integer tag = partGenerationMap.get(new GroupPartitionId(grpId, partId));

            assert tag == null || tag >= 0 : "Negative tag=" + tag;

            return tag == null ? INIT_PART_GENERATION : tag;
        }

        /**
         * Gets loaded pages map.
         */
        public LoadedPagesMap loadedPages() {
            return loadedPages;
        }

        /**
         * Gets checkpoint pages.
         */
        public CheckpointPages checkpointPages() {
            return checkpointPages;
        }

        /**
         * Gets page pool.
         */
        public PagePool pool() {
            return pool;
        }

        /**
         * Increments partition generation due to partition invalidation (e.g. partition was rebalanced to other node
         * and evicted).
         *
         * @param grpId Cache group ID.
         * @param partId Partition ID.
         * @return New partition generation.
         */
        private int incrementPartGeneration(int grpId, int partId) {
            assert getWriteHoldCount() > 0;

            GroupPartitionId grpPart = new GroupPartitionId(grpId, partId);

            Integer gen = partGenerationMap.get(grpPart);

            if (gen == null)
                gen = INIT_PART_GENERATION;

            if (gen == Integer.MAX_VALUE) {
                U.warn(log, "Partition tag overflow [grpId=" + grpId + ", partId=" + partId + "]");

                partGenerationMap.put(grpPart, 0);

                return 0;
            }
            else {
                partGenerationMap.put(grpPart, gen + 1);

                return gen + 1;
            }
        }

        /**
         * @param grpId Cache group id.
         */
        private void resetGroupPartitionsGeneration(int grpId) {
            assert getWriteHoldCount() > 0;

            partGenerationMap.keySet().removeIf(grpPart -> grpPart.getGroupId() == grpId);
        }
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
    private static class ClearSegmentRunnable implements Runnable {
        /** */
        private final Segment seg;

        /** Clear element filter for (cache group ID, page ID). */
        LoadedPagesMap.KeyPredicate clearPred;

        /** */
        private final CountDownFuture doneFut;

        /** */
        private final int pageSize;

        /** */
        private final boolean rmvDirty;

        /**
         * @param seg Segment.
         * @param clearPred Clear predicate for (cache group ID, page ID).
         * @param doneFut Completion future.
         */
        private ClearSegmentRunnable(
            Segment seg,
            LoadedPagesMap.KeyPredicate clearPred,
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
                        GridLongList list = seg.loadedPages.removeIf(base, boundary, clearPred);

                        ptrs.addAll(list);

                        base = boundary;
                    }
                    finally {
                        seg.writeLock().unlock();
                    }

                    for (int i = 0; i < ptrs.size(); i++) {
                        long relPtr = ptrs.get(i);

                        long absPtr = seg.pool.absolute(relPtr);

                        if (rmvDirty) {
                            FullPageId fullId = PageHeader.fullPageId(absPtr);

                            if (seg.dirtyPages.remove(fullId))
                                seg.dirtyPagesCntr.decrementAndGet();
                        }

                        GridUnsafe.setMemory(absPtr + PAGE_OVERHEAD, pageSize, (byte)0);

                        seg.pageReplacementPolicy.onRemove(relPtr);

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

    /**
     * Throttling enabled and its type enum.
     */
    public enum ThrottlingPolicy {
        /** All ways of throttling are disabled. */
        DISABLED,
        /** Only exponential throttling is used to protect from CP buffer overflow. */
        CHECKPOINT_BUFFER_ONLY,
        /** Target ratio based: CP progress is used as border. */
        TARGET_RATIO_BASED,
        /** Speed based. CP writting speed and estimated ideal speed are used as border */
        SPEED_BASED
    }
}
