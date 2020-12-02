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
import java.util.Set;
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
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.PageStoreWriter;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionCountersIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.TrackingPageIO;
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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DELAYED_REPLACED_PAGE_WRITE;
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
    public static final long INVALID_REL_PTR = RELATIVE_PTR_MASK;

    /** Pointer which means that this page is outdated (for example, cache was destroyed, partition eviction'd happened */
    private static final long OUTDATED_REL_PTR = INVALID_REL_PTR + 1;

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

    /** Number of random pages that will be picked for eviction. */
    public static final int RANDOM_PAGES_EVICT_NUM = 5;

    /** Try again tag. */
    public static final int TRY_AGAIN_TAG = -1;

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
    private final boolean useBackwardShiftMap
        = IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_LOADED_PAGES_BACKWARD_SHIFT_MAP, true);

    /** */
    private final ExecutorService asyncRunner = new ThreadPoolExecutor(
        0,
        Runtime.getRuntime().availableProcessors(),
        30L,
        TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(Runtime.getRuntime().availableProcessors()));

    /** Page store manager. */
    private IgnitePageStoreManager storeMgr;

    /** */
    private IgniteWriteAheadLogManager walMgr;

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
    private OffheapReadWriteLock rwLock;

    /** Flush dirty page closure. When possible, will be called by evictPage(). */
    private final PageStoreWriter flushDirtyPage;

    /**
     * Delayed page replacement (rotation with disk) tracker. Because other thread may require exactly the same page to be loaded from store,
     * reads are protected by locking.
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
    private ThrottlingPolicy throttlingPlc;

    /** Checkpoint progress provider. Null disables throttling. */
    @Nullable private final IgniteOutClosure<CheckpointProgress> cpProgressProvider;

    /** Field updater. */
    private static final AtomicIntegerFieldUpdater<PageMemoryImpl> pageReplacementWarnedFieldUpdater =
        AtomicIntegerFieldUpdater.newUpdater(PageMemoryImpl.class, "pageReplacementWarned");

    /** Flag indicating page replacement started (rotation with disk), allocating new page requires freeing old one. */
    private volatile int pageReplacementWarned;

    /** */
    private long[] sizes;

    /** Memory metrics to track dirty pages count and page replace rate. */
    private final DataRegionMetricsImpl memMetrics;

    /**
     * {@code False} if memory was not started or already stopped and is not supposed for any usage.
     */
    private volatile boolean started;

    /**
     * @param directMemoryProvider Memory allocator to use.
     * @param sizes segments sizes, last is checkpoint pool size.
     * @param ctx Cache shared context.
     * @param pageSize Page size.
     * @param flushDirtyPage write callback invoked when a dirty page is removed for replacement.
     * @param changeTracker Callback invoked to track changes in pages.
     * @param stateChecker Checkpoint lock state provider. Used to ensure lock is held by thread, which modify pages.
     * @param memMetrics Memory metrics to track dirty pages count and page replace rate.
     * @param throttlingPlc Write throttle enabled and its type. Null equal to none.
     * @param cpProgressProvider checkpoint progress, base for throttling. Null disables throttling.
     */
    public PageMemoryImpl(
        DirectMemoryProvider directMemoryProvider,
        long[] sizes,
        GridCacheSharedContext<?, ?> ctx,
        int pageSize,
        PageStoreWriter flushDirtyPage,
        @Nullable GridInClosure3X<Long, FullPageId, PageMemoryEx> changeTracker,
        CheckpointLockStateChecker stateChecker,
        DataRegionMetricsImpl memMetrics,
        @Nullable ThrottlingPolicy throttlingPlc,
        IgniteOutClosure<CheckpointProgress> cpProgressProvider
    ) {
        assert ctx != null;
        assert pageSize > 0;
        assert memMetrics != null;

        log = ctx.logger(PageMemoryImpl.class);

        this.ctx = ctx;
        this.directMemoryProvider = directMemoryProvider;
        this.sizes = sizes;
        this.flushDirtyPage = flushDirtyPage;
        delayedPageReplacementTracker =
            getBoolean(IGNITE_DELAYED_REPLACED_PAGE_WRITE, true)
                ? new DelayedPageReplacementTracker(pageSize, flushDirtyPage, log, sizes.length - 1) :
                null;
        this.changeTracker = changeTracker;
        this.stateChecker = stateChecker;
        this.throttlingPlc = throttlingPlc != null ? throttlingPlc : ThrottlingPolicy.CHECKPOINT_BUFFER_ONLY;
        this.cpProgressProvider = cpProgressProvider;

        storeMgr = ctx.pageStore();
        walMgr = ctx.wal();
        encMgr = ctx.kernalContext().encryption();
        encryptionDisabled = ctx.gridConfig().getEncryptionSpi() instanceof NoopEncryptionSpi;

        assert storeMgr != null;
        assert walMgr != null;
        assert encMgr != null;

        sysPageSize = pageSize + PAGE_OVERHEAD;

        encPageSize = CU.encryptedPageSize(pageSize, ctx.kernalContext().config().getEncryptionSpi());

        rwLock = new OffheapReadWriteLock(128);

        this.memMetrics = memMetrics;
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

            for (int i = 0; i < regs - 1; i++) {
                assert i < segments.length;

                DirectMemoryRegion reg = regions.get(i);

                totalAllocated += reg.size();

                segments[i] = new Segment(i, regions.get(i), checkpointPool.pages() / segments.length, throttlingPlc);

                pages += segments[i].pages();
                totalTblSize += segments[i].tableSize();
            }

            initWriteThrottle();

            this.segments = segments;

            if (log.isInfoEnabled())
                log.info("Started page memory [memoryAllocated=" + U.readableSize(totalAllocated, false) +
                    ", pages=" + pages +
                    ", tableSize=" + U.readableSize(totalTblSize, false) +
                    ", checkpointBuffer=" + U.readableSize(checkpointBuf, false) +
                    ']');

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
        assert flags == PageIdAllocator.FLAG_DATA && partId <= PageIdAllocator.MAX_PARTITION_ID ||
            flags == PageIdAllocator.FLAG_IDX && partId == PageIdAllocator.INDEX_PARTITION :
            "flags = " + flags + ", partId = " + partId;

        assert started;
        assert stateChecker.checkpointLockIsHeldByThread();

        if (isThrottlingEnabled())
            writeThrottle.onMarkDirty(false);

        long pageId = storeMgr.allocatePage(grpId, partId, flags);

        assert PageIdUtils.pageIndex(pageId) > 0; //it's crucial for tracking pages (zero page is super one)

        // We need to allocate page in memory for marking it dirty to save it in the next checkpoint.
        // Otherwise it is possible that on file will be empty page which will be saved at snapshot and read with error
        // because there is no crc inside them.
        Segment seg = segment(grpId, pageId);

        DelayedDirtyPageStoreWrite delayedWriter = delayedPageReplacementTracker != null
            ? delayedPageReplacementTracker.delayedPageWrite() : null;

        FullPageId fullId = new FullPageId(pageId, grpId);

        seg.writeLock().lock();

        boolean isTrackingPage =
            changeTracker != null && trackingIO.trackingPageFor(pageId, realPageSize(grpId)) == pageId;

        try {
            long relPtr = seg.loadedPages.get(
                grpId,
                PageIdUtils.effectivePageId(pageId),
                seg.partGeneration(grpId, partId),
                INVALID_REL_PTR,
                OUTDATED_REL_PTR
            );

            if (relPtr == OUTDATED_REL_PTR)
                relPtr = refreshOutdatedPage(seg, grpId, pageId, false);

            if (relPtr == INVALID_REL_PTR)
                relPtr = seg.borrowOrAllocateFreePage(pageId);

            if (relPtr == INVALID_REL_PTR)
                relPtr = seg.removePageForReplacement(delayedWriter == null ? flushDirtyPage : delayedWriter);

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
                    trackingIO.initNewPage(pageAddr, pageId, realPageSize(grpId));

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
                "  ^-- Enable Ignite persistence (DataRegionConfiguration.persistenceEnabled)" + U.nl() +
                "  ^-- Enable eviction or expiration policies"
            );

            e.initCause(oom);

            ctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
        finally {
            seg.writeLock().unlock();
        }

        //Finish replacement only when an exception wasn't thrown otherwise it possible to corrupt B+Tree.
        if (delayedWriter != null)
            delayedWriter.finishReplacement();

        //we have allocated 'tracking' page, we need to allocate regular one
        return isTrackingPage ? allocatePage(grpId, partId, flags) : pageId;
    }

    /**
     * @return Data region configuration.
     */
    private DataRegionConfiguration getDataRegionConfiguration() {
        DataStorageConfiguration memCfg = ctx.kernalContext().config().getDataStorageConfiguration();

        assert memCfg != null;

        String dataRegionName = memMetrics.getName();

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
    @Override public long metaPageId(int grpId) {
        assert started;

        return storeMgr.metaPageId(grpId);
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

        FullPageId fullId = new FullPageId(pageId, grpId);

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

                statHolder.trackLogicalRead(absPtr + PAGE_OVERHEAD);

                return absPtr;
            }
        }
        finally {
            seg.readLock().unlock();
        }

        DelayedDirtyPageStoreWrite delayedWriter = delayedPageReplacementTracker != null
            ? delayedPageReplacementTracker.delayedPageWrite() : null;

        seg.writeLock().lock();

        long lockedPageAbsPtr = -1;
        boolean readPageFromStore = false;

        try {
            // Double-check.
            long relPtr = seg.loadedPages.get(
                grpId,
                PageIdUtils.effectivePageId(pageId),
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
                    relPtr = seg.removePageForReplacement(delayedWriter == null ? flushDirtyPage : delayedWriter);

                absPtr = seg.absolute(relPtr);

                PageHeader.fullPageId(absPtr, fullId);
                PageHeader.writeTimestamp(absPtr, U.currentTimeMillis());

                assert !PageHeader.isAcquired(absPtr) :
                    "Pin counter must be 0 for a new page [relPtr=" + U.hexLong(relPtr) +
                        ", absPtr=" + U.hexLong(absPtr) + ']';

                // We can clear dirty flag after the page has been allocated.
                setDirty(fullId, absPtr, false, false);

                seg.loadedPages.put(
                    grpId,
                    PageIdUtils.effectivePageId(pageId),
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

                relPtr = refreshOutdatedPage(seg, grpId, pageId, false);

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
            }
            else
                absPtr = seg.absolute(relPtr);

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

            if (delayedWriter != null)
                delayedWriter.finishReplacement();

            if (readPageFromStore) {
                assert lockedPageAbsPtr != -1 : "Page is expected to have a valid address [pageId=" + fullId +
                    ", lockedPageAbsPtr=" + U.hexLong(lockedPageAbsPtr) + ']';

                assert isPageWriteLocked(lockedPageAbsPtr) : "Page is expected to be locked: [pageId=" + fullId + "]";

                long pageAddr = lockedPageAbsPtr + PAGE_OVERHEAD;

                ByteBuffer buf = wrapPointer(pageAddr, pageSize());

                long actualPageId = 0;

                try {
                    storeMgr.read(grpId, pageId, buf);

                    statHolder.trackPhysicalAndLogicalRead(pageAddr);

                    actualPageId = PageIO.getPageId(buf);

                    memMetrics.onPageRead();
                }
                catch (IgniteDataIntegrityViolationException e) {
                    U.warn(log, "Failed to read page (data integrity violation encountered, will try to " +
                        "restore using existing WAL) [fullPageId=" + fullId + ']', e);

                    buf.rewind();

                    tryToRestorePage(fullId, buf);

                    statHolder.trackPhysicalAndLogicalRead(pageAddr);

                    memMetrics.onPageRead();
                }
                finally {
                    rwLock.writeUnlock(lockedPageAbsPtr + PAGE_LOCK_OFFSET,
                        actualPageId == 0 ? OffheapReadWriteLock.TAG_LOCK_ALWAYS : PageIdUtils.tag(actualPageId));
                }
            }
        }
    }

    /**
     * @param seg Segment.
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @param rmv {@code True} if page should be removed.
     * @return Relative pointer to refreshed page.
     */
    private long refreshOutdatedPage(Segment seg, int grpId, long pageId, boolean rmv) {
        assert seg.writeLock().isHeldByCurrentThread();

        int tag = seg.partGeneration(grpId, PageIdUtils.partId(pageId));

        long relPtr = seg.loadedPages.refresh(grpId, PageIdUtils.effectivePageId(pageId), tag);

        long absPtr = seg.absolute(relPtr);

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
            seg.loadedPages.remove(grpId, PageIdUtils.effectivePageId(pageId));

        CheckpointPages cpPages = seg.checkpointPages;

        if (cpPages != null)
            cpPages.markAsSaved(new FullPageId(pageId, grpId));

        Collection<FullPageId> dirtyPages = seg.dirtyPages;

        if (dirtyPages != null) {
            if (dirtyPages.remove(new FullPageId(pageId, grpId)))
                seg.dirtyPagesCntr.decrementAndGet();
        }

        return relPtr;
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
        if (encryptionDisabled || encMgr.groupKey(grpId) == null)
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

        memMetrics.resetDirtyPages();

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
                    relPtr = refreshOutdatedPage(
                        seg,
                        fullId.groupId(),
                        fullId.effectivePageId(),
                        true
                    );

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

                memMetrics.onPageWritten();

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

                throw new IgniteException(CHECKPOINT_POOL_OVERFLOW_ERROR_MSG + ": " + memMetrics.getName());
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
            assert PageIO.getVersion(tmpAbsPtr + PAGE_OVERHEAD) != 0 : "Invalid state. Version is 0! pageId = " + U.hexLong(fullId.pageId());

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

                    memMetrics.incrementDirtyPages();
                }
            }
        }
        else {
            Segment seg = segment(pageId.groupId(), pageId.pageId());

            if (seg.dirtyPages.remove(pageId)) {
                seg.dirtyPagesCntr.decrementAndGet();

                memMetrics.decrementDirtyPages();
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

    /** @return Data region metrics. */
    public DataRegionMetricsImpl metrics() {
        return memMetrics;
    }

    /** {@inheritDoc} */
    @Override public boolean shouldThrottle() {
        return writeThrottle.shouldThrottle();
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
    private class Segment extends ReentrantReadWriteLock {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private static final double FULL_SCAN_THRESHOLD = 0.4;

        /** Pointer to acquired pages integer counter. */
        private static final int ACQUIRED_PAGES_SIZEOF = 4;

        /** Padding to read from word beginning. */
        private static final int ACQUIRED_PAGES_PADDING = 4;

        /** Page ID to relative pointer map. */
        private final LoadedPagesMap loadedPages;

        /** Pointer to acquired pages integer counter. */
        private long acquiredPagesPtr;

        /** */
        private PagePool pool;

        /** Bytes required to store {@link #loadedPages}. */
        private long memPerTbl;

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
                : requiredSegmentTableMemory(pages);

            loadedPages = useBackwardShiftMap
                ? new RobinHoodBackwardShiftHashMap(ldPagesAddr, memPerTbl)
                : new FullPageIdTable(ldPagesAddr, memPerTbl, true);

            DirectMemoryRegion poolRegion = region.slice(memPerTbl + ldPagesMapOffInRegion);

            pool = new PagePool(idx, poolRegion, sysPageSize, rwLock);

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
         * @param saveDirtyPage implementation to save dirty page to persistent storage.
         * @return {@code True} if it is ok to replace this page, {@code false} if another page should be selected.
         * @throws IgniteCheckedException If failed to write page to the underlying store during eviction.
         */
        private boolean preparePageRemoval(FullPageId fullPageId, long absPtr, PageStoreWriter saveDirtyPage) throws IgniteCheckedException {
            assert writeLock().isHeldByCurrentThread();

            // Do not evict cache meta pages.
            if (fullPageId.pageId() == storeMgr.metaPageId(fullPageId.groupId()))
                return false;

            if (PageHeader.isAcquired(absPtr))
                return false;

            clearRowCache(fullPageId, absPtr);

            if (isDirty(absPtr)) {
                CheckpointPages checkpointPages = this.checkpointPages;
                // Can evict a dirty page only if should be written by a checkpoint.
                // These pages does not have tmp buffer.
                if (checkpointPages != null && checkpointPages.allowToSave(fullPageId)) {
                    assert storeMgr != null;

                    memMetrics.updatePageReplaceRate(U.currentTimeMillis() - PageHeader.readTimestamp(absPtr));

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

                final GridQueryRowCacheCleaner cleaner = ctx.kernalContext().query()
                    .getIndexing().rowCacheCleaner(fullPageId.groupId());

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
         * Removes random oldest page for page replacement from memory to storage.
         *
         * @return Relative address for removed page, now it can be replaced by allocated or reloaded page.
         * @throws IgniteCheckedException If failed to evict page.
         * @param saveDirtyPage Replaced page writer, implementation to save dirty page to persistent storage.
         */
        private long removePageForReplacement(PageStoreWriter saveDirtyPage) throws IgniteCheckedException {
            assert getWriteHoldCount() > 0;

            if (pageReplacementWarned == 0) {
                if (pageReplacementWarnedFieldUpdater.compareAndSet(PageMemoryImpl.this, 0, 1)) {
                    String msg = "Page replacements started, pages will be rotated with disk, this will affect " +
                        "storage performance (consider increasing DataRegionConfiguration#setMaxSize for " +
                        "data region): " + memMetrics.getName();

                    U.warn(log, msg);

                    if (ctx.gridEvents().isRecordable(EventType.EVT_PAGE_REPLACEMENT_STARTED)) {
                        ctx.kernalContext().closure().runLocalSafe(new GridPlainRunnable() {
                            @Override public void run() {
                                ctx.gridEvents().record(new PageReplacementStartedEvent(
                                    ctx.localNode(),
                                    msg,
                                    memMetrics.getName()));
                            }
                        });
                    }
                }
            }

            final ThreadLocalRandom rnd = ThreadLocalRandom.current();

            final int cap = loadedPages.capacity();

            if (acquiredPages() >= loadedPages.size()) {
                DataRegionConfiguration dataRegionCfg = getDataRegionConfiguration();

                throw new IgniteOutOfMemoryException("Failed to evict page from segment (all pages are acquired)."
                    + U.nl() + "Out of memory in data region [" +
                    "name=" + dataRegionCfg.getName() +
                    ", initSize=" + U.readableSize(dataRegionCfg.getInitialSize(), false) +
                    ", maxSize=" + U.readableSize(dataRegionCfg.getMaxSize(), false) +
                    ", persistenceEnabled=" + dataRegionCfg.isPersistenceEnabled() + "] Try the following:" + U.nl() +
                    "  ^-- Increase maximum off-heap memory size (DataRegionConfiguration.maxSize)" + U.nl() +
                    "  ^-- Enable Ignite persistence (DataRegionConfiguration.persistenceEnabled)" + U.nl() +
                    "  ^-- Enable eviction or expiration policies"
                );
            }

            // With big number of random picked pages we may fall into infinite loop, because
            // every time the same page may be found.
            Set<Long> ignored = null;

            long relRmvAddr = INVALID_REL_PTR;

            int iterations = 0;

            while (true) {
                long cleanAddr = INVALID_REL_PTR;
                long cleanTs = Long.MAX_VALUE;
                long dirtyAddr = INVALID_REL_PTR;
                long dirtyTs = Long.MAX_VALUE;
                long metaAddr = INVALID_REL_PTR;
                long metaTs = Long.MAX_VALUE;

                for (int i = 0; i < RANDOM_PAGES_EVICT_NUM; i++) {
                    ++iterations;

                    if (iterations > pool.pages() * FULL_SCAN_THRESHOLD)
                        break;

                    // We need to lookup for pages only in current segment for thread safety,
                    // so peeking random memory will lead to checking for found page segment.
                    // It's much faster to check available pages for segment right away.
                    ReplaceCandidate nearest = loadedPages.getNearestAt(rnd.nextInt(cap));

                    assert nearest != null && nearest.relativePointer() != INVALID_REL_PTR;

                    long rndAddr = nearest.relativePointer();

                    int partGen = nearest.generation();

                    final long absPageAddr = absolute(rndAddr);

                    FullPageId fullId = PageHeader.fullPageId(absPageAddr);

                    // Check page mapping consistency.
                    assert fullId.equals(nearest.fullId()) : "Invalid page mapping [tableId=" + nearest.fullId() +
                        ", actual=" + fullId + ", nearest=" + nearest;

                    boolean outdated = partGen < partGeneration(fullId.groupId(), PageIdUtils.partId(fullId.pageId()));

                    if (outdated)
                        return refreshOutdatedPage(this, fullId.groupId(), fullId.pageId(), true);

                    boolean pinned = PageHeader.isAcquired(absPageAddr);

                    boolean skip = ignored != null && ignored.contains(rndAddr);

                    final boolean dirty = isDirty(absPageAddr);

                    CheckpointPages checkpointPages = this.checkpointPages;

                    if (relRmvAddr == rndAddr || pinned || skip ||
                        fullId.pageId() == storeMgr.metaPageId(fullId.groupId()) ||
                        (dirty && (checkpointPages == null || !checkpointPages.contains(fullId)))
                    ) {
                        i--;

                        continue;
                    }

                    final long pageTs = PageHeader.readTimestamp(absPageAddr);

                    final boolean storMeta = isStoreMetadataPage(absPageAddr);

                    if (pageTs < cleanTs && !dirty && !storMeta) {
                        cleanAddr = rndAddr;

                        cleanTs = pageTs;
                    }
                    else if (pageTs < dirtyTs && dirty && !storMeta) {
                        dirtyAddr = rndAddr;

                        dirtyTs = pageTs;
                    }
                    else if (pageTs < metaTs && storMeta) {
                        metaAddr = rndAddr;

                        metaTs = pageTs;
                    }

                    if (cleanAddr != INVALID_REL_PTR)
                        relRmvAddr = cleanAddr;
                    else if (dirtyAddr != INVALID_REL_PTR)
                        relRmvAddr = dirtyAddr;
                    else
                        relRmvAddr = metaAddr;
                }

                if (relRmvAddr == INVALID_REL_PTR)
                    return tryToFindSequentially(cap, saveDirtyPage);

                final long absRmvAddr = absolute(relRmvAddr);

                final FullPageId fullPageId = PageHeader.fullPageId(absRmvAddr);

                if (!preparePageRemoval(fullPageId, absRmvAddr, saveDirtyPage)) {
                    if (iterations > 10) {
                        if (ignored == null)
                            ignored = new HashSet<>();

                        ignored.add(relRmvAddr);
                    }

                    if (iterations > pool.pages() * FULL_SCAN_THRESHOLD)
                        return tryToFindSequentially(cap, saveDirtyPage);

                    continue;
                }

                loadedPages.remove(
                    fullPageId.groupId(),
                    fullPageId.effectivePageId()
                );

                return relRmvAddr;
            }
        }

        /**
         * @param absPageAddr Absolute page address
         * @return {@code True} if page is related to partition metadata, which is loaded in saveStoreMetadata().
         */
        private boolean isStoreMetadataPage(long absPageAddr) {
            try {
                long dataAddr = absPageAddr + PAGE_OVERHEAD;

                int type = PageIO.getType(dataAddr);
                int ver = PageIO.getVersion(dataAddr);

                PageIO io = PageIO.getPageIO(type, ver);

                return io instanceof PagePartitionMetaIO
                    || io instanceof PagesListMetaIO
                    || io instanceof PagePartitionCountersIO;
            }
            catch (IgniteCheckedException ignored) {
                return false;
            }
        }

        /**
         * Will scan all segment pages to find one to evict it
         *
         * @param cap Capacity.
         * @param saveDirtyPage Evicted page writer.
         */
        private long tryToFindSequentially(int cap, PageStoreWriter saveDirtyPage) throws IgniteCheckedException {
            assert getWriteHoldCount() > 0;

            long prevAddr = INVALID_REL_PTR;
            int pinnedCnt = 0;
            int failToPrepare = 0;

            for (int i = 0; i < cap; i++) {
                final ReplaceCandidate nearest = loadedPages.getNearestAt(i);

                assert nearest != null && nearest.relativePointer() != INVALID_REL_PTR;

                final long addr = nearest.relativePointer();

                int partGen = nearest.generation();

                final long absPageAddr = absolute(addr);

                FullPageId fullId = PageHeader.fullPageId(absPageAddr);

                if (partGen < partGeneration(fullId.groupId(), PageIdUtils.partId(fullId.pageId())))
                    return refreshOutdatedPage(this, fullId.groupId(), fullId.pageId(), true);

                boolean pinned = PageHeader.isAcquired(absPageAddr);

                if (pinned)
                    pinnedCnt++;

                if (addr == prevAddr || pinned)
                    continue;

                final long absEvictAddr = absolute(addr);

                final FullPageId fullPageId = PageHeader.fullPageId(absEvictAddr);

                if (preparePageRemoval(fullPageId, absEvictAddr, saveDirtyPage)) {
                    loadedPages.remove(
                        fullPageId.groupId(),
                        fullPageId.effectivePageId()
                    );

                    return addr;
                }
                else
                    failToPrepare++;

                prevAddr = addr;
            }

            DataRegionConfiguration dataRegionCfg = getDataRegionConfiguration();

            throw new IgniteOutOfMemoryException("Failed to find a page for eviction [segmentCapacity=" + cap +
                ", loaded=" + loadedPages.size() +
                ", maxDirtyPages=" + maxDirtyPages +
                ", dirtyPages=" + dirtyPagesCntr +
                ", cpPages=" + (checkpointPages == null ? 0 : checkpointPages.size()) +
                ", pinnedInSegment=" + pinnedCnt +
                ", failedToPrepare=" + failToPrepare +
                ']' + U.nl() + "Out of memory in data region [" +
                "name=" + dataRegionCfg.getName() +
                ", initSize=" + U.readableSize(dataRegionCfg.getInitialSize(), false) +
                ", maxSize=" + U.readableSize(dataRegionCfg.getMaxSize(), false) +
                ", persistenceEnabled=" + dataRegionCfg.isPersistenceEnabled() + "] Try the following:" + U.nl() +
                "  ^-- Increase maximum off-heap memory size (DataRegionConfiguration.maxSize)" + U.nl() +
                "  ^-- Enable Ignite persistence (DataRegionConfiguration.persistenceEnabled)" + U.nl() +
                "  ^-- Enable eviction or expiration policies"
            );
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
         * @return Partition generation. Growing, 1-based partition version. Changed
         */
        private int partGeneration(int grpId, int partId) {
            assert getReadHoldCount() > 0 || getWriteHoldCount() > 0;

            Integer tag = partGenerationMap.get(new GroupPartitionId(grpId, partId));

            assert tag == null || tag >= 0 : "Negative tag=" + tag;

            return tag == null ? INIT_PART_GENERATION : tag;
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
    private static class ClearSegmentRunnable implements Runnable {
        /** */
        private Segment seg;

        /** Clear element filter for (cache group ID, page ID). */
        LoadedPagesMap.KeyPredicate clearPred;

        /** */
        private CountDownFuture doneFut;

        /** */
        private int pageSize;

        /** */
        private boolean rmvDirty;

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
