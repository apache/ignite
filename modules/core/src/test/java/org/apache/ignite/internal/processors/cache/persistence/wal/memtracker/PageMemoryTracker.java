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

package org.apache.ignite.internal.processors.cache.persistence.wal.memtracker;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.InitNewPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PageDeltaRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.RecycleRecord;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FsyncModeFileWriteAheadLogManager;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginContext;
import org.mockito.Mockito;

/**
 * Page memory tracker.
 *
 * Replicates Ignite's page memory changes to own managed memory region by intercepting WAL records and
 * applying page snapshots and deltas.
 */
public class PageMemoryTracker implements IgnitePlugin {
    /** Plugin context. */
    private final PluginContext ctx;

    /** Config. */
    private final PageMemoryTrackerConfiguration cfg;

    /** Logger. */
    private final IgniteLogger log;

    /** Grid context. */
    private final GridKernalContext gridCtx;

    /** Page allocator mutex. */
    private final Object pageAllocatorMux = new Object();

    /** Pages. */
    private final Map<FullPageId, DirectMemoryPage> pages = new ConcurrentHashMap<>();

    /** Page slots. */
    private volatile DirectMemoryPageSlot[] pageSlots;

    /** Free page slots. */
    private final BitSet freeSlots = new BitSet();

    /** Last allocated page index. */
    private volatile int lastPageIdx;

    /** Free page slots count. */
    private volatile int freeSlotsCnt;

    /** Page size. */
    private volatile int pageSize;

    /** Page memory mock. */
    private volatile PageMemory pageMemoryMock;

    /** Memory provider. */
    private volatile DirectMemoryProvider memoryProvider;

    /** Memory region. */
    private volatile DirectMemoryRegion memoryRegion;

    /** Max pages. */
    private volatile int maxPages;

    /** Tracking started. */
    private volatile boolean started;

    /** Tracker was started with empty PDS. */
    private volatile boolean emptyPds;

    /** Statistics. */
    private final ConcurrentMap<WALRecord.RecordType, AtomicInteger> stats = new ConcurrentHashMap<>();

    /** Checkpoint listener. */
    private DbCheckpointListener checkpointLsnr;

    /**
     * @param ctx Plugin context.
     * @param cfg Configuration.
     */
    PageMemoryTracker(PluginContext ctx, PageMemoryTrackerConfiguration cfg) {
        this.ctx = ctx;
        this.cfg = cfg;
        log = ctx.log(getClass());
        gridCtx = ((IgniteEx)ctx.grid()).context();
    }

    /**
     * Creates WAL manager.
     */
    IgniteWriteAheadLogManager createWalManager() {
        if (isEnabled()) {
            if (ctx.igniteConfiguration().getDataStorageConfiguration().getWalMode() == WALMode.FSYNC) {
                return new FsyncModeFileWriteAheadLogManager(gridCtx) {
                    @Override public WALPointer log(WALRecord record) throws IgniteCheckedException {
                        WALPointer res = super.log(record);

                        applyWalRecord(record);

                        return res;
                    }

                    @Override public void resumeLogging(WALPointer lastPtr) throws IgniteCheckedException {
                        super.resumeLogging(lastPtr);

                        emptyPds = (lastPtr == null);
                    }
                };
            }
            else {
                return new FileWriteAheadLogManager(gridCtx) {
                    @Override public WALPointer log(WALRecord record) throws IgniteCheckedException {
                        WALPointer res = super.log(record);

                        applyWalRecord(record);

                        return res;
                    }

                    @Override public void resumeLogging(WALPointer lastPtr) throws IgniteCheckedException {
                        super.resumeLogging(lastPtr);

                        emptyPds = (lastPtr == null);
                    }
                };
            }
        }

        return null;
    }

    /**
     * Creates page store manager.
     */
    IgnitePageStoreManager createPageStoreManager() {
        if (isEnabled()) {
            return new FilePageStoreManager(gridCtx) {
                @Override public void shutdownForCacheGroup(CacheGroupContext grp,
                    boolean destroy) throws IgniteCheckedException {
                    super.shutdownForCacheGroup(grp, destroy);

                    cleanupPages(fullPageId -> fullPageId.groupId() == grp.groupId());
                }

                @Override public void onPartitionDestroyed(int grpId, int partId, int tag) throws IgniteCheckedException {
                    super.onPartitionDestroyed(grpId, partId, tag);

                    cleanupPages(fullPageId -> fullPageId.groupId() == grpId
                        && PageIdUtils.partId(fullPageId.pageId()) == partId);
                }
            };
        }

        return null;
    }

    /**
     * Start tracking pages.
     */
    synchronized void start() {
        if (!isEnabled() || started)
            return;

        pageSize = ctx.igniteConfiguration().getDataStorageConfiguration().getPageSize();

        pageMemoryMock = Mockito.mock(PageMemory.class);

        Mockito.doReturn(pageSize).when(pageMemoryMock).pageSize();

        GridCacheSharedContext sharedCtx = gridCtx.cache().context();

        // Initialize one memory region for all data regions of target ignite node.
        long maxMemorySize = 0;

        for (DataRegion dataRegion : sharedCtx.database().dataRegions()) {
            if (dataRegion.pageMemory() instanceof PageMemoryImpl)
                maxMemorySize += dataRegion.config().getMaxSize();
        }

        long[] chunks = new long[] {maxMemorySize};

        memoryProvider = new UnsafeMemoryProvider(log);

        memoryProvider.initialize(chunks);

        memoryRegion = memoryProvider.nextRegion();

        maxPages = (int)(maxMemorySize / pageSize);

        pageSlots = new DirectMemoryPageSlot[maxPages];

        freeSlotsCnt = maxPages;

        if (cfg.isCheckPagesOnCheckpoint()) {
            checkpointLsnr = ctx -> {
                if (!checkPages(false))
                    throw new IgniteCheckedException("Page memory is inconsistent after applying WAL delta records.");
            };

            ((GridCacheDatabaseSharedManager)gridCtx.cache().context().database()).addCheckpointListener(checkpointLsnr);
        }

        lastPageIdx = 0;

        started = true;

        log.info("PageMemory tracker started, " + U.readableSize(maxMemorySize, false) + " offheap memory allocated.");
    }

    /**
     * Stop tracking, release resources.
     */
    synchronized void stop() {
        if (!started)
            return;

        started = false;

        pages.clear();

        pageSlots = null;

        freeSlots.clear();

        stats.clear();

        memoryProvider.shutdown(true);

        if (checkpointLsnr != null) {
            ((GridCacheDatabaseSharedManager)gridCtx.cache().context().database())
                .removeCheckpointListener(checkpointLsnr);

            checkpointLsnr = null;
        }

        log.info("PageMemory tracker stopped.");
    }

    /**
     * Is plugin enabled.
     */
    private boolean isEnabled() {
        return (cfg != null && cfg.isEnabled() && CU.isPersistenceEnabled(ctx.igniteConfiguration()));
    }


    /**
     * Cleanup pages by predicate.
     *
     * @param pred Predicate.
     */
    private void cleanupPages(IgnitePredicate<FullPageId> pred) {
        synchronized (pageAllocatorMux) {
            for (Map.Entry<FullPageId, DirectMemoryPage> pageEntry : pages.entrySet()) {
                if (pred.apply(pageEntry.getKey())) {
                    pages.remove(pageEntry.getKey());

                    freeSlots.set(pageEntry.getValue().slot().index());

                    freeSlotsCnt++;
                }
            }
        }
    }

    /**
     * Allocates new page for given FullPageId.
     *
     * @param fullPageId Full page id.
     */
    private DirectMemoryPage allocatePage(FullPageId fullPageId) throws IgniteCheckedException {
        synchronized (pageAllocatorMux) {
            // Double check.
            DirectMemoryPage page = pages.get(fullPageId);

            if (page != null)
                return page;

            if (freeSlotsCnt == 0)
                throw new IgniteCheckedException("Can't allocate new page");

            int pageIdx;

            if (lastPageIdx < maxPages)
                pageIdx = lastPageIdx++;
            else {
                pageIdx = freeSlots.nextSetBit(0);

                assert pageIdx >= 0;

                freeSlots.clear(pageIdx);
            }

            freeSlotsCnt--;

            long pageAddr = memoryRegion.address() + ((long)pageIdx) * pageSize;

            DirectMemoryPageSlot pageSlot = pageSlots[pageIdx];
            if (pageSlot == null)
                pageSlot = pageSlots[pageIdx] = new DirectMemoryPageSlot(pageAddr, pageIdx);

            pageSlot.lock();

            try {
                page = new DirectMemoryPage(pageSlot);

                page.fullPageId(fullPageId);

                pages.put(fullPageId, page);

                if (pageSlot.owningPage() != null) {
                    // Clear memory if slot was already used.
                    ByteBuffer pageBuf = GridUnsafe.wrapPointer(pageAddr, pageSize);

                    pageBuf.put(new byte[pageSize]);
                }

                pageSlot.owningPage(page);
            }
            finally {
                pageSlot.unlock();
            }

            return page;
        }
    }

    /**
     * Gets or allocates page for given FullPageId.
     *
     * @param fullPageId Full page id.
     * @return Page.
     */
    private DirectMemoryPage page(FullPageId fullPageId) throws IgniteCheckedException {
        DirectMemoryPage page = pages.get(fullPageId);

        if (page == null)
            page = allocatePage(fullPageId);

        return page;
    }

    /**
     * Apply WAL record to local memory region.
     */
    private void applyWalRecord(WALRecord record) throws IgniteCheckedException {
        if (!started)
            return;

        if (record instanceof PageSnapshot) {
            PageSnapshot snapshot = (PageSnapshot)record;

            int grpId = snapshot.fullPageId().groupId();
            long pageId = snapshot.fullPageId().pageId();

            FullPageId fullPageId = new FullPageId(pageId, grpId);

            DirectMemoryPage page = page(fullPageId);

            page.lock();

            try {
                PageUtils.putBytes(page.address(), 0, snapshot.pageData());

                page.fullPageId(fullPageId);

                page.changeHistory().clear();

                page.changeHistory().add(record);
            }
            finally {
                page.unlock();
            }
        }
        else if (record instanceof PageDeltaRecord) {
            PageDeltaRecord deltaRecord = (PageDeltaRecord)record;

            int grpId = deltaRecord.groupId();
            long pageId = deltaRecord.pageId();

            FullPageId fullPageId = new FullPageId(pageId, grpId);

            DirectMemoryPage page = page(fullPageId);

            page.lock();

            try {
                deltaRecord.applyDelta(pageMemoryMock, page.address());

                // Set new fullPageId after recycle or after new page init, because pageId tag is changed.
                if (record instanceof RecycleRecord)
                    page.fullPageId(new FullPageId(((RecycleRecord)record).newPageId(), grpId));
                else if (record instanceof InitNewPageRecord)
                    page.fullPageId(new FullPageId(((InitNewPageRecord)record).newPageId(), grpId));

                page.changeHistory().add(record);
            }
            finally {
                page.unlock();
            }
        }
        else
            return;

        // Increment statistics.
        AtomicInteger statCnt = stats.get(record.type());

        if (statCnt == null) {
            statCnt = new AtomicInteger();

            AtomicInteger oldCnt = stats.putIfAbsent(record.type(), statCnt);

            if (oldCnt != null)
                statCnt = oldCnt;
        }

        statCnt.incrementAndGet();
    }

    /**
     * Total count of allocated pages in page store.
     */
    private long pageStoreAllocatedPages() {
        IgnitePageStoreManager pageStoreMgr = gridCtx.cache().context().pageStore();

        assert pageStoreMgr != null;

        long totalAllocated = pageStoreMgr.pagesAllocated(MetaStorage.METASTORAGE_CACHE_ID);

        for (CacheGroupContext ctx : gridCtx.cache().cacheGroups())
            totalAllocated += pageStoreMgr.pagesAllocated(ctx.groupId());

        return totalAllocated;
    }

    /**
     * Checks if there are any differences between the Ignite's data regions content and pages inside the tracker.
     *
     * @param checkAll Check all tracked pages, otherwise check until first error.
     * @return {@code true} if content of all tracked pages equals to content of these pages in the ignite instance.
     */
    public boolean checkPages(boolean checkAll) throws IgniteCheckedException {
        if (!started)
            throw new IgniteCheckedException("Page memory checking only possible when tracker is started.");

        GridCacheProcessor cacheProc = gridCtx.cache();

        boolean res = true;

        synchronized (pageAllocatorMux) {
            long totalAllocated = pageStoreAllocatedPages();

            long metaId = ((PageMemoryEx)cacheProc.context().database().metaStorage().pageMemory()).metaPageId(
                MetaStorage.METASTORAGE_CACHE_ID);

            // Meta storage meta page is counted as allocated, but never used in current implementation.
            // This behavior will be fixed by https://issues.apache.org/jira/browse/IGNITE-8735
            if (!pages.containsKey(new FullPageId(metaId, MetaStorage.METASTORAGE_CACHE_ID))
                && pages.containsKey(new FullPageId(metaId + 1, MetaStorage.METASTORAGE_CACHE_ID)))
                totalAllocated--;

            log.info(">>> Total tracked pages: " + pages.size());
            log.info(">>> Total allocated pages: " + totalAllocated);

            dumpStats();

            if (emptyPds && pages.size() != totalAllocated) {
                res = false;

                log.error("Started from empty PDS, but tracked pages count not equals to allocated pages count");

                if (!checkAll)
                    return false;
            }
        }

        Set<Integer> groupsWarned = new HashSet<>();

        for (DirectMemoryPage page : pages.values()) {
            FullPageId fullPageId = page.fullPageId();

            PageMemory pageMem;

            if (fullPageId.groupId() == MetaStorage.METASTORAGE_CACHE_ID)
                pageMem = cacheProc.context().database().metaStorage().pageMemory();
            else {
                CacheGroupContext ctx = cacheProc.cacheGroup(fullPageId.groupId());

                if (ctx != null)
                    pageMem = ctx.dataRegion().pageMemory();
                else {
                    if (!groupsWarned.contains(fullPageId.groupId())) {
                        log.warning("Cache group " + fullPageId.groupId() + " not found.");

                        groupsWarned.add(fullPageId.groupId());
                    }

                    continue;
                }
            }

            assert pageMem instanceof PageMemoryImpl;

            long rmtPage = pageMem.acquirePage(fullPageId.groupId(), fullPageId.pageId());

            try {
                long rmtPageAddr = pageMem.readLock(fullPageId.groupId(), fullPageId.pageId(), rmtPage);

                try {
                    page.lock();

                    try {
                        if (rmtPageAddr == 0L) {
                            res = false;

                            log.error("Can't lock page: " + fullPageId);

                            dumpHistory(page);
                        }
                        else {
                            ByteBuffer locBuf = GridUnsafe.wrapPointer(page.address(), pageSize);
                            ByteBuffer rmtBuf = GridUnsafe.wrapPointer(rmtPageAddr, pageSize);

                            if (!locBuf.equals(rmtBuf)) {
                                res = false;

                                log.error("Page buffers are not equals: " + fullPageId);

                                dumpDiff(locBuf, rmtBuf);

                                dumpHistory(page);
                            }
                        }

                        if (!res && !checkAll)
                            return false;
                    }
                    finally {
                        page.unlock();
                    }
                }
                finally {
                    if (rmtPageAddr != 0L)
                        pageMem.readUnlock(fullPageId.groupId(), fullPageId.pageId(), rmtPage);
                }
            }
            finally {
                pageMem.releasePage(fullPageId.groupId(), fullPageId.pageId(), rmtPage);
            }
        }

        return res;
    }

    /**
     * Dump statistics to log.
     */
    private void dumpStats() {
        log.info(">>> Processed WAL records:");

        for (Map.Entry<WALRecord.RecordType, AtomicInteger> entry : stats.entrySet())
            log.info("        " + entry.getKey() + '=' + entry.getValue().get());
    }

    /**
     * Dump difference between two ByteBuffers to log.
     *
     * @param buf1 Buffer 1.
     * @param buf2 Buffer 2.
     */
    private void dumpDiff(ByteBuffer buf1, ByteBuffer buf2) {
        log.error(">>> Diff:");

        for (int i = 0; i < Math.min(buf1.remaining(), buf2.remaining()); i++) {
            byte b1 = buf1.get(buf1.position() + i);
            byte b2 = buf2.get(buf2.position() + i);

            if (b1 != b2)
                log.error(String.format("        0x%04X: %02X %02X", i, b1, b2));
        }

        if (buf1.remaining() < buf2.remaining()) {
            for (int i = buf1.remaining(); i < buf2.remaining(); i++)
                log.error(String.format("        0x%04X:    %02X", i, buf2.get(buf2.position() + i)));
        }
        else if (buf1.remaining() > buf2.remaining()) {
            for (int i = buf2.remaining(); i < buf1.remaining(); i++)
                log.error(String.format("        0x%04X: %02X", i, buf1.get(buf1.position() + i)));
        }
    }

    /**
     * Dump page change history to log.
     *
     * @param page Page.
     */
    private void dumpHistory(DirectMemoryPage page) {
        log.error(">>> Change history:");

        for (WALRecord record : page.changeHistory())
            log.error("        " + record);
    }

    /**
     *
     */
    private static class DirectMemoryPage {
        /** Page slot. */
        private final DirectMemoryPageSlot slot;

        /** Change history. */
        private final List<WALRecord> changeHist = new LinkedList<>();

        /** Full page id. */
        private volatile FullPageId fullPageId;

        /**
         * @param slot Memory page slot.
         */
        private DirectMemoryPage(DirectMemoryPageSlot slot) {
            this.slot = slot;
        }

        /**
         * Lock page.
         */
        public void lock() throws IgniteCheckedException {
            slot.lock();

            if (slot.owningPage() != this) {
                slot.unlock();

                throw new IgniteCheckedException("Memory slot owning page changed, can't access page buffer.");
            }
        }

        /**
         * Unlock page.
         */
        public void unlock() {
            slot.unlock();
        }

        /**
         * @return Page address.
         */
        public long address() {
            return slot.address();
        }

        /**
         * Change history.
         */
        public List<WALRecord> changeHistory() {
            return changeHist;
        }

        /**
         * @return Full page id.
         */
        public FullPageId fullPageId() {
            return fullPageId;
        }

        /**
         * @param fullPageId Full page id.
         */
        public void fullPageId(FullPageId fullPageId) {
            this.fullPageId = fullPageId;
        }

        /**
         * @return Memory page slot.
         */
        public DirectMemoryPageSlot slot() {
            return slot;
        }
    }

    /**
     *
     */
    private static class DirectMemoryPageSlot {
        /** Page slot address. */
        private final long addr;

        /** Page slot index. */
        private final int idx;

        /** Page lock. */
        private final Lock lock = new ReentrantLock();

        /** Owning page. */
        private DirectMemoryPage owningPage;

        /**
         * @param addr Page address.
         * @param idx Page slot index
         */
        private DirectMemoryPageSlot(long addr, int idx) {
            this.addr = addr;
            this.idx = idx;
        }

        /**
         * Lock page slot.
         */
        public void lock() {
            lock.lock();
        }

        /**
         * Unlock page slot.
         */
        public void unlock() {
            lock.unlock();
        }

        /**
         * @return Page slot address.
         */
        public long address() {
            return addr;
        }

        /**
         * @return Page slot index.
         */
        public int index() {
            return idx;
        }

        /**
         * @return Owning page.
         */
        public DirectMemoryPage owningPage() {
            return owningPage;
        }

        /**
         * @param owningPage Owning page.
         */
        public void owningPage(DirectMemoryPage owningPage) {
            this.owningPage = owningPage;
        }
    }
}
