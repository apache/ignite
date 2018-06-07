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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
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
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginContext;
import org.mockito.Mockito;

import static org.junit.Assert.fail;

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

    /** Last allocated page index. */
    private final AtomicInteger lastPageIdx = new AtomicInteger();

    /** Pages. */
    private final Map<FullPageId, DirectMemoryPage> pages = new ConcurrentHashMap<>();

    /** Page allocator mutex. */
    private final Object pageAllocatorMux = new Object();

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
    private volatile boolean started = false;

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
        this.log = ctx.log(getClass());
        this.gridCtx = ((IgniteEx)ctx.grid()).context();
    }

    /**
     * Creates WAL manager.
     */
    IgniteWriteAheadLogManager createWalManager() {
        if (ctx.igniteConfiguration().getDataStorageConfiguration().getWalMode() == WALMode.FSYNC)
            return new FsyncModeFileWriteAheadLogManager(gridCtx) {
                @Override public WALPointer log(WALRecord record) throws IgniteCheckedException {
                    WALPointer res = super.log(record);

                    applyWalRecord(record);

                    return res;
                }
            };
        else
            return new FileWriteAheadLogManager(gridCtx) {
                @Override public WALPointer log(WALRecord record) throws IgniteCheckedException {
                    WALPointer res = super.log(record);

                    applyWalRecord(record);

                    return res;
                }
            };
    }

    /**
     * Start tracking pages.
     */
    synchronized void start() {
        if (started)
            return;

        pageSize = ctx.igniteConfiguration().getDataStorageConfiguration().getPageSize();
        pageMemoryMock = Mockito.mock(PageMemory.class);

        Mockito.doReturn(pageSize).when(pageMemoryMock).pageSize();

        GridCacheSharedContext sharedCtx = gridCtx.cache().context();

        // Initialize memory region.
        long maxMemorySize = 0;

        for (DataRegion dataRegion : sharedCtx.database().dataRegions()) {
            if (dataRegion.pageMemory() instanceof PageMemoryImpl)
                maxMemorySize += dataRegion.config().getMaxSize();
        }

        long[] chunks = new long[] { maxMemorySize };

        memoryProvider = new UnsafeMemoryProvider(log);

        memoryProvider.initialize(chunks);

        memoryRegion = memoryProvider.nextRegion();

        maxPages = (int)(maxMemorySize / pageSize);

        if (cfg != null && cfg.isCheckPagesOnCheckpoint()) {
            checkpointLsnr = new DbCheckpointListener() {
                @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {
                    if (!checkPages(false))
                        fail("Page memory is inconsistent after applying WAL delta records.");
                }
            };

            ((GridCacheDatabaseSharedManager)gridCtx.cache().context().database()).addCheckpointListener(checkpointLsnr);
        }

        started = true;
    }

    /**
     * Stop tracking, release resources.
     */
    synchronized void stop() {
        if (!started)
            return;

        started = false;

        pages.clear();

        stats.clear();

        lastPageIdx.set(0);

        memoryProvider.shutdown();

        if (checkpointLsnr != null) {
            ((GridCacheDatabaseSharedManager)gridCtx.cache().context().database())
                .removeCheckpointListener(checkpointLsnr);

            checkpointLsnr = null;
        }
    }

    /**
     * Allocates new page for given FullPageId
     *
     * @param fullPageId Full page id.
     */
    private DirectMemoryPage allocatePage(FullPageId fullPageId) {
        synchronized (pageAllocatorMux) {
            // Double check.
            DirectMemoryPage page = pages.get(fullPageId);

            if (page != null)
                return page;

            int pageIdx = lastPageIdx.getAndIncrement();

            if (pageIdx >= maxPages)
                fail("Can't allocate new page");

            long pageAddr = memoryRegion.address() + ((long)pageIdx) * pageSize;

            page = new DirectMemoryPage(pageAddr);

            page.fullPageId(fullPageId);

            pages.put(fullPageId, page);

            return page;
        }
    }

    /**
     * Gets or allocates page for given FullPageId
     *
     * @param fullPageId Full page id.
     * @return Page.
     */
    private DirectMemoryPage page(FullPageId fullPageId) {
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

                // Page corruptor TODO: remove
                if (new Random().nextInt(5000) == 0)
                    GridUnsafe.putByte(page.address() + new Random().nextInt(pageSize),
                        (byte)(new Random().nextInt(256)));
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
     * Checks if there are any differences between the Ignite's data regions content and pages inside the tracker.
     *
     * @param checkAll Check all tracked pages, otherwise check until first error.
     * @return {@code true} if content of all tracked pages equals to content of these pages in the ignite instance.
     */
    public boolean checkPages(boolean checkAll) throws IgniteCheckedException {
        if (!started)
            throw new IgniteCheckedException("Page tracking is not started.");

        GridCacheProcessor cacheProc = gridCtx.cache();

        synchronized (pageAllocatorMux) {
            IgnitePageStoreManager pageStoreMgr = cacheProc.context().pageStore();

            assert pageStoreMgr != null;

            long totalAllocated = pageStoreMgr.pagesAllocated(MetaStorage.METASTORAGE_CACHE_ID);

            for (CacheGroupContext ctx : cacheProc.cacheGroups())
                totalAllocated += pageStoreMgr.pagesAllocated(ctx.groupId());

            long metaId = ((PageMemoryEx)cacheProc.context().database().metaStorage().pageMemory()).metaPageId(
                MetaStorage.METASTORAGE_CACHE_ID);

            // Meta storage meta page is counted as allocated, but never used in current implementation.
            if (!pages.containsKey(new FullPageId(metaId, MetaStorage.METASTORAGE_CACHE_ID))
                && pages.containsKey(new FullPageId(metaId + 1, MetaStorage.METASTORAGE_CACHE_ID)))
                totalAllocated--;

            log.info(">>> Total tracked pages: " + pages.size());
            log.info(">>> Total allocated pages: " + totalAllocated);
        }
        // TODO: How to determine we are started with clear persistence dir

        dumpStats();

        boolean res = true;

        for (DirectMemoryPage page : pages.values()) {
            FullPageId fullPageId = page.fullPageId();

            PageMemory pageMem;

            if (fullPageId.groupId() == MetaStorage.METASTORAGE_CACHE_ID)
                pageMem = cacheProc.context().database().metaStorage().pageMemory();
            else
                pageMem = cacheProc.cacheGroup(fullPageId.groupId()).dataRegion().pageMemory();

            assert pageMem instanceof PageMemoryImpl;

            long rmtPage = pageMem.acquirePage(fullPageId.groupId(), fullPageId.pageId());

            try {
                long rmtPageAddr = pageMem.readLock(fullPageId.groupId(), fullPageId.pageId(), rmtPage);

                try {
                    page.lock();

                    try {
                        if (rmtPageAddr == 0L) {
                            res = false;

                            log.info("Can't lock page: " + fullPageId);

                            dumpHistory(page);
                        }
                        else {
                            ByteBuffer locBuf = GridUnsafe.wrapPointer(page.address(), pageSize);
                            ByteBuffer rmtBuf = GridUnsafe.wrapPointer(rmtPageAddr, pageSize);

                            if (!locBuf.equals(rmtBuf)) {
                                res = false;

                                log.info("Page buffers are not equals: " + fullPageId);

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
        log.info(">>> Diff:");

        for (int i = 0; i < Math.min(buf1.remaining(), buf2.remaining()); i++) {
            byte b1 = buf1.get(buf1.position() + i);
            byte b2 = buf2.get(buf2.position() + i);

            if (b1 != b2)
                log.info(String.format("        0x%04X: %02X %02X", i, b1, b2));
        }

        if (buf1.remaining() < buf2.remaining()) {
            for (int i = buf1.remaining(); i < buf2.remaining(); i++)
                log.info(String.format("        0x%04X:    %02X", i, buf2.get(buf2.position() + i)));
        }
        else if (buf1.remaining() > buf2.remaining()) {
            for (int i = buf2.remaining(); i < buf1.remaining(); i++)
                log.info(String.format("        0x%04X: %02X", i, buf1.get(buf1.position() + i)));
        }
    }

    /**
     * Dump page change history to log.
     *
     * @param page Page.
     */
    private void dumpHistory(DirectMemoryPage page) {
        log.info(">>> Change history:");

        for (WALRecord record : page.changeHistory())
            log.info("        " + record);
    }

    /**
     *
     */
    private static class DirectMemoryPage {
        /** Page address. */
        private final long addr;

        /** Page lock. */
        private final Lock lock = new ReentrantLock();

        /** Change history. */
        private final Queue<WALRecord> changeHist = new LinkedList<>();

        /** Full page id. */
        private volatile FullPageId fullPageId;

        /**
         * @param addr Page address.
         */
        private DirectMemoryPage(long addr) {
            this.addr = addr;
        }

        /**
         * Lock page.
         */
        public void lock() {
            lock.lock();
        }

        /**
         * Unlock page.
         */
        public void unlock() {
            lock.unlock();
        }

        /**
         * @return Page address.
         */
        public long address() {
            return addr;
        }

        /**
         * Change history.
         */
        public Queue<WALRecord> changeHistory() {
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
    }
}
