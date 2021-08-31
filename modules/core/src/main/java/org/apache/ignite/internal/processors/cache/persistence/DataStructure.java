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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.delta.RecycleRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.RotatedIdPartRecord;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMetrics;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIoResolver;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.MAX_PARTITION_ID;
import static org.apache.ignite.internal.pagemem.PageIdUtils.MAX_ITEMID_NUM;

/**
 * Base class for all the data structures based on {@link PageMemory}.
 */
public abstract class DataStructure {
    /** For tests. */
    public static Random rnd;

    /** */
    private final String name;

    /** */
    private final PageLockListener lockLsnr;

    /** */
    protected final int grpId;

    /** */
    protected final String grpName;

    /** */
    protected final PageMemory pageMem;

    /** */
    @Nullable
    protected final IgniteWriteAheadLogManager wal;

    /** */
    protected ReuseList reuseList;

    /** */
    protected final PageIoResolver pageIoRslvr;

    /** */
    protected final byte pageFlag;

    /** */
    protected final PageMetrics metrics;

    /**
     * @param name Structure name (for debugging purposes).
     * @param cacheGrpId Cache group id.
     * @param grpName Group name.
     * @param pageMem Page memory.
     * @param wal Write ahead log manager.
     * @param pageLockTrackerManager Page lock tracker manager.
     * @param pageIoRslvr Page IO resolver.
     * @param pageFlag Default flag value for allocated pages.
     */
    public DataStructure(
        String name,
        int cacheGrpId,
        @Nullable String grpName,
        PageMemory pageMem,
        @Nullable IgniteWriteAheadLogManager wal,
        PageLockTrackerManager pageLockTrackerManager,
        PageIoResolver pageIoRslvr,
        byte pageFlag
    ) {
        assert !F.isEmpty(name);
        assert pageMem != null;

        this.name = name;
        this.grpId = cacheGrpId;
        this.grpName = grpName;
        this.pageMem = pageMem;
        this.wal = wal;
        this.lockLsnr = pageLockTrackerManager.createPageLockTracker(name);
        this.pageIoRslvr = pageIoRslvr;
        this.pageFlag = pageFlag;
        this.metrics = pageMem.metrics().cacheGrpPageMetrics(cacheGrpId);
    }

    /**
     * @return Tree name.
     */
    public final String name() {
        return name;
    }

    /**
     * @return Cache group ID.
     */
    public final int groupId() {
        return grpId;
    }

    /**
     * @param max Max.
     * @return Random value from {@code 0} (inclusive) to the given max value (exclusive).
     */
    public static int randomInt(int max) {
        Random rnd0 = rnd != null ? rnd : ThreadLocalRandom.current();

        return rnd0.nextInt(max);
    }

    /**
     * Shorthand for {@code allocatePage(bag, true)}.
     *
     * @param bag Reuse bag.
     * @return Allocated page.
     * @throws IgniteCheckedException If failed.
     */
    protected final long allocatePage(ReuseBag bag) throws IgniteCheckedException {
        return allocatePage(bag, true);
    }

    /**
     * @param bag Reuse Bag.
     * @param useRecycled Use recycled page.
     * @return Allocated page.
     * @throws IgniteCheckedException If failed.
     */
    protected final long allocatePage(ReuseBag bag, boolean useRecycled) throws IgniteCheckedException {
        long pageId = 0;

        if (useRecycled && reuseList != null) {
            pageId = bag != null ? bag.pollFreePage() : 0;

            if (pageId == 0)
                pageId = reuseList.takeRecycledPage();

            // Recycled. "pollFreePage" result should be reinitialized to move rotatedId to itemId.
            if (pageId != 0)
                pageId = reuseList.initRecycledPage(pageId, pageFlag, null);
        }

        if (pageId == 0)
            pageId = allocatePageNoReuse();

        assert pageId != 0;

        assert PageIdUtils.flag(pageId) == FLAG_IDX && PageIdUtils.partId(pageId) == INDEX_PARTITION ||
            PageIdUtils.flag(pageId) != FLAG_IDX && PageIdUtils.partId(pageId) <= MAX_PARTITION_ID :
            PageIdUtils.toDetailString(pageId);

        assert PageIdUtils.flag(pageId) != FLAG_DATA || PageIdUtils.itemId(pageId) == 0 : PageIdUtils.toDetailString(pageId);

        return pageId;
    }

    /**
     * @return Page ID of newly allocated page.
     * @throws IgniteCheckedException If failed.
     */
    protected long allocatePageNoReuse() throws IgniteCheckedException {
        return pageMem.allocatePage(grpId, PageIdAllocator.INDEX_PARTITION, FLAG_IDX);
    }

    /**
     * @param pageId Page ID.
     * @param statHolder Statistics holder to track IO operations.
     * @return Page absolute pointer.
     * @throws IgniteCheckedException If failed.
     */
    protected final long acquirePage(long pageId, IoStatisticsHolder statHolder) throws IgniteCheckedException {
        assert PageIdUtils.flag(pageId) == FLAG_IDX && PageIdUtils.partId(pageId) == INDEX_PARTITION ||
            PageIdUtils.flag(pageId) != FLAG_IDX && PageIdUtils.partId(pageId) <= MAX_PARTITION_ID :
            U.hexLong(pageId) + " flag=" + PageIdUtils.flag(pageId) + " part=" + PageIdUtils.partId(pageId);

        return pageMem.acquirePage(grpId, pageId, statHolder);
    }

    /**
     * @param pageId Page ID.
     * @param page Page pointer.
     */
    protected final void releasePage(long pageId, long page) {
        pageMem.releasePage(grpId, pageId, page);
    }

    /**
     * @param pageId Page ID
     * @param page Page pointer.
     * @return Page address or {@code 0} if failed to lock due to recycling.
     */
    protected final long tryWriteLock(long pageId, long page) {
        return PageHandler.writeLock(pageMem, grpId, pageId, page, lockLsnr, true);
    }

    /**
     * @param pageId Page ID
     * @param page Page pointer.
     * @return Page address.
     */
    protected final long writeLock(long pageId, long page) {
        return PageHandler.writeLock(pageMem, grpId, pageId, page, lockLsnr, false);
    }

    /**
     * <p> Note: Default WAL record policy will be used. </p>
     *
     * @param pageId Page ID
     * @param page Page pointer.
     * @param pageAddr Page address.
     * @param dirty Dirty flag.
     */
    protected final void writeUnlock(long pageId, long page, long pageAddr, boolean dirty) {
        writeUnlock(pageId, page, pageAddr, null, dirty);
    }

    /**
     * @param pageId Page ID
     * @param page Page pointer.
     * @return Page address.
     */
    protected final long readLock(long pageId, long page) {
        return PageHandler.readLock(pageMem, grpId, pageId, page, lockLsnr);
    }

    /**
     * @param pageId Page ID
     * @param page Page pointer.
     * @param pageAddr Page address.
     */
    protected final void readUnlock(long pageId, long page, long pageAddr) {
        PageHandler.readUnlock(pageMem, grpId, pageId, page, pageAddr, lockLsnr);
    }

    /**
     * @param pageId Page ID
     * @param page Page pointer.
     * @param pageAddr Page address.
     * @param walPlc Full page WAL record policy.
     * @param dirty Dirty flag.
     */
    protected final void writeUnlock(long pageId, long page, long pageAddr, Boolean walPlc, boolean dirty) {
        PageHandler.writeUnlock(pageMem, grpId, pageId, page, pageAddr, lockLsnr, walPlc, dirty);
    }

    /**
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param walPlc Full page WAL record policy.
     * @return {@code true} If we need to make a delta WAL record for the change in this page.
     */
    protected final boolean needWalDeltaRecord(long pageId, long page, Boolean walPlc) {
        return PageHandler.isWalDeltaRecordNeeded(pageMem, grpId, pageId, page, wal, walPlc);
    }

    /**
     * @param pageId Page ID.
     * @param h Handler.
     * @param intArg Argument of type {@code int}.
     * @param lockFailed Result in case of lock failure due to page recycling.
     * @param statHolder Statistics holder to track IO operations.
     * @return Handler result.
     * @throws IgniteCheckedException If failed.
     */
    protected final <R> R write(
        long pageId,
        PageHandler<?, R> h,
        int intArg,
        R lockFailed,
        IoStatisticsHolder statHolder) throws IgniteCheckedException {
        return PageHandler.writePage(pageMem, grpId, pageId, lockLsnr, h,
            null, null, null, null, intArg, lockFailed, statHolder, pageIoRslvr);
    }

    /**
     * @param pageId Page ID.
     * @param h Handler.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @param lockFailed Result in case of lock failure due to page recycling.
     * @param statHolder Statistics holder to track IO operations.
     * @return Handler result.
     * @throws IgniteCheckedException If failed.
     */
    protected final <X, R> R write(
        long pageId,
        PageHandler<X, R> h,
        X arg,
        int intArg,
        R lockFailed,
        IoStatisticsHolder statHolder) throws IgniteCheckedException {
        return PageHandler.writePage(pageMem, grpId, pageId, lockLsnr, h,
            null, null, null, arg, intArg, lockFailed, statHolder, pageIoRslvr);
    }

    /**
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param h Handler.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @param lockFailed Result in case of lock failure due to page recycling.
     * @param statHolder Statistics holder to track IO operations.
     * @return Handler result.
     * @throws IgniteCheckedException If failed.
     */
    protected final <X, R> R write(
        long pageId,
        long page,
        PageHandler<X, R> h,
        X arg,
        int intArg,
        R lockFailed,
        IoStatisticsHolder statHolder) throws IgniteCheckedException {
        return PageHandler.writePage(pageMem, grpId, pageId, page, lockLsnr, h,
            null, null, null, arg, intArg, lockFailed, statHolder, pageIoRslvr);
    }

    /**
     * @param pageId Page ID.
     * @param h Handler.
     * @param init IO for new page initialization or {@code null} if it is an existing page.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @param lockFailed Result in case of lock failure due to page recycling.
     * @param statHolder Statistics holder to track IO operations.
     * @return Handler result.
     * @throws IgniteCheckedException If failed.
     */
    protected final <X, R> R write(
        long pageId,
        PageHandler<X, R> h,
        PageIO init,
        X arg,
        int intArg,
        R lockFailed,
        IoStatisticsHolder statHolder) throws IgniteCheckedException {
        return PageHandler.writePage(pageMem, grpId, pageId, lockLsnr, h,
            init, wal, null, arg, intArg, lockFailed, statHolder, pageIoRslvr);
    }

    /**
     * @param pageId Page ID.
     * @param h Handler.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @param lockFailed Result in case of lock failure due to page recycling.
     * @param statHolder Statistics holder to track IO operations.
     * @return Handler result.
     * @throws IgniteCheckedException If failed.
     */
    protected final <X, R> R read(
        long pageId,
        PageHandler<X, R> h,
        X arg,
        int intArg,
        R lockFailed,
        IoStatisticsHolder statHolder) throws IgniteCheckedException {
        return PageHandler.readPage(pageMem, grpId, pageId, lockLsnr,
            h, arg, intArg, lockFailed, statHolder, pageIoRslvr);
    }

    /**
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param h Handler.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @param lockFailed Result in case of lock failure due to page recycling.
     * @param statHolder Statistics holder to track IO operations.
     * @return Handler result.
     * @throws IgniteCheckedException If failed.
     */
    protected final <X, R> R read(
        long pageId,
        long page,
        PageHandler<X, R> h,
        X arg,
        int intArg,
        R lockFailed,
        IoStatisticsHolder statHolder) throws IgniteCheckedException {
        return PageHandler.readPage(pageMem, grpId, pageId, page, lockLsnr, h,
            arg, intArg, lockFailed, statHolder, pageIoRslvr);
    }

    /**
     * @param pageId Page ID.
     * @param init IO for new page initialization.
     * @throws IgniteCheckedException if failed.
     */
    protected final void init(long pageId, PageIO init) throws IgniteCheckedException {
        PageHandler.initPage(pageMem, grpId, pageId, init, wal, lockLsnr, IoStatisticsHolderNoOp.INSTANCE);
    }

    /**
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param pageAddr Page address.
     * @param walPlc Full page WAL record policy.
     * @return Recycled page ID.
     * @throws IgniteCheckedException If failed.
     */
    protected final long recyclePage(
        long pageId,
        long page,
        long pageAddr,
        Boolean walPlc) throws IgniteCheckedException {
        long recycled = 0;

        boolean needWalDeltaRecord = needWalDeltaRecord(pageId, page, walPlc);

        if (PageIdUtils.flag(pageId) == FLAG_DATA) {
            int rotatedIdPart = PageIO.getRotatedIdPart(pageAddr);

            if (rotatedIdPart != 0) {
                recycled = PageIdUtils.link(pageId, rotatedIdPart);

                PageIO.setRotatedIdPart(pageAddr, 0);

                if (needWalDeltaRecord)
                    wal.log(new RotatedIdPartRecord(grpId, pageId, 0));
            }
        }

        if (recycled == 0)
            recycled = PageIdUtils.rotatePageId(pageId);

        assert PageIdUtils.itemId(recycled) > 0 && PageIdUtils.itemId(recycled) <= MAX_ITEMID_NUM : U.hexLong(recycled);

        PageIO.setPageId(pageAddr, recycled);

        if (needWalDeltaRecord)
            wal.log(new RecycleRecord(grpId, pageId, recycled));

        if (PageIO.isIndexPage(PageIO.getType(pageAddr)))
            metrics.indexPages().decrement();

        return recycled;
    }

    /**
     * @return Page size without encryption overhead.
     */
    protected int pageSize() {
        return pageMem.realPageSize(grpId);
    }

    /**
     * Frees the resources allocated by this structure.
     */
    public void close() {
        lockLsnr.close();
    }
}
