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

package org.apache.ignite.internal.processors.cache.persistence.freelist;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageSetFreeListPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.InitNewPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PageListMetaResetCountRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PagesListAddPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PagesListInitNewPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PagesListRemovePageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PagesListSetNextRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PagesListSetPreviousRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.RotatedIdPartRecord;
import org.apache.ignite.internal.processors.cache.persistence.DataStructure;
import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListNodeIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.internal.util.GridArrays;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdUtils.MAX_ITEMID_NUM;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getPageId;

/**
 * Striped doubly-linked list of page IDs optionally organized in buckets.
 */
public abstract class PagesList extends DataStructure {
    /** */
    private static final int TRY_LOCK_ATTEMPTS =
            IgniteSystemProperties.getInteger("IGNITE_PAGES_LIST_TRY_LOCK_ATTEMPTS", 10);

    /** */
    private static final int MAX_STRIPES_PER_BUCKET =
        IgniteSystemProperties.getInteger("IGNITE_PAGES_LIST_STRIPES_PER_BUCKET",
            Math.max(8, Runtime.getRuntime().availableProcessors()));

    /** */
    private final boolean pagesListCachingDisabledSysProp =
        IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_PAGES_LIST_DISABLE_ONHEAP_CACHING, false);

    /** */
    protected final AtomicLongArray bucketsSize;

    /** */
    protected volatile boolean changed;

    /** Page ID to store list metadata. */
    private final long metaPageId;

    /** Number of buckets. */
    private final int buckets;

    /** Name (for debug purposes). */
    protected final String name;

    /** Flag to enable/disable onheap list caching. */
    private volatile boolean onheapListCachingEnabled;

    /** */
    private final PageHandler<Void, Boolean> cutTail = new CutTail();

    /** */
    private final PageHandler<Void, Boolean> putBucket = new PutBucket();

    /** Logger. */
    protected final IgniteLogger log;

    /**
     *
     */
    private final class CutTail extends PageHandler<Void, Boolean> {
        @Override public Boolean run(
            int cacheId,
            long pageId,
            long page,
            long pageAddr,
            PageIO iox,
            Boolean walPlc,
            Void ignore,
            int bucket,
            IoStatisticsHolder statHolder) throws IgniteCheckedException {
            assert getPageId(pageAddr) == pageId;

            PagesListNodeIO io = (PagesListNodeIO)iox;

            long tailId = io.getNextId(pageAddr);

            assert tailId != 0;

            io.setNextId(pageAddr, 0L);

            if (needWalDeltaRecord(pageId, page, walPlc))
                wal.log(new PagesListSetNextRecord(cacheId, pageId, 0L));

            updateTail(bucket, tailId, pageId);

            return TRUE;
        }
    }

    /**
     *
     */
    private final class PutBucket extends PageHandler<Void, Boolean> {
        /** {@inheritDoc} */
        @Override public Boolean run(
            int cacheId,
            long pageId,
            long page,
            long pageAddr,
            PageIO iox,
            Boolean walPlc,
            Void ignore,
            int oldBucket,
            IoStatisticsHolder statHolder
        ) throws IgniteCheckedException {
            decrementBucketSize(oldBucket);

            // Recalculate bucket because page free space can be changed concurrently.
            int freeSpace = ((AbstractDataPageIO)iox).getFreeSpace(pageAddr);

            int newBucket = getBucketIndex(freeSpace);

            if (newBucket != oldBucket && log.isDebugEnabled()) {
                log.debug("Bucket changed when moving from heap to PageMemory [list=" + name + ", oldBucket=" + oldBucket +
                    ", newBucket=" + newBucket + ", pageId=" + pageId + ']');
            }

            if (newBucket >= 0)
                put(null, pageId, page, pageAddr, newBucket, statHolder);

            return TRUE;
        }
    }

    /**
     * @param cacheId Cache ID.
     * @param name Name (for debug purpose).
     * @param pageMem Page memory.
     * @param buckets Number of buckets.
     * @param wal Write ahead log manager.
     * @param metaPageId Metadata page ID.
     */
    protected PagesList(
        int cacheId,
        String name,
        PageMemory pageMem,
        int buckets,
        IgniteWriteAheadLogManager wal,
        long metaPageId,
        PageLockListener lockLsnr,
        GridKernalContext ctx
    ) {
        super(cacheId, null, pageMem, wal, lockLsnr);

        this.name = name;
        this.buckets = buckets;
        this.metaPageId = metaPageId;

        onheapListCachingEnabled = isCachingApplicable();

        log = ctx.log(PagesList.class);

        bucketsSize = new AtomicLongArray(buckets);
    }

    /**
     * @param metaPageId Metadata page ID.
     * @param initNew {@code True} if new list if created, {@code false} if should be initialized from metadata.
     * @throws IgniteCheckedException If failed.
     */
    protected final void init(long metaPageId, boolean initNew) throws IgniteCheckedException {
        if (metaPageId != 0L) {
            if (initNew)
                init(metaPageId, PagesListMetaIO.VERSIONS.latest());
            else {
                Map<Integer, GridLongList> bucketsData = new HashMap<>();

                long nextId = metaPageId;

                while (nextId != 0) {
                    final long pageId = nextId;
                    final long page = acquirePage(pageId, IoStatisticsHolderNoOp.INSTANCE);

                    try {
                        long pageAddr = readLock(pageId, page); // No concurrent recycling on init.

                        assert pageAddr != 0L;

                        try {
                            PagesListMetaIO io = PagesListMetaIO.VERSIONS.forPage(pageAddr);

                            io.getBucketsData(pageAddr, bucketsData);

                            nextId = io.getNextMetaPageId(pageAddr);

                            assert nextId != pageId :
                                "Loop detected [next=" + U.hexLong(nextId) + ", cur=" + U.hexLong(pageId) + ']';
                        }
                        finally {
                            readUnlock(pageId, page, pageAddr);
                        }
                    }
                    finally {
                        releasePage(pageId, page);
                    }
                }

                for (Map.Entry<Integer, GridLongList> e : bucketsData.entrySet()) {
                    int bucket = e.getKey();
                    long bucketSize = 0;

                    Stripe[] old = getBucket(bucket);
                    assert old == null;

                    long[] upd = e.getValue().array();

                    Stripe[] tails = new Stripe[upd.length];

                    for (int i = 0; i < upd.length; i++) {
                        long tailId = upd[i];

                        long prevId = tailId;
                        int cnt = 0;

                        while (prevId != 0L) {
                            final long pageId = prevId;
                            final long page = acquirePage(pageId, IoStatisticsHolderNoOp.INSTANCE);
                            try {
                                long pageAddr = readLock(pageId, page);

                                assert pageAddr != 0L;

                                try {
                                    PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(pageAddr);

                                    cnt += io.getCount(pageAddr);
                                    prevId = io.getPreviousId(pageAddr);

                                    // In reuse bucket the page itself can be used as a free page.
                                    if (isReuseBucket(bucket) && prevId != 0L)
                                        cnt++;
                                }
                                finally {
                                    readUnlock(pageId, page, pageAddr);
                                }
                            }
                            finally {
                                releasePage(pageId, page);
                            }
                        }

                        Stripe stripe = new Stripe(tailId, cnt == 0);
                        tails[i] = stripe;
                        bucketSize += cnt;
                    }

                    boolean ok = casBucket(bucket, null, tails);

                    assert ok;

                    bucketsSize.set(bucket, bucketSize);
                }
            }
        }
    }

    /**
     * @return {@code True} if onheap caching is applicable for this pages list. {@code False} if caching is disabled
     * explicitly by system property or if page list belongs to in-memory data region (in this case onheap caching
     * makes no sense).
     */
    private boolean isCachingApplicable() {
        return !pagesListCachingDisabledSysProp && (wal != null);
    }

    /**
     * Save metadata without exclusive lock on it.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void saveMetadata(IoStatisticsHolder statHolder) throws IgniteCheckedException {
        long nextPageId = metaPageId;

        assert nextPageId != 0;

        flushBucketsCache(statHolder);

        if (!changed)
            return;

        // This guaranteed that any concurrently changes of list will be detected.
        changed = false;

        try {
            long unusedPageId = writeFreeList(nextPageId);

            markUnusedPagesDirty(unusedPageId);
        }
        catch (Throwable e) {
            changed = true; // Return changed flag due to exception.

            throw e;
        }
    }

    /**
     * Flush onheap cached pages lists to page memory.
     */
    private void flushBucketsCache(IoStatisticsHolder statHolder) throws IgniteCheckedException {
        if (!isCachingApplicable())
            return;

        onheapListCachingEnabled = false;

        try {
            for (int bucket = 0; bucket < buckets; bucket++) {
                PagesCache pagesCache = getBucketCache(bucket, false);

                if (pagesCache == null)
                    continue;

                GridLongList pages = pagesCache.flush();

                if (pages != null) {
                    for (int i = 0; i < pages.size(); i++) {
                        long pageId = pages.get(i);

                        if (log.isDebugEnabled()) {
                            log.debug("Move page from heap to PageMemory [list=" + name + ", bucket=" + bucket +
                                ", pageId=" + pageId + ']');
                        }

                        Boolean res = write(pageId, putBucket, bucket, null, statHolder);

                        if (res == null) {
                            // Return page to onheap pages list if can't lock it.
                            pagesCache.add(pageId);
                        }
                    }
                }
            }
        }
        finally {
            onheapListCachingEnabled = true;
        }
    }

    /**
     * Write free list data to page memory.
     *
     * @param nextPageId First free page id.
     * @return Unused free list page id.
     * @throws IgniteCheckedException If failed.
     */
    private long writeFreeList(long nextPageId) throws IgniteCheckedException {
        long curId = 0L;
        long curPage = 0L;
        long curAddr = 0L;

        PagesListMetaIO curIo = null;

        try {
            for (int bucket = 0; bucket < buckets; bucket++) {
                Stripe[] tails = getBucket(bucket);

                if (tails != null) {
                    int tailIdx = 0;

                    while (tailIdx < tails.length) {
                        int written = curPage != 0L ?
                            curIo.addTails(pageMem.realPageSize(grpId), curAddr, bucket, tails, tailIdx) :
                            0;

                        if (written == 0) {
                            if (nextPageId == 0L) {
                                nextPageId = allocatePageNoReuse();

                                if (curPage != 0L) {
                                    curIo.setNextMetaPageId(curAddr, nextPageId);

                                    releaseAndClose(curId, curPage, curAddr);
                                }

                                curId = nextPageId;
                                curPage = acquirePage(curId, IoStatisticsHolderNoOp.INSTANCE);
                                curAddr = writeLock(curId, curPage);

                                curIo = PagesListMetaIO.VERSIONS.latest();

                                curIo.initNewPage(curAddr, curId, pageSize());
                            }
                            else {
                                releaseAndClose(curId, curPage, curAddr);

                                curId = nextPageId;
                                curPage = acquirePage(curId, IoStatisticsHolderNoOp.INSTANCE);
                                curAddr = writeLock(curId, curPage);

                                curIo = PagesListMetaIO.VERSIONS.forPage(curAddr);

                                curIo.resetCount(curAddr);
                            }

                            nextPageId = curIo.getNextMetaPageId(curAddr);
                        }
                        else
                            tailIdx += written;
                    }
                }
            }
        }
        finally {
            releaseAndClose(curId, curPage, curAddr);
        }

        return nextPageId;
    }

    /**
     * Mark unused pages as dirty.
     *
     * @param nextPageId First unused page.
     * @throws IgniteCheckedException If failed.
     */
    private void markUnusedPagesDirty(long nextPageId) throws IgniteCheckedException {
        while (nextPageId != 0L) {
            long pageId = nextPageId;

            long page = acquirePage(pageId, IoStatisticsHolderNoOp.INSTANCE);
            try {
                long pageAddr = writeLock(pageId, page);

                try {
                    PagesListMetaIO io = PagesListMetaIO.VERSIONS.forPage(pageAddr);

                    io.resetCount(pageAddr);

                    if (needWalDeltaRecord(pageId, page, null))
                        wal.log(new PageListMetaResetCountRecord(grpId, pageId));

                    nextPageId = io.getNextMetaPageId(pageAddr);
                }
                finally {
                    writeUnlock(pageId, page, pageAddr, true);
                }
            }
            finally {
                releasePage(pageId, page);
            }
        }
    }

    /**
     * @param pageId Page ID.
     * @param page Page absolute pointer.
     * @param pageAddr Page address.
     */
    private void releaseAndClose(long pageId, long page, long pageAddr) {
        if (page != 0L) {
            try {
                // No special WAL record because we most likely changed the whole page.
                writeUnlock(pageId, page, pageAddr, TRUE, true);
            }
            finally {
                releasePage(pageId, page);
            }
        }
    }

    /**
     * Gets bucket index by page freespace.
     *
     * @return Bucket index or -1 if page doesn't belong to any bucket.
     */
    protected abstract int getBucketIndex(int freeSpace);

    /**
     * @param bucket Bucket index.
     * @return Bucket.
     */
    protected abstract Stripe[] getBucket(int bucket);

    /**
     * @param bucket Bucket index.
     * @param exp Expected bucket.
     * @param upd Updated bucket.
     * @return {@code true} If succeeded.
     */
    protected abstract boolean casBucket(int bucket, Stripe[] exp, Stripe[] upd);

    /**
     * @param bucket Bucket index.
     * @return {@code true} If it is a reuse bucket.
     */
    protected abstract boolean isReuseBucket(int bucket);

    /**
     * @param bucket Bucket index.
     * @return Bucket cache.
     */
    protected abstract PagesCache getBucketCache(int bucket, boolean create);

    /**
     * @param io IO.
     * @param prevId Previous page ID.
     * @param prev Previous page buffer.
     * @param nextId Next page ID.
     * @param next Next page buffer.
     */
    private void setupNextPage(PagesListNodeIO io, long prevId, long prev, long nextId, long next) {
        assert io.getNextId(prev) == 0L;

        io.initNewPage(next, nextId, pageSize());
        io.setPreviousId(next, prevId);

        io.setNextId(prev, nextId);
    }

    /**
     * Adds stripe to the given bucket.
     *
     * @param bucket Bucket.
     * @param bag Reuse bag.
     * @param reuse {@code True} if possible to use reuse list.
     * @throws IgniteCheckedException If failed.
     * @return Tail page ID.
     */
    private Stripe addStripe(int bucket, ReuseBag bag, boolean reuse) throws IgniteCheckedException {
        long pageId = allocatePage(bag, reuse);

        init(pageId, PagesListNodeIO.VERSIONS.latest());

        Stripe stripe = new Stripe(pageId, true);

        for (; ; ) {
            Stripe[] old = getBucket(bucket);
            Stripe[] upd;

            if (old != null) {
                int len = old.length;

                upd = Arrays.copyOf(old, len + 1);

                upd[len] = stripe;
            }
            else
                upd = new Stripe[] {stripe};

            if (casBucket(bucket, old, upd)) {
                changed();

                return stripe;
            }
        }
    }

    /**
     * @param bucket Bucket index.
     * @param oldTailId Old tail page ID to replace.
     * @param newTailId New tail page ID.
     * @return {@code True} if stripe was removed.
     */
    private boolean updateTail(int bucket, long oldTailId, long newTailId) {
        int idx = -1;

        try {
            for (; ; ) {
                Stripe[] tails = getBucket(bucket);

                if (log.isDebugEnabled()) {
                    log.debug("Update tail [list=" + name + ", bucket=" + bucket + ", oldTailId=" + oldTailId +
                        ", newTailId=" + newTailId + ", tails=" + Arrays.toString(tails));
                }

                // Tail must exist to be updated.
                assert !F.isEmpty(tails) : "Missing tails [bucket=" + bucket + ", tails=" + Arrays.toString(tails) +
                    ", metaPage=" + U.hexLong(metaPageId) + ']';

                idx = findTailIndex(tails, oldTailId, idx);

                assert tails[idx].tailId == oldTailId;

                if (newTailId == 0L) {
                    if (tails.length <= MAX_STRIPES_PER_BUCKET / 2) {
                        tails[idx].empty = true;

                        return false;
                    }

                    Stripe[] newTails;

                    if (tails.length != 1)
                        newTails = GridArrays.remove(tails, idx);
                    else
                        newTails = null; // Drop the bucket completely.

                    if (casBucket(bucket, tails, newTails)) {
                        // Reset tailId for invalidation of locking when stripe was taken concurrently.
                        tails[idx].tailId = 0L;

                        return true;
                    }
                }
                else {
                    // It is safe to assign new tail since we do it only when write lock on tail is held.
                    tails[idx].tailId = newTailId;

                    return true;
                }
            }
        }
        finally {
            changed();
        }
    }

    /**
     * @param tails Tails.
     * @param tailId Tail ID to find.
     * @param expIdx Expected index.
     * @return First found index of the given tail ID.
     */
    private static int findTailIndex(Stripe[] tails, long tailId, int expIdx) {
        if (expIdx != -1 && tails.length > expIdx && tails[expIdx].tailId == tailId)
            return expIdx;

        for (int i = 0; i < tails.length; i++) {
            if (tails[i].tailId == tailId)
                return i;
        }

        throw new IllegalStateException("Tail not found: " + tailId);
    }

    /**
     * @param bucket Bucket.
     * @param bag Reuse bag.
     * @return Page ID where the given page
     * @throws IgniteCheckedException If failed.
     */
    private Stripe getPageForPut(int bucket, ReuseBag bag) throws IgniteCheckedException {
        // Striped pool optimization.
        IgniteThread igniteThread = IgniteThread.current();

        Stripe[] tails = getBucket(bucket);

        if (igniteThread != null && igniteThread.policy() == GridIoPolicy.DATA_STREAMER_POOL) {
            int stripeIdx = igniteThread.stripe();

            assert stripeIdx != -1 : igniteThread;

            while (tails == null || stripeIdx >= tails.length) {
                addStripe(bucket, bag, true);

                tails = getBucket(bucket);
            }

            return tails[stripeIdx];
        }

        if (tails == null)
            return addStripe(bucket, bag, true);

        return randomTail(tails);
    }

    /**
     * @param tails Tails.
     * @return Random tail.
     */
    private static Stripe randomTail(Stripe[] tails) {
        int len = tails.length;

        assert len != 0;

        return tails[randomInt(len)];
    }

    /**
     * !!! For tests only, does not provide any correctness guarantees for concurrent access.
     *
     * @param bucket Bucket index.
     * @return Number of pages stored in this list.
     * @throws IgniteCheckedException If failed.
     */
    protected final long storedPagesCount(int bucket) throws IgniteCheckedException {
        long res = 0;

        Stripe[] tails = getBucket(bucket);

        if (tails != null) {
            for (Stripe tail : tails) {
                long tailId = tail.tailId;

                while (tailId != 0L) {
                    final long pageId = tailId;
                    final long page = acquirePage(pageId, IoStatisticsHolderNoOp.INSTANCE);
                    try {
                        long pageAddr = readLock(pageId, page);

                        assert pageAddr != 0L;

                        try {
                            PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(pageAddr);

                            int cnt = io.getCount(pageAddr);

                            assert cnt >= 0;

                            res += cnt;
                            tailId = io.getPreviousId(pageAddr);

                            // In reuse bucket the page itself can be used as a free page.
                            if (isReuseBucket(bucket) && tailId != 0L)
                                res++;
                        }
                        finally {
                            readUnlock(pageId, page, pageAddr);
                        }
                    }
                    finally {
                        releasePage(pageId, page);
                    }
                }
            }
        }

        assert res == bucketsSize.get(bucket) : "Wrong bucket size counter [exp=" + res + ", cntr=" + bucketsSize.get(bucket) + ']';

        return res;
    }

    /**
     * @param bag Reuse bag.
     * @param dataId Data page ID.
     * @param dataPage Data page pointer.
     * @param dataAddr Data page address.
     * @param bucket Bucket.
     * @param statHolder Statistics holder to track IO operations.
     * @throws IgniteCheckedException If failed.
     */
    protected final void put(
        @Nullable ReuseBag bag,
        final long dataId,
        final long dataPage,
        final long dataAddr,
        int bucket,
        IoStatisticsHolder statHolder)
        throws IgniteCheckedException {
        assert bag == null ^ dataAddr == 0L;

        if (bag != null && bag.isEmpty()) // Skip allocating stripe for empty bag.
            return;

        if (bag == null && onheapListCachingEnabled &&
            putDataPage(getBucketCache(bucket, true), dataId, dataPage, dataAddr, bucket)) {
            // Successfully put page to the onheap pages list cache.
            if (log.isDebugEnabled()) {
                log.debug("Put page to pages list cache [list=" + name + ", bucket=" + bucket +
                    ", dataId=" + dataId + ']');
            }

            return;
        }

        for (int lockAttempt = 0; ;) {
            Stripe stripe = getPageForPut(bucket, bag);

            // No need to continue if bag has been utilized at getPageForPut (free page can be used for pagelist).
            if (bag != null && bag.isEmpty())
                return;

            final long tailId = stripe.tailId;

            // Stripe was removed from bucket concurrently.
            if (tailId == 0L)
                continue;

            final long tailPage = acquirePage(tailId, statHolder);

            try {
                long tailAddr = writeLockPage(tailId, tailPage, bucket, lockAttempt++, bag); // Explicit check.

                if (tailAddr == 0L) {
                    // No need to continue if bag has been utilized at writeLockPage.
                    if (bag != null && bag.isEmpty())
                        return;
                    else
                        continue;
                }

                if (stripe.tailId != tailId) {
                    // Another thread took the last page.
                    writeUnlock(tailId, tailPage, tailAddr, false);

                    lockAttempt--; // Ignore current attempt.

                    continue;
                }

                assert PageIO.getPageId(tailAddr) == tailId
                    : "tailId = " + U.hexLong(tailId) + ", pageId = " + U.hexLong(PageIO.getPageId(tailAddr));
                assert PageIO.getType(tailAddr) == PageIO.T_PAGE_LIST_NODE
                    : "tailId = " + U.hexLong(tailId) + ", type = " + PageIO.getType(tailAddr);

                boolean ok = false;

                try {
                    PagesListNodeIO io = PageIO.getPageIO(tailAddr);

                    ok = bag != null ?
                        // Here we can always take pages from the bag to build our list.
                        putReuseBag(tailId, tailPage, tailAddr, io, bag, bucket, statHolder) :
                        // Here we can use the data page to build list only if it is empty and
                        // it is being put into reuse bucket. Usually this will be true, but there is
                        // a case when there is no reuse bucket in the free list, but then deadlock
                        // on node page allocation from separate reuse list is impossible.
                        // If the data page is not empty it can not be put into reuse bucket and thus
                        // the deadlock is impossible as well.
                        putDataPage(tailId, tailPage, tailAddr, io, dataId, dataPage, dataAddr, bucket, statHolder);

                    if (ok) {
                        if (log.isDebugEnabled()) {
                            log.debug("Put page to pages list [list=" + name + ", bucket=" + bucket +
                                ", dataId=" + dataId + ", tailId=" + tailId + ']');
                        }

                        stripe.empty = false;

                        return;
                    }
                }
                finally {
                    writeUnlock(tailId, tailPage, tailAddr, ok);
                }
            }
            finally {
                releasePage(tailId, tailPage);
            }
        }
    }

    /**
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param pageAddr Page address.
     * @param io IO.
     * @param dataId Data page ID.
     * @param dataPage Data page pointer.
     * @param dataAddr Data page address.
     * @param bucket Bucket.
     * @param statHolder Statistics holder to track IO operations.
     * @return {@code true} If succeeded.
     * @throws IgniteCheckedException If failed.
     */
    private boolean putDataPage(
        final long pageId,
        final long page,
        final long pageAddr,
        PagesListNodeIO io,
        final long dataId,
        final long dataPage,
        final long dataAddr,
        int bucket,
        IoStatisticsHolder statHolder
    ) throws IgniteCheckedException {
        if (io.getNextId(pageAddr) != 0L)
            return false; // Splitted.

        int idx = io.addPage(pageAddr, dataId, pageSize());

        if (idx == -1)
            handlePageFull(pageId, page, pageAddr, io, dataId, dataPage, dataAddr, bucket, statHolder);
        else {
            incrementBucketSize(bucket);

            if (needWalDeltaRecord(pageId, page, null))
                wal.log(new PagesListAddPageRecord(grpId, pageId, dataId));

            AbstractDataPageIO dataIO = PageIO.getPageIO(dataAddr);
            dataIO.setFreeListPageId(dataAddr, pageId);

            if (needWalDeltaRecord(dataId, dataPage, null))
                wal.log(new DataPageSetFreeListPageRecord(grpId, dataId, pageId));
        }

        return true;
    }

    /**
     * @param dataId Data page ID.
     * @param dataPage Data page pointer.
     * @param dataAddr Data page address.
     * @param bucket Bucket.
     * @return {@code true} If succeeded.
     * @throws IgniteCheckedException If failed.
     */
    private boolean putDataPage(
        PagesCache pagesCache,
        final long dataId,
        final long dataPage,
        final long dataAddr,
        int bucket
    ) throws IgniteCheckedException {
        if (pagesCache.add(dataId)) {
            incrementBucketSize(bucket);

            AbstractDataPageIO dataIO = PageIO.getPageIO(dataAddr);

            if (dataIO.getFreeListPageId(dataAddr) != 0L) {
                dataIO.setFreeListPageId(dataAddr, 0L);

                // Actually, there is no real need for this WAL record, but it has relatively low cost and provides
                // anytime consistency between page memory and WAL (without this record WAL is consistent with
                // page memory only at the time of checkpoint, but it doesn't affect recovery guarantees).
                if (needWalDeltaRecord(dataId, dataPage, null))
                    wal.log(new DataPageSetFreeListPageRecord(grpId, dataId, 0L));
            }

            return true;
        }
        else
            return false;
    }

    /**
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param pageAddr Page address.
     * @param io IO.
     * @param dataId Data page ID.
     * @param data Data page pointer.
     * @param dataAddr Data page address.
     * @param bucket Bucket index.
     * @param statHolder Statistics holder to track IO operations.
     * @throws IgniteCheckedException If failed.
     * */
    private void handlePageFull(
        final long pageId,
        final long page,
        final long pageAddr,
        PagesListNodeIO io,
        final long dataId,
        final long data,
        final long dataAddr,
        int bucket,
        IoStatisticsHolder statHolder
    ) throws IgniteCheckedException {
        AbstractDataPageIO dataIO = PageIO.getPageIO(dataAddr);

        // Attempt to add page failed: the node page is full.
        if (isReuseBucket(bucket)) {
            // If we are on the reuse bucket, we can not allocate new page, because it may cause deadlock.
            assert dataIO.isEmpty(dataAddr); // We can put only empty data pages to reuse bucket.

            // Change page type to index and add it as next node page to this list.
            long newDataId = PageIdUtils.changeType(dataId, FLAG_IDX);

            setupNextPage(io, pageId, pageAddr, newDataId, dataAddr);

            if (needWalDeltaRecord(pageId, page, null))
                wal.log(new PagesListSetNextRecord(grpId, pageId, newDataId));

            if (needWalDeltaRecord(dataId, data, null))
                wal.log(new PagesListInitNewPageRecord(
                    grpId,
                    dataId,
                    io.getType(),
                    io.getVersion(),
                    newDataId,
                    pageId, 0L));

            // In reuse bucket the page itself can be used as a free page.
            incrementBucketSize(bucket);

            updateTail(bucket, pageId, newDataId);
        }
        else {
            // Just allocate a new node page and add our data page there.
            final long nextId = allocatePage(null);
            final long nextPage = acquirePage(nextId, statHolder);

            try {
                long nextPageAddr = writeLock(nextId, nextPage); // Newly allocated page.

                assert nextPageAddr != 0L;

                // Here we should never write full page, because it is known to be new.
                Boolean nextWalPlc = FALSE;

                try {
                    setupNextPage(io, pageId, pageAddr, nextId, nextPageAddr);

                    if (needWalDeltaRecord(pageId, page, null))
                        wal.log(new PagesListSetNextRecord(grpId, pageId, nextId));

                    int idx = io.addPage(nextPageAddr, dataId, pageSize());

                    if (needWalDeltaRecord(nextId, nextPage, nextWalPlc))
                        wal.log(new PagesListInitNewPageRecord(
                            grpId,
                            nextId,
                            io.getType(),
                            io.getVersion(),
                            nextId,
                            pageId,
                            dataId
                        ));

                    assert idx != -1;

                    dataIO.setFreeListPageId(dataAddr, nextId);

                    if (needWalDeltaRecord(dataId, data, null))
                        wal.log(new DataPageSetFreeListPageRecord(grpId, dataId, nextId));

                    incrementBucketSize(bucket);

                    updateTail(bucket, pageId, nextId);
                }
                finally {
                    writeUnlock(nextId, nextPage, nextPageAddr, nextWalPlc, true);
                }
            }
            finally {
                releasePage(nextId, nextPage);
            }
        }
    }

    /**
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param pageAddr Page address.
     * @param io IO.
     * @param bag Reuse bag.
     * @param bucket Bucket.
     * @param statHolder Statistics holder to track IO operations.
     * @return {@code true} If succeeded.
     * @throws IgniteCheckedException if failed.
     */
    private boolean putReuseBag(
        final long pageId,
        final long page,
        final long pageAddr,
        PagesListNodeIO io,
        ReuseBag bag,
        int bucket,
        IoStatisticsHolder statHolder
    ) throws IgniteCheckedException {
        assert bag != null : "bag is null";
        assert !bag.isEmpty() : "bag is empty";

        if (io.getNextId(pageAddr) != 0L)
            return false; // Splitted.

        long nextId;

        long prevId = pageId;
        long prevPage = page;
        long prevAddr = pageAddr;

        Boolean walPlc = null;

        GridLongList locked = null; // TODO may be unlock right away and do not keep all these pages locked?

        try {
            while ((nextId = bag.pollFreePage()) != 0L) {
                assert PageIdUtils.itemId(nextId) > 0 && PageIdUtils.itemId(nextId) <= MAX_ITEMID_NUM : U.hexLong(nextId);

                int idx = io.addPage(prevAddr, nextId, pageSize());

                if (idx == -1) { // Attempt to add page failed: the node page is full.
                    final long nextPage = acquirePage(nextId, statHolder);

                    try {
                        long nextPageAddr = writeLock(nextId, nextPage); // Page from reuse bag can't be concurrently recycled.

                        assert nextPageAddr != 0L;

                        if (locked == null)
                            locked = new GridLongList(6);

                        locked.add(nextId);
                        locked.add(nextPage);
                        locked.add(nextPageAddr);

                        setupNextPage(io, prevId, prevAddr, nextId, nextPageAddr);

                        if (needWalDeltaRecord(prevId, prevPage, walPlc))
                            wal.log(new PagesListSetNextRecord(grpId, prevId, nextId));

                        // Here we should never write full page, because it is known to be new.
                        if (needWalDeltaRecord(nextId, nextPage, FALSE))
                            wal.log(new PagesListInitNewPageRecord(
                                grpId,
                                nextId,
                                io.getType(),
                                io.getVersion(),
                                nextId,
                                prevId,
                                0L
                            ));

                        // In reuse bucket the page itself can be used as a free page.
                        if (isReuseBucket(bucket))
                            incrementBucketSize(bucket);

                        // Switch to this new page, which is now a part of our list
                        // to add the rest of the bag to the new page.
                        prevAddr = nextPageAddr;
                        prevId = nextId;
                        prevPage = nextPage;
                        // Starting from tis point all wal records are written for reused pages from the bag.
                        // This mean that we use delta records only.
                        walPlc = FALSE;
                    }
                    finally {
                        releasePage(nextId, nextPage);
                    }
                }
                else {
                    // TODO: use single WAL record for bag?
                    if (needWalDeltaRecord(prevId, prevPage, walPlc))
                        wal.log(new PagesListAddPageRecord(grpId, prevId, nextId));

                    incrementBucketSize(bucket);
                }
            }
        }
        finally {
            if (locked != null) {
                // We have to update our bucket with the new tail.
                updateTail(bucket, pageId, prevId);

                // Release write.
                for (int i = 0; i < locked.size(); i += 3)
                    writeUnlock(locked.get(i), locked.get(i + 1), locked.get(i + 2), FALSE, true);
            }
        }

        return true;
    }

    /**
     * @param bucket Bucket index.
     * @return Page for take.
     */
    private Stripe getPageForTake(int bucket) {
        Stripe[] tails = getBucket(bucket);

        if (tails == null || bucketsSize.get(bucket) == 0)
            return null;

        int len = tails.length;

        // Striped pool optimization.
        IgniteThread igniteThread = IgniteThread.current();

        if (igniteThread != null && igniteThread.policy() == GridIoPolicy.DATA_STREAMER_POOL) {
            int stripeIdx = igniteThread.stripe();

            assert stripeIdx != -1 : igniteThread;

            if (stripeIdx >= len)
                return null;

            Stripe stripe = tails[stripeIdx];

            return stripe.empty ? null : stripe;
        }

        int init = randomInt(len);
        int cur = init;

        while (true) {
            Stripe stripe = tails[cur];

            if (!stripe.empty)
                return stripe;

            if ((cur = (cur + 1) % len) == init)
                return null;
        }
    }

    /**
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param bucket Bucket.
     * @param lockAttempt Lock attempts counter.
     * @param bag Reuse bag.
     * @return Page address if page is locked of {@code null} if can retry lock.
     * @throws IgniteCheckedException If failed.
     */
    private long writeLockPage(long pageId, long page, int bucket, int lockAttempt, ReuseBag bag)
        throws IgniteCheckedException {
        // Striped pool optimization.
        IgniteThread igniteThread = IgniteThread.current();

        if (igniteThread != null && igniteThread.policy() == GridIoPolicy.DATA_STREAMER_POOL) {
            assert igniteThread.stripe() != -1 : igniteThread;

            return writeLock(pageId, page);
        }

        long pageAddr = tryWriteLock(pageId, page);

        if (pageAddr != 0L)
            return pageAddr;

        if (lockAttempt == TRY_LOCK_ATTEMPTS) {
            Stripe[] stripes = getBucket(bucket);

            if (stripes == null || stripes.length < MAX_STRIPES_PER_BUCKET) {
                addStripe(bucket, bag, !isReuseBucket(bucket));

                return 0L;
            }
        }

        return lockAttempt < TRY_LOCK_ATTEMPTS ? 0L : writeLock(pageId, page); // Must be explicitly checked further.
    }

    /**
     * @param bucket Bucket index.
     * @param initIoVers Optional IO to initialize page.
     * @param statHolder Statistics holder to track IO operations.
     * @return Removed page ID.
     * @throws IgniteCheckedException If failed.
     */
    protected long takeEmptyPage(int bucket, @Nullable IOVersions initIoVers,
        IoStatisticsHolder statHolder) throws IgniteCheckedException {
        PagesCache pagesCache = getBucketCache(bucket, false);

        long pageId;

        if (pagesCache != null && (pageId = pagesCache.poll()) != 0L) {
            decrementBucketSize(bucket);

            if (log.isDebugEnabled()) {
                log.debug("Take page from pages list cache [list=" + name + ", bucket=" + bucket +
                    ", pageId=" + pageId + ']');
            }

            return pageId;
        }

        for (int lockAttempt = 0; ;) {
            Stripe stripe = getPageForTake(bucket);

            if (stripe == null)
                return 0L;

            final long tailId = stripe.tailId;

            // Stripe was removed from bucket concurrently.
            if (tailId == 0L)
                continue;

            final long tailPage = acquirePage(tailId, statHolder);

            try {
                long tailAddr = writeLockPage(tailId, tailPage, bucket, lockAttempt++, null); // Explicit check.

                if (tailAddr == 0L)
                    continue;

                if (stripe.empty || stripe.tailId != tailId) {
                    // Another thread took the last page.
                    writeUnlock(tailId, tailPage, tailAddr, false);

                    if (bucketsSize.get(bucket) > 0) {
                        lockAttempt--; // Ignore current attempt.

                        continue;
                    }
                    else
                        return 0L;
                }

                assert PageIO.getPageId(tailAddr) == tailId
                    : "tailId = " + U.hexLong(tailId) + ", pageId = " + U.hexLong(PageIO.getPageId(tailAddr));
                assert PageIO.getType(tailAddr) == PageIO.T_PAGE_LIST_NODE
                    : "tailId = " + U.hexLong(tailId) + ", type = " + PageIO.getType(tailAddr);

                boolean dirty = false;
                long dataPageId;
                long recycleId = 0L;

                try {
                    PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(tailAddr);

                    if (io.getNextId(tailAddr) != 0) {
                        // It is not a tail anymore, retry.
                        continue;
                    }

                    pageId = io.takeAnyPage(tailAddr);

                    if (pageId != 0L) {
                        decrementBucketSize(bucket);

                        if (needWalDeltaRecord(tailId, tailPage, null))
                            wal.log(new PagesListRemovePageRecord(grpId, tailId, pageId));

                        dirty = true;

                        assert !isReuseBucket(bucket) ||
                            PageIdUtils.itemId(pageId) > 0 && PageIdUtils.itemId(pageId) <= MAX_ITEMID_NUM
                            : "Incorrectly recycled pageId in reuse bucket: " + U.hexLong(pageId);

                        dataPageId = pageId;

                        if (io.isEmpty(tailAddr)) {
                            long prevId = io.getPreviousId(tailAddr);

                            // If we got an empty page in non-reuse bucket, move it back to reuse list
                            // to prevent empty page leak to data pages.
                            if (!isReuseBucket(bucket)) {
                                if (prevId != 0L) {
                                    Boolean ok = write(prevId, cutTail, null, bucket, FALSE, statHolder);

                                    assert ok == TRUE : ok;

                                    recycleId = recyclePage(tailId, tailPage, tailAddr, null);
                                }
                                else
                                    stripe.empty = true;
                            }
                            else
                                stripe.empty = prevId == 0L;
                        }
                    }
                    else {
                        // The tail page is empty, but stripe is not. It might
                        // happen only if we are in reuse bucket and it has
                        // a previous page, so, the current page can be collected
                        assert isReuseBucket(bucket);

                        long prevId = io.getPreviousId(tailAddr);

                        assert prevId != 0L;

                        Boolean ok = write(prevId, cutTail, bucket, FALSE, statHolder);

                        assert ok == TRUE : ok;

                        decrementBucketSize(bucket);

                        if (initIoVers != null) {
                            int partId = PageIdUtils.partId(tailId);

                            dataPageId = initReusedPage(tailId, tailPage, tailAddr, partId, FLAG_DATA, initIoVers.latest());
                        } else
                            dataPageId = recyclePage(tailId, tailPage, tailAddr, null);

                        dirty = true;
                    }

                    // If we do not have a previous page (we are at head), then we still can return
                    // current page but we have to drop the whole stripe. Since it is a reuse bucket,
                    // we will not do that, but just return 0L, because this may produce contention on
                    // meta page.
                }
                finally {
                    writeUnlock(tailId, tailPage, tailAddr, dirty);
                }

                // Put recycled page (if any) to the reuse bucket after tail is unlocked.
                if (recycleId != 0L) {
                    assert !isReuseBucket(bucket);

                    reuseList.addForRecycle(new SingletonReuseBag(recycleId));
                }

                if (log.isDebugEnabled()) {
                    log.debug("Take page from pages list [list=" + name + ", bucket=" + bucket +
                        ", dataPageId=" + dataPageId + ", tailId=" + tailId + ']');
                }

                return dataPageId;
            }
            finally {
                releasePage(tailId, tailPage);
            }
        }
    }

    /**
     * Reused page must obtain correctly assembled page id, then initialized by proper {@link PageIO} instance and
     * non-zero {@code itemId} of reused page id must be saved into special place.
     *
     * @param reusedPageId Reused page id.
     * @param reusedPage Reused page.
     * @param reusedPageAddr Reused page address.
     * @param partId Partition id.
     * @param flag Flag.
     * @param initIo Initial io.
     * @return Prepared page id.
     * @throws IgniteCheckedException In case of failure.
     */
    protected final long initReusedPage(long reusedPageId, long reusedPage, long reusedPageAddr,
        int partId, byte flag, PageIO initIo) throws IgniteCheckedException {

        long newPageId = PageIdUtils.pageId(partId, flag, PageIdUtils.pageIndex(reusedPageId));

        initIo.initNewPage(reusedPageAddr, newPageId, pageSize());

        boolean needWalDeltaRecord = needWalDeltaRecord(reusedPageId, reusedPage, null);

        if (needWalDeltaRecord) {
            assert PageIdUtils.partId(reusedPageId) == PageIdUtils.partId(newPageId) :
                "Partition consistency failure: " +
                "newPageId=" + Long.toHexString(newPageId) + " (newPartId: " + PageIdUtils.partId(newPageId) + ") " +
                "reusedPageId=" + Long.toHexString(reusedPageId) + " (partId: " + PageIdUtils.partId(reusedPageId) + ")";

            wal.log(new InitNewPageRecord(grpId, reusedPageId, initIo.getType(),
                initIo.getVersion(), newPageId));
        }

        int itemId = PageIdUtils.itemId(reusedPageId);

        if (itemId != 0) {
            PageIO.setRotatedIdPart(reusedPageAddr, itemId);

            if (needWalDeltaRecord)
                wal.log(new RotatedIdPartRecord(grpId, newPageId, itemId));
        }

        return newPageId;
    }

    /**
     * Removes data page from bucket, merges bucket list if needed.
     *
     * @param dataId Data page ID.
     * @param dataPage Data page pointer.
     * @param dataAddr Data page address.
     * @param dataIO Data page IO.
     * @param bucket Bucket index.
     * @param statHolder Statistics holder to track IO operations.
     * @return {@code True} if page was removed.
     * @throws IgniteCheckedException If failed.
     */
    protected final boolean removeDataPage(
        final long dataId,
        final long dataPage,
        final long dataAddr,
        AbstractDataPageIO dataIO,
        int bucket,
        IoStatisticsHolder statHolder)
        throws IgniteCheckedException {
        final long pageId = dataIO.getFreeListPageId(dataAddr);

        if (pageId == 0L) { // Page cached in onheap list.
            assert isCachingApplicable() : "pageId==0L, but caching is not applicable for this pages list: " + name;

            PagesCache pagesCache = getBucketCache(bucket, false);

            // Pages cache can be null here if page was taken for put from free list concurrently.
            if (pagesCache == null || !pagesCache.removePage(dataId)) {
                if (log.isDebugEnabled()) {
                    log.debug("Remove page from pages list cache failed [list=" + name + ", bucket=" + bucket +
                        ", dataId=" + dataId + "]: " + ((pagesCache == null) ? "cache is null" : "page not found"));
                }

                return false;
            }

            decrementBucketSize(bucket);

            if (log.isDebugEnabled()) {
                log.debug("Remove page from pages list cache [list=" + name + ", bucket=" + bucket +
                    ", dataId=" + dataId + ']');
            }

            return true;
        }

        if (log.isDebugEnabled()) {
            log.debug("Remove page from pages list [list=" + name + ", bucket=" + bucket + ", dataId=" + dataId +
                ", pageId=" + pageId + ']');
        }

        final long page = acquirePage(pageId, statHolder);

        try {
            long nextId;

            long recycleId = 0L;

            long pageAddr = writeLock(pageId, page); // Explicit check.

            if (pageAddr == 0L)
                return false;

            boolean rmvd = false;

            try {
                PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(pageAddr);

                rmvd = io.removePage(pageAddr, dataId);

                if (!rmvd)
                    return false;

                decrementBucketSize(bucket);

                if (needWalDeltaRecord(pageId, page, null))
                    wal.log(new PagesListRemovePageRecord(grpId, pageId, dataId));

                // Reset free list page ID.
                dataIO.setFreeListPageId(dataAddr, 0L);

                if (needWalDeltaRecord(dataId, dataPage, null))
                    wal.log(new DataPageSetFreeListPageRecord(grpId, dataId, 0L));

                if (!io.isEmpty(pageAddr))
                    return true; // In optimistic case we still have something in the page and can leave it as is.

                // If the page is empty, we have to try to drop it and link next and previous with each other.
                nextId = io.getNextId(pageAddr);

                // If there are no next page, then we can try to merge without releasing current write lock,
                // because if we will need to lock previous page, the locking order will be already correct.
                if (nextId == 0L) {
                    long prevId = io.getPreviousId(pageAddr);

                    recycleId = mergeNoNext(pageId, page, pageAddr, prevId, bucket, statHolder);
                }
            }
            finally {
                writeUnlock(pageId, page, pageAddr, rmvd);
            }

            // Perform a fair merge after lock release (to have a correct locking order).
            if (nextId != 0L)
                recycleId = merge(pageId, page, nextId, bucket, statHolder);

            if (recycleId != 0L)
                reuseList.addForRecycle(new SingletonReuseBag(recycleId));

            return true;
        }
        finally {
            releasePage(pageId, page);
        }
    }

    /**
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param pageAddr Page address.
     * @param prevId Previous page ID.
     * @param bucket Bucket index.
     * @param statHolder Statistics holder to track IO operations.
     * @return Page ID to recycle.
     * @throws IgniteCheckedException If failed.
     */
    private long mergeNoNext(
        long pageId,
        long page,
        long pageAddr,
        long prevId,
        int bucket,
        IoStatisticsHolder statHolder)
        throws IgniteCheckedException {
        // If we do not have a next page (we are tail) and we are on reuse bucket,
        // then we can leave as is as well, because it is normal to have an empty tail page here.
        if (isReuseBucket(bucket))
            return 0L;

        if (prevId != 0L) { // Cut tail if we have a previous page.
            Boolean ok = write(prevId, cutTail, null, bucket, FALSE, statHolder);

            assert ok == TRUE : ok;
        }
        else {
            // If we don't have a previous, then we are tail page of free list, just drop the stripe.
            boolean rmvd = updateTail(bucket, pageId, 0L);

            if (!rmvd)
                return 0L;
        }

        return recyclePage(pageId, page, pageAddr, null);
    }

    /**
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param nextId Next page ID.
     * @param bucket Bucket index.
     * @param statHolder Statistics holder to track IO operations.
     * @return Page ID to recycle.
     * @throws IgniteCheckedException If failed.
     */
    private long merge(
        final long pageId,
        final long page,
        long nextId,
        int bucket,
        IoStatisticsHolder statHolder)
        throws IgniteCheckedException {
        assert nextId != 0; // We should do mergeNoNext then.

        // Lock all the pages in correct order (from next to previous) and do the merge in retry loop.
        for (;;) {
            final long curId = nextId;
            final long curPage = curId == 0L ? 0L : acquirePage(curId, statHolder);
            try {
                boolean write = false;

                final long curAddr = curPage == 0L ? 0L : writeLock(curId, curPage); // Explicit check.
                final long pageAddr = writeLock(pageId, page); // Explicit check.

                if (pageAddr == 0L) {
                    if (curAddr != 0L) // Unlock next page if needed.
                        writeUnlock(curId, curPage, curAddr, false);

                    return 0L; // Someone has merged or taken our empty page concurrently. Nothing to do here.
                }

                try {
                    PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(pageAddr);

                    if (!io.isEmpty(pageAddr))
                        return 0L; // No need to merge anymore.

                    // Check if we see a consistent state of the world.
                    if (io.getNextId(pageAddr) == curId && (curId == 0L) == (curAddr == 0L)) {
                        long recycleId = doMerge(pageId, page, pageAddr, io, curId, curPage, curAddr, bucket, statHolder);

                        write = true;

                        return recycleId; // Done.
                    }

                    // Reread next page ID and go for retry.
                    nextId = io.getNextId(pageAddr);
                }
                finally {
                    if (curAddr != 0L)
                        writeUnlock(curId, curPage, curAddr, write);

                    writeUnlock(pageId, page, pageAddr, write);
                }
            }
            finally {
                if (curPage != 0L)
                    releasePage(curId, curPage);
            }
        }
    }

    /**
     * @param pageId Page ID.
     * @param page Page absolute pointer.
     * @param pageAddr Page address.
     * @param io IO.
     * @param nextId Next page ID.
     * @param nextPage Next page absolute pointer.
     * @param nextAddr Next page address.
     * @param bucket Bucket index.
     * @param statHolder Statistics holder to track IO operations.
     * @return Page to recycle.
     * @throws IgniteCheckedException If failed.
     */
    private long doMerge(
        long pageId,
        long page,
        long pageAddr,
        PagesListNodeIO io,
        long nextId,
        long nextPage,
        long nextAddr,
        int bucket,
        IoStatisticsHolder statHolder
    ) throws IgniteCheckedException {
        long prevId = io.getPreviousId(pageAddr);

        if (nextId == 0L)
            return mergeNoNext(pageId, page, pageAddr, prevId, bucket, statHolder);
        else {
            // No one must be able to merge it while we keep a reference.
            assert getPageId(nextAddr) == nextId;

            if (prevId == 0L) { // No previous page: we are at head.
                // These references must be updated at the same time in write locks.
                assert PagesListNodeIO.VERSIONS.forPage(nextAddr).getPreviousId(nextAddr) == pageId;

                PagesListNodeIO nextIO = PagesListNodeIO.VERSIONS.forPage(nextAddr);
                nextIO.setPreviousId(nextAddr, 0);

                if (needWalDeltaRecord(nextId, nextPage, null))
                    wal.log(new PagesListSetPreviousRecord(grpId, nextId, 0L));
            }
            else // Do a fair merge: link previous and next to each other.
                fairMerge(prevId, pageId, nextId, nextPage, nextAddr, statHolder);

            return recyclePage(pageId, page, pageAddr, null);
        }
    }

    /**
     * Link previous and next to each other.
     * @param prevId Previous Previous page ID.
     * @param pageId Page ID.
     * @param nextId Next page ID.
     * @param nextPage Next page absolute pointer.
     * @param nextAddr Next page address.
     * @param statHolder Statistics holder to track IO operations.
     * @throws IgniteCheckedException If failed.
     */
    private void fairMerge(
        final long prevId,
        long pageId,
        long nextId,
        long nextPage,
        long nextAddr,
        IoStatisticsHolder statHolder)
        throws IgniteCheckedException {
        long prevPage = acquirePage(prevId, statHolder);

        try {
            final long prevAddr = writeLock(prevId, prevPage); // No check, we keep a reference.
            assert prevAddr != 0L;
            try {
                PagesListNodeIO prevIO = PagesListNodeIO.VERSIONS.forPage(prevAddr);
                PagesListNodeIO nextIO = PagesListNodeIO.VERSIONS.forPage(nextAddr);

                // These references must be updated at the same time in write locks.
                assert prevIO.getNextId(prevAddr) == pageId;
                assert nextIO.getPreviousId(nextAddr) == pageId;

                prevIO.setNextId(prevAddr, nextId);

                if (needWalDeltaRecord(prevId, prevPage, null))
                    wal.log(new PagesListSetNextRecord(grpId, prevId, nextId));

                nextIO.setPreviousId(nextAddr, prevId);

                if (needWalDeltaRecord(nextId, nextPage, null))
                    wal.log(new PagesListSetPreviousRecord(grpId, nextId, prevId));
            }
            finally {
                writeUnlock(prevId, prevPage, prevAddr, true);
            }
        }
        finally {
            releasePage(prevId, prevPage);
        }
    }

    /**
     * Increments bucket size and updates changed flag.
     *
     * @param bucket Bucket number.
     */
    private void incrementBucketSize(int bucket) {
        bucketsSize.incrementAndGet(bucket);
    }

    /**
     * Increments bucket size and updates changed flag.
     *
     * @param bucket Bucket number.
     */
    private void decrementBucketSize(int bucket) {
        bucketsSize.decrementAndGet(bucket);
    }

    /**
     * Mark free list was changed.
     */
    private void changed() {
        // Ok to have a race here, see the field javadoc.
        if (!changed)
            changed = true;
    }

    /**
     * Pages list name.
     */
    public String name() {
        return name;
    }

    /**
     * Buckets count.
     */
    public int bucketsCount() {
        return buckets;
    }

    /**
     * Bucket size.
     *
     * @param bucket Bucket.
     */
    public long bucketSize(int bucket) {
        return bucketsSize.get(bucket);
    }

    /**
     * Stripes count.
     *
     * @param bucket Bucket.
     */
    public int stripesCount(int bucket) {
        Stripe[] stripes = getBucket(bucket);

        return stripes == null ? 0 : stripes.length;
    }

    /**
     * Cached pages count.
     *
     * @param bucket Bucket.
     */
    public int cachedPagesCount(int bucket) {
        PagesCache pagesCache = getBucketCache(bucket, false);

        return pagesCache == null ? 0 : pagesCache.size();
    }

    /**
     * Singleton reuse bag.
     */
    private static final class SingletonReuseBag implements ReuseBag {
        /** */
        long pageId;

        /**
         * @param pageId Page ID.
         */
        SingletonReuseBag(long pageId) {
            this.pageId = pageId;
        }

        /** {@inheritDoc} */
        @Override public void addFreePage(long pageId) {
            throw new IllegalStateException("Should never be called.");
        }

        /** {@inheritDoc} */
        @Override public long pollFreePage() {
            long res = pageId;

            pageId = 0L;

            return res;
        }

        /** {@inheritDoc} */
        @Override public boolean isEmpty() {
            return pageId == 0L;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SingletonReuseBag.class, this, "pageId", U.hexLong(pageId));
        }
    }

    /** Class to store page-list cache onheap. */
    public static class PagesCache {
        /** Pages cache max size. */
        private static final int MAX_SIZE =
            IgniteSystemProperties.getInteger("IGNITE_PAGES_LIST_CACHING_MAX_CACHE_SIZE", 64);

        /** Stripes count. Must be power of 2. */
        private static final int STRIPES_COUNT =
            IgniteSystemProperties.getInteger("IGNITE_PAGES_LIST_CACHING_STRIPES_COUNT", 4);

        /** Threshold of flush calls on empty cache to allow GC of stripes (flush invoked twice per checkpoint). */
        private static final int EMPTY_FLUSH_GC_THRESHOLD =
            IgniteSystemProperties.getInteger("IGNITE_PAGES_LIST_CACHING_EMPTY_FLUSH_GC_THRESHOLD", 10);

        /** Mutexes for each stripe. */
        private final Object[] stripeLocks = new Object[STRIPES_COUNT];

        /** Page lists. */
        private final GridLongList[] stripes = new GridLongList[STRIPES_COUNT];

        /** Atomic updater for nextStripeIdx field. */
        private static final AtomicIntegerFieldUpdater<PagesCache> nextStripeUpdater = AtomicIntegerFieldUpdater
            .newUpdater(PagesCache.class, "nextStripeIdx");

        /** Atomic updater for size field. */
        private static final AtomicIntegerFieldUpdater<PagesCache> sizeUpdater = AtomicIntegerFieldUpdater
            .newUpdater(PagesCache.class, "size");

        /** Access counter to provide round-robin stripes polling. */
        private volatile int nextStripeIdx;

        /** Cache size. */
        private volatile int size;

        /** Count of flush calls with empty cache. */
        private int emptyFlushCnt;

        /** Global (per data region) limit of caches for page lists. */
        private final AtomicLong pagesCacheLimit;

        /**
         * Default constructor.
         */
        public PagesCache(@Nullable AtomicLong pagesCacheLimit) {
            assert U.isPow2(STRIPES_COUNT) : STRIPES_COUNT;

            for (int i = 0; i < STRIPES_COUNT; i++)
                stripeLocks[i] = new Object();

            this.pagesCacheLimit = pagesCacheLimit;
        }

        /**
         * Remove page from the list.
         *
         * @param pageId Page id.
         * @return {@code True} if page was found and succesfully removed, {@code false} if page not found.
         */
        public boolean removePage(long pageId) {
            int stripeIdx = (int)pageId & (STRIPES_COUNT - 1);

            synchronized (stripeLocks[stripeIdx]) {
                GridLongList stripe = stripes[stripeIdx];

                boolean rmvd = stripe != null && stripe.removeValue(0, pageId) >= 0;

                if (rmvd) {
                    if (sizeUpdater.decrementAndGet(this) == 0 && pagesCacheLimit != null)
                        pagesCacheLimit.incrementAndGet();
                }

                return rmvd;
            }
        }

        /**
         * Poll next page from the list.
         *
         * @return pageId.
         */
        public long poll() {
            if (size == 0)
                return 0L;

            for (int i = 0; i < STRIPES_COUNT; i++) {
                int stripeIdx = nextStripeUpdater.getAndIncrement(this) & (STRIPES_COUNT - 1);

                synchronized (stripeLocks[stripeIdx]) {
                    GridLongList stripe = stripes[stripeIdx];

                    if (stripe != null && !stripe.isEmpty()) {
                        if (sizeUpdater.decrementAndGet(this) == 0 && pagesCacheLimit != null)
                            pagesCacheLimit.incrementAndGet();

                        return stripe.remove();
                    }
                }
            }

            return 0L;
        }

        /**
         * Flush all stripes to one list and clear stripes.
         */
        public GridLongList flush() {
            GridLongList res = null;

            if (size == 0) {
                boolean stripesChanged = false;

                if (++emptyFlushCnt >= EMPTY_FLUSH_GC_THRESHOLD) {
                    for (int i = 0; i < STRIPES_COUNT; i++) {
                        synchronized (stripeLocks[i]) {
                            GridLongList stripe = stripes[i];

                            if (stripe != null) {
                                if (stripe.isEmpty())
                                    stripes[i] = null;
                                else {
                                    // Pages were concurrently added to the stripe.
                                    stripesChanged = true;

                                    break;
                                }
                            }
                        }
                    }
                }

                if (!stripesChanged)
                    return null;
            }

            emptyFlushCnt = 0;

            for (int i = 0; i < STRIPES_COUNT; i++) {
                synchronized (stripeLocks[i]) {
                    GridLongList stripe = stripes[i];

                    if (stripe != null && !stripe.isEmpty()) {
                        if (res == null)
                            res = new GridLongList(size);

                        if (sizeUpdater.addAndGet(this, -stripe.size()) == 0 && pagesCacheLimit != null)
                            pagesCacheLimit.incrementAndGet();

                        res.addAll(stripe);

                        stripe.clear();
                    }
                }
            }

            return res;
        }

        /**
         * Add pageId to the tail of the list.
         *
         * @param pageId Page id.
         * @return {@code True} if page can be added, {@code false} if list is full.
         */
        public boolean add(long pageId) {
            assert pageId != 0L;

            // Ok with race here.
            if (size == 0 && pagesCacheLimit != null && pagesCacheLimit.get() <= 0)
                return false; // Pages cache limit exceeded.

            // Ok with race here.
            if (size >= MAX_SIZE)
                return false;

            int stripeIdx = (int)pageId & (STRIPES_COUNT - 1);

            synchronized (stripeLocks[stripeIdx]) {
                GridLongList stripe = stripes[stripeIdx];

                if (stripe == null)
                    stripes[stripeIdx] = stripe = new GridLongList(MAX_SIZE / STRIPES_COUNT);

                if (stripe.size() >= MAX_SIZE / STRIPES_COUNT)
                    return false;
                else {
                    stripe.add(pageId);

                    if (sizeUpdater.getAndIncrement(this) == 0 && pagesCacheLimit != null)
                        pagesCacheLimit.decrementAndGet();

                    return true;
                }
            }
        }

        /**
         * Cache size.
         */
        public int size() {
            return size;
        }
    }

    /**
     *
     */
    public static final class Stripe {
        /** */
        public volatile long tailId;

        /** */
        public volatile boolean empty;

        /**
         * @param tailId Tail ID.
         * @param empty Empty flag.
         */
        Stripe(long tailId, boolean empty) {
            this.tailId = tailId;
            this.empty = empty;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Stripe stripe = (Stripe)o;

            return F.eq(tailId, stripe.tailId) && F.eq(empty, stripe.empty);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(tailId, empty);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return Long.toString(tailId);
        }
    }
}
