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

package org.apache.ignite.internal.processors.cache.database.freelist;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.pagemem.Page;
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
import org.apache.ignite.internal.pagemem.wal.record.delta.RecycleRecord;
import org.apache.ignite.internal.processors.cache.database.DataStructure;
import org.apache.ignite.internal.processors.cache.database.freelist.io.PagesListMetaIO;
import org.apache.ignite.internal.processors.cache.database.freelist.io.PagesListNodeIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;
import org.apache.ignite.internal.util.GridArrays;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.processors.cache.database.tree.io.PageIO.getPageId;
import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.initPage;
import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.isWalDeltaRecordNeeded;
import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.writePage;

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
            Math.min(8, Runtime.getRuntime().availableProcessors() * 2));

    /** */
    protected final AtomicLong[] bucketsSize;

    /** */
    protected volatile boolean changed;

    /** Page ID to store list metadata. */
    private final long metaPageId;

    /** Number of buckets. */
    private final int buckets;

    /** Name (for debug purposes). */
    protected final String name;

    /** */
    private final PageHandler<Void, Boolean> cutTail = new CutTail();

    /**
     *
     */
    private class CutTail extends PageHandler<Void, Boolean> {
        /** {@inheritDoc} */
        @Override public Boolean run(Page page, PageIO pageIo, long pageAddr, Void ignore, int bucket)
            throws IgniteCheckedException {
            assert getPageId(pageAddr) == page.id();

            PagesListNodeIO io = (PagesListNodeIO)pageIo;

            long tailId = io.getNextId(pageAddr);

            assert tailId != 0;

            io.setNextId(pageAddr, 0L);

            if (isWalDeltaRecordNeeded(wal, page))
                wal.log(new PagesListSetNextRecord(cacheId, page.id(), 0L));

            updateTail(bucket, tailId, page.id());

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
        long metaPageId
    ) {
        super(cacheId, pageMem, wal);

        this.name = name;
        this.buckets = buckets;
        this.metaPageId = metaPageId;

        bucketsSize = new AtomicLong[buckets];

        for (int i = 0; i < buckets; i++)
            bucketsSize[i] = new AtomicLong();
    }

    /**
     * @param metaPageId Metadata page ID.
     * @param initNew {@code True} if new list if created, {@code false} if should be initialized from metadata.
     * @throws IgniteCheckedException If failed.
     */
    protected final void init(long metaPageId, boolean initNew) throws IgniteCheckedException {
        if (metaPageId != 0L) {
            if (initNew) {
                try (Page page = page(metaPageId)) {
                    initPage(pageMem, page, this, PagesListMetaIO.VERSIONS.latest(), wal);
                }
            }
            else {
                Map<Integer, GridLongList> bucketsData = new HashMap<>();

                long nextPageId = metaPageId;

                while (nextPageId != 0) {
                    try (Page page = page(nextPageId)) {
                        long pageAddr = readLock(page); // No concurrent recycling on init.

                        assert pageAddr != 0L;

                        try {
                            PagesListMetaIO io = PagesListMetaIO.VERSIONS.forPage(pageAddr);

                            io.getBucketsData(pageAddr, bucketsData);

                            long next0 = io.getNextMetaPageId(pageAddr);

                            assert next0 != nextPageId :
                                "Loop detected [next=" + U.hexLong(next0) + ", cur=" + U.hexLong(nextPageId) + ']';

                            nextPageId = next0;
                        }
                        finally {
                            readUnlock(page, pageAddr);
                        }
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

                        long pageId = tailId;
                        int cnt = 0;

                        while (pageId != 0L) {
                            try (Page page = page(pageId)) {
                                long pageAddr = readLock(page);

                                assert pageAddr != 0L;

                                try {
                                    PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(pageAddr);

                                    cnt += io.getCount(pageAddr);
                                    pageId = io.getPreviousId(pageAddr);

                                    // In reuse bucket the page itself can be used as a free page.
                                    if (isReuseBucket(bucket) && pageId != 0L)
                                        cnt++;
                                }
                                finally {
                                    readUnlock(page, pageAddr);
                                }
                            }
                        }

                        Stripe stripe = new Stripe(tailId, cnt == 0);
                        tails[i] = stripe;
                        bucketSize += cnt;
                    }

                    boolean ok = casBucket(bucket, null, tails);
                    assert ok;

                    bucketsSize[bucket].set(bucketSize);

                    changed = true;
                }
            }
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void saveMetadata() throws IgniteCheckedException {
        assert metaPageId != 0;

        Page curPage = null;
        long curPageAddr = 0L;
        PagesListMetaIO curIo = null;

        long nextPageId = metaPageId;

        if (!changed)
            return;

        try {
            for (int bucket = 0; bucket < buckets; bucket++) {
                Stripe[] tails = getBucket(bucket);

                if (tails != null) {
                    int tailIdx = 0;

                    while (tailIdx < tails.length) {
                        int written = curPage != null ? curIo.addTails(pageMem.pageSize(), curPageAddr, bucket, tails, tailIdx) : 0;

                        if (written == 0) {
                            if (nextPageId == 0L) {
                                nextPageId = allocatePageNoReuse();

                                if (curPage != null) {
                                    curIo.setNextMetaPageId(curPageAddr, nextPageId);

                                    releaseAndClose(curPage, curPageAddr);
                                    curPage = null;
                                }

                                curPage = page(nextPageId);
                                curPageAddr = writeLock(curPage);

                                curIo = PagesListMetaIO.VERSIONS.latest();

                                curIo.initNewPage(curPageAddr, nextPageId, pageSize());
                            }
                            else {
                                releaseAndClose(curPage, curPageAddr);
                                curPage = null;

                                curPage = page(nextPageId);
                                curPageAddr = writeLock(curPage);

                                curIo = PagesListMetaIO.VERSIONS.forPage(curPageAddr);

                                curIo.resetCount(curPageAddr);
                            }

                            nextPageId = curIo.getNextMetaPageId(curPageAddr);
                        }
                        else
                            tailIdx += written;
                    }
                }
            }
        }
        finally {
            releaseAndClose(curPage, curPageAddr);
        }

        while (nextPageId != 0L) {
            try (Page page = page(nextPageId)) {
                long pageAddr = writeLock(page);

                try {
                    PagesListMetaIO io = PagesListMetaIO.VERSIONS.forPage(pageAddr);

                    io.resetCount(pageAddr);

                    if (PageHandler.isWalDeltaRecordNeeded(wal, page))
                        wal.log(new PageListMetaResetCountRecord(cacheId, nextPageId));

                    nextPageId = io.getNextMetaPageId(pageAddr);
                }
                finally {
                    writeUnlock(page, pageAddr, true);
                }
            }
        }

        changed = false;
    }

    /**
     * @param page Page.
     * @param buf Buffer.
     */
    private void releaseAndClose(Page page, long buf) {
        if (page != null) {
            try {
                // No special WAL record because we most likely changed the whole page.
                page.fullPageWalRecordPolicy(true);

                writeUnlock(page, buf, true);
            }
            finally {
                page.close();
            }
        }
    }

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
     * @param reuse {@code True} if possible to use reuse list.
     * @throws IgniteCheckedException If failed.
     * @return Tail page ID.
     */
    private Stripe addStripe(int bucket, boolean reuse) throws IgniteCheckedException {
        long pageId = reuse ? allocatePage(null) : allocatePageNoReuse();

        try (Page page = page(pageId)) {
            initPage(pageMem, page, this, PagesListNodeIO.VERSIONS.latest(), wal);
        }

        Stripe stripe = new Stripe(pageId, true);

        for (;;) {
            Stripe[] old = getBucket(bucket);
            Stripe[] upd;

            if (old != null) {
                int len = old.length;

                upd = Arrays.copyOf(old, len + 1);

                upd[len] = stripe;
            }
            else
                upd = new Stripe[]{stripe};

            if (casBucket(bucket, old, upd))
                return stripe;
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

        for (;;) {
            Stripe[] tails = getBucket(bucket);

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

                if (casBucket(bucket, tails, newTails))
                    return true;
            }
            else {
                // It is safe to assign new tail since we do it only when write lock on tail is held.
                tails[idx].tailId = newTailId;

                return true;
            }
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
     * @return Page ID where the given page
     * @throws IgniteCheckedException If failed.
     */
    private Stripe getPageForPut(int bucket) throws IgniteCheckedException {
        Stripe[] tails = getBucket(bucket);

        if (tails == null)
            return addStripe(bucket, true);

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
                long pageId = tail.tailId;

                while (pageId != 0L) {
                    try (Page page = page(pageId)) {
                        long pageAddr = readLock(page);

                        assert pageAddr != 0L;

                        try {
                            PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(pageAddr);

                            res += io.getCount(pageAddr);
                            pageId = io.getPreviousId(pageAddr);

                            // In reuse bucket the page itself can be used as a free page.
                            if (isReuseBucket(bucket) && pageId != 0L)
                                res++;
                        }
                        finally {
                            readUnlock(page, pageAddr);
                        }
                    }
                }
            }
        }

        assert res == bucketsSize[bucket].get() : "Wrong bucket size counter [exp=" + res + ", cntr=" + bucketsSize[bucket].get() + ']';

        return res;
    }

    /**
     * @param bag Reuse bag.
     * @param dataPage Data page.
     * @param dataPageAddr Data page address.
     * @param bucket Bucket.
     * @throws IgniteCheckedException If failed.
     */
    protected final void put(ReuseBag bag, Page dataPage, long dataPageAddr, int bucket)
        throws IgniteCheckedException {
        assert bag == null ^ dataPageAddr == 0L;

        for (int lockAttempt = 0; ;) {
            Stripe stripe = getPageForPut(bucket);

            long tailId = stripe.tailId;

            try (Page tail = page(tailId)) {
                long pageAddr = writeLockPage(tail, bucket, lockAttempt++); // Explicit check.

                if (pageAddr == 0L) {
                    if (isReuseBucket(bucket) && lockAttempt == TRY_LOCK_ATTEMPTS)
                        addStripeForReuseBucket(bucket);

                    continue;
                }

                assert PageIO.getPageId(pageAddr) == tailId : "pageId = " + PageIO.getPageId(pageAddr) + ", tailId = " + tailId;
                assert PageIO.getType(pageAddr) == PageIO.T_PAGE_LIST_NODE;

                boolean ok = false;

                try {
                    PagesListNodeIO io = PageIO.getPageIO(pageAddr);

                    ok = bag != null ?
                        // Here we can always take pages from the bag to build our list.
                        putReuseBag(tailId, tail, pageAddr, io, bag, bucket) :
                        // Here we can use the data page to build list only if it is empty and
                        // it is being put into reuse bucket. Usually this will be true, but there is
                        // a case when there is no reuse bucket in the free list, but then deadlock
                        // on node page allocation from separate reuse list is impossible.
                        // If the data page is not empty it can not be put into reuse bucket and thus
                        // the deadlock is impossible as well.
                        putDataPage(tailId, tail, pageAddr, io, dataPage, dataPageAddr, bucket);

                    if (ok) {
                        stripe.empty = false;

                        return;
                    }
                }
                finally {
                    writeUnlock(tail, pageAddr, ok);
                }
            }
        }
    }

    /**
     * @param pageId Page ID.
     * @param page Page.
     * @param pageAddr Page address.
     * @param io IO.
     * @param dataPage Data page.
     * @param dataPageAddr Data page address.
     * @param bucket Bucket.
     * @return {@code true} If succeeded.
     * @throws IgniteCheckedException If failed.
     */
    private boolean putDataPage(
        long pageId,
        Page page,
        long pageAddr,
        PagesListNodeIO io,
        Page dataPage,
        long dataPageAddr,
        int bucket
    ) throws IgniteCheckedException {
        if (io.getNextId(pageAddr) != 0L)
            return false; // Splitted.

        long dataPageId = dataPage.id();

        int idx = io.addPage(pageAddr, dataPageId, pageSize());

        if (idx == -1)
            handlePageFull(pageId, page, pageAddr, io, dataPage, dataPageAddr, bucket);
        else {
            incrementBucketSize(bucket);

            if (isWalDeltaRecordNeeded(wal, page))
                wal.log(new PagesListAddPageRecord(cacheId, pageId, dataPageId));

            DataPageIO dataIO = DataPageIO.VERSIONS.forPage(dataPageAddr);
            dataIO.setFreeListPageId(dataPageAddr, pageId);

            if (isWalDeltaRecordNeeded(wal, dataPage))
                wal.log(new DataPageSetFreeListPageRecord(cacheId, dataPage.id(), pageId));
        }

        return true;
    }

    /**
     * @param pageId Page ID.
     * @param page Page.
     * @param pageAddr Page address.
     * @param io IO.
     * @param dataPage Data page.
     * @param dataPageAddr Data page address.
     * @param bucket Bucket index.
     * @throws IgniteCheckedException If failed.
     */
    private void handlePageFull(
        long pageId,
        Page page,
        long pageAddr,
        PagesListNodeIO io,
        Page dataPage,
        long dataPageAddr,
        int bucket
    ) throws IgniteCheckedException {
        long dataPageId = dataPage.id();
        DataPageIO dataIO = DataPageIO.VERSIONS.forPage(dataPageAddr);

        // Attempt to add page failed: the node page is full.
        if (isReuseBucket(bucket)) {
            // If we are on the reuse bucket, we can not allocate new page, because it may cause deadlock.
            assert dataIO.isEmpty(dataPageAddr); // We can put only empty data pages to reuse bucket.

            // Change page type to index and add it as next node page to this list.
            dataPageId = PageIdUtils.changeType(dataPageId, FLAG_IDX);

            setupNextPage(io, pageId, pageAddr, dataPageId, dataPageAddr);

            if (isWalDeltaRecordNeeded(wal, page))
                wal.log(new PagesListSetNextRecord(cacheId, pageId, dataPageId));

            if (isWalDeltaRecordNeeded(wal, dataPage))
                wal.log(new PagesListInitNewPageRecord(
                    cacheId,
                    dataPageId,
                    io.getType(),
                    io.getVersion(),
                    dataPageId,
                    pageId, 0L));

            // In reuse bucket the page itself can be used as a free page.
            incrementBucketSize(bucket);

            updateTail(bucket, pageId, dataPageId);
        }
        else {
            // Just allocate a new node page and add our data page there.
            long nextId = allocatePage(null);

            try (Page next = page(nextId)) {
                long nextPageAddr = writeLock(next); // Newly allocated page.

                assert nextPageAddr != 0L;

                try {
                    setupNextPage(io, pageId, pageAddr, nextId, nextPageAddr);

                    if (isWalDeltaRecordNeeded(wal, page))
                        wal.log(new PagesListSetNextRecord(cacheId, pageId, nextId));

                    int idx = io.addPage(nextPageAddr, dataPageId, pageSize());

                    // Here we should never write full page, because it is known to be new.
                    next.fullPageWalRecordPolicy(FALSE);

                    if (isWalDeltaRecordNeeded(wal, next))
                        wal.log(new PagesListInitNewPageRecord(
                            cacheId,
                            nextId,
                            io.getType(),
                            io.getVersion(),
                            nextId,
                            pageId,
                            dataPageId
                        ));

                    assert idx != -1;

                    dataIO.setFreeListPageId(dataPageAddr, nextId);

                    if (isWalDeltaRecordNeeded(wal, dataPage))
                        wal.log(new DataPageSetFreeListPageRecord(cacheId, dataPageId, nextId));

                    incrementBucketSize(bucket);

                    updateTail(bucket, pageId, nextId);
                }
                finally {
                    writeUnlock(next, nextPageAddr, true);
                }
            }
        }
    }

    /**
     * @param pageId Page ID.
     * @param page Page.
     * @param pageAddr Page address.
     * @param io IO.
     * @param bag Reuse bag.
     * @param bucket Bucket.
     * @return {@code true} If succeeded.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    private boolean putReuseBag(
        final long pageId,
        Page page,
        final long pageAddr,
        PagesListNodeIO io,
        ReuseBag bag,
        int bucket
    ) throws IgniteCheckedException {
        if (io.getNextId(pageAddr) != 0L)
            return false; // Splitted.

        long nextId;
        long prevPageAddr = pageAddr;
        long prevId = pageId;

        List<Page> locked = null; // TODO may be unlock right away and do not keep all these pages locked?
        List<Long> lockedAddrs = null;

        try {
            while ((nextId = bag.pollFreePage()) != 0L) {
                int idx = io.addPage(prevPageAddr, nextId, pageSize());

                if (idx == -1) { // Attempt to add page failed: the node page is full.
                    try (Page next = page(nextId)) {
                        long nextPageAddr = writeLock(next); // Page from reuse bag can't be concurrently recycled.

                        assert nextPageAddr != 0L;

                        if (locked == null) {
                            lockedAddrs = new ArrayList<>(2);
                            locked = new ArrayList<>(2);
                        }

                        locked.add(next);
                        lockedAddrs.add(nextPageAddr);

                        setupNextPage(io, prevId, prevPageAddr, nextId, nextPageAddr);

                        if (isWalDeltaRecordNeeded(wal, page))
                            wal.log(new PagesListSetNextRecord(cacheId, prevId, nextId));

                        // Here we should never write full page, because it is known to be new.
                        next.fullPageWalRecordPolicy(FALSE);

                        if (isWalDeltaRecordNeeded(wal, next))
                            wal.log(new PagesListInitNewPageRecord(
                                cacheId,
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
                        prevPageAddr = nextPageAddr;
                        prevId = nextId;
                        page = next;
                    }
                }
                else {
                    // TODO: use single WAL record for bag?
                    if (isWalDeltaRecordNeeded(wal, page))
                        wal.log(new PagesListAddPageRecord(cacheId, prevId, nextId));

                    incrementBucketSize(bucket);
                }
            }
        }
        finally {
            if (locked != null) {
                // We have to update our bucket with the new tail.
                updateTail(bucket, pageId, prevId);

                // Release write.
                for (int i = 0; i < locked.size(); i++)
                    writeUnlock(locked.get(i), lockedAddrs.get(i), true);
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

        if (tails == null || bucketsSize[bucket].get() == 0)
            return null;

        int len = tails.length;
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
     * @param page Page.
     * @param bucket Bucket.
     * @param lockAttempt Lock attempts counter.
     * @return Page address if page is locked of {@code null} if can retry lock.
     * @throws IgniteCheckedException If failed.
     */
    private long writeLockPage(Page page, int bucket, int lockAttempt)
        throws IgniteCheckedException {
        long pageAddr = tryWriteLock(page);

        if (pageAddr != 0L)
            return pageAddr;

        if (lockAttempt == TRY_LOCK_ATTEMPTS) {
            Stripe[] stripes = getBucket(bucket);

            if (stripes == null || stripes.length < MAX_STRIPES_PER_BUCKET) {
                if (!isReuseBucket(bucket))
                    addStripe(bucket, true);

                return 0L;
            }
        }

        return lockAttempt < TRY_LOCK_ATTEMPTS ? 0L : writeLock(page); // Must be explicitly checked further.
    }

    /**
     * @param bucket Bucket.
     * @throws IgniteCheckedException If failed.
     */
    private void addStripeForReuseBucket(int bucket) throws IgniteCheckedException {
        assert isReuseBucket(bucket);

        Stripe[] stripes = getBucket(bucket);

        if (stripes == null || stripes.length < MAX_STRIPES_PER_BUCKET)
            addStripe(bucket, false);
    }

    /**
     * @param bucket Bucket index.
     * @param initIoVers Optional IO to initialize page.
     * @return Removed page ID.
     * @throws IgniteCheckedException If failed.
     */
    protected final long takeEmptyPage(int bucket, @Nullable IOVersions initIoVers) throws IgniteCheckedException {
        for (int lockAttempt = 0; ;) {
            Stripe stripe = getPageForTake(bucket);

            if (stripe == null)
                return 0L;

            long tailId = stripe.tailId;

            try (Page tail = page(tailId)) {
                long tailPageAddr = writeLockPage(tail, bucket, lockAttempt++); // Explicit check.

                if (tailPageAddr == 0L) {
                    if (isReuseBucket(bucket) && lockAttempt == TRY_LOCK_ATTEMPTS)
                        addStripeForReuseBucket(bucket);

                    continue;
                }

                if (stripe.empty) {
                    // Another thread took the last page.
                    writeUnlock(tail, tailPageAddr, false);

                    if (bucketsSize[bucket].get() > 0) {
                        lockAttempt--; // Ignore current attempt.

                        continue;
                    }
                    else
                        return 0L;
                }

                assert PageIO.getPageId(tailPageAddr) == tailId : "tailId = " + tailId + ", tailPageId = " + PageIO.getPageId(tailPageAddr);
                assert PageIO.getType(tailPageAddr) == PageIO.T_PAGE_LIST_NODE;

                boolean dirty = false;
                long ret;
                long recycleId = 0L;

                try {
                    PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(tailPageAddr);

                    if (io.getNextId(tailPageAddr) != 0) {
                        // It is not a tail anymore, retry.
                        continue;
                    }

                    long pageId = io.takeAnyPage(tailPageAddr);

                    if (pageId != 0L) {
                        decrementBucketSize(bucket);

                        if (isWalDeltaRecordNeeded(wal, tail))
                            wal.log(new PagesListRemovePageRecord(cacheId, tailId, pageId));

                        dirty = true;

                        ret = pageId;

                        if (io.isEmpty(tailPageAddr)) {
                            long prevId = io.getPreviousId(tailPageAddr);

                            // If we got an empty page in non-reuse bucket, move it back to reuse list
                            // to prevent empty page leak to data pages.
                            if (!isReuseBucket(bucket)) {
                                if (prevId != 0L) {
                                    try (Page prev = page(prevId)) {
                                        // Lock pages from next to previous.
                                        Boolean ok = writePage(pageMem, prev, this, cutTail, null, bucket, FALSE);

                                        assert ok == TRUE : ok;
                                    }

                                    recycleId = recyclePage(tailId, tail, tailPageAddr);
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

                        long prevId = io.getPreviousId(tailPageAddr);

                        assert prevId != 0L;

                        try (Page prev = page(prevId)) {
                            // Lock pages from next to previous.
                            Boolean ok = writePage(pageMem, prev, this, cutTail, null, bucket, FALSE);

                            assert ok == TRUE : ok;

                            decrementBucketSize(bucket);
                        }

                        if (initIoVers != null) {
                            tailId = PageIdUtils.changeType(tailId, FLAG_DATA);

                            PageIO initIo = initIoVers.latest();

                            initIo.initNewPage(tailPageAddr, tailId, pageSize());

                            if (isWalDeltaRecordNeeded(wal, tail)) {
                                wal.log(new InitNewPageRecord(cacheId, tail.id(), initIo.getType(),
                                    initIo.getVersion(), tailId));
                            }
                        }
                        else
                            tailId = recyclePage(tailId, tail, tailPageAddr);

                        dirty = true;

                        ret = tailId;
                    }

                    // If we do not have a previous page (we are at head), then we still can return
                    // current page but we have to drop the whole stripe. Since it is a reuse bucket,
                    // we will not do that, but just return 0L, because this may produce contention on
                    // meta page.
                }
                finally {
                    writeUnlock(tail, tailPageAddr, dirty);
                }

                // Put recycled page (if any) to the reuse bucket after tail is unlocked.
                if (recycleId != 0L) {
                    assert !isReuseBucket(bucket);

                    reuseList.addForRecycle(new SingletonReuseBag(recycleId));
                }

                return ret;
            }
        }
    }

    /**
     * @param dataPage Data page.
     * @param dataPageAddr Data page address.
     * @param dataIO Data page IO.
     * @param bucket Bucket index.
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if page was removed.
     */
    protected final boolean removeDataPage(Page dataPage, long dataPageAddr, DataPageIO dataIO, int bucket)
        throws IgniteCheckedException {
        long dataPageId = dataPage.id();

        long pageId = dataIO.getFreeListPageId(dataPageAddr);

        assert pageId != 0;

        try (Page page = page(pageId)) {
            long nextId;

            long recycleId = 0L;

            long pageAddr = writeLock(page); // Explicit check.

            if (pageAddr == 0L)
                return false;

            boolean rmvd = false;

            try {
                PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(pageAddr);

                rmvd = io.removePage(pageAddr, dataPageId);

                if (!rmvd)
                    return false;

                decrementBucketSize(bucket);

                if (isWalDeltaRecordNeeded(wal, page))
                    wal.log(new PagesListRemovePageRecord(cacheId, pageId, dataPageId));

                // Reset free list page ID.
                dataIO.setFreeListPageId(dataPageAddr, 0L);

                if (isWalDeltaRecordNeeded(wal, dataPage))
                    wal.log(new DataPageSetFreeListPageRecord(cacheId, dataPageId, 0L));

                if (!io.isEmpty(pageAddr))
                    return true; // In optimistic case we still have something in the page and can leave it as is.

                // If the page is empty, we have to try to drop it and link next and previous with each other.
                nextId = io.getNextId(pageAddr);

                // If there are no next page, then we can try to merge without releasing current write lock,
                // because if we will need to lock previous page, the locking order will be already correct.
                if (nextId == 0L) {
                    long prevId = io.getPreviousId(pageAddr);

                    recycleId = mergeNoNext(pageId, page, pageAddr, prevId, bucket);
                }
            }
            finally {
                writeUnlock(page, pageAddr, rmvd);
            }

            // Perform a fair merge after lock release (to have a correct locking order).
            if (nextId != 0L)
                recycleId = merge(pageId, page, nextId, bucket);

            if (recycleId != 0L)
                reuseList.addForRecycle(new SingletonReuseBag(recycleId));

            return true;
        }
    }

    /**
     * @param page Page.
     * @param pageId Page ID.
     * @param pageAddr Page address.
     * @param prevId Previous page ID.
     * @param bucket Bucket index.
     * @return Page ID to recycle.
     * @throws IgniteCheckedException If failed.
     */
    private long mergeNoNext(long pageId, Page page, long pageAddr, long prevId, int bucket)
        throws IgniteCheckedException {
        // If we do not have a next page (we are tail) and we are on reuse bucket,
        // then we can leave as is as well, because it is normal to have an empty tail page here.
        if (isReuseBucket(bucket))
            return 0L;

        if (prevId != 0L) { // Cut tail if we have a previous page.
            try (Page prev = page(prevId)) {
                Boolean ok = writePage(pageMem, prev, this, cutTail, null, bucket, FALSE);

                assert ok == TRUE: ok; // Because we keep lock on current tail and do a world consistency check.
            }
        }
        else {
            // If we don't have a previous, then we are tail page of free list, just drop the stripe.
            boolean rmvd = updateTail(bucket, pageId, 0L);

            if (!rmvd)
                return 0L;
        }

        return recyclePage(pageId, page, pageAddr);
    }

    /**
     * @param pageId Page ID.
     * @param page Page.
     * @param nextId Next page ID.
     * @param bucket Bucket index.
     * @return Page ID to recycle.
     * @throws IgniteCheckedException If failed.
     */
    private long merge(long pageId, Page page, long nextId, int bucket)
        throws IgniteCheckedException {
        assert nextId != 0; // We should do mergeNoNext then.

        // Lock all the pages in correct order (from next to previous) and do the merge in retry loop.
        for (;;) {
            try (Page next = nextId == 0L ? null : page(nextId)) {
                boolean write = false;

                long nextPageAddr = next == null ? 0L : writeLock(next); // Explicit check.
                long pageAddr = writeLock(page); // Explicit check.

                if (pageAddr == 0L) {
                    if (nextPageAddr != 0L) // Unlock next page if needed.
                        writeUnlock(next, nextPageAddr, false);

                    return 0L; // Someone has merged or taken our empty page concurrently. Nothing to do here.
                }

                try {
                    PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(pageAddr);

                    if (!io.isEmpty(pageAddr))
                        return 0L; // No need to merge anymore.

                    // Check if we see a consistent state of the world.
                    if (io.getNextId(pageAddr) == nextId && (nextId == 0L) == (nextPageAddr == 0L)) {
                        long recycleId = doMerge(pageId, page, pageAddr, io, next, nextId, nextPageAddr, bucket);

                        write = true;

                        return recycleId; // Done.
                    }

                    // Reread next page ID and go for retry.
                    nextId = io.getNextId(pageAddr);
                }
                finally {
                    if (nextPageAddr != 0L)
                        writeUnlock(next, nextPageAddr, write);

                    writeUnlock(page, pageAddr, write);
                }
            }
        }
    }

    /**
     * @param page Page.
     * @param pageId Page ID.
     * @param io IO.
     * @param pageAddr Page address.
     * @param next Next page.
     * @param nextId Next page ID.
     * @param nextPageAddr Next page address.
     * @param bucket Bucket index.
     * @return Page to recycle.
     * @throws IgniteCheckedException If failed.
     */
    private long doMerge(
        long pageId,
        Page page,
        long pageAddr,
        PagesListNodeIO io,
        Page next,
        long nextId,
        long nextPageAddr,
        int bucket
    ) throws IgniteCheckedException {
        long prevId = io.getPreviousId(pageAddr);

        if (nextId == 0L)
            return mergeNoNext(pageId, page, pageAddr, prevId, bucket);
        else {
            // No one must be able to merge it while we keep a reference.
            assert getPageId(nextPageAddr) == nextId;

            if (prevId == 0L) { // No previous page: we are at head.
                // These references must be updated at the same time in write locks.
                assert PagesListNodeIO.VERSIONS.forPage(nextPageAddr).getPreviousId(nextPageAddr) == pageId;

                PagesListNodeIO nextIO = PagesListNodeIO.VERSIONS.forPage(nextPageAddr);
                nextIO.setPreviousId(nextPageAddr, 0);

                if (isWalDeltaRecordNeeded(wal, next))
                    wal.log(new PagesListSetPreviousRecord(cacheId, nextId, 0L));
            }
            else // Do a fair merge: link previous and next to each other.
                fairMerge(prevId, pageId, nextId, next, nextPageAddr);

            return recyclePage(pageId, page, pageAddr);
        }
    }

    /**
     * Link previous and next to each other.
     *
     * @param prevId Previous Previous page ID.
     * @param pageId Page ID.
     * @param next Next page.
     * @param nextId Next page ID.
     * @param nextPageAddr Next page address.
     * @throws IgniteCheckedException If failed.
     */
    private void fairMerge(long prevId,
        long pageId,
        long nextId,
        Page next,
        long nextPageAddr)
        throws IgniteCheckedException {
        try (Page prev = page(prevId)) {
            long prevPageAddr = writeLock(prev); // No check, we keep a reference.

            assert prevPageAddr != 0L;

            try {
                PagesListNodeIO prevIO = PagesListNodeIO.VERSIONS.forPage(prevPageAddr);
                PagesListNodeIO nextIO = PagesListNodeIO.VERSIONS.forPage(nextPageAddr);

                // These references must be updated at the same time in write locks.
                assert prevIO.getNextId(prevPageAddr) == pageId;
                assert nextIO.getPreviousId(nextPageAddr) == pageId;

                prevIO.setNextId(prevPageAddr, nextId);

                if (isWalDeltaRecordNeeded(wal, prev))
                    wal.log(new PagesListSetNextRecord(cacheId, prevId, nextId));

                nextIO.setPreviousId(nextPageAddr, prevId);

                if (isWalDeltaRecordNeeded(wal, next))
                    wal.log(new PagesListSetPreviousRecord(cacheId, nextId, prevId));
            }
            finally {
                writeUnlock(prev, prevPageAddr, true);
            }
        }
    }

    /**
     * @param page Page.
     * @param pageId Page ID.
     * @param pageAddr Page address.
     * @return Rotated page ID.
     * @throws IgniteCheckedException If failed.
     */
    private long recyclePage(long pageId, Page page, long pageAddr) throws IgniteCheckedException {
        pageId = PageIdUtils.rotatePageId(pageId);

        PageIO.setPageId(pageAddr, pageId);

        if (isWalDeltaRecordNeeded(wal, page))
            wal.log(new RecycleRecord(cacheId, page.id(), pageId));

        return pageId;
    }

    /**
     * Increments bucket size and updates changed flag.
     *
     * @param bucket Bucket number.
     */
    private void incrementBucketSize(int bucket) {
        bucketsSize[bucket].incrementAndGet();

        // Ok to have a race here, see the field javadoc.
        if (!changed)
            changed = true;
    }

    /**
     * Increments bucket size and updates changed flag.
     *
     * @param bucket Bucket number.
     */
    private void decrementBucketSize(int bucket) {
        bucketsSize[bucket].decrementAndGet();

        // Ok to have a race here, see the field javadoc.
        if (!changed)
            changed = true;
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
        @Override public String toString() {
            return S.toString(SingletonReuseBag.class, this, "pageId", U.hexLong(pageId));
        }
    }

    /**
     *
     */
    public static final class Stripe {
        /** */
        public volatile long tailId;

        /** */
        volatile boolean empty;

        /**
         * @param tailId Tail ID.
         * @param empty Empty flag.
         */
        Stripe(long tailId, boolean empty) {
            this.tailId = tailId;
            this.empty = empty;
        }
    }
}
